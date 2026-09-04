// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package egressinject implements the ext_proc handler that runs on the egress
// gateway's decrypted MITM leg: it fetches the requesting actor's egress policy
// from the ateapi control plane, matches each outbound request against it, and
// on a matching rule that carries a credential injection asks the credential
// provider for the value to inject and sets it as a request header. Actor
// identity comes from the CA-signed client cert the gateway verified on the
// CONNECT leg, relayed here as the ate.actor.identity filter-state attribute —
// never from a request header.
//
// The header name and prefix come from the egress policy's inject_static_headers
// effect. For each such header the injector calls the provider with the
// credential URI, the destination host, and the header name; the provider
// resolves the value for that (destination, header) from whatever it stores and
// returns just that value. The injector then sets the header to prefix+value,
// overwriting any value the actor sent. Keeping the lookup in the provider means
// the injector never parses the credential's storage format and behaves the same
// across providers.
package egressinject

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// actorIdentityAttribute is the CEL request attribute the MITM ext_proc filter
// forwards, carrying the actor's verified identity URI. It is the filter-state
// object ate.actor.identity the CONNECT leg set from the peer cert's URI SAN.
const actorIdentityAttribute = "filter_state['dev.ate.actor.identity']"

// schemeHeader is the HTTP/2 pseudo-header carrying the request scheme. On the
// MITM leg the TLS chain yields "https" and the cleartext chain "http"; the
// injector refuses to inject a credential over anything but https.
const schemeHeader = ":scheme"

// Handler injects credentials into matched egress requests.
type Handler struct {
	apiClient policyClient
	provider  credproviderpb.CredentialProviderClient
	// providerClass is the credential-provider class this injector serves (e.g.
	// "kubernetes.io", parsed from the substrate-secret:// prefix). A credential
	// URI of any other class is refused. Empty disables the check (dev only).
	providerClass string
}

// New builds the injector handler. apiClient fetches an actor's egress policy
// from the ateapi control plane per request; providerClass is the credential
// provider class this injector serves, and a policy URI of another class fails
// closed.
func New(apiClient policyClient, provider credproviderpb.CredentialProviderClient, providerClass string) *Handler {
	return &Handler{apiClient: apiClient, provider: provider, providerClass: providerClass}
}

func (h *Handler) Direction() extproc.Direction { return extproc.DirectionEgressInject }

// HandleRequestHeaders fetches the actor's egress policy, matches the request
// against it, and on a matching credential-injection rule asks the provider for
// each named header's value and injects it. The injector is not an egress
// authorization gate: a request with no matching injection passes through
// unchanged. A policy or provider failure fails closed; a matched rule whose
// headers the provider resolves to no value passes through unchanged.
func (h *Handler) HandleRequestHeaders(ctx context.Context, md *extproc.RequestMetadata) (extproc.Result, error) {
	identity := md.Attribute(actorIdentityAttribute)
	host := hostFromAuthority(md.Host)

	atespace, actor, err := parseActorURI(identity)
	if err != nil {
		// No usable identity means no policy can be fetched, so there is nothing
		// to inject; pass the request through, but say why at the server.
		slog.WarnContext(ctx, "egress-inject: unusable actor identity", slog.String("host", host), slog.Any("err", err))
		return passThrough(host), nil
	}

	policy, err := h.apiClient.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actor},
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// The actor has no egress policy: nothing to inject.
			slog.InfoContext(ctx, "egress-inject: actor has no egress policy",
				slog.String("atespace", atespace), slog.String("actor", actor), slog.String("host", host))
			return passThrough(host), nil
		}
		// Fail closed: we cannot tell whether a credential was required, so the
		// request must not go out potentially missing it.
		slog.ErrorContext(ctx, "egress-inject: fetching egress policy failed",
			slog.String("atespace", atespace), slog.String("actor", actor),
			slog.String("host", host), slog.Any("err", err))
		return extproc.Result{Target: host}, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
			"egress-inject: egress policy unavailable for %s", host)
	}

	injections, matched := evaluate(policy, host)
	if !matched {
		slog.InfoContext(ctx, "egress-inject: no matching policy rule",
			slog.String("atespace", atespace), slog.String("actor", actor), slog.String("host", host))
		return passThrough(host), nil
	}
	if len(injections) == 0 {
		// A rule matched but injects nothing: pass the request through unchanged.
		return passThrough(host), nil
	}

	// Refuse to inject over cleartext: on the cleartext MITM chain the request is
	// re-originated without upstream TLS, so a secret would leave the pod in the
	// clear. We refuse before fetching anything — test scheme != "https" (not ==
	// "http") so a missing or unknown scheme also fails closed. The egress-policy
	// API has no per-rule cleartext opt-in, so this refusal is unconditional for a
	// matched injection rule, even if the provider turns out to resolve no value
	// for this host.
	if scheme := strings.ToLower(md.Header(schemeHeader)); scheme != "https" {
		slog.WarnContext(ctx, "egress-inject: refusing to inject credential over cleartext",
			slog.String("atespace", atespace), slog.String("actor", actor),
			slog.String("host", host), slog.String("scheme", scheme))
		return extproc.Result{Target: host}, extproc.NewReqError(envoy_type.StatusCode_Forbidden,
			"egress-inject: refusing to inject a credential over cleartext to %s", host)
	}

	setHeaders, err := h.resolveHeaders(ctx, injections, host, identity, atespace, actor)
	if err != nil {
		return extproc.Result{Target: host}, err
	}
	if len(setHeaders) == 0 {
		// The rule matched, but the provider resolved no value for any of its
		// headers at this host: nothing to inject, pass through unchanged.
		slog.InfoContext(ctx, "egress-inject: no credential resolved for host",
			slog.String("atespace", atespace), slog.String("actor", actor), slog.String("host", host))
		return passThrough(host), nil
	}

	slog.InfoContext(ctx, "egress-inject: injecting credential(s)",
		slog.String("atespace", atespace), slog.String("actor", actor),
		slog.String("host", host), slog.Int("headers", len(setHeaders)))

	return extproc.Result{
		Target: host,
		Response: &extprocv3.HeadersResponse{
			Response: &extprocv3.CommonResponse{
				HeaderMutation: &extprocv3.HeaderMutation{SetHeaders: setHeaders},
			},
		},
	}, nil
}

// resolveHeaders asks the provider for each injection's value and returns the
// header mutations to apply. Header name and prefix come from the policy; the
// provider resolves the value for (destination, header). A fetch failure or an
// unusable header name/value fails closed. An injection the provider resolves to
// no value contributes nothing (the caller treats an empty result as
// pass-through).
func (h *Handler) resolveHeaders(ctx context.Context, injections []*ateapipb.CredentialHeaderInjection, host, identity, atespace, actor string) ([]*corev3.HeaderValueOption, error) {
	var setHeaders []*corev3.HeaderValueOption

	for _, inj := range injections {
		if err := validateInjectHeader(inj.GetHeader()); err != nil {
			slog.ErrorContext(ctx, "egress-inject: policy names an unusable header",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("header", inj.GetHeader()), slog.Any("err", err))
			return nil, extproc.WrapReqError(envoy_type.StatusCode_InternalServerError, err,
				"egress-inject: policy for %s names an unusable header", host)
		}

		// Confirm the credential URI targets the provider class this injector
		// serves before dialing it: the configured provider fronts one class, so a
		// URI of another class cannot be resolved here and must fail closed rather
		// than be sent to the wrong provider.
		if h.providerClass != "" {
			class, err := credentialURIClass(inj.GetCredentialUri())
			if err != nil {
				slog.ErrorContext(ctx, "egress-inject: policy names an unparseable credential URI",
					slog.String("atespace", atespace), slog.String("actor", actor),
					slog.String("host", host), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
				return nil, extproc.WrapReqError(envoy_type.StatusCode_InternalServerError, err,
					"egress-inject: policy for %s names an unparseable credential URI", host)
			}
			if class != h.providerClass {
				slog.ErrorContext(ctx, "egress-inject: credential URI targets an unserved provider class",
					slog.String("atespace", atespace), slog.String("actor", actor),
					slog.String("host", host), slog.String("uri", inj.GetCredentialUri()),
					slog.String("class", class), slog.String("provider", h.providerClass))
				return nil, extproc.NewReqError(envoy_type.StatusCode_InternalServerError,
					"egress-inject: credential URI for %s targets provider class %q, this injector serves %q", host, class, h.providerClass)
			}
		}

		// The provider resolves the value for this (destination, header) from the
		// credential it fronts and returns just that value; we name and format the
		// header ourselves.
		resp, err := h.provider.RequestSecret(ctx, &credproviderpb.RequestSecretRequest{
			Uri: inj.GetCredentialUri(),
			Context: &credproviderpb.SecretRequestContext{
				ActorIdentity: identity,
				Destination:   host,
				HeaderKey:     inj.GetHeader(),
			},
		})
		if err != nil {
			// Fail closed: a credential we were told to inject but could not fetch
			// must not let the request out potentially missing it.
			slog.ErrorContext(ctx, "egress-inject: credential fetch failed",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
			return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
				"egress-inject: credential unavailable for %s", host)
		}

		value, err := sanitizeHeaderValue(resp.GetSecret())
		if err != nil {
			// Fail closed: a value Envoy would reject (control chars) or that could
			// enable header injection (CR/LF) must not go upstream.
			slog.ErrorContext(ctx, "egress-inject: unusable credential value",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("header", inj.GetHeader()), slog.Any("err", err))
			return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
				"egress-inject: unusable credential value for %s", host)
		}
		if len(value) == 0 {
			// The provider resolved no value for this header at the destination:
			// inject nothing for it (do not send a bare prefix).
			continue
		}

		// Overwrite any header the actor set itself, so a client cannot pre-seed a
		// value that survives injection.
		setHeaders = append(setHeaders, &corev3.HeaderValueOption{
			Header:       &corev3.HeaderValue{Key: inj.GetHeader(), RawValue: append([]byte(inj.GetPrefix()), value...)},
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		})
	}
	return setHeaders, nil
}

// sanitizeHeaderValue prepares a resolved credential value for use as (part of)
// an HTTP header value. It trims a trailing newline (a secret created from a file
// commonly carries one), then rejects any control character, which Envoy would
// reject as an invalid header value and, in the case of CR/LF, would allow
// header injection. An empty value is returned as-is; the caller treats it as
// "no value to inject" rather than sending a bare prefix.
func sanitizeHeaderValue(value []byte) ([]byte, error) {
	value = bytes.TrimRight(value, "\r\n")
	for _, b := range value {
		if b < 0x20 || b == 0x7f {
			return nil, fmt.Errorf("credential value contains a control character")
		}
	}
	return value, nil
}

// passThrough lets the request proceed unchanged, injecting nothing. The
// injector makes no egress allow/deny decision: a request with no credential to
// inject is simply forwarded.
func passThrough(host string) extproc.Result {
	return extproc.Result{Target: host, Response: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}}}
}
