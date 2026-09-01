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
// on a matching rule that carries a credential injection fetches a credential
// set from the credential provider and injects the headers it holds for the
// destination host. Actor identity comes from the CA-signed client cert the
// gateway verified on the CONNECT leg, relayed here as the ate.actor.identity
// filter-state attribute — never from a request header.
//
// A credential is a JSON "credential set" keyed by destination host, each value
// a map of complete HTTP header name→value pairs:
//
//	{
//	  "github.com":      {"Authorization": "Bearer <token>", "X-Custom": "v"},
//	  "api.example.com": {"X-Api-Key": "<key>"}
//	}
//
// The provider returns this blob verbatim; the injector selects the entry for
// the request's destination host and injects those headers literally. The
// policy's per-injection header/prefix fields are unused placeholders in this
// model — the header names and values come entirely from the credential set.
package egressinject

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
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
const actorIdentityAttribute = "filter_state['ate.actor.identity']"

// schemeHeader is the HTTP/2 pseudo-header carrying the request scheme. On the
// MITM leg the TLS chain yields "https" and the cleartext chain "http"; the
// injector refuses to inject a credential over anything but https.
const schemeHeader = ":scheme"

// NoMatchAction is what the handler does with a request no policy rule matches.
type NoMatchAction string

const (
	// NoMatchAllow lets an unmatched request proceed unchanged (no injection).
	NoMatchAllow NoMatchAction = "allow"
	// NoMatchDeny rejects an unmatched request with 403.
	NoMatchDeny NoMatchAction = "deny"
)

// ParseNoMatchAction validates a --on-no-match flag value.
func ParseNoMatchAction(s string) (NoMatchAction, error) {
	switch NoMatchAction(s) {
	case NoMatchAllow:
		return NoMatchAllow, nil
	case NoMatchDeny:
		return NoMatchDeny, nil
	default:
		return "", fmt.Errorf("invalid on-no-match %q (want allow or deny)", s)
	}
}

// credentialSet is a decoded credential-set secret: destination host → the HTTP
// headers to inject for requests to that host. Header values are complete (no
// prefix is applied by the injector).
type credentialSet map[string]map[string]string

// Handler injects credentials into matched egress requests.
type Handler struct {
	apiClient policyClient
	provider  credproviderpb.CredentialProviderClient
	onNoMatch NoMatchAction
}

// New builds the injector handler. apiClient fetches an actor's egress policy
// from the ateapi control plane per request.
func New(apiClient policyClient, provider credproviderpb.CredentialProviderClient, onNoMatch NoMatchAction) *Handler {
	return &Handler{apiClient: apiClient, provider: provider, onNoMatch: onNoMatch}
}

func (h *Handler) Direction() extproc.Direction { return extproc.DirectionEgressInject }

// HandleRequestHeaders fetches the actor's egress policy, matches the request
// against it, and on a matching credential-injection rule fetches the credential
// set and injects the headers it holds for the destination host. Unmatched
// requests follow onNoMatch; a policy or provider failure fails closed; a
// matched host with no entry in the credential set passes through unchanged.
func (h *Handler) HandleRequestHeaders(ctx context.Context, md *extproc.RequestMetadata) (extproc.Result, error) {
	identity := md.Attribute(actorIdentityAttribute)
	host := hostFromAuthority(md.Host)

	atespace, actor, err := parseActorURI(identity)
	if err != nil {
		// No usable identity means no policy can be fetched. Treat it as a
		// non-match so onNoMatch governs, but say why at the server.
		slog.WarnContext(ctx, "egress-inject: unusable actor identity", slog.String("host", host), slog.Any("err", err))
		return h.noMatch(host)
	}

	policy, err := h.apiClient.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actor},
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// The actor has no egress policy: nothing to inject.
			slog.InfoContext(ctx, "egress-inject: actor has no egress policy",
				slog.String("atespace", atespace), slog.String("actor", actor), slog.String("host", host))
			return h.noMatch(host)
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
		return h.noMatch(host)
	}
	if len(injections) == 0 {
		// A rule matched but injects nothing: the request is authorized, pass it
		// through unchanged.
		return passThrough(host), nil
	}

	// Refuse to inject over cleartext: on the cleartext MITM chain the request is
	// re-originated without upstream TLS, so a secret would leave the pod in the
	// clear. We refuse before fetching anything — test scheme != "https" (not ==
	// "http") so a missing or unknown scheme also fails closed. The egress-policy
	// API has no per-rule cleartext opt-in, so this refusal is unconditional for a
	// matched injection rule, even if the credential set turns out to hold no
	// entry for this host.
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
		// The rule authorizes egress, but no credential set holds an entry for
		// this host: nothing to inject, pass through unchanged.
		slog.InfoContext(ctx, "egress-inject: no credentials for host in the credential set",
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

// resolveHeaders fetches each injection's credential set and returns the header
// mutations for the entry matching host. A fetch failure, an unparseable set, or
// an unusable header name/value fails closed. A set with no entry for host
// contributes nothing (the caller treats an empty result as pass-through).
func (h *Handler) resolveHeaders(ctx context.Context, injections []*ateapipb.CredentialHeaderInjection, host, identity, atespace, actor string) ([]*corev3.HeaderValueOption, error) {
	lookupHost := normalizeHost(host)
	var setHeaders []*corev3.HeaderValueOption

	for _, inj := range injections {
		resp, err := h.provider.RequestSecret(ctx, &credproviderpb.RequestSecretRequest{
			Uri: inj.GetCredentialUri(),
			Context: &credproviderpb.SecretRequestContext{
				Scope:         credproviderpb.SecretScope_SECRET_SCOPE_EGRESS_CREDENTIAL_INJECTION,
				ActorIdentity: identity,
				Destination:   host,
			},
		})
		if err != nil {
			// Fail closed: a credential set we were told to consult but could not
			// fetch must not let the request out potentially missing a credential.
			slog.ErrorContext(ctx, "egress-inject: credential fetch failed",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
			return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
				"egress-inject: credential unavailable for %s", host)
		}

		set, err := parseCredentialSet(resp.GetSecret())
		if err != nil {
			slog.ErrorContext(ctx, "egress-inject: unusable credential set",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
			return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
				"egress-inject: unusable credential set for %s", host)
		}

		headers := set[lookupHost]
		// Inject headers in a deterministic order regardless of JSON/map ordering.
		for _, name := range sortedKeys(headers) {
			if err := validateInjectHeader(name); err != nil {
				slog.ErrorContext(ctx, "egress-inject: credential set names an unusable header",
					slog.String("atespace", atespace), slog.String("actor", actor),
					slog.String("host", host), slog.String("header", name), slog.Any("err", err))
				return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
					"egress-inject: credential set for %s names an unusable header", host)
			}
			value, err := sanitizeHeaderValue([]byte(headers[name]))
			if err != nil {
				slog.ErrorContext(ctx, "egress-inject: unusable credential value",
					slog.String("atespace", atespace), slog.String("actor", actor),
					slog.String("host", host), slog.String("header", name), slog.Any("err", err))
				return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
					"egress-inject: unusable credential value for %s", host)
			}
			// Overwrite any header the actor set itself, so a client cannot pre-seed
			// a value that survives injection.
			setHeaders = append(setHeaders, &corev3.HeaderValueOption{
				Header:       &corev3.HeaderValue{Key: name, RawValue: value},
				AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
			})
		}
	}
	return setHeaders, nil
}

// parseCredentialSet decodes a credential-set secret: a JSON object mapping a
// destination host to the headers to inject for it. Host keys are normalized so
// lookup is case- and trailing-dot-insensitive. A blob that is not such an
// object (e.g. a bare token) is rejected so a misconfigured secret fails closed.
func parseCredentialSet(b []byte) (credentialSet, error) {
	var raw map[string]map[string]string
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("credential set is not a JSON object of host->headers: %w", err)
	}
	set := make(credentialSet, len(raw))
	for host, headers := range raw {
		set[normalizeHost(host)] = headers
	}
	return set, nil
}

// normalizeHost lowercases a hostname and drops a trailing dot so credential-set
// keys and request hosts compare equal regardless of case or an absolute-form
// trailing dot.
func normalizeHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

// sortedKeys returns the keys of m in ascending order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sanitizeHeaderValue prepares a credential-set value for use as an HTTP header
// value. It trims a trailing newline (a secret created from a file commonly
// carries one), then rejects an empty value or one containing any control
// character, which Envoy would reject as an invalid header value and, in the
// case of CR/LF, would allow header injection.
func sanitizeHeaderValue(value []byte) ([]byte, error) {
	value = bytes.TrimRight(value, "\r\n")
	if len(value) == 0 {
		return nil, fmt.Errorf("credential value is empty")
	}
	for _, b := range value {
		if b < 0x20 || b == 0x7f {
			return nil, fmt.Errorf("credential value contains a control character")
		}
	}
	return value, nil
}

// passThrough is the result that authorizes a request without mutating it.
func passThrough(host string) extproc.Result {
	return extproc.Result{Target: host, Response: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}}}
}

// noMatch applies the configured no-match action.
func (h *Handler) noMatch(host string) (extproc.Result, error) {
	if h.onNoMatch == NoMatchDeny {
		return extproc.Result{Target: host}, extproc.NewReqError(envoy_type.StatusCode_Forbidden,
			"egress-inject: no policy permits egress to %s", host)
	}
	// Allow: proceed unchanged.
	return passThrough(host), nil
}
