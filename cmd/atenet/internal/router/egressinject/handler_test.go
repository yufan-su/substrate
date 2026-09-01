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

package egressinject

import (
	"context"
	"errors"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// fakeProvider is a stub CredentialProviderClient recording the last request.
type fakeProvider struct {
	resp   *credproviderpb.RequestSecretResponse
	err    error
	gotReq *credproviderpb.RequestSecretRequest
}

func (f *fakeProvider) RequestSecret(_ context.Context, req *credproviderpb.RequestSecretRequest, _ ...grpc.CallOption) (*credproviderpb.RequestSecretResponse, error) {
	f.gotReq = req
	return f.resp, f.err
}

// providerWithSet returns a fakeProvider serving the given credential-set JSON.
func providerWithSet(json string) *fakeProvider {
	return &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte(json)}}
}

// fakePolicyClient is a stub ateapi policy client: it returns the policy keyed
// by the requested actor, NotFound when the actor is absent, or a fixed error.
type fakePolicyClient struct {
	policies map[string]*ateapipb.EgressPolicy // keyed by "atespace/actor"
	err      error
	gotReq   *ateapipb.GetActorEgressPolicyRequest
}

func (f *fakePolicyClient) GetActorEgressPolicy(_ context.Context, in *ateapipb.GetActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.gotReq = in
	if f.err != nil {
		return nil, f.err
	}
	p, ok := f.policies[in.GetActor().GetAtespace()+"/"+in.GetActor().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no egress policy")
	}
	return p, nil
}

// sampleAPIClient serves the sample policy for team-a/my-actor.
func sampleAPIClient() *fakePolicyClient {
	return &fakePolicyClient{policies: map[string]*ateapipb.EgressPolicy{
		"team-a/my-actor": sampleEgressPolicy(),
	}}
}

const testActorURI = "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor"

// sampleSet is a credential set holding two headers for api.example.com.
const sampleSet = `{"api.example.com": {"Authorization": "Bearer s3cr3t", "X-Custom-Header": "value"}}`

func metadataFor(t *testing.T, identity, host string) *extproc.RequestMetadata {
	t.Helper()
	// Default to https so the HTTPS path is what unscoped tests exercise.
	return metadataForScheme(t, identity, host, "https")
}

func metadataForScheme(t *testing.T, identity, host, scheme string) *extproc.RequestMetadata {
	t.Helper()
	st, err := structpb.NewStruct(map[string]any{actorIdentityAttribute: identity})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	md := &extproc.RequestMetadata{
		Host:       host,
		Attributes: map[string]*structpb.Struct{"efp": st},
	}
	if scheme != "" {
		md.Headers = map[string]string{schemeHeader: scheme}
	}
	return md
}

func TestHandleRequestHeadersInjects(t *testing.T) {
	provider := providerWithSet(sampleSet)
	h := New(sampleAPIClient(), provider, NoMatchAllow)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com:443"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}

	// Both headers from the host's entry are injected, in sorted order.
	setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders()
	if len(setHeaders) != 2 {
		t.Fatalf("got %d header mutations, want 2", len(setHeaders))
	}
	got := map[string]string{}
	for _, hv := range setHeaders {
		got[hv.GetHeader().GetKey()] = string(hv.GetHeader().GetRawValue())
		if hv.GetAppendAction() != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
			t.Errorf("append action = %v, want OVERWRITE_IF_EXISTS_OR_ADD", hv.GetAppendAction())
		}
	}
	if got["Authorization"] != "Bearer s3cr3t" {
		t.Errorf("Authorization = %q, want %q", got["Authorization"], "Bearer s3cr3t")
	}
	if got["X-Custom-Header"] != "value" {
		t.Errorf("X-Custom-Header = %q, want %q", got["X-Custom-Header"], "value")
	}
	if setHeaders[0].GetHeader().GetKey() != "Authorization" || setHeaders[1].GetHeader().GetKey() != "X-Custom-Header" {
		t.Errorf("headers not in sorted order: %q, %q", setHeaders[0].GetHeader().GetKey(), setHeaders[1].GetHeader().GetKey())
	}

	// The provider was asked for the policy's URI with the attested context.
	if got := provider.gotReq.GetUri(); got != "substrate-secret://secretmanager.googleapis.com/projects/yufans-test/secrets/egress-creds/versions/latest" {
		t.Errorf("provider URI = %q", got)
	}
	if got := provider.gotReq.GetContext().GetScope(); got != credproviderpb.SecretScope_SECRET_SCOPE_EGRESS_CREDENTIAL_INJECTION {
		t.Errorf("scope = %v", got)
	}
	if got := provider.gotReq.GetContext().GetActorIdentity(); got != testActorURI {
		t.Errorf("actor identity = %q", got)
	}
	if got := provider.gotReq.GetContext().GetDestination(); got != "api.example.com" {
		t.Errorf("destination = %q, want api.example.com", got)
	}
}

func TestHandleRequestHeadersFetchesPolicyForActor(t *testing.T) {
	api := sampleAPIClient()
	h := New(api, providerWithSet(sampleSet), NoMatchAllow)

	if _, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com")); err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if got := api.gotReq.GetActor(); got.GetAtespace() != "team-a" || got.GetName() != "my-actor" {
		t.Errorf("GetActorEgressPolicy actor = %+v, want team-a/my-actor", got)
	}
}

func TestHandleRequestHeadersCleartextDenied(t *testing.T) {
	provider := providerWithSet(sampleSet)
	h := New(sampleAPIClient(), provider, NoMatchAllow)

	// A matched rule carries an injection; the API has no cleartext opt-in, so
	// an http request is always refused before the credential is fetched.
	_, err := h.HandleRequestHeaders(context.Background(), metadataForScheme(t, testActorURI, "api.example.com", "http"))
	assertReqErrCode(t, err, envoy_type.StatusCode_Forbidden)
	if provider.gotReq != nil {
		t.Error("provider was called for a cleartext request that should have been refused")
	}
}

func TestHandleRequestHeadersMissingSchemeDenied(t *testing.T) {
	provider := providerWithSet(sampleSet)
	h := New(sampleAPIClient(), provider, NoMatchAllow)

	// An absent scheme must fail closed, not be treated as https.
	_, err := h.HandleRequestHeaders(context.Background(), metadataForScheme(t, testActorURI, "api.example.com", ""))
	assertReqErrCode(t, err, envoy_type.StatusCode_Forbidden)
	if provider.gotReq != nil {
		t.Error("provider was called for a request with no scheme")
	}
}

func TestHandleRequestHeadersHostNotInSetPassesThrough(t *testing.T) {
	// The rule matches api.example.com (authorized), but the credential set only
	// carries an entry for another host: nothing to inject, pass through.
	provider := providerWithSet(`{"github.com": {"Authorization": "Bearer other"}}`)
	h := New(sampleAPIClient(), provider, NoMatchDeny)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if muts := res.Response.GetResponse().GetHeaderMutation(); muts != nil {
		t.Errorf("got header mutation %+v, want none", muts)
	}
	if provider.gotReq == nil {
		t.Error("provider should have been consulted for the credential set")
	}
}

func TestHandleRequestHeadersMatchedNoInjectionPassesThrough(t *testing.T) {
	// A rule matches but injects nothing: the request is authorized and passes
	// through unchanged, over cleartext too (no secret leaves the pod).
	api := &fakePolicyClient{policies: map[string]*ateapipb.EgressPolicy{
		"team-a/my-actor": {Rules: []*ateapipb.EgressRule{{
			Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}},
		}}},
	}}
	provider := &fakeProvider{}
	h := New(api, provider, NoMatchDeny)

	res, err := h.HandleRequestHeaders(context.Background(), metadataForScheme(t, testActorURI, "api.example.com", "http"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if muts := res.Response.GetResponse().GetHeaderMutation(); muts != nil {
		t.Errorf("got header mutation %+v, want none", muts)
	}
	if provider.gotReq != nil {
		t.Error("provider was called for a rule with no injection")
	}
}

func TestHandleRequestHeadersNoMatchAllow(t *testing.T) {
	provider := &fakeProvider{}
	h := New(sampleAPIClient(), provider, NoMatchAllow)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "other.example.com"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if muts := res.Response.GetResponse().GetHeaderMutation(); muts != nil {
		t.Errorf("got header mutation %+v, want none", muts)
	}
	if provider.gotReq != nil {
		t.Error("provider was called on a non-matching request")
	}
}

func TestHandleRequestHeadersNoMatchDeny(t *testing.T) {
	h := New(sampleAPIClient(), &fakeProvider{}, NoMatchDeny)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "other.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_Forbidden)
}

func TestHandleRequestHeadersNoPolicyForActorFollowsNoMatch(t *testing.T) {
	provider := &fakeProvider{}
	// The API has no policy for this actor (NotFound). Deny so it is observable.
	h := New(&fakePolicyClient{}, provider, NoMatchDeny)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_Forbidden)
	if provider.gotReq != nil {
		t.Error("provider was called though the actor has no egress policy")
	}
}

func TestHandleRequestHeadersPolicyFetchFailsClosed(t *testing.T) {
	provider := &fakeProvider{}
	// A transport-level failure (not NotFound) must fail closed even under allow.
	h := New(&fakePolicyClient{err: status.Error(codes.Unavailable, "ateapi down")}, provider, NoMatchAllow)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
	if provider.gotReq != nil {
		t.Error("provider was called though the egress policy could not be fetched")
	}
}

func TestHandleRequestHeadersProviderFailsClosed(t *testing.T) {
	provider := &fakeProvider{err: errors.New("provider down")}
	h := New(sampleAPIClient(), provider, NoMatchAllow)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

func TestHandleRequestHeadersMalformedSetFailsClosed(t *testing.T) {
	// A blob that is not a host->headers JSON object (e.g. a bare token, or empty)
	// must fail closed rather than be injected blindly.
	for _, blob := range []string{"", "s3cr3t", `{"api.example.com": "not-an-object"}`} {
		provider := providerWithSet(blob)
		h := New(sampleAPIClient(), provider, NoMatchAllow)
		_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
		assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
	}
}

func TestHandleRequestHeadersControlCharValueFailsClosed(t *testing.T) {
	// An embedded CR/LF in a value would enable header injection; fail closed.
	provider := providerWithSet(`{"api.example.com": {"X-Api-Key": "a\r\nb"}}`)
	h := New(sampleAPIClient(), provider, NoMatchAllow)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

func TestHandleRequestHeadersUnusableHeaderNameFailsClosed(t *testing.T) {
	// A credential set naming a system header the gateway forbids mutating must
	// fail closed rather than be dropped silently.
	provider := providerWithSet(`{"api.example.com": {"Host": "evil.example.com"}}`)
	h := New(sampleAPIClient(), provider, NoMatchAllow)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

func TestParseCredentialSet(t *testing.T) {
	set, err := parseCredentialSet([]byte(`{"API.Example.com.": {"Authorization": "Bearer x"}}`))
	if err != nil {
		t.Fatalf("parseCredentialSet: %v", err)
	}
	// Host keys are normalized (lowercased, trailing dot dropped) for lookup.
	if got := set["api.example.com"]["Authorization"]; got != "Bearer x" {
		t.Errorf("normalized lookup = %q, want %q", got, "Bearer x")
	}
	for _, bad := range []string{"", "s3cr3t", "[]", `{"h": "not-an-object"}`} {
		if _, err := parseCredentialSet([]byte(bad)); err == nil {
			t.Errorf("parseCredentialSet(%q) = nil error, want error", bad)
		}
	}
}

func TestSanitizeHeaderValue(t *testing.T) {
	tests := []struct {
		name    string
		in      []byte
		want    string
		wantErr bool
	}{
		{name: "plain", in: []byte("tok"), want: "tok"},
		{name: "trailing newline trimmed", in: []byte("tok\n"), want: "tok"},
		{name: "trailing crlf trimmed", in: []byte("tok\r\n"), want: "tok"},
		{name: "empty", in: []byte(""), wantErr: true},
		{name: "only newline", in: []byte("\n"), wantErr: true},
		{name: "embedded lf", in: []byte("to\nk"), wantErr: true},
		{name: "embedded cr", in: []byte("to\rk"), wantErr: true},
		{name: "embedded tab", in: []byte("to\tk"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanitizeHeaderValue(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("sanitizeHeaderValue(%q) = %q, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("sanitizeHeaderValue(%q) unexpected error: %v", tc.in, err)
			}
			if string(got) != tc.want {
				t.Errorf("sanitizeHeaderValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestHandleRequestHeadersBadIdentityFollowsNoMatch(t *testing.T) {
	provider := &fakeProvider{}
	// Deny so an unusable identity is observably rejected rather than silently allowed.
	h := New(sampleAPIClient(), provider, NoMatchDeny)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, "not-a-spiffe-uri", "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_Forbidden)
	if provider.gotReq != nil {
		t.Error("provider was called despite an unusable identity")
	}
}

func assertReqErrCode(t *testing.T, err error, want envoy_type.StatusCode) {
	t.Helper()
	var reqErr *extproc.ReqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error %v is not a *extproc.ReqError", err)
	}
	if reqErr.StatusCode != int(want) {
		t.Errorf("status code = %d, want %d", reqErr.StatusCode, int(want))
	}
}
