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

// This file implements the CredentialProvider plugin API backed by Google
// Cloud Secret Manager. It resolves ate-secret:// URIs of the provider
// "secretmanager.googleapis.com" to a secret version's payload read straight
// from the Secret Manager API — so Substrate never stores the secret, it only
// brokers an access the provider's own identity is authorized to perform.
package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net/url"
	"strings"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// ProviderName is the ate-secret:// URI host this backend serves.
const ProviderName = "secretmanager.googleapis.com"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "ate-secret"

// defaultVersion is the Secret Manager version alias used when a URI omits one.
const defaultVersion = "latest"

// crc32cTable is the polynomial Secret Manager checksums payloads with.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// SecretRef is a parsed ate-secret:// URI for the secretmanager.googleapis.com
// provider. The path is the Secret Manager resource name, with the version
// optional:
//
//	ate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>/versions/<version>
//	ate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>
//
// The second form resolves the "latest" version alias.
type SecretRef struct {
	// Project is the Google Cloud project ID or number owning the secret.
	Project string
	// Secret is the Secret Manager secret ID.
	Secret string
	// Version is the secret version: a positive integer or the alias "latest".
	Version string
}

// ResourceName is the Secret Manager resource name of the referenced version.
func (r SecretRef) ResourceName() string {
	return fmt.Sprintf("projects/%s/secrets/%s/versions/%s", r.Project, r.Secret, r.Version)
}

// ParseURI parses an ate-secret:// URI of the secretmanager.googleapis.com
// provider. It rejects any other scheme or provider name, and a malformed
// resource name, so a misrouted URI fails loudly rather than accessing the
// wrong store.
func ParseURI(raw string) (SecretRef, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return SecretRef{}, fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != uriScheme {
		return SecretRef{}, fmt.Errorf("malformed credential URI %q: scheme is %q, want %q", raw, u.Scheme, uriScheme)
	}
	if u.Host != ProviderName {
		return SecretRef{}, fmt.Errorf("credential URI %q: provider is %q, this provider serves %q", raw, u.Host, ProviderName)
	}
	// The grammar is scheme/host/path only; a query or fragment means the caller
	// assumed a syntax this provider does not honor, so reject it rather than
	// silently ignore it.
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return SecretRef{}, fmt.Errorf("credential URI %q: query and fragment components are not allowed", raw)
	}
	// A Secret Manager resource name never needs percent-encoding.
	if u.EscapedPath() != u.Path {
		return SecretRef{}, fmt.Errorf("credential URI %q: path must not contain percent-encoding", raw)
	}

	const want = "projects/<project>/secrets/<secret>[/versions/<version>]"
	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(segments) != 4 && len(segments) != 6 {
		return SecretRef{}, fmt.Errorf("credential URI %q: want %s, got path %q", raw, want, u.Path)
	}
	for i, s := range segments {
		if s == "" {
			return SecretRef{}, fmt.Errorf("credential URI %q: empty path segment %d", raw, i)
		}
	}
	if segments[0] != "projects" || segments[2] != "secrets" {
		return SecretRef{}, fmt.Errorf("credential URI %q: want %s, got path %q", raw, want, u.Path)
	}
	ref := SecretRef{Project: segments[1], Secret: segments[3], Version: defaultVersion}
	if len(segments) == 6 {
		if segments[4] != "versions" {
			return SecretRef{}, fmt.Errorf("credential URI %q: want %s, got path %q", raw, want, u.Path)
		}
		if !validVersion(segments[5]) {
			return SecretRef{}, fmt.Errorf("credential URI %q: version %q must be a positive integer or %q", raw, segments[5], defaultVersion)
		}
		ref.Version = segments[5]
	}
	return ref, nil
}

// validVersion reports whether v names a Secret Manager version: the "latest"
// alias or a positive integer without leading zeros.
func validVersion(v string) bool {
	if v == defaultVersion {
		return true
	}
	if v == "" || v[0] == '0' {
		return false
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// secretAccessor is the subset of the Secret Manager client this backend needs.
// The real *secretmanager.Client satisfies it; tests supply a fake.
type secretAccessor interface {
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
}

// Server implements credproviderpb.CredentialProviderServer over Google Cloud
// Secret Manager.
//
// It applies no per-actor authorization: every caller the mTLS layer admits
// (the egress gateway) may resolve any secret the provider's own identity can
// access. What an actor reaches is bounded by the secrets that identity is
// granted and by the URIs its EgressPolicy names. An atespace→project mapping,
// the counterpart of the Kubernetes provider's atespace→namespace policy, is
// future work.
type Server struct {
	credproviderpb.UnimplementedCredentialProviderServer

	client secretAccessor
}

// NewServer builds a Secret Manager-backed credential provider.
func NewServer(client secretAccessor) *Server {
	return &Server{client: client}
}

// FetchSecret resolves one ate-secret:// URI to its secret version's payload,
// returned verbatim as the credential's raw bytes. The caller decides how to
// use them; the egress gateway prepends the policy's prefix.
func (s *Server) FetchSecret(ctx context.Context, req *credproviderpb.FetchSecretRequest) (*credproviderpb.FetchSecretResponse, error) {
	ref, err := ParseURI(req.GetUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	slog.InfoContext(ctx, "resolving credential",
		slog.String("provider", ProviderName),
		slog.String("project", ref.Project),
		slog.String("secret", ref.Secret),
		slog.String("version", ref.Version),
		slog.String("actor", req.GetActorSpiffeId()),
	)

	resp, err := s.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: ref.ResourceName()})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			return nil, status.Errorf(codes.NotFound, "secret version %s not found", ref.ResourceName())
		case codes.PermissionDenied:
			return nil, status.Errorf(codes.PermissionDenied, "not permitted to access secret version %s", ref.ResourceName())
		default:
			return nil, status.Errorf(codes.Unavailable, "accessing secret version %s: %v", ref.ResourceName(), err)
		}
	}

	payload := resp.GetPayload()
	if payload == nil {
		return nil, status.Errorf(codes.NotFound, "secret version %s has no payload", ref.ResourceName())
	}
	// Verify the payload against the server-provided CRC32C when present, so a
	// corrupted response fails closed rather than injecting a mangled credential.
	if payload.DataCrc32C != nil {
		if got := int64(crc32.Checksum(payload.GetData(), crc32cTable)); got != payload.GetDataCrc32C() {
			return nil, status.Errorf(codes.Unavailable, "secret version %s: payload CRC32C mismatch", ref.ResourceName())
		}
	}
	return &credproviderpb.FetchSecretResponse{OpaqueBytes: payload.GetData()}, nil
}
