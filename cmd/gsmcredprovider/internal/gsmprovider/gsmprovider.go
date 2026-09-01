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

// Package gsmprovider implements the CredentialProvider plugin API backed by
// Google Cloud Secret Manager. It resolves substrate-secret:// URIs of the
// class "secretmanager.googleapis.com" to a secret version's payload accessed
// straight from the Secret Manager API — so Substrate never stores the secret,
// it only brokers an access the provider is authorized to perform.
//
// This POC performs no per-actor authorization: any request resolves any secret
// the provider's Secret Manager access permits. Authorization is left to the
// Secret Manager IAM granted to the provider's identity.
package gsmprovider

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net/url"
	"strings"

	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// ProviderClass is the substrate-secret:// URI host this backend serves. It is
// distinct from the kubernetes.io class so that, once the injector routes by
// URI class, a request is dispatched here on the class alone.
const ProviderClass = "secretmanager.googleapis.com"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "substrate-secret"

// defaultVersion is the Secret Manager version alias used when a URI omits one.
const defaultVersion = "latest"

// SecretRef is a parsed substrate-secret:// URI for the
// secretmanager.googleapis.com class. The URI path is the Secret Manager
// resource name, with the version optional:
//
//	substrate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>/versions/<version>
//	substrate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>            (version defaults to "latest")
type SecretRef struct {
	// Project is the Google Cloud project ID owning the secret.
	Project string
	// Secret is the Secret Manager secret ID.
	Secret string
	// Version is the secret version, a positive integer or the alias "latest".
	// It defaults to "latest" when the URI omits the "versions/<version>" tail.
	Version string
}

// ResourceName is the Secret Manager resource name for the referenced version.
func (r SecretRef) ResourceName() string {
	return fmt.Sprintf("projects/%s/secrets/%s/versions/%s", r.Project, r.Secret, r.Version)
}

// ParseURI parses a substrate-secret:// URI of the secretmanager.googleapis.com
// class. Its path is a Secret Manager resource name. It rejects any other
// scheme, provider class, or malformed resource name so a misrouted URI fails
// loudly rather than accessing the wrong store.
func ParseURI(raw string) (SecretRef, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return SecretRef{}, fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != uriScheme {
		return SecretRef{}, fmt.Errorf("credential URI %q: scheme is %q, want %q", raw, u.Scheme, uriScheme)
	}
	if u.Host != ProviderClass {
		return SecretRef{}, fmt.Errorf("credential URI %q: provider class is %q, this provider serves %q", raw, u.Host, ProviderClass)
	}

	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	// The path is the Secret Manager resource name:
	//   projects/<project>/secrets/<secret>[/versions/<version>]
	if len(segments) != 4 && len(segments) != 6 {
		return SecretRef{}, fmt.Errorf("credential URI %q: want projects/<project>/secrets/<secret>[/versions/<version>], got path %q", raw, u.Path)
	}
	for i, s := range segments {
		if s == "" {
			return SecretRef{}, fmt.Errorf("credential URI %q: empty path segment %d", raw, i)
		}
	}
	if segments[0] != "projects" || segments[2] != "secrets" {
		return SecretRef{}, fmt.Errorf("credential URI %q: want projects/<project>/secrets/<secret>[/versions/<version>], got path %q", raw, u.Path)
	}

	ref := SecretRef{
		Project: segments[1],
		Secret:  segments[3],
		Version: defaultVersion,
	}
	if len(segments) == 6 {
		if segments[4] != "versions" {
			return SecretRef{}, fmt.Errorf("credential URI %q: want projects/<project>/secrets/<secret>[/versions/<version>], got path %q", raw, u.Path)
		}
		ref.Version = segments[5]
	}
	return ref, nil
}

// secretAccessor is the subset of the Secret Manager client this backend needs.
// The real *secretmanager.Client satisfies it; tests supply a fake.
type secretAccessor interface {
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
}

// Server implements credproviderpb.CredentialProviderServer over Google Cloud
// Secret Manager.
type Server struct {
	credproviderpb.UnimplementedCredentialProviderServer

	client secretAccessor
}

// NewServer builds a Secret Manager-backed credential provider.
func NewServer(client secretAccessor) *Server {
	return &Server{client: client}
}

// RequestSecret resolves one substrate-secret:// URI to its secret payload.
func (s *Server) RequestSecret(ctx context.Context, req *credproviderpb.RequestSecretRequest) (*credproviderpb.RequestSecretResponse, error) {
	ref, err := ParseURI(req.GetUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	slog.InfoContext(ctx, "resolving credential",
		slog.String("project", ref.Project),
		slog.String("secret", ref.Secret),
		slog.String("version", ref.Version),
		slog.String("scope", req.GetContext().GetScope().String()),
		slog.String("actor", req.GetContext().GetActorIdentity()),
		slog.String("destination", req.GetContext().GetDestination()),
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
		got := int64(crc32.Checksum(payload.GetData(), crc32.MakeTable(crc32.Castagnoli)))
		if got != payload.GetDataCrc32C() {
			return nil, status.Errorf(codes.Unavailable, "secret version %s: payload CRC32C mismatch", ref.ResourceName())
		}
	}
	return &credproviderpb.RequestSecretResponse{Secret: payload.GetData()}, nil
}
