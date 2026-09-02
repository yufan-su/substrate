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

// Package smprovider implements the CredentialProvider plugin API backed by
// Google Cloud Secret Manager. It resolves substrate-secret:// URIs of the
// class "secretmanager.googleapis.com" whose path is a Secret Manager version
// resource name. The secret payload is a JSON object mapping an upstream host
// to the request headers to inject for it:
//
//	{
//	  "generativelanguage.googleapis.com": {"x-goog-api-key": "AIza..."},
//	  "api.example.com": {"authorization": "Bearer sk-...", "x-custom": "v"}
//	}
//
// Given the target host and header the egress injector is resolving, the
// provider returns the single matching value. Substrate never stores the
// secret; it only brokers a read the provider's Google identity is authorized
// to perform.
package smprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// ProviderClass is the substrate-secret:// URI host this backend serves.
const ProviderClass = "secretmanager.googleapis.com"

// uriScheme is the only scheme a credential URI may carry.
const uriScheme = "substrate-secret"

// versionClient is the subset of *secretmanager.Client the server calls, so a
// fake can stand in for tests.
type versionClient interface {
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
}

// ParseResource extracts the Secret Manager version resource name from a
// substrate-secret:// URI of the secretmanager.googleapis.com class:
//
//	substrate-secret://secretmanager.googleapis.com/projects/P/secrets/S/versions/V
//
// It returns "projects/P/secrets/S/versions/V". It rejects any other scheme or
// provider class, and a path that is not a well-formed version resource name,
// so a misrouted or malformed URI fails loudly rather than reaching Secret
// Manager as a bad request.
func ParseResource(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != uriScheme {
		return "", fmt.Errorf("credential URI %q: scheme is %q, want %q", raw, u.Scheme, uriScheme)
	}
	if u.Host != ProviderClass {
		return "", fmt.Errorf("credential URI %q: provider class is %q, this provider serves %q", raw, u.Host, ProviderClass)
	}

	resource := strings.Trim(u.Path, "/")
	segments := strings.Split(resource, "/")
	// A version resource name is projects/<p>/secrets/<s>/versions/<v>.
	if len(segments) != 6 || segments[0] != "projects" || segments[2] != "secrets" || segments[4] != "versions" {
		return "", fmt.Errorf("credential URI %q: path must be projects/<p>/secrets/<s>/versions/<v>", raw)
	}
	for i, s := range segments {
		if s == "" {
			return "", fmt.Errorf("credential URI %q: empty path segment %d", raw, i)
		}
	}
	return resource, nil
}

// Server implements credproviderpb.CredentialProviderServer over Secret
// Manager.
type Server struct {
	credproviderpb.UnimplementedCredentialProviderServer

	client versionClient
}

// NewServer builds a Secret Manager-backed credential provider over an
// already-constructed client.
func NewServer(client versionClient) *Server {
	return &Server{client: client}
}

// RequestSecret resolves one substrate-secret:// URI to the header value the
// injector asked for, selected by target host and header from the secret's
// host->{header: value} map.
func (s *Server) RequestSecret(ctx context.Context, req *credproviderpb.RequestSecretRequest) (*credproviderpb.RequestSecretResponse, error) {
	resource, err := ParseResource(req.GetUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// The lookup needs both keys: without a host and header there is no entry to
	// select, so fail closed rather than guess.
	host := strings.ToLower(strings.TrimSuffix(req.GetContext().GetTargetHost(), "."))
	if host == "" {
		return nil, status.Error(codes.InvalidArgument, "target host is required")
	}
	header := req.GetHeader()
	if header == "" {
		return nil, status.Error(codes.InvalidArgument, "header is required")
	}

	slog.InfoContext(ctx, "resolving credential",
		slog.String("resource", resource),
		slog.String("host", host),
		slog.String("header", header),
		slog.String("actor", req.GetContext().GetActorIdentity()),
	)

	resp, err := s.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: resource})
	if err != nil {
		// Preserve the Secret Manager gRPC code where it maps cleanly, so the
		// injector sees NotFound/PermissionDenied rather than a blanket failure.
		switch status.Code(err) {
		case codes.NotFound:
			return nil, status.Errorf(codes.NotFound, "secret version %s not found", resource)
		case codes.PermissionDenied:
			return nil, status.Errorf(codes.PermissionDenied, "not permitted to access secret version %s", resource)
		default:
			return nil, status.Errorf(codes.Unavailable, "accessing secret version %s: %v", resource, err)
		}
	}

	value, err := selectHeaderValue(resp.GetPayload().GetData(), host, header)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "secret version %s: %v", resource, err)
	}
	return &credproviderpb.RequestSecretResponse{Secret: value}, nil
}

// selectHeaderValue parses the secret payload as a host->{header: value} JSON
// object and returns the value for host and header. Host and header matching is
// case-insensitive (both are case-insensitive in HTTP), so the secret author
// need not match the policy's exact casing.
func selectHeaderValue(payload []byte, host, header string) ([]byte, error) {
	var byHost map[string]map[string]string
	if err := json.Unmarshal(payload, &byHost); err != nil {
		return nil, fmt.Errorf("payload is not a JSON host->{header: value} object: %w", err)
	}

	headers, ok := byHost[host]
	if !ok {
		// A case-insensitive host lookup, since the JSON key is exact.
		for h, hdrs := range byHost {
			if strings.EqualFold(h, host) {
				headers, ok = hdrs, true
				break
			}
		}
	}
	if !ok {
		return nil, fmt.Errorf("no credentials for host %q", host)
	}

	for h, v := range headers {
		if strings.EqualFold(h, header) {
			return []byte(v), nil
		}
	}
	return nil, fmt.Errorf("no credential for header %q on host %q", header, host)
}

// NewClient constructs a Secret Manager client using Application Default
// Credentials (Workload Identity in-cluster).
func NewClient(ctx context.Context) (*secretmanager.Client, error) {
	return secretmanager.NewClient(ctx)
}
