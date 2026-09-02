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

package smprovider

import (
	"context"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

func TestParseResource(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    string
		wantErr bool
	}{
		{
			name: "version resource",
			uri:  "substrate-secret://secretmanager.googleapis.com/projects/p1/secrets/s1/versions/3",
			want: "projects/p1/secrets/s1/versions/3",
		},
		{
			name: "latest version",
			uri:  "substrate-secret://secretmanager.googleapis.com/projects/p1/secrets/s1/versions/latest",
			want: "projects/p1/secrets/s1/versions/latest",
		},
		{name: "wrong scheme", uri: "https://secretmanager.googleapis.com/projects/p1/secrets/s1/versions/3", wantErr: true},
		{name: "wrong provider class", uri: "substrate-secret://kubernetes.io/projects/p1/secrets/s1/versions/3", wantErr: true},
		{name: "missing versions segment", uri: "substrate-secret://secretmanager.googleapis.com/projects/p1/secrets/s1", wantErr: true},
		{name: "malformed shape", uri: "substrate-secret://secretmanager.googleapis.com/foo/p1/bar/s1/baz/3", wantErr: true},
		{name: "empty segment", uri: "substrate-secret://secretmanager.googleapis.com/projects//secrets/s1/versions/3", wantErr: true},
		{name: "unparseable", uri: "://://", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseResource(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseResource(%q) = %q, want error", tc.uri, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseResource(%q) unexpected error: %v", tc.uri, err)
			}
			if got != tc.want {
				t.Errorf("ParseResource(%q) = %q, want %q", tc.uri, got, tc.want)
			}
		})
	}
}

// fakeClient is a versionClient returning a fixed payload or error.
type fakeClient struct {
	payload []byte
	err     error
}

func (f *fakeClient) AccessSecretVersion(_ context.Context, _ *secretmanagerpb.AccessSecretVersionRequest, _ ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &secretmanagerpb.AccessSecretVersionResponse{
		Payload: &secretmanagerpb.SecretPayload{Data: f.payload},
	}, nil
}

const sampleSecret = `{
  "generativelanguage.googleapis.com": {"x-goog-api-key": "AIzaKEY"},
  "api.example.com": {"Authorization": "Bearer sk-123", "X-Custom-Header": "value"}
}`

func TestRequestSecret(t *testing.T) {
	uri := "substrate-secret://secretmanager.googleapis.com/projects/p1/secrets/s1/versions/latest"

	tests := []struct {
		name    string
		host    string
		header  string
		want    string
		wantErr codes.Code
	}{
		{name: "single header host", host: "generativelanguage.googleapis.com", header: "x-goog-api-key", want: "AIzaKEY"},
		{name: "one of several headers", host: "api.example.com", header: "Authorization", want: "Bearer sk-123"},
		{name: "case-insensitive header", host: "api.example.com", header: "authorization", want: "Bearer sk-123"},
		{name: "case-insensitive host", host: "API.EXAMPLE.COM", header: "X-Custom-Header", want: "value"},
		{name: "trailing dot host", host: "api.example.com.", header: "X-Custom-Header", want: "value"},
		{name: "unknown host", host: "nope.example.com", header: "Authorization", wantErr: codes.NotFound},
		{name: "unknown header", host: "api.example.com", header: "X-Absent", wantErr: codes.NotFound},
		{name: "missing host", host: "", header: "Authorization", wantErr: codes.InvalidArgument},
		{name: "missing header", host: "api.example.com", header: "", wantErr: codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(&fakeClient{payload: []byte(sampleSecret)})
			resp, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{
				Uri:    uri,
				Header: tc.header,
				Context: &credproviderpb.SecretRequestContext{
					ActorIdentity: "spiffe://substrate-actor.local/atespace/team/actor/a1",
					TargetHost:    tc.host,
				},
			})
			if tc.wantErr != codes.OK {
				if status.Code(err) != tc.wantErr {
					t.Fatalf("RequestSecret error = %v, want code %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("RequestSecret unexpected error: %v", err)
			}
			if got := string(resp.GetSecret()); got != tc.want {
				t.Errorf("RequestSecret secret = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRequestSecretBadURI(t *testing.T) {
	srv := NewServer(&fakeClient{payload: []byte(sampleSecret)})
	_, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{
		Uri:     "substrate-secret://kubernetes.io/projects/p1/secrets/s1/versions/1",
		Header:  "Authorization",
		Context: &credproviderpb.SecretRequestContext{TargetHost: "api.example.com"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RequestSecret error = %v, want InvalidArgument", err)
	}
}

func TestRequestSecretMapsAccessErrors(t *testing.T) {
	uri := "substrate-secret://secretmanager.googleapis.com/projects/p1/secrets/s1/versions/1"
	tests := []struct {
		name string
		in   error
		want codes.Code
	}{
		{name: "not found", in: status.Error(codes.NotFound, "no version"), want: codes.NotFound},
		{name: "permission denied", in: status.Error(codes.PermissionDenied, "denied"), want: codes.PermissionDenied},
		{name: "other becomes unavailable", in: status.Error(codes.Internal, "boom"), want: codes.Unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(&fakeClient{err: tc.in})
			_, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{
				Uri:     uri,
				Header:  "Authorization",
				Context: &credproviderpb.SecretRequestContext{TargetHost: "api.example.com"},
			})
			if status.Code(err) != tc.want {
				t.Fatalf("RequestSecret error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRequestSecretMalformedPayload(t *testing.T) {
	uri := "substrate-secret://secretmanager.googleapis.com/projects/p1/secrets/s1/versions/1"
	srv := NewServer(&fakeClient{payload: []byte("not json")})
	_, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{
		Uri:     uri,
		Header:  "Authorization",
		Context: &credproviderpb.SecretRequestContext{TargetHost: "api.example.com"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("RequestSecret error = %v, want NotFound", err)
	}
}
