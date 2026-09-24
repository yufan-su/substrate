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

package main

import (
	"context"
	"hash/crc32"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// fakeAccessor is a stand-in for the Secret Manager client. It serves payloads
// keyed by resource name and can be told to return a gRPC status error.
type fakeAccessor struct {
	// byName maps a resource name to the raw payload to return.
	byName map[string][]byte
	// errByName maps a resource name to an error to return instead.
	errByName map[string]error
	// corrupt, when true, returns a payload whose CRC32C does not match the data.
	corrupt bool
	// noPayload, when true, returns a response with no payload at all.
	noPayload bool
	// gotName records the last requested resource name.
	gotName string
}

func (f *fakeAccessor) AccessSecretVersion(_ context.Context, req *secretmanagerpb.AccessSecretVersionRequest, _ ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	f.gotName = req.GetName()
	if err, ok := f.errByName[req.GetName()]; ok {
		return nil, err
	}
	data, ok := f.byName[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no such secret version")
	}
	if f.noPayload {
		return &secretmanagerpb.AccessSecretVersionResponse{Name: req.GetName()}, nil
	}
	crc := int64(crc32.Checksum(data, crc32cTable))
	if f.corrupt {
		crc++ // deliberately wrong
	}
	return &secretmanagerpb.AccessSecretVersionResponse{
		Name:    req.GetName(),
		Payload: &secretmanagerpb.SecretPayload{Data: data, DataCrc32C: &crc},
	}, nil
}

func TestParseURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    SecretRef
		wantErr bool
	}{
		{
			name: "with version",
			uri:  "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/versions/5",
			want: SecretRef{Project: "proj-123", Secret: "example-api", Version: "5"},
		},
		{
			name: "latest alias",
			uri:  "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/versions/latest",
			want: SecretRef{Project: "proj-123", Secret: "example-api", Version: "latest"},
		},
		{
			name: "without version defaults to latest",
			uri:  "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api",
			want: SecretRef{Project: "proj-123", Secret: "example-api", Version: "latest"},
		},
		{name: "wrong scheme", uri: "https://secretmanager.googleapis.com/projects/proj-123/secrets/example-api", wantErr: true},
		{name: "wrong provider", uri: "ate-secret://k8s.io/projects/proj-123/secrets/example-api", wantErr: true},
		{name: "too few segments", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets", wantErr: true},
		{name: "missing projects keyword", uri: "ate-secret://secretmanager.googleapis.com/proj-123/example-api/versions/1", wantErr: true},
		{name: "wrong secrets keyword", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secret/example-api", wantErr: true},
		{name: "wrong versions keyword", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/version/5", wantErr: true},
		{name: "non-numeric version", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/versions/newest", wantErr: true},
		{name: "zero version", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/versions/0", wantErr: true},
		{name: "leading-zero version", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/versions/05", wantErr: true},
		{name: "too many segments", uri: "ate-secret://secretmanager.googleapis.com/projects/p/secrets/s/versions/1/extra/x", wantErr: true},
		{name: "empty segment", uri: "ate-secret://secretmanager.googleapis.com/projects//secrets/example-api", wantErr: true},
		{name: "query not allowed", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api?version=1", wantErr: true},
		{name: "fragment not allowed", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api#x", wantErr: true},
		{name: "percent-encoded separator", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example%2Fapi", wantErr: true},
		{name: "percent-encoding of any kind", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example%2Dapi", wantErr: true},
		{name: "space in path", uri: "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example api", wantErr: true},
		{name: "unparseable", uri: "://://", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseURI(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseURI(%q) = %+v, want error", tc.uri, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseURI(%q) unexpected error: %v", tc.uri, err)
			}
			if got != tc.want {
				t.Errorf("ParseURI(%q) = %+v, want %+v", tc.uri, got, tc.want)
			}
		})
	}
}

func TestResourceName(t *testing.T) {
	ref := SecretRef{Project: "proj-123", Secret: "example-api", Version: "latest"}
	if got, want := ref.ResourceName(), "projects/proj-123/secrets/example-api/versions/latest"; got != want {
		t.Errorf("ResourceName() = %q, want %q", got, want)
	}
}

func TestFetchSecret(t *testing.T) {
	const uri = "ate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api"
	const name = "projects/proj-123/secrets/example-api/versions/latest"
	tests := []struct {
		name     string
		accessor *fakeAccessor
		uri      string
		want     string
		wantCode codes.Code
	}{
		{
			// The payload comes back verbatim; the gateway trims and prefixes it.
			name:     "found",
			accessor: &fakeAccessor{byName: map[string][]byte{name: []byte("s3cr3t\n")}},
			uri:      uri,
			want:     "s3cr3t\n",
		},
		{
			name:     "explicit version",
			accessor: &fakeAccessor{byName: map[string][]byte{"projects/proj-123/secrets/example-api/versions/5": []byte("v5")}},
			uri:      uri + "/versions/5",
			want:     "v5",
		},
		{
			name:     "not found",
			accessor: &fakeAccessor{errByName: map[string]error{name: status.Error(codes.NotFound, "gone")}},
			uri:      uri,
			wantCode: codes.NotFound,
		},
		{
			name:     "permission denied maps through",
			accessor: &fakeAccessor{errByName: map[string]error{name: status.Error(codes.PermissionDenied, "nope")}},
			uri:      uri,
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "other error is unavailable",
			accessor: &fakeAccessor{errByName: map[string]error{name: status.Error(codes.Internal, "boom")}},
			uri:      uri,
			wantCode: codes.Unavailable,
		},
		{
			name:     "no payload is not found",
			accessor: &fakeAccessor{byName: map[string][]byte{name: []byte("s3cr3t")}, noPayload: true},
			uri:      uri,
			wantCode: codes.NotFound,
		},
		{
			name:     "crc mismatch fails closed",
			accessor: &fakeAccessor{byName: map[string][]byte{name: []byte("s3cr3t")}, corrupt: true},
			uri:      uri,
			wantCode: codes.Unavailable,
		},
		{
			name:     "bad uri",
			accessor: &fakeAccessor{},
			uri:      "ate-secret://k8s.io/projects/proj-123/secrets/example-api",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(tc.accessor)
			resp, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{Uri: tc.uri})
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("FetchSecret(%q) code = %v, want %v (err=%v)", tc.uri, status.Code(err), tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("FetchSecret(%q) unexpected error: %v", tc.uri, err)
			}
			if got := string(resp.GetOpaqueBytes()); got != tc.want {
				t.Errorf("FetchSecret(%q) = %q, want %q", tc.uri, got, tc.want)
			}
		})
	}
}

// A URI that fails to parse is refused before Secret Manager is dialed.
func TestFetchSecretDoesNotDialOnBadURI(t *testing.T) {
	accessor := &fakeAccessor{}
	_, err := NewServer(accessor).FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
		Uri: "ate-secret://secretmanager.googleapis.com/nope",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
	if accessor.gotName != "" {
		t.Errorf("Secret Manager was asked for %q; a bad URI must not be dialed", accessor.gotName)
	}
}
