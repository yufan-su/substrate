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

package gsmprovider

import (
	"context"
	"hash/crc32"
	"testing"

	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

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
	crc := int64(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
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
			uri:  "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/versions/5",
			want: SecretRef{Project: "proj-123", Secret: "example-api", Version: "5"},
		},
		{
			name: "without version defaults to latest",
			uri:  "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api",
			want: SecretRef{Project: "proj-123", Secret: "example-api", Version: "latest"},
		},
		{name: "wrong scheme", uri: "https://secretmanager.googleapis.com/projects/proj-123/secrets/example-api", wantErr: true},
		{name: "wrong provider class", uri: "substrate-secret://kubernetes.io/projects/proj-123/secrets/example-api", wantErr: true},
		{name: "too few segments", uri: "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets", wantErr: true},
		{name: "missing projects keyword", uri: "substrate-secret://secretmanager.googleapis.com/proj-123/example-api/versions/1", wantErr: true},
		{name: "wrong secrets keyword", uri: "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secret/example-api", wantErr: true},
		{name: "wrong versions keyword", uri: "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/version/5", wantErr: true},
		{name: "too many segments", uri: "substrate-secret://secretmanager.googleapis.com/projects/p/secrets/s/versions/1/extra/x", wantErr: true},
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

func TestRequestSecret(t *testing.T) {
	const name = "projects/proj-123/secrets/example-api/versions/latest"
	tests := []struct {
		name     string
		accessor *fakeAccessor
		uri      string
		want     string
		wantCode codes.Code
	}{
		{
			name:     "found",
			accessor: &fakeAccessor{byName: map[string][]byte{name: []byte("s3cr3t")}},
			uri:      "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api",
			want:     "s3cr3t",
		},
		{
			name:     "explicit version",
			accessor: &fakeAccessor{byName: map[string][]byte{"projects/proj-123/secrets/example-api/versions/5": []byte("v5")}},
			uri:      "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api/versions/5",
			want:     "v5",
		},
		{
			name:     "not found",
			accessor: &fakeAccessor{errByName: map[string]error{name: status.Error(codes.NotFound, "gone")}},
			uri:      "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api",
			wantCode: codes.NotFound,
		},
		{
			name:     "permission denied maps through",
			accessor: &fakeAccessor{errByName: map[string]error{name: status.Error(codes.PermissionDenied, "nope")}},
			uri:      "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api",
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "other error is unavailable",
			accessor: &fakeAccessor{errByName: map[string]error{name: status.Error(codes.Internal, "boom")}},
			uri:      "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api",
			wantCode: codes.Unavailable,
		},
		{
			name:     "crc mismatch fails closed",
			accessor: &fakeAccessor{byName: map[string][]byte{name: []byte("s3cr3t")}, corrupt: true},
			uri:      "substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/example-api",
			wantCode: codes.Unavailable,
		},
		{
			name:     "bad uri",
			accessor: &fakeAccessor{},
			uri:      "substrate-secret://kubernetes.io/projects/proj-123/secrets/example-api",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(tc.accessor)
			resp, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{Uri: tc.uri})
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("RequestSecret(%q) code = %v, want %v (err=%v)", tc.uri, status.Code(err), tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RequestSecret(%q) unexpected error: %v", tc.uri, err)
			}
			if got := string(resp.GetSecret()); got != tc.want {
				t.Errorf("RequestSecret(%q) = %q, want %q", tc.uri, got, tc.want)
			}
		})
	}
}
