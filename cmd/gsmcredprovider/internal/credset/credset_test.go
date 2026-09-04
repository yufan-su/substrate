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

package credset

import "testing"

func TestValueFor(t *testing.T) {
	const set = `{
		"api.example.com": {"Authorization": "Bearer s3cr3t", "X-Api-Key": "k"},
		"github.com": {"Authorization": "Bearer gh"}
	}`

	tests := []struct {
		name        string
		destination string
		headerKey   string
		want        string
		wantNil     bool
	}{
		{name: "exact match", destination: "api.example.com", headerKey: "Authorization", want: "Bearer s3cr3t"},
		{name: "other header of same host", destination: "api.example.com", headerKey: "X-Api-Key", want: "k"},
		{name: "host case-insensitive", destination: "API.Example.com", headerKey: "Authorization", want: "Bearer s3cr3t"},
		{name: "host trailing dot", destination: "api.example.com.", headerKey: "Authorization", want: "Bearer s3cr3t"},
		{name: "host absent", destination: "absent.example.com", headerKey: "Authorization", wantNil: true},
		{name: "header absent for host", destination: "github.com", headerKey: "X-Api-Key", wantNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ValueFor([]byte(set), tc.destination, tc.headerKey)
			if err != nil {
				t.Fatalf("ValueFor: %v", err)
			}
			if tc.wantNil {
				if got != nil {
					t.Errorf("ValueFor = %q, want nil", got)
				}
				return
			}
			if string(got) != tc.want {
				t.Errorf("ValueFor = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValueForMalformedFailsClosed(t *testing.T) {
	for _, bad := range []string{
		``,                        // empty
		`"just a string"`,         // a bare token, not an object
		`{"host": "not-headers"}`, // host value is not a header map
		`{`,                       // truncated
	} {
		if _, err := ValueFor([]byte(bad), "api.example.com", "Authorization"); err == nil {
			t.Errorf("ValueFor(%q) = nil error, want error", bad)
		}
	}
}
