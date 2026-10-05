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
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandler(t *testing.T) {
	h := newHandler(32)
	for _, tc := range []struct {
		path     string
		wantBody int
	}{
		{"/", 32},
		{"/anything", 32},
		{"/healthz", len("ok\n")},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != http.StatusOK || rec.Body.Len() != tc.wantBody {
			t.Errorf("GET %s = %d with %d bytes, want 200 with %d", tc.path, rec.Code, rec.Body.Len(), tc.wantBody)
		}
	}
}
