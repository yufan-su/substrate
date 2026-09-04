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

// Package credset decodes the credential-set format the Secret Manager provider
// stores: a JSON object mapping a destination host to the HTTP headers to inject
// for it.
//
//	{
//	  "github.com":      {"Authorization": "Bearer <token>", "X-Custom": "v"},
//	  "api.example.com": {"X-Api-Key": "<key>"}
//	}
//
// A provider stores this blob and, given a destination and the header the caller
// intends to inject, returns just that header's value — so the injector never
// parses the storage format and behaves the same across providers.
package credset

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ValueFor decodes a credential-set secret and returns the value for header
// headerKey at destination. Host keys are matched case- and trailing-dot-
// insensitively. It returns a nil value (and no error) when the set holds no
// entry for the host or no such header, so a caller can treat "no credential for
// this request" as pass-through. It errors when the blob is not a valid
// credential set, so a misconfigured secret fails closed.
func ValueFor(secret []byte, destination, headerKey string) ([]byte, error) {
	var raw map[string]map[string]string
	if err := json.Unmarshal(secret, &raw); err != nil {
		return nil, fmt.Errorf("credential set is not a JSON object of host->headers: %w", err)
	}
	wantHost := normalizeHost(destination)
	for host, headers := range raw {
		if normalizeHost(host) != wantHost {
			continue
		}
		if v, ok := headers[headerKey]; ok {
			return []byte(v), nil
		}
		// The host is present but carries no such header: nothing to inject.
		return nil, nil
	}
	// No entry for the host: nothing to inject.
	return nil, nil
}

// normalizeHost lowercases a hostname and drops a trailing dot so credential-set
// keys and request hosts compare equal regardless of case or an absolute-form
// trailing dot.
func normalizeHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(host, "."))
}
