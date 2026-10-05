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

// Package egressapi is the HTTP contract between the egress-tests driver and
// the actor: the routes the actor serves, the request that starts its loop, and
// the stats it reports.
package egressapi

import (
	"errors"
	"fmt"
	"net/url"
)

// Routes the actor serves on port 80, the only inbound port of a sandbox.
const (
	ReadyzRoute = "/readyz"
	StartRoute  = "/start"
	StatsRoute  = "/stats"
	StopRoute   = "/stop"
)

// MaxURLs bounds StartRequest.URLs. The egress policy admits at most 256
// hostnames, so a larger list could never be allowed anyway.
const MaxURLs = 256

// DefaultRequestTimeoutMs applies when StartRequest.RequestTimeoutMs is zero.
const DefaultRequestTimeoutMs = 5000

// StartRequest is the JSON body of POST /start.
type StartRequest struct {
	// URLs are requested in order, one after another, round and round until
	// the loop is stopped.
	URLs []string `json:"urls"`
	// NewConnPerRequest opens a new TCP connection for every request instead
	// of keeping one alive per URL.
	NewConnPerRequest bool `json:"newConnPerRequest"`
	// RequestTimeoutMs bounds one request; zero means DefaultRequestTimeoutMs.
	RequestTimeoutMs int64 `json:"requestTimeoutMs"`
	// IntervalMs is the pause after each request; zero sends back to back.
	IntervalMs int64 `json:"intervalMs"`
}

// Validate reports whether r can start a loop.
func (r *StartRequest) Validate() error {
	if len(r.URLs) == 0 {
		return errors.New("urls is required")
	}
	if len(r.URLs) > MaxURLs {
		return fmt.Errorf("at most %d urls, got %d", MaxURLs, len(r.URLs))
	}
	for _, raw := range r.URLs {
		u, err := url.Parse(raw)
		if err != nil {
			return fmt.Errorf("url %q: %w", raw, err)
		}
		if u.Scheme != "http" || u.Host == "" {
			return fmt.Errorf("url %q: want an absolute http:// URL", raw)
		}
	}
	if r.RequestTimeoutMs < 0 {
		return fmt.Errorf("requestTimeoutMs cannot be negative: %d", r.RequestTimeoutMs)
	}
	if r.IntervalMs < 0 {
		return fmt.Errorf("intervalMs cannot be negative: %d", r.IntervalMs)
	}
	return nil
}
