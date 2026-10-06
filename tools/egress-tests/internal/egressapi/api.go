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

import "fmt"

// Routes the actor serves on port 80, the only inbound port of a sandbox.
const (
	ReadyzRoute = "/readyz"
	StartRoute  = "/start"
	StatsRoute  = "/stats"
	StopRoute   = "/stop"
)

// DefaultRequestTimeoutMs applies when StartRequest.RequestTimeoutMs is zero.
const DefaultRequestTimeoutMs = 5000

// The schemes the loop can request endpoints over.
const (
	SchemeHTTP  = "http"
	SchemeHTTPS = "https"
)

// StartRequest is the JSON body of POST /start.
type StartRequest struct {
	// Endpoints is C: the loop requests EndpointURL(0) through
	// EndpointURL(Endpoints-1) in order, round and round until it is stopped.
	Endpoints int `json:"endpoints"`
	// NewConnPerRequest opens a new TCP connection for every request instead
	// of keeping one alive per endpoint.
	NewConnPerRequest bool `json:"newConnPerRequest"`
	// RequestTimeoutMs bounds one request; zero means DefaultRequestTimeoutMs.
	RequestTimeoutMs int64 `json:"requestTimeoutMs"`
	// IntervalMs is the pause after each request; zero sends back to back.
	IntervalMs int64 `json:"intervalMs"`
	// Scheme is SchemeHTTP or SchemeHTTPS; empty means SchemeHTTP.
	Scheme string `json:"scheme,omitempty"`
}

// URLScheme is the scheme the loop requests endpoints over.
func (r *StartRequest) URLScheme() string {
	if r.Scheme == "" {
		return SchemeHTTP
	}
	return r.Scheme
}

// Validate reports whether r can start a loop.
func (r *StartRequest) Validate() error {
	if r.Endpoints < 1 || r.Endpoints > MaxEndpoints {
		return fmt.Errorf("endpoints must be between 1 and %d, got %d", MaxEndpoints, r.Endpoints)
	}
	if r.RequestTimeoutMs < 0 {
		return fmt.Errorf("requestTimeoutMs cannot be negative: %d", r.RequestTimeoutMs)
	}
	if r.IntervalMs < 0 {
		return fmt.Errorf("intervalMs cannot be negative: %d", r.IntervalMs)
	}
	if r.Scheme != "" && r.Scheme != SchemeHTTP && r.Scheme != SchemeHTTPS {
		return fmt.Errorf("scheme must be %s or %s, got %q", SchemeHTTP, SchemeHTTPS, r.Scheme)
	}
	return nil
}
