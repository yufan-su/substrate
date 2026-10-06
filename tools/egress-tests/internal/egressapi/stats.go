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

package egressapi

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Stats is what one actor's loop measured since it started. GET /stats and
// POST /stop return it as JSON.
type Stats struct {
	// Scheme is the scheme the loop requested endpoints over.
	Scheme  string        `json:"scheme,omitempty"`
	Elapsed time.Duration `json:"elapsed"`
	// Requests counts every request the loop finished, failed ones included.
	Requests  int64 `json:"requests"`
	Successes int64 `json:"successes"`
	// NewConns counts requests that went out on a connection opened for them
	// rather than on a kept-alive one.
	NewConns int64 `json:"newConns"`
	// Errors counts failed requests by Classify's class.
	Errors map[string]int64 `json:"errors,omitempty"`
	// Endpoints holds per-endpoint counts, indexed by endpoint number.
	Endpoints []Endpoint `json:"endpoints,omitempty"`
	// Latency holds the end-to-end time of successful requests.
	Latency Histogram `json:"latency"`
	// DNS holds lookup times, recorded only for requests that did a lookup.
	DNS Histogram `json:"dns"`
	// TLS holds handshake times, recorded only for handshakes that completed.
	TLS Histogram `json:"tls"`
}

// Endpoint holds the counts for one endpoint.
type Endpoint struct {
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
}

// Merge adds o's counts to s. Elapsed becomes the longer of the two, since
// merged loops run side by side rather than one after another.
func (s *Stats) Merge(o *Stats) {
	s.Scheme = cmp.Or(s.Scheme, o.Scheme)
	s.Elapsed = max(s.Elapsed, o.Elapsed)
	s.Requests += o.Requests
	s.Successes += o.Successes
	s.NewConns += o.NewConns
	for class, n := range o.Errors {
		if s.Errors == nil {
			s.Errors = make(map[string]int64)
		}
		s.Errors[class] += n
	}
	if len(o.Endpoints) > len(s.Endpoints) {
		s.Endpoints = append(s.Endpoints, make([]Endpoint, len(o.Endpoints)-len(s.Endpoints))...)
	}
	for i, e := range o.Endpoints {
		s.Endpoints[i].Requests += e.Requests
		s.Endpoints[i].Errors += e.Errors
	}
	s.Latency.Merge(o.Latency)
	s.DNS.Merge(o.DNS)
	s.TLS.Merge(o.TLS)
}

// Clone returns a deep copy of s.
func (s *Stats) Clone() *Stats {
	c := *s
	c.Errors = maps.Clone(s.Errors)
	c.Endpoints = slices.Clone(s.Endpoints)
	c.Latency = s.Latency.clone()
	c.DNS = s.DNS.clone()
	c.TLS = s.TLS.clone()
	return &c
}

// maxOtherLen bounds the message kept for an error Classify does not
// recognize, so one odd error cannot flood the report.
const maxOtherLen = 80

// Classify names the outcome of a request in a few low-cardinality classes.
// It returns "" for a success: no error and HTTP 200.
func Classify(err error, status int) string {
	if err == nil {
		if status == 200 {
			return ""
		}
		return fmt.Sprintf("HTTP %d", status)
	}
	var netErr net.Error
	var dnsErr *net.DNSError
	var opErr *net.OpError
	switch {
	case errors.As(err, &dnsErr):
		if dnsErr.IsNotFound {
			return "dns: not found"
		}
		if dnsErr.IsTimeout {
			return "dns: timeout"
		}
		return "dns: error"
	case errors.As(err, new(x509.UnknownAuthorityError)):
		return "tls: unknown authority"
	case errors.As(err, new(x509.HostnameError)):
		return "tls: hostname mismatch"
	case errors.As(err, new(x509.CertificateInvalidError)):
		return "tls: invalid certificate"
	case errors.As(err, new(tls.RecordHeaderError)),
		// net/http replaces the record error with this when the server
		// answered in plain HTTP, without wrapping it.
		strings.Contains(err.Error(), "server gave HTTP response to HTTPS client"):
		return "tls: not a TLS server"
	case errors.As(err, &opErr) && opErr.Op == "remote error":
		// crypto/tls reports an alert from the peer this way; its message,
		// such as "tls: handshake failure", names the alert.
		return "tls: peer alert: " + strings.TrimPrefix(opErr.Err.Error(), "tls: ")
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection reset"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "EOF"
	}
	// *url.Error repeats the method and URL, which only adds cardinality.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	msg := err.Error()
	if len(msg) > maxOtherLen {
		msg = msg[:maxOtherLen]
	}
	return "other: " + msg
}
