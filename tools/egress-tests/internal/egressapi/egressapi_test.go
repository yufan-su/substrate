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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"
)

// withinBucket reports whether got is no lower than want and at most one
// bucket width (~12%) above it.
func withinBucket(got, want time.Duration) bool {
	return got >= want && float64(got) <= float64(want)*1.13
}

func TestHistogramQuantiles(t *testing.T) {
	var h Histogram
	// 1ms, 2ms, ..., 1000ms: the q-th quantile is q*1000ms.
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	for _, tc := range []struct {
		q    float64
		want time.Duration
	}{
		{0.5, 500 * time.Millisecond},
		{0.9, 900 * time.Millisecond},
		{0.99, 990 * time.Millisecond},
	} {
		if got := h.Quantile(tc.q); !withinBucket(got, tc.want) {
			t.Errorf("Quantile(%v) = %v, want within one bucket above %v", tc.q, got, tc.want)
		}
	}
	if got := h.Quantile(1); got != time.Second {
		t.Errorf("Quantile(1) = %v, want the max sample 1s", got)
	}
	if got, want := h.Mean(), 500500*time.Microsecond; got != want {
		t.Errorf("Mean() = %v, want %v", got, want)
	}
	if h.Count != 1000 {
		t.Errorf("Count = %d, want 1000", h.Count)
	}
}

func TestHistogramEmpty(t *testing.T) {
	var h Histogram
	if h.Quantile(0.5) != 0 || h.Mean() != 0 || h.Max() != 0 {
		t.Errorf("empty histogram: Quantile=%v Mean=%v Max=%v, want all 0", h.Quantile(0.5), h.Mean(), h.Max())
	}
	h.Merge(Histogram{})
	if h.Counts != nil {
		t.Errorf("merging an empty histogram allocated buckets")
	}
}

func TestHistogramOverflow(t *testing.T) {
	var h Histogram
	h.Record(2 * time.Minute)
	if got := h.Counts[len(h.Counts)-1]; got != 1 {
		t.Fatalf("overflow bucket = %d, want 1", got)
	}
	if got := h.Quantile(0.5); got != 2*time.Minute {
		t.Errorf("Quantile(0.5) = %v, want the overflowed sample 2m", got)
	}
}

func TestHistogramMergeMatchesSingleRecorder(t *testing.T) {
	var a, b, all Histogram
	for i := 1; i <= 500; i++ {
		d := time.Duration(i*i) * time.Microsecond
		all.Record(d)
		if i%2 == 0 {
			a.Record(d)
		} else {
			b.Record(d)
		}
	}
	var merged Histogram
	merged.Merge(a)
	merged.Merge(b)
	for _, q := range []float64{0.1, 0.5, 0.9, 0.99, 1} {
		if merged.Quantile(q) != all.Quantile(q) {
			t.Errorf("Quantile(%v): merged %v, single recorder %v", q, merged.Quantile(q), all.Quantile(q))
		}
	}
	if merged.Count != all.Count || merged.SumMicros != all.SumMicros || merged.MaxMicros != all.MaxMicros {
		t.Errorf("merged totals %+v differ from %+v", merged, all)
	}
}

func TestStatsMergeAndJSON(t *testing.T) {
	a := &Stats{
		Elapsed:   time.Minute,
		Requests:  10,
		Successes: 9,
		NewConns:  2,
		Errors:    map[string]int64{"timeout": 1},
		Endpoints: []Endpoint{{Requests: 5}, {Requests: 5, Errors: 1}},
	}
	a.Latency.Record(time.Millisecond)
	b := &Stats{
		Scheme:    SchemeHTTPS,
		Elapsed:   2 * time.Minute,
		Requests:  6,
		Successes: 4,
		Errors:    map[string]int64{"timeout": 1, "HTTP 503": 1},
		Endpoints: []Endpoint{{Requests: 2}, {Requests: 2}, {Requests: 2, Errors: 2}},
	}
	b.DNS.Record(time.Millisecond)
	b.TLS.Record(3 * time.Millisecond)

	var got Stats
	got.Merge(a)
	got.Merge(b)
	if got.Elapsed != 2*time.Minute || got.Requests != 16 || got.Successes != 13 || got.NewConns != 2 {
		t.Errorf("merged totals = %+v", got)
	}
	if got.Errors["timeout"] != 2 || got.Errors["HTTP 503"] != 1 {
		t.Errorf("merged errors = %v", got.Errors)
	}
	wantEndpoints := []Endpoint{{Requests: 7}, {Requests: 7, Errors: 1}, {Requests: 2, Errors: 2}}
	if fmt.Sprint(got.Endpoints) != fmt.Sprint(wantEndpoints) {
		t.Errorf("merged endpoints = %v, want %v", got.Endpoints, wantEndpoints)
	}
	if got.Latency.Count != 1 || got.DNS.Count != 1 || got.TLS.Count != 1 {
		t.Errorf("merged histograms: latency %d, dns %d, tls %d samples; want 1 each", got.Latency.Count, got.DNS.Count, got.TLS.Count)
	}
	if got.Scheme != SchemeHTTPS {
		t.Errorf("merged scheme = %q, want %q from the stats that set one", got.Scheme, SchemeHTTPS)
	}

	raw, err := json.Marshal(&got)
	if err != nil {
		t.Fatal(err)
	}
	var back Stats
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Latency.Quantile(0.5) != got.Latency.Quantile(0.5) || back.TLS.Count != 1 || back.Scheme != SchemeHTTPS ||
		back.Requests != got.Requests || back.Errors["HTTP 503"] != 1 {
		t.Errorf("JSON round trip lost data: %s", raw)
	}
}

func TestStatsCloneIsDeep(t *testing.T) {
	s := &Stats{Errors: map[string]int64{"EOF": 1}, Endpoints: []Endpoint{{Requests: 1}}}
	s.Latency.Record(time.Millisecond)
	s.TLS.Record(time.Millisecond)
	c := s.Clone()
	c.Errors["EOF"]++
	c.Endpoints[0].Requests++
	c.Latency.Record(time.Millisecond)
	c.TLS.Record(time.Millisecond)
	if s.Errors["EOF"] != 1 || s.Endpoints[0].Requests != 1 || s.Latency.Count != 1 || s.TLS.Count != 1 {
		t.Errorf("mutating the clone changed the original: %+v", s)
	}
	var sum int64
	for _, n := range s.Latency.Counts {
		sum += n
	}
	if sum != 1 {
		t.Errorf("original latency buckets hold %d samples after mutating the clone, want 1", sum)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	wrap := func(err error) error { return &url.Error{Op: "Get", URL: "http://x/", Err: err} }
	for _, tc := range []struct {
		name   string
		err    error
		status int
		want   string
	}{
		{"success", nil, 200, ""},
		{"http status", nil, 503, "HTTP 503"},
		{"deadline", wrap(context.DeadlineExceeded), 0, "timeout"},
		{"net timeout", wrap(&net.OpError{Op: "dial", Err: timeoutErr{}}), 0, "timeout"},
		{"refused", wrap(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), 0, "connection refused"},
		{"reset", wrap(&net.OpError{Op: "read", Err: syscall.ECONNRESET}), 0, "connection reset"},
		{"eof", wrap(io.EOF), 0, "EOF"},
		{"unexpected eof", wrap(io.ErrUnexpectedEOF), 0, "EOF"},
		{"dns not found", wrap(&net.DNSError{Err: "no such host", Name: "x", IsNotFound: true}), 0, "dns: not found"},
		{"dns other", wrap(&net.DNSError{Err: "server misbehaving", Name: "x"}), 0, "dns: error"},
		{"other strips url", wrap(errors.New("boom")), 0, "other: boom"},
		{"unknown authority", wrap(&tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}), 0, "tls: unknown authority"},
		{"hostname mismatch", wrap(&tls.CertificateVerificationError{Err: x509.HostnameError{Host: "x"}}), 0, "tls: hostname mismatch"},
		{"expired", wrap(&tls.CertificateVerificationError{Err: x509.CertificateInvalidError{Reason: x509.Expired}}), 0, "tls: invalid certificate"},
		{"not tls", wrap(tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}), 0, "tls: not a TLS server"},
		{"plain http answer", wrap(errors.New("http: server gave HTTP response to HTTPS client")), 0, "tls: not a TLS server"},
		{"peer alert", wrap(&net.OpError{Op: "remote error", Err: errors.New("tls: handshake failure")}), 0, "tls: peer alert: handshake failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err, tc.status); got != tc.want {
				t.Errorf("Classify() = %q, want %q", got, tc.want)
			}
		})
	}

	long := Classify(errors.New(strings.Repeat("x", 500)), 0)
	if len(long) != len("other: ")+maxOtherLen {
		t.Errorf("unrecognized error not truncated: %d bytes", len(long))
	}
}

func TestStartRequestValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		req     StartRequest
		wantErr bool
	}{
		{"ok", StartRequest{Endpoints: 10}, false},
		{"http", StartRequest{Endpoints: 10, Scheme: SchemeHTTP}, false},
		{"https", StartRequest{Endpoints: 10, Scheme: SchemeHTTPS}, false},
		{"bad scheme", StartRequest{Endpoints: 10, Scheme: "ftp"}, true},
		{"most endpoints", StartRequest{Endpoints: MaxEndpoints}, false},
		{"no endpoints", StartRequest{}, true},
		{"too many endpoints", StartRequest{Endpoints: MaxEndpoints + 1}, true},
		{"negative timeout", StartRequest{Endpoints: 1, RequestTimeoutMs: -1}, true},
		{"negative interval", StartRequest{Endpoints: 1, IntervalMs: -1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.req.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestEndpointNames(t *testing.T) {
	if got, want := ServiceName(7), "egress-target-7"; got != want {
		t.Errorf("ServiceName(7) = %q, want %q", got, want)
	}
	if got, want := EndpointHost(7), "egress-target-7.egress-tests-targets.svc.cluster.local"; got != want {
		t.Errorf("EndpointHost(7) = %q, want %q", got, want)
	}
	if got, want := EndpointURL(7, SchemeHTTP), "http://egress-target-7.egress-tests-targets.svc.cluster.local/"; got != want {
		t.Errorf("EndpointURL(7, http) = %q, want %q", got, want)
	}
	if got, want := EndpointURL(7, SchemeHTTPS), "https://egress-target-7.egress-tests-targets.svc.cluster.local/"; got != want {
		t.Errorf("EndpointURL(7, https) = %q, want %q", got, want)
	}
	if got, want := EndpointHostPattern, "*.egress-tests-targets.svc.cluster.local"; got != want {
		t.Errorf("EndpointHostPattern = %q, want %q", got, want)
	}
	if got := (&StartRequest{}).URLScheme(); got != SchemeHTTP {
		t.Errorf("URLScheme() with no scheme = %q, want %q", got, SchemeHTTP)
	}
}
