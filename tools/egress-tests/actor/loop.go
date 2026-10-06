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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

// maxBodyBytes bounds what the actor reads, from a /start body or from an
// endpoint's response.
const maxBodyBytes = 1 << 20

// server serves the egressapi routes. At most one loop runs at a time.
type server struct {
	// endpointURL maps an endpoint number and scheme to the URL the loop
	// requests. Tests point it at local servers.
	endpointURL func(i int, scheme string) string
	// rootCAs verifies HTTPS endpoints; nil uses the system roots, which
	// SSL_CERT_FILE extends with the gateway's MITM CA. Tests set their own.
	rootCAs *x509.CertPool

	mu  sync.Mutex
	run *loopRun // nil when no loop is running
}

func newServer() *server {
	return &server{endpointURL: egressapi.EndpointURL}
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+egressapi.ReadyzRoute, func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST "+egressapi.StartRoute, s.handleStart)
	mux.HandleFunc("GET "+egressapi.StatsRoute, s.handleStats)
	mux.HandleFunc("POST "+egressapi.StopRoute, s.handleStop)
	return mux
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req egressapi.StartRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		http.Error(w, "decoding start request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run != nil {
		http.Error(w, "a loop is already running; POST "+egressapi.StopRoute+" first", http.StatusConflict)
		return
	}
	urls := make([]string, req.Endpoints)
	for i := range urls {
		urls[i] = s.endpointURL(i, req.URLScheme())
	}
	s.run = startLoop(req, urls, s.rootCAs)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	if run == nil {
		http.Error(w, "no loop is running", http.StatusNotFound)
		return
	}
	writeJSON(w, run.snapshot())
}

func (s *server) handleStop(w http.ResponseWriter, _ *http.Request) {
	// Held across the wait so a /start cannot slip in before the loop exits.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run == nil {
		http.Error(w, "no loop is running", http.StatusNotFound)
		return
	}
	stats := s.run.stop()
	s.run = nil
	writeJSON(w, stats)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// loopRun is one started loop and what it has measured.
type loopRun struct {
	cancel    context.CancelFunc
	done      chan struct{}
	started   time.Time
	transport *http.Transport

	mu    sync.Mutex
	stats egressapi.Stats
}

func startLoop(req egressapi.StartRequest, urls []string, roots *x509.CertPool) *loopRun {
	timeoutMs := req.RequestTimeoutMs
	if timeoutMs == 0 {
		timeoutMs = egressapi.DefaultRequestTimeoutMs
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &loopRun{
		cancel:    cancel,
		done:      make(chan struct{}),
		started:   time.Now(),
		transport: newTransport(req.NewConnPerRequest, roots),
	}
	r.stats.Scheme = req.URLScheme()
	r.stats.Endpoints = make([]egressapi.Endpoint, len(urls))
	client := &http.Client{
		Transport: r.transport,
		// Count a redirect as the endpoint's answer rather than following it
		// somewhere the egress policy may not allow.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	go r.loop(ctx, client, urls, time.Duration(timeoutMs)*time.Millisecond, time.Duration(req.IntervalMs)*time.Millisecond)
	return r
}

// newTransport keeps one connection alive per endpoint, or none at all when
// newConnPerRequest is set, and verifies HTTPS endpoints against roots. It
// ignores proxy environment variables: the sandbox's egress capture is the
// only proxy under test.
func newTransport(newConnPerRequest bool, roots *x509.CertPool) *http.Transport {
	return &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:     &tls.Config{RootCAs: roots},
		ForceAttemptHTTP2:   false, // HTTP/1.1 over either scheme: one request per connection at a time
		MaxIdleConns:        0,     // no total cap, so C endpoints keep C connections
		MaxIdleConnsPerHost: 1,     // requests are sequential, so one per endpoint is enough
		IdleConnTimeout:     5 * time.Minute,
		DisableKeepAlives:   newConnPerRequest,
		// No TLSHandshakeTimeout: the request's own timeout bounds the
		// handshake, so a stalled one is counted as a timeout like any other.
	}
}

// loop requests every URL in turn, round and round, until ctx is cancelled.
func (r *loopRun) loop(ctx context.Context, client *http.Client, urls []string, timeout, interval time.Duration) {
	defer close(r.done)
	for ctx.Err() == nil {
		for i, u := range urls {
			if ctx.Err() != nil {
				return
			}
			r.request(ctx, client, i, u, timeout)
			if interval > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(interval):
				}
			}
		}
	}
}

// requestTrace collects what httptrace reports about one request. The dial,
// and with it the DNS callbacks, can still be running on another goroutine
// after a timed-out request returns, hence the lock.
type requestTrace struct {
	mu         sync.Mutex
	gotConn    bool
	newConn    bool
	dnsStart   time.Time
	dns        time.Duration
	didDNS     bool
	tlsStart   time.Time
	tls        time.Duration
	tlsStarted bool
	didTLS     bool
}

func (t *requestTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.gotConn, t.newConn = true, !info.Reused
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dnsStart = time.Now()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dns, t.didDNS = time.Since(t.dnsStart), true
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.tlsStart, t.tlsStarted = time.Now(), true
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			t.mu.Lock()
			defer t.mu.Unlock()
			if err == nil {
				t.tls, t.didTLS = time.Since(t.tlsStart), true
			}
		},
	}
}

func (r *loopRun) request(ctx context.Context, client *http.Client, i int, u string, timeout time.Duration) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	trace := &requestTrace{}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(reqCtx, trace.clientTrace()), http.MethodGet, u, nil)
	if err != nil {
		r.record(i, 0, 0, err, trace)
		return
	}

	start := time.Now()
	status := 0
	resp, err := client.Do(req)
	if err == nil {
		status = resp.StatusCode
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		resp.Body.Close()
	}
	latency := time.Since(start)
	if ctx.Err() != nil {
		// Stopped mid-request: the failure is the stop, not the egress path.
		return
	}
	r.record(i, latency, status, err, trace)
}

func (r *loopRun) record(i int, latency time.Duration, status int, err error, trace *requestTrace) {
	trace.mu.Lock()
	newConn := trace.gotConn && trace.newConn
	didDNS, dns := trace.didDNS, trace.dns
	tlsStarted, didTLS, tlsTime := trace.tlsStarted, trace.didTLS, trace.tls
	trace.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.stats
	s.Requests++
	s.Endpoints[i].Requests++
	if newConn {
		s.NewConns++
	}
	if didDNS {
		s.DNS.Record(dns)
	}
	if didTLS {
		s.TLS.Record(tlsTime)
	}
	class := egressapi.Classify(err, status)
	if class != "" && tlsStarted && !didTLS && !strings.HasPrefix(class, "tls: ") {
		// A failure inside the handshake, such as the gateway closing a
		// connection whose SNI no rule allows, is told apart from the same
		// error in an HTTP exchange.
		class = "tls handshake: " + class
	}
	if class == "" {
		s.Successes++
		s.Latency.Record(latency)
		return
	}
	if s.Errors == nil {
		s.Errors = make(map[string]int64)
	}
	s.Errors[class]++
	s.Endpoints[i].Errors++
}

// snapshot returns a copy of the stats so far.
func (r *loopRun) snapshot() *egressapi.Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats.Clone()
	s.Elapsed = time.Since(r.started)
	return s
}

// stop cancels the loop, waits for it to exit, and returns the final stats.
func (r *loopRun) stop() *egressapi.Stats {
	r.cancel()
	<-r.done
	r.transport.CloseIdleConnections()
	return r.snapshot()
}
