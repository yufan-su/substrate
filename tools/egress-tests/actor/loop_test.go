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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/targetcert"
)

// target is an endpoint that counts the TCP connections opened to it.
type target struct {
	*httptest.Server
	conns    atomic.Int64
	requests atomic.Int64
}

func newTarget(t *testing.T, h http.HandlerFunc) *target {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.Start()
	t.Cleanup(tg.Close)
	return tg
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }

// quietLog discards a test server's own error log.
var quietLog = log.New(io.Discard, "", 0)

// host is the target's host:port.
func (tg *target) host() string { return tg.Listener.Addr().String() }

// serve returns the actor's handler with endpoint i mapped to hosts[i], a
// host:port, in place of the in-cluster Service names. The URL keeps the
// scheme the loop asks for.
func serve(hosts ...string) http.Handler {
	return serveTLS(nil, hosts...)
}

// serveTLS is serve, verifying HTTPS endpoints against roots.
func serveTLS(roots *x509.CertPool, hosts ...string) http.Handler {
	s := newServer()
	s.rootCAs = roots
	s.endpointURL = func(i int, scheme string) string { return scheme + "://" + hosts[i] + "/" }
	return s.handler()
}

// newTLSTarget is newTarget serving HTTPS with httptest's certificate, which
// the returned pool trusts.
func newTLSTarget(t *testing.T, h http.HandlerFunc) (*target, *x509.CertPool) {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.Config.ErrorLog = quietLog // failed handshakes are what some tests expect
	tg.StartTLS()
	t.Cleanup(tg.Close)
	roots := x509.NewCertPool()
	roots.AddCert(tg.Certificate())
	return tg, roots
}

func post(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, &buf))
	return rec
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// runLoop starts a loop with req, lets it run until cond holds on the live
// stats (or fails after 10s), then stops it and returns the final stats.
func runLoop(t *testing.T, h http.Handler, req egressapi.StartRequest, cond func(*egressapi.Stats) bool) *egressapi.Stats {
	t.Helper()
	if rec := post(t, h, egressapi.StartRoute, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		rec := get(h, egressapi.StatsRoute)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /stats = %d %s", rec.Code, rec.Body)
		}
		var s egressapi.Stats
		if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
			t.Fatal(err)
		}
		if cond(&s) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not reached; last stats %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
	rec := post(t, h, egressapi.StopRoute, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /stop = %d %s", rec.Code, rec.Body)
	}
	var final egressapi.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &final); err != nil {
		t.Fatal(err)
	}
	return &final
}

func atLeast(n int64) func(*egressapi.Stats) bool {
	return func(s *egressapi.Stats) bool { return s.Requests >= n }
}

func TestKeepAliveReusesOneConnectionPerEndpoint(t *testing.T) {
	targets := []*target{newTarget(t, okHandler), newTarget(t, okHandler), newTarget(t, okHandler)}
	var hosts []string
	for _, tg := range targets {
		hosts = append(hosts, tg.host())
	}
	h := serve(hosts...)

	s := runLoop(t, h, egressapi.StartRequest{Endpoints: len(hosts)}, atLeast(30))
	if s.Scheme != egressapi.SchemeHTTP || s.TLS.Count != 0 {
		t.Errorf("a start with no scheme ran over %q with %d TLS handshakes, want http with none", s.Scheme, s.TLS.Count)
	}

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v", s.Successes, s.Requests, s.Errors)
	}
	for i, tg := range targets {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("target %d saw %d connections, want 1 kept alive", i, got)
		}
	}
	if s.NewConns != int64(len(targets)) {
		t.Errorf("NewConns = %d, want %d (one per endpoint)", s.NewConns, len(targets))
	}
	if s.Latency.Count != s.Successes {
		t.Errorf("latency samples %d, want one per success (%d)", s.Latency.Count, s.Successes)
	}
	for i, e := range s.Endpoints {
		if e.Requests < 10 {
			t.Errorf("endpoint %d got %d requests; the loop should cycle through all endpoints", i, e.Requests)
		}
	}
}

func TestNewConnPerRequest(t *testing.T) {
	tg := newTarget(t, okHandler)
	h := serve(tg.host())

	s := runLoop(t, h, egressapi.StartRequest{Endpoints: 1, NewConnPerRequest: true}, atLeast(10))

	if s.NewConns != s.Requests {
		t.Errorf("NewConns = %d, want one per request (%d)", s.NewConns, s.Requests)
	}
	if got := tg.conns.Load(); got < s.Requests {
		t.Errorf("target saw %d connections for %d requests, want at least one each", got, s.Requests)
	}
}

func TestRequestTimeoutIsClassified(t *testing.T) {
	release := make(chan struct{})
	slow := newTarget(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) })
	h := serve(slow.host())

	s := runLoop(t, h, egressapi.StartRequest{Endpoints: 1, RequestTimeoutMs: 20}, atLeast(2))

	if s.Errors["timeout"] != s.Requests || s.Successes != 0 {
		t.Errorf("errors %v, successes %d for %d requests; want every request a timeout", s.Errors, s.Successes, s.Requests)
	}
	if s.Latency.Count != 0 {
		t.Errorf("failed requests recorded %d latency samples, want 0", s.Latency.Count)
	}
	if s.Endpoints[0].Errors != s.Requests {
		t.Errorf("endpoint errors = %d, want %d", s.Endpoints[0].Errors, s.Requests)
	}
}

func TestNon200IsAnError(t *testing.T) {
	ok := newTarget(t, okHandler)
	failing := newTarget(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "egress denied", http.StatusServiceUnavailable)
	})
	h := serve(ok.host(), failing.host())

	s := runLoop(t, h, egressapi.StartRequest{Endpoints: 2}, atLeast(10))

	if s.Errors["HTTP 503"] == 0 {
		t.Errorf("errors = %v, want HTTP 503 counted", s.Errors)
	}
	if s.Endpoints[0].Errors != 0 || s.Endpoints[1].Errors != s.Endpoints[1].Requests {
		t.Errorf("endpoints = %+v, want only the second one failing", s.Endpoints)
	}
}

func TestIntervalPacesRequests(t *testing.T) {
	tg := newTarget(t, okHandler)
	h := serve(tg.host())

	start := time.Now()
	s := runLoop(t, h, egressapi.StartRequest{Endpoints: 1, IntervalMs: 20}, atLeast(5))
	elapsed := time.Since(start)

	// n requests with a 20ms pause after each cannot finish in under (n-1)*20ms.
	if minimum := time.Duration(s.Requests-1) * 20 * time.Millisecond; elapsed < minimum {
		t.Errorf("%d requests took %v, want at least %v with a 20ms interval", s.Requests, elapsed, minimum)
	}
}

func TestDNSRecordedOnlyForHostnames(t *testing.T) {
	tg := newTarget(t, okHandler)
	u, err := url.Parse(tg.URL)
	if err != nil {
		t.Fatal(err)
	}
	byIP := runLoop(t, serve(tg.host()), egressapi.StartRequest{Endpoints: 1}, atLeast(3))
	if byIP.DNS.Count != 0 {
		t.Errorf("an IP target recorded %d DNS lookups, want 0", byIP.DNS.Count)
	}

	byName := serve("localhost:" + u.Port())
	s := runLoop(t, byName, egressapi.StartRequest{Endpoints: 1, NewConnPerRequest: true}, atLeast(3))
	if s.DNS.Count == 0 {
		t.Errorf("a hostname target with a new connection per request recorded no DNS lookups (stats %+v)", s)
	}
}

func TestStartStopLifecycle(t *testing.T) {
	tg := newTarget(t, okHandler)
	h := serve(tg.host())
	req := egressapi.StartRequest{Endpoints: 1}

	if rec := get(h, egressapi.StatsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /stats before start = %d, want 404", rec.Code)
	}
	if rec := post(t, h, egressapi.StopRoute, nil); rec.Code != http.StatusNotFound {
		t.Errorf("POST /stop before start = %d, want 404", rec.Code)
	}

	first := runLoop(t, h, req, atLeast(5))
	afterStop := tg.requests.Load()
	time.Sleep(20 * time.Millisecond)
	if got := tg.requests.Load(); got != afterStop {
		t.Errorf("target got %d requests after /stop returned, want none", got-afterStop)
	}

	if rec := post(t, h, egressapi.StartRoute, req); rec.Code != http.StatusNoContent {
		t.Fatalf("restart = %d %s", rec.Code, rec.Body)
	}
	if rec := post(t, h, egressapi.StartRoute, req); rec.Code != http.StatusConflict {
		t.Errorf("second /start while running = %d, want 409", rec.Code)
	}
	rec := post(t, h, egressapi.StopRoute, nil)
	var second egressapi.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	// The target saw every request of both runs (plus any cut short by a
	// stop), so a second run that started from zero counts no more than the
	// target saw after the first run.
	if sinceRestart := tg.requests.Load() - first.Requests; second.Requests > sinceRestart {
		t.Errorf("restarted loop counted %d requests, more than the %d sent since the restart: the old counts were kept", second.Requests, sinceRestart)
	}
}

func TestStartRejectsBadRequests(t *testing.T) {
	h := newServer().handler()
	for _, body := range []string{`not json`, `{}`, `{"endpoints":0}`, `{"endpoints":1025}`, `{"endpoints":1,"scheme":"ftp"}`} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, egressapi.StartRoute, strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("POST /start %s = %d, want 400", body, rec.Code)
		}
	}
	if rec := get(h, egressapi.ReadyzRoute); rec.Code != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", rec.Code)
	}
}

func TestDefaultURLsAreTheEndpointServices(t *testing.T) {
	s := newServer()
	for _, i := range []int{0, 9, 255} {
		for _, scheme := range []string{egressapi.SchemeHTTP, egressapi.SchemeHTTPS} {
			if got, want := s.endpointURL(i, scheme), egressapi.EndpointURL(i, scheme); got != want {
				t.Errorf("endpoint %d over %s = %q, want %q", i, scheme, got, want)
			}
		}
	}
	if s.rootCAs != nil {
		t.Error("the actor's roots are set by default; they must stay nil so SSL_CERT_FILE applies")
	}
}

func TestHTTPSKeepAliveHandshakesOncePerEndpoint(t *testing.T) {
	a, _ := newTLSTarget(t, okHandler)
	b, _ := newTLSTarget(t, okHandler)
	roots := x509.NewCertPool()
	roots.AddCert(a.Certificate())
	roots.AddCert(b.Certificate())
	h := serveTLS(roots, a.host(), b.host())

	s := runLoop(t, h, egressapi.StartRequest{Endpoints: 2, Scheme: egressapi.SchemeHTTPS}, atLeast(20))

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v", s.Successes, s.Requests, s.Errors)
	}
	if s.Scheme != egressapi.SchemeHTTPS {
		t.Errorf("Scheme = %q, want https", s.Scheme)
	}
	if s.TLS.Count != 2 || s.NewConns != 2 {
		t.Errorf("%d TLS handshakes and %d new connections, want one of each per endpoint (2)", s.TLS.Count, s.NewConns)
	}
	for i, tg := range []*target{a, b} {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("target %d saw %d connections, want 1 kept alive", i, got)
		}
	}
}

func TestHTTPSNewConnHandshakesEveryRequest(t *testing.T) {
	tg, roots := newTLSTarget(t, okHandler)
	h := serveTLS(roots, tg.host())

	s := runLoop(t, h, egressapi.StartRequest{Endpoints: 1, Scheme: egressapi.SchemeHTTPS, NewConnPerRequest: true}, atLeast(10))

	if s.Successes != s.Requests || s.TLS.Count != s.Requests {
		t.Errorf("%d successes and %d TLS handshakes for %d requests, want one handshake per successful request", s.Successes, s.TLS.Count, s.Requests)
	}
}

func TestHTTPSFailuresAreClassified(t *testing.T) {
	bundle, err := targetcert.Generate(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpointCert, err := tls.X509KeyPair(bundle.Cert, bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	endpointRoots := x509.NewCertPool()
	endpointRoots.AppendCertsFromPEM(bundle.CA)
	// Serves the endpoints' certificate, which names no IP address.
	wrongName := &target{Server: httptest.NewUnstartedServer(http.HandlerFunc(okHandler))}
	wrongName.TLS = &tls.Config{Certificates: []tls.Certificate{endpointCert}}
	wrongName.Config.ErrorLog = quietLog
	wrongName.StartTLS()
	t.Cleanup(wrongName.Close)

	untrusted, _ := newTLSTarget(t, okHandler)
	plain := newTarget(t, okHandler)

	// Accepts, reads the ClientHello so closing sends a FIN rather than a
	// reset, and closes: a peer that refuses the handshake, as the gateway
	// does for an SNI no rule allows.
	closer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closer.Close() })
	go func() {
		for {
			conn, err := closer.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Read(make([]byte, 4096))
			conn.Close()
		}
	}()

	for _, tc := range []struct {
		name  string
		roots *x509.CertPool
		host  string
		want  string
	}{
		{"untrusted CA", x509.NewCertPool(), untrusted.host(), "tls: unknown authority"},
		{"hostname mismatch", endpointRoots, wrongName.host(), "tls: hostname mismatch"},
		{"plain HTTP server", x509.NewCertPool(), plain.host(), "tls: not a TLS server"},
		{"peer closes during handshake", x509.NewCertPool(), closer.Addr().String(), "tls handshake: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := serveTLS(tc.roots, tc.host)
			s := runLoop(t, h, egressapi.StartRequest{Endpoints: 1, Scheme: egressapi.SchemeHTTPS}, atLeast(2))
			var matched int64
			for class, n := range s.Errors {
				if strings.HasPrefix(class, tc.want) {
					matched += n
				}
			}
			if matched != s.Requests || s.Successes != 0 || s.TLS.Count != 0 {
				t.Errorf("errors %v, %d successes, %d handshakes for %d requests; want every request classed %q", s.Errors, s.Successes, s.TLS.Count, s.Requests, tc.want)
			}
		})
	}
}
