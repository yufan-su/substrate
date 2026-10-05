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
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
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
	var urls []string
	for _, tg := range targets {
		urls = append(urls, tg.URL+"/")
	}
	h := newServer().handler()

	s := runLoop(t, h, egressapi.StartRequest{URLs: urls}, atLeast(30))

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
	h := newServer().handler()

	s := runLoop(t, h, egressapi.StartRequest{URLs: []string{tg.URL + "/"}, NewConnPerRequest: true}, atLeast(10))

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
	h := newServer().handler()

	s := runLoop(t, h, egressapi.StartRequest{URLs: []string{slow.URL + "/"}, RequestTimeoutMs: 20}, atLeast(2))

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
	h := newServer().handler()

	s := runLoop(t, h, egressapi.StartRequest{URLs: []string{ok.URL + "/", failing.URL + "/"}}, atLeast(10))

	if s.Errors["HTTP 503"] == 0 {
		t.Errorf("errors = %v, want HTTP 503 counted", s.Errors)
	}
	if s.Endpoints[0].Errors != 0 || s.Endpoints[1].Errors != s.Endpoints[1].Requests {
		t.Errorf("endpoints = %+v, want only the second one failing", s.Endpoints)
	}
}

func TestIntervalPacesRequests(t *testing.T) {
	tg := newTarget(t, okHandler)
	h := newServer().handler()

	start := time.Now()
	s := runLoop(t, h, egressapi.StartRequest{URLs: []string{tg.URL + "/"}, IntervalMs: 20}, atLeast(5))
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
	h := newServer().handler()

	byIP := runLoop(t, h, egressapi.StartRequest{URLs: []string{tg.URL + "/"}}, atLeast(3))
	if byIP.DNS.Count != 0 {
		t.Errorf("an IP target recorded %d DNS lookups, want 0", byIP.DNS.Count)
	}

	byName := "http://localhost:" + u.Port() + "/"
	s := runLoop(t, h, egressapi.StartRequest{URLs: []string{byName}, NewConnPerRequest: true}, atLeast(3))
	if s.DNS.Count == 0 {
		t.Errorf("a hostname target with a new connection per request recorded no DNS lookups (stats %+v)", s)
	}
}

func TestStartStopLifecycle(t *testing.T) {
	tg := newTarget(t, okHandler)
	h := newServer().handler()
	req := egressapi.StartRequest{URLs: []string{tg.URL + "/"}}

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
	for _, body := range []string{`not json`, `{}`, `{"urls":["https://x/"]}`} {
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
