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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// fakeGetter answers raw API server paths. Each Go process's CPU counter
// rises by 0.01 s per read; Envoy's cx_total comes from envoyCx.
type fakeGetter struct {
	mu      sync.Mutex
	reads   map[string]int
	fail    map[string]bool // path -> always fail
	envoyCx func() float64
}

func newFakeGetter() *fakeGetter {
	return &fakeGetter{reads: map[string]int{}, fail: map[string]bool{}, envoyCx: func() float64 { return 0 }}
}

func (g *fakeGetter) GetRaw(_ context.Context, path string, params url.Values) ([]byte, error) {
	g.mu.Lock()
	g.reads[path]++
	n := g.reads[path]
	fail := g.fail[path]
	g.mu.Unlock()
	if fail {
		return nil, errors.New("proxy error")
	}
	switch {
	case strings.HasSuffix(path, ":9090/proxy/metrics"):
		return fmt.Appendf(nil, "# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total %.2f\nprocess_resident_memory_bytes 5e+07\n", 100+0.01*float64(n)), nil
	case strings.HasSuffix(path, ":15000/proxy/stats"):
		if params.Get("filter") != envoyStatsFilter {
			return nil, fmt.Errorf("unexpected filter %q", params.Get("filter"))
		}
		return fmt.Appendf(nil, "%s: %.0f\ncluster.mitm_internal.upstream_cx_active: 3\n", envoyCxTotal, g.envoyCx()), nil
	}
	return nil, fmt.Errorf("unexpected path %s", path)
}

func (g *fakeGetter) readsOf(path string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reads[path]
}

// addSystemPods creates the gateway and ateapi pods the sampler looks for.
func addSystemPods(t *testing.T, k8s kubernetes.Interface) {
	t.Helper()
	for name, app := range map[string]string{
		"atenet-egress-a":  "atenet-egress",
		"ate-api-server-a": "ate-api-server",
		"ate-api-server-b": "ate-api-server",
	} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ate-system", Labels: map[string]string{"app": app}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		}
		if _, err := k8s.CoreV1().Pods("ate-system").Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestParseProcessMetrics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		body             string
		wantCPU, wantRSS float64
		wantErr          bool
	}{
		{name: "both", body: "# HELP x\nprocess_cpu_seconds_total 127.37\nprocess_resident_memory_bytes 5.8896384e+07\n", wantCPU: 127.37, wantRSS: 58896384},
		{name: "with timestamp", body: "process_cpu_seconds_total 1.5 1791287695035\n", wantCPU: 1.5},
		{name: "labeled lookalike ignored", body: "process_cpu_seconds_total_x 9\nprocess_cpu_seconds_total 2\n", wantCPU: 2},
		{name: "missing cpu", body: "process_resident_memory_bytes 1\n", wantErr: true},
		{name: "bad value", body: "process_cpu_seconds_total abc\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cpu, rss, err := parseProcessMetrics([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if cpu != tc.wantCPU || rss != tc.wantRSS {
				t.Errorf("got cpu %v rss %v, want %v %v", cpu, rss, tc.wantCPU, tc.wantRSS)
			}
		})
	}
}

func TestParseEnvoyStats(t *testing.T) {
	t.Parallel()
	got := parseEnvoyStats([]byte("cluster.mitm_internal.upstream_cx_total: 42\ncluster.x.upstream_rq_time: P0(nan,1)\nnot a stat\n"))
	if len(got) != 1 || got[envoyCxTotal] != 42 {
		t.Errorf("parseEnvoyStats = %v, want only %s=42", got, envoyCxTotal)
	}
}

func TestCheckEnvoyConnections(t *testing.T) {
	t.Parallel()
	at := func(label string, cx ...float64) []envoySample {
		var out []envoySample
		for i, v := range cx {
			out = append(out, envoySample{Label: label, Pod: fmt.Sprint(i), Counters: map[string]float64{envoyCxTotal: v}})
		}
		return out
	}
	for _, tc := range []struct {
		name     string
		samples  []envoySample
		newConns int64
		want     bool
	}{
		{name: "equal", samples: append(at(markStartBegin, 10), at(markStopEnd, 110)...), newConns: 100, want: true},
		{name: "summed over pods", samples: append(at(markStartBegin, 5, 5), at(markStopEnd, 55, 65)...), newConns: 110, want: true},
		{name: "within slack", samples: append(at(markStartBegin, 0), at(markStopEnd, 4)...), newConns: 2, want: true},
		{name: "off by more than 2%", samples: append(at(markStartBegin, 0), at(markStopEnd, 1100)...), newConns: 1000, want: false},
		{name: "no begin read", samples: at(markStopEnd, 10), newConns: 10, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := checkEnvoyConnections(tc.samples, tc.newConns); got.Pass != tc.want {
				t.Errorf("checkEnvoyConnections = %+v, want pass %v", got, tc.want)
			}
		})
	}
}

func TestCheckLiveGaps(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	series := func(offsets ...time.Duration) []liveSample {
		var out []liveSample
		for _, o := range offsets {
			out = append(out, liveSample{T: t0.Add(o), Component: "gateway", Container: "ext-proc", Pod: "p"})
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		samples []liveSample
		want    bool
	}{
		{name: "every second", samples: series(0, time.Second, 2*time.Second), want: true},
		{name: "one missed poll", samples: series(0, 2*time.Second), want: true},
		{name: "two missed polls", samples: series(0, 3*time.Second), want: false},
		{name: "single read", samples: series(0), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := checkLiveGaps(tc.samples, time.Second)
			if len(got) != 1 || got[0].Pass != tc.want {
				t.Errorf("checkLiveGaps = %+v, want one result with pass %v", got, tc.want)
			}
		})
	}
}

func TestCheckDriverOverhead(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		cores float64
		want  bool
	}{{0.03, true}, {0.5, true}, {0.8, false}} {
		got := checkDriverOverhead(driverUsage{WholeRun: usageWindow{Cores: tc.cores}})
		if got.Pass != tc.want {
			t.Errorf("checkDriverOverhead(%v cores) = %+v, want pass %v", tc.cores, got, tc.want)
		}
	}
}

func TestResourceSampler(t *testing.T) {
	t.Parallel()
	k8s := fake.NewSimpleClientset()
	addSystemPods(t, k8s)
	get := newFakeGetter()
	broken := podProxyPath("ate-system", "ate-api-server-b", 9090, "/metrics")
	get.fail[broken] = true
	s := newResourceSampler(get, k8s, 5*time.Millisecond)

	if err := s.start(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.mark(t.Context(), markSteadyBegin)
	time.Sleep(40 * time.Millisecond)
	s.mark(t.Context(), markSteadyEnd)
	rep := s.stop()
	if again := s.stop(); len(again.Live) != len(rep.Live) {
		t.Errorf("second stop changed the samples: %d then %d", len(rep.Live), len(again.Live))
	}

	pods := map[string]int{}
	labeled := 0
	for _, l := range rep.Live {
		pods[l.Pod]++
		if l.Label != "" {
			labeled++
		}
	}
	if pods["atenet-egress-a"] < 3 || pods["ate-api-server-a"] < 3 {
		t.Errorf("live reads per pod = %v, want at least 3 for each working pod", pods)
	}
	if pods["ate-api-server-b"] != 0 || rep.Errors["live"] != get.readsOf(broken) {
		t.Errorf("broken pod: %d samples and %d errors after %d reads, want 0 samples and one error per read",
			pods["ate-api-server-b"], rep.Errors["live"], get.readsOf(broken))
	}
	if labeled != 4 {
		t.Errorf("%d labeled live reads, want 2 marks x 2 working pods", labeled)
	}
	if len(rep.Envoy) < 3 || len(rep.Sources) != 2 {
		t.Errorf("%d envoy reads and sources %+v, want at least 3 reads and 2 sources", len(rep.Envoy), rep.Sources)
	}
	if rep.Driver.WholeRun.WallSeconds <= 0 || rep.Driver.Steady.WallSeconds <= 0 {
		t.Errorf("driver usage %+v, want whole-run and steady windows", rep.Driver)
	}
}

func TestRunWithResources(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	addSystemPods(t, r.k8s)
	get := newFakeGetter()
	// The gateway opens one connection per started loop; the fake router's
	// loops each report one new connection.
	get.envoyCx = func() float64 {
		fr.mu.Lock()
		defer fr.mu.Unlock()
		return 50 + float64(len(fr.starts))
	}
	r.res = newResourceSampler(get, r.k8s, 5*time.Millisecond)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	var names []string
	for i, p := range rep.Phases {
		names = append(names, p.Name)
		if p.End.Before(p.Start) || (i > 0 && p.Start.Before(rep.Phases[i-1].End)) {
			t.Errorf("phase %s runs %v to %v, out of order", p.Name, p.Start, p.End)
		}
	}
	if got, want := strings.Join(names, ","), "create,resume,start,steady,stop,suspend"; got != want {
		t.Errorf("phases = %s, want %s", got, want)
	}
	if rep.Resources == nil {
		t.Fatal("no resources section")
	}
	// Overhead and gaps depend on the test machine's load; only check they ran.
	checks := map[string]int{}
	for _, v := range rep.Resources.Verify {
		checks[v.Check]++
		if v.Check == "envoy-connections" && !v.Pass {
			t.Errorf("envoy-connections failed: %+v", v)
		}
	}
	if checks["envoy-connections"] != 1 || checks["overhead"] != 1 || checks["coverage"] != 3 {
		t.Errorf("self-checks run = %v, want envoy-connections, overhead and coverage for 3 processes", checks)
	}
	if len(rep.LoopTimeline) == 0 || rep.LoopTimeline[len(rep.LoopTimeline)-1].Requests == 0 {
		t.Errorf("loop timeline = %+v, want the progress polls", rep.LoopTimeline)
	}
}
