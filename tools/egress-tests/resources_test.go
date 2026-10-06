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
	"maps"
	"net/url"
	"os"
	"regexp"
	"slices"
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
	// envoyPartial answers the Envoy read with one series instead of all.
	envoyPartial bool
}

// envoyBody renders /stats/prometheus text with every stat of every cluster
// the sampler reads, taking values from counters and 0 elsewhere.
func envoyBody(counters map[string]float64) []byte {
	var b strings.Builder
	for _, metric := range slices.Sorted(maps.Keys(envoyStats)) {
		fmt.Fprintf(&b, "# TYPE %s counter\n", metric)
		for _, c := range envoyClusters {
			fmt.Fprintf(&b, "%s{envoy_cluster_name=%q} %g\n", metric, c, counters["cluster."+c+"."+envoyStats[metric]])
		}
	}
	return []byte(b.String())
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
	case strings.HasSuffix(path, ":8080/proxy/healthz"):
		return []byte("ok\n"), nil
	case strings.HasSuffix(path, "/proxy/metrics/cadvisor"):
		// cAdvisor refreshes every third read, 15 s apart.
		ts := 1_700_000_000_000 + int64((n-1)/3)*15_000
		cpu := 10 + float64((n-1)/3)
		return fmt.Appendf(nil, `container_cpu_usage_seconds_total{container="envoy",cpu="total",image="envoy",namespace="ate-system",pod="atenet-egress-a"} %v %d
container_memory_working_set_bytes{container="envoy",image="envoy",namespace="ate-system",pod="atenet-egress-a"} 6e+07 %d
`, cpu, ts, ts), nil
	case path == "/apis/metrics.k8s.io/v1beta1/namespaces/ate-system/pods":
		return []byte(`{"items":[{"metadata":{"name":"atenet-egress-a","namespace":"ate-system"},"timestamp":"2023-11-14T22:13:20Z","window":"30s",
"containers":[{"name":"envoy","usage":{"cpu":"812m","memory":"64Mi"}},{"name":"other","usage":{"cpu":"1"}}]}]}`), nil
	case strings.HasPrefix(path, "/apis/metrics.k8s.io/"):
		return []byte(`{"items":[]}`), nil
	case strings.HasSuffix(path, ":15090/proxy/stats/prometheus"):
		if params.Get("filter") != envoyStatsFilter {
			return nil, fmt.Errorf("unexpected filter %q", params.Get("filter"))
		}
		if g.envoyPartial {
			return []byte("envoy_cluster_upstream_cx_total{envoy_cluster_name=\"mitm_internal\"} 1\n"), nil
		}
		return envoyBody(map[string]float64{envoyCxTotal: g.envoyCx(), "cluster.mitm_internal.upstream_cx_active": 3}), nil
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
		containers := []corev1.Container{{Name: "ate-api-server"}}
		if app == "atenet-egress" {
			containers = []corev1.Container{{Name: "envoy"}, {Name: "ext-proc"}}
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ate-system", Labels: map[string]string{"app": app}},
			Spec:       corev1.PodSpec{NodeName: "node-a", Containers: containers},
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
	body := `# HELP envoy_cluster_upstream_cx_total Multiline...
# TYPE envoy_cluster_upstream_cx_total counter
envoy_cluster_upstream_cx_total{envoy_cluster_name="mitm_internal"} 42
envoy_cluster_upstream_cx_total{envoy_cluster_name="egress_forward_proxy_cleartext"} 7 1791287695035
envoy_cluster_upstream_cx_total{envoy_cluster_name="ext_proc"} 9
envoy_cluster_upstream_cx_active{envoy_cluster_name="mitm_internal"} 3
envoy_cluster_circuit_breakers_default_cx_open{envoy_cluster_name="mitm_internal"} 1
envoy_cluster_upstream_rq_pending_active{envoy_cluster_name="mitm_internal"} 11
envoy_cluster_upstream_rq_pending_overflow{envoy_cluster_name="mitm_internal"} 0
envoy_cluster_upstream_rq_cancelled{envoy_cluster_name="mitm_internal"} 76
envoy_cluster_circuit_breakers_high_cx_open{envoy_cluster_name="mitm_internal"} 1
envoy_cluster_upstream_cx_total_x{envoy_cluster_name="mitm_internal"} 1
envoy_cluster_upstream_cx_length_ms_bucket{envoy_cluster_name="mitm_internal",le="0.5"} 0
envoy_server_uptime{} 12
not a metric
`
	got := parseEnvoyStats([]byte(body))
	want := map[string]float64{
		envoyCxTotal: 42,
		"cluster.egress_forward_proxy_cleartext.upstream_cx_total": 7,
		"cluster.mitm_internal.upstream_cx_active":                 3,
		"cluster.mitm_internal.circuit_breakers.default.cx_open":   1,
		"cluster.mitm_internal.upstream_rq_pending_active":         11,
		"cluster.mitm_internal.upstream_rq_pending_overflow":       0,
		"cluster.mitm_internal.upstream_rq_cancelled":              76,
	}
	if !maps.Equal(got, want) {
		t.Errorf("parseEnvoyStats = %v, want %v", got, want)
	}
}

// TestEnvoyStatsFilterMatchesTable pins the filter sent to Envoy to the
// stats the parser reads, so neither can drift from the other.
func TestEnvoyStatsFilterMatchesTable(t *testing.T) {
	t.Parallel()
	filter := regexp.MustCompile(envoyStatsFilter)
	for _, c := range envoyClusters {
		for _, stat := range envoyStats {
			if name := "cluster." + c + "." + stat; !filter.MatchString(name) {
				t.Errorf("envoyStatsFilter does not select %s", name)
			}
		}
	}
	for _, name := range []string{"cluster.ext_proc.upstream_cx_total", "cluster.mitm_internal.upstream_cx_total_x", "cluster.mitm_internal.circuit_breakers.high.cx_open"} {
		if filter.MatchString(name) {
			t.Errorf("envoyStatsFilter selects %s", name)
		}
	}
}

// A read whose body lacks any of the expected series is a failed read, not
// a sample of zeros: Envoy serves every stat of a cluster from the start.
func TestReadEnvoyRejectsPartialBody(t *testing.T) {
	t.Parallel()
	get := newFakeGetter()
	get.envoyPartial = true
	s := newResourceSampler(get, fake.NewSimpleClientset(), time.Second)
	s.readEnvoy(t.Context(), "atenet-egress-a", "")
	get.envoyPartial = false
	s.readEnvoy(t.Context(), "atenet-egress-a", "")
	rep := s.stop()
	if len(rep.Envoy) != 1 || rep.Errors["envoy"] != 1 {
		t.Errorf("%d envoy samples and %d errors, want the partial body counted as an error and the full one kept", len(rep.Envoy), rep.Errors["envoy"])
	}
}

// TestEnvoyStatsNamesAreReal reads a dump captured from a gateway, so the
// Prometheus names in envoyStats are the ones Envoy actually serves.
func TestEnvoyStatsNamesAreReal(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile("testdata/envoy-stats-prometheus.txt")
	if err != nil {
		t.Fatal(err)
	}
	got := parseEnvoyStats(body)
	for _, c := range envoyClusters {
		for _, stat := range envoyStats {
			if _, ok := got["cluster."+c+"."+stat]; !ok {
				t.Errorf("the gateway dump has no %s for %s", stat, c)
			}
		}
	}
	if len(got) != len(envoyClusters)*len(envoyStats) {
		t.Errorf("parsed %d stats from the dump, want %d", len(got), len(envoyClusters)*len(envoyStats))
	}
	if got[envoyCxTotal] == 0 {
		t.Errorf("the dump's %s is 0; it was captured during a run and should carry real values", envoyCxTotal)
	}
}

func TestCheckEnvoyConnections(t *testing.T) {
	t.Parallel()
	// at builds one Envoy pod's read at a mark, with the pending-queue
	// counters at zero unless set.
	type counters struct{ cx, over, cancelled, refused, queued float64 }
	at := func(label string, pods ...counters) []envoySample {
		var out []envoySample
		for i, c := range pods {
			out = append(out, envoySample{Label: label, Pod: fmt.Sprint(i), Counters: map[string]float64{
				envoyCxTotal: c.cx, envoyCxOverflow: c.over, envoyRqCancelled: c.cancelled, envoyRqRefused: c.refused, envoyPendingActive: c.queued}})
		}
		return out
	}
	noQueue := func(label string, cx float64) []envoySample {
		return []envoySample{{Label: label, Pod: "0", Counters: map[string]float64{envoyCxTotal: cx}}}
	}
	for _, tc := range []struct {
		name     string
		samples  []envoySample
		newConns int64
		want     bool
		wantInfo bool
	}{
		{name: "equal", samples: append(at(markStartBegin, counters{cx: 10}), at(markStopEnd, counters{cx: 110})...), newConns: 100, want: true},
		{name: "summed over pods", samples: append(at(markStartBegin, counters{cx: 5}, counters{cx: 5}), at(markStopEnd, counters{cx: 55}, counters{cx: 65})...), newConns: 110, want: true},
		{name: "within slack", samples: append(at(markStartBegin, counters{}), at(markStopEnd, counters{cx: 4})...), newConns: 2, want: true},
		{name: "off by more than 2%", samples: append(at(markStartBegin, counters{}), at(markStopEnd, counters{cx: 1100})...), newConns: 1000, want: false},
		{name: "held, then cancelled or refused, explains the gap", samples: append(at(markStartBegin, counters{over: 1}), at(markStopEnd, counters{cx: 1044, over: 176, cancelled: 140, refused: 4})...), newConns: 1188, want: true},
		{name: "still queued at the end explains the gap", samples: append(at(markStartBegin, counters{}), at(markStopEnd, counters{cx: 990, over: 10, queued: 10})...), newConns: 1000, want: true},
		{name: "over-count under overflow fails", samples: append(at(markStartBegin, counters{}), at(markStopEnd, counters{cx: 1300, over: 176, cancelled: 140})...), newConns: 1188, want: false},
		{name: "gap the queue does not explain fails", samples: append(at(markStartBegin, counters{}), at(markStopEnd, counters{cx: 1000, over: 176, cancelled: 50})...), newConns: 1188, want: false},
		{name: "no pending counters is informational", samples: append(noQueue(markStartBegin, 0), noQueue(markStopEnd, 100)...), newConns: 100, wantInfo: true},
		{name: "no begin read", samples: at(markStopEnd, counters{cx: 10}), newConns: 10, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := checkEnvoyConnections(tc.samples, tc.newConns)
			if got.Pass != tc.want || got.Info != tc.wantInfo {
				t.Errorf("checkEnvoyConnections = %+v, want pass %v info %v", got, tc.want, tc.wantInfo)
			}
			if !strings.HasPrefix(got.Got, "no reading") && !strings.Contains(got.Got, "opened") {
				t.Errorf("got %q, want the opened count kept", got.Got)
			}
		})
	}
}

func TestCheckLiveGaps(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	// series reads every step for n intervals, with the listed intervals
	// stretched to slow.
	series := func(step time.Duration, n int, slow time.Duration, at ...int) []liveSample {
		var out []liveSample
		t := t0
		for i := 0; i <= n; i++ {
			out = append(out, liveSample{T: t, Component: "gateway", Container: "ext-proc", Pod: "p"})
			d := step
			if slices.Contains(at, i) {
				d = slow
			}
			t = t.Add(d)
		}
		return out
	}
	tenSlow := []int{5, 15, 25, 35, 45, 55, 65, 75, 85, 95}
	for _, tc := range []struct {
		name               string
		samples            []liveSample
		interval           time.Duration
		wantPass, wantInfo bool
	}{
		{"every second", series(time.Second, 130, 0), time.Second, true, false},
		{"one 2.4 s gap in 130 intervals", series(time.Second, 130, 2400*time.Millisecond, 60), time.Second, false, true},
		{"one 6 s gap", series(time.Second, 130, 6*time.Second, 60), time.Second, false, false},
		{"ten 2.5 s gaps in 130 intervals", series(time.Second, 130, 2500*time.Millisecond, tenSlow...), time.Second, false, false},
		{"single read", series(time.Second, 0, 0), time.Second, false, false},
		{"5 s interval, one 12 s gap", series(5*time.Second, 100, 12*time.Second, 50), 5 * time.Second, false, true},
		{"5 s interval, one 16 s gap", series(5*time.Second, 100, 16*time.Second, 50), 5 * time.Second, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := checkLiveGaps(tc.samples, tc.interval, phaseMark{Start: t0, End: t0.Add(time.Hour)})
			if len(got) != 1 || got[0].Pass != tc.wantPass || got[0].Info != tc.wantInfo {
				t.Errorf("checkLiveGaps = %+v, want pass %v info %v", got, tc.wantPass, tc.wantInfo)
			}
			if !strings.HasPrefix(got[0].Got, "n=") {
				t.Errorf("got %q, want the read count and longest gap kept", got[0].Got)
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
	s.cadvisorInterval = 2 * time.Millisecond

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
	if len(rep.Envoy) < 3 || len(rep.Sources) != 3 {
		t.Errorf("%d envoy reads and sources %+v, want at least 3 reads and 3 sources", len(rep.Envoy), rep.Sources)
	}
	cadvisorReads := get.readsOf("/api/v1/nodes/node-a/proxy/metrics/cadvisor")
	if want := (cadvisorReads + 2) / 3; len(rep.Samples) != want || cadvisorReads < 6 {
		t.Errorf("%d cAdvisor samples after %d reads, want one per new timestamp (%d)", len(rep.Samples), cadvisorReads, want)
	}
	if len(rep.MetricsServer) != 1 || rep.MetricsServer[0].CPUCores != 0.812 || rep.MetricsServer[0].Window != 30*time.Second {
		t.Errorf("metrics-server readings = %+v, want envoy at 0.812 cores over 30s", rep.MetricsServer)
	}
	if !slices.Equal(s.missing, []string{"workers", "router", "targets", "dns"}) {
		t.Errorf("missing components = %v, want the four with no pods in the fake", s.missing)
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
		if strings.HasPrefix(v.Scope, "live ") {
			checks["live coverage"]++
		}
		if v.Check == "envoy-connections" && !v.Pass {
			t.Errorf("envoy-connections failed: %+v", v)
		}
	}
	if checks["envoy-connections"] != 1 || checks["overhead"] != 1 || checks["live coverage"] != 3 || checks["conservation"] == 0 {
		t.Errorf("self-checks run = %v, want envoy-connections, overhead, conservation and live coverage for 3 processes", checks)
	}
	if len(rep.LoopTimeline) == 0 || rep.LoopTimeline[len(rep.LoopTimeline)-1].Requests == 0 {
		t.Errorf("loop timeline = %+v, want the progress polls", rep.LoopTimeline)
	}
}

func TestSettle(t *testing.T) {
	t.Parallel()
	s := newResourceSampler(newFakeGetter(), fake.NewSimpleClientset(), time.Second)
	t0 := time.Unix(1000, 0)
	clock := t0
	var mu sync.Mutex
	s.now = func() time.Time { mu.Lock(); defer mu.Unlock(); clock = clock.Add(10 * time.Second); return clock }
	s.lastTS = map[string]time.Time{"a": t0.Add(time.Hour), "b": t0}
	start := time.Now()
	s.settle(t.Context(), t0.Add(time.Minute))
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("settle ran %v, want it bounded by settleTimeout on the sampler clock", elapsed)
	}
	s.lastTS["b"] = t0.Add(2 * time.Hour)
	start = time.Now()
	s.settle(t.Context(), t0.Add(time.Minute))
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("settle waited %v with every series already past the mark", elapsed)
	}
}

// TestReadProcessStampsMidpoint checks that a live read is stamped halfway
// through its round trip and keeps the round trip.
func TestReadProcessStampsMidpoint(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		rtt  time.Duration
	}{
		{name: "fast", rtt: 20 * time.Millisecond},
		{name: "slow", rtt: 2400 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newResourceSampler(newFakeGetter(), fake.NewSimpleClientset(), time.Second)
			calls := 0
			s.now = func() time.Time {
				calls++
				if calls == 1 {
					return start
				}
				return start.Add(tc.rtt)
			}
			s.readProcess(t.Context(), liveTargets[0], "atenet-egress-a", "")
			if len(s.rep.Live) != 1 {
				t.Fatalf("%d live samples, want 1", len(s.rep.Live))
			}
			got := s.rep.Live[0]
			if want := start.Add(tc.rtt / 2); !got.T.Equal(want) || got.RTT != tc.rtt {
				t.Errorf("sample at %v with RTT %v, want %v with RTT %v", got.T, got.RTT, want, tc.rtt)
			}
		})
	}
}
