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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// liveSeries builds one process's reads every second from 0 to end, with
// the steady marks at begin and stop.
func liveSeries(component, container, pod string, cores float64, begin, stop, end time.Duration) []liveSample {
	t0 := time.Unix(1000, 0)
	var out []liveSample
	for d := time.Duration(0); d <= end; d += time.Second {
		l := liveSample{T: t0.Add(d), Component: component, Container: container, Pod: pod, ProcessCPUSeconds: cores * d.Seconds()}
		switch d {
		case begin:
			l.Label = markSteadyBegin
		case stop:
			l.Label = markSteadyEnd
		}
		out = append(out, l)
	}
	return out
}

// testResourceReport is a 60 s steady window at 1000 req/s with readings
// 16 s apart: gateway envoy at 1 core and ext-proc at 0.5, two workers at
// 0.25 each, and kube-dns read once inside steady.
func testResourceReport() *report {
	t0 := time.Unix(1000, 0)
	s := time.Second
	at := func(d ...time.Duration) []time.Duration { return d }
	every16 := at(0, 16*s, 32*s, 48*s, 64*s, 80*s)
	var samples []cadvisorSample
	samples = append(samples, series("gateway", "gw", "envoy", 1, every16...)...)
	samples = append(samples, series("gateway", "gw", "ext-proc", 0.5, every16...)...)
	samples = append(samples, series("gateway", "gw", "POD", 1.5, every16...)...)
	samples = append(samples, series("workers", "w1", "ateom", 0.25, every16...)...)
	samples = append(samples, series("workers", "w2", "ateom", 0.25, every16...)...)
	samples = append(samples, series("dns", "d1", "kubedns", 0.01, 0, 40*s, 90*s)...)
	for i := range samples {
		samples[i].WorkingSetBytes = 64 << 20
	}
	rep := &report{
		Phases: []phaseMark{{Name: "steady", Start: t0.Add(10 * s), End: t0.Add(70 * s)}},
		Loop:   &egressapi.Stats{Requests: 60000, Elapsed: 60 * s},
		Resources: &resourceReport{
			Samples: samples,
			Live:    liveSeries("gateway", "ext-proc", "gw", 0.4, 10*s, 70*s, 80*s),
			Driver:  driverUsage{Steady: usageWindow{CPUSeconds: 1.2, WallSeconds: 60, Cores: 0.02}},
			Envoy: []envoySample{
				{T: t0.Add(10 * s), Label: markSteadyBegin, Pod: "gw", Counters: map[string]float64{"cluster.mitm_internal.upstream_cx_overflow": 0, "cluster.mitm_internal.upstream_cx_active": 40}},
				{T: t0.Add(70 * s), Label: markSteadyEnd, Pod: "gw", Counters: map[string]float64{"cluster.mitm_internal.upstream_cx_overflow": 3, "cluster.mitm_internal.upstream_cx_active": 100,
					"cluster.mitm_internal.circuit_breakers.default.cx_open": 1, "cluster.egress_forward_proxy_cleartext.upstream_cx_active": 7,
					"cluster.mitm_internal.upstream_rq_pending_total": 160, "cluster.mitm_internal.upstream_rq_cancelled": 50}},
			},
		},
	}
	return rep
}

func TestSummarizeResources(t *testing.T) {
	t.Parallel()
	rep := testResourceReport()
	summarizeResources(rep)
	c := rep.Resources.Components

	gw := c["gateway"]
	if gw == nil {
		t.Fatal("no gateway summary")
	}
	for _, tc := range []struct {
		name      string
		got, want float64
	}{
		{"gateway mean", gw.Steady.CPUCores.Mean, 1.5},
		{"gateway max", gw.Steady.CPUCores.Max, 1.5},
		{"gateway cores per 1000 req/s", gw.Steady.CoresPerKrps, 1.5},
		{"gateway coverage", gw.Steady.Coverage, 48.0 / 60},
		{"envoy mean", gw.Containers["envoy"].CPUCores.Mean, 1},
		{"ext-proc exact", *gw.Containers["ext-proc"].LiveCPUCores, 0.4},
		{"workers mean", c["workers"].Steady.CPUCores.Mean, 0.5},
		{"workers working set", c["workers"].Steady.WorkingSetBytes, 2 * 64 << 20},
		{"driver", c["driver"].Steady.CPUCores.Mean, 0.02},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !near(tc.got, tc.want) {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
	if gw.Pods != 1 || c["workers"].Pods != 2 {
		t.Errorf("pods: gateway %d workers %d, want 1 and 2", gw.Pods, c["workers"].Pods)
	}
	if dns := c["dns"]; !dns.Steady.Insufficient || dns.Steady.CoresPerKrps != 0 {
		t.Errorf("dns steady = %+v, want insufficient with no per-request figure", dns.Steady)
	}
	metrics := map[string]int{}
	for _, p := range rep.Resources.Series {
		metrics[p.Source+" "+p.Metric]++
	}
	if metrics["cadvisor cpu_cores"] == 0 || metrics["live cpu_cores"] != 80 || metrics["envoy cluster.mitm_internal.upstream_cx_active"] != 2 {
		t.Errorf("series rows by source and metric = %v", metrics)
	}

	var out bytes.Buffer
	rep.printResources(&out)
	for _, want := range []string{
		"steady cores mean/max: gateway 1.50/1.50 workers 0.50/0.50 (cAdvisor: insufficient for dns)",
		"cores per 1000 req/s: gateway 1.50 workers 0.50 (cAdvisor coverage 80%)",
		"gateway/ext-proc 0.400",
		"driver 0.020",
		"gateway 128Mi workers 128Mi",
		"mitm_internal cx overflow +3, active max 100; cleartext cx active max 7; mitm_internal connection breaker OPENED; mitm_internal CONNECTs queued +160, cancelled +50, refused +0 pending +0 active",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("printed resources lack %q:\n%s", want, out.String())
		}
	}
}

func TestMaxSum(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(0, 0)
	s := time.Second
	steady := phaseMark{Start: t0, End: t0.Add(20 * s)}
	a := []segment{{from: t0, to: t0.Add(10 * s), cores: 1}, {from: t0.Add(10 * s), to: t0.Add(20 * s), cores: 3}}
	b := []segment{{from: t0.Add(5 * s), to: t0.Add(15 * s), cores: 2}}
	if got := maxSum([][]segment{a, b}, steady); got != 5 {
		t.Errorf("maxSum = %v, want 5: 3 cores from a and 2 from b overlap in [10s, 15s)", got)
	}
}

func TestLoopSeries(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(0, 0)
	var h1, h2 egressapi.Histogram
	for range 100 {
		h1.Record(time.Millisecond)
	}
	h2 = h1
	h2.Counts = append([]int64(nil), h1.Counts...)
	for range 100 {
		h2.Record(50 * time.Millisecond)
	}
	got := loopSeries([]loopPoint{
		{T: t0, Requests: 100, Latency: h1},
		{T: t0.Add(5 * time.Second), Requests: 600, Latency: h2},
	})
	if len(got) != 2 || got[0].Metric != "req_per_s" || got[0].Value != 100 {
		t.Fatalf("loopSeries = %+v, want 100 req/s then a p99", got)
	}
	// The second interval holds only the 50 ms samples.
	if got[1].Metric != "p99_ms" || got[1].Value < 45 || got[1].Value > 56 {
		t.Errorf("p99 of the interval = %+v, want about 50 ms", got[1])
	}
}

func TestPrintVerify(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		verify   []verifyResult
		wantPass bool
		want     []string
	}{
		{"all pass", []verifyResult{{Check: "coverage", Pass: true}, {Check: "coverage", Pass: true}}, true, []string{"coverage 2/2 ok", "verify     PASS"}},
		{"info grouped by reason", []verifyResult{
			{Check: "coverage", Scope: "a", Info: true, Note: "insufficient"},
			{Check: "coverage", Scope: "b", Info: true, Note: "insufficient"},
			{Check: "coverage", Scope: "c", Info: true, Note: "insufficient"},
			{Check: "sanity", Scope: "d", Info: true, Note: "window starts early"},
		}, true, []string{"INFO coverage: insufficient (3 series)", "INFO sanity: window starts early (d)", "coverage 0/3 ok, 3 info; sanity 0/1 ok, 1 info", "PASS"}},
		{"one failure", []verifyResult{{Check: "sanity", Scope: "gateway/envoy@gw", Got: "0.2", Want: "0.8"}, {Check: "overhead", Pass: true}}, false,
			[]string{"FAIL sanity gateway/envoy@gw: got 0.2, want 0.8", "sanity 0/1 ok; overhead 1/1 ok", "FAIL (1)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rep := &report{Resources: &resourceReport{Verify: tc.verify}}
			var out bytes.Buffer
			if got := rep.printVerify(&out); got != tc.wantPass {
				t.Errorf("printVerify = %v, want %v", got, tc.wantPass)
			}
			for _, w := range tc.want {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output lacks %q:\n%s", w, out.String())
				}
			}
		})
	}
}

func TestCheckMetricsServer(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	samples := series("gateway", "gw", "envoy", 0.8, 0, 16*s, 32*s, 48*s)
	// Readings at 9 and 41 s bracket both ends of a 10–40 s window, so the
	// bounds alone cannot judge it.
	sparse := series("gateway", "gw", "envoy", 0.8, 0, 9*s, 41*s, 48*s)
	everySecond := series("gateway", "gw", "envoy", 0.8, stamps(0, s, 49)...)
	gapAt10 := series("gateway", "gw", "envoy", 0.8, append([]time.Duration{0, 8 * s}, stamps(13*s, s, 36)...)...)
	window := podMetricsSample{T: t0.Add(40 * s), Window: 30 * s, CPUCores: 0.8}
	for _, tc := range []struct {
		name     string
		ms       podMetricsSample
		samples  []cadvisorSample
		reader   []cadvisorSample
		wantPass bool
		wantInfo bool
		wantGot  string
	}{
		{"agree", podMetricsSample{T: t0.Add(40 * s), Window: 30 * s, CPUCores: 0.78}, samples, nil, true, false, "cAdvisor bounds"},
		{"double counted", podMetricsSample{T: t0.Add(40 * s), Window: 30 * s, CPUCores: 1.6}, samples, nil, false, false, "cAdvisor bounds"},
		{"window before the readings", podMetricsSample{T: t0.Add(10 * s), Window: 30 * s, CPUCores: 0.8}, samples, nil, false, true, ""},
		{"sparse cAdvisor alone", window, sparse, nil, false, false, "cAdvisor bounds"},
		{"reader rows cover the window", window, sparse, everySecond, true, false, "cgroup reader 0.800"},
		{"reader gap at an edge", window, samples, gapAt10, true, false, "cAdvisor bounds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.ms.Component, tc.ms.Pod, tc.ms.Container = "gateway", "gw", "envoy"
			got := checkMetricsServer([]podMetricsSample{tc.ms}, tc.samples, tc.reader)
			if len(got) != 1 || got[0].Pass != tc.wantPass || got[0].Info != tc.wantInfo || !strings.Contains(got[0].Got, tc.wantGot) {
				t.Errorf("checkMetricsServer = %+v, want pass %v info %v got %q", got, tc.wantPass, tc.wantInfo, tc.wantGot)
			}
		})
	}
}

func TestCheckSteadyAttribution(t *testing.T) {
	t.Parallel()
	s := time.Second
	steady := phaseMark{Start: time.Unix(1010, 0), End: time.Unix(1070, 0)}
	good := liveSeries("gateway", "ext-proc", "gw", 0.4, 10*s, 70*s, 80*s)
	got := checkSteadyAttribution(good, steady)
	if len(got) != 1 || !got[0].Pass {
		t.Errorf("checkSteadyAttribution = %+v, want one pass", got)
	}
	// A mark that read the wrong counter puts the mean outside the rates.
	bad := liveSeries("gateway", "ext-proc", "gw", 0.4, 10*s, 70*s, 80*s)
	for i := range bad {
		if bad[i].Label == markSteadyEnd {
			bad[i].ProcessCPUSeconds *= 2
		}
	}
	if got := checkSteadyAttribution(bad, steady); len(got) != 1 || got[0].Pass {
		t.Errorf("checkSteadyAttribution with a bad mark = %+v, want a failure", got)
	}
}

func TestCheckIdlePhases(t *testing.T) {
	t.Parallel()
	s := time.Second
	t0 := time.Unix(1000, 0)
	phases := []phaseMark{
		{Name: "create", Start: t0, End: t0.Add(10 * s)},
		{Name: "steady", Start: t0.Add(10 * s), End: t0.Add(70 * s)},
		{Name: "suspend", Start: t0.Add(70 * s), End: t0.Add(80 * s)},
	}
	// ext-proc idles at 0.01 core and works at 0.5 during steady; spike adds
	// load during create.
	live := func(spike float64) []liveSample {
		var out []liveSample
		var cpu float64
		for d := time.Duration(0); d <= 80*s; d += s {
			out = append(out, liveSample{T: t0.Add(d), Component: "gateway", Container: "ext-proc", Pod: "gw", ProcessCPUSeconds: cpu})
			switch {
			case d >= 10*s && d < 70*s:
				cpu += 0.5
			case d < 10*s:
				cpu += 0.01 + spike
			default:
				cpu += 0.01
			}
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		reused int
		spike  float64
		want   []bool
	}{
		{"rerun at idle", 12, 0, []bool{true, true}},
		{"rerun with load in create", 12, 0.3, []bool{false, true}},
		{"first run is not judged", 0, 0.3, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rep := &report{Config: runConfig{Actors: 12}, Phases: phases}
			rep.Create.ActorsReused = tc.reused
			got := checkIdlePhases(live(tc.spike), rep)
			if len(got) != len(tc.want) {
				t.Fatalf("checkIdlePhases = %+v, want %d results", got, len(tc.want))
			}
			for i, w := range tc.want {
				if got[i].Pass != w {
					t.Errorf("%s: %+v, want pass %v", got[i].Scope, got[i], w)
				}
			}
		})
	}
}

func TestSettledOf(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	steady := phaseMark{Name: "steady", Start: t0, End: t0.Add(60 * s)}
	// polls builds one poll per 5 s, with new connections and requests at
	// each.
	polls := func(conns []int64) []loopPoint {
		var out []loopPoint
		for i, c := range conns {
			out = append(out, loopPoint{T: t0.Add(time.Duration(5*(i+1)) * s), NewConns: c, Requests: int64(500 * (i + 1))})
		}
		return out
	}
	full := []int64{400, 800, 1000, 1000, 1001, 1001, 1001, 1001, 1001, 1001, 1001, 1001}
	for _, tc := range []struct {
		name      string
		cfg       runConfig
		timeline  []loopPoint
		wantOK    bool
		wantStart time.Duration
		wantRule  string
		wantRate  float64
	}{
		{
			name:      "plateau, one poll after it",
			cfg:       runConfig{Endpoints: 100, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:  polls(full),
			wantOK:    true,
			wantStart: 20 * s,
			wantRule:  settledByPlateau,
			wantRate:  100,
		},
		{
			name:      "polls coarser than a round",
			cfg:       runConfig{Endpoints: 10, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:  polls(full),
			wantOK:    true,
			wantStart: 1500 * time.Millisecond,
			wantRule:  settledByRounds,
			wantRate:  100,
		},
		{
			name:      "no plateau",
			cfg:       runConfig{Endpoints: 100, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:  polls([]int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200}),
			wantOK:    true,
			wantStart: 15 * s,
			wantRule:  settledByRounds,
			wantRate:  100,
		},
		{
			name:     "no plateau and no pacing",
			cfg:      runConfig{Endpoints: 100, ProgressInterval: 5 * s},
			timeline: polls([]int64{100, 200, 300}),
		},
		{
			name:     "round outlasts steady",
			cfg:      runConfig{Endpoints: 100, RequestInterval: time.Second, ProgressInterval: 120 * s},
			timeline: polls(full),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w, ok := settledOf(tc.cfg, steady, tc.timeline)
			if ok != tc.wantOK {
				t.Fatalf("settledOf() ok = %v, want %v (window %+v)", ok, tc.wantOK, w)
			}
			if !ok {
				return
			}
			if got := w.Start.Sub(t0); got != tc.wantStart || w.Rule != tc.wantRule || !near(w.ReqPerS, tc.wantRate) {
				t.Errorf("settled from +%v by %q at %.3f req/s, want +%v by %q at %.3f req/s",
					got, w.Rule, w.ReqPerS, tc.wantStart, tc.wantRule, tc.wantRate)
			}
		})
	}
}

func TestSummarizeResourcesSettled(t *testing.T) {
	t.Parallel()
	rep := testResourceReport()
	t0 := time.Unix(1000, 0)
	s := time.Second
	// A round over 10 endpoints at 1 s takes 10 s; polls every 20 s are
	// too coarse, so the window starts 15 s into steady, at t0+25 s.
	rep.Config = runConfig{Endpoints: 10, RequestInterval: s, ProgressInterval: 20 * s}
	rep.LoopTimeline = []loopPoint{
		{T: t0.Add(30 * s), Requests: 20000, NewConns: 10},
		{T: t0.Add(50 * s), Requests: 40000, NewConns: 10},
		{T: t0.Add(70 * s), Requests: 60000, NewConns: 10},
	}
	summarizeResources(rep)
	w := rep.Resources.Settled
	if w == nil || !w.Start.Equal(t0.Add(25*s)) || w.Rule != settledByRounds || !near(w.ReqPerS, 1000) {
		t.Fatalf("settled window = %+v, want from t0+25s by %q at 1000 req/s", w, settledByRounds)
	}
	gw := rep.Resources.Components["gateway"].Settled
	if gw == nil || !near(gw.CPUCores.Mean, 1.5) || !near(gw.CoresPerKrps, 1.5) || !near(gw.Coverage, 32.0/45) {
		t.Errorf("gateway settled = %+v, want 1.5 cores, 1.5 cores/krps, coverage 32/45", gw)
	}
	var out bytes.Buffer
	rep.printResources(&out)
	want := "cores per 1000 req/s, settled: gateway 1.50 workers 0.50 (from steady +15.0s by 1.5 rounds, 1000.0 req/s)"
	if !strings.Contains(out.String(), want) {
		t.Errorf("printed resources lack %q:\n%s", want, out.String())
	}
}

// TestPrintRequestBreaker checks the memory line for a run where max_requests,
// not max_connections, refused CONNECTs: no connection overflow, active
// overflow and an open request breaker.
func TestPrintRequestBreaker(t *testing.T) {
	t.Parallel()
	rep := testResourceReport()
	t0 := time.Unix(1000, 0)
	rep.Resources.Envoy = []envoySample{
		{T: t0.Add(10 * time.Second), Label: markSteadyBegin, Pod: "gw", Counters: map[string]float64{
			"cluster.mitm_internal.upstream_cx_overflow": 0, "cluster.mitm_internal.upstream_rq_active_overflow": 0}},
		{T: t0.Add(70 * time.Second), Label: markSteadyEnd, Pod: "gw", Counters: map[string]float64{
			"cluster.mitm_internal.upstream_cx_overflow": 0, "cluster.mitm_internal.upstream_rq_active_overflow": 76,
			"cluster.mitm_internal.circuit_breakers.default.rq_open": 1}},
	}
	summarizeResources(rep)
	var out bytes.Buffer
	rep.printResources(&out)
	want := "mitm_internal request breaker OPENED; mitm_internal CONNECTs queued +0, cancelled +0, refused +0 pending +76 active"
	if !strings.Contains(out.String(), want) {
		t.Errorf("printed resources lack %q:\n%s", want, out.String())
	}
}
