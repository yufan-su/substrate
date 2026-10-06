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
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
)

// Phase marks that force a read of every resource source.
const (
	markStartBegin  = "start:begin"
	markSteadyBegin = "steady:begin"
	markSteadyEnd   = "steady:end"
	markStopEnd     = "stop:end"
)

// verifyResult is one self-check of the resource data.
type verifyResult struct {
	Check string `json:"check"`
	Scope string `json:"scope"`
	Want  string `json:"want"`
	Got   string `json:"got"`
	Pass  bool   `json:"pass"`
	// Info results report a fact but never fail the run.
	Info bool `json:"info,omitempty"`
}

// Thresholds of the self-checks.
const (
	// The gateway opens one mitm_internal connection per actor connection;
	// 2% allows for connections the run did not make.
	cxTolerance = 0.02
	cxSlack     = 2
	// A driver above half a core is doing more than control traffic.
	maxDriverCores = 0.5
	// A live source may miss one poll.
	maxLiveGapPolls = 2
)

// within reports whether got is within rel of want, or within abs of it.
func within(got, want, rel, abs float64) bool {
	d := math.Abs(got - want)
	return d <= abs || d <= rel*math.Abs(want)
}

// checkEnvoyConnections compares the connections the gateway opened toward
// the actors' tunnels, from the start of the loops to the end of the stop,
// with the new connections the actors counted.
func checkEnvoyConnections(samples []envoySample, actorNewConns int64) verifyResult {
	res := verifyResult{Check: "envoy-connections", Scope: envoyCxTotal,
		Want: fmt.Sprintf("%d (actor new connections) ±%.0f%%", actorNewConns, 100*cxTolerance)}
	begin, okB := sumLabeled(samples, markStartBegin, envoyCxTotal)
	end, okE := sumLabeled(samples, markStopEnd, envoyCxTotal)
	if !okB || !okE {
		res.Got = "no reading at " + markStartBegin + " and " + markStopEnd
		return res
	}
	delta := end - begin
	res.Got = fmt.Sprintf("%.0f", delta)
	res.Pass = within(delta, float64(actorNewConns), cxTolerance, cxSlack)
	return res
}

// sumLabeled adds counter over the Envoy pods read at the labeled mark.
func sumLabeled(samples []envoySample, label, counter string) (float64, bool) {
	var sum float64
	found := false
	for _, s := range samples {
		if s.Label != label {
			continue
		}
		v, ok := s.Counters[counter]
		if !ok {
			return 0, false
		}
		sum += v
		found = true
	}
	return sum, found
}

// checkDriverOverhead bounds the driver's own CPU over the run.
func checkDriverOverhead(d driverUsage) verifyResult {
	return verifyResult{
		Check: "overhead", Scope: "driver",
		Want: fmt.Sprintf("≤ %.2f core", maxDriverCores),
		Got:  fmt.Sprintf("%.3f core", d.WholeRun.Cores),
		Pass: d.WholeRun.Cores <= maxDriverCores,
	}
}

// checkLiveGaps reports, per process, the longest gap between reads. Labeled
// reads count too: they are reads like any other.
func checkLiveGaps(samples []liveSample, interval time.Duration) []verifyResult {
	byPod := map[string][]time.Time{}
	for _, s := range samples {
		key := s.Component + "/" + s.Container + "@" + s.Pod
		byPod[key] = append(byPod[key], s.T)
	}
	limit := maxLiveGapPolls * interval
	var out []verifyResult
	for _, key := range slices.Sorted(maps.Keys(byPod)) {
		ts := byPod[key]
		slices.SortFunc(ts, func(a, b time.Time) int { return a.Compare(b) })
		var gap time.Duration
		for i := 1; i < len(ts); i++ {
			gap = max(gap, ts[i].Sub(ts[i-1]))
		}
		out = append(out, verifyResult{
			Check: "coverage", Scope: "live " + key,
			Want: fmt.Sprintf("max gap ≤ %v", limit),
			Got:  fmt.Sprintf("n=%d max gap %v", len(ts), gap.Round(time.Millisecond)),
			Pass: len(ts) >= 2 && gap <= limit,
		})
	}
	return out
}

// Thresholds of the cgroup self-checks.
const (
	// The pod cgroup is its containers' sum plus an idle pause; the 2%
	// covers interpolating counters that cAdvisor stamps at different times.
	rollupTolerance = 0.02
	// A Go process is its container's only process; the cgroup can exceed it
	// by charging exec'd probes, and the counter ticks at 0.01 s.
	processTolerance = 0.03
	// cpuSlack is the absolute slack, in CPU seconds, of both checks.
	cpuSlack = 0.05
)

// point is one counter reading.
type point struct {
	t time.Time
	v float64
}

// counterAt interpolates the counter linearly at t. It reports false when t
// lies outside the readings.
func counterAt(ps []point, t time.Time) (float64, bool) {
	if len(ps) == 0 || t.Before(ps[0].t) || t.After(ps[len(ps)-1].t) {
		return 0, false
	}
	i, _ := slices.BinarySearchFunc(ps, t, func(p point, t time.Time) int { return p.t.Compare(t) })
	if ps[i].t.Equal(t) {
		return ps[i].v, true
	}
	a, b := ps[i-1], ps[i]
	f := t.Sub(a.t).Seconds() / b.t.Sub(a.t).Seconds()
	return a.v + f*(b.v-a.v), true
}

// commonSpan is the longest interval every series covers.
func commonSpan(series ...[]point) (from, to time.Time, ok bool) {
	for i, ps := range series {
		if len(ps) < 2 {
			return from, to, false
		}
		if i == 0 || ps[0].t.After(from) {
			from = ps[0].t
		}
		if i == 0 || ps[len(ps)-1].t.Before(to) {
			to = ps[len(ps)-1].t
		}
	}
	return from, to, to.After(from)
}

// deltaOver is each series' increase over their common span.
func deltaOver(series ...[]point) (deltas []float64, span time.Duration, ok bool) {
	from, to, ok := commonSpan(series...)
	if !ok {
		return nil, 0, false
	}
	for _, ps := range series {
		a, _ := counterAt(ps, from)
		b, _ := counterAt(ps, to)
		deltas = append(deltas, b-a)
	}
	return deltas, to.Sub(from), true
}

// cpuSeries groups the cAdvisor CPU counters by component/container@pod.
func cpuSeries(samples []cadvisorSample) map[string][]point {
	out := map[string][]point{}
	for _, s := range samples {
		out[s.key()] = append(out[s.key()], point{s.T, s.CPUSeconds})
	}
	for _, ps := range out {
		slices.SortFunc(ps, func(a, b point) int { return a.t.Compare(b.t) })
	}
	return out
}

// checkPodRollups compares each complete pod's cgroup with its containers'
// sum over the span they all cover.
func checkPodRollups(samples []cadvisorSample, pods map[string]cadvisorPod) []verifyResult {
	series := cpuSeries(samples)
	var out []verifyResult
	for _, nsPod := range slices.Sorted(maps.Keys(pods)) {
		p := pods[nsPod]
		if !p.complete {
			continue
		}
		pod := nsPod[strings.Index(nsPod, "/")+1:]
		parts := [][]point{series[p.component+"/"+podContainer+"@"+pod]}
		for _, c := range p.containers {
			parts = append(parts, series[p.component+"/"+c+"@"+pod])
		}
		res := verifyResult{Check: "conservation", Scope: "pod rollup " + p.component + "@" + pod,
			Want: fmt.Sprintf("pod = Σ containers ±%.0f%%", 100*rollupTolerance)}
		deltas, span, ok := deltaOver(parts...)
		if !ok {
			res.Got, res.Info = "too few readings", true
			out = append(out, res)
			continue
		}
		var sum float64
		for _, d := range deltas[1:] {
			sum += d
		}
		res.Got = fmt.Sprintf("pod %.2fs Σ %.2fs over %v", deltas[0], sum, span.Round(time.Second))
		res.Pass = within(sum, deltas[0], rollupTolerance, cpuSlack)
		out = append(out, res)
	}
	return out
}

// checkProcessVsCgroup compares each Go process's CPU counter with its
// container's cgroup over the span both cover.
func checkProcessVsCgroup(live []liveSample, samples []cadvisorSample) []verifyResult {
	proc := map[string][]point{}
	for _, l := range live {
		k := l.Component + "/" + l.Container + "@" + l.Pod
		proc[k] = append(proc[k], point{l.T, l.ProcessCPUSeconds})
	}
	cg := cpuSeries(samples)
	var out []verifyResult
	for _, k := range slices.Sorted(maps.Keys(proc)) {
		ps := proc[k]
		slices.SortFunc(ps, func(a, b point) int { return a.t.Compare(b.t) })
		res := verifyResult{Check: "conservation", Scope: "process vs cgroup " + k,
			Want: fmt.Sprintf("equal ±%.0f%%", 100*processTolerance)}
		deltas, span, ok := deltaOver(ps, cg[k])
		if !ok {
			res.Got, res.Info = "too few readings", true
			out = append(out, res)
			continue
		}
		res.Got = fmt.Sprintf("process %.2fs cgroup %.2fs over %v", deltas[0], deltas[1], span.Round(time.Second))
		res.Pass = within(deltas[0], deltas[1], processTolerance, cpuSlack)
		out = append(out, res)
	}
	return out
}

// checkCadvisorCoverage reports, per series, the readings, the longest gap
// and whether steady holds two readings. Fewer is not a failure: cAdvisor
// cannot resolve a short steady window, and the report marks it insufficient.
func checkCadvisorCoverage(samples []cadvisorSample, steady phaseMark) []verifyResult {
	byKey := map[string][]time.Time{}
	for _, s := range samples {
		byKey[s.key()] = append(byKey[s.key()], s.T)
	}
	var out []verifyResult
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		ts := byKey[k]
		slices.SortFunc(ts, func(a, b time.Time) int { return a.Compare(b) })
		var gap time.Duration
		inSteady := 0
		for i, t := range ts {
			if i > 0 {
				gap = max(gap, t.Sub(ts[i-1]))
			}
			if !t.Before(steady.Start) && !t.After(steady.End) {
				inSteady++
			}
		}
		res := verifyResult{Check: "coverage", Scope: "cadvisor " + k, Want: "≥ 2 readings in steady",
			Got:  fmt.Sprintf("n=%d max gap %v, %d in steady", len(ts), gap.Round(time.Millisecond), inSteady),
			Pass: inSteady >= 2}
		if !res.Pass {
			res.Info, res.Got = true, res.Got+": insufficient"
		}
		out = append(out, res)
	}
	return out
}

// checkComponentsFound fails for every component that matched no pod or
// produced no reading.
func checkComponentsFound(missing []string, samples []cadvisorSample) []verifyResult {
	seen := map[string]bool{}
	for _, s := range samples {
		seen[s.Component] = true
	}
	var out []verifyResult
	for _, t := range cadvisorTargets {
		res := verifyResult{Check: "coverage", Scope: "component " + t.component, Want: "readings from a Running pod"}
		switch {
		case slices.Contains(missing, t.component):
			res.Got = "no Running pod matches " + t.selector + " in " + t.namespace
		case !seen[t.component]:
			res.Got = "no cAdvisor reading"
		default:
			res.Got, res.Pass = "ok", true
		}
		out = append(out, res)
	}
	return out
}
