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
	// Info results report what the data cannot judge, for the reason in
	// Note, and never fail the run.
	Info bool   `json:"info,omitempty"`
	Note string `json:"note,omitempty"`
}

// info marks res informational for reason.
func (res verifyResult) info(reason string) verifyResult {
	res.Info, res.Note = true, reason
	if res.Got == "" {
		res.Got = reason
	}
	return res
}

// Thresholds of the self-checks.
const (
	// The gateway opens one mitm_internal connection per actor connection;
	// 2% allows for connections the run did not make.
	cxTolerance = 0.02
	cxSlack     = 2
	// A driver above half a core is doing more than control traffic.
	maxDriverCores = 0.5
	// A live source may miss one poll: a slow API server round trip costs
	// one point of a cumulative counter, resolution rather than CPU.
	maxLiveGapPolls = 2
	// Live gaps fail above this or above three intervals, whichever is
	// longer, or when too many intervals are slow.
	maxLiveGap = 5 * time.Second
	// maxSlowShare is the share of intervals that may exceed the soft gap.
	maxSlowShare = 0.05
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

// gapVerdict judges one series' read times. It fails when a gap exceeds
// hard or more than maxSlowShare of the intervals exceed soft. A few gaps
// over soft are reported as INFO; they cost resolution, not data.
func gapVerdict(res verifyResult, ts []time.Time, soft, hard time.Duration) verifyResult {
	var gap time.Duration
	slow := 0
	for i := 1; i < len(ts); i++ {
		d := ts[i].Sub(ts[i-1])
		gap = max(gap, d)
		if d > soft {
			slow++
		}
	}
	intervals := max(len(ts)-1, 1)
	res.Want = fmt.Sprintf("no gap over %v, at most %.0f%% of intervals over %v", hard, 100*maxSlowShare, soft)
	res.Got = fmt.Sprintf("n=%d max gap %v, %d over %v", len(ts), gap.Round(time.Millisecond), slow, soft)
	switch {
	case len(ts) < 2 || gap > hard || float64(slow) > maxSlowShare*float64(intervals):
	case slow > 0:
		res = res.info(fmt.Sprintf("%d gaps over %v, longest %v", slow, soft, gap.Round(time.Millisecond)))
	default:
		res.Pass = true
	}
	return res
}

// checkLiveGaps judges, per process, the gaps between reads within the run.
// Labeled reads count too: they are reads like any other.
func checkLiveGaps(samples []liveSample, interval time.Duration, run phaseMark) []verifyResult {
	byPod := map[string][]time.Time{}
	for _, s := range samples {
		if s.T.Before(run.Start) || s.T.After(run.End) {
			continue
		}
		key := s.Component + "/" + s.Container + "@" + s.Pod
		byPod[key] = append(byPod[key], s.T)
	}
	var out []verifyResult
	for _, key := range slices.Sorted(maps.Keys(byPod)) {
		ts := byPod[key]
		slices.SortFunc(ts, func(a, b time.Time) int { return a.Compare(b) })
		out = append(out, gapVerdict(verifyResult{Check: "coverage", Scope: "live " + key}, ts, maxLiveGapPolls*interval, max(maxLiveGap, 3*interval)))
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
	// pauseCores allows for the idle pause container in the pod cgroup.
	pauseCores = 0.005
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

// counterBounds is the range a counter can hold at t: exact at a reading,
// otherwise anywhere between the readings around t, since the rate between
// them is unknown.
func counterBounds(ps []point, t time.Time) (lo, hi float64, ok bool) {
	if len(ps) == 0 || t.Before(ps[0].t) || t.After(ps[len(ps)-1].t) {
		return 0, 0, false
	}
	i, _ := slices.BinarySearchFunc(ps, t, func(p point, t time.Time) int { return p.t.Compare(t) })
	if ps[i].t.Equal(t) {
		return ps[i].v, ps[i].v, true
	}
	return ps[i-1].v, ps[i].v, true
}

// bounds is the range a sum of counter increases can take.
type bounds struct{ lo, hi float64 }

func (b bounds) mid() float64   { return (b.lo + b.hi) / 2 }
func (b bounds) width() float64 { return b.hi - b.lo }

// deltaBounds is the range of the counter's increase from from to to.
func deltaBounds(ps []point, from, to time.Time) (bounds, bool) {
	flo, fhi, ok1 := counterBounds(ps, from)
	tlo, thi, ok2 := counterBounds(ps, to)
	return bounds{tlo - fhi, thi - flo}, ok1 && ok2
}

// maxEdgeShare is how much of a delta its unknown edges may make up before a
// conservation check cannot judge it; a longer run narrows the edges.
const maxEdgeShare = 0.25

// compareBounds passes when the two ranges overlap once widened by slack. It
// is informational when either range is too wide to judge.
func compareBounds(res verifyResult, a, b bounds, slack float64) verifyResult {
	res.Got = fmt.Sprintf("%.2f..%.2fs vs %.2f..%.2fs", a.lo, a.hi, b.lo, b.hi)
	if a.width() > maxEdgeShare*math.Abs(a.mid()) || b.width() > maxEdgeShare*math.Abs(b.mid()) {
		return res.info("edges dominate, run longer")
	}
	res.Pass = a.lo-slack <= b.hi && b.lo-slack <= a.hi
	return res
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
		from, to, ok := commonSpan(parts...)
		if !ok {
			out = append(out, res.info("too few readings"))
			continue
		}
		podDelta, _ := deltaBounds(parts[0], from, to)
		var sum bounds
		for _, ps := range parts[1:] {
			d, _ := deltaBounds(ps, from, to)
			sum.lo, sum.hi = sum.lo+d.lo, sum.hi+d.hi
		}
		slack := rollupTolerance*podDelta.mid() + cpuSlack + pauseCores*to.Sub(from).Seconds()
		out = append(out, compareBounds(res, podDelta, sum, slack))
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
		from, to, ok := commonSpan(ps, cg[k])
		if !ok {
			out = append(out, res.info("too few readings"))
			continue
		}
		proc, _ := deltaBounds(ps, from, to)
		cgroup, _ := deltaBounds(cg[k], from, to)
		out = append(out, compareBounds(res, proc, cgroup, processTolerance*cgroup.mid()+cpuSlack))
	}
	return out
}

// checkCadvisorCoverage reports, per series, the readings, the longest gap
// and whether steady holds two readings. Fewer is not a failure: cAdvisor
// cannot resolve a short steady window, and the report marks it insufficient.
func checkCadvisorCoverage(samples []cadvisorSample, steady phaseMark) []verifyResult {
	byKey := map[string][]time.Time{}
	for _, s := range samples {
		// Pod rows feed only the rollup check.
		if s.Container != podContainer {
			byKey[s.key()] = append(byKey[s.key()], s.T)
		}
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
			res = res.info("insufficient: fewer than 2 readings in steady")
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

// Thresholds of the cross-checks.
const (
	// metrics-server averages a 17 to 37 s window against cAdvisor's 12 to
	// 20 s readings, so only gross errors (wrong container, a doubled pod
	// row, unit mix-ups) should exceed 15%.
	metricsServerTolerance = 0.15
	metricsServerSlack     = 0.02
	// A phase with no egress traffic should stay within this of idle.
	idleSlack = 0.05
)

// checkMetricsServer compares each metrics-server reading with the cAdvisor
// counters over the same window.
func checkMetricsServer(ms []podMetricsSample, samples []cadvisorSample) []verifyResult {
	series := cpuSeries(samples)
	var out []verifyResult
	for _, m := range ms {
		key := m.Component + "/" + m.Container + "@" + m.Pod
		res := verifyResult{Check: "sanity", Scope: key, Want: fmt.Sprintf("metrics-server %.3f ±%.0f%%", m.CPUCores, 100*metricsServerTolerance)}
		// metrics-server's window runs between two kubelet readings, which
		// are cAdvisor readings of the same container; the samples must hold
		// both ends.
		d, ok := deltaBounds(series[key], m.T.Add(-m.Window), m.T)
		if !ok || m.Window <= 0 {
			out = append(out, res.info("window starts before the first cAdvisor reading"))
			continue
		}
		// The window's ends fall between cAdvisor readings, so the sampler
		// knows the rate only as a range. metrics-server must fall inside it,
		// widened by the tolerance. A range wide enough to hide a doubled
		// counter cannot judge anything.
		w := m.Window.Seconds()
		lo, hi := max(d.lo, 0)/w, d.hi/w
		res.Got = fmt.Sprintf("sampler %.3f..%.3f", lo, hi)
		if hi >= 2*max(lo, m.CPUCores) {
			out = append(out, res.info("cAdvisor readings too sparse around the window to catch a doubled counter"))
			continue
		}
		res.Pass = m.CPUCores >= lo*(1-metricsServerTolerance)-metricsServerSlack &&
			m.CPUCores <= hi*(1+metricsServerTolerance)+metricsServerSlack
		out = append(out, res)
	}
	return out
}

// liveRates is each Go process's CPU rate between consecutive reads.
func liveRates(live []liveSample) map[string][]segment {
	byKey := map[string][]liveSample{}
	for _, l := range live {
		k := l.Component + "/" + l.Container + "@" + l.Pod
		byKey[k] = append(byKey[k], l)
	}
	out := map[string][]segment{}
	for k, ls := range byKey {
		slices.SortFunc(ls, func(a, b liveSample) int { return a.T.Compare(b.T) })
		for i := 1; i < len(ls); i++ {
			if dt := ls[i].T.Sub(ls[i-1].T).Seconds(); dt > 0 {
				out[k] = append(out[k], segment{from: ls[i-1].T, to: ls[i].T, cores: (ls[i].ProcessCPUSeconds - ls[i-1].ProcessCPUSeconds) / dt})
			}
		}
	}
	return out
}

// checkSteadyAttribution checks that each process's steady mean, from the
// reads forced at the marks, lies within the rates of the periodic reads
// inside steady; a mean outside them is a window or units error.
func checkSteadyAttribution(live []liveSample, steady phaseMark) []verifyResult {
	means := map[string]float64{}
	byKey := map[string][]liveSample{}
	for _, l := range live {
		k := l.Component + "/" + l.Container + "@" + l.Pod
		byKey[k] = append(byKey[k], l)
	}
	for k, ls := range byKey {
		if m, ok := liveSteadyCores(ls); ok {
			means[k] = m
		}
	}
	var periodic []liveSample
	for _, l := range live {
		if l.Label == "" {
			periodic = append(periodic, l)
		}
	}
	rates := liveRates(periodic)
	var out []verifyResult
	for _, k := range slices.Sorted(maps.Keys(means)) {
		segs := inside(rates[k], steady)
		res := verifyResult{Check: "phases", Scope: "steady mean " + k, Want: "within the per-interval min and max"}
		if len(segs) == 0 {
			out = append(out, res.info("no interval inside steady"))
			continue
		}
		lo, hi := segs[0].cores, segs[0].cores
		for _, s := range segs {
			lo, hi = min(lo, s.cores), max(hi, s.cores)
		}
		m := means[k]
		res.Got = fmt.Sprintf("mean %.3f in [%.3f, %.3f]", m, lo, hi)
		res.Pass = m >= lo-1e-9 && m <= hi+1e-9
		out = append(out, res)
	}
	return out
}

// checkIdlePhases checks, on a run that created no actors, that ext-proc's
// CPU over the create and suspend phases stays near its idle rate, the
// lowest rate over any 5 s of the run: no egress traffic flows then.
func checkIdlePhases(live []liveSample, rep *report) []verifyResult {
	// Only a rerun that reused every actor and rewrote no policy has an idle
	// create phase to judge; a policy write is work for ext-proc.
	if rep.Create.ActorsReused != rep.Config.Actors || rep.Create.PoliciesUpdated > 0 {
		return nil
	}
	var out []verifyResult
	rates := liveRates(live)
	for _, k := range slices.Sorted(maps.Keys(rates)) {
		segs := rates[k]
		if !strings.HasPrefix(k, "gateway/ext-proc@") {
			continue
		}
		idle := -1.0
		for i := range segs {
			var cpu, dt float64
			for j := i; j < len(segs) && dt < 5; j++ {
				d := segs[j].to.Sub(segs[j].from).Seconds()
				cpu, dt = cpu+segs[j].cores*d, dt+d
			}
			if dt >= 5 && (idle < 0 || cpu/dt < idle) {
				idle = cpu / dt
			}
		}
		if idle < 0 {
			continue
		}
		for _, name := range []string{"create", "suspend"} {
			ph := rep.phase(name)
			var cpu, dt float64
			for _, s := range segs {
				if !s.from.Before(ph.Start) && !s.to.After(ph.End) {
					d := s.to.Sub(s.from).Seconds()
					cpu, dt = cpu+s.cores*d, dt+d
				}
			}
			res := verifyResult{Check: "phases", Scope: name + " " + k, Want: fmt.Sprintf("≤ idle %.3f + %.2f", idle, idleSlack)}
			if dt == 0 {
				res = res.info("phase shorter than one poll")
			} else {
				res.Got = fmt.Sprintf("%.3f", cpu/dt)
				res.Pass = cpu/dt <= idle+idleSlack
			}
			out = append(out, res)
		}
	}
	return out
}
