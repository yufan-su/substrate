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
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

// componentOrder is the order components are printed in.
var componentOrder = []string{"gateway", "workers", "ateapi", "targets", "dns", "router"}

// componentSummary is what one component used over the steady window.
type componentSummary struct {
	Pods   int           `json:"pods"`
	Steady steadySummary `json:"steady"`
	// Settled is the same summary over the settled window.
	Settled    *steadySummary            `json:"settled,omitempty"`
	Containers map[string]*steadySummary `json:"containers,omitempty"`
}

type steadySummary struct {
	CPUCores       meanMax `json:"cpuCores"`
	CoresPerKrps   float64 `json:"coresPerKrps,omitempty"`
	ThrottledRatio float64 `json:"throttledRatio"`
	// WorkingSetBytes is the sum over pods of each one's steady maximum.
	WorkingSetBytes float64 `json:"workingSetBytes"`
	// Coverage is the shortest fraction of steady any series' readings span.
	Coverage     float64  `json:"coverage"`
	Insufficient bool     `json:"insufficient,omitempty"`
	LiveCPUCores *float64 `json:"liveCpuCores,omitempty"`
}

type meanMax struct {
	Mean float64 `json:"mean"`
	Max  float64 `json:"max"`
}

// seriesPoint is one row of the long-format series the plots read.
type seriesPoint struct {
	T         time.Time `json:"t"`
	Source    string    `json:"source"`
	Component string    `json:"component"`
	Pod       string    `json:"pod,omitempty"`
	Container string    `json:"container,omitempty"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
}

// segment is the usage between two consecutive readings of one series.
type segment struct {
	from, to          time.Time
	cores             float64
	periods, throttle float64
}

// Rules that can start the settled window.
const (
	settledByPlateau = "new-connection plateau"
	settledByRounds  = "1.5 rounds"
)

// settledWindow is steady minus the first full round over the endpoints,
// whose connection setup and DNS lookups inflate per-request cost.
type settledWindow struct {
	Start time.Time `json:"start"`
	// Rule says which rule started the window.
	Rule    string  `json:"rule"`
	ReqPerS float64 `json:"reqPerS"`
}

// settledPlateauTolerance is the growth in new connections, as a fraction
// of those already open, that still counts as a plateau; reconnects after
// a timeout add a few.
const settledPlateauTolerance = 0.01

// settledOf finds the settled window. It starts one progress poll after
// the first poll after which new connections stop growing. When polls come
// further apart than one round over the endpoints, or no plateau shows,
// it starts 1.5 rounds into steady instead.
func settledOf(cfg runConfig, steady phaseMark, timeline []loopPoint) (settledWindow, bool) {
	var polls []loopPoint
	for _, p := range timeline {
		if !p.T.Before(steady.Start) && !p.T.After(steady.End) {
			polls = append(polls, p)
		}
	}
	round := time.Duration(cfg.Endpoints) * cfg.RequestInterval
	var w settledWindow
	if cfg.ProgressInterval <= round {
		for i := 0; i+1 < len(polls); i++ {
			grew := polls[i+1].NewConns - polls[i].NewConns
			if polls[i].NewConns > 0 && float64(grew) <= settledPlateauTolerance*float64(polls[i].NewConns) {
				w = settledWindow{Start: polls[i+1].T, Rule: settledByPlateau}
				break
			}
		}
	}
	if w.Rule == "" {
		if round <= 0 {
			return w, false
		}
		w = settledWindow{Start: steady.Start.Add(round * 3 / 2), Rule: settledByRounds}
	}
	if len(polls) == 0 {
		return w, false
	}
	last := polls[len(polls)-1]
	if !last.T.After(w.Start) {
		return w, false
	}
	w.ReqPerS = (float64(last.Requests) - requestsAt(polls, steady.Start, w.Start)) / last.T.Sub(w.Start).Seconds()
	return w, true
}

// requestsAt interpolates the cumulative request count at t, counting from
// zero at the steady start.
func requestsAt(polls []loopPoint, start, t time.Time) float64 {
	prev := loopPoint{T: start}
	for _, p := range polls {
		if !p.T.Before(t) {
			span := p.T.Sub(prev.T).Seconds()
			if span <= 0 {
				return float64(p.Requests)
			}
			f := t.Sub(prev.T).Seconds() / span
			return float64(prev.Requests) + f*float64(p.Requests-prev.Requests)
		}
		prev = p
	}
	return float64(prev.Requests)
}

// steadyRequestRate is the loops' merged request rate.
func steadyRequestRate(rep *report) float64 {
	if rep.Loop == nil || rep.Loop.Elapsed <= 0 {
		return 0
	}
	return float64(rep.Loop.Requests) / rep.Loop.Elapsed.Seconds()
}

// summarizeResources fills the components and series of rep.Resources.
func summarizeResources(rep *report) {
	res := rep.Resources
	steady := rep.phase("steady")
	res.Components = map[string]*componentSummary{}
	preIdle := rep.phase("pre-idle")
	res.Baseline = nil
	if !preIdle.End.IsZero() {
		res.Baseline = map[string]*steadySummary{}
	}
	var settled phaseMark
	if !steady.End.IsZero() {
		if w, ok := settledOf(rep.Config, steady, rep.LoopTimeline); ok {
			res.Settled = &w
			settled = phaseMark{Name: "settled", Start: w.Start, End: steady.End}
		}
	}
	// The cgroup readers, when present, replace cAdvisor for the containers
	// they read: same counters, one-second resolution.
	samples, split := res.Samples, []cadvisorSample(nil)
	fromReader := map[string]bool{}
	if res.Cgreader != nil {
		var containers []cadvisorSample
		containers, split = cgreaderAsCadvisor(res.Cgreader)
		for _, c := range containers {
			fromReader[c.key()] = true
		}
		samples = containers
		for _, c := range res.Samples {
			if !fromReader[c.key()] {
				samples = append(samples, c)
			}
		}
	}
	bySeries := map[string][]cadvisorSample{}
	for _, s := range samples {
		bySeries[s.key()] = append(bySeries[s.key()], s)
	}
	type compAcc struct {
		segs, settledSegs [][]segment
		pods              map[string]bool
		periods, throttle float64
	}
	acc := map[string]*compAcc{}
	for _, key := range slices.Sorted(maps.Keys(bySeries)) {
		ss := bySeries[key]
		slices.SortFunc(ss, func(a, b cadvisorSample) int { return a.T.Compare(b.T) })
		comp, ctr := ss[0].Component, ss[0].Container
		source := "cadvisor"
		if fromReader[key] {
			source = "cgreader"
		}
		var segs []segment
		for i := 1; i < len(ss); i++ {
			a, b := ss[i-1], ss[i]
			dt := b.T.Sub(a.T).Seconds()
			if dt <= 0 {
				continue
			}
			seg := segment{from: a.T, to: b.T, cores: (b.CPUSeconds - a.CPUSeconds) / dt,
				periods: b.CFSPeriods - a.CFSPeriods, throttle: b.CFSThrottled - a.CFSThrottled}
			segs = append(segs, seg)
			res.Series = append(res.Series, seriesPoint{T: b.T, Source: source, Component: comp, Pod: b.Pod, Container: ctr, Metric: "cpu_cores", Value: seg.cores})
			if seg.periods > 0 {
				res.Series = append(res.Series, seriesPoint{T: b.T, Source: source, Component: comp, Pod: b.Pod, Container: ctr, Metric: "throttled_ratio", Value: seg.throttle / seg.periods})
			}
		}
		for _, s := range ss {
			res.Series = append(res.Series, seriesPoint{T: s.T, Source: source, Component: comp, Pod: s.Pod, Container: ctr, Metric: "working_set_bytes", Value: s.WorkingSetBytes})
		}
		if ctr == podContainer || steady.End.IsZero() {
			continue
		}
		if !preIdle.End.IsZero() {
			b := steadyOf(ss, segs, preIdle).steadySummary
			for _, k := range []string{comp, comp + "/" + ctr} {
				if res.Baseline[k] == nil {
					res.Baseline[k] = &steadySummary{Coverage: 1}
				}
				addTo(res.Baseline[k], b)
			}
		}
		sum := steadyOf(ss, segs, steady)
		cs := res.Components[comp]
		if cs == nil {
			cs = &componentSummary{Containers: map[string]*steadySummary{}, Steady: steadySummary{Coverage: 1}}
			res.Components[comp] = cs
			acc[comp] = &compAcc{pods: map[string]bool{}}
		}
		a := acc[comp]
		a.pods[ss[0].Pod] = true
		a.segs = append(a.segs, inside(segs, steady))
		a.periods += sum.periods
		a.throttle += sum.throttle
		addTo(&cs.Steady, sum.steadySummary)
		if res.Settled != nil {
			if cs.Settled == nil {
				cs.Settled = &steadySummary{Coverage: 1}
			}
			a.settledSegs = append(a.settledSegs, inside(segs, settled))
			addTo(cs.Settled, steadyOf(ss, segs, settled).steadySummary)
		}
		if cs.Containers[ctr] == nil {
			cs.Containers[ctr] = &steadySummary{Coverage: 1}
		}
		addTo(cs.Containers[ctr], sum.steadySummary)
	}
	addSplit(res, split, steady)
	for comp, cs := range res.Components {
		a := acc[comp]
		if a == nil {
			continue
		}
		cs.Pods = len(a.pods)
		cs.Steady.CPUCores.Max = maxSum(a.segs, steady)
		if cs.Settled != nil {
			cs.Settled.CPUCores.Max = maxSum(a.settledSegs, settled)
		}
		if a.periods > 0 {
			cs.Steady.ThrottledRatio = a.throttle / a.periods
		}
	}
	addLive(rep)
	if rps := steadyRequestRate(rep); rps > 0 {
		for _, cs := range res.Components {
			if !cs.Steady.Insufficient {
				cs.Steady.CoresPerKrps = cs.Steady.CPUCores.Mean / (rps / 1000)
			}
		}
	}
	if res.Settled != nil && res.Settled.ReqPerS > 0 {
		for _, cs := range res.Components {
			if cs.Settled != nil && !cs.Settled.Insufficient {
				cs.Settled.CoresPerKrps = cs.Settled.CPUCores.Mean / (res.Settled.ReqPerS / 1000)
			}
		}
	}
	res.Series = append(res.Series, envoySeries(res.Envoy)...)
	res.Series = append(res.Series, loopSeries(rep.LoopTimeline)...)
}

// addSplit adds the workers' actors and atunnel parts, from the cgroup
// readers' leaves, as containers of the workers component. They are parts
// of ateom, so they do not add to the component's total.
func addSplit(res *resourceReport, split []cadvisorSample, steady phaseMark) {
	bySeries := map[string][]cadvisorSample{}
	for _, s := range split {
		bySeries[s.key()] = append(bySeries[s.key()], s)
	}
	for _, key := range slices.Sorted(maps.Keys(bySeries)) {
		ss := bySeries[key]
		slices.SortFunc(ss, func(a, b cadvisorSample) int { return a.T.Compare(b.T) })
		var segs []segment
		for i := 1; i < len(ss); i++ {
			a, b := ss[i-1], ss[i]
			if !b.T.After(a.T) {
				continue
			}
			seg := segment{from: a.T, to: b.T, cores: (b.CPUSeconds - a.CPUSeconds) / b.T.Sub(a.T).Seconds()}
			segs = append(segs, seg)
			res.Series = append(res.Series, seriesPoint{T: b.T, Source: "cgreader", Component: b.Component, Pod: b.Pod, Container: b.Container, Metric: "cpu_cores", Value: seg.cores})
		}
		cs := res.Components[ss[0].Component]
		if cs == nil || steady.End.IsZero() {
			continue
		}
		if cs.Containers[ss[0].Container] == nil {
			cs.Containers[ss[0].Container] = &steadySummary{Coverage: 1}
		}
		addTo(cs.Containers[ss[0].Container], steadyOf(ss, segs, steady).steadySummary)
	}
}

type seriesSteady struct {
	steadySummary
	periods, throttle float64
}

// steadyOf summarizes one series over the readings inside steady.
func steadyOf(ss []cadvisorSample, segs []segment, steady phaseMark) seriesSteady {
	var in []cadvisorSample
	for _, s := range ss {
		if !s.T.Before(steady.Start) && !s.T.After(steady.End) {
			in = append(in, s)
		}
	}
	var out seriesSteady
	for _, s := range in {
		out.WorkingSetBytes = max(out.WorkingSetBytes, s.WorkingSetBytes)
	}
	if len(in) < 2 {
		out.Insufficient = true
		return out
	}
	first, last := in[0], in[len(in)-1]
	span := last.T.Sub(first.T)
	if span <= 0 {
		out.Insufficient = true
		return out
	}
	out.CPUCores.Mean = (last.CPUSeconds - first.CPUSeconds) / span.Seconds()
	out.Coverage = span.Seconds() / steady.End.Sub(steady.Start).Seconds()
	out.periods, out.throttle = last.CFSPeriods-first.CFSPeriods, last.CFSThrottled-first.CFSThrottled
	if out.periods > 0 {
		out.ThrottledRatio = out.throttle / out.periods
	}
	for _, sg := range inside(segs, steady) {
		out.CPUCores.Max = max(out.CPUCores.Max, sg.cores)
	}
	return out
}

// inside keeps the segments that lie wholly within steady.
func inside(segs []segment, steady phaseMark) []segment {
	var out []segment
	for _, s := range segs {
		if !s.from.Before(steady.Start) && !s.to.After(steady.End) {
			out = append(out, s)
		}
	}
	return out
}

// addTo adds one series' summary into a sum over series.
func addTo(dst *steadySummary, s steadySummary) {
	dst.CPUCores.Mean += s.CPUCores.Mean
	dst.CPUCores.Max += s.CPUCores.Max
	dst.WorkingSetBytes += s.WorkingSetBytes
	dst.Insufficient = dst.Insufficient || s.Insufficient
	if !s.Insufficient {
		dst.Coverage = min(dst.Coverage, s.Coverage)
	} else {
		dst.Coverage = 0
	}
	if s.ThrottledRatio > dst.ThrottledRatio {
		dst.ThrottledRatio = s.ThrottledRatio
	}
}

// maxSum is the highest summed rate over every second of steady, where
// each series contributes the rate of the segment covering that second.
func maxSum(series [][]segment, steady phaseMark) float64 {
	var best float64
	for t := steady.Start; !t.After(steady.End); t = t.Add(time.Second) {
		var sum float64
		for _, segs := range series {
			for _, s := range segs {
				if !t.Before(s.from) && t.Before(s.to) {
					sum += s.cores
					break
				}
			}
		}
		best = max(best, sum)
	}
	return best
}

// addLive adds the Go processes' exact steady CPU, from the reads forced at
// the steady marks, and their per-interval rates.
func addLive(rep *report) {
	res := rep.Resources
	byKey := map[string][]liveSample{}
	for _, l := range res.Live {
		k := l.Component + "/" + l.Container + "@" + l.Pod
		byKey[k] = append(byKey[k], l)
	}
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		ls := byKey[k]
		slices.SortFunc(ls, func(a, b liveSample) int { return a.T.Compare(b.T) })
		for i := 1; i < len(ls); i++ {
			a, b := ls[i-1], ls[i]
			if dt := b.T.Sub(a.T).Seconds(); dt > 0 {
				res.Series = append(res.Series, seriesPoint{T: b.T, Source: "live", Component: b.Component, Pod: b.Pod, Container: b.Container,
					Metric: "cpu_cores", Value: (b.ProcessCPUSeconds - a.ProcessCPUSeconds) / dt})
			}
			res.Series = append(res.Series, seriesPoint{T: b.T, Source: "live", Component: b.Component, Pod: b.Pod, Container: b.Container, Metric: "rss_bytes", Value: b.RSSBytes})
		}
		cores, ok := liveSteadyCores(ls)
		if !ok {
			continue
		}
		comp, ctr := ls[0].Component, ls[0].Container
		cs := res.Components[comp]
		if cs == nil {
			cs = &componentSummary{Containers: map[string]*steadySummary{}, Steady: steadySummary{Insufficient: true}}
			res.Components[comp] = cs
		}
		c := cs.Containers[ctr]
		if c == nil {
			c = &steadySummary{Insufficient: true}
			cs.Containers[ctr] = c
		}
		c.LiveCPUCores = addPtr(c.LiveCPUCores, cores)
	}
	if d := res.Driver.Steady; d.WallSeconds > 0 {
		cores := d.Cores
		res.Components["driver"] = &componentSummary{Steady: steadySummary{CPUCores: meanMax{Mean: cores, Max: cores}, Coverage: 1, LiveCPUCores: &cores}}
	}
}

func addPtr(p *float64, v float64) *float64 {
	if p != nil {
		v += *p
	}
	return &v
}

// liveSteadyCores is a process's mean CPU between the steady marks.
func liveSteadyCores(ls []liveSample) (float64, bool) {
	var a, b *liveSample
	for i := range ls {
		switch ls[i].Label {
		case markSteadyBegin:
			a = &ls[i]
		case markSteadyEnd:
			b = &ls[i]
		}
	}
	if a == nil || b == nil || !b.T.After(a.T) {
		return 0, false
	}
	return (b.ProcessCPUSeconds - a.ProcessCPUSeconds) / b.T.Sub(a.T).Seconds(), true
}

// envoySeries emits the gateway's connection counters as they were read.
func envoySeries(samples []envoySample) []seriesPoint {
	var out []seriesPoint
	for _, s := range samples {
		for _, name := range slices.Sorted(maps.Keys(s.Counters)) {
			out = append(out, seriesPoint{T: s.T, Source: "envoy", Component: "gateway", Pod: s.Pod, Container: "envoy", Metric: name, Value: s.Counters[name]})
		}
	}
	return out
}

// loopSeries turns the cumulative progress polls into per-interval request
// rates and p99 latencies.
func loopSeries(timeline []loopPoint) []seriesPoint {
	var out []seriesPoint
	for i := 1; i < len(timeline); i++ {
		a, b := timeline[i-1], timeline[i]
		dt := b.T.Sub(a.T).Seconds()
		if dt <= 0 {
			continue
		}
		out = append(out, seriesPoint{T: b.T, Source: "loop", Component: "actors", Metric: "req_per_s", Value: float64(b.Requests-a.Requests) / dt})
		d := histogramDelta(b.Latency, a.Latency)
		if d.Count > 0 {
			out = append(out, seriesPoint{T: b.T, Source: "loop", Component: "actors", Metric: "p99_ms",
				Value: float64(d.Quantile(0.99).Microseconds()) / 1000})
		}
	}
	return out
}

// histogramDelta is the samples b holds beyond a, for cumulative snapshots.
func histogramDelta(b, a egressapi.Histogram) egressapi.Histogram {
	d := egressapi.Histogram{Count: b.Count - a.Count, SumMicros: b.SumMicros - a.SumMicros, MaxMicros: b.MaxMicros}
	if d.Count <= 0 {
		return egressapi.Histogram{}
	}
	d.Counts = slices.Clone(b.Counts)
	for i := range min(len(a.Counts), len(d.Counts)) {
		d.Counts[i] -= a.Counts[i]
	}
	return d
}

// printResources writes the cpu and memory summary lines.
func (rep *report) printResources(w io.Writer) {
	res := rep.Resources
	if res == nil || len(res.Components) == 0 {
		return
	}
	var cpu, perK, settledK, mem, insufficient []string
	minCov := 1.0
	for _, name := range componentOrder {
		cs, ok := res.Components[name]
		if !ok {
			continue
		}
		if cs.Steady.Insufficient {
			insufficient = append(insufficient, name)
		} else {
			cpu = append(cpu, fmt.Sprintf("%s %.2f/%.2f", name, cs.Steady.CPUCores.Mean, cs.Steady.CPUCores.Max))
			minCov = min(minCov, cs.Steady.Coverage)
		}
		if cs.Steady.CoresPerKrps > 0 {
			perK = append(perK, fmt.Sprintf("%s %.2f", name, cs.Steady.CoresPerKrps))
		}
		if cs.Settled != nil && cs.Settled.CoresPerKrps > 0 {
			settledK = append(settledK, fmt.Sprintf("%s %.2f", name, cs.Settled.CoresPerKrps))
		}
		if cs.Steady.WorkingSetBytes > 0 {
			mem = append(mem, fmt.Sprintf("%s %s", name, bytesIEC(cs.Steady.WorkingSetBytes)))
		}
	}
	line := strings.Join(cpu, " ")
	if len(insufficient) > 0 {
		line += " (cAdvisor: insufficient for " + strings.Join(insufficient, ",") + ")"
	}
	fmt.Fprintf(w, "%-10s steady cores mean/max: %s\n", "cpu", line)
	if len(perK) > 0 {
		fmt.Fprintf(w, "%-10s cores per 1000 req/s: %s (cAdvisor coverage %.0f%%)\n", "", strings.Join(perK, " "), 100*minCov)
	}
	if st := res.Settled; st != nil && len(settledK) > 0 {
		fmt.Fprintf(w, "%-10s cores per 1000 req/s, settled: %s (from steady +%.1fs by %s, %.1f req/s)\n", "",
			strings.Join(settledK, " "), st.Start.Sub(rep.phase("steady").Start).Seconds(), st.Rule, st.ReqPerS)
	}
	if len(res.Baseline) > 0 {
		var base []string
		for _, name := range componentOrder {
			if b := res.Baseline[name]; b != nil && !b.Insufficient {
				base = append(base, fmt.Sprintf("%s %.3f", name, b.CPUCores.Mean))
			}
		}
		fmt.Fprintf(w, "%-10s pre-idle baseline cores: %s\n", "", strings.Join(base, " "))
	}
	var live []string
	for _, name := range append(slices.Clone(componentOrder), "driver") {
		cs, ok := res.Components[name]
		if !ok {
			continue
		}
		for _, ctr := range slices.Sorted(maps.Keys(cs.Containers)) {
			if c := cs.Containers[ctr]; c.LiveCPUCores != nil {
				live = append(live, fmt.Sprintf("%s/%s %.3f", name, ctr, *c.LiveCPUCores))
			}
		}
		if name == "driver" && cs.Steady.LiveCPUCores != nil {
			live = append(live, fmt.Sprintf("driver %.3f", *cs.Steady.LiveCPUCores))
		}
	}
	if len(live) > 0 {
		fmt.Fprintf(w, "%-10s steady cores, exact: %s\n", "", strings.Join(live, " "))
	}
	cx := ""
	if over, ok := envoyDelta(res.Envoy, "cluster.mitm_internal.upstream_cx_overflow"); ok {
		cx = fmt.Sprintf("; mitm_internal cx overflow +%.0f, active max %.0f", over, envoyMax(res.Envoy, "cluster.mitm_internal.upstream_cx_active"))
		cx += fmt.Sprintf("; cleartext cx active max %.0f", envoyMax(res.Envoy, "cluster.egress_forward_proxy_cleartext.upstream_cx_active"))
		for _, c := range envoyClusters {
			if envoyMax(res.Envoy, "cluster."+c+".circuit_breakers.default.cx_open") > 0 {
				cx += "; " + c + " connection breaker OPENED"
			}
			if envoyMax(res.Envoy, "cluster."+c+".circuit_breakers.default.rq_open") > 0 {
				cx += "; " + c + " request breaker OPENED"
			}
		}
		active, _ := envoyDelta(res.Envoy, "cluster.mitm_internal.upstream_rq_active_overflow")
		if over > 0 || active > 0 {
			queued, _ := envoyDelta(res.Envoy, "cluster.mitm_internal.upstream_rq_pending_total")
			cancelled, _ := envoyDelta(res.Envoy, "cluster.mitm_internal.upstream_rq_cancelled")
			pending, _ := envoyDelta(res.Envoy, "cluster.mitm_internal.upstream_rq_pending_overflow")
			cx += fmt.Sprintf("; mitm_internal CONNECTs queued +%.0f, cancelled +%.0f, refused +%.0f pending +%.0f active",
				queued, cancelled, pending, active)
		}
	}
	if len(mem) > 0 || cx != "" {
		fmt.Fprintf(w, "%-10s steady max working set: %s%s\n", "memory", strings.Join(mem, " "), cx)
	}
}

// envoyDelta is the counter's increase over steady, summed over pods.
func envoyDelta(samples []envoySample, counter string) (float64, bool) {
	a, okA := sumLabeled(samples, markSteadyBegin, counter)
	b, okB := sumLabeled(samples, markSteadyEnd, counter)
	return b - a, okA && okB
}

func envoyMax(samples []envoySample, counter string) float64 {
	var m float64
	for _, s := range samples {
		m = max(m, s.Counters[counter])
	}
	return m
}

func bytesIEC(b float64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGi", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.0fMi", b/(1<<20))
	default:
		return fmt.Sprintf("%.0fKi", b/(1<<10))
	}
}

// printVerify writes the self-check block and reports whether all passed.
func (rep *report) printVerify(w io.Writer) bool {
	res := rep.Resources
	if res == nil {
		return true
	}
	type tally struct{ ok, info, n int }
	counts := map[string]*tally{}
	var order []string
	failed := 0
	// INFO results are grouped by reason; listing every scope buries the
	// failures.
	infos := map[string][]string{}
	var infoOrder []string
	for _, v := range res.Verify {
		t := counts[v.Check]
		if t == nil {
			t = &tally{}
			counts[v.Check] = t
			order = append(order, v.Check)
		}
		t.n++
		switch {
		case v.Pass:
			t.ok++
		case v.Info:
			t.info++
			k := v.Check + ": " + v.Note
			if infos[k] == nil {
				infoOrder = append(infoOrder, k)
			}
			infos[k] = append(infos[k], v.Scope)
		default:
			failed++
			fmt.Fprintf(w, "%-10s FAIL %s %s: got %s, want %s\n", "verify", v.Check, v.Scope, v.Got, v.Want)
		}
	}
	for _, k := range infoOrder {
		scopes := infos[k]
		if len(scopes) <= 2 {
			fmt.Fprintf(w, "%-10s INFO %s (%s)\n", "verify", k, strings.Join(scopes, ", "))
		} else {
			fmt.Fprintf(w, "%-10s INFO %s (%d series)\n", "verify", k, len(scopes))
		}
	}
	var parts []string
	for _, c := range order {
		part := fmt.Sprintf("%s %d/%d ok", c, counts[c].ok, counts[c].n)
		if counts[c].info > 0 {
			part += fmt.Sprintf(", %d info", counts[c].info)
		}
		parts = append(parts, part)
	}
	fmt.Fprintf(w, "%-10s %s\n", "verify", strings.Join(parts, "; "))
	if failed > 0 {
		fmt.Fprintf(w, "%-10s FAIL (%d)\n", "verify", failed)
		return false
	}
	fmt.Fprintf(w, "%-10s PASS\n", "verify")
	return true
}
