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
	"slices"
	"strings"
	"time"
)

// Thresholds of the cgroup reader checks.
const (
	// Half a one-second sample moves a reading across a phase line.
	maxClockOffset = 500 * time.Millisecond
	// A wider spread means the node clock stepped during the run.
	maxClockSpread = time.Second
	// Pulls slower than this say little about the offset; the median uses
	// the faster ones.
	maxOffsetRTT = 500 * time.Millisecond
	// Two missed ticks are tolerable; a third points at a stalled reader.
	maxCgreaderGap = 3 * time.Second
	// Reader gaps fail above this, or when too many intervals are slow.
	maxCgreaderHardGap = 10 * time.Second
	// cgroup v2 keeps processes in leaves only, so in a round with no
	// removed leaf the leaves sum exactly; the slack covers rounding.
	leafTolerance = 0.005
	leafSlack     = 0.02
	// The reader and cAdvisor read the same cpu.stat.
	cgreaderTolerance = 0.02
	// The reader reads a few dozen small files a second.
	maxReaderCores = 0.02
)

// medianOffset is a node's typical clock offset from the driver, over the
// pulls answered within maxOffsetRTT, or over all of them if none was.
func medianOffset(n *cgNode) time.Duration {
	if n == nil || len(n.Offsets) == 0 {
		return 0
	}
	var ds, all []time.Duration
	for _, o := range n.Offsets {
		all = append(all, o.Offset)
		if o.RTT <= maxOffsetRTT {
			ds = append(ds, o.Offset)
		}
	}
	if len(ds) == 0 {
		ds = all
	}
	slices.Sort(ds)
	return ds[len(ds)/2]
}

// offsetDisagreement is how far apart a node's offset estimates are once
// each is widened by its uncertainty: a pull's answer was stamped somewhere
// within its round trip, so its offset lies within ±RTT/2 of the estimate.
// It is zero when every interval shares a point.
func offsetDisagreement(offsets []clockOffset) time.Duration {
	var maxLo, minHi time.Duration
	for i, o := range offsets {
		lo, hi := o.Offset-o.RTT/2, o.Offset+o.RTT/2
		if i == 0 || lo > maxLo {
			maxLo = lo
		}
		if i == 0 || hi < minHi {
			minHi = hi
		}
	}
	return max(0, maxLo-minHi)
}

// checkCgreaderNodes checks each reader lost no rows and kept a steady clock.
func checkCgreaderNodes(cg *cgreaderReport) []verifyResult {
	var out []verifyResult
	for _, node := range slices.Sorted(maps.Keys(cg.Nodes)) {
		n := cg.Nodes[node]
		out = append(out, verifyResult{Check: "coverage", Scope: "cgreader ring " + node, Want: "no rows lost, no restarts",
			Got: fmt.Sprintf("%d lost, %d restarts", n.Lost, n.Restarts), Pass: n.Lost == 0 && n.Restarts == 0})
		res := verifyResult{Check: "clocks", Scope: node, Want: fmt.Sprintf("spread ≤ %v", maxClockSpread)}
		if len(n.Offsets) == 0 {
			out = append(out, res.info("no pull answered"))
			continue
		}
		med := medianOffset(n)
		gap := offsetDisagreement(n.Offsets)
		res.Want = fmt.Sprintf("offset intervals (±RTT/2) within %v", maxClockSpread)
		res.Got = fmt.Sprintf("median %v, intervals %v apart", med.Round(time.Millisecond), gap.Round(time.Millisecond))
		res.Pass = gap <= maxClockSpread
		if res.Pass && (med > maxClockOffset || med < -maxClockOffset) {
			res = res.info(fmt.Sprintf("node clock %v off the driver's; plots shift it", med.Round(time.Millisecond)))
		}
		out = append(out, res)
	}
	return out
}

// cgSeries groups reader rows by series, in time order.
func cgSeries(samples []cgSample) map[string][]cgSample {
	out := map[string][]cgSample{}
	for _, s := range samples {
		out[s.key()] = append(out[s.key()], s)
	}
	for _, ss := range out {
		slices.SortFunc(ss, func(a, b cgSample) int { return a.T.Compare(b.T) })
	}
	return out
}

// checkCgreaderTargets fails a requested container cgroup that produced
// fewer than two rows. The driver derives each path from the pod, for the
// systemd driver with containerd scopes under the QoS slices; a path that
// does not exist on the node yields no rows, and the summary would fall
// back to cAdvisor for that container without a word. Pod cgroups are left
// to the leaf-sum check and the readers to the overhead check.
func checkCgreaderTargets(cg *cgreaderReport) []verifyResult {
	rows := map[string]int{}
	for _, s := range cg.Samples {
		if s.Leaf == "" && !s.Gone {
			rows[s.Node+"|"+s.Pod+"|"+s.Container]++
		}
	}
	var out []verifyResult
	for _, r := range cg.Requested {
		if r.Component == "cgreader" || r.Container == podContainer {
			continue
		}
		n := rows[r.Node+"|"+r.Pod+"|"+r.Container]
		res := verifyResult{Check: "coverage", Scope: "cgreader " + r.Component + "/" + r.Container + "@" + r.Pod,
			Want: "at least 2 rows", Got: fmt.Sprintf("%d rows for %s", n, r.Path), Pass: n >= 2}
		if !res.Pass {
			res.Got += "; check the cgroup path layout on " + r.Node
		}
		out = append(out, res)
	}
	return out
}

// checkCgreaderGaps reports each container series' longest gap.
func checkCgreaderGaps(samples []cgSample) []verifyResult {
	series := cgSeries(samples)
	var out []verifyResult
	for _, k := range slices.Sorted(maps.Keys(series)) {
		ss := series[k]
		if ss[0].Leaf != "" {
			continue
		}
		ts := make([]time.Time, len(ss))
		for i, s := range ss {
			ts[i] = s.T
		}
		out = append(out, gapVerdict(verifyResult{Check: "coverage", Scope: "cgreader " + k}, ts, maxCgreaderGap, maxCgreaderHardGap))
	}
	return out
}

// leafRound is one reader round of a container with child cgroups: leaf
// name ("" for the container) to the row's index in the samples.
type leafRound map[string]int

// leafRounds groups the reader rows of each container that has child
// cgroups by round, in time order. Every row of a round carries the same
// timestamp.
func leafRounds(samples []cgSample) map[string][]leafRound {
	byParent := map[string]map[time.Time]leafRound{}
	withLeaves := map[string]bool{}
	for i, s := range samples {
		parent := s.Component + "/" + s.Container + "@" + s.Pod
		if byParent[parent] == nil {
			byParent[parent] = map[time.Time]leafRound{}
		}
		if byParent[parent][s.T] == nil {
			byParent[parent][s.T] = leafRound{}
		}
		byParent[parent][s.T][s.Leaf] = i
		withLeaves[parent] = withLeaves[parent] || s.Leaf != ""
	}
	out := map[string][]leafRound{}
	for parent, rounds := range byParent {
		if !withLeaves[parent] {
			continue
		}
		for _, t := range slices.SortedFunc(maps.Keys(rounds), func(a, b time.Time) int { return a.Compare(b) }) {
			if _, ok := rounds[t][""]; ok {
				out[parent] = append(out[parent], rounds[t])
			}
		}
	}
	return out
}

// actorOf is the actor a leaf belongs to: a sandbox has a <uid>-_pause and
// a <uid>-actor cgroup. Other leaves are their own owner.
func actorOf(leaf string) string {
	for _, suffix := range []string{"-_pause", "-actor"} {
		if uid, ok := strings.CutSuffix(leaf, suffix); ok {
			return uid
		}
	}
	return leaf
}

// removalShare splits a round in which leaves disappeared. It returns the
// container's increase, the surviving leaves' increase, and the leaves that
// disappeared, grouped by actor.
func removalShare(samples []cgSample, prev, cur leafRound) (container, survivors float64, gone map[string][]string) {
	container = samples[cur[""]].CPUSeconds - samples[prev[""]].CPUSeconds
	gone = map[string][]string{}
	for leaf, i := range cur {
		switch {
		case leaf == "":
		case samples[i].Gone:
			gone[actorOf(leaf)] = append(gone[actorOf(leaf)], leaf)
		default:
			var before float64 // a new leaf counts from zero
			if j, ok := prev[leaf]; ok {
				before = samples[j].CPUSeconds
			}
			survivors += samples[i].CPUSeconds - before
		}
	}
	return container, survivors, gone
}

// inferRemovedLeaves credits, in each round where exactly one actor's
// cgroups disappeared, what the container used beyond its surviving leaves
// to that actor's _pause row: the kernel keeps a removed cgroup's usage in
// its parent, but its use after the last read cannot be read. With two or
// more actors gone in one round the remainder cannot be split.
func inferRemovedLeaves(samples []cgSample) {
	for _, rounds := range leafRounds(samples) {
		for r := 1; r < len(rounds); r++ {
			container, survivors, gone := removalShare(samples, rounds[r-1], rounds[r])
			if len(gone) != 1 {
				continue
			}
			for _, leaves := range gone {
				target := leaves[0]
				for _, l := range leaves {
					if strings.HasSuffix(l, "-_pause") {
						target = l
					}
				}
				// A gone row repeats the leaf's last read, so the actor's other
				// gone leaves add nothing to the round.
				g := &samples[rounds[r][target]]
				final := g.CPUSeconds + max(0, container-survivors)
				g.InferredCPUSeconds = &final
			}
		}
	}
}

// checkLeafSums compares each container that has child cgroups, such as a
// worker's ateom with its actors' _pause leaves and the ateom leaf, with
// its leaves, one reader round at a time. A leaf new in a round counts from
// zero. A removed actor's inferred final counter closes its last round;
// a round where two or more actors disappeared cannot be split, so it is
// left out and reported.
func checkLeafSums(samples []cgSample) []verifyResult {
	var out []verifyResult
	rounds := leafRounds(samples)
	for _, parent := range slices.Sorted(maps.Keys(rounds)) {
		rs := rounds[parent]
		res := verifyResult{Check: "conservation", Scope: "leaf sum " + parent, Want: fmt.Sprintf("Σ leaves = container ±%.1f%%", 100*leafTolerance)}
		var container, leaves, credited, unsplit float64
		judged, unsplitRounds := 0, 0
		for r := 1; r < len(rs); r++ {
			prev, cur := rs[r-1], rs[r]
			pc, lc, gone := removalShare(samples, prev, cur)
			split := true
			for _, ls := range gone {
				for _, l := range ls {
					row := samples[cur[l]]
					switch {
					case row.InferredCPUSeconds != nil:
						before := row.CPUSeconds // the last read
						if j, ok := prev[l]; ok {
							before = samples[j].CPUSeconds
						}
						lc += *row.InferredCPUSeconds - before
						credited += *row.InferredCPUSeconds - row.CPUSeconds
					case len(gone) > 1:
						split = false
					}
				}
			}
			if !split {
				unsplitRounds++
				unsplit += pc - lc
				continue
			}
			judged++
			container += pc
			leaves += lc
		}
		if judged == 0 {
			out = append(out, res.info("too few readings"))
			continue
		}
		res.Got = fmt.Sprintf("Σ %.3fs container %.3fs over %d rounds", leaves, container, judged)
		if credited > 0 {
			res.Got += fmt.Sprintf(", %.3fs inferred for removed actors", credited)
		}
		res.Pass = within(leaves, container, leafTolerance, leafSlack)
		out = append(out, res)
		if unsplitRounds > 0 {
			out = append(out, verifyResult{Check: "conservation", Scope: "removed leaves " + parent,
				Want: "one actor removed per round", Got: fmt.Sprintf("%d rounds, %.3fs unattributed", unsplitRounds, unsplit)}.
				info("several actors removed in one round; their last use cannot be split"))
		}
	}
	return out
}

// checkCgreaderVsCadvisor compares each container's reader series with its
// cAdvisor readings over the span both cover. Both are on the node's clock.
func checkCgreaderVsCadvisor(cg []cgSample, cadvisor []cadvisorSample) []verifyResult {
	reader := map[string][]point{}
	for _, s := range cg {
		if s.Leaf == "" && !s.Gone {
			reader[s.key()] = append(reader[s.key()], point{s.T, s.CPUSeconds})
		}
	}
	cad := cpuSeries(cadvisor)
	var out []verifyResult
	for _, k := range slices.Sorted(maps.Keys(reader)) {
		rs, cs := reader[k], cad[k]
		if len(cs) == 0 {
			continue
		}
		slices.SortFunc(rs, func(a, b point) int { return a.t.Compare(b.t) })
		res := verifyResult{Check: "conservation", Scope: "cgreader vs cadvisor " + k, Want: fmt.Sprintf("equal ±%.0f%%", 100*cgreaderTolerance)}
		var in []point
		for _, c := range cs {
			if !c.t.Before(rs[0].t) && !c.t.After(rs[len(rs)-1].t) {
				in = append(in, c)
			}
		}
		if len(in) < 2 {
			out = append(out, res.info("fewer than two cAdvisor readings inside the reader's span"))
			continue
		}
		from, to := in[0].t, in[len(in)-1].t
		cadDelta := bounds{in[len(in)-1].v - in[0].v, in[len(in)-1].v - in[0].v}
		readerDelta, _ := deltaBounds(rs, from, to)
		out = append(out, compareBounds(res, readerDelta, cadDelta, cgreaderTolerance*cadDelta.mid()+cpuSlack))
	}
	return out
}

// checkReaderOverhead bounds each reader's own CPU, and fails for a node
// whose reader left no readings of itself. A counter that dropped, after a
// restart, adds nothing for that interval.
func checkReaderOverhead(cg *cgreaderReport) []verifyResult {
	self := map[string][]cgSample{}
	for _, s := range cg.Samples {
		if s.Component == "cgreader" && s.Container == cgreaderContainer && s.Leaf == "" && !s.Gone {
			self[s.Node] = append(self[s.Node], s)
		}
	}
	var out []verifyResult
	for _, node := range slices.Sorted(maps.Keys(cg.Nodes)) {
		res := verifyResult{Check: "overhead", Scope: "cgreader " + node, Want: fmt.Sprintf("≤ %.2f core", maxReaderCores)}
		ss := self[node]
		slices.SortFunc(ss, func(a, b cgSample) int { return a.T.Compare(b.T) })
		if len(ss) < 2 || !ss[len(ss)-1].T.After(ss[0].T) {
			res.Got = "no readings of the reader itself"
			out = append(out, res)
			continue
		}
		var used float64
		for i := 1; i < len(ss); i++ {
			if d := ss[i].CPUSeconds - ss[i-1].CPUSeconds; d > 0 {
				used += d
			}
		}
		cores := used / ss[len(ss)-1].T.Sub(ss[0].T).Seconds()
		res.Got, res.Pass = fmt.Sprintf("%.4f core", cores), cores <= maxReaderCores
		out = append(out, res)
	}
	return out
}

// cgreaderNodeRows returns the readers' container rows on their nodes' own
// clocks, for checks against data the kubelet stamped.
func cgreaderNodeRows(cg *cgreaderReport) []cadvisorSample {
	var rows []cadvisorSample
	for _, s := range cg.Samples {
		if s.Leaf != "" || s.Gone || s.Component == "cgreader" {
			continue
		}
		rows = append(rows, cadvisorSample{T: s.T, Component: s.Component, Pod: s.Pod, Container: s.Container,
			CPUSeconds: s.CPUSeconds, CFSPeriods: s.CFSPeriods, CFSThrottled: s.CFSThrottled, WorkingSetBytes: s.WorkingSetBytes})
	}
	return rows
}

// cgreaderAsCadvisor turns the reader's container rows into cAdvisor-shaped
// samples on the driver's clock, so the steady summaries can use them. It
// adds two derived containers for each worker: atunnel, the ateom leaf, and
// actors, a counter built from the actors' leaves' increases round by round,
// so actors that come and go, including a removed actor's inferred last
// use, keep it monotonic.
func cgreaderAsCadvisor(cg *cgreaderReport) (containers, split []cadvisorSample) {
	for _, s := range cg.Samples {
		row := cadvisorSample{T: s.T.Add(-medianOffset(cg.Nodes[s.Node])), Component: s.Component, Pod: s.Pod, Container: s.Container,
			CPUSeconds: s.CPUSeconds, CFSPeriods: s.CFSPeriods, CFSThrottled: s.CFSThrottled, WorkingSetBytes: s.WorkingSetBytes}
		switch {
		case s.Gone || s.Component == "cgreader":
		case s.Leaf == "":
			containers = append(containers, row)
		case s.Component == "workers" && s.Leaf == "ateom":
			row.Container = "atunnel"
			split = append(split, row)
		}
	}
	for _, rs := range leafRounds(cg.Samples) {
		first := cg.Samples[rs[0][""]]
		// The pod cgroup has leaves too, its container scopes; only ateom
		// holds the actors.
		if first.Component != "workers" || first.Container != "ateom" {
			continue
		}
		offset := medianOffset(cg.Nodes[first.Node])
		var cum float64
		for r, cur := range rs {
			var ws float64
			for leaf, i := range cur {
				s := cg.Samples[i]
				// Both of an actor's leaves count: the sandbox runs in the
				// _pause cgroup, its gofer and shim in the actor cgroup.
				if actorOf(leaf) == leaf {
					continue
				}
				if !s.Gone {
					ws += s.WorkingSetBytes
				}
				if r == 0 {
					continue
				}
				now := s.CPUSeconds
				if s.InferredCPUSeconds != nil {
					now = *s.InferredCPUSeconds
				}
				var before float64
				if j, ok := rs[r-1][leaf]; ok {
					before = cg.Samples[j].CPUSeconds
				}
				cum += now - before
			}
			t := cg.Samples[cur[""]].T.Add(-offset)
			split = append(split, cadvisorSample{T: t, Component: "workers", Pod: first.Pod, Container: "actors", CPUSeconds: cum, WorkingSetBytes: ws})
		}
	}
	return containers, split
}
