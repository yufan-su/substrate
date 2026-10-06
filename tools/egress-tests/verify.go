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
