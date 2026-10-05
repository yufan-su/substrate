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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

// report is everything one run measured. --output writes it as JSON;
// durations are in nanoseconds.
type report struct {
	Config      runConfig        `json:"config"`
	Started     time.Time        `json:"started"`
	Finished    time.Time        `json:"finished"`
	Interrupted bool             `json:"interrupted"`
	Create      createResult     `json:"create"`
	Resume      resumeResult     `json:"resume"`
	Start       phaseResult      `json:"start"`
	Stop        phaseResult      `json:"stop"`
	Suspend     phaseResult      `json:"suspend"`
	Loop        *egressapi.Stats `json:"loop,omitempty"`
	Actors      []actorResult    `json:"actors,omitempty"`
}

type phaseResult struct {
	Attempted int           `json:"attempted"`
	Succeeded int           `json:"succeeded"`
	Failed    int           `json:"failed"`
	Duration  time.Duration `json:"duration"`
	// Errors counts failures by kind: the gRPC code, or the error itself
	// when it is not a gRPC status.
	Errors map[string]int `json:"errors,omitempty"`
	// Examples keeps the first few failures in full.
	Examples []string `json:"examples,omitempty"`
}

type createResult struct {
	phaseResult
	ActorsReused    int `json:"actorsReused"`
	PoliciesUpdated int `json:"policiesUpdated"`
}

type resumeResult struct {
	phaseResult
	ResumeLatency latencySummary `json:"resumeLatency"`
	ReadyLatency  latencySummary `json:"readyLatency"`
}

type actorResult struct {
	Name          string           `json:"name"`
	ResumeLatency time.Duration    `json:"resumeLatency"`
	ReadyLatency  time.Duration    `json:"readyLatency"`
	Error         string           `json:"error,omitempty"`
	Stats         *egressapi.Stats `json:"stats,omitempty"`
}

func (a *actorRun) result() actorResult {
	res := actorResult{Name: a.name, ResumeLatency: a.resumeLatency, ReadyLatency: a.readyLatency, Stats: a.stats}
	if a.err != nil {
		res.Error = a.err.Error()
	}
	return res
}

// maxExamples bounds phaseResult.Examples.
const maxExamples = 3

// phase counts the outcomes of one phase from many goroutines.
type phase struct {
	mu        sync.Mutex
	succeeded int
	failed    int
	errors    map[string]int
	examples  []string
}

func newPhase() *phase { return &phase{errors: make(map[string]int)} }

func (p *phase) succeed() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.succeeded++
}

func (p *phase) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed++
	p.errors[errorKind(err)]++
	if len(p.examples) < maxExamples {
		p.examples = append(p.examples, err.Error())
	}
}

func (p *phase) result(attempted int, d time.Duration) phaseResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	res := phaseResult{Attempted: attempted, Succeeded: p.succeeded, Failed: p.failed, Duration: d, Examples: p.examples}
	if len(p.errors) > 0 {
		res.Errors = maps.Clone(p.errors)
	}
	return res
}

// errorKind groups errors without the actor names they carry: a failure that
// hits thousands of actors is one kind, not thousands.
func errorKind(err error) string {
	var kind string
	var se *statusError
	if errors.As(err, &se) {
		kind = fmt.Sprintf("HTTP %d", se.code)
	} else if s, ok := status.FromError(err); ok {
		kind = s.Code().String()
	} else {
		// Wrapping adds context in front; the cause is the last part.
		kind = err.Error()
		if i := strings.LastIndex(kind, ": "); i >= 0 {
			kind = kind[i+2:]
		}
	}
	if errors.Is(err, context.DeadlineExceeded) && kind != codes.DeadlineExceeded.String() {
		kind = "gave up retrying " + kind
	}
	return kind
}

type latencySummary struct {
	Count int           `json:"count"`
	P50   time.Duration `json:"p50"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

func (s latencySummary) String() string {
	if s.Count == 0 {
		return "no samples"
	}
	return fmt.Sprintf("p50 %v p99 %v max %v", round(s.P50), round(s.P99), round(s.Max))
}

// summarize returns nearest-rank percentiles of durs.
func summarize(durs []time.Duration) latencySummary {
	if len(durs) == 0 {
		return latencySummary{}
	}
	sorted := slices.Sorted(slices.Values(durs))
	at := func(q float64) time.Duration {
		i := int(math.Ceil(q*float64(len(sorted)))) - 1
		return sorted[min(max(i, 0), len(sorted)-1)]
	}
	return latencySummary{Count: len(sorted), P50: at(0.5), P99: at(0.99), Max: sorted[len(sorted)-1]}
}

// round trims a duration to three significant digits for printing.
func round(d time.Duration) time.Duration {
	switch {
	case d >= 10*time.Second:
		return d.Round(100 * time.Millisecond)
	case d >= time.Second:
		return d.Round(10 * time.Millisecond)
	case d >= 10*time.Millisecond:
		return d.Round(100 * time.Microsecond)
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}

func printPhase(w io.Writer, name string, p phaseResult, extra string) {
	fmt.Fprintf(w, "%-10s %d/%d ok in %v%s\n", name, p.Succeeded, p.Attempted, round(p.Duration), extra)
	if p.Failed == 0 {
		return
	}
	fmt.Fprintf(w, "%-10s %d failed: %s\n", "", p.Failed, formatCounts(p.Errors))
	for _, ex := range p.Examples {
		fmt.Fprintf(w, "%-10s   e.g. %s\n", "", ex)
	}
}

// formatCounts prints counts largest first.
func formatCounts[V int | int64](counts map[string]V) string {
	keys := slices.SortedFunc(maps.Keys(counts), func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), cmp.Compare(a, b))
	})
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, counts[k])
	}
	return strings.Join(parts, " ")
}

// print writes the human-readable summary of the run.
func (rep *report) print(w io.Writer) {
	c := rep.Config
	fmt.Fprintf(w, "\n== egress-tests: actors=%d parallel=%d endpoints=%d conn-mode=%s duration=%v\n",
		c.Actors, c.Parallel, c.Endpoints, c.ConnMode, c.Duration)
	if rep.Interrupted {
		fmt.Fprintln(w, "!! interrupted: the numbers below cover only part of the run")
	}
	printPhase(w, "create", rep.Create.phaseResult,
		fmt.Sprintf(" (%d actors reused, %d policies updated)", rep.Create.ActorsReused, rep.Create.PoliciesUpdated))
	printPhase(w, "resume", rep.Resume.phaseResult, "")
	fmt.Fprintf(w, "%-10s resume %s; ready %s\n", "", rep.Resume.ResumeLatency, rep.Resume.ReadyLatency)
	printPhase(w, "start", rep.Start, "")

	if s := rep.Loop; s != nil && s.Requests > 0 {
		secs := s.Elapsed.Seconds()
		fmt.Fprintf(w, "%-10s %v: %d requests, %.1f req/s, %.3f%% success, %d new connections\n",
			"loop", round(s.Elapsed), s.Requests, float64(s.Requests)/secs, 100*float64(s.Successes)/float64(s.Requests), s.NewConns)
		fmt.Fprintf(w, "%-10s p50 %v p90 %v p99 %v p99.9 %v max %v (mean %v)\n", "latency",
			round(s.Latency.Quantile(0.5)), round(s.Latency.Quantile(0.9)), round(s.Latency.Quantile(0.99)),
			round(s.Latency.Quantile(0.999)), round(s.Latency.Max()), round(s.Latency.Mean()))
		if s.DNS.Count > 0 {
			fmt.Fprintf(w, "%-10s %d lookups, p50 %v p99 %v max %v\n", "dns",
				s.DNS.Count, round(s.DNS.Quantile(0.5)), round(s.DNS.Quantile(0.99)), round(s.DNS.Max()))
		}
		if len(s.Errors) > 0 {
			fmt.Fprintf(w, "%-10s %s\n", "errors", formatCounts(s.Errors))
		}
		rep.printPerActor(w)
		printWorstEndpoints(w, s)
	} else {
		fmt.Fprintf(w, "%-10s no requests measured\n", "loop")
	}
	printPhase(w, "stop", rep.Stop, "")
	printPhase(w, "suspend", rep.Suspend, "")
}

func (rep *report) printPerActor(w io.Writer) {
	var rates []float64
	for _, a := range rep.Actors {
		if a.Stats != nil && a.Stats.Elapsed > 0 {
			rates = append(rates, float64(a.Stats.Requests)/a.Stats.Elapsed.Seconds())
		}
	}
	if len(rates) == 0 {
		return
	}
	slices.Sort(rates)
	fmt.Fprintf(w, "%-10s req/s per actor: min %.1f median %.1f max %.1f\n", "",
		rates[0], rates[len(rates)/2], rates[len(rates)-1])
}

// printWorstEndpoints lists the endpoints with the most errors, if any failed.
func printWorstEndpoints(w io.Writer, s *egressapi.Stats) {
	idx := make([]int, 0, len(s.Endpoints))
	for i, e := range s.Endpoints {
		if e.Errors > 0 {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return
	}
	slices.SortFunc(idx, func(a, b int) int {
		return cmp.Or(cmp.Compare(s.Endpoints[b].Errors, s.Endpoints[a].Errors), cmp.Compare(a, b))
	})
	var parts []string
	for _, i := range idx[:min(len(idx), 5)] {
		parts = append(parts, fmt.Sprintf("%s (%d/%d)", egressapi.ServiceName(i), s.Endpoints[i].Errors, s.Endpoints[i].Requests))
	}
	fmt.Fprintf(w, "%-10s most errors: %s\n", "", strings.Join(parts, ", "))
}
