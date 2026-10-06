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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Process metrics every Go component serves on its Prometheus port. They are
// read from /proc at scrape time, so each scrape is current.
const (
	metricProcessCPU = "process_cpu_seconds_total"
	metricProcessRSS = "process_resident_memory_bytes"
)

// envoyStatsFilter selects the gateway's connection and pending-queue
// counters, and the gauge that is 1 while a cluster's connection breaker is
// open, from /stats. A CONNECT past max_connections waits in the pending
// queue (pending_total, then cancelled if the client gives up) and is
// refused only when that queue is full too (pending_overflow). A stream
// that gets a connection but would exceed max_requests is refused on
// attach (active_overflow); rq_open is 1 while that breaker is open.
const envoyStatsFilter = `^cluster\.(mitm_internal|egress_forward_proxy_cleartext)\.(upstream_cx_(total|active|overflow)|upstream_rq_(pending_total|pending_overflow|active_overflow|cancelled)|circuit_breakers\.default\.(cx_open|rq_open))$`

// envoyClusters are the gateway clusters the sampler reads.
var envoyClusters = []string{"mitm_internal", "egress_forward_proxy_cleartext"}

// envoyCxTotal counts the connections the gateway opened toward the actors'
// tunnels, one per actor connection.
const (
	envoyCxTotal    = "cluster.mitm_internal.upstream_cx_total"
	envoyCxOverflow = "cluster.mitm_internal.upstream_cx_overflow"
)

// parseProcessMetrics reads the process CPU and RSS from Prometheus text.
func parseProcessMetrics(body []byte) (cpuSeconds, rssBytes float64, err error) {
	found := map[string]float64{}
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, rest, ok := strings.Cut(line, " ")
		if !ok || (name != metricProcessCPU && name != metricProcessRSS) {
			continue
		}
		// An optional timestamp may follow the value.
		v, err := strconv.ParseFloat(strings.Fields(rest)[0], 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parsing %s: %w", name, err)
		}
		found[name] = v
	}
	if err := sc.Err(); err != nil {
		return 0, 0, err
	}
	cpu, ok := found[metricProcessCPU]
	if !ok {
		return 0, 0, fmt.Errorf("no %s in the metrics", metricProcessCPU)
	}
	return cpu, found[metricProcessRSS], nil
}

// parseEnvoyStats reads Envoy's "name: value" /stats lines. Histogram lines
// and other non-numeric values are skipped.
// envoyMemoryInterval is how often the sampler reads Envoy's /memory.
const envoyMemoryInterval = 5 * time.Second

// envoyMemory is Envoy's /memory: the allocator's view of the heap, which
// the cgroup's working set mixes with page cache and freed pages.
type envoyMemory struct {
	Allocated string `json:"allocated"`
	HeapSize  string `json:"heap_size"`
	Physical  string `json:"total_physical_bytes"`
}

// parseEnvoyMemory reads allocated, heap size and physical bytes from
// Envoy's /memory, which reports them as decimal strings.
func parseEnvoyMemory(body []byte) (allocated, heap, physical float64, err error) {
	var m envoyMemory
	if err := json.Unmarshal(body, &m); err != nil {
		return 0, 0, 0, fmt.Errorf("parsing /memory: %w", err)
	}
	vals := make([]float64, 3)
	for i, s := range []string{m.Allocated, m.HeapSize, m.Physical} {
		if vals[i], err = strconv.ParseFloat(s, 64); err != nil {
			return 0, 0, 0, fmt.Errorf("parsing /memory value %q: %w", s, err)
		}
	}
	return vals[0], vals[1], vals[2], nil
}

func parseEnvoyStats(body []byte) map[string]float64 {
	out := map[string]float64{}
	for line := range strings.Lines(string(body)) {
		name, value, ok := strings.Cut(strings.TrimSpace(line), ": ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		out[name] = v
	}
	return out
}
