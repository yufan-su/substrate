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
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Process metrics every Go component serves on its Prometheus port. They are
// read from /proc at scrape time, so each scrape is current.
const (
	metricProcessCPU = "process_cpu_seconds_total"
	metricProcessRSS = "process_resident_memory_bytes"
)

// envoyClusters are the gateway clusters the sampler reads: the actors'
// tunnels, and the TLS and cleartext connections to the targets, which
// HTTPS and HTTP requests use in turn.
var envoyClusters = []string{"mitm_internal", "egress_forward_proxy", "egress_forward_proxy_cleartext"}

// envoyStats maps each Prometheus metric the sampler reads from the gateway
// to the admin stat it is reported under, so the report keys stay
// cluster.<cluster>.<stat> whatever the endpoint. The gateway's admin API
// is loopback-only; its envoy_metrics listener forwards GET /ready and
// GET /stats/prometheus to it and nothing else. The connection counters
// come with the gauge that is 1 while a cluster's connection breaker is
// open.
var envoyStats = map[string]string{
	"envoy_cluster_upstream_cx_total":                "upstream_cx_total",
	"envoy_cluster_upstream_cx_active":               "upstream_cx_active",
	"envoy_cluster_upstream_cx_overflow":             "upstream_cx_overflow",
	"envoy_cluster_circuit_breakers_default_cx_open": "circuit_breakers.default.cx_open",
}

// envoyStatsFilter selects the sampler's stats on /stats/prometheus, which
// filters on the admin stat name.
var envoyStatsFilter = envoyFilter()

func envoyFilter() string {
	stats := make([]string, 0, len(envoyStats))
	for _, stat := range envoyStats {
		stats = append(stats, regexp.QuoteMeta(stat))
	}
	slices.Sort(stats)
	return `^cluster\.(` + strings.Join(envoyClusters, "|") + `)\.(` + strings.Join(stats, "|") + `)$`
}

// envoyCxTotal counts the connections the gateway opened toward the actors'
// tunnels, one per actor connection.
const envoyCxTotal = "cluster.mitm_internal.upstream_cx_total"

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

// parseEnvoyStats reads the gateway's /stats/prometheus text and returns
// the stats in envoyStats for the clusters in envoyClusters, keyed by their
// admin names. Every other line, including comments and histogram buckets,
// is skipped.
func parseEnvoyStats(body []byte) map[string]float64 {
	out := map[string]float64{}
	for line := range strings.Lines(string(body)) {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		name, rest, ok := strings.Cut(line, "{")
		if !ok {
			continue
		}
		stat, ok := envoyStats[name]
		if !ok {
			continue
		}
		labels, value, ok := strings.Cut(rest, "}")
		if !ok {
			continue
		}
		cluster := promLabel(labels, "envoy_cluster_name")
		if !slices.Contains(envoyClusters, cluster) {
			continue
		}
		// An optional timestamp may follow the value.
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		out["cluster."+cluster+"."+stat] = v
	}
	return out
}

// promLabel returns the value of label name in a Prometheus label list such
// as `a="x",b="y"`, or "" when it is absent.
func promLabel(labels, name string) string {
	for _, kv := range strings.Split(labels, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if ok && k == name {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}
