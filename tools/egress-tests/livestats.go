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
	"fmt"
	"regexp"
	"slices"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
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
// come with the pending-queue counters and the gauge that is 1 while a
// cluster's connection breaker is open. Every CONNECT passes the pending
// queue while its connection is set up; one past max_connections waits
// there (cx_overflow counts the holds, pending_active is the queue depth)
// until it is admitted, cancelled when the client gives up, or refused
// when the queue is full too (pending_overflow).
var envoyStats = map[string]string{
	"envoy_cluster_upstream_cx_total":                "upstream_cx_total",
	"envoy_cluster_upstream_cx_active":               "upstream_cx_active",
	"envoy_cluster_upstream_cx_overflow":             "upstream_cx_overflow",
	"envoy_cluster_upstream_rq_pending_active":       "upstream_rq_pending_active",
	"envoy_cluster_upstream_rq_pending_overflow":     "upstream_rq_pending_overflow",
	"envoy_cluster_upstream_rq_cancelled":            "upstream_rq_cancelled",
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
const (
	envoyCxTotal       = "cluster.mitm_internal.upstream_cx_total"
	envoyCxOverflow    = "cluster.mitm_internal.upstream_cx_overflow"
	envoyRqCancelled   = "cluster.mitm_internal.upstream_rq_cancelled"
	envoyRqRefused     = "cluster.mitm_internal.upstream_rq_pending_overflow"
	envoyPendingActive = "cluster.mitm_internal.upstream_rq_pending_active"
)

// parseProcessMetrics reads the process CPU and RSS from Prometheus text.
func parseProcessMetrics(body []byte) (cpuSeconds, rssBytes float64, err error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	value := func(name string) (float64, bool) {
		for _, m := range families[name].GetMetric() {
			if v, ok := sampleValue(m); ok {
				return v, true
			}
		}
		return 0, false
	}
	cpu, ok := value(metricProcessCPU)
	if !ok {
		return 0, 0, fmt.Errorf("no %s in the metrics", metricProcessCPU)
	}
	rss, _ := value(metricProcessRSS)
	return cpu, rss, nil
}

// sampleValue is a counter's, gauge's or untyped sample's value.
func sampleValue(m *dto.Metric) (float64, bool) {
	switch {
	case m.Counter != nil:
		return m.Counter.GetValue(), true
	case m.Gauge != nil:
		return m.Gauge.GetValue(), true
	case m.Untyped != nil:
		return m.Untyped.GetValue(), true
	}
	return 0, false
}

// parseEnvoyStats decodes the gateway's /stats/prometheus text and returns
// the stats in envoyStats for the clusters in envoyClusters, keyed by their
// admin names. Other families and clusters are dropped. A body that is not
// Prometheus text yields nothing, which readEnvoy counts as a failed read.
func parseEnvoyStats(body []byte) map[string]float64 {
	out := map[string]float64{}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return out
	}
	for metric, stat := range envoyStats {
		for _, m := range families[metric].GetMetric() {
			cluster := labelMap(m)["envoy_cluster_name"]
			if !slices.Contains(envoyClusters, cluster) {
				continue
			}
			if v, ok := sampleValue(m); ok {
				out["cluster."+cluster+"."+stat] = v
			}
		}
	}
	return out
}
