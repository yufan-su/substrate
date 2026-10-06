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
	"testing"
	"time"
)

// cadvisorFixture is a trimmed /metrics/cadvisor body: two gateway
// containers, the pod cgroup, the pause container, a container the test
// does not target, and a pod outside the targets.
const cadvisorFixture = `# HELP container_cpu_usage_seconds_total Cumulative cpu time consumed in seconds.
# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="",cpu="total",id="/kubepods.slice/p.slice",image="",name="",namespace="ate-system",pod="gw"} 30.5 1791289219021
container_cpu_usage_seconds_total{container="",cpu="total",id="/kubepods.slice/p.slice/cri-containerd-a.scope",image="gke.gcr.io/pause:3.8",name="a",namespace="ate-system",pod="gw"} 0.01 1791289219021
container_cpu_usage_seconds_total{container="envoy",cpu="total",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 20.25 1791289219021
container_cpu_usage_seconds_total{container="ext-proc",cpu="total",id="/y",image="atenet",name="c",namespace="ate-system",pod="gw"} 10.24 1791289221000
container_cpu_usage_seconds_total{container="other",cpu="total",id="/z",image="o",name="d",namespace="ate-system",pod="gw"} 99 1791289219021
container_cpu_usage_seconds_total{container="envoy",cpu="total",id="/w",image="envoy",name="e",namespace="ate-system",pod="router"} 5 1791289219021
# TYPE container_cpu_cfs_periods_total counter
container_cpu_cfs_periods_total{container="envoy",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 1000 1791289219021
# TYPE container_cpu_cfs_throttled_periods_total counter
container_cpu_cfs_throttled_periods_total{container="envoy",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 25 1791289219021
# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{container="envoy",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 6.2e+07 1791289219021
`

func TestParseCadvisor(t *testing.T) {
	t.Parallel()
	pods := map[string]cadvisorPod{"ate-system/gw": {component: "gateway", containers: []string{"envoy", "ext-proc"}}}
	rows, err := parseCadvisor([]byte(cadvisorFixture), pods)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]cadvisorSample{}
	for _, r := range rows {
		got[r.Container] = r
	}
	if len(got) != 3 {
		t.Fatalf("parsed containers %v, want POD, envoy and ext-proc only", rows)
	}
	for _, tc := range []struct {
		container string
		want      cadvisorSample
	}{
		{"POD", cadvisorSample{T: time.UnixMilli(1791289219021), Component: "gateway", Pod: "gw", Container: "POD", CPUSeconds: 30.5}},
		{"envoy", cadvisorSample{T: time.UnixMilli(1791289219021), Component: "gateway", Pod: "gw", Container: "envoy",
			CPUSeconds: 20.25, CFSPeriods: 1000, CFSThrottled: 25, WorkingSetBytes: 6.2e7}},
		{"ext-proc", cadvisorSample{T: time.UnixMilli(1791289221000), Component: "gateway", Pod: "gw", Container: "ext-proc", CPUSeconds: 10.24}},
	} {
		t.Run(tc.container, func(t *testing.T) {
			if g := got[tc.container]; g != tc.want {
				t.Errorf("got %+v, want %+v", g, tc.want)
			}
		})
	}
	if _, err := parseCadvisor([]byte("not { prometheus"), pods); err == nil {
		t.Errorf("parseCadvisor accepted malformed text")
	}
}

func TestCounterAt(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(100, 0)
	ps := []point{{t0, 10}, {t0.Add(10 * time.Second), 20}, {t0.Add(20 * time.Second), 40}}
	for _, tc := range []struct {
		name   string
		at     time.Duration
		want   float64
		wantOK bool
	}{
		{"first", 0, 10, true},
		{"between", 5 * time.Second, 15, true},
		{"exact middle", 10 * time.Second, 20, true},
		{"second segment", 15 * time.Second, 30, true},
		{"last", 20 * time.Second, 40, true},
		{"before", -time.Second, 0, false},
		{"after", 21 * time.Second, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := counterAt(ps, t0.Add(tc.at))
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("counterAt(+%v) = %v, %v; want %v, %v", tc.at, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// series builds cAdvisor samples of one container at the given times, with
// CPU rising at cores.
func series(component, pod, container string, cores float64, at ...time.Duration) []cadvisorSample {
	t0 := time.Unix(1000, 0)
	var out []cadvisorSample
	for _, d := range at {
		out = append(out, cadvisorSample{T: t0.Add(d), Component: component, Pod: pod, Container: container, CPUSeconds: cores * d.Seconds()})
	}
	return out
}

func TestCheckPodRollups(t *testing.T) {
	t.Parallel()
	s := time.Second
	pods := map[string]cadvisorPod{"ate-system/gw": {component: "gateway", containers: []string{"envoy", "ext-proc"}, complete: true}}
	for _, tc := range []struct {
		name     string
		samples  [][]cadvisorSample
		pods     map[string]cadvisorPod
		wantPass bool
		wantInfo bool
	}{
		{name: "sum matches, stamped apart",
			samples: [][]cadvisorSample{series("gateway", "gw", "POD", 1.5, 0, 16*s, 32*s, 48*s), series("gateway", "gw", "envoy", 1, 3*s, 19*s, 35*s, 51*s), series("gateway", "gw", "ext-proc", 0.5, 0, 14*s, 30*s, 50*s)},
			pods:    pods, wantPass: true},
		{name: "a container missing from the sum",
			samples: [][]cadvisorSample{series("gateway", "gw", "POD", 2, 0, 16*s, 32*s), series("gateway", "gw", "envoy", 1, 0, 16*s, 32*s), series("gateway", "gw", "ext-proc", 0.5, 0, 16*s, 32*s)},
			pods:    pods, wantPass: false},
		{name: "too few readings",
			samples: [][]cadvisorSample{series("gateway", "gw", "POD", 1, 0), series("gateway", "gw", "envoy", 1, 0)},
			pods:    pods, wantInfo: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var all []cadvisorSample
			for _, ss := range tc.samples {
				all = append(all, ss...)
			}
			got := checkPodRollups(all, tc.pods)
			if len(got) != 1 || got[0].Pass != tc.wantPass || got[0].Info != tc.wantInfo {
				t.Errorf("checkPodRollups = %+v, want pass %v info %v", got, tc.wantPass, tc.wantInfo)
			}
		})
	}
	incomplete := map[string]cadvisorPod{"kube-system/dns": {component: "dns", containers: []string{"kubedns"}}}
	if got := checkPodRollups(series("dns", "dns", "POD", 1, 0, s), incomplete); len(got) != 0 {
		t.Errorf("checked a pod whose containers are not all sampled: %+v", got)
	}
}

func TestCheckProcessVsCgroup(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	live := func(cores float64) []liveSample {
		var out []liveSample
		for i := range 61 {
			d := time.Duration(i) * time.Second
			out = append(out, liveSample{T: t0.Add(d), Component: "gateway", Container: "ext-proc", Pod: "gw", ProcessCPUSeconds: cores * d.Seconds()})
		}
		return out
	}
	cg := series("gateway", "gw", "ext-proc", 0.4, 0, 16*time.Second, 33*time.Second, 49*time.Second)
	for _, tc := range []struct {
		name string
		live []liveSample
		want bool
	}{
		{"equal", live(0.4), true},
		{"within 3%", live(0.39), true},
		{"process 20% low", live(0.32), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := checkProcessVsCgroup(tc.live, cg)
			if len(got) != 1 || got[0].Pass != tc.want {
				t.Errorf("checkProcessVsCgroup = %+v, want pass %v", got, tc.want)
			}
		})
	}
}

func TestCheckCadvisorCoverage(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	for _, tc := range []struct {
		name     string
		steady   time.Duration
		wantPass bool
	}{
		{"two readings in a 40s steady", 40 * s, true},
		{"one reading in a 10s steady", 10 * s, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			samples := series("gateway", "gw", "envoy", 1, 0, 16*s, 32*s, 48*s)
			steady := phaseMark{Name: "steady", Start: t0.Add(10 * s), End: t0.Add(10*s + tc.steady)}
			got := checkCadvisorCoverage(samples, steady)
			if len(got) != 1 || got[0].Pass != tc.wantPass || got[0].Info == tc.wantPass {
				t.Errorf("checkCadvisorCoverage = %+v, want pass %v, info when insufficient", got, tc.wantPass)
			}
		})
	}
}

func TestCheckComponentsFound(t *testing.T) {
	t.Parallel()
	samples := series("gateway", "gw", "envoy", 1, 0)
	got := map[string]bool{}
	for _, r := range checkComponentsFound([]string{"dns"}, samples) {
		got[r.Scope] = r.Pass
	}
	if !got["component gateway"] || got["component dns"] || got["component workers"] || len(got) != len(cadvisorTargets) {
		t.Errorf("checkComponentsFound = %v, want only gateway passing", got)
	}
}
