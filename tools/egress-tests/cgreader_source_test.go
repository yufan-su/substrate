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
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/cgapi"
)

func TestCgroupPath(t *testing.T) {
	t.Parallel()
	pod := func(qos corev1.PodQOSClass) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("7062ce39-3e75-4281")}, Status: corev1.PodStatus{QOSClass: qos}}
	}
	for _, tc := range []struct {
		name        string
		qos         corev1.PodQOSClass
		containerID string
		want        string
	}{
		{"burstable pod", corev1.PodQOSBurstable, "", "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod7062ce39_3e75_4281.slice"},
		{"besteffort container", corev1.PodQOSBestEffort, "containerd://abc",
			"kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod7062ce39_3e75_4281.slice/cri-containerd-abc.scope"},
		{"guaranteed container", corev1.PodQOSGuaranteed, "containerd://abc", "kubepods.slice/kubepods-pod7062ce39_3e75_4281.slice/cri-containerd-abc.scope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := cgroupPath(pod(tc.qos), tc.containerID); got != tc.want {
				t.Errorf("cgroupPath = %q, want %q", got, tc.want)
			}
		})
	}
}

// fakeReaders answers the cgroup readers' samples route from scripted
// responses, one per pull, and records the targets each pull sent.
type fakeReaders struct {
	mu        sync.Mutex
	responses []cgapi.Response
	since     []string
	targets   []string
}

func (f *fakeReaders) GetRaw(_ context.Context, path string, params url.Values) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.since = append(f.since, params.Get("since"))
	f.targets = params["target"]
	resp := f.responses[0]
	if len(f.responses) > 1 {
		f.responses = f.responses[1:]
	}
	return json.Marshal(resp)
}

// newPullSampler is a sampler with one reader on node-a, mapped to one
// worker scope, answering from get.
func newPullSampler(t *testing.T, get rawGetter, k8s *fake.Clientset, scope string) *resourceSampler {
	t.Helper()
	s := newResourceSampler(get, k8s, time.Second)
	s.cg = &cgreaderState{targets: map[string]map[string]cgTarget{"node-a": {scope: {"workers", "w1", "ateom"}}},
		lastSeq: map[string]uint64{}, started: map[string]bool{}, boot: map[string]int64{}}
	s.rep.Cgreader = &cgreaderReport{Nodes: map[string]*cgNode{"node-a": {Reader: "r1"}}}
	return s
}

func TestPullReader(t *testing.T) {
	t.Parallel()
	const scope = "kubepods.slice/x.slice/cri-containerd-a.scope"
	row := func(seq uint64, usage int64) cgapi.Row {
		return cgapi.Row{Seq: seq, WallNanos: int64(seq) * 1e9, Target: scope, UsageUsec: usage}
	}
	get := &fakeReaders{responses: []cgapi.Response{
		// The first pull only learns where the ring is.
		{WallNanos: 3e9, Oldest: 1, Latest: 100, Boot: 7},
		// Then two pages, and a pull that skips seq 104.
		{WallNanos: 5e9, Oldest: 1, Latest: 103, Boot: 7, Rows: []cgapi.Row{row(101, 1e6), row(102, 2e6)}, More: true},
		{WallNanos: 5e9, Oldest: 1, Latest: 103, Boot: 7, Rows: []cgapi.Row{row(103, 3e6)}},
		{WallNanos: 9e9, Oldest: 105, Latest: 106, Boot: 7, Rows: []cgapi.Row{row(105, 5e6), row(106, 6e6)}},
	}}
	s := newPullSampler(t, get, fake.NewSimpleClientset(), scope)
	clock := time.Unix(3, 0)
	s.now = func() time.Time { return clock }

	s.pullReader(t.Context(), "node-a")
	clock = time.Unix(5, 0)
	s.pullReader(t.Context(), "node-a")
	clock = time.Unix(8, 0)
	s.pullReader(t.Context(), "node-a")

	n := s.rep.Cgreader.Nodes["node-a"]
	if got := get.since; strings.Join(got, ",") != "latest,100,102,103" {
		t.Errorf("pulls asked since %v, want latest, 100, then the second page from 102, then 103", got)
	}
	if n.Lost != 1 || n.Restarts != 0 {
		t.Errorf("lost %d restarts %d, want 1 (seq 104) and 0", n.Lost, n.Restarts)
	}
	if len(n.Offsets) != 3 || n.Offsets[2].Offset != time.Second {
		t.Errorf("offsets = %+v, want one per pull, not per page, the last 1s", n.Offsets)
	}
	samples := s.rep.Cgreader.Samples
	if len(samples) != 5 || samples[4].CPUSeconds != 6 || samples[4].Container != "ateom" || samples[4].Pod != "w1" {
		t.Errorf("samples = %+v, want 5 rows mapped to workers/ateom@w1 with usage in seconds", samples)
	}
	if len(get.targets) != 1 || get.targets[0] != scope {
		t.Errorf("targets sent = %v, want [%s]", get.targets, scope)
	}
}

func TestPullReaderRestart(t *testing.T) {
	t.Parallel()
	const scope = "kubepods.slice/x.slice/cri-containerd-a.scope"
	k8s := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "egress-tests", UID: "r-1"},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSBurstable,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "cgreader", ContainerID: "containerd://new"}}},
	})
	for _, tc := range []struct {
		name  string
		after cgapi.Response
	}{
		{"new boot time", cgapi.Response{Latest: 900, Boot: 8}},
		{"seq behind the last pull", cgapi.Response{Latest: 3, Boot: 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			get := &fakeReaders{responses: []cgapi.Response{
				{Latest: 500, Boot: 7},
				tc.after,
				{Latest: tc.after.Latest, Boot: tc.after.Boot, Rows: []cgapi.Row{{Seq: 1, Target: scope}}},
			}}
			s := newPullSampler(t, get, k8s, scope)
			s.cg.targets["node-a"]["old-self"] = cgTarget{"cgreader", "r1", "cgreader"}
			s.pullReader(t.Context(), "node-a")
			s.pullReader(t.Context(), "node-a")

			if n := s.rep.Cgreader.Nodes["node-a"]; n.Restarts != 1 {
				t.Errorf("restarts = %d, want 1", n.Restarts)
			}
			if got := strings.Join(get.since, ","); got != "latest,500,0" {
				t.Errorf("pulls asked since %s, want latest, 500, then 0 on the restarted reader", got)
			}
			if len(s.rep.Cgreader.Samples) != 1 {
				t.Errorf("samples = %+v, want the restarted reader's first row", s.rep.Cgreader.Samples)
			}
			self := ""
			for path, tgt := range s.cg.targets["node-a"] {
				if tgt.component == "cgreader" && tgt.container == "cgreader" {
					self = path
				}
			}
			if !strings.HasSuffix(self, "cri-containerd-new.scope") {
				t.Errorf("reader self target = %q, want the new container's scope", self)
			}
		})
	}
}

func TestResolveCgreader(t *testing.T) {
	t.Parallel()
	k8s := fake.NewSimpleClientset()
	mk := func(ns, name, uid, node string, labels map[string]string, qos corev1.PodQOSClass, containers ...string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid), Labels: labels},
			Spec:       corev1.PodSpec{NodeName: node},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, QOSClass: qos},
		}
		for _, c := range containers {
			p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{Name: c, ContainerID: "containerd://" + c + "-id"})
		}
		if _, err := k8s.CoreV1().Pods(ns).Create(t.Context(), p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	mk("egress-tests", "reader-a", "r-a", "node-a", map[string]string{"app": "egress-tests-cgreader"}, corev1.PodQOSBurstable, "cgreader")
	mk("ate-system", "gw", "g-1", "node-a", map[string]string{"app": "atenet-egress"}, corev1.PodQOSBurstable, "envoy", "ext-proc", "sidecar")
	mk("egress-tests", "w-b", "w-b", "node-b", map[string]string{"ate.dev/worker-pool": "egress-tests"}, corev1.PodQOSBurstable, "ateom")

	get := newFakeGetter()
	s := newResourceSampler(get, k8s, time.Second)
	if err := s.resolveCgreader(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := get.readsOf(podProxyPath("egress-tests", "reader-a", 8080, "/healthz")); n != 1 {
		t.Errorf("warm-up requests to reader-a = %d, want 1 before the first timed pull", n)
	}
	got := map[string]bool{}
	for _, tgt := range s.cg.targets["node-a"] {
		got[tgt.component+"/"+tgt.container+"@"+tgt.pod] = true
	}
	for _, want := range []string{"cgreader/cgreader@reader-a", "cgreader/POD@reader-a", "gateway/envoy@gw", "gateway/ext-proc@gw", "gateway/POD@gw"} {
		if !got[want] {
			t.Errorf("node-a targets %v lack %s", got, want)
		}
	}
	if got["gateway/sidecar@gw"] || len(s.cg.targets) != 1 {
		t.Errorf("targets = %v, want no unlisted container and nothing for node-b, which has no reader", s.cg.targets)
	}
	requested := map[string]string{}
	for _, r := range s.rep.Cgreader.Requested {
		requested[r.Component+"/"+r.Container+"@"+r.Pod] = r.Node + ":" + r.Path
	}
	if len(requested) != len(got) || requested["gateway/envoy@gw"] != "node-a:"+cgroupPath(gwPod(k8s, t), "containerd://envoy-id") {
		t.Errorf("requested = %v, want every target recorded with its node and path", requested)
	}

	empty := newResourceSampler(newFakeGetter(), fake.NewSimpleClientset(), time.Second)
	if err := empty.resolveCgreader(t.Context()); err == nil {
		t.Errorf("resolveCgreader succeeded with no reader pods")
	}
}

func gwPod(k8s *fake.Clientset, t *testing.T) *corev1.Pod {
	t.Helper()
	p, err := k8s.CoreV1().Pods("ate-system").Get(t.Context(), "gw", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// A requested container cgroup that produced no rows fails: the driver's
// path did not match the node's layout, and nothing else would say so.
func TestCheckCgreaderTargets(t *testing.T) {
	t.Parallel()
	cg := &cgreaderReport{
		Requested: []cgRequest{
			{Node: "node-a", Path: "kubepods.slice/a/cri-containerd-1.scope", Component: "gateway", Pod: "gw", Container: "envoy"},
			{Node: "node-a", Path: "kubepods.slice/a/cri-containerd-2.scope", Component: "gateway", Pod: "gw", Container: "ext-proc"},
			{Node: "node-a", Path: "kubepods.slice/a", Component: "gateway", Pod: "gw", Container: podContainer},
			{Node: "node-a", Path: "kubepods.slice/r/cri-containerd-3.scope", Component: "cgreader", Pod: "r1", Container: cgreaderContainer},
		},
		Samples: append(cgRows("node-a", "gateway", "gw", "envoy", "", 0.5, 0, 9), cgRows("node-a", "gateway", "gw", "ext-proc", "", 0.1, 0, 0)...),
	}
	got := map[string]bool{}
	for _, v := range checkCgreaderTargets(cg) {
		got[v.Scope] = v.Pass
	}
	want := map[string]bool{"cgreader gateway/envoy@gw": true, "cgreader gateway/ext-proc@gw": false}
	if !maps.Equal(got, want) {
		t.Errorf("checkCgreaderTargets = %v, want %v: pod cgroups and the reader itself are not judged here", got, want)
	}
}

// cgRows builds one series' rows every second from 0 to n-1 s.
func cgRows(node, component, pod, container, leaf string, cores float64, from, to int) []cgSample {
	var out []cgSample
	for i := from; i <= to; i++ {
		out = append(out, cgSample{T: time.Unix(int64(1000+i), 0), Node: node, Component: component, Pod: pod, Container: container,
			Leaf: leaf, CPUSeconds: 100 + cores*float64(i)})
	}
	return out
}

func TestCheckLeafSums(t *testing.T) {
	t.Parallel()
	row := func(i int, leaf string, cpu float64, gone bool) cgSample {
		return cgSample{T: time.Unix(int64(1000+i), 0), Node: "n", Component: "workers", Pod: "w", Container: "ateom", Leaf: leaf, CPUSeconds: cpu, Gone: gone}
	}
	// The shape of a resumed worker: an idle old actor, the ateom leaf at
	// 0.01 core, and actors whose sandboxes (a _pause and an actor cgroup
	// each) appear at 5 s already holding restore CPU and are removed at
	// 30 s. The container also holds each removed actor's last, unread
	// 0.110 s of suspend work.
	shape := func(actors int, extraPerRound float64) []cgSample {
		var out []cgSample
		container, atunnel := 100.0, 10.0
		use := make([]float64, actors)
		for i := 0; i <= 40; i++ {
			if i > 0 {
				atunnel += 0.01
				container += 0.01 + extraPerRound
			}
			for a := range use {
				switch {
				case i == 5:
					use[a] = 0.145
					container += 0.145
				case i > 5 && i < 30:
					use[a] += 0.025
					container += 0.025
				case i == 30:
					container += 0.110
				}
			}
			out = append(out, row(i, "", container, false), row(i, "ateom", atunnel, false), row(i, "old-_pause", 8.9, false))
			for a, u := range use {
				uid := fmt.Sprintf("a%d", a)
				if i >= 5 && i <= 30 {
					out = append(out, row(i, uid+"-_pause", u, i == 30), row(i, uid+"-actor", 0, i == 30))
				}
			}
		}
		return out
	}
	for _, tc := range []struct {
		name     string
		rows     []cgSample
		wantPass bool
		wantGot  string
		wantInfo bool
	}{
		{"one actor removed: its last use is inferred", shape(1, 0), true, "0.110s inferred for removed actors", false},
		{"two actors removed in one round: left out", shape(2, 0), true, "over 39 rounds", true},
		{"container charges outside the leaves", shape(1, 0.002), false, "inferred", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inferRemovedLeaves(tc.rows)
			got := checkLeafSums(tc.rows)
			if len(got) == 0 || got[0].Pass != tc.wantPass || !strings.Contains(got[0].Got, tc.wantGot) {
				t.Fatalf("checkLeafSums = %+v, want pass %v and %q", got, tc.wantPass, tc.wantGot)
			}
			if hasInfo := len(got) == 2 && got[1].Info; hasInfo != tc.wantInfo {
				t.Errorf("checkLeafSums = %+v, want an INFO line for unsplit removals: %v", got, tc.wantInfo)
			}
		})
	}

	// The inferred counter lands on the _pause row, not the actor row.
	rows := shape(1, 0)
	inferRemovedLeaves(rows)
	for _, r := range rows {
		switch {
		case r.Leaf == "a0-_pause" && r.Gone:
			if r.InferredCPUSeconds == nil || math.Abs(*r.InferredCPUSeconds-(r.CPUSeconds+0.110)) > 1e-9 {
				t.Errorf("gone _pause row inferred %v from last read %v, want last read + 0.110", r.InferredCPUSeconds, r.CPUSeconds)
			}
		case r.InferredCPUSeconds != nil:
			t.Errorf("row %s inferred %v, want only the gone _pause row", r.Leaf, *r.InferredCPUSeconds)
		}
	}
}

func TestCheckCgreaderVsCadvisor(t *testing.T) {
	t.Parallel()
	reader := cgRows("n", "gateway", "gw", "envoy", "", 1, 0, 600)
	for _, tc := range []struct {
		name  string
		cores float64
		want  bool
	}{{"agree", 1, true}, {"cAdvisor double", 2, false}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cad []cadvisorSample
			for i := 3; i <= 590; i += 16 {
				cad = append(cad, cadvisorSample{T: time.Unix(int64(1000+i), 0), Component: "gateway", Pod: "gw", Container: "envoy", CPUSeconds: 100 + tc.cores*float64(i)})
			}
			got := checkCgreaderVsCadvisor(reader, cad)
			if len(got) != 1 || got[0].Pass != tc.want {
				t.Errorf("checkCgreaderVsCadvisor = %+v, want pass %v", got, tc.want)
			}
		})
	}
}

func TestCheckCgreaderNodes(t *testing.T) {
	t.Parallel()
	offsets := func(ds ...time.Duration) []clockOffset {
		var out []clockOffset
		for _, d := range ds {
			out = append(out, clockOffset{Offset: d})
		}
		return out
	}
	cg := &cgreaderReport{Nodes: map[string]*cgNode{
		"a-ok":     {Offsets: offsets(10*time.Millisecond, 20*time.Millisecond)},
		"b-lost":   {Lost: 3, Offsets: offsets(0)},
		"c-skewed": {Offsets: offsets(800*time.Millisecond, 810*time.Millisecond)},
		"d-step":   {Offsets: offsets(0, 2*time.Second)},
	}}
	got := map[string]verifyResult{}
	for _, r := range checkCgreaderNodes(cg) {
		got[r.Check+" "+r.Scope] = r
	}
	for _, tc := range []struct {
		key        string
		pass, info bool
	}{
		{"coverage cgreader ring a-ok", true, false},
		{"coverage cgreader ring b-lost", false, false},
		{"clocks a-ok", true, false},
		{"clocks c-skewed", true, true},
		{"clocks d-step", false, false},
	} {
		if r := got[tc.key]; r.Pass != tc.pass || r.Info != tc.info {
			t.Errorf("%s = %+v, want pass %v info %v", tc.key, r, tc.pass, tc.info)
		}
	}
}

func TestCheckReaderOverheadAndGaps(t *testing.T) {
	t.Parallel()
	busy := cgRows("node-a", "cgreader", "r", cgreaderContainer, "", 0.05, 0, 60)
	idle := cgRows("node-b", "cgreader", "r2", cgreaderContainer, "", 0.004, 0, 60)
	// node-d restarted: its counter fell back to zero halfway.
	restarted := cgRows("node-d", "cgreader", "r4", cgreaderContainer, "", 0.004, 0, 60)
	for i := range restarted[30:] {
		restarted[30+i].CPUSeconds -= 100
	}
	cg := &cgreaderReport{
		Nodes:   map[string]*cgNode{"node-a": {}, "node-b": {}, "node-c": {}, "node-d": {}},
		Samples: append(append(busy, idle...), restarted...),
	}
	got := map[string]verifyResult{}
	for _, r := range checkReaderOverhead(cg) {
		got[r.Scope] = r
	}
	if got["cgreader node-a"].Pass || !got["cgreader node-b"].Pass || !got["cgreader node-d"].Pass {
		t.Errorf("checkReaderOverhead = %+v, want node-a over, node-b and the restarted node-d under", got)
	}
	if r, ok := got["cgreader node-c"]; !ok || r.Pass || r.Info {
		t.Errorf("node-c with no self readings = %+v, want a failure", r)
	}
	gappy := cgRows("n", "gateway", "gw", "envoy", "", 1, 0, 10)
	gappy = append(gappy[:4], gappy[8:]...)
	if r := checkCgreaderGaps(gappy); len(r) != 1 || r[0].Pass {
		t.Errorf("checkCgreaderGaps = %+v, want a 4 s gap to fail", r)
	}
}

func TestCgreaderAsCadvisor(t *testing.T) {
	t.Parallel()
	cg := &cgreaderReport{Nodes: map[string]*cgNode{"n": {Offsets: []clockOffset{{Offset: 2 * time.Second}}}}}
	cg.Samples = append(cg.Samples, cgRows("n", "workers", "w", "ateom", "", 0.6, 0, 1)...)
	cg.Samples = append(cg.Samples, cgRows("n", "workers", "w", "ateom", "ateom", 0.1, 0, 1)...)
	cg.Samples = append(cg.Samples, cgRows("n", "workers", "w", "ateom", "a1-_pause", 0.2, 0, 1)...)
	cg.Samples = append(cg.Samples, cgRows("n", "workers", "w", "ateom", "a2-_pause", 0.3, 0, 1)...)
	cg.Samples = append(cg.Samples, cgRows("n", "cgreader", "r", "cgreader", "", 0.01, 0, 1)...)
	// The worker's pod cgroup, whose leaves are its container scopes.
	cg.Samples = append(cg.Samples, cgRows("n", "workers", "w", "POD", "", 0.7, 0, 1)...)
	cg.Samples = append(cg.Samples, cgRows("n", "workers", "w", "POD", "cri-containerd-x.scope", 0.6, 0, 1)...)
	containers, split := cgreaderAsCadvisor(cg)
	if len(containers) != 4 || !containers[0].T.Equal(time.Unix(998, 0)) {
		t.Errorf("containers = %+v, want the ateom rows shifted 2 s onto the driver clock", containers)
	}
	parts := map[string]float64{}
	seen := map[string]int{}
	for _, s := range split {
		seen[s.Container+"@"+s.T.String()]++
		if seen[s.Container+"@"+s.T.String()] > 1 {
			t.Errorf("two %s rows at %v: the pod cgroup's leaves must not add an actors series", s.Container, s.T)
		}
		if s.T.Equal(time.Unix(999, 0)) {
			parts[s.Container] = s.CPUSeconds
		}
	}
	if math.Abs(parts["atunnel"]-100.1) > 1e-9 || math.Abs(parts["actors"]-0.5) > 1e-9 {
		t.Errorf("split at the second read = %v, want atunnel 100.1 and actors 0.2+0.3 since the first read", parts)
	}
}

func TestOffsetIntervals(t *testing.T) {
	t.Parallel()
	ms := time.Millisecond
	// smoke-cgreader-2: the first pull took 2.85 s and read −1.19 s; the
	// other twelve took about 100 ms and read about 90 ms.
	slowFirst := []clockOffset{{Offset: -1190 * ms, RTT: 2850 * ms}}
	for i := range 12 {
		slowFirst = append(slowFirst, clockOffset{Offset: time.Duration(85+i) * ms, RTT: time.Duration(95+i) * ms})
	}
	for _, tc := range []struct {
		name       string
		offsets    []clockOffset
		wantPass   bool
		wantMedian time.Duration
	}{
		{"one slow pull among fast ones", slowFirst, true, 91 * ms},
		{"intervals 1.5 s apart", []clockOffset{{Offset: 0, RTT: 100 * ms}, {Offset: 1600 * ms, RTT: 100 * ms}}, false, 1600 * ms},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := &cgNode{Offsets: tc.offsets}
			if got := medianOffset(n); got != tc.wantMedian {
				t.Errorf("medianOffset = %v, want %v", got, tc.wantMedian)
			}
			got := checkCgreaderNodes(&cgreaderReport{Nodes: map[string]*cgNode{"n": n}})
			var clocks verifyResult
			for _, r := range got {
				if r.Check == "clocks" {
					clocks = r
				}
			}
			if clocks.Pass != tc.wantPass {
				t.Errorf("clocks = %+v, want pass %v", clocks, tc.wantPass)
			}
		})
	}
}
