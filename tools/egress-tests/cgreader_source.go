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
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/cgapi"
)

// The cgroup reader DaemonSet that deploy.sh --cgreader installs.
const (
	cgreaderNamespace = "egress-tests"
	cgreaderSelector  = "app=egress-tests-cgreader"
	cgreaderContainer = "cgreader"
	// cgreaderPullInterval paces the pulls; the readers sample every second
	// on their own clock.
	cgreaderPullInterval = 5 * time.Second
)

// cgTarget is one cgroup a reader samples for the driver.
type cgTarget struct {
	component, pod, container string
}

// cgSample is one cgroup row from a reader, on the node's clock.
type cgSample struct {
	T             time.Time `json:"t"`
	UptimeSeconds float64   `json:"uptimeSeconds"`
	Node          string    `json:"node"`
	Seq           uint64    `json:"seq"`
	Component     string    `json:"component"`
	Pod           string    `json:"pod"`
	Container     string    `json:"container"`
	Leaf          string    `json:"leaf,omitempty"`
	Gone          bool      `json:"gone,omitempty"`
	CPUSeconds    float64   `json:"cpuSeconds"`
	// InferredCPUSeconds, on a gone row, is the leaf's final counter: its
	// last read plus what its container used beyond the other leaves in the
	// round it disappeared. It is derived, not read.
	InferredCPUSeconds *float64 `json:"inferredCpuSeconds,omitempty"`
	CFSPeriods         float64  `json:"cfsPeriods,omitempty"`
	CFSThrottled       float64  `json:"cfsThrottled,omitempty"`
	CPUSomeSeconds     float64  `json:"cpuSomeSeconds,omitempty"`
	WorkingSetBytes    float64  `json:"workingSetBytes"`
}

func (s cgSample) key() string {
	k := s.Component + "/" + s.Container + "@" + s.Pod
	if s.Leaf != "" {
		k += "/" + s.Leaf
	}
	return k
}

// cgNode is what the driver learned about one node's reader.
type cgNode struct {
	Reader string `json:"reader"`
	// Lost counts rows the reader dropped before the driver pulled them.
	Lost int `json:"lost"`
	// Restarts counts reader restarts seen during the run.
	Restarts int `json:"restarts"`
	// Offsets holds, per pull, the node clock minus the driver clock.
	Offsets []clockOffset `json:"offsets"`
}

type clockOffset struct {
	T      time.Time     `json:"t"`
	Offset time.Duration `json:"offset"`
	RTT    time.Duration `json:"rtt"`
}

// cgreaderReport is the cgroup reader section of the resources report.
type cgreaderReport struct {
	Nodes   map[string]*cgNode `json:"nodes"`
	Samples []cgSample         `json:"samples,omitempty"`
	// Requested lists the cgroups the driver asked each reader for, so a
	// path that never produced a row can be reported.
	Requested []cgRequest `json:"requested,omitempty"`
}

// cgRequest is one cgroup the driver asked a reader to sample.
type cgRequest struct {
	Node      string `json:"node"`
	Path      string `json:"path"`
	Component string `json:"component"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
}

// cgreaderState is the sampler's view of the readers.
type cgreaderState struct {
	targets map[string]map[string]cgTarget // node -> cgroup path -> target
	lastSeq map[string]uint64
	started map[string]bool  // the first pull, from the newest row, is done
	boot    map[string]int64 // the reader process's start time
}

// cgroupPath is the cgroup v2 path, below the root, of a pod's cgroup or of
// one container's, under the kubelet's systemd driver.
func cgroupPath(p *corev1.Pod, containerID string) string {
	uid := strings.ReplaceAll(string(p.UID), "-", "_")
	var slice string
	switch p.Status.QOSClass {
	case corev1.PodQOSGuaranteed:
		slice = "kubepods.slice/kubepods-pod" + uid + ".slice"
	case corev1.PodQOSBestEffort:
		slice = "kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod" + uid + ".slice"
	default:
		slice = "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod" + uid + ".slice"
	}
	if containerID == "" {
		return slice
	}
	_, id, _ := strings.Cut(containerID, "://")
	return slice + "/cri-containerd-" + id + ".scope"
}

// podTargets returns the cgroups to read for one pod: the pod cgroup, as
// container POD, and each wanted container's.
func podTargets(p *corev1.Pod, component string, containers []string) map[string]cgTarget {
	out := map[string]cgTarget{cgroupPath(p, ""): {component, p.Name, podContainer}}
	for _, cs := range p.Status.ContainerStatuses {
		if slices.Contains(containers, cs.Name) && cs.ContainerID != "" {
			out[cgroupPath(p, cs.ContainerID)] = cgTarget{component, p.Name, cs.Name}
		}
	}
	return out
}

// resolveCgreader maps every target pod's cgroups to the reader on its node,
// and adds each reader's own container to measure its overhead.
func (s *resourceSampler) resolveCgreader(ctx context.Context) error {
	readers, err := s.k8s.CoreV1().Pods(cgreaderNamespace).List(ctx, metav1.ListOptions{LabelSelector: cgreaderSelector})
	if err != nil {
		return fmt.Errorf("listing cgroup readers: %w", err)
	}
	s.cg = &cgreaderState{targets: map[string]map[string]cgTarget{}, lastSeq: map[string]uint64{},
		started: map[string]bool{}, boot: map[string]int64{}}
	s.rep.Cgreader = &cgreaderReport{Nodes: map[string]*cgNode{}}
	for i := range readers.Items {
		p := &readers.Items[i]
		if p.Status.Phase != corev1.PodRunning || p.Spec.NodeName == "" {
			continue
		}
		s.rep.Cgreader.Nodes[p.Spec.NodeName] = &cgNode{Reader: p.Name}
		s.cg.targets[p.Spec.NodeName] = podTargets(p, "cgreader", []string{cgreaderContainer})
	}
	// The first request on the sampler's client also sets up its
	// connection; keep that out of the first timed pull's round trip.
	for node, n := range s.rep.Cgreader.Nodes {
		if _, err := s.get.GetRaw(ctx, podProxyPath(cgreaderNamespace, n.Reader, cgapi.Port, "/healthz"), nil); err != nil {
			delete(s.rep.Cgreader.Nodes, node)
			delete(s.cg.targets, node)
			s.fail(ctx, "cgreader", node)
		}
	}
	if len(s.rep.Cgreader.Nodes) == 0 {
		return fmt.Errorf("--usage-cgroup-reader: no Running pod matches %s in %s; deploy it with tools/egress-tests/deploy.sh --deploy --cgreader", cgreaderSelector, cgreaderNamespace)
	}
	for _, t := range cadvisorTargets {
		list, err := s.k8s.CoreV1().Pods(t.namespace).List(ctx, metav1.ListOptions{LabelSelector: t.selector})
		if err != nil {
			return fmt.Errorf("listing pods %s in %s: %w", t.selector, t.namespace, err)
		}
		for i := range list.Items {
			p := &list.Items[i]
			targets, ok := s.cg.targets[p.Spec.NodeName]
			if p.Status.Phase != corev1.PodRunning || !ok {
				continue
			}
			for path, tgt := range podTargets(p, t.component, t.containers) {
				targets[path] = tgt
			}
		}
	}
	for _, node := range slices.Sorted(maps.Keys(s.cg.targets)) {
		for _, path := range slices.Sorted(maps.Keys(s.cg.targets[node])) {
			tgt := s.cg.targets[node][path]
			s.rep.Cgreader.Requested = append(s.rep.Cgreader.Requested, cgRequest{Node: node, Path: path, Component: tgt.component, Pod: tgt.pod, Container: tgt.container})
		}
	}
	return nil
}

// pollCgreader pulls every reader's new rows.
func (s *resourceSampler) pollCgreader(ctx context.Context) {
	var wg sync.WaitGroup
	for node := range s.cg.targets {
		wg.Go(func() { s.pullReader(ctx, node) })
	}
	wg.Wait()
}

// pullReader pulls a reader's new rows, page by page until it has caught
// up. The first pull starts from the newest row, so rows left from earlier
// runs are skipped. A reader that restarted is counted, read again from its
// start, and its own container, which has a new ID, is looked up again.
func (s *resourceSampler) pullReader(ctx context.Context, node string) {
	measured := false
	for range maxPagesPerPull {
		s.mu.Lock()
		st := s.cg
		started, since, boot := st.started[node], st.lastSeq[node], st.boot[node]
		reader := s.rep.Cgreader.Nodes[node].Reader
		targets := maps.Clone(st.targets[node])
		s.mu.Unlock()

		params := url.Values{"since": {cgapi.SinceLatest}}
		if started {
			params.Set("since", strconv.FormatUint(since, 10))
		}
		for _, path := range slices.Sorted(maps.Keys(targets)) {
			params.Add("target", path)
		}
		sent := s.now()
		body, err := s.get.GetRaw(ctx, podProxyPath(cgreaderNamespace, reader, cgapi.Port, cgapi.SamplesRoute), params)
		received := s.now()
		var resp cgapi.Response
		if err == nil {
			err = json.Unmarshal(body, &resp)
		}
		if err != nil {
			s.fail(ctx, "cgreader", node)
			return
		}

		s.mu.Lock()
		n := s.rep.Cgreader.Nodes[node]
		s.rep.Reads[sourceKey("cgreader", node)]++ // one per answered pull, pages included
		if !measured {
			mid := sent.Add(received.Sub(sent) / 2)
			n.Offsets = append(n.Offsets, clockOffset{T: mid, Offset: time.Unix(0, resp.WallNanos).Sub(mid), RTT: received.Sub(sent)})
			measured = true
		}
		restarted := started && (resp.Boot != boot || resp.Latest < since)
		switch {
		case !started:
			st.started[node], st.lastSeq[node], st.boot[node] = true, resp.Latest, resp.Boot
			s.mu.Unlock()
			return
		case restarted:
			n.Restarts++
			st.lastSeq[node], st.boot[node] = 0, resp.Boot
			s.mu.Unlock()
			s.reresolveSelf(ctx, node)
			continue
		}
		if resp.Oldest > since+1 {
			n.Lost += int(resp.Oldest - since - 1)
			since = resp.Oldest - 1
		}
		for _, row := range resp.Rows {
			if row.Seq > since+1 {
				n.Lost += int(row.Seq - since - 1)
			}
			since = max(since, row.Seq)
			tgt, ok := targets[row.Target]
			if !ok {
				continue
			}
			s.rep.Cgreader.Samples = append(s.rep.Cgreader.Samples, cgSample{
				T: time.Unix(0, row.WallNanos), UptimeSeconds: row.UptimeSeconds, Node: node, Seq: row.Seq,
				Component: tgt.component, Pod: tgt.pod, Container: tgt.container, Leaf: row.Leaf, Gone: row.Gone,
				CPUSeconds: float64(row.UsageUsec) / 1e6, CFSPeriods: float64(row.NrPeriods), CFSThrottled: float64(row.NrThrottled),
				CPUSomeSeconds: float64(row.CPUSomeUsec) / 1e6, WorkingSetBytes: float64(row.WorkingSetBytes),
			})
		}
		st.lastSeq[node] = since
		s.mu.Unlock()
		if !resp.More {
			return
		}
	}
}

// maxPagesPerPull bounds one pull; the next pull continues where it ended.
const maxPagesPerPull = 20

// reresolveSelf looks up a restarted reader's container again, so its own
// usage keeps being read.
func (s *resourceSampler) reresolveSelf(ctx context.Context, node string) {
	s.mu.Lock()
	name := s.rep.Cgreader.Nodes[node].Reader
	s.mu.Unlock()
	p, err := s.k8s.CoreV1().Pods(cgreaderNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		s.fail(ctx, "cgreader", node)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	targets := s.cg.targets[node]
	maps.DeleteFunc(targets, func(_ string, t cgTarget) bool { return t.component == "cgreader" })
	maps.Copy(targets, podTargets(p, "cgreader", []string{cgreaderContainer}))
}
