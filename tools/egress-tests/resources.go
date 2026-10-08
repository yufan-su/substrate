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
	"fmt"
	"net/url"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// rawGetter fetches a raw API server path, such as a pod or node proxy URL.
// It is the seam the resource tests replace.
type rawGetter interface {
	GetRaw(ctx context.Context, path string, params url.Values) ([]byte, error)
}

type restRawGetter struct{ c rest.Interface }

func (g restRawGetter) GetRaw(ctx context.Context, path string, params url.Values) ([]byte, error) {
	req := g.c.Get().AbsPath(path)
	for k, vs := range params {
		for _, v := range vs {
			req = req.Param(k, v)
		}
	}
	return req.DoRaw(ctx)
}

func podProxyPath(ns, pod string, port int, path string) string {
	return fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%d/proxy%s", ns, pod, port, path)
}

// liveTarget is a Go component whose process counters are read on each poll.
type liveTarget struct {
	component, namespace, selector, container string
	port                                      int
}

var liveTargets = []liveTarget{
	{component: "gateway", namespace: "ate-system", selector: "app=atenet-egress", container: "ext-proc", port: 9090},
	{component: "ateapi", namespace: "ate-system", selector: "app=ate-api-server", container: "ate-api-server", port: 9090},
}

// envoyTarget is the gateway Envoy whose stats are read on each poll, through
// its envoy_metrics listener. The admin API itself listens on loopback only,
// which the API server's pod proxy cannot reach.
var envoyTarget = struct {
	namespace, selector string
	port                int
	path                string
}{"ate-system", "app=atenet-egress", 15090, "/stats/prometheus"}

// resourceReport is the resources section of the report.
type resourceReport struct {
	Sources []sourceInfo   `json:"sources"`
	Live    []liveSample   `json:"live,omitempty"`
	Envoy   []envoySample  `json:"envoy,omitempty"`
	Driver  driverUsage    `json:"driver"`
	Errors  map[string]int `json:"errors,omitempty"`
	Verify  []verifyResult `json:"verify,omitempty"`
}

// sourceInfo says how often a source is read and whose clock stamps it.
type sourceInfo struct {
	Name     string        `json:"name"`
	Interval time.Duration `json:"interval"`
	Clock    string        `json:"clock"`
}

// liveSample is one read of a Go process's counters. Label names the phase
// mark that forced the read, if any.
type liveSample struct {
	T                 time.Time `json:"t"`
	Label             string    `json:"label,omitempty"`
	Component         string    `json:"component"`
	Pod               string    `json:"pod"`
	Container         string    `json:"container"`
	ProcessCPUSeconds float64   `json:"processCpuSeconds"`
	RSSBytes          float64   `json:"rssBytes"`
}

type envoySample struct {
	T        time.Time          `json:"t"`
	Label    string             `json:"label,omitempty"`
	Pod      string             `json:"pod"`
	Counters map[string]float64 `json:"counters"`
}

// driverUsage is the driver's own CPU, from getrusage, over the whole run and
// over the steady window.
type driverUsage struct {
	WholeRun usageWindow `json:"wholeRun"`
	Steady   usageWindow `json:"steady"`
}

type usageWindow struct {
	CPUSeconds  float64 `json:"cpuSeconds"`
	WallSeconds float64 `json:"wallSeconds"`
	Cores       float64 `json:"cores"`
}

// rusageMark is the driver's CPU time at one moment.
type rusageMark struct {
	at  time.Time
	cpu float64
}

func selfCPU() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

func usageBetween(a, b rusageMark) usageWindow {
	w := usageWindow{CPUSeconds: b.cpu - a.cpu, WallSeconds: b.at.Sub(a.at).Seconds()}
	if w.WallSeconds > 0 {
		w.Cores = w.CPUSeconds / w.WallSeconds
	}
	return w
}

// resourceSampler reads the CPU and memory of the components on the egress
// path while a run goes on. Each source polls on its own cadence; phase
// marks force an extra read of every source so windows line up with them.
type resourceSampler struct {
	get      rawGetter
	k8s      kubernetes.Interface
	interval time.Duration
	now      func() time.Time

	mu      sync.Mutex
	rep     resourceReport
	live    map[liveTarget][]string // target -> pod names
	envoys  []string
	rusage  map[string]rusageMark
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped bool
}

func newResourceSampler(get rawGetter, k8s kubernetes.Interface, interval time.Duration) *resourceSampler {
	return &resourceSampler{
		get:      get,
		k8s:      k8s,
		interval: interval,
		now:      time.Now,
		rusage:   map[string]rusageMark{},
		rep:      resourceReport{Errors: map[string]int{}},
	}
}

// start finds the target pods and begins polling. Polling runs until stop,
// independent of ctx's cancellation, so cleanup after an interrupt is still
// measured.
func (s *resourceSampler) start(ctx context.Context) error {
	s.live = map[liveTarget][]string{}
	for _, t := range liveTargets {
		pods, err := runningPods(ctx, s.k8s, t.namespace, t.selector)
		if err != nil {
			return err
		}
		s.live[t] = pods
	}
	envoys, err := runningPods(ctx, s.k8s, envoyTarget.namespace, envoyTarget.selector)
	if err != nil {
		return err
	}
	s.envoys = envoys
	s.rep.Sources = append(s.rep.Sources,
		sourceInfo{Name: "live", Interval: s.interval, Clock: "driver"},
		sourceInfo{Name: "envoy", Interval: s.interval, Clock: "driver"})
	s.markRusage("run:begin")

	pollCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.wg.Go(func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-pollCtx.Done():
				return
			case <-ticker.C:
				s.pollLive(pollCtx, "")
			}
		}
	})
	return nil
}

// mark records a phase boundary: it reads every source once, labeled, and
// notes the driver's CPU time.
func (s *resourceSampler) mark(ctx context.Context, label string) {
	s.markRusage(label)
	s.pollLive(context.WithoutCancel(ctx), label)
}

func (s *resourceSampler) markRusage(label string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rusage[label] = rusageMark{at: s.now(), cpu: selfCPU()}
}

// stop ends polling and returns the collected section. It is safe to call
// more than once.
func (s *resourceSampler) stop() *resourceReport {
	s.mu.Lock()
	stopped := s.stopped
	s.stopped = true
	s.mu.Unlock()
	if !stopped {
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
		s.markRusage("run:end")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.Driver.WholeRun = usageBetween(s.rusage["run:begin"], s.rusage["run:end"])
	if a, ok := s.rusage[markSteadyBegin]; ok {
		if b, ok := s.rusage[markSteadyEnd]; ok {
			s.rep.Driver.Steady = usageBetween(a, b)
		}
	}
	rep := s.rep
	return &rep
}

// pollLive reads every Go process and Envoy once, in parallel.
func (s *resourceSampler) pollLive(ctx context.Context, label string) {
	var wg sync.WaitGroup
	for t, pods := range s.live {
		for _, pod := range pods {
			wg.Go(func() { s.readProcess(ctx, t, pod, label) })
		}
	}
	for _, pod := range s.envoys {
		wg.Go(func() { s.readEnvoy(ctx, pod, label) })
	}
	wg.Wait()
}

func (s *resourceSampler) readProcess(ctx context.Context, t liveTarget, pod, label string) {
	body, err := s.get.GetRaw(ctx, podProxyPath(t.namespace, pod, t.port, "/metrics"), nil)
	at := s.now()
	if err == nil {
		var cpu, rss float64
		if cpu, rss, err = parseProcessMetrics(body); err == nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.rep.Live = append(s.rep.Live, liveSample{T: at, Label: label, Component: t.component, Pod: pod,
				Container: t.container, ProcessCPUSeconds: cpu, RSSBytes: rss})
			return
		}
	}
	s.fail("live")
}

func (s *resourceSampler) readEnvoy(ctx context.Context, pod, label string) {
	body, err := s.get.GetRaw(ctx, podProxyPath(envoyTarget.namespace, pod, envoyTarget.port, envoyTarget.path),
		url.Values{"filter": {envoyStatsFilter}})
	at := s.now()
	var counters map[string]float64
	if err == nil {
		counters = parseEnvoyStats(body)
	}
	// Envoy serves every stat of a cluster from the start, zeros included, so
	// a body missing one means a renamed metric or cluster, not an idle
	// gateway. Such a read is a failure, or the gaps would pass unnoticed.
	if err != nil || len(counters) < len(envoyClusters)*len(envoyStats) {
		s.fail("envoy")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.Envoy = append(s.rep.Envoy, envoySample{T: at, Label: label, Pod: pod, Counters: counters})
}

func (s *resourceSampler) fail(source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.Errors[source]++
}

// runningPods lists the names of the Running pods that match selector.
func runningPods(ctx context.Context, k8s kubernetes.Interface, ns, selector string) ([]string, error) {
	list, err := k8s.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("listing pods %s in %s: %w", selector, ns, err)
	}
	var names []string
	for _, p := range list.Items {
		if p.Status.Phase == corev1.PodRunning {
			names = append(names, p.Name)
		}
	}
	return names, nil
}
