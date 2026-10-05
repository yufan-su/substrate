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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

// fakeAPI is an in-memory ateapi holding just enough state for the driver.
// Methods the driver does not call panic through the nil embedded interface.
type fakeAPI struct {
	ateapipb.ControlClient

	mu       sync.Mutex
	actors   map[string]string // name -> state: "suspended" or "running"
	policies map[string]*ateapipb.EgressPolicy
	calls    map[string][]string // RPC -> actor names, in call order
	// resumeErrs are returned by ResumeActor, one per call, before it succeeds.
	resumeErrs []error
	nextUID    int
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		actors:   make(map[string]string),
		policies: make(map[string]*ateapipb.EgressPolicy),
		calls:    make(map[string][]string),
	}
}

func (f *fakeAPI) record(rpc, name string) {
	f.calls[rpc] = append(f.calls[rpc], name)
}

func (f *fakeAPI) callCount(rpc string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls[rpc])
}

func (f *fakeAPI) callNames(rpc string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(slices.Values(f.calls[rpc]))
}

func (f *fakeAPI) isRunning(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actors[name] == "running"
}

func (f *fakeAPI) CreateActor(_ context.Context, in *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetMetadata().GetName()
	f.record("CreateActor", name)
	if _, ok := f.actors[name]; ok {
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	f.actors[name] = "suspended"
	return in.GetActor(), nil
}

func (f *fakeAPI) CreateActorEgressPolicy(_ context.Context, in *ateapipb.CreateActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("CreateActorEgressPolicy", name)
	if _, ok := f.policies[name]; ok {
		return nil, status.Error(codes.AlreadyExists, "policy exists")
	}
	f.nextUID++
	p := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: in.GetActor().GetAtespace(), Name: "default", Uid: fmt.Sprint(f.nextUID)},
		Rules:    in.GetEgressPolicy().GetRules(),
	}
	f.policies[name] = p
	return p, nil
}

func (f *fakeAPI) GetActorEgressPolicy(_ context.Context, in *ateapipb.GetActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("GetActorEgressPolicy", in.GetActor().GetName())
	p, ok := f.policies[in.GetActor().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no policy")
	}
	return p, nil
}

func (f *fakeAPI) UpdateActorEgressPolicy(_ context.Context, in *ateapipb.UpdateActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("UpdateActorEgressPolicy", name)
	cur, ok := f.policies[name]
	if !ok {
		return nil, status.Error(codes.NotFound, "no policy")
	}
	if in.GetEgressPolicy().GetMetadata().GetUid() != cur.GetMetadata().GetUid() {
		return nil, status.Error(codes.FailedPrecondition, "uid precondition failed")
	}
	f.policies[name] = in.GetEgressPolicy()
	return in.GetEgressPolicy(), nil
}

func (f *fakeAPI) ResumeActor(_ context.Context, in *ateapipb.ResumeActorRequest, _ ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("ResumeActor", name)
	if len(f.resumeErrs) > 0 {
		err := f.resumeErrs[0]
		f.resumeErrs = f.resumeErrs[1:]
		return nil, err
	}
	if _, ok := f.actors[name]; !ok {
		return nil, status.Error(codes.NotFound, "no actor")
	}
	f.actors[name] = "running"
	return &ateapipb.ResumeActorResponse{Resumed: true}, nil
}

func (f *fakeAPI) SuspendActor(_ context.Context, in *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("SuspendActor", name)
	f.actors[name] = "suspended"
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeAPI) DeleteActor(_ context.Context, in *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	if !in.GetAnyState() {
		return nil, status.Error(codes.FailedPrecondition, "test expects AnyState deletes")
	}
	f.record("DeleteActor", name)
	if _, ok := f.actors[name]; !ok {
		return nil, status.Error(codes.NotFound, "no actor")
	}
	delete(f.actors, name)
	delete(f.policies, name)
	return &ateapipb.Actor{}, nil
}

// fakeRouter stands in for the atenet router and the actors behind it: it
// answers the egressapi routes for running actors of fakeAPI.
type fakeRouter struct {
	t   *testing.T
	api *fakeAPI

	mu      sync.Mutex
	starts  map[string]egressapi.StartRequest
	running map[string]bool // actor -> loop running
	stops   []string
	// startFault makes /start fail for an actor: "lost-answer" starts the
	// loop but answers 502, "refuse" answers 500 without starting it.
	startFault map[string]string
}

func newFakeRouter(t *testing.T, api *fakeAPI) (*fakeRouter, *httptest.Server) {
	fr := &fakeRouter{t: t, api: api, starts: make(map[string]egressapi.StartRequest), running: make(map[string]bool), startFault: make(map[string]string)}
	srv := httptest.NewServer(fr)
	t.Cleanup(srv.Close)
	return fr, srv
}

// loopStats is what every fake actor reports: 100 requests to endpoint 0,
// one of which timed out.
func loopStats() *egressapi.Stats {
	s := &egressapi.Stats{
		Elapsed:   time.Second,
		Requests:  100,
		Successes: 99,
		NewConns:  1,
		Errors:    map[string]int64{"timeout": 1},
		Endpoints: []egressapi.Endpoint{{Requests: 100, Errors: 1}},
	}
	for range 99 {
		s.Latency.Record(time.Millisecond)
	}
	return s
}

func (fr *fakeRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get(atenet.TargetActorHeader)
	atespace, name, ok := strings.Cut(target, "/")
	if !ok || atespace != "egress-tests" {
		http.Error(w, "bad "+atenet.TargetActorHeader+" header: "+target, http.StatusBadRequest)
		return
	}
	if !fr.api.isRunning(name) {
		http.Error(w, "actor not running", http.StatusServiceUnavailable)
		return
	}
	fr.mu.Lock()
	defer fr.mu.Unlock()
	switch r.Method + " " + r.URL.Path {
	case "GET " + egressapi.ReadyzRoute:
		w.Write([]byte("ok"))
	case "POST " + egressapi.StartRoute:
		if fr.running[name] {
			http.Error(w, "already running", http.StatusConflict)
			return
		}
		var req egressapi.StartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if fr.startFault[name] == "refuse" {
			http.Error(w, "refused", http.StatusInternalServerError)
			return
		}
		fr.starts[name] = req
		fr.running[name] = true
		if fr.startFault[name] == "lost-answer" {
			http.Error(w, "upstream reset", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "GET " + egressapi.StatsRoute:
		json.NewEncoder(w).Encode(loopStats())
	case "POST " + egressapi.StopRoute:
		if !fr.running[name] {
			http.Error(w, "no loop", http.StatusNotFound)
			return
		}
		fr.running[name] = false
		fr.stops = append(fr.stops, name)
		json.NewEncoder(w).Encode(loopStats())
	default:
		http.NotFound(w, r)
	}
}

func services(n int) []runtime.Object {
	var objs []runtime.Object
	for i := range n {
		objs = append(objs, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName(i), Namespace: targetNamespace}})
	}
	return objs
}

func testConfig() runConfig {
	return runConfig{
		Actors:            12,
		Parallel:          3,
		Endpoints:         4,
		Duration:          30 * time.Millisecond,
		ConnMode:          connModeKeepAlive,
		RequestTimeout:    2 * time.Second,
		CreateConcurrency: 4,
		ResumeTimeout:     5 * time.Second,
		ProgressInterval:  10 * time.Millisecond,
		Atespace:          "egress-tests",
		Template:          "egress-tests-actor",
	}
}

func newTestRunner(t *testing.T, cfg runConfig, api *fakeAPI, deployed int) (*runner, *fakeRouter, *bytes.Buffer) {
	t.Helper()
	fr, srv := newFakeRouter(t, api)
	out := &bytes.Buffer{}
	r := newRunner(cfg, api, fake.NewSimpleClientset(services(deployed)...), newRouterClient(srv.URL, cfg.Atespace, cfg.Parallel), out)
	r.backoff = backoff{initial: time.Millisecond, max: 5 * time.Millisecond, rpcTimeout: time.Second}
	r.pollInterval = time.Millisecond
	return r, fr, out
}

func wantHosts(n int) []string {
	var hosts []string
	for i := range n {
		hosts = append(hosts, fmt.Sprintf("egress-target-%d.egress-tests-targets.svc.cluster.local", i))
	}
	return hosts
}

func TestRun(t *testing.T) {
	for _, mode := range []string{connModeKeepAlive, connModeNewConn} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig()
			cfg.ConnMode = mode
			api := newFakeAPI()
			r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)

			rep, err := r.run(t.Context())
			if err != nil {
				t.Fatalf("run: %v\n%s", err, out)
			}

			if got := api.callCount("CreateActor"); got != cfg.Actors {
				t.Errorf("CreateActor called %d times, want %d", got, cfg.Actors)
			}
			if len(api.policies) != cfg.Actors {
				t.Errorf("%d egress policies, want one per actor (%d)", len(api.policies), cfg.Actors)
			}
			for name, p := range api.policies {
				hosts := p.GetRules()[0].GetHttp().GetHostnames()
				if !slices.Equal(hosts, wantHosts(cfg.Endpoints)) {
					t.Errorf("policy of %s allows %v, want %v", name, hosts, wantHosts(cfg.Endpoints))
				}
			}

			resumed := api.callNames("ResumeActor")
			if want := []string{"egress-0", "egress-1", "egress-2"}; !slices.Equal(resumed, want) {
				t.Errorf("resumed %v, want %v", resumed, want)
			}
			wantURLs := endpointURLs(cfg.Endpoints)
			for _, name := range resumed {
				req, ok := fr.starts[name]
				if !ok {
					t.Errorf("loop of %s never started", name)
					continue
				}
				if !slices.Equal(req.URLs, wantURLs) {
					t.Errorf("%s started with %v, want %v", name, req.URLs, wantURLs)
				}
				if req.NewConnPerRequest != (mode == connModeNewConn) || req.RequestTimeoutMs != 2000 {
					t.Errorf("%s started with %+v, want new-conn=%v and a 2000ms timeout", name, req, mode == connModeNewConn)
				}
			}
			if got := slices.Sorted(slices.Values(fr.stops)); !slices.Equal(got, resumed) {
				t.Errorf("stopped %v, want every resumed actor %v", got, resumed)
			}
			if got := api.callNames("SuspendActor"); !slices.Equal(got, resumed) {
				t.Errorf("suspended %v, want every resumed actor %v", got, resumed)
			}

			if rep.Create.Succeeded != cfg.Actors || rep.Resume.Succeeded != 3 || rep.Start.Succeeded != 3 || rep.Suspend.Succeeded != 3 {
				t.Errorf("report phases: create %+v resume %+v start %+v suspend %+v", rep.Create, rep.Resume, rep.Start, rep.Suspend)
			}
			if rep.Loop.Requests != 300 || rep.Loop.Successes != 297 || rep.Loop.Errors["timeout"] != 3 {
				t.Errorf("merged loop stats = %+v, want 3 actors x 100 requests", rep.Loop)
			}
			if len(rep.Actors) != 3 || rep.Resume.ResumeLatency.Count != 3 {
				t.Errorf("report has %d actors and %d resume samples, want 3 each", len(rep.Actors), rep.Resume.ResumeLatency.Count)
			}

			var printed bytes.Buffer
			rep.print(&printed)
			for _, want := range []string{"300 requests", "timeout=3", "egress-target-0 (3/300)"} {
				if !strings.Contains(printed.String(), want) {
					t.Errorf("printed report lacks %q:\n%s", want, printed.String())
				}
			}
			if _, err := json.Marshal(rep); err != nil {
				t.Errorf("report does not marshal: %v", err)
			}
		})
	}
}

func TestRunReusesActorsAndUpdatesPolicies(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()

	// A first run with fewer endpoints leaves actors and narrower policies.
	small := cfg
	small.Endpoints = 2
	r, _, out := newTestRunner(t, small, api, cfg.Endpoints)
	if _, err := r.run(t.Context()); err != nil {
		t.Fatalf("first run: %v\n%s", err, out)
	}

	r, _, out = newTestRunner(t, cfg, api, cfg.Endpoints)
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("second run: %v\n%s", err, out)
	}
	if rep.Create.ActorsReused != cfg.Actors || rep.Create.PoliciesUpdated != cfg.Actors {
		t.Errorf("second run reused %d actors and updated %d policies, want %d of each", rep.Create.ActorsReused, rep.Create.PoliciesUpdated, cfg.Actors)
	}
	for name, p := range api.policies {
		if hosts := p.GetRules()[0].GetHttp().GetHostnames(); !slices.Equal(hosts, wantHosts(cfg.Endpoints)) {
			t.Errorf("policy of %s allows %v after the rerun, want %v", name, hosts, wantHosts(cfg.Endpoints))
		}
	}

	// A third run with the same endpoints changes nothing.
	r, _, out = newTestRunner(t, cfg, api, cfg.Endpoints)
	updatesBefore := api.callCount("UpdateActorEgressPolicy")
	rep, err = r.run(t.Context())
	if err != nil {
		t.Fatalf("third run: %v\n%s", err, out)
	}
	if rep.Create.PoliciesUpdated != 0 || api.callCount("UpdateActorEgressPolicy") != updatesBefore {
		t.Errorf("an unchanged rerun updated %d policies", rep.Create.PoliciesUpdated)
	}
}

func TestPreflightFailsOnMissingServices(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	r, _, _ := newTestRunner(t, cfg, api, 2)

	_, err := r.run(t.Context())
	if err == nil {
		t.Fatal("run succeeded with 2 of 4 endpoint Services deployed")
	}
	for _, want := range []string{"egress-target-2", "egress-target-3", "--endpoints 4"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if n := api.callCount("CreateActor"); n != 0 {
		t.Errorf("CreateActor called %d times before the preflight passed", n)
	}
}

func TestResumeRetriesFullPool(t *testing.T) {
	cfg := testConfig()
	cfg.Parallel = 1
	api := newFakeAPI()
	api.resumeErrs = []error{
		status.Error(codes.ResourceExhausted, "no worker has capacity"),
		status.Error(codes.Aborted, "concurrent update conflict"),
	}
	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Resume.Succeeded != 1 || api.callCount("ResumeActor") != 3 {
		t.Errorf("resume succeeded %d times after %d calls, want 1 after 3", rep.Resume.Succeeded, api.callCount("ResumeActor"))
	}
}

func TestResumeGivesUpOnCrash(t *testing.T) {
	cfg := testConfig()
	cfg.Parallel = 1
	api := newFakeAPI()
	api.resumeErrs = []error{status.Error(codes.Aborted, "actor entered ACTOR_STATE_CRASHED")}
	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Resume.Failed != 1 || rep.Resume.Errors["Aborted"] != 1 || api.callCount("ResumeActor") != 1 {
		t.Errorf("resume = %+v after %d calls, want one Aborted failure and no retry", rep.Resume, api.callCount("ResumeActor"))
	}
	if api.callCount("SuspendActor") != 0 {
		t.Errorf("suspended an actor that never resumed")
	}
}

func TestStartRecoversLeftoverLoop(t *testing.T) {
	cfg := testConfig()
	cfg.Parallel = 1
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	// An interrupted run left egress-0's loop running.
	fr.running["egress-0"] = true

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Start.Succeeded != 1 {
		t.Errorf("start = %+v, want the leftover loop stopped and restarted", rep.Start)
	}
	if len(fr.stops) != 2 {
		t.Errorf("stops = %v, want two: the leftover loop and the final collect", fr.stops)
	}
}

func TestStopReachesLoopsWhoseStartFailed(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.startFault["egress-1"] = "lost-answer"
	fr.startFault["egress-2"] = "refuse"

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Start.Succeeded != 1 || rep.Start.Failed != 2 {
		t.Errorf("start = %+v, want 1 ok and 2 failed", rep.Start)
	}
	// egress-1's loop runs although its start failed; the stop phase must
	// still reach it. egress-2 has no loop, which is not a stop failure.
	if got := slices.Sorted(slices.Values(fr.stops)); !slices.Equal(got, []string{"egress-0", "egress-1"}) {
		t.Errorf("stopped %v, want egress-0 and egress-1", got)
	}
	if fr.running["egress-1"] {
		t.Errorf("egress-1's loop is still running after the run")
	}
	if rep.Stop.Succeeded != 3 || rep.Stop.Failed != 0 {
		t.Errorf("stop = %+v, want 3 ok", rep.Stop)
	}
	if rep.Loop.Requests != 200 {
		t.Errorf("merged requests = %d, want the 200 of the two loops that ran", rep.Loop.Requests)
	}
}

func TestRunInterruptedStillStopsAndSuspends(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = time.Hour
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		// Interrupt once every loop is running.
		for {
			fr.mu.Lock()
			n := len(fr.starts)
			fr.mu.Unlock()
			if n == cfg.Parallel {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	rep, err := r.run(ctx)
	if err == nil || !rep.Interrupted {
		t.Fatalf("run = %v, interrupted %v; want the cancellation reported\n%s", err, rep.Interrupted, out)
	}
	if len(fr.stops) != cfg.Parallel || api.callCount("SuspendActor") != cfg.Parallel {
		t.Errorf("after the interrupt: %d loops stopped, %d actors suspended; want %d each", len(fr.stops), api.callCount("SuspendActor"), cfg.Parallel)
	}
	if rep.Loop.Requests != 300 {
		t.Errorf("interrupted run lost the loop stats: %+v", rep.Loop)
	}
}

func TestCleanup(t *testing.T) {
	api := newFakeAPI()
	for i := range 5 {
		api.actors[actorName(i)] = "running"
	}
	b := backoff{initial: time.Millisecond, max: time.Millisecond, rpcTimeout: time.Second}

	res := cleanup(t.Context(), api, b, "egress-tests", 7, 3, io.Discard)

	if res.Succeeded != 7 || res.NotFound != 2 || res.Failed != 0 {
		t.Errorf("cleanup = %+v, want 7 done of which 2 did not exist", res)
	}
	if len(api.actors) != 0 {
		t.Errorf("actors left after cleanup: %v", api.actors)
	}
}

func TestEndpointNamesAndPolicy(t *testing.T) {
	if got, want := endpointHost(7), "egress-target-7.egress-tests-targets.svc.cluster.local"; got != want {
		t.Errorf("endpointHost(7) = %q, want %q", got, want)
	}
	if got, want := endpointURLs(2), []string{
		"http://egress-target-0.egress-tests-targets.svc.cluster.local/",
		"http://egress-target-1.egress-tests-targets.svc.cluster.local/",
	}; !slices.Equal(got, want) {
		t.Errorf("endpointURLs(2) = %v, want %v", got, want)
	}

	p := buildPolicy("ns", 3)
	if p.GetMetadata().GetName() != "default" || p.GetMetadata().GetAtespace() != "ns" {
		t.Errorf("policy metadata = %v, want default in ns", p.GetMetadata())
	}
	if len(p.GetRules()) != 1 || p.GetRules()[0].GetHttp().GetPorts() != nil {
		t.Errorf("policy rules = %v, want one http rule on the default port", p.GetRules())
	}
	if !slices.Equal(p.GetRules()[0].GetHttp().GetHostnames(), wantHosts(3)) {
		t.Errorf("policy hostnames = %v", p.GetRules()[0].GetHttp().GetHostnames())
	}

	reordered := buildPolicy("ns", 3)
	slices.Reverse(reordered.Rules[0].Http.Hostnames)
	if !samePolicyHosts(reordered, p) || samePolicyHosts(buildPolicy("ns", 2), p) {
		t.Errorf("samePolicyHosts should ignore order and notice a different set")
	}
}

func TestConfigValidate(t *testing.T) {
	valid := testConfig()
	if err := valid.validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	for name, mutate := range map[string]func(*runConfig){
		"parallel above actors": func(c *runConfig) { c.Parallel = c.Actors + 1 },
		"no endpoints":          func(c *runConfig) { c.Endpoints = 0 },
		"too many endpoints":    func(c *runConfig) { c.Endpoints = maxEndpoints + 1 },
		"bad conn mode":         func(c *runConfig) { c.ConnMode = "pooled" },
		"zero duration":         func(c *runConfig) { c.Duration = 0 },
		"negative interval":     func(c *runConfig) { c.RequestInterval = -time.Second },
	} {
		c := testConfig()
		mutate(&c)
		if c.validate() == nil {
			t.Errorf("%s: validate accepted %+v", name, c)
		}
	}
}

func TestErrorKind(t *testing.T) {
	deadline := fmt.Errorf("resume: %w", fmt.Errorf("%w (last error: %w)", context.DeadlineExceeded, status.Error(codes.ResourceExhausted, "pool full for egress-7")))
	for _, tc := range []struct {
		err  error
		want string
	}{
		{status.Error(codes.NotFound, "actor egress-1 not found"), "NotFound"},
		{fmt.Errorf("create actor: %w", status.Error(codes.Internal, "x")), "Internal"},
		{deadline, "gave up retrying ResourceExhausted"},
		{&statusError{code: 503, body: "no healthy upstream"}, "HTTP 503"},
		{fmt.Errorf("start: %w", fmt.Errorf("dial tcp 127.0.0.1:1: connect: connection refused")), "connection refused"},
	} {
		if got := errorKind(tc.err); got != tc.want {
			t.Errorf("errorKind(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}
