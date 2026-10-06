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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/targetcert"
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
	// suspendErr, if set, is what SuspendActor returns.
	suspendErr error
	// The lostAnswer counters make that many calls commit and then return
	// DeadlineExceeded, as when the answer is lost after the server wrote.
	createLostAnswer int
	updateLostAnswer int
	// resumeLostAnswer makes every ResumeActor commit and then time out.
	resumeLostAnswer bool
	nextUID          int
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
	if f.createLostAnswer > 0 {
		f.createLostAnswer--
		return nil, status.Error(codes.DeadlineExceeded, "answer lost")
	}
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
		Metadata: &ateapipb.ResourceMetadata{Atespace: in.GetActor().GetAtespace(), Name: "default", Uid: fmt.Sprint(f.nextUID), Version: 1},
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
	if in.GetEgressPolicy().GetMetadata().GetVersion() != cur.GetMetadata().GetVersion() {
		return nil, status.Error(codes.Aborted, "EgressPolicy version conflict")
	}
	updated := proto.Clone(in.GetEgressPolicy()).(*ateapipb.EgressPolicy)
	updated.Metadata.Version = cur.GetMetadata().GetVersion() + 1
	f.policies[name] = updated
	if f.updateLostAnswer > 0 {
		f.updateLostAnswer--
		return nil, status.Error(codes.DeadlineExceeded, "answer lost")
	}
	return updated, nil
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
	if f.resumeLostAnswer {
		return nil, status.Error(codes.DeadlineExceeded, "answer lost")
	}
	return &ateapipb.ResumeActorResponse{Resumed: true}, nil
}

func (f *fakeAPI) SuspendActor(_ context.Context, in *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("SuspendActor", name)
	if f.suspendErr != nil {
		return nil, f.suspendErr
	}
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

// services returns the Services of endpoints 0 through n-1, exposing ports.
func services(n int, ports ...int32) []runtime.Object {
	var objs []runtime.Object
	for i := range n {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: egressapi.ServiceName(i), Namespace: egressapi.TargetNamespace}}
		for _, p := range ports {
			svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{Port: p})
		}
		objs = append(objs, svc)
	}
	return objs
}

// gatewaySetup is what deploy.sh --deploy --https leaves behind: the target's
// TLS Secret, the CA ConfigMap and a gateway patched for it and rolled out.
type gatewaySetup struct {
	secret *corev1.Secret
	cm     *corev1.ConfigMap
	dep    *appsv1.Deployment
}

func newGatewaySetup(t *testing.T) *gatewaySetup {
	t.Helper()
	bundle, err := targetcert.Generate(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(bundle.CA)
	replicas := int32(1)
	return &gatewaySetup{
		secret: &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: targetTLSSecret, Namespace: egressapi.TargetNamespace},
			Data:       map[string][]byte{targetcert.CAFile: bundle.CA, targetcert.CertFile: bundle.Cert, targetcert.KeyFile: bundle.Key},
		},
		cm: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: gatewayCAConfigMap, Namespace: installdefaults.SystemNamespace},
			Data:       map[string]string{targetcert.CAFile: string(bundle.CA)},
		},
		dep: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: gatewayDeployment, Namespace: installdefaults.SystemNamespace, Generation: 2},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{gatewayCAAnnotation: hex.EncodeToString(sum[:])}},
					Spec: corev1.PodSpec{InitContainers: []corev1.Container{
						{Name: gatewayTrustInitContainer}, {Name: "sdsmint"},
					}},
				},
			},
			Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1},
		},
	}
}

func (g *gatewaySetup) objects() []runtime.Object {
	var objs []runtime.Object
	for _, o := range []runtime.Object{g.secret, g.cm, g.dep} {
		if !reflect.ValueOf(o).IsNil() {
			objs = append(objs, o)
		}
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
		Scheme:            egressapi.SchemeHTTP,
	}
}

func newTestRunner(t *testing.T, cfg runConfig, api *fakeAPI, deployed int) (*runner, *fakeRouter, *bytes.Buffer) {
	t.Helper()
	return newTestRunnerWith(t, cfg, api, services(deployed)...)
}

// newTestRunnerWith is newTestRunner with the cluster holding exactly objs.
func newTestRunnerWith(t *testing.T, cfg runConfig, api *fakeAPI, objs ...runtime.Object) (*runner, *fakeRouter, *bytes.Buffer) {
	t.Helper()
	fr, srv := newFakeRouter(t, api)
	out := &bytes.Buffer{}
	r := newRunner(cfg, api, fake.NewSimpleClientset(objs...), newRouterClient(srv.URL, cfg.Atespace, cfg.Parallel), out)
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
				if len(p.GetRules()) != 2 {
					t.Errorf("policy of %s has %d rules, want an http and an https rule", name, len(p.GetRules()))
					continue
				}
				if hosts := p.GetRules()[0].GetHttp().GetHostnames(); !slices.Equal(hosts, wantHosts(cfg.Endpoints)) {
					t.Errorf("http rule of %s allows %v, want %v", name, hosts, wantHosts(cfg.Endpoints))
				}
				if hosts := p.GetRules()[1].GetHttps().GetHostnames(); !slices.Equal(hosts, wantHosts(cfg.Endpoints)) {
					t.Errorf("https rule of %s allows %v, want %v", name, hosts, wantHosts(cfg.Endpoints))
				}
			}

			resumed := api.callNames("ResumeActor")
			if len(resumed) != cfg.Parallel || len(slices.Compact(slices.Clone(resumed))) != cfg.Parallel {
				t.Errorf("resumed %v, want %d distinct actors", resumed, cfg.Parallel)
			}
			for _, name := range resumed {
				if _, ok := api.actors[name]; !ok {
					t.Errorf("resumed %s, which the run did not create", name)
				}
			}
			for _, name := range resumed {
				req, ok := fr.starts[name]
				if !ok {
					t.Errorf("loop of %s never started", name)
					continue
				}
				if req.Endpoints != cfg.Endpoints {
					t.Errorf("%s started with %d endpoints, want %d", name, req.Endpoints, cfg.Endpoints)
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
			for _, want := range []string{"scheme=http ", "request-interval=0s", "300 requests", "timeout=3", "egress-target-0 (3/300)"} {
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
	cfg.Actors, cfg.Parallel = 1, 1 // so the run resumes egress-0
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
	cfg.Actors = cfg.Parallel // so the run resumes egress-0 to egress-2
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

func TestCleanupDeadlineStartsAfterTheLoops(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = 100 * time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	// Shorter than the run: a deadline counted from the resume would be spent
	// before the loops are stopped.
	r.cleanupTimeout = 50 * time.Millisecond

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if len(fr.stops) != cfg.Parallel || rep.Suspend.Succeeded != cfg.Parallel {
		t.Errorf("%d loops stopped and %d actors suspended, want %d each", len(fr.stops), rep.Suspend.Succeeded, cfg.Parallel)
	}
	if rep.Loop.Requests != 300 {
		t.Errorf("merged requests = %d, want 300", rep.Loop.Requests)
	}
}

func TestRunFailsWhenActorsAreLeftRunning(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	api.suspendErr = status.Error(codes.FailedPrecondition, "cannot suspend")
	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	rep, err := r.run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "3 actors were not suspended") {
		t.Fatalf("run = %v, want an error naming the 3 unsuspended actors\n%s", err, out)
	}
	if rep == nil || rep.Suspend.Failed != cfg.Parallel {
		t.Errorf("report = %+v, want it returned with the suspend failures", rep)
	}
}

func TestPickActorsAtRandom(t *testing.T) {
	cfg := testConfig()
	cfg.Actors, cfg.Parallel = 20, 5
	created := make([]bool, cfg.Actors)
	for i := range created {
		created[i] = i != 3 // egress-3 failed to create
	}
	firstFive := []string{"egress-0", "egress-1", "egress-2", "egress-4", "egress-5"}

	picks := map[string]bool{}
	for seed := range uint64(20) {
		r := newRunner(cfg, nil, nil, nil, io.Discard)
		r.rand = rand.New(rand.NewPCG(seed, seed))
		names := r.pickActors(created)
		if len(names) != cfg.Parallel || len(slices.Compact(slices.Clone(names))) != cfg.Parallel {
			t.Fatalf("seed %d picked %v, want %d distinct actors", seed, names, cfg.Parallel)
		}
		if slices.Contains(names, "egress-3") {
			t.Fatalf("seed %d picked egress-3, which was not created", seed)
		}
		picks[strings.Join(names, ",")] = true

		again := newRunner(cfg, nil, nil, nil, io.Discard)
		again.rand = rand.New(rand.NewPCG(seed, seed))
		if !slices.Equal(again.pickActors(created), names) {
			t.Fatalf("seed %d picked differently on a second runner", seed)
		}
	}
	if len(picks) < 2 || (len(picks) == 1 && picks[strings.Join(firstFive, ",")]) {
		t.Errorf("20 seeds picked %v; want the pick to vary instead of always the first created actors", picks)
	}

	// Fewer created than wanted: all of them.
	r := newRunner(cfg, nil, nil, nil, io.Discard)
	few := make([]bool, cfg.Actors)
	few[7], few[11] = true, true
	if got := r.pickActors(few); !slices.Equal(got, []string{"egress-7", "egress-11"}) {
		t.Errorf("pick with two created = %v, want both, in order", got)
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

func TestBuildPolicy(t *testing.T) {
	p := buildPolicy("ns", 3)
	if p.GetMetadata().GetName() != "default" || p.GetMetadata().GetAtespace() != "ns" {
		t.Errorf("policy metadata = %v, want default in ns", p.GetMetadata())
	}
	rules := p.GetRules()
	if len(rules) != 2 || rules[0].GetHttp() == nil || rules[1].GetHttps() == nil {
		t.Fatalf("policy rules = %v, want an http rule and an https rule", rules)
	}
	if rules[0].GetHttp().GetPorts() != nil || rules[1].GetHttps().GetPorts() != nil {
		t.Errorf("policy rules = %v, want both on their default ports", rules)
	}
	if !slices.Equal(rules[0].GetHttp().GetHostnames(), wantHosts(3)) || !slices.Equal(rules[1].GetHttps().GetHostnames(), wantHosts(3)) {
		t.Errorf("policy hostnames = %v", rules)
	}

	reordered := buildPolicy("ns", 3)
	slices.Reverse(reordered.Rules[0].Http.Hostnames)
	slices.Reverse(reordered.Rules)
	httpOnly := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Http: &ateapipb.HTTPRule{Hostnames: wantHosts(3)}}}}
	switch {
	case !samePolicyRules(reordered, p):
		t.Error("samePolicyRules told apart policies that differ only in order")
	case samePolicyRules(buildPolicy("ns", 2), p):
		t.Error("samePolicyRules missed a different set of hostnames")
	case samePolicyRules(httpOnly, p):
		t.Error("samePolicyRules took an http-only policy for one that also allows https")
	}
}

// Policies an earlier driver created allow only HTTP; a run brings each up to
// both rules once and then leaves them alone.
func TestRunUpgradesHTTPOnlyPolicies(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	for i := range cfg.Actors {
		name := actorName(i)
		api.actors[name] = "suspended"
		api.nextUID++
		api.policies[name] = &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: cfg.Atespace, Name: "default", Uid: fmt.Sprint(api.nextUID)},
			Rules:    []*ateapipb.EgressRule{{Http: &ateapipb.HTTPRule{Hostnames: wantHosts(cfg.Endpoints)}}},
		}
	}

	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Create.PoliciesUpdated != cfg.Actors {
		t.Errorf("updated %d policies, want all %d http-only ones", rep.Create.PoliciesUpdated, cfg.Actors)
	}
	for name, p := range api.policies {
		if len(p.GetRules()) != 2 {
			t.Errorf("policy of %s has %d rules after the run, want 2", name, len(p.GetRules()))
		}
	}

	r, _, out = newTestRunner(t, cfg, api, cfg.Endpoints)
	if rep, err = r.run(t.Context()); err != nil {
		t.Fatalf("second run: %v\n%s", err, out)
	}
	if rep.Create.PoliciesUpdated != 0 {
		t.Errorf("second run updated %d policies, want none", rep.Create.PoliciesUpdated)
	}
}

func TestRunHTTPS(t *testing.T) {
	cfg := testConfig()
	cfg.Scheme = egressapi.SchemeHTTPS
	api := newFakeAPI()
	objs := append(services(cfg.Endpoints, 80, httpsPort), newGatewaySetup(t).objects()...)
	r, fr, out := newTestRunnerWith(t, cfg, api, objs...)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if len(fr.starts) != cfg.Parallel {
		t.Fatalf("%d loops started, want %d", len(fr.starts), cfg.Parallel)
	}
	for name, req := range fr.starts {
		if req.Scheme != egressapi.SchemeHTTPS {
			t.Errorf("%s started over %q, want https", name, req.Scheme)
		}
	}
	var printed bytes.Buffer
	rep.print(&printed)
	if !strings.Contains(printed.String(), "scheme=https ") {
		t.Errorf("report header does not say https:\n%s", printed.String())
	}
}

func TestPreflightHTTPS(t *testing.T) {
	const n = 3
	for _, tc := range []struct {
		name    string
		scheme  string
		ports   []int32
		mutate  func(*gatewaySetup)
		wantErr string // "" means preflight passes
	}{
		{name: "patched gateway", scheme: egressapi.SchemeHTTPS, ports: []int32{80, httpsPort}},
		{name: "http needs no gateway setup", scheme: egressapi.SchemeHTTP, mutate: func(g *gatewaySetup) { *g = gatewaySetup{} }},
		{name: "service without https port", scheme: egressapi.SchemeHTTPS, ports: []int32{80}, wantErr: "--https"},
		{name: "no target secret", scheme: egressapi.SchemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.secret = nil }, wantErr: "--deploy --https"},
		{name: "no CA configmap", scheme: egressapi.SchemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.cm = nil }, wantErr: "--patch-gateway"},
		{name: "gateway trusts another CA", scheme: egressapi.SchemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) {
				g.cm.Data[targetcert.CAFile] = string(newGatewaySetup(t).secret.Data[targetcert.CAFile])
			}, wantErr: "--patch-gateway"},
		{name: "gateway not patched", scheme: egressapi.SchemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) {
				g.dep.Spec.Template.Spec.InitContainers = g.dep.Spec.Template.Spec.InitContainers[1:]
			}, wantErr: "--patch-gateway"},
		{name: "patch annotation is stale", scheme: egressapi.SchemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.dep.Spec.Template.Annotations[gatewayCAAnnotation] = "0000" }, wantErr: "--patch-gateway"},
		{name: "rollout in progress", scheme: egressapi.SchemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.dep.Status.UpdatedReplicas = 0 }, wantErr: "rollout status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGatewaySetup(t)
			if tc.mutate != nil {
				tc.mutate(g)
			}
			k8s := fake.NewSimpleClientset(append(services(n, tc.ports...), g.objects()...)...)
			err := preflight(t.Context(), k8s, n, tc.scheme)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("preflight = %v, want it to pass", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("preflight = %v, want an error mentioning %q", err, tc.wantErr)
			}
		})
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
		"too many endpoints":    func(c *runConfig) { c.Endpoints = egressapi.MaxEndpoints + 1 },
		"bad conn mode":         func(c *runConfig) { c.ConnMode = "pooled" },
		"zero duration":         func(c *runConfig) { c.Duration = 0 },
		"negative interval":     func(c *runConfig) { c.RequestInterval = -time.Second },
		"bad scheme":            func(c *runConfig) { c.Scheme = "ftp" },
		"sub-ms interval":       func(c *runConfig) { c.RequestInterval = 500 * time.Microsecond },
		"fractional interval":   func(c *runConfig) { c.RequestInterval = 1500 * time.Microsecond },
		"sub-ms timeout":        func(c *runConfig) { c.RequestTimeout = 500 * time.Microsecond },
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

func TestOutcomeUnknown(t *testing.T) {
	t.Parallel()
	gaveUp := func(ctxErr, last error) error {
		return fmt.Errorf("%w (last error: %w)", ctxErr, last)
	}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"FailedPrecondition", status.Error(codes.FailedPrecondition, "x"), false},
		{"AlreadyExists", status.Error(codes.AlreadyExists, "x"), false},
		{"NotFound", status.Error(codes.NotFound, "x"), false},
		{"ResourceExhausted", status.Error(codes.ResourceExhausted, "x"), false},
		{"Aborted", status.Error(codes.Aborted, "x"), false},
		{"DeadlineExceeded code", status.Error(codes.DeadlineExceeded, "x"), true},
		{"Canceled code", status.Error(codes.Canceled, "x"), true},
		{"Unavailable code", status.Error(codes.Unavailable, "x"), true},
		{"Unknown code", status.Error(codes.Unknown, "x"), true},
		{"error with no status", fmt.Errorf("connection reset"), true},
		{"wrapped Unavailable", fmt.Errorf("resume: %w", status.Error(codes.Unavailable, "x")), true},
		{"wrapped context.DeadlineExceeded", fmt.Errorf("resume: %w", context.DeadlineExceeded), true},
		{"wrapped context.Canceled", fmt.Errorf("resume: %w", context.Canceled), true},
		{"gave up after refusal", gaveUp(context.DeadlineExceeded, status.Error(codes.ResourceExhausted, "x")), true},
		{"canceled after refusal", gaveUp(context.Canceled, status.Error(codes.FailedPrecondition, "x")), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := outcomeUnknown(tc.err); got != tc.want {
				t.Errorf("outcomeUnknown(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestLostAnswers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		setup func(api *fakeAPI)
		check func(t *testing.T, r *runner, api *fakeAPI)
	}{
		{
			name:  "policy update committed before its answer was lost",
			setup: func(api *fakeAPI) { api.updateLostAnswer = 1 },
			check: func(t *testing.T, r *runner, api *fakeAPI) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				if _, err := api.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
					Actor: &ateapipb.ObjectRef{Atespace: "egress-tests", Name: "egress-0"}, EgressPolicy: buildPolicy("egress-tests", 1),
				}); err != nil {
					t.Fatal(err)
				}
				want := buildPolicy("egress-tests", 3)
				changed, err := r.ensurePolicy(ctx, "egress-0", want)
				if err != nil || !changed {
					t.Fatalf("ensurePolicy = %v, %v; want true, nil", changed, err)
				}
				if got := api.policies["egress-0"].GetRules()[0].GetHttp().GetHostnames(); !slices.Equal(got, wantHosts(3)) {
					t.Errorf("policy hosts = %v, want %v", got, wantHosts(3))
				}
				if got := api.callCount("UpdateActorEgressPolicy"); got != 1 {
					t.Errorf("UpdateActorEgressPolicy called %d times, want 1: the retry should see the policy already updated", got)
				}
			},
		},
		{
			name:  "create committed before its answer was lost",
			setup: func(api *fakeAPI) { api.createLostAnswer = 1 },
			check: func(t *testing.T, r *runner, api *fakeAPI) {
				existed, err := r.ensureActor(t.Context(), "egress-0")
				if err != nil || existed {
					t.Errorf("ensureActor = %v, %v; want false, nil", existed, err)
				}
			},
		},
		{
			name:  "resume committed but every answer timed out",
			setup: func(api *fakeAPI) { api.resumeLostAnswer = true },
			check: func(t *testing.T, r *runner, api *fakeAPI) {
				r.cfg.Actors, r.cfg.Parallel = 1, 1 // so the run resumes egress-0
				r.cfg.ResumeTimeout = 50 * time.Millisecond
				rep, err := r.run(t.Context())
				if err != nil {
					t.Fatalf("run: %v", err)
				}
				if rep.Resume.Failed != 1 {
					t.Errorf("resume = %+v, want one failure", rep.Resume)
				}
				if api.isRunning("egress-0") {
					t.Errorf("egress-0 left running after a resume whose answer was lost")
				}
				if got := api.callNames("SuspendActor"); !slices.Equal(got, []string{"egress-0"}) {
					t.Errorf("suspended %v, want [egress-0]", got)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := newFakeAPI()
			tc.setup(api)
			r, _, _ := newTestRunner(t, testConfig(), api, testConfig().Endpoints)
			tc.check(t, r, api)
		})
	}
}
