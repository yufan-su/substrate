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
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

const (
	connModeKeepAlive = "keepalive"
	connModeNewConn   = "new-conn"
)

// runConfig is what one run does. It is echoed into the report.
type runConfig struct {
	Actors            int           `json:"actors"`
	Parallel          int           `json:"parallel"`
	Endpoints         int           `json:"endpoints"`
	Duration          time.Duration `json:"duration"`
	ConnMode          string        `json:"connMode"`
	RequestTimeout    time.Duration `json:"requestTimeout"`
	RequestInterval   time.Duration `json:"requestInterval"`
	CreateConcurrency int           `json:"createConcurrency"`
	ResumeTimeout     time.Duration `json:"resumeTimeout"`
	ProgressInterval  time.Duration `json:"progressInterval"`
	Atespace          string        `json:"atespace"`
	Template          string        `json:"template"`
	Scheme            string        `json:"scheme"`
}

func (c *runConfig) validate() error {
	switch {
	case c.Actors < 1:
		return fmt.Errorf("--actors must be at least 1, got %d", c.Actors)
	case c.Parallel < 1 || c.Parallel > c.Actors:
		return fmt.Errorf("--parallel must be between 1 and --actors (%d), got %d", c.Actors, c.Parallel)
	case c.Endpoints < 1 || c.Endpoints > egressapi.MaxEndpoints:
		return fmt.Errorf("--endpoints must be between 1 and %d, got %d", egressapi.MaxEndpoints, c.Endpoints)
	case c.Duration <= 0:
		return fmt.Errorf("--duration must be positive, got %v", c.Duration)
	case c.ConnMode != connModeKeepAlive && c.ConnMode != connModeNewConn:
		return fmt.Errorf("--conn-mode must be %s or %s, got %q", connModeKeepAlive, connModeNewConn, c.ConnMode)
	case c.RequestTimeout < time.Millisecond || c.RequestTimeout%time.Millisecond != 0:
		return fmt.Errorf("--request-timeout must be a positive whole number of milliseconds, got %v", c.RequestTimeout)
	case c.RequestInterval < 0 || c.RequestInterval%time.Millisecond != 0:
		return fmt.Errorf("--request-interval must be zero or a whole number of milliseconds, got %v", c.RequestInterval)
	case c.CreateConcurrency < 1:
		return fmt.Errorf("--create-concurrency must be at least 1, got %d", c.CreateConcurrency)
	case c.ResumeTimeout <= 0:
		return fmt.Errorf("--resume-timeout must be positive, got %v", c.ResumeTimeout)
	case c.ProgressInterval < 0:
		return fmt.Errorf("--progress-interval cannot be negative, got %v", c.ProgressInterval)
	case c.Scheme != egressapi.SchemeHTTP && c.Scheme != egressapi.SchemeHTTPS:
		return fmt.Errorf("--scheme must be %s or %s, got %q", egressapi.SchemeHTTP, egressapi.SchemeHTTPS, c.Scheme)
	case c.Atespace == "" || c.Template == "":
		return errors.New("--atespace and --template are required")
	}
	return nil
}

// actorName is the name of actor i. Names are stable across runs, so a rerun
// reuses the actors an earlier run created.
func actorName(i int) string {
	return fmt.Sprintf("egress-%d", i)
}

// runner carries out one run against a cluster.
type runner struct {
	cfg    runConfig
	api    ateapipb.ControlClient
	k8s    kubernetes.Interface
	router *routerClient
	out    io.Writer

	backoff backoff
	// createTimeout bounds the retries of one actor's create and policy.
	createTimeout time.Duration
	// pollInterval paces the readiness polls after a resume.
	pollInterval time.Duration
	// cleanupTimeout bounds stopping the loops and suspending the actors,
	// which still happen after the run is interrupted.
	cleanupTimeout time.Duration
	// rand picks which actors a run resumes.
	rand *rand.Rand
}

func newRunner(cfg runConfig, api ateapipb.ControlClient, k8s kubernetes.Interface, router *routerClient, out io.Writer) *runner {
	return &runner{
		cfg:            cfg,
		api:            api,
		k8s:            k8s,
		router:         router,
		out:            out,
		backoff:        defaultBackoff,
		createTimeout:  2 * time.Minute,
		pollInterval:   200 * time.Millisecond,
		cleanupTimeout: 3 * time.Minute,
		rand:           rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
}

func (r *runner) logf(format string, args ...any) {
	fmt.Fprintf(r.out, "%s  "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

// run creates the actors, resumes Parallel of them, runs their loops for
// Duration, and suspends them again. When ctx ends early, the loops that
// started are still stopped and collected and the resumed actors suspended;
// the partial report comes back with ctx's error.
func (r *runner) run(ctx context.Context) (*report, error) {
	rep := &report{Config: r.cfg, Started: time.Now()}
	defer func() { rep.Finished = time.Now() }()

	if err := preflight(ctx, r.k8s, r.cfg.Endpoints, r.cfg.Scheme); err != nil {
		return nil, err
	}

	created := r.createActors(ctx, rep)
	if ctx.Err() != nil {
		rep.Interrupted = true
		return rep, ctx.Err()
	}

	names := r.pickActors(created)
	if len(names) < r.cfg.Parallel {
		r.logf("only %d actors were created; resuming %d instead of %d", len(names), len(names), r.cfg.Parallel)
	}

	actors := r.resumeActors(ctx, names, rep)

	var ready []*actorRun
	for _, a := range actors {
		if a.ready {
			ready = append(ready, a)
		}
	}
	if ctx.Err() == nil {
		r.steady(ctx, r.startLoops(ctx, ready, rep))
	}

	// Everything resumed is put back, even after an interrupt, under its own
	// deadline that starts only now, whatever the run's duration.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cleanupTimeout)
	defer cancel()
	// Every ready actor gets a stop: an interrupt can land after an actor
	// started its loop but before the driver saw the answer.
	r.stopLoops(cleanupCtx, ready, rep)
	r.suspendActors(cleanupCtx, actors, rep)

	if ctx.Err() != nil {
		rep.Interrupted = true
		return rep, ctx.Err()
	}
	if rep.Stop.Failed > 0 || rep.Suspend.Failed > 0 {
		return rep, fmt.Errorf("%d loops were not stopped and %d actors were not suspended; they may still be sending traffic, so suspend them or run cleanup",
			rep.Stop.Failed, rep.Suspend.Failed)
	}
	return rep, nil
}

// createActors creates actors 0 through Actors-1 and gives each the egress
// policy. Actors and policies that already exist are reused. It returns, per
// actor, whether it is ready to resume.
func (r *runner) createActors(ctx context.Context, rep *report) []bool {
	n := r.cfg.Actors
	want := buildPolicy(r.cfg.Atespace, r.cfg.Endpoints)
	ok := make([]bool, n)
	res := newPhase()
	var reused, updated, done atomic.Int64

	stopProgress := r.progress(ctx, func() { r.logf("create: %d/%d", done.Load(), n) })
	start := time.Now()
	forEach(n, r.cfg.CreateConcurrency, func(i int) {
		defer done.Add(1)
		if ctx.Err() != nil {
			return
		}
		actorCtx, cancel := context.WithTimeout(ctx, r.createTimeout)
		defer cancel()
		existed, err := r.ensureActor(actorCtx, actorName(i))
		if err != nil {
			res.fail(fmt.Errorf("create actor: %w", err))
			return
		}
		if existed {
			reused.Add(1)
		}
		changed, err := r.ensurePolicy(actorCtx, actorName(i), want)
		if err != nil {
			res.fail(fmt.Errorf("egress policy: %w", err))
			return
		}
		if changed {
			updated.Add(1)
		}
		res.succeed()
		ok[i] = true
	})
	stopProgress()

	rep.Create = createResult{
		phaseResult:     res.result(n, time.Since(start)),
		ActorsReused:    int(reused.Load()),
		PoliciesUpdated: int(updated.Load()),
	}
	r.logf("create: %d/%d ready in %v (%d reused, %d policies updated)",
		rep.Create.Succeeded, n, rep.Create.Duration.Round(time.Millisecond), rep.Create.ActorsReused, rep.Create.PoliciesUpdated)
	return ok
}

// ensureActor creates the actor and reports whether it already existed. An
// AlreadyExists after an attempt whose outcome is unknown may be that
// attempt's own commit, so it does not count as existing.
func (r *runner) ensureActor(ctx context.Context, name string) (existed bool, err error) {
	var maybeCreated bool
	err = r.backoff.retry(ctx, func(ctx context.Context) error {
		_, err := r.api.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: r.cfg.Atespace, Name: name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: r.cfg.Template},
			},
		})
		if status.Code(err) == codes.AlreadyExists {
			existed = !maybeCreated
			return nil
		}
		if outcomeUnknown(err) {
			maybeCreated = true
		}
		return err
	})
	return existed, err
}

// ensurePolicy gives the actor exactly the want policy and reports whether it
// had to replace a different one.
func (r *runner) ensurePolicy(ctx context.Context, name string, want *ateapipb.EgressPolicy) (changed bool, err error) {
	ref := &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: name}
	var exists bool
	err = r.backoff.retry(ctx, func(ctx context.Context) error {
		_, err := r.api.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{Actor: ref, EgressPolicy: want})
		if status.Code(err) == codes.AlreadyExists {
			exists = true
			return nil
		}
		return err
	})
	if err != nil || !exists {
		return false, err
	}

	// Each attempt reads the policy again: the update needs the current UID
	// and version as preconditions, and a version conflict (Aborted) means
	// another write, possibly an earlier attempt's, moved them.
	err = r.backoff.retry(ctx, func(ctx context.Context) error {
		existing, err := r.api.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref})
		if err != nil {
			return err
		}
		if samePolicyRules(existing, want) {
			return nil
		}
		// Set before the call: an update that commits but loses its answer
		// finds matching rules on the next attempt.
		changed = true
		update := &ateapipb.EgressPolicy{Metadata: existing.GetMetadata(), Rules: want.GetRules()}
		_, err = r.api.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{Actor: ref, EgressPolicy: update})
		return err
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

// pickActors returns the names of Parallel actors chosen at random among the
// created ones, or of all of them if fewer were created. Picking at random
// spreads runs over the whole population instead of always resuming the same
// first actors, so successive runs also resume actors that never ran.
func (r *runner) pickActors(created []bool) []string {
	var idx []int
	for i, ok := range created {
		if ok {
			idx = append(idx, i)
		}
	}
	r.rand.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })
	idx = idx[:min(len(idx), r.cfg.Parallel)]
	slices.Sort(idx)
	names := make([]string, len(idx))
	for i, n := range idx {
		names[i] = actorName(n)
	}
	return names
}

// actorRun is one resumed actor and what happened to it.
type actorRun struct {
	name string
	// resumeLatency runs from the first ResumeActor attempt to its success,
	// including retries while the worker pool was full.
	resumeLatency time.Duration
	// readyLatency runs from the resume to the first answer through the router.
	readyLatency time.Duration
	resumed      bool
	// mayBeRunning is set when a resume failed in a way that may still have
	// committed, such as a timeout, so the actor is suspended anyway.
	mayBeRunning bool
	ready        bool
	started      bool
	err          error
	stats        *egressapi.Stats
}

// resumeActors resumes every named actor at once and waits until each answers
// through the router.
func (r *runner) resumeActors(ctx context.Context, names []string, rep *report) []*actorRun {
	actors := make([]*actorRun, len(names))
	res := newPhase()
	start := time.Now()
	forEach(len(names), len(names), func(i int) {
		a := &actorRun{name: names[i]}
		actors[i] = a
		actorCtx, cancel := context.WithTimeout(ctx, r.cfg.ResumeTimeout)
		defer cancel()

		resumeStart := time.Now()
		err := r.backoff.retry(actorCtx, func(ctx context.Context) error {
			_, err := r.api.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: a.name},
			})
			if outcomeUnknown(err) {
				a.mayBeRunning = true
			}
			return err
		})
		if err != nil {
			a.err = fmt.Errorf("resume: %w", err)
			res.fail(a.err)
			return
		}
		a.resumed = true
		a.resumeLatency = time.Since(resumeStart)

		readyStart := time.Now()
		if err := r.waitReady(actorCtx, a.name); err != nil {
			a.err = fmt.Errorf("waiting for %s through the router: %w", egressapi.ReadyzRoute, err)
			res.fail(a.err)
			return
		}
		a.ready = true
		a.readyLatency = time.Since(readyStart)
		res.succeed()
	})

	rep.Resume = resumeResult{phaseResult: res.result(len(names), time.Since(start))}
	var resumeLat, readyLat []time.Duration
	for _, a := range actors {
		if a.ready {
			resumeLat = append(resumeLat, a.resumeLatency)
			readyLat = append(readyLat, a.readyLatency)
		}
	}
	rep.Resume.ResumeLatency = summarize(resumeLat)
	rep.Resume.ReadyLatency = summarize(readyLat)
	r.logf("resume: %d/%d ready in %v (resume %s; ready %s)", rep.Resume.Succeeded, len(names),
		rep.Resume.Duration.Round(time.Millisecond), rep.Resume.ResumeLatency, rep.Resume.ReadyLatency)
	return actors
}

// waitReady polls the actor's readiness route through the router: right after
// a resume, the route to the actor can take a moment to settle.
func (r *runner) waitReady(ctx context.Context, name string) error {
	for {
		err := r.router.do(ctx, http.MethodGet, name, egressapi.ReadyzRoute, nil, nil)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %w)", ctx.Err(), err)
		case <-time.After(r.pollInterval):
		}
	}
}

// startLoops starts the loops of the ready actors at the same moment and
// returns the actors whose loop started.
func (r *runner) startLoops(ctx context.Context, ready []*actorRun, rep *report) []*actorRun {
	req := egressapi.StartRequest{
		Endpoints:         r.cfg.Endpoints,
		NewConnPerRequest: r.cfg.ConnMode == connModeNewConn,
		RequestTimeoutMs:  r.cfg.RequestTimeout.Milliseconds(),
		IntervalMs:        r.cfg.RequestInterval.Milliseconds(),
		Scheme:            r.cfg.Scheme,
	}
	res := newPhase()
	start := time.Now()
	forEach(len(ready), len(ready), func(i int) {
		a := ready[i]
		err := r.router.do(ctx, http.MethodPost, a.name, egressapi.StartRoute, req, nil)
		var se *statusError
		if errors.As(err, &se) && se.code == http.StatusConflict {
			// A loop left running by an interrupted run: stop it and start over.
			if err = r.router.do(ctx, http.MethodPost, a.name, egressapi.StopRoute, nil, nil); err == nil {
				err = r.router.do(ctx, http.MethodPost, a.name, egressapi.StartRoute, req, nil)
			}
		}
		if err != nil {
			a.err = fmt.Errorf("start: %w", err)
			res.fail(a.err)
			return
		}
		a.started = true
		res.succeed()
	})

	var started []*actorRun
	for _, a := range ready {
		if a.started {
			started = append(started, a)
		}
	}
	rep.Start = res.result(len(ready), time.Since(start))
	r.logf("start: %d/%d loops running against %d endpoints (%s, %s)", len(started), len(ready), r.cfg.Endpoints, r.cfg.Scheme, r.cfg.ConnMode)
	return started
}

// steady lets the loops run for Duration, printing progress along the way.
func (r *runner) steady(ctx context.Context, started []*actorRun) {
	if len(started) == 0 {
		return
	}
	start := time.Now()
	var last egressapi.Stats
	lastAt := start
	stopProgress := r.progress(ctx, func() {
		cur := r.pollStats(ctx, started)
		now := time.Now()
		secs := now.Sub(lastAt).Seconds()
		reqs, errs := cur.Requests-last.Requests, (cur.Requests-cur.Successes)-(last.Requests-last.Successes)
		r.logf("loop: %v elapsed, %.1f req/s, %.1f errors/s, p99 %v (cumulative)",
			now.Sub(start).Round(time.Second), float64(reqs)/secs, float64(errs)/secs, cur.Latency.Quantile(0.99))
		last, lastAt = *cur, now
	})
	defer stopProgress()

	select {
	case <-ctx.Done():
	case <-time.After(r.cfg.Duration):
	}
}

// pollStats merges the live stats of every running loop; actors that do not
// answer are left out.
func (r *runner) pollStats(ctx context.Context, actors []*actorRun) *egressapi.Stats {
	var mu sync.Mutex
	merged := &egressapi.Stats{}
	forEach(len(actors), len(actors), func(i int) {
		var s egressapi.Stats
		if err := r.router.do(ctx, http.MethodGet, actors[i].name, egressapi.StatsRoute, nil, &s); err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		merged.Merge(&s)
	})
	return merged
}

// stopLoops stops the loop of every ready actor and keeps its final stats.
// An actor whose start was never confirmed may have no loop, which is fine;
// one that did start and has no loop lost it, which is a failure.
func (r *runner) stopLoops(ctx context.Context, ready []*actorRun, rep *report) {
	res := newPhase()
	start := time.Now()
	forEach(len(ready), len(ready), func(i int) {
		a := ready[i]
		var s egressapi.Stats
		err := r.router.do(ctx, http.MethodPost, a.name, egressapi.StopRoute, nil, &s)
		var se *statusError
		switch {
		case err == nil:
			a.stats = &s
			res.succeed()
		case !a.started && errors.As(err, &se) && se.code == http.StatusNotFound:
			res.succeed()
		default:
			a.err = fmt.Errorf("stop: %w", err)
			res.fail(a.err)
		}
	})
	rep.Stop = res.result(len(ready), time.Since(start))

	merged := &egressapi.Stats{}
	for _, a := range ready {
		if a.stats != nil {
			merged.Merge(a.stats)
		}
		rep.Actors = append(rep.Actors, a.result())
	}
	rep.Loop = merged
}

// outcomeUnknown reports whether a failed call may still have committed on the
// server: it timed out, lost its connection, or failed with no status code,
// rather than being refused.
func outcomeUnknown(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Canceled, codes.Unavailable, codes.Unknown:
		return true
	}
	return false
}

// suspendActors suspends every actor that resumed, or may have, so the next
// run finds them with no loop running. Suspending an actor that is already
// suspended succeeds.
func (r *runner) suspendActors(ctx context.Context, actors []*actorRun, rep *report) {
	var toSuspend []*actorRun
	for _, a := range actors {
		if a.resumed || a.mayBeRunning {
			toSuspend = append(toSuspend, a)
		}
	}
	res := newPhase()
	start := time.Now()
	forEach(len(toSuspend), len(toSuspend), func(i int) {
		err := r.backoff.retry(ctx, func(ctx context.Context) error {
			_, err := r.api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: toSuspend[i].name},
			})
			return err
		})
		if err != nil {
			res.fail(fmt.Errorf("suspend: %w", err))
			return
		}
		res.succeed()
	})
	rep.Suspend = res.result(len(toSuspend), time.Since(start))
	r.logf("suspend: %d/%d resumed or possibly resumed actors suspended in %v", rep.Suspend.Succeeded, len(toSuspend), rep.Suspend.Duration.Round(time.Millisecond))
}

// progress calls report every ProgressInterval until the returned stop func
// is called. A zero interval disables it.
func (r *runner) progress(ctx context.Context, report func()) (stop func()) {
	if r.cfg.ProgressInterval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		ticker := time.NewTicker(r.cfg.ProgressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				report()
			}
		}
	})
	return func() {
		close(done)
		wg.Wait()
	}
}
