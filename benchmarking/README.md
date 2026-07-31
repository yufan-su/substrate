# Substrate Benchmarking

This is the nascent suite for benchmarking Substrate's performance at scale.

## Deploy benchmarks

> [!IMPORTANT]
> Source the environment configuration file (e.g., `source .ate-dev-env.sh`)
> first so `PROJECT_ID`, `BUCKET_NAME`, etc. are set.

Note that deploying the benchmarks does not run them. You must visit Locust's
web UI to start a test.

A single wrapper deploys the scale workloads, builds and pushes the Locust
image, then deploys the Locust workers:

```bash
./benchmarking/deploy_locust.sh --deploy
```

Useful flags:

* `--worker-count N` — number of `WorkerPool` replicas (default 1).
* `--skip-build` — reuse the existing `:latest` locust image (skip the
  `docker build && docker push` step).

To tear everything down (locust then workloads, in reverse order):

```bash
./benchmarking/deploy_locust.sh --delete
```

The same operations are also reachable from the top-level installer for
convenience:

```bash
./hack/install-ate.sh --deploy-benchmarks
./hack/install-ate.sh --delete-benchmarks
```

The installer accepts `--benchmark-worker-count N` (default `1`).
`--skip-build` is only available when invoking
`benchmarking/deploy_locust.sh` directly.

## Running Tests

### Locust Web UI
* Run `kubectl port-forward svc/locust -n benchmarking 8089:8089`
* Visit `http://localhost:8089` in your browser to configure and start the load test.

The different user classes you can select are different types of load behaviors
you can throw at the system. Note that the "CounterUser" load type requires
that the counter demo be installed.

You can also configure things like the number of users, how quickly those users
are spawned, the frequency with which requests are made and whether or not tracing is
enabled.

### 24h soak test

`SoakUser` (`locust/tests/soak.py`) answers a different question from the other
user classes: not "how fast can Substrate go" but "is Substrate still working
N hours from now". Each user creates one glutton actor and then loops for the
whole run, doing two things:

| Stat row | What it covers |
| --- | --- |
| `ActorPing` | Data path: router → worker → sandbox → actor, every iteration |
| `ActorSuspend` / `ActorResume` | Lifecycle: checkpoint, worker release, scheduling, snapshot restore — every `--soak-pings-per-cycle` iterations |

The cycle is what makes this a soak of Substrate rather than of the router.
Pinging alone leaves the actor in `STATUS_RUNNING` forever, so resume would be
exercised exactly once, at t=0, and with `boot=True` — which skips snapshots
entirely. A restore that degrades at hour 18, a scheduler that leaks worker
assignments, or a snapshot store that slows as it fills would all be invisible.
Set `--soak-pings-per-cycle=0` to turn cycling off and get the old data-path-only
behaviour.

Note the two latency rows per operation. `SuspendActorCycle` / `ResumeActorCycle`
are the gRPC calls — the control plane *accepting* the request, which returns
long before the work is done. `ActorSuspend` / `ActorResume` are the wall-clock
to actually reach the terminal status, checkpoint upload and snapshot restore
included. The second pair is the one to watch for drift.

**Worker capacity.** A worker holds exactly one actor, and the scheduler only
places onto workers with no assignment
(`cmd/ateapi/internal/scheduling/scheduling.go`). So the pool needs at least as
many workers as you run users, or the surplus users fail in `on_start` with
`no free workers available`. Scale it before starting — `WorkerPoolSpec` is
mutable and has a scale subresource, so this needs no `ko apply`:

```bash
kubectl scale workerpool benchmark-ateom -n benchmark-workloads --replicas=12
```

Leave headroom above the user count. At exactly 1:1 there is no free worker
anywhere in the fleet, so the first worker pod restart strands its actor until
the replacement is Ready — which reads as a Substrate failure when it is really
a self-inflicted ceiling.

Deploy with `--no-boomer`, which drops the `boomer-glutton` container from the
pod:

```bash
./benchmarking/deploy_locust.sh --deploy --no-boomer
```

Without it the pod runs two workers, and locust's dispatcher hands spawns out
by worker-ID sort order — so a run where you picked `SoakUser` can land on the
boomer worker instead. boomer sums the `user_classes_count` map and throws the
class *names* away ([`runner.go`'s
`sumUsersAmount`](../vendor/github.com/myzhan/boomer/runner.go)), then echoes
them back verbatim in its `spawn_complete` reply. The dashboard therefore shows
`SoakUser: 1` with complete confidence while glutton is what actually runs, and
the only visible symptom is that the stats table says `GluttonPing` rather than
`ActorPing`. Deploy without the flag again before running a glutton benchmark;
`GluttonUser` still appears in the class picker either way, but with
`--no-boomer` nothing can execute it.

Then start it from the web UI:

1. `kubectl port-forward svc/locust -n benchmarking 8089:8089`
2. Select **only** `SoakUser` in the class picker.
3. Users `3`, ramp-up `1`, run time blank (or `24h`).
4. Set `--min-wait-time` to `10` and `--max-wait-time` to `15`. The default
   (0–0.5s) would be a load test, not a soak.
5. Leave `--trace-probability` at `0`. 24h of sampled spans is a lot of export
   volume for no benefit here — failures are already classified in the stats.

Before starting, check the UI reports **1 worker** connected — with
`--no-boomer` that one worker is the Python one, and a `0` means
`locust-python-worker` failed to attach and nothing will run your class.

The first ping only starts once the actor reaches `STATUS_RUNNING`, which can
take a minute or two on a cold boot. Closing the browser or dropping the
port-forward does not stop the run; the test lives in the master pod. Once
requests start accruing, confirm the stats table names `ActorPing` and not
`GluttonPing`; the latter means a boomer worker picked up the spawn.

**Pass criteria:** zero failures, and `ActorPing` p50/p95 over the final hour
within noise of the first hour.

When a ping does fail, the Failures tab says which side broke — the user
immediately re-checks the control plane and reports either
`control plane reports <STATUS> — actor-side failure` or
`control plane also unreachable (ate-api-server suspect)`.

The master writes a rolling CSV history, so results survive the browser
session and can be graphed afterwards:

```bash
kubectl cp -n benchmarking -c locust-master \
  "$(kubectl get pod -n benchmarking -l app=locust -o name | head -1 | cut -d/ -f2)":/tmp/soak_stats_history.csv \
  soak_stats_history.csv
```

Two caveats worth holding onto while reading the results:

* If the **master pod restarts**, the in-memory stats reset and the UI will
  look like a fresh run. Check `kubectl get pod -n benchmarking -l app=locust`
  for a non-zero restart count before trusting a clean result.
* Locust sees the data path and the actor lifecycle, but nothing else. A
  worker-pod restart, an OOMKill, or memory creep in a controller is invisible
  unless it makes a ping or a cycle slow or fail. Install the Prometheus stack
  below if you need to watch `ate-system` itself.
* **Snapshots accumulate.** Every suspend mints a new snapshot id under
  `gs://$BUCKET_NAME/benchmark-workloads/glutton/<actor>/`
  (`workflow_suspend.go`), repoints `LatestSnapshotInfo` at it, and deletes
  nothing. A cycling 24h run leaves one checkpoint per cycle per user — with 10
  users at the default interval, a few thousand. Budget the storage, and clean
  up afterwards:

  ```bash
  gsutil -m rm -r "gs://$BUCKET_NAME/benchmark-workloads/glutton/sb-*"
  ```

  Do not run that while a soak is live. Growth in resume latency as the prefix
  fills is a legitimate finding, not an artifact — it is one of the things
  cycling is there to catch.

### Viewing Traces
You must have enabled otel tracing for your cluster to view traces.

You can find trace IDs by viewing the `logs` tab in the Locust UI

## Optional: Prometheus + Grafana

Locust provides graphs, statistics, etc. via the UI. However, you
can install Prometheus/Grafana if you want richer details or
the ability to perform deeper analysis. Skip this section if
you're only using the Locust web UI.

```bash
kubectl apply -f benchmarking/monitoring.yaml
```

Once installed:

* Run `kubectl port-forward svc/grafana -n benchmarking 3000:3000`
* Visit `http://localhost:3000` in your browser.

## Development

### Rebuilding gRPC Python clients

Make sure you have a virtual environment created (`python3 -m venv venv`)
and activated (`source venv/bin/activate`).

Install project requirements: `pip install -r requirements.txt`

Then run `generate_protos.sh` to generate the Python proto clients.
