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
* `--no-boomer` — deploy without the `boomer-glutton` container. See
  [Boomer claims spawns for every user class](#boomer-claims-spawns-for-every-user-class);
  use it for anything that is not a glutton benchmark.

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
you can throw at the system. Note that the "CounterUser" and "SoakUser" load
types require that the counter demo be installed. "SoakUser" is not a load
test — see [Long-running soak](#long-running-soak-soakuser).

You can also configure things like the number of users, how quickly those users
are spawned, the frequency with which requests are made and whether or not tracing is
enabled.

### Boomer claims spawns for every user class

The locust pod normally runs two workers on the master's ZMQ port: the Python
worker, and `boomer-glutton`, a Go re-implementation of `GluttonUser`. Boomer
is not class-aware. Its `sumUsersAmount`
(`vendor/github.com/myzhan/boomer/runner.go`) adds up the counts in a spawn
message and throws the class names away, so it answers a spawn for *any* class
the master assigns it and runs glutton regardless of what you picked.

This fails quietly. `spawnComplete` echoes the original `user_classes_count`
back verbatim, so the web UI keeps reporting the class you selected; the only
visible tell is glutton's rows appearing in the stats table. Locust's
dispatcher is deterministic rather than class-aware, so with a small user count
the spawn may land entirely on boomer.

Deploy with `--no-boomer` for any run that is not a glutton benchmark:

```bash
./benchmarking/deploy_locust.sh --deploy --no-boomer
```

The pod then has two containers instead of three. Redeploy without the flag
before running a glutton benchmark again.

### Viewing Traces
You must have enabled otel tracing for your cluster to view traces.

You can find trace IDs by viewing the `logs` tab in the Locust UI

## Long-running soak (`SoakUser`)

`SoakUser` (`locust/tests/soak.py`) answers a different question from the other
user classes. They all measure a throughput ceiling; this one asks whether
Substrate is still working N hours from now. Each user brings up exactly one
counter actor and holds it for the whole run.

**Prerequisite:** the counter demo, installed with
`./hack/install-ate.sh --deploy-demo-counter`.

### What each iteration covers

* **`ActorPing`** — every iteration. A `POST /` through `atenet-router` to the
  actor and back, covering the data path (router → worker → sandbox → actor).
* **`ActorSuspend` / `ActorResume`** — every `--soak-pings-per-cycle`
  iterations. A full suspend/resume, covering checkpoint upload, worker
  release, scheduler placement and snapshot restore.

The cycle is not optional decoration. Pinging alone leaves the actor
`STATUS_RUNNING` forever, so resume runs exactly once — at startup, with
`boot=True`, which skips snapshots entirely. A restore that degrades at hour 18
or a scheduler that leaks worker assignments would be invisible. Set
`--soak-pings-per-cycle 0` to disable cycling and test only the data path.

### What a ping asserts

The counter demo answers with two counters that have deliberately different
durability, so a ping proves more than "something replied":

| Counter | Where it lives | Across a cycle | Assertion |
| --- | --- | --- | --- |
| `preserved file counter` | `durableDir` at `/home/counter` | **survives** | strictly increasing for the whole run |
| `preserved memory count` | process memory | resets | `+1` per ping within a window with no cycle |

The file counter is the headline assertion: it proves the DurableDir →
snapshot store → restore round trip preserved state on every cycle. A reset
means a cycle silently lost the volume.

The memory counter resets because `SuspendActor` takes an `EXTERNAL` checkpoint
scoped by the template's `onCommit`, which the counter sets to `Data`
(`demos/counter/counter.yaml.tmpl`); `SNAPSHOT_SCOPE_DATA` excludes memory and
the rest of rootfs. (`onPause: Full` would preserve it, but only `PauseActor`
uses that scope.) Between cycles, though, nothing should be restarting the
actor, so a memory counter that jumps means the actor was silently replaced —
which a check that only looked at HTTP status would read as perfect health.

### Reading the latency rows

Each transition produces **two** rows, and the difference between them is the
point:

* `SuspendActorCycle` / `ResumeActorCycle` — the RPC's own server-side handler
  time. That is just the control plane accepting the request; it returns long
  before the work is done.
* `ActorSuspend` / `ActorResume` — wall-clock until the actor actually reaches
  its terminal status, checkpoint upload or snapshot restore included.

Watch the second pair. It is the one that drifts as the snapshot store fills.

### Worker capacity

The scheduler only places onto workers with no assignment
(`cmd/ateapi/internal/scheduling/scheduling.go`) — one actor per worker — and
returns `ErrNoCapacity` otherwise. The counter demo has its **own** WorkerPool,
`counter` in `ate-demo-counter` with `replicas: 5`, not `benchmark-ateom`, so
`--worker-count` does not affect it.

A cycling user releases its worker mid-cycle and needs a free slot to resume
onto, so leave headroom above the user count. At exactly 1:1 the first worker
pod restart strands an actor and reads as a Substrate failure.

```bash
kubectl scale workerpool counter -n ate-demo-counter --replicas=12
```

### Running it

Deploy with `--no-boomer` (see above), port-forward, then in the web UI select
**only** `SoakUser`:

| Setting | Value |
| --- | --- |
| Users | 3 |
| `--min-wait-time` / `--max-wait-time` | 10 / 15 |
| `--trace-probability` | 0 |
| Run time | `24h`, or blank |

The default 0–0.5s wait would turn this into a load test. `--soak-channel-max-age`
(default 3600s) controls how often each user rebuilds its ateapi channel; it
exists because pod certificates are capped at 24h, exactly the length of the
run, and Python gRPC bakes the certificate in at channel construction.

**Do a short run first.** With `--soak-pings-per-cycle 3` a 15-minute run forces
several cycles. Check that `ActorPing` failures are 0, that `ActorSuspend` /
`ActorResume` are present and larger than their `*Cycle` counterparts, and that
`kubectl ate get actors -a benchmark` shows no leftover `sb-*` actors
afterwards. Rerun once with `--soak-channel-max-age 120` to exercise the channel
rebuild — that is the one path the real run cannot validate until it is too
late.

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
