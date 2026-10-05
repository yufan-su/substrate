# egress-tests

A scale test for actor egress. It creates **A** actors, resumes **B** of them,
and has each resumed actor run a for loop that sends HTTP requests to **C**
in-cluster endpoints. Every request takes the actor egress path:

```
actor ─▶ sandbox nftables redirect ─▶ atunnel (per-actor mTLS CONNECT)
      ─▶ atenet-egress (Envoy + ext_proc policy check) ─▶ egress-target-<i> Service
```

The test has three pieces:

| Piece | What it is |
|---|---|
| `actor/` | The workload. `POST /start` starts the loop, `GET /stats` reports progress, `POST /stop` stops the loop and returns its final stats. |
| `target/` | A small HTTP server. One Deployment of it backs every endpoint Service. |
| `.` (driver) | `go run ./tools/egress-tests run`: creates the actors, gives each one an egress policy, resumes B of them, runs the loops, and prints a report. |

## Endpoints and the egress policy

Endpoint `i` is the Service DNS name
`egress-target-<i>.egress-tests-targets.svc.cluster.local`, on port 80. Each
endpoint has its own Service, so it has its own name and ClusterIP. All the
Services select the same single `egress-target` pod.

The naming scheme is fixed in code, in
[internal/egressapi/endpoints.go](internal/egressapi/endpoints.go). The driver
sends each actor only the count C in the `POST /start` body (`{"endpoints": C,
...}`). The actor then calls endpoints 0 to C−1 in order, round and round. The
driver builds the egress policy from the same names, so the two always match.

An actor without an egress policy gets no egress at all. Right after creating
each actor, the driver gives it this policy:

```yaml
rules:
- http:
    hostnames:
    - egress-target-0.egress-tests-targets.svc.cluster.local
    # ... up to egress-target-<C-1>; no ports means port 80
```

A rerun with a different C updates the existing policies.

## Prerequisites

- A cluster with Substrate installed, including the `atenet-egress` gateway.
- `source .ate-dev-env.sh`. `deploy.sh` reads `BUCKET_NAME` and `KO_DOCKER_REPO`
  from it.
- `kubectl` pointed at the cluster, and `jq`.

## Deploy

```bash
./tools/egress-tests/deploy.sh --deploy --workers 4 --worker-memory 8Gi --endpoints 100
```

This does four things:
1. Creates the `egress-tests` WorkerPool.
2. Creates the `egress-tests/egress-tests-actor` actor template and waits for
   its golden snapshot.
3. Deploys the target server.
4. Creates the endpoint Services `egress-target-0` up to
   `egress-target-<C-1>`.

Deploy at least as many endpoints as the largest `--endpoints` you plan to run.

Size the pool for B running actors. A worker hosts up to `--worker-memory` /
`--actor-memory` actors, and at most 1000. For example, B=100 at the default
256Mi needs about 26Gi of worker memory, plus headroom. When the pool is full,
`ResumeActor` returns ResourceExhausted. The driver keeps retrying until
`--resume-timeout`.

## Run

```bash
go run ./tools/egress-tests run --actors 1000 --parallel 10 --endpoints 10 --duration 5m
```

| Flag | Default | Meaning |
|---|---|---|
| `--actors` | 1000 | **A**: actors to create. Actors are named `egress-<i>`. A rerun reuses the ones that already exist. |
| `--parallel` | 1 | **B**: actors to resume, all running their loops at the same time. These are `egress-0` up to `egress-<B-1>`. |
| `--endpoints` | 10 | **C**: endpoints each loop calls, in order, round and round. At most 256. |
| `--duration` | 5m | How long the loops run once they have all started. |
| `--conn-mode` | `keepalive` | `keepalive` keeps one connection per endpoint. `new-conn` opens a new connection for every request. |
| `--request-timeout` | 5s | Timeout of one request. |
| `--request-interval` | 100ms | Pause after each request, which sets each actor's rate. 0 sends back to back at full speed. |
| `--create-concurrency` | 32 | How many actors are created at the same time. |
| `--resume-timeout` | 5m | Per actor: how long to keep resuming, then waiting for it to answer. |
| `--progress-interval` | 30s | How often to print progress. 0 turns it off. |
| `--output` | | Write the full report, including per-actor stats, as JSON to this file. Durations are in nanoseconds. |
| `--kubeconfig`, `--context` | | Which cluster to use. |
| `--api-endpoint`, `--router-url` | | When left empty, the driver port-forwards to the `api` and `atenet-router` Services. |

### Pacing

Each loop has one request in flight. With the default `--request-interval` of
100ms, an actor sends about 10 requests per second, and one round over C
endpoints takes about C × 100ms. B=100 then sends about 1,000 req/s in total,
which one gateway replica handles. With `--request-interval 0`, each actor sends
as fast as responses come back: about 800 req/s per actor on a c3-standard-4
cluster. Use that only to find the gateway's limit, raising B (1, 2, 4, 8)
until latency and errors climb.

The run goes through these phases:
1. **Preflight**: checks that the C endpoint Services exist.
2. **Create**: creates the A actors and their policies.
3. **Resume**: resumes B actors and waits until each one answers through the
   router.
4. **Start**: starts all the loops at the same moment.
5. **Run**: waits `--duration`, printing progress.
6. **Stop**: stops the loops and collects their stats.
7. **Suspend**: suspends the B actors.

After Ctrl-C, the loops that started are still stopped and the actors
suspended.

### Why the loop waits for `/start`

Every actor restores from the template's golden snapshot, which is taken
after the actor boots. If the loop started at boot, it would run in the golden
actor, which has no egress policy, so every request would fail. Those failures
would then be frozen into every actor's snapshot. Starting on request keeps the
snapshot idle. It also lets the driver start all B loops together and change
C or the connection mode without rebuilding the template.

## Reading the report

This is a real run: `--actors 100 --parallel 1 --endpoints 10 --duration 2m
--request-interval 0`, on two c3-standard-4 nodes with one gateway replica.

```
== egress-tests: actors=100 parallel=1 endpoints=10 conn-mode=keepalive request-interval=0s duration=2m0s
create     100/100 ok in 386.4ms (0 actors reused, 0 policies updated)
resume     1/1 ok in 396.4ms
           resume p50 298.8ms p99 298.8ms max 298.8ms; ready p50 97.6ms p99 97.6ms max 97.6ms
start      1/1 ok in 32.9ms
loop       2m0.1s: 99142 requests, 825.7 req/s, 100.000% success, 10 new connections
latency    p50 1.26ms p90 1.41ms p99 2ms p99.9 3.98ms max 42.9ms (mean 1.2ms)
dns        10 lookups, p50 17.8ms p99 32.2ms max 32.2ms
           req/s per actor: min 825.7 median 825.7 max 825.7
stop       1/1 ok in 33.9ms
suspend    1/1 ok in 398.9ms
```

When requests fail, the report adds an `errors` line with counts per class,
and a line naming the endpoints with the most errors.

- **Latency** is measured inside the actor, for successful requests only. It
  includes the DNS lookup when the request needed one.
- **New connections** tell the two modes apart. In keepalive mode the count is
  about B×C. In new-conn mode it is about one per request.
- **DNS** counts lookups. Go does not cache DNS. Every new connection resolves
  the name through the sandbox DNS relay and the cluster DNS, including the
  `ndots:5` search-suffix tries.
- **Errors** are grouped into classes: `timeout`, `connection refused`,
  `connection reset`, `EOF`, `dns: …`, `HTTP <code>`, and `other: …`. An
  `HTTP 403` means the gateway denied the request.

Resume and ready latency are measured by the driver. Resume latency runs
from the first `ResumeActor` attempt until it succeeds, retries included.
Ready latency runs from then until the actor first answers through the router.

## Suggested matrix

Step up one factor at a time, and save each run with `--output`:

1. A=1000, C=10, keepalive, with B = 1, then 10, then 100.
2. The same with C=100.
3. A=10000. The rerun creates only the missing actors.
4. new-conn, starting again from B=1, while you watch the control plane.

Keep the default 100ms interval for these, so the factors under test, not the
request rate, are what changes. Run `--request-interval 0` separately to find
the gateway's limit.

Record the `atenet-egress` replica count with each result. The default is 1.

## Limits to expect

- **CPU per request.** Measured with `kubectl top` at about 820 req/s, every
  1,000 req/s costs roughly 0.94 core in the gateway (Envoy plus ext_proc),
  0.85 core in the workers (actor, gVisor, atunnel), and 0.14 core in the
  target. So the gateway saturates long before the single target pod does. Add
  target replicas only after scaling the gateway out.

- **Keep-alive mode hits Envoy's default circuit breaker.** Every actor
  connection is its own atunnel tunnel, and its own connection in the gateway's
  `mitm_internal` cluster. That cluster and `egress_forward_proxy_cleartext`
  set no `circuit_breakers`, so Envoy's default of 1024 connections applies.
  Past roughly 1k open tunnels (B×C), expect CONNECTs to fail. The actor sees
  `EOF` or `connection reset`. To confirm, port-forward to the gateway's Envoy
  admin port (`kubectl -n ate-system port-forward deploy/atenet-egress 15000`)
  and read `cluster.mitm_internal.upstream_cx_overflow` under `/stats`.
- **New-conn mode loads the control plane.** Every CONNECT makes the gateway
  call ateapi `GetActor`, uncached, to authenticate the actor.
- **Confirming the gateway carried the traffic.**
  `kubectl -n ate-system logs deploy/atenet-egress -c envoy --tail=3` should
  show JSON lines with `"leg":"cleartext"`, the actor's SPIFFE ID
  (`ateom-for-actor/egress-tests/egress-<n>`), and an `egress-target-<i>`
  authority. The keys are in alphabetical order, so grep for one key at a time.
- **Logging volume.** Envoy writes an access log line for every CONNECT and
  every request, so keep high-rate runs short. The kubelet also rotates the
  log quickly: at about 800 req/s, only the last ~25 seconds survive.

## Cleanup

```bash
go run ./tools/egress-tests cleanup --actors 10000   # the largest --actors any run used
./tools/egress-tests/deploy.sh --delete
```

`cleanup` deletes `egress-0` up to `egress-<actors-1>` in any state. Each
actor's egress policy is deleted with it. `deploy.sh --delete` removes the
template, the atespace, the endpoints and the worker pool.
