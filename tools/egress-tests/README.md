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
| `target/` | A small HTTP and HTTPS server. One Deployment of it backs every endpoint Service. |
| `.` (driver) | `go run ./tools/egress-tests run`: creates the actors, gives each one an egress policy, resumes B of them, runs the loops, and prints a report. |

## Endpoints and the egress policy

Endpoint `i` is the Service DNS name
`egress-target-<i>.egress-tests-targets.svc.cluster.local`, on port 80 for HTTP
and 443 for HTTPS. Each endpoint has its own Service, so it has its own name and
ClusterIP. All the Services select the same five `egress-target` pods.

The naming scheme is fixed in code, in
[internal/egressapi/endpoints.go](internal/egressapi/endpoints.go). The driver
sends each actor only the count C in the `POST /start` body (`{"endpoints": C,
...}`). The actor then calls endpoints 0 to C−1 in order, round and round. The
driver builds the egress policy from the same names, so the two always match.

An actor without an egress policy gets no egress at all. Right after creating
each actor, the driver gives it this policy:

```yaml
rules:
- http:          # no ports means port 80
    hostnames:
    - egress-target-0.egress-tests-targets.svc.cluster.local
    # ... up to egress-target-<C-1>
- https:         # no ports means port 443
    hostnames:
    - egress-target-0.egress-tests-targets.svc.cluster.local
    # ... the same names
```

Both schemes are always allowed, so switching `--scheme` changes no policy. A
rerun with a different C updates the existing policies, as does the first run
after an upgrade from a driver that allowed only HTTP.

## Prerequisites

- A cluster with Substrate installed, including the `atenet-egress` gateway.
- `source .ate-dev-env.sh`. `deploy.sh` reads `BUCKET_NAME` and `KO_DOCKER_REPO`
  from it.
- `kubectl` pointed at the cluster, and `jq`.

## Deploy

```bash
./tools/egress-tests/deploy.sh --deploy --workers 4 --worker-memory 8Gi --endpoints 100
```

This does five things:
1. Creates the `egress-tests` WorkerPool.
2. Creates the `egress-tests/egress-tests-actor` actor template and waits for
   its golden snapshot.
3. Issues the target's HTTPS certificate into Secret `egress-target-tls`, the
   first time only.
4. Deploys the target server, serving HTTP on 8080 and HTTPS on 8443.
5. Creates the endpoint Services `egress-target-0` up to
   `egress-target-<C-1>`.

Add `--https` to also patch the egress gateway for HTTPS runs (see
[HTTPS](#https)).

Deploy at least as many endpoints as the largest `--endpoints` you plan to run.

`--deploy` recreates the actor template, and that strands existing actors:
those never resumed restore from the template's golden snapshot, which is
deleted with it. So `--deploy` refuses while any actors exist. Delete them with
`cleanup` first, or keep the current template with `--skip-template`.

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
| `--parallel` | 1 | **B**: actors to resume, all running their loops at the same time. Each run picks them at random from the A actors. |
| `--endpoints` | 10 | **C**: endpoints each loop calls, in order, round and round. At most 256. |
| `--duration` | 5m | How long the loops run once they have all started. |
| `--scheme` | `http` | `http`, or `https` through the gateway's TLS interception. HTTPS needs `deploy.sh --https`; see [HTTPS](#https). |
| `--conn-mode` | `keepalive` | `keepalive` keeps one connection per endpoint. `new-conn` opens a new connection for every request. |
| `--request-timeout` | 5s | Timeout of one request. |
| `--request-interval` | 100ms | Pause after each request, which sets each actor's rate. 0 sends back to back at full speed. |
| `--create-concurrency` | 32 | How many actors are created at the same time. |
| `--resume-timeout` | 5m | Per actor: how long to keep resuming, then waiting for it to answer. |
| `--progress-interval` | 30s | How often to print progress. 0 turns it off. |
| `--usage` | off | Sample the CPU and memory of the components on the egress path. See [Usage sampling](#usage-sampling). |
| `--usage-verify` | off | With `--usage`, print the self-checks and fail the run if one fails. |
| `--usage-cgroup-reader` | off | With `--usage`, also pull one-second cgroup readings from the cgroup reader DaemonSet. |
| `--usage-live-interval` | 1s | How often `--usage` reads the Go process counters and Envoy's stats. |
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
3. **Resume**: resumes B actors, picked at random from the A, and waits until each one answers through the
   router.
4. **Start**: starts all the loops at the same moment.
5. **Run**: waits `--duration`, printing progress.
6. **Stop**: stops the loops and collects their stats.
7. **Suspend**: suspends the B actors.

After Ctrl-C, the loops that started are still stopped and the actors
suspended.

### HTTPS

`--scheme https` sends every request over TLS, through the gateway's TLS
interception path (its `egress_tls_mitm` filter chain):
1. The gateway terminates the actor's TLS with a certificate it mints for each
   name.
2. It checks each decrypted request against the policy.
3. It opens its own TLS connection to the target.

Most agent egress is HTTPS, so this path is the one that matters at scale.

Each request therefore uses two TLS connections:

| | TLS #1: actor → gateway | TLS #2: gateway → target |
|---|---|---|
| Certificate | minted for the name by the gateway's sdsmint, signed by the gateway's MITM CA | the target's own `*.egress-tests-targets.svc.cluster.local`, from Secret `egress-target-tls` |
| Why it's trusted | the actor template projects the `egress-mitm.ate.dev` trust bundle and points `SSL_CERT_FILE` at it | `deploy.sh --https` patches the gateway so its root bundle includes the target's CA |

**The gateway patch.** The gateway verifies every certificate it gets from a
destination against the public roots in its Envoy image, and the install has no
way to add a CA. `--patch-gateway`, or `--deploy --https`, works around that on
test clusters:
- It patches the `atenet-egress` Deployment with an init container that appends
  the target's CA to that root bundle. It changes no `envoy.yaml`, and it rolls
  the gateway.
- The CA can vouch only for the endpoint names: it is name-constrained to their
  domain, and its private key was thrown away after it signed the target's
  certificate (see [internal/targetcert](internal/targetcert/targetcert.go)).
- Reinstalling Substrate does not undo the patch. Run `--patch-gateway` again
  after a reinstall to pick up a new Envoy image, and `--unpatch-gateway` to
  remove it. `--delete` removes it too.
- `run --scheme https` checks the patch first and stops with a hint if it is
  missing, stale or still rolling out.

**The actor deliberately does not trust the target's CA.** If the gateway ever
passed TLS straight through instead of intercepting it, the actor would get the
target's certificate and fail with `tls: unknown authority`. So a run with no
errors shows that every request went through decrypt, policy check and
re-encrypt.

Setup, starting from a cluster that has actors from earlier runs:

```bash
go run ./tools/egress-tests cleanup --actors 1000      # the template changes once for HTTPS
./tools/egress-tests/deploy.sh --deploy --https --workers 8 --worker-memory 4Gi --endpoints 100
go run ./tools/egress-tests run --actors 100 --parallel 10 --endpoints 100 --scheme https
```

### Why the loop waits for `/start`

Every actor restores from the template's golden snapshot, which is taken
after the actor boots. If the loop started at boot, it would run in the golden
actor, which has no egress policy, so every request would fail. Those failures
would then be frozen into every actor's snapshot. Starting on request keeps the
snapshot idle. It also lets the driver start all B loops together and change
C or the connection mode without rebuilding the template.

## Usage sampling

`--usage` measures what the egress path spends during a run. It reads
three sources through the API server, each on its own cadence:

| Source | What | Cadence | Path |
|---|---|---|---|
| live | `process_cpu_seconds_total` and RSS of `ext-proc` and `ate-api-server` | `--usage-live-interval` (1s) and at each phase boundary | `pods/<pod>:9090/proxy/metrics` |
| envoy | the gateway's `mitm_internal` and `egress_forward_proxy_cleartext` connection counters | same | `pods/<pod>:15090/proxy/stats/prometheus`, the `envoy_metrics` listener; the admin API itself is loopback-only |
| cadvisor | container CPU, CFS throttling and working set of the workers, gateway, ateapi, router, targets and kube-dns | polled every 5s; the kubelet refreshes each container every 12 to 20s | `nodes/<node>/proxy/metrics/cadvisor` |

The worker pod is the smallest unit cAdvisor sees: it holds the actors,
gVisor, atunnel and the sandbox DNS relay together.

The report then adds `cpu` and `memory` lines: each component's mean and
peak cores over the steady window, cores per 1000 req/s, the exact steady
CPU of the Go processes and the driver, and the gateway's connection
overflow. `--output` adds a `resources` section with the raw readings, a
long-format `series` for plots, per-component summaries, and the
self-checks. `phases` and `loopTimeline` hold the phase boundaries and the
progress polls; use `--progress-interval 5s` for a finer timeline.

**Run at least 45s.** A cAdvisor series needs two readings inside the
steady window. A shorter run marks cAdvisor-only components
`insufficient`; the live sources still resolve one second.

**Permissions.** The driver uses your kubeconfig identity. It needs `get`
on `nodes/proxy`, `list` on pods in `egress-tests`, `egress-tests-targets`,
`ate-system` and `kube-system`, `get` on `pods/proxy` in `ate-system` and,
with `--usage-cgroup-reader`, in `egress-tests`, and `list` on
`pods.metrics.k8s.io` in those namespaces. `nodes/proxy` also
reaches the kubelet's exec and attach endpoints, so grant it only to
people who could exec into pods anyway.

**Self-checks.** `--usage-verify` prints the checks and fails the run
when one fails: the gateway's new connections match the actors' own count,
each pod cgroup equals its containers' sum, each Go process counter matches
its container's cgroup, metrics-server falls within the sampler's range
for its window within 15%, every series has its readings, the Go
processes' steady means fall within their per-interval rates, a rerun's
create and suspend phases stay near idle, and the driver stays under half
a core. `INFO` lines report what a short run cannot resolve.

Gaps between reads are judged in two tiers, since one slow API server
round trip costs a point of a cumulative counter, not CPU:

| Source | Fails when | INFO when |
|---|---|---|
| live (1s) | a gap over 5s or three intervals, whichever is longer, or over 5% of intervals over two intervals | a few intervals over two intervals |
| cgroup reader (1s) | a gap over 10s, or over 5% of intervals over 3s | a few intervals over 3s |
| cAdvisor (12 to 20s) | never | fewer than two readings in steady |

**One-second cgroup readings.** cAdvisor cannot show behavior shorter than
its refresh, and it cannot split a worker's CPU between its actors and
atunnel. `deploy.sh --deploy --cgreader` adds the `egress-tests-cgreader`
DaemonSet: one pod per node reads the cgroup v2 files of the cgroups the
driver names, every second, from the host's cgroup tree mounted read-only,
with no Kubernetes API access. Run with `--usage-cgroup-reader` and the
driver pulls the rows every 5s through `pods/proxy` in `egress-tests`. The
summaries then use these readings for every container they cover, the
workers gain `actors` (the actors' `_pause` and `actor` cgroups) and `atunnel` (the
`ateom` cgroup) parts. An actor's cgroups vanish when it suspends, before
its last second of checkpoint work can be read; when one actor vanishes in
a round, the driver credits it what the container used beyond its other
cgroups, as `inferredCpuSeconds` on the gone row. `--usage-verify` adds
checks: no rows lost, every requested container cgroup answered, node
clocks steady, the leaves summing to their container, the readings
agreeing with cAdvisor within 2%, and each reader under 0.02 core.
`deploy.sh --delete` removes the DaemonSet.

The reader answers on its pod IP without authentication, so while it runs
any pod in the cluster can read every node's per-second cgroup counters
under `kubepods.slice`. It is for development clusters only, and a
namespace at the `baseline` Pod Security level rejects its host mount.

Before a real measurement, run a short smoke test:

```bash
go run ./tools/egress-tests run --actors 10 --parallel 1 --endpoints 10 \
  --duration 30s --usage --usage-verify --progress-interval 5s
```

It should end with `verify     PASS`. At 30s the cAdvisor-only components
are `insufficient`, as expected.

### Plotting runs

`plot/plot.py` draws one or more `--output` reports as one self-contained
HTML page: CPU, working set, throttling, the gateway's connections, req/s
and p99, one panel each, on a shared axis of seconds since the steady
window started. Each component keeps one color; each run gets a dash style.
`plot/verify.py` runs the checks that span runs: gateway cost per 1000
req/s across B, each run's own connection verdict, runs with and without
`--usage`, and each run's driver overhead. `make verify` runs their tests
through `hack/verify/egress-plot.sh`.

```bash
python3 -m venv /tmp/egress-plot && /tmp/egress-plot/bin/pip install -r tools/egress-tests/plot/requirements.txt
/tmp/egress-plot/bin/python tools/egress-tests/plot/plot.py b10.json b100.json -o runs.html
/tmp/egress-plot/bin/python tools/egress-tests/plot/verify.py b*.json
```

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
- **TLS** (HTTPS only) counts completed handshakes between the actor and the
  gateway, with their times. In keepalive mode that's about B×C; in new-conn
  mode it's one per request.
- **Errors** are grouped into classes: `timeout`, `connection refused`,
  `connection reset`, `EOF`, `dns: …`, `HTTP <code>`, and `other: …`. An
  `HTTP 403` means the gateway denied the request.
- **HTTPS error classes:**
  - `tls: unknown authority`, `tls: hostname mismatch`, `tls: invalid certificate`,
    `tls: not a TLS server` and `tls: peer alert: …` name what failed verification.
  - A `tls handshake: ` prefix marks a failure partway through a handshake. For
    example, `tls handshake: EOF` is the gateway closing a connection whose name
    no rule allows.
  - `HTTP 503` in HTTPS mode usually means the gateway couldn't verify the
    target. Check `upstream_failure` in the gateway's access log, and whether
    the gateway is patched.

The create line's reused count is the actors whose `CreateActor` returned
`AlreadyExists` with no earlier attempt that timed out or lost its connection.
An actor that existed before the run, but whose first attempt timed out
before reaching the server, counts as created, not reused.

Resume and ready latency are measured by the driver. Resume latency runs
from the first `ResumeActor` attempt until it succeeds, retries included.
Ready latency runs from then until the actor first answers through the router.
Because each run picks its B actors at random, they usually mix actors that
never ran, which restore from the template's golden snapshot, with actors an
earlier run suspended, which restore from their own snapshot.

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
  target. The gateway saturates long before the target does; its five replicas
  leave plenty of room even after scaling the gateway out.

- **Keep-alive mode holds one tunnel per actor and endpoint.** Every actor
  connection is its own atunnel tunnel, and its own connection in the gateway's
  `mitm_internal` cluster.
  - That cluster allows 16,384 tunnels per gateway replica.
  - Past that, the gateway refuses new tunnels, and the actor sees `EOF` or
    `connection reset`.
  - To confirm, port-forward to the gateway's Envoy admin port
    (`kubectl -n ate-system port-forward deploy/atenet-egress 15000`) and read
    `cluster.mitm_internal.upstream_cx_overflow` under `/stats`.
  - The clusters that connect to destinations (`egress_forward_proxy` and
    `egress_forward_proxy_cleartext`) still use Envoy's defaults of 1,024
    connections. Bursts of simultaneous requests to the same destination push
    those counts up.
- **HTTPS costs more per connection.**
  - Session resumption is off, so new-conn mode does a full handshake with the
    gateway on every request.
  - The gateway's minted certificates last 15 minutes, so runs longer than that
    mint them again.
  - Expect more gateway CPU and memory than the same HTTP run, for TLS on both
    sides.
- **New-conn mode loads the control plane.** Every CONNECT makes the gateway
  call ateapi `GetActor`, uncached, to authenticate the actor.
- **Confirming the gateway carried the traffic.**
  `kubectl -n ate-system logs deploy/atenet-egress -c envoy --tail=3` should
  show JSON lines with the actor's SPIFFE ID
  (`ateom-for-actor/egress-tests/egress-<n>`), an `egress-target-<i>` authority,
  and `"leg":"cleartext"` (HTTP) or `"leg":"mitm"` (HTTPS). For HTTPS,
  `"upstream_failure"` should be empty. The keys are in alphabetical order, so
  grep for one key at a time.
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
gateway patch, the template, the atespace, the endpoints and the worker pool.
To remove only the gateway patch, use `deploy.sh --unpatch-gateway`.
