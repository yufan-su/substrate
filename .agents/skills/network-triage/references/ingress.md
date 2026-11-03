# Ingress: caller to actor

Path: caller → **ingress router** (`envoy` on TLS 8443 or CONNECT 8444, then
ext_proc in `atenet-router`) → ate-api-server `ResumeActor` → mTLS CONNECT to
**atunnel** on the worker's 8443 → actor at 169.254.17.2. The router Service
exposes TLS on 443.

The caller names the actor in header `ate-target-actor: <atespace>/<actor>`. On
the CONNECT listener the header and port go on the outer `CONNECT <host>:<port>`
request. Headers on the inner request don't route. The default actor port is 80.

The router's own errors have a short `text/plain` body naming the actor. Envoy
writes the rest: an empty 500 or 504 when ext_proc fails, and a `text/plain`
body starting `upstream connect error` or `upstream request timeout` for
failures past the router.

Collect the evidence first:

```bash
scripts/ingress.sh <atespace> <actor>
```

It prints the actor's own logs, the router's lines for the actor and errors for any actor, Envoy
stats, recent requests, router metrics, and atunnel's lines on the actor's
worker. Only Envoy's tunnel lines name the actor, and a healthy tunnel logs
when it closes. Match per-request lines by time and status.

## Ingress router

atenet-router, in `ate-system` with pods `app=atenet-router`. Container `envoy`
terminates caller TLS on 8443 and CONNECT on 8444, and writes the access log.
Container `atenet-router` runs ext_proc: it reads `ate-target-actor`, asks
ate-api-server to resume the actor, and points `envoy` at the actor's worker.

| Caller sees | Root cause | Confirm |
|---|---|---|
| Connection refused on 8443 or 8444 | Itself: `--ingress-allowed-spiffe-ids` is empty, so it serves no listener | `atenet-router` log `No --ingress-allowed-spiffe-ids, so the router serves no traffic` |
| TLS handshake fails, no HTTP response | Caller: its cert doesn't chain to `--ingress-client-ca-file`, or its SPIFFE ID isn't in the allowlist | `envoy` admin stats `listener.0.0.0.0_8443.ssl.fail_verify_san` (SPIFFE ID not allowed), `ssl.fail_verify_error` (bad chain), `ssl.fail_verify_no_cert`. Port 8444 has the same stats |
| 400 `Bad Request` on the CONNECT listener | Caller: the CONNECT authority has no port | `envoy` access log flag `DPE` |
| 404 `invalid actor reference` | Caller: `ate-target-actor` missing or malformed, or sent only on the inner CONNECT request | `atenet-router` log `Error during ext_proc RequestHeaders processing` |
| 404 `actor <atespace>/<name> not found` | Caller: the actor doesn't exist | `kubectl ate get actor` |
| 503 `actor ... unavailable: no worker has room for the actor` | WorkerPool capacity. It answers after `--parked-request-budget` (5s) | `kubectl ate get workers`; metric `atenet.router.parking.wait.duration` with `outcome=budget_exhausted` |
| 503 `actor "..." unavailable: router at capacity` | Itself: parked requests hit `--parked-request-max` | metric `atenet.router.parking.rejected` |
| 503 `actor ... unavailable: <reason>` | ate-api-server: actor state, such as `CRASHED` or a lock conflict | `atenet-router` log `Error during ext_proc RequestHeaders processing`, whose `err` is the body; actor state |
| 503 `actor ... unavailable` with no reason | ate-api-server: down or unreachable | ate-api-server pods and logs |
| 504 `actor ... request timed out` | ate-api-server: slow to resume | ate-api-server logs |
| 401 `actor ... authentication required`, 403 `actor ... access denied` | ate-api-server rejects the router's identity, or [pod certificates](shared-causes.md#pod-certificates) | ate-api-server logs |
| 500 `error resuming actor ...` or `actor ... routing failed` | ate-api-server: unknown error, or no worker IP or SPIFFE ID in the assignment | ate-api-server logs. For `routing failed`, `ResumeActor result` has an empty `workerIP` or `workerSPIFFEID` |
| 500, no body, no router log for the request | Itself: ext_proc down | `envoy` stats `ext_proc.streams_failed`, `cluster.ate-cluster.upstream_cx_connect_fail`; `atenet-router` `/statusz` |
| 500, no body, under heavy load | Itself: ext_proc circuit breaker `--extproc-max-requests` | `envoy` stat `cluster.ate-cluster.upstream_rq_active_overflow`; `envoy` log `reset reason: overflow` |
| 504, no body | Itself or ate-api-server: ext_proc didn't answer in time. Envoy gives it `--parked-request-budget` plus 5s, 10s by default, or 5s with `--parked-request-max=0` | `envoy` stat `ext_proc.message_timeouts`; `atenet-router` log `Error during ext_proc RequestHeaders processing` with `canceled by client` |
| 504 `upstream request timeout` after about 5 minutes | Itself: `--route-timeout` (default 5m). It caps each request, including each request inside a CONNECT tunnel | `envoy` flag `UT` |
| Requests cut off during a router rollout | Itself: drain force-stops requests still open at `--drain-timeout` | `atenet-router` logs `Shutdown signal received; draining`, `ext_proc drain deadline exceeded; forcing stop` |

**Hand-off:** if `atenet-router` logged `Route ok` for the request, the router
resolved the worker. Go to atunnel (ingress).

Metric `atenet.router.route.duration` carries `ate.router.outcome`: `ok`,
`not_found`, `no_capacity`, `failed_precondition`, `lock_conflict`,
`unavailable`, `timeout`, `denied`, `resume_error`, `cancelled`, `rate_limited`.

## atunnel (ingress)

atunnel runs in container `worker` of the worker pod, in the WorkerPool
namespace with label `ate.dev/worker-pool=<pool>`. It accepts the router's mTLS
CONNECT on 8443 and forwards it to the actor at 169.254.17.2. Its log lines
contain `atunnel` or `ingress:`.

With the default `--upstream-connect=true`, every failure past the router
reaches the caller as a 503 with body `upstream connect error or
disconnect/reset before headers. reset reason: connection termination`. The
`envoy` log line `tunnel actor=... connect_response=...` and the worker logs
tell them apart.

| Caller sees | Root cause | Confirm |
|---|---|---|
| 503; worker log `atunnel CONNECT upstream failed` | Actor app: not listening on that port | `worker` log has `actor`, `port`, `err`. `connect_response` has `:status` 502 |
| 503; worker log `http: TLS handshake error ... ingress: client is not "spiffe://..."` | [Pod certificates](shared-causes.md#pod-certificates): the router's identity doesn't match the worker's `--atunnel-client-identity`, often because the router runs in another namespace or ServiceAccount | Router namespace and ServiceAccount. The `envoy` tunnel line has `connect_response=-` |
| 503; `envoy` tunnel line `connect_response` has `:status` 421 and `x-ate-assignment-stale: true` | ate-api-server: stale assignment, the actor isn't active on that worker. The worker logs the reject only at debug | Compare `WORKER IPS` in `kubectl ate get actor` with the IP in the tunnel line |
| Hangs then fails, for every actor in a pool | [Cluster network](shared-causes.md#networkpolicy): a NetworkPolicy blocks router to worker | `scripts/shared-causes.sh <atespace> <actor>` |
| 421 `misdirected request` with `X-Ate-Assignment-Stale: true` | ate-api-server: stale assignment. Only with `--upstream-connect=false` | |
| 502 `bad gateway` | Actor app: not listening. Only with `--upstream-connect=false` | `worker` log `atunnel upstream request failed` |