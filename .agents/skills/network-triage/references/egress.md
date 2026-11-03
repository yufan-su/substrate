# Egress: actor to the outside

Path:

- DNS: actor → **sandbox network** → **DNS relay** on 169.254.17.1:53, which
  answers only names the egress policy allows → the worker pod's own resolvers.
- TCP: actor → **sandbox network** redirects it → **atunnel** on 15001 → mTLS
  CONNECT to the **egress gateway** at `atenet-egress.ate-system.svc:443` →
  `ext-proc` checks identity and policy → `envoy` MITM or cleartext leg, checked
  per request → credentials from the **credential provider** → origin.

The actor uses no proxy settings. Only HTTP, HTTPS, and TLS whose SNI matches a
`tls_passthrough` rule reach the origin.

Collect the evidence first:

```bash
scripts/egress.sh <atespace> <actor>
```

It prints the actor's own logs and egress policy, the DNS relay and atunnel lines on its
worker, the egress gateway's lines for the actor and errors for any actor, the
gateway's stats, and the credential provider's warnings and namespace policy.
Some lines are logged once per change, so widen the window, such as
`SINCE=24h`, when the problem started earlier.

Read the policy first. With no policy, all egress is denied, DNS included.
Policy edits take up to 10s to reach the egress gateway and 5s to reach the DNS
relay.

## Sandbox network

The `worker` container builds it for each actor: netns `worker-actor:<uid>`
with a /30 link (actor 169.254.17.2, gateway 169.254.17.1) over a veth on
gVisor or a tap on microVM, plus nftables rules. On gVisor the gateway end,
atunnel, the DNS relay, and the nftables rules live in a second netns,
`worker-actor:<uid>-at`. The rules allow traffic inside the /30, such as DNS to
the relay, and redirect the actor's TCP to atunnel. They reject the rest, and
nothing logs it.

| Actor sees | Root cause | Confirm |
|---|---|---|
| ping fails at once with `Destination Net Unreachable`. UDP other than DNS times out | Itself, by design: non-TCP IPv4 traffic leaving the /30 gets ICMP net-unreachable, which UDP clients ignore | Nothing logged |
| 169.254.169.254 (metadata server) or another link-local address fails at once with `no route to host` | Itself, by design: 169.254.0.0/16 and fe80::/10 get an ICMP admin-prohibited reject | Nothing logged |

If the sandbox network fails to build, the actor goes `CRASHED` on resume. See
[setup.md](setup.md#actor-never-gets-a-network).

## DNS relay

Part of atunnel, in container `worker` of the worker pod. It answers the
actor's DNS on 169.254.17.1:53 for names the egress policy allows, and returns
NXDOMAIN for the rest. Its log lines contain `dns relay` or `egress policy`.

| Actor sees | Root cause | Confirm |
|---|---|---|
| NXDOMAIN for one name | Customer config: no `http`, `https`, or `tls_passthrough` rule matches the host. Nothing logs it | Compare the name with the policy |
| NXDOMAIN for every name | Customer config: no egress policy. Or the relay hasn't fetched the policy through atelet since the actor resumed | `worker` log `Actor has no egress policy; blocking all DNS queries`, or `Could not fetch the actor's egress policy; refusing its DNS queries`. Each is logged once per change, so widen `--since`. Later fetch failures keep the cached policy and log `Could not refresh the actor's egress policy; using stale cached policy` |
| SERVFAIL or timeout for an allowed name | [Cluster network](shared-causes.md#networkpolicy): kube-dns down or failing, or a NetworkPolicy blocks DNS from worker pods | Usually nothing logs it. Check kube-dns pods and the pool namespace's NetworkPolicies. `dns relay: could not send query to upstream` appears only when the send fails, and `dns relay: could not reach any upstream` only for DNS over TCP |
## atunnel (egress)

atunnel runs in container `worker` of the worker pod, in the WorkerPool
namespace with label `ate.dev/worker-pool=<pool>`. It takes the actor's TCP on
15001 and tunnels it over mTLS to the egress gateway. Its log lines contain
`atunnel`.

The actor sees its TCP connection open, then close or reset with no bytes. It
waits up to 10s if the gateway doesn't answer. The worker logs
`atunnel failed to open egress tunnel` with `err`. That line has no actor
attributes, so match it on time and `destination`.

| Worker log | Root cause | Confirm |
|---|---|---|
| `err` has `egress: connecting to egress gateway` | Egress gateway down, or [cluster network](shared-causes.md#networkpolicy): a NetworkPolicy blocks worker to atenet-egress:443, or the worker pod can't resolve `atenet-egress.ate-system.svc` | `kubectl -n ate-system get pods -l app=atenet-egress`; kube-dns |
| `err` has `egress: reading CONNECT response: context deadline exceeded` | Egress gateway overloaded: it held the CONNECT past atunnel's 10s limit | Gateway pods and `envoy` stats |
| `err` has `egress: gateway TLS handshake` with `failed to verify certificate` | [Pod certificates](shared-causes.md#pod-certificates): the worker doesn't trust the gateway's servicedns certificate | |
| `err` has `egress: gateway TLS handshake` with `remote error: tls:` | Install config: the gateway rejected the actor certificate, because its `actor-id-ca-certs` Secret doesn't match the CA ate-api-server signs with | `kubectl -n ate-system get secret actor-id-ca-certs` |
| `Atunnel actor certificate expired` or `renewal was denied` | atelet or ate-api-server: renewal kept failing until the certificate expired, or ate-api-server denied it, usually for a stale activation | atelet and ate-api-server logs. Each line is logged once per activation |
| No line at info | ate-api-server config: no egress gateway set. The log is debug only | ate-api-server `--default-egress-gateway-address` |

**Hand-off:** if `err` has `gateway rejected CONNECT with <status>: <body>`,
atunnel reached the egress gateway and it refused. Go to Egress gateway.
## Egress gateway

atenet-egress, in `ate-system` with pods `app=atenet-egress`. Container
`ext-proc` checks actor identity and egress policy, and injects credentials.
Container `envoy` runs the MITM and cleartext legs to the origin and writes the
access log. Container `sdsmint` mints the MITM certificates.

CONNECT refused, seen in atunnel's `err`:

| `err` has | Root cause | Confirm |
|---|---|---|
| `gateway rejected CONNECT with 403 Forbidden: <body>` | The body says why: `invalid actor certificate` (the certificate failed ext-proc's check), `unknown actor` or `actor ... is <STATE>, not placed on a worker` (ate-api-server), or a bare `egress denied` (customer config: no policy or no rules) | `ext-proc` log containing `egress denied` |
| `gateway rejected CONNECT with 503` | ate-api-server unreachable, slow, or refusing the identity check or policy lookup, or the actor CA failed to load | The body says which: `egress identity check unavailable for ...`, `egress unavailable: policy lookup failed` (`ext-proc` log `egress policy lookup failed`), or `egress unavailable: actor-identity CA failed to load` |

Requests the actor sends:

| Actor sees | Root cause | Confirm |
|---|---|---|
| TLS closes at ClientHello, no HTTP response | Customer config: SNI matches no `https` or `tls_passthrough` rule, or no SNI | `envoy` JSON log with `"leg":"clienthello"`. `chain` `"denied"` is a policy denial; `null` means the 1s inspector timed out on a server-speaks-first protocol |
| `certificate signed by unknown authority` | Customer config: the actor doesn't trust the egress MITM CA | `docs/guides/egress-trust-bundle.md` |
| 403 `egress denied` | Customer config: host or port not allowed. IP-literal hosts match only a `*` rule. Default ports are 80 and 443 | `ext-proc` log `egress denied: no rule allows the destination` with `host`, `dialedPort`, `rule` |
| 403 `WebSocket egress is not supported` or `CONNECT egress is not supported` | Itself: unsupported | |
| 400, or close after about 1s | Itself: non-HTTP TCP is unsupported | `docs/guides/egress-traffic.md` |
| 502, 503, or 504 with Envoy's default body | Origin unreachable from the gateway: DNS, connect, or TLS to the origin failed | `envoy` access log `status`, `flags`, `upstream_failure` |
| 500 `egress denied` on a request that injects an `ate-secret://` credential | Install config: no credential provider (`--credential-provider='{"enabled":false}'`). Or customer config: the URI names a provider this gateway doesn't serve, or a bad URI or header name | `ext-proc` log `egress denied: policy requires credential injection but no credential provider is configured`, `egress denied: credential URI names a provider this gateway does not serve`, and similar |
| Actor JWT not injected: 403, 503, or 500 | ate-api-server: JWT mint failed | `ext-proc` log `egress denied: actor JWT mint failed` |
| 401 from the origin | Customer config: injection only replaces a header the actor already sends | Check the actor sends the placeholder header |

**Hand-off:** a 403 or 503 on a request that needs a provider credential, with
`ext-proc` log `egress denied: credential fetch failed`, comes from the
credential provider.

The egress gateway reports `atenet.router.route.duration` with
`ate.router.outcome`. The outcome follows the gRPC code of the cause first, such
as `not_found`, `timeout`, `denied`, or `failed_precondition`. Otherwise it
follows the HTTP status: `denied` for 403, `unavailable` for 503,
`resume_error` for 500.

## Credential provider

k8s-credential-provider, in `ate-system` with pods
`app=k8s-credential-provider`. It returns the secrets the egress gateway
injects. ate-setup deploys it only when `--credential-provider` includes
`{"name":"k8s.io"}`.

`ext-proc` logs `egress denied: credential fetch failed` with the `uri` for
each of these, except an empty secret, which logs
`egress denied: unusable credential`.

| Actor sees | Root cause | Confirm |
|---|---|---|
| 403 | Customer config: the atespace isn't granted the namespace, or the Secret, key, or label grant is missing | Provider log `credential request denied: atespace not permitted for namespace` or `credential request denied: secret does not match the grant`. A missing Secret or key logs no denial, so read the `err` in the `ext-proc` line |
| 503 | Itself: down, slower than 4s, or a Kubernetes API error. Or customer config: the secret is empty | Provider pods and logs |

The provider reads its namespace policy only at startup, so an edit needs a
restart. Ask before restarting it.
