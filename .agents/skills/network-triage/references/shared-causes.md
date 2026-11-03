# Shared causes

The root cause is why the request failed. It is either the component itself or
one of these. The tables in ingress.md, egress.md, and setup.md use these names.

| Root cause | Examples | Owner |
|---|---|---|
| ate-api-server | Resume fails, actor `CRASHED`, stale worker assignment, actor certificate or JWT not issued | `area/api-machinery` |
| WorkerPool capacity | No worker has room for the actor | `area/scheduling` |
| Worker registration | The worker can't register with atelet on its node, so it gets no actors | `area/node` |
| Pod certificates | mTLS between components fails: podcertcontroller, trust bundles | `area/security`, `area/identity` |
| Cluster network | NetworkPolicy, CNI, kube-dns | customer, or `area/network` |
| Install config | ate-setup flags, a missing or mismatched Secret or ConfigMap | customer, or `area/network` |
| Customer config | EgressPolicy rules, actor doesn't trust the MITM CA, credential placeholder header missing | customer |
| Actor app | Not listening on the port it is called on | customer |
| Caller | Missing or malformed `ate-target-actor`, untrusted client certificate | customer |

Cluster network and pod certificates break hops in both directions, so the rest
of this file covers them. The component is the hop they block.

## NetworkPolicy

Root cause "Cluster network". It blocks atunnel (ingress) for router to worker,
atunnel (egress) for worker to egress gateway, and the DNS relay for relay to
kube-dns. Blocking DNS from worker pods also breaks atunnel (egress), which
resolves the egress gateway's name.

- ate-controller creates `substrate-<pool>` in each WorkerPool namespace. It
  allows ingress on all ports only from `app=atenet-router` in ate-controller's
  namespace. A router in another namespace or without that label can't reach
  workers.
- If a NetworkPolicy owned by something else has that name, ate-controller
  emits event `NetworkPolicyConflict`
  (`refusing to take over NetworkPolicy`). The pool then runs without its
  policy. That is a security gap, not an outage.
- `ate-system` has `default-deny-ingress`. The ingress router's `https` and
  `connect-tls` ports accept any source. The egress gateway's `https` accepts
  only pods labeled `ate.dev/worker-pool`, from any namespace.
- Substrate ships no egress policies. A customer default-deny egress policy in
  a WorkerPool namespace breaks worker to atenet-egress:443 and DNS from the
  worker pod, which the DNS relay and atunnel both need. In `ate-system` it
  also breaks router to worker 8443 and most of the control plane, such as
  router and egress gateway to ate-api-server, ate-api-server to Postgres, and
  DNS for every pod.
- An extra customer ingress policy only adds allow rules, so it breaks nothing.
- If the CNI doesn't enforce NetworkPolicy, none of this applies and mTLS alone
  protects the traffic.

```bash
scripts/shared-causes.sh [<atespace> <actor>]
```

It prints every NetworkPolicy, `NetworkPolicyConflict` events, and, with an
actor, the `substrate-<pool>` policy of the actor's WorkerPool. It also prints
the pod certificate checks below.

## Pod certificates

Root cause "Pod certificates". The ingress router and atunnel authenticate each
other with podidentity pod certificates from podcertcontroller. The egress
gateway serves a servicedns pod certificate. atunnel presents the actor's
certificate to it, which ate-api-server issues through atelet. The gateway's
access log shows it as
`peer_san=spiffe://substrate-actor.local/worker-for-actor/<atespace>/<actor>`.
If the certificate isn't issued, the root cause is ate-api-server. If the
gateway rejects it, its `actor-id-ca-certs` Secret doesn't match the CA
ate-api-server signs with, and the root cause is install config.

SPIFFE IDs look like `spiffe://cluster.local/ns/<ns>/sa/<sa>`. atunnel accepts
only `spiffe://cluster.local/ns/ate-system/sa/atenet-router` by default.
Certificates reload without a restart: atunnel ingress on each connection,
Envoy on file change. atunnel's egress client reads the gateway's trust bundle
once per actor activation.

`scripts/shared-causes.sh` prints the trust bundles, every
PodCertificateRequest, and podcertcontroller's
`Error while handling PodCertificateRequest` lines.
