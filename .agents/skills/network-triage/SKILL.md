---
name: network-triage
description: Finds which Agent Substrate networking component a customer's network problem comes from. Use when a support engineer has a ticket about Substrate networking, such as requests to an actor failing with an error or timeout, an actor's outbound calls or DNS failing, networking pods not running, or an actor crashing on resume. Sorts the issue into setup, ingress, or egress, walks that path hop by hop, names the component and root cause with evidence, and checks the repo's issues for a known match. Read-only.
---

# Network Triage

Name the networking component the customer's problem comes from, its root
cause, and the evidence, and give next steps.

Rules:

1. **Read-only.** Run only the scripts in `scripts/`, `get`, `describe`, `logs`,
   `top`, admin-port GETs, and `gcloud` `describe` and `list`.
   Ask before anything that changes state, including Envoy admin POSTs such as
   `/logging`, restarts, and `kubectl ate` lifecycle commands.
2. **Cite evidence.** Back each conclusion with the status and body the caller
   saw, a log line, or a config value. Label guesses as guesses.
3. **Rule out hops in order.** If the evidence doesn't single out one component,
   list the hops you cleared and the data that would decide between the rest.
4. **Protect customer data.** The upstream repo is public. Replace atespace and
   actor names, IPs, and hostnames before drafting a GitHub issue.

This skill covers the default Envoy dataplane. The agentgateway dataplane
(`ate-setup --atenet-dataplane=agentgateway`, `manifests/ate-install/components/agentgateway`)
runs one `agentgateway` container in both atenet-router and atenet-egress. It
replaces `envoy` and `atenet-router` in the router, and `envoy`, `ext-proc`, and
`sdsmint` in the egress gateway, so the Envoy and ext_proc steps don't apply.

## Components and root causes

The **component** is the networking piece where the request failed: Ingress
router, atunnel, DNS relay, Egress gateway, Credential provider, or Sandbox
network. It is the piece that returned the error or dropped the connection,
even when the cause is elsewhere: a 503 the ingress router returns because the
actor is `CRASHED` is component Ingress router, root cause ate-api-server. Each
reference file describes the components on its path. The component is none
only when no networking piece is involved, such as a worker that never
registers.

The **root cause** is why it failed: the component itself, or a cause outside
networking listed in [references/shared-causes.md](references/shared-causes.md).

## 1. Collect facts

Ask the user for what's missing:

- Who was calling whom: a caller reaching an actor, or the actor reaching out,
  including DNS.
- The exact status code and body, or the client error text (curl output, TLS
  error, `Could not resolve host`).
- Atespace, actor, time window, sandbox class (`gvisor` or `microvm`).
- Scope: one actor, one WorkerPool, or every actor.
- What changed just before it started, such as an install or upgrade, a new
  NetworkPolicy, a CNI change, or an egress policy edit. Recent changes are the
  most likely cause.

The scripts in `scripts/` collect the evidence for each step. They only read,
print each command before its output, and keep going when one command fails.
`SINCE` sets how far back logs go (default `1h`), and `MAX_LINES` caps each log
section (default 200). If you can't reach the cluster, send the script to the
customer and ask for its output.

Then record the cluster:

```bash
scripts/cluster.sh [<gke-cluster> <gke-location>]
```

- Substrate version, from the image tags. A from-source install has
  digest-only images and no tag.
- Kubernetes version of the control plane and of each node pool. Pod
  certificates need GKE 1.37, or 1.36 with the beta APIs enabled. On 1.36,
  nodes created before the APIs were enabled fail pod certificate mounts with
  `MountVolume.SetUp failed: unimplemented` until they are replaced.
- Platform and CNI. On GKE, `ADVANCED_DATAPATH` is Dataplane V2, which
  enforces NetworkPolicy. On the legacy datapath, NetworkPolicy is enforced
  only if `networkPolicy.enabled` is true. Elsewhere, find the CNI pods in
  `kube-system`.
- Sizing: node count, machine types, architecture, zones, WorkerPools, and
  worker usage from the `ACTORS` column of `kubectl ate get workers`.

## 2. Check setup

A component that isn't up looks like a request failure further down the path.
Check that first:

```bash
scripts/setup.sh [<atespace> <actor>]
```

Ready doesn't mean working: the router stays Ready when it can't reach
ate-api-server. If a pod isn't Running and Ready, the router logs health check
failures, or the actor is `CRASHED` with a network error in
`status.crash.message`, read [references/setup.md](references/setup.md). The
category is **setup**. A `CRASHED` actor without a network error isn't a setup
problem: triage the failing requests in step 3.

## 3. Pick the direction

Each piece on the path writes its errors in its own form, so the first error
the customer saw tells you where to start.

| First signal | Category | Start at |
|---|---|---|
| No HTTP response from the router: connection refused, or a TLS failure | ingress | ingress.md § Ingress router |
| A short `text/plain` body naming the actor, such as `actor … unavailable`, `error resuming actor …`, or `invalid actor reference` | ingress | ingress.md § Ingress router |
| An empty 500 or 504, or `upstream request timeout` after about 5 minutes | ingress | ingress.md § Ingress router |
| Envoy's `upstream connect error …` body, or a 503 after the router logged `Route ok` | ingress | ingress.md § atunnel (ingress) |
| The actor's DNS fails: `bad address`, `Could not resolve host`, NXDOMAIN, SERVFAIL | egress | egress.md § DNS relay |
| ping, UDP other than DNS, or the metadata server fails | egress | egress.md § Sandbox network |
| The actor's TCP connection opens, then closes with no data | egress | egress.md § atunnel (egress) |
| The actor gets an HTTP error from the gateway, such as 403 `egress denied`, or its TLS handshake fails | egress | egress.md § Egress gateway |
| Errors only on requests that inject a credential | egress | egress.md § Egress gateway, then § Credential provider |
| The actor's own error, and nothing fails on either path | none | Actor app |

[references/ingress.md](references/ingress.md) covers the ingress router and
atunnel (ingress). [references/egress.md](references/egress.md) covers the
sandbox network, the DNS relay, atunnel (egress), the egress gateway, and the
credential provider. Run the script at the top of the file first.

**When the request reaches the actor.** A caller's request can fail because of
the actor's own outbound call: an agent that can't reach its model API returns
an error to its caller. The ingress hop worked: the router logged `Route ok`,
and the actor's logs show the request arriving and its outbound call failing.
Both scripts print the actor's logs. Triage the outbound call: the category is
egress, and the caller's error is only evidence.

In that file, start at the row that matches what the caller saw. Confirm with the log line
before you name the component. Follow the hand-offs at the end of each section.
NetworkPolicy and pod certificates break hops in both directions; they are in
[references/shared-causes.md](references/shared-causes.md).

## 4. Check known issues

Search the repo's issues and merged PRs with the exact error text or log
message, then the component and symptom in plain words. For each phrase, the
script searches issues labeled `area/network`, then all issues, because not
every issue has a label, then merged PRs. `--view` prints one issue or PR with
its comments, and the merge commit of a merged PR.

```bash
scripts/known-issues.sh '<error text>' '<log message>' '<symptom in plain words>'
scripts/known-issues.sh --view <number>
```

The script uses `gh` when it is installed and logged in. Otherwise it calls the
GitHub API with `curl`, which allows 10 searches a minute, about three phrases.
Set `GITHUB_TOKEN` to raise that.

Sort each candidate:

- **Match:** same component, symptom, and root cause. If a merged PR fixes it,
  find the first release that has the fix with
  `git tag --list 'v*' --contains <merge-sha> --sort=v:refname | head -1`, after fetching
  upstream tags. Compare it with the customer's version.
- **Similar:** same component or symptom, but a different trigger, version, or
  root cause. Say what differs. It can still give a workaround or the owner.
- **None:** list the searches you ran.

Don't comment on or open issues without approval.

## 5. Know the gaps

- atunnel and the DNS relay have no metrics. Use logs. The egress gateway's
  ext_proc denials count in `atenet.router.route.duration` with
  `ate.router.outcome=denied`. A TLS connection refused at the ClientHello
  shows only in the `leg: clienthello` access log.
- The DNS relay logs neither a name the policy denies nor an upstream that
  doesn't answer. Compare the name with the egress policy.
- The worker logs at info. atunnel's stale-assignment reject and "egress not
  active" lines are debug only.
- The egress gateway's per-request logs (`leg: mitm`, `leg: cleartext`) keep
  only status 400 and above, response flags, non-OK gRPC, and a 1% sample.
  Envoy `runtime_modify` can't change this: it answers 503
  `No admin layer specified`. The CONNECT log (`[egress] authority=...`) and
  `leg: passthrough` record every tunnel. `leg: clienthello` records only
  connections that matched no filter chain (`NR`).
- The worker image has no shell, so `kubectl exec` into it doesn't work.

## 6. Report

```
Category:    setup | ingress | egress
Component:   Ingress router | atunnel (ingress) | atunnel (egress) | DNS relay | Egress gateway | Credential provider | Sandbox network | none
Root cause:  the component itself, or a root cause from shared-causes.md
Caller saw:  <status, body, or error text, or the pod status>
Cluster:     <platform and version, CNI, nodes and machine types, WorkerPools>, Substrate <tag>
Evidence:    log lines and config, each with its pod and container
Ruled out:   hops checked and cleared
Known issue: #<number> match | similar | none found; fixed in <tag>, customer runs <tag>
Confidence:  high | medium | low
Next steps:  for the support engineer; for the customer, with commands or a workaround
Owner:       area/network for any component, or the root cause's owner in shared-causes.md
```
