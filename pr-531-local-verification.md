# PR #531 — Local verification report

**PR:** [agent-substrate/substrate#531](https://github.com/agent-substrate/substrate/pull/531) — *atenet: pick up refreshed pod certificate*
**Branch tested:** `router-cert-sds-reload` (PR head)
**Date:** 2026-07-27
**Environment:** local `kind` cluster (single control-plane node), Envoy `envoyproxy/envoy:v1.30-latest`

---

## 1. What the change does

The atenet router's Envoy HTTPS listener previously embedded the serving cert as an **inline file `DataSource`** — Envoy read the file **once at listener creation**, so when kubelet rotated the projected `servicedns` pod certificate the running Envoy kept the stale cert and it expired after ~24h.

PR #531 switches the listener to **SDS** (secret `https_serving_cert`, delivered over ADS) and attaches a `WatchedDirectory` = `/run/servicedns.podcert.ate.dev`, so Envoy re-reads the bundle when the projected volume rotates. It also guards the HTTPS listener so it is skipped entirely when `--envoy-cert-path` is empty (an empty-filename SDS secret would be NACKed by Envoy).

**Goal of verification:** prove Envoy serves a freshly-rotated cert **without a restart**.

Relevant code: `cmd/atenet/internal/router/xds.go` (`buildHttpsListener`, `buildTlsSecret`, `HTTPSCertSecretName`).

---

## 2. Summary of results

| Check | Result |
|---|---|
| Envoy receives SDS secret with `watched_directory` | ✅ confirmed |
| Envoy hot-reloads a rotated pod cert (new serial, **0 restarts**) | ✅ confirmed |

**Evidence of hot-reload** (from the rotation watcher):

| time (UTC) | served serial | valid_from | envoy_restarts |
|---|---|---|---|
| 00:42–00:58 | `4420a1547bd9a6017667c0f1aa8235691165fd17` | 00:42:28Z | 0 |
| **00:59:13 →** | **`4cfe86e09cfcaf9a4be2ff4ef728445d2c530091`** | **00:55:31Z** | **0** |

The served serial changed while the envoy container's restart count stayed at `0` — Envoy re-read the rotated cert in place. On `main` (inline-file cert) the serial would have stayed pinned until expiry.

---

## 3. Prerequisites / setup

### 3.1 Check out the PR
```bash
git fetch origin pull/531/head:pr-531 && git switch pr-531
```

### 3.2 Stand up kind + install the stack
```bash
./hack/create-kind-cluster.sh          # kind cluster + local registry :5001 + feature gates
./hack/install-ate-kind.sh --deploy-ate-system
```
`install-ate-kind.sh` is a thin wrapper around `install-ate.sh` that exports the
kind-specific env for you (`ATE_INSTALL_KIND=true`, `KO_DOCKER_REPO=localhost:5001`,
`KO_DEFAULTPLATFORMS=linux/$(go env GOARCH)`, `NO_DEV_ENV=true`,
`BUCKET_NAME=ate-snapshots`), unsets GCP vars, then forwards args to `install-ate.sh`.

---

## 4. Forcing a fast cert rotation (test-only signer edits)

By default the `servicedns` signer issues **24h** certs and starts refresh **30 min before expiry**, so a natural kubelet-driven rotation would take ~23.5h — impractical to watch. To force a fast rotation we temporarily shortened the cert lifetime in
`cmd/podcertcontroller/internal/servicednssigner/servicednssigner.go`.

> ⚠️ **Two Kubernetes `PodCertificateRequest` API floors constrain how short you can go** — both must be satisfied or every `servicedns` cert is refused and the dependent pods hang in `ContainerCreating` ("credential bundle is not issued yet"):
> 1. **leaf lifetime ≥ 1 hour** — a 40-min cert is rejected: `leaf certificate lifetime must be >= 1 hour`.
> 2. **`beginRefreshAt` ≥ `notBefore + 10m`** — otherwise: `status.beginRefreshAt ... must be at least 10 minutes after status.notBefore`.

Edits used (revert after testing — see §7):

```go
// line ~156
lifetime := 65 * time.Minute            // was: 24 * time.Hour   (>= 1h floor)

// line ~164
beginRefreshAt := notBefore.Add(12 * time.Minute)  // was: notAfter.Add(-30 * time.Minute)
```

**Resulting timing** (all derived from one `now = clock.Now()`):

| value | vs `notBefore` | vs `now` (issue time) |
|---|---|---|
| `notBefore` = `now − 2m` | 0 | −2m |
| `beginRefreshAt` = `notBefore + 12m` | +12m (≥10m ✅) | **+10m** |
| `notAfter` = `notBefore + 65m` | +65m (≥1h ✅) | +63m |

→ kubelet begins refreshing ~10 min after each issue, so the cert **rotates roughly every ~10 min**.

Redeploy just the controller to pick up the edit:
```bash
KO_DOCKER_REPO=localhost:5001 ./hack/run-tool.sh ko apply -f manifests/ate-install/pod-certificate-controller.yaml
kubectl -n podcertificate-controller-system rollout status deploy/podcertificate-controller
```

Confirm the PCRs are issued and pods are up:
```bash
kubectl get podcertificaterequests -A -o wide | grep servicedns   # all should be Issued
kubectl -n ate-system get pods                                     # ate-api-server / atenet-router / valkey Running
```

---

## 5. Verifying the SDS wiring in the running Envoy

The `envoy` container image has no `curl`, so reach the admin API from the host via `port-forward`.

```bash
POD=$(kubectl -n ate-system get pod -l app=atenet-router -o name | head -1)
kubectl -n ate-system port-forward "$POD" 9901:9901 &   # admin on :9901
```

**a) SDS secret + watched directory** (the core of the PR):
```bash
curl -s "localhost:9901/config_dump?resource=dynamic_active_secrets" | python3 -m json.tool
```
Observed: secret `https_serving_cert` delivered dynamically with
`"watched_directory": { "path": "/run/servicedns.podcert.ate.dev" }`.

**b) Currently loaded serving cert** (serial + validity):
```bash
curl -s "localhost:9901/certs" | python3 -m json.tool \
  | grep -iE "serial_number|valid_from|expiration_time"
```
Observed baseline: serial `4420…fd17`, valid `00:42:28Z → 01:47:28Z` (65-min lifetime, confirming the signer edit).

**c) Baseline envoy restart count** (must stay 0 across rotation):
```bash
kubectl -n ate-system get "$POD" \
  -o jsonpath='{range .status.containerStatuses[*]}{.name}{" restarts="}{.restartCount}{"\n"}{end}'
```

---

## 6. Verifying hot-reload across a rotation

Watcher used (keeps the port-forward alive, polls the served serial + envoy restart count every 20s for ~15 min, flags serial changes):

```bash
cat > /tmp/watch-cert-rotation.sh <<'EOF'
set -u
POD=$(kubectl -n ate-system get pod -l app=atenet-router -o name | head -1); POD=${POD#pod/}
echo "watching pod=$POD"
( while true; do kubectl -n ate-system port-forward "pod/$POD" 9901:9901 >/dev/null 2>&1; sleep 1; done ) &
PF=$!; trap 'kill $PF 2>/dev/null' EXIT
sleep 3; prev=""; end=$((SECONDS+900))
while [ "$SECONDS" -lt "$end" ]; do
  j=$(curl -s localhost:9901/certs 2>/dev/null)
  serial=$(printf '%s' "$j" | jq -r '.certificates[0].cert_chain[0].serial_number // empty')
  vfrom=$(printf '%s' "$j" | jq -r '.certificates[0].cert_chain[0].valid_from // empty')
  rc=$(kubectl -n ate-system get "pod/$POD" -o jsonpath='{.status.containerStatuses[?(@.name=="envoy")].restartCount}' 2>/dev/null)
  ts=$(date -u +%H:%M:%SZ)
  if [ -n "$serial" ] && [ "$serial" != "$prev" ]; then
    echo "$ts  *** SERIAL CHANGE ***  serial=$serial valid_from=$vfrom envoy_restarts=$rc"; prev=$serial
  else
    echo "$ts  serial=${serial:-none} valid_from=${vfrom:-?} envoy_restarts=${rc:-?}"
  fi
  sleep 20
done
echo "done watching"
EOF
bash /tmp/watch-cert-rotation.sh
```

> Paste the `cat … <<'EOF' … EOF` block whole. If the shell reports `unexpected EOF while looking for matching '"'`, the copy introduced curly "smart quotes"; check with `grep -n '[”“’]' /tmp/watch-cert-rotation.sh`.

**Pass criteria:** the served serial changes to a new value while `envoy_restarts` stays `0`.

**Observed** (abridged log):
```
00:53:49Z  serial=4420a1547bd9a6017667c0f1aa8235691165fd17 valid_from=2026-07-28T00:42:28Z envoy_restarts=0
...
00:58:52Z  serial=4420a1547bd9a6017667c0f1aa8235691165fd17 valid_from=2026-07-28T00:42:28Z envoy_restarts=0
00:59:13Z  *** SERIAL CHANGE ***  serial=4cfe86e09cfcaf9a4be2ff4ef728445d2c530091 valid_from=2026-07-28T00:55:31Z envoy_restarts=0
01:03:15Z  serial=4cfe86e09cfcaf9a4be2ff4ef728445d2c530091 valid_from=2026-07-28T00:55:31Z envoy_restarts=0
```

✅ Serial changed `4420…fd17 → 4cfe…0091`, `envoy_restarts` stayed `0`. Hot-reload confirmed.

---

## 7. Cleanup

Revert the test-only signer edits before committing:
```go
// cmd/podcertcontroller/internal/servicednssigner/servicednssigner.go
lifetime       := 24 * time.Hour                    // restore
beginRefreshAt := notAfter.Add(-30 * time.Minute)   // restore
```

Tear down:
```bash
./hack/install-ate.sh --delete-all
kind delete cluster
docker rm -f kind-registry
git switch - && git branch -D pr-531
```

---

## Appendix: notes for reproduction

- **`KO_DOCKER_REPO=localhost:5001`** is required in any shell that runs `ko` **directly** against the kind-local registry — e.g. the manual controller redeploy in §4. `install-ate-kind.sh` exports it for you, but a bare `ko apply` in a fresh shell does not. Without it, ko targets the default GCR repo.
- All `servicedns`-serving pods (ate-api-server, atenet-router, valkey) depend on the `servicedns` signer, so the §4 lifetime/`beginRefreshAt` values must satisfy both API floors — otherwise the signer can't issue and those pods stay in `ContainerCreating`.
- kubelet does not refresh exactly at `beginRefreshAt` — it polls, so expect a lag of a minute or two (observed: `beginRefreshAt` ≈ 00:54:28Z, actual rotation reflected in Envoy at 00:59:13Z).
