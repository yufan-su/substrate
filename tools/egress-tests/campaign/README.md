# Reproducing the egress gateway cost ladder, up to a million tunnels

This branch is the code that measured the egress gateway at 100k, 300k and 1,000,000 open
tunnels on October 8, 2026. It is the `egress-incluster` tooling on the fork's `egress-test` base
(`47ed2247`, upstream `601c03ba` plus the mitm_internal breaker and the atunnel tunnel-open
timeout), plus four commits:

- `gvisor: move the sandbox asset to release 20260907.0`. The 2026-09-02 nightly kills 1 to 8% of
  the sandboxes under load, see https://github.com/agent-substrate/substrate/issues/2377.
- `atenet-egress: size the gateway for a million tunnels`. The gateway ConfigMap values the ladder
  ran with, which were only ever applied to the live cluster before.
- `egress-tests: give the campaign Pod 8Gi requested and 48Gi limit`.
- This README, the gateway pin patch and the node system configs.

What it reproduces, HTTP keep-alive, one request per 100 ms per actor, settled window:

| rung | actors x destinations | tunnels | req/s | gateway cores per 1000 req/s | Envoy heap | settled p99 |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| 1 | 100 x 1000 | 100,000 | 977 | 0.82 | 8.6 GiB | 5.6 ms |
| 2 | 300 x 1000 | 298,000 | 2,910 | 0.81 | 23.0 GiB | 6.3 ms |
| 3 | 1000 x 300 | 285,300 | 9,282 | 0.77 | 21.8 GiB | 4.5 ms |
| 4 | 1000 x 1000 | 1,000,000 | 9,700 | 0.86 | 75.9 GiB | 12.6 ms |

Memory is 0.076 to 0.078 MiB of Envoy heap per open tunnel. The gateway pod reached 82.7 GiB of
working set at rung 4. The full write-up is the measurement note that accompanies this branch.

## 1. Cluster

A GKE cluster with Dataplane V2 and three node pools. The default pool runs the control plane
(six c3-standard-4 were enough). Create the other two one at a time; a concurrent create leaves the
cluster RECONCILING.

```
gcloud container node-pools create gateway --cluster CLUSTER --zone ZONE \
  --machine-type c3-highmem-22 --num-nodes 1 --disk-type pd-balanced --disk-size 100 \
  --node-labels ate.dev/pool=gateway,ate.dev/substrate-version=TAG \
  --node-taints ate.dev/gateway=true:NoSchedule \
  --system-config-from-file tools/egress-tests/manifests/node-system-config-gateway.yaml
gcloud container node-pools create workers --cluster CLUSTER --zone ZONE \
  --machine-type c3-standard-22 --num-nodes 8 --disk-type pd-balanced --disk-size 100 \
  --node-labels ate.dev/pool=workers,ate.dev/substrate-version=TAG \
  --system-config-from-file tools/egress-tests/manifests/node-system-config-workers.yaml
```

`TAG` is the version label the installer puts on nodes; atelet's DaemonSet selects on it, so the
workers must carry it and the gateway's taint keeps atelet off the gateway. The system configs raise
`fs.nr_open`, `fs.file-max` and the conntrack table; GKE applies them at node creation only. The
gateway node needs about 90 GiB free for Envoy at a million tunnels. c3-highmem-22 has 162.7 GiB
allocatable and ended rung 4 with 119 GiB available.

## 2. Install Substrate from this branch

Build the images from this checkout and install as `README.md` and `CONTRIBUTING.md` describe
(`make build-images`, `hack/install-ate.sh`). Then check the two things this branch changes:

```
kubectl get sandboxconfig gvisor-default -o jsonpath='{.spec.assets.amd64.gvisor.url}'
# gs://gvisor/releases/release/20260907.0/x86_64/gvisor.tar.zstd
kubectl -n ate-system get cm atenet-egress -o yaml | grep -c 1100000
# 6
```

Do not run the ladder on the 2026-09-02 nightly or any build before gVisor `80bb741691be`.

## 3. Pin the gateway and raise its file limit

```
kubectl -n ate-system patch deployment atenet-egress --type strategic \
  --patch-file tools/egress-tests/manifests/gateway-pin-1m.yaml
kubectl -n ate-system rollout status deployment/atenet-egress
POD=$(kubectl -n ate-system get pod -l app=atenet-egress -o jsonpath='{.items[0].metadata.name}')
kubectl -n ate-system exec $POD -c envoy -- grep 'open files' /proc/1/limits
# Max open files 8388608 8388608
```

The patch moves the gateway onto the gateway pool and wraps Envoy in `ulimit -HSn 8388608` before
dropping to uid 65532 with no capabilities. A tunnel holds two file descriptors. The driver reads
Envoy's admin API on the pod IP, port 15000, through the API server proxy; this base still binds it
there. An installer redeploy of atenet-egress drops the patch; reapply it.

## 4. Deploy the workload

```
tools/egress-tests/deploy.sh --kubeconfig KUBECONFIG --context CONTEXT --deploy \
  --workers 80 --worker-memory 7Gi --actor-memory 512Mi \
  --endpoints 1000 --port-per-service --cgreader --wait-timeout 900
```

Why these values:

- A worker pod holds worker memory divided by the actor memory limit, so 7Gi at 512Mi is 14 actors
  per pod and 80 pods hold 1120. Ten 7Gi pods fit a c3-standard-22. A 1000-actor rung needs more
  than 1000 slots or the driver gives up after its five-minute resume window.
- `--port-per-service` gives every endpoint Service its own backend port. Dataplane V2 translates
  pod-to-ClusterIP traffic per packet, and 1000 Services on five shared backend ports collide on the
  post-DNAT 4-tuple. Without it rung 1 returns about 5% 503s.
- `--actor-memory 512Mi` because an actor's RSS grows about 50 MiB per own-snapshot resume cycle.

Set kube-dns to at least four replicas: patch the `kube-dns-autoscaler` ConfigMap in kube-system
with `"min":4` in its linear parameters. Golden snapshots depend on the CPU feature set of the node
they were taken on; keep all worker nodes on one machine type.

## 5. Build the campaign image and run the ladder

```
KO_DOCKER_REPO=REPO tools/egress-tests/campaign/image/build.sh --push
# prints the image by digest
tools/egress-tests/deploy.sh --kubeconfig KUBECONFIG --context CONTEXT --campaign \
  --campaign-plan ladder --campaign-id RUN --campaign-pool workers --image IMAGE@sha256:...
kubectl --context CONTEXT -n egress-tests logs -f egress-campaign
```

The runner executes the ladder from inside the cluster and stops at OWNER steps. At each one it
prints two commands: restart the gateway, then touch a go file. Run them in that order:

```
kubectl -n ate-system rollout restart deploy/atenet-egress && \
  kubectl -n ate-system rollout status deploy/atenet-egress --timeout=300s
kubectl -n egress-tests exec egress-campaign -- touch /out/RUN/go/STEP
```

The step before rung 4 asks for the 80 x 7Gi reshape; with section 4 it is already in place, so
restart and continue. The runner resumes by file presence: delete the Pod, redeploy it, and it
skips the rungs whose reports exist. To rerun a rung, rename its `.json`, `.txt` and `.gate.json`
in `/out/RUN` first.

A single rung without the plan, from the runner Pod:

```
kubectl -n egress-tests exec egress-campaign -- sh -c 'cd /out/RUN && nohup /work/egress-tests run \
  --api-endpoint api.ate-system.svc:443 --api-token-file /var/run/ateapi/token \
  --router-url http://atenet-router.ate-system.svc \
  --actors 1000 --pick first --parallel 1000 --endpoints 1000 --duration 34m \
  --request-interval 100ms --progress-interval 15s \
  --resources --resources-cgreader --resources-verify --pre-idle 30s --wait-for-cleartext-idle 8m \
  --output /out/RUN/L4-c1000-b1000.json > /out/RUN/L4-c1000-b1000.txt 2>&1 < /dev/null &'
```

For HTTPS, add `--scheme https`. Each actor's policy lists the endpoints it calls by name, so the
rule has C hostnames; `--policy-hosts wildcard` replaces the list with one pattern covering every
endpoint, which is the shape the gateway's per-tunnel policy copy does not grow with.

## 6. Reading the result

`/out/RUN/<rung>.txt` ends with the summary: the `loop` line (requests, req/s, success, new
connections), `latency`, `cpu` with `cores per 1000 req/s, settled`, and `memory` with the Envoy
heap. The `.json` holds every sample; `README.md` in the parent directory explains the report.
Copy large files with `kubectl cp --retries=5`; `kubectl exec cat` truncates files over 100 MB
without an error. Checksum against `sha256sum` run inside the Pod.

The `resources-verify` overhead checks for the cgroup reader and the driver fail at this scale by
design; nothing else should.

## 7. Known limits and failure modes

- **Suspend deadline.** The driver gives each `SuspendActor` attempt 2 minutes (`defaultBackoff` in
  `retry.go`). Suspending 1000 actors at once takes 28 to 31 s. With the earlier 30 s deadline the
  retry hit a sandbox that had just saved and exited, and ateapi marked the actor CRASHED. The
  1000 x 1000 rung of record suspended in 29.9 s and lost none; a 7-minute repeat at 30.1 s lost 45.
- **More than one gateway replica.** Spreading replicas one per node needs a pod anti-affinity on
  `kubernetes.io/hostname`; with it, a rolling restart deadlocks at the default 25% max unavailable,
  because the new pod cannot schedule while an old one holds every node. Set `maxUnavailable: 100%`
  for the restart and put it back. Replicas split the tunnels evenly but each keeps its own
  connections to every target and its own policy cache, so CPU per request rises with the count.
- **HTTPS opening phase.** With 1000 actors opening tunnels at once, the first pass sees client-side
  TLS handshake and request timeouts (about 0.01 to 0.03% of requests) and actor DNS lookups slow
  to a p50 above 60 ms. Nothing fails at the gateway; the steady window is clean.
- **gVisor before `80bb741691be`.** Sandboxes die silently during the tunnel-opening phase, stay
  RUNNING, and their snapshots fail to restore. This branch pins release 20260907.0.
- **anetd memory on small nodes.** Re-pointing 1000 Services at once took two 16 GiB system nodes
  NotReady. Change Services in batches.
- **No gateway memory limit or overload manager.** A node smaller than the gateway's heap plus 10%
  is killed by the node, not drained by Envoy.
- **Sentry logs.** ateom runs runsc without `-debug-log` or `-panic-log`. The investigation of the
  gVisor race used a one-commit ateom change that writes them under `/var/lib/ate/runsc-logs/`;
  it is on the `ateom-panic-log` branch, not here.

## 8. Cleanup

```
tools/egress-tests/deploy.sh --kubeconfig KUBECONFIG --context CONTEXT --delete-campaign --purge
tools/egress-tests/deploy.sh --kubeconfig KUBECONFIG --context CONTEXT --delete
```

Then delete or shrink the gateway and workers pools.
