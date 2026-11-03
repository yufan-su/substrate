# Setup: a networking piece isn't up

## Where the components run

Ready means little for most of these pods.

| Component | Pods | Containers | Ready means |
|---|---|---|---|
| Ingress router | `ate-system`, `app=atenet-router` | `atenet-router`, `envoy` | `atenet-router` isn't draining, and `envoy` is live. Neither checks ate-api-server, so a Ready router can still fail every request |
| Egress gateway | `ate-system`, `app=atenet-egress` | `sdsmint`, `envoy`, `ext-proc` | `ext-proc` isn't draining, and `envoy` loaded its config. `sdsmint` has no probes, so a failure shows only as restarts |
| Credential provider | `ate-system`, `app=k8s-credential-provider` | one | Its gRPC listener is open |
| atunnel, DNS relay | WorkerPool namespace, `ate.dev/worker-pool=<pool>` | `worker` | The worker registered with atelet. atunnel's ingress listeners open before that. The DNS relay and atunnel's egress listener open for each actor |
| Sandbox network | Built inside the worker pod for each actor | `worker` | Nothing. It is built on each run or restore, so a failure shows as a `CRASHED` actor |
| podcertcontroller | `podcertificate-controller-system`, `app=podcertificate-controller` | one | Not a networking component, but every pod above needs its certificates |

## Pod stuck in ContainerCreating

Every networking pod mounts pod certificates. `kubectl describe pod` shows
`FailedMount` events.

| Signal | Root cause | Confirm |
|---|---|---|
| `FailedMount` on a pod certificate or trust bundle volume | Pod certificates: podcertcontroller down, or the cluster lacks the PodCertificateRequest and ClusterTrustBundle APIs. They need GKE 1.37, or 1.36 with the beta APIs enabled | `kubectl get podcertificaterequests -A`; podcertcontroller logs |
| podcertcontroller log `Error discovering PodCertificateRequest API` or `Error discovering ClusterTrustBundle API` | Pod certificates: the cluster lacks the APIs | `kubectl api-resources \| grep -i -E 'podcertificate\|clustertrustbundle'` |
| `MountVolume.SetUp failed: unimplemented` | Pod certificates: on GKE 1.36, the node pool predates enabling the beta APIs. See [`tools/setup-gcp`](https://github.com/ai-on-gke/substrate-gke/tree/main/tools/setup-gcp) in substrate-gke | Node pool creation time |
| `FailedMount` on Secret `actor-id-ca-certs` or `egress-mitm-ca-pool` (egress gateway), or ConfigMap `k8s-credential-provider-namespace-policy` (credential provider) | Install: the object is missing | `kubectl -n ate-system get secret,cm` |

## Pod in CrashLoopBackOff

Read the last run with `kubectl logs <pod> -c <container> --previous`.

| Container | Log contains | Component | Root cause |
|---|---|---|---|
| `atenet-router` | `invalid router configuration`, `loading --ingress-client-ca-file`, `building ateapi dial options` | Ingress router | Bad flag, or an unreadable CA file or trust bundle. An unreachable ate-api-server doesn't fail startup |
| `ext-proc` | `invalid router configuration`, `loading --actor-identity-ca-file`, `building ateapi dial options`, `dial credential provider` | Egress gateway | Bad flag, `actor-id-ca-certs` without a usable `ca.crt`, or an unreadable CA file. A wrong provider address doesn't fail startup. It shows as failed injections |
| `sdsmint` | `while loading CA pool` | Egress gateway | Bad `egress-mitm-ca-pool` contents |
| k8s-credential-provider | `credprovider exited with error` with `server credentials` or `namespace policy` | Credential provider | The podidentity trust bundle (`--client-ca-file`) or the namespace policy file |
| `worker` | `Error while executing` with `configuring the atelet connection`, such as `load worker identity` | atunnel | [Pod certificates](shared-causes.md#pod-certificates): projected bundle missing or unreadable |
| `worker` | `Error while executing` with `while opening atunnel listener` or `while opening atunnel CONNECT listener` | atunnel (ingress) | Port conflict in the worker pod |
| `worker` | `Error while executing` with `configuring DNS relay`, such as `/etc/resolv.conf names no usable nameserver` | DNS relay | Cluster network: the worker pod's resolv.conf has no nameserver |
| `worker` | `Failed to register worker` | none | Worker registration: unreadable worker pod certificate, or atelet refused the request or the epoch. An unreachable atelet is retried, as in the Not Ready row below |
| ate-api-server | `Invalid --default-egress-gateway-address` | atunnel (egress) | Install config: malformed gateway address |

## Running and Ready, but not working

| Signal | Component | Root cause | Confirm |
|---|---|---|---|
| `atenet-router` or `ext-proc` logs `ATE API gRPC health check failed` every second | Ingress router or Egress gateway | ate-api-server unreachable, ate-api-server authz rejects the component's identity, or [pod certificates](shared-causes.md#pod-certificates). The error text says which | `.health` in `/statusz` on port 4040 |
| `atenet-router` logs `Router dataplane health check failed` | Ingress router | Itself: `envoy` not serving | `envoy` `/ready` and `/listeners` on 9901 |
| `atenet-router` logs `No --ingress-allowed-spiffe-ids, so the router serves no traffic` | Ingress router | Install config: ate-setup sets this only from `--ingress-allowed-spiffe-ids` (`ATE_INGRESS_ALLOWED_SPIFFE_IDS`), with no default | `/listeners` on 9901 has no `ingress_https_listener` or `connect_terminate_tls` |
| No `k8s-credential-provider` Deployment, and requests that inject an `ate-secret://` credential get 500 | Egress gateway | Install config: ate-setup ran with `--credential-provider '{"enabled":false}'` | `ext-proc` args `--credential-provider-name` |
| Worker not Ready, `/readyz` body `registered check failed` | none | Worker registration: not registered with atelet. It gets no actors | `worker` log `Could not acquire a worker epoch; will retry`; `kubectl -n ate-system logs <atelet-pod>` on that node |
| Event `NetworkPolicyConflict` | none | [Cluster network](shared-causes.md#networkpolicy): a `substrate-<pool>` NetworkPolicy owned by something else exists, so the pool runs without its policy | |

The health check log line has two `msg` keys. JSON parsers such as `jq` keep
only the second, which is the error, so grep the raw log.

## Actor never gets a network

A worker error during run or restore moves the actor to `CRASHED`, unless atelet
was unavailable or the call timed out or was canceled. Those leave the actor
assigned so a retry can finish. The reason is in `status.crash.message` of
`kubectl ate get actor -o yaml`, as `atelet <RPC>: <message>`. If it matches
none of these rows, the crash has no network cause. Triage the requests that
fail because of it by direction, in ingress.md or egress.md.

| Message contains | Component | Root cause | Confirm |
|---|---|---|---|
| `while setting up the sandbox network:` then `while creating the actor netns`, `while creating the veth pair`, `while installing the actor egress redirect`, `while opening actor egress listeners`, `while opening the actor DNS socket` | Sandbox network | Itself: usually the worker lacks `NET_ADMIN`, for example removed by an admission policy. Without `SYS_ADMIN` the worker crash-loops at startup instead | `kubectl get pod <worker-pod> -o yaml`, container `worker` securityContext |
| `while building tap`, `while building restore tap`, `setting the gateway MAC on tap`, `while configuring guest network` | Sandbox network | Itself on microVM, or the guest kata-agent | `worker` logs around the failure. The `guest network config failed; dump` line needs `--guest-debug`, which installs don't set |
| `while obtaining actor certificate:` ... `mint actor certificate` | atunnel (egress) | ate-api-server: it signs the actor certificate through atelet | ate-api-server and atelet logs |
| `wakeup probe for "<container>" never returned 200 within` | none | Actor app: not answering 200 on its probe path at 169.254.17.2 | `kubectl ate logs actor <actor> -a <atespace>` |

## Commands

```bash
scripts/setup.sh [<atespace> <actor>]
```

It describes every networking pod that isn't Running and Ready, and prints the
last run of each restarted container. It prints the router's health check
failures, its `/statusz` health and flags, and Envoy's listeners. With an
actor, it prints the actor's state and crash message, its WorkerPool's pods,
and the worker's `/readyz`. For pod certificate problems, also run
`scripts/shared-causes.sh`.
