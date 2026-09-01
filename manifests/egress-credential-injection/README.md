# Egress credential injection (POC)

A proof of concept for the Substrate Credential Provider design's **egress
credential injection** scope: transparently adding a credential (e.g.
`Authorization: Bearer <token>`) to an actor's outbound request based on an
egress policy, with the secret fetched on demand from an external store that
Substrate never persists.

## Pieces

- **`credprovider`** (`cmd/credprovider`) — a gRPC service implementing the
  `CredentialProvider.RequestSecret` plugin API (`pkg/proto/credproviderpb`),
  backed by Kubernetes Secrets. It resolves
  `substrate-secret://kubernetes.io/<provider>/<namespace>/<secret>[/<key>]`.
  It is the only component here with Kubernetes access.
- **`atenet egress-inject`** (`cmd/atenet/internal/router/egressinject`) — the
  ext_proc server the egress gateway's MITM leg dials
  (`additional_egress_ext_proc`). For each decrypted request it fetches the
  requesting actor's egress policy from `ateapi` and, on a matching rule that
  carries a credential injection, calls `credprovider` and injects the header.
- **egress policy** — the actor egress policy resource served by the `ateapi`
  `Control` API (`GetActorEgressPolicy`, `pkg/proto/ateapipb`). Each actor has at
  most one policy (named `default`); its `hostnames` rules carry the
  `inject_static_headers` effects the injector applies. Create one with
  `CreateActorEgressPolicy`.
- **namespace authorization** (`internal/proto/nsauthzpb`) — an atespace→namespace
  mapping `credprovider` enforces (default-deny) so an atespace can only resolve
  secrets from its permitted namespaces. Loaded from the
  `credprovider-namespace-policy` ConfigMap in `namespace-policy.yaml`.

## Request flow

```
actor --HTTPS--> egress gateway (MITM: terminates TLS with an sdsmint leaf)
                      |
                      | decrypted request, on the MITM leg
                      v
              atenet egress-inject  (ext_proc)
                 | GetActorEgressPolicy(actor from ate.actor.identity)
                 | match Host against the policy's rules
                 v
              credprovider.RequestSecret(uri, context)
                 | read K8s Secret
                 v
              header mutation: Authorization: Bearer <secret>
                      |
                      v
              re-originated TLS to the real origin, credential attached
```

Actor identity comes from the CA-signed client cert the gateway verified on the
CONNECT leg, relayed to the injector as the `ate.actor.identity` filter-state
attribute — never from a client-supplied header.

The same injector runs on both the MITM leg's TLS chain (HTTPS) and its cleartext
chain (plaintext HTTP). Because the cleartext path re-originates without upstream
TLS, injecting a credential there sends it in the clear to the origin — so the
injector **always refuses** to inject a credential on a cleartext request and
fails closed. (The actor egress policy API models no per-rule cleartext opt-in.)

## Install

One flag deploys the whole stack (provider, injector, namespace policy, sample
secret) and wires the sdsmint egress gateway to the injector:

```
hack/install-ate.sh --experimental-egress-credential-injection
```

It implies `--experimental-use-sdsmint` and requires the (default)
`--atenet-router=envoy`. To deploy the pieces by hand instead, see the manifests
in this directory and the underlying
`--experimental-additional-egress-extproc-service ate-system/atenet-egress-inject:50051`
flag.

## Verify

First give the actor an egress policy through the `ateapi` `Control` API. Create
one `default` policy for atespace `team-a`, actor `my-actor` with a `hostnames`
rule for `api.example.com` whose `inject_static_headers` effect sets header
`Authorization`, prefix `Bearer `, and credential URI
`substrate-secret://kubernetes.io/team-secrets/ns1/example-api`
(`CreateActorEgressPolicy`). The change takes effect immediately — the injector
fetches the policy per request, no restart needed.

Then, from that actor (provisioned to trust the MITM CA, see
`demos/egress/egress-mitm.yaml.tmpl`):

```
curl https://api.example.com/anything
```

The origin should see `Authorization: Bearer poc-example-api-token`. A request to
a host not covered by the policy is passed through unchanged (the injector is
started with `--on-no-match=allow`).

A plaintext `curl http://api.example.com/anything` is always denied 403 (the
origin never sees the credential; there is no cleartext opt-in). A request whose
atespace is not granted the secret's namespace in `namespace-policy.yaml` is
denied by `credprovider`, and the injector fails closed (503).

The resolved secret is sanitized before it becomes a header value: a trailing
newline (common when a Secret is created from a file) is trimmed, and a secret
that is empty or contains a control character — which Envoy would reject and, for
CR/LF, could turn into header injection — fails closed (503) rather than being
sent as a bare `Bearer ` or a malformed header.

## Alternative backend: Google Secret Manager (`gsmcredprovider`)

`credprovider` is one implementation of the `CredentialProvider` plugin API.
`gsmcredprovider` (`cmd/gsmcredprovider`) is a second, backed by Google Cloud
Secret Manager instead of Kubernetes Secrets. It is a drop-in alternative: the
injector dials a single provider address, so a deployment points
`--credential-provider-address` at whichever backend it wants — the two are not
run side by side yet (a future step routes by URI class so both can serve at
once, which is why each backend claims a distinct provider class).

- **URI form**: the path is the Secret Manager resource name,
  `substrate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>/versions/<version>`.
  The `/versions/<version>` tail is optional and defaults to `latest`; a version
  is a positive integer or the `latest` alias.
- **Authorization**: none in the provider (POC). Any request resolves any secret
  the provider's own Secret Manager access permits — authorization is delegated
  entirely to the IAM granted to its identity. (Unlike `credprovider`, there is
  no atespace→project mapping; keeping it was judged too heavy for the POC.)
- **GCP access**: Application Default Credentials. In-cluster this is a GKE
  Workload Identity binding on the `gsmcredprovider` ServiceAccount to a Google
  service account holding `roles/secretmanager.secretAccessor` on the served
  secrets (per-secret) or project (project-wide). It needs **no** Kubernetes
  Secret RBAC.
- **Integrity**: when Secret Manager returns a payload CRC32C, the provider
  verifies it and fails closed on a mismatch.

To use it instead of the Kubernetes backend, deploy `gsmcredprovider.yaml`
(after setting the `iam.gke.io/gcp-service-account` annotation and creating the
matching IAM binding) and repoint the injector:

```
--credential-provider-address=gsmcredprovider.ate-system.svc:50051
--provider-server-name=gsmcredprovider.ate-system.svc
```

Then reference a Secret Manager URI in the actor egress policy's
`inject_static_headers` credential, e.g.
`substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/egress-creds/versions/latest`.
The cleartext refusal, credential sanitization, and fail-closed behavior
described above are the injector's and apply unchanged regardless of backend.

## Credential format: host-keyed credential sets

A credential is not a bare token but a **credential set**: a JSON object mapping
a destination host to the complete HTTP headers to inject for requests to that
host.

```json
{
  "github.com": {
    "Authorization": "Bearer <literal token>",
    "X-Custom-Header": "value"
  },
  "api.example.com": {
    "X-Api-Key": "<key>"
  }
}
```

Store that JSON as the secret value (e.g. a Secret Manager secret version, or a
Kubernetes Secret key). The injector, on a matching hostname rule, fetches the
set through the provider, looks up the entry for the request's destination host,
and injects **all** of that entry's headers with their literal values. This is
why a single policy rule + one credential set can serve many hosts, and why the
provider (`gsmcredprovider` / `credprovider`) needs no knowledge of headers — it
just returns the JSON blob.

Consequences for the egress policy: the `inject_static_headers` entry's `header`
and `prefix` fields are **ignored** — header names and values come entirely from
the set — so `header` is set to a placeholder (e.g. `X-Substrate-Credential-Set`)
only to satisfy the required-field validation, and `credential_uri` points at the
credential set. A matched host that has **no** entry in the set passes through
uninjected (the rule still authorizes egress); a set that is missing, unparseable,
or holds an unusable header name/value fails closed.

## POC simplifications (not production-ready)

- **Attestation**: the injector passes the actor's SPIFFE URI as the attested
  `actor_identity`. Production should pass a verifiable Actor JWT (a `MintJWT`
  RPC exists but is not yet integrated) so the provider can independently verify
  the caller's assertion.
- **No caching**: both the egress policy and the credential are fetched per
  request — the policy from `ateapi`, the credential from `credprovider`. A real
  deployment needs caching (with a TTL/revocation story) to bound the load each
  egress request puts on the control plane and the provider.
- **Broad RBAC**: `credprovider` enforces the atespace→namespace mapping in
  process, but is still granted `get secrets` cluster-wide. The defense-in-depth
  follow-up is to scope the RBAC to the served namespaces so the mapping is not
  the only gate.
- **Static namespace mapping**: the namespace mapping is loaded once at startup,
  so editing the `credprovider-namespace-policy` ConfigMap requires restarting
  `credprovider`; a watch-based reload is a follow-up. (The egress policy is now
  live via `ateapi` and needs no restart.)
- **Blast radius**: injected secrets are plaintext in the shared gateway data
  plane. See the design's "sealed credentials (AEAD)" note.
- **`ip_blocks` rules are not evaluated**: the injector matches on hostname and
  has no original destination IP on the MITM leg, so it cannot evaluate an
  `ip_blocks` egress rule and currently skips it (treated as non-matching). A
  policy that orders an `ip_blocks` rule before a hostname rule is therefore not
  faithfully reproduced here — the control plane's first-match ordering may
  differ. Avoid pairing `ip_blocks` rules with credential injection until the
  destination IP is plumbed to this leg.
- Only the `kubernetes.io` provider class and the egress-injection scope are
  implemented.
