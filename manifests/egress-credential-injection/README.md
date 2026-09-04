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
  requesting actor's egress policy from `ateapi` and, on a matching rule, asks the
  provider for each named header's value (passing the credential URI, the
  destination, and the header name) and injects `prefix + value`. The injector is
  not an egress authorization gate: a request with no matching injection passes
  through unchanged.
- **egress policy** — the actor egress policy resource served by the `ateapi`
  `Control` API (`GetActorEgressPolicy`, `pkg/proto/ateapipb`). Each actor has at
  most one policy (named `default`); its `hostnames` rules carry the
  `inject_static_headers` effects the injector applies. Create one with
  `CreateActorEgressPolicy`.
- **namespace authorization** — an atespace→namespace mapping `credprovider`
  enforces (default-deny) so an atespace can only resolve secrets from its
  permitted namespaces. Loaded from the `credprovider-namespace-policy` ConfigMap
  in `namespace-policy.yaml`.

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
              provider.RequestSecret(uri, {destination, header})
                 | resolve the value (K8s Secret value / Secret Manager set entry)
                 v
              header mutation: <header>: <prefix><value>
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
`--atenet-router=envoy`. Two flags configure which credential provider the
injector uses:

- `--credential-provider-name NAME` — the provider class the injector serves, as
  a `substrate-secret://` class prefix (e.g. `substrate-secret://kubernetes.io`);
  a policy credential URI of any other class is refused. Default
  `substrate-secret://kubernetes.io`.
- `--credential-provider-address HOST:PORT` — where the injector dials the
  provider. Default `credprovider.ate-system.svc:50051`.

To deploy the pieces by hand instead, see the manifests in this directory and the
underlying
`--experimental-additional-egress-extproc-service ate-system/atenet-egress-inject:50051`
flag.

## Verify

First give the actor an egress policy through the `ateapi` `Control` API. Create
one `default` policy for atespace `team-a`, actor `my-actor` with a `hostnames`
rule for `api.example.com` whose `inject_static_headers` effect sets `header` to
`Authorization`, `prefix` to `Bearer `, and `credential_uri` to
`substrate-secret://kubernetes.io/team-secrets/ns1/example-api`
(`CreateActorEgressPolicy`). The change takes effect immediately — the injector
fetches the policy per request, no restart needed.

Then, from that actor (provisioned to trust the MITM CA, see
`demos/egress/egress-mitm.yaml.tmpl`):

```
curl https://api.example.com/anything
```

The origin should see `Authorization: Bearer poc-example-api-token` — the sample
Secret's value with the policy's `Bearer ` prefix. A request to a host not
covered by the policy is passed through unchanged, and so is a matched request
whose header the provider resolves to no value.

A plaintext `curl http://api.example.com/anything` is always denied 403 (the
origin never sees the credential; there is no cleartext opt-in). A request whose
atespace is not granted the secret's namespace in `namespace-policy.yaml` is
denied by `credprovider`, and the injector fails closed (503). A policy whose
credential URI names a provider class other than the injector's
`--credential-provider-name` (default `substrate-secret://kubernetes.io`) is
refused before the provider is dialed.

Each resolved value is sanitized before it becomes a header value: a trailing
newline (common when a Secret is created from a file) is trimmed, and a value
that contains a control character — which Envoy would reject and, for CR/LF,
could turn into header injection — fails closed (503). An empty value injects
nothing rather than sending a bare prefix.

## How a value is resolved

The egress policy names which header(s) to inject for a host
(`inject_static_headers`, each with a `header` and optional `prefix`). For each,
the injector calls the provider with the credential URI, the destination, and the
header name; the **provider** returns the value, and the injector injects
`prefix + value`, overwriting any value the actor sent. The injector never parses
the credential's storage format — that is the provider's job — so it behaves the
same regardless of backend. Each provider stores its credential differently:

- **`credprovider`** returns the referenced Kubernetes Secret value verbatim. The
  destination and header the injector passes are ignored: one Secret holds one
  value.
- **`gsmcredprovider`** stores a **credential set** — a JSON object mapping a
  destination host to the headers available for it — and returns the value at
  `[destination][header]` (see below).

A header the provider resolves to no value passes through uninjected; a resolved
value with an unusable (control) character, or a policy that names a system
header the gateway forbids mutating, fails closed.

## Alternative backend: Google Secret Manager (`gsmcredprovider`)

`credprovider` is one implementation of the `CredentialProvider` plugin API.
`gsmcredprovider` (`cmd/gsmcredprovider`) is a second, backed by Google Cloud
Secret Manager instead of Kubernetes Secrets. It is a drop-in alternative: the
injector dials a single provider address, so a deployment points
`--credential-provider-address` at whichever backend it wants. (The two are not
run side by side yet; a future step routes by the URI's provider class so both
can serve at once.)

- **Credential format**: unlike `credprovider`'s one-value-per-Secret, a Secret
  Manager secret holds a **credential set** — a JSON object mapping a destination
  host to the headers available for it — and the provider returns the value at
  `[destination][header]` for the request. Host keys match case- and
  trailing-dot-insensitively; a set with no such entry resolves to no value (the
  injector passes through); a blob that is not such an object fails closed.

  ```json
  {
    "github.com":      {"Authorization": "Bearer <token>", "X-Custom": "v"},
    "api.example.com": {"X-Api-Key": "<key>"}
  }
  ```
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

To deploy this backend instead of the Kubernetes one, first do the GCP prep:
create the Google service account, grant it `roles/secretmanager.secretAccessor`
on the secrets it serves, and bind it to the `ate-system/gsmcredprovider` KSA via
Workload Identity. Then select it at install time:

```
hack/install-ate.sh --experimental-egress-credential-injection \
  --credential-provider-backend secretmanager \
  --gsm-service-account gsmcredprovider@<project>.iam.gserviceaccount.com
```

`--credential-provider-backend secretmanager` deploys `gsmcredprovider.yaml`
(instead of `credprovider.yaml`), stamps the `--gsm-service-account` into its
Workload Identity annotation, and points the injector's provider class, address,
and server name at the GSM service. (`kubesecret` is the default backend.)

Then reference a Secret Manager URI in the actor egress policy's
`inject_static_headers` credential, e.g.
`substrate-secret://secretmanager.googleapis.com/projects/proj-123/secrets/egress-creds/versions/latest`.
The cleartext refusal, credential sanitization, and fail-closed behavior
described above are the injector's and apply unchanged regardless of backend.

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
- **One provider at a time**: the injector confirms a credential URI's provider
  class matches its configured `--credential-provider-name`, but dials a single
  provider address. Two backends (`kubernetes.io`, `secretmanager.googleapis.com`)
  exist but cannot serve side by side yet — routing by URI class to one of several
  providers is a follow-up.
