#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Deploys, or deletes, what the egress scale test runs against: the worker
# pool, the actor template, and the endpoint Services with the server behind
# them. With --https it also patches the egress gateway to trust the target's
# certificate. See tools/egress-tests/README.md.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

MANIFEST_DIR="tools/egress-tests/manifests"
ATESPACE="egress-tests"
TEMPLATE="egress-tests-actor"
POOL_NAMESPACE="egress-tests"
TARGET_NAMESPACE="egress-tests-targets"
# The egress policy admits at most this many hostnames.
MAX_ENDPOINTS=256
# The egress gateway and what --patch-gateway adds to it. Keep in sync with
# gateway.go and the gateway-trust-*.yaml manifests.
SYSTEM_NAMESPACE="ate-system"
GATEWAY_DEPLOYMENT="atenet-egress"
GATEWAY_CA_CONFIGMAP="egress-tests-target-ca"
TARGET_TLS_SECRET="egress-target-tls"

WORKER_COUNT=2
ACTOR_MEMORY="256Mi"
WORKER_MEMORY=""
ENDPOINTS=100
WAIT_TIMEOUT_SECS=300
HTTPS=false
SKIP_TEMPLATE=false
CGREADER=false
KUBECONFIG_FILE=""
KUBE_CONTEXT=""
# --kubeconfig and --context, for every kubectl, kubectl-ate and ko call.
CLUSTER_ARGS=()

usage() {
  echo "Usage: $0 --deploy|--delete|--patch-gateway|--unpatch-gateway [options]"
  echo ""
  echo "Actions:"
  echo "  --deploy                Deploy the worker pool, the actor template and the endpoints"
  echo "  --delete                Delete them again, and unpatch the gateway"
  echo "                          (run 'go run ./tools/egress-tests cleanup' first)"
  echo "  --patch-gateway         Make the egress gateway trust the target's certificate, for"
  echo "                          --scheme https runs. Rolls the gateway; test clusters only."
  echo "  --unpatch-gateway       Undo --patch-gateway"
  echo ""
  echo "Options:"
  echo "  --https                 With --deploy, also --patch-gateway"
  echo "  --skip-template         With --deploy, keep the existing actor template. Recreating it"
  echo "                          strands existing actors, so --deploy refuses while any exist."
  echo "  --workers N             WorkerPool replicas (default: ${WORKER_COUNT})"
  echo "  --worker-memory SIZE    Memory request and limit of each worker pod (default: unset)."
  echo "                          A worker hosts up to worker memory / actor memory actors."
  echo "  --actor-memory SIZE     Memory limit of each actor (default: ${ACTOR_MEMORY})"
  echo "  --endpoints C           Endpoint Services to create, egress-target-0 up to"
  echo "                          egress-target-<C-1> (default: ${ENDPOINTS}, at most ${MAX_ENDPOINTS})"
  echo "  --wait-timeout SECONDS  How long to wait for each rollout and the golden snapshot (default: ${WAIT_TIMEOUT_SECS})"
  echo "  --cgreader              Also deploy the cgroup reader DaemonSet for run --resources-cgreader"
  echo "  --kubeconfig FILE       Kubeconfig for every kubectl, kubectl-ate and ko call"
  echo "                          (default: the usual loading rules)"
  echo "  --context CTX           Kubeconfig context for every call (default: the current context)"
}

# Scratch space for kubectl-ate and generated certificates, removed on exit.
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# kubectl-ate runs once per poll while waiting for the golden snapshot, so it
# is built once up front.
KUBECTL_ATE_BIN="${WORK_DIR}/kubectl-ate"

build_kubectl_ate() {
  go build -o "${KUBECTL_ATE_BIN}" ./cmd/kubectl-ate
}

run_kubectl_ate() {
  "${KUBECTL_ATE_BIN}" ${CLUSTER_ARGS[@]+"${CLUSTER_ARGS[@]}"} "$@"
}

# Every kubectl call in this script goes through here, so none falls back to
# the current context when --context is given.
kubectl() {
  command kubectl ${CLUSTER_ARGS[@]+"${CLUSTER_ARGS[@]}"} "$@"
}

# ko apply passes what follows -- to kubectl apply; ko delete passes all of
# its arguments to kubectl delete.
ko_apply() {
  if ((${#CLUSTER_ARGS[@]})); then
    hack/run-tool.sh ko apply -f - -- "${CLUSTER_ARGS[@]}"
  else
    hack/run-tool.sh ko apply -f -
  fi
}

ko_delete() {
  hack/run-tool.sh ko delete --ignore-not-found -f - ${CLUSTER_ARGS[@]+"${CLUSTER_ARGS[@]}"}
}

sha256_file() {
  if command -v sha256sum &>/dev/null; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

substitute() {
  local manifest="$1"
  local worker_template=""
  # One flow-style line, so an unset WORKER_MEMORY leaves only a blank line.
  if [[ -n "${WORKER_MEMORY}" ]]; then
    worker_template="template: {resources: {requests: {memory: \"${WORKER_MEMORY}\"}, limits: {memory: \"${WORKER_MEMORY}\"}}}"
  fi
  # --delete runs without BUCKET_NAME; only the actor template uses it.
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME:-}|g" \
      -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
      -e "s|\${ACTOR_MEMORY}|${ACTOR_MEMORY}|g" \
      -e "s|\${WORKER_TEMPLATE}|${worker_template}|g" \
      "${manifest}"
}

# render_services prints one Service per endpoint.
render_services() {
  local i
  for ((i = 0; i < ENDPOINTS; i++)); do
    echo "---"
    sed -e "s|\${INDEX}|${i}|g" "${MANIFEST_DIR}/target-service.yaml.tmpl"
  done
}

# wait_actortemplate_ready polls the actor template until its golden snapshot
# exists, failing fast when the template reconciler reports an error.
wait_actortemplate_ready() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECS))
  local json snapshot error_message

  while ((SECONDS < deadline)); do
    if json=$(run_kubectl_ate get actor-template "${TEMPLATE}" -a "${ATESPACE}" -o json 2>/dev/null); then
      snapshot=$(jq -r '.status.goldenSnapshotStatus.goldenTag.name // empty' <<<"${json}")
      if [[ -n "${snapshot}" ]]; then
        return 0
      fi
      error_message=$(jq -r '.status.goldenSnapshotStatus.errorMessage // empty' <<<"${json}")
      if [[ -n "${error_message}" ]]; then
        echo "actor template ${ATESPACE}/${TEMPLATE} failed: ${error_message}" >&2
        return 1
      fi
    fi
    sleep 5
  done

  echo "timed out waiting for the golden snapshot of actor template ${ATESPACE}/${TEMPLATE}" >&2
  return 1
}

# ensure_no_actors refuses to go on while actors exist. Recreating the
# template deletes the golden snapshot that actors never resumed still restore
# from, and resume uses the new template for the rest.
ensure_no_actors() {
  local count
  count=$(run_kubectl_ate get actors -a "${ATESPACE}" -o json 2>/dev/null | jq '.actors // [] | length' || echo 0)
  if ((count > 0)); then
    echo "Error: ${count} actors exist in atespace ${ATESPACE}, and recreating the actor template would strand them." >&2
    echo "Delete them first with 'go run ./tools/egress-tests cleanup --actors <largest --actors used>'," >&2
    echo "or keep the current template with --skip-template." >&2
    exit 1
  fi
}

# ensure_target_tls_secret gives the target a certificate for every endpoint
# name, issued once by a CA whose key is then discarded.
ensure_target_tls_secret() {
  kubectl create namespace "${TARGET_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  if kubectl -n "${TARGET_NAMESPACE}" get secret "${TARGET_TLS_SECRET}" &>/dev/null; then
    return 0
  fi
  echo "Issuing the target's HTTPS certificate..."
  go run ./tools/egress-tests certs --out "${WORK_DIR}/certs"
  kubectl -n "${TARGET_NAMESPACE}" create secret generic "${TARGET_TLS_SECRET}" \
    --type=kubernetes.io/tls \
    --from-file=tls.crt="${WORK_DIR}/certs/tls.crt" \
    --from-file=tls.key="${WORK_DIR}/certs/tls.key" \
    --from-file=ca.crt="${WORK_DIR}/certs/ca.crt" >/dev/null
  # Pods running from an earlier Secret do not see the new one.
  if kubectl -n "${TARGET_NAMESPACE}" get deployment egress-target &>/dev/null; then
    kubectl -n "${TARGET_NAMESPACE}" rollout restart deployment/egress-target
  fi
}

# patch_gateway_trust makes the egress gateway trust the target's CA when it
# re-originates TLS, and waits for the gateway to roll.
patch_gateway_trust() {
  local envoy_image ca_file ca_sha256
  envoy_image=$(kubectl -n "${SYSTEM_NAMESPACE}" get deployment "${GATEWAY_DEPLOYMENT}" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="envoy")].image}')
  if [[ -z "${envoy_image}" ]]; then
    echo "Error: ${SYSTEM_NAMESPACE}/${GATEWAY_DEPLOYMENT} has no envoy container; --https supports only the Envoy dataplane." >&2
    exit 1
  fi
  if ! kubectl -n "${TARGET_NAMESPACE}" get secret "${TARGET_TLS_SECRET}" &>/dev/null; then
    echo "Error: the target has no certificate yet; run $0 --deploy --https first." >&2
    exit 1
  fi

  ca_file="${WORK_DIR}/target-ca.crt"
  kubectl -n "${TARGET_NAMESPACE}" get secret "${TARGET_TLS_SECRET}" \
    -o go-template='{{index .data "ca.crt" | base64decode}}' >"${ca_file}"
  kubectl -n "${SYSTEM_NAMESPACE}" create configmap "${GATEWAY_CA_CONFIGMAP}" \
    --from-file=ca.crt="${ca_file}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  ca_sha256=$(sha256_file "${ca_file}")

  echo "Patching ${SYSTEM_NAMESPACE}/${GATEWAY_DEPLOYMENT} to trust the target's CA..."
  sed -e "s|\${ENVOY_IMAGE}|${envoy_image}|g" \
      -e "s|\${TARGET_CA_SHA256}|${ca_sha256}|g" \
      "${MANIFEST_DIR}/gateway-trust-patch.yaml.tmpl" >"${WORK_DIR}/gateway-trust-patch.yaml"
  kubectl -n "${SYSTEM_NAMESPACE}" patch deployment "${GATEWAY_DEPLOYMENT}" \
    --type strategic --patch-file "${WORK_DIR}/gateway-trust-patch.yaml"
  if ! kubectl -n "${SYSTEM_NAMESPACE}" rollout status deployment/"${GATEWAY_DEPLOYMENT}" \
    --timeout="${WAIT_TIMEOUT_SECS}s"; then
    echo "Error: the patched gateway did not become ready; the previous pod keeps serving." >&2
    echo "Undo the patch with $0 --unpatch-gateway." >&2
    exit 1
  fi
}

# unpatch_gateway_trust puts the gateway back on the public roots alone. The
# CA ConfigMap goes last, once no gateway pod mounts it.
unpatch_gateway_trust() {
  if kubectl -n "${SYSTEM_NAMESPACE}" get deployment "${GATEWAY_DEPLOYMENT}" \
    -o jsonpath='{.spec.template.spec.initContainers[*].name}' 2>/dev/null | grep -qw egress-tests-trust; then
    echo "Removing the egress-tests trust patch from ${SYSTEM_NAMESPACE}/${GATEWAY_DEPLOYMENT}..."
    kubectl -n "${SYSTEM_NAMESPACE}" patch deployment "${GATEWAY_DEPLOYMENT}" \
      --type strategic --patch-file "${MANIFEST_DIR}/gateway-trust-unpatch.yaml"
    kubectl -n "${SYSTEM_NAMESPACE}" rollout status deployment/"${GATEWAY_DEPLOYMENT}" \
      --timeout="${WAIT_TIMEOUT_SECS}s"
  fi
  kubectl -n "${SYSTEM_NAMESPACE}" delete configmap "${GATEWAY_CA_CONFIGMAP}" --ignore-not-found >/dev/null
}

deploy() {
  if [[ -z "${BUCKET_NAME:-}" || -z "${KO_DOCKER_REPO:-}" ]]; then
    echo "Error: BUCKET_NAME and KO_DOCKER_REPO must be set (see .ate-dev-env.sh)." >&2
    exit 1
  fi
  if ! command -v jq &>/dev/null; then
    echo "Error: jq is required to wait for the actor template's golden snapshot." >&2
    exit 1
  fi
  build_kubectl_ate

  echo "Deploying the worker pool (workers=${WORKER_COUNT}, worker_memory=${WORKER_MEMORY:-unset})..."
  substitute "${MANIFEST_DIR}/workerpool.yaml.tmpl" | ko_apply
  kubectl wait --for=create deployment/egress-tests \
    --namespace="${POOL_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  kubectl rollout status deployment/egress-tests \
    --namespace="${POOL_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"

  # The store requires the atespace to exist before the template.
  run_kubectl_ate create atespace "${ATESPACE}" >/dev/null 2>&1 \
    || run_kubectl_ate get atespace "${ATESPACE}" >/dev/null

  if [[ "${SKIP_TEMPLATE}" == true ]]; then
    echo "Keeping the existing actor template ${ATESPACE}/${TEMPLATE}."
  else
    ensure_no_actors
    # Templates are immutable, so a changed one is deleted and created again.
    echo "Creating actor template ${ATESPACE}/${TEMPLATE} (actor_memory=${ACTOR_MEMORY})..."
    run_kubectl_ate delete actor-template "${TEMPLATE}" -a "${ATESPACE}" >/dev/null 2>&1 || true
    substitute "${MANIFEST_DIR}/actor-template.yaml.tmpl" \
      | hack/run-tool.sh ko resolve -f - \
      | run_kubectl_ate create actor-template -f -
    echo "Waiting for the golden snapshot..."
    wait_actortemplate_ready
  fi

  ensure_target_tls_secret
  echo "Deploying ${ENDPOINTS} endpoints in ${TARGET_NAMESPACE}..."
  ko_apply <"${MANIFEST_DIR}/targets.yaml.tmpl"
  render_services | kubectl apply -f - >/dev/null
  kubectl rollout status deployment/egress-target \
    --namespace="${TARGET_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  echo "Ready: endpoints egress-target-0 up to egress-target-$((ENDPOINTS - 1)).${TARGET_NAMESPACE}.svc.cluster.local"

  if [[ "${HTTPS}" == true ]]; then
    patch_gateway_trust
    echo "Ready for --scheme https."
  fi

  if [[ "${CGREADER}" == "true" ]]; then
    echo "Deploying the cgroup reader DaemonSet..."
    ko_apply <"${MANIFEST_DIR}/cgreader.yaml.tmpl"
    kubectl rollout status daemonset/egress-tests-cgreader \
      --namespace="${POOL_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  fi
}

delete() {
  build_kubectl_ate
  echo "Deleting the egress-tests deployment..."
  unpatch_gateway_trust
  run_kubectl_ate delete actor-template "${TEMPLATE}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  run_kubectl_ate delete atespace "${ATESPACE}" >/dev/null 2>&1 \
    || echo "atespace ${ATESPACE} not deleted: it may still hold actors; run 'go run ./tools/egress-tests cleanup' first"
  kubectl delete namespace "${TARGET_NAMESPACE}" --ignore-not-found
  kubectl delete daemonset egress-tests-cgreader --namespace="${POOL_NAMESPACE}" --ignore-not-found
  # The pool manifest has ko:// image references, so it goes through ko.
  substitute "${MANIFEST_DIR}/workerpool.yaml.tmpl" | ko_delete
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy)
      action="deploy"
      ;;
    --delete)
      action="delete"
      ;;
    --patch-gateway)
      action="patch-gateway"
      ;;
    --unpatch-gateway)
      action="unpatch-gateway"
      ;;
    --https)
      HTTPS=true
      ;;
    --skip-template)
      SKIP_TEMPLATE=true
      ;;
    --workers)
      shift
      WORKER_COUNT="$1"
      ;;
    --workers=*)
      WORKER_COUNT="${1#*=}"
      ;;
    --worker-memory)
      shift
      WORKER_MEMORY="$1"
      ;;
    --worker-memory=*)
      WORKER_MEMORY="${1#*=}"
      ;;
    --actor-memory)
      shift
      ACTOR_MEMORY="$1"
      ;;
    --actor-memory=*)
      ACTOR_MEMORY="${1#*=}"
      ;;
    --endpoints)
      shift
      ENDPOINTS="$1"
      ;;
    --endpoints=*)
      ENDPOINTS="${1#*=}"
      ;;
    --wait-timeout)
      shift
      WAIT_TIMEOUT_SECS="$1"
      ;;
    --wait-timeout=*)
      WAIT_TIMEOUT_SECS="${1#*=}"
      ;;
    --cgreader)
      CGREADER=true
      ;;
    --kubeconfig)
      shift
      KUBECONFIG_FILE="$1"
      ;;
    --kubeconfig=*)
      KUBECONFIG_FILE="${1#*=}"
      ;;
    --context)
      shift
      KUBE_CONTEXT="$1"
      ;;
    --context=*)
      KUBE_CONTEXT="${1#*=}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

for value in "${WORKER_COUNT}" "${ENDPOINTS}" "${WAIT_TIMEOUT_SECS}"; do
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    echo "Error: --workers, --endpoints and --wait-timeout take whole numbers, got '${value}'" >&2
    exit 1
  fi
done
if ((ENDPOINTS < 1 || ENDPOINTS > MAX_ENDPOINTS)); then
  echo "Error: --endpoints must be between 1 and ${MAX_ENDPOINTS}, got ${ENDPOINTS}" >&2
  exit 1
fi
if [[ -n "${KUBECONFIG_FILE}" ]]; then
  CLUSTER_ARGS+=(--kubeconfig "${KUBECONFIG_FILE}")
fi
if [[ -n "${KUBE_CONTEXT}" ]]; then
  CLUSTER_ARGS+=(--context "${KUBE_CONTEXT}")
fi

case "${action}" in
  deploy) deploy ;;
  delete) delete ;;
  patch-gateway) patch_gateway_trust ;;
  unpatch-gateway) unpatch_gateway_trust ;;
  *)
    usage
    exit 1
    ;;
esac
