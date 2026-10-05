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
# them. See tools/egress-tests/README.md.

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

WORKER_COUNT=2
ACTOR_MEMORY="256Mi"
WORKER_MEMORY=""
ENDPOINTS=100
WAIT_TIMEOUT_SECS=300

usage() {
  echo "Usage: $0 --deploy|--delete [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                Deploy the worker pool, the actor template and the endpoints"
  echo "  --delete                Delete them again (run 'go run ./tools/egress-tests cleanup' first)"
  echo "  --workers N             WorkerPool replicas (default: ${WORKER_COUNT})"
  echo "  --worker-memory SIZE    Memory request and limit of each worker pod (default: unset)."
  echo "                          A worker hosts up to worker memory / actor memory actors."
  echo "  --actor-memory SIZE     Memory limit of each actor (default: ${ACTOR_MEMORY})"
  echo "  --endpoints C           Endpoint Services to create, egress-target-0 up to"
  echo "                          egress-target-<C-1> (default: ${ENDPOINTS}, at most ${MAX_ENDPOINTS})"
  echo "  --wait-timeout SECONDS  How long to wait for each rollout and the golden snapshot (default: ${WAIT_TIMEOUT_SECS})"
}

# kubectl-ate runs once per poll while waiting for the golden snapshot, so it
# is built once up front.
KUBECTL_ATE_BIN=""

build_kubectl_ate() {
  local dir
  dir="$(mktemp -d)"
  trap 'rm -rf '"${dir}" EXIT
  KUBECTL_ATE_BIN="${dir}/kubectl-ate"
  go build -o "${KUBECTL_ATE_BIN}" ./cmd/kubectl-ate
}

run_kubectl_ate() {
  "${KUBECTL_ATE_BIN}" "$@"
}

substitute() {
  local manifest="$1"
  local worker_template=""
  # One flow-style line, so an unset WORKER_MEMORY leaves only a blank line.
  if [[ -n "${WORKER_MEMORY}" ]]; then
    worker_template="template: {resources: {requests: {memory: \"${WORKER_MEMORY}\"}, limits: {memory: \"${WORKER_MEMORY}\"}}}"
  fi
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
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
  substitute "${MANIFEST_DIR}/workerpool.yaml.tmpl" | hack/run-tool.sh ko apply -f -
  kubectl wait --for=create deployment/egress-tests \
    --namespace="${POOL_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  kubectl rollout status deployment/egress-tests \
    --namespace="${POOL_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"

  # The store requires the atespace to exist before the template.
  run_kubectl_ate create atespace "${ATESPACE}" >/dev/null 2>&1 \
    || run_kubectl_ate get atespace "${ATESPACE}" >/dev/null

  # Templates are immutable, so a changed one is deleted and created again.
  echo "Creating actor template ${ATESPACE}/${TEMPLATE} (actor_memory=${ACTOR_MEMORY})..."
  run_kubectl_ate delete actor-template "${TEMPLATE}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  substitute "${MANIFEST_DIR}/actor-template.yaml.tmpl" \
    | hack/run-tool.sh ko resolve -f - \
    | run_kubectl_ate create actor-template -f -
  echo "Waiting for the golden snapshot..."
  wait_actortemplate_ready

  echo "Deploying ${ENDPOINTS} endpoints in ${TARGET_NAMESPACE}..."
  hack/run-tool.sh ko apply -f - <"${MANIFEST_DIR}/targets.yaml.tmpl"
  render_services | kubectl apply -f - >/dev/null
  kubectl rollout status deployment/egress-target \
    --namespace="${TARGET_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  echo "Ready: endpoints egress-target-0 up to egress-target-$((ENDPOINTS - 1)).${TARGET_NAMESPACE}.svc.cluster.local"
}

delete() {
  build_kubectl_ate
  echo "Deleting the egress-tests deployment..."
  run_kubectl_ate delete actor-template "${TEMPLATE}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  run_kubectl_ate delete atespace "${ATESPACE}" >/dev/null 2>&1 \
    || echo "atespace ${ATESPACE} not deleted: it may still hold actors; run 'go run ./tools/egress-tests cleanup' first"
  kubectl delete namespace "${TARGET_NAMESPACE}" --ignore-not-found
  # The pool manifest has ko:// image references, so it goes through ko.
  substitute "${MANIFEST_DIR}/workerpool.yaml.tmpl" | hack/run-tool.sh ko delete --ignore-not-found -f -
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

case "${action}" in
  deploy) deploy ;;
  delete) delete ;;
  *)
    usage
    exit 1
    ;;
esac
