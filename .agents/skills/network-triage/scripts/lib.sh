# shellcheck shell=bash

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

# Helpers for the network-triage scripts. Each check prints the command it
# runs and keeps going when that command fails, so one bad pod doesn't hide
# the rest. Everything here only reads.
#
# SINCE sets how far back logs go (default 1h). MAX_LINES caps each log
# section (default 200).

SINCE="${SINCE:-1h}"
MAX_LINES="${MAX_LINES:-200}"
FORWARD_PIDS=""
ATE_OK=1

cleanup() {
  local pid
  for pid in ${FORWARD_PIDS}; do
    kill "${pid}" 2>/dev/null || true
  done
}
trap cleanup EXIT

section() {
  printf '\n===== %s =====\n' "$*"
}

need() {
  if ! command -v "$1" >/dev/null; then
    echo "$1 is required." >&2
    exit 1
  fi
}

# run prints a command, runs it, and reports a non-zero exit.
run() {
  printf '\n$ %s\n' "$*"
  "$@" 2>&1 || printf '(exit %d)\n' "$?"
}

# ate_failed records that kubectl ate failed and says who can run it.
ate_failed() {
  ATE_OK=0
  printf '%s\n' \
    '(Only the customer can run kubectl ate: Substrate is their workload, and' \
    'ate-api-server checks every call against their access policy. Send this' \
    'script to the customer and ask for its output. The rest of this script' \
    'still runs.)'
}

# run_ate runs kubectl ate with the given arguments, like run. After the first
# failure, it skips later calls.
run_ate() {
  printf '\n$ kubectl ate %s\n' "$*"
  if (( ! ATE_OK )); then
    echo '(skipped: kubectl ate failed above)'
    return 0
  fi
  if ! kubectl ate "$@" 2>&1; then
    ate_failed
  fi
}

# tail_lines prints the last MAX_LINES lines of stdin.
tail_lines() {
  local all count
  all="$(cat)"
  if [[ -z "${all}" ]]; then
    echo '(none)'
    return
  fi
  count="$(printf '%s\n' "${all}" | wc -l | tr -d ' ')"
  if (( count > MAX_LINES )); then
    printf '(the last %s of %s lines)\n' "${MAX_LINES}" "${count}"
  fi
  printf '%s\n' "${all}" | tail -n "${MAX_LINES}"
}

# logs prints a container's logs since SINCE. With a fourth argument, it keeps
# only lines that match that extended regex.
logs() {
  local ns="$1" target="$2" container="$3" pattern="${4:-}" out
  printf '\n$ kubectl -n %s logs %s -c %s --since=%s' "${ns}" "${target}" "${container}" "${SINCE}"
  if [[ -n "${pattern}" ]]; then
    printf " | grep -E '%s'" "${pattern}"
  fi
  printf '\n'
  if ! out="$(kubectl -n "${ns}" logs "${target}" -c "${container}" --since="${SINCE}" 2>&1)"; then
    printf '%s\n(kubectl logs failed)\n' "${out}"
    return 0
  fi
  if [[ -n "${pattern}" ]]; then
    out="$(printf '%s\n' "${out}" | grep -E -- "${pattern}" || true)"
  fi
  printf '%s' "${out}" | tail_lines
}

# forward port-forwards a free local port to port $3 of $2 in namespace $1,
# and sets FWD_PORT to the local port.
forward() {
  local log pid
  log="$(mktemp)"
  kubectl -n "$1" port-forward "$2" ":$3" >"${log}" 2>&1 &
  pid=$!
  FORWARD_PIDS="${FORWARD_PIDS} ${pid}"
  FWD_PORT=""
  for _ in $(seq 1 100); do
    FWD_PORT="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "${log}" | head -n 1)"
    if [[ -n "${FWD_PORT}" ]]; then
      rm -f "${log}"
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  printf '\n(port-forward to %s/%s port %s failed: %s)\n' "$1" "$2" "$3" "$(cat "${log}")"
  rm -f "${log}"
  return 1
}

# http_get fetches a URL. With a second argument, it keeps only lines that
# match that extended regex.
http_get() {
  local url="$1" pattern="${2:-}" out
  printf '\n$ curl -s %s' "${url}"
  if [[ -n "${pattern}" ]]; then
    printf " | grep -E '%s'" "${pattern}"
  fi
  printf '\n'
  if ! out="$(curl -s --max-time 10 "${url}" 2>&1)"; then
    printf '%s\n(curl failed)\n' "${out}"
    return 0
  fi
  if [[ -n "${pattern}" ]]; then
    out="$(printf '%s\n' "${out}" | grep -E -- "${pattern}" || true)"
  fi
  printf '%s' "${out}" | tail_lines
}

# load_actor reads actor $2 in atespace $1. It sets ACTOR_JSON, WORKER_NS,
# WORKER_POD, WORKER_POOL, and ACTOR_PATTERN, a regex that matches the actor in
# component logs. If kubectl ate can't read the actor, the worker fields stay
# empty.
# shellcheck disable=SC2034 # The scripts that source this file read these.
load_actor() {
  local atespace="${1//./\\.}" name="${2//./\\.}"
  need jq
  # Components log the actor as "atespace":"a","name":"n"; Envoy logs a/n.
  ACTOR_PATTERN="\"atespace\":\"${atespace}\",\"name\":\"${name}\"|${atespace}/${name}"
  WORKER_NS=""
  WORKER_POD=""
  WORKER_POOL=""
  section "Actor"
  printf '\n$ kubectl ate get actor %s -a %s -o json\n' "$2" "$1"
  if ! ACTOR_JSON="$(kubectl ate get actor "$2" -a "$1" -o json 2>&1)"; then
    printf '%s\n' "${ACTOR_JSON}"
    ACTOR_JSON=""
    ate_failed
    return 0
  fi
  WORKER_NS="$(jq -r '.status.workerAssignment.workerNamespace // ""' <<<"${ACTOR_JSON}")"
  WORKER_POD="$(jq -r '.status.workerAssignment.workerPod // ""' <<<"${ACTOR_JSON}")"
  WORKER_POOL="$(jq -r '.status.workerAssignment.workerPool // ""' <<<"${ACTOR_JSON}")"
  jq '{uid: .metadata.uid, state: .status.state, workerAssignment: .status.workerAssignment, crash: .status.crash}' <<<"${ACTOR_JSON}"
}

# no_worker explains why a worker log section is empty.
no_worker() {
  if (( ATE_OK )); then
    echo "The actor has no worker, so there are no worker logs to read."
  else
    echo "kubectl ate couldn't read the actor, so its worker is unknown."
  fi
}

# actor_logs prints actor $2's lifecycle events, then its containers' output.
# These show whether a request reached the actor and what its own outbound
# calls returned. kubectl ate logs reads only a RUNNING actor's worker.
actor_logs() {
  local source out
  section "Actor logs"
  for source in lifecycle containers; do
    printf '\n$ kubectl ate logs actor %s -a %s --source=%s\n' "$2" "$1" "${source}"
    if (( ! ATE_OK )); then
      echo '(skipped: kubectl ate failed above)'
      continue
    fi
    if ! out="$(kubectl ate logs actor "$2" -a "$1" --source="${source}" 2>&1)"; then
      printf '%s\n(kubectl ate logs failed)\n' "${out}"
      continue
    fi
    printf '%s' "${out}" | tail_lines
  done
}
