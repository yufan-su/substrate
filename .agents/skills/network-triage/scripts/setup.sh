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

# Checks that the networking pieces are up. It describes every networking pod
# that isn't Running and Ready, prints the last run of each restarted
# container, and prints the router's health and listeners. With an actor, it
# also prints the actor's state, its worker pod, and that worker's readiness.
#
# Usage: setup.sh [<atespace> <actor>]

set -o errexit -o nounset -o pipefail

# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ "$#" -ne 0 && "$#" -ne 2 ]]; then
  echo "Usage: $0 [<atespace> <actor>]" >&2
  exit 2
fi

need jq

NETWORK_PODS='app in (atenet-router,atenet-egress,k8s-credential-provider)'

# details describes the pods in namespace $1 matching selector $2 that aren't
# Running with every container ready, and prints each restarted container's
# last run.
details() {
  local pod container
  for pod in $(kubectl -n "$1" get pods -l "$2" -o json | jq -r '.items[]
      | select(.status.phase != "Running" or ([.status.containerStatuses[]?.ready] | all | not))
      | .metadata.name'); do
    section "Pod $1/${pod} is not ready"
    run kubectl -n "$1" describe pod "${pod}"
    for container in $(kubectl -n "$1" get pod "${pod}" -o json | jq -r '
        ((.status.initContainerStatuses // []) + (.status.containerStatuses // []))[]
        | select(.restartCount > 0) | .name'); do
      run kubectl -n "$1" logs "${pod}" -c "${container}" --previous --tail=50
    done
  done
}

section "Networking pods"
run kubectl -n ate-system get pods -o wide -l "${NETWORK_PODS}"
run kubectl -n podcertificate-controller-system get pods -o wide
details ate-system "${NETWORK_PODS}"
details podcertificate-controller-system app=podcertificate-controller

section "Ingress router health"
SINCE=10m logs ate-system deploy/atenet-router atenet-router 'health check failed'
if forward ate-system deploy/atenet-router 4040; then
  printf '\n$ curl -s localhost:4040/statusz?format=json | jq {build_tag, mode, health, flags}\n'
  curl -s --max-time 10 "http://127.0.0.1:${FWD_PORT}/statusz?format=json" \
    | jq '{build_tag, mode, health, flags}' || echo '(failed)'
fi
if forward ate-system deploy/atenet-router 9901; then
  http_get "http://127.0.0.1:${FWD_PORT}/listeners"
fi

if [[ "$#" -eq 2 ]]; then
  load_actor "$1" "$2"
  print_actor
  if [[ -n "${WORKER_POD}" ]]; then
    section "Worker"
    run kubectl -n "${WORKER_NS}" get pods -o wide -l "ate.dev/worker-pool=${WORKER_POOL}"
    if forward "${WORKER_NS}" "pod/${WORKER_POD}" 8080; then
      http_get "http://127.0.0.1:${FWD_PORT}/readyz"
    fi
  fi
fi
