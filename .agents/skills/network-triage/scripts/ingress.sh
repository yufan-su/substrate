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

# Collects the ingress evidence for one actor: its own logs, the ingress
# router's lines for it, router errors, Envoy stats, recent requests and
# metrics, and atunnel's lines on the actor's worker.
#
# Only the customer can run its kubectl ate calls. When they fail, it says
# so and still collects what kubectl can read.
#
# Usage: ingress.sh <atespace> <actor>

set -o errexit -o nounset -o pipefail

# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <atespace> <actor>" >&2
  exit 2
fi

load_actor "$1" "$2"
actor_logs "$1" "$2"

section "Ingress router: lines for this actor"
logs ate-system deploy/atenet-router atenet-router "${ACTOR_PATTERN}"
# Only tunnel lines name the actor. Match per-request lines by time and status.
logs ate-system deploy/atenet-router envoy "${ACTOR_PATTERN}"

section "Ingress router: errors for any actor"
logs ate-system deploy/atenet-router atenet-router 'Error during ext_proc|health check failed'

section "Ingress router: Envoy stats"
if forward ate-system deploy/atenet-router 9901; then
  http_get "http://127.0.0.1:${FWD_PORT}/stats?filter=ext_proc%7Cactor_connect%7Cate-cluster%7Cssl"
fi

section "Ingress router: recent requests and metrics"
if forward ate-system deploy/atenet-router 4040; then
  printf '\n$ curl -s localhost:4040/statusz?format=json | jq <health, queries for %s>\n' "$2"
  curl -s --max-time 10 "http://127.0.0.1:${FWD_PORT}/statusz?format=json" \
    | jq --arg actor "$2" '{health, queries: ([.queries[]? | select(tostring | contains($actor))] | .[-20:])}' \
    || echo '(failed)'
fi
if forward ate-system deploy/atenet-router 9090; then
  http_get "http://127.0.0.1:${FWD_PORT}/metrics" '^atenet_router'
fi

section "atunnel (ingress)"
if [[ -n "${WORKER_POD}" ]]; then
  logs "${WORKER_NS}" "pod/${WORKER_POD}" worker 'atunnel|ingress:'
else
  no_worker
fi
