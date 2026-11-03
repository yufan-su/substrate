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

# Collects the egress evidence for one actor: its own logs, its egress policy,
# the DNS relay and atunnel lines on its worker, the egress gateway's lines and
# errors, and the credential provider's warnings and namespace policy.
#
# Only the customer can run its kubectl ate calls. When they fail, it says
# so and still collects what kubectl can read.
#
# Usage: egress.sh <atespace> <actor>

set -o errexit -o nounset -o pipefail

# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <atespace> <actor>" >&2
  exit 2
fi

load_actor "$1" "$2"
actor_logs "$1" "$2"

section "Egress policy"
run_ate get egress-policy "$2" -a "$1" -o yaml

section "DNS relay and atunnel (egress)"
if [[ -n "${WORKER_POD}" ]]; then
  logs "${WORKER_NS}" "pod/${WORKER_POD}" worker 'dns relay|egress policy|atunnel|certificate'
else
  no_worker
fi

section "Egress gateway: lines for this actor"
logs ate-system deploy/atenet-egress ext-proc "${ACTOR_PATTERN}"
logs ate-system deploy/atenet-egress envoy "${ACTOR_PATTERN}"

section "Egress gateway: errors for any actor"
logs ate-system deploy/atenet-egress ext-proc 'lookup failed|unavailable|credential provider'
logs ate-system deploy/atenet-egress envoy '"leg":"clienthello"'
logs ate-system deploy/atenet-egress sdsmint 'certificate request denied'
if forward ate-system deploy/atenet-egress 15090; then
  http_get "http://127.0.0.1:${FWD_PORT}/stats/prometheus" 'overflow|ext_proc|fail'
fi

section "Credential provider"
logs ate-system deploy/k8s-credential-provider k8s-credential-provider '"level":"(WARN|ERROR)"'
run kubectl -n ate-system get cm k8s-credential-provider-namespace-policy -o yaml
