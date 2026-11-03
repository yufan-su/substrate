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

# Checks the causes that break hops in both directions: NetworkPolicies and
# pod certificates. With an actor, it also prints the NetworkPolicy of the
# actor's WorkerPool.
#
# Usage: shared-causes.sh [<atespace> <actor>]

set -o errexit -o nounset -o pipefail

# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ "$#" -ne 0 && "$#" -ne 2 ]]; then
  echo "Usage: $0 [<atespace> <actor>]" >&2
  exit 2
fi

section "NetworkPolicies"
run kubectl get networkpolicy -A
run kubectl get events -A --field-selector reason=NetworkPolicyConflict
if [[ "$#" -eq 2 ]]; then
  load_actor "$1" "$2"
  if [[ -n "${WORKER_POOL}" ]]; then
    run kubectl -n "${WORKER_NS}" get networkpolicy "substrate-${WORKER_POOL}" -o yaml
  else
    echo "The actor has no worker, so its WorkerPool is unknown."
  fi
fi

section "Pod certificates"
run kubectl get clustertrustbundles -l podcert.ate.dev/canarying=live
run kubectl get podcertificaterequests -A
logs podcertificate-controller-system deploy/podcertificate-controller controller \
  'Error while handling PodCertificateRequest'
