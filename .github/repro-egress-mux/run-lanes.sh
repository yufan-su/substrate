#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs the gVisor e2e lane ITERATIONS times, as pr-workflow.yaml runs it once,
# and saves logs from the pods on the egress path after each run. Exits nonzero
# if any run failed, after all runs.

set -o errexit -o nounset -o pipefail

iterations="${ITERATIONS:-6}"
artifacts="${ARTIFACTS:?}"
node="kind-control-plane"

# save_logs copies the kubelet's log files, rotated ones included, so nothing is
# lost to a --tail.
save_logs() {
  local dir="$1"
  mkdir -p "${dir}"
  docker exec "${node}" sh -c \
    'cd /var/log/pods && tar -czf - $(ls -d ate-system_atenet-egress-* ate-system_atelet-* ate-demo-egress_* 2>/dev/null)' \
    > "${dir}/pod-logs.tgz"
  kubectl --context kind-kind get pods -A -o wide > "${dir}/pods.txt"
}

status=0
for i in $(seq 1 "${iterations}"); do
  dir="${artifacts}/iter-${i}"
  echo "=== iteration ${i} of ${iterations} starting at $(date -u +%FT%TZ)"
  if E2E_JUNIT_FILE="${dir}/e2e-gvisor-preview.xml" \
    hack/run-e2e-kind.sh -args --no-color --preview="${ATE_PREVIEW}"; then
    echo "=== iteration ${i}: pass"
  else
    echo "=== iteration ${i}: FAIL"
    status=1
  fi
  save_logs "${dir}"
done
exit "${status}"
