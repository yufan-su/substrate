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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

if [ -z "${PROJECT_ID:-}" ]; then
  echo "Error: PROJECT_ID environment variable must be set." >&2
  exit 1
fi

MANIFEST="benchmarking/locust/manifests/locust.yaml"

NO_BOOMER=0

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy      Deploy the locust workers"
  echo "  --delete      Delete the locust workers"
  echo "  --no-boomer   Omit the boomer-glutton container (see below)"
  echo "  -h|--help     Show this help message"
  echo ""
  echo "--no-boomer is for running a Python-only user class (e.g. SoakUser)"
  echo "without the risk of the master handing its spawn to boomer, which runs"
  echo "glutton regardless of the class name it was sent. GluttonUser still"
  echo "appears in the class picker but has no worker to run it — redeploy"
  echo "without the flag before running a glutton benchmark."
  echo ""
  echo "Pass --no-boomer to --delete too, or just use a plain --delete: the"
  echo "objects deleted are the same Deployment and Service either way."
}

# render prints the manifest with env vars substituted, dropping the
# boomer-glutton container when --no-boomer is set.
#
# The drop is a text filter rather than a post-apply `kubectl patch` so the
# deployment rolls once instead of twice (the manifest uses strategy:
# Recreate, so each roll is a full stop/start). It skips from the container's
# `- name:` line to the next line at the same indent, i.e. the next list item
# or the `volumes:` key; everything belonging to the container is indented
# deeper and so is consumed.
render() {
  if [[ "${NO_BOOMER}" -eq 1 ]]; then
    envsubst < "${MANIFEST}" | awk '
      /^      - name: boomer-glutton$/ { skip = 1; next }
      skip && /^      [^ ]/           { skip = 0 }
      !skip                           { print }
    '
  else
    envsubst < "${MANIFEST}"
  fi
}

deploy() {
  # The locust manifest targets the `benchmarking` namespace (so prometheus
  # can scrape it when that stack is installed). Ensure it exists either way —
  # benchmarking/monitoring.yaml is otherwise optional.
  echo "Ensuring benchmarking namespace exists..."
  kubectl create namespace benchmarking --dry-run=client -o yaml | kubectl apply -f -
  if [[ "${NO_BOOMER}" -eq 1 ]]; then
    echo "Deploying Locust load without boomer (PROJECT_ID=${PROJECT_ID})..."
  else
    echo "Deploying Locust load (PROJECT_ID=${PROJECT_ID})..."
  fi
  render | kubectl apply -f -
}

delete() {
  echo "Deleting Locust load..."
  render | kubectl delete --ignore-not-found -f -
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy) action="deploy" ;;
    --delete) action="delete" ;;
    --no-boomer) NO_BOOMER=1 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Error: Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ "${action}" == "deploy" ]]; then
  deploy
elif [[ "${action}" == "delete" ]]; then
  delete
fi
