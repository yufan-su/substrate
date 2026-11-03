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

# Prints the cluster facts for a triage report: Substrate version, Kubernetes
# version, nodes, WorkerPools, and workers. On GKE, pass the cluster name and
# location to also print the control plane version, datapath, and node pools.
#
# Usage: cluster.sh [<gke-cluster> <gke-location>]

set -o errexit -o nounset -o pipefail

# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ "$#" -ne 0 && "$#" -ne 2 ]]; then
  echo "Usage: $0 [<gke-cluster> <gke-location>]" >&2
  exit 2
fi

section "Substrate version"
run kubectl -n ate-system get deploy,ds \
  -o 'custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[*].image'

section "Kubernetes version"
run kubectl version

section "Nodes"
run kubectl get nodes -o wide \
  -L node.kubernetes.io/instance-type,kubernetes.io/arch,topology.kubernetes.io/zone

section "WorkerPools and workers"
run kubectl get workerpool -A
run kubectl ate get workers

if [[ "$#" -eq 2 ]]; then
  section "GKE"
  run gcloud container clusters describe "$1" --location "$2" \
    --format='value(currentMasterVersion,networkConfig.datapathProvider,networkPolicy.enabled)'
  run gcloud container node-pools list --cluster "$1" --location "$2"
fi
