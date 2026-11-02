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

# Runs the network-triage skill evals in cases.yaml. The skill runs in a temp
# directory that holds only the skill, so it can't read the repo, its git
# history, or the expected answers. Extra arguments go to promptfoo, such as
# --repeat 3 or --filter-pattern DNS.
#
# Auth: ANTHROPIC_API_KEY, or a local Claude Code login.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
EVALS="${ROOT}/.agents/evals/network-triage"

EVAL_DIR="$(mktemp -d)"
trap 'rm -rf "${EVAL_DIR}"' EXIT
mkdir -p "${EVAL_DIR}/.claude/skills"
cp -R "${ROOT}/.agents/skills/network-triage" "${EVAL_DIR}/.claude/skills/"
export EVAL_DIR
export PROMPTFOO_DISABLE_TELEMETRY=1

npx --yes \
  --package promptfoo@0.125.1 \
  --package @anthropic-ai/claude-agent-sdk@0.3.316 \
  promptfoo eval --config "${EVALS}/promptfooconfig.yaml" --no-cache "$@"
