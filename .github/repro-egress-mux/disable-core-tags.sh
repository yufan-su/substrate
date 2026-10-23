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

# Reverts the runsc core tagging commit in the working tree, for the control leg.

set -o errexit -o nounset -o pipefail

sha="$(git log -1 --format=%H --grep='^Enable runsc core tagging')"
if [[ -z "${sha}" ]]; then
  echo "core tagging commit not found" >&2
  exit 1
fi
git revert --no-commit "${sha}"
git status --short
