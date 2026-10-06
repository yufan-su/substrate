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


# Runs the unit tests of tools/egress-tests/plot, the Python plotting and
# cross-run checks of the egress scale test. They need plotly, so this script
# keeps a venv at tools/egress-tests/plot/venv, as python-licenses.sh does,
# and creates it on demand. Go's test target cannot reach them, and a test
# nothing runs is not a test.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

source hack/util/venv.sh

PLOT_DIR="tools/egress-tests/plot"
VENV="${PLOT_DIR}/venv"

ensure_venv "${VENV}"
venv_sync_requirements "${VENV}" "${PLOT_DIR}/requirements.txt"

(cd "${PLOT_DIR}" && "${ROOT}/${VENV}/bin/python" -m unittest test_plot)
