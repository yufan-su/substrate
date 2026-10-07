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

# Builds the campaign image from the checked-out tree: the linux/amd64
# egress-tests driver, campaign.py, plot/fit.py, plot/runs.py and the
# predictor page generator, and under /work/ref the base check's reference
# run, named in /work/ref/base-check-ref, and whichever provisional rows'
# JSONs exist. The tag is ${KO_DOCKER_REPO}/egress-campaign:<short commit>.
# See tools/egress-tests/README.md.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

REF_DIR="/tmp/egress-tests-resources-plan-2026-10-06/tip"
# Keep in sync with PROVISIONAL_FILES in campaign.py.
REF_FILES=(t3-b100-c10.json t1a-b10-c100.json t1b-b10-c100.json t2-b11-c100.json)
MODE="local"

usage() {
  echo "Usage: $0 [--push | --cloud-build] [--ref-dir DIR]"
  echo ""
  echo "  (default)      docker build for linux/amd64; nothing is pushed"
  echo "  --push         also docker push the tag; refuses a dirty tree"
  echo "  --cloud-build  build and push with gcloud builds submit instead of docker;"
  echo "                 refuses a dirty tree"
  echo "  --ref-dir DIR  where the provisional rows' JSONs are (default: ${REF_DIR})"
  echo ""
  echo "BASE_CHECK_REF=PATH names the B=100 C=10 run the base check compares with"
  echo "(default: DIR/t3-b100-c10.json)."
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --push) MODE="push" ;;
    --cloud-build) MODE="cloud-build" ;;
    --ref-dir)
      shift
      REF_DIR="$1"
      ;;
    --ref-dir=*) REF_DIR="${1#*=}" ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
  echo "Error: KO_DOCKER_REPO must be set (see .ate-dev-env.sh)." >&2
  exit 1
fi
TAG="${KO_DOCKER_REPO}/egress-campaign:$(git rev-parse --short=8 HEAD)"
if [[ -n "$(git status --porcelain)" ]]; then
  if [[ "${MODE}" != "local" ]]; then
    echo "Error: the tree is dirty; commit first so ${TAG} names the exact source." >&2
    exit 1
  fi
  TAG="${TAG}-dirty"
fi
BASE_CHECK_REF="${BASE_CHECK_REF:-${REF_DIR}/t3-b100-c10.json}"
if [[ ! -f "${BASE_CHECK_REF}" ]]; then
  echo "Error: ${BASE_CHECK_REF} does not exist; set BASE_CHECK_REF or pass --ref-dir." >&2
  exit 1
fi
BASE_CHECK_NAME="$(basename "${BASE_CHECK_REF}")"
if ! [[ "${BASE_CHECK_NAME}" =~ ^[A-Za-z0-9._-]+\.json$ ]]; then
  echo "Error: BASE_CHECK_REF ${BASE_CHECK_REF} must be a .json file with a plain name." >&2
  exit 1
fi

CONTEXT_DIR="$(mktemp -d)"
trap 'rm -rf "${CONTEXT_DIR}"' EXIT
mkdir -p "${CONTEXT_DIR}/campaign" "${CONTEXT_DIR}/plot" "${CONTEXT_DIR}/ref"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o "${CONTEXT_DIR}/egress-tests" ./tools/egress-tests
cp tools/egress-tests/campaign/image/Dockerfile "${CONTEXT_DIR}/"
cp tools/egress-tests/campaign/campaign.py "${CONTEXT_DIR}/campaign/"
cp tools/egress-tests/plot/fit.py tools/egress-tests/plot/runs.py tools/egress-tests/plot/predictor.py \
  tools/egress-tests/plot/predictor.html.tmpl "${CONTEXT_DIR}/plot/"
cp tools/egress-tests/plot/requirements.txt "${CONTEXT_DIR}/"
# The Pod passes /work/ref/$(cat /work/ref/base-check-ref) to --base-check-ref.
cp "${BASE_CHECK_REF}" "${CONTEXT_DIR}/ref/${BASE_CHECK_NAME}"
echo "${BASE_CHECK_NAME}" >"${CONTEXT_DIR}/ref/base-check-ref"
echo "base check ref: ${BASE_CHECK_NAME}, from ${BASE_CHECK_REF}"
for f in "${REF_FILES[@]}"; do
  if [[ -f "${REF_DIR}/${f}" ]]; then
    cp "${REF_DIR}/${f}" "${CONTEXT_DIR}/ref/"
    echo "ref: ${f}"
  else
    echo "ref: ${f} missing; the fit leaves it out"
  fi
done

case "${MODE}" in
  local|push)
    docker build --platform linux/amd64 -t "${TAG}" "${CONTEXT_DIR}"
    if [[ "${MODE}" == "push" ]]; then
      docker push "${TAG}"
      echo "pushed ${TAG}: $(docker inspect --format '{{index .RepoDigests 0}}' "${TAG}")"
    else
      echo "built ${TAG}: $(docker inspect --format '{{.Id}}' "${TAG}"); not pushed"
    fi
    ;;
  cloud-build)
    gcloud builds submit --tag "${TAG}" "${CONTEXT_DIR}"
    echo "pushed ${TAG}: $(gcloud container images describe "${TAG}" --format='value(image_summary.fully_qualified_digest)')"
    ;;
esac
