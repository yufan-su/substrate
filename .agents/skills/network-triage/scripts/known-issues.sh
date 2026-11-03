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

# Searches the repo's issues and merged PRs for each phrase: issues labeled
# area/network, then all issues, because not every issue has a label, then
# merged PRs. With --view, prints one issue or PR with its comments, and the
# merge commit of a merged PR.
#
# It uses gh when gh is installed and logged in. Otherwise it calls the GitHub
# API with curl, which allows 10 searches a minute without a token. Set
# GITHUB_TOKEN to raise that.
#
# Usage: known-issues.sh <phrase> [<phrase> ...]
#        known-issues.sh --view <number>

set -o errexit -o nounset -o pipefail

# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
  echo "Usage: $0 <phrase> [<phrase> ...]" >&2
  echo "       $0 --view <number>" >&2
  exit 2
}

if [[ "$#" -eq 0 ]] || [[ "$1" == --view && "$#" -ne 2 ]]; then
  usage
fi
need jq

REPO=agent-substrate/substrate
if command -v gh >/dev/null && gh auth status >/dev/null 2>&1; then
  TRANSPORT=gh
else
  TRANSPORT=curl
fi

# api GETs a GitHub API path, such as /repos/o/r/issues/1, and prints the JSON.
api() {
  if [[ "${TRANSPORT}" == gh ]]; then
    gh api "${1#/}"
    return
  fi
  local auth=()
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    auth=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
  fi
  curl -sS --max-time 20 -H 'Accept: application/vnd.github+json' \
    ${auth[@]+"${auth[@]}"} "https://api.github.com$1"
}

# search prints the issues or PRs that match GitHub search query $1, one line
# each, formatted by jq filter $2.
search() {
  local json
  printf '\n$ %s search: %s\n' "${TRANSPORT}" "$1"
  if ! json="$(api "/search/issues?per_page=20&q=$(jq -rn --arg q "$1" '$q | @uri')" 2>&1)" \
      || ! jq -e '.items' >/dev/null 2>&1 <<<"${json}"; then
    printf '(search failed: %s)\n' "$(jq -r '.message // .' <<<"${json}" 2>/dev/null || echo "${json}")"
    return 0
  fi
  jq -r ".items[] | ${2}" <<<"${json}" | tail_lines
}

# view prints issue or PR $1 with its comments.
view() {
  local json
  if ! json="$(api "/repos/${REPO}/issues/$1" 2>&1)" || ! jq -e '.number' >/dev/null 2>&1 <<<"${json}"; then
    printf 'Cannot read #%s: %s\n' "$1" "$(jq -r '.message // .' <<<"${json}" 2>/dev/null || echo "${json}")" >&2
    exit 1
  fi
  jq -r '"#\(.number) [\(.state)] \(.title)\n\(.html_url)\nlabels: \([.labels[].name] | join(", "))\n\n\(.body // "")"' <<<"${json}"
  if jq -e '.pull_request' >/dev/null <<<"${json}"; then
    api "/repos/${REPO}/pulls/$1" \
      | jq -r '"\nmerged: \(.merged_at // "no")\nmerge commit: \(.merge_commit_sha // "none")"'
  fi
  api "/repos/${REPO}/issues/$1/comments?per_page=100" \
    | jq -r '.[] | "\n--- \(.user.login), \(.created_at[0:10])\n\(.body)"'
}

if [[ "$1" == --view ]]; then
  view "$2"
  exit 0
fi

ISSUE='"#\(.number)\t\(.state)\t\(.title)"'
PR='"#\(.number)\tmerged \(.pull_request.merged_at[0:10])\t\(.title)"'
for phrase in "$@"; do
  section "\"${phrase}\""
  search "repo:${REPO} is:issue label:area/network \"${phrase}\"" "${ISSUE}"
  search "repo:${REPO} is:issue \"${phrase}\"" "${ISSUE}"
  search "repo:${REPO} is:pr is:merged \"${phrase}\"" "${PR}"
done
