#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

event_file=${GITHUB_EVENT_PATH:?}
base=$(jq -r '.pull_request.base.ref' "$event_file")
if [[ "$base" == dev ]]; then
  exit 0
fi

workspace=$(mktemp -d)
trap 'rm -rf "$workspace"' EXIT
jq -r '.pull_request.body // ""' "$event_file" > "$workspace/body.md"
bash "$(dirname "$0")/check-candidate.sh" "$workspace/body.md" \
  "$(jq -r '.pull_request.head.sha' "$event_file")"

if [[ "$base" == staging ]]; then
  candidates=$(gh pr list --repo "${GH_REPO:?}" --state open --base main --head staging \
    --json number,isCrossRepository --jq '[.[] | select(.isCrossRepository == false)]') ||
    ci_fail CI_GITHUB_API 'Cannot check open release candidates. Verify gh authentication and pull-request read access.' 3
  if [[ "$(jq length <<< "$candidates")" != 0 ]]; then
    ci_fail CI_STAGING_FROZEN "Staging is frozen by open staging-to-main PR(s): $(jq -r 'map("#" + (.number | tostring)) | join(", ")' <<< "$candidates"). Complete or close that candidate before promoting more changes into staging."
  fi
fi
