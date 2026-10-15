#!/usr/bin/env bash
set -euo pipefail

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
    --json number,isCrossRepository --jq '[.[] | select(.isCrossRepository == false)]')
  if [[ "$(jq length <<< "$candidates")" != 0 ]]; then
    echo 'Staging is frozen by an open candidate PR. Close it before preparing a replacement.' >&2
    exit 1
  fi
fi
