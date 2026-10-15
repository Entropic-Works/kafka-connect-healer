#!/usr/bin/env bash
set -euo pipefail

body_file=${1:?Usage: check-candidate.sh PR_BODY_FILE SOURCE_SHA}
source_sha=${2:?Usage: check-candidate.sh PR_BODY_FILE SOURCE_SHA}
recorded_sha=$(sed -n 's/^<!-- promotion-source: \([0-9a-f]\{40\}\) -->$/\1/p' "$body_file")
if [[ ! "$source_sha" =~ ^[0-9a-f]{40}$ || "$recorded_sha" != "$source_sha" ]]; then
  echo 'The candidate changed or has no source snapshot. Close the candidate PR and deliberately prepare a replacement with Promote.' >&2
  exit 1
fi
