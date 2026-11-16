#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

body_file=${1:?Usage: check-candidate.sh PR_BODY_FILE SOURCE_SHA}
source_sha=${2:?Usage: check-candidate.sh PR_BODY_FILE SOURCE_SHA}
if [[ ! "$source_sha" =~ ^[0-9a-f]{40}$ ]]; then
  ci_fail CI_SOURCE_SHA_INVALID 'Expected a full, lowercase 40-character source commit SHA.' 2
fi
# GitHub description edits can change LF to CRLF without changing the snapshot.
recorded_sha=$(sed $'s/\r$//' "$body_file" | sed -n 's/^<!-- promotion-source: \([0-9a-f]\{40\}\) -->$/\1/p')
if [[ -z "$recorded_sha" ]]; then
  ci_fail CI_CANDIDATE_MISSING 'The PR description must contain exactly one <!-- promotion-source: FULL_SHA --> line. Restore the reviewed source marker; line endings may be LF or CRLF.'
fi
if [[ "$recorded_sha" == *$'\n'* ]]; then
  ci_fail CI_CANDIDATE_DUPLICATE 'The PR description contains multiple source markers. Keep exactly one reviewed source snapshot.'
fi
if [[ "$recorded_sha" != "$source_sha" ]]; then
  ci_fail CI_CANDIDATE_CHANGED "The candidate records $recorded_sha, but the source branch is at $source_sha. Review the new source and deliberately prepare a replacement promotion PR."
fi
printf 'Candidate source verified: %s\n' "$source_sha"
