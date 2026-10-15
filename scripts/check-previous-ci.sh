#!/usr/bin/env bash
set -euo pipefail

# A metadata-only run still needs to report the required gate. Reuse evidence
# only from full CI on this PR's exact source and base commits, never push CI or
# another metadata-only run. A newer failed full run invalidates older success.
repo=${GH_REPO:?}
event=${GITHUB_EVENT_PATH:?}
current_run=${GITHUB_RUN_ID:?}
head_sha=$(jq -er '.pull_request.head.sha' "$event")
base_sha=$(jq -er '.pull_request.base.sha' "$event")
base_ref=$(jq -er '.pull_request.base.ref' "$event")
number=$(jq -er '.number' "$event")
workspace=$(mktemp -d)
trap 'rm -rf "$workspace"' EXIT

for ((attempt = 0; attempt < 100; attempt++)); do
  gh api "repos/$repo/actions/workflows/ci.yml/runs?event=pull_request&head_sha=$head_sha&per_page=100" > "$workspace/runs.json"
  jq -r --arg current "$current_run" --arg head "$head_sha" --arg base "$base_sha" \
    --arg ref "$base_ref" --argjson number "$number" '
    .workflow_runs | sort_by(.id) | reverse | .[]
    | select((.id | tostring) != $current and .event == "pull_request" and .head_sha == $head)
    | select(any(.pull_requests[]; .number == $number and .base.ref == $ref and .base.sha == $base and .head.sha == $head))
    | [.id, .status, (.conclusion // "pending")] | @tsv
  ' "$workspace/runs.json" > "$workspace/candidates.tsv"
  waiting=false
  while IFS=$'\t' read -r run_id status conclusion; do
    [[ -n "$run_id" ]] || continue
    gh api "repos/$repo/actions/runs/$run_id/jobs?per_page=100" > "$workspace/jobs.json"
    # All four jobs are skipped on metadata-only runs, so they are not evidence.
    if jq -e '[.jobs[] | select(.name == "branch-policy" or .name == "verify" or .name == "container" or .name == "integration")]
      | length == 4 and all(.[]; .conclusion == "skipped")' "$workspace/jobs.json" >/dev/null; then
      continue
    fi
    if [[ "$status" != completed ]]; then
      waiting=true
      break
    fi
    [[ "$conclusion" == success ]]
    jq -e '
      .jobs as $jobs | ["branch-policy", "verify", "container", "integration", "CI required"]
      | all(.[]; . as $name | any($jobs[]; .name == $name and .conclusion == "success"))
    ' "$workspace/jobs.json" >/dev/null
    echo "Full CI passed for these source/base commits in run $run_id."
    exit 0
  done < "$workspace/candidates.tsv"
  if [[ "$waiting" != true ]]; then
    echo 'No successful full CI exists for this PR and its current source/base commits. Rerun full PR validation.' >&2
    exit 1
  fi
  sleep 15
done
echo 'Timed out waiting for full PR validation.' >&2
exit 1
