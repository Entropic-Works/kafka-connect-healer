#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

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

poll_attempts=${CI_POLL_ATTEMPTS:-100}
poll_interval=${CI_POLL_INTERVAL_SECONDS:-15}
if [[ ! "$poll_attempts" =~ ^[1-9][0-9]*$ || ! "$poll_interval" =~ ^[0-9]+$ ]]; then
  ci_fail CI_WAIT_INVALID 'CI polling attempts must be positive and the interval must be nonnegative integer seconds.' 2
fi
for ((attempt = 0; attempt < poll_attempts; attempt++)); do
  gh api --paginate --slurp "repos/$repo/actions/workflows/ci.yml/runs?event=pull_request&head_sha=$head_sha&per_page=100" \
    | jq '{workflow_runs: [.[].workflow_runs[]]}' > "$workspace/runs.json" ||
    ci_fail CI_GITHUB_API 'Cannot read previous CI runs. Verify gh authentication and Actions read permission.' 3
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
    gh api --paginate --slurp "repos/$repo/actions/runs/$run_id/jobs?per_page=100" \
      | jq '{jobs: [.[].jobs[]]}' > "$workspace/jobs.json" ||
      ci_fail CI_GITHUB_API "Cannot read CI jobs for run $run_id. Verify Actions read permission." 3
    # Identify actual reuse runs. A failed planning job can also skip every
    # validation job; it must invalidate older success rather than be ignored.
    if jq -e 'any(.jobs[]; (.name == "Reuse previous CI" or .name == "metadata") and .conclusion != "skipped")' "$workspace/jobs.json" >/dev/null; then
      continue
    fi
    if [[ "$status" != completed ]]; then
      if [[ "$attempt" == 0 ]]; then
        echo "Waiting for full CI run $run_id for PR #$number: https://github.com/$repo/actions/runs/$run_id"
      fi
      waiting=true
      break
    fi
    if [[ "$conclusion" != success ]]; then
      ci_fail CI_PREVIOUS_FAILED "Full CI run $run_id is $conclusion for PR #$number. Inspect https://github.com/$repo/actions/runs/$run_id and rerun full validation after correcting the failure; description edits cannot replace failed CI."
    fi
    # Accept the previous job names during the workflow migration, while still
    # requiring every validation stage and the protected aggregate check.
    if ! jq -e '
      .jobs as $jobs | [["Plan and promotion policy", "branch-policy"], ["Go checks", "verify"],
        ["Container and Kubernetes", "container"], ["Kafka Connect integration", "integration"], ["CI required"]]
      | all(.[]; . as $names | any($jobs[]; .name as $name | ($names | index($name)) != null and .conclusion == "success"))
    ' "$workspace/jobs.json" >/dev/null; then
      ci_fail CI_PREVIOUS_INCOMPLETE "Run $run_id is missing successful policy, Go, container, integration, or CI required checks. Run full PR validation; branch-push and partial results are not promotion evidence."
    fi
    echo "Full CI passed for these source/base commits in run $run_id."
    ci_summary '## Reused validation' '' "PR #$number: source \`$head_sha\`, base \`$base_sha\`." '' \
      "[Full CI run $run_id](https://github.com/$repo/actions/runs/$run_id) passed every required stage."
    exit 0
  done < "$workspace/candidates.tsv"
  if [[ "$waiting" != true ]]; then
    ci_fail CI_PREVIOUS_MISSING "No full CI exists for PR #$number at source $head_sha and base $base_sha. Run full PR validation; another PR or branch-push run cannot satisfy this gate."
  fi
  if ((attempt + 1 < poll_attempts)); then sleep "$poll_interval"; fi
done
ci_fail CI_PREVIOUS_TIMEOUT "Timed out waiting for full CI on PR #$number. Check the pending run, then rerun this metadata check after full validation completes." 4
