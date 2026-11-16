#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

needs=${NEEDS_JSON:?Set NEEDS_JSON to the workflow needs context}
integration_required=${INTEGRATION_REQUIRED:-false}
if ! jq -e 'type == "object" and has("plan")' <<< "$needs" >/dev/null; then
  ci_fail CI_RESULTS_INVALID 'The CI gate did not receive valid job results.' 2
fi
case "$integration_required" in
  true|false) ;;
  *) ci_fail CI_RESULTS_INVALID 'INTEGRATION_REQUIRED must be true or false.' 2 ;;
esac

labels='{"plan":"Plan and promotion policy","verify":"Go checks","metadata":"Reuse previous CI","container":"Container and Kubernetes","integration":"Kafka Connect integration"}'
mode=$(jq -r '.plan.outputs.mode // "unknown"' <<< "$needs")
ci_summary '## CI results' '' "Validation mode: **$mode**" '' '| Check | Result |' '| --- | --- |'
while IFS=$'\t' read -r name result; do
  printf '%s: %s\n' "$name" "$result"
  ci_summary "| $name | $result |"
done < <(jq -r --argjson labels "$labels" 'to_entries[] | [($labels[.key] // .key), .value.result] | @tsv' <<< "$needs")

if [[ "$(jq -r '.plan.result' <<< "$needs")" != success ]]; then
  ci_fail CI_PLAN_FAILED 'Plan and promotion policy did not pass. Check its failed step and error code; dependent validation jobs were not started.'
fi
case "$mode" in
  feature)
    if [[ "$integration_required" == true ]]; then
      ci_fail CI_MODE_INVALID 'Feature-only checks cannot satisfy a PR or release that requires integration.' 2
    fi
    required=(verify)
    ;;
  metadata) required=(metadata) ;;
  full)
    required=(verify container)
    if [[ "$integration_required" == true ]]; then required+=(integration); fi
    ;;
  *) ci_fail CI_MODE_INVALID "Unknown CI mode $mode. Inspect the planning step." 2 ;;
esac

failed=false
for job in "${required[@]}"; do
  result=$(jq -r --arg job "$job" '.[$job].result // "missing"' <<< "$needs")
  if [[ "$result" != success ]]; then
    label=$(jq -r --arg job "$job" '.[$job]' <<< "$labels")
    ci_error CI_CHECK_FAILED "$label is $result. Open that job for the original failure and recovery instructions; skipped or missing required checks cannot pass CI."
    failed=true
  fi
done
# A branch push does not require integration, but a failed job is never ignored.
if [[ "$mode" == full && "$integration_required" == false ]]; then
  result=$(jq -r '.integration.result // "missing"' <<< "$needs")
  if [[ "$result" != skipped && "$result" != success ]]; then
    ci_error CI_CHECK_FAILED "Kafka Connect integration is $result; expected skipped or success for this branch run."
    failed=true
  fi
fi
if [[ "$failed" == true ]]; then exit 1; fi
ci_summary '' 'All required checks passed.'
echo 'All required checks passed.'
