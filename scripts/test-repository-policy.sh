#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
promotion="$repo_root/scripts/check-promotion.sh"
release="$repo_root/scripts/check-release-tag.sh"
candidate="$repo_root/scripts/check-candidate.sh"
ci_mode="$repo_root/scripts/ci-mode.sh"
promote="$repo_root/scripts/promote.sh"
state="$repo_root/scripts/check-promotion-state.sh"
select_image="$repo_root/scripts/select-release-image.sh"
previous_ci="$repo_root/scripts/check-previous-ci.sh"

expect_pass() {
  if ! "$@" >/dev/null 2>&1; then
    echo "Expected success: $*" >&2
    exit 1
  fi
}
expect_fail() {
  if "$@" >/dev/null 2>&1; then
    echo "Expected rejection: $*" >&2
    exit 1
  fi
}

expect_pass env BASE_BRANCH=dev HEAD_BRANCH=feature HEAD_REPOSITORY=contributor/fork BASE_REPOSITORY=owner/repo bash "$promotion"
expect_pass env BASE_BRANCH=staging HEAD_BRANCH=dev HEAD_REPOSITORY=owner/repo BASE_REPOSITORY=owner/repo bash "$promotion"
expect_pass env BASE_BRANCH=main HEAD_BRANCH=staging HEAD_REPOSITORY=owner/repo BASE_REPOSITORY=owner/repo bash "$promotion"
expect_fail env BASE_BRANCH=staging HEAD_BRANCH=feature HEAD_REPOSITORY=owner/repo BASE_REPOSITORY=owner/repo bash "$promotion"
expect_fail env BASE_BRANCH=main HEAD_BRANCH=dev HEAD_REPOSITORY=owner/repo BASE_REPOSITORY=owner/repo bash "$promotion"
expect_fail env BASE_BRANCH=main HEAD_BRANCH=feature HEAD_REPOSITORY=owner/repo BASE_REPOSITORY=owner/repo bash "$promotion"
expect_fail env BASE_BRANCH=main HEAD_BRANCH=staging HEAD_REPOSITORY=contributor/fork BASE_REPOSITORY=owner/repo bash "$promotion"
expect_fail env BASE_BRANCH=production HEAD_BRANCH=staging HEAD_REPOSITORY=owner/repo BASE_REPOSITORY=owner/repo bash "$promotion"

for tag in 0.1.0 1.2.3 1.2.3-rc.1 1.2.3-alpha-beta.0; do
  expect_pass bash "$release" "$tag"
done
for tag in v1.2.3 01.2.3 1.02.3 1.2.03 1.2 1.2.3- 1.2.3-rc..1 1.2.3-rc.01 1.2.3+build; do
  expect_fail bash "$release" "$tag"
done

# Fixtures are isolated and never change the project's Git refs or config.
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

# Exercise CI event selection, including metadata edits that must not publish a
# fresh passing required check or interrupt code validation.
expect_mode() {
  local expected=$1
  shift
  local actual
  actual=$("$@" bash "$ci_mode")
  [[ "$actual" == "$expected" ]] || { echo "Expected CI mode $expected, got $actual" >&2; exit 1; }
}
expect_mode feature env GITHUB_EVENT_NAME=push GITHUB_REF=refs/heads/feature
for branch in dev staging main; do
  expect_mode full env GITHUB_EVENT_NAME=push GITHUB_REF="refs/heads/$branch"
done
expect_mode full env GITHUB_EVENT_NAME=push GITHUB_REF=refs/tags/1.2.3
expect_mode full env GITHUB_EVENT_NAME=workflow_call
event="$fixture/event.json"
export GITHUB_EVENT_PATH="$event"
printf '%s\n' '{"action":"opened"}' > "$event"
expect_mode full env GITHUB_EVENT_NAME=pull_request
printf '%s\n' '{"action":"edited","changes":{"title":{"from":"old"}},"pull_request":{"body":"notes"}}' > "$event"
expect_mode ignore env GITHUB_EVENT_NAME=pull_request
printf '%s\n' '{"action":"edited","changes":{"base":{"ref":{"from":"dev"}}}}' > "$event"
expect_mode full env GITHUB_EVENT_NAME=pull_request
source_sha=1111111111111111111111111111111111111111
other_sha=2222222222222222222222222222222222222222
body="$fixture/body.md"
printf '<!-- promotion-source: %s -->\n' "$source_sha" > "$body"
expect_pass bash "$candidate" "$body" "$source_sha"
expect_fail bash "$candidate" "$body" "$other_sha"
expect_fail bash "$candidate" "$body" short
jq -n --rawfile body "$body" '{action:"edited",changes:{body:{from:$body}},pull_request:{body:($body + "Release notes")}}' > "$event"
expect_mode ignore env GITHUB_EVENT_NAME=pull_request
jq -n --rawfile body "$body" '{action:"edited",changes:{title:{from:"old title"}},pull_request:{body:$body}}' > "$event"
expect_mode ignore env GITHUB_EVENT_NAME=pull_request
jq -n --rawfile body "$body" '{action:"edited",changes:{body:{from:""}},pull_request:{body:$body}}' > "$event"
expect_mode full env GITHUB_EVENT_NAME=pull_request
jq -n --rawfile body "$body" '{action:"edited",changes:{body:{from:$body}},pull_request:{body:""}}' > "$event"
expect_mode full env GITHUB_EVENT_NAME=pull_request
printf '<!-- promotion-source: %s -->\n' "$source_sha" >> "$body"
expect_fail bash "$candidate" "$body" "$source_sha"
printf 'No candidate snapshot\n' > "$body"
expect_fail bash "$candidate" "$body" "$source_sha"

# Test the real orchestration against a local GitHub CLI fixture. No remote refs,
# PRs, or releases are changed by these tests.
mkdir "$fixture/bin"
cat > "$fixture/bin/gh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  'api repos/owner/repo/actions/workflows/ci.yml/runs?'*) cat "${MOCK_CI_RUNS:?}" ;;
  'api repos/owner/repo/actions/runs/200/jobs?'*) cat "${MOCK_METADATA_JOBS:?}" ;;
  'api repos/owner/repo/actions/runs/'*'/jobs?'*) cat "${MOCK_PREVIOUS_JOBS:?}" ;;
  'api repos/owner/repo/git/ref/heads/'*)
    if [[ -n "${MOCK_REF_CALLS_FILE:-}" ]]; then
      count=$(cat "$MOCK_REF_CALLS_FILE")
      count=$((count + 1))
      echo "$count" > "$MOCK_REF_CALLS_FILE"
      if [[ "$count" -ge 3 ]]; then
        echo 2222222222222222222222222222222222222222
        exit 0
      fi
    fi
    echo "${MOCK_SOURCE_SHA:?}"
    ;;
  'api repos/owner/repo/compare/'*)
    if [[ "${MOCK_EMPTY:-false}" == true ]]; then
      echo '{"files":[],"commits":[]}'
      exit 0
    fi
    printf '{"files":[{"filename":"change.go"}],"commits":[{"sha":"%s","parents":[{}],"commit":{"message":"Fix connector restart"}}]}\n' "$MOCK_SOURCE_SHA"
    ;;
  'pr list --repo owner/repo --state open --base main --head staging --json number,isCrossRepository --jq '*)
    if [[ "${MOCK_FROZEN:-false}" == true ]]; then echo '[{"number":123}]'; else echo '[]'; fi
    ;;
  'pr list '*) cat "${MOCK_EXISTING:?}" ;;
  'pr create '*)
    printf '%s\n' "$@" > "${MOCK_CREATED:?}"
    while [[ "$#" -gt 0 ]]; do
      if [[ "$1" == --body-file ]]; then cp "$2" "$MOCK_CREATED.body"; break; fi
      shift
    done
    echo 'https://github.com/owner/repo/pull/123'
    ;;
  *) echo "Unexpected gh invocation: $*" >&2; exit 1 ;;
esac
MOCK
chmod +x "$fixture/bin/gh"
export PATH="$fixture/bin:$PATH" GH_REPO=owner/repo
export MOCK_SOURCE_SHA="$source_sha" MOCK_EXISTING="$fixture/existing.json" MOCK_CREATED="$fixture/created"
echo '[]' > "$MOCK_EXISTING"
expect_fail env TARGET_BRANCH=production RELEASE_VERSION=1.2.3 EXPECTED_SHA="$source_sha" bash "$promote"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=1.2.3-rc.1 EXPECTED_SHA="$source_sha" bash "$promote"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=1.2.3 EXPECTED_SHA="$other_sha" bash "$promote"
expect_fail env TARGET_BRANCH=staging RELEASE_VERSION=1.2.3 EXPECTED_SHA="$source_sha" MOCK_FROZEN=true bash "$promote"
[[ ! -e "$MOCK_CREATED" ]]
for target in staging main; do
  expect_pass env TARGET_BRANCH="$target" RELEASE_VERSION=1.2.3 EXPECTED_SHA="$source_sha" bash "$promote"
  expect_pass bash "$candidate" "$MOCK_CREATED.body" "$source_sha"
  [[ "$(cat "$MOCK_CREATED.body")" == *'Fix connector restart'* ]]
done
jq -n --arg sha "$source_sha" --rawfile body "$MOCK_CREATED.body" \
  '[{number:123,title:"Promote 1.2.3: staging to main",headRefOid:$sha,body:$body,url:"https://github.com/owner/repo/pull/123"}]' > "$MOCK_EXISTING"
rm "$MOCK_CREATED"
expect_pass env TARGET_BRANCH=main RELEASE_VERSION=1.2.3 EXPECTED_SHA="$source_sha" bash "$promote"
[[ ! -e "$MOCK_CREATED" ]]
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=1.2.4 EXPECTED_SHA="$source_sha" bash "$promote"
echo '[]' > "$MOCK_EXISTING"
echo 0 > "$fixture/ref-calls"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=1.2.3 EXPECTED_SHA="$source_sha" \
  MOCK_REF_CALLS_FILE="$fixture/ref-calls" bash "$promote"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=1.2.3 EXPECTED_SHA="$source_sha" MOCK_EMPTY=true bash "$promote"
[[ ! -e "$MOCK_CREATED" ]]
jq -n --arg sha "$source_sha" --rawfile body "$MOCK_CREATED.body" \
  '{pull_request:{base:{ref:"staging"},head:{sha:$sha},body:$body}}' > "$event"
expect_pass bash "$state"
expect_fail env MOCK_FROZEN=true bash "$state"
jq --arg sha "$other_sha" '.pull_request.head.sha = $sha' "$event" > "$fixture/changed.json"
expect_fail env GITHUB_EVENT_PATH="$fixture/changed.json" bash "$state"
jq '.pull_request.base.ref = "dev"' "$fixture/changed.json" > "$event"
expect_pass env MOCK_FROZEN=true bash "$state"

# A notes-only run must prove prior full CI for this PR and both current commits.
export GITHUB_RUN_ID=201 MOCK_CI_RUNS="$fixture/ci-runs.json"
export MOCK_PREVIOUS_JOBS="$fixture/full-jobs.json" MOCK_METADATA_JOBS="$fixture/metadata-jobs.json"
jq -n --arg head "$source_sha" --arg base "$other_sha" \
  '{number:18,pull_request:{head:{sha:$head},base:{ref:"staging",sha:$base}}}' > "$event"
jq -n '{jobs:(["branch-policy","verify","container","integration","CI required"]
  | map({name:.,conclusion:"success"}))}' > "$MOCK_PREVIOUS_JOBS"
jq -n '{jobs:(["branch-policy","verify","container","integration"]
  | map({name:.,conclusion:"skipped"}))}' > "$MOCK_METADATA_JOBS"
jq -n --arg head "$source_sha" --arg base "$other_sha" '{workflow_runs:[
  {id:200,event:"pull_request",head_sha:$head,status:"completed",conclusion:"success",
   pull_requests:[{number:18,head:{sha:$head},base:{ref:"staging",sha:$base}}]},
  {id:100,event:"pull_request",head_sha:$head,status:"completed",conclusion:"success",
   pull_requests:[{number:18,head:{sha:$head},base:{ref:"staging",sha:$base}}]}
]}' > "$MOCK_CI_RUNS"
cp "$MOCK_CI_RUNS" "$fixture/good-ci-runs.json"
expect_pass bash "$previous_ci"
jq '.workflow_runs[1].conclusion = "failure"' "$fixture/good-ci-runs.json" > "$MOCK_CI_RUNS"
expect_fail bash "$previous_ci"
for mutation in '.workflow_runs[1].head_sha = "other"' \
  '.workflow_runs[1].pull_requests[0].base.sha = "other"' \
  '.workflow_runs[1].pull_requests[0].number = 19' \
  '.workflow_runs[1].event = "push"' \
  '.workflow_runs[1].id = 201'; do
  jq "$mutation" "$fixture/good-ci-runs.json" > "$MOCK_CI_RUNS"
  expect_fail bash "$previous_ci"
done
jq '.workflow_runs += [(.workflow_runs[1] | .id = 300 | .conclusion = "failure")]' \
  "$fixture/good-ci-runs.json" > "$MOCK_CI_RUNS"
expect_fail bash "$previous_ci"
cp "$fixture/good-ci-runs.json" "$MOCK_CI_RUNS"
jq '.jobs |= map(select(.name != "integration"))' "$MOCK_PREVIOUS_JOBS" > "$fixture/missing-integration.json"
expect_fail env MOCK_PREVIOUS_JOBS="$fixture/missing-integration.json" bash "$previous_ci"

# Select runnable platform manifests, excluding provenance/SBOM attestations.
digest_a=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
digest_b=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
image="example/healer@$digest_a"
manifest="$fixture/manifest.json"
jq -n --arg a "$digest_a" --arg b "$digest_b" '{manifests:[
  {digest:$a,platform:{os:"linux",architecture:"amd64"}},
  {digest:$b,platform:{os:"linux",architecture:"arm64"}},
  {digest:$a,platform:{os:"unknown",architecture:"unknown"}}
]}' > "$manifest"
[[ "$(bash "$select_image" "$manifest" "$image" amd64)" == "example/healer@$digest_a" ]]
[[ "$(bash "$select_image" "$manifest" "$image" arm64)" == "example/healer@$digest_b" ]]
expect_fail bash "$select_image" "$manifest" example/healer:1.2.3 arm64
expect_fail bash "$select_image" "$manifest" "$image" ppc64le
echo '{"manifests":[]}' > "$manifest"
expect_fail bash "$select_image" "$manifest" "$image" arm64
jq -n --arg digest "$digest_a" '{manifests:[
  {digest:$digest,platform:{os:"linux",architecture:"arm64"}},
  {digest:$digest,platform:{os:"linux",architecture:"arm64"}}
]}' > "$manifest"
expect_fail bash "$select_image" "$manifest" "$image" arm64

# The current-base requirement applies only to dev; tag mutation has no bypass.
jq -e '.conditions.ref_name.include == ["refs/heads/dev"] and
  .bypass_actors == [] and .rules[0].parameters.strict_required_status_checks_policy == true' \
  "$repo_root/.github/rulesets/dev-current-base.json" >/dev/null
jq -e '.bypass_actors == [] and ([.rules[].type] | sort) == ["deletion","update"]' \
  "$repo_root/.github/rulesets/tag-ruleset.json" >/dev/null
jq -e '.conditions.ref_name.include == ["refs/tags/v*", "refs/tags/[0-9]*"] and
  .rules == [{"type":"creation"}]' \
  "$repo_root/.github/rulesets/release-tag-creation.json" >/dev/null

# Exercise real ancestry: staging has a candidate that main has not received.
cd "$fixture"
git init -q
git -c user.name='Policy test' -c user.email='policy@example.invalid' commit -q --allow-empty -m stable
git update-ref refs/remotes/origin/main HEAD
git -c user.name='Policy test' -c user.email='policy@example.invalid' commit -q --allow-empty -m candidate
git update-ref refs/remotes/origin/staging HEAD
expect_pass bash "$release" 1.2.3-rc.1 --check-branch
expect_fail bash "$release" 1.2.3 --check-branch
git update-ref refs/remotes/origin/main HEAD
expect_pass bash "$release" 1.2.3 --check-branch
git -c user.name='Policy test' -c user.email='policy@example.invalid' commit -q --allow-empty -m unpromoted
expect_fail bash "$release" 1.2.3 --check-branch
expect_fail bash "$release" 1.2.3-rc.1 --check-branch

echo 'Repository policy checks passed.'
