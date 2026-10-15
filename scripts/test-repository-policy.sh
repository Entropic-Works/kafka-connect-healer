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

for tag in v0.1.0 v1.2.3 v1.2.3-rc.1 v1.2.3-alpha-beta.0; do
  expect_pass bash "$release" "$tag"
done
for tag in 1.2.3 v01.2.3 v1.02.3 v1.2.03 v1.2 v1.2.3- v1.2.3-rc..1 v1.2.3-rc.01 v1.2.3+build; do
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
expect_mode full env GITHUB_EVENT_NAME=push GITHUB_REF=refs/tags/v1.2.3
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
expect_fail env TARGET_BRANCH=production RELEASE_VERSION=v1.2.3 EXPECTED_SHA="$source_sha" bash "$promote"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=v1.2.3-rc.1 EXPECTED_SHA="$source_sha" bash "$promote"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=v1.2.3 EXPECTED_SHA="$other_sha" bash "$promote"
expect_fail env TARGET_BRANCH=staging RELEASE_VERSION=v1.2.3 EXPECTED_SHA="$source_sha" MOCK_FROZEN=true bash "$promote"
[[ ! -e "$MOCK_CREATED" ]]
for target in staging main; do
  expect_pass env TARGET_BRANCH="$target" RELEASE_VERSION=v1.2.3 EXPECTED_SHA="$source_sha" bash "$promote"
  expect_pass bash "$candidate" "$MOCK_CREATED.body" "$source_sha"
  [[ "$(cat "$MOCK_CREATED.body")" == *'Fix connector restart'* ]]
done
jq -n --arg sha "$source_sha" --rawfile body "$MOCK_CREATED.body" \
  '[{number:123,title:"Promote v1.2.3: staging to main",headRefOid:$sha,body:$body,url:"https://github.com/owner/repo/pull/123"}]' > "$MOCK_EXISTING"
rm "$MOCK_CREATED"
expect_pass env TARGET_BRANCH=main RELEASE_VERSION=v1.2.3 EXPECTED_SHA="$source_sha" bash "$promote"
[[ ! -e "$MOCK_CREATED" ]]
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=v1.2.4 EXPECTED_SHA="$source_sha" bash "$promote"
echo '[]' > "$MOCK_EXISTING"
echo 0 > "$fixture/ref-calls"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=v1.2.3 EXPECTED_SHA="$source_sha" \
  MOCK_REF_CALLS_FILE="$fixture/ref-calls" bash "$promote"
expect_fail env TARGET_BRANCH=main RELEASE_VERSION=v1.2.3 EXPECTED_SHA="$source_sha" MOCK_EMPTY=true bash "$promote"
[[ ! -e "$MOCK_CREATED" ]]
jq -n --arg sha "$source_sha" --rawfile body "$MOCK_CREATED.body" \
  '{pull_request:{base:{ref:"staging"},head:{sha:$sha},body:$body}}' > "$event"
expect_pass bash "$state"
expect_fail env MOCK_FROZEN=true bash "$state"
jq --arg sha "$other_sha" '.pull_request.head.sha = $sha' "$event" > "$fixture/changed.json"
expect_fail env GITHUB_EVENT_PATH="$fixture/changed.json" bash "$state"
jq '.pull_request.base.ref = "dev"' "$fixture/changed.json" > "$event"
expect_pass env MOCK_FROZEN=true bash "$state"

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
expect_fail bash "$select_image" "$manifest" example/healer:v1.2.3 arm64
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

# Exercise real ancestry: staging has a candidate that main has not received.
cd "$fixture"
git init -q
git -c user.name='Policy test' -c user.email='policy@example.invalid' commit -q --allow-empty -m stable
git update-ref refs/remotes/origin/main HEAD
git -c user.name='Policy test' -c user.email='policy@example.invalid' commit -q --allow-empty -m candidate
git update-ref refs/remotes/origin/staging HEAD
expect_pass bash "$release" v1.2.3-rc.1 --check-branch
expect_fail bash "$release" v1.2.3 --check-branch
git update-ref refs/remotes/origin/main HEAD
expect_pass bash "$release" v1.2.3 --check-branch
git -c user.name='Policy test' -c user.email='policy@example.invalid' commit -q --allow-empty -m unpromoted
expect_fail bash "$release" v1.2.3 --check-branch
expect_fail bash "$release" v1.2.3-rc.1 --check-branch

echo 'Repository policy checks passed.'
