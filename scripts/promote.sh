#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

repo=${GH_REPO:?Set GH_REPO to owner/repository}
target=${TARGET_BRANCH:?Set TARGET_BRANCH to staging or main}
version=${RELEASE_VERSION:?Set RELEASE_VERSION to MAJOR.MINOR.PATCH}
expected_sha=${EXPECTED_SHA:?Set EXPECTED_SHA to the full reviewed source commit}
script_directory=$(cd "$(dirname "$0")" && pwd)

bash "$script_directory/check-release-tag.sh" "$version"
if [[ "$version" == *-* || ! "$expected_sha" =~ ^[0-9a-f]{40}$ ]]; then
  ci_fail CI_PROMOTION_INPUT 'Use a stable MAJOR.MINOR.PATCH release version and a full 40-character source SHA for promotion.' 2
fi
case "$target" in
  staging) source=dev ;;
  main) source=staging ;;
  *) ci_fail CI_TARGET_UNSUPPORTED 'Promote only to staging (from dev) or main (from staging).' 2 ;;
esac

source_sha=$(gh api "repos/$repo/git/ref/heads/$source" --jq '.object.sha') ||
  ci_fail CI_GITHUB_API "Cannot read $source. Verify gh authentication and repository access." 3
base_sha=$(gh api "repos/$repo/git/ref/heads/$target" --jq '.object.sha') ||
  ci_fail CI_GITHUB_API "Cannot read $target. Verify gh authentication and repository access." 3
if [[ "$source_sha" != "$expected_sha" ]]; then
  ci_fail CI_SOURCE_CHANGED "$source is at $source_sha, but EXPECTED_SHA is $expected_sha. Review the current commit and rerun with its full SHA."
fi

if [[ "$target" == staging ]]; then
  candidates=$(gh pr list --repo "$repo" --state open --base main --head staging \
    --json number,isCrossRepository --jq '[.[] | select(.isCrossRepository == false)]') ||
    ci_fail CI_GITHUB_API 'Cannot check open release candidates. Verify pull-request read access.' 3
  if [[ "$(jq length <<< "$candidates")" != 0 ]]; then
    ci_fail CI_STAGING_FROZEN 'Staging is frozen by an open staging-to-main PR. Complete or close that candidate before promoting more changes into staging.'
  fi
fi

workspace=$(mktemp -d)
trap 'rm -rf "$workspace"' EXIT
gh pr list --repo "$repo" --state open --base "$target" --head "$source" \
  --json number,title,body,headRefOid,url > "$workspace/existing.json" ||
  ci_fail CI_GITHUB_API 'Cannot list existing promotion PRs. Verify gh authentication and pull-request read access.' 3
if [[ "$(jq length "$workspace/existing.json")" != 0 ]]; then
  jq -r '.[0].body' "$workspace/existing.json" > "$workspace/body.md"
  bash "$script_directory/check-candidate.sh" "$workspace/body.md" "$expected_sha"
  jq -e --arg title "Promote $version: $source to $target" --arg sha "$expected_sha" \
    'length == 1 and .[0].title == $title and .[0].headRefOid == $sha' "$workspace/existing.json" >/dev/null ||
    ci_fail CI_PROMOTION_EXISTS 'An existing promotion PR does not match the requested version/source. Review it before preparing a replacement.'
  jq -r '.[0].url' "$workspace/existing.json"
  exit 0
fi

# Compare immutable commits so the generated summary describes the reviewed input.
gh api "repos/$repo/compare/$base_sha...$source_sha" > "$workspace/compare.json" ||
  ci_fail CI_GITHUB_API 'Cannot compare the reviewed promotion commits. Verify repository access.' 3
if [[ "$(jq '.files | length' "$workspace/compare.json")" == 0 ]]; then
  ci_fail CI_PROMOTION_EMPTY "There are no file changes to promote from $source to $target."
fi
{
  printf '<!-- promotion-source: %s -->\n\n' "$source_sha"
  printf 'Promote **%s** from `%s` to `%s`.\n\n' "$version" "$source" "$target"
  printf 'Reviewed source commit: `%s`\n\n' "$source_sha"
  printf '## Included changes\n\n'
  jq -r '.commits[] | select(.parents | length < 2) | "- " + (.commit.message | split("\n")[0]) + " (`" + .sha[0:7] + "`)"' "$workspace/compare.json"
  printf '\n[Compare source](https://github.com/%s/compare/%s...%s)\n\n' "$repo" "$base_sha" "$source_sha"
  printf '## Release validation\n\n'
  printf -- '- [ ] Full CI, including real Kafka/Connect integration, passes on this PR.\n'
  printf -- '- [ ] Configuration/API compatibility and known limitations have been reviewed.\n'
  printf -- '- [ ] Candidate validation and tested Kafka/Connect versions are recorded below.\n\n'
  printf 'Record release-specific validation here before merging.\n'
} > "$workspace/body.md"

# A branch can advance while the summary is generated. CI also checks this snapshot.
current_sha=$(gh api "repos/$repo/git/ref/heads/$source" --jq '.object.sha') ||
  ci_fail CI_GITHUB_API "Cannot recheck $source before creating the PR." 3
if [[ "$current_sha" != "$expected_sha" ]]; then
  ci_fail CI_SOURCE_CHANGED "$source advanced to $current_sha while preparing the promotion. Review it and rerun with the current source SHA."
fi
gh pr create --repo "$repo" --base "$target" --head "$source" \
  --title "Promote $version: $source to $target" --body-file "$workspace/body.md" ||
  ci_fail CI_PROMOTION_CREATE 'Could not create the promotion PR. Check the gh error above. If GitHub Actions is forbidden from creating PRs, run scripts/promote.sh locally using your maintainer gh login.' 3
