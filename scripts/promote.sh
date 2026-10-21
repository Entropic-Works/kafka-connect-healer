#!/usr/bin/env bash
set -euo pipefail

repo=${GH_REPO:?Set GH_REPO to owner/repository}
target=${TARGET_BRANCH:?Set TARGET_BRANCH to staging or main}
version=${RELEASE_VERSION:?Set RELEASE_VERSION to MAJOR.MINOR.PATCH}
expected_sha=${EXPECTED_SHA:?Set EXPECTED_SHA to the full reviewed source commit}
script_directory=$(cd "$(dirname "$0")" && pwd)

bash "$script_directory/check-release-tag.sh" "$version"
if [[ "$version" == *-* || ! "$expected_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo 'Use a stable release version and a full source SHA for promotion.' >&2
  exit 1
fi
case "$target" in
  staging) source=dev ;;
  main) source=staging ;;
  *) echo 'Promote only to staging or main.' >&2; exit 1 ;;
esac

source_sha=$(gh api "repos/$repo/git/ref/heads/$source" --jq '.object.sha')
base_sha=$(gh api "repos/$repo/git/ref/heads/$target" --jq '.object.sha')
if [[ "$source_sha" != "$expected_sha" ]]; then
  echo "$source moved. Review its current commit and rerun with that SHA." >&2
  exit 1
fi

if [[ "$target" == staging ]]; then
  candidates=$(gh pr list --repo "$repo" --state open --base main --head staging \
    --json number,isCrossRepository --jq '[.[] | select(.isCrossRepository == false)]')
  if [[ "$(jq length <<< "$candidates")" != 0 ]]; then
    echo 'Staging is frozen by an open candidate PR. Close it before preparing a replacement.' >&2
    exit 1
  fi
fi

workspace=$(mktemp -d)
trap 'rm -rf "$workspace"' EXIT
gh pr list --repo "$repo" --state open --base "$target" --head "$source" \
  --json number,title,body,headRefOid,url > "$workspace/existing.json"
if [[ "$(jq length "$workspace/existing.json")" != 0 ]]; then
  jq -r '.[0].body' "$workspace/existing.json" > "$workspace/body.md"
  bash "$script_directory/check-candidate.sh" "$workspace/body.md" "$expected_sha"
  jq -e --arg title "Promote $version: $source to $target" --arg sha "$expected_sha" \
    'length == 1 and .[0].title == $title and .[0].headRefOid == $sha' "$workspace/existing.json" >/dev/null
  jq -r '.[0].url' "$workspace/existing.json"
  exit 0
fi

# Compare immutable commits so the generated summary describes the reviewed input.
gh api "repos/$repo/compare/$base_sha...$source_sha" > "$workspace/compare.json"
if [[ "$(jq '.files | length' "$workspace/compare.json")" == 0 ]]; then
  echo 'There are no source changes to promote.' >&2
  exit 1
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
[[ "$(gh api "repos/$repo/git/ref/heads/$source" --jq '.object.sha')" == "$expected_sha" ]]
gh pr create --repo "$repo" --base "$target" --head "$source" \
  --title "Promote $version: $source to $target" --body-file "$workspace/body.md"
