#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
promotion="$repo_root/scripts/check-promotion.sh"
release="$repo_root/scripts/check-release-tag.sh"

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

# Exercise real ancestry: staging has a candidate that main has not received.
# This fixture is isolated and never changes the project's Git refs or config.
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
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
