#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

release_tag=${1:?Usage: check-release-tag.sh MAJOR.MINOR.PATCH[-prerelease] [--check-branch]}
version_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
if [[ ! "$release_tag" =~ $version_pattern ]]; then
  ci_fail CI_RELEASE_VERSION 'Expected MAJOR.MINOR.PATCH or MAJOR.MINOR.PATCH-prerelease, without leading zeros or a v prefix.' 2
fi

release_branch=main
if [[ "$release_tag" == *-* ]]; then
  release_branch=staging
  IFS=. read -r -a identifiers <<< "${release_tag#*-}"
  for identifier in "${identifiers[@]}"; do
    if [[ "$identifier" =~ ^[0-9]+$ && "$identifier" == 0?* ]]; then
      ci_fail CI_RELEASE_VERSION 'Numeric prerelease identifiers must not have leading zeros.' 2
    fi
  done
fi

if [[ "${2:-}" == --check-branch ]]; then
  git show-ref --verify --quiet "refs/remotes/origin/$release_branch" ||
    ci_fail CI_RELEASE_BRANCH_MISSING "origin/$release_branch is missing. Fetch protected branches before validating the release tag." 2
  if ! git merge-base --is-ancestor HEAD "origin/$release_branch"; then
    ci_fail CI_RELEASE_UNPROMOTED "Release commit must have been promoted to $release_branch before tagging. Follow dev -> staging -> main for stable releases."
  fi
elif [[ -n "${2:-}" ]]; then
  ci_fail CI_RELEASE_ARGUMENT "Unknown release-check argument: $2" 2
fi

echo "Validated $release_tag ($release_branch)."
