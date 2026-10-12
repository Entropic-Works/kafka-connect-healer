#!/usr/bin/env bash
set -euo pipefail

release_tag=${1:?Usage: check-release-tag.sh vMAJOR.MINOR.PATCH[-prerelease] [--check-branch]}
version_pattern='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
if [[ ! "$release_tag" =~ $version_pattern ]]; then
  echo 'Expected vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-prerelease, without leading zeros.' >&2
  exit 1
fi

release_branch=production
if [[ "$release_tag" == *-* ]]; then
  release_branch=staging
  IFS=. read -r -a identifiers <<< "${release_tag#*-}"
  for identifier in "${identifiers[@]}"; do
    if [[ "$identifier" =~ ^[0-9]+$ && "$identifier" == 0?* ]]; then
      echo 'Numeric prerelease identifiers must not have leading zeros.' >&2
      exit 1
    fi
  done
fi

if [[ "${2:-}" == --check-branch ]]; then
  git show-ref --verify --quiet "refs/remotes/origin/$release_branch"
  if ! git merge-base --is-ancestor HEAD "origin/$release_branch"; then
    echo "Release commit must have been promoted to $release_branch." >&2
    exit 1
  fi
elif [[ -n "${2:-}" ]]; then
  echo "Unknown argument: $2" >&2
  exit 1
fi

echo "Validated $release_tag ($release_branch)."
