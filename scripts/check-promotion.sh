#!/usr/bin/env bash
set -euo pipefail

# GitHub event values are passed through environment variables, never shell code.
case "${BASE_BRANCH:?}" in
  staging) expected=dev ;;
  production) expected=staging ;;
  dev|main) exit 0 ;;
  *) echo "Unsupported pull request target: $BASE_BRANCH" >&2; exit 1 ;;
esac

if [[ "${HEAD_BRANCH:?}" != "$expected" || "${HEAD_REPOSITORY:?}" != "${BASE_REPOSITORY:?}" ]]; then
  echo "Promote to $BASE_BRANCH from $expected in the same repository." >&2
  exit 1
fi
