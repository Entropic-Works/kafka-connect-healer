#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

# GitHub event values are passed through environment variables, never shell code.
case "${BASE_BRANCH:?}" in
  staging) expected=dev ;;
  main) expected=staging ;;
  dev) exit 0 ;;
  *) ci_fail CI_TARGET_UNSUPPORTED "Unsupported PR target $BASE_BRANCH. Contributions target dev; promotions target staging or main." 2 ;;
esac

if [[ "${HEAD_BRANCH:?}" != "$expected" || "${HEAD_REPOSITORY:?}" != "${BASE_REPOSITORY:?}" ]]; then
  ci_fail CI_PROMOTION_PATH "PR targets $BASE_BRANCH from $HEAD_BRANCH ($HEAD_REPOSITORY). Promote from $expected in $BASE_REPOSITORY instead."
fi
