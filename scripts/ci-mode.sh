#!/usr/bin/env bash
set -euo pipefail

# Metadata-only edits reuse verified full CI without canceling or rerunning tests.
case "${GITHUB_EVENT_NAME:?}" in
  pull_request)
    if jq -e '
      .action == "edited" and (.changes | has("base") | not) and
      (([ (if (.changes | has("body")) then (.changes.body.from // "") else (.pull_request.body // "") end)
           | scan("(?m)^<!-- promotion-source: [0-9a-f]{40} -->$") ]) ==
       ([ (.pull_request.body // "") | scan("(?m)^<!-- promotion-source: [0-9a-f]{40} -->$") ]))
    ' "${GITHUB_EVENT_PATH:?}" >/dev/null; then
      echo ignore
    else
      echo full
    fi
    ;;
  push)
    case "${GITHUB_REF:?}" in
      refs/heads/dev|refs/heads/staging|refs/heads/main|refs/tags/*) echo full ;;
      *) echo feature ;;
    esac
    ;;
  *) echo full ;;
esac
