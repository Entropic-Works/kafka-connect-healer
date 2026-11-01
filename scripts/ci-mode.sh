#!/usr/bin/env bash
set -euo pipefail

# Compare canonical marker lines, so a browser changing LF/CRLF is metadata only.
case "${GITHUB_EVENT_NAME:?}" in
  pull_request)
    if jq -e '
      def markers: gsub("\r\n"; "\n") | [scan("(?m)^<!-- promotion-source: [0-9a-f]{40} -->$")];
      .action == "edited" and (.changes | has("base") | not) and
      (((if (.changes | has("body")) then (.changes.body.from // "") else (.pull_request.body // "") end) | markers) ==
       ((.pull_request.body // "") | markers))
    ' "${GITHUB_EVENT_PATH:?}" >/dev/null; then
      echo metadata
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
