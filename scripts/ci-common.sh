#!/usr/bin/env bash

# Shared diagnostics for local helpers and GitHub Actions. Codes remain stable
# when wording changes; numeric exits distinguish policy, input, API, and timeout.
ci_error() {
  local code=$1 message=$2 annotation
  printf 'ERROR [%s] %s\n' "$code" "$message" >&2
  if [[ "${GITHUB_ACTIONS:-false}" == true ]]; then
    annotation=${message//%/%25}
    annotation=${annotation//$'\r'/%0D}
    annotation=${annotation//$'\n'/%0A}
    printf '::error title=%s::%s\n' "$code" "$annotation" >&2
  fi
}

ci_fail() {
  ci_error "$1" "$2"
  exit "${3:-1}"
}

ci_summary() {
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '%s\n' "$@" >> "$GITHUB_STEP_SUMMARY"
  fi
}
