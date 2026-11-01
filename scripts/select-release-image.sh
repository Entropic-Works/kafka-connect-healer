#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ci-common.sh"

manifest=${1:?Usage: select-release-image.sh MANIFEST_JSON IMAGE@INDEX_DIGEST ARCH}
image=${2:?}
architecture=${3:?}
if [[ ! "$image" =~ @sha256:[0-9a-f]{64}$ ]]; then
  ci_fail CI_IMAGE_DIGEST 'Release verification requires an immutable IMAGE@sha256:INDEX_DIGEST reference.' 2
fi
case "$architecture" in
  amd64|arm64) ;;
  *) ci_fail CI_IMAGE_ARCHITECTURE 'Release verification supports amd64 and arm64.' 2 ;;
esac

# Attestation manifests have unknown/unknown platforms and must not be selected.
digest=$(jq -er --arg architecture "$architecture" '
  [.manifests[] | select(.platform.os == "linux" and .platform.architecture == $architecture)]
  | if length == 1 then .[0].digest else error("Expected exactly one platform manifest") end
  | select(test("^sha256:[0-9a-f]{64}$"))
' "$manifest") || ci_fail CI_IMAGE_MANIFEST "Expected exactly one Linux $architecture image with a valid digest in the release index. Inspect the published manifest."
printf '%s@%s\n' "${image%@*}" "$digest"
