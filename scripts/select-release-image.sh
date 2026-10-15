#!/usr/bin/env bash
set -euo pipefail

manifest=${1:?Usage: select-release-image.sh MANIFEST_JSON IMAGE@INDEX_DIGEST ARCH}
image=${2:?}
architecture=${3:?}
if [[ ! "$image" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo 'Release verification requires an immutable image index digest.' >&2
  exit 1
fi
case "$architecture" in
  amd64|arm64) ;;
  *) echo 'Unsupported release architecture.' >&2; exit 1 ;;
esac

# Attestation manifests have unknown/unknown platforms and must not be selected.
digest=$(jq -er --arg architecture "$architecture" '
  [.manifests[] | select(.platform.os == "linux" and .platform.architecture == $architecture)]
  | if length == 1 then .[0].digest else error("Expected exactly one platform manifest") end
  | select(test("^sha256:[0-9a-f]{64}$"))
' "$manifest")
printf '%s@%s\n' "${image%@*}" "$digest"
