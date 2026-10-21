#!/bin/sh
set -eu

# Run from the root module after downloading dependencies. Match the binary's
# GOOS/GOARCH when cross-compiling. Keep all notices from each imported module.
if [ "$#" -ne 1 ]; then
  echo "usage: sh scripts/collect-licenses.sh NEW_OUTPUT_DIRECTORY" >&2
  exit 1
fi
output=$1
packages=$(go list -mod=readonly -deps -f '{{with .Module}}{{if not .Main}}{{.Path}}{{"\t"}}{{if .Version}}{{.Version}}{{else}}(local){{end}}{{"\t"}}{{.Dir}}{{end}}{{end}}' ./cmd/kafka-connect-healer)
mkdir -p "$(dirname "$output")"
# Require a new directory so removed dependencies cannot leave stale notices.
mkdir "$output"
touch "$output/modules.txt"
tab=$(printf '\t')
printf '%s\n' "$packages" | LC_ALL=C sort -u | while IFS="$tab" read -r module version directory; do
  [ -n "$module" ] || continue
  notice=$(find "$directory" -type f \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' -o -iname 'PATENTS*' \) -print -quit)
  if [ -z "$notice" ]; then
    echo "no license or notice found for $module $version" >&2
    exit 1
  fi
  printf '%s %s\n' "$module" "$version" >> "$output/modules.txt"
  find "$directory" -type f \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' -o -iname 'PATENTS*' \) \
    -exec sh -eu -c '
      root=$1
      destination=$2
      shift 2
      for source do
        relative=${source#"$root"/}
        target="$destination/$relative"
        mkdir -p "${target%/*}"
        cp "$source" "$target"
      done
    ' sh "$directory" "$output/$module" {} +
done
