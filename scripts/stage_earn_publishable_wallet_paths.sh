#!/usr/bin/env bash
set -euo pipefail

chain="${1:?chain is required}"
address_file="${2:?address file is required}"
scope="${3:---all}"
if [ "$#" -gt 3 ]; then
  echo "usage: $0 CHAIN ADDRESS_FILE [--all|--verified-only]" >&2
  exit 2
fi
case "$scope" in
  --all|--verified-only) ;;
  *)
    echo "invalid publication scope: $scope" >&2
    exit 2
    ;;
esac
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pathspec_file="$(mktemp "${TMPDIR:-/tmp}/earn-publishable-pathspec.XXXXXX")"
trap 'rm -f "$pathspec_file"' EXIT
start_seconds=$SECONDS

staged_count="$(
  python3 "$script_dir/build_earn_publishable_wallet_pathspec.py" \
    "$chain" \
    "$address_file" \
    "$pathspec_file" \
    "$scope"
)"

if [ "$staged_count" -gt 0 ]; then
  git add -A -f \
    --pathspec-from-file="$pathspec_file" \
    --pathspec-file-nul
fi

elapsed_seconds=$((SECONDS - start_seconds))
echo "Staged ${staged_count} publishable wallet paths for ${chain} in ${elapsed_seconds}s"
