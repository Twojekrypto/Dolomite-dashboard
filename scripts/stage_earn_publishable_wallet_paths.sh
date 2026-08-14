#!/usr/bin/env bash
set -euo pipefail

chain="${1:?chain is required}"
address_file="${2:?address file is required}"

while IFS= read -r address; do
  [ -n "$address" ] || continue
  history_path="data/earn-subaccount-history/${chain}/${address}.json"
  ledger_path="data/earn-verified-ledger/${chain}/${address}.json"
  resolved_path="data/earn-resolved-interest-ledger/${chain}/${address}.json"
  for path in "$history_path" "$ledger_path" "$resolved_path"; do
    if [ -f "$path" ] || git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
      git add -A -f -- "$path"
    fi
  done
done < "$address_file"
