#!/usr/bin/env bash
set -euo pipefail

commit_message="${1:?commit message is required}"
status_output="${EARN_FRESHNESS_STATUS_OUTPUT:-data/earn-freshness/status.json}"
quality_output="${EARN_QUALITY_STATUS_OUTPUT:-data/earn-quality/status.json}"
actions_output="${EARN_FRESHNESS_ACTIONS_OUTPUT:-}"
attempts="${EARN_PUSH_ATTEMPTS:-40}"
retry_sleep_seconds="${EARN_PUSH_RETRY_SLEEP_SECONDS:-5}"
max_retry_sleep_seconds="${EARN_PUSH_MAX_RETRY_SLEEP_SECONDS:-30}"
git_remote="${EARN_GIT_REMOTE:-origin}"
git_branch="${EARN_GIT_BRANCH:-master}"

resolve_rebase_modify_delete_conflicts() {
  local path
  local ours_exists
  local theirs_exists
  local resolved_count=0
  local delete_paths="$rebase_address_dir/rebase-delete-paths.nul"
  local keep_paths="$rebase_address_dir/rebase-keep-paths.nul"

  : > "$delete_paths"
  : > "$keep_paths"

  while IFS= read -r -d '' path; do
    [ -n "$path" ] || continue
    ours_exists=false
    theirs_exists=false
    if git cat-file -e ":2:$path" 2>/dev/null; then
      ours_exists=true
    fi
    if git cat-file -e ":3:$path" 2>/dev/null; then
      theirs_exists=true
    fi

    # During rebase, stage 3 is the fresh producer commit being replayed.
    if [ "$ours_exists" = "true" ] && [ "$theirs_exists" = "false" ]; then
      printf '%s\0' "$path" >> "$delete_paths"
      resolved_count=$((resolved_count + 1))
    elif [ "$ours_exists" = "false" ] && [ "$theirs_exists" = "true" ]; then
      printf '%s\0' "$path" >> "$keep_paths"
      resolved_count=$((resolved_count + 1))
    fi
  done < <(git diff --name-only --diff-filter=U -z)

  if [ -s "$delete_paths" ]; then
    git rm -f -q --pathspec-from-file="$delete_paths" --pathspec-file-nul
  fi
  if [ -s "$keep_paths" ]; then
    git checkout --theirs --pathspec-from-file="$keep_paths" --pathspec-file-nul
    git add --pathspec-from-file="$keep_paths" --pathspec-file-nul
  fi

  if ! git diff --quiet --diff-filter=U; then
    return 1
  fi
  if [ "$resolved_count" -eq 0 ]; then
    return 1
  fi

  echo "Resolved $resolved_count modify/delete conflicts using the fresh producer result."
  GIT_EDITOR=true git rebase --continue
}

if git diff --staged --quiet; then
  freshness_script="update_earn_freshness_status.py"
  refresh_args=(python3 "$freshness_script" --output "$status_output")
  if [ -n "$actions_output" ]; then
    refresh_args+=(--actions-output "$actions_output")
  fi
  "${refresh_args[@]}"
  git add "$status_output"
  if [ -f build_earn_quality_status.py ]; then
    python3 build_earn_quality_status.py --output "$quality_output"
    git add "$quality_output"
  fi
  if git diff --staged --quiet; then
    echo "No staged changes to commit"
    exit 0
  fi
fi

ledger_chains="$(
  git diff --cached --name-only |
    sed -n 's#^data/earn-verified-ledger/\([^/][^/]*\)/.*#\1#p' |
    tr '[:upper:]' '[:lower:]' |
    sort -u
)"
ledger_manifest_staged=false
if git diff --cached --name-only -- data/earn-verified-ledger/manifest.json | grep -q .; then
  ledger_manifest_staged=true
fi
if [ "$ledger_manifest_staged" = "true" ] && [ -n "${CHAIN:-}" ]; then
  ledger_chains="$(
    printf "%s\n%s\n" "$ledger_chains" "$CHAIN" |
      sed '/^$/d' |
      tr '[:upper:]' '[:lower:]' |
      sort -u
  )"
fi
ledger_sync_all=false
if [ "$ledger_manifest_staged" = "true" ] && [ -z "$ledger_chains" ]; then
  ledger_sync_all=true
fi

resolved_chains="$(
  git diff --cached --name-only |
    sed -n 's#^data/earn-resolved-interest-ledger/\([^/][^/]*\)/.*#\1#p' |
    tr '[:upper:]' '[:lower:]' |
    sort -u
)"
resolved_manifest_staged=false
if git diff --cached --name-only -- data/earn-resolved-interest-ledger/manifest.json | grep -q .; then
  resolved_manifest_staged=true
fi
if [ "$resolved_manifest_staged" = "true" ] && [ -n "${CHAIN:-}" ]; then
  resolved_chains="$(
    printf "%s\n%s\n" "$resolved_chains" "$CHAIN" |
      sed '/^$/d' |
      tr '[:upper:]' '[:lower:]' |
      sort -u
  )"
fi
resolved_sync_all=false
if [ "$resolved_manifest_staged" = "true" ] && [ -z "$resolved_chains" ]; then
  resolved_sync_all=true
fi

canonical_chains="$(
  git diff --cached --name-only |
    sed -n 's#^data/earn-subaccount-history/\([^/][^/]*\)/0x[0-9a-fA-F]*\.json$#\1#p' |
    tr '[:upper:]' '[:lower:]' |
    sort -u
)"
canonical_manifest_staged=false
if git diff --cached --name-only -- data/earn-subaccount-history/manifest.json | grep -q .; then
  canonical_manifest_staged=true
fi
if [ "$canonical_manifest_staged" = "true" ] && [ -n "${CHAIN:-}" ]; then
  canonical_chains="$(
    printf "%s\n%s\n" "$canonical_chains" "$CHAIN" |
      sed '/^$/d' |
      tr '[:upper:]' '[:lower:]' |
      sort -u
  )"
fi

rebase_address_dir="$(mktemp -d "${TMPDIR:-/tmp}/earn-rebase-addresses.XXXXXX")"
trap 'rm -rf "$rebase_address_dir"' EXIT
while IFS= read -r ledger_path; do
  case "$ledger_path" in
    data/earn-verified-ledger/*/0x*.json)
      chain="$(printf '%s' "$ledger_path" | cut -d/ -f3 | tr '[:upper:]' '[:lower:]')"
      address="$(basename "$ledger_path" .json | tr '[:upper:]' '[:lower:]')"
      printf '%s\n' "$address" >> "$rebase_address_dir/${chain}.txt"
      ;;
  esac
done < <(git diff --cached --name-only)
for address_file in "$rebase_address_dir"/*.txt; do
  [ -f "$address_file" ] || continue
  sort -u -o "$address_file" "$address_file"
done

git commit -m "$commit_message"

# Any tracked files left modified-but-unstaged by generation steps would make
# every `git pull --rebase` attempt below fail with "You have unstaged changes".
# Surface them loudly, then drop them so the push loop can proceed.
if ! git diff --quiet; then
  echo "::warning::Discarding unstaged tracked changes left in the working tree (stage them in the workflow if they should be committed):"
  git diff --name-only | sed 's/^/::warning::  /'
  git checkout -- .
fi

pushed=false
for i in $(seq 1 "$attempts"); do
  snapshot_manifest_before_pull="$(git rev-parse HEAD:data/earn-snapshots/manifest.json 2>/dev/null || true)"
  pull_succeeded=false
  if git pull --rebase -X theirs "$git_remote" "$git_branch"; then
    pull_succeeded=true
  elif { [ -d .git/rebase-merge ] || [ -d .git/rebase-apply ]; } && resolve_rebase_modify_delete_conflicts; then
    pull_succeeded=true
  fi
  if [ "$pull_succeeded" = "true" ]; then
    snapshot_manifest_after_pull="$(git rev-parse HEAD:data/earn-snapshots/manifest.json 2>/dev/null || true)"
    if [ -n "$snapshot_manifest_before_pull" ] && [ "$snapshot_manifest_before_pull" != "$snapshot_manifest_after_pull" ]; then
      echo "Snapshot manifest changed during rebase; rebuilding affected EARN ledgers."
      for address_file in "$rebase_address_dir"/*.txt; do
        [ -s "$address_file" ] || continue
        chain="$(basename "$address_file" .txt)"
        python3 build_earn_resolved_interest_ledger.py --chain "$chain" --address-file "$address_file"
        python3 build_earn_verified_ledger.py --chain "$chain" --address-file "$address_file"
        python3 build_earn_verified_ledger_shards.py --chain "$chain" --address-file "$address_file"
        while IFS= read -r address; do
          [ -n "$address" ] || continue
          ledger_path="data/earn-verified-ledger/${chain}/${address}.json"
          resolved_path="data/earn-resolved-interest-ledger/${chain}/${address}.json"
          if [ -f "$ledger_path" ] || git ls-files --error-unmatch "$ledger_path" >/dev/null 2>&1; then
            git add -f -- "$ledger_path"
          fi
          if [ -f "$resolved_path" ] || git ls-files --error-unmatch "$resolved_path" >/dev/null 2>&1; then
            git add -f -- "$resolved_path"
          fi
        done < "$address_file"
      done
      [ -f data/earn-verified-ledger/manifest.json ] && git add -f data/earn-verified-ledger/manifest.json
      [ -d data/earn-verified-ledger-shards ] && git add -f data/earn-verified-ledger-shards
      [ -f data/earn-resolved-interest-ledger/manifest.json ] && git add -f data/earn-resolved-interest-ledger/manifest.json
      if [ -f build_earn_representative_audit.py ]; then
        python3 build_earn_representative_audit.py --check
        [ -f data/earn-quality/representative-audit.json ] && git add data/earn-quality/representative-audit.json
      fi
    fi
    if [ -n "$ledger_chains" ] || [ "$ledger_sync_all" = "true" ]; then
      sync_args=(python3 scripts/sync_earn_verified_manifest.py --base-ref "$git_remote/$git_branch")
      if [ "$ledger_sync_all" = "true" ]; then
        sync_args+=(--all-chains)
      else
        for chain in $ledger_chains; do
          sync_args+=(--chain "$chain")
        done
      fi
      "${sync_args[@]}"
      git add -f data/earn-verified-ledger/manifest.json
    fi
    if [ -n "$resolved_chains" ] || [ "$resolved_sync_all" = "true" ]; then
      resolved_sync_args=(python3 scripts/sync_earn_resolved_manifest.py)
      if [ "$resolved_sync_all" = "true" ]; then
        resolved_sync_args+=(--all-chains)
      else
        for chain in $resolved_chains; do
          resolved_sync_args+=(--chain "$chain")
        done
      fi
      "${resolved_sync_args[@]}"
      git add -f data/earn-resolved-interest-ledger/manifest.json
    fi
    if [ -n "$canonical_chains" ]; then
      canonical_sync_args=(python3 scripts/sync_earn_subaccount_manifest.py --base-ref "$git_remote/$git_branch")
      for chain in $canonical_chains; do
        canonical_sync_args+=(--chain "$chain")
      done
      "${canonical_sync_args[@]}"
      git add -f data/earn-subaccount-history/manifest.json
    fi
    refresh_args=(python3 update_earn_freshness_status.py --output "$status_output")
    if [ -n "$actions_output" ]; then
      refresh_args+=(--actions-output "$actions_output")
    fi
    "${refresh_args[@]}"
    git add "$status_output"
    if [ -f build_earn_quality_status.py ]; then
      python3 build_earn_quality_status.py --output "$quality_output"
      git add "$quality_output"
    fi
    if ! git diff --staged --quiet; then
      git commit --amend --no-edit
    fi
    if git push "$git_remote" "HEAD:$git_branch"; then
      pushed=true
      break
    fi
  elif [ -d .git/rebase-merge ] || [ -d .git/rebase-apply ]; then
    git rebase --abort || true
  fi
  if [ "$i" -lt "$attempts" ]; then
    sleep_for=$((retry_sleep_seconds + i))
    if [ "$sleep_for" -gt "$max_retry_sleep_seconds" ]; then
      sleep_for="$max_retry_sleep_seconds"
    fi
    sleep_for=$((sleep_for + (RANDOM % 4)))
    echo "Push attempt $i failed, retrying after remote moved in ${sleep_for}s..."
    sleep "$sleep_for"
  fi
done

if [ "$pushed" != "true" ]; then
  echo "Failed to push after $attempts attempts."
  exit 1
fi

if [ "${EARN_DISPATCH_FRESHNESS_AFTER_PUSH:-false}" = "true" ]; then
  dispatch_token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
  allow_remediation="${EARN_FRESHNESS_ALLOW_REMEDIATION_AFTER_PUSH:-true}"
  if [ "$allow_remediation" != "true" ] && [ "$allow_remediation" != "false" ]; then
    echo "Invalid EARN_FRESHNESS_ALLOW_REMEDIATION_AFTER_PUSH=$allow_remediation; expected true or false."
    exit 1
  fi
  if [ -z "$dispatch_token" ] || ! command -v gh >/dev/null 2>&1; then
    echo "Cannot dispatch EARN freshness monitor; gh CLI or GitHub token is unavailable."
    exit 1
  fi
  export GH_TOKEN="$dispatch_token"
  echo "Dispatching EARN freshness monitor for $git_branch after producer push."
  gh workflow run monitor-earn-freshness.yml \
    --ref "$git_branch" \
    -f "allow_remediation=$allow_remediation"
fi

if [ "${EARN_DISPATCH_PAGES_AFTER_PUSH:-true}" = "true" ]; then
  deploy_token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
  if [ -n "$deploy_token" ] && command -v gh >/dev/null 2>&1; then
    export GH_TOKEN="$deploy_token"
    echo "Dispatching GitHub Pages deploy for $git_branch after EARN data push."
    if ! gh workflow run pages.yml --ref "$git_branch"; then
      echo "::warning::Failed to dispatch GitHub Pages deploy after EARN data push."
    fi
  else
    echo "Skipping GitHub Pages deploy dispatch; gh CLI or GitHub token is unavailable."
  fi
fi
