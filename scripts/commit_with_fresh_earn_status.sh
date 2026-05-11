#!/usr/bin/env bash
set -euo pipefail

commit_message="${1:?commit message is required}"
status_output="${EARN_FRESHNESS_STATUS_OUTPUT:-data/earn-freshness/status.json}"
actions_output="${EARN_FRESHNESS_ACTIONS_OUTPUT:-}"
attempts="${EARN_PUSH_ATTEMPTS:-40}"
retry_sleep_seconds="${EARN_PUSH_RETRY_SLEEP_SECONDS:-5}"
max_retry_sleep_seconds="${EARN_PUSH_MAX_RETRY_SLEEP_SECONDS:-30}"
git_remote="${EARN_GIT_REMOTE:-origin}"
git_branch="${EARN_GIT_BRANCH:-master}"

if git diff --staged --quiet; then
  echo "No staged changes to commit"
  exit 0
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

git commit -m "$commit_message"

pushed=false
for i in $(seq 1 "$attempts"); do
  if git pull --rebase -X theirs "$git_remote" "$git_branch"; then
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
    refresh_args=(python3 update_earn_freshness_status.py --output "$status_output")
    if [ -n "$actions_output" ]; then
      refresh_args+=(--actions-output "$actions_output")
    fi
    "${refresh_args[@]}"
    git add "$status_output"
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
