#!/usr/bin/env bash
set -euo pipefail

commit_message="${1:?commit message is required}"
status_output="${EARN_FRESHNESS_STATUS_OUTPUT:-data/earn-freshness/status.json}"
actions_output="${EARN_FRESHNESS_ACTIONS_OUTPUT:-}"
attempts="${EARN_PUSH_ATTEMPTS:-12}"
retry_sleep_seconds="${EARN_PUSH_RETRY_SLEEP_SECONDS:-5}"
git_remote="${EARN_GIT_REMOTE:-origin}"
git_branch="${EARN_GIT_BRANCH:-master}"

if git diff --staged --quiet; then
  echo "No staged changes to commit"
  exit 0
fi

ledger_chains="$(
  git diff --cached --name-only |
    sed -n 's#^data/earn-verified-ledger/\([^/][^/]*\)/.*#\1#p' |
    sort -u
)"

git commit -m "$commit_message"

pushed=false
for i in $(seq 1 "$attempts"); do
  if git pull --rebase -X theirs "$git_remote" "$git_branch"; then
    if [ -n "$ledger_chains" ]; then
      sync_args=(python3 scripts/sync_earn_verified_manifest.py --base-ref "$git_remote/$git_branch")
      for chain in $ledger_chains; do
        sync_args+=(--chain "$chain")
      done
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
  fi
  echo "Push attempt $i failed, retrying after remote moved..."
  sleep "$retry_sleep_seconds"
done

if [ "$pushed" != "true" ]; then
  echo "Failed to push after $attempts attempts."
  exit 1
fi
