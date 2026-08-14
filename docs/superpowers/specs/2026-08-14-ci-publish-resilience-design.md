# CI Publish Resilience Design

## Objective

Resolve the actionable failures from GitHub Actions runs `31781356566`, `31784072282`, and `31721771546` without duplicating fixes that are already live.

## Diagnosed state

### EARN canonical coverage

The EARN backfill completes generation and all audit checks, then fails in `Commit publishable canonical coverage` because it unconditionally passes an absent per-wallet verified-ledger path to `git add`.

This remains reproducible in a newer run, so it requires a workflow fix.

### oDOLO flows

The referenced run failed because `claim_reconciliation["source_wallets"]` no longer matched the canonical reconciliation schema. Current `master` uses the canonical claim-wallet fields and a later workflow run is successful.

No additional production change is required.

### Assets icon synchronization

The referenced run received a malformed chunked HTTP response while reading Dolomite's official asset manifest. Later runs succeeded, confirming a transient upstream/network failure. The current fetch is single-attempt, so a bounded retry is justified to prevent the same transient response from failing the whole scheduled refresh.

## EARN workflow change

For each per-wallet history, verified-ledger, and resolved-ledger path:

- stage it when it exists in the working tree;
- also stage it when Git already tracks it, so legitimate deletions are preserved;
- skip an absent, untracked path instead of failing the job;
- use `git add -A -f -- "$path"` for explicit path handling.

Manifest and shard staging behavior remains unchanged.

## Icon fetch retry

- Keep the existing 25-second timeout per request.
- Make at most three attempts with short bounded backoff.
- Retry transport failures, aborted/terminated response bodies, invalid response bodies, HTTP 408/429, and HTTP 5xx responses.
- Do not retry permanent non-rate-limit HTTP 4xx responses.
- Validate the manifest with the existing minimum-icon guard before overwriting the generated registry.
- If all attempts fail, exit non-zero and preserve the previous checked-in generated file.

## Tests and verification

- Add a workflow contract test proving absent, untracked per-wallet files are guarded and tracked deletions can still be staged.
- Add Node tests for retry-after-transient-failure, bounded exhaustion, and permanent HTTP 4xx fail-fast behavior.
- Run the targeted EARN workflow contract test, token-icon tests, shell/YAML source checks already used by the repository, and `git diff --check`.
- Push one verified release commit to production `master`.
