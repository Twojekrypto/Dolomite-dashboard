# CI Publish Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make EARN publishing tolerate optional per-wallet artifacts and make official icon synchronization recover from transient HTTP failures.

**Architecture:** Move per-wallet Git staging into a small shell boundary that can be exercised in a temporary repository, then call it from the workflow. Keep icon registry generation unchanged while wrapping only the external manifest read in a three-attempt retry with dependency injection for deterministic Node tests.

**Tech Stack:** GitHub Actions YAML, Bash, Python `unittest`, Node.js ESM and built-in test runner.

## Global Constraints

- Do not change EARN classification or generated financial data.
- Existing, tracked, and deleted per-wallet artifacts must be stageable; absent untracked artifacts must be skipped.
- Icon synchronization must keep a 25-second timeout per request and at most three attempts.
- Permanent non-rate-limit HTTP 4xx responses must fail immediately.
- Failed synchronization must not overwrite the checked-in icon registry.
- Do not add dependencies or secrets.

---

### Task 1: Safely stage optional EARN artifacts

**Files:**
- Create: `scripts/stage_earn_publishable_wallet_paths.sh`
- Modify: `.github/workflows/backfill-earn-canonical-coverage.yml`
- Modify: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: positional `CHAIN` and address-file arguments plus the existing three `data/earn-*` directory trees.
- Produces: a Git index containing existing/new files and tracked deletions, without failing for an absent untracked path.

- [ ] **Step 1: Write the failing behavioral test**

Create a temporary Git repository, commit one verified-ledger file, delete it, create only the corresponding history file, leave the resolved ledger absent, invoke the repository helper, and assert the staged status is exactly an added history plus deleted ledger:

```python
def test_publishable_wallet_staging_skips_absent_untracked_artifacts(self):
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)
        address = "0x1111111111111111111111111111111111111111"
        # Initialize Git, commit the ledger baseline, then delete it.
        # Create history only and write address to publishable.txt.
        subprocess.run(
            ["bash", str(EARN_STAGE_HELPER), "ethereum", "publishable.txt"],
            cwd=repo,
            check=True,
        )
        staged = subprocess.check_output(
            ["git", "diff", "--cached", "--name-status"], cwd=repo, text=True
        ).splitlines()
        self.assertEqual(
            [
                f"A\tdata/earn-subaccount-history/ethereum/{address}.json",
                f"D\tdata/earn-verified-ledger/ethereum/{address}.json",
            ],
            sorted(staged),
        )
```

Also assert the workflow invokes `scripts/stage_earn_publishable_wallet_paths.sh "$CHAIN" "${{ steps.publishable.outputs.path }}"`.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
python3 -m unittest tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_publishable_wallet_staging_skips_absent_untracked_artifacts
```

Expected: FAIL because the helper does not exist.

- [ ] **Step 3: Add the minimal staging helper and workflow call**

Implement:

```bash
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
```

Replace the workflow's three per-address `git add` branches with the helper invocation. Keep manifest and shard staging unchanged.

- [ ] **Step 4: Run focused and surrounding EARN contract tests**

Run:

```bash
python3 -m unittest tests.test_earn_dashboard_contracts
bash -n scripts/stage_earn_publishable_wallet_paths.sh
```

Expected: PASS.

- [ ] **Step 5: Commit the EARN fix**

```bash
git add scripts/stage_earn_publishable_wallet_paths.sh .github/workflows/backfill-earn-canonical-coverage.yml tests/test_earn_dashboard_contracts.py
git commit -m "fix: stage optional EARN coverage artifacts safely"
```

---

### Task 2: Retry transient official-icon manifest failures

**Files:**
- Modify: `scripts/sync_dolomite_token_icons.mjs`
- Modify: `tests/dolomite-token-icons.test.mjs`

**Interfaces:**
- Consumes: `fetchOfficialManifest({ fetchImpl, sleepImpl, maxAttempts })` with production defaults.
- Produces: parsed manifest JSON or the final error after bounded retry.

- [ ] **Step 1: Write failing retry behavior tests**

Add tests whose in-memory fetch boundary returns complete response-shaped objects:

```js
test('official manifest fetch retries a terminated response body', async () => {
  let attempts = 0;
  const manifest = { files: { 'static/media/DOLO.svg': './static/media/DOLO.hash.svg' } };
  const actual = await fetchOfficialManifest({
    fetchImpl: async () => {
      attempts += 1;
      return attempts === 1
        ? { ok: true, status: 200, json: async () => { throw new TypeError('terminated'); } }
        : { ok: true, status: 200, json: async () => manifest };
    },
    sleepImpl: async () => {},
  });
  assert.deepEqual(actual, manifest);
  assert.equal(attempts, 2);
});
```

Add separate tests proving three transport failures stop after attempt three and HTTP 404 stops after attempt one.

- [ ] **Step 2: Run the Node test and verify RED**

Run:

```bash
node --test tests/dolomite-token-icons.test.mjs
```

Expected: FAIL because `fetchOfficialManifest` is not exported and has no retry contract.

- [ ] **Step 3: Implement bounded retry**

Export `fetchOfficialManifest(options = {})`, defaulting to global `fetch`, a Promise-based delay, and three attempts. Create a fresh `AbortController` per attempt. Retry thrown transport/body errors plus HTTP 408, 429, and 5xx; throw permanent 4xx immediately. Delay only between attempts using a short linear backoff.

```js
const MAX_MANIFEST_ATTEMPTS = 3;
const RETRYABLE_HTTP_STATUS = (status) => status === 408 || status === 429 || status >= 500;

export async function fetchOfficialManifest(options = {}) {
  const fetchImpl = options.fetchImpl || globalThis.fetch;
  const sleepImpl = options.sleepImpl || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  const maxAttempts = options.maxAttempts || MAX_MANIFEST_ATTEMPTS;
  // Attempt loop; create/clear one timeout per request and preserve the final error.
}
```

- [ ] **Step 4: Run targeted checks**

Run:

```bash
node --test tests/dolomite-token-icons.test.mjs
node --check scripts/sync_dolomite_token_icons.mjs
```

Expected: PASS.

- [ ] **Step 5: Commit the icon retry**

```bash
git add scripts/sync_dolomite_token_icons.mjs tests/dolomite-token-icons.test.mjs
git commit -m "fix: retry transient Dolomite icon manifest failures"
```
