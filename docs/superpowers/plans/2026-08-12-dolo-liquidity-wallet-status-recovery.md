# DOLO Liquidity Wallet Status Recovery Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restore truthful `Verified` LP positions by recognizing Safe and EIP-7702 wallets, and prevent transient Routescan rate limits from making otherwise refreshable Ethereum sources `Stale`.

**Architecture:** Keep the existing fail-closed attribution path: only exact EIP-7702 designators and Safe proxies whose slot-zero singleton matches the shared official allowlist are treated as wallets; every unknown contract or failed classification remains unavailable. Keep Routescan as the indexed-log source, but retry transient responses and retry missing receipts through canonical RPC before using the explorer fallback.

**Tech Stack:** Python 3, unittest, requests, EVM JSON-RPC, static JSON artifact, GitHub Pages.

---

### Task 1: Add fail-closed wallet classification regressions

**Files:**
- Modify: `tests/test_generate_dolo_liquidity.py`
- Modify: `generate_dolo_liquidity.py`

1. Add failing tests for an exact EIP-7702 designator, an official Safe singleton in slot zero, an ordinary contract, and an RPC classification failure.
2. Run the focused tests and confirm they fail for the current all-bytecode-is-contract behavior.
3. Import the shared Safe singleton allowlist and implement the smallest exact classifier.
4. Run the focused tests and the full liquidity Python suite.

### Task 2: Make transient log and receipt rate limits recoverable

**Files:**
- Modify: `tests/test_generate_dolo_liquidity.py`
- Modify: `generate_dolo_liquidity.py`

1. Add failing tests showing transient HTTP 429 log requests are retried and missing batched receipts are retried through canonical RPC before Routescan.
2. Confirm the focused tests fail against the current implementation.
3. Add bounded retry handling for transient Routescan responses and canonical-RPC receipt recovery without weakening malformed-data checks.
4. Run focused and full Python tests plus `py_compile`.

### Task 3: Regenerate and validate the production artifact

**Files:**
- Modify: `data/dolo-liquidity.json`

1. Run the official generator against live sources.
2. Validate the generated artifact and assert that visible Safe/EIP-7702 positions are no longer `Unavailable`.
3. Confirm the Ethereum adapter sources are fresh; if a source remains unavailable after bounded retries, preserve fail-closed status and report the exact external blocker rather than promoting data.
4. Run artifact validator, UI contract tests, syntax checks, and `git diff --check`.

### Task 4: Verify, publish, and observe production

**Files:**
- Verify: `dolo-preview.html`

1. Serve the dashboard locally and inspect the LP table at desktop and mobile sizes, including `Verified` visibility and status tooltips.
2. Review the final diff for scope and secrets.
3. Commit, fetch and rebase onto the latest production `master`, rerun the relevant checks, then push `HEAD:master`.
4. Watch the triggered GitHub Actions runs and confirm the live GitHub Pages artifact serves the new generated timestamp and status counts.
