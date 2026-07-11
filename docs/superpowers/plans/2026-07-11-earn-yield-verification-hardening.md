# Earn Yield Verification Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure only complete, unmodified replay may feed strict verified Earn yield totals.

**Architecture:** Carry explicit per-market provenance from replay construction through verification status. Preserve diagnostic/reconciled display values but exclude them from strict verification and the verified aggregate. Use a render-local cache only for identical default yield calculations.

**Tech Stack:** Static HTML/JavaScript, Python `unittest`, Node.js syntax checks.

## Global Constraints

- Preserve BigInt arithmetic; do not introduce floating-point wei calculations.
- Keep snapshot/netflow data as explicitly non-strict evidence.
- Touch only Earn yield code and its tests.
- Push the reviewed result to `master`, as expressly authorized by the user.

---

### Task 1: Establish semantic regression tests

**Files:**
- Modify: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: `earn_shouldTrustSnapshotSupplementedReplay(entry)` and `earn_getStrictVerificationStatus(entry)` from `dashboard-core.js`.
- Produces: regression coverage that executes the classifier, not merely searches source text.

- [ ] **Step 1: Write a failing test for truncated replay**

Add a Node-backed unit test that extracts the three classifier helpers, builds an otherwise exactly reconciled entry with `snapshotIncomplete: true` and `subgraphReplayTruncated: true`, and asserts `earn_getStrictVerificationStatus(entry) === 'coverage_incomplete'`.

- [ ] **Step 2: Run it to verify it fails**

Run: `python3 -m unittest tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_truncated_replay_cannot_be_strict_verified -v`

Expected: FAIL because the current classifier returns `verified`.

- [ ] **Step 3: Write a failing test for replay-state adjustment**

Add the same executable classifier test with `replayStateAdjusted: true` and otherwise exact balances. Assert `coverage_incomplete`.

- [ ] **Step 4: Run it to verify it fails**

Run: `python3 -m unittest tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_reconciled_replay_state_cannot_be_strict_verified -v`

Expected: FAIL because the current classifier does not receive or reject that provenance.

### Task 2: Propagate provenance and harden strict verification

**Files:**
- Modify: `dashboard-core.js`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: `earn_reconcileReplayToCurrentPositions`, `earn_buildReplayVerification`, and `earn_getStrictVerificationStatus`.
- Produces: `earn_replayReconciledMarkets: Set<string>` and verification entries with `replayStateAdjusted`.

- [ ] **Step 1: Add per-market provenance**

Declare and reset `earn_replayReconciledMarkets` with the other replay state. When `earn_reconcileReplayToCurrentPositions` adjusts a state, record its normalized market ID.

- [ ] **Step 2: Feed provenance into verification**

In `earn_buildReplayVerification`, set `replayStateAdjusted` from the set, include it in `snapshotIncomplete`, and retain `subgraphReplayTruncated` separately for presentation.

- [ ] **Step 3: Make strict eligibility defensive**

Make `earn_shouldTrustSnapshotSupplementedReplay` return false for truncated or replay-state-adjusted entries. Make `earn_getStrictVerificationStatus` return `coverage_incomplete` for either flag before any trusted-supplement path.

- [ ] **Step 4: Explain the actual gap**

Extend verification title/presentation copy so a replay-state correction explains that verification is withheld until complete ordered replay is available.

- [ ] **Step 5: Run the two regression tests**

Run: `python3 -m unittest tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_truncated_replay_cannot_be_strict_verified tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_reconciled_replay_state_cannot_be_strict_verified -v`

Expected: PASS.

### Task 3: Remove duplicate default calculations inside a render

**Files:**
- Modify: `dashboard-core.js`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: `earn_renderResults(assets, opts)` and `earn_calculateYield(position, opts)`.
- Produces: render-local lookup of equivalent default calculations.

- [ ] **Step 1: Add a render-local cache**

At the start of `earn_renderResults`, create a `Map` keyed by asset object and a `getDefaultYieldCalc(asset)` helper that calls `earn_calculateYield(asset)` once per asset for default options.

- [ ] **Step 2: Reuse it in table and summary paths**

Replace default-option calls within `earn_renderResults` with the helper. Do not cache calls that explicitly pass non-default options.

- [ ] **Step 3: Add a source contract**

Assert the render function contains `getDefaultYieldCalc` and uses it for both the supply table and verified summary, guarding against comparator-style repeated recomputation.

- [ ] **Step 4: Run targeted checks**

Run: `node --check dashboard-core.js && python3 -m unittest tests.test_earn_dashboard_contracts -v`

Expected: PASS.

### Task 4: Verify and publish

**Files:**
- Verify: `dashboard-core.js`, `tests/test_earn_dashboard_contracts.py`

- [ ] **Step 1: Run the full audit suite**

Run: `npm run check:earn-audit`

Expected: all Earn audit checks pass.

- [ ] **Step 2: Browser-test two real lookups**

Run a local server and check a standard Earn address plus a borrow-route address. Confirm the page renders, has no relevant console error, and that incomplete status is not displayed as Verified.

- [ ] **Step 3: Review the exact diff**

Run: `git diff --check && git diff -- dashboard-core.js tests/test_earn_dashboard_contracts.py docs/superpowers`

Expected: only scoped changes; no whitespace errors.

- [ ] **Step 4: Commit and push**

Run: `git add dashboard-core.js tests/test_earn_dashboard_contracts.py docs/superpowers && git commit -m "fix: harden earn yield verification" && git push dolomite-dashboard master`
