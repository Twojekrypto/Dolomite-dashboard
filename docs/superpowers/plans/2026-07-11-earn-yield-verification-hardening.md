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

- [x] **Step 1: Write a failing test for truncated replay**

Add a Node-backed unit test that extracts the three classifier helpers, builds an otherwise exactly reconciled entry with `snapshotIncomplete: true` and `subgraphReplayTruncated: true`, and asserts `earn_getStrictVerificationStatus(entry) === 'coverage_incomplete'`.

- [x] **Step 2: Run it to verify it fails**

Run: `python3 -m unittest tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_truncated_replay_cannot_be_strict_verified -v`

Expected: FAIL because the current classifier returns `verified`.

- [x] **Step 3: Write a failing test for replay-state adjustment**

Add the same executable classifier test with `replayStateAdjusted: true` and otherwise exact balances. Assert `coverage_incomplete`.

- [x] **Step 4: Run it to verify it fails**

Run: `python3 -m unittest tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_reconciled_replay_state_cannot_be_strict_verified -v`

Expected: FAIL because the current classifier does not receive or reject that provenance.

- [x] **Step 5: Write and fail a snapshot-supplement regression**

Build an otherwise exactly reconciled entry with `snapshotIncomplete: true`, assert `coverage_incomplete`, and confirm it fails while the former trust path is still active.

### Task 2: Propagate provenance and harden strict verification

**Files:**
- Modify: `dashboard-core.js`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: `earn_reconcileReplayToCurrentPositions`, `earn_buildReplayVerification`, and `earn_getStrictVerificationStatus`.
- Produces: `earn_replayReconciledMarkets: Set<string>` and verification entries with `replayStateAdjusted`.

- [x] **Step 1: Add per-market provenance**

Declare and reset `earn_replayReconciledMarkets` with the other replay state. When `earn_reconcileReplayToCurrentPositions` adjusts a state, record its normalized market ID.

- [x] **Step 2: Feed provenance into verification**

In `earn_buildReplayVerification`, set `replayStateAdjusted` from the set, include it in `snapshotIncomplete`, and retain `subgraphReplayTruncated` separately for presentation. If a fallback query was globally truncated, carry a global incomplete flag into every market because the omitted events cannot be attributed safely.

- [x] **Step 3: Make strict eligibility defensive**

Remove the trusted snapshot-supplement path. Make `earn_getStrictVerificationStatus` return `coverage_incomplete` for any incomplete snapshot, truncated replay, or adjusted replay state before any verified path.

- [x] **Step 4: Explain the actual gap**

Extend verification title/presentation copy so a replay-state correction explains that verification is withheld until complete ordered replay is available.

- [x] **Step 5: Run the two regression tests**

Run: `python3 -m unittest tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_truncated_replay_cannot_be_strict_verified tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_reconciled_replay_state_cannot_be_strict_verified -v`

Expected: PASS.

### Task 3: Remove duplicate default calculations inside a render

**Files:**
- Modify: `dashboard-core.js`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: `earn_renderResults(assets, opts)` and `earn_calculateYield(position, opts)`.
- Produces: render-local lookup of equivalent default calculations.

- [x] **Step 1: Add a render-local cache**

At the start of `earn_renderResults`, create a `WeakMap` keyed by asset object and a `getVerifiedYieldCalc(asset)` helper that calls `earn_calculateYield(asset, { requireVerifiedInterest: true })` once per asset per render.

- [x] **Step 2: Reuse it in table and summary paths**

Replace equivalent verified-interest calls within `earn_renderResults` with the helper. Do not cache calls that use different options.

- [x] **Step 3: Add a source contract**

Execute the helper in a Node-backed test and assert it computes once per asset, then assert `earn_renderResults` uses it in both the supply table and verified summary.

- [x] **Step 4: Run targeted checks**

Run: `node --check dashboard-core.js && python3 -m unittest tests.test_earn_dashboard_contracts -v`

Expected: PASS.

### Task 4: Mark snapshot/netflow evidence as inferred

**Files:**
- Modify: `build_earn_verified_ledger.py`, `build_earn_quality_status.py`
- Test: `tests/test_build_earn_verified_ledger.py`, `tests/test_build_earn_quality_status.py`
- Regenerate: `data/earn-quality/status.json`

- [x] **Step 1: Classify pairwise snapshot/netflow matches conservatively**

Fresh canonical snapshots may reconcile balances, but they cannot prove interest exactly when principal can change inside the interval. Keep the raw source method, but classify strict status as `inferred`.

- [x] **Step 2: Defend generated quality output against prior ledger versions**

Reclassify the legacy static `netflow+snapshot` and `recent-cycle+snapshot` entries while building quality status, so status output remains correct until all ledger files are regenerated.

- [x] **Step 3: Add and run regression tests**

Run targeted ledger/status tests and regenerate `data/earn-quality/status.json`.

- [x] **Step 4: Keep strict and inferred presentation distinct**

Reuse fresh snapshot/netflow evidence without aggregating it as verified, and display the historical checkmark only when every contributing ledger entry is strict.

### Task 5: Verify and publish

**Files:**
- Verify: `dashboard-core.js`, `tests/test_earn_dashboard_contracts.py`

- [x] **Step 1: Run the full audit suite**

Run: `npm run check:earn-audit`

Expected: all Earn audit checks pass.

- [ ] **Step 2: Browser-test two real lookups**

Run a local server and check a standard Earn address plus a borrow-route address. Confirm the page renders, has no relevant console error, and that incomplete status is not displayed as Verified.

The local server responded successfully, but the available in-app browser session could not attach newly created tabs. This remains an environment-level manual follow-up rather than a reason to weaken verification logic.

- [x] **Step 3: Review the exact diff**

Run: `git diff --check && git diff -- dashboard-core.js tests/test_earn_dashboard_contracts.py docs/superpowers`

Expected: only scoped changes; no whitespace errors.

- [x] **Step 4: Commit and push**

Run: `git add dashboard-core.js build_earn_verified_ledger.py build_earn_quality_status.py data/earn-quality/status.json tests/test_earn_dashboard_contracts.py tests/test_build_earn_verified_ledger.py tests/test_build_earn_quality_status.py docs/superpowers && git commit -m "fix: harden earn yield verification" && git push dolomite-dashboard master`

### Task 6: Remove remaining non-exact strict bypasses and rebuild old ledgers

**Files:**
- Modify: `dashboard-core.js`, `tests/test_earn_dashboard_contracts.py`
- Regenerate: `data/earn-verified-ledger/*/*.json`, `data/earn-verified-ledger/manifest.json`, `data/earn-quality/status.json`

**Interfaces:**
- Consumes: `earn_getStrictVerificationStatus(entry)`, strict-status-to-total aggregation, and the verified-ledger builder.
- Produces: strict `Verified` only for exact current replay reconciliation; diagnostics may retain adjusted values but cannot enter strict totals.

- [x] **Step 1: Add two executable regression tests**

Assert that (a) a same-`Par`, non-zero `Wei` drift and (b) a missing live borrow/collateral that replay still expects both produce `mismatch`, not `verified`.

- [x] **Step 2: Verify both tests fail before the fix**

Run the two targeted `EarnDashboardContractsTest` tests; each must reproduce the current erroneous `verified` status.

- [x] **Step 3: Keep strict verification exact**

Remove the two permissive branches from `earn_getStrictVerificationStatus`. Keep adjusted values only for explicitly non-strict diagnostic paths; remove `live_balance_adjusted` from trusted replay status/method sets and invalidate lookup cache v13.

- [x] **Step 4: Verify the regression tests and full contract suite**

Run JavaScript syntax check and the targeted contracts; confirm an adjusted result cannot become `trustedForTotal`.

- [x] **Step 5: Rebuild every existing ledger and quality status**

Run `build_earn_verified_ledger.py --existing-addresses` for every ledger chain, then regenerate `data/earn-quality/status.json`. Confirm no `verified` static `netflow+snapshot` or `recent-cycle+snapshot` entry remains.

- [ ] **Step 6: Run full audit, review, commit, and push**

Run `npm run check:earn-audit`, `git diff --check`, stage only scoped files, commit, and push `master` without force.
