# veDOLO Borrow Fee Rebate Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the veDOLO Borrow Fee Rebates panel reconcile with the current official Dolomite claim state, published epoch artifacts, and contract correction semantics.

**Architecture:** Keep Berachain rolling-claims events as the source for realized user savings, but replay each epoch as a replaceable cumulative snapshot so same-epoch corrections can remove a rolled-back publication. Enrich every retained epoch from the official `liquidity-mining-data` artifacts for claim boundaries and the exact per-market maximum-rebate formula, then make validation and UI confidence fail closed when official inputs disagree.

**Tech Stack:** Python 3, `web3.py`, `requests`, `unittest`, static HTML/JavaScript, GitHub Pages.

**Spec:** User-approved correctness repair following the 2026-09-03 veDOLO Borrow Fee Rebates audit in this task.

## Global Constraints

- Preserve onchain published amounts; never fabricate replacements for anomalous official source data.
- Treat the current official aggregated rebate artifact as the authoritative latest published epoch.
- Keep daily chart allocation explicitly described as an estimate.
- Add regression coverage before production changes.
- Push the verified result to production branch `master` on remote `dolomite-dashboard`.

---

### Task 1: Replay legacy and corrected rolling-claims publications

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py`
- Modify: `fetch_dolomite_revenue.py`

**Interfaces:**
- Consumes: rolling-claims calldata and `MarketIdToMerkleRootSet` logs.
- Produces: `fee_rebate_transaction_context_from_input(...)` and corrected per-epoch realized rebate rows.

- [ ] Add a failing test proving the four-argument legacy handler decodes epoch 1 with implicit increment.
- [ ] Add a failing replay test proving `_incrementEpoch=false` replaces the same epoch snapshot and cancels rolled-back epoch 10.
- [ ] Run the focused tests and confirm the expected failures.
- [ ] Add the legacy ABI and snapshot-based replay state.
- [ ] Run the focused tests and confirm they pass.

### Task 2: Use official epoch artifacts for maximum rebate and provenance

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py`
- Modify: `fetch_dolomite_revenue.py`

**Interfaces:**
- Consumes: official aggregate metadata and `finalized/80094/borrow-interest/epoch-N-output.json`.
- Produces: authoritative epoch cap, claim boundaries, official per-market `maxRebateUSD`, source provenance, and anomaly status.

- [ ] Add a failing pure-formula test using hand-derived per-market values and the official 5% revenue tolerance.
- [ ] Add a failing reconciliation test that removes epochs newer than the aggregate artifact and flags `saved > official max` as a source anomaly.
- [ ] Run the focused tests and confirm the expected failures.
- [ ] Implement official artifact fetching, exact Decimal arithmetic, authoritative trimming, caching through preserved epoch audit fields, and anomaly metadata.
- [ ] Run the focused tests and confirm they pass.

### Task 3: Fail-closed validation and UI confidence

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py`
- Modify: `validate_data.py`
- Modify: `revenue-preview.html`
- Modify: `revenue/index.html`

**Interfaces:**
- Consumes: official audit/anomaly fields embedded in `dolomite_revenue.json`.
- Produces: validator enforcement and UI that does not present utilization as verified when selected epochs contain a source anomaly.

- [ ] Add failing validator/UI contract tests for official provenance, anomaly enforcement, and estimated daily allocation copy.
- [ ] Run focused tests and confirm the expected failures.
- [ ] Update the validator contract and both Revenue route sources.
- [ ] Run focused tests and confirm they pass.

### Task 4: Regenerate, verify, deploy

**Files:**
- Modify: `dolomite_revenue.json`
- Modify if regenerated: `data/dolomite-revenue-onchain-overrides.json`

**Interfaces:**
- Consumes: fixed generator plus live official/RPC data.
- Produces: production artifact with epochs 1-9, no rolled-back epoch 10, official max values, and source anomaly flags.

- [ ] Run the generator and verify total published savings reconcile to retained epochs.
- [ ] Run unit tests, data validation, Python compilation, and JavaScript syntax extraction/checks.
- [ ] Serve the dashboard locally and inspect the Revenue route at desktop and mobile widths.
- [ ] Commit only task-related files.
- [ ] Rebase/fast-forward against current `dolomite-dashboard/master`, push `HEAD:master`, wait for Pages, and verify cache-busted live JSON/page.
