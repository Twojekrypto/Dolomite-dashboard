# EARN Canonical Round-Robin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make canonical wallet backfill converge and display truthful progress independently from live chain freshness.

**Architecture:** Reuse per-wallet `lastScannedBlock` and `scanRange` as durable state. The selector schedules missing and least-recently-scanned histories first; the freshness generator publishes separate historical and head-coverage fields consumed by the existing EARN pill.

**Tech Stack:** Python 3.11, `unittest`, static JavaScript, GitHub Actions, GitHub Pages.

## Global Constraints

- Preserve the strict two-hour head-recency policy.
- Do not add dependencies or a new generated cursor file.
- Keep existing coverage fields compatible with workflow and audit consumers.
- Production remains the `master` branch.

---

### Task 1: Fair Canonical Selection

**Files:**
- Modify: `tests/test_select_earn_canonical_hot_addresses.py`
- Modify: `select_earn_canonical_hot_addresses.py`

**Interfaces:**
- Consumes: canonical history JSON with `lastScannedBlock`.
- Produces: `build_selection(...) -> (selected_addresses, metadata)` with finite oldest-first selection.

- [ ] Add a failing test proving a high-score wallet refreshed in the previous batch cannot jump ahead of an older wallet.
- [ ] Add a failing test proving missing histories are selected before stale histories.
- [ ] Run `python3 -m unittest tests.test_select_earn_canonical_hot_addresses -v` and confirm the new assertions fail.
- [ ] Order missing wallets first and stale wallets by `(lastScannedBlock, -score, address)` after priority addresses.
- [ ] Re-run the targeted selector tests and confirm they pass.

### Task 2: Truthful Backfill Coverage

**Files:**
- Modify: `tests/test_update_earn_freshness_status.py`
- Modify: `update_earn_freshness_status.py`

**Interfaces:**
- Consumes: chain manifest `fromBlock`/`lastBlock` and wallet history `scanRange`/`lastScannedBlock`.
- Produces: canonical coverage fields `backfilledWalletCount`, `incompleteBackfillWalletCount`, and `headFreshWalletCount` while retaining compatible existing fields.

- [ ] Add failing tests where an old but full-range wallet is backfilled but not head-fresh, and a short-range wallet is incomplete.
- [ ] Run `python3 -m unittest tests.test_update_earn_freshness_status -v` and confirm failure on missing fields.
- [ ] Extend `_canonical_coverage_status` and use historical completion for backlog reasons.
- [ ] Re-run the freshness tests and confirm they pass.

### Task 3: Dashboard Contract And Workflow Guard

**Files:**
- Modify: `tests/test_earn_dashboard_contracts.py`
- Modify: `dashboard-core.js`
- Modify: `.github/workflows/update-earn-ethereum-canonical-history.yml`

**Interfaces:**
- Consumes: `canonical.coverage.backfilledWalletCount`.
- Produces: `canonical backfill · X/Y wallets` using finite historical progress.

- [ ] Add failing contract tests for the new dashboard field and workflow selector behavior.
- [ ] Run the contract tests and confirm the new assertions fail.
- [ ] Update the formatter and workflow comments/metadata without changing the 30-minute cadence or two-hour recency limit.
- [ ] Re-run contract tests and JavaScript syntax validation.

### Task 4: Full Verification And Deployment

**Files:**
- Verify all modified files.

**Interfaces:**
- Consumes: completed Tasks 1-3.
- Produces: tested production commit on `master`.

- [ ] Run `npm run check:earn-audit` and require zero failures.
- [ ] Run `python3 -m py_compile select_earn_canonical_hot_addresses.py update_earn_freshness_status.py`.
- [ ] Run `node --check dashboard-core.js` and `git diff --check`.
- [ ] Serve with `python3 -m http.server`, inspect `/earn/`, and verify the rendered pill and console.
- [ ] Commit, push `master`, wait for GitHub Pages, and verify the public status JSON and page.
