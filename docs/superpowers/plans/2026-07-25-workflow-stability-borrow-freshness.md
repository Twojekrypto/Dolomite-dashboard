# Workflow Stability and Borrow Freshness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the last-24-hour workflow failures, standardize Borrow freshness metadata, and make the 24-hour lending-position change understandable at a glance.

**Architecture:** Preserve the existing strict audit and explicit deployment topology. Repair the same-day EARN snapshot race with a full snapshot-tree fingerprint, coalesce only already-queued workflow dispatches, and route transient TVL market failures through the existing 0.1% coverage guard. Reuse a single Borrow freshness component for all three locations.

**Tech Stack:** GitHub Actions, Bash, Python 3.12, static HTML/CSS/JavaScript, unittest, Playwright/Chromium.

## Task 1: Lock the EARN race and queue behavior with tests

**Files:**

- Modify: `tests/test_earn_commit_helper.py`
- Modify: `tests/test_pages_workflow_contracts.py`

1. Add a failing integration test where a same-day snapshot JSON changes during rebase without a manifest change.
2. Add a failing test proving a queued Monitor run suppresses a duplicate dispatch.
3. Add a failing workflow contract requiring Pages queue coalescing.
4. Run the focused tests and confirm the expected failures.
5. Change `scripts/commit_with_fresh_earn_status.sh` to compare the complete snapshot directory tree and coalesce queued Monitor runs.
6. Change `.github/workflows/monitor-earn-freshness.yml` to coalesce queued Pages runs.
7. Re-run the tests to green.

## Task 2: Route TVL endpoint noise through the strict coverage guard

**Files:**

- Modify: `tests/test_fetch_dolomite_total_supply_history.py`
- Modify: `fetch_dolomite_total_supply_history.py`

1. Add failing tests for one immaterial failed market and one material missing-market share.
2. Represent exhausted metric requests as empty histories with `fetchError`.
3. Extract and reuse a strict official-snapshot coverage validator.
4. Re-run the focused tests to green.

## Task 3: Implement the approved Borrow presentation

**Files:**

- Modify: `tests/test_borrow_ux_contracts.py`
- Modify: `liquidation-preview.html`

1. Add failing source contracts for the shared DOLO-style freshness component and two-line change badge.
2. Replace the three freshness variants with the shared component.
3. Remove dynamic freshness colors and keep only text updates.
4. Calculate the 24-hour percentage from the history baseline and render signed absolute and percentage values.
5. Re-run Borrow contracts to green.

## Task 4: Verify behavior and production deployment

1. Run all focused regression suites and syntax checks.
2. Run the representative EARN audit.
3. Start `python3 -m http.server` and inspect Borrow at desktop and mobile sizes with Playwright.
4. Have the browser QA subagent independently inspect alignment and computed values required by repository lessons.
5. Rebase on current `dolomite-dashboard/master`, push `HEAD:master`, and dispatch the EARN snapshot and TVL refresh workflows.
6. Monitor the affected workflows and final Pages deployment to successful completion.
