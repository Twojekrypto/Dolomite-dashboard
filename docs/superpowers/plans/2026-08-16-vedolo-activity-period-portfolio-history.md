# veDOLO Activity Period, Portfolio, and History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an independent Position Activity period filter, surface transfer/merge/split/extension events in Portfolio and History, and cap veDOLO Holders/Ready to Claim at 10 rows.

**Architecture:** Extend the existing dependency-free `vedolo-position-activity.js` module with small pure helpers for period filtering, wallet participation, Portfolio rows, and History events. The three pages consume those helpers while preserving their current rendering and data-loading architectures.

**Tech Stack:** Static HTML/CSS/JavaScript, Node.js contract tests, Python static contract tests, Playwright/browser QA.

## Global Constraints

- Reuse `vedolo_flows.json`; do not add or alter data sources, RPC calls, generators, or JSON schemas.
- Internal transfer/merge/split/extension events must never count as newly locked DOLO.
- Position Activity defaults to `7D`; Flow and Activity filter state remain independent.
- Holders and Ready to Claim render at most 10 real rows per page.
- Preserve existing Graphite + Gold table and dropdown UX.

---

### Task 1: Shared veDOLO activity adapters

**Files:**
- Modify: `vedolo-position-activity.js`
- Modify: `tests/vedolo_position_activity.test.js`

**Interfaces:**
- Consumes: raw `locks`, `transfers`, wallet address, and inclusive UNIX timestamp bounds.
- Produces: `activityTouchesAddress(row,address)`, `filterActivityRows(rows,options)`, `buildPortfolioActivityRows(...)`, and `buildHistoryActivityEvents(...)`.

- [ ] **Step 1: Write failing contracts** for sender/recipient transfers, owner-only merge/split/extend, inclusive period bounds, exact semantic deduplication, and neutral History/Portfolio event metadata.
- [ ] **Step 2: Run `node tests/vedolo_position_activity.test.js`** and confirm the new exports are missing.
- [ ] **Step 3: Implement the pure helpers** using normalized exact addresses and transaction/action/position identity keys.
- [ ] **Step 4: Rerun the focused Node test** and confirm all contracts pass.
- [ ] **Step 5: Commit the shared semantic adapter change.**

### Task 2: veDOLO page filters and pagination

**Files:**
- Modify: `vedolo-preview.html`
- Modify: `vedolo/index.html`
- Modify: `tests/vedolo_filter_table_ux.test.js`

**Interfaces:**
- Consumes: `filterActivityRows` from Task 1.
- Produces: independent `state.activity.period`, `#dd-position-activity-period`, and 10-row Holders/Ready to Claim state.

- [ ] **Step 1: Add failing static/runtime contracts** for the left-group search/action layout, right period dropdown, `7D` default, independent reset behavior, and both `perPage:10` values.
- [ ] **Step 2: Run the focused Node test** and record the expected failures.
- [ ] **Step 3: Move the action dropdown beside search, add the period dropdown on the right, wire it to Position Activity only, and change the two page sizes to 10.**
- [ ] **Step 4: Update mobile rules** so search is full width and action/period controls wrap without document overflow.
- [ ] **Step 5: Advance the veDOLO route cache key and rerun focused tests.**

### Task 3: Portfolio semantic activity

**Files:**
- Modify: `portfolio-preview.html`
- Modify: `portfolio/index.html`
- Modify: `tests/test_portfolio_preview_contracts.py`

**Interfaces:**
- Consumes: `buildPortfolioActivityRows` and existing exercise/lock rows.
- Produces: Portfolio route rows for `transfer`, `merge`, `split`, and `extend`, deduplicated against existing events.

- [ ] **Step 1: Add failing contracts** that require the shared script, all four route options, exact wallet matching, and exclusion of internal actions from the fallback `Total locked` sum.
- [ ] **Step 2: Run the focused Python contract module** and confirm failure against the current route list.
- [ ] **Step 3: Append shared semantic rows in `buildExerciseRows`, add route labels/tooltips, and render position transitions without inventing payment or price values.**
- [ ] **Step 4: Exclude transfer/merge/split/extend from fallback principal totals and advance the Portfolio route cache key.**
- [ ] **Step 5: Rerun Portfolio contracts and commit.**

### Task 4: Transaction History semantic activity

**Files:**
- Modify: `history/index.html`
- Modify: `history/history.js`
- Modify: `tests/test_history_tax_export_contracts.py`

**Interfaces:**
- Consumes: `buildHistoryActivityEvents`, current wallet, selected date bounds, and Berachain selection.
- Produces: grouped History rows/actions `vedoloTransfer`, `vedoloMerge`, `vedoloSplit`, and `vedoloExtend`.

- [ ] **Step 1: Add failing contracts** for loading the shared classifier, action options/labels, Berachain/date scoping, grouping before render, and neutral review-safe event semantics.
- [ ] **Step 2: Run the focused History contract module** and confirm the shared semantic path is absent.
- [ ] **Step 3: Fetch `vedolo_flows.json` alongside existing History requests, build exact-wallet events, append before `groupEvents`, and register action filter/table labels.**
- [ ] **Step 4: Keep internal actions out of deposit/withdraw semantics and advance History cache keys.**
- [ ] **Step 5: Rerun History contracts and commit.**

### Task 5: Full verification and production push

**Files:**
- Verify only: all changed files

**Interfaces:**
- Consumes: completed Tasks 1–4.
- Produces: tested production commit on `master`.

- [ ] **Step 1: Run focused and repository validation commands**, including Node syntax extraction, Python contract modules, `validate_data.py`, and `git diff --check`.
- [ ] **Step 2: Serve with `python3 -m http.server` and verify veDOLO, Portfolio, and History at 1440×900, 1024×768, and 390×844.**
- [ ] **Step 3: Exercise Position Activity action/period controls, Portfolio route filtering, History action filtering, Holders paging, and Ready to Claim paging.**
- [ ] **Step 4: Fetch the latest remote `master`, reconcile without discarding unrelated updates, rerun the critical checks, and push the tested commit to production.**
- [ ] **Step 5: Verify the live GitHub Pages routes use the new cache keys and report the deployed commit.**
