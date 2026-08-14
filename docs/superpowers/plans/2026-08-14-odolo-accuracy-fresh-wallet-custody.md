# oDOLO Accuracy, Fresh Wallet Sizing, and Custody Classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the oDOLO discount/exerciser metrics contract-accurate, keep oDOLO publication independent from early-exit RPC outages, size Fresh 10K+ rows to actual results with a 10-row page cap, prevent cross-window custody false positives, and reuse canonical DOLO holder names in distribution details.

**Architecture:** Keep the static-dashboard structure and existing generated JSON pipeline. Add exact lock seconds to the exerciser artifact, reproduce the deployed discount calculator's weekly rounding in the oDOLO page, and make the table renderers consume period-specific row sets. Keep custody classification heuristic but require one individual flow observation to satisfy a rule; never combine maxima from unrelated chain/period/role rows.

**Tech Stack:** Static HTML/CSS/JavaScript, Python 3.11 data scripts, `unittest`, Node test runner, GitHub Actions, Playwright/browser QA.

## Global Constraints

- Production is GitHub Pages from `master`.
- Preserve Graphite + Gold UX and existing table patterns.
- Use integer/raw units for source calculations; round only for presentation.
- Keep verified labels authoritative over historical snapshot labels.
- Preserve the last verified early-exit artifact when its independent RPC refresh fails.

---

### Task 1: Exact oDOLO Discount and Exerciser Counts

**Files:**
- Modify: `generate_exercisers.py`
- Modify: `odolo-preview.html`
- Modify: `tests/test_odolo_exercise_metrics.py`
- Modify: `tests/test_odolo_preview_contracts.py`
- Modify: `odolo/index.html`

**Interfaces:**
- Consumes: immutable exercise calldata and existing `exercisers_by_address.json` rows.
- Produces: per-transaction `lock_seconds`; `protocolDiscountFromDurationSeconds(seconds)`; period-specific exerciser counts and a dynamic average-lock guide.

- [ ] **Step 1: Write failing contracts**

Add literal tests for 3.5 days = 2.5%, 7 days = 5%, 14 days = 5.436893%, 365 days = 27.718447%, and 721.1 days = 50%; require a two-wallet period count independent of the global mixed-method total; require zero-USDC dust to remain a plotted exercise; require the average guide to be derived from visible points; require exact `lock_seconds` in generated transactions.

- [ ] **Step 2: Verify RED**

Run:

```bash
python3 -m unittest tests.test_odolo_exercise_metrics tests.test_odolo_preview_contracts -v
```

Expected: the new deployed-calculator, period-count, dust, average-guide, and lock-seconds assertions fail on current production code.

- [ ] **Step 3: Implement minimally**

Persist `extract_lock_duration_seconds(tx)` in cache/output, calculate the schedule as 0–5% linearly during week one and then `ceil((seconds - WEEK) / WEEK)` over 103 weeks capped at 50%, derive the guide from rendered points, count non-search period rows in the header, and admit finite zero-USDC exercises to the curve.

- [ ] **Step 4: Verify GREEN**

Run the Task 1 command again and require zero failures.

### Task 2: Fresh 10K+ Dynamic Rows with 10-Row Pagination

**Files:**
- Modify: `dolo-preview.html`
- Modify: `tests/fresh-wallets-preview-contract.test.js`
- Modify: `index.html`
- Modify: `dolo/index.html`

**Interfaces:**
- Consumes: the filtered Fresh 10K+ wallet row list.
- Produces: `freshPageModel(rows, page, pageSize)` and an unpadded table body showing 0–10 actual rows.

- [ ] **Step 1: Write failing behavior tests**

Execute the pure pagination helper with 2 rows and 12 rows. Require two visible rows for the first case, ten then two rows for the second case, and no `.fresh-spacer-row` output.

- [ ] **Step 2: Verify RED**

Run:

```bash
node --test tests/fresh-wallets-preview-contract.test.js
```

Expected: helper/page-size/unpadded-body assertions fail.

- [ ] **Step 3: Implement minimally**

Set `freshPageSize` to 10, use the pure page model in `renderFreshWallets`, and remove artificial spacer rows and stable-body height logic from this table only.

- [ ] **Step 4: Verify GREEN**

Run the Task 2 command and require zero failures.

### Task 3: Custody Heuristic and Canonical Holder Names

**Files:**
- Modify: `dolo-preview.html`
- Modify: `tests/test_dolo_address_labels.py`
- Modify: `tests/holder-distribution-contract.test.js`

**Interfaces:**
- Consumes: individual accumulator/seller observations plus shared `DOLO_ADDR_LABELS`.
- Produces: row-local `isPotentialCustodyObservation(item, role)` and canonical-label precedence in historical holder details.

- [ ] **Step 1: Write failing regressions**

Use the real bad pattern for `0xc0bb…d584`: a four-transaction Berachain accumulator row plus a separate 296-transaction Ethereum seller row must not synthesize one custody signal. Require qualifying custody rows to remain detected. Require the current shared label to override a stale snapshot label.

- [ ] **Step 2: Verify RED**

Run:

```bash
node --test tests/holder-distribution-contract.test.js
python3 -m unittest tests.test_dolo_address_labels -v
```

Expected: new row-local and label-precedence assertions fail; the previously stale locale assertion is corrected to the actual `en-US` contract.

- [ ] **Step 3: Implement minimally**

Evaluate custody predicates per flow row before merging labels and choose `sharedAddressInfo(addr).label` before `item.label` for historical rows. Do not add an invented identity label for the audited EOA.

- [ ] **Step 4: Verify GREEN**

Run the Task 3 commands and require zero failures.

### Task 4: Resilient oDOLO Publication

**Files:**
- Modify: `.github/workflows/update-odolo-data.yml`
- Modify: `tests/test_pages_workflow_contracts.py`

**Interfaces:**
- Consumes: independently generated exerciser, average-lock, contract, and early-exit artifacts.
- Produces: a workflow where an early-exit RPC outage preserves its verified artifact but does not block publishing newly verified exercise data.

- [ ] **Step 1: Write failing workflow contract**

Parse the workflow and require the early-exit step to continue on error while validation and commit still run afterward.

- [ ] **Step 2: Verify RED**

Run:

```bash
python3 -m unittest tests.test_pages_workflow_contracts -v
```

Expected: the resilience contract fails because the step currently aborts the job.

- [ ] **Step 3: Implement minimally**

Mark only `Fetch early exit penalty data` as `continue-on-error: true`; retain fail-closed artifact preservation and subsequent full validation.

- [ ] **Step 4: Verify GREEN**

Run the Task 4 command and require zero failures.

### Task 5: Full Verification and Production Publish

**Files:**
- Verify all modified files and regenerated artifacts.

- [ ] **Step 1: Regenerate and validate**

Run focused Python/Node tests, `python3 -m py_compile`, `node --check` for inline scripts, `validate_data.py`, and `git diff --check`.

- [ ] **Step 2: Browser QA**

Serve with literal `python3 -m http.server`, then verify `/odolo/` and `/dolo/` at desktop and mobile sizes. Check curve labels/counts, Fresh 2-row and 10-row behavior, custody classification, holder-detail naming, console health, clipping, and interactions.

- [ ] **Step 3: Review**

Run correctness/regression and maintainability/security passes. Confirm no secrets or unrelated files are staged.

- [ ] **Step 4: Commit and publish**

Commit the scoped change, push the commit to `dolomite-dashboard/master`, trigger/observe Pages deployment, and verify the live routes expose the new cache versions and rendered behavior.
