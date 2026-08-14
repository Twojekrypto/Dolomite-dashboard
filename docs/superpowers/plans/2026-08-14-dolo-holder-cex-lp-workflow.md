# DOLO Holder, CEX, LP and Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the separate Team/Investor allocation history, add an exact per-exchange CEX change breakdown, render readable compact LP balances, and make Early Exits validation semantic instead of file-size brittle.

**Architecture:** Extend each generated CEX daily point with canonical exchange aggregates derived from the same reconstructed address balances already used for the total line. The static DOLO page computes the selected-range delta from the first and last visible points and renders an accessible expandable breakdown below the existing CEX chart. LP formatting remains BigInt-safe and changes only compact presentation; Early Exits keeps its existing fail-closed schema and reconciliation contracts.

**Tech Stack:** Static HTML/CSS/JavaScript, Python 3 data generators and validators, Node test runner, Python `unittest`, GitHub Actions, GitHub Pages.

## Global Constraints

- Keep `DOLO Holder Distribution Over Time` limited to market wallets.
- Keep Core Team and Investor allocations in a separate allocation card.
- Preserve verified CEX labels; never infer a new CEX classification from behavior alone.
- Keep exact LP balances available in the existing tooltip/details UI.
- Keep strict Early Exits coverage and reconciliation checks.
- Verify UI through `python3 -m http.server` at desktop and mobile widths.

---

### Task 1: Incremental Early Exits checkpoint recovery

**Files:**
- Modify: `tests/test_fetch_early_exits.py`
- Modify: `fetch_early_exits.py`
- Modify: `.github/workflows/update-odolo-data.yml`

**Interfaces:**
- Consumes: the checked-in schema-v2 `early_exits.json` checkpoint and fresh logs strictly after `coverage.toBlock`.
- Produces: `merge_incremental_output(previous, fresh_events, fresh_calculations, latest_block, updated_at=None)` with exact cumulative counts/raw totals and deduplicated early-exit rows.

- [ ] **Step 1: Write the failing regression**

Add a previous checkpoint fixture with 9,063 total withdrawals, 7,999 early exits and 1,064 normal exits. Merge three fresh normal events and assert the result has 9,066 total withdrawals, 7,999 historical early rows, an advanced `coverage.toBlock`, and unchanged raw penalty totals. Add a second test proving an invalid/regressing checkpoint raises instead of rebuilding from a partial history.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `python3 -m unittest tests.test_fetch_early_exits.FetchEarlyExitsTest.test_incremental_merge_preserves_full_audited_history`

Expected: FAIL because `merge_incremental_output` does not exist.

- [ ] **Step 3: Implement fail-closed checkpoint loading and exact merge**

Load the existing artifact before any RPC write. Require schema 2, deployment-block coverage, exact count relationships, unique early-exit event IDs, and raw aggregate reconciliation. Fetch from `previous.coverage.toBlock + 1`; when no new Withdraw logs exist, advance only the covered block and freshness timestamp. When fresh events exist, reconcile them exactly, append only new early exits, and add integer raw totals before formatting decimals.

- [ ] **Step 4: Remove the poisoned cache bootstrap**

Delete only the `Restore early-exits cache` Actions step and stop reading/writing `early_exits_cache.json` in the official main path. Keep `continue-on-error` so independent oDOLO data still publishes if the incremental refresh fails before writing.

- [ ] **Step 5: Verify GREEN and workflow contracts**

Run:

```bash
python3 -m unittest tests.test_fetch_early_exits tests.test_validate_early_exits tests.test_pages_workflow_contracts
python3 validate_data.py early_exits.json
python3 -m py_compile fetch_early_exits.py validate_data.py
```

Expected: all tests and validation pass.

- [ ] **Step 6: Commit**

```bash
git add fetch_early_exits.py tests/test_fetch_early_exits.py .github/workflows/update-odolo-data.yml
git commit -m "fix: preserve full early exit history incrementally"
```

### Task 2: Canonical CEX history breakdown

**Files:**
- Modify: `tests/test_fresh_wallets.py`
- Modify: `generate_dolo_flows.py:1132-1205`
- Generate in production workflow after push: `dolo_flows.json`

**Interfaces:**
- Consumes: `build_cex_supply_point(liquid_balances, holder_rows, address_labels)`.
- Produces: each CEX history point includes `exchanges: [{name: str, liquid: float, wallets: int}]`, sorted by decreasing liquid balance.

- [ ] **Step 1: Write failing generator contracts**

Create a test with two `Binance` labels and one `Coinbase` label. Assert the supply point preserves the exact total and wallet count while grouping numbered/deposit wallet variants under canonical `Binance`:

```python
self.assertEqual(point["exchanges"], [
    {"name": "Binance", "liquid": 150.0, "wallets": 2},
    {"name": "Coinbase", "liquid": 25.0, "wallets": 1},
])
```

Also assert non-CEX labels are excluded.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `python3 -m unittest tests.test_fresh_wallets -k cex_supply_point_groups_canonical_exchanges`

Expected: FAIL because `exchanges` is absent.

- [ ] **Step 3: Implement canonical aggregation**

Add `canonical_cex_name(label)` which strips address-role suffixes such as numeric wallet IDs, `Deposit`, `Hot Wallet`, and `Cold Wallet`, while preserving the verified label family. In `build_cex_supply_point`, use only rows already classified as `cex`, aggregate balance and positive-balance wallet count, round each aggregate to two decimals, and sort deterministically by `(-liquid, name)`.

- [ ] **Step 4: Wire aggregates into every daily point**

Add `"exchanges": cex["exchanges"]` to `calculate_cex_supply_history`. Do not hand-edit `dolo_flows.json`. The isolated local worktree has no production RPC secrets and its only available cache ends in June, so the full official artifact generation is deferred to Task 4's post-push GitHub Actions run, which has the configured RPC endpoints.

- [ ] **Step 5: Verify generator/data contracts**

Run:

```bash
python3 -m unittest tests.test_fresh_wallets tests.test_dolo_address_labels
python3 -m py_compile generate_dolo_flows.py
python3 validate_data.py dolo_flows.json
```

Expected: all local code/fixture checks pass. Task 4 must validate the generated production artifact and confirm the latest allocation metrics remain 24 wallets / approximately 202.53M DOLO before declaring completion.

- [ ] **Step 6: Commit**

```bash
git add generate_dolo_flows.py tests/test_fresh_wallets.py
git commit -m "feat: add verified CEX supply breakdown history"
```

### Task 3: CEX drilldown and LP value presentation

**Files:**
- Modify: `tests/holder-distribution-contract.test.js`
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`
- Modify route/cache references if the project cache contract requires them: `index.html`, `dolo/index.html`

**Interfaces:**
- Consumes: `cex_supply_history[].exchanges`, `cexSupplyBrushSel`, and `compactRawAmount(raw, decimals)`.
- Produces: `buildCexExchangeBreakdown(fullModel)` and readable compact LP values with exact tooltips unchanged.

- [ ] **Step 1: Write failing UI contracts**

Add tests that execute `buildCexExchangeBreakdown` with two daily points and assert exact current/change output for positive and negative exchanges, verify the CEX details toggle and address-safe layout exist, and assert:

```javascript
assert.equal(compactRawAmount('731333962797000000000000', 18), '731K');
assert.equal(compactRawAmount('16387908718238867098667283', 18), '16.4M');
```

Also assert `.dolo-lp-token-value` no longer uses `text-overflow:ellipsis`.

- [ ] **Step 2: Run focused Node tests and verify RED**

Run:

```bash
node --test tests/holder-distribution-contract.test.js
node --test tests/dolo-liquidity-ui.test.js
```

Expected: new CEX and LP assertions fail.

- [ ] **Step 3: Implement the selected-range CEX breakdown**

Below the existing CEX flow rail, add a `CEX details` disclosure. `buildCexExchangeBreakdown` must locate the first and last visible history points, union their exchange names, calculate `current`, `start`, and `change`, and sort by current balance. Render exchange name, current DOLO and signed change; use green for increases, muted red for decreases, and neutral gray for zero. Keep the disclosure keyboard-accessible and rerender it whenever the chart brush changes.

- [ ] **Step 4: Implement adaptive BigInt-safe LP compaction**

For suffix values at or above 100 units, render a rounded integer suffix (`731K`); below 100 keep one decimal (`16.4M`, `1.2K`). Promote a rounded `1000K` to `1M`. Remove ellipsis from `.dolo-lp-token-value` and tighten only the internal icon gap if needed; do not change the table width allocation.

- [ ] **Step 5: Verify Node tests and syntax**

Run:

```bash
node --test tests/holder-distribution-contract.test.js tests/dolo-liquidity-ui.test.js
node --check dolo-preview.html
git diff --check
```

Expected: all focused tests pass; if `node --check` cannot parse HTML, extract the inline script using the existing project test helper and check that generated JavaScript instead.

- [ ] **Step 6: Browser-verify desktop and mobile**

Start `python3 -m http.server 8765` and inspect `http://127.0.0.1:8765/dolo-preview.html` at 1440x900 and 390x844. Record computed styles and bounding boxes proving the LP value has no clipping/table expansion, the CEX disclosure stays inside the card, and Team/Investor remains a separate card with 24 wallets / approximately 202.53M DOLO.

- [ ] **Step 7: Commit**

```bash
git add dolo-preview.html tests/holder-distribution-contract.test.js tests/dolo-liquidity-ui.test.js index.html dolo/index.html
git commit -m "feat: add CEX drilldown and readable LP balances"
```

### Task 4: Render the separate Team/Investor allocation card

**Files:**
- Modify: `tests/holder-distribution-contract.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Consumes: existing `renderAllocationChart`, `renderAllocationBrush`, and the generated allocation metrics in `holder_bucket_history`.
- Produces: a visible `Team & Investor Allocations Over Time` card between market holder distribution and CEX supply, with the SVG/brush/legend IDs already consumed by the renderer.

- [ ] **Step 1: Write the failing DOM contract**

Add a test that parses the static section order and requires exactly one allocation card between the holder-distribution and CEX cards. Assert the card owns every renderer dependency: `allocation-chart-count`, `allocation-chart-meta`, `allocationChartWrap`, `allocationChartSvg`, `allocationChartGrid`, `allocationChartArea`, `allocationChartLines`, `allocationChartDots`, `allocationChartHoverLine`, `allocationChartHoverDot`, `allocationChartAxis`, `allocationChartTip`, `allocationBrushWrap`, `allocationBrushSvg`, `allocationBrushArea`, `allocationBrushLine`, `allocationBrushOverlay`, `allocationBrushDimL`, `allocationBrushWindow`, `allocationBrushHandleL`, `allocationBrushHandleR`, `allocationBrushDimR`, `allocationBrushLabel`, and `allocationChartLegend`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `node --test tests/holder-distribution-contract.test.js`

Expected: FAIL because the allocation card markup is absent.

- [ ] **Step 3: Add the minimal existing-system markup**

Insert one `holder-chart-card allocation-chart-card` section after `DOLO Holder Distribution Over Time` and before `CEX Supply Over Time`. Reuse the existing holder SVG, brush, tooltip, legend, focus, and responsive classes; add only allocation-specific gradient colors already defined by `ALLOCATION_CHART_SERIES`. Copy must state that this card is allocation history, not freely distributed market supply. Do not add a second data model or hard-code `24`/`202.53M` in markup—the existing renderer must populate values from generated history.

- [ ] **Step 4: Verify contract and syntax**

Run:

```bash
node --test tests/holder-distribution-contract.test.js tests/dolo-liquidity-ui.test.js
git diff --check
```

Extract the inline scripts and verify JavaScript syntax using the same method recorded in Task 3.

- [ ] **Step 5: Commit**

```bash
git add dolo-preview.html tests/holder-distribution-contract.test.js
git commit -m "fix: render Team and Investor allocation history"
```

### Task 5: Final local verification

**Files:**
- Verify only; modify no files unless a scoped regression is found.

**Interfaces:**
- Consumes: commits from Tasks 1-4.
- Produces: full local regression and browser evidence ready for whole-branch review. Production push and workflow dispatch happen only after that review is clean.

- [ ] **Step 1: Run the relevant full regression groups**

Run:

```bash
python3 -m unittest tests.test_validate_early_exits tests.test_pages_workflow_contracts tests.test_fresh_wallets tests.test_dolo_address_labels
node --test tests/holder-distribution-contract.test.js tests/dolo-liquidity-ui.test.js
python3 -m py_compile validate_data.py generate_dolo_flows.py
python3 validate_data.py early_exits.json dolo_flows.json
git diff --check
```

- [ ] **Step 2: Review scope**

Confirm only planned files changed and record any remote movement that must be reconciled before production push.

- [ ] **Step 3: Record post-review deployment checks**

Record the exact post-review sequence: push verified commits to `master`; dispatch `Update oDOLO Data` and the DOLO flows/holders workflow; monitor both; verify the Early Exits checkpoint stays cumulative and generated `dolo_flows.json` contains exact per-exchange aggregates on every CEX history point; validate the generated artifact.

- [ ] **Step 4: Define live acceptance evidence**

The final controller must open `https://twojekrypto.github.io/Dolomite-dashboard/dolo/` with a cache-busting query after both workflows finish and confirm the CEX breakdown, separate Team/Investor card, and readable LP values at desktop and mobile widths.
