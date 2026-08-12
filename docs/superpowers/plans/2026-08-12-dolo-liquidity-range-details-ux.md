# DOLO Liquidity Range and Details UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the DOLO Liquidity Providers table and make price ranges, rounded amounts, quality evidence, and zero-value tooltips understandable and visually consistent.

**Architecture:** Keep the existing static table component in `dolo-preview.html` and change only its presentation helpers, column schema, and scoped CSS. Use exact `BigInt` arithmetic for two-decimal display rounding, preserve the full exact value in table hover text, and keep quality data in the existing expanded details. Validate source contracts with Node tests and rendered behavior with Playwright against a local HTTP server.

**Tech Stack:** Static HTML/CSS/JavaScript, Node test runner, Python Playwright, GitHub Pages.

## Global Constraints

- The active table has exactly eight columns: Chain, Address, Pair, Price Range, DOLO, Paired Asset, Value, Details.
- `Status` is absent from the active row and remains available as `Data status` inside Details.
- `Tick interval` is not rendered anywhere in the LP details.
- Current token amounts use exact `BigInt` mathematical rounding to two fractional digits.
- Exact table hover values remain unchanged but anchor to the visible numeric text only.
- `In range` uses muted green; `Out of range` uses muted red; colour is not the only status signal.
- Near-full upper bounds read `Protocol maximum`; no scientific-notation protocol ceiling is shown.
- The ten-row fixed viewport, 68px desktop row height, mobile horizontal table scroll, and page-level no-overflow contract remain intact.

---

### Task 1: Lock the eight-column and details contracts

**Files:**
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Consumes: `ACTIVE_HEADERS`, `ACTIVE_WIDTHS`, `activeRow(row)`, `detailRow(row)`, and `render()`.
- Produces: an eight-column active table with `colspan="8"` detail/spacer rows and no row-level quality chip.

- [ ] **Step 1: Write failing source-contract assertions**

Require `ACTIVE_HEADERS` to omit `quality`, require `ACTIVE_WIDTHS` to contain eight percentages summing to 100, require both generated row spans to equal eight, and reject `<td class="status-cell">` in `activeRow`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `node --test tests/dolo-liquidity-ui.test.js`  
Expected: failures showing the existing nine-column widths, `colspan="9"`, and row-level status cell.

- [ ] **Step 3: Implement the eight-column schema**

Set headers to:

```js
const ACTIVE_HEADERS = [
  ["chainKey","Chain"], ["wallet","Address"], ["pair","Pair"],
  ["rangeStatus","Price Range"], ["doloRaw","DOLO"],
  ["pairedRaw","Paired Asset"], ["valueUsd","Value"], ["details","Details"],
];
const ACTIVE_WIDTHS = [10,22,14,15,10,13,10,6];
```

Remove the status `<td>` from `activeRow`, change detail and spacer `colspan` to `8`, and leave `Data status` in `detailRow`.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `node --test tests/dolo-liquidity-ui.test.js`  
Expected: all column and details tests pass.

### Task 2: Add exact two-decimal amount rounding and readable bounds

**Files:**
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Produces: `roundedRawAmount(raw, decimals, fractionDigits = 2) -> string`.
- Updates: `detailRow(row)` and `rangePresentation(row)`.

- [ ] **Step 1: Write failing formatting assertions**

Extract the inline LP script in the existing test harness and assert:

```js
roundedRawAmount("16387908718238867098667283", 18) === "16,387,908.72"
roundedRawAmount("271676083206709108742668", 18) === "271,676.08"
roundedRawAmount("0", 6) === "0.00"
```

Also require `Current DOLO`, `Current paired asset`, `Protocol maximum`, and absence of `Tick interval`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `node --test tests/dolo-liquidity-ui.test.js`  
Expected: failure because `roundedRawAmount` and the new copy do not exist.

- [ ] **Step 3: Implement exact rounding and bounds copy**

Use integer scale and half-up rounding:

```js
const unit = 10n ** BigInt(decimals);
const scale = 10n ** BigInt(fractionDigits);
const rounded = (BigInt(raw) * scale + unit / 2n) / unit;
```

Format the resulting whole and fixed two-digit fractional parts with grouping. In `detailRow`, use the rounded helper and rename the two metrics. Remove the ticks variable and evidence item. For near-full ranges, render the real lower bound plus `Protocol maximum`; include `${pairedSymbol} per DOLO` as the unit.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `node --test tests/dolo-liquidity-ui.test.js`  
Expected: exact formatting and readable-bound contracts pass.

### Task 3: Apply restrained range colours and fix amount tooltip anchoring

**Files:**
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Updates: `rangeCell(row)` to emit `is-in-range` / `is-out-of-range` modifier classes.
- Updates: `tokenAmountCell(raw, decimals, symbol)` so `data-tooltip` belongs to `.dolo-lp-token-value`.

- [ ] **Step 1: Write failing semantic and anchor assertions**

Require:

```html
<div class="dolo-lp-range is-in-range">
<span class="dolo-lp-token-value" data-tooltip="0 USDC">0</span>
```

Require scoped CSS where `.is-in-range > span` uses `var(--up)` and `.is-out-of-range > span` uses `var(--down)`. Reject `data-tooltip` on `.dolo-lp-token-amount`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `node --test tests/dolo-liquidity-ui.test.js`  
Expected: failures for missing modifier classes and container-level tooltip anchor.

- [ ] **Step 3: Implement scoped colour and numeric tooltip anchor**

Add only these range colour rules:

```css
.dolo-lp-range.is-in-range>span{color:var(--up)}
.dolo-lp-range.is-out-of-range>span{color:var(--down)}
```

Keep secondary range text neutral. Move `data-tooltip` to the numeric span, add `.dolo-lp-token-value`, and retain the token icon outside the trigger.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `node --test tests/dolo-liquidity-ui.test.js`  
Expected: all focused UI contracts pass.

### Task 4: Browser verification, integration, and production deployment

**Files:**
- Modify: `dolo-preview.html`
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `docs/superpowers/plans/2026-08-12-dolo-liquidity-range-details-ux.md`

**Interfaces:**
- Consumes: completed table/render helpers from Tasks 1–3.
- Produces: verified GitHub Pages deployment on `master`.

- [ ] **Step 1: Run complete targeted checks**

Run:

```bash
node --test tests/dolo-liquidity-ui.test.js tests/dolo-liquidity-workflow.test.js
python3 -m unittest tests.test_generate_dolo_liquidity tests.test_validate_dolo_liquidity
python3 validate_data.py data/dolo-liquidity.json
git diff --check
```

Expected: zero failures.

- [ ] **Step 2: Verify rendered behavior locally**

Serve with `python3 -m http.server 8765 --bind 127.0.0.1`. At 1440×900 and 390×844 verify with Playwright:

- eight headers and eight cells per active row;
- no page-level horizontal overflow;
- Details contains rounded amounts and no Tick interval;
- near-full row shows `Protocol maximum`;
- computed range colours equal the project `--up` / `--down` values;
- hovering a visible zero value anchors the shared tooltip to the numeric span and keeps its horizontal center within 4px of the trigger center;
- no console errors.

- [ ] **Step 3: Self-review the diff**

Review once for correctness/regressions and once for maintainability/security. Confirm no data pipeline, unrelated table, secret, dependency, or global tooltip behavior changed.

- [ ] **Step 4: Commit and push production**

Commit the spec, plan, tests, and UI change with a focused message, rebase onto the latest `dolomite-dashboard/master`, rerun the complete targeted checks, and push `HEAD:master` without force.

- [ ] **Step 5: Verify live Pages**

Wait for the Pages workflow and smoke test to succeed. Open the public DOLO page with a unique query parameter and repeat the key DOM assertions for eight columns, rounded values, readable bounds, semantic range colours, and zero-amount tooltip placement.
