# Supply Card and Pagination Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Supplier Leaderboard and Asset Activity match the DOLO Holders card, count, freshness, and pagination hierarchy while removing the manual history button and duplicate Selected Market divider.

**Architecture:** Keep the existing static-page and Supply overlay architecture. Use `liquidation-preview.html` for the semantic markup and core history state, `supply/supply-draft.js` for shared count/footer rendering, and `supply/supply-draft.css` for the DOLO-style presentation. Source-contract tests lock the exact DOM and CSS hooks; browser tests verify computed styles, dimensions, and interactions.

**Tech Stack:** Static HTML, vanilla JavaScript, CSS, Python `unittest`, local HTTP server, Browser Playwright API, GitHub Pages.

## Global Constraints

- Do not modify RPC calls, subgraph queries, static history generation, dataset coverage, market metrics, or freshness timestamps.
- Do not add configuration, dependencies, secrets, or generated data files.
- Keep the existing automatic full-history loading triggered by 90D, 180D, All, or a matching custom date range.
- Use the existing Graphite + Gold design tokens and DOLO Holders as the visual reference.
- Preserve stable 10-row table viewports, current search/type/date filters, sorting, pagination, row hover, and mobile layouts.
- Make surgical edits only; do not reformat `liquidation-preview.html`.
- The live site deploys from `master`; push the verified implementation with `HEAD:master`.

---

### Task 1: Lock dynamic counts and two-content footers

**Files:**
- Create: `tests/supply-ui-behavior.test.js`
- Modify: `tests/test_supply_table_ux_contracts.py`
- Modify: `supply/supply-draft.js:1003-1080`
- Modify: `supply/supply-draft.css:2376-2413,2676-2688`
- Modify: `liquidation-preview.html:12154-12158,12191-12197`

**Interfaces:**
- Consumes: `currentSupplyOverview.supplierCount`, `currentSupplyData.length`, `currentSupplyActivity.length`, filtered row counts, `supplyPage`, `supplyActivityPage`, `SUPPLY_PER_PAGE`, and `SUPPLY_ACTIVITY_PAGE_SIZE`.
- Produces: `formatSupplyCountBadge(total, filtered, noun) -> string`, `syncSupplierCountBadge(filteredRows) -> void`, `syncActivityCountBadge(filteredRows) -> void`, and footer markup containing only `.supply-page-range` plus `.supply-pager-controls`.

- [ ] **Step 1: Write failing behavior tests for the rendered count and footer output**

Create `tests/supply-ui-behavior.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert/strict');
const ui = require('../supply/supply-draft.js');

test('count badges expose total and filtered rows in the DOLO Holders hierarchy', () => {
  assert.equal(ui.formatSupplyCountBadge(777, 777, 'suppliers'), '777 suppliers · showing 777');
  assert.equal(ui.formatSupplyCountBadge(706, 84, 'events'), '706 events · showing 84');
});

test('table footer renders the visible range and centered pager without a redundant total', () => {
  const html = ui.buildSupplyTableFooter(1, 78, 777, 10, 'supply_goPage');
  assert.match(html, /class="supply-page-range">1–10 of 777</);
  assert.match(html, /class="supply-pager-controls"/);
  assert.match(html, />1 \/ 78</);
  assert.doesNotMatch(html, /flow-pager-total|777 wallets|777 events/);
});

test('empty table footer preserves a stable zero range and disabled navigation', () => {
  const html = ui.buildSupplyTableFooter(1, 1, 0, 10, 'supply_goPage');
  assert.match(html, /class="supply-page-range">0–0 of 0</);
  assert.equal((html.match(/disabled/g) || []).length, 4);
});
```

Remove the obsolete `test_supply_footers_use_dolo_holders_three_column_shape` source-text test from `tests/test_supply_table_ux_contracts.py`; the Node test now exercises the production renderer directly.

- [ ] **Step 2: Run the focused tests and confirm the intended failure**

Run:

```bash
node --test tests/supply-ui-behavior.test.js
```

Expected: FAIL because `supply/supply-draft.js` is not yet safe to load under Node and does not export the production render helpers.

- [ ] **Step 3: Add DOLO-style count badge markup and rendering**

Change both existing header count spans to include `supply-count-badge`:

```html
<span class="header-count supply-count-badge" id="supply-count-header"></span>
<span class="header-count supply-count-badge" id="supply-activity-count"></span>
```

Move `buildSupplyTableFooter()` to the top of the Supply draft IIFE, add `formatSupplyCountBadge()`, and expose both real production functions to Node before any browser-only initialization:

```js
function formatSupplyCountBadge(totalRows, filteredRows, noun) {
  const total = Math.max(0, Number(totalRows) || 0);
  const showing = Math.max(0, Number(filteredRows) || 0);
  return `${total.toLocaleString()} ${noun} · showing ${showing.toLocaleString()}`;
}

if (typeof module === 'object' && module.exports && typeof document === 'undefined') {
  module.exports = { formatSupplyCountBadge, buildSupplyTableFooter };
  return;
}

function syncSupplierCountBadge(filteredRows) {
  const count = document.getElementById('supply-count-header');
  if (!count) return;
  let total = Number(filteredRows) || 0;
  try {
    total = Number(currentSupplyOverview?.supplierCount) || currentSupplyData.length || total;
  } catch (error) {}
  count.textContent = formatSupplyCountBadge(total, filteredRows, 'suppliers');
}

function syncActivityCountBadge(filteredRows) {
  const count = document.getElementById('supply-activity-count');
  if (!count) return;
  let total = Number(filteredRows) || 0;
  try {
    total = currentSupplyActivity.length || total;
  } catch (error) {}
  count.textContent = formatSupplyCountBadge(total, filteredRows, 'events');
}
```

Call `syncSupplierCountBadge(totalLen)` from `renderSupplyDraftPagination()` and `syncActivityCountBadge(totalLen)` from `renderSupplyDraftActivityPagination()` after the original renderers have calculated their filtered counts. This deliberately overwrites the legacy parenthesized text without duplicating filtering logic.

- [ ] **Step 4: Remove the redundant footer total while preserving exact centering**

Reduce `buildSupplyTableFooter()` to:

```js
return `
  <span class="supply-page-range">${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}</span>
  <span class="supply-pager-controls">
    <button class="flow-pager-btn" aria-label="First page" onclick="${pageHandler}(1)" ${disabledPrev}>«</button>
    <button class="flow-pager-btn" aria-label="Previous page" onclick="${pageHandler}(${currentPage - 1})" ${disabledPrev}>‹</button>
    <span class="flow-pager-info">${currentPage} / ${totalPages}</span>
    <button class="flow-pager-btn" aria-label="Next page" onclick="${pageHandler}(${currentPage + 1})" ${disabledNext}>›</button>
    <button class="flow-pager-btn" aria-label="Last page" onclick="${pageHandler}(${totalPages})" ${disabledNext}>»</button>
  </span>
`;
```

Remove the unused `noun` parameter and its two call-site arguments. Retain the three-column grid so the pager remains geometrically centered with an empty third column, exactly like DOLO Holders.

- [ ] **Step 5: Style the count badges and run the tests**

Give `.supply-count-badge` the DOLO Holders treatment:

```css
body.supply-draft-route .supply-count-badge {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 3px 8px;
  border: 1px solid var(--supply-line-2);
  border-radius: 999px;
  background: var(--supply-bg-3);
  color: var(--supply-fg-2);
  font-family: var(--supply-mono);
  font-size: 11px;
  font-weight: 500;
  line-height: 1.45;
  white-space: nowrap;
}
```

Delete the `.flow-pager-total` Supply rules and mobile selector. Run:

```bash
node --check supply/supply-draft.js
node --test tests/supply-ui-behavior.test.js
python3 -m unittest tests.test_supply_table_ux_contracts tests.test_supply_activity_ui_contracts
```

Expected: PASS.

- [ ] **Step 6: Commit the independently testable count/footer change**

```bash
git add tests/supply-ui-behavior.test.js tests/test_supply_table_ux_contracts.py liquidation-preview.html supply/supply-draft.js supply/supply-draft.css
git commit -m "fix: align supply counts and pagination"
```

---

### Task 2: Remove the manual history control and expose a quiet automatic state

**Files:**
- Modify: `tests/supply-ui-behavior.test.js`
- Modify: `liquidation-preview.html:12212-12221,16463-16514,17497-17517,17562-17598,20216-20227`
- Modify: `supply/supply-draft.js:1-40`
- Modify: `supply/supply-draft.css:1136-1243,1334-1353,2613-2632`

**Interfaces:**
- Consumes: `currentSupplyOverview.activityStage`, `currentSupplyOverview.activityFullLoading`, and a new presentation-only `currentSupplyOverview.activityFullError`.
- Produces: `getSupplyActivityHistoryPresentation(overview) -> {copy, mode}`, `setSupplyActivityHistoryState(copy, mode) -> void` where `mode` is `loading`, `full`, `error`, or an empty string; one borderless `#supply-activity-history-pill`; no `#supply-activity-load-all-btn`.

- [ ] **Step 1: Write failing behavior tests for every history state**

Extend `tests/supply-ui-behavior.test.js`:

```js
test('activity history presentation distinguishes loading, recent, full, and error states', () => {
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation(null), {
    copy: 'Loading latest 30D activity…', mode: 'loading',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({ activityStage: 'recent' }), {
    copy: '30D history', mode: '',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({
    activityStage: 'recent', activityFullLoading: true,
  }), {
    copy: 'Loading full history…', mode: 'loading',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({ activityStage: 'full' }), {
    copy: 'Full history', mode: 'full',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({ activityFullError: true }), {
    copy: 'Full history unavailable', mode: 'error',
  });
});
```

- [ ] **Step 2: Run the tests and confirm failure**

Run:

```bash
node --test tests/supply-ui-behavior.test.js
```

Expected: FAIL because the production helper is not exported yet.

- [ ] **Step 3: Remove only the manual button markup**

Keep the accessible state container:

```html
<div class="supply-activity-history-inline">
  <div class="supply-activity-history-state" id="supply-activity-history-pill" aria-live="polite">
    <span class="supply-activity-history-dot" id="supply-activity-history-dot"></span>
    <span id="supply-activity-history-state">Loading latest 30D activity…</span>
  </div>
</div>
```

Do not remove `supplyLoadFullActivityHistory()` or `supplyActivityLoadAllHandler`; `supply/supply-draft.js` still calls them automatically for periods wider than 30 days.

- [ ] **Step 4: Make the core history-state renderer independent of the deleted button**

Add `getSupplyActivityHistoryPresentation()` beside the other exported helpers in `supply/supply-draft.js`, include it in `module.exports`, and use it from `updateSupplyActivityHistoryAction()`. Add the DOM adapter:

```js
function setSupplyActivityHistoryState(copy, mode = '') {
  const badge = document.getElementById('supply-activity-history-pill');
  const state = document.getElementById('supply-activity-history-state');
  const dot = document.getElementById('supply-activity-history-dot');
  if (!badge || !state || !dot) return;
  ['loading', 'full', 'error'].forEach(name => {
    badge.classList.remove(name);
    state.classList.remove(name);
    dot.classList.remove(name);
  });
  if (mode) {
    badge.classList.add(mode);
    state.classList.add(mode);
    dot.classList.add(mode);
  }
  state.textContent = copy;
}
```

Refactor `updateSupplyActivityHistoryAction()` to consume the tested production result:

```js
const presentation = getSupplyActivityHistoryPresentation(currentSupplyOverview);
setSupplyActivityHistoryState(presentation.copy, presentation.mode);
syncSupplyActivityFullLoadingPanel();
```

Use the helper in reset/loading paths. In the full-hydration catch, set `activityFullError = true` before rerendering; clear it when a new full load starts or completes.

- [ ] **Step 5: Restyle status as borderless metadata**

Delete all `.supply-activity-history-btn` rules. Make `.supply-activity-history-state` a quiet inline row:

```css
body.supply-draft-route .supply-activity-history-state {
  display: inline-flex !important;
  align-items: center !important;
  gap: 7px !important;
  min-height: 36px !important;
  padding: 0 !important;
  border: 0 !important;
  border-radius: 0 !important;
  background: transparent !important;
  color: var(--supply-fg-3) !important;
  font-family: var(--supply-mono) !important;
  font-size: 11px !important;
  white-space: nowrap !important;
  box-shadow: none !important;
}
```

Keep the gold loading dot, green full-history dot, and add a muted red error dot. On mobile, let the status and period selector share the action row without stretching either control.

- [ ] **Step 6: Run targeted checks and commit**

Run:

```bash
node --check supply/supply-draft.js
node --test tests/supply-ui-behavior.test.js
python3 -m unittest tests.test_supply_activity_ui_contracts tests.test_supply_table_ux_contracts
```

Expected: PASS.

Commit:

```bash
git add tests/supply-ui-behavior.test.js liquidation-preview.html supply/supply-draft.js supply/supply-draft.css
git commit -m "fix: simplify supply activity history status"
```

---

### Task 3: Match the Asset Activity shell and header geometry to DOLO Holders

**Files:**
- Modify: `supply/supply-draft.css:536-544,606-635,703-710,967-980,2691-2703`

**Interfaces:**
- Consumes: `.supply-draft-activity-continuous-surface`, `.supply-activity-header`, `.supply-data-updated`, and `.supply-intel-stats`.
- Produces: a card shell with `var(--bg-2)`, one-pixel border, 22-pixel radius, DOLO-style shadow; header-aligned freshness; exactly one divider below Selected Market.

- [ ] **Step 1: Capture the failing computed-style baseline in the browser**

Serve the unchanged page with `python3 -m http.server 8765`, load `/supply/`, and record:

- `getComputedStyle(#supply-activity-card)`: current border is `0px`, so it does not match DOLO Holders;
- `getComputedStyle(#supply-activity-card .table-card-inner)`: current duplicate shadow is present;
- `getComputedStyle(.supply-intel-header).borderBottomWidth` and `getComputedStyle(.supply-intel-stats).borderTopWidth`: both are `1px`, producing the duplicate line.

- [ ] **Step 2: Confirm the baseline fails the approved acceptance values**

Expected: Activity outer border is not `1px`, the inner shadow is not `none`, and both neighboring divider widths are `1px`.

- [ ] **Step 3: Apply the single-shell surface**

Use:

```css
body.supply-draft-route #supply-activity-card.supply-draft-activity-continuous-surface {
  background: var(--bg-2, #141417) !important;
  border: 1px solid var(--supply-line-2) !important;
  border-radius: 22px !important;
  box-shadow: 0 1px 0 rgba(255, 255, 255, .02) inset, 0 1px 2px rgba(0, 0, 0, .4) !important;
}
```

Keep the inner/header/stats/toolbar/table/footer backgrounds transparent, and suppress the inner shell's duplicate box shadow and border. Preserve `overflow: visible` so date/type dropdowns are not clipped.

- [ ] **Step 4: Align freshness and remove the duplicate divider**

Keep `.supply-intel-header`'s bottom border. Change `.supply-intel-stats` to `border-top: 0 !important`. Align `.supply-activity-header .supply-data-updated` to the title line using `align-self: flex-start` and a small top offset while retaining the current responsive wrap.

- [ ] **Step 5: Run tests and commit**

Run:

Repeat the same browser measurements. Expected: Activity outer border `1px`, radius `22px`, inner shadow `none`, header bottom border `1px`, statistics top border `0px`. Then run:

```bash
python3 -m unittest tests.test_data_freshness_surface_contracts tests.test_supply_table_ux_contracts tests.test_supply_activity_ui_contracts
git diff --check
```

Expected: browser values match and all existing regressions pass.

Commit:

```bash
git add supply/supply-draft.css
git commit -m "fix: match supply activity holder surface"
```

---

### Task 4: Cache bust, browser verification, review, and live deployment

**Files:**
- Modify: `supply/index.html:22-29`

**Interfaces:**
- Consumes: the completed HTML/CSS/JS implementation and GitHub Pages route loader.
- Produces: cache-busted production assets and a verified `master` deployment.

- [ ] **Step 1: Update the Supply asset version**

Append the suffix `-holder-card-parity-20260727` to the route `version` and both `supply-draft.css` / `supply-draft.js` query strings in `supply/index.html`. Keep the three values consistent.

- [ ] **Step 2: Run targeted and repository checks**

Run:

```bash
node --check supply/supply-draft.js
node --test tests/supply-ui-behavior.test.js
python3 -m unittest \
  tests.test_supply_table_ux_contracts \
  tests.test_supply_activity_ui_contracts \
  tests.test_data_freshness_surface_contracts
python3 run_earn_audit_checks.py
git diff --check
```

Expected: all tests pass; JavaScript and diff checks are silent.

- [ ] **Step 3: Verify the local Supply page in a real browser**

Start:

```bash
python3 -m http.server 8765
```

At `http://127.0.0.1:8765/supply/`, verify at desktop width:

1. Asset Activity computed background is `rgb(20, 20, 23)`, border is one pixel, radius is 22 pixels, and the inner shell has no duplicate shadow.
2. Supplier badge reads `<total> suppliers · showing <filtered>`; Asset Activity reads `<total> events · showing <filtered>`.
3. Searching and type/date filtering updates only `showing`, resets/clamps the page, and preserves the card height.
4. Both footers show `1–10 of N` at left and the pager centered; no right-side wallets/events label exists.
5. There is no `Load older tx` button.
6. Selecting 90D or All starts automatic loading and exposes `Loading full history…`; completion exposes `Full history`.
7. `Data updated` occupies the same upper-right title position as DOLO Holders.
8. Selected Market shows one horizontal divider between its header and statistics rail.

Repeat at 390-pixel viewport width and confirm no horizontal clipping, count/freshness collisions, or stretched history status.

- [ ] **Step 4: Perform two review passes**

Correctness/regression pass:

- confirm supplier total uses `supplierCount` with a loaded-row fallback;
- confirm activity total uses the loaded unfiltered activity array;
- confirm filters still own the `showing` count;
- confirm automatic history loading still calls `supplyActivityLoadAllHandler`;
- confirm zero-row footers remain stable.

Maintainability/security pass:

- confirm no data query, external URL, secret, dependency, or workflow changed;
- confirm obsolete button CSS and JavaScript branches are removed rather than hidden;
- confirm no unrelated selectors or preview-page sections were reformatted.

- [ ] **Step 5: Commit the cache version and any browser-proven corrections**

```bash
git add supply/index.html
git commit -m "chore: refresh supply page assets"
```

- [ ] **Step 6: Hand the verified commit to the controller**

Record the exact committed HEAD in the report and leave the worktree clean. Do not push from the task subagent: the controller runs the whole-branch review first, then owns the rebase, production push, Actions verification, and cache-busted live smoke test.

## Controller deployment after the whole-branch review

After the final SDD reviewer approves the complete branch, run:

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
git push dolomite-dashboard HEAD:master
```

If the remote advances during the push, verify that incoming commits affect generated data only, fetch/rebase again, and retry without force. Wait for `Deploy GitHub Pages`, then open the cache-busted production route:

```bash
ui_commit_sha="$(git rev-parse HEAD)"
open "https://twojekrypto.github.io/Dolomite-dashboard/supply/?v=${ui_commit_sha}"
```

Repeat the desktop smoke checks for the Activity card, dynamic counts, two-content footers, automatic history status, freshness placement, and single Selected Market divider. Confirm the live HTML references the new Supply asset version.
