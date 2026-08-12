# DOLO Liquidity Table UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DOLO Liquidity Providers and DOLO Flows match the Dolomite Assets table language while keeping liquidity rows and columns stable for extreme concentrated-liquidity ranges.

**Architecture:** Keep the feature inside the existing static `dolo-preview.html` route and reuse its current state, filtering, and generated `data/dolo-liquidity.json`. Add explicit layout contracts in CSS/HTML, small pure rendering helpers for range classification and compact bounds, and extend the existing Node contract test before each production edit. Route entry points receive only a cache-key refresh after the UI is verified.

**Tech Stack:** Static HTML, CSS, browser JavaScript, Node `node:test`, Playwright browser verification through the existing local test tooling, GitHub Pages.

## Global Constraints

- Preserve the existing Graphite + Gold identity and reuse the Dolomite Assets Details and sortable-header language.
- Keep ten table slots per page; opening Details may increase card height.
- Active columns are exactly `9%, 13%, 19%, 14%, 10%, 12%, 10%, 7%, 6%`.
- History columns are exactly `10%, 9%, 13%, 19%, 9%, 11%, 13%, 10%, 6%`.
- Raw extreme price bounds never appear in collapsed rows.
- Do not change LP calculations, ownership classification, source quality, filters, pagination, or generated data.
- Only the table wrapper may overflow horizontally; the document must not.
- Verify 1440×900, 1024×768, 768×1024, and 390×844 before deployment.

---

### Task 1: Lock the liquidity layout and range contracts

**Files:**
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Consumes: existing `doloLpState`, `ACTIVE_HEADERS`, `HISTORY_HEADERS`, `activeRow(row)`, `historyRow(row)`, `detailRow(row)`, and `renderHeaders()`.
- Produces: `compactRangeBound(value) -> string`, `rangePresentation(row) -> { state: string, detail: string }`, mode-specific `<colgroup data-dolo-lp-columns="active|history">`, and stable `aria-sort` headers.

- [ ] **Step 1: Add failing layout and range tests**

Add contracts that require:

```js
assert.match(html, /\.dolo-lp-table\{[^}]*table-layout:fixed/s);
assert.match(html, /data-dolo-lp-columns="active"/);
assert.match(html, /data-dolo-lp-columns="history"/);
assert.match(html, /function compactRangeBound\(value\)/);
assert.match(html, /function rangePresentation\(row\)/);
assert.match(html, /Near-full range/);
assert.match(html, /Always active/);
assert.match(html, /aria-sort="\$\{ariaSort\}"/);
assert.doesNotMatch(html, /\$\{esc\(row\.rangeLower\)\}.*\$\{esc\(row\.rangeUpper\)\}/s);
```

Also require explicit Active and History percentage arrays and ten spacer slots.

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `node --test tests/dolo-liquidity-ui.test.js`

Expected: FAIL because fixed colgroups, compact range helpers, and `aria-sort` are absent.

- [ ] **Step 3: Implement fixed geometry and compact ranges**

In `dolo-preview.html`:

```js
const ACTIVE_WIDTHS = [9,13,19,14,10,12,10,7,6];
const HISTORY_WIDTHS = [10,9,13,19,9,11,13,10,6];

function compactRangeBound(value){
  const numeric = Number(value);
  if(!Number.isFinite(numeric)) return "Unavailable";
  if(numeric === 0) return "0";
  const magnitude = Math.abs(numeric);
  if(magnitude < 0.001 || magnitude >= 1e7) return numeric.toExponential(2).replace("e-","e−").replace("e+","e+");
  return new Intl.NumberFormat("en-US", {maximumSignificantDigits:4, useGrouping:false}).format(numeric);
}
```

Implement `rangePresentation(row)` so full-range positions show `Full range / Always active`, near-protocol extremes show `In range|Out of range / Near-full range`, and all remaining bounded positions show `In range|Out of range / Custom range`. Render only this classification in `rangeCell(row)`.

Have `renderHeaders()` generate the mode-specific `<colgroup>` before `<thead>`, add a stable `.sort` span for sortable columns, and set `aria-sort` to `ascending`, `descending`, or `none`.

- [ ] **Step 4: Add range evidence to Details**

For active concentrated positions, append:

```html
<div class="dolo-lp-detail dolo-lp-range-detail">
  <div class="dolo-lp-detail-label">Lower bound</div>
  <div class="dolo-lp-detail-value">...</div>
</div>
<div class="dolo-lp-detail dolo-lp-range-detail">
  <div class="dolo-lp-detail-label">Upper bound</div>
  <div class="dolo-lp-detail-value">...</div>
</div>
<div class="dolo-lp-detail dolo-lp-range-detail">
  <div class="dolo-lp-detail-label">Tick interval</div>
  <div class="dolo-lp-detail-value">...</div>
</div>
```

Use `compactRangeBound()` for visible bounds and render missing fields as `Unavailable`.

- [ ] **Step 5: Run the focused test and confirm GREEN**

Run: `node --test tests/dolo-liquidity-ui.test.js`

Expected: all tests pass.

- [ ] **Step 6: Commit Task 1**

```bash
git add tests/dolo-liquidity-ui.test.js dolo-preview.html
git commit -m "fix: stabilize DOLO liquidity table geometry"
```

---

### Task 2: Standardize header, controls, Details, and DOLO Flows sorting

**Files:**
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Consumes: existing `.card-head`, `.holder-bucket-mode`, `.dd`, `.dust-pill`, `.flows-tbl`, and Dolomite Assets `.asset-toggle` visual contract.
- Produces: `.dolo-lp-mode-row`, `.dolo-lp-toolbar-primary`, `.dolo-lp-toolbar-secondary`, `.dolo-lp-details-btn` with label and chevron, and shared Assets-style sortable-header rules.

- [ ] **Step 1: Add failing interaction and visual contract tests**

Require the following contracts:

```js
assert.match(html, /class="dolo-lp-head-separator"/);
assert.match(html, /class="dolo-lp-mode-row"/);
assert.match(html, /class="tb-right dolo-lp-toolbar-secondary"/);
assert.match(html, /class="dolo-lp-details-btn"[^>]*><span>\$\{expanded \? "Hide" : "Details"\}<\/span>\$\{CHEV_DOWN_ICO\}/);
assert.match(html, /\.dolo-lp-details-btn\{[^}]*height:24px[^}]*border-radius:999px/s);
assert.match(html, /\.flows-tbl thead th\{[^}]*font-size:10px[^}]*letter-spacing:1\.6px/s);
assert.match(html, /\.dolo-lp-table thead th\{[^}]*font-size:10px[^}]*letter-spacing:1\.6px/s);
```

Verify that `Low-liq pools` is outside `.tb-left`, inside the right group, and that hidden History controls reserve no desktop gap in Active mode.

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `node --test tests/dolo-liquidity-ui.test.js`

Expected: FAIL because the separator, toolbar grouping, Details pill, and exact sort typography are absent.

- [ ] **Step 3: Restructure header and toolbar**

Change the card hierarchy to:

```html
<div class="card-head dolo-lp-head">...</div>
<div class="dolo-lp-head-separator" aria-hidden="true"></div>
<div class="dolo-lp-mode-row">...Active positions / History...</div>
...
<div class="toolbar dolo-lp-toolbar">
  <div class="tb-left dolo-lp-toolbar-primary">...search / Chain / Pairs / DEXes...</div>
  <div class="tb-right dolo-lp-toolbar-secondary">
    <div class="dolo-lp-history-controls" id="dolo-lp-history-controls" hidden>...</div>
    <div class="dust-pill dolo-lp-low-liq" ...>...</div>
  </div>
</div>
```

Keep all existing IDs and event bindings unchanged.

- [ ] **Step 4: Match Dolomite Assets visual controls**

Set both table headers to Inter 10 px, weight 600, 1.6 px tracking, uppercase, with gold active marker. Restyle `.dolo-lp-details-btn` to the 24 px gold pill and render `<span>Details|Hide</span>` plus `CHEV_DOWN_ICO`; rotate its chevron when expanded and preserve a 44 px mobile touch target.

- [ ] **Step 5: Run the focused test and confirm GREEN**

Run: `node --test tests/dolo-liquidity-ui.test.js`

Expected: all tests pass.

- [ ] **Step 6: Commit Task 2**

```bash
git add tests/dolo-liquidity-ui.test.js dolo-preview.html
git commit -m "style: align DOLO liquidity table controls"
```

---

### Task 3: Verify rendering, refresh route caches, and publish

**Files:**
- Modify: `index.html`
- Modify: `dolo/index.html`
- Verify: `dolo-preview.html`
- Verify: `data/dolo-liquidity.json`

**Interfaces:**
- Consumes: the completed static route and existing route-loader cache query.
- Produces: a unique cache key in both production entry points and a deployed GitHub Pages revision.

- [ ] **Step 1: Run source and syntax checks**

Run:

```bash
node --test tests/dolo-liquidity-ui.test.js
node --test tests/dolo-liquidity-workflow.test.js
node --test tests/dolo-flow-protocol-filter.test.js
node --check shared-hover-tooltips.js
git diff --check
```

Expected: all tests and syntax checks pass with no whitespace errors.

- [ ] **Step 2: Verify the exact rendering in a local browser**

Serve with `python3 -m http.server 8000` and inspect `dolo-preview.html` at 1440×900, 1024×768, 768×1024, and 390×844. Assert via computed styles and bounding boxes:

- `table-layout` is `fixed`;
- ordinary rows are equal height and ten slots remain rendered;
- burn-address and `0xe90d…1e95` rows do not change header widths;
- the document has no horizontal overflow;
- only `.tbl-wrap` scrolls on narrow screens;
- Details expands without clipping and its width remains stable;
- Active/History, sorting, filters, dropdowns, and Low-liq all remain operable.

- [ ] **Step 3: Add a failing cache-key contract**

Update the expected version in `tests/dolo-liquidity-ui.test.js` to `dolo-liquidity-table-ux-20260812`, run the focused test, and confirm it fails against both route entry points.

- [ ] **Step 4: Refresh only the two route cache keys**

Replace the old `dolo-preview.html?...` cache query in `index.html` and `dolo/index.html` with `dolo-liquidity-table-ux-20260812` exactly once per file.

- [ ] **Step 5: Run final verification and commit**

Run:

```bash
node --test tests/dolo-liquidity-ui.test.js
node --test tests/dolo-liquidity-workflow.test.js
node --test tests/dolo-flow-protocol-filter.test.js
git diff --check
git status --short
```

Then commit:

```bash
git add tests/dolo-liquidity-ui.test.js index.html dolo/index.html
git commit -m "chore: publish DOLO liquidity table UX"
```

- [ ] **Step 6: Reconcile with production and push live**

Fetch `dolomite-dashboard/master`, rebase the feature commits onto it, rerun the focused checks, then push the reviewed `HEAD` to `master`.

- [ ] **Step 7: Confirm GitHub Pages and live cache**

Wait for the deployment workflow to finish successfully, then open `https://twojekrypto.github.io/Dolomite-dashboard/` and verify the new cache key and the DOLO liquidity table behavior. If the workflow fails, inspect and fix the failure before reporting completion.
