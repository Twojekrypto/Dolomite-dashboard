# Supply Table UX Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align Supply Pool Health, Borrow chain filtering, Supply footers, Asset Activity, and DOLO/veDOLO summaries with their existing dashboard UX references.

**Architecture:** Keep the existing static-page architecture. Put TVL-specific control behavior in `tvl/supply-health.js` and `tvl/supply-health.css`; layer Supply styling and footer normalization in `supply/supply-draft.js` and `.css`; make only surgical source edits to the shared Borrow, DOLO, and veDOLO preview pages. Contract tests assert the source-level hooks that prevent the specific regressions requested.

**Tech Stack:** Static HTML, vanilla JavaScript, CSS, Node test runner, Python `unittest`, Playwright via local HTTP server.

## Global Constraints

- Do not change market calculations, generated data, RPC calls, or GitHub Action workflows.
- Preserve Graphite + Gold tokens, existing keyboard behavior, and mobile layouts.
- Do not add dependencies or secrets.
- The production branch is `master`; deploy the final implementation with `HEAD:master`.
- Do not reformat large preview files outside the exact requested markup, styling, and state-copy edits.

---

### Task 1: Lock the requested UX contracts with focused tests

**Files:**
- Create: `tests/test_supply_table_ux_contracts.py`
- Modify: `tests/supply-pool-health.test.js`

**Interfaces:**
- Consumes: static source in `tvl-preview.html`, `tvl/supply-health.js`, `liquidation-preview.html`, `supply/supply-draft.js`, `supply/supply-draft.css`, `dolo-preview.html`, and `portfolio-preview.html`.
- Produces: regression checks for filter placement/options, footer summaries, loading copy, and summary-rail hooks.

- [x] **Step 1: Write the failing source-contract tests**

```python
def test_lending_filter_omits_retired_network_options():
    lending_menu = section_after(LIQUIDATION, 'id="chain-menu"')
    self.assertNotIn('polygon_zkevm', lending_menu)
    self.assertNotIn('Botanix', lending_menu)

def test_supply_footers_have_left_range_and_centered_controls():
    self.assertIn('supply-page-range', SUPPLY_JS)
    self.assertIn('supply-pager-controls', SUPPLY_JS)

def test_activity_loading_copy_replaces_ready_copy():
    self.assertNotIn("'30D ready'", LIQUIDATION)
    self.assertIn('Loading latest 30D activity', LIQUIDATION)
```

```js
test('Supply Health control group keeps search and network adjacent', () => {
  assert.match(source, /supply-health-toolbar[\s\S]*supply-health-search-shell[\s\S]*supply-health-chain-dropdown/);
});
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `node --test tests/supply-pool-health.test.js && python3 -m unittest tests.test_supply_table_ux_contracts`

Expected: FAIL because the retired options, footer markup, and loading copy still exist.

- [x] **Step 3: Commit only after all implementation tasks pass**

Do not commit an intentionally failing test. It will be included in the implementation commit after Tasks 2–4.

### Task 2: Align Supply Pool Health and Lending Positions network controls

**Files:**
- Modify: `tvl-preview.html:900-922`
- Modify: `tvl/supply-health.js:408-529`
- Modify: `tvl/supply-health.css:41-160,665-681`
- Modify: `liquidation-preview.html:10382-10410,14060-14375`

**Interfaces:**
- Consumes: `state.filters.chains` / `healthChains` in Supply Health, and `chainFilterSelection` in Lending Positions.
- Produces: multi-select menus with the Assets visual language; Lending Positions only advertises active-chain choices.

- [x] **Step 1: Implement the minimal TVL markup/style alignment**

Keep search followed immediately by the dropdown inside `.supply-health-toolbar`. Give the dropdown the Assets menu vocabulary without changing its IDs:

```html
<div class="tvl-dd supply-health-chain-dropdown" id="supply-health-chain-dropdown">
  <button class="tvl-dd-btn" id="supply-health-chain-filter" aria-expanded="false">
    <svg class="icon" aria-hidden="true"></svg>
    <span class="lbl" id="supply-health-chain-label">All Chains</span>
    <span class="count" id="supply-health-chain-count">5/5</span>
    <span class="dd-clear" id="supply-health-chain-clear" role="button" aria-label="Reset network filter"></span>
    <svg class="chev" aria-hidden="true"></svg>
  </button>
  <div class="tvl-dd-panel" id="supply-health-chain-panel">
    <div class="tvl-dd-panel-head">Chain</div>
    <div class="tvl-dd-list" id="supply-health-chain-list"></div>
  </div>
</div>
```

Use `.tvl-dd-opt.select-all`, `.dd-opt-check`, chain icons, and `.active` states matching `assets-preview.html`; retain mobile single-column overrides.

- [x] **Step 2: Implement filter-state compatibility and retired-chain removal**

In `renderSupplyHealthChainFilter`, render the explicit all-chain row before individual active chains. In Lending Positions, remove the retired `base` (Botanix) and `polygon_zkevm` menu options and sanitize the selected set before calling `setChainTriggerState()`:

```js
const ACTIVE_LENDING_CHAIN_KEYS = new Set(['ethereum', 'berachain', 'arbitrum', 'mantle', 'xlayer']);
chainFilterSelection = new Set([...chainFilterSelection].filter(key => ACTIVE_LENDING_CHAIN_KEYS.has(key)));
```

Do not remove these chains from historical position data or generic chain labels.

- [x] **Step 3: Run targeted tests**

Run: `node --test tests/supply-pool-health.test.js && python3 -m unittest tests.test_supply_pool_health_contracts tests.test_supply_table_ux_contracts`

Expected: PASS.

### Task 3: Normalize Supplier Leaderboard and Asset Activity presentation

**Files:**
- Modify: `liquidation-preview.html:12212-12319,16460-16480,17520-17534,18939-19045`
- Modify: `supply/supply-draft.js:810-1059`
- Modify: `supply/supply-draft.css:1311-1486,2349-2422,2664-2860`

**Interfaces:**
- Consumes: `supplyPage`, `supplyActivityPage`, filtered row counts, `currentSupplyActivity`, and existing period dropdown IDs.
- Produces: a shared three-column footer (`supply-page-range`, `supply-pager-controls`, `flow-pager-total`) and a right-aligned period filter.

- [x] **Step 1: Add a reusable footer renderer in the Supply draft layer**

Produce stable footer markup for one and many pages:

```js
function renderSupplyFooter(page, totalPages, visibleRows, totalRows, goPage) {
  const start = totalRows ? ((page - 1) * 10) + 1 : 0;
  const end = Math.min(page * 10, totalRows);
  return `<span class="supply-page-range">${start}–${end} of ${totalRows}</span>` +
    `<span class="supply-pager-controls"><button class="flow-pager-btn" onclick="${goPage}(${page - 1})">‹</button><span class="flow-pager-info">${page} / ${totalPages}</span><button class="flow-pager-btn" onclick="${goPage}(${page + 1})">›</button></span>` +
    `<span class="flow-pager-total">${totalRows} ${visibleRows === totalRows ? 'results' : 'matching'}</span>`;
}
```

Patch the existing global renderers rather than duplicating data filtering; preserve disabled button behavior and page clamping.

- [x] **Step 2: Move the period control and replace readiness copy**

Place `#supply-activity-period-filter` in the `.supply-activity-toolbar-actions` cluster after the history controls so CSS can align it right. Change loading-state text to `Loading latest 30D activity…`, mark the state container `aria-live="polite"`, and clear or replace it with freshness metadata after loading; never render `30D ready`.

- [x] **Step 3: Apply the continuous Activity surface**

Use the outer `#supply-activity-card` as the single graphite surface. Set nested summary/table/footer backgrounds transparent, keep only one border transition between header, rows, and footer, and retain the current row hover and transaction-type colors. The period control stays right-aligned on desktop and becomes full-width below 720px.

- [x] **Step 4: Run targeted tests and a syntax check**

Run: `node --check supply/supply-draft.js && python3 -m unittest tests.test_supply_activity_ui_contracts tests.test_supply_table_ux_contracts`

Expected: PASS.

### Task 4: Restyle Fresh Wallet and veDOLO Position Activity summaries

**Files:**
- Modify: `dolo-preview.html:509-518,4790-4800`
- Modify: `portfolio-preview.html:1911-1920,3440-3475` and the CSS block that owns `.pf-exercise-summary`

**Interfaces:**
- Consumes: existing calculated values and `renderFreshStats` / `renderExerciseSummary` functions.
- Produces: metric rails with `.selected-market`-style label/value/subvalue hierarchy; no metric data changes.

- [x] **Step 1: Use the Selected Market metric hierarchy in both summary renderers**

Each metric must follow the same visual order:

```html
<div class="fresh-stat">
  <div class="label">Metric label</div>
  <div class="value">Primary value</div>
  <div class="sub">One explanatory line</div>
</div>
```

Keep the existing Fresh Wallet values (`Fresh Wallets`, `DOLO Received`, `Current Exposure`, `Retention`) and existing Portfolio veDOLO Position Activity measures. Use subtle desktop dividers and a one-column mobile stack.

- [x] **Step 2: Add source-contract coverage**

Extend `tests/test_supply_table_ux_contracts.py` to assert each summary container has the metric-rail classes and the existing render functions retain their values.

- [x] **Step 3: Run targeted tests**

Run: `python3 -m unittest tests.test_dolo_preview_contracts tests.test_vedolo_preview_contracts tests.test_supply_table_ux_contracts`

Expected: PASS.

### Task 5: Browser verification, review, and deployment

**Files:**
- Modify: cache-version references only when needed for changed static assets.

**Interfaces:**
- Consumes: the completed static UI and local `python3 -m http.server`.
- Produces: verified desktop/mobile behavior and a production deployment on `master`.

- [x] **Step 1: Run full checks**

Run: `python3 run_earn_audit_checks.py` and `git diff --check`.

Expected: the audit passes and diff check is silent.

- [x] **Step 2: Browser-verify exact interactions**

Start `python3 -m http.server 8765`. Verify with computed styles and bounding boxes:

1. TVL search and chain trigger are adjacent on desktop; menu supports multi-select, reset, outside click, Escape, and remains one-column on mobile.
2. Lending Positions menu lacks Polygon zkEVM and Botanix.
3. Supplier Leaderboard and Asset Activity footers show the range at left and centered pager.
4. Asset Activity period control is right aligned; loading exposes the new copy; the card remains one continuous surface.
5. Fresh Wallet and veDOLO summary rails use the requested hierarchy on desktop and stack cleanly on mobile.

- [x] **Step 3: Review and commit**

Run a correctness/regression review and a maintainability/security review. Stage only requested UI, test, version, and documentation files. Commit with `feat: align supply tables with dashboard UX`.

- [ ] **Step 4: Push and verify production**

Push `HEAD:master`, wait for Deploy GitHub Pages plus related checks, then open cache-busted live TVL, Supply, Borrow, DOLO, and veDOLO routes to verify the deployed assets.
