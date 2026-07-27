# TVL Supply Pool Health Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Supply Pool Health to TVL with asset/network discovery controls and restyle Asset Activity as a four-metric Selected Market rail.

**Architecture:** Extract Supply Pool Health into TVL-owned CSS and JavaScript loaded by `tvl-preview.html`, while preserving the existing generated JSON contract. Remove the Supply-route mount and styles, then simplify its existing activity summary renderer without changing activity calculations.

**Tech Stack:** Static HTML, CSS, browser JavaScript, Python `unittest`, GitHub Pages.

## Global Constraints

- Keep the current `data/supply-health/latest.json` source and audit classifications unchanged.
- Keep a stable ten-row Supply Pool Health viewport after filtering.
- Default the Supply Pool Health network filter to all networks and keep it independent from Token Composition.
- Do not add dependencies or reformat unrelated parts of the large preview files.
- Preserve the Graphite + Gold visual identity.

---

### Task 1: Move Supply Pool Health to TVL

**Files:**
- Create: `tvl/supply-health.css`
- Create: `tvl/supply-health.js`
- Modify: `tvl-preview.html`
- Modify: `supply/supply-draft.js`
- Modify: `supply/supply-draft.css`
- Modify: `PROJECT_STATE.md`
- Test: `tests/test_supply_pool_health_contracts.py`

**Interfaces:**
- Consumes: `data/supply-health/latest.json` with top-level `generatedAt` and `markets[]`.
- Produces: `#supply-health-card`, `renderSupplyHealthTable()`, and `window.supplyHealthGoPage(page)`.

- [ ] **Step 1: Update the contract test to require TVL ownership**

```python
def test_supply_health_is_owned_by_tvl(self):
    html = TVL_HTML.read_text()
    script = TVL_SCRIPT.read_text()
    supply_script = SUPPLY_SCRIPT.read_text()
    self.assertIn('id="supply-health-card"', html)
    self.assertIn("function renderSupplyHealthTable", script)
    self.assertNotIn("installSupplyHealthCard();", supply_script)
```

- [ ] **Step 2: Run the focused test and confirm it fails**

Run: `python3 -m unittest tests.test_supply_pool_health_contracts`

Expected: failure because the card and renderer are still Supply-owned.

- [ ] **Step 3: Add the TVL card immediately after Token Composition**

Add a `section#supply-health-card` with a header, freshness text, an asset
search with a clear button, an independent chain multi-select, the eight-column
table, pager, and footnote. Load the new stylesheet in `<head>` and the new
script after the page's existing inline script.

```html
<link rel="stylesheet" href="tvl/supply-health.css?v=20260727">
...
<input id="supply-health-search" type="search" placeholder="Search asset">
<button id="supply-health-search-clear" type="button" aria-label="Clear asset search">×</button>
```

- [ ] **Step 4: Implement filter, sorting, pagination, and detail rendering**

Use route-local state and reset page/expansion on filter changes:

```javascript
const supplyHealthState = {
  payload: null,
  query: '',
  chains: new Set(),
  sortField: 'supplyUsd',
  sortAsc: false,
  expandedKey: '',
  page: 1,
};

function getFilteredSupplyHealthMarkets() {
  const query = supplyHealthState.query.trim().toLowerCase();
  return supplyHealthState.payload.markets.filter(market => {
    const matchesChain = supplyHealthState.chains.size === 0
      || supplyHealthState.chains.has(market.chain);
    const haystack = [market.symbol, market.name, market.tokenId, market.chain]
      .join(' ').toLowerCase();
    return matchesChain && (!query || haystack.includes(query));
  });
}
```

Render spacer rows until ten visible rows, dispatch an `input` event from the
search clear control, and keep focus on the search field.

- [ ] **Step 5: Move the existing health visual rules into TVL ownership**

Scope new styles to `#supply-health-card`. Set primary table cells to
`var(--fg-1)`, keep secondary labels muted, and reuse `.tvl-dd*` control
language for the network selector. Preserve the gold hover/expanded rail and
responsive horizontal table scrolling.

- [ ] **Step 6: Remove the Supply-route mount, renderer, and route-only styles**

Remove the Supply Pool Health state/functions and the two boot calls from
`supply/supply-draft.js`; remove the matching card CSS block from
`supply/supply-draft.css`. Update `PROJECT_STATE.md` to state that the card is
on TVL.

- [ ] **Step 7: Run the focused contract test**

Run: `python3 -m unittest tests.test_supply_pool_health_contracts`

Expected: all tests pass.

- [ ] **Step 8: Commit the TVL move**

```bash
git add tvl/supply-health.css tvl/supply-health.js tvl-preview.html \
  supply/supply-draft.js supply/supply-draft.css \
  tests/test_supply_pool_health_contracts.py PROJECT_STATE.md
git commit -m "feat: move supply pool health to TVL"
```

### Task 2: Restyle Asset Activity

**Files:**
- Modify: `supply/supply-draft.js`
- Modify: `supply/supply-draft.css`
- Test: `tests/test_supply_pool_health_contracts.py`

**Interfaces:**
- Consumes: `summarizeSupplyActivityRows(rows, cutoffTs)`.
- Produces: exactly four `.supply-activity-stat` children in `#supply-activity-stats`.

- [ ] **Step 1: Add a failing Asset Activity contract**

```python
def test_asset_activity_uses_four_metric_rail(self):
    source = SUPPLY_SCRIPT.read_text()
    css = SUPPLY_CSS.read_text()
    self.assertNotIn("label: `Net Flow · ${meta.short}`", source)
    self.assertIn("grid-template-columns: repeat(4, minmax(0, 1fr))", css)
```

- [ ] **Step 2: Run the focused test and confirm it fails**

Run: `python3 -m unittest tests.test_supply_pool_health_contracts`

Expected: failure because Net Flow and the five-column grid still exist.

- [ ] **Step 3: Remove the Net Flow cell and unused presentation code**

Keep the period cutoff and summary calculations. Render only Deposits,
Withdrawals, Transfers, and Active Wallets:

```javascript
const cells = [
  { label: 'Deposits', value: supplyDraftFormatUsd(summary.inflowUsd), cls: 'deposit' },
  { label: 'Withdrawals', value: supplyDraftFormatUsd(summary.outflowUsd), cls: 'withdraw' },
  { label: 'Transfers', value: supplyDraftFormatUsd(summary.internalUsd), cls: 'transfer' },
  { label: 'Active Wallets', value: Number(summary.wallets || 0).toLocaleString('en-US'), cls: '' },
];
```

- [ ] **Step 4: Match the Selected Market metric rail**

Use four equal transparent columns, vertical separators, compact uppercase
labels, prominent values, and balanced padding. Preserve the existing semantic
deposit/withdraw/transfer value colors.

- [ ] **Step 5: Run the focused test**

Run: `python3 -m unittest tests.test_supply_pool_health_contracts`

Expected: all tests pass.

- [ ] **Step 6: Commit the activity summary**

```bash
git add supply/supply-draft.js supply/supply-draft.css \
  tests/test_supply_pool_health_contracts.py
git commit -m "style: refine supply asset activity metrics"
```

### Task 3: Browser Verification and Production Deployment

**Files:**
- Verify only: `tvl-preview.html`, `tvl/supply-health.js`, `tvl/supply-health.css`, `supply/supply-draft.js`, `supply/supply-draft.css`

**Interfaces:**
- Consumes: local routes `/tvl/` and `/supply/`.
- Produces: verified production commit on `dolomite-dashboard/master`.

- [ ] **Step 1: Run static and project checks**

```bash
node --check tvl/supply-health.js
node --check supply/supply-draft.js
python3 -m unittest tests.test_supply_pool_health_contracts
python3 run_earn_audit_checks.py
git diff --check
```

- [ ] **Step 2: Start the required local server**

Run: `python3 -m http.server 8765`

Expected: local server listens on port `8765`.

- [ ] **Step 3: Verify TVL in a real browser**

Open `http://127.0.0.1:8765/tvl/` and verify:

- Supply Pool Health is below Token Composition.
- Primary body cells compute to the white foreground token.
- Search and clear work and preserve the ten-row viewport height.
- The independent network selector filters rows and resets correctly.
- Sorting, pagination, keyboard expansion, and details work.
- Browser console contains no new errors.

- [ ] **Step 4: Verify Supply in a real browser**

Open `http://127.0.0.1:8765/supply/`, select a market, and verify:

- Asset Activity has four metrics and no Net Flow.
- The metric rail and Selected Market rail have matching column count,
  transparent background, divider rhythm, and value hierarchy.
- Browser console contains no new errors.

- [ ] **Step 5: Rebase safely and rerun the focused checks**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
python3 -m unittest tests.test_supply_pool_health_contracts
node --check tvl/supply-health.js
git diff --check
```

- [ ] **Step 6: Push production and verify GitHub Pages**

```bash
git push dolomite-dashboard HEAD:master
```

Wait for the Pages workflow to finish, then open
`https://twojekrypto.github.io/Dolomite-dashboard/tvl/?v=<commit>` and confirm
the card and controls are served from the pushed commit.
