# Table Surface and Supply Icon Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the requested veDOLO and Liquidation History table geometry visually consistent while removing duplicated Supply details, unifying SolvBTC artwork, and removing the Supply selector rail.

**Architecture:** Keep the existing static route architecture and make surgical edits in the current page-specific CSS/JS. Encode each approved visual contract in executable source tests first, then verify browser-computed styles and geometry through the local HTTP route loader.

**Tech Stack:** Static HTML/CSS/JavaScript, Node.js test runner, Python `unittest`, local `python3 -m http.server`, browser automation.

## Global Constraints

- Preserve the existing Graphite + Gold identity and existing analytics behavior.
- Use one continuous `var(--bg-2)` surface for veDOLO Position Activity; only the table column-header row uses `var(--bg-1)`.
- Use Dolomite Assets as the source of truth for SolvBTC artwork.
- Do not introduce new dependencies or broad component refactors.
- Audit all affected table-column selectors and verify computed values and bounding boxes.
- Deploy the final rebased result to production `master`.

---

### Task 1: Lock the table-surface and column-geometry contracts

**Files:**
- Modify: `tests/test_institutional_table_market_ux_contracts.py`
- Modify: `tests/table-chain-ux-contract.test.js`
- Modify: `portfolio-preview.html`
- Modify: `liquidation-preview.html`

**Interfaces:**
- Consumes: Existing `#pf-exercises-section` zones and five-column `#liquidation-history-table`.
- Produces: One flat veDOLO card surface and Liquidation History widths `11% / 15.8% / 39% / 18.2% / 16%`.

- [ ] **Step 1: Write failing source-contract tests**

```python
def test_vedolo_activity_uses_one_continuous_surface(self):
    section = first_rule(PORTFOLIO, "#pf-exercises-section{")
    self.assertIn("background:var(--bg-2)", section)
    for selector in (
        "#pf-exercises-section .card-head",
        "#pf-exercises-section .pf-exercise-summary.selected-market-rail",
        "#pf-exercises-section .pf-filters",
        "#pf-exercises-section .tbl-foot",
    ):
        rule = first_rule(PORTFOLIO, selector)
        self.assertIn("background:transparent", rule)
        self.assertNotIn("linear-gradient", rule)
    thead = first_rule(PORTFOLIO, "#pf-exercises-section .pf-table thead th")
    self.assertIn("background:var(--bg-1)", thead)

def test_liquidation_history_uses_lending_position_column_rhythm(self):
    self.assertIn(
        "#liquidation-history-table colgroup col:nth-child(1) { width: 11% !important; }",
        BORROW,
    )
    self.assertIn(
        "#liquidation-history-table colgroup col:nth-child(2) { width: 15.8% !important; }",
        BORROW,
    )
```

Update the existing Node source assertions to the same five-column widths and
remove the obsolete six-column spacer expectations.

- [ ] **Step 2: Run the focused tests and verify the new assertions fail**

Run:

```bash
python3 -m unittest tests.test_institutional_table_market_ux_contracts -v
node --test tests/table-chain-ux-contract.test.js
```

Expected: FAIL on the old veDOLO gradients and old `7.8% / 19%` Liquidation History widths. Any unrelated pre-existing assertion failure must be recorded separately rather than weakened.

- [ ] **Step 3: Implement the minimal CSS changes**

In `portfolio-preview.html`, set the section shell to `background:var(--bg-2)`,
remove its internal green gradients, keep transparent backgrounds for the card
head, summary, filters, table body, and footer, and set the exercise table head
to `var(--bg-1)`.

In `liquidation-preview.html`, use:

```css
body.route-liquidation #liquidation-history-table colgroup col:nth-child(1) { width: 11% !important; }
body.route-liquidation #liquidation-history-table colgroup col:nth-child(2) { width: 15.8% !important; }
body.route-liquidation #liquidation-history-table colgroup col:nth-child(3) { width: 39% !important; }
body.route-liquidation #liquidation-history-table colgroup col:nth-child(4) { width: 18.2% !important; }
body.route-liquidation #liquidation-history-table colgroup col:nth-child(5) { width: 16% !important; }
```

Do not add transforms or spacer columns.

- [ ] **Step 4: Run the focused tests and verify they pass**

Run the commands from Step 2. Expected: all assertions owned by this task PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_institutional_table_market_ux_contracts.py tests/table-chain-ux-contract.test.js portfolio-preview.html liquidation-preview.html
git commit -m "Polish veDOLO surface and liquidation columns"
```

### Task 2: Remove duplicated Supply Pool Health detail identity

**Files:**
- Modify: `tests/supply-pool-health.test.js`
- Modify: `tests/test_supply_pool_health_contracts.py`
- Modify: `tvl/supply-health.js`
- Modify: `tvl/supply-health.css`

**Interfaces:**
- Consumes: `renderSupplyHealthDetail(market)`.
- Produces: Detail markup containing only the three analytical sections and the Supply market action.

- [ ] **Step 1: Write the failing detail contract**

```javascript
assert.doesNotMatch(detail, /Market intelligence/);
assert.doesNotMatch(detail, /supply-health-detail-head/);
assert.doesNotMatch(detail, /supply-health-detail-head-metric/);
assert.match(detail, /Quality anatomy/);
assert.match(detail, /Market momentum/);
assert.match(detail, /Supply concentration/);
```

Add a Python source assertion that the removed header selectors are absent from
the CSS while `.supply-health-detail-content` remains edge-to-edge.

- [ ] **Step 2: Run tests and verify failure**

```bash
node --test tests/supply-pool-health.test.js
python3 -m unittest tests.test_supply_pool_health_contracts -v
```

Expected: FAIL because the detail header and its CSS still exist.

- [ ] **Step 3: Remove the duplicated header and rebalance spacing**

Delete `.supply-health-detail-head` markup from `renderSupplyHealthDetail`.
Remove the now-dead detail identity/head metric CSS and its responsive
overrides. Let `.supply-health-detail-content` begin immediately inside the
expanded row with even section padding.

- [ ] **Step 4: Run tests and verify they pass**

Run the commands from Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/supply-pool-health.test.js tests/test_supply_pool_health_contracts.py tvl/supply-health.js tvl/supply-health.css
git commit -m "Remove duplicated Supply Health detail header"
```

### Task 3: Make SolvBTC artwork canonical across Supply surfaces

**Files:**
- Modify: `tests/institutional-table-market-ux.test.js`
- Modify: `tests/token-icon-presentation-contract.test.js`
- Modify: `supply/supply-draft.js`
- Modify: `tvl-preview.html`

**Interfaces:**
- Consumes: `getSupplyMarketIconPresentation(token, chain)` and TVL `tokenIcon(symbol, row)`.
- Produces: The Dolomite Assets SolvBTC icon URL `https://app.dolomite.io/static/media/solvBTC.326d594ebd54e4317f078b70f72a58b4.svg` for the Berachain SolvBTC market in Supply Health, Selected Market, Supplier Leaderboard, and selector rows.

- [ ] **Step 1: Change the tests to require Assets-parity artwork**

```javascript
const OFFICIAL_SOLVBTC_ICON =
  'https://app.dolomite.io/static/media/solvBTC.326d594ebd54e4317f078b70f72a58b4.svg';

assert.equal(
  supplyUi.getSupplyMarketIconPresentation({
    id: '0xcc0966d8418d412c599a6421b760a847eb169a8c',
    symbol: 'SolvBTC.BBN',
  }, 'berachain').src,
  OFFICIAL_SOLVBTC_ICON,
);
assert.equal(
  tvlTokenIcon('SolvBTC.BBN', {
    chain: 'berachain',
    addr: '0xcc0966d8418d412c599a6421b760a847eb169a8c',
  }),
  OFFICIAL_SOLVBTC_ICON,
);
```

- [ ] **Step 2: Run tests and verify failure**

```bash
node --test tests/institutional-table-market-ux.test.js tests/token-icon-presentation-contract.test.js
```

Expected: FAIL because both current overrides use the distinct
`solvBTCbbn.*.svg` asset.

- [ ] **Step 3: Correct the canonical address-first mappings**

Replace the BBN-specific image constant and the corresponding address/symbol
overrides in `supply/supply-draft.js` and `tvl-preview.html` with the exact
Assets SolvBTC image URL. Keep the address-first resolution order so every
Supply identity surface receives the same result.

- [ ] **Step 4: Run tests and verify they pass**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/institutional-table-market-ux.test.js tests/token-icon-presentation-contract.test.js supply/supply-draft.js tvl-preview.html
git commit -m "Unify SolvBTC artwork across Supply views"
```

### Task 4: Remove the Supply selector’s gold left rail

**Files:**
- Modify: `tests/test_institutional_table_market_ux_contracts.py`
- Modify: `tests/test_supply_table_ux_contracts.py`
- Modify: `supply/supply-draft.css`

**Interfaces:**
- Consumes: `.supply-draft-market-option.active`.
- Produces: Selection feedback via background and `Selected` label only; no `::before` rail.

- [ ] **Step 1: Write the failing rail-removal contract**

```python
self.assertNotIn(
    "#asset-options-container .supply-draft-market-option.active::before",
    styles,
)
self.assertIn(
    "#asset-options-container .supply-draft-market-option.active {",
    styles,
)
self.assertIn(".supply-draft-option-selected", styles)
```

- [ ] **Step 2: Run tests and verify failure**

```bash
python3 -m unittest tests.test_institutional_table_market_ux_contracts tests.test_supply_table_ux_contracts -v
```

Expected: FAIL because the active `::before` rule still paints the gold rail.

- [ ] **Step 3: Remove only the rail rules**

Delete the `.supply-draft-market-option::before` and
`.supply-draft-market-option.active::before` blocks from
`supply/supply-draft.css`. Preserve the active background wash and selected
label.

- [ ] **Step 4: Run tests and verify they pass**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_institutional_table_market_ux_contracts.py tests/test_supply_table_ux_contracts.py supply/supply-draft.css
git commit -m "Remove Supply market selector rail"
```

### Task 5: Browser verification, integration checks, and production deployment

**Files:**
- Modify only if verification exposes a scoped defect: files from Tasks 1–4.

**Interfaces:**
- Consumes: Local route-loader pages `/portfolio/`, `/borrow/`, `/tvl/`, `/supply/`, and `/assets/`.
- Produces: Verified, rebased production commit on `master`.

- [ ] **Step 1: Run the focused integration suites**

```bash
node --test tests/institutional-table-market-ux.test.js tests/supply-pool-health.test.js tests/supply-ui-behavior.test.js tests/token-icon-presentation-contract.test.js
python3 -m unittest tests.test_institutional_table_market_ux_contracts tests.test_portfolio_preview_contracts tests.test_supply_pool_health_contracts tests.test_supply_table_ux_contracts -v
git diff --check
```

Expected: all task-owned tests PASS. Record any unrelated pre-existing failure
with its exact assertion.

- [ ] **Step 2: Serve the static dashboard and verify browser-computed UX**

Run:

```bash
python3 -m http.server 8765
```

In the browser:

- Load a portfolio wallet that renders veDOLO Position Activity.
- Assert computed `backgroundColor`/`backgroundImage` for the card head,
  summary, filters, body, and footer form one surface; only `thead th` is
  darker.
- On `/borrow/`, compare Chain, Liquidated wallet, and Date cell bounding boxes;
  require positive gaps and no overlap at desktop and mobile table widths.
- On `/tvl/` and `/supply/`, compare the final absolute `src` of the SolvBTC
  image with `/assets/`.
- Open Supply Pool Health details and confirm the first visible content is the
  analytical section grid.
- Open the Supply selector and require
  `getComputedStyle(activeOption, "::before").content === "none"`.

- [ ] **Step 3: Rebase onto current production**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
```

Resolve only scoped conflicts, preserve generated data commits, then repeat
Step 1.

- [ ] **Step 4: Push production and verify Pages**

```bash
git push dolomite-dashboard HEAD:master
```

Verify the resulting GitHub Pages workflow reaches success and confirm the
production pages return the pushed commit’s updated assets.
