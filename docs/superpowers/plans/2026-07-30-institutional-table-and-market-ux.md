# Institutional Table and Market UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align veDOLO and Borrow tables with DOLO Holders, add Liquidation History sorting, and make Supply market details, icons, and asset selection consistent.

**Architecture:** Keep the existing static-page structure and add narrow final
style overrides plus small pure helpers. The Supply route owns a canonical icon
presentation function that existing renderers consume at runtime; TVL retains
its local resolver but shares the same exact official address/symbol mappings.

**Tech Stack:** Static HTML/CSS/JavaScript, Node test runner, Python unittest,
Playwright through the repository webapp-testing workflow, GitHub Pages.

## Global Constraints

- Preserve Graphite + Gold and existing data semantics.
- Do not add dependencies or reformat the large preview files.
- Use DOLO Holders as the measured visual reference.
- Keep active-only sorting arrows and ignore nested filter-control clicks.
- Push the verified result directly to production `master` as requested.

---

### Task 1: Regression contracts

**Files:**
- Create: `tests/institutional-table-market-ux.test.js`
- Modify: `tests/test_supply_pool_health_contracts.py`

**Interfaces:**
- Consumes: preview HTML/CSS/JS as executable/static dashboard assets
- Produces: observable contracts for sorting, icon presentation, details
  geometry, column alignment, and route asset versions

- [ ] **Step 1: Write failing Node behavior tests**

Add fixtures that call the exported Liquidation History row sorter with
numeric, date, and text values, and call the Supply icon presentation helper
with the exact dplvGLP and SolvBTC.BBN markets. Assert hand-derived row order and
official media filenames.

- [ ] **Step 2: Write failing browser-source contract tests**

Assert that the expanded Supply Health panel has no border/margin/shadow,
claimable header and cell alignment classes agree, Borrow headers expose
sortable fields, and route cache versions change.

- [ ] **Step 3: Run tests to verify RED**

Run:

```bash
node --test tests/institutional-table-market-ux.test.js
python3 -m unittest tests.test_supply_pool_health_contracts
```

Expected: failures for missing exported sort/icon behavior and old inset detail
panel styles.

### Task 2: veDOLO surfaces and alignment

**Files:**
- Modify: `portfolio-preview.html`
- Modify: `portfolio/index.html`
- Modify: `vedolo-preview.html`
- Modify: `vedolo/index.html`

**Interfaces:**
- Consumes: existing `selected-market-rail`, `.tbl`, and route loader
- Produces: continuous header/summary background and aligned claimable columns

- [ ] **Step 1: Match the summary surface**

Give `#pf-exercises-section .pf-exercise-summary` the same quiet tinted
background as its card header while retaining metric dividers and responsive
grids.

- [ ] **Step 2: Align claimable columns**

Use the same header and cell alignment for Route/Position ID (center) and
numeric value/date columns (right), keeping Address left aligned.

- [ ] **Step 3: Bump route versions and run contracts**

Update only the affected route loader version strings and run the focused tests.

### Task 3: Borrow table parity and sorting

**Files:**
- Modify: `liquidation-preview.html`
- Modify: `borrow/index.html`
- Modify: `liquidation/index.html`

**Interfaces:**
- Produces: `sortLiquidationHistoryRows(rows, field, direction)` and
  `syncLiquidationHistorySortHeaders()`
- Consumes: existing history filters, pagination, and filter buttons

- [ ] **Step 1: Implement the pure sorter**

Normalize Chain/Address to lowercase strings, Date to timestamps, and
Collateral/Debt to finite numbers. Apply descending Date and transaction hash
tie-breakers, and export the helper under CommonJS without running the browser
bootstrap.

- [ ] **Step 2: Wire sortable headers**

Add sortable metadata to Chain, Address, Date, Collateral, and Debt. Ignore
events originating from buttons, inputs, or popovers; reset pagination after a
sort change and synchronize active arrow plus `aria-sort`.

- [ ] **Step 3: Apply DOLO Holders visual parity**

Add final route-scoped overrides for title, metadata, headers, body typography,
hover rail, continuous card background, and `var(--bg-2)` footer. Do not change
column count; audit all related `nth-child` rules.

- [ ] **Step 4: Bump route versions and verify**

Run Node syntax, targeted contracts, and a browser click test for every field.

### Task 4: Supply details, canonical icons, and market directory

**Files:**
- Modify: `tvl/supply-health.css`
- Modify: `tvl/supply-health.js`
- Modify: `tvl-preview.html`
- Modify: `tvl/index.html`
- Modify: `supply/supply-draft.css`
- Modify: `supply/supply-draft.js`
- Modify: `liquidation-preview.html`
- Modify: `supply/index.html`

**Interfaces:**
- Produces: `getSupplyMarketIconPresentation(token, chain)` returning
  `{src, frameClass, imageClass}` and `applySupplyMarketIcon(element, token,
  chain)`
- Consumes: exact market address, canonical presentation map, and existing
  Supply renderers

- [ ] **Step 1: Flatten Supply Health details**

Remove the panel margin, border, radius, shadow, and duplicate rail. Let the
detail row own the surface and rail; tighten head/body spacing and use only
section dividers.

- [ ] **Step 2: Add canonical official icon mappings**

Map Arbitrum `0x5c80...b043` to `plvGLP...svg` and Berachain
`0xcc09...9a8c` to `solvBTCbbn...svg`; use exact address before symbol
fallback.

- [ ] **Step 3: Share Supply-route icon presentation**

Expose one runtime helper and invoke it from the selected control, Selected
Market, and Supplier Leaderboard renderers. Preserve image fallback behavior.

- [ ] **Step 4: Redesign asset dropdown**

Render each active market as a 40px-row directory entry with canonical icon,
symbol, descriptive name, market ID, and shortened address. Add a working clear
button to search, active rail/text, keyboard focus, internal scrolling, and
responsive width.

- [ ] **Step 5: Bump route versions and verify**

Run focused tests and browser-check the exact rendered icon sources, dropdown
bounds, search clear behavior, no checkbox, deep-link selection, and details
tray bounds.

### Task 5: Production verification and publish

**Files:**
- Modify: only files listed above

- [ ] **Step 1: Run complete relevant checks**

Run Node syntax checks, all new/focused Node tests, applicable Python contract
tests, and inspect `git diff --check`.

- [ ] **Step 2: Browser visual verification**

Serve with `python3 -m http.server`, inspect the five routes at desktop and
mobile widths, capture screenshots, and compare computed values/bounding boxes
to the DOLO Holders reference.

- [ ] **Step 3: Review scope and commit**

Inspect `git status`, the scoped diff, and `nth-child`/ID references. Stage only
the plan's files and commit with a terse message.

- [ ] **Step 4: Rebase and push production**

Fetch `dolomite-dashboard/master`, rebase without overwriting unrelated
auto-updates, rerun smoke checks, and push `HEAD:master`.

- [ ] **Step 5: Verify workflows and live site**

Watch the triggered GitHub Actions to completion and load versioned production
URLs to confirm the deployed commit and UI behavior.
