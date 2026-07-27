# Data Freshness and Surface Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add trustworthy Supply freshness metadata and align the requested filter checkers, card surfaces, and veDOLO units with the dashboard's established UX.

**Architecture:** Keep the static dashboard architecture intact. Extend the existing Supply GraphQL bundle with `_meta.block.timestamp`, store that snapshot time in the existing Supply page state, and render it through one small shared freshness updater. Make all visual changes through narrowly scoped existing CSS and markup rather than introducing dependencies or restructuring the large preview pages.

**Tech Stack:** Static HTML, CSS, vanilla JavaScript, Python `unittest` source contracts, Node syntax/tests, GitHub Pages.

## Global Constraints

- Keep all metric calculations and source data unchanged.
- Use the Supply subgraph bundle block timestamp for Selected Market, Supplier Leaderboard, and Asset Activity.
- Show `Data updating` when an authoritative timestamp is unavailable.
- Match the DOLO Holders metadata and `var(--bg-2)` card-surface patterns.
- Do not add dependencies or change configuration, workflows, generated datasets, RPC behavior, or classification rules.
- Preserve existing keyboard, responsive, loading, hover, and filtering behavior.

---

### Task 1: Add failing UX regression contracts

**Files:**
- Create: `tests/test_data_freshness_surface_contracts.py`

**Interfaces:**
- Consumes: existing static markup and CSS class/ID contracts.
- Produces: regression checks for the implementation tasks below.

- [ ] **Step 1: Write the failing tests**

```python
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TVL_STYLES = (ROOT / "tvl" / "supply-health.css").read_text(encoding="utf-8")
SUPPLY_VIEW = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
SUPPLY_STYLES = (ROOT / "supply" / "supply-draft.css").read_text(encoding="utf-8")
DOLO_VIEW = (ROOT / "dolo-preview.html").read_text(encoding="utf-8")
PORTFOLIO_VIEW = (ROOT / "portfolio-preview.html").read_text(encoding="utf-8")


class DataFreshnessSurfaceContractsTest(unittest.TestCase):
    def test_supply_health_selected_network_has_assets_style_checker(self):
        self.assertIn(".supply-health-chain-dropdown .dd-opt-check {", TVL_STYLES)
        self.assertIn("width: 15px", TVL_STYLES)
        self.assertIn(".tvl-dd-opt.active .dd-opt-check", TVL_STYLES)
        self.assertIn(".tvl-dd-opt.active .dd-opt-check svg", TVL_STYLES)

    def test_supply_cards_share_authoritative_data_timestamp(self):
        self.assertIn("_meta { block { timestamp } }", SUPPLY_VIEW)
        self.assertIn('id="supply-intel-asof"', SUPPLY_VIEW)
        self.assertIn('id="supply-leaderboard-data-updated"', SUPPLY_VIEW)
        self.assertIn('id="supply-activity-data-updated"', SUPPLY_VIEW)
        self.assertIn("function renderSupplyDataFreshness", SUPPLY_VIEW)
        self.assertIn("Data updated ·", SUPPLY_VIEW)
        self.assertIn("Data updating", SUPPLY_VIEW)

    def test_asset_activity_uses_solid_holder_surface(self):
        self.assertIn("background: var(--bg-2, #141417) !important", SUPPLY_STYLES)

    def test_fresh_wallets_use_solid_holder_surface(self):
        self.assertIn(".fresh-wallets-card{", DOLO_VIEW)
        fresh_block = DOLO_VIEW.split(".fresh-wallets-card{", 1)[1].split("}", 1)[0]
        self.assertIn("background:var(--bg-2)", fresh_block)
        self.assertNotIn("linear-gradient", fresh_block)

    def test_position_activity_uses_consistent_surface_and_correct_units(self):
        rail = PORTFOLIO_VIEW.split(
            "#pf-exercises-section .pf-exercise-summary.selected-market-rail{", 1
        )[1].split("}", 1)[0]
        self.assertIn("background:var(--bg-2)", rail)
        self.assertIn('fmtCompact(lockedVe)} <span class="unit">DOLO</span>', PORTFOLIO_VIEW)
        self.assertNotIn('fmtCompact(claimVe)} <span class="unit">veDOLO</span>', PORTFOLIO_VIEW)
        self.assertNotIn('fmtCompact(currentVote)} <span class="unit">veDOLO</span>', PORTFOLIO_VIEW)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the contracts and confirm the red state**

Run:

```bash
python3 -m unittest tests.test_data_freshness_surface_contracts -v
```

Expected: failures for the missing checker styles, missing Supply metadata/timestamp updater, gradient surfaces, and old veDOLO units.

- [ ] **Step 3: Commit the failing regression contracts**

```bash
git add tests/test_data_freshness_surface_contracts.py
git commit -m "test: cover freshness and surface UX"
```

---

### Task 2: Restore the Supply Pool Health selected-network checker

**Files:**
- Modify: `tvl/supply-health.css`
- Modify: `tvl-preview.html`
- Modify: `tvl/index.html`
- Test: `tests/test_data_freshness_surface_contracts.py`
- Test: `tests/test_supply_pool_health_contracts.py`

**Interfaces:**
- Consumes: `.tvl-dd-opt.active` and `.dd-opt-check` markup already emitted by `renderSupplyHealthChainOptions()`.
- Produces: a 15×15 gold selected checkbox with visible check icon.

- [ ] **Step 1: Add scoped Assets-style checkbox rules**

Add rules under `#supply-health-card .supply-health-chain-dropdown` for the checker size, radius, border, background, grid alignment, SVG dimensions/opacity, and the active gold background/border/icon state. Keep the existing option order rule.

- [ ] **Step 2: Bump TVL cache keys**

Increment the query suffix for `tvl/supply-health.css`, `tvl/supply-health.js`, and the TVL route loader so the new styles cannot be hidden by GitHub Pages/browser cache.

- [ ] **Step 3: Run focused tests**

Run:

```bash
python3 -m unittest \
  tests.test_data_freshness_surface_contracts.DataFreshnessSurfaceContractsTest.test_supply_health_selected_network_has_assets_style_checker \
  tests.test_supply_pool_health_contracts -v
node --check tvl/supply-health.js
```

Expected: PASS.

- [ ] **Step 4: Commit the checker fix**

```bash
git add tvl/supply-health.css tvl-preview.html tvl/index.html
git commit -m "fix: restore supply health selection checker"
```

---

### Task 3: Add shared authoritative Supply freshness metadata

**Files:**
- Modify: `liquidation-preview.html`
- Modify: `supply/supply-draft.css`
- Modify: `supply/index.html`
- Modify: `borrow/index.html`
- Modify: `liquidation/index.html`
- Test: `tests/test_data_freshness_surface_contracts.py`
- Test: `tests/test_supply_table_ux_contracts.py`

**Interfaces:**
- Consumes: `SUPPLY_CHAIN_BUNDLE_QUERY`, its returned `_meta.block.timestamp`, and existing `supplyFormatRelativeTime(timestamp)`.
- Produces: `renderSupplyDataFreshness(timestamp = currentSupplyDataUpdatedAt)` and three metadata nodes sharing the same timestamp.

- [ ] **Step 1: Add the timestamp and metadata markup**

Add `_meta { block { timestamp } }` to `SUPPLY_CHAIN_BUNDLE_QUERY`. Add `#supply-leaderboard-data-updated` to the Supplier Leaderboard header and `#supply-activity-data-updated` to the Asset Activity header; retain `#supply-intel-asof` for Selected Market.

- [ ] **Step 2: Implement one shared freshness renderer**

Define `currentSupplyDataUpdatedAt`, normalize the GraphQL timestamp to a numeric Unix timestamp, and implement:

```js
function renderSupplyDataFreshness(timestamp = currentSupplyDataUpdatedAt) {
    const nodes = [
        document.getElementById('supply-intel-asof'),
        document.getElementById('supply-leaderboard-data-updated'),
        document.getElementById('supply-activity-data-updated')
    ].filter(Boolean);
    const numeric = Number(timestamp);
    const ready = Number.isFinite(numeric) && numeric > 0;
    currentSupplyDataUpdatedAt = ready ? numeric : null;
    nodes.forEach(node => {
        node.classList.toggle('is-updating', !ready);
        node.textContent = ready
            ? `Data updated · ${supplyFormatRelativeTime(numeric)}`
            : 'Data updating';
    });
}
```

Call it with the selected Supply bundle's `_meta.block.timestamp` after a successful bundle application, call it without a timestamp while refreshing, and refresh only the relative labels with one existing or new 30-second interval. Remove code that clears `#supply-intel-asof` after rendering.

- [ ] **Step 3: Match DOLO Holders metadata styling**

Style the shared metadata class as right-aligned muted 11px monospaced text with a 6px gold status dot and no pill background. Give `.is-updating` a subdued dot and preserve header wrapping at narrow widths.

- [ ] **Step 4: Make Asset Activity one solid holder surface**

Change `#supply-activity-card.supply-draft-activity-continuous-surface` to `background: var(--bg-2, #141417) !important;` and retain transparent nested sections and visible row hover state.

- [ ] **Step 5: Bump shared Supply preview cache keys**

Increment the Supply, Borrow, and Liquidation route-loader suffixes and the `supply-draft.css`/`supply-draft.js` suffixes referenced by the Supply route.

- [ ] **Step 6: Run focused tests**

Run:

```bash
python3 -m unittest \
  tests.test_data_freshness_surface_contracts.DataFreshnessSurfaceContractsTest.test_supply_cards_share_authoritative_data_timestamp \
  tests.test_data_freshness_surface_contracts.DataFreshnessSurfaceContractsTest.test_asset_activity_uses_solid_holder_surface \
  tests.test_supply_table_ux_contracts -v
node --check supply/supply-draft.js
```

Expected: Supply freshness, Asset Activity, and existing Supply table tests PASS.

- [ ] **Step 7: Commit the Supply implementation**

```bash
git add liquidation-preview.html supply/supply-draft.css supply/index.html borrow/index.html liquidation/index.html
git commit -m "feat: show authoritative supply data freshness"
```

---

### Task 4: Align DOLO and veDOLO surfaces and units

**Files:**
- Modify: `dolo-preview.html`
- Modify: `dolo/index.html`
- Modify: `portfolio-preview.html`
- Modify: `portfolio/index.html`
- Test: `tests/test_data_freshness_surface_contracts.py`
- Test: `tests/test_supply_table_ux_contracts.py`

**Interfaces:**
- Consumes: existing `.fresh-wallets-card`, `.pf-exercise-summary`, and `renderExerciseSummary(rows)` patterns.
- Produces: solid summary/card surfaces and corrected primary-value units.

- [ ] **Step 1: Align Fresh Wallets with DOLO Holders**

Replace the `.fresh-wallets-card` gradient with `background: var(--bg-2);` and keep its metric rail transparent so the card reads as one surface.

- [ ] **Step 2: Align the veDOLO summary rail**

Set `#pf-exercises-section .pf-exercise-summary.selected-market-rail` to `background: var(--bg-2);` and explicitly keep `.pf-exercise-summary-item` transparent so all metric cells share one tone.

- [ ] **Step 3: Correct primary-value units**

In `renderExerciseSummary(rows)`, render Total Locked with `<span class="unit">DOLO</span>`, and render the oDOLO Exercises and Vote Power primary values without a `veDOLO` suffix. Leave `USDC per veDOLO` unchanged under Average Price.

- [ ] **Step 4: Bump DOLO and Portfolio cache keys**

Increment the route-loader suffixes in `dolo/index.html` and `portfolio/index.html`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
python3 -m unittest \
  tests.test_data_freshness_surface_contracts \
  tests.test_supply_table_ux_contracts -v
```

Expected: PASS.

- [ ] **Step 6: Commit the surface and unit fixes**

```bash
git add dolo-preview.html dolo/index.html portfolio-preview.html portfolio/index.html
git commit -m "fix: align holder surfaces and position units"
```

---

### Task 5: Browser verification, full checks, and production deployment

**Files:**
- Verify only: all files changed in Tasks 1–4.

**Interfaces:**
- Consumes: local HTTP routes and pushed GitHub Pages deployment.
- Produces: browser evidence, passing tests, and a live production commit on `master`.

- [ ] **Step 1: Run the complete targeted verification set**

Run:

```bash
node --check tvl/supply-health.js
node --check supply/supply-draft.js
node --test tests/supply-pool-health.test.js
python3 -m unittest \
  tests.test_data_freshness_surface_contracts \
  tests.test_supply_pool_health_contracts \
  tests.test_supply_table_ux_contracts -v
python3 run_earn_audit_checks.py
git diff --check
```

Expected: all checks PASS with no whitespace errors.

- [ ] **Step 2: Verify the local UI in a real browser**

Serve the worktree with `python3 -m http.server 8765`, then verify:

- a selected Supply Pool Health network has a computed 15×15 gold checker and visible icon;
- Selected Market, Supplier Leaderboard, and Asset Activity show the same non-empty `Data updated · …` label or the honest `Data updating` fallback;
- Asset Activity and Fresh Wallets have the same solid computed background color as DOLO Holders;
- the veDOLO rail cells share one computed background and primary units read `DOLO`, no suffix, and no suffix as specified;
- desktop and narrow layouts do not overlap or clip;
- the browser console has no new warnings or errors.

- [ ] **Step 3: Perform two review passes**

First review correctness/regression risks: timestamp propagation, loading states, cache keys, responsive wrapping, selector scope, and untouched calculations. Second review maintainability/security: no duplicated intervals, no broad selectors, no injected unescaped data, no dependency/config/secret changes.

- [ ] **Step 4: Rebase and push production**

Fetch `dolomite-dashboard/master`, rebase without force, rerun the narrow tests if the rebase changes relevant files, then push:

```bash
git push dolomite-dashboard HEAD:master
```

- [ ] **Step 5: Verify GitHub Pages and live routes**

Confirm the Pages workflow for the pushed commit succeeds, then smoke-check:

- `https://twojekrypto.github.io/Dolomite-dashboard/tvl/`
- `https://twojekrypto.github.io/Dolomite-dashboard/supply/`
- `https://twojekrypto.github.io/Dolomite-dashboard/dolo/`
- `https://twojekrypto.github.io/Dolomite-dashboard/portfolio/`

Expected: all four routes load the new cache versions and display the approved UX.
