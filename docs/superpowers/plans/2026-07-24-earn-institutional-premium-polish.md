# EARN Institutional Premium Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved institutional-ledger Portfolio Value summary, an Assets-matched APR/APY switch, and the compact `Carry` label without changing any financial data or calculations.

**Architecture:** Keep the existing EARN markup, renderers, and state hooks. Make the visible copy change in `dashboard-core.js`, express the redesign through scoped shared and dedicated-route CSS, regenerate the EARN JavaScript/HTML bundle, and bump both shared and dedicated cache versions. Contract tests lock the exact control geometry, copy, summary hierarchy, and deployment versions before browser verification.

**Tech Stack:** Static HTML/CSS/JavaScript, Python `unittest`, Node.js syntax/test runner, generated EARN bundle, GitHub Pages.

## Global Constraints

- Preserve all existing portfolio, yield, reward, APR/APY, verification, and carry calculations.
- Preserve Portfolio Value, supply/borrow-route/verification counters, Total Yield Earned, and Rewards.
- Do not add fonts, packages, images, APIs, generated data files, or persistent state.
- Keep the Graphite + Gold identity with `#09090B`, `#151518`, `#C9A227`, `#E4C15A`, `#F4F3EF`, and `#75B87B`.
- Keep Inter for labels/body and JetBrains Mono for financial values.
- The visible label is `Carry`; its existing explanatory tooltip remains.
- The EARN APR/APY control must match Dolomite Assets: approximately `116px × 36px` outer size, `3px` padding, `10px` radius, `52px × 28px` slider, centered `28px` options with `0 14px` padding and `18px` line height.
- The card must not clip or overflow on desktop, tablet, or mobile.
- Preserve the existing reduced-motion behavior and add no continuous animation.
- Production is GitHub Pages from `master`; never overwrite newer automated commits on the remote branch.

---

## File Structure

- `dashboard-core.js`: authoritative EARN labels, state, and rendering; only visible carry copy changes here.
- `dashboard-core.css`: shared EARN APR/APY geometry and institutional-ledger styling for the main dashboard route.
- `earn/earn-draft.css`: dedicated `/earn/` route overrides loaded after the shared CSS.
- `earn/earn-core.js`: generated dedicated EARN runtime; never edit directly.
- `earn/earn-core.html`: generated dedicated EARN shell; never edit directly.
- `build_earn_bundle.py`: authoritative dedicated-bundle version and generator.
- `earn/index.html`: route loader and dedicated CSS cache version.
- `dashboard-core.html`: shared CSS/JavaScript cache versions.
- `tests/test_earn_dashboard_contracts.py`: source-level EARN rendering and quality-label contracts.
- `tests/test_earn_premium_ux_contracts.py`: APR/APY, summary, dedicated route, and cache contracts.
- `tests/test_earn_layout_contracts.py`: generated bundle version contract.
- `tests/test_emode_ux_contracts.py`: changed-page cache-version contract.

### Task 1: Compact Carry Copy and Exact APR/APY Geometry

**Files:**
- Modify: `tests/test_earn_dashboard_contracts.py:148-157`
- Modify: `tests/test_earn_premium_ux_contracts.py:38-66`
- Modify: `dashboard-core.js:10370-10396`
- Modify: `dashboard-core.js:11772-11784`
- Modify: `dashboard-core.css:11001-11044`
- Modify: `earn/earn-draft.css:531-545`
- Generate: `earn/earn-core.js`

**Interfaces:**
- Consumes: existing `earn_toggleAprApy()`, `.right`, `.active`, `data-tip`, and `earn-quality-marker` behavior.
- Produces: visible `Carry` quality labels and Assets-parity geometry on `#earn-apr-pill`; no state or calculation changes.

- [ ] **Step 1: Write failing contracts for the visible label and switch geometry**

Replace the `Inferred Carry` assertion in `test_non_strict_yield_quality_can_override_verified_balance_badge` and expand the APR/APY geometry test:

```python
# tests/test_earn_dashboard_contracts.py
self.assertIn(
    "inferred.status === 'pre_snapshot_carry' ? 'Carry' : 'Inferred'",
    self.source,
)
self.assertIn(
    "isPreSnapshotCarry ? 'Carry' : 'Inferred'",
    self.source,
)
self.assertIn(
    "? 'Carry' : 'Inferred'",
    self.source,
)
self.assertNotIn("'Inferred Carry'", self.source)
```

```python
# tests/test_earn_premium_ux_contracts.py
def test_earn_apr_toggle_uses_assets_gold_switch_geometry(self):
    self.assertIn("#earn-supply-section #earn-apr-pill {", self.css)
    start = self.css.index("#earn-supply-section #earn-apr-pill {")
    end = self.css.index(".earn-asset-table {", start)
    switch_css = self.css[start:end]
    for rule in (
        "height: 36px",
        "padding: 3px",
        "border-radius: 10px",
        "width: 52px",
        "height: 28px",
        "display: inline-flex",
        "align-items: center",
        "padding: 0 14px",
        "line-height: 18px",
        "transform: translateX(52px)",
        "background: var(--earn-gold",
    ):
        self.assertIn(rule, switch_css)
    self.assertNotIn("min-width: 48px", switch_css)
    self.assertIn(
        "#earn-supply-section #earn-apr-pill .apr-pill-opt.active",
        switch_css,
    )

def test_dedicated_earn_route_keeps_assets_switch_geometry(self):
    start = self.draft_css.index(
        "body.earn-draft-route #earn-supply-section #earn-apr-pill {"
    )
    end = self.draft_css.index(
        "body.earn-draft-route .earn-error",
        start,
    )
    switch_css = self.draft_css[start:end]
    for rule in (
        "height: 36px !important",
        "padding: 3px !important",
        "width: 52px !important",
        "height: 28px !important",
        "display: inline-flex !important",
        "align-items: center !important",
        "padding: 0 14px !important",
        "line-height: 18px !important",
        "transform: translateX(52px) !important",
    ):
        self.assertIn(rule, switch_css)
```

- [ ] **Step 2: Run the focused contracts and confirm the expected failures**

Run:

```bash
python3 -m unittest \
  tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_non_strict_yield_quality_can_override_verified_balance_badge \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_earn_apr_toggle_uses_assets_gold_switch_geometry \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_dedicated_earn_route_keeps_assets_switch_geometry -v
```

Expected: failures report the old `Inferred Carry` copy and missing `52px`, inline-flex, `18px`, and fixed translation rules.

- [ ] **Step 3: Implement the compact label without changing status or tooltip logic**

Make these exact label-only substitutions in `dashboard-core.js`:

```javascript
label: inferred.status === 'pre_snapshot_carry' ? 'Carry' : 'Inferred',
```

```javascript
label: isPreSnapshotCarry ? 'Carry' : 'Inferred',
```

```javascript
label: (method === 'all-netflow-pre-snapshot-carry' || method === 'recent-cycle-pre-snapshot-carry') ? 'Carry' : 'Inferred',
```

```javascript
rawLabel: (method === 'all-netflow-pre-snapshot-carry' || method === 'recent-cycle-pre-snapshot-carry') ? 'Carry' : 'Inferred',
```

Leave the adjacent `title`, `rawStatus`, `status`, and calculation branches unchanged.

- [ ] **Step 4: Match the Assets switch geometry in shared CSS**

Replace the EARN-scoped APR/APY geometry in `dashboard-core.css` with:

```css
#earn-supply-section #earn-apr-pill {
    height: 36px;
    padding: 3px;
    border-radius: 10px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    background: rgba(255, 255, 255, 0.035);
    backdrop-filter: none;
}

#earn-supply-section #earn-apr-pill:hover {
    border-color: rgba(201, 162, 39, 0.32);
    background: rgba(201, 162, 39, 0.045);
    box-shadow: 0 0 14px rgba(201, 162, 39, 0.08);
}

#earn-supply-section #earn-apr-pill .apr-pill-slider {
    top: 3px;
    left: 3px;
    width: 52px;
    height: 28px;
    border-radius: 8px;
    background: var(--earn-gold, #c9a227);
    box-shadow: 0 2px 8px rgba(201, 162, 39, 0.35);
}

#earn-supply-section #earn-apr-pill .apr-pill-slider.right {
    transform: translateX(52px);
}

#earn-supply-section #earn-apr-pill .apr-pill-opt {
    display: inline-flex;
    align-items: center;
    height: 28px;
    padding: 0 14px;
    border-radius: 8px;
    color: var(--earn-fg-3, #6b6a66);
    font-size: 12px;
    font-weight: 600;
    line-height: 18px;
}

#earn-supply-section #earn-apr-pill .apr-pill-opt.active {
    color: var(--earn-bg-0, #09090b);
}
```

- [ ] **Step 5: Lock the same geometry on the dedicated EARN route**

Expand the existing dedicated-route block in `earn/earn-draft.css` to:

```css
body.earn-draft-route #earn-supply-section #earn-apr-pill {
  height: 36px !important;
  padding: 3px !important;
  border-radius: 10px !important;
}

body.earn-draft-route #earn-supply-section #earn-apr-pill .apr-pill-slider {
  width: 52px !important;
  height: 28px !important;
  background: var(--earn-gold) !important;
  border-color: transparent !important;
  box-shadow: 0 2px 8px rgba(201, 162, 39, .35) !important;
}

body.earn-draft-route #earn-supply-section #earn-apr-pill .apr-pill-slider.right {
  transform: translateX(52px) !important;
}

body.earn-draft-route #earn-supply-section #earn-apr-pill .apr-pill-opt {
  display: inline-flex !important;
  align-items: center !important;
  height: 28px !important;
  padding: 0 14px !important;
  line-height: 18px !important;
}

body.earn-draft-route #earn-supply-section #earn-apr-pill .apr-pill-opt.active {
  color: var(--earn-bg-0) !important;
}
```

- [ ] **Step 6: Regenerate the dedicated runtime and run the focused checks**

Run:

```bash
python3 build_earn_bundle.py
python3 -m unittest \
  tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_non_strict_yield_quality_can_override_verified_balance_badge \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_earn_apr_toggle_uses_assets_gold_switch_geometry \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_dedicated_earn_route_keeps_assets_switch_geometry -v
node --check dashboard-core.js
node --check earn/earn-core.js
python3 build_earn_bundle.py --check
```

Expected: three unit tests pass, both JavaScript syntax checks exit `0`, and the generator prints `Generated EARN assets are current.`

- [ ] **Step 7: Review and commit the compact-label/control deliverable**

Run:

```bash
git diff --check
git diff -- tests/test_earn_dashboard_contracts.py tests/test_earn_premium_ux_contracts.py dashboard-core.js dashboard-core.css earn/earn-draft.css earn/earn-core.js
git add tests/test_earn_dashboard_contracts.py tests/test_earn_premium_ux_contracts.py dashboard-core.js dashboard-core.css earn/earn-draft.css earn/earn-core.js
git commit -m "fix: align EARN carry label and APR toggle"
```

Expected: the diff contains only visible copy, scoped switch CSS, tests, and the generated JavaScript mirror.

### Task 2: Institutional Ledger Portfolio Summary

**Files:**
- Modify: `tests/test_earn_premium_ux_contracts.py:102-144`
- Modify: `dashboard-core.css:9735-10340`
- Modify: `earn/earn-draft.css:573-790`
- Modify: `earn/earn-draft.css:2518-2635`

**Interfaces:**
- Consumes: unchanged `.earn-summary-main`, `.earn-summary-metrics`, `.earn-summary-stat`, `.earn-summary-hero`, `.earn-summary-chip-row`, and rewards mini-list markup.
- Produces: one connected ledger surface with a gold rail and hairline-separated metrics; no JavaScript or data changes.

- [ ] **Step 1: Add failing institutional-ledger contracts**

Add these tests to `tests/test_earn_premium_ux_contracts.py`:

```python
def test_shared_summary_uses_one_institutional_ledger_surface(self):
    start = self.css.index("/* Institutional Ledger */")
    end = self.css.index("/* ═══════ Filter Bar", start)
    ledger_css = self.css[start:end]
    for rule in (
        "grid-template-columns: minmax(0, 1.3fr) minmax(360px, 0.7fr)",
        "gap: 0",
        ".earn-summary-main::after",
        "width: 1px",
        "#c9a227",
        ".earn-summary-metrics .earn-summary-stat + .earn-summary-stat",
        "border-left: 1px solid",
        "background: transparent",
        "box-shadow: none",
    ):
        self.assertIn(rule, ledger_css)

def test_dedicated_summary_uses_gold_rail_without_nested_metric_cards(self):
    start = self.draft_css.index(
        "body.earn-draft-route .earn-summary-card {"
    )
    end = self.draft_css.index(
        "body.earn-draft-route #earn-supply-section",
        start,
    )
    ledger_css = self.draft_css[start:end]
    for rule in (
        "gap: 0 !important",
        "padding: 0 !important",
        "body.earn-draft-route .earn-summary-main::after",
        "width: 1px !important",
        "background: transparent !important",
        "border-left: 1px solid var(--earn-line-2) !important",
        "box-shadow: none !important",
    ):
        self.assertIn(rule, ledger_css)

def test_summary_rail_and_metrics_stack_without_mobile_overflow(self):
    mobile = self.draft_css[self.draft_css.index("@media (max-width: 980px)"):]
    self.assertIn("height: 1px !important", mobile)
    self.assertIn("width: auto !important", mobile)
    self.assertIn("border-top: 1px solid var(--earn-line-2) !important", mobile)
    self.assertIn("grid-template-columns: minmax(0, 1fr) !important", mobile)
```

- [ ] **Step 2: Run the summary contracts and confirm they fail**

Run:

```bash
python3 -m unittest \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_shared_summary_uses_one_institutional_ledger_surface \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_dedicated_summary_uses_gold_rail_without_nested_metric_cards \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_summary_rail_and_metrics_stack_without_mobile_overflow -v
```

Expected: failures report the missing `Institutional Ledger` block, zero-gap layout, gold rail, and mobile separator rules.

- [ ] **Step 3: Add the shared institutional-ledger override after existing summary primitives**

Insert this block immediately before `/* ═══════ Filter Bar ═══════ */` in `dashboard-core.css`:

```css
/* Institutional Ledger */
.earn-summary-card,
.earn-summary-card.cols-8,
.earn-summary-card.cols-6 {
    grid-template-columns: minmax(0, 1.3fr) minmax(360px, 0.7fr);
    gap: 0;
    padding: 0;
    border-radius: 20px;
    background:
        radial-gradient(620px 220px at 14% -18%, rgba(201, 162, 39, 0.11), transparent 64%),
        linear-gradient(180deg, #151518 0%, #09090b 100%);
    border: 1px solid rgba(255, 255, 255, 0.075);
    box-shadow: inset 0 1px 0 rgba(244, 243, 239, 0.035), 0 20px 48px rgba(0, 0, 0, 0.28);
}

.earn-summary-card::before {
    height: 1px;
    background: linear-gradient(90deg, transparent 0%, rgba(201, 162, 39, 0.72) 18%, rgba(228, 193, 90, 0.34) 52%, transparent 100%);
}

.earn-summary-card::after {
    display: none;
}

.earn-summary-main {
    display: flex;
    padding: 26px 30px;
    position: relative;
}

.earn-summary-main::after {
    content: "";
    position: absolute;
    top: 24px;
    right: 0;
    bottom: 24px;
    width: 1px;
    background: linear-gradient(180deg, transparent, #c9a227 18%, rgba(228, 193, 90, 0.76) 50%, #c9a227 82%, transparent);
    box-shadow: 0 0 16px rgba(201, 162, 39, 0.12);
    transition: box-shadow 180ms ease, opacity 180ms ease;
    opacity: 0.82;
}

.earn-summary-card:hover .earn-summary-main::after {
    opacity: 1;
    box-shadow: 0 0 20px rgba(201, 162, 39, 0.18);
}

.earn-summary-metrics {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0;
    padding: 26px 0;
}

.earn-summary-main .earn-summary-stat,
.earn-summary-main .earn-summary-stat.is-primary,
.earn-summary-metrics .earn-summary-stat,
.earn-summary-metrics .earn-summary-stat.kpi-primary {
    min-height: 0;
    padding: 0;
    border: 0;
    border-radius: 0;
    background: transparent;
    box-shadow: none;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
    overflow: visible;
}

.earn-summary-main .earn-summary-stat,
.earn-summary-main .earn-summary-stat.is-primary {
    padding-right: 30px;
}

.earn-summary-metrics .earn-summary-stat,
.earn-summary-metrics .earn-summary-stat.kpi-primary {
    padding: 0 24px;
}

.earn-summary-metrics .earn-summary-stat + .earn-summary-stat {
    border-left: 1px solid rgba(255, 255, 255, 0.075);
}

.earn-summary-main .earn-summary-stat:hover,
.earn-summary-main .earn-summary-stat.is-primary:hover,
.earn-summary-metrics .earn-summary-stat:hover,
.earn-summary-metrics .earn-summary-stat.kpi-primary:hover {
    background: transparent;
    box-shadow: none;
}

@media (max-width: 980px) {
    .earn-summary-card,
    .earn-summary-card.cols-8,
    .earn-summary-card.cols-6 {
        grid-template-columns: minmax(0, 1fr);
    }

    .earn-summary-main {
        padding: 24px;
    }

    .earn-summary-main::after {
        top: auto;
        left: 24px;
        right: 24px;
        bottom: 0;
        width: auto;
        height: 1px;
        background: linear-gradient(90deg, transparent, #c9a227 18%, rgba(228, 193, 90, 0.76) 50%, #c9a227 82%, transparent);
    }

    .earn-summary-main .earn-summary-stat,
    .earn-summary-main .earn-summary-stat.is-primary {
        padding-right: 0;
    }

    .earn-summary-metrics {
        padding: 22px 0;
    }
}

@media (max-width: 640px) {
    .earn-summary-main {
        padding: 20px;
    }

    .earn-summary-main::after {
        left: 20px;
        right: 20px;
    }

    .earn-summary-metrics {
        grid-template-columns: minmax(0, 1fr);
        padding: 0 20px;
    }

    .earn-summary-metrics .earn-summary-stat,
    .earn-summary-metrics .earn-summary-stat.kpi-primary {
        padding: 20px 0;
    }

    .earn-summary-metrics .earn-summary-stat + .earn-summary-stat {
        border-top: 1px solid rgba(255, 255, 255, 0.075);
        border-left: 0;
    }
}
```

- [ ] **Step 4: Replace the dedicated-route card overrides with the same ledger hierarchy**

In `earn/earn-draft.css`, keep the existing color tokens but change the summary rules to these exact structural values:

```css
body.earn-draft-route .earn-summary-card {
  grid-template-columns: minmax(0, 1.3fr) minmax(360px, .7fr) !important;
  align-items: stretch !important;
  gap: 0 !important;
  margin-bottom: 24px !important;
  padding: 0 !important;
  border-radius: 22px !important;
  border: 1px solid var(--earn-line-2) !important;
  overflow: hidden !important;
  background:
    radial-gradient(620px 220px at 14% -18%, rgba(201, 162, 39, .11), transparent 64%),
    linear-gradient(180deg, #151518, #09090b) !important;
  box-shadow: inset 0 1px 0 rgba(244, 243, 239, .035), 0 20px 48px rgba(0, 0, 0, .28) !important;
}

body.earn-draft-route .earn-summary-card::before {
  background: linear-gradient(90deg, transparent, rgba(201, 162, 39, .72) 18%, rgba(228, 193, 90, .34) 52%, transparent) !important;
}

body.earn-draft-route .earn-summary-card::after {
  display: none !important;
}

body.earn-draft-route .earn-summary-main {
  display: flex !important;
  padding: 26px 30px !important;
  position: relative !important;
}

body.earn-draft-route .earn-summary-main::after {
  content: "" !important;
  position: absolute !important;
  top: 24px !important;
  right: 0 !important;
  bottom: 24px !important;
  width: 1px !important;
  background: linear-gradient(180deg, transparent, var(--earn-gold) 18%, var(--earn-gold-hi) 50%, var(--earn-gold) 82%, transparent) !important;
  box-shadow: 0 0 16px rgba(201, 162, 39, .12) !important;
  opacity: .82 !important;
}

body.earn-draft-route .earn-summary-card:hover .earn-summary-main::after {
  opacity: 1 !important;
  box-shadow: 0 0 20px rgba(201, 162, 39, .18) !important;
}

body.earn-draft-route .earn-summary-metrics {
  display: grid !important;
  grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
  gap: 0 !important;
  padding: 26px 0 !important;
}

body.earn-draft-route .earn-summary-main .earn-summary-stat,
body.earn-draft-route .earn-summary-main .earn-summary-stat.is-primary,
body.earn-draft-route .earn-summary-metrics .earn-summary-stat,
body.earn-draft-route .earn-summary-metrics .earn-summary-stat.kpi-primary {
  min-height: 0 !important;
  border: 0 !important;
  border-radius: 0 !important;
  background: transparent !important;
  box-shadow: none !important;
  backdrop-filter: none !important;
  -webkit-backdrop-filter: none !important;
  overflow: visible !important;
}

body.earn-draft-route .earn-summary-main .earn-summary-stat,
body.earn-draft-route .earn-summary-main .earn-summary-stat.is-primary {
  padding: 0 30px 0 0 !important;
}

body.earn-draft-route .earn-summary-metrics .earn-summary-stat,
body.earn-draft-route .earn-summary-metrics .earn-summary-stat.kpi-primary {
  padding: 0 24px !important;
}

body.earn-draft-route .earn-summary-metrics .earn-summary-stat + .earn-summary-stat {
  border-left: 1px solid var(--earn-line-2) !important;
}

body.earn-draft-route .earn-summary-main .earn-summary-stat:hover,
body.earn-draft-route .earn-summary-main .earn-summary-stat.is-primary:hover,
body.earn-draft-route .earn-summary-metrics .earn-summary-stat:hover,
body.earn-draft-route .earn-summary-metrics .earn-summary-stat.kpi-primary:hover {
  background: transparent !important;
  box-shadow: none !important;
}
```

Update the existing responsive blocks:

```css
@media (max-width: 980px) {
  body.earn-draft-route .earn-summary-card,
  body.earn-draft-route .earn-summary-card.cols-8,
  body.earn-draft-route .earn-summary-card.cols-6 {
    grid-template-columns: minmax(0, 1fr) !important;
    padding: 0 !important;
  }

  body.earn-draft-route .earn-summary-main {
    padding: 24px !important;
  }

  body.earn-draft-route .earn-summary-main::after {
    top: auto !important;
    left: 24px !important;
    right: 24px !important;
    bottom: 0 !important;
    width: auto !important;
    height: 1px !important;
    background: linear-gradient(90deg, transparent, var(--earn-gold) 18%, var(--earn-gold-hi) 50%, var(--earn-gold) 82%, transparent) !important;
  }

  body.earn-draft-route .earn-summary-main .earn-summary-stat,
  body.earn-draft-route .earn-summary-main .earn-summary-stat.is-primary {
    padding-right: 0 !important;
  }

  body.earn-draft-route .earn-summary-metrics {
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    padding: 22px 0 !important;
  }
}

@media (max-width: 560px) {
  body.earn-draft-route .earn-summary-card {
    padding: 0 !important;
  }

  body.earn-draft-route .earn-summary-main {
    padding: 20px !important;
  }

  body.earn-draft-route .earn-summary-main::after {
    left: 20px !important;
    right: 20px !important;
  }

  body.earn-draft-route .earn-summary-metrics {
    grid-template-columns: minmax(0, 1fr) !important;
    padding: 0 20px !important;
  }

  body.earn-draft-route .earn-summary-metrics .earn-summary-stat,
  body.earn-draft-route .earn-summary-metrics .earn-summary-stat.kpi-primary {
    padding: 20px 0 !important;
  }

  body.earn-draft-route .earn-summary-metrics .earn-summary-stat + .earn-summary-stat {
    border-top: 1px solid var(--earn-line-2) !important;
    border-left: 0 !important;
  }
}
```

- [ ] **Step 5: Run the focused summary tests**

Run:

```bash
python3 -m unittest \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_summary_contains_only_approved_metric_groups \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_shared_summary_uses_one_institutional_ledger_surface \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_dedicated_summary_uses_gold_rail_without_nested_metric_cards \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_summary_rail_and_metrics_stack_without_mobile_overflow -v
```

Expected: four tests pass and no removed summary metric label reappears.

- [ ] **Step 6: Review and commit the institutional-ledger deliverable**

Run:

```bash
git diff --check
git diff -- tests/test_earn_premium_ux_contracts.py dashboard-core.css earn/earn-draft.css
git add tests/test_earn_premium_ux_contracts.py dashboard-core.css earn/earn-draft.css
git commit -m "feat: polish EARN portfolio as institutional ledger"
```

Expected: the commit contains only scoped summary CSS and its contracts; no renderer, calculation, data, or table-column changes.

### Task 3: Cache Versions, Full Verification, and Production Publication

**Files:**
- Modify: `tests/test_earn_premium_ux_contracts.py:144-155`
- Modify: `tests/test_earn_layout_contracts.py:94-100`
- Modify: `tests/test_emode_ux_contracts.py:53-72`
- Modify: `build_earn_bundle.py:15`
- Modify: `dashboard-core.html:25,4982`
- Modify: `earn/index.html:23-25`
- Generate: `earn/earn-core.html`
- Verify: all files changed in Tasks 1-3

**Interfaces:**
- Consumes: completed label, switch, summary, and contract commits from Tasks 1-2.
- Produces: fresh immutable asset URLs, passing EARN checks, browser evidence, and a production `master` update based on the latest remote tip.

- [ ] **Step 1: Change cache-version contracts first**

Use these exact expected versions in the relevant tests:

```python
# tests/test_earn_premium_ux_contracts.py
def test_dedicated_earn_bundle_uses_premium_ux_cache_version(self):
    version = "earn-core-20260724-institutional-premium"
    builder = (ROOT / "build_earn_bundle.py").read_text(encoding="utf-8")
    route = (ROOT / "earn/index.html").read_text(encoding="utf-8")
    self.assertIn(version, builder)
    self.assertGreaterEqual(route.count(version), 2)

def test_shared_dashboard_assets_use_premium_ux_cache_version(self):
    version = "core-split-20260724-earn-institutional-premium"
    self.assertIn(f"dashboard-core.css?v={version}", self.html)
    self.assertIn(f"dashboard-core.js?v={version}", self.html)
```

```python
# tests/test_earn_layout_contracts.py
self.assertIn(
    "earn-core-20260724-institutional-premium",
    self.bundle_builder,
)
```

```python
# tests/test_emode_ux_contracts.py
"earn/index.html": "earn-core-20260724-institutional-premium",
```

```python
# tests/test_emode_ux_contracts.py
self.assertIn(
    "dashboard-core.css?v=core-split-20260724-earn-institutional-premium",
    self.earn_html,
)
self.assertIn(
    "dashboard-core.js?v=core-split-20260724-earn-institutional-premium",
    self.earn_html,
)
```

- [ ] **Step 2: Run the version contracts and confirm they fail**

Run:

```bash
python3 -m unittest \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_dedicated_earn_bundle_uses_premium_ux_cache_version \
  tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_shared_dashboard_assets_use_premium_ux_cache_version \
  tests.test_earn_layout_contracts.EarnLayoutContractsTest.test_bundle_builder_keeps_static_layout_cache_and_local_editor_guard \
  tests.test_emode_ux_contracts.EModeUxContractsTest.test_changed_pages_have_fresh_cache_versions -v
```

Expected: failures show only the old `20260723` versions.

- [ ] **Step 3: Apply the fresh shared and dedicated cache versions**

Change `build_earn_bundle.py`:

```python
STATIC_LAYOUT_VERSION = "earn-core-20260724-institutional-premium"
```

Change both shared asset tags in `dashboard-core.html`:

```html
<link rel="stylesheet" href="dashboard-core.css?v=core-split-20260724-earn-institutional-premium">
```

```html
<script src="dashboard-core.js?v=core-split-20260724-earn-institutional-premium"></script>
```

Change the route loader values in `earn/index.html`:

```javascript
"version": "earn-core-20260724-institutional-premium",
"styles": [
  "earn/earn-draft.css?v=earn-core-20260724-institutional-premium"
],
```

- [ ] **Step 4: Regenerate and run all targeted automated checks**

Run:

```bash
python3 build_earn_bundle.py
python3 build_earn_bundle.py --check
node --check dashboard-core.js
node --check earn/earn-core.js
node --test tests/yield-labels.test.js
python3 -m unittest \
  tests.test_earn_dashboard_contracts \
  tests.test_earn_premium_ux_contracts \
  tests.test_earn_layout_contracts \
  tests.test_emode_ux_contracts -v
npm run check:earn-audit
```

Expected: the generator is current, syntax checks exit `0`, the Node test file passes, all four Python suites pass, and the EARN audit exits `0`.

- [ ] **Step 5: Verify desktop and mobile computed styles in a real browser**

Start the static server:

```bash
python3 -m http.server 8000
```

Open `http://127.0.0.1:8000/earn/`, enter wallet `0x5be9a4959308a0d0c7bc0870e319314d8d957dbb`, and run this in the browser page context after results load:

```javascript
(() => {
  const pill = document.querySelector('#earn-apr-pill');
  const slider = document.querySelector('#earn-apr-pill-slider');
  const option = document.querySelector('#earn-apr-opt');
  const card = document.querySelector('#earn-summary-card');
  const main = document.querySelector('.earn-summary-main');
  const metrics = [...document.querySelectorAll('.earn-summary-metrics .earn-summary-stat')];
  const pillStyle = getComputedStyle(pill);
  const sliderStyle = getComputedStyle(slider);
  const optionStyle = getComputedStyle(option);
  const railStyle = getComputedStyle(main, '::after');
  return {
    pill: {
      width: pillStyle.width,
      height: pillStyle.height,
      padding: pillStyle.padding,
      borderRadius: pillStyle.borderRadius,
    },
    slider: {
      width: sliderStyle.width,
      height: sliderStyle.height,
      transform: sliderStyle.transform,
    },
    option: {
      display: optionStyle.display,
      alignItems: optionStyle.alignItems,
      height: optionStyle.height,
      padding: optionStyle.padding,
      lineHeight: optionStyle.lineHeight,
    },
    ledger: {
      gap: getComputedStyle(card).gap,
      railWidth: railStyle.width,
      railHeight: railStyle.height,
      secondMetricLeftBorder: metrics[1] ? getComputedStyle(metrics[1]).borderLeftWidth : null,
      overflowX: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    },
    carryLabels: [...document.querySelectorAll('.earn-quality-marker')]
      .map((node) => node.textContent.trim())
      .filter((label) => label.includes('Carry')),
  };
})()
```

Desktop expected values:

- pill height `36px`, width approximately `116px`, padding `3px`, radius `10px`
- slider `52px × 28px`
- option `inline-flex`, centered, `28px` high, `0px 14px` padding, `18px` line height
- card gap `0px`
- vertical rail width `1px`
- second metric left border `1px`
- horizontal overflow `0`
- any pre-snapshot row displays `Carry`, never `Inferred Carry`

Toggle to APY and confirm the slider transform translates exactly `52px` while both labels remain vertically centered.

Set the viewport to `390 × 844` and rerun the ledger portion. Expected:

- rail width is the available horizontal span and rail height is `1px`
- metrics use one column
- the second metric has a `1px` top border and no left border
- horizontal overflow remains `0`
- counters wrap inside the card

Check the browser console and expect no errors.

- [ ] **Step 6: Perform two scoped reviews and commit the publication assets**

First review:

```bash
git diff --check
git diff --stat
git diff -- build_earn_bundle.py dashboard-core.html earn/index.html earn/earn-core.html tests/test_earn_premium_ux_contracts.py tests/test_earn_layout_contracts.py tests/test_emode_ux_contracts.py
```

Fresh-eyes review:

```bash
git status --short
git diff --name-only
rg -n "Inferred Carry|20260723-premium-ux-2|core-split-20260723-earn-premium-ux" \
  dashboard-core.js earn/earn-core.js build_earn_bundle.py dashboard-core.html earn/index.html \
  tests/test_earn_dashboard_contracts.py tests/test_earn_premium_ux_contracts.py \
  tests/test_earn_layout_contracts.py tests/test_emode_ux_contracts.py
```

Expected: no stale visible copy or old cache version remains in the scoped source, generated assets, or contract tests.

Commit:

```bash
git add build_earn_bundle.py dashboard-core.html earn/index.html earn/earn-core.html tests/test_earn_premium_ux_contracts.py tests/test_earn_layout_contracts.py tests/test_emode_ux_contracts.py
git commit -m "build: publish EARN institutional premium assets"
```

- [ ] **Step 7: Integrate onto the latest production tip and push without losing automation commits**

Fetch and inspect:

```bash
git fetch dolomite-dashboard master
git log --oneline --decorate -5 dolomite-dashboard/master
git diff --name-status dolomite-dashboard/master...HEAD
```

Create a temporary production integration worktree from the fetched remote tip, then cherry-pick only this feature's approved spec and implementation commits:

```bash
git worktree add ../dolomite-earn-institutional-publish -b codex/earn-institutional-premium-publish dolomite-dashboard/master
cd ../dolomite-earn-institutional-publish
git cherry-pick e6a3eea97d1
git cherry-pick "$(git log codex/total-supply-over-time --format=%H --grep='^docs: approve EARN institutional premium plan$' -1)"
git cherry-pick "$(git log codex/total-supply-over-time --format=%H --grep='^fix: align EARN carry label and APR toggle$' -1)"
git cherry-pick "$(git log codex/total-supply-over-time --format=%H --grep='^feat: polish EARN portfolio as institutional ledger$' -1)"
git cherry-pick "$(git log codex/total-supply-over-time --format=%H --grep='^build: publish EARN institutional premium assets$' -1)"
```

Before cherry-picking, verify that every subject lookup returns exactly one commit hash. If any cherry-pick conflicts because production changed the same scoped files, stop the cherry-pick, inspect the latest production implementation, and reapply the approved changes against that code before continuing; never force the stale branch over `master`.

Rerun the release checks in the integration worktree:

```bash
python3 build_earn_bundle.py --check
node --check dashboard-core.js
node --check earn/earn-core.js
python3 -m unittest \
  tests.test_earn_dashboard_contracts \
  tests.test_earn_premium_ux_contracts \
  tests.test_earn_layout_contracts \
  tests.test_emode_ux_contracts -v
npm run check:earn-audit
git diff --check dolomite-dashboard/master...HEAD
```

Expected: all checks pass against the latest production base.

Push the integration tip:

```bash
git push dolomite-dashboard HEAD:master
```

Expected: a non-forced fast-forward update of `master`.

- [ ] **Step 8: Verify the deployed route**

After GitHub Pages publishes, open:

```text
https://twojekrypto.github.io/Dolomite-dashboard/earn/
```

Confirm:

- the route loads `earn-core-20260724-institutional-premium`
- the shared page loads `core-split-20260724-earn-institutional-premium`
- the desktop and mobile card match the local ledger structure
- APR/APY remains centered in both states
- visible pre-snapshot quality copy is `Carry`
- the console has no errors

If deployment is still pending, inspect the latest Pages workflow and wait for its terminal status before reporting completion.
