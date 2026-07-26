# Risk Simulator Balanced Panels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the redundant Risk Simulator status and risk-level copy, then give the desktop builder and result panels an equal, fully used 50/50 composition.

**Architecture:** Keep the current static single-file implementation and extend its final scoped Borrow CSS override. Remove the obsolete DOM and JavaScript update paths, then let the surviving primary and secondary result cards fill a three-row result grid. Lock the user-facing structure with focused Python contract tests and verify computed browser geometry.

**Tech Stack:** Static HTML, CSS Grid/Flexbox, vanilla JavaScript, Python `unittest`, Chromium browser verification.

## Global Constraints

- Changes remain surgical within `liquidation-preview.html` and `tests/test_borrow_ux_contracts.py`.
- No data files, workflow logic, dependencies, or upstream risk methodology are changed.
- Desktop panels use exact 50/50 width above 980 px and identical computed height.
- `Scenario active · Updated live as you edit`, its inactive equivalent, and the entire `Risk level / High impact` row are absent.
- The existing threshold result remains `role="status" aria-live="polite" aria-atomic="true"`.
- At 980 px and below the two panels retain their stacked reading order.
- At 390 px the page has no horizontal overflow.

---

### Task 1: Lock the simplified result contract

**Files:**
- Modify: `tests/test_borrow_ux_contracts.py:255-335`

**Interfaces:**
- Consumes: `SOURCE`, the complete text of `liquidation-preview.html`.
- Produces: regression contracts for removed copy/IDs, equal desktop columns, and the result-card fill grid.

- [ ] **Step 1: Replace the obsolete causal-flow and desktop-layout assertions**

Update `test_simulator_explains_the_causal_flow` so it retains the meaningful
scenario and live-result contracts while explicitly rejecting the removed
elements:

```python
def test_simulator_explains_the_causal_flow_without_redundant_status_copy(self):
    for contract in (
        "Build Scenario",
        "Price shock",
        "Scenario Result",
        'id="sim-impact-headline"',
        "positions cross HF 1.0",
    ):
        with self.subTest(contract=contract):
            self.assertIn(contract, SOURCE)
    for obsolete in (
        'id="sim-impact-state"',
        'id="sim-risk-level"',
        'class="sim-impact-risk-row"',
        "Scenario active · Updated live as you edit",
        "Adjust a token to simulate impact",
        "Risk level",
        "High impact",
    ):
        with self.subTest(obsolete=obsolete):
            self.assertNotIn(obsolete, SOURCE)
```

Replace the status-specific CSS test with a result-fill contract:

```python
def test_result_cards_fill_the_simplified_result_panel(self):
    self.assertIn(
        "grid-template-rows: auto minmax(140px, 1.1fr) minmax(132px, .9fr) !important;",
        SOURCE,
    )
    self.assertIn(
        "grid-template-columns: repeat(2, minmax(0, 1fr)) !important;",
        SOURCE,
    )
    self.assertIn(
        "body.route-liquidation #sim-card .sim-impact-secondary-grid > .liquidation-sim-metric",
        SOURCE,
    )
```

Keep the mobile row-height assertions in a renamed independent test:

```python
def test_mobile_scenario_rows_do_not_reserve_empty_slots(self):
    self.assertIn(
        "const rowSlots = isCompact ? Math.min(rows.length, visibleRows) : visibleRows;",
        SOURCE,
    )
    self.assertIn(
        "height = (rowHeight * rowSlots) + (rowGap * Math.max(0, rowSlots - 1));",
        SOURCE,
    )
```

Change the desktop contract string in
`test_scenario_desk_uses_balanced_desktop_panels` to:

```python
"grid-template-columns: repeat(2, minmax(0, 1fr)) !important;",
```

- [ ] **Step 2: Run the focused tests and verify the new contract fails**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts
```

Expected: failures report the still-present `sim-impact-state`,
`sim-risk-level`, 56/44 grid, or missing result-fill CSS.

- [ ] **Step 3: Commit the failing contract**

```bash
git add tests/test_borrow_ux_contracts.py
git commit -m "test: define balanced risk simulator contract"
```

---

### Task 2: Simplify and balance the Risk Simulator

**Files:**
- Modify: `liquidation-preview.html:9300-9465`
- Modify: `liquidation-preview.html:9970-10005`
- Modify: `liquidation-preview.html:13175-13215`

**Interfaces:**
- Consumes: existing `updateInstitutionalImpactState(crossingCount, hasScenario)` calls and existing result metric IDs.
- Produces: the same function signature without status/risk-level DOM dependencies; equal panel geometry and a filled result-card grid.

- [ ] **Step 1: Remove the redundant result markup**

Reduce the result header to one visible heading:

```html
<div class="sim-result-head">
    <span>Scenario Result</span>
</div>
```

Delete the complete block:

```html
<div class="sim-impact-risk-row">
    <span>Risk level</span>
    <strong id="sim-risk-level" data-level="none">No impact</strong>
</div>
```

Leave the threshold card and both exposure cards unchanged.

- [ ] **Step 2: Remove the obsolete JavaScript update paths**

Keep the public function signature and the state still used by visible cards:

```javascript
function updateInstitutionalImpactState(crossingCount, hasScenario) {
    const count = Math.max(0, Number(crossingCount || 0));
    const impactHeadline = document.getElementById('sim-impact-headline');
    const metrics = document.querySelector('#sim-card .liquidation-sim-metrics');

    if (impactHeadline) impactHeadline.classList.toggle('is-risk', count > 0);
    if (metrics) metrics.classList.toggle('has-scenario', !!hasScenario);
}
```

Remove the `impactState`, `riskLevel`, `total`, `ratio`, and `riskState`
variables and their update branches.

- [ ] **Step 3: Implement equal desktop columns and filled result rows**

In the final scoped desktop CSS override, use:

```css
body.route-liquidation #sim-card.liquidation-sim-card {
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
}
body.route-liquidation #sim-card .liquidation-sim-metrics {
    grid-template-columns: minmax(0, 1fr) !important;
    grid-template-rows: auto minmax(140px, 1.1fr) minmax(132px, .9fr) !important;
    align-content: stretch !important;
}
body.route-liquidation #sim-card .sim-impact-primary {
    order: 1 !important;
    height: 100% !important;
}
body.route-liquidation #sim-card .sim-impact-secondary-grid {
    order: 2 !important;
    display: grid !important;
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    gap: 10px !important;
    min-height: 0 !important;
}
body.route-liquidation #sim-card .sim-impact-secondary-grid > .liquidation-sim-metric {
    height: 100% !important;
}
```

Delete the final overrides dedicated to `.sim-result-head small`,
`.sim-result-head small.is-active`, and `.sim-impact-risk-row`.

At the existing `max-width: 980px` breakpoint, reset result rows to intrinsic
stacked-page sizing:

```css
body.route-liquidation #sim-card .liquidation-sim-metrics {
    grid-template-rows: auto auto auto !important;
}
```

At `max-width: 620px`, stack the two secondary cards:

```css
body.route-liquidation #sim-card .sim-impact-secondary-grid {
    grid-template-columns: minmax(0, 1fr) !important;
}
```

- [ ] **Step 4: Run focused and project checks**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts
python3 run_earn_audit_checks.py
git diff --check
```

Expected: all checks pass with no whitespace errors.

- [ ] **Step 5: Commit the implementation**

```bash
git add liquidation-preview.html
git commit -m "feat: balance risk simulator panels"
```

---

### Task 3: Verify real browser geometry and interaction

**Files:**
- Verify: `liquidation-preview.html`
- Verify: `borrow_positions.json` and other existing fetched fixtures through the local HTTP server

**Interfaces:**
- Consumes: the completed static page.
- Produces: computed geometry and interaction evidence; no production code unless verification exposes a defect.

- [ ] **Step 1: Start the static server**

Run:

```bash
python3 -m http.server 8765
```

Open:

```text
http://127.0.0.1:8765/liquidation-preview.html
```

- [ ] **Step 2: Verify desktop computed geometry at 1440 px**

Use Chromium to assert:

```javascript
const builder = document.querySelector('#sim-multi-panel').getBoundingClientRect();
const result = document.querySelector('.liquidation-sim-metrics').getBoundingClientRect();
({
  widthDelta: Math.abs(builder.width - result.width),
  heightDelta: Math.abs(builder.height - result.height),
  overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
  removedCopyVisible: document.body.innerText.includes('Risk level') ||
    document.body.innerText.includes('Scenario active · Updated live as you edit')
});
```

Expected:

- `widthDelta <= 1`
- `heightDelta <= 1`
- `overflow === 0`
- `removedCopyVisible === false`

Visually confirm that the threshold card occupies the upper result area and
Debt/Collateral form an evenly sized lower row without a blank bottom band.

- [ ] **Step 3: Verify responsive layout at 390 px**

Assert that the builder precedes the result panel, both widths fit the viewport,
Debt and Collateral stack, and:

```javascript
document.documentElement.scrollWidth <= document.documentElement.clientWidth
```

Expected: `true`.

- [ ] **Step 4: Verify live interaction**

Change an asset shock with the numeric input and the `−`/`+` controls, add and
remove an asset, and reset the scenario. Confirm that threshold crossings,
Debt Exposed, Collateral Exposed, and wallet details update without console
errors.

- [ ] **Step 5: Run final verification**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts
python3 run_earn_audit_checks.py
git diff --check
git status --short --branch
```

Expected: tests and audits pass; the branch contains only the planned commits.
