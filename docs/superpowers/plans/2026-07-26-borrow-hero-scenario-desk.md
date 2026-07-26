# Borrow Hero and Institutional Scenario Desk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the heavy Borrow position-change badge with a truthful DOLO-style percentage chip and rebuild the multi-asset Risk Simulator as a manual institutional scenario desk.

**Architecture:** Keep the static single-file architecture and existing risk calculations. Change only the Borrow contracts and `liquidation-preview.html`: simplify the hero renderer, make each multi-asset row use one normalized manual input plus visual feedback, and append a narrow final CSS override for the approved 56/44 desktop workspace and stacked mobile layout.

**Tech Stack:** Static HTML, CSS, browser JavaScript, Python `unittest` source-contract tests, Chromium browser QA, GitHub Pages from `master`.

## Global Constraints

- Preserve the Graphite + Gold visual identity and reuse existing variables.
- Use exact `change24h` data first; otherwise show the fallback's real rounded window.
- Never label incomplete position-count history as 24 hours.
- Keep the valid scenario domain at `−100%` through `+500%`.
- Step buttons move by exactly 1 percentage point; direct input may contain decimals.
- Remove the `−5%`, `−10%`, and `−25%` presets and their complete JavaScript/CSS path.
- Do not change risk formulas, position data, liquidation thresholds, wallet routing, generated JSON, dependencies, or Borrow table columns.
- Keep edits surgical; do not broadly reformat `liquidation-preview.html`.
- Verify static UI through `python3 -m http.server`, never through `file://`.
- Verify computed browser styles and bounding boxes at 1440 px and 390 px.

---

## File map

- Modify `tests/test_borrow_ux_contracts.py`: encode the compact hero, manual scenario controls, accessibility, and final responsive-layout contracts.
- Modify `liquidation-preview.html`: update only the Borrow hero markup/renderer and Risk Simulator markup, row interactions, and final CSS override.
- No generated data, workflow, dependency, or table files change.

### Task 1: Compact truthful Borrow hero chip

**Files:**
- Modify: `tests/test_borrow_ux_contracts.py:22-60`
- Modify: `liquidation-preview.html:6202-6264`
- Modify: `liquidation-preview.html:9952-9963`
- Modify: `liquidation-preview.html:12451-12511`

**Interfaces:**
- Consumes: `history.change24h`, `history.fallbackChange`, and `formatPositionCountWindow(windowSeconds) -> string | null`.
- Produces: `renderPositionCount24h(history) -> void`, updating `#stat-total-change-percent`, chip state classes, icon, title, and accessible label.

- [ ] **Step 1: Replace the old two-line hero contracts with failing compact-chip contracts**

```python
class TestBorrowHeroUx(unittest.TestCase):
    def test_hero_has_source_backed_truthful_position_change(self):
        for contract in (
            'id="stat-total-row"',
            'id="stat-total-change"',
            'id="stat-total-change-percent"',
            "data/liquidation-risk/position-count-history.json",
            "renderPositionCount24h",
            "fallbackChange",
            "windowSeconds",
            "formatPositionCountWindow",
            "24-hour baseline is still building",
            ".toFixed(2)",
            "const windowLabel",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

    def test_change_chip_contains_only_percentage_and_real_window(self):
        for obsolete in (
            'id="stat-total-change-value"',
            'id="stat-total-change-unit"',
            'class="stat-total-change-copy"',
            'class="stat-total-change-primary"',
        ):
            with self.subTest(obsolete=obsolete):
                self.assertNotIn(obsolete, SOURCE)
        self.assertIn(
            '<span id="stat-total-change-percent" class="stat-total-change-label">— · awaiting history</span>',
            SOURCE,
        )
        self.assertIn("percent.textContent = '— · awaiting history';", SOURCE)

    def test_change_chip_uses_compact_dolo_style(self):
        self.assertIn(
            "body.route-liquidation .stat-total-change {\n"
            "            display: inline-flex !important;\n"
            "            align-items: center !important;\n"
            "            gap: 6px !important;\n"
            "            min-height: 30px !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation .stat-total-change-label {\n"
            "            color: currentColor !important;\n"
            "            opacity: 1 !important;",
            SOURCE,
        )
```

- [ ] **Step 2: Run the focused tests and confirm the expected failure**

Run:

```bash
python3 -m unittest \
  tests.test_borrow_ux_contracts.TestBorrowHeroUx.test_hero_has_source_backed_truthful_position_change \
  tests.test_borrow_ux_contracts.TestBorrowHeroUx.test_change_chip_contains_only_percentage_and_real_window \
  tests.test_borrow_ux_contracts.TestBorrowHeroUx.test_change_chip_uses_compact_dolo_style -v
```

Expected: the first test passes, while the compact markup and CSS tests fail because the absolute count/unit and 46 px two-line badge still exist.

- [ ] **Step 3: Simplify the hero markup**

Replace the contents of `#stat-total-change` with:

```html
<div id="stat-total-change" class="stat-total-change unavailable" aria-label="Position change history unavailable">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12h14"/></svg>
    <span id="stat-total-change-percent" class="stat-total-change-label">— · awaiting history</span>
</div>
```

- [ ] **Step 4: Reduce the renderer to percentage, real window, and semantic metadata**

Use this renderer body:

```javascript
function renderPositionCount24h(history) {
    const chip = document.getElementById('stat-total-change');
    const percent = document.getElementById('stat-total-change-percent');
    if (!chip || !percent) return;

    chip.classList.remove('positive', 'negative', 'neutral', 'unavailable');
    const exactChange = history?.change24h;
    const comparison = exactChange || history?.fallbackChange;
    const rawChange = comparison?.change;
    const change = rawChange === null || rawChange === undefined ? Number.NaN : Number(rawChange);
    const rawBaseline = comparison?.baselineCount;
    const baseline = rawBaseline === null || rawBaseline === undefined ? Number.NaN : Number(rawBaseline);
    const percentage = Number.isFinite(baseline) && baseline > 0
        ? (change / baseline) * 100
        : Number.NaN;
    const windowLabel = exactChange
        ? '24h'
        : formatPositionCountWindow(comparison?.windowSeconds);
    const icon = chip.querySelector('svg');

    if (!Number.isFinite(change) || !windowLabel) {
        chip.classList.add('unavailable');
        percent.textContent = '— · awaiting history';
        chip.setAttribute('aria-label', 'Position change history unavailable');
        chip.removeAttribute('title');
        if (icon) icon.innerHTML = '<path d="M5 12h14"/>';
        return;
    }

    const state = change > 0 ? 'positive' : change < 0 ? 'negative' : 'neutral';
    const direction = percentage > 0 ? 'up' : percentage < 0 ? 'down' : 'unchanged';
    chip.classList.add(state);
    percent.textContent = Number.isFinite(percentage)
        ? `${percentage > 0 ? '+' : percentage < 0 ? '−' : ''}${Math.abs(percentage).toFixed(2)}% · ${windowLabel}`
        : `— · ${windowLabel}`;
    chip.setAttribute(
        'aria-label',
        Number.isFinite(percentage)
            ? `${Math.abs(percentage).toFixed(2)} percent ${direction} over ${exactChange ? '24 hours' : windowLabel}${exactChange ? '' : '. The 24-hour baseline is still building'}`
            : `Position change percentage unavailable over ${windowLabel}`
    );
    if (exactChange) {
        chip.removeAttribute('title');
    } else {
        chip.title = `Nearest available comparison: ${windowLabel}. The 24-hour baseline is still building.`;
    }
    if (icon) {
        icon.innerHTML = change > 0
            ? '<path d="m5 15 7-7 7 7"/>'
            : change < 0
                ? '<path d="m5 9 7 7 7-7"/>'
                : '<path d="M5 12h14"/>';
    }
}
```

- [ ] **Step 5: Replace the two-line badge CSS with the compact DOLO-style rules**

```css
body.route-liquidation .stat-total-change {
    display: inline-flex !important;
    align-items: center !important;
    gap: 6px !important;
    min-height: 30px !important;
    padding: 6px 10px !important;
    margin-bottom: 7px !important;
    border: 1px solid var(--line-2) !important;
    border-radius: 9px !important;
    background: var(--bg-2) !important;
    color: var(--fg-3) !important;
    font-family: var(--mono) !important;
    font-variant-numeric: tabular-nums !important;
    white-space: nowrap !important;
}
body.route-liquidation .stat-total-change svg {
    width: 13px !important;
    height: 13px !important;
    flex: 0 0 auto !important;
}
body.route-liquidation .stat-total-change-label {
    color: currentColor !important;
    opacity: 1 !important;
    font-size: 11px !important;
    font-weight: 650 !important;
    letter-spacing: .01em !important;
}
```

Keep the existing `.positive`, `.negative`, `.neutral`, and `.unavailable` state colors.

- [ ] **Step 6: Run the focused hero tests**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts.TestBorrowHeroUx -v
```

Expected: all `TestBorrowHeroUx` tests pass, including committed history consistency.

- [ ] **Step 7: Commit the hero change**

```bash
git add tests/test_borrow_ux_contracts.py liquidation-preview.html
git commit -m "fix: simplify Borrow position change chip"
```

### Task 2: Manual multi-asset scenario behavior and semantics

**Files:**
- Modify: `tests/test_borrow_ux_contracts.py:214-250`
- Modify: `liquidation-preview.html:10124-10135`
- Modify: `liquidation-preview.html:10234-10261`
- Modify: `liquidation-preview.html:13576-13741`

**Interfaces:**
- Consumes: `addMultiAssetRow(defaultToken?, defaultPct?, options?)`, `runMultiAssetSim()`, `updateMultiScenarioSummary()`, and the existing token selector.
- Produces: `clampMultiAssetPct(value) -> number`, `updateMultiRowMoveUi(row) -> void`, and row CSS variables `--sim-shock-left` / `--sim-shock-width`.

- [ ] **Step 1: Replace the preset contract with failing manual-desk contracts**

```python
class TestBorrowInstitutionalLiveImpactUx(unittest.TestCase):
    def test_simulator_explains_the_causal_flow(self):
        for contract in (
            "Build Scenario",
            "Price shock",
            "Scenario Result",
            'id="sim-impact-headline"',
            'id="sim-impact-state"',
            'id="sim-risk-level"',
            "Adjust a token to simulate impact",
            "positions cross HF 1.0",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

    def test_scenario_rows_use_one_manual_input_without_presets(self):
        for obsolete in (
            'class="sim-multi-presets"',
            'class="sim-multi-preset"',
            'data-pct="-5"',
            'data-pct="-10"',
            'data-pct="-25"',
            "applyMultiAssetPreset",
        ):
            with self.subTest(obsolete=obsolete):
                self.assertNotIn(obsolete, SOURCE)
        self.assertIn('data-step="-1"', SOURCE)
        self.assertIn('data-step="1"', SOURCE)
        self.assertIn('step="any"', SOURCE)
        self.assertIn('class="sim-multi-shock-rail"', SOURCE)
        self.assertIn("function clampMultiAssetPct(value)", SOURCE)
        self.assertIn("'--sim-shock-left'", SOURCE)
        self.assertIn("'--sim-shock-width'", SOURCE)

    def test_builder_actions_live_in_header_and_result_is_announced(self):
        header = SOURCE[
            SOURCE.index('<div class="sim-multi-header">'):
            SOURCE.index('<div class="sim-multi-builder">', SOURCE.index('<div class="sim-multi-header">'))
        ]
        self.assertIn('class="sim-multi-header-actions"', header)
        self.assertIn('id="sim-multi-add"', header)
        self.assertIn("Add Asset", header)
        self.assertIn('id="sim-multi-reset"', header)
        self.assertIn("Reset Scenario", header)
        self.assertNotIn('class="sim-multi-summary-card"', SOURCE)
        self.assertIn(
            'class="liquidation-sim-metric liq sim-impact-primary" role="status" aria-live="polite" aria-atomic="true"',
            SOURCE,
        )
```

- [ ] **Step 2: Run the focused simulator tests and confirm failure**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts.TestBorrowInstitutionalLiveImpactUx -v
```

Expected: failures report the old `Live Impact` copy, presets, 5-point controls, footer action card, and missing live-region attributes.

- [ ] **Step 3: Move actions into the builder header and make the primary result a live region**

Change the result heading to `Scenario Result` and add the live-region attributes to `.sim-impact-primary`:

```html
<div class="sim-result-head">
    <span>Scenario Result</span>
    <small id="sim-impact-state">Adjust a token to simulate impact</small>
</div>
<div class="liquidation-sim-metric liq sim-impact-primary" role="status" aria-live="polite" aria-atomic="true">
```

Replace the builder header/footer structure with:

```html
<div class="sim-multi-header">
    <div class="sim-multi-header-main">
        <span class="sim-multi-header-title">Build Scenario</span>
        <div class="sim-multi-header-copy">Set a signed price shock for each asset. Results update live.</div>
    </div>
    <div class="sim-multi-header-actions">
        <button type="button" class="sim-multi-add" id="sim-multi-add" onclick="addMultiAssetRow()">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            Add Asset
        </button>
        <button type="button" class="sim-multi-reset" id="sim-multi-reset" onclick="resetMultiAssetRows()">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round"><polyline points="1 4 1 10 7 10"></polyline><path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10"></path></svg>
            Reset Scenario
        </button>
    </div>
</div>
```

Delete `.sim-multi-summary-card` and its nested action block.

- [ ] **Step 4: Replace preset synchronization with bounded rail feedback**

```javascript
function clampMultiAssetPct(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return 0;
    return Math.max(-100, Math.min(500, number));
}

function updateMultiRowMoveUi(row) {
    const pctInput = row.querySelector('input[type="number"]');
    const pct = clampMultiAssetPct(parseFloat(pctInput?.value) || 0);
    const width = pct < 0
        ? Math.min(50, Math.abs(pct) / 2)
        : Math.min(50, pct / 10);
    const left = pct < 0 ? 50 - width : 50;
    row.classList.toggle('is-drop', pct < 0);
    row.classList.toggle('is-rise', pct > 0);
    row.style.setProperty('--sim-shock-left', `${left}%`);
    row.style.setProperty('--sim-shock-width', `${width}%`);
}
```

Delete `applyMultiAssetPreset`, `window.applyMultiAssetPreset`, and all `.sim-multi-preset` iteration.

- [ ] **Step 5: Render 1-point manual controls and the bipolar rail**

Use asset-specific labels and one visual-feedback rail:

```javascript
`<div class="sim-multi-move-cell">` +
    `<div class="sim-multi-move-layout">` +
        `<div class="sim-multi-move-control">` +
            `<button type="button" class="sim-multi-step" data-step="-1" aria-label="Decrease ${token} price shock by 1 percentage point">−</button>` +
            `<span class="sim-multi-input-wrap">` +
                `<input type="number" class="sim-multi-pct-input" min="-100" max="500" value="${initialPct}" step="any" inputmode="decimal" aria-label="${token} price shock percentage">` +
                `<span class="sim-multi-pct-symbol">%</span>` +
            `</span>` +
            `<button type="button" class="sim-multi-step" data-step="1" aria-label="Increase ${token} price shock by 1 percentage point">+</button>` +
        `</div>` +
        `<div class="sim-multi-shock-rail" aria-hidden="true">` +
            `<span class="sim-multi-shock-fill"></span>` +
            `<i class="sim-multi-shock-zero"></i>` +
        `</div>` +
    `</div>` +
`</div>`
```

When a row's token changes, synchronize all three accessible labels:

```javascript
pctInput.setAttribute('aria-label', `${sym} price shock percentage`);
row.querySelector('[data-step="-1"]')?.setAttribute(
    'aria-label',
    `Decrease ${sym} price shock by 1 percentage point`
);
row.querySelector('[data-step="1"]')?.setAttribute(
    'aria-label',
    `Increase ${sym} price shock by 1 percentage point`
);
```

- [ ] **Step 6: Normalize direct entry on commit and preserve live calculations**

Keep the existing `input` listener. Add:

```javascript
pctInput.addEventListener('change', () => {
    pctInput.value = clampMultiAssetPct(pctInput.value);
    updateMultiRowMoveUi(row);
    updateMultiScenarioSummary();
    if (window._multiAssetMode) runMultiAssetSim();
});
```

Update the step-button calculation to:

```javascript
const current = parseFloat(pctInput.value) || 0;
const next = clampMultiAssetPct(current + (parseFloat(btn.dataset.step) || 0));
pctInput.value = next;
updateMultiRowMoveUi(row);
updateMultiScenarioSummary();
if (window._multiAssetMode) runMultiAssetSim();
```

- [ ] **Step 7: Run the focused simulator tests**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts.TestBorrowInstitutionalLiveImpactUx -v
```

Expected: all institutional simulator contracts pass and no preset code remains.

- [ ] **Step 8: Commit the scenario semantics**

```bash
git add tests/test_borrow_ux_contracts.py liquidation-preview.html
git commit -m "feat: make Borrow scenarios fully manual"
```

### Task 3: Institutional desktop and responsive visual system

**Files:**
- Modify: `tests/test_borrow_ux_contracts.py:214-280`
- Modify: `liquidation-preview.html:9402-9641`

**Interfaces:**
- Consumes: `.sim-multi-header-actions`, `.sim-multi-shock-rail`, `.sim-multi-shock-fill`, `.sim-impact-primary`, and existing grid areas `basket`, `result`, and `wallets`.
- Produces: a 56/44 desktop grid above 980 px and a single-column layout at or below 980 px.

- [ ] **Step 1: Add failing layout and responsive CSS contracts**

```python
def test_scenario_desk_uses_balanced_desktop_panels(self):
    for contract in (
        "grid-template-columns: minmax(0, 1.12fr) minmax(340px, .88fr) !important;",
        "body.route-liquidation #sim-card .sim-multi-header-actions {",
        "body.route-liquidation #sim-card .sim-multi-shock-rail {",
        "body.route-liquidation #sim-card .sim-multi-shock-fill {",
        "left: var(--sim-shock-left, 50%) !important;",
        "width: var(--sim-shock-width, 0%) !important;",
        "body.route-liquidation #sim-card #sim-multi-panel,\n"
        "        body.route-liquidation #sim-card .liquidation-sim-metrics {\n"
        "            height: 100% !important;",
    ):
        with self.subTest(contract=contract):
            self.assertIn(contract, SOURCE)

def test_scenario_desk_stacks_without_mobile_preset_space(self):
    self.assertIn(
        "@media (max-width: 980px) {\n"
        "            body.route-liquidation #sim-card.liquidation-sim-card {\n"
        "                grid-template-columns: 1fr !important;",
        SOURCE,
    )
    self.assertIn(
        "body.route-liquidation #sim-card .sim-multi-header-actions {\n"
        "                width: 100% !important;",
        SOURCE,
    )
    self.assertNotIn(".sim-multi-presets", SOURCE)
    self.assertNotIn(".sim-multi-preset", SOURCE)
```

- [ ] **Step 2: Run the new layout tests and confirm failure**

Run:

```bash
python3 -m unittest \
  tests.test_borrow_ux_contracts.TestBorrowInstitutionalLiveImpactUx.test_scenario_desk_uses_balanced_desktop_panels \
  tests.test_borrow_ux_contracts.TestBorrowInstitutionalLiveImpactUx.test_scenario_desk_stacks_without_mobile_preset_space -v
```

Expected: failures identify the old 1.06/.94 split, missing header-action/rail styles, and obsolete preset CSS.

- [ ] **Step 3: Replace the current Institutional Live Impact override with the final desk rules**

Use one final narrow override block at the existing `/* Institutional Live Impact */` location:

```css
/* Institutional Scenario Desk: manual inputs, causal results, responsive balance. */
body.route-liquidation #sim-card.liquidation-sim-card {
    grid-template-columns: minmax(0, 1.12fr) minmax(340px, .88fr) !important;
    align-items: stretch !important;
    column-gap: 14px !important;
}
body.route-liquidation #sim-card #sim-multi-panel,
body.route-liquidation #sim-card .liquidation-sim-metrics {
    height: 100% !important;
    min-height: 0 !important;
}
body.route-liquidation #sim-card #sim-multi-panel {
    padding: 15px !important;
}
body.route-liquidation #sim-card #sim-multi-panel .sim-multi-header {
    display: flex !important;
    align-items: center !important;
    justify-content: space-between !important;
    gap: 16px !important;
    margin-bottom: 12px !important;
}
body.route-liquidation #sim-card .sim-multi-header-main {
    min-width: 0 !important;
}
body.route-liquidation #sim-card .sim-multi-header-actions {
    display: inline-flex !important;
    align-items: center !important;
    justify-content: flex-end !important;
    gap: 7px !important;
    flex: 0 0 auto !important;
}
body.route-liquidation #sim-card .sim-multi-header-actions .sim-multi-add,
body.route-liquidation #sim-card .sim-multi-header-actions .sim-multi-reset {
    width: auto !important;
    min-width: 0 !important;
    height: 32px !important;
    min-height: 32px !important;
    padding: 0 11px !important;
}
body.route-liquidation #sim-card .sim-multi-header-actions .sim-multi-add {
    color: var(--gold) !important;
    border-color: var(--gold-line) !important;
    background: rgba(201,162,39,.08) !important;
}
body.route-liquidation #sim-card .sim-multi-builder {
    display: block !important;
    height: auto !important;
}
body.route-liquidation #sim-card .sim-multi-editor {
    padding: 9px !important;
}
body.route-liquidation #sim-card #sim-multi-panel .sim-multi-column-heads,
body.route-liquidation #sim-card #sim-multi-panel .sim-multi-row {
    grid-template-columns: 30px minmax(120px, .74fr) minmax(190px, 1.26fr) !important;
    gap: 10px !important;
}
body.route-liquidation #sim-card #sim-multi-panel .sim-multi-row {
    min-height: 64px !important;
    padding: 8px 9px !important;
}
body.route-liquidation #sim-card .sim-multi-move-layout {
    display: grid !important;
    grid-template-columns: minmax(0, 1fr) !important;
    gap: 7px !important;
}
body.route-liquidation #sim-card .sim-multi-move-control {
    grid-template-columns: 30px minmax(70px, 1fr) 30px !important;
    gap: 5px !important;
}
body.route-liquidation #sim-card .sim-multi-step {
    width: 30px !important;
    height: 30px !important;
    min-height: 30px !important;
}
body.route-liquidation #sim-card .sim-multi-shock-rail {
    position: relative !important;
    height: 3px !important;
    margin: 0 4px !important;
    overflow: hidden !important;
    border-radius: 999px !important;
    background: rgba(255,255,255,.055) !important;
}
body.route-liquidation #sim-card .sim-multi-shock-fill {
    position: absolute !important;
    inset-block: 0 !important;
    left: var(--sim-shock-left, 50%) !important;
    width: var(--sim-shock-width, 0%) !important;
    border-radius: inherit !important;
    background: var(--gold) !important;
    transition: left .16s var(--ease), width .16s var(--ease), background .16s var(--ease) !important;
}
body.route-liquidation #sim-card .sim-multi-row.is-drop .sim-multi-shock-fill {
    background: var(--down) !important;
}
body.route-liquidation #sim-card .sim-multi-row.is-rise .sim-multi-shock-fill {
    background: var(--up) !important;
}
body.route-liquidation #sim-card .sim-multi-shock-zero {
    position: absolute !important;
    top: -2px !important;
    bottom: -2px !important;
    left: calc(50% - .5px) !important;
    width: 1px !important;
    background: var(--fg-3) !important;
    opacity: .62 !important;
}
body.route-liquidation #sim-card .liquidation-sim-metrics {
    grid-template-columns: minmax(0, 1fr) !important;
    grid-template-rows: auto minmax(126px, auto) auto auto !important;
    align-content: stretch !important;
}
body.route-liquidation #sim-card .sim-impact-primary {
    order: 1 !important;
}
body.route-liquidation #sim-card .sim-impact-secondary-grid {
    order: 2 !important;
}
body.route-liquidation #sim-card .sim-impact-risk-row {
    order: 3 !important;
}
@media (max-width: 980px) {
    body.route-liquidation #sim-card.liquidation-sim-card {
        grid-template-columns: 1fr !important;
        grid-template-areas:
            "header"
            "basket"
            "result"
            "wallets" !important;
    }
    body.route-liquidation #sim-card #sim-multi-panel,
    body.route-liquidation #sim-card .liquidation-sim-metrics {
        height: auto !important;
    }
}
@media (max-width: 620px) {
    body.route-liquidation #sim-card #sim-multi-panel .sim-multi-header {
        align-items: flex-start !important;
        flex-direction: column !important;
    }
    body.route-liquidation #sim-card .sim-multi-header-actions {
        width: 100% !important;
        display: grid !important;
        grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) !important;
    }
    body.route-liquidation #sim-card .sim-multi-header-actions .sim-multi-add,
    body.route-liquidation #sim-card .sim-multi-header-actions .sim-multi-reset {
        width: 100% !important;
    }
    body.route-liquidation #sim-card #sim-multi-panel .sim-multi-row {
        grid-template-columns: 30px minmax(0, 1fr) !important;
        grid-template-areas:
            "remove asset"
            ". shock" !important;
        min-height: 118px !important;
    }
    body.route-liquidation #sim-card #sim-multi-panel .sim-multi-remove {
        grid-area: remove !important;
    }
    body.route-liquidation #sim-card #sim-multi-panel .sim-multi-token-cell {
        grid-area: asset !important;
    }
    body.route-liquidation #sim-card #sim-multi-panel .sim-multi-move-cell {
        grid-area: shock !important;
    }
}
@media (prefers-reduced-motion: reduce) {
    body.route-liquidation #sim-card .sim-multi-shock-fill {
        transition: none !important;
    }
}
```

Delete all obsolete `.sim-multi-presets` and `.sim-multi-preset` rules, including their mobile overrides.

- [ ] **Step 4: Run the complete Borrow UX contract suite**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts -v
```

Expected: all Borrow UX contracts pass.

- [ ] **Step 5: Commit the visual system**

```bash
git add tests/test_borrow_ux_contracts.py liquidation-preview.html
git commit -m "style: refine Borrow institutional scenario desk"
```

### Task 4: Browser QA, regression checks, production push

**Files:**
- Verify: `liquidation-preview.html`
- Verify: `tests/test_borrow_ux_contracts.py`
- Verify: `docs/superpowers/specs/2026-07-26-borrow-hero-scenario-desk-design.md`

**Interfaces:**
- Consumes: the completed static page and local HTTP endpoint.
- Produces: verified desktop/mobile UI and production `master` containing the implementation.

- [ ] **Step 1: Run repository-level checks**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts -v
python3 run_earn_audit_checks.py
git diff --check
git status --short
```

Expected: tests and audit checks pass; `git diff --check` is silent; the worktree contains no uncommitted production edits.

- [ ] **Step 2: Start the static dashboard server**

Run:

```bash
python3 -m http.server 8765
```

Open:

```text
http://127.0.0.1:8765/liquidation-preview.html
```

- [ ] **Step 3: Perform required independent browser QA at 1440 px**

Use the repository browser-testing skill and a browser QA subagent. Inspect the Borrow route and execute:

```javascript
const card = document.querySelector('#sim-card');
const builder = document.querySelector('#sim-multi-panel');
const result = document.querySelector('#sim-card .liquidation-sim-metrics');
const chip = document.querySelector('#stat-total-change');
const styles = getComputedStyle(card);
({
  pageOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
  gridColumns: styles.gridTemplateColumns,
  panelHeightDelta: Math.abs(builder.getBoundingClientRect().height - result.getBoundingClientRect().height),
  chipHeight: chip.getBoundingClientRect().height,
  chipText: document.querySelector('#stat-total-change-percent')?.textContent?.trim(),
  presets: document.querySelectorAll('.sim-multi-preset').length,
  railCount: document.querySelectorAll('.sim-multi-shock-rail').length
})
```

Acceptance:

- `pageOverflow` equals `0`;
- grid columns resolve close to the 56/44 ratio;
- `panelHeightDelta` is at most `1`;
- chip height is compact and its text contains the window calculated from the
  current `fallbackChange.windowSeconds` (or `24h` once `change24h` exists);
- `presets` equals `0`;
- `railCount` equals the visible scenario-row count;
- threshold crossings are the first major result card.

- [ ] **Step 4: Verify desktop interactions**

In the first row:

1. type `-7.5` and commit; confirm the input remains `-7.5`, the row becomes `is-drop`, the rail fills left of zero, and results update;
2. click plus once; confirm `-6.5`;
3. click minus once; confirm `-7.5`;
4. enter `700` and commit; confirm normalization to `500`;
5. enter `-200` and commit; confirm normalization to `-100`;
6. use Add Asset, remove the added row, then Reset Scenario; confirm exactly three default rows at `0%`.

- [ ] **Step 5: Perform required browser QA at 390 px**

Execute:

```javascript
const actions = document.querySelector('.sim-multi-header-actions');
const firstInput = document.querySelector('.sim-multi-pct-input');
({
  pageOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
  cardColumns: getComputedStyle(document.querySelector('#sim-card')).gridTemplateColumns,
  actionWidth: actions.getBoundingClientRect().width,
  viewportWidth: document.documentElement.clientWidth,
  inputVisible: firstInput.getBoundingClientRect().width > 60
})
```

Acceptance:

- `pageOverflow` equals `0`;
- card resolves to one column;
- both header actions remain visible without overlap;
- the input and ± controls remain on one line and `inputVisible` is true;
- selector, rail, result cards, and wallet disclosure stack in reading order.

- [ ] **Step 6: Apply and verify any browser-only correction**

If browser QA finds a computed-layout defect, first add an exact regression assertion to `tests/test_borrow_ux_contracts.py`, confirm it fails, patch only the relevant final CSS selector, rerun the Borrow suite and both viewport checks, then commit:

```bash
git add tests/test_borrow_ux_contracts.py liquidation-preview.html
git commit -m "fix: resolve Borrow scenario desk browser regression"
```

- [ ] **Step 7: Synchronize with production and push**

Run:

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
python3 -m unittest tests.test_borrow_ux_contracts -v
python3 run_earn_audit_checks.py
git push dolomite-dashboard HEAD:master
```

Expected: push succeeds without force and updates production `master`.

- [ ] **Step 8: Verify GitHub Pages**

Monitor the repository Actions runs started by the push. Require the relevant checks and Pages deployment to complete successfully, then open the deployed Borrow route and confirm:

- hero count plus one compact signed percentage/window chip;
- no absolute position delta;
- no preset buttons;
- manual 1-point controls and rail feedback;
- equal-height desktop panels;
- stacked mobile layout without horizontal overflow.
