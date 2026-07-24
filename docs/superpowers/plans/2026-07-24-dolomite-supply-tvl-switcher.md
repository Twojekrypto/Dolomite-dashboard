# Dolomite Supply & TVL Switcher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate Total Supply and TVL history into one accessible switchable card, and remove the duplicate Portfolio Value supply/borrow sentence.

**Architecture:** Keep the proven history renderer and its two independent brush-state instances, but place both chart panels inside one semantic card and reveal exactly one panel through a small controller. Update the shared EARN source first, regenerate the dedicated EARN bundle, and cache-bust both routes.

**Tech Stack:** Static HTML/CSS/JavaScript, Python `unittest` contract tests, generated EARN bundle, Playwright browser verification.

## Global Constraints

- Preserve the existing Graphite + Gold identity and existing chart renderer.
- The visible title is exactly `Dolomite Supply & TVL`.
- The switch labels are exactly `TOTAL SUPPLY` and `TVL`.
- `TOTAL SUPPLY` is active by default.
- Do not add overlay/compare mode or dependencies.
- Keep each metric's existing tooltip, range badge, and independent mini-chart brush.
- Remove only the redundant Portfolio Value sentence; keep supply, borrow-route, and verification chips.
- Update route versions so GitHub Pages clients receive the new assets.
- Use test-first development and make surgical edits only.

---

### Task 1: One switchable protocol-history card

**Files:**
- Modify: `tests/test_tvl_preview_contracts.py`
- Modify: `tvl-preview.html`
- Modify: `tvl/index.html`

**Interfaces:**
- Consumes: existing `createHistoryChart(config)`, `supplyHistoryChart`, and `tvlHistoryChart`.
- Produces: `setProtocolHistoryMetric(metric)` where `metric` is `"supply"` or `"tvl"`, plus the `protocol-history-metric` button group and two mutually exclusive panels.

- [ ] **Step 1: Replace the dual-card test with a failing combined-card contract**

```python
def test_supply_and_tvl_share_one_switchable_history_card(self):
    text = TVL_PREVIEW.read_text(encoding="utf-8")
    self.assertEqual(text.count("<h2>Dolomite Supply &amp; TVL</h2>"), 1)
    self.assertNotIn("<h2>Total Supply Over Time</h2>", text)
    self.assertNotIn("<h2>TVL Over Time</h2>", text)
    self.assertIn('id="protocol-history-metric"', text)
    self.assertIn('data-history-metric="supply"', text)
    self.assertIn('data-history-metric="tvl"', text)
    self.assertIn('data-history-panel="supply"', text)
    self.assertIn('data-history-panel="tvl" hidden', text)
    self.assertIn('function setProtocolHistoryMetric(metric)', text)
    self.assertIn('setProtocolHistoryMetric("supply")', text)
```

- [ ] **Step 2: Run the focused contract and verify RED**

Run:

```bash
python3 -m unittest tests.test_tvl_preview_contracts.TvlPreviewContractsTest.test_supply_and_tvl_share_one_switchable_history_card
```

Expected: FAIL because the two old headings/cards still exist and the controller is absent.

- [ ] **Step 3: Implement the combined markup and institutional segmented control**

Replace the two sibling card shells with one:

```html
<section class="card protocol-history-card" id="protocol-history-card">
  <div class="card-head protocol-history-head">
    <div class="card-title">
      <h2>Dolomite Supply &amp; TVL</h2>
      <span class="count-badge" id="protocolHistoryRangeBadge">All Time</span>
    </div>
    <div class="card-meta"><span class="pulse"></span>drag the window below to zoom</div>
  </div>
  <div class="protocol-history-toolbar">
    <div class="history-metric-mode" id="protocol-history-metric" role="group" aria-label="Protocol history metric">
      <button type="button" class="active" data-history-metric="supply" aria-pressed="true">TOTAL SUPPLY</button>
      <button type="button" data-history-metric="tvl" aria-pressed="false">TVL</button>
    </div>
  </div>
</section>
```

Inside that section, wrap the existing supply `chart-wrap` and `brush-wrap`
siblings in:

```html
<div class="protocol-history-panel" data-history-panel="supply">
</div>
```

Wrap the existing TVL `chart-wrap` and `brush-wrap` siblings in:

```html
<div class="protocol-history-panel" data-history-panel="tvl" hidden>
</div>
```

The inner SVG, tooltip, brush, and element IDs remain byte-for-byte unchanged;
only their enclosing card and panel wrappers change.

Add component-scoped styles matching the holder switch:

```css
.protocol-history-toolbar{display:flex;align-items:center;padding:14px 24px 0}
.history-metric-mode{display:inline-grid;grid-template-columns:repeat(2,minmax(128px,1fr));height:34px;padding:3px;border:1px solid rgba(255,255,255,.075);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.028),rgba(255,255,255,.012))}
.history-metric-mode button{height:28px;padding:0 14px;border:0;border-radius:999px;background:transparent;color:var(--fg-3);font-family:var(--mono);font-size:10px;font-weight:800;letter-spacing:.36px;cursor:pointer}
.history-metric-mode button.active,.history-metric-mode button:hover,.history-metric-mode button:focus-visible{background:linear-gradient(180deg,rgba(255,255,255,.062),rgba(201,162,39,.072));color:var(--fg-1);box-shadow:inset 0 0 0 1px rgba(201,162,39,.22)}
.history-metric-mode button:focus-visible{outline:1px solid var(--gold);outline-offset:1px}
```

Wire the switch without changing chart math:

```javascript
function setProtocolHistoryMetric(metric){
  const nextMetric = metric === "tvl" ? "tvl" : "supply";
  document.querySelectorAll("[data-history-metric]").forEach(button => {
    const active = button.dataset.historyMetric === nextMetric;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
  document.querySelectorAll("[data-history-panel]").forEach(panel => {
    panel.hidden = panel.dataset.historyPanel !== nextMetric;
  });
  const badge = document.getElementById("protocolHistoryRangeBadge");
  const source = document.getElementById(nextMetric === "supply" ? "supplyRangeBadge" : "rangeBadge");
  if (badge && source) badge.textContent = source.textContent;
}
```

Keep the per-panel range badges as visually hidden internal state, call the
shared badge sync from each chart render, register click listeners once during
boot, and call `setProtocolHistoryMetric("supply")`.

- [ ] **Step 4: Bump the TVL route version**

Append `combined-history-20260724` to the version string in `tvl/index.html`
and update the matching route assertion.

- [ ] **Step 5: Run the focused TVL suite and verify GREEN**

Run:

```bash
python3 -m unittest tests.test_tvl_preview_contracts
node --check <(python3 -c 'import re,pathlib; t=pathlib.Path("tvl-preview.html").read_text(); print(re.search(r"<script>([\\s\\S]*)</script>\\s*</body>",t).group(1))')
```

Expected: all TVL contract tests pass and the extracted JavaScript parses.

- [ ] **Step 6: Commit the switchable card**

```bash
git add tvl-preview.html tvl/index.html tests/test_tvl_preview_contracts.py
git commit -m "feat: combine supply and TVL history"
```

### Task 2: Remove duplicate Portfolio Value counters

**Files:**
- Modify: `tests/test_earn_premium_ux_contracts.py`
- Modify: `dashboard-core.js`
- Modify: `build_earn_bundle.py`
- Modify: `dashboard-core.html`
- Modify: `earn/index.html`
- Regenerate: `earn/earn-core.html`
- Regenerate: `earn/earn-core.js`
- Modify version assertions in: `tests/test_earn_layout_contracts.py`
- Modify version assertions in: `tests/test_emode_ux_contracts.py`

**Interfaces:**
- Consumes: existing `earn-summary-supply-chip`, `earn-summary-borrow-chip`, and verification chip.
- Produces: Portfolio Value markup with no `earn-summary-portfolio-sub` element or `N supply · N borrow` string.

- [ ] **Step 1: Add a failing Portfolio Value deduplication contract**

```python
def test_portfolio_value_uses_chips_without_duplicate_counter_sentence(self):
    summary_scope = self.js.index("const summaryRunId =")
    summary_start = self.js.index("summaryEl.innerHTML = `", summary_scope)
    summary_end = self.js.index("summaryEl.classList.add('visible')", summary_start)
    summary_js = self.js[summary_start:summary_end]
    self.assertNotIn("earn-summary-portfolio-sub", summary_js)
    self.assertNotIn("portfolioSub", summary_js)
    self.assertIn('id="earn-summary-supply-chip"', summary_js)
    self.assertIn('id="earn-summary-borrow-chip"', summary_js)
```

Also assert that `earn_updateSummaryDebt()` no longer queries or writes
`earn-summary-portfolio-sub`.

- [ ] **Step 2: Run the focused contract and verify RED**

Run:

```bash
python3 -m unittest tests.test_earn_premium_ux_contracts.EarnPremiumUxContractsTest.test_portfolio_value_uses_chips_without_duplicate_counter_sentence
```

Expected: FAIL because `portfolioSub` and `earn-summary-portfolio-sub` are present.

- [ ] **Step 3: Remove only the redundant sentence and update path**

Delete:

```javascript
const portfolioSubEl = document.getElementById('earn-summary-portfolio-sub');
if (portfolioSubEl) {
  portfolioSubEl.innerHTML = `<span class="sub-highlight">${activeCount}</span> supply · <span style="color:#f87171">${positions.length}</span> borrow`;
}
const portfolioSub = `<span class="sub-highlight">${activeCount}</span> supply · <span style="color:#f87171">${borrowRouteCount}</span> borrow`;
<div class="earn-summary-sub" id="earn-summary-portfolio-sub">${portfolioSub}</div>
```

Keep all three existing summary chips unchanged.

- [ ] **Step 4: Bump EARN asset versions and regenerate the dedicated bundle**

Use `earn-core-20260724-portfolio-dedupe` in `build_earn_bundle.py` and
`earn/index.html`; use `core-split-20260724-portfolio-dedupe` for the shared
`dashboard-core.js` URL. Update exact-version assertions, then run:

```bash
python3 build_earn_bundle.py
python3 build_earn_bundle.py --check
```

Expected: the generator writes synchronized `earn/earn-core.html` and
`earn/earn-core.js`, then reports `Generated EARN assets are current.`

- [ ] **Step 5: Run EARN tests and verify GREEN**

Run:

```bash
python3 -m unittest tests.test_earn_premium_ux_contracts tests.test_earn_layout_contracts tests.test_build_earn_bundle tests.test_emode_ux_contracts
node --check dashboard-core.js
node --check earn/earn-core.js
```

Expected: all selected tests pass and both JavaScript files parse.

- [ ] **Step 6: Commit the Portfolio cleanup**

```bash
git add dashboard-core.js dashboard-core.html build_earn_bundle.py earn/index.html earn/earn-core.html earn/earn-core.js tests/test_earn_premium_ux_contracts.py tests/test_earn_layout_contracts.py tests/test_emode_ux_contracts.py
git commit -m "fix: remove duplicate EARN portfolio counters"
```

### Task 3: Browser and release verification

**Files:**
- Verify: `tvl-preview.html`
- Verify: `earn/earn-core.html`
- Verify: all changed tests and generated assets

**Interfaces:**
- Consumes: completed Task 1 and Task 2 behavior.
- Produces: browser evidence for layout, toggle behavior, accessibility state, and clean release checks.

- [ ] **Step 1: Run the complete targeted test set**

```bash
python3 -m unittest tests.test_tvl_preview_contracts tests.test_earn_premium_ux_contracts tests.test_earn_layout_contracts tests.test_build_earn_bundle tests.test_emode_ux_contracts
python3 build_earn_bundle.py --check
node --check dashboard-core.js
node --check earn/earn-core.js
```

Expected: all tests pass, bundle is current, and JavaScript parses.

- [ ] **Step 2: Serve the real static dashboard**

```bash
python3 -m http.server 8765
```

Open `http://127.0.0.1:8765/tvl/` in a browser.

- [ ] **Step 3: Verify the TVL interaction and computed layout**

Confirm:

- one `Dolomite Supply & TVL` card exists;
- Total Supply is visible by default;
- the active switch has `aria-pressed="true"`;
- clicking TVL hides supply and reveals TVL;
- the main and mini-chart paths differ between metrics;
- clicking back restores supply;
- `getComputedStyle()` reports aligned 34px switch height, visible gold focus
  treatment, and no horizontal overflow at desktop and mobile widths.

- [ ] **Step 4: Verify the EARN markup**

Open `http://127.0.0.1:8765/earn/`, render or inspect the summary runtime, and
confirm that Portfolio Value is followed directly by the three chip statuses
with no `N supply · N borrow` sentence.

- [ ] **Step 5: Review the final diff and push**

```bash
git diff dolomite-dashboard/master...HEAD --check
git status --short
git push -u dolomite-dashboard codex/merge-supply-tvl-chart:master
```

Expected: no whitespace errors, only scoped files are changed, and the latest
commit is pushed to production `master`.
