# Revenue Section Header Hierarchy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give five Revenue sections a shared title/freshness row, full-width divider, and subtitle/controls row while matching the DOLO hero freshness treatment and remaining responsive from desktop to mobile.

**Architecture:** Keep the static-page architecture and implement the hierarchy inside `revenue-preview.html` with shared semantic classes and one shared freshness update path. Protect the contract with source-level regression tests, retain all existing control IDs and event bindings, and cache-bust the Revenue route only after the new markup and CSS pass browser verification.

**Tech Stack:** Static HTML, CSS, vanilla JavaScript, Node `assert`, Python `unittest`, GitHub Pages.

## Global Constraints

- Apply the shared hierarchy to Protocol Revenue by Chain, Dolomite Revenue Over Time, Borrow Interest Over Time, veDOLO Borrow Fee Rebates, and Current discount users.
- Primary row: title left and `Data updated · … ago` right.
- Full-width one-pixel divider immediately below the primary row.
- Secondary row: subtitle left and existing section-specific controls right.
- Reuse the current Revenue generation timestamp and English relative-age formatter; do not change data calculations.
- Match the Dolomite Revenue hero freshness typography, color, dot, and spacing to the DOLO hero.
- Preserve the Graphite + Gold design and all current chart, brush, table, simulation, and filter behavior.
- At mobile widths, stack content without horizontal document overflow.

---

### Task 1: Lock the shared header contract with failing tests

**Files:**
- Modify: `tests/table-surface-consistency.test.js`
- Modify: `tests/test_fetch_dolomite_revenue.py`

**Interfaces:**
- Consumes: static source from `revenue-preview.html` and `dolo-preview.html`.
- Produces: regression assertions for `.revenue-section-primary`, `.revenue-section-secondary`, `[data-revenue-updated]`, responsive CSS, and DOLO/Revenue hero freshness parity.

- [ ] **Step 1: Add a failing structural test**

Add assertions to `tests/table-surface-consistency.test.js` that require five shared headers and their order:

```js
assert.strictEqual(
  (revenue.match(/class="panel-head revenue-section-head/g) || []).length,
  5,
  'All five Revenue sections should use the shared header hierarchy',
);
assert.strictEqual(
  (revenue.match(/class="revenue-section-primary"/g) || []).length,
  5,
  'Each Revenue section should expose a title/freshness row',
);
assert.strictEqual(
  (revenue.match(/class="revenue-section-secondary"/g) || []).length,
  5,
  'Each Revenue section should expose a subtitle/controls row',
);
assert.strictEqual(
  (revenue.match(/data-revenue-updated/g) || []).length,
  5,
  'Each Revenue section should expose one freshness target',
);
assert(
  revenue.includes('.revenue-section-primary{') &&
    /\.revenue-section-primary\{[^}]*border-bottom:1px solid var\(--line-2\)/s.test(revenue),
  'The full-width divider should belong to the primary row',
);
```

Add a Python contract in `tests/test_fetch_dolomite_revenue.py` that checks the shared updater and exact relative copy:

```python
self.assertIn('document.querySelectorAll("[data-revenue-updated]")', html)
self.assertIn('Data updated · ${dataAgeLabel(revenueData.generatedAt)}', html)
self.assertEqual(html.count('data-revenue-updated'), 5)
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
node tests/table-surface-consistency.test.js
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_revenue_panel_headers_use_the_holders_table_divider
```

Expected: both targeted contracts fail because the five primary/secondary rows and common freshness targets do not exist yet.

- [ ] **Step 3: Commit the failing contract**

```bash
git add tests/table-surface-consistency.test.js tests/test_fetch_dolomite_revenue.py
git commit -m "test: define Revenue header hierarchy"
```

### Task 2: Implement the shared hierarchy and freshness updater

**Files:**
- Modify: `revenue-preview.html`

**Interfaces:**
- Consumes: existing IDs and handlers for date controls, series toggles, simulation, mode toggles, and current-discount metadata.
- Produces: `.revenue-section-primary`, `.revenue-section-secondary`, `.revenue-section-updated`, and five `[data-revenue-updated]` targets updated by `renderHero()`.

- [ ] **Step 1: Add the shared CSS hierarchy**

Replace the section-specific bottom-border approach with shared rows:

```css
.panel-head.revenue-section-head{display:block;padding:0;border-bottom:0}
.revenue-section-primary{
  display:flex;align-items:center;justify-content:space-between;gap:16px;
  padding:20px 24px 18px;border-bottom:1px solid var(--line-2)
}
.revenue-section-secondary{
  display:flex;align-items:center;justify-content:space-between;gap:18px;
  padding:14px 24px 16px
}
.revenue-section-updated{
  display:inline-flex;align-items:center;gap:6px;white-space:nowrap;
  color:var(--fg-3);font-family:var(--mono);font-size:11px
}
.revenue-section-updated .dot{
  width:6px;height:6px;border-radius:50%;background:var(--gold);
  box-shadow:0 0 8px var(--gold)
}
```

At `max-width:1080px`, stack the secondary content and stretch its controls. At `max-width:720px`, stack the primary row, add an 8px freshness offset, reduce horizontal padding to 16px, and keep all controls within the panel width.

- [ ] **Step 2: Restructure all five section headers**

For every scoped section, preserve the existing control markup and IDs but move them into this shared shape:

```html
<div class="panel-head revenue-section-head">
  <div class="revenue-section-primary">
    <div class="panel-title">Section title</div>
    <div class="revenue-section-updated" data-revenue-updated>
      <span class="dot"></span><span>Loading data update…</span>
    </div>
  </div>
  <div class="revenue-section-secondary">
    <div class="panel-sub">Section description.</div>
    <div><!-- existing controls --></div>
  </div>
</div>
```

Remove the `chainDataUpdated` ID because its only caller will be replaced by the shared data-attribute updater. Do not rename date, chart, brush, toggle, simulation, or current-discount metadata IDs.

- [ ] **Step 3: Update all five freshness labels from one code path**

In `renderHero()`, retain the hero audit-state handling and add:

```js
document.querySelectorAll("[data-revenue-updated]").forEach(el => {
  const text = el.querySelector("span:last-child");
  if (text) text.textContent = `Data updated · ${dataAgeLabel(revenueData.generatedAt)}`;
});
```

Remove the one-off `chainDataUpdated.textContent` assignment; source inspection shows it is the only JavaScript reference to that ID.

- [ ] **Step 4: Run the targeted tests and verify GREEN**

Run:

```bash
node tests/table-surface-consistency.test.js
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_revenue_panel_headers_use_the_holders_table_divider
```

Expected: both targeted contracts pass with zero failures.

- [ ] **Step 5: Commit the implementation**

```bash
git add revenue-preview.html
git commit -m "Polish Revenue section header hierarchy"
```

### Task 3: Cache-bust, verify responsive rendering, and publish

**Files:**
- Modify: `revenue/index.html`
- Modify: `tests/test_fetch_dolomite_revenue.py`

**Interfaces:**
- Consumes: the final `revenue-preview.html` hierarchy and the route loader version contract.
- Produces: a uniquely versioned Revenue route and verified GitHub Pages deployment.

- [ ] **Step 1: Bump the Revenue route version**

Append `-header-hierarchy-20260803` to the current `version` value in `revenue/index.html` and update only the exact corresponding test assertion.

- [ ] **Step 2: Run source, syntax, and regression checks**

Run:

```bash
git diff --check
node tests/table-surface-consistency.test.js
python3 -m unittest tests.test_fetch_dolomite_revenue
npm run check:earn-audit
```

Expected: all commands exit 0 and report no failures.

- [ ] **Step 3: Start the static site and verify four viewport classes**

Run the reviewed literal server command:

```bash
python3 -m http.server 8765
```

Inspect `http://127.0.0.1:8765/revenue/?v=header-hierarchy-20260803` at:

- 1440 × 1000 — wide desktop
- 1024 × 768 — laptop/small desktop
- 768 × 1024 — tablet portrait
- 390 × 844 — mobile

For every viewport, verify:

```js
({
  overflow: document.documentElement.scrollWidth > document.documentElement.clientWidth,
  headers: [...document.querySelectorAll('.revenue-section-head')].map(head => ({
    primaryWidth: head.querySelector('.revenue-section-primary').getBoundingClientRect().width,
    dividerWidth: getComputedStyle(head.querySelector('.revenue-section-primary')).borderBottomWidth,
    title: head.querySelector('.panel-title').textContent.trim(),
    freshness: head.querySelector('[data-revenue-updated] span:last-child').textContent.trim(),
  })),
})
```

Expected: `overflow` is false, there are five headers, every divider is `1px`, all freshness labels begin with `Data updated ·`, and no title, subtitle, or control is clipped.

- [ ] **Step 4: Compare hero freshness with DOLO computed styles**

At the same desktop viewport, compare `.hero-live` on `/revenue/` and `/dolo/` for `fontFamily`, `fontSize`, `color`, `display`, `alignItems`, and `gap`; compare `.hero-live .dot` for width, height, background color, and box shadow. Expected: all compared values match.

- [ ] **Step 5: Perform two review passes**

Correctness/regression pass: confirm all five sections follow primary → divider → secondary → content and that all existing controls still respond.

Maintainability/security pass: confirm the change adds no dependencies, configuration, secrets, inline event changes, or unrelated refactors.

- [ ] **Step 6: Commit the version bump**

```bash
git add revenue/index.html tests/test_fetch_dolomite_revenue.py
git commit -m "Deploy Revenue header hierarchy"
```

- [ ] **Step 7: Publish to production and verify Pages**

Push the implementation commits to `master`, wait for the triggered `Deploy GitHub Pages` run to succeed, then load the public route with a unique query parameter and repeat the five-header/overflow computed-style check.
