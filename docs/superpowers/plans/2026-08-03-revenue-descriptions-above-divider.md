# Revenue Descriptions Above Divider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every Revenue section description directly below its title and above the full-width separator, while keeping section freshness and controls aligned and placing the live Berachain wallet/range summary immediately beside the shortened `Current discount` title.

**Architecture:** Keep the existing five-panel shared header contract in `revenue-preview.html`, but split the primary row into a left copy stack (`title line + description`) and the existing right freshness label. The title line supports optional inline metadata, used only by `Current discount`; the separator remains the primary row's bottom border, while the optional secondary row becomes controls-only.

**Tech Stack:** Static HTML, CSS, vanilla JavaScript, Node `assert`, Python `unittest`, GitHub Pages.

## Global Constraints

- Apply the layout to `Protocol Revenue by Chain`, `Dolomite Revenue Over Time`, `Borrow Interest Over Time`, `veDOLO Borrow Fee Rebates`, and the renamed `Current discount` section.
- Every explanatory description must appear directly below its section title and above the separator.
- Keep `Data updated · … ago` in the primary header area on the right at desktop widths.
- Keep the one-pixel separator full width and directly below the title/description/freshness block.
- Keep existing date controls, series toggles, mode toggles, and simulation controls below the separator.
- Rename `Current discount users` to `Current discount`.
- Keep `veBorrowDiscountUsersMeta` dynamic and render `148 Berachain wallets · 75D`-style copy immediately beside `Current discount`, above the separator.
- Preserve every current control ID, event handler, chart behavior, dataset calculation, and English relative-age formatter.
- Reuse the Graphite + Gold design tokens; add no new dependency or configuration.
- At narrow widths, let the inline Current discount metadata wrap below the title only when necessary, without clipping or horizontal document overflow.

---

### Task 1: Lock the revised header hierarchy with failing source contracts

**Files:**
- Modify: `tests/table-surface-consistency.test.js`
- Modify: `tests/test_fetch_dolomite_revenue.py`

**Interfaces:**
- Consumes: the static five-section markup and shared Revenue header CSS in `revenue-preview.html`.
- Produces: regression assertions for five title/description stacks, four controls-only secondary rows, divider placement, and the inline dynamic discount metadata hook.

- [ ] **Step 1: Add a failing structural contract to the Node test**

Extend the existing Revenue block in `tests/table-surface-consistency.test.js` with exact source assertions:

```js
assert.strictEqual(
  (revenue.match(/class="revenue-section-copy"/g) || []).length,
  5,
  'Each Revenue section should group its title and description above the divider',
);
assert.strictEqual(
  (revenue.match(/class="revenue-section-title-line"/g) || []).length,
  5,
  'Each Revenue section should expose one title line above the divider',
);
assert.strictEqual(
  (revenue.match(/class="revenue-section-tools"/g) || []).length,
  4,
  'Only the four sections with controls should expose a row below the divider',
);
assert(
  /class="revenue-section-copy">\s*<div class="revenue-section-title-line">\s*<div class="panel-title">Protocol Revenue by Chain<\/div>\s*<\/div>\s*<div class="panel-sub">Chain breakdown of net protocol-retained revenue for selected date range\.<\/div>/s.test(revenue),
  'Protocol Revenue description should sit directly below its title',
);
assert(
  /class="revenue-section-title-line">\s*<div class="panel-title">Current discount<\/div>\s*<div class="veborrow-discount-meta" id="veBorrowDiscountUsersMeta">/s.test(revenue),
  'Current discount should keep the live wallet/range metadata directly beside its title',
);
```

- [ ] **Step 2: Add a failing `Current discount` contract to the Python test**

Extend `test_revenue_panel_headers_use_the_holders_table_divider` in `tests/test_fetch_dolomite_revenue.py`:

```python
self.assertEqual(html.count('class="revenue-section-copy"'), 5)
self.assertEqual(html.count('class="revenue-section-title-line"'), 5)
self.assertEqual(html.count('class="revenue-section-secondary revenue-section-tools"'), 4)
self.assertRegex(
    html,
    r'<div class="panel-title">Current discount</div>\s*<div class="veborrow-discount-meta" id="veBorrowDiscountUsersMeta">',
)
self.assertNotIn('<div class="panel-title">Current discount users</div>', html)
self.assertIn(
    '${Math.round(latestDaily).toLocaleString("en-US")} Berachain wallets · ${model.length.toLocaleString("en-US")}D',
    html,
)
```

- [ ] **Step 3: Run the two targeted tests and confirm RED**

Run:

```bash
node tests/table-surface-consistency.test.js
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_revenue_panel_headers_use_the_holders_table_divider
```

Expected: both commands fail because `.revenue-section-copy`, `.revenue-section-title-line`, and the inline Current discount metadata arrangement are not present yet.

- [ ] **Step 4: Commit the failing contract**

```bash
git add tests/table-surface-consistency.test.js tests/test_fetch_dolomite_revenue.py
git commit -m "test: define Revenue copy hierarchy"
```

### Task 2: Move descriptions above the divider in all five sections

**Files:**
- Modify: `revenue-preview.html`

**Interfaces:**
- Consumes: `.revenue-section-primary`, `.revenue-section-secondary`, `.panel-title`, `.panel-sub`, `.revenue-section-updated`, and every existing section control wrapper.
- Produces: `.revenue-section-copy` for the title/description stack, `.revenue-section-title-line` for optional inline title metadata, and `.revenue-section-tools` for the four controls-only rows.

- [ ] **Step 1: Change the shared CSS contract**

Update the shared header rules without changing the visual tokens:

```css
.revenue-section-primary{
  display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:20px;
  padding:20px 24px 18px;border-bottom:1px solid var(--line-2)
}
.revenue-section-copy{min-width:0}
.revenue-section-title-line{display:flex;align-items:center;gap:10px;flex-wrap:wrap;min-width:0}
.revenue-section-copy .panel-sub{margin-top:5px;max-width:680px}
.revenue-section-tools{
  display:flex;align-items:center;justify-content:flex-end;gap:18px;
  min-height:58px;padding:12px 24px 14px
}
```

Keep `.revenue-section-updated` unchanged so it continues to match the established DOLO freshness treatment.

- [ ] **Step 2: Restructure the Protocol Revenue header**

Use this exact semantic order while preserving every date-control ID:

```html
<div class="panel-head revenue-section-head chain-panel-head">
  <div class="revenue-section-primary">
    <div class="revenue-section-copy">
      <div class="revenue-section-title-line">
        <div class="panel-title">Protocol Revenue by Chain</div>
      </div>
      <div class="panel-sub">Chain breakdown of net protocol-retained revenue for selected date range.</div>
    </div>
    <div class="revenue-section-updated" id="chainDataUpdated" data-revenue-updated>…</div>
  </div>
  <div class="revenue-section-secondary revenue-section-tools">
    <div class="chain-range-tools" aria-label="Protocol Revenue by Chain date range">…</div>
  </div>
</div>
```

- [ ] **Step 3: Apply the same title/description stack to the other four headers**

For `Dolomite Revenue Over Time`, `Borrow Interest Over Time`, `veDOLO Borrow Fee Rebates`, and `Current discount`:

1. Wrap `.panel-title` in `.revenue-section-title-line`.
2. Move the existing `.panel-sub` inside `.revenue-section-copy`, immediately after `.revenue-section-title-line`.
3. Keep `[data-revenue-updated]` as the second child of `.revenue-section-primary`.
4. Add `revenue-section-tools` to the four existing `.revenue-section-secondary` rows that contain real controls.
5. Leave the existing control wrappers and IDs unchanged.

The resulting `Current discount` primary copy must retain the live metadata node directly beside the title and omit the now-unnecessary secondary row:

```html
<div class="revenue-section-primary">
  <div class="revenue-section-copy">
    <div class="revenue-section-title-line">
      <div class="panel-title">Current discount</div>
      <div class="veborrow-discount-meta" id="veBorrowDiscountUsersMeta">Berachain active discount</div>
    </div>
    <div class="panel-sub">Daily Berachain wallets using discount from current debt and veDOLO vote weight.</div>
  </div>
  <div class="revenue-section-updated" data-revenue-updated>…</div>
</div>
```

In `renderVeBorrowDiscountUsersChart`, shorten the dynamic copy to:

```js
meta.textContent = model.length
  ? `${Math.round(latestDaily).toLocaleString("en-US")} Berachain wallets · ${model.length.toLocaleString("en-US")}D`
  : "Berachain active discount";
```

This renders the requested title line as `Current discount  148 Berachain wallets · 75D`.

- [ ] **Step 4: Adjust responsive rules**

At `max-width:1080px`, keep title/description and freshness on one row when they fit, but allow every controls toolbar to use the full panel width:

```css
@media (max-width:1080px){
  .revenue-section-tools{justify-content:flex-end}
  .revenue-section-tools .revenue-series-control,
  .revenue-section-tools .chain-range-tools,
  .revenue-section-tools .veborrow-panel-actions{width:100%}
}
```

At `max-width:720px`, stack the freshness under the description, keep controls full width, and let the inline Current discount metadata wrap naturally:

```css
@media (max-width:720px){
  .revenue-section-primary{display:block;padding:18px 16px 14px}
  .revenue-section-updated{margin-top:9px}
  .revenue-section-tools{display:block;min-height:0;padding:13px 16px 16px}
  .revenue-section-title-line{column-gap:8px;row-gap:7px}
}
```

- [ ] **Step 5: Run targeted tests and confirm GREEN**

Run:

```bash
node tests/table-surface-consistency.test.js
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_revenue_panel_headers_use_the_holders_table_divider
```

Expected: both commands exit 0.

- [ ] **Step 6: Audit IDs and commit the implementation**

Run:

```bash
rg -n "chainDataUpdated|chainRangeFrom|chainRangeTo|veBorrowSimToggle|veBorrowDiscountUsersMeta|data-revenue-updated" revenue-preview.html
```

Expected: every existing interactive ID still has its original markup and JavaScript references, with five freshness targets. Then commit:

```bash
git add revenue-preview.html
git commit -m "Polish Revenue description hierarchy"
```

### Task 3: Version, browser-verify, and publish the revised hierarchy

**Files:**
- Modify: `revenue/index.html`
- Modify: `tests/test_fetch_dolomite_revenue.py`

**Interfaces:**
- Consumes: the completed five-header layout and existing route-loader version contract.
- Produces: a cache-busted Revenue route and a verified production deployment.

- [ ] **Step 1: Bump the Revenue route version and its exact test assertion**

Append `-copy-above-divider-20260803` to the current `version` in `revenue/index.html`, then update only the matching assertion in `tests/test_fetch_dolomite_revenue.py`.

- [ ] **Step 2: Run source and regression checks**

Run:

```bash
git diff --check
node tests/table-surface-consistency.test.js
python3 -m unittest tests.test_fetch_dolomite_revenue
npm run check:earn-audit
```

Expected: every command exits 0 with no test failures.

- [ ] **Step 3: Serve the site locally and verify four viewport classes**

Run the literal local command:

```bash
python3 -m http.server 8765
```

Inspect `/revenue/?v=copy-above-divider-20260803` at:

- `1440 × 1000` — wide desktop
- `1024 × 768` — laptop/small desktop
- `768 × 1024` — tablet portrait
- `390 × 844` — mobile

Use computed values and bounding boxes to verify the real rendered layout:

```js
({
  overflow: document.documentElement.scrollWidth > document.documentElement.clientWidth,
  headers: [...document.querySelectorAll('.revenue-section-head')].map(head => {
    const primary = head.querySelector('.revenue-section-primary');
    const title = head.querySelector('.panel-title');
    const subtitle = head.querySelector('.panel-sub');
    const tools = head.querySelector('.revenue-section-tools');
    return {
      title: title.textContent.trim(),
      titleBottom: title.getBoundingClientRect().bottom,
      subtitleTop: subtitle.getBoundingClientRect().top,
      subtitleBottom: subtitle.getBoundingClientRect().bottom,
      separatorY: primary.getBoundingClientRect().bottom,
      separatorWidth: getComputedStyle(primary).borderBottomWidth,
      toolsInsidePanel: !tools || tools.getBoundingClientRect().right <= head.getBoundingClientRect().right + 1,
    };
  }),
})
```

Expected for every section: no document overflow, title above subtitle, subtitle above separator, a `1px` separator, and every optional controls row contained inside the panel.

- [ ] **Step 4: Verify controls and dynamic metadata in the browser**

Confirm that:

- date inputs and the calendar still open and update the Protocol Revenue range;
- Revenue series toggles and Daily/Cumulative buttons still change the chart state;
- Borrow Interest Daily/Cumulative still changes the chart state;
- veDOLO simulation and mode controls still respond;
- `#veBorrowDiscountUsersMeta` renders the live `N Berachain wallets · ND` value immediately beside `Current discount` on desktop and wraps cleanly under the title only when the mobile width requires it;
- browser console and page error collections remain empty.

- [ ] **Step 5: Perform correctness and maintainability review passes**

Correctness/regression pass: confirm all five descriptions moved above the separator and no control or dynamic metadata disappeared.

Maintainability/security pass: confirm the change is limited to shared Revenue markup/CSS/tests/versioning and adds no dependencies, configuration, secrets, or unrelated refactors.

- [ ] **Step 6: Commit the route version and deploy to production**

```bash
git add revenue/index.html tests/test_fetch_dolomite_revenue.py
git commit -m "Deploy Revenue copy hierarchy"
git push dolomite-dashboard HEAD:master
```

Wait for `Deploy GitHub Pages`, `Earn Audit`, and `Secret Leak Guard` to pass. Then reload the public Revenue route with a unique query parameter and repeat the computed hierarchy/overflow check against production.
