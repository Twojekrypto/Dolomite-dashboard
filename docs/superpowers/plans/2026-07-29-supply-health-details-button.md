# Supply Pool Health Details Button Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Supply Pool Health 30D change into expanded details and replace the chevron-only control with the Dolomite Assets Details pill.

**Architecture:** Keep the existing static HTML/CSS/JS component and update its single table contract from nine to eight columns. Reuse the Assets button markup and visual dimensions without introducing shared dependencies or new runtime state.

**Tech Stack:** Static HTML, CSS, browser JavaScript, Python `unittest`, Node.js test runner, Playwright browser QA.

## Global Constraints

- Preserve the existing Graphite + Gold visual identity.
- Keep the ten-row, 662px internal table viewport stable.
- Keep Chain, Asset, Quality, and Details visible at 840px and below.
- Audit every `nth-child` selector and every table `colspan`.
- Use no new dependencies, configuration, or generated data.

---

### Task 1: Lock the eight-column and Details interaction contract

**Files:**
- Modify: `tests/test_supply_pool_health_contracts.py`

**Interfaces:**
- Consumes: `tvl-preview.html`, `tvl/supply-health.js`, and `tvl/supply-health.css`
- Produces: regression coverage for the eight-column table, relocated 30D metric, Assets-style pill, and responsive column contract

- [ ] **Step 1: Write the failing test**

Change the existing column-contract test so it requires:

```python
self.assertNotIn('data-health-sort="supply30dPct"', html)
self.assertIn("30D Supply Change", source)
self.assertIn('<td colspan="8">', source)
self.assertIn('<span>${expanded ? "Hide" : "Details"}</span>', source)
self.assertIn('<polyline points="6 9 12 15 18 9"/>', source)
```

Also update the asserted centered and narrow-screen `nth-child` selectors from
nine columns to eight.

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests/test_supply_pool_health_contracts.py
```

Expected: failure because the table still renders `30D Change`, uses
`colspan="9"`, and has a chevron-only square control.

- [ ] **Step 3: Commit the red test**

```bash
git add tests/test_supply_pool_health_contracts.py
git commit -m "test: define Supply Health details contract"
```

### Task 2: Implement the compact table and Assets-style Details pill

**Files:**
- Modify: `tvl-preview.html`
- Modify: `tvl/supply-health.js`
- Modify: `tvl/supply-health.css`

**Interfaces:**
- Consumes: `market.growth.supply30dPct` and existing row expansion state
- Produces: eight-column collapsed rows and a `Details`/`Hide` pill

- [ ] **Step 1: Remove the 30D table header and cell**

Delete the `supply30dPct` header and collapsed data cell. Keep the underlying
data and formatter untouched.

- [ ] **Step 2: Move 30D change into Market Signals**

Insert this detail metric before the existing signals:

```javascript
{
  label: '30D Supply Change',
  value: formatHealthSignedPct(growth.supply30dPct),
  tone: healthSignedClass(growth.supply30dPct),
  featured: true,
}
```

Render `featured` as `supply-health-detail-stat featured` so it spans the
two-column signal grid.

- [ ] **Step 3: Replace the square chevron with the Assets pill**

Render:

```javascript
<button type="button" class="supply-health-row-toggle" ...>
  <span>${expanded ? 'Hide' : 'Details'}</span>
  <svg viewBox="0 0 24 24" ...><polyline points="6 9 12 15 18 9"/></svg>
</button>
```

Copy the Assets control geometry and interaction styling under the
Supply Health card scope.

- [ ] **Step 4: Update all column-dependent layout**

Change detail/empty/spacer colspans to eight. Rebalance desktop widths and
update centered/mobile selectors so mobile retains columns 1, 2, 7, and 8.

- [ ] **Step 5: Bump Supply Health CSS and JS cache keys**

Use a deterministic `20260729-details-button` suffix in `tvl-preview.html`.

- [ ] **Step 6: Run targeted tests**

Run:

```bash
python3 -m unittest tests/test_supply_pool_health_contracts.py
node --test tests/supply-pool-health.test.js
```

Expected: all targeted tests pass.

### Task 3: Verify layout and publish

**Files:**
- No additional production files

**Interfaces:**
- Consumes: locally served TVL route
- Produces: browser measurements and production deployment

- [ ] **Step 1: Run browser QA**

Serve with:

```bash
python3 -m http.server 8765
```

Verify at desktop and 820px:

```javascript
const table = document.querySelector('#supply-health-table');
const button = document.querySelector('.supply-health-row-toggle');
({
  columns: table.tHead.rows[0].cells.length,
  overflow: table.scrollWidth <= table.parentElement.clientWidth,
  buttonHeight: button.getBoundingClientRect().height,
  buttonWidth: button.getBoundingClientRect().width,
  label: button.textContent.trim(),
  background: getComputedStyle(button).backgroundColor,
});
```

Expected: 8 columns, no horizontal overflow, 24px-high compact button with
visible `Details`, and responsive visibility limited to Chain, Asset, Quality,
and Details at 820px. Expand a row and confirm `30D Supply Change` is visible
and the button changes to `Hide`.

- [ ] **Step 2: Run final relevant regression suites**

```bash
python3 -m unittest tests/test_supply_pool_health_contracts.py tests/test_supply_table_ux_contracts.py
node --test tests/supply-pool-health.test.js tests/token-icon-presentation-contract.test.js
```

Expected: all tests pass.

- [ ] **Step 3: Commit and push production**

```bash
git add tvl-preview.html tvl/supply-health.js tvl/supply-health.css
git commit -m "refine Supply Health details"
git push dolomite-dashboard HEAD:master
```

- [ ] **Step 4: Verify GitHub Pages**

Wait for the newest Pages workflow containing the pushed commit to succeed,
then load `/tvl/?v=20260729-details-button&t=<timestamp>` and confirm the new
cache marker plus live eight-column UI.
