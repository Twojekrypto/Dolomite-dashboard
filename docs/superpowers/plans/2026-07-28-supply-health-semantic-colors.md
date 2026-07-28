# Supply Pool Health Semantic Colors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add restrained semantic colours to selected Supply Pool Health values while preserving the existing Graphite + Gold table hierarchy.

**Architecture:** A pure JavaScript helper maps the existing concentration methodology to CSS classes. The renderer attaches those classes to current cells, while card-scoped CSS owns the palette and the existing data pipeline remains unchanged.

**Tech Stack:** Static HTML, scoped CSS, browser JavaScript, Node test runner, Python contract tests, GitHub Pages.

## Global Constraints

- Keep `Supply` and `Average` white.
- Use muted blue for `Suppliers`.
- Use the existing methodology breakpoints for `Top 10` and `Largest`.
- Preserve the existing signed colours for `30D Change` and grade colours for `Quality`.
- Do not change data generation, sorting, pagination, column widths, or column order.
- Colour must remain supporting information; the numeric percentage must stay visible.

---

### Task 1: Semantic classes and scoped palette

**Files:**
- Modify: `tests/supply-pool-health.test.js`
- Modify: `tests/test_supply_pool_health_contracts.py`
- Modify: `tvl/supply-health.js`
- Modify: `tvl/supply-health.css`
- Modify: `tvl-preview.html`
- Modify: `tvl/index.html`

**Interfaces:**
- Consumes: `market.top10Pct`, `market.largestPct`, and the existing row renderer.
- Produces: `healthConcentrationClass(metric, value) -> string`, returning `health-concentration-low`, `health-concentration-moderate`, `health-concentration-high`, or an empty string.

- [x] **Step 1: Write the failing helper tests**

```js
assert.equal(healthConcentrationClass('top10', 40), 'health-concentration-low');
assert.equal(healthConcentrationClass('top10', 60), 'health-concentration-moderate');
assert.equal(healthConcentrationClass('top10', 61), 'health-concentration-high');
assert.equal(healthConcentrationClass('largest', 20), 'health-concentration-low');
assert.equal(healthConcentrationClass('largest', 40), 'health-concentration-moderate');
assert.equal(healthConcentrationClass('largest', 41), 'health-concentration-high');
assert.equal(healthConcentrationClass('largest', null), '');
```

- [x] **Step 2: Run the focused test and verify red**

Run: `node --test tests/supply-pool-health.test.js`

Expected: FAIL because `healthConcentrationClass` is not exported.

- [x] **Step 3: Implement the pure helper and row classes**

```js
function healthConcentrationClass(metric, value) {
  if (
    value == null
    || String(value).trim() === ''
    || !Number.isFinite(Number(value))
  ) return '';
  const numeric = Number(value);
  const lowMax = metric === 'largest' ? 20 : 40;
  const moderateMax = metric === 'largest' ? 40 : 60;
  if (numeric <= lowMax) return 'health-concentration-low';
  if (numeric <= moderateMax) return 'health-concentration-moderate';
  return 'health-concentration-high';
}
```

Attach `health-participation` to Suppliers and attach the helper result to the
Top 10 and Largest cells.

- [x] **Step 4: Add the scoped palette and CSS contract**

Define `--health-participation`, `--health-concentration-low`,
`--health-concentration-moderate`, and `--health-concentration-high` on
`#supply-health-card`. Add card-scoped selectors for the new cell classes and
assert their presence in `tests/test_supply_pool_health_contracts.py`.

- [x] **Step 5: Bump static asset versions**

Update the Supply Health CSS and JS query versions in `tvl-preview.html`, then
append the same semantic-colour marker to the TVL route version in
`tvl/index.html`.

- [x] **Step 6: Run focused and full verification**

Run:

```bash
node --test tests/supply-pool-health.test.js
python3 -m unittest tests.test_supply_pool_health_contracts
python3 run_earn_audit_checks.py
```

Expected: all tests pass.

- [x] **Step 7: Verify browser-computed behaviour**

Serve with `python3 -m http.server 8765`, open `/tvl/`, and confirm:

- Suppliers uses the muted blue computed colour.
- Top 10 and Largest match the expected low/moderate/high classes and colours.
- Average and Supply remain `var(--fg-1)`.
- Columns 3–7 remain centered with no horizontal overflow.
- Sorting Top 10 and Largest retains the correct classes after rerender.

- [ ] **Step 8: Commit and deploy**

```bash
git add docs/superpowers/specs/2026-07-28-supply-health-semantic-colors-design.md \
  docs/superpowers/plans/2026-07-28-supply-health-semantic-colors.md \
  tests/supply-pool-health.test.js tests/test_supply_pool_health_contracts.py \
  tvl/supply-health.js tvl/supply-health.css tvl-preview.html tvl/index.html
git commit -m "feat: add semantic supply health colors"
git push dolomite-dashboard HEAD:master
```

Wait for GitHub Pages, then repeat the browser-computed checks on the live
cache-busted TVL route.
