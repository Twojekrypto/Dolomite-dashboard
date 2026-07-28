# Supply Health Concentration Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Top 10` and `Largest` visually consistent by using one stable concentration color while retaining concentration levels in hover explanations.

**Architecture:** Keep concentration thresholds as a pure JavaScript semantic helper, but stop returning CSS tone classes from it. Rendering will apply one card-scoped class to both concentration columns and attach unified `data-tip` explanations built from the semantic level.

**Tech Stack:** Static HTML, scoped CSS, browser JavaScript, Node test runner, Python `unittest`, Playwright Chromium.

## Global Constraints

- `Top 10` and `Largest` use one shared, subdued warm-gold text color in every data row.
- Low, moderate, and high concentration are not encoded by changing the number color.
- Thresholds remain unchanged: `Top 10` is 40%/60%; `Largest` is 20%/40%.
- Data source, score calculation, sorting, pagination, filters, and expanded rows do not change.
- The shared `data-tip` tooltip system is reused; no new dependency or tooltip implementation is introduced.

---

### Task 1: Preserve concentration semantics without color classes

**Files:**
- Modify: `tests/supply-pool-health.test.js`
- Modify: `tvl/supply-health.js`

**Interfaces:**
- Produces: `healthConcentrationLevel(metric: "top10" | "largest", value: unknown): "low" | "moderate" | "high" | ""`
- Produces: `formatHealthConcentrationTip(metric: "top10" | "largest", value: unknown): string`

- [ ] **Step 1: Replace the class test with failing semantic and tooltip tests**

```js
assert.equal(healthConcentrationLevel('top10', 40), 'low');
assert.equal(healthConcentrationLevel('top10', 60.01), 'high');
assert.equal(healthConcentrationLevel('largest', 40), 'moderate');
assert.equal(healthConcentrationLevel('largest', null), '');
assert.equal(
  formatHealthConcentrationTip('largest', 40.01),
  'Largest supplier concentration: 40.0% · High',
);
```

- [ ] **Step 2: Run the focused Node test and verify the new API is missing**

Run: `node --test tests/supply-pool-health.test.js`

Expected: FAIL because `healthConcentrationLevel` and
`formatHealthConcentrationTip` are not exported.

- [ ] **Step 3: Implement the pure semantic helpers**

```js
function healthConcentrationLevel(metric, value) {
  const numeric = Number(value);
  if (value == null || String(value).trim() === '' || !Number.isFinite(numeric)) return '';
  const lowMax = metric === 'largest' ? 20 : metric === 'top10' ? 40 : null;
  const moderateMax = metric === 'largest' ? 40 : metric === 'top10' ? 60 : null;
  if (lowMax == null || moderateMax == null) return '';
  if (numeric <= lowMax) return 'low';
  if (numeric <= moderateMax) return 'moderate';
  return 'high';
}

function formatHealthConcentrationTip(metric, value) {
  const level = healthConcentrationLevel(metric, value);
  if (!level) return '';
  const label = metric === 'largest' ? 'Largest supplier' : 'Top 10 suppliers';
  return `${label} concentration: ${formatHealthPct(value)} · ${level[0].toUpperCase()}${level.slice(1)}`;
}
```

Export both helpers from the existing CommonJS test export block.

- [ ] **Step 4: Re-run the focused Node test**

Run: `node --test tests/supply-pool-health.test.js`

Expected: PASS.

### Task 2: Apply one stable column tone and unified hover explanations

**Files:**
- Modify: `tests/test_supply_pool_health_contracts.py`
- Modify: `tvl/supply-health.js`
- Modify: `tvl/supply-health.css`
- Modify: `tvl-preview.html`

**Interfaces:**
- Consumes: `formatHealthConcentrationTip(metric, value)` from Task 1.
- Produces: concentration cells with `class="num health-concentration"` and escaped `data-tip` text.

- [ ] **Step 1: Write a failing presentation contract**

Update the semantic-color contract to require:

```python
self.assertIn('class="num health-concentration"', source)
self.assertIn("formatHealthConcentrationTip('top10', market.top10Pct)", source)
self.assertIn("formatHealthConcentrationTip('largest', market.largestPct)", source)
self.assertIn(
    "#supply-health-card .supply-health-table tbody td.health-concentration",
    styles,
)
self.assertNotIn("health-concentration-low", styles)
self.assertNotIn("health-concentration-moderate", styles)
self.assertNotIn("health-concentration-high", styles)
```

- [ ] **Step 2: Run the Python contract and verify it fails on the old palette**

Run: `python3 -m unittest tests.test_supply_pool_health_contracts`

Expected: FAIL because the current rendering and CSS still use threshold tone
classes.

- [ ] **Step 3: Replace the palette with one scoped accent**

In `tvl/supply-health.css`, define one muted warm concentration token and one
cell selector:

```css
#supply-health-card {
  --health-participation: var(--yield);
  --health-concentration: #c2b489;
}

#supply-health-card .supply-health-table tbody td.health-concentration {
  color: var(--health-concentration);
}
```

Remove the three obsolete concentration variables and selectors.

- [ ] **Step 4: Attach semantic tooltips without changing table behavior**

Render both cells with the stable class and escaped tooltip copy:

```js
<td class="num health-concentration"
    data-tip="${escapeHealthHtml(formatHealthConcentrationTip('top10', market.top10Pct))}">
  ${formatHealthPct(market.top10Pct)}
</td>
```

Use the equivalent `largest` call for the `Largest` cell. Remove
`top10Tone`/`largestTone`.

- [ ] **Step 5: Bump static asset versions**

Change both Supply Pool Health URLs in `tvl-preview.html` to:

```html
<link rel="stylesheet" href="tvl/supply-health.css?v=20260728-concentration-parity">
<script defer src="tvl/supply-health.js?v=20260728-concentration-parity"></script>
```

- [ ] **Step 6: Run targeted automated verification**

Run:

```bash
node --check tvl/supply-health.js
node --test tests/supply-pool-health.test.js
python3 -m unittest tests.test_supply_pool_health_contracts tests.test_data_freshness_surface_contracts
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 7: Verify computed presentation in Chromium**

Serve with `python3 -m http.server 8765`, open
`http://127.0.0.1:8765/tvl/?v=concentration-parity`, wait for Supply Pool Health
rows, and verify:

```js
const top10 = [...document.querySelectorAll('#supply-health-table tbody .supply-health-row td:nth-child(5)')];
const largest = [...document.querySelectorAll('#supply-health-table tbody .supply-health-row td:nth-child(6)')];
new Set([...top10, ...largest].map(cell => getComputedStyle(cell).color)).size === 1;
```

Hover a `Largest` cell and assert `#unified-tooltip` contains `concentration`
and one of `Low`, `Moderate`, or `High`. Compare table bounding boxes before
and after hover to confirm no layout change.

- [ ] **Step 8: Review, commit, and deploy**

Run correctness/regression and maintainability/security diff reviews, then:

```bash
git add tvl/supply-health.js tvl/supply-health.css tvl-preview.html \
  tests/supply-pool-health.test.js tests/test_supply_pool_health_contracts.py \
  docs/superpowers/plans/2026-07-28-supply-health-concentration-parity.md
git commit -m "fix: simplify supply concentration colors"
git push dolomite-dashboard HEAD:master
```

Verify the cache-busted live URL and the final remote SHA.
