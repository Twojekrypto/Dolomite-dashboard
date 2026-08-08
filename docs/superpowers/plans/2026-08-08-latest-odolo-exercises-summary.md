# Latest oDOLO Exercises Summary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Latest oDOLO Exercises `14D` period and a responsive five-field period summary whose totals remain independent of table search.

**Architecture:** Put numeric aggregation in a small browser-and-Node-compatible helper so formulas receive real behavior tests. Keep period filtering, rendering and responsive styling in the existing `odolo-preview.html` page, and update the oDOLO route cache version so production receives the new preview immediately.

**Tech Stack:** Static HTML/CSS/JavaScript, Node `node:test`, Python `unittest`, native Python Playwright, GitHub Pages route loader.

## Global Constraints

- Preserve the existing Graphite + Gold identity and use `#75b87b` as the Latest Exercises activity accent.
- Keep `7D` as the default selection.
- Add `14D` only to Latest oDOLO Exercises; unrelated period dropdown markup remains unchanged.
- Summary inputs are the complete selected-period USDC exercise rows before search.
- `Avg Exercise Price = total USDC Paid / total veDOLO Received`.
- `Avg Lock = sum(veDOLO × lock days) / sum(veDOLO)` across positive-veDOLO rows with a finite, non-negative lock.
- Do not change the data pipeline or the existing `isUsdcExerciseTx` qualification rule.
- Add no dependency and do not reformat unrelated parts of the monolithic preview.

## Visual direction

**Subject and job:** A DeFi analyst needs to understand recent oDOLO exercise activity before inspecting individual transactions.

**Palette:** Graphite `#09090b`, raised graphite `#141417`, protocol green `#75b87b`, Dolomite gold `#c9a227`, primary text `#f4f3ef`, muted text `#6b6a66`.

**Typography:** Inter remains the structural/UI face; JetBrains Mono remains the utility and numeric face. Labels use restrained uppercase tracking; values use tabular numerals.

**Layout:** One continuous ledger rail with five equal desktop columns, a balanced 3+2 tablet grid, and one compact mobile column.

```text
Desktop
┌ Exercises ┬ veDOLO Received ┬ USDC Paid ┬ Avg Price ┬ Avg Lock ┐
└───────────┴─────────────────┴───────────┴───────────┴──────────┘
┌ Search                                               Period 14D ┐

Tablet
┌ Exercises ┬ veDOLO Received ┬ USDC Paid ┐
├─────────── Avg Price ────────┬────────── Avg Lock ──────────────┤

Mobile
┌ Exercises       ┐
├ veDOLO Received ┤
├ USDC Paid       ┤
├ Avg Price       ┤
└ Avg Lock        ┘
```

**Signature:** A faint green activity wash runs through one uninterrupted rail; internal dividers encode metric grouping rather than creating five unrelated cards.

**Self-critique:** A generic dashboard treatment would add colorful cards, gradients and trend badges. This design removes those defaults and uses the transaction-ledger metaphor already specific to Dolomite. The sole expressive move is the protocol-green activity wash, keeping the user-requested Fresh 10K+ parity intact.

---

### Task 1: Tested exercise summary calculator

**Files:**
- Create: `odolo-exercise-summary.js`
- Create: `tests/odolo-exercise-summary.test.js`

**Interfaces:**
- Consumes: normalized rows shaped as `{addr?: string, vedolo?: number|string, usdc?: number|string, lockDays?: number|string}`.
- Produces: `summarizeLatestOdoloExercises(rows)` returning `{exercises, uniqueWallets, vedoloReceived, usdcPaid, avgExercisePrice, avgLockDays}` where the two averages are `number | null`.
- Browser export: `window.summarizeLatestOdoloExercises`.
- Node export: `module.exports.summarizeLatestOdoloExercises`.

- [ ] **Step 1: Write failing behavior tests**

Create `tests/odolo-exercise-summary.test.js` with hand-derived fixtures:

```js
const assert = require('node:assert/strict');
const test = require('node:test');
const { summarizeLatestOdoloExercises } = require('../odolo-exercise-summary.js');

test('summarizes volume and normalizes duplicate wallet casing', () => {
  const result = summarizeLatestOdoloExercises([
    {addr:'0xAbC', vedolo:100, usdc:4, lockDays:30},
    {addr:'0xabc', vedolo:300, usdc:18, lockDays:90},
  ]);
  assert.deepEqual(result, {
    exercises:2,
    uniqueWallets:1,
    vedoloReceived:400,
    usdcPaid:22,
    avgExercisePrice:0.055,
    avgLockDays:75,
  });
});

test('ignores malformed amounts and returns null averages without a valid denominator', () => {
  const result = summarizeLatestOdoloExercises([
    {addr:'0x1', vedolo:'bad', usdc:-5, lockDays:Infinity},
    {addr:'', vedolo:0, usdc:0, lockDays:0},
  ]);
  assert.equal(result.exercises, 2);
  assert.equal(result.uniqueWallets, 1);
  assert.equal(result.vedoloReceived, 0);
  assert.equal(result.usdcPaid, 0);
  assert.equal(result.avgExercisePrice, null);
  assert.equal(result.avgLockDays, null);
});
```

The first test catches wrong wallet normalization, arithmetic price averaging and unweighted lock averaging. The second catches negative/invalid totals and `NaN`/`Infinity` averages.

- [ ] **Step 2: Run the tests and verify RED**

Run: `node --test tests/odolo-exercise-summary.test.js`

Expected: FAIL because `odolo-exercise-summary.js` does not exist.

- [ ] **Step 3: Implement the smallest pure calculator**

Create `odolo-exercise-summary.js` as an IIFE. Normalize only finite non-negative numbers; accumulate price totals independently; include a lock weight only when `vedolo > 0` and `lockDays` is finite and non-negative. Export the exact interface above to both `window` and CommonJS.

```js
(function(root){
  function nonNegative(value){
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : 0;
  }

  function summarizeLatestOdoloExercises(rows){
    const source = Array.isArray(rows) ? rows : [];
    const wallets = new Set();
    let vedoloReceived = 0;
    let usdcPaid = 0;
    let lockWeightedTotal = 0;
    let lockWeight = 0;

    source.forEach(row => {
      const address = String(row?.addr || '').trim().toLowerCase();
      if(address) wallets.add(address);
      const vedolo = nonNegative(row?.vedolo);
      const usdc = nonNegative(row?.usdc);
      vedoloReceived += vedolo;
      usdcPaid += usdc;
      const lockDays = Number(row?.lockDays);
      if(vedolo > 0 && Number.isFinite(lockDays) && lockDays >= 0){
        lockWeightedTotal += vedolo * lockDays;
        lockWeight += vedolo;
      }
    });

    return {
      exercises:source.length,
      uniqueWallets:wallets.size,
      vedoloReceived,
      usdcPaid,
      avgExercisePrice:vedoloReceived > 0 ? usdcPaid / vedoloReceived : null,
      avgLockDays:lockWeight > 0 ? lockWeightedTotal / lockWeight : null,
    };
  }

  root.summarizeLatestOdoloExercises = summarizeLatestOdoloExercises;
  if(typeof module !== 'undefined' && module.exports){
    module.exports = {summarizeLatestOdoloExercises};
  }
})(typeof window !== 'undefined' ? window : globalThis);
```

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `node --test tests/odolo-exercise-summary.test.js`

Expected: 2 tests PASS with no warnings.

- [ ] **Step 5: Commit the calculator**

```bash
git add odolo-exercise-summary.js tests/odolo-exercise-summary.test.js
git commit -m "feat: calculate latest oDOLO exercise summary"
```

---

### Task 2: 14D control, summary rail and search-independent rendering

**Files:**
- Modify: `tests/test_odolo_preview_contracts.py`
- Modify: `odolo-preview.html:13-15,256-299,703-752,1427-1485,1804-1811,2069-2073,2941-3035`
- Modify: `odolo/index.html:14-18`

**Interfaces:**
- Consumes: `window.summarizeLatestOdoloExercises(periodRows)` from Task 1.
- Produces: `getLatestExerciseRows()` for full selected-period rows, `filterLatestExerciseRowsBySearch(rows, query)` for visible search results, and `renderLatestExerciseSummary(rows)` for the rail.

- [ ] **Step 1: Add failing UI contract tests**

Extend `tests/test_odolo_preview_contracts.py` with tests that:

```python
def test_latest_exercises_offers_fourteen_day_period_without_changing_default(self):
    latest_section = re.search(
        r'<section class="card latest-activity-section" id="latest-exercises-section">(?P<body>.*?)</section>',
        self.html,
        re.S,
    ).group("body")
    self.assertRegex(latest_section, r'data-period="7d".*data-period="14d".*data-period="30d"')
    self.assertIn('{key:"14d",  short:"14D",  label:"Last 14 days"}', self.html)
    self.assertIn('"14d":14', self.html)
    self.assertIn('latestPeriod: "7d"', self.html)

def test_latest_exercise_summary_precedes_toolbar_and_uses_period_rows(self):
    latest_section = re.search(
        r'<section class="card latest-activity-section" id="latest-exercises-section">(?P<body>.*?)</section>',
        self.html,
        re.S,
    ).group("body")
    self.assertLess(latest_section.index('id="latest-exercise-stats"'), latest_section.index('class="toolbar"'))
    for label in ("Exercises", "veDOLO Received", "USDC Paid", "Avg Exercise Price", "Avg Lock"):
        self.assertIn(label, self.html)
    self.assertIn('const periodRows = getLatestExerciseRows();', self.latest_render)
    self.assertIn('renderLatestExerciseSummary(periodRows);', self.latest_render)
    self.assertIn('const rows = filterLatestExerciseRowsBySearch(periodRows, state.qLatest);', self.latest_render)
    self.assertIn('`${periodRows.length.toLocaleString("en-US")} · ${periodLabel}`', self.latest_render)
```

Also require `<script src="odolo-exercise-summary.js?...">` and the new route cache-bust token.

The tests catch a missing 14-day cutoff, wrong option order, accidental default change, summary placement below controls, and search-dependent header/summary totals.

- [ ] **Step 2: Run the contract test and verify RED**

Run: `python3 -m unittest tests.test_odolo_preview_contracts`

Expected: FAIL because the `14D` option, rail, helper script and render separation are absent.

- [ ] **Step 3: Add the 14D period contract**

In `odolo-preview.html`:

- Load `odolo-exercise-summary.js` after `odolo-address-meta.js` with a dated cache query.
- Add `{key:"14d", short:"14D", label:"Last 14 days"}` between 7D and 30D in `PERIODS`.
- Add `<div class="dd-opt" data-period="14d">…<span class="dd-opt-name">14 days</span></div>` between the 7-day and 30-day Latest Exercises options only.
- Add `"14d":14` to `periodCutoff` while keeping `state.latestPeriod = "7d"`.

- [ ] **Step 4: Add the summary rail markup and styling**

Insert `<div class="latest-exercise-stats selected-market-rail" id="latest-exercise-stats" aria-live="polite"></div>` immediately after `.card-head` and before `.toolbar`.

Add scoped styles:

```css
.latest-exercise-stats{
  display:grid;grid-template-columns:repeat(5,minmax(0,1fr));
  border-bottom:1px solid rgba(117,184,123,.12);
  background:linear-gradient(90deg,rgba(117,184,123,.045),rgba(117,184,123,.012) 58%,transparent);
}
.latest-exercise-stat{min-width:0;padding:18px 20px;border-left:1px solid var(--line-1)}
.latest-exercise-stat:first-child{border-left:0}
.latest-exercise-stat .label{display:flex;align-items:center;gap:7px;font-size:10px;font-weight:800;letter-spacing:1px;text-transform:uppercase;color:var(--fg-3)}
.latest-exercise-stat .label svg{width:13px;height:13px;color:var(--latest-exercise-accent)}
.latest-exercise-stat .value{margin-top:10px;font-family:var(--mono);font-size:22px;font-weight:600;line-height:1;color:var(--fg-1);font-variant-numeric:tabular-nums;white-space:nowrap}
.latest-exercise-stat .sub{margin-top:7px;font-family:var(--mono);font-size:10.5px;color:var(--fg-3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
```

At `max-width:980px`, use six grid tracks: the first three metrics span two tracks and the final two span three tracks, with a top divider before metric four. At `max-width:640px`, make every metric span all six tracks, remove left borders, and add top borders after the first metric.

- [ ] **Step 5: Separate period rows from search and render the summary**

Remove query filtering from `getLatestExerciseRows()`. Add:

```js
function filterLatestExerciseRowsBySearch(rows, query){
  const q = String(query || "").trim().toLowerCase();
  if(!q) return rows;
  return rows.filter(row => String(row.addr || "").toLowerCase().includes(q)
    || String(row.label || "").toLowerCase().includes(q));
}
```

Add `renderLatestExerciseSummary(rows)` that shows placeholders while `LIVE.exercisers` is absent, otherwise calls `window.summarizeLatestOdoloExercises(rows)` and writes these five metric blocks in order:

```js
const summary = window.summarizeLatestOdoloExercises(rows);
const walletLabel = summary.uniqueWallets === 1 ? "unique wallet" : "unique wallets";
const avgPrice = summary.avgExercisePrice == null ? "—" : fmtPrice(summary.avgExercisePrice);
const avgLock = summary.avgLockDays == null ? "—" : fmtLock(summary.avgLockDays);
```

Use `fmtNum(summary.vedoloReceived)` and `fmtUsd(summary.usdcPaid)` for compact totals. In `renderLatestExercises()`, create `periodRows`, render the summary, derive `rows` through the search helper, and use `periodRows.length` in the title count. Keep footer pagination based on `rows.length`.

- [ ] **Step 6: Update route cache-busting**

Append `-latest-exercise-summary-20260808` to the oDOLO route `version` string in `odolo/index.html`. This ensures GitHub Pages clients fetch the changed preview after deployment.

- [ ] **Step 7: Run focused and related tests and verify GREEN**

Run:

```bash
python3 -m unittest tests.test_odolo_preview_contracts
node --test tests/odolo-exercise-summary.test.js tests/odolo-address-meta.test.js
git diff --check
```

Expected: all tests PASS and `git diff --check` prints nothing.

- [ ] **Step 8: Commit the UI integration**

```bash
git add odolo-preview.html odolo/index.html tests/test_odolo_preview_contracts.py
git commit -m "feat: add latest oDOLO exercise activity rail"
```

---

### Task 3: Browser verification and responsive polish

**Files:**
- Modify only if a verified issue appears: `odolo-preview.html`
- Temporary, do not commit: `/tmp/verify_odolo_latest_summary.py`, `/tmp/odolo-latest-summary-*.png`

**Interfaces:**
- Consumes: rendered `/odolo/` route and IDs `#latest-exercises-section`, `#latest-exercise-stats`, `#q-latest`, `#dd-latest-period`.
- Produces: measured proof of no overflow, correct metric arrangement, immediate period/search interaction and no console errors.

- [ ] **Step 1: Materialize only required live JSON fixtures in the sparse worktree**

Add the root JSON files requested by `odolo-preview.html` to sparse checkout: `odolo_contract_data.json`, `exercised_usd.json`, `avg_lock_data.json`, `exercisers_by_address.json`, `odolo_flows.json`, and `vesting_investors.json`.

- [ ] **Step 2: Inspect the local server helper contract**

Run:

```bash
python3 "/Users/adamszybki/Desktop/Draft/Dolomite website/skills/webapp-testing/scripts/with_server.py" --help
```

- [ ] **Step 3: Write and run a native Playwright verification script**

The script must load `http://127.0.0.1:8765/odolo/`, wait for network idle and `#latest-exercise-stats`, capture console/page errors, and inspect 1280×900, 820×1000 and 390×844 viewports.

For every viewport assert:

```python
assert page.evaluate("document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1")
assert page.locator("#latest-exercise-stats .latest-exercise-stat").count() == 5
```

At 1280px, assert all five metric top coordinates are equal. At 820px, assert metrics 1–3 share a top coordinate and metrics 4–5 share a second top coordinate. At 390px, assert all five top coordinates are strictly increasing.

Click the period button, choose `data-period="14d"`, and assert the button label becomes `14D`. Capture the five summary values, type a wallet/address fragment that reduces the table, and assert the five values remain identical. Check that the title badge still ends in `· 14D` while the footer reflects search results.

Run through the reviewed literal server command:

```bash
python3 "/Users/adamszybki/Desktop/Draft/Dolomite website/skills/webapp-testing/scripts/with_server.py" \
  --server "python3 -m http.server 8765" --port 8765 \
  -- python3 /tmp/verify_odolo_latest_summary.py
```

- [ ] **Step 4: If the browser exposes a defect, add a failing regression assertion before changing production CSS/JS**

For a behavioral defect, extend the focused automated test that should catch it and watch RED. For a geometry defect, add the failing Playwright bounding-box assertion, then make the smallest scoped CSS change and rerun until GREEN.

- [ ] **Step 5: Run the final verification set**

Run:

```bash
node --check odolo-exercise-summary.js
node --test tests/odolo-exercise-summary.test.js tests/odolo-address-meta.test.js
python3 -m unittest tests.test_odolo_preview_contracts
git diff --check
git status --short
```

Expected: syntax valid, all focused tests PASS, no whitespace errors, and only intentional files changed.

- [ ] **Step 6: Commit verified visual polish only if Task 3 changed production code**

```bash
git add odolo-preview.html tests/test_odolo_preview_contracts.py
git commit -m "fix: polish responsive oDOLO exercise summary"
```

Do not create an empty commit when browser verification requires no production change.
