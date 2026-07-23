# Total Supply Over Time Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Total Supply history card above TVL and correct the existing TVL card to render Net TVL.

**Architecture:** The DeFiLlama fetcher preserves Net TVL and publishes a second timestamp-aligned Total Supply series. The frontend builds both histories and drives two independently configured instances of one shared chart/brush controller.

**Tech Stack:** Python 3 standard library + `requests`, static HTML/CSS/JavaScript, SVG, Node VM contract probes, Python `unittest`, Playwright browser verification.

## Global Constraints

- `Total Supply = Net TVL + Total Borrowed`, in USD.
- The new card sits immediately above `TVL Over Time`.
- Both cards have identical Graphite + Gold UX, tooltip, and mini-chart.
- Brush state is independent per card.
- Historical normalization is allowed only when current/history drift is within 5%.
- No new dependency, secret, live browser replay, or unrelated refactor.
- Use TDD, surgical edits, `python3 -m http.server`, and browser-computed geometry.

---

### Task 1: Split the historical data contract

**Files:**
- Create: `tests/test_fetch_defillama.py`
- Modify: `fetch_defillama.py`
- Modify: `validate_data.py`
- Modify: `defillama_data.json`

**Interfaces:**
- Produces: `build_history_series(base_tvl_history, borrowed_history) -> tuple[list[dict], list[dict]]`
- Produces JSON keys: `tvl` and `totalSupply`
- Consumes: DeFiLlama point objects containing `date` and `totalLiquidityUSD`

- [ ] **Step 1: Write the failing unit tests**

Add tests which import `build_history_series` and assert:

```python
net, supply = build_history_series(
    [{"date": 10, "totalLiquidityUSD": 100}, {"date": 20, "totalLiquidityUSD": 120}],
    [{"date": 10, "totalLiquidityUSD": 40}, {"date": 20, "totalLiquidityUSD": 50}],
)
self.assertEqual(net, [
    {"date": 10, "totalLiquidityUSD": 100},
    {"date": 20, "totalLiquidityUSD": 120},
])
self.assertEqual(supply, [
    {"date": 10, "totalLiquidityUSD": 140},
    {"date": 20, "totalLiquidityUSD": 170},
])
```

Add a timestamp-alignment case where borrowed has one additional date and a
validator contract assertion requiring `totalSupply`.

- [ ] **Step 2: Run RED**

Run:

```bash
python3 -m unittest tests/test_fetch_defillama.py tests/test_tvl_preview_contracts.py -v
```

Expected: failure because `build_history_series` and `totalSupply` do not exist.

- [ ] **Step 3: Implement the minimal split**

Add `build_history_series` to sanitize, sort, and merge the two source arrays.
Publish Net TVL as `output["tvl"]` and combined supply as
`output["totalSupply"]`. Extend the `defillama_data.json` validator to require a
sorted, populated Total Supply history with the same point contract.

- [ ] **Step 4: Run GREEN and refresh generated data**

Run:

```bash
python3 -m unittest tests/test_fetch_defillama.py tests/test_tvl_preview_contracts.py -v
python3 fetch_defillama.py
python3 validate_data.py defillama_data.json
```

Expected: all tests and validation pass; the generated JSON has both histories.

### Task 2: Add two reusable, independent chart instances

**Files:**
- Modify: `tests/test_tvl_preview_contracts.py`
- Modify: `tvl-preview.html`
- Modify: `tvl/index.html`

**Interfaces:**
- Consumes: `llamaData.tvl` and `llamaData.totalSupply`
- Produces: `snapshot.history` for Net TVL
- Produces: `snapshot.supplyHistory` for Total Supply
- Produces: `createHistoryChart(config)` with independent brush state

- [ ] **Step 1: Write failing frontend contract tests**

Add a Node probe asserting that a payload with distinct series returns distinct
`snapshot.history` and `snapshot.supplyHistory` values. Add markup assertions
for `Total Supply Over Time`, `TVL Over Time`, and unique supply/TVL chart and
brush IDs. Assert the Total Supply section occurs first and that the route cache
version changes.

- [ ] **Step 2: Run RED**

Run:

```bash
python3 -m unittest tests/test_tvl_preview_contracts.py -v
```

Expected: failures for the absent `supplyHistory`, card, and controller.

- [ ] **Step 3: Implement the shared controller and second card**

Refactor history parsing to accept an explicit raw array and current metric.
Store both histories in `applyTvlSnapshot`. Insert the Total Supply card above
TVL using supply-prefixed IDs and unique gradient IDs. Replace the single
global brush implementation with two `createHistoryChart(config)` instances
that share geometry/hover/drag code but own separate selection state.

- [ ] **Step 4: Run GREEN and static checks**

Run:

```bash
python3 -m unittest tests/test_tvl_preview_contracts.py tests/test_fetch_defillama.py tests/test_fetch_dolomite_tvl.py -v
node --check <(sed -n '/<script>/,/<\\/script>/p' tvl-preview.html)
git diff --check
```

If process substitution is unavailable for `node --check`, use the existing
Node VM extraction probe from `tests/test_tvl_preview_contracts.py`.

Expected: all tests pass, JavaScript parses, and diff check is clean.

### Task 3: Browser verification, review, and production publish

**Files:**
- Create temporarily outside Git tracking: browser verification script and screenshots
- Review only: all changed files

**Interfaces:**
- Consumes: local `/tvl/` route served over HTTP
- Produces: geometry, interaction, console, and screenshot evidence

- [ ] **Step 1: Run the local server helper**

First run:

```bash
python3 skills/webapp-testing/scripts/with_server.py --help
```

Then run the reviewed literal server command:

```bash
python3 skills/webapp-testing/scripts/with_server.py \
  --server "python3 -m http.server 8000" --port 8000 \
  -- python3 /tmp/verify_tvl_history_charts.py
```

- [ ] **Step 2: Verify browser contracts**

The Playwright script must wait for network idle, collect console errors, and
assert:

```python
assert page.locator("text=Total Supply Over Time").count() == 1
assert page.locator("text=TVL Over Time").count() == 1
assert supply_card.bounding_box()["y"] < tvl_card.bounding_box()["y"]
assert page.locator("#supplyChartLine").get_attribute("d")
assert page.locator("#chartLine").get_attribute("d")
assert page.locator("#supplyBrushLine").get_attribute("d")
assert page.locator("#brushLine").get_attribute("d")
```

Drag the supply handle, record both badges/windows, and prove only the supply
state changed. Repeat for TVL. Measure matching widths and SVG heights at
1440×1000 and 390×844. Save screenshots to `/tmp`.

- [ ] **Step 3: Run the full targeted verification**

Run fresh:

```bash
python3 -m unittest tests/test_fetch_defillama.py tests/test_fetch_dolomite_tvl.py tests/test_tvl_preview_contracts.py tests/test_pages_workflow_contracts.py -v
python3 -m py_compile fetch_defillama.py validate_data.py
python3 validate_data.py defillama_data.json dolomite_tvl.json
git diff --check
git status --short
```

Expected: zero failures, valid generated data, clean diff, and only scoped files.

- [ ] **Step 4: Perform two self-review passes**

Correctness/regression pass: verify formulas, history fallback, timestamp
alignment, independent state, route cache, and no change to current hero values.

Maintainability/security pass: verify no secrets, live endpoints, dependency,
unsafe HTML source, duplicate event listeners, or unrelated changes.

- [ ] **Step 5: Commit and push**

Stage only:

```bash
git add \
  docs/superpowers/specs/2026-07-23-total-supply-over-time-design.md \
  docs/superpowers/plans/2026-07-23-total-supply-over-time.md \
  tests/test_fetch_defillama.py tests/test_tvl_preview_contracts.py \
  fetch_defillama.py validate_data.py defillama_data.json \
  tvl-preview.html tvl/index.html
git commit -m "Add Total Supply history chart"
git push dolomite-dashboard HEAD:master
```

Expected: production `master` accepts the fast-forward push. If data workflows
advance `master`, fetch and rebase the scoped commit, rerun verification, then
retry.

- [ ] **Step 6: Verify deployment**

Watch the resulting Pages run to completion and open:

```text
https://twojekrypto.github.io/Dolomite-dashboard/tvl/?v=<commit>&t=<timestamp>
```

Confirm both live chart cards, mini-charts, independent brushes, and no console
errors.
