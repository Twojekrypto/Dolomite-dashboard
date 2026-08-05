# DOLO Flow Protocol Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show verified protocol addresses on both sides of DOLO Flow transfers and provide a working `Protocol` address-type filter.

**Architecture:** Keep the generated JSON schema intact. Adjust the existing dynamic exclusion policy so verified protocol contracts remain eligible for flow leaderboards, then extend the existing shared address-type model and dropdown rather than creating a parallel filter.

**Tech Stack:** Python 3 `unittest`, static HTML/CSS/JavaScript, Node.js test runner, Playwright browser verification, GitHub Actions, GitHub Pages.

## Global Constraints

- Production is served from `dolomite-dashboard/master`.
- Preserve Graphite + Gold styling and the existing filter interaction model.
- Keep explicit infrastructure, router, bot, mint, burn, DOLO, and oDOLO exclusions unchanged.
- Do not add dependencies or change the DOLO Flow JSON schema.
- Use `python3 -m http.server` for browser verification.

---

### Task 1: Protect protocol flow visibility with failing tests

**Files:**
- Modify: `tests/test_generate_dolo_flows_integrity.py`
- Create: `tests/dolo-flow-protocol-filter.test.js`

**Interfaces:**
- Consumes: `select_dynamic_flow_exclusions()`, `calculate_flows()`, `get_top()`, and the `mapType()`/address-type constants embedded in `dolo-preview.html`.
- Produces: regression coverage for paired protocol transfers and protocol UI classification.

- [ ] **Step 1: Add the Python regression test**

```python
def test_protocol_transfer_keeps_both_visible_flow_sides(self):
    excluded = flows.select_dynamic_flow_exclusions(
        {CHAINLINK_REWARDS_CLAIM, ECOSYSTEM_INCENTIVES_2},
        {
            CHAINLINK_REWARDS_CLAIM: {"type": "protocol"},
            ECOSYSTEM_INCENTIVES_2: {"type": "protocol"},
        },
    )
    net = flows.calculate_flows(
        [(CHAINLINK_REWARDS_CLAIM, ECOSYSTEM_INCENTIVES_2, 2_213_363 * 10**18, 100)],
        excluded,
    )
    self.assertEqual(flows.get_top(net, {}, 10, "seller", excluded)[0]["address"], CHAINLINK_REWARDS_CLAIM)
    self.assertEqual(flows.get_top(net, {}, 10, "accumulator", excluded)[0]["address"], ECOSYSTEM_INCENTIVES_2)
```

- [ ] **Step 2: Add the JavaScript behavior test**

```javascript
test("protocol labels stay independently filterable", () => {
  assert.equal(mapType({ type: "protocol" }, { contract_wallet_type: "safe" }), "protocol");
  assert.equal(ADDRESS_TYPES.includes("protocol"), true);
});
```

- [ ] **Step 3: Run both tests and verify RED**

Run: `python3 -m unittest tests.test_generate_dolo_flows_integrity && node --test tests/dolo-flow-protocol-filter.test.js`

Expected: the Python test fails because protocol contracts are excluded, and the JavaScript test fails because protocol maps to `ca` and is not selectable.

### Task 2: Implement the minimal data and UI change

**Files:**
- Modify: `generate_dolo_flows.py:1408-1418`
- Modify: `dolo-preview.html:722-737, 1138-1172, 1254-1300, 1635-1655, 1670-1680, 5098-5165, 5353-5366`

**Interfaces:**
- Consumes: the tests from Task 1 and existing `dolo-address-labels.js` classifications.
- Produces: visible protocol contracts in generated top-flow rows and a `Protocol` UI filter/badge.

- [ ] **Step 1: Preserve verified protocol contracts**

Add `protocol` to the label types skipped by `select_dynamic_flow_exclusions()` while leaving explicit infrastructure exclusions unchanged.

- [ ] **Step 2: Add protocol to the shared UI type model**

Add `protocol: "Protocol"` to `TYPE_LABELS`, add its tooltip and restrained blue-gray styles, and return `protocol` from `mapType()` before the generic contract branch.

- [ ] **Step 3: Add the matching dropdown options**

Add an active `Protocol` row to both existing type dropdowns and let their existing `ADDRESS_TYPES`-derived counts, toggle logic, and reset logic handle the eighth type.

- [ ] **Step 4: Run tests and verify GREEN**

Run: `python3 -m unittest tests.test_generate_dolo_flows_integrity && node --test tests/dolo-flow-protocol-filter.test.js`

Expected: all tests pass with zero failures.

### Task 3: Verify behavior and production deployment

**Files:**
- Verify: `dolo-preview.html`
- Verify: `.github/workflows/update-dolo-flows.yml`

**Interfaces:**
- Consumes: the implementation from Task 2.
- Produces: browser evidence, a production commit, refreshed DOLO Flow JSON, and a live Pages deployment.

- [ ] **Step 1: Run static checks**

Run: `python3 -m py_compile generate_dolo_flows.py && python3 -m unittest tests.test_generate_dolo_flows_integrity && node --test tests/dolo-flow-protocol-filter.test.js`

Expected: exit code `0` for every command.

- [ ] **Step 2: Verify the browser UI**

Run the project through `python3 -m http.server`, open `dolo-preview.html`, verify the `Protocol` option toggles protocol rows, and compare toolbar/table bounding boxes at `1440x1000` and `390x844`.

- [ ] **Step 3: Review the diff**

Run: `git diff --check && git diff -- generate_dolo_flows.py dolo-preview.html tests/test_generate_dolo_flows_integrity.py tests/dolo-flow-protocol-filter.test.js`

Expected: no whitespace errors and no unrelated file changes.

- [ ] **Step 4: Commit and push production**

Stage only the spec, plan, generator, preview, and two test files. Commit with `fix: show protocol addresses in DOLO flows`, then push the current commit to `dolomite-dashboard/master`.

- [ ] **Step 5: Refresh and verify live data**

Dispatch `Update DOLO Flows Data`, wait for success, wait for `Deploy GitHub Pages`, and verify the receiver address plus the `Protocol` filter on `https://twojekrypto.github.io/Dolomite-dashboard/?v=<commit>&t=<timestamp>`.
