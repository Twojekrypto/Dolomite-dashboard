# Identical Address Cell Highlighting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Highlight only visible table cells that contain the same full wallet address as the hovered or focused address cell in the ten audited activity and position tables.

**Architecture:** Extend the existing delegated address-tooltip script with an opt-in, table-scoped address-matching controller. The controller normalizes canonical `data-full-addr` values, deduplicates matches by `td`, and applies source/peer CSS classes without changing row state or layout. Static table attributes define the approved scope; no data generator or API changes are needed.

**Tech Stack:** Static HTML, vanilla JavaScript, CSS, Node.js built-in test runner, Python unittest contract tests, Python Playwright browser verification.

## Global Constraints

- Match only complete EVM addresses represented by `0x` plus 40 hexadecimal characters.
- Match only currently rendered cells in the nearest opted-in table.
- Do not emphasize a source cell when it has no visible peer in that table.
- Never highlight a full row or add a left-side rail.
- Preserve existing row hover, risk colors, zebra backgrounds, links, copy actions, sorting, filtering, and pagination.
- Use the existing Graphite + Gold design tokens and avoid layout-changing borders.
- Do not add dependencies, generated data files, configuration, secrets, workflows, or API requests.
- Production deployment is the `dolomite-dashboard/master` branch.

---

### Task 1: Shared address-match controller and visual states

**Files:**
- Modify: `shared-hover-tooltips.js`
- Modify: `shared-hover-tooltips.css`
- Create: `tests/address-match-highlighting.test.js`

**Interfaces:**
- Consumes: `.addr-tooltip-wrap[data-full-addr]` within `table[data-address-match-cells]`.
- Produces: delegated pointer/focus behavior and the cell classes `.address-match-source` and `.address-match-peer`.

- [ ] **Step 1: Write the failing runtime test**

Create a Node test fixture that executes the real `shared-hover-tooltips.js` inside `vm`, supplies a minimal DOM with two opted-in tables, and dispatches stored `pointerover`/`pointerout` handlers. Assert these hand-derived outcomes:

```js
assert.equal(sourceCell.classList.contains('address-match-source'), true);
assert.equal(peerCell.classList.contains('address-match-peer'), true);
assert.equal(otherAddressCell.classList.contains('address-match-peer'), false);
assert.equal(sameAddressOtherTableCell.classList.contains('address-match-peer'), false);
assert.equal(duplicateWrapperCell.classList.contains('address-match-peer'), true);
assert.equal(malformedCell.classList.contains('address-match-source'), false);
```

After `pointerout`, assert every address-match class is removed. The production mutation that this catches is missing exact-address validation, missing per-table scoping, wrapper-level duplication, or incomplete cleanup.

- [ ] **Step 2: Run the runtime test and verify RED**

Run: `node --test tests/address-match-highlighting.test.js`

Expected: FAIL because `shared-hover-tooltips.js` does not yet register the address-match pointer handlers or apply the source/peer cell classes.

- [ ] **Step 3: Implement the minimal delegated controller**

Add private helpers inside the existing IIFE:

```js
function normalizeMatchAddress(value) {
  var address = cleanText(value).toLowerCase();
  return /^0x[a-f0-9]{40}$/.test(address) ? address : '';
}

function addressMatchTrigger(target) {
  var trigger = target.closest && target.closest('.addr-tooltip-wrap[data-full-addr]');
  var table = trigger && trigger.closest('table[data-address-match-cells]');
  var cell = trigger && trigger.closest('td');
  var address = trigger && normalizeMatchAddress(trigger.getAttribute('data-full-addr'));
  return table && cell && address ? { trigger: trigger, table: table, cell: cell, address: address } : null;
}
```

Implement `clearAddressMatches(table)` and `showAddressMatches(data)` so every unique matching `td` is classified once. Register delegated `pointerover`, `pointerout`, `focusin`, and `focusout` handlers. Ignore pointer transitions that remain inside the active source cell, and clear the active state on scroll, resize, and window blur. Do not intercept any click or touch event.

Add non-layout CSS:

```css
table[data-address-match-cells] td.address-match-source {
  background-color: color-mix(in srgb, var(--gold, #c9a227) 12%, transparent);
  box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--gold, #c9a227) 56%, transparent);
}
table[data-address-match-cells] td.address-match-peer {
  background-color: color-mix(in srgb, var(--gold, #c9a227) 7%, transparent);
  box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--gold, #c9a227) 26%, transparent);
}
```

Use a 140 ms background/box-shadow transition and a `prefers-reduced-motion: reduce` override. If `color-mix()` would conflict with current browser support in local verification, replace it with fixed RGBA values derived from the same gold token without changing class semantics.

- [ ] **Step 4: Run the runtime test and verify GREEN**

Run: `node --test tests/address-match-highlighting.test.js`

Expected: PASS with exact matching, per-table scoping, cell deduplication, malformed-address rejection, and cleanup covered.

- [ ] **Step 5: Commit the shared behavior**

```bash
git add shared-hover-tooltips.js shared-hover-tooltips.css tests/address-match-highlighting.test.js
git commit -m "feat: highlight repeated wallet cells"
```

### Task 2: Opt in only the audited tables

**Files:**
- Modify: `vedolo-preview.html`
- Modify: `odolo-preview.html`
- Modify: `liquidation-preview.html`
- Modify: `vedolo/index.html`
- Modify: `odolo/index.html`
- Modify: `borrow/index.html`
- Modify: `liquidation/index.html`
- Modify: `supply/index.html`
- Create: `tests/test_address_match_table_scope.py`

**Interfaces:**
- Consumes: `table[data-address-match-cells]` from Task 1.
- Produces: exactly ten opted-in live table elements and no opt-in on aggregate or token-contract tables.

- [ ] **Step 1: Write the failing static scope test**

Parse each preview HTML file with a small `html.parser.HTMLParser` subclass and collect the IDs/classes of tables carrying `data-address-match-cells`. Assert the literal expected set:

```python
expected_ids = {
    "exits-table", "locks-table", "unlocks-table", "claimable-table",
    "tbl-latest-ex", "tbl-latest-pair", "positions-table",
    "liquidation-history-table", "supply-activity-table",
}
self.assertEqual(found_ids, expected_ids)
self.assertEqual(found_class_only, {"positions-table sim-atrisk-table"})
```

Also assert that every opted-in table has at least one address wrapper produced by its static markup or renderer source. The production mutation this catches is over-broad opt-in, a missed audited table, or enabling a unique-address/token-contract table.

- [ ] **Step 2: Run the scope test and verify RED**

Run: `python3 -m unittest tests.test_address_match_table_scope -v`

Expected: FAIL with an empty discovered scope because no table has the opt-in attribute yet.

- [ ] **Step 3: Add the ten opt-in attributes**

Add the boolean `data-address-match-cells` attribute only to:

- `vedolo-preview.html`: `#exits-table`, `#locks-table`, `#unlocks-table`, `#claimable-table`
- `odolo-preview.html`: `#tbl-latest-ex`, `#tbl-latest-pair`
- `liquidation-preview.html`: `.positions-table.sim-atrisk-table`, `#positions-table`, `#liquidation-history-table`, `#supply-activity-table`

Do not alter column markup, renderer output, `nth-child` selectors, or any excluded table.

Load `shared-hover-tooltips.js` and `shared-hover-tooltips.css` with the cache key `20260804-address-match` on all three affected previews. Mark `liquidation-preview.html` with `window.__DOLO_INLINE_TOOLTIP_ACTIVE=true`, then load the shared script dynamically after the route loader closes its written document; its specialized tooltip remains authoritative and only the shared address matcher is installed. Append `address-match-20260804` to the five affected route preview versions so Borrow, Liquidations, Supply, veDOLO, and oDOLO fetch the updated preview immediately.

- [ ] **Step 4: Run the scope and runtime tests and verify GREEN**

Run:

```bash
python3 -m unittest tests.test_address_match_table_scope -v
node --test tests/address-match-highlighting.test.js
```

Expected: both suites PASS.

- [ ] **Step 5: Commit the audited scope**

```bash
git add vedolo-preview.html odolo-preview.html liquidation-preview.html tests/test_address_match_table_scope.py
git commit -m "feat: enable repeated wallet matching"
```

### Task 3: Browser verification, regression review, and production publish

**Files:**
- Create temporarily outside the repository: `/tmp/verify_address_match.py`
- Verify: `shared-hover-tooltips.js`, `shared-hover-tooltips.css`, `vedolo-preview.html`, `odolo-preview.html`, `liquidation-preview.html`

**Interfaces:**
- Consumes: completed runtime behavior and opt-in scope from Tasks 1–2.
- Produces: measured browser evidence and a production `master` push.

- [ ] **Step 1: Check the local server helper contract**

Run: `python3 skills/webapp-testing/scripts/with_server.py --help`

Expected: usage output describing `--server`, `--port`, and the trailing Playwright command.

- [ ] **Step 2: Create and run focused Playwright verification**

Use `python3 -m http.server 8765` through the reviewed helper command. The Playwright script must load the relevant preview pages, wait for network idle, locate visible duplicate addresses (switching to an all-time period or sorting by wallet when necessary), hover the first matching address, and capture:

```python
source_style = page.locator("td.address-match-source").evaluate("el => getComputedStyle(el).backgroundColor")
peer_count = page.locator("td.address-match-peer").count()
source_box_after = page.locator("td.address-match-source").bounding_box()
row_style_after = source_row.evaluate("el => getComputedStyle(el).backgroundColor")
```

Assert `peer_count >= 1`, the source and peer computed backgrounds differ from their pre-hover values, the source cell and row bounding boxes are unchanged, another table is not highlighted, and no console/page errors occur. Repeat at 1440, 1024, 768, and 390 CSS-pixel viewport widths; on the phone width verify no persistent class remains after pointer movement or focus loss.

- [ ] **Step 3: Run targeted and neighboring regression checks**

Run:

```bash
node --check shared-hover-tooltips.js
node --test tests/address-match-highlighting.test.js
python3 -m unittest tests.test_address_match_table_scope tests.test_vedolo_preview_contracts tests.test_odolo_preview_contracts tests.test_borrow_ux_contracts tests.test_supply_activity_ui_contracts -v
git diff --check
```

Expected: every command exits 0 with no test failures or whitespace errors.

- [ ] **Step 4: Perform two review passes**

Correctness/regression pass: inspect the full branch diff against `dolomite-dashboard/master`, map every changed production line to the approved scope, and verify no row-level class or renderer data changed.

Maintainability/security pass: confirm one shared implementation, no duplicate page scripts, no secret/config/workflow/generated-data changes, exact EVM validation, and no click interception or unsafe HTML construction.

- [ ] **Step 5: Publish directly to production master**

After fresh verification, fetch `dolomite-dashboard/master`, confirm the branch can fast-forward from its base or rebase safely if only automated data commits advanced, then push the verified commit range:

```bash
git push dolomite-dashboard HEAD:master
```

Verify remote state with `git ls-remote dolomite-dashboard refs/heads/master` and confirm the returned SHA equals local `HEAD`. Then check the relevant GitHub Pages workflow/deployment status without creating a pull request, because the user explicitly requested the live production update.
