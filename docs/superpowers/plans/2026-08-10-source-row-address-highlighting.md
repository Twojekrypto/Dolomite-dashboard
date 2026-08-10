# Source-Row Address Highlighting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Quietly highlight the repeated wallet address in the hovered source row as well as identical visible addresses in other rows, while preserving the stronger direct-address hover state.

**Architecture:** Keep the existing delegated controller and opt-in table boundary. Change only row-derived match selection so it first proves that an address has a visible peer in a different row, then applies the quiet peer class to the displayed source wrapper and every exact peer; direct address mode continues to replace row mode and style only its trigger as the source.

**Tech Stack:** Vanilla JavaScript, CSS, Node.js built-in test runner, static HTML cache keys, browser `getComputedStyle()` and bounding-box verification.

## Global Constraints

- Matching stays inside the nearest `table[data-address-match-cells]`.
- Only exact normalized `0x` plus 40 hexadecimal-character addresses participate.
- Label-only, hidden, malformed, cross-table, and unscoped wrappers remain excluded.
- A unique address receives no match state on row hover.
- Row hover uses the existing quiet peer treatment; direct address hover/focus uses the existing strong source treatment.
- Do not change cursor semantics, padding, borders, cell size, row height, or table geometry.
- Advance the shared hover asset cache key in every page that loads the asset.

---

### Task 1: Prove and implement source-row address matching

**Files:**
- Modify: `tests/address-match-highlighting.test.js:313-340`
- Modify: `shared-hover-tooltips.js:215-232`

**Interfaces:**
- Consumes: the existing fake DOM fixture and delegated event controller loaded from `shared-hover-tooltips.js`.
- Produces: regression assertions for source-row quiet state, multi-address rows, unique addresses, and direct-address priority; `showRowAddressMatches(data)` covers source and peer wrappers only for addresses repeated in another visible row.

- [ ] **Step 1: Change the row-hover test to require a quiet source address**

Replace the current source exclusion assertion with the following exact expectations:

```javascript
test('row hover quietly highlights the source address and exact peers in other rows', () => {
  const fixture = buildFixture();

  fixture.document.dispatch('pointerover', fixture.source.cell);

  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-active'), true);
  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-source'), false);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.duplicateWrapperPeer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.source.labelTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.hidden.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.crossTable.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.peer.row.classList.contains('address-match-peer'), false);
  assert.equal(fixture.peer.cell.classList.contains('address-match-peer'), false);
});
```

- [ ] **Step 2: Extend the multi-address test**

Before direct hover, assert both repeated source addresses are quiet and, after direct hover, assert only the targeted source is strong:

```javascript
fixture.document.dispatch('mouseover', fixture.multiSource.cellOutsideAddress);
assert.equal(fixture.multiSource.primary.addressTrigger.classList.contains('address-match-peer'), true);
assert.equal(fixture.multiSource.secondary.addressTrigger.classList.contains('address-match-peer'), true);
assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), true);

fixture.document.dispatch('mouseover', fixture.multiSource.primary.addressTrigger);
assert.equal(fixture.multiSource.primary.addressTrigger.classList.contains('address-match-source'), true);
assert.equal(fixture.multiSource.primary.addressTrigger.classList.contains('address-match-peer'), false);
assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
assert.equal(fixture.multiSource.secondary.addressTrigger.classList.contains('address-match-active'), false);
assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), false);
```

- [ ] **Step 3: Run the focused test and verify RED**

Run: `node --test --test-name-pattern="row hover|row matches every" tests/address-match-highlighting.test.js`

Expected: FAIL because `fixture.source.addressTrigger` and both displayed multi-source wrappers do not yet receive `address-match-peer` in row mode.

- [ ] **Step 4: Implement a two-pass repeated-address selection**

Replace the row exclusion loop with a candidate map and peer proof:

```javascript
function showRowAddressMatches(data) {
  if (activeAddressMatch && activeAddressMatch.mode === 'row' && activeAddressMatch.row === data.row) return;
  clearAddressMatches();

  var candidatesByAddress = {};
  data.addresses.forEach(function (address) {
    candidatesByAddress[address] = [];
  });
  data.table.querySelectorAll('.addr-tooltip-wrap[data-full-addr]').forEach(function (trigger) {
    if (!isRenderedAddressTrigger(trigger)) return;
    var address = normalizeMatchAddress(trigger.getAttribute('data-full-addr'));
    if (!address || !candidatesByAddress[address]) return;
    candidatesByAddress[address].push(trigger);
  });

  var elements = [];
  Object.keys(candidatesByAddress).forEach(function (address) {
    var candidates = candidatesByAddress[address];
    var hasPeerRow = candidates.some(function (trigger) {
      return !data.row.contains(trigger);
    });
    if (!hasPeerRow) return;
    candidates.forEach(function (trigger) {
      if (elements.indexOf(trigger) === -1) elements.push(trigger);
    });
  });
  if (!elements.length) return;

  elements.forEach(function (element) {
    element.classList.add('address-match-active', 'address-match-peer');
  });
  activeAddressMatch = { mode: 'row', table: data.table, row: data.row, elements: elements };
}
```

- [ ] **Step 5: Run the focused tests and verify GREEN**

Run: `node --test --test-name-pattern="row hover|row matches every|does not emphasize" tests/address-match-highlighting.test.js`

Expected: all selected tests PASS, including the unique-address guard.

- [ ] **Step 6: Run the complete controller suite**

Run: `node --test tests/address-match-highlighting.test.js`

Expected: all tests PASS, including dynamic rows, focus, touch cleanup, scroll reconciliation, table isolation, and single-install behavior.

- [ ] **Step 7: Commit the passing test and implementation together**

```bash
git add shared-hover-tooltips.js tests/address-match-highlighting.test.js
git commit -m "fix: highlight repeated address in source row"
```

---

### Task 2: Publish the shared asset and verify geometry

**Files:**
- Modify: `assets-preview.html:11-12`
- Modify: `dolo-preview.html:16-17`
- Modify: `liquidation-preview.html:14,20539`
- Modify: `odolo-preview.html:11-12`
- Modify: `portfolio-preview.html:11-12`
- Modify: `revenue-preview.html:11-12`
- Modify: `rewards-preview.html:11-12`
- Modify: `tvl-preview.html:11-12`
- Modify: `vedolo-preview.html:11-12`
- Verify: `shared-hover-tooltips.css`

**Interfaces:**
- Consumes: `shared-hover-tooltips.js` and `shared-hover-tooltips.css` static assets.
- Produces: cache key `20260810-source-row-addresses` on every stylesheet and script reference.

- [ ] **Step 1: Update every shared hover cache key**

Mechanically replace `20260809-row-address-peers` with `20260810-source-row-addresses` only in the nine listed HTML pages.

- [ ] **Step 2: Prove no stale page reference remains**

Run: `rg -n "20260809-row-address-peers|shared-hover-tooltips\.(js|css)\?v=" --glob '*.html'`

Expected: no old key; every shared hover asset reference uses `20260810-source-row-addresses`.

- [ ] **Step 3: Serve and inspect desktop and phone widths**

Run: `python3 -m http.server 8765`

Inspect one repeated address in `odolo-preview.html`, `vedolo-preview.html`, and `liquidation-preview.html` at 1440×900 and 390×844. Capture before/after `getBoundingClientRect()` values for the address wrapper, cell, row, and table while moving from row padding onto the address.

Expected: row hover styles the displayed source address and exact peers quietly; direct address hover styles only its trigger strongly; every recorded `x`, `y`, `width`, and `height` remains unchanged within 0.1 px.

- [ ] **Step 4: Run syntax and behavior checks**

Run:

```bash
node --check shared-hover-tooltips.js
node --test tests/address-match-highlighting.test.js
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 5: Commit cache keys**

```bash
git add assets-preview.html dolo-preview.html liquidation-preview.html odolo-preview.html portfolio-preview.html revenue-preview.html rewards-preview.html tvl-preview.html vedolo-preview.html
git commit -m "chore: refresh repeated-address hover assets"
```
