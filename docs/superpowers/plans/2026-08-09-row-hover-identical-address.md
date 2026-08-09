# Row Hover Identical Address Highlighting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the shared repeated-wallet interaction so hovering a supported table row quietly highlights exact copies of its wallet address in other visible rows, while direct address hover/focus keeps the existing stronger treatment.

**Architecture:** Keep the ten-table opt-in contract and shared delegated controller. Add a row-derived match mode beside the existing direct-address mode, with direct address interaction taking priority and a single reconciliation path preventing flicker when the pointer moves between a row and its address. Reuse the existing peer CSS treatment and advance shared-asset/route cache keys.

**Tech Stack:** Static HTML/CSS, browser JavaScript, Node.js built-in test runner with a minimal fake DOM, Python `unittest`, Playwright through the repository webapp-testing helper, Git/GitHub Pages.

## Global Constraints

- Matching stays inside the nearest `table[data-address-match-cells]` and never crosses tables or pages.
- Only exact normalized `0x` plus 40-hex-character wallet addresses participate.
- Row hover paints only matching address wrappers in other rows; it never adds visual classes to peer rows or cells.
- Direct address hover/focus keeps the existing Premium Strong source treatment and quiet peer treatment.
- `Asset Activity` row hover considers both wallet addresses independently.
- The existing ten-table allowlist remains unchanged.
- No table geometry, data, sorting, filtering, pagination, row-click, risk/status color, revenue metric, rebate value, dependency, or workflow behavior changes.
- The veDOLO rebate gap investigation is report-only in this change; no financial values are inferred or rewritten.

---

### Task 1: Specify row-derived matching in the behavior test

**Files:**
- Modify: `tests/address-match-highlighting.test.js`
- Test: `tests/address-match-highlighting.test.js`

**Interfaces:**
- Consumes: Existing delegated listeners installed by `shared-hover-tooltips.js` and canonical values in `.addr-tooltip-wrap[data-full-addr]`.
- Produces: Test fixtures with real `tbody > tr > td` structure and failing assertions for row-hover peer state.

- [ ] **Step 1: Make the fake DOM represent table rows accurately**

Extend `FakeElement.matches()` with the selectors used by production row matching:

```js
if (selector === 'tr') return this.tagName === 'TR';
if (selector === 'tbody') return this.tagName === 'TBODY';
```

Give the fake element a layout-visibility flag so the test exercises the same visible-wrapper boundary as the browser:

```js
// In the constructor:
this.hiddenFromLayout = Boolean(options.hiddenFromLayout);

// Beside getBoundingClientRect():
getClientRects() {
  return this.hiddenFromLayout ? [] : [this.getBoundingClientRect()];
}
```

Allow event-specific fields such as `pointerType` in `FakeDocument.dispatch()`:

```js
dispatch(type, target, relatedTarget = null, extra = {}) {
  const event = { type, target, relatedTarget, clientX: 110, clientY: 110, ...extra };
  (this.listeners.get(type) || []).forEach((handler) => handler(event));
}
```

Create a `tbody` in each fixture table and change the row helper so it can append one or more canonical addresses to the same row:

```js
function appendAddressToRow(row, address, options = {}) {
  const cell = row.appendChild(new FakeElement('td'));
  // Preserve the existing known-label and displayed-address wrappers.
  return { row, cell, labelTrigger, addressTrigger };
}

function appendAddressRow(tbody, addresses, options = {}) {
  const row = tbody.appendChild(new FakeElement('tr'));
  const cells = addresses.map((address, index) => (
    appendAddressToRow(row, address, options[index] || {})
  ));
  return { row, cells };
}
```

Keep the existing fixture aliases `source`, `peer`, `duplicateWrapperPeer`, `other`, `malformed`, `crossTable`, and `unscoped` pointing to the first cell result so the nine existing tests continue to exercise unchanged direct-address behavior. Expose `tableBody` plus an `appendVisibleRow(addresses)` fixture method for post-initialization rows. Add one layout-hidden peer wrapper carrying the repeated canonical address.

- [ ] **Step 2: Add the failing one-address row-hover test**

Add a test that hovers the source cell rather than its address wrapper and asserts the observable hierarchy:

```js
test('row hover quietly highlights exact peer addresses in other rows only', () => {
  const fixture = buildFixture();

  fixture.document.dispatch('pointerover', fixture.source.cell);

  assert.equal(fixture.source.addressTrigger.classList.contains('address-match-active'), false);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.duplicateWrapperPeer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.hidden.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.crossTable.addressTrigger.classList.contains('address-match-peer'), false);
  assert.equal(fixture.peer.row.classList.contains('address-match-peer'), false);
  assert.equal(fixture.peer.cell.classList.contains('address-match-peer'), false);
});
```

This catches the production bug where delegated matching starts only from the address wrapper.

In the existing direct-address test, also assert the layout-hidden duplicate does not gain `address-match-peer`. This catches an implementation that scopes correctly but still queries invisible wrappers.

- [ ] **Step 3: Add the failing multi-address and priority tests**

Add a fixture row containing both canonical addresses and another row for each peer. Assert that row hover activates both peer groups, then direct hover on the first source address narrows the active group and applies the strong source class:

```js
test('row hover matches every canonical wallet in the row and direct address hover takes priority', () => {
  const fixture = buildFixture({ multiAddressRow: true });

  fixture.document.dispatch('mouseover', fixture.multiSource.cellOutsideAddress);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), true);

  fixture.document.dispatch('mouseover', fixture.multiSource.primary.addressTrigger);
  assert.equal(fixture.multiSource.primary.addressTrigger.classList.contains('address-match-source'), true);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);
  assert.equal(fixture.other.addressTrigger.classList.contains('address-match-peer'), false);
});
```

Also add a test that dispatches `mouseout` from one child to another child of the same row and confirms peers remain active, then exits the row and confirms every class clears.

Add one dynamic-render test that appends a repeated-address row after `shared-hover-tooltips.js` has installed its listeners, hovers the original row, and observes the new peer without rebinding:

```js
test('delegated row matching includes rows rendered after controller installation', () => {
  const fixture = buildFixture();
  const latePeer = fixture.appendVisibleRow(['0x1111111111111111111111111111111111111111']).cells[0];

  fixture.document.dispatch('mouseover', fixture.source.cell);

  assert.equal(latePeer.addressTrigger.classList.contains('address-match-peer'), true);
});
```

Add a touch-cleanup test so the production `pointerdown` guard is test-first:

```js
test('touch pointerdown clears row-derived cosmetic matching without intercepting the event', () => {
  const fixture = buildFixture();
  fixture.document.dispatch('mouseover', fixture.source.cell);
  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), true);

  fixture.document.dispatch('pointerdown', fixture.source.cell, null, { pointerType: 'touch' });

  assert.equal(fixture.peer.addressTrigger.classList.contains('address-match-peer'), false);
});
```

- [ ] **Step 4: Run the focused test and verify RED**

Run:

```bash
node --test tests/address-match-highlighting.test.js
```

Expected: the new row-hover tests fail because `addressMatchTrigger()` returns no match when the event target is outside `.addr-tooltip-wrap[data-full-addr]`; the original nine tests remain green.

- [ ] **Step 5: Commit the failing specification**

```bash
git add tests/address-match-highlighting.test.js
git commit -m "test: specify row hover wallet matching"
```

---

### Task 2: Implement the two-level delegated match controller

**Files:**
- Modify: `shared-hover-tooltips.js`
- Test: `tests/address-match-highlighting.test.js`

**Interfaces:**
- Consumes: `normalizeMatchAddress(value)`, `isDisplayedAddressTrigger(trigger)`, opted-in table markup, and the row fixtures from Task 1.
- Produces: `rowAddressMatchData(target)`, `showRowAddressMatches(data)`, and one reconciled delegated event path for `row` and `address` modes.

- [ ] **Step 1: Add row-address discovery helpers**

Add a rendered-address helper and row discovery immediately after `addressMatchTrigger()`:

```js
function isRenderedAddressTrigger(trigger) {
  if (!isDisplayedAddressTrigger(trigger)) return false;
  if (typeof trigger.getClientRects === 'function' && trigger.getClientRects().length === 0) return false;
  return true;
}

function rowAddressMatchData(target) {
  var row = target.closest && target.closest('tr');
  var table = row && row.closest('table[data-address-match-cells]');
  if (!row || !table || !row.parentElement || row.parentElement.tagName !== 'TBODY') return null;

  var addresses = [];
  row.querySelectorAll('.addr-tooltip-wrap[data-full-addr]').forEach(function (trigger) {
    var address = normalizeMatchAddress(trigger.getAttribute('data-full-addr'));
    if (!address || !isRenderedAddressTrigger(trigger) || addresses.indexOf(address) !== -1) return;
    addresses.push(address);
  });
  return addresses.length ? { mode: 'row', row: row, table: table, addresses: addresses } : null;
}

function matchDataForTarget(target) {
  var address = addressMatchTrigger(target);
  if (address) {
    address.mode = 'address';
    return address;
  }
  return rowAddressMatchData(target);
}
```

Update `addressMatchTrigger()` and the existing `showAddressMatches()` peer query to use `isRenderedAddressTrigger()` rather than text-shape validation alone. This keeps direct address matching inside the same visible-wrapper boundary as row hover.

This keeps direct address interaction authoritative and rejects header/footer rows.

- [ ] **Step 2: Add row-derived peer application**

Keep `clearAddressMatches()` as the single cleanup function and add:

```js
function showRowAddressMatches(data) {
  if (activeAddressMatch && activeAddressMatch.mode === 'row' && activeAddressMatch.row === data.row) return;
  clearAddressMatches();

  var peers = [];
  data.table.querySelectorAll('.addr-tooltip-wrap[data-full-addr]').forEach(function (trigger) {
    if (data.row.contains(trigger) || !isRenderedAddressTrigger(trigger)) return;
    var address = normalizeMatchAddress(trigger.getAttribute('data-full-addr'));
    if (!address || data.addresses.indexOf(address) === -1) return;
    peers.push(trigger);
  });
  if (!peers.length) return;

  peers.forEach(function (trigger) {
    trigger.classList.add('address-match-active', 'address-match-peer');
  });
  activeAddressMatch = { mode: 'row', table: data.table, row: data.row, elements: peers };
}
```

Update the existing direct-address state to store `mode: 'address'` without changing its source/peer classes.

- [ ] **Step 3: Reconcile hover transitions without flicker**

Replace the address-only over/out path with a mode-aware controller:

```js
function showMatchForTarget(target) {
  var data = matchDataForTarget(target);
  if (!data) {
    clearAddressMatches();
  } else if (data.mode === 'address') {
    showAddressMatches(data);
  } else {
    showRowAddressMatches(data);
  }
}

function handleAddressMatchOver(event) {
  lastPointer = { x: event.clientX, y: event.clientY };
  showMatchForTarget(event.target);
}

function handleAddressMatchOut(event) {
  lastPointer = { x: event.clientX, y: event.clientY };
  if (event.relatedTarget) {
    showMatchForTarget(event.relatedTarget);
  } else {
    clearAddressMatches();
  }
}
```

Keep the existing delegated pointer/mouse listener installation and direct-address focus behavior. Update viewport reconciliation so it re-evaluates `document.elementFromPoint()` for both modes rather than assuming `activeAddressMatch.trigger` exists.

- [ ] **Step 4: Prevent touch from leaving cosmetic row state**

Add a delegated `pointerdown` listener that clears row-derived state when `event.pointerType` is present and is not `mouse`, without calling `preventDefault()` or `stopPropagation()`:

```js
document.addEventListener('pointerdown', function (event) {
  if (event.pointerType && event.pointerType !== 'mouse' && activeAddressMatch && activeAddressMatch.mode === 'row') {
    clearAddressMatches();
  }
});
```

This preserves every existing tap action.

- [ ] **Step 5: Run the focused test and verify GREEN**

Run:

```bash
node --test tests/address-match-highlighting.test.js
```

Expected: all original and new tests pass with zero failures.

- [ ] **Step 6: Refactor only duplicated match-state code**

If both application functions repeat class cleanup or rendered-trigger validation, extract only a small local helper such as `canonicalAddressForMatch(trigger)`. Do not change tooltip handling or table markup. Re-run:

```bash
node --test tests/address-match-highlighting.test.js
```

Expected: all tests remain green.

- [ ] **Step 7: Commit the controller**

```bash
git add shared-hover-tooltips.js tests/address-match-highlighting.test.js
git commit -m "feat: reveal repeated wallets from row hover"
```

---

### Task 3: Advance cache contracts without changing the table allowlist

**Files:**
- Modify: `tests/test_address_match_table_scope.py`
- Modify: `assets-preview.html`
- Modify: `dolo-preview.html`
- Modify: `portfolio-preview.html`
- Modify: `revenue-preview.html`
- Modify: `rewards-preview.html`
- Modify: `tvl-preview.html`
- Modify: `vedolo-preview.html`
- Modify: `odolo-preview.html`
- Modify: `liquidation-preview.html`
- Modify: `assets/index.html`
- Modify: `dolo/index.html`
- Modify: `portfolio/index.html`
- Modify: `revenue/index.html`
- Modify: `rewards/index.html`
- Modify: `tvl/index.html`
- Modify: `vedolo/index.html`
- Modify: `odolo/index.html`
- Modify: `borrow/index.html`
- Modify: `liquidation/index.html`
- Modify: `supply/index.html`
- Test: `tests/test_address_match_table_scope.py`

**Interfaces:**
- Consumes: Shared controller behavior from Task 2 and the current allowlist contract.
- Produces: Shared asset version `20260809-row-address-peers` and route loader suffix `row-address-peers-20260809`.

- [ ] **Step 1: Make the cache contract fail on the old version**

Change the test constants only:

```python
version = "20260809-row-address-peers"
```

and replace the route assertion with:

```python
self.assertIn("row-address-peers-20260809", route, route_name)
```

- [ ] **Step 2: Run the cache contract and verify RED**

Run:

```bash
python3 -m unittest tests.test_address_match_table_scope
```

Expected: cache-key assertions fail on the old `20260804-address-strong-final` and `address-strong-final-20260804` values while allowlist assertions remain green.

- [ ] **Step 3: Update only the shared asset and route cache strings**

Replace every shared tooltip asset query in the nine preview files with:

```text
shared-hover-tooltips.css?v=20260809-row-address-peers
shared-hover-tooltips.js?v=20260809-row-address-peers
```

Update the deferred fallback URL in `liquidation-preview.html` to the same JavaScript query string. Append `-row-address-peers-20260809` once to each of the eleven route loader `version` strings. Do not change any table attributes or content.

- [ ] **Step 4: Run cache and behavior tests and verify GREEN**

Run:

```bash
python3 -m unittest tests.test_address_match_table_scope
node --test tests/address-match-highlighting.test.js
```

Expected: the ten-table allowlist, all preview cache keys, all route keys, and all match behaviors pass.

- [ ] **Step 5: Commit cache propagation**

```bash
git add tests/test_address_match_table_scope.py *-preview.html assets/index.html dolo/index.html portfolio/index.html revenue/index.html rewards/index.html tvl/index.html vedolo/index.html odolo/index.html borrow/index.html liquidation/index.html supply/index.html
git commit -m "chore: refresh row address matching assets"
```

---

### Task 4: Verify browser behavior, regression scope, and production deployment

**Files:**
- Verify: `shared-hover-tooltips.js`
- Verify: `shared-hover-tooltips.css`
- Verify: all files changed in Tasks 1–3
- Create temporarily outside the repository: `/tmp/verify_row_address_matches.py`

**Interfaces:**
- Consumes: Completed controller and fresh cache keys.
- Produces: Fresh automated, computed-style, bounding-box, responsive, Git, and GitHub Pages evidence.

- [ ] **Step 1: Run syntax and targeted regression checks**

Run:

```bash
node --check shared-hover-tooltips.js
node --test tests/address-match-highlighting.test.js
python3 -m unittest tests.test_address_match_table_scope tests.test_responsive_layout_contracts tests.test_table_ui_consistency_contracts
git diff --check
```

Expected: every command exits 0 with zero failed tests and no whitespace errors.

- [ ] **Step 2: Inspect the local server helper interface**

Run:

```bash
python3 skills/webapp-testing/scripts/with_server.py --help
```

Expected: usage information describing `--server`, `--port`, and the command separator.

- [ ] **Step 3: Browser-check representative routes at desktop and phone widths**

Write `/tmp/verify_row_address_matches.py` with synchronous Playwright. For each representative route (`/borrow/`, `/supply/`, `/vedolo/`, `/odolo/`):

1. open a fresh page and wait for `domcontentloaded` plus the target opted-in table;
2. locate two visible wrappers with the same normalized `data-full-addr` in that table;
3. capture source row, peer row, source wrapper, and peer wrapper bounding boxes and computed backgrounds/shadows;
4. hover a non-address cell in the source row and assert only the peer wrapper gains `address-match-peer`;
5. hover the source wrapper and assert it gains `address-match-source` while peers remain `address-match-peer`;
6. assert source/peer row backgrounds and every captured bounding box remain unchanged;
7. move the pointer outside the table and assert all match classes clear;
8. repeat at `1440x1000` and `390x844`, and assert `document.documentElement.scrollWidth === document.documentElement.clientWidth` unless the route's existing table viewport intentionally scrolls inside its own wrapper.

Run it through the reviewed literal server command:

```bash
python3 skills/webapp-testing/scripts/with_server.py --server "python3 -m http.server 8765" --port 8765 -- python3 /tmp/verify_row_address_matches.py
```

Expected: the script prints a passing result for all four routes and both viewports, with no console errors attributable to the change.

- [ ] **Step 4: Review the exact production diff**

Run:

```bash
git status --short
git diff dolomite-dashboard/master...HEAD -- shared-hover-tooltips.js shared-hover-tooltips.css tests/address-match-highlighting.test.js tests/test_address_match_table_scope.py
git diff --stat dolomite-dashboard/master...HEAD
```

Expected: only the design/plan, shared controller, tests, cache strings, and route version suffixes are changed. `shared-hover-tooltips.css` may remain unchanged because the row interaction reuses its existing quiet peer style. No revenue file, generated JSON, or workflow appears in the diff.

- [ ] **Step 5: Rebase on the latest production master and repeat decisive verification**

Run:

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
node --check shared-hover-tooltips.js
node --test tests/address-match-highlighting.test.js
python3 -m unittest tests.test_address_match_table_scope
git diff --check dolomite-dashboard/master...HEAD
```

Expected: rebase succeeds and every decisive check exits 0.

- [ ] **Step 6: Push the verified branch to production master**

Run:

```bash
git push dolomite-dashboard HEAD:master
```

Expected: Git reports the production `master` advanced to the verified commit.

- [ ] **Step 7: Verify GitHub Pages deployment and live assets**

Poll the repository Actions/Pages state until the workflow for the pushed commit is successful. Then fetch the live route and shared asset with cache bypass, confirm `20260809-row-address-peers` is referenced, and run the representative live hover assertion once on `https://twojekrypto.github.io/Dolomite-dashboard/`.

- [ ] **Step 8: Report the deployed scope and rebate diagnosis**

Report:

- the production commit and successful Pages check;
- the ten tables receiving row-hover peer-address matching;
- direct address hover remains the stronger gold treatment;
- no financial data changed;
- epoch 9 (16–22 July) is hidden by the parser's positive-delta-only assumption after an official downward root correction;
- 30 July onward remains pending because rolling claims are published only through epoch 10 while later epochs are not yet available.
