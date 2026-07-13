# Portfolio Open Borrows Layout Editor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local-only editor that lets the user reorder and resize the real Portfolio Open Borrows columns, insert one resizable blank spacer, and export the chosen 100%-width layout as JSON.

**Architecture:** Put deterministic layout validation and transforms in a small CommonJS/browser-compatible `portfolio-layout-editor.js` module, then let its DOM adapter decorate the existing Open Borrows table only when `?layoutEditor=1` is present. Add stable `data-column` keys to the existing table so sorting and rerenders keep working while the editor reorders complete columns. Keep editor styling in a removable `portfolio-layout-editor.css` file.

**Tech Stack:** Static HTML/CSS/JavaScript, Node.js built-in test runner, Python `unittest`, browser verification through a local HTTP server.

## Global Constraints

- The editor is local-only and must not be pushed before the user approves the exported layout.
- Preserve the existing uncommitted E-Mode icon changes in `portfolio-preview.html` and `tests/test_portfolio_preview_contracts.py`.
- Without `?layoutEditor=1`, Portfolio must retain its current rendering and behavior.
- Keep exactly six required data keys: `chain`, `account`, `health`, `emode`, `collateral`, and `debt`.
- Allow at most one optional `spacer` key.
- Every valid layout must have finite positive widths summing to `100%` within `0.001`.
- Resize one column against its nearest visible neighbor and enforce per-column minimum widths.
- Preserve existing sorting when the user clicks outside the dedicated drag handle.
- Use `python3 -m http.server`; do not test through `file://`.
- Do not stage or commit temporary editor implementation files. Keep them as an inspectable local working-tree change until the user chooses the final layout.

---

### Task 1: Implement And Test The Layout Model

**Files:**
- Create: `portfolio-layout-editor.js`
- Create: `tests/open-borrows-layout-editor.test.js`

**Interfaces:**
- Produces: `createDefaultLayout() -> Layout`, `normalizeLayout(value) -> Layout|null`, `reorderLayout(layout, movedKey, targetKey, placeAfter) -> Layout`, `resizeLayout(layout, key, deltaPx, tableWidthPx) -> Layout`, `addSpacer(layout) -> Layout`, and `removeSpacer(layout) -> Layout`.
- `Layout` is `{ version: 1, order: string[], widths: Record<string, number> }`.

- [ ] **Step 1: Write failing model tests**

Create `tests/open-borrows-layout-editor.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert/strict');
const editor = require('../portfolio-layout-editor.js');

const total = layout => Object.values(layout.widths).reduce((sum, value) => sum + value, 0);

test('default layout contains each required column once and totals 100%', () => {
  const layout = editor.createDefaultLayout();
  assert.deepEqual(layout.order, ['chain', 'account', 'health', 'emode', 'collateral', 'debt']);
  assert.equal(Number(total(layout).toFixed(6)), 100);
});

test('normalization rejects duplicate, unknown, missing, or multi-spacer layouts', () => {
  const base = editor.createDefaultLayout();
  assert.equal(editor.normalizeLayout({ ...base, order: ['chain', 'chain', 'health', 'emode', 'collateral', 'debt'] }), null);
  assert.equal(editor.normalizeLayout({ ...base, order: [...base.order, 'unknown'], widths: { ...base.widths, unknown: 1 } }), null);
  assert.equal(editor.normalizeLayout({ ...base, order: base.order.slice(1), widths: { ...base.widths } }), null);
  assert.equal(editor.normalizeLayout({ ...base, order: [...base.order, 'spacer', 'spacer'], widths: { ...base.widths, spacer: 4 } }), null);
});

test('reorder moves one complete key without changing widths', () => {
  const base = editor.createDefaultLayout();
  const moved = editor.reorderLayout(base, 'debt', 'account', false);
  assert.deepEqual(moved.order, ['chain', 'debt', 'account', 'health', 'emode', 'collateral']);
  assert.deepEqual(moved.widths, base.widths);
});

test('resize widens one column, shrinks its neighbor, and preserves 100%', () => {
  const base = editor.createDefaultLayout();
  const resized = editor.resizeLayout(base, 'chain', 60, 1200);
  assert.equal(resized.widths.chain, 16);
  assert.equal(resized.widths.account, 9);
  assert.equal(Number(total(resized).toFixed(6)), 100);
});

test('resize clamps at the selected and neighboring minimum widths', () => {
  const base = editor.createDefaultLayout();
  const tooWide = editor.resizeLayout(base, 'chain', 500, 1200);
  assert.ok(tooWide.widths.account >= editor.MIN_WIDTH_PX.account / 12);
  const tooNarrow = editor.resizeLayout(base, 'chain', -500, 1200);
  assert.ok(tooNarrow.widths.chain >= editor.MIN_WIDTH_PX.chain / 12);
  assert.equal(Number(total(tooNarrow).toFixed(6)), 100);
});

test('one spacer can be added and removed while preserving 100%', () => {
  const base = editor.createDefaultLayout();
  const added = editor.addSpacer(base);
  assert.equal(added.order.filter(key => key === 'spacer').length, 1);
  assert.deepEqual(editor.addSpacer(added), added);
  assert.equal(Number(total(added).toFixed(6)), 100);
  const removed = editor.removeSpacer(added);
  assert.deepEqual(removed.order, base.order);
  assert.equal(Number(total(removed).toFixed(6)), 100);
});
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
node --test tests/open-borrows-layout-editor.test.js
```

Expected: FAIL because `portfolio-layout-editor.js` does not exist.

- [ ] **Step 3: Implement the minimal pure layout model**

Create `portfolio-layout-editor.js` with a browser/CommonJS wrapper and these exact constants and transforms:

```js
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.OpenBorrowLayoutEditor = api;
})(typeof window !== 'undefined' ? window : null, function (root) {
  'use strict';

  const VERSION = 1;
  const DATA_KEYS = ['chain', 'account', 'health', 'emode', 'collateral', 'debt'];
  const DEFAULT_ORDER = DATA_KEYS.slice();
  const DEFAULT_WIDTHS = { chain: 11, account: 14, health: 11, emode: 8, collateral: 28, debt: 28 };
  const MIN_WIDTH_PX = { chain: 86, account: 110, health: 105, emode: 78, collateral: 150, debt: 150, spacer: 20 };
  const SPACER_WIDTH = 4;
  const clone = value => JSON.parse(JSON.stringify(value));
  const sumWidths = (order, widths) => order.reduce((sum, key) => sum + Number(widths[key]), 0);

  function createDefaultLayout() {
    return { version: VERSION, order: DEFAULT_ORDER.slice(), widths: { ...DEFAULT_WIDTHS } };
  }

  function normalizeLayout(value) {
    if (!value || value.version !== VERSION || !Array.isArray(value.order) || !value.widths) return null;
    const order = value.order.slice();
    if (new Set(order).size !== order.length) return null;
    if (DATA_KEYS.some(key => !order.includes(key))) return null;
    if (order.some(key => !DATA_KEYS.includes(key) && key !== 'spacer')) return null;
    if (order.filter(key => key === 'spacer').length > 1) return null;
    const widths = {};
    for (const key of order) {
      const width = Number(value.widths[key]);
      if (!Number.isFinite(width) || width <= 0) return null;
      widths[key] = width;
    }
    const total = sumWidths(order, widths);
    if (Math.abs(total - 100) > 0.001) return null;
    return { version: VERSION, order, widths };
  }

  function reorderLayout(layout, movedKey, targetKey, placeAfter) {
    const current = normalizeLayout(layout);
    if (!current || movedKey === targetKey || !current.order.includes(movedKey) || !current.order.includes(targetKey)) return clone(layout);
    const order = current.order.filter(key => key !== movedKey);
    let index = order.indexOf(targetKey) + (placeAfter ? 1 : 0);
    order.splice(index, 0, movedKey);
    return { ...current, order };
  }

  function resizeLayout(layout, key, deltaPx, tableWidthPx) {
    const current = normalizeLayout(layout);
    if (!current || !current.order.includes(key) || !(tableWidthPx > 0)) return clone(layout);
    const index = current.order.indexOf(key);
    const neighbor = current.order[index + 1] || current.order[index - 1];
    if (!neighbor) return current;
    const delta = Number(deltaPx) / Number(tableWidthPx) * 100;
    const minSelected = MIN_WIDTH_PX[key] / tableWidthPx * 100;
    const minNeighbor = MIN_WIDTH_PX[neighbor] / tableWidthPx * 100;
    const low = minSelected - current.widths[key];
    const high = current.widths[neighbor] - minNeighbor;
    const applied = Math.max(low, Math.min(high, delta));
    const widths = { ...current.widths };
    widths[key] += applied;
    widths[neighbor] -= applied;
    return { ...current, widths };
  }

  function addSpacer(layout) {
    const current = normalizeLayout(layout);
    if (!current || current.order.includes('spacer')) return clone(layout);
    const donor = current.widths.collateral >= current.widths.debt ? 'collateral' : 'debt';
    const order = current.order.slice();
    order.splice(order.indexOf('emode') + 1, 0, 'spacer');
    return { ...current, order, widths: { ...current.widths, [donor]: current.widths[donor] - SPACER_WIDTH, spacer: SPACER_WIDTH } };
  }

  function removeSpacer(layout) {
    const current = normalizeLayout(layout);
    if (!current || !current.order.includes('spacer')) return clone(layout);
    const index = current.order.indexOf('spacer');
    const recipient = current.order[index + 1] || current.order[index - 1];
    const order = current.order.filter(key => key !== 'spacer');
    const widths = { ...current.widths, [recipient]: current.widths[recipient] + current.widths.spacer };
    delete widths.spacer;
    return { ...current, order, widths };
  }

  const api = { VERSION, DATA_KEYS, DEFAULT_ORDER, DEFAULT_WIDTHS, MIN_WIDTH_PX, createDefaultLayout, normalizeLayout, reorderLayout, resizeLayout, addSpacer, removeSpacer };
  return api;
});
```

- [ ] **Step 4: Run the model tests and verify GREEN**

Run:

```bash
node --test tests/open-borrows-layout-editor.test.js
node --check portfolio-layout-editor.js
```

Expected: six tests pass and syntax check exits `0`.

---

### Task 2: Integrate The Local Editor With The Real Borrow Table

**Files:**
- Modify: `portfolio-preview.html:1284-1334,1878-1881,3220-3266,3401-3405,3528-3530`
- Modify: `tests/test_portfolio_preview_contracts.py:12-100`
- Modify: `portfolio-layout-editor.js`
- Create: `portfolio-layout-editor.css`
- Test: `tests/test_portfolio_preview_contracts.py`
- Test: `tests/open-borrows-layout-editor.test.js`

**Interfaces:**
- Consumes: the Task 1 layout API and table nodes identified by `.pf-borrow-positions`, `#pf-borrows-body`, and `#pf-borrows-section`.
- Produces: `initDomEditor()`, `applyLayoutToTable(table, layout)`, versioned `localStorage` persistence, toolbar commands, drag/drop, resize, and JSON export.

- [ ] **Step 1: Add failing Portfolio integration assertions**

Add to `PortfolioPreviewContractsTest.test_open_borrows_uses_risk_positions_ux`:

```python
        self.assertIn('src="portfolio-layout-editor.js?v=20260713-layout-lab-1"', self.html)
        for key in ("chain", "account", "health", "emode", "collateral", "debt"):
            self.assertIn(f'data-column="{key}"', self.html)
        self.assertIn('data-column="collateral"><div class="pf-money-cell">', borrow_render)
        self.assertIn('data-column="debt"><div class="pf-money-cell">', borrow_render)
        self.assertNotIn('#pf-borrows-section .pf-table tbody td:nth-child(5)', self.html)
```

- [ ] **Step 2: Run the focused contract test and verify RED**

Run:

```bash
python3 -m unittest tests/test_portfolio_preview_contracts.py -v
```

Expected: `test_open_borrows_uses_risk_positions_ux` fails because the editor script and stable column keys are absent.

- [ ] **Step 3: Give every borrow column a stable DOM key**

Change the table structure to:

```html
<colgroup>
  <col data-column="chain"><col data-column="account"><col data-column="health">
  <col data-column="emode"><col data-column="collateral"><col data-column="debt">
</colgroup>
<thead><tr>
  <th data-column="chain" data-sort="chain" data-table="bor">...</th>
  <th data-column="account">...</th>
  <th data-column="health" data-sort="hf" data-table="bor">...</th>
  <th data-column="emode" data-sort="emode" data-table="bor">...</th>
  <th data-column="collateral" data-sort="collateral" data-table="bor">...</th>
  <th data-column="debt" data-sort="debt" data-table="bor">...</th>
</tr></thead>
```

Add matching `data-column` attributes to every borrow data-cell and skeleton-cell template. Replace borrow-only `nth-child` styling with key selectors:

```css
#pf-borrows-section [data-column="chain"],
#pf-borrows-section [data-column="account"],
#pf-borrows-section [data-column="health"],
#pf-borrows-section [data-column="emode"]{overflow:visible}
#pf-borrows-section [data-column="collateral"],
#pf-borrows-section [data-column="debt"]{text-align:left;white-space:normal;overflow:visible}
```

Load the local editor immediately before `</body>`:

```html
<script src="portfolio-layout-editor.js?v=20260713-layout-lab-1"></script>
```

- [ ] **Step 4: Implement the DOM adapter and persistence**

Extend the browser branch of `portfolio-layout-editor.js` with:

```js
const STORAGE_KEY = 'dolomite:open-borrows-layout-editor:v1';
const EDITOR_QUERY = 'layoutEditor';

function isEditorEnabled() {
  return !!root && new URLSearchParams(root.location.search).get(EDITOR_QUERY) === '1';
}

function readStoredLayout() {
  try { return normalizeLayout(JSON.parse(root.localStorage.getItem(STORAGE_KEY))) || createDefaultLayout(); }
  catch (_) { return createDefaultLayout(); }
}

function persistLayout(layout) {
  const valid = normalizeLayout(layout);
  if (valid) root.localStorage.setItem(STORAGE_KEY, JSON.stringify(valid));
  return valid;
}

function ensureSpacerNodes(table, layout) {
  const enabled = layout.order.includes('spacer');
  const group = table.querySelector('colgroup');
  const headRow = table.tHead.rows[0];
  let col = group.querySelector('[data-column="spacer"]');
  let th = headRow.querySelector('[data-column="spacer"]');
  if (enabled && !col) { col = document.createElement('col'); col.dataset.column = 'spacer'; group.appendChild(col); }
  if (enabled && !th) { th = document.createElement('th'); th.dataset.column = 'spacer'; th.innerHTML = '<span class="pf-layout-spacer-label">Spacer</span>'; headRow.appendChild(th); }
  if (!enabled) { col && col.remove(); th && th.remove(); }
}

function applyLayoutToTable(table, layout) {
  const valid = normalizeLayout(layout);
  if (!valid) return false;
  ensureSpacerNodes(table, valid);
  const containers = [table.querySelector('colgroup'), table.tHead.rows[0]];
  containers.forEach(container => valid.order.forEach(key => {
    const node = container.querySelector(`[data-column="${key}"]`);
    if (node) container.appendChild(node);
  }));
  valid.order.forEach(key => {
    const col = table.querySelector(`col[data-column="${key}"]`);
    if (col) col.style.width = `${valid.widths[key]}%`;
  });
  table.tBodies[0].querySelectorAll('tr').forEach(row => {
    if (row.children.length <= 1) { if (row.firstElementChild) row.firstElementChild.colSpan = valid.order.length; return; }
    let spacer = row.querySelector('[data-column="spacer"]');
    if (valid.order.includes('spacer') && !spacer) { spacer = document.createElement('td'); spacer.dataset.column = 'spacer'; row.appendChild(spacer); }
    if (!valid.order.includes('spacer')) { spacer && spacer.remove(); }
    valid.order.forEach(key => {
      const cell = row.querySelector(`[data-column="${key}"]`);
      if (cell) row.appendChild(cell);
    });
  });
  return true;
}

function downloadLayout(layout, table) {
  const valid = normalizeLayout(layout);
  if (!valid) throw new Error('Invalid layout');
  const payload = { ...valid, spacer: valid.order.includes('spacer'), tableWidthPx: Math.round(table.getBoundingClientRect().width), savedAt: new Date().toISOString() };
  const url = URL.createObjectURL(new Blob([JSON.stringify(payload, null, 2) + '\n'], { type: 'application/json' }));
  const link = document.createElement('a');
  link.href = url; link.download = 'open-borrows-layout-draft.json'; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}
```

`initDomEditor()` must inject `portfolio-layout-editor.css`, insert one toolbar before `#pf-borrows-filters`, decorate each header with one `.pf-layout-drag-handle` and one `.pf-layout-resize-handle`, and observe `#pf-borrows-body` with a guarded `MutationObserver`. Drag/drop calls `reorderLayout`; pointer resizing calls `resizeLayout` from a snapshot captured on `pointerdown`; add/remove, reset, and save call the Task 1 APIs. Every accepted edit calls `persistLayout()` and `applyLayoutToTable()`.

Use this event structure so drag, resize, status, rerender, and cancellation behavior are explicit:

```js
function initDomEditor() {
  const section = document.querySelector('#pf-borrows-section');
  const table = section && section.querySelector('.pf-borrow-positions');
  const filters = section && section.querySelector('#pf-borrows-filters');
  if (!section || !table || !filters || section.querySelector('.pf-layout-toolbar')) return;
  document.body.classList.add('pf-layout-editor');
  const css = document.createElement('link');
  css.rel = 'stylesheet'; css.href = 'portfolio-layout-editor.css?v=20260713-layout-lab-1';
  document.head.appendChild(css);

  const toolbar = document.createElement('div');
  toolbar.className = 'pf-layout-toolbar';
  toolbar.innerHTML = '<span class="pf-layout-toolbar-label">Open Borrows layout</span><button type="button" class="pf-layout-button" data-layout-spacer>Add spacer</button><button type="button" class="pf-layout-button" data-layout-reset>Reset</button><button type="button" class="pf-layout-button" data-layout-save>Save layout</button><span class="pf-layout-status" aria-live="polite"></span>';
  filters.before(toolbar);

  let layout = readStoredLayout();
  let draggedKey = '';
  let resize = null;
  let observer = null;
  const status = toolbar.querySelector('.pf-layout-status');
  const spacerButton = toolbar.querySelector('[data-layout-spacer]');

  const render = () => {
    if (observer) observer.disconnect();
    applyLayoutToTable(table, layout);
    decorateHeaders();
    spacerButton.textContent = layout.order.includes('spacer') ? 'Remove spacer' : 'Add spacer';
    if (observer) observer.observe(table.tBodies[0], { childList: true });
  };
  const accept = next => {
    const valid = persistLayout(next);
    if (!valid) { status.textContent = 'Invalid layout'; return; }
    layout = valid; status.textContent = ''; render();
  };
  const clearDropMarker = () => table.querySelectorAll('.pf-layout-drop-before,.pf-layout-drop-after').forEach(node => node.classList.remove('pf-layout-drop-before', 'pf-layout-drop-after'));

  function decorateHeaders() {
    table.querySelectorAll('thead th[data-column]').forEach(th => {
      if (th.dataset.layoutDecorated === '1') return;
      th.dataset.layoutDecorated = '1';
      {
        const drag = document.createElement('span');
        drag.className = 'pf-layout-drag-handle'; drag.draggable = true; drag.textContent = '::'; drag.setAttribute('aria-label', `Move ${th.dataset.column} column`);
        drag.addEventListener('click', event => event.stopPropagation());
        drag.addEventListener('dragstart', event => { event.stopPropagation(); draggedKey = th.dataset.column; event.dataTransfer.effectAllowed = 'move'; });
        drag.addEventListener('dragend', () => { draggedKey = ''; clearDropMarker(); });
        th.prepend(drag);
      }
      {
        const handle = document.createElement('span');
        handle.className = 'pf-layout-resize-handle'; handle.setAttribute('aria-label', `Resize ${th.dataset.column} column`);
        handle.addEventListener('pointerdown', event => {
          event.preventDefault(); event.stopPropagation();
          resize = { key: th.dataset.column, startX: event.clientX, startLayout: clone(layout), tableWidth: table.getBoundingClientRect().width };
          handle.setPointerCapture(event.pointerId);
        });
        th.appendChild(handle);
      }
      th.addEventListener('dragover', event => {
        if (!draggedKey || draggedKey === th.dataset.column) return;
        event.preventDefault(); clearDropMarker();
        th.classList.add(event.clientX < th.getBoundingClientRect().left + th.getBoundingClientRect().width / 2 ? 'pf-layout-drop-before' : 'pf-layout-drop-after');
      });
      th.addEventListener('drop', event => {
        if (!draggedKey || draggedKey === th.dataset.column) return;
        event.preventDefault();
        accept(reorderLayout(layout, draggedKey, th.dataset.column, event.clientX >= th.getBoundingClientRect().left + th.getBoundingClientRect().width / 2));
        draggedKey = ''; clearDropMarker();
      });
    });
  }

  const finishPointer = () => { resize = null; };
  root.addEventListener('pointermove', event => { if (resize) accept(resizeLayout(resize.startLayout, resize.key, event.clientX - resize.startX, resize.tableWidth)); });
  root.addEventListener('pointerup', finishPointer);
  root.addEventListener('pointercancel', finishPointer);
  root.addEventListener('blur', () => { finishPointer(); draggedKey = ''; clearDropMarker(); });
  document.addEventListener('keydown', event => { if (event.key === 'Escape') { finishPointer(); draggedKey = ''; clearDropMarker(); } });
  spacerButton.addEventListener('click', () => accept(layout.order.includes('spacer') ? removeSpacer(layout) : addSpacer(layout)));
  toolbar.querySelector('[data-layout-reset]').addEventListener('click', () => accept(createDefaultLayout()));
  toolbar.querySelector('[data-layout-save]').addEventListener('click', () => { try { downloadLayout(layout, table); status.textContent = 'Saved'; } catch (_) { status.textContent = 'Invalid layout'; } });
  observer = new MutationObserver(render);
  render();
}
```

Keep `clone` inside the module closure so the resize snapshot is immutable. Disconnecting the observer around `applyLayoutToTable()` is required because moving existing cells creates asynchronous child-list records; a boolean guard alone would permit a render loop after the guard is cleared.

Initialize only in a browser and only for the query-gated mode:

```js
if (isEditorEnabled()) {
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initDomEditor, { once: true });
  else initDomEditor();
}
```

- [ ] **Step 5: Add isolated editor styling**

Create `portfolio-layout-editor.css` with the local toolbar, fixed header dimensions, resize cursor, drop marker, and visible editor-only spacer:

```css
body.pf-layout-editor #pf-borrows-section{overflow:visible}
.pf-layout-toolbar{display:flex;align-items:center;gap:8px;padding:10px 22px;border-bottom:1px solid var(--line-1);background:rgba(201,162,39,.035)}
.pf-layout-toolbar-label{margin-right:auto;color:var(--fg-2);font-size:11px;font-weight:700}
.pf-layout-button{height:30px;padding:0 11px;border:1px solid var(--line-2);border-radius:7px;background:var(--bg-3);color:var(--fg-2);font:700 10px var(--font);cursor:pointer}
.pf-layout-button:hover,.pf-layout-button:focus-visible{color:var(--gold);border-color:var(--gold-line);outline:0}
.pf-layout-status{min-width:72px;color:var(--fg-3);font:600 10px var(--mono)}
body.pf-layout-editor .pf-borrow-positions th[data-column]{position:relative;padding-left:30px;padding-right:18px}
.pf-layout-drag-handle{position:absolute;left:8px;top:50%;width:14px;height:22px;transform:translateY(-50%);cursor:grab;color:var(--fg-3)}
.pf-layout-drag-handle:active{cursor:grabbing}
.pf-layout-resize-handle{position:absolute;right:-4px;top:0;width:9px;height:100%;z-index:4;cursor:col-resize}
.pf-layout-resize-handle::after{content:"";position:absolute;left:4px;top:20%;width:1px;height:60%;background:var(--line-3)}
.pf-layout-drop-before::before,.pf-layout-drop-after::after{content:"";position:absolute;top:0;bottom:0;width:2px;background:var(--gold);box-shadow:0 0 8px rgba(201,162,39,.55);z-index:5}
.pf-layout-drop-before::before{left:0}.pf-layout-drop-after::after{right:0}
[data-column="spacer"]{background:rgba(201,162,39,.025)}
.pf-layout-spacer-label{color:var(--gold);font-size:8px;letter-spacing:.7px}
#pf-borrows-body td[data-column="spacer"]{padding:0!important}
```

- [ ] **Step 6: Run automated checks and verify GREEN**

Run:

```bash
node --test tests/open-borrows-layout-editor.test.js
node --check portfolio-layout-editor.js
python3 -m unittest tests/test_portfolio_preview_contracts.py -v
git diff --check
```

Expected: all tests pass, syntax exits `0`, and `git diff --check` prints nothing.

---

### Task 3: Verify The Complete Local Editing Flow And Open It For The User

**Files:**
- Verify: `portfolio-preview.html`
- Verify: `portfolio-layout-editor.js`
- Verify: `portfolio-layout-editor.css`
- Verify download: `~/Downloads/open-borrows-layout-draft.json`

**Interfaces:**
- Consumes: the query-gated editor and real borrow wallet `0x0480f1cbe27fd5eae8ae7c4d5ff764e30f91aa5f`.
- Produces: a running local URL ready for user interaction and a verified exported JSON draft.

- [ ] **Step 1: Start a local HTTP server on an unused port**

Run:

```bash
python3 -m http.server 4173
```

Expected: server remains running and serves the repository root at `http://127.0.0.1:4173/`.

- [ ] **Step 2: Open the editor with real borrow data**

Open:

```text
http://127.0.0.1:4173/portfolio-preview.html?layoutEditor=1#0x0480f1cbe27fd5eae8ae7c4d5ff764e30f91aa5f
```

Expected: Open Borrows loads at least one real row and the local-only toolbar is visible.

- [ ] **Step 3: Verify geometry and editor gating in the browser**

Evaluate:

```js
const table = document.querySelector('.pf-borrow-positions');
const order = [...table.querySelectorAll('thead th[data-column]')].map(node => node.dataset.column);
const widths = [...table.querySelectorAll('col[data-column]')].map(node => node.getBoundingClientRect().width);
({
  order,
  rowOrder: [...document.querySelector('#pf-borrows-body tr.pf-row').children].map(node => node.dataset.column),
  tableWidth: table.getBoundingClientRect().width,
  columnWidth: widths.reduce((sum, width) => sum + width, 0),
  overflow: table.scrollWidth - table.clientWidth,
  toolbar: !!document.querySelector('.pf-layout-toolbar')
});
```

Expected: header and row orders match, column widths equal the table width within two pixels, overflow is `0`, and toolbar is `true`. Open the same URL without `layoutEditor=1`; expected toolbar is absent and canonical order is unchanged.

- [ ] **Step 4: Exercise drag, resize, spacer, rerender, and persistence**

Manually drag `Debt` before `Account ID`, resize `Chain`, add the spacer, drag it between `E-Mode` and `Collateral`, then click the `Debt` sort header and toggle `Hide Dust`.

Expected: row cells always match header order, the table remains 100% wide, sorting still works, the spacer remains blank, and refreshing preserves the edited layout.

- [ ] **Step 5: Verify the exported file**

Click `Save layout`, then run:

```bash
python3 -m json.tool "$HOME/Downloads/open-borrows-layout-draft.json"
```

Expected: valid JSON with `version: 1`, every required key exactly once, at most one `spacer`, `spacer: true` when enabled, a positive `tableWidthPx`, and widths totaling `100`.

- [ ] **Step 6: Run the focused regression suite and leave the server running**

Run:

```bash
node --test tests/open-borrows-layout-editor.test.js && \
node --check portfolio-layout-editor.js && \
python3 -m unittest tests/test_portfolio_preview_contracts.py tests/test_earn_dashboard_contracts.py -q && \
git diff --check
```

Expected: all checks pass. Do not push, stage, or commit temporary editor files. Keep the HTTP server alive and give the user the local editor URL.
