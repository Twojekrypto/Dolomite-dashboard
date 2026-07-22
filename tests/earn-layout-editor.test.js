const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const editor = require('../earn/earn-layout-editor.js');
const core = fs.readFileSync('earn/earn-core.html', 'utf8');
const source = fs.readFileSync('earn/earn-layout-editor.js', 'utf8');
const editorCss = fs.readFileSync('earn/earn-layout-editor.css', 'utf8');

const total = layout => layout.order.reduce((sum, key) => sum + layout.widths[key], 0);

test('supply, borrow and Past & Routed defaults contain all required keys and total 100%', () => {
  assert.deepEqual(editor.createDefaultLayout('supply').order, ['token', 'quality', 'price', 'supply', 'balance', 'yield', 'details']);
  assert.deepEqual(editor.createDefaultLayout('borrow').order, ['health', 'emode', 'collateral', 'debt', 'pnl', 'details']);
  assert.deepEqual(editor.createDefaultLayout('past').order, ['token', 'quality', 'yield', 'details']);
  for (const name of ['supply', 'borrow', 'past']) {
    assert.equal(Number(total(editor.createDefaultLayout(name)).toFixed(6)), 100);
  }
});

test('editor is restricted to an explicit loopback query', () => {
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'localhost', search: '?layoutEditor=1' }), true);
  assert.equal(editor.isLocalEditorEnabled({ hostname: '127.0.0.1', search: '?layoutEditor=1' }), true);
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'twojekrypto.github.io', search: '?layoutEditor=1' }), false);
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'localhost', search: '' }), false);
});

test('local editor controls stay available in a narrow local viewport', () => {
  assert.doesNotMatch(editorCss, /@media \(max-width: 700px\)[\s\S]*?\.earn-layout-editor-toolbar[\s\S]*?display: none !important/);
});

test('reorder and one spacer preserve the complete supply schema', () => {
  const base = editor.createDefaultLayout('supply');
  const moved = editor.reorderLayout('supply', base, 'details', 'price', false);
  assert.deepEqual(moved.order, ['token', 'quality', 'details', 'price', 'supply', 'balance', 'yield']);
  const added = editor.addSpacer('supply', moved);
  assert.equal(added.order.filter(key => key === 'spacer').length, 1);
  assert.deepEqual(editor.addSpacer('supply', added), added);
  assert.equal(Number(total(added).toFixed(6)), 100);
  assert.equal(Number(total(editor.removeSpacer('supply', added)).toFixed(6)), 100);
});

test('resize can shrink a column to the non-zero safety floor without changing other columns', () => {
  const base = editor.createDefaultLayout('borrow');
  const shrunk = editor.resizeLayout('borrow', base, 'details', -10_000, 1100);
  assert.equal(shrunk.widths.details, 0.1);
  for (const key of base.order.filter(key => key !== 'details')) {
    assert.equal(shrunk.widths[key], base.widths[key]);
  }
  assert.ok(total(shrunk) < 100);
});

test('resizing grows only the dragged column and exposes horizontal table width', () => {
  const base = editor.createDefaultLayout('supply');
  const widened = editor.resizeLayout('supply', base, 'details', 300, 1000);
  assert.ok(widened.widths.details > base.widths.details);
  for (const key of base.order.filter(key => key !== 'details')) {
    assert.equal(widened.widths[key], base.widths[key]);
  }
  assert.ok(editor.getLayoutTableWidth('supply', widened) > 100);
});

test('saved state requires both valid table layouts', () => {
  const valid = editor.normalizeSavedLayouts({
    version: 1,
    supply: editor.createDefaultLayout('supply'),
    borrow: editor.createDefaultLayout('borrow'),
  });
  assert.ok(valid);
  assert.equal(editor.normalizeSavedLayouts({ version: 1, supply: valid.supply }), null);
});

test('previous saved layouts gain the default Past & Routed layout without changing supply or borrow', () => {
  const previous = {
    version: 1,
    supply: editor.createDefaultLayout('supply'),
    borrow: editor.createDefaultLayout('borrow'),
  };
  const migrated = editor.normalizeSavedLayouts(previous);
  assert.ok(migrated);
  assert.deepEqual(migrated.past, editor.createDefaultLayout('past'));
  assert.deepEqual(migrated.supply, previous.supply);
  assert.deepEqual(migrated.borrow, previous.borrow);
});

test('Past & Routed supports reordering and one optional spacer', () => {
  const base = editor.createDefaultLayout('past');
  const moved = editor.reorderLayout('past', base, 'details', 'quality', false);
  assert.deepEqual(moved.order, ['token', 'details', 'quality', 'yield']);
  const withSpacer = editor.addSpacer('past', moved);
  assert.equal(withSpacer.order.filter(key => key === 'spacer').length, 1);
  assert.equal(Number(total(withSpacer).toFixed(6)), 100);
  assert.deepEqual(editor.removeSpacer('past', withSpacer).order, moved.order);
});

test('legacy supply layout gains Quality without changing saved widths or order', () => {
  const legacy = {
    version: 1,
    supply: {
      version: 1,
      order: ['balance', 'token', 'details', 'spacer', 'price', 'supply', 'yield'],
      widths: { token: 32, price: 10, supply: 20, balance: 16, yield: 14, details: 4, spacer: 4 },
    },
    borrow: editor.createDefaultLayout('borrow'),
  };
  const migrated = editor.normalizeSavedLayouts(legacy);
  assert.deepEqual(migrated.supply.order, ['balance', 'token', 'quality', 'details', 'spacer', 'price', 'supply', 'yield']);
  assert.equal(migrated.supply.widths.price, 10);
  assert.equal(migrated.supply.widths.quality, 11);
});

test('legacy borrow layout gains E-Mode without changing saved widths or order', () => {
  const legacy = {
    version: 1,
    supply: editor.createDefaultLayout('supply'),
    borrow: {
      version: 1,
      order: ['debt', 'health', 'details', 'spacer', 'collateral', 'pnl'],
      widths: { health: 18, collateral: 25, debt: 24, pnl: 17, details: 12, spacer: 4 },
    },
  };
  const migrated = editor.normalizeSavedLayouts(legacy);
  assert.deepEqual(migrated.borrow.order, ['debt', 'health', 'emode', 'details', 'spacer', 'collateral', 'pnl']);
  assert.equal(migrated.borrow.widths.collateral, 25);
  assert.equal(migrated.borrow.widths.emode, 11);
});

test('core loads the editor only behind loopback and query checks', () => {
  assert.match(core, /hostname === 'localhost'/);
  assert.match(core, /new URLSearchParams\(window\.location\.search\)\.get\('layoutEditor'\) !== '1'/);
  assert.match(core, /earn-layout-editor\.js/);
  assert.match(core, /earn-layout-editor\.css/);
});

test('DOM adapter registers all EARN table schemas and re-applies after DOM mutations', () => {
  assert.match(source, /const api = factory\(root\)/);
  assert.match(source, /earn-supply-section/);
  assert.match(source, /earn-lending-section/);
  assert.match(source, /earn-past-section/);
  assert.match(core, /data-earn-layout-table="past"/);
  assert.match(source, /\['supply', 'borrow', 'past'\]/);
  assert.match(source, /new MutationObserver/);
  assert.match(source, /style\.setProperty\('width', `\$\{valid\.widths\[key\]\}%`, 'important'\)/);
  assert.match(source, /earn-layout-draft\.json/);
});
