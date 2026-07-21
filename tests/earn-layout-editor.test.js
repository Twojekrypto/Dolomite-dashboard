const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const editor = require('../earn/earn-layout-editor.js');
const core = fs.readFileSync('earn/earn-core.html', 'utf8');
const source = fs.readFileSync('earn/earn-layout-editor.js', 'utf8');

const total = layout => layout.order.reduce((sum, key) => sum + layout.widths[key], 0);

test('supply and borrow defaults contain all required keys and total 100%', () => {
  assert.deepEqual(editor.createDefaultLayout('supply').order, ['token', 'price', 'supply', 'balance', 'yield', 'details']);
  assert.deepEqual(editor.createDefaultLayout('borrow').order, ['health', 'collateral', 'debt', 'pnl', 'details']);
  for (const name of ['supply', 'borrow']) {
    assert.equal(Number(total(editor.createDefaultLayout(name)).toFixed(6)), 100);
  }
});

test('editor is restricted to an explicit loopback query', () => {
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'localhost', search: '?layoutEditor=1' }), true);
  assert.equal(editor.isLocalEditorEnabled({ hostname: '127.0.0.1', search: '?layoutEditor=1' }), true);
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'twojekrypto.github.io', search: '?layoutEditor=1' }), false);
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'localhost', search: '' }), false);
});

test('reorder and one spacer preserve the complete supply schema', () => {
  const base = editor.createDefaultLayout('supply');
  const moved = editor.reorderLayout('supply', base, 'details', 'price', false);
  assert.deepEqual(moved.order, ['token', 'details', 'price', 'supply', 'balance', 'yield']);
  const added = editor.addSpacer('supply', moved);
  assert.equal(added.order.filter(key => key === 'spacer').length, 1);
  assert.deepEqual(editor.addSpacer('supply', added), added);
  assert.equal(Number(total(added).toFixed(6)), 100);
  assert.equal(Number(total(editor.removeSpacer('supply', added)).toFixed(6)), 100);
});

test('resize preserves technical minimums without changing other column constraints', () => {
  const base = editor.createDefaultLayout('borrow');
  const widened = editor.resizeLayout('borrow', base, 'details', 500, 1100);
  assert.ok(widened.widths.details > base.widths.details);
  for (const key of widened.order) {
    assert.ok(widened.widths[key] >= editor.SCHEMAS.borrow.minimums[key] / 11);
  }
  assert.ok(total(widened) > 100);
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

test('core loads the editor only behind loopback and query checks', () => {
  assert.match(core, /hostname === 'localhost'/);
  assert.match(core, /new URLSearchParams\(window\.location\.search\)\.get\('layoutEditor'\) !== '1'/);
  assert.match(core, /earn-layout-editor\.js/);
  assert.match(core, /earn-layout-editor\.css/);
});

test('DOM adapter registers both table schemas and re-applies after DOM mutations', () => {
  assert.match(source, /const api = factory\(root\)/);
  assert.match(source, /earn-supply-section/);
  assert.match(source, /earn-lending-section/);
  assert.match(source, /new MutationObserver/);
  assert.match(source, /style\.setProperty\('width', `\$\{valid\.widths\[key\]\}%`, 'important'\)/);
  assert.match(source, /earn-layout-draft\.json/);
});
