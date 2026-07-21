const test = require('node:test');
const assert = require('node:assert/strict');
const editor = require('../earn/earn-layout-editor.js');

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

test('resize uses available donor width and never crosses technical minimums', () => {
  const base = editor.createDefaultLayout('borrow');
  const widened = editor.resizeLayout('borrow', base, 'details', 500, 1100);
  assert.ok(widened.widths.details > base.widths.details);
  for (const key of widened.order) {
    assert.ok(widened.widths[key] >= editor.SCHEMAS.borrow.minimums[key] / 11);
  }
  assert.equal(Number(total(widened).toFixed(6)), 100);
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
