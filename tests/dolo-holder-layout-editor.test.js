const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const editor = require('../dolo-holder-layout-editor.js');
const preview = fs.readFileSync('dolo-preview.html', 'utf8');
const editorSource = fs.readFileSync('dolo-holder-layout-editor.js', 'utf8');

const total = layout => layout.order.reduce((sum, key) => sum + layout.widths[key], 0);

test('distribution and details layouts contain every required key and total 100%', () => {
  for (const name of ['distribution', 'details']) {
    const layout = editor.createDefaultLayout(name);
    assert.deepEqual(layout.order, editor.SCHEMAS[name].keys);
    assert.equal(Number(total(layout).toFixed(6)), 100);
  }
});

test('the saved holder table defaults preserve the approved column proportions', () => {
  assert.deepEqual(editor.createDefaultLayout('distribution').widths, {
    group:50.630228, balance:14.756621, wallets:10.347846, change:13.807863, details:10.457442,
  });
  assert.deepEqual(editor.createDefaultLayout('details').widths, {
    chain:13.1423, address:57.0044, dolo:17.2978, change:12.5555,
  });
});

test('normalization rejects missing, duplicate, unknown, multi-spacer, and non-100% layouts', () => {
  const layout = editor.createDefaultLayout('details');
  assert.equal(editor.normalizeLayout('details', {...layout, order:['chain', 'chain', 'dolo', 'change']}), null);
  assert.equal(editor.normalizeLayout('details', {...layout, order:['chain', 'address', 'dolo', 'change', 'unknown'], widths:{...layout.widths, unknown:1}}), null);
  assert.equal(editor.normalizeLayout('details', {...layout, order:['chain', 'address', 'dolo', 'change', 'spacer', 'spacer'], widths:{...layout.widths, spacer:4}}), null);
  assert.equal(editor.normalizeLayout('details', {...layout, widths:{...layout.widths, chain:30}}), null);
});

test('reorder moves a complete column without changing its widths', () => {
  const base = editor.createDefaultLayout('distribution');
  const moved = editor.reorderLayout('distribution', base, 'details', 'balance', false);
  assert.deepEqual(moved.order, ['group', 'details', 'balance', 'wallets', 'change']);
  assert.deepEqual(moved.widths, base.widths);
});

test('resize preserves total width while using every remaining column before reaching the technical minimum', () => {
  const base = editor.createDefaultLayout('details');
  const widened = editor.resizeLayout('details', base, 'chain', 600, 1000);
  assert.ok(widened.widths.chain > 60);
  assert.ok(widened.widths.address >= editor.SCHEMAS.details.minimums.address / 10);
  assert.ok(widened.widths.dolo >= editor.SCHEMAS.details.minimums.dolo / 10);
  assert.ok(widened.widths.change >= editor.SCHEMAS.details.minimums.change / 10);
  assert.equal(Number(total(widened).toFixed(6)), 100);

  const narrowed = editor.resizeLayout('details', base, 'chain', -600, 1000);
  assert.ok(narrowed.widths.chain >= editor.SCHEMAS.details.minimums.chain / 10);
  assert.equal(Number(total(narrowed).toFixed(6)), 100);
});

test('one spacer can move and is removed without losing width', () => {
  const base = editor.createDefaultLayout('distribution');
  const added = editor.addSpacer('distribution', base);
  const moved = editor.reorderLayout('distribution', added, 'spacer', 'change', false);
  assert.equal(moved.order.filter(key => key === 'spacer').length, 1);
  assert.equal(Number(total(moved).toFixed(6)), 100);
  const removed = editor.removeSpacer('distribution', moved);
  assert.deepEqual(removed.order, base.order);
  assert.equal(Number(total(removed).toFixed(6)), 100);
});

test('local editor is gated by the query and saves the two layouts together', () => {
  assert.equal(typeof editor.initDoloHolderLayoutEditor, 'function');
  assert.equal(typeof editor.normalizeSavedLayouts, 'function');
  assert.match(editorSource, /dolomite:dolo-holder-layout-editor:v1/);
  assert.match(editorSource, /holderDistribution/);
  assert.match(editorSource, /holderDetails/);
  assert.doesNotMatch(preview, /<script[^>]+src="dolo-holder-layout-editor\.js/);
  assert.match(preview, /layoutEditor[\s\S]*dolo-holder-layout-editor\.js/);
});

test('saved layout requires valid distribution and Details sections', () => {
  const valid = editor.normalizeSavedLayouts({
    version: 1,
    holderDistribution: editor.createDefaultLayout('distribution'),
    holderDetails: editor.createDefaultLayout('details'),
  });
  assert.ok(valid);
  assert.equal(editor.normalizeSavedLayouts({version: 1, holderDistribution: valid.holderDistribution}), null);
});

test('editor reorders detail columns through pointer drag rather than native HTML drag events', () => {
  assert.match(editorSource, /drag\.addEventListener\("pointerdown", event => startColumnDrag\(event, name, container, key\)\)/);
  assert.doesNotMatch(editorSource, /drag\.addEventListener\("dragstart"/);
});

test('distribution columns drag from their header without visible grip buttons', () => {
  assert.match(editorSource, /const directHeaderDrag = name === "distribution";/);
  assert.match(editorSource, /if\(directHeaderDrag\)\{\s*drag\?\.remove\(\);/);
  assert.match(editorSource, /header\.addEventListener\("pointerdown", event => \{\s*if\(event\.target\.closest\("\.dolo-layout-resize-handle"\)\) return;\s*startColumnDrag\(event, name, container, key\);/);
});

test('resize captures and always releases the pointer after editing a column', () => {
  assert.match(editorSource, /function startResize[\s\S]*source\.setPointerCapture\?\.\(event\.pointerId\)/);
  assert.match(editorSource, /function startResize[\s\S]*root\.addEventListener\("pointercancel", stop, \{once:true\}\)/);
  assert.match(editorSource, /function startResize[\s\S]*source\.releasePointerCapture\?\.\(event\.pointerId\)/);
});
