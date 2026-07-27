const test = require('node:test');
const assert = require('node:assert/strict');
const ui = require('../supply/supply-draft.js');

test('count badges expose total and filtered rows in the DOLO Holders hierarchy', () => {
  assert.equal(ui.formatSupplyCountBadge(777, 777, 'suppliers'), '777 suppliers · showing 777');
  assert.equal(ui.formatSupplyCountBadge(706, 84, 'events'), '706 events · showing 84');
});

test('table footer renders the visible range and centered pager without a redundant total', () => {
  const html = ui.buildSupplyTableFooter(1, 78, 777, 10, 'supply_goPage');
  assert.match(html, /class="supply-page-range">1–10 of 777</);
  assert.match(html, /class="supply-pager-controls"/);
  assert.match(html, />1 \/ 78</);
  assert.doesNotMatch(html, /flow-pager-total|777 wallets|777 events/);
});

test('empty table footer preserves a stable zero range and disabled navigation', () => {
  const html = ui.buildSupplyTableFooter(1, 1, 0, 10, 'supply_goPage');
  assert.match(html, /class="supply-page-range">0–0 of 0</);
  assert.equal((html.match(/disabled/g) || []).length, 4);
});

test('activity history presentation distinguishes loading, recent, full, and error states', () => {
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation(null), {
    copy: 'Loading latest 30D activity…', mode: 'loading',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({ activityStage: 'recent' }), {
    copy: '30D history', mode: '',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({
    activityStage: 'recent', activityFullLoading: true,
  }), {
    copy: 'Loading full history…', mode: 'loading',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({ activityStage: 'full' }), {
    copy: 'Full history', mode: 'full',
  });
  assert.deepEqual(ui.getSupplyActivityHistoryPresentation({ activityFullError: true }), {
    copy: 'Full history unavailable', mode: 'error',
  });
});
