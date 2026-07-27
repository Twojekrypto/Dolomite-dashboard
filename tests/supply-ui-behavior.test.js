const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ui = require('../supply/supply-draft.js');

const liquidationSource = fs.readFileSync(
  path.join(__dirname, '..', 'liquidation-preview.html'),
  'utf8',
);
const assetChangeStart = liquidationSource.indexOf('async function onSupplyAssetChange() {');
const assetChangeEnd = liquidationSource.indexOf('\n        // ─── Launch', assetChangeStart);
const onSupplyAssetChangeSource = liquidationSource.slice(assetChangeStart, assetChangeEnd).trim();

async function captureNewMarketLoadingPresentation(previousOverview) {
  const context = vm.createContext({
    getSupplyActivityHistoryPresentation: ui.getSupplyActivityHistoryPresentation,
  });
  vm.runInContext(`
    const elements = new Map([
      ['supply-chain-select', { value: 'ethereum', style: {} }],
      ['supply-asset-select', { value: 'new-token', style: {} }],
    ]);
    const document = {
      getElementById(id) {
        if (!elements.has(id)) elements.set(id, { style: {}, value: '', innerHTML: '', textContent: '' });
        return elements.get(id);
      },
    };
    let currentSupplyOverview = ${JSON.stringify(previousOverview)};
    let supplyTabRequestId = 0;
    const SUPPLY_ACTIVITY_DEFAULT_DAYS = 30;
    const SUPPLY_CACHE_TTLS_MS = {
      suppliers: 1,
      activity: 1,
      fullActivity: 1,
      history: 1,
    };
    const supplySuppliersCache = new Map();
    const supplyRecentActivityCache = new Map();
    const supplyFullActivityCache = new Map();
    const supplyFlowHistoryCache = new Map();
    const currentSupplyTokensMap = {
      'new-token': { id: 'new-token', symbol: 'NEW', decimals: 18, marketId: '1' },
    };
    const currentSupplyOraclePrices = {};
    const currentSupplyInterestIndexes = {};
    const DOLOMITE_MINI_GRAPHS = { ethereum: 'https://example.invalid/subgraph' };
    const getSupplyCacheChainKey = chain => chain;
    const getFreshSupplyCacheData = () => null;
    const getAnySupplyCacheData = () => null;
    const setSupplyIntelLoading = () => {};
    let transitionPresentation = null;
    const setSupplyActivityLoading = () => {
      transitionPresentation = getSupplyActivityHistoryPresentation(currentSupplyOverview);
      throw new Error('captured-new-market-loading');
    };
    ${onSupplyAssetChangeSource}
    globalThis.transitionPromise = onSupplyAssetChange();
  `, context);

  await assert.rejects(context.transitionPromise, /captured-new-market-loading/);
  return vm.runInContext('transitionPresentation', context);
}

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

test('new-market loading does not inherit a previous full history status', async () => {
  assert.deepEqual(
    await captureNewMarketLoadingPresentation({ activityStage: 'full' }),
    { copy: 'Loading latest 30D activity…', mode: 'loading' },
  );
});

test('new-market loading does not inherit a previous full-history error', async () => {
  assert.deepEqual(
    await captureNewMarketLoadingPresentation({ activityFullError: true }),
    { copy: 'Loading latest 30D activity…', mode: 'loading' },
  );
});
