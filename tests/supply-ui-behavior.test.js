const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ui = require('../supply/supply-draft.js');

const DGM_BTC = '0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef';
const SAVETH_BASE = '0x23e3df1196b3249c9b0a9476f990f105591872de';
const DSAVETH = '0x51bc8e41cbec0aa97ec07c73597829c70b2eed46';
const SAVETH_ICON = 'https://app.dolomite.io/static/media/savETH.1c28535854c4a65f2a4786a2f02ae499.svg';

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

test('count badges show only the unfiltered supplier and event totals', () => {
  assert.equal(ui.formatSupplyCountBadge(3620, 'suppliers'), '3 620 suppliers');
  assert.equal(ui.formatSupplyCountBadge(209394, 'events'), '209 394 events');
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

test('Supply resolves exact GM labels and icons by chain plus address', () => {
  const presentation = ui.getSupplyMarketPresentation({
    id: DGM_BTC,
    symbol: 'dGM',
    name: 'Dolomite Isolation: GMX Market',
  }, 'arbitrum');
  assert.equal(presentation.symbol, 'gmBTC-USD');
  assert.match(presentation.icon, /WBTC-GM/);
});

test('Supply presents the active dsavETH wrapper as savETH with the official icon', () => {
  const presentation = ui.getSupplyMarketPresentation({
    id: DSAVETH,
    symbol: 'dsavETH',
    name: 'Dolomite Isolation: Staked avETH',
  }, 'arbitrum');
  assert.equal(presentation.symbol, 'savETH');
  assert.equal(presentation.icon, SAVETH_ICON);
});

test('Supply removes the obsolete savETH duplicate when active dsavETH exists', () => {
  const tokens = [
    { id: SAVETH_BASE, symbol: 'savETH', supplyLiquidityUSD: '0' },
    { id: DSAVETH, symbol: 'dsavETH', supplyLiquidityUSD: '1081512.89' },
  ];
  assert.deepEqual(
    ui.getSelectableSupplyMarkets(tokens, 'arbitrum').map(token => token.id),
    [DSAVETH],
  );
});

test('Supply removes matured dPT markets but preserves future and undated tokens', () => {
  const now = Date.UTC(2026, 6, 29);
  const tokens = [
    { id: '0x1', symbol: 'dPT-rsETH-26SEP2024' },
    { id: '0x2', symbol: 'dPT-rsETH-26SEP2027' },
    { id: '0x3', symbol: 'WETH' },
  ];
  assert.deepEqual(
    ui.getSelectableSupplyMarkets(tokens, 'arbitrum', now).map(token => token.id),
    ['0x2', '0x3'],
  );
});

test('Supply asset search matches resolved and raw market identities', () => {
  const token = {
    id: DGM_BTC,
    symbol: 'dGM',
    name: 'Dolomite Isolation: GMX Market',
  };
  assert.deepEqual(ui.filterSupplyMarketOptions([token], 'gmBTC-USD', 'arbitrum'), [token]);
  assert.deepEqual(ui.filterSupplyMarketOptions([token], 'dGM', 'arbitrum'), [token]);
});

test('Supply deep links require a supported chain and exact token address', () => {
  assert.deepEqual(
    ui.parseSupplyMarketDeepLink(`?chain=arbitrum&asset=${DGM_BTC}`),
    { chain: 'arbitrum', asset: DGM_BTC },
  );
  assert.equal(ui.parseSupplyMarketDeepLink('?chain=arbitrum&asset=dGM'), null);
  assert.equal(ui.parseSupplyMarketDeepLink('?chain=unknown&asset=0x1111111111111111111111111111111111111111'), null);
});
