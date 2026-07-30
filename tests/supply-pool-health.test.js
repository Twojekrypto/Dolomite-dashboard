const test = require('node:test');
const assert = require('node:assert/strict');
const publishedSupplyHealth = require('../data/supply-health/latest.json');

const {
  buildSupplyMarketHref,
  clearSupplyHealthSearch,
  filterSupplyHealthMarkets,
  formatHealthConcentrationTip,
  formatHealthUsd,
  formatSupplyHealthDisclosureLabel,
  formatSupplyHealthPageRange,
  getSupplyHealthIconPresentation,
  getSupplyHealthMarketPresentation,
  healthConcentrationLevel,
  isExpiredSupplyHealthMarket,
  paginateSupplyHealthMarkets,
  renderSupplyHealthDetail,
  updateSupplyHealthFilters,
} = require('../tvl/supply-health.js');

const markets = [
  {
    chain: 'ethereum',
    symbol: 'USD1',
    name: 'World Liberty Financial USD',
    tokenId: '0x1111',
    supplyUsd: 300,
  },
  {
    chain: 'arbitrum',
    symbol: 'GM-ETH',
    name: 'GMX ETH Market',
    tokenId: '0x2222',
    supplyUsd: 200,
  },
  {
    chain: 'berachain',
    symbol: 'HONEY',
    name: 'Honey',
    tokenId: '0x3333',
    supplyUsd: 100,
  },
];

test('Supply Health builds an exact chain and address market link', () => {
  assert.equal(
    buildSupplyMarketHref({
      chain: 'arbitrum',
      tokenId: '0x1E8E8B7A2F827B3BC12B00EE402145061B7050EF',
    }),
    './supply/?chain=arbitrum&asset=0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef',
  );
});

test('Supply Health disclosure label reflects the expanded state', () => {
  assert.equal(formatSupplyHealthDisclosureLabel('USD1', false), 'Show USD1 details');
  assert.equal(formatSupplyHealthDisclosureLabel('USD1', true), 'Hide USD1 details');
});

test('asset search matches symbol, name, token id, and chain', () => {
  assert.deepEqual(
    filterSupplyHealthMarkets(markets, { query: 'usd1', chains: new Set() })
      .map(market => market.symbol),
    ['USD1'],
  );
  assert.deepEqual(
    filterSupplyHealthMarkets(markets, { query: 'gmx eth', chains: new Set() })
      .map(market => market.symbol),
    ['GM-ETH'],
  );
  assert.deepEqual(
    filterSupplyHealthMarkets(markets, { query: '0x3333', chains: new Set() })
      .map(market => market.symbol),
    ['HONEY'],
  );
  assert.deepEqual(
    filterSupplyHealthMarkets(markets, { query: 'ethereum', chains: new Set() })
      .map(market => market.symbol),
    ['USD1'],
  );
});

test('network filter is independent and combines with asset search', () => {
  assert.deepEqual(
    filterSupplyHealthMarkets(markets, {
      query: '',
      chains: new Set(['arbitrum', 'berachain']),
    }).map(market => market.symbol),
    ['GM-ETH', 'HONEY'],
  );
  assert.deepEqual(
    filterSupplyHealthMarkets(markets, {
      query: 'honey',
      chains: new Set(['arbitrum']),
    }),
    [],
  );
});

test('known dGM wrappers resolve to their specific GM market labels', () => {
  assert.deepEqual(
    getSupplyHealthMarketPresentation({
      chain: 'arbitrum',
      tokenId: '0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef',
      symbol: 'dGM',
      name: 'Dolomite Isolation: GMX Market',
    }),
    {
      symbol: 'gmBTC-USD',
      name: 'Dolomite GM Market',
    },
  );
});

test('Supply Health icon presentation uses canonical market identity', () => {
  const calls = [];
  const market = {
    chain: 'arbitrum',
    tokenId: '0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef',
    symbol: 'dGM',
  };
  const presentation = getSupplyHealthIconPresentation(market, {
    icon: (symbol, row) => {
      calls.push([symbol, row]);
      return 'official-gm-icon.svg';
    },
    frame: symbol => symbol.startsWith('gm') ? 'full-logo' : '',
    image: () => '',
  });

  assert.deepEqual(calls, [[
    'gmBTC-USD',
    { chain: 'arbitrum', addr: market.tokenId },
  ]]);
  assert.deepEqual(presentation, {
    src: 'official-gm-icon.svg',
    frameClass: 'full-logo',
    imageClass: '',
  });
});

test('Supply Health renders one market dossier with evidence and exact navigation', () => {
  const detail = renderSupplyHealthDetail({
    chain: 'ethereum',
    tokenId: '0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d',
    symbol: 'USD1',
    name: 'World Liberty Financial USD',
    supplyUsd: 278049648.05,
    medianWalletUsd: 159.27,
    gini: 0.9841,
    largestPct: 40.02,
    score: {
      wallet: 96.5,
      concentration: 15,
      stability: 99.5,
      growth: 67.1,
      resilience: 60,
      total: 64.6,
      grade: 'C',
    },
    growth: {
      supply30dPct: 16.68,
      supply7dPct: -1.13,
      wallets30dPct: -8.96,
      avgDailyChange30dPct: 1.05,
    },
    topWallets: [{
      address: '0x5be9a4959308a0d0c7bc0870e319314d8d957dbb',
      sharePct: 40.02,
      usd: 111278882.78,
    }],
  });

  assert.doesNotMatch(detail, /Market intelligence/);
  assert.doesNotMatch(detail, /supply-health-detail-head/);
  assert.doesNotMatch(detail, /supply-health-detail-head-metric/);
  assert.match(detail, /Quality anatomy/);
  assert.match(detail, /Market momentum/);
  assert.match(detail, /Supply concentration/);
  assert.match(detail, /supply-health-supplier-bar/);
  assert.match(detail, /Open Supply market/);
  assert.match(detail, /asset=0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d/);
});

test('unknown market presentation falls back to raw subgraph metadata', () => {
  assert.deepEqual(
    getSupplyHealthMarketPresentation({
      chain: 'arbitrum',
      tokenId: '0x9999',
      symbol: 'dGM',
      name: 'Dolomite Isolation: GMX Market',
    }),
    {
      symbol: 'dGM',
      name: 'Dolomite Isolation: GMX Market',
    },
  );
});

test('asset search matches resolved GM labels and the original dGM symbol', () => {
  const dgmMarket = {
    chain: 'arbitrum',
    tokenId: '0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef',
    symbol: 'dGM',
    name: 'Dolomite Isolation: GMX Market',
    supplyUsd: 2801914.88,
  };

  assert.deepEqual(
    filterSupplyHealthMarkets([dgmMarket], {
      query: 'gmBTC-USD',
      chains: new Set(),
    }),
    [dgmMarket],
  );
  assert.deepEqual(
    filterSupplyHealthMarkets([dgmMarket], {
      query: 'dGM',
      chains: new Set(),
    }),
    [dgmMarket],
  );
});

test('published dGM wrappers have unique specific display symbols', () => {
  const dgmMarkets = publishedSupplyHealth.markets.filter(
    market => String(market.symbol).toLowerCase() === 'dgm',
  );
  const displaySymbols = dgmMarkets.map(
    market => getSupplyHealthMarketPresentation(market).symbol,
  );

  assert.ok(dgmMarkets.length > 0);
  assert.equal(displaySymbols.includes('dGM'), false);
  assert.equal(new Set(displaySymbols).size, dgmMarkets.length);
});

test('expired dated dPT markets are hidden without removing active or undated markets', () => {
  const now = Date.UTC(2026, 6, 28);

  assert.equal(
    isExpiredSupplyHealthMarket({ symbol: 'dPT-rsETH-26SEP2024' }, now),
    true,
  );
  assert.equal(
    isExpiredSupplyHealthMarket({ symbol: 'dPT-weETH-27JUN2024' }, now),
    true,
  );
  assert.equal(
    isExpiredSupplyHealthMarket({ symbol: 'dPT-rsETH-26SEP2027' }, now),
    false,
  );
  assert.equal(
    isExpiredSupplyHealthMarket({ symbol: 'PT-rsETH-26SEP2024' }, now),
    false,
  );
  assert.equal(
    isExpiredSupplyHealthMarket({ symbol: 'dPT-rsETH' }, now),
    false,
  );
  assert.equal(
    isExpiredSupplyHealthMarket({ symbol: 'dsavETH' }, now),
    false,
  );
});

test('pagination always reports enough spacer rows for a ten-row viewport', () => {
  const page = paginateSupplyHealthMarkets(markets, 1, 10);

  assert.deepEqual(page.rows.map(market => market.symbol), ['USD1', 'GM-ETH', 'HONEY']);
  assert.equal(page.spacerCount, 7);
  assert.equal(page.totalPages, 1);
  assert.equal(page.page, 1);
});

test('pagination clamps an out-of-range page after filtering', () => {
  const twelveMarkets = Array.from({ length: 12 }, (_, index) => ({
    symbol: `ASSET-${index + 1}`,
  }));
  const page = paginateSupplyHealthMarkets(twelveMarkets, 9, 10);

  assert.equal(page.page, 2);
  assert.equal(page.totalPages, 2);
  assert.deepEqual(page.rows.map(market => market.symbol), ['ASSET-11', 'ASSET-12']);
  assert.equal(page.spacerCount, 8);
});

test('pagination range matches the DOLO Holders footer hierarchy', () => {
  assert.equal(formatSupplyHealthPageRange?.(14192, 1, 10), '1–10 of 14 192');
  assert.equal(formatSupplyHealthPageRange?.(14192, 3, 10), '21–30 of 14 192');
  assert.equal(formatSupplyHealthPageRange?.(0, 1, 10), '0–0 of 0');
});

test('missing USD values render as unavailable instead of zero', () => {
  assert.equal(formatHealthUsd(null), '—');
  assert.equal(formatHealthUsd(undefined), '—');
});

test('concentration levels preserve the Supply Health methodology breakpoints', () => {
  assert.equal(healthConcentrationLevel('top10', 40), 'low');
  assert.equal(healthConcentrationLevel('top10', 40.01), 'moderate');
  assert.equal(healthConcentrationLevel('top10', 60), 'moderate');
  assert.equal(healthConcentrationLevel('top10', 60.01), 'high');

  assert.equal(healthConcentrationLevel('largest', 20), 'low');
  assert.equal(healthConcentrationLevel('largest', 20.01), 'moderate');
  assert.equal(healthConcentrationLevel('largest', 40), 'moderate');
  assert.equal(healthConcentrationLevel('largest', 40.01), 'high');

  assert.equal(healthConcentrationLevel('top10', null), '');
  assert.equal(healthConcentrationLevel('largest', undefined), '');
  assert.equal(healthConcentrationLevel('top10', ''), '');
  assert.equal(healthConcentrationLevel('largest', 'not-a-number'), '');
  assert.equal(healthConcentrationLevel('unknown', 10), '');
});

test('concentration tooltip keeps the risk level without changing the number color', () => {
  assert.equal(
    formatHealthConcentrationTip('largest', 40.01),
    'Largest supplier concentration: 40.0% · High',
  );
  assert.equal(
    formatHealthConcentrationTip('top10', 55),
    'Top 10 suppliers concentration: 55.0% · Moderate',
  );
  assert.equal(formatHealthConcentrationTip('largest', null), '');
});

test('changing a discovery filter resets pagination and expanded details', () => {
  const state = {
    query: '',
    chains: new Set(),
    page: 4,
    expandedKey: 'ethereum:0x1111',
  };

  updateSupplyHealthFilters(state, { query: 'USD1' });

  assert.equal(state.query, 'USD1');
  assert.equal(state.page, 1);
  assert.equal(state.expandedKey, '');
});

test('search clear uses the native input path and preserves focus', () => {
  const events = [];
  const input = {
    value: 'USD1',
    dispatchEvent(event) {
      events.push(event.type);
    },
    focus() {
      events.push('focus');
    },
  };

  clearSupplyHealthSearch(input);

  assert.equal(input.value, '');
  assert.deepEqual(events, ['input', 'focus']);
});
