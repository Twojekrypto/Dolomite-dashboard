const test = require('node:test');
const assert = require('node:assert/strict');

const {
  clearSupplyHealthSearch,
  filterSupplyHealthMarkets,
  formatHealthUsd,
  formatSupplyHealthPageRange,
  healthConcentrationClass,
  paginateSupplyHealthMarkets,
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

test('concentration classes follow the existing Supply Health methodology breakpoints', () => {
  assert.equal(healthConcentrationClass('top10', 40), 'health-concentration-low');
  assert.equal(healthConcentrationClass('top10', 40.01), 'health-concentration-moderate');
  assert.equal(healthConcentrationClass('top10', 60), 'health-concentration-moderate');
  assert.equal(healthConcentrationClass('top10', 60.01), 'health-concentration-high');

  assert.equal(healthConcentrationClass('largest', 20), 'health-concentration-low');
  assert.equal(healthConcentrationClass('largest', 20.01), 'health-concentration-moderate');
  assert.equal(healthConcentrationClass('largest', 40), 'health-concentration-moderate');
  assert.equal(healthConcentrationClass('largest', 40.01), 'health-concentration-high');

  assert.equal(healthConcentrationClass('top10', null), '');
  assert.equal(healthConcentrationClass('largest', undefined), '');
  assert.equal(healthConcentrationClass('top10', ''), '');
  assert.equal(healthConcentrationClass('largest', 'not-a-number'), '');
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
