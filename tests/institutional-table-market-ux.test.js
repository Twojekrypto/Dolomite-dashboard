const test = require('node:test');
const assert = require('node:assert/strict');

let liquidationHistorySort = {};
try {
  liquidationHistorySort = require('../liquidation/liquidation-history-sort.js');
} catch (error) {
  liquidationHistorySort = {};
}

const supplyUi = require('../supply/supply-draft.js');

const rows = [
  {
    chain: 'berachain',
    liquidatedAddress: '0xbbbb',
    timestamp: 300,
    collateralSeizedUSD: 50,
    debtRepaidUSD: 20,
    txHash: '0x03',
  },
  {
    chain: 'arbitrum',
    liquidatedAddress: '0xCCCC',
    timestamp: 100,
    collateralSeizedUSD: 75,
    debtRepaidUSD: 10,
    txHash: '0x01',
  },
  {
    chain: 'arbitrum',
    liquidatedAddress: '0xaaaa',
    timestamp: 200,
    collateralSeizedUSD: 75,
    debtRepaidUSD: 30,
    txHash: '0x02',
  },
];

test('Liquidation History sorts text, date, and USD fields deterministically', () => {
  assert.equal(
    typeof liquidationHistorySort.sortLiquidationHistoryRows,
    'function',
    'Liquidation History needs an executable row sorter',
  );

  const sort = liquidationHistorySort.sortLiquidationHistoryRows;
  assert.deepEqual(
    sort(rows, 'chain', 'asc').map(row => row.txHash),
    ['0x02', '0x01', '0x03'],
  );
  assert.deepEqual(
    sort(rows, 'address', 'asc').map(row => row.txHash),
    ['0x02', '0x03', '0x01'],
  );
  assert.deepEqual(
    sort(rows, 'date', 'desc').map(row => row.txHash),
    ['0x03', '0x02', '0x01'],
  );
  assert.deepEqual(
    sort(rows, 'collateral', 'desc').map(row => row.txHash),
    ['0x02', '0x01', '0x03'],
  );
  assert.deepEqual(
    sort(rows, 'debt', 'asc').map(row => row.txHash),
    ['0x01', '0x03', '0x02'],
  );
});

test('Supply market icon presentation uses official address-first artwork', () => {
  assert.equal(
    typeof supplyUi.getSupplyMarketIconPresentation,
    'function',
    'Supply views need one canonical icon presentation helper',
  );

  const dplv = supplyUi.getSupplyMarketIconPresentation({
    id: '0x5c80ac681b6b0e7ef6e0751211012601e6cfb043',
    symbol: 'dplvGLP',
  }, 'arbitrum');
  const solv = supplyUi.getSupplyMarketIconPresentation({
    id: '0xcc0966d8418d412c599a6421b760a847eb169a8c',
    symbol: 'SolvBTC.BBN',
  }, 'berachain');

  assert.match(dplv.src, /plvGLP\.24551c9e68ef10245cc45fb0b96cfdff\.svg$/);
  assert.equal(dplv.frameClass, 'full-logo');
  assert.match(solv.src, /solvBTC\.326d594ebd54e4317f078b70f72a58b4\.svg$/);
  assert.equal(solv.frameClass, '');
});
