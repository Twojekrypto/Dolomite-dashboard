const assert = require('node:assert/strict');
const test = require('node:test');
const { filterPrograms, programSearchText } = require('../rewards-search.js');

const programs = [
  {
    status: 'LIVE', provider: 'Dolomite', name: 'Lend USD1', action: 'LEND',
    chain: 'berachain', marketTokens: ['USD1'], rewardTokens: ['oDOLO'],
  },
  {
    status: 'LIVE', provider: 'Merkl', name: 'Supply WETH', action: 'LEND',
    chain: 'arbitrum', marketTokens: ['WETH'], rewardTokens: ['ARB'],
  },
  {
    status: 'ENDED', provider: 'Merkl', name: 'Supply USDC', action: 'LEND',
    chain: 'ethereum', marketTokens: ['USDC'], rewardTokens: ['DOLO'],
  },
];
const chains = { berachain: 'Berachain', arbitrum: 'Arbitrum', ethereum: 'Ethereum' };

test('search text covers campaign, token, reward, provider, and network', () => {
  const text = programSearchText(programs[0], chains);
  for (const term of ['lend usd1', 'supply usd1', 'dolomite', 'usd1', 'odolo', 'berachain']) {
    assert.match(text, new RegExp(term, 'i'));
  }
});

test('filters the selected table status without mutating the source', () => {
  assert.deepEqual(filterPrograms(programs, 'LIVE', 'arb', chains), [programs[1]]);
  assert.deepEqual(filterPrograms(programs, 'LIVE', 'odolo', chains), [programs[0]]);
  assert.deepEqual(filterPrograms(programs, 'ENDED', 'ethereum', chains), [programs[2]]);
  assert.equal(programs.length, 3);
});

test('normalizes whitespace and returns every row for an empty query', () => {
  assert.deepEqual(filterPrograms(programs, 'LIVE', '  supply   weth  ', chains), [programs[1]]);
  assert.deepEqual(filterPrograms(programs, 'LIVE', '   ', chains), programs.slice(0, 2));
});
