const assert = require('node:assert/strict');
const test = require('node:test');

global.window = {};
require('../dolo-address-labels.js');
require('../odolo-address-meta.js');

const ENS_CLAIMERS = [
  ['0x7bfee91193d9df2ac0bfe90191d40f23c773c060', '7bfee.eth'],
  ['0x87db27ac8459ab6602f7a6155b48f6b184065da0', 'atheon.eth'],
];

test('keeps known EOA claimers and their provenance from the shared registry', () => {
  const labels = window.buildOdoloAddressMeta(window.cloneDoloAddressLabels());

  ENS_CLAIMERS.forEach(([address, label]) => {
    assert.equal(labels[address].label, label);
    assert.equal(labels[address].type, 'eoa');
    assert.equal(labels[address].sharedType, 'eoa');
    assert.equal(labels[address].badgeLabel, 'EOA');
    assert.equal(labels[address].source, 'ens-reverse');
    assert.equal(labels[address].confidence, 'confirmed');
    assert.equal(labels[address].tooltip, 'EOA · ENS reverse · Confirmed identity label');
  });
});

test('preserves shared metadata while adapting types to existing oDOLO styles', () => {
  const labels = window.buildOdoloAddressMeta({
    '0x1111111111111111111111111111111111111111': {
      label: 'Treasury Safe', type: 'multisig', source: 'dolomite-docs', confidence: 'confirmed',
    },
    '0x2222222222222222222222222222222222222222': {
      label: 'Potential CEX/MM', type: 'watch', source: 'heuristic-flow-pattern', confidence: 'potential',
    },
    '0x3333333333333333333333333333333333333333': {
      label: 'Pool', type: 'lp', source: 'public-pool-label', confidence: 'confirmed',
    },
    '0x4444444444444444444444444444444444444444': {
      label: 'Liquidator', type: 'liquidator', source: 'behavioral-label', confidence: 'confirmed',
    },
    '0x6666666666666666666666666666666666666666': {
      label: 'Fallback Contract', type: 'ca',
    },
  });

  assert.deepEqual(
    [labels['0x1111111111111111111111111111111111111111'].type, labels['0x1111111111111111111111111111111111111111'].badgeLabel],
    ['ca', 'Safe'],
  );
  assert.deepEqual(
    [labels['0x2222222222222222222222222222222222222222'].type, labels['0x2222222222222222222222222222222222222222'].badgeLabel],
    ['mm', 'Potential'],
  );
  assert.match(labels['0x2222222222222222222222222222222222222222'].tooltip, /Potential confidence$/);
  assert.deepEqual(
    [labels['0x3333333333333333333333333333333333333333'].type, labels['0x3333333333333333333333333333333333333333'].badgeLabel],
    ['ca', 'LP'],
  );
  assert.deepEqual(
    [labels['0x4444444444444444444444444444444444444444'].type, labels['0x4444444444444444444444444444444444444444'].badgeLabel],
    ['bot', 'Liquidator'],
  );
  assert.deepEqual(
    [labels['0x6666666666666666666666666666666666666666'].type, labels['0x6666666666666666666666666666666666666666'].badgeLabel],
    ['ca', 'Contract'],
  );
});

test('does not silently drop a future labeled type', () => {
  const address = '0x5555555555555555555555555555555555555555';
  const labels = window.buildOdoloAddressMeta({
    [address]: {label: 'Future known entity', type: 'future-type', source: 'manual-review', confidence: 'confirmed'},
  });

  assert.equal(labels[address].label, 'Future known entity');
  assert.equal(labels[address].type, 'eoa');
  assert.equal(labels[address].sharedType, 'future-type');
  assert.equal(labels[address].badgeLabel, 'Known');
  assert.equal(labels[address].tooltip, 'Known · Manual review · Confirmed');
});

test('normalizes vesting labels added after the initial shared registry clone', () => {
  const meta = window.normalizeOdoloAddressMeta({
    label: 'Strategic Investor',
    type: 'investor',
    source: 'official-claim-contract-transfer',
    confidence: 'confirmed',
  });

  assert.equal(meta.type, 'investor');
  assert.equal(meta.badgeLabel, 'Investor');
  assert.equal(meta.tooltip, 'Investor · Official claim contract · Confirmed');
});
