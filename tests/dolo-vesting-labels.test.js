const assert = require('node:assert/strict');
const test = require('node:test');

global.window = {};
require('../dolo-address-labels.js');

const EARLY = '0x1111111111111111111111111111111111111111';
const INVESTOR = '0x2222222222222222222222222222222222222222';
const OVERLAP = '0x3333333333333333333333333333333333333333';
const EXISTING = '0x4444444444444444444444444444444444444444';

test('merges verified investor provenance with Early Investor priority', () => {
  const labels = {
    [EXISTING]: { label: 'Known Exchange', type: 'cex', confidence: 'confirmed' },
  };
  const result = window.mergeDoloVestingLabels(labels, {
    early_investors: [EARLY, OVERLAP, EXISTING],
    investors: [INVESTOR, OVERLAP],
    team: [INVESTOR, OVERLAP], // legacy broken payload: exact investor duplicate
  });

  assert.equal(result.changed, true);
  assert.equal(labels[EARLY].label, 'Early Investor');
  assert.equal(labels[INVESTOR].label, 'Investor');
  assert.equal(labels[OVERLAP].label, 'Early Investor');
  assert.equal(labels[OVERLAP].alsoReceivedLongTermTranche, true);
  assert.match(labels[OVERLAP].description, /long-term investor tranche/i);
  assert.equal(labels[EXISTING].label, 'Known Exchange');
  assert.equal(Object.values(labels).some(info => info.label === 'Core Team'), false);
});

test('uses structured wallet records and ignores malformed addresses', () => {
  const labels = {};
  window.mergeDoloVestingLabels(labels, {
    wallets: [
      { address: INVESTOR, label: 'Investor', claimSources: ['investor_claims'] },
      { address: 'not-an-address', label: 'Early Investor' },
    ],
    early_investors: [],
    investors: [],
    team: [],
  });

  assert.equal(labels[INVESTOR].label, 'Investor');
  assert.equal(Object.keys(labels).length, 1);
});
