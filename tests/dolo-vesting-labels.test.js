const assert = require('node:assert/strict');
const test = require('node:test');

global.window = {};
require('../dolo-address-labels.js');

const EARLY = '0x1111111111111111111111111111111111111111';
const INVESTOR = '0x2222222222222222222222222222222222222222';
const OVERLAP = '0x3333333333333333333333333333333333333333';
const EXISTING = '0x4444444444444444444444444444444444444444';

test('merges verified investor provenance with Strategic Investor priority', () => {
  const labels = {
    [EXISTING]: { label: 'Known Exchange', type: 'cex', confidence: 'confirmed' },
  };
  const result = window.mergeDoloVestingLabels(labels, {
    early_investors: [EARLY, OVERLAP, EXISTING],
    investors: [INVESTOR, OVERLAP],
    team: [INVESTOR, OVERLAP], // legacy broken payload: exact investor duplicate
  });

  assert.equal(result.changed, true);
  assert.equal(labels[EARLY].label, 'Strategic Investor');
  assert.equal(labels[EARLY].labelDetail, '2024 strategic round · $900K');
  assert.equal(labels[EARLY].attributionStatus, 'high-confidence-onchain-attribution');
  assert.equal(labels[INVESTOR].label, 'Long-term Investor');
  assert.equal(labels[INVESTOR].labelDetail, '3-year vesting · 1-year cliff');
  assert.equal(labels[OVERLAP].label, 'Strategic Investor');
  assert.equal(labels[OVERLAP].labelDetail, '2024 strategic round · $900K');
  assert.equal(labels[OVERLAP].alsoReceivedLongTermTranche, true);
  assert.match(labels[OVERLAP].description, /long-term investor tranche/i);
  assert.equal(labels[EXISTING].label, 'Known Exchange');
  assert.equal(Object.values(labels).some(info => info.label === 'Core Team'), false);
});

test('normalizes structured legacy wallet labels and ignores malformed addresses', () => {
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

  assert.equal(labels[INVESTOR].label, 'Long-term Investor');
  assert.equal(labels[INVESTOR].labelDetail, '3-year vesting · 1-year cliff');
  assert.equal(Object.keys(labels).length, 1);
});

test('prefers strategic_investors while retaining early_investors compatibility', () => {
  const labels = {};
  window.mergeDoloVestingLabels(labels, {
    strategic_investors: [EARLY],
    early_investors: [EXISTING],
    investors: [],
    team: [],
  });

  assert.equal(labels[EARLY].label, 'Strategic Investor');
  assert.equal(labels[EXISTING], undefined);
});
