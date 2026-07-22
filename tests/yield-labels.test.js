const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const coreSource = fs.readFileSync('dashboard-core.js', 'utf8');
const assetsSource = fs.readFileSync('assets-preview.html', 'utf8');
const earnBundle = fs.readFileSync('earn/earn-core.js', 'utf8');

test('long yield labels use compact names in Assets and EARN', () => {
  assert.match(assetsSource, /function compactYieldSourceLabel\(label\)[\s\S]*?"ETH Staking": "Staking"/);
  assert.match(coreSource, /displayLabel === 'ETH Staking' \|\| displayLabel === 'ETH Staking Yield' \|\| displayLabel === 'Staking Yield'/);
  assert.match(coreSource, /'ETH Staking': 'Staking'/);
  assert.match(earnBundle, /'ETH Staking': 'Staking'/);
});
