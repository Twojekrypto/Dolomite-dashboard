const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const coreSource = fs.readFileSync('dashboard-core.js', 'utf8');
const assetsSource = fs.readFileSync('assets-preview.html', 'utf8');
const earnBundle = fs.readFileSync('earn/earn-core.js', 'utf8');
const assetsData = JSON.parse(fs.readFileSync('assets_live.json', 'utf8'));

test('long yield labels use compact names in Assets and EARN', () => {
  assert.match(assetsSource, /function compactYieldSourceLabel\(label\)[\s\S]*?"ETH Staking": "Staking"/);
  assert.match(assetsSource, /"ETH Restaking": "Restaking"/);
  assert.match(assetsSource, /"GLV Performance": "GLV Perf\."/);
  assert.match(assetsSource, /"GM Performance": "GM Perf\."/);
  assert.match(assetsSource, /"Net Lending Rewards": "Lending"/);
  assert.match(coreSource, /displayLabel === 'ETH Staking' \|\| displayLabel === 'ETH Staking Yield' \|\| displayLabel === 'Staking Yield'/);
  assert.match(coreSource, /'ETH Restaking': 'Restaking'/);
  assert.match(coreSource, /'Net Lending Rewards': 'Lending'/);
  assert.match(coreSource, /'ETH Staking': 'Staking'/);
  assert.match(earnBundle, /'GM Performance': 'GM Perf\.'/);
  assert.match(earnBundle, /'ETH Staking': 'Staking'/);
});

test('Supply breakdown uses the same canonical labels in Assets and EARN', () => {
  assert.match(assetsSource, /parts\.push\(\{k:"lending", l:"Lending", v:r\.lending\}\)/);
  assert.match(assetsSource, /parts\.push\(\{k:"odolo",\s+l:"oDOLO",\s+v:r\.odolo\}\)/);
  assert.match(assetsSource, /parts\.push\(\{k:"yield",\s+l:"Yield",\s+v:r\.yield\}\)/);

  const earnRendererStart = coreSource.indexOf('// Build Supply APR cell (same breakdown as assets tab)');
  const earnRenderer = coreSource.slice(
    earnRendererStart,
    coreSource.indexOf('const supplyLinesHtml', earnRendererStart),
  );
  assert.match(earnRenderer, /label: 'Lending'/);
  assert.match(earnRenderer, /label: 'oDOLO'/);
  assert.match(earnRenderer, /label: 'Yield', rate: rateData\.extYieldApr/);
  assert.doesNotMatch(earnRenderer, /rewardSymbol|Rewards`/);
});

test('external Yield aggregate equals its source sum for every published market', () => {
  const mismatches = assetsData.rows
    .filter((row) => Array.isArray(row.yieldSources) && row.yieldSources.length > 0)
    .map((row) => ({
      chain: row.chain,
      marketId: row.marketId,
      symbol: row.sym,
      delta: Math.abs(
        Number(row.yield || 0)
        - row.yieldSources.reduce((sum, source) => sum + Number(source.rate || 0), 0),
      ),
    }))
    .filter((row) => row.delta > 1e-6);

  assert.deepEqual(mismatches, []);
});

test('EARN keeps negative external yield sources instead of dropping the market breakdown', () => {
  assert.match(coreSource, /if \(yr !== 0\) yieldSources\.push/);
  assert.match(coreSource, /rateData && Number\.isFinite\(Number\(rateData\.apy\)\)/);
  assert.match(coreSource, /\(rateData\.extYieldApr \|\| 0\) !== 0/);
  assert.match(earnBundle, /if \(yr !== 0\) yieldSources\.push/);
});
