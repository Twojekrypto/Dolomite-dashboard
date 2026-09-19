const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const source = fs.readFileSync('revenue-preview.html', 'utf8');

test('cached Revenue excludes post-retirement activity but retains historical earnings and rebates', () => {
  const start = source.indexOf('function activeRevenueRow(');
  assert.ok(start >= 0);
  const code = source.slice(start, source.indexOf('function allRows(', start));
  const normalize = new Function('n', 'rowTs', code + '; return activeRevenueRow;')(
    x => Number(x) || 0, row => Date.parse(row.date + 'T00:00:00Z') / 1000);
  const active = {feesUSD: 200, revenueUSD: 90, grossRevenueUSD: 100, borrowFeeRebateUSD: 10, supplySideRevenueUSD: 100};
  const retired = {feesUSD: 10, revenueUSD: 5, grossRevenueUSD: 5, borrowFeeRebateUSD: 0, supplySideRevenueUSD: 5};
  const row = {...Object.fromEntries(Object.keys(active).map(k => [k, active[k] + retired[k]])),
    date: '2026-09-20', chains: {Ethereum: active, 'X Layer': retired}};
  const past = {...row, date: '2026-09-18'};
  assert.deepEqual(normalize(past), past);
  const current = normalize(row);
  assert.equal(current.revenueUSD, 90);
  assert.equal(current.borrowFeeRebateUSD, 10);
  assert.deepEqual(current.chains, {Ethereum: active});
  assert.deepEqual(normalize(current), current);
  assert.equal(row.revenueUSD, 95, 'original artifact remains unmodified');
});
