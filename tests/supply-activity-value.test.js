const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const fixture = require('./fixtures/wbera-repay-value.json');
const modules = ['../supply-activity-semantics.js', '../local-previews/dolo-graphite-interface/supply-semantics.js']
  .filter(name => fs.existsSync(path.resolve(__dirname, name)));

for (const modulePath of modules) {
  const api = require(modulePath);
  test(modulePath + ': recover historical USD for a one-unit repayment rounding difference', () => {
    const original = JSON.stringify(fixture);
    const row = api.unpack(fixture);
    assert.equal(row.usd, '8170.524939464356447186');
    assert.equal(row.semantics.usdStatus, 'verified');
    assert.equal(row.amount, '92158.097260185595240867');
    assert.deepEqual(api.actions(row), ['repay']);
    assert.equal(JSON.stringify(fixture), original, 'cached source evidence stays immutable');
    assert.deepEqual(api.unpack(api.pack(row)), row);
  });
  test(modulePath + ': do not fabricate quotes for unverifiable or mismatched evidence', () => {
    const cases = [
      meta => { meta.legs[0].amount = '92158.097260185595240865'; },
      meta => { meta.legs[0].decimals = null; },
      meta => { meta.legs[0].usd = null; },
      meta => { meta.legs[0].usd = '0'; },
      meta => { meta.legs[0].ambiguous = true; },
      meta => { meta.status = 'unavailable'; },
      meta => { meta.quantityStatus = 'unavailable'; },
      meta => { meta.legs.push({...meta.legs[0]}); },
      meta => { meta.sourceIds.push('another-event'); },
      meta => { meta.legs[0].wallet = 'another-wallet'; },
      meta => { meta.legs = null; },
      meta => { meta.legs = [null]; },
    ];
    for (const change of cases) {
      const packed = structuredClone(fixture); change(packed[8]);
      const row = api.unpack(packed);
      assert.equal(row.semantics.usdStatus, 'unavailable');
      assert.equal(row.usd, '0');
    }
  });
  test(modulePath + ': preserve existing exact quotes and legacy activity', () => {
    const known = structuredClone(fixture); known[5]='8170.524939464356447186'; known[8].usdStatus='verified';
    assert.equal(api.unpack(known).usd, known[5]);
    assert.equal(api.unpack(fixture.slice(0,8)).semantics, undefined);
  });
}
