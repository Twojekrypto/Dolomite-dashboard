const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const coreSource = fs.readFileSync('dashboard-core.js', 'utf8');
const assetsSource = fs.readFileSync('assets-preview.html', 'utf8');
const earnBundle = fs.readFileSync('earn/earn-core.js', 'utf8');

test('Sky Savings Rate uses the compact Sky Rate label in Assets and EARN', () => {
  assert.match(assetsSource, /function compactYieldSourceLabel\(label\)[\s\S]*?Sky Savings Rate[\s\S]*?Sky Rate/);
  assert.match(coreSource, /displayLabel === 'Sky Savings Rate'\) displayLabel = 'Sky Rate';/);
  assert.match(coreSource, /const cleaned =[\s\S]*?cleaned === 'Sky Savings Rate' \? 'Sky Rate' : cleaned;/);
  assert.match(earnBundle, /cleaned === 'Sky Savings Rate' \? 'Sky Rate' : cleaned;/);
});
