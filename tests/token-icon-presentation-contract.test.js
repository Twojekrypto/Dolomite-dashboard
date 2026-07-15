const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');

test('assets, portfolio, and token composition preserve full composite token logos', () => {
  for (const file of ['assets-preview.html', 'portfolio-preview.html', 'tvl-preview.html']) {
    const source = read(file);
    assert(source.includes('FULL_LOGO_SYMBOL_PATTERN'), `${file} should classify full token logos`);
    assert(source.includes('tokenIconFrameClass'), `${file} should add the full-logo frame class`);
    assert(source.includes('full-logo'), `${file} should style full token logos`);
  }
});

test('earn and liquidation tables preserve full composite token logos', () => {
  const earnJs = read('dashboard-core.js');
  const earnCss = read('dashboard-core.css');
  const liquidation = read('liquidation-preview.html');

  assert(earnJs.includes('earnTokenIconFrameClass'), 'Earn should classify full token logos');
  assert(earnCss.includes('.assets-token-icon.full-logo'), 'Earn asset rows should not crop full logos into circles');
  assert(earnCss.includes('.earn-token-pill-icon.full-logo'), 'Earn position pills should not crop full logos into circles');
  assert(liquidation.includes('tokenIconFrameClass'), 'Liquidation views should classify full token logos');
  assert(liquidation.includes('.token-pill-icon.full-logo'), 'Lending, liquidation, and supply pills should not crop full logos into circles');
  assert(liquidation.includes("'gmARB-USD'"), 'Liquidation views should resolve GM market logos');
  assert(liquidation.includes("'PT-wstETH'"), 'Liquidation views should resolve short PT logos');
});
