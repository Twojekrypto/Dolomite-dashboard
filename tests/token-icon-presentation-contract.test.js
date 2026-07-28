const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');
const OFFICIAL_SAVETH_ICON = 'https://app.dolomite.io/static/media/savETH.1c28535854c4a65f2a4786a2f02ae499.svg';
const OFFICIAL_DARB_ICON = 'https://app.dolomite.io/static/media/oARB.a2c6c20bd4a19274b88e208e43f7ffa3.svg';

function loadTokenIconResolver(file) {
  const source = read(file);
  const start = source.indexOf('const DOLO_CDN');
  const end = source.indexOf('const GRAYSCALE_SYMBOLS', start);
  assert.notEqual(start, -1, `${file} should define its Dolomite icon map`);
  assert.notEqual(end, -1, `${file} should define its token icon resolver`);
  return vm.runInNewContext(`(() => {
    ${source.slice(start, end)}
    return tokenIcon;
  })()`);
}

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

test('assets and Supply Pool Health resolve the official savETH icon', () => {
  const assetsTokenIcon = loadTokenIconResolver('assets-preview.html');
  const tvlTokenIcon = loadTokenIconResolver('tvl-preview.html');

  assert.equal(assetsTokenIcon('savETH'), OFFICIAL_SAVETH_ICON);
  assert.equal(tvlTokenIcon('savETH'), OFFICIAL_SAVETH_ICON);
  assert.equal(tvlTokenIcon('dsavETH'), OFFICIAL_SAVETH_ICON);
});

test('Supply Pool Health resolves dARB to the official Dolomite ARB market icon', () => {
  const tvlTokenIcon = loadTokenIconResolver('tvl-preview.html');

  assert.equal(tvlTokenIcon('dARB'), OFFICIAL_DARB_ICON);
});
