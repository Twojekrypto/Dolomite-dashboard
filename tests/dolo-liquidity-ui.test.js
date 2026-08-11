const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const html = fs.readFileSync(path.resolve(__dirname, '../dolo-preview.html'), 'utf8');
const rootRoute = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
const doloRoute = fs.readFileSync(path.resolve(__dirname, '../dolo/index.html'), 'utf8');

test('liquidity card is one shell between flows and fresh wallets', () => {
  const flows = html.indexOf('<h2>DOLO Flows</h2>');
  const liquidity = html.indexOf('<h2>DOLO Liquidity Providers</h2>');
  const fresh = html.indexOf('<h2>Fresh 10K+ DOLO Wallets</h2>');
  assert.ok(flows >= 0 && liquidity > flows && fresh > liquidity);
  for (const id of [
    'dolo-lp-count', 'dolo-lp-meta', 'dolo-lp-summary', 'dolo-lp-search',
    'dolo-lp-chain', 'dolo-lp-pairs', 'dolo-lp-dexes', 'dolo-lp-low-liquidity',
    'dolo-lp-history-period', 'dolo-lp-history-action', 'dolo-lp-head',
    'dolo-lp-body', 'dolo-lp-info', 'dolo-lp-pager',
  ]) assert.match(html, new RegExp(`id=["']${id}["']`));
  assert.match(html, /Active positions/);
  assert.match(html, />History</);
  assert.match(html, /data-address-match-cells/);
  assert.match(html, /shared-hover-tooltips\.js/);
});

test('liquidity UI keeps exact active/history schemas and stable ten-row pagination', () => {
  for (const label of ['Chain', 'Pair', 'Wallet', 'Price Range', 'DOLO', 'Paired Asset', 'Value', 'Status', 'Details']) {
    assert.match(html, new RegExp(label.replace(' ', '\\s*')));
  }
  for (const label of ['Date', 'Action']) assert.match(html, new RegExp(label));
  assert.match(html, /pageSize:\s*10/);
  assert.match(html, /Array\.from\(\{length:\s*Math\.max\(0,\s*doloLpState\.pageSize/);
  assert.match(html, /data-dolo-lp-details/);
});

test('mode, filters, null pricing and action groups are explicit behavior contracts', () => {
  assert.match(html, /mode:\s*["']active["']/);
  assert.match(html, /historyPeriod:\s*["']30d["']/);
  assert.match(html, /activeSort:\s*\{key:["']valueUsd["'],\s*dir:["']desc["']/);
  assert.match(html, /historySort:\s*\{key:["']timestamp["'],\s*dir:["']desc["']/);
  assert.match(html, /row\.liquidityUsd\s*===\s*null/);
  assert.match(html, /Added.*Increased/);
  assert.match(html, /Removed.*Closed/);
  assert.match(html, /data\/dolo-liquidity\.json\?v=20260811-dolo-liquidity-v2/);
  assert.match(html, /Data unavailable — try again later/);
});

test('wallet and provenance UX is scoped and does not confuse poolId with a contract', () => {
  assert.match(html, /addr-tooltip-wrap[^>]+data-full-addr/);
  assert.match(html, /debank\.com\/profile/);
  assert.match(html, /poolIdentifierType\s*===\s*["']contract["']/);
  assert.match(html, /Custodied \/ unresolved/);
  assert.match(html, /dolo-lp-details-grid/);
  assert.match(
    html,
    /byId\("dolo-lp-count"\)\.textContent[^;]+rows\.map\(row\s*=>\s*normalized\(row\.beneficialOwner\)\)/,
  );
});

test('both DOLO route entry points advance the liquidity feature cache once', () => {
  const version = 'dolo-liquidity-live-20260811';
  for (const [name, source] of [['index.html', rootRoute], ['dolo/index.html', doloRoute]]) {
    assert.equal(source.split(version).length - 1, 1, name);
  }
  assert.match(html, /data\/dolo-liquidity\.json\?v=20260811-dolo-liquidity-v2/);
});

test('Details remains usable in Safari and meets the mobile touch target', () => {
  assert.match(html, /\.dolo-lp-details-btn\{[^}]*-webkit-appearance:none[^}]*appearance:none[^}]*overflow:visible/s);
  assert.match(html, /@media \(max-width:640px\)\{[\s\S]*?\.dolo-lp-details-btn\{[^}]*min-height:44px/s);
});

test('the stable-height shell still shows one readable empty-state message', () => {
  assert.match(html, /\.tbl tbody tr\.tbl-spacer-row\.dolo-lp-empty:first-child td\{[^}]*color:var\(--fg-4\)/);
  assert.match(html, /No verified positions match these filters\./);
});

test('history-only controls reserve their footprint to prevent mode layout shift', () => {
  assert.match(html, /\.dolo-lp-history-controls\[hidden\]\{[^}]*visibility:hidden[^}]*pointer-events:none/);
  assert.doesNotMatch(html, /\.dolo-lp-history-controls\[hidden\]\{[^}]*display:none/);
});

test('liquidity controls reuse the established DOLO dropdown and holder mode patterns', () => {
  assert.doesNotMatch(html, /id=["']dolo-lp-warning["']/);
  assert.doesNotMatch(html, /Partial sources/);
  assert.doesNotMatch(html, /<select[^>]+id=["']dolo-lp-chain["']/);

  for (const id of ['dolo-lp-chain', 'dolo-lp-pairs', 'dolo-lp-dexes']) {
    assert.match(html, new RegExp(`<[^>]+(?=[^>]*id=["']${id}["'])(?=[^>]*class=["'][^"']*\\bdd\\b)[^>]*>`));
    assert.match(html, new RegExp(`<button(?=[^>]*id=["']${id}-btn["'])(?=[^>]*class=["'][^"']*\\bdd-btn\\b)[^>]*>`));
    assert.match(html, new RegExp(`<div(?=[^>]*id=["']${id}-panel["'])(?=[^>]*class=["'][^"']*\\bdd-panel\\b)[^>]*>`));
  }

  assert.match(html, /class=["'][^"']*holder-bucket-mode[^"']*dolo-lp-mode[^"']*["']/);
  assert.match(html, /class=["'][^"']*dust-pill[^"']*["'][^>]+id=["']dolo-lp-low-liquidity["']/);
  assert.match(html, />Low-liq pools</);
});

test('active/history mode sits directly below the liquidity title', () => {
  const cardStart = html.indexOf('<section class="card dolo-lp-card"');
  const summaryStart = html.indexOf('<div class="dolo-lp-summary"', cardStart);
  const header = html.slice(cardStart, summaryStart);
  const title = header.indexOf('<h2>DOLO Liquidity Providers</h2>');
  const mode = header.indexOf('class="holder-bucket-mode dolo-lp-mode"');
  const meta = header.indexOf('id="dolo-lp-meta"');

  assert.ok(title >= 0 && mode > title && meta > mode);
});
