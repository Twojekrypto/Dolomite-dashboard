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
    'dolo-lp-hide-dust',
    'dolo-lp-history-period', 'dolo-lp-history-action', 'dolo-lp-head',
    'dolo-lp-body', 'dolo-lp-info', 'dolo-lp-pager',
  ]) assert.match(html, new RegExp(`id=["']${id}["']`));
  assert.match(html, /Active positions/);
  assert.match(html, />History</);
  assert.match(html, /data-address-match-cells/);
  assert.match(html, /shared-hover-tooltips\.js/);
});

test('liquidity UI keeps exact active/history schemas and stable ten-row pagination', () => {
  for (const label of ['Chain', 'Address', 'Pair', 'Price Range', 'DOLO', 'Paired Asset', 'Value', 'Status', 'Details']) {
    assert.match(html, new RegExp(label.replace(' ', '\\s*')));
  }
  for (const label of ['Date', 'Action']) assert.match(html, new RegExp(label));
  assert.match(html, /pageSize:\s*10/);
  assert.match(html, /Array\.from\(\{length:\s*Math\.max\(0,\s*doloLpState\.pageSize/);
  assert.match(html, /data-dolo-lp-details/);
});

test('liquidity table uses fixed mode-specific column geometry', () => {
  assert.match(html, /\.dolo-lp-table\{[^}]*table-layout:fixed/s);
  assert.match(html, /const ACTIVE_WIDTHS\s*=\s*\[10,20,13,13,9,11,10,8,6\]/);
  assert.match(html, /const HISTORY_WIDTHS\s*=\s*\[10,9,20,13,9,10,12,11,6\]/);
  assert.match(html, /id="dolo-lp-columns" data-dolo-lp-columns="active"/);
  assert.match(html, /columns\.dataset\.doloLpColumns\s*=\s*doloLpState\.mode/);
  assert.match(html, /widths\.map\(width\s*=>\s*`<col style="width:\$\{width\}%">`\)/);
  assert.match(html, /Array\.from\(\{length:\s*Math\.max\(0,\s*doloLpState\.pageSize/);
});

test('collapsed price range is classified instead of rendering raw bounds', () => {
  assert.match(html, /function compactRangeBound\(value\)/);
  assert.match(html, /function rangePresentation\(row\)/);
  assert.match(html, /Near-full range/);
  assert.match(html, /Custom range/);
  assert.match(html, /Always active/);
  assert.doesNotMatch(html, /`\$\{number\(Number\(row\.rangeLower\)\)\}–\$\{number\(Number\(row\.rangeUpper\)\)\}`/);
});

test('expanded liquidity details carry exact range evidence and accessible sort state', () => {
  for (const label of ['Exact lower bound', 'Exact upper bound', 'Tick interval']) {
    assert.match(html, new RegExp(label));
  }
  assert.match(html, /exactRangeBound\(row\.rangeLower\)/);
  assert.match(html, /exactRangeBound\(row\.rangeUpper\)/);
  assert.match(html, /aria-sort="\$\{ariaSort\}"/);
  assert.match(html, /class="sort" aria-hidden="true">\$\{marker\}<\/span>/);
});

test('liquidity quantities use on-chain token decimals and expose exact position evidence', () => {
  assert.match(html, /function pairDecimals\(pool\)[\s\S]*?pool\?\.pairedDecimals/);
  assert.doesNotMatch(html, /\["USDC","USDC\.e","USD1"\]\.includes\(pool\?\.pairedSymbol\)/);
  assert.match(html, /function exactRawAmount\(raw, decimals\)/);
  assert.match(html, /DOLO amount/);
  assert.match(html, /Paired amount/);
  assert.match(html, /Position value/);
  assert.match(html, /Exact lower bound/);
  assert.match(html, /Exact upper bound/);
  assert.match(html, /function exactRangeBound\(value\)/);
  assert.match(html, /dolo-lp-exact/);
  assert.match(html, /function roundedRawAmount\(raw, decimals\)/);
  assert.match(html, /roundedRawAmount\(row\.doloRaw,18\)/);
  assert.match(html, /roundedRawAmount\(row\.pairedRaw,pairDecimals\(pool\)\)/);
});

test('chain selection narrows the pair menu and All pairs is an exclusive reset', () => {
  assert.match(html, /function availablePairValues\(\)/);
  assert.match(html, /pool\.chainKey\s*===\s*doloLpState\.chain/);
  assert.match(html, /data-dolo-lp-filter-all/);
  assert.match(html, />All pairs</);
  assert.match(html, /doloLpState\.pairs\.clear\(\)/);
  assert.match(html, /renderPairFilter\(\)/);
});

test('active and history rows place Address before Pair', () => {
  assert.match(html, /\["chainKey","Chain"\],\["wallet","Address"\],\["pair","Pair"\]/);
  assert.match(html, /\["timestamp","Date"\],\["chainKey","Chain"\],\["wallet","Address"\],\["pair","Pair"\]/);
  assert.match(html, /<td>\$\{chainCell\(row\.chainKey\)\}<\/td><td>\$\{walletCell\(row\)\}<\/td><td>\$\{pairCell\(pool\)\}<\/td>/);
});

test('mode, filters, null pricing and action groups are explicit behavior contracts', () => {
  assert.match(html, /mode:\s*["']active["']/);
  assert.match(html, /historyPeriod:\s*["']30d["']/);
  assert.match(html, /activeSort:\s*\{key:["']valueUsd["'],\s*dir:["']desc["']/);
  assert.match(html, /historySort:\s*\{key:["']timestamp["'],\s*dir:["']desc["']/);
  assert.match(html, /row\.liquidityUsd\s*===\s*null/);
  assert.match(html, /Added.*Increased/);
  assert.match(html, /Removed.*Closed/);
  assert.match(html, /data\/dolo-liquidity\.json\?v=20260812-filter-ux/);
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
  const version = 'dolo-liquidity-filter-ux-20260812';
  for (const [name, source] of [['index.html', rootRoute], ['dolo/index.html', doloRoute]]) {
    assert.equal(source.split(version).length - 1, 1, name);
  }
  assert.match(html, /data\/dolo-liquidity\.json\?v=20260812-filter-ux/);
});

test('Details remains usable in Safari and meets the mobile touch target', () => {
  assert.match(html, /\.dolo-lp-details-btn\{[^}]*-webkit-appearance:none[^}]*appearance:none[^}]*overflow:visible/s);
  assert.match(html, /\.dolo-lp-details-btn\{[^}]*height:24px[^}]*border-radius:999px/s);
  assert.match(html, /\.dolo-lp-details-btn svg\{[^}]*width:12px[^}]*transition:transform/s);
  assert.match(html, /\.dolo-lp-details-btn\[aria-expanded="true"\] svg\{[^}]*transform:rotate\(180deg\)/s);
  assert.match(html, /<span>\$\{expanded\s*\?\s*"Hide"\s*:\s*"Details"\}<\/span>\$\{CHEV_DOWN_ICO\}/);
  assert.match(html, /@media \(max-width:640px\)\{[\s\S]*?\.dolo-lp-details-btn\{[^}]*min-height:44px/s);
});

test('the stable-height shell still shows one readable empty-state message', () => {
  assert.match(html, /\.tbl tbody tr\.tbl-spacer-row\.dolo-lp-empty:first-child td\{[^}]*color:var\(--fg-4\)/);
  assert.match(html, /No verified positions match these filters\./);
});

test('history controls hide without displacing the right-aligned low-liquidity switch', () => {
  assert.match(html, /\.dolo-lp-history-controls\[hidden\]\{[^}]*display:none!important/);
  assert.match(html, /class="tb-right dolo-lp-toolbar-secondary"/);
  const toolbarStart = html.indexOf('<div class="toolbar dolo-lp-toolbar">');
  const toolbarEnd = html.indexOf('</div>\n\n    <div class="tbl-wrap">', toolbarStart);
  const toolbar = html.slice(toolbarStart, toolbarEnd);
  assert.ok(toolbar.indexOf('class="tb-left dolo-lp-toolbar-primary"') >= 0);
  assert.ok(toolbar.indexOf('id="dolo-lp-low-liquidity"') > toolbar.indexOf('class="tb-right dolo-lp-toolbar-secondary"'));
  assert.ok(toolbar.indexOf('id="dolo-lp-hide-dust"') > toolbar.indexOf('id="dolo-lp-low-liquidity"'));
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
  assert.match(html, /class=["'][^"']*dust-pill[^"']*["'][^>]+id=["']dolo-lp-hide-dust["']/);
  assert.match(html, />Low-liq pools</);
  assert.match(html, />Hide dust</);
});

test('dust filtering, asset-style chain badges and contained status chips are explicit', () => {
  assert.match(html, /const POSITION_DUST_USD\s*=\s*10/);
  assert.match(html, /hideDust:\s*true/);
  assert.match(html, /doloLpState\.hideDust\s*&&\s*finite\(row\.valueUsd\)\s*&&\s*row\.valueUsd\s*<\s*POSITION_DUST_USD/);
  assert.match(html, /class="chain-badge"><img src="\$\{esc\(chain\.icon\)\}"/);
  assert.match(html, /\.dolo-lp-card \.chain-badge\{[^}]*font-size:12\.5px[^}]*font-weight:500/s);
  assert.match(html, /\.dolo-lp-chip\{[^}]*max-width:100%[^}]*overflow:hidden[^}]*text-overflow:ellipsis/s);
  assert.match(html, /#dolo-lp-meta\{[^}]*color:var\(--fg-3\)/s);
});

test('header metadata is separated before the active/history mode row', () => {
  const cardStart = html.indexOf('<section class="card dolo-lp-card"');
  const summaryStart = html.indexOf('<div class="dolo-lp-summary"', cardStart);
  const header = html.slice(cardStart, summaryStart);
  const title = header.indexOf('<h2>DOLO Liquidity Providers</h2>');
  const meta = header.indexOf('id="dolo-lp-meta"');
  const separator = header.indexOf('class="dolo-lp-head-separator"');
  const modeRow = header.indexOf('class="dolo-lp-mode-row"');
  const mode = header.indexOf('class="holder-bucket-mode dolo-lp-mode"');

  assert.ok(title >= 0 && meta > title && separator > meta && modeRow > separator && mode > modeRow);
});

test('liquidity and flow sortable headers match Dolomite Assets typography', () => {
  assert.match(html, /\.dolo-lp-table thead th\{[^}]*font-size:10px[^}]*font-weight:600[^}]*letter-spacing:1\.6px/s);
  assert.match(html, /\.flows-tbl thead th\{[^}]*font-size:10px[^}]*font-weight:600[^}]*letter-spacing:1\.6px/s);
  assert.match(html, /function syncFlowSortHeaders\(\)[\s\S]*?th\.setAttribute\("aria-sort",\s*active\s*\?\s*\(sort\.dir\s*===\s*"asc"\s*\?\s*"ascending"\s*:\s*"descending"\)\s*:\s*"none"\)/);
});
