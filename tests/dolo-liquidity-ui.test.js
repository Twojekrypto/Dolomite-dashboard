const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const html = fs.readFileSync(path.resolve(__dirname, '../dolo-preview.html'), 'utf8');
const rootRoute = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
const doloRoute = fs.readFileSync(path.resolve(__dirname, '../dolo/index.html'), 'utf8');

function extractNamedFunction(name) {
  const marker = `function ${name}(`;
  const start = html.indexOf(marker);
  assert.notEqual(start, -1, `${name} must exist`);
  const bodyStart = html.indexOf('{', start);
  let depth = 0;
  let quote = null;
  let escaped = false;
  for (let index = bodyStart; index < html.length; index += 1) {
    const char = html[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === '"' || char === "'" || char === '`') {
      quote = char;
      continue;
    }
    if (char === '{') depth += 1;
    if (char === '}') {
      depth -= 1;
      if (depth === 0) {
        const source = html.slice(start, index + 1);
        return Function(`"use strict"; return (${source});`)();
      }
    }
  }
  assert.fail(`${name} has no closing brace`);
}

test('liquidity card remains one stable active-position shell', () => {
  const flows = html.indexOf('<h2>DOLO Flows</h2>');
  const liquidity = html.indexOf('<h2>DOLO Liquidity Providers</h2>');
  const fresh = html.indexOf('<h2>Fresh 10K+ DOLO Wallets</h2>');
  assert.ok(flows >= 0 && liquidity > flows && fresh > liquidity);
  for (const id of [
    'dolo-lp-count', 'dolo-lp-meta', 'dolo-lp-summary', 'dolo-lp-search',
    'dolo-lp-chain', 'dolo-lp-pairs', 'dolo-lp-dexes', 'dolo-lp-hide-dust',
    'dolo-lp-head', 'dolo-lp-body', 'dolo-lp-info', 'dolo-lp-pager',
  ]) assert.match(html, new RegExp(`id=["']${id}["']`));
  for (const removed of [
    'dolo-lp-low-liquidity', 'dolo-lp-history-period', 'dolo-lp-history-action',
    'dolo-lp-history-controls', 'data-dolo-lp-mode',
  ]) assert.doesNotMatch(html, new RegExp(removed));
  assert.doesNotMatch(html, />History<\/button>/);
  assert.match(html, /data-address-match-cells/);
});

test('active table schema and fixed ten-row geometry stay explicit', () => {
  for (const label of ['Chain', 'Address', 'Pair', 'Price Range', 'DOLO', 'Paired Asset', 'Value', 'Details']) {
    assert.match(html, new RegExp(label.replace(' ', '\\s*')));
  }
  const headers = html.match(/const ACTIVE_HEADERS\s*=\s*\[([\s\S]*?)\];/)?.[1] || '';
  assert.doesNotMatch(headers, /\["quality","Status"\]/);
  assert.match(html, /\.dolo-lp-table\{[^}]*table-layout:fixed/s);
  assert.match(html, /const ACTIVE_WIDTHS\s*=\s*\[10,22,14,15,10,13,10,6\]/);
  assert.doesNotMatch(html, /const HISTORY_WIDTHS/);
  assert.match(html, /pageSize:\s*10/);
  assert.match(html, /Array\.from\(\{length:\s*Math\.max\(0,\s*doloLpState\.pageSize/);
  assert.match(html, /<td colspan="8"/);
  assert.doesNotMatch(html, /<td class="status-cell">\$\{chip\(row\.quality\)\}<\/td>/);
});

test('pool eligibility is fail-closed at ten thousand dollars', () => {
  assert.match(html, /const MIN_POOL_LIQUIDITY_USD\s*=\s*10000/);
  assert.match(html, /function poolPassesLiquidityFloor\(pool\)/);
  assert.match(html, /finite\(pool\.liquidityUsd\)\s*&&\s*pool\.liquidityUsd\s*>=\s*MIN_POOL_LIQUIDITY_USD/);
  assert.match(html, /function eligiblePools\(\)/);
  assert.doesNotMatch(html, /includeLowLiquidity/);
  assert.doesNotMatch(html, /LOW_LIQUIDITY_USD/);
  assert.doesNotMatch(html, /Low-liq pools/);
});

test('chain narrows eligible Pair and DEX choices', () => {
  assert.match(html, /function availablePairValues\(\)/);
  assert.match(html, /function availableDexValues\(\)/);
  assert.match(html, /eligiblePools\(\)/);
  assert.match(html, /pool\.chainKey\s*===\s*doloLpState\.chain/);
  assert.match(html, /renderPairFilter\(\)/);
  assert.match(html, /renderDexFilter\(\)/);
});

test('filtered Chain has an independent accessible clear action', () => {
  assert.match(html, /data-dolo-lp-clear="chain"/);
  assert.match(html, /data-dolo-lp-clear="chain"[^>]*tabindex="0"/);
  assert.match(html, /function resetDoloLpChainFilter\(\)/);
  assert.match(html, /doloLpState\.chain\s*=\s*"all"/);
  assert.match(html, /kind\s*===\s*"chain"/);
  assert.match(html, /clear\.addEventListener\("keydown"/);
});

test('All pairs and All DEXes are exclusive resets', () => {
  assert.match(html, /data-dolo-lp-pair-filter-all/);
  assert.match(html, /data-dolo-lp-dex-filter-all/);
  assert.match(html, />All pairs</);
  assert.match(html, />All DEXes</);
  assert.match(html, /doloLpState\.pairs\.clear\(\)/);
  assert.match(html, /doloLpState\.dexes\.clear\(\)/);
  assert.match(html, /const allSelected\s*=\s*selected\.size\s*===\s*0/);
  assert.match(html, /selected:doloLpState\.dexes/);
});

test('position dust stays independently controllable below ten dollars', () => {
  assert.match(html, /const POSITION_DUST_USD\s*=\s*10/);
  assert.match(html, /hideDust:\s*true/);
  assert.match(html, /doloLpState\.hideDust\s*&&\s*finite\(row\.valueUsd\)\s*&&\s*row\.valueUsd\s*<\s*POSITION_DUST_USD/);
  assert.match(html, /id="dolo-lp-hide-dust"/);
  assert.match(html, />Hide dust</);
});

test('compact raw token amounts are BigInt-safe and match row notation', () => {
  const compactRawAmount = extractNamedFunction('compactRawAmount');
  assert.equal(compactRawAmount('1235000000000000000000', 18), '1.2K');
  assert.equal(compactRawAmount('1250000000000000000', 18), '1.25');
  assert.equal(compactRawAmount('1000000', 6), '1');
  assert.equal(compactRawAmount('0', 18), '0');
  assert.equal(compactRawAmount('not-wei', 18), 'Unavailable');
  assert.match(html, /function tokenAmountCell\(raw,decimals,symbol\)/);
  assert.match(html, /class="dolo-lp-token-amount"/);
  assert.match(html, /dolomite-token-icons\.generated\.js/);
  assert.match(html, /DOLOMITE_TOKEN_ICONS/);
  assert.match(html, /exactRawAmount\(raw,decimals\)/);
});

test('Details rounds current token amounts exactly to two decimals', () => {
  const roundedRawAmount = extractNamedFunction('roundedRawAmount');
  assert.equal(roundedRawAmount('16387908718238867098667283', 18), '16,387,908.72');
  assert.equal(roundedRawAmount('271676083206709108742668', 18), '271,676.08');
  assert.equal(roundedRawAmount('0', 6), '0.00');
  assert.equal(roundedRawAmount('not-wei', 18), 'Unavailable');
  assert.match(html, />Current DOLO</);
  assert.match(html, />Current paired asset</);
  assert.match(html, /roundedRawAmount\(row\.doloRaw,18\)/);
  assert.match(html, /roundedRawAmount\(row\.pairedRaw,pairedDecimals\)/);
});

test('every liquidity status has a plain-English Details explanation', () => {
  assert.match(html, /function statusPresentation\(value\)/);
  for (const phrase of [
    'fully verified on-chain',
    'evidence is incomplete',
    'older snapshot',
    'could not be fully verified',
    'outside the active price range',
  ]) assert.match(html, new RegExp(phrase, 'i'));
  assert.match(html, />Data status</);
  assert.match(html, /\$\{esc\(status\.label\)\}<\/strong> — \$\{esc\(status\.explanation\)\}/);
  assert.doesNotMatch(html, /class="dolo-lp-chip/);
});

test('verification status stays in Details and separate from price-range status', () => {
  assert.match(html, /class="dolo-lp-range\$\{rangeClass/);
  assert.doesNotMatch(html, /<td class="status-cell">/);
  assert.match(html, /const statusValue\s*=\s*row\.quality/);
  assert.match(html, />Data status</);
  assert.doesNotMatch(
    html,
    /chip\(row\.rangeStatus\s*===\s*"out_of_range"\s*\?\s*"out_of_range"\s*:\s*row\.quality\)/,
  );
});

test('Details follows the Dolomite Assets information hierarchy', () => {
  for (const className of [
    'dolo-lp-detail-panel', 'dolo-lp-detail-head', 'dolo-lp-detail-eyebrow',
    'dolo-lp-detail-title', 'dolo-lp-detail-meta', 'dolo-lp-detail-pill',
    'dolo-lp-detail-content', 'dolo-lp-detail-overview', 'dolo-lp-detail-evidence',
  ]) assert.match(html, new RegExp(className));
  for (const label of [
    'Current DOLO', 'Current paired asset', 'Range status', 'Pool liquidity',
    'Ownership', 'Position ID', 'Price bounds', 'Data status', 'Links',
  ]) assert.match(html, new RegExp(label));
  assert.doesNotMatch(html, />Tick interval</);
  assert.match(html, /class="dolo-lp-bound-row"><span>Lower bound<\/span>/);
  assert.match(html, /class="dolo-lp-bound-row"><span>Upper bound<\/span>/);
  assert.match(html, /title="\$\{esc\(exactRangeBound\(row\.rangeLower\)\)\}"/);
  assert.match(html, /Protocol maximum/);
  assert.match(html, /\$\{esc\(pairedSymbol\)\} per DOLO/);
  assert.match(html, /Source health/);
  assert.match(html, /aria-expanded="\$\{expanded\}"/);
  assert.match(html, /<span>\$\{expanded\s*\?\s*"Hide"\s*:\s*"Details"\}<\/span>\$\{CHEV_DOWN_ICO\}/);
});

test('Price bounds use readable fixed notation and one shared unit', () => {
  const formatRangeBound = extractNamedFunction('formatRangeBound');
  assert.equal(formatRangeBound('0.000999'), '0.000999');
  assert.doesNotMatch(formatRangeBound('0.000999'), /e/i);
  assert.equal(formatRangeBound('2.95428e-39'), '2.95 × 10⁻³⁹');
  assert.equal(formatRangeBound('3.38492e38'), '3.38 × 10³⁸');
  assert.match(html, /class="dolo-lp-bound-row"><span>Lower bound<\/span>/);
  assert.match(html, /class="dolo-lp-bound-row"><span>Upper bound<\/span>/);
  assert.match(html, /class="dolo-lp-bound-unit">\$\{esc\(pairedSymbol\)\} per DOLO/);
  assert.equal((html.match(/class="dolo-lp-bound-unit"/g) || []).length, 1);
});

test('liquidity summary reuses the Fresh wallets metric rail', () => {
  assert.match(html, /class="dolo-lp-summary fresh-wallet-stats selected-market-rail"/);
  assert.match(html, /class="fresh-stat selected-market-metric primary"/);
  assert.match(html, /class="fresh-stat selected-market-metric"/);
  assert.match(html, /class="label">\$\{icon\}\$\{label\}<\/div><div class="value">\$\{value\}<\/div><div class="sub">\$\{sub\}/);
  assert.match(html, /verified owners/i);
  assert.match(html, /\.fresh-wallet-stats\.selected-market-rail \.fresh-stat \.value\{[^}]*font-size:24px/s);
});

test('identity, numeric and action columns keep explicit alignment groups', () => {
  assert.match(html, /const LP_IDENTITY_COLUMNS\s*=\s*new Set\(\["chainKey","wallet","pair","rangeStatus"\]\)/);
  assert.match(html, /const LP_NUMERIC_COLUMNS\s*=\s*new Set\(\["doloRaw","pairedRaw","valueUsd"\]\)/);
  assert.match(html, /<td class="identity">\$\{chainCell\(row\.chainKey\)\}<\/td><td class="identity">\$\{walletCell\(row\)\}<\/td><td class="identity">\$\{pairCell\(pool\)\}<\/td><td class="identity">\$\{rangeCell\(row\)\}<\/td>/);
  assert.match(html, /\.dolo-lp-table \.identity\{text-align:left\}/);
  assert.match(html, /\.dolo-lp-table \.num\{[^}]*text-align:right/s);
  assert.match(html, /\.dolo-lp-table td\.details-cell\{text-align:center/);
});

test('finite Value cells use the muted Latest Exercises price blue', () => {
  assert.match(html, /class="num dolo-lp-value\$\{finite\(row\.valueUsd\)\s*\?\s*""\s*:\s*" is-muted"\}"/);
  assert.match(html, /\.dolo-lp-value\{color:#9ab7c2\}/);
  assert.match(html, /\.dolo-lp-value\.is-muted\{color:var\(--fg-4\)\}/);
});

test('unresolved protocol custody is named without inventing a wallet', () => {
  assert.match(html, /Kodiak Island custody/);
  assert.match(html, /Uniswap v4 vault custody/);
  assert.match(html, /Custody unresolved/);
  assert.match(html, /row\.attributionReason/);
  assert.match(html, /data-dolo-lp-copy="\$\{esc\(address\)\}"/);
});

test('mobile Details opens inside the visible table viewport', () => {
  assert.match(html, /@media \(max-width:640px\)\{[\s\S]*?\.dolo-lp-detail-panel\{[^}]*width:calc\(100vw - 48px\)[^}]*max-width:calc\(100vw - 48px\)/s);
  assert.match(html, /const opening\s*=\s*doloLpState\.expandedId\s*!==\s*details\.dataset\.doloLpDetails/);
  assert.match(html, /if\(opening\s*&&\s*window\.innerWidth\s*<=\s*640\)[\s\S]*?wrap\.scrollLeft\s*=\s*0/);
});

test('successful LP freshness uses the same gold pulse as DOLO Flows', () => {
  assert.match(html, /#dolo-lp-meta\{[^}]*color:var\(--fg-3\)/s);
  assert.match(html, /innerHTML=`<span class="pulse"><\/span>Data updated · \$\{agoLabel\(data\.generatedAt\)\}`/);
});

test('wallet, chain and details cells remain contained and accessible', () => {
  assert.match(html, /class="chain-badge"><img src="\$\{esc\(chain\.icon\)\}"/);
  assert.match(html, /\.dolo-lp-card \.chain-badge\{[^}]*font-size:12\.5px[^}]*font-weight:500/s);
  assert.match(html, /addr-tooltip-wrap[^>]+data-full-addr/);
  assert.match(html, /debank\.com\/profile/);
  assert.match(html, /\.dolo-lp-details-btn\{[^}]*-webkit-appearance:none[^}]*appearance:none[^}]*overflow:visible/s);
  assert.match(html, /@media \(max-width:640px\)\{[\s\S]*?\.dolo-lp-details-btn\{[^}]*min-height:44px/s);
});

test('price-range states use restrained semantic text colours', () => {
  assert.match(html, /\.dolo-lp-range\.is-in-range>span\{color:var\(--up\)\}/);
  assert.match(html, /\.dolo-lp-range\.is-out-of-range>span\{color:var\(--down\)\}/);
  assert.match(html, /row\.rangeStatus\s*===\s*"in_range"\s*\?\s*"is-in-range"/);
  assert.match(html, /row\.rangeStatus\s*===\s*"out_of_range"\s*\?\s*"is-out-of-range"/);
});

test('exact token tooltip is anchored to numeric text only', () => {
  assert.match(html, /class="dolo-lp-token-value" data-tooltip="\$\{esc\(tooltip\)\}"/);
  assert.doesNotMatch(html, /class="dolo-lp-token-amount" data-tooltip=/);
  assert.match(html, /\.dolo-lp-token-value\{[^}]*cursor:default/);
});

test('stable-height shell shows one readable empty-state message', () => {
  assert.match(html, /\.tbl tbody tr\.tbl-spacer-row\.dolo-lp-empty:first-child td\{[^}]*color:var\(--fg-4\)/);
  assert.match(html, /No active positions match these filters\./);
});

test('liquidity dropdowns and sortable headers retain dashboard parity', () => {
  for (const id of ['dolo-lp-chain', 'dolo-lp-pairs', 'dolo-lp-dexes']) {
    assert.match(html, new RegExp(`<[^>]+(?=[^>]*id=["']${id}["'])(?=[^>]*class=["'][^"']*\\bdd\\b)[^>]*>`));
    assert.match(html, new RegExp(`<button(?=[^>]*id=["']${id}-btn["'])(?=[^>]*class=["'][^"']*\\bdd-btn\\b)[^>]*>`));
    assert.match(html, new RegExp(`<div(?=[^>]*id=["']${id}-panel["'])(?=[^>]*class=["'][^"']*\\bdd-panel\\b)[^>]*>`));
  }
  assert.match(html, /\.dolo-lp-table thead th\{[^}]*font-size:10px[^}]*font-weight:600[^}]*letter-spacing:1\.6px/s);
  assert.match(html, /function syncFlowSortHeaders\(\)[\s\S]*?th\.setAttribute\("aria-sort",\s*active\s*\?\s*\(sort\.dir\s*===\s*"asc"\s*\?\s*"ascending"\s*:\s*"descending"\)\s*:\s*"none"\)/);
});

test('both production entry points advance the active-only liquidity cache once', () => {
  const version = 'lp-coverage-parity-20260813';
  for (const [name, source] of [['index.html', rootRoute], ['dolo/index.html', doloRoute]]) {
    assert.equal(source.split(version).length - 1, 1, name);
  }
  assert.match(html, /data\/dolo-liquidity\.json\?v=20260813-lp-coverage-parity/);
});
