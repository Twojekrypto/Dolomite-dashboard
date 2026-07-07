const assert = require('assert');
const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');

const rewards = read('rewards-preview.html');
const liquidation = read('liquidation-preview.html');
const portfolio = read('portfolio-preview.html');
const historyHtml = read('history/index.html');
const historyCss = read('history/history.css');
const historyJs = read('history/history.js');
const assets = read('assets-preview.html');

function between(source, start, end) {
  const a = source.indexOf(start);
  assert(a >= 0, `Missing start marker: ${start}`);
  const b = source.indexOf(end, a + start.length);
  assert(b >= 0, `Missing end marker: ${end}`);
  return source.slice(a, b);
}

function assertBefore(source, first, second, message) {
  const a = source.indexOf(first);
  const b = source.indexOf(second);
  assert(a >= 0, `Missing expected text: ${first}`);
  assert(b >= 0, `Missing expected text: ${second}`);
  assert(a < b, message);
}

{
  const liveTable = between(rewards, '<!-- LIVE PROGRAMS -->', '<!-- ENDED PROGRAMS -->');
  assert(!liveTable.includes('data-sort="rank"'), 'Live Programs should not render the # ranking column');
  assertBefore(liveTable, '<th data-sort="chain"', '<th data-sort="name"', 'Live Programs should put Chain before Program');

  const renderLive = between(rewards, 'function renderLive()', 'function renderPast()');
  assert(!renderLive.includes('<span class="rank">${index + 1}</span>'), 'Live Programs rows should not render row numbers');
  assertBefore(renderLive, '<td>${chainBadge(program.chain)}</td>', '<td>${programCell(program)}</td>', 'Live Programs row should put Chain before Program');
}

{
  assert(portfolio.includes('.pf-chain-chip{display:inline-flex;align-items:center;gap:7px;color:var(--fg-1);font-size:12.5px;font-weight:500'), 'Portfolio chain chips should match the Rewards chain badge styling');
  const chainChip = between(portfolio, 'function chainChip(chainKey)', 'function tokenCell(row)');
  assert(chainChip.includes('${c.name || c.short}</span>'), 'Portfolio chain chips should show the full chain name, not only the short code');
}

{
  assertBefore(historyHtml, '<col class="col-chain">', '<col class="col-date">', 'History results should put Chain col before Date');
  assertBefore(historyHtml, '<th>Chain</th>', '<th>Date</th>', 'History results should put Chain header before Date');
  const rowHtml = between(historyJs, 'function rowHtml(row, expanded, index = 0)', 'function displayActionsForRow(row)');
  assertBefore(rowHtml, '<td class="chain-td">${chainChip(row.chainKey)}</td>', '<td class="date-td">', 'History result rows should put Chain before Date');
  assert(historyCss.includes('.chain-chip{display:inline-flex;align-items:center;gap:7px;color:var(--fg-1);font-size:12.5px;font-weight:500'), 'History chain chips should match the Rewards chain badge styling');
  assert(historyJs.includes('<span class="chain-name">${escapeHtml(chain.name)}</span>'), 'History chain chips should show the full chain name');
}

{
  assertBefore(assets, '<th data-sort="chain"', '<th data-sort="name"', 'Dolomite Assets should add Chain before Asset');
  assert(assets.includes('function assetChainBadge(chainKey)'), 'Dolomite Assets should render the chain in its own badge helper');
  assert(assets.includes('<td style="padding-left:32px">${assetChainBadge(r.chain)}</td>'), 'Dolomite Assets rows should start with the Chain column');
  assert(!assets.includes('<span class="tag chain"><img src="${chain.icon}"'), 'Dolomite Assets should not repeat chain inside the Asset cell');
  assert(assets.includes('<td colspan="6">'), 'Dolomite Assets detail row should span the added Chain column');
}

{
  assert(liquidation.includes('--liquidation-history-visible-rows: 10;'), 'Liquidation History should size the scroll area for 10 visible rows');
  assert(liquidation.includes('font-size: 12.5px !important;'), 'Liquidation chain badges should use Rewards-like text sizing');
  assert(liquidation.includes('font-weight: 500 !important;'), 'Liquidation chain badges should use Rewards-like font weight');
  assert(liquidation.includes('color: var(--fg-1) !important;'), 'Liquidation chain badges should use Rewards-like foreground color');
}
