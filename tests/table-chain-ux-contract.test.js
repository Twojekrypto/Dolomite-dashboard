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
const footer = read('protocol-footer.css');

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
  assert(footer.includes('body .card > .card-head,'), 'Shared table surface CSS should cover card headers');
  assert(footer.includes('border-bottom: 0 !important;'), 'Shared table surface CSS should remove horizontal table separators');
  assert(!liveTable.includes('data-sort="rank"'), 'Live Programs should not render the # ranking column');
  assertBefore(liveTable, '<th data-sort="chain"', '<th data-sort="name"', 'Live Programs should put Chain before Program');
  assert(liveTable.includes('id="rwRateSwitch"'), 'Live Programs should expose an APR/APY rate switch');
  assert(liveTable.includes('class="pill-switch"'), 'Live Programs rate switch should reuse the Dolomite Assets pill UX');
  const cardToolsCss = between(rewards, '.card-tools{', '.pulse{');
  assert(cardToolsCss.includes('flex-direction:column'), 'Live Programs tools should stack meta above the APR/APY switch');
  assert(cardToolsCss.includes('align-items:flex-end'), 'Live Programs tools should align the APR/APY switch under the meta on the right');
  const liveHead = between(liveTable, '<div class="card-tools">', '</div>\n    </div>\n    <div class="tbl-wrap">');
  assertBefore(liveHead, 'id="rwLiveMeta"', 'id="rwRateSwitch"', 'Live Programs should place the APR/APY switch under the daily rewards meta');
  assert(liveTable.includes('data-rate-mode="APR"'), 'Live Programs rate switch should include APR mode');
  assert(liveTable.includes('data-rate-mode="APY"'), 'Live Programs rate switch should include APY mode');
  assert(liveTable.includes('<th data-sort="apr" class="num" style="width:110px">Supply</th>'), 'Live Programs should label the APR/APY column as Supply');
  assert(!liveTable.includes('<th data-sort="apr" class="num" style="width:110px">APR</th>'), 'Live Programs should not label the Supply column as APR');
  assert(!liveTable.includes('estimated (EST)'), 'Live Programs copy should not describe oDOLO rewards with an EST badge');
  assert(rewards.includes('<div class="val" id="rwRewardContext">—</div>'), 'Rewards hero should not show LIVE as the Reward Value');
  const renderHero = between(rewards, 'function renderHero()', 'function renderAll()');
  assert(renderHero.includes("document.getElementById('rwRewardContext').textContent = fmtUsd(daily);"), 'Rewards hero should populate Reward Value with the daily rewards amount');

  const renderLive = between(rewards, 'function renderLive()', 'function renderPast()');
  assert(!renderLive.includes('<span class="rank">${index + 1}</span>'), 'Live Programs rows should not render row numbers');
  assertBefore(renderLive, '<td>${chainBadge(program.chain)}</td>', '<td>${programCell(program)}</td>', 'Live Programs row should put Chain before Program');
  assert(renderLive.includes('fmtSupplyRate(program.apr)'), 'Live Programs rows should format the Supply column through the active APR/APY mode');
  assert(!renderLive.includes('apr-est'), 'Live Programs rows should not render EST badges next to oDOLO rewards');

  const endedTable = between(rewards, '<!-- ENDED PROGRAMS -->', '</section>');
  assert(!endedTable.includes('<th style="width:56px">#</th>'), 'Ended Programs should not render the # ranking column');
  assertBefore(endedTable, '<th style="width:150px">Chain</th>', '<th>Program</th>', 'Ended Programs should put Chain before Program');
  assert(!endedTable.includes('Last TVL'), 'Ended Programs should not report TVL as the historical size metric');
  assertBefore(endedTable, 'Start Supply', 'End Supply', 'Ended Programs should show supply at campaign start before supply at campaign end');
  assertBefore(endedTable, '<th class="num" style="width:150px">End Supply</th>', '<th class="num" style="width:150px">Rewards</th>', 'Ended Programs should show historical Merkl rewards after supply columns');
  assertBefore(endedTable, '<th class="num" style="width:150px">Rewards</th>', '<th style="width:130px;padding-right:32px">Ended</th>', 'Ended Programs should show rewards before the end date');
  const renderPast = between(rewards, 'function renderPast()', 'function renderHero()');
  assert(!renderPast.includes('<span class="rank">${index + 1}</span>'), 'Ended Programs rows should not render row numbers');
  assertBefore(renderPast, '<td>${chainBadge(program.chain)}</td>', '<td>${programCell(program)}</td>', 'Ended Programs rows should put Chain before Program');
  assert(renderPast.includes("supplyCell(program, 'start')"), 'Ended Programs rows should render start supply from supply history');
  assert(renderPast.includes("supplyCell(program, 'end')"), 'Ended Programs rows should render end supply from supply history');
  assert(renderPast.includes('endedRewardsCell(program)'), 'Ended Programs rows should render historical reward amounts');
  assert(!renderPast.includes('fmtUsd(program.tvlUsd)'), 'Ended Programs rows should not render TVL in the historical supply columns');
}

{
  assert(portfolio.includes('.pf-chain-chip{display:inline-flex;align-items:center;gap:7px;color:var(--fg-1);font-size:12.5px;font-weight:500'), 'Portfolio chain chips should match the Rewards chain badge styling');
  const chainChip = between(portfolio, 'function chainChip(chainKey)', 'function tokenCell(row)');
  assert(chainChip.includes('${c.name || c.short}</span>'), 'Portfolio chain chips should show the full chain name, not only the short code');
}

{
  assertBefore(historyHtml, '<col class="col-chain">', '<col class="col-date">', 'History results should put Chain col before Date');
  assertBefore(historyHtml, 'data-history-sort="chain"', 'data-history-sort="date"', 'History results should put Chain header before Date');
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
  assert(liquidation.includes('body.route-liquidation #liquidation-history-table colgroup col:nth-child(2) { width: 19% !important; }'), 'Liquidation History should size the Liquidated wallet column before Date');
  assert(liquidation.includes('body.route-liquidation #liquidation-history-table colgroup col:nth-child(3) { width: 39% !important; }'), 'Liquidation History should absorb the former spacer into Date so money columns do not move');
  assert(liquidation.includes('body.route-liquidation #liquidation-history-table colgroup col:nth-child(4) { width: 0% !important; }'), 'Liquidation History spacer column should collapse so it cannot render as a black bar');
  assert(liquidation.includes('body.route-liquidation #liquidation-history-table colgroup col:nth-child(5) { width: 18.2% !important; }'), 'Liquidation History collateral column should stay visually fixed');
  assert(liquidation.includes('body.route-liquidation #liquidation-history-table colgroup col:nth-child(6) { width: 16% !important; }'), 'Liquidation History debt column should stay visually fixed');
  assert(liquidation.includes('body.route-liquidation #liquidation-history-table tbody td:nth-child(3) {\n            padding-left: 0 !important;'), 'Liquidation History Date column should sit closer to the Liquidated wallet column without moving money columns');
  assert(liquidation.includes('transform: translateX(-12px) !important;'), 'Liquidation History Date text should move closer without changing column widths');
  assert(liquidation.includes('body.route-liquidation #liquidation-history-table thead th.col-spacer,\n        body.route-liquidation #liquidation-history-table tbody td.col-spacer {\n            width: 0 !important;'), 'Liquidation History spacer cells should have no visible width');
  const walletOverflowRule = between(liquidation, '.liquidation-history-table tbody td:nth-child(2)', '.liquidation-history-table tbody td:first-child');
  assert(walletOverflowRule.includes('overflow: visible;'), 'Liquidation History address tools should remain visible in the second column');

  const historyHead = between(liquidation, '<table class="liquidation-history-table"', '<tbody id="liquidation-history-body"');
  assertBefore(historyHead, '<th>Liquidated wallet</th>', '<th>Date</th>', 'Liquidation History should put Liquidated wallet before Date');

  const historyRows = between(liquidation, 'body.innerHTML = pageRows.map(row => {', 'if (pageRows.length < LIQUIDATION_HISTORY_PAGE_SIZE)');
  assertBefore(historyRows, 'renderLiquidationHistoryAddress(row.liquidatedAddress, chain)', 'supplyFormatActivityDate(row.timestamp)', 'Liquidation History rows should put wallet cells before date cells');
}
