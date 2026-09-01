import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const preview = fs.readFileSync("dolo-preview.html", "utf8");

function extractNamedFunctionSource(name) {
  const marker = `function ${name}(`;
  const start = preview.indexOf(marker);
  assert.notEqual(start, -1, `${name} must exist`);
  const bodyStart = preview.indexOf("{", start);
  let depth = 0;
  let quote = null;
  let escaped = false;
  for (let index = bodyStart; index < preview.length; index += 1) {
    const char = preview[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === '"' || char === "'" || char === '`') {
      quote = char;
      continue;
    }
    if (char === "{") depth += 1;
    if (char === "}") {
      depth -= 1;
      if (depth === 0) return preview.slice(start, index + 1);
    }
  }
  assert.fail(`${name} has no closing brace`);
}

function extractStaticSections(html) {
  const sections = [];
  const sectionTag = /<\/?section\b[^>]*>/gi;
  let depth = 0;
  let start = -1;
  let match;
  while((match = sectionTag.exec(html))){
    if(match[0].startsWith("</")){
      depth -= 1;
      if(depth === 0 && start !== -1){
        sections.push(html.slice(start, sectionTag.lastIndex));
        start = -1;
      }
    } else {
      if(depth === 0) start = match.index;
      depth += 1;
    }
  }
  return sections;
}

function extractCexExchangeBreakdown(cexSupplyBrushSel, cexSupplyBrushDomainKey = "") {
  const source = [
    `let cexSupplyBrushSel = ${JSON.stringify(cexSupplyBrushSel)};`,
    `let cexSupplyBrushDomainKey = ${JSON.stringify(cexSupplyBrushDomainKey)};`,
    extractNamedFunctionSource("ensureCexSupplyBrushSelection"),
    extractNamedFunctionSource("visibleCexSupplyHistory"),
    extractNamedFunctionSource("buildCexExchangeBreakdown"),
    "return buildCexExchangeBreakdown;",
  ].join("\n");
  return Function(`"use strict"; ${source}`)();
}

function buildAllocationSeriesFromFixtures(history, nowModel, baseTs) {
  const source = [
    `const DOLO_HOLDER_BUCKET_HISTORY = ${JSON.stringify(history)};`,
    `const nowModel = ${JSON.stringify(nowModel)};`,
    `const baseTs = ${JSON.stringify(baseTs)};`,
    "const safeHolderNum = value => Number.isFinite(Number(value)) ? Number(value) : 0;",
    "function parseHolderTimestamp(){ return 0; }",
    "function getHolderDistribution(){ return nowModel; }",
    "function holderHistoryBaseTs(){ return baseTs; }",
    extractNamedFunctionSource("allocationPointFromSource"),
    extractNamedFunctionSource("buildAllocationSeries"),
    "return buildAllocationSeries();",
  ].join("\n");
  return Function(`"use strict"; ${source}`)();
}

function buildCexSupplySeriesFromFixtures(history, current, baseTs) {
  const source = [
    `const DOLO_CEX_SUPPLY_HISTORY = ${JSON.stringify(history)};`,
    "const DOLO_BALANCE_CHANGES = {};",
    "const doloAllHistoryStartTs = 0;",
    `const current = ${JSON.stringify(current)};`,
    `const baseTs = ${JSON.stringify(baseTs)};`,
    "const safeHolderNum = value => Number.isFinite(Number(value)) ? Number(value) : 0;",
    "function parseHolderTimestamp(value){ return Number(value) || 0; }",
    "function holderCexCurrentSupply(){ return current; }",
    "function holderHistoryBaseTs(){ return baseTs; }",
    "function holderCexFlow(){ return {net:0, inflow:0, outflow:0}; }",
    extractNamedFunctionSource("buildCexSupplySeries"),
    "return buildCexSupplySeries();",
  ].join("\n");
  return Function(`"use strict"; ${source}`)();
}

function extractHolderLegendRangeRows() {
  const source = [
    "const safeHolderNum = value => Number.isFinite(Number(value)) ? Number(value) : 0;",
    extractNamedFunctionSource("holderLegendRangeRows"),
    "return holderLegendRangeRows;",
  ].join("\n");
  return Function(`"use strict"; ${source}`)();
}

function holderHistorySourceKeyFromFixture(includeVeDolo) {
  const source = [
    "const state = {includeVeDolo:false};",
    extractNamedFunctionSource("holderHistorySourceKey"),
    "return holderHistorySourceKey;",
  ].join("\n");
  return Function(`"use strict"; ${source}`)()(includeVeDolo);
}

test("holder distribution keeps Total exposure as the permanent source", () => {
  assert.match(preview, /id="holder-bucket-mode"/);
  assert.match(preview, /data-holder-bucket-view="whales" aria-pressed="true"/);
  assert.match(preview, /data-holder-bucket-view="smaller" aria-pressed="false"/);
  assert.match(preview, /id="holder-metric-mode"/);
  assert.match(preview, /data-holder-metric="balance"/);
  assert.match(preview, /data-holder-metric="changePct"/);
  assert.match(preview, /aria-pressed="true"/);
  const holderCard = extractStaticSections(preview).find(section => /id="holder-distribution-card"/.test(section));
  assert.ok(holderCard);
  assert.doesNotMatch(holderCard, /data-holder-exposure|Wallet balance/);
  assert.equal(holderHistorySourceKeyFromFixture(false), "total_exposure");
  assert.equal(holderHistorySourceKeyFromFixture(true), "total_exposure_with_vedolo");
  assert.match(preview, /Entered balance range/);
  assert.match(preview, /Positive DOLO supplied or held as collateral/);
  assert.doesNotMatch(preview, /return "Entered range"|return "Moved out of range"/);
  const historyResolver = preview.slice(
    preview.indexOf("function getHolderHistory("),
    preview.indexOf("function holderWindowLabel(")
  );
  assert.doesNotMatch(historyResolver, /getHolderDistributionAtPoint|getHolderDistribution\(/);
  assert.match(historyResolver, /if\(precomputed\) return precomputed;\s*return null;/);
  const precomputedResolver = preview.slice(
    preview.indexOf("function getPrecomputedHolderHistory("),
    preview.indexOf("function getHolderHistory(")
  );
  assert.match(precomputedResolver, /if\(rawPoints\.some\(point => !point \|\| point\.ts <= 0\)\) return null;/);
  const chartRendererStart = preview.indexOf("function renderHolderDistributionChart(");
  const chartRenderer = preview.slice(
    chartRendererStart,
    preview.indexOf("function toggleHolderDistributionPin", chartRendererStart)
  );
  assert.match(chartRenderer, /if\(!fullModel\)/);
  assert.match(chartRenderer, /Total exposure history is unavailable/);
});

test("Team and Investor allocations are merged into holder ranges without a standalone card", () => {
  const cards = extractStaticSections(preview).filter(section => /\bholder-chart-card\b/.test(section));
  const allocationCards = cards.filter(section => /id="allocation-chart-card"/.test(section));
  const cardIds = cards.map(section => section.match(/id="([^"]+)"/)?.[1]);
  assert.equal(allocationCards.length, 0);
  assert.deepEqual(cardIds.slice(cardIds.indexOf("holder-distribution-card"), cardIds.indexOf("cex-supply-card") + 1), ["holder-distribution-card", "cex-supply-card"]);
  assert.match(preview, /<template id="allocation-chart-legacy" aria-hidden="true">/);
  assert.match(preview, /const holderAudience = "holders"/);
  assert.match(preview, /Market \+ Team\/Investor/);
});

test("allocation series appends a matching runtime Now point", () => {
  const series = buildAllocationSeriesFromFixtures(
    [{
      key: "hist_1",
      ts: 1_000,
      liquid: {market: {whales: {
        allocationTotal: 300,
        teamTotal: 200,
        investorTotal: 100,
        allocationWallets: 3,
        teamWallets: 2,
        investorWallets: 1,
      }}},
    }],
    {
      allocationTotal: 302,
      teamTotal: 201,
      investorTotal: 101,
      allocationWallets: 3,
      teamWallets: 2,
      investorWallets: 1,
    },
    10_000,
  );

  assert.deepEqual(series.points.map(point => point.key), ["hist_1", "now"]);
  assert.equal(series.current.key, "now");
});

test("allocation series rejects a runtime Now point with mismatched Team and Investor composition", () => {
  const series = buildAllocationSeriesFromFixtures(
    [{
      key: "hist_1",
      ts: 1_000,
      liquid: {market: {whales: {
        allocationTotal: 300,
        teamTotal: 200,
        investorTotal: 100,
        allocationWallets: 3,
        teamWallets: 2,
        investorWallets: 1,
      }}},
    }],
    {
      allocationTotal: 300,
      teamTotal: 100,
      investorTotal: 200,
      allocationWallets: 3,
      teamWallets: 1,
      investorWallets: 2,
    },
    10_000,
  );

  assert.deepEqual(series.points.map(point => point.key), ["hist_1"]);
  assert.equal(series.current.key, "hist_1");
});

test("allocation series rejects component wallet-count drift when balances still match", () => {
  const series = buildAllocationSeriesFromFixtures(
    [{
      key: "hist_1",
      ts: 1_000,
      liquid: {market: {whales: {
        allocationTotal: 300,
        teamTotal: 200,
        investorTotal: 100,
        allocationWallets: 4,
        teamWallets: 3,
        investorWallets: 1,
      }}},
    }],
    {
      allocationTotal: 300,
      teamTotal: 200,
      investorTotal: 100,
      allocationWallets: 4,
      teamWallets: 1,
      investorWallets: 3,
    },
    10_000,
  );

  assert.deepEqual(series.points.map(point => point.key), ["hist_1"]);
  assert.equal(series.current.key, "hist_1");
});

test("allocation series does not manufacture a runtime point without a generated baseline", () => {
  const series = buildAllocationSeriesFromFixtures(
    [],
    {
      allocationTotal: 300,
      teamTotal: 200,
      investorTotal: 100,
      allocationWallets: 3,
      teamWallets: 2,
      investorWallets: 1,
    },
    10_000,
  );

  assert.deepEqual(series.points, []);
  assert.equal(series.current.key, "now");
});

test("holder distribution explains scope and keeps the Change header concise", () => {
  assert.match(preview, /holder-source-exclusion/);
  assert.match(preview, /<span data-column="change">Change<\/span>/);
});

test("holder legend compares the exact visible endpoints without dropping Team and Investor balances", () => {
  const holderLegendRangeRows = extractHolderLegendRangeRows();
  const rows = holderLegendRangeRows({
    points: [
      {key: "hist_20260601", buckets: [{total: 254_030_706.10, allocationTotal: 210_766_381.84}]},
      {key: "hist_20260815", buckets: [{total: 244_674_520.62, allocationTotal: 200_060_103.13}]},
    ],
    current: {key: "hist_20260815", buckets: [{total: 244_674_520.62, allocationTotal: 200_060_103.13}]},
  });

  assert.equal(rows[0].firstTotal, 254_030_706.10);
  assert.equal(rows[0].currentTotal, 244_674_520.62);
  assert.equal(Math.round(rows[0].delta * 100) / 100, -9_356_185.48);
  assert.equal(Math.round(rows[0].pct * 100) / 100, -3.68);
});

test("CEX exchange breakdown reports sorted selected-range balance changes", () => {
  const buildCexExchangeBreakdown = extractCexExchangeBreakdown({from: 1, to: 2});
  const breakdown = buildCexExchangeBreakdown({
    points: [
      {
        ts: 1,
        exchanges: [
          {name: "Coinbase", liquid: 1000000, wallets: 2},
          {name: "Kraken", liquid: 2000000, wallets: 1},
        ],
      },
      {
        ts: 2,
        exchanges: [
          {name: "Coinbase", liquid: 1400000, wallets: 3},
          {name: "Kraken", liquid: 1500000, wallets: 1},
          {name: "Bitget", liquid: 2500000, wallets: 2},
        ],
      },
    ],
  });

  assert.deepEqual(breakdown, [
    {name: "Bitget", current: 2500000, start: 0, change: 2500000},
    {name: "Kraken", current: 1500000, start: 2000000, change: -500000},
    {name: "Coinbase", current: 1400000, start: 1000000, change: 400000},
  ]);
});

test("CEX exchange breakdown uses the chart's two nearest points for an empty brush interval", () => {
  const buildCexExchangeBreakdown = extractCexExchangeBreakdown({from: 48, to: 52}, "0:200:3");
  const breakdown = buildCexExchangeBreakdown({
    points: [
      {ts: 0, exchanges: [{name: "Binance", liquid: 100, wallets: 1}]},
      {ts: 100, exchanges: [{name: "Binance", liquid: 160, wallets: 1}]},
      {ts: 200, exchanges: [{name: "Binance", liquid: 220, wallets: 1}]},
    ],
  });

  assert.deepEqual(breakdown, [
    {name: "Binance", current: 160, start: 100, change: 60},
  ]);
});

test("CEX exchange breakdown stays empty when the chart endpoint has no exchange snapshot", () => {
  const buildCexExchangeBreakdown = extractCexExchangeBreakdown({from: 0, to: 200});
  const breakdown = buildCexExchangeBreakdown({
    points: [
      {ts: 0, exchanges: [{name: "Binance", liquid: 100, wallets: 1}]},
      {ts: 100, exchanges: [{name: "Binance", liquid: 160, wallets: 1}]},
      {ts: 200},
    ],
  });

  assert.deepEqual(breakdown, []);
});

test("CEX exact-data series keeps the generated endpoint and default breakdown populated", () => {
  const series = buildCexSupplySeriesFromFixtures(
    [
      {key: "day_1", timestamp: 1_000, liquid: 100, wallets: 1, exchanges: [{name: "Binance", liquid: 100, wallets: 1}]},
      {key: "day_2", timestamp: 2_000, liquid: 160, wallets: 1, exchanges: [{name: "Binance", liquid: 160, wallets: 1}]},
    ],
    {total: 160, wallets: 1},
    10_000,
  );
  const breakdown = extractCexExchangeBreakdown({from: 1_000, to: 10_000})(series);

  assert.deepEqual(series.points.map(point => point.key), ["day_1", "day_2"]);
  assert.deepEqual(breakdown, [
    {name: "Binance", current: 160, start: 100, change: 60},
  ]);
});

test("CEX one-point exact-data series keeps its generated endpoint", () => {
  const exchanges = [{name: "Binance", liquid: 160, wallets: 1}];
  const series = buildCexSupplySeriesFromFixtures(
    [{key: "day_1", timestamp: 1_000, liquid: 160, wallets: 1, exchanges}],
    {total: 160, wallets: 1},
    10_000,
  );

  assert.deepEqual(series.points.map(point => point.key), ["day_1"]);
  assert.equal(series.currentPoint.key, "day_1");
  assert.deepEqual(series.currentPoint.exchanges, exchanges);
  assert.deepEqual(extractCexExchangeBreakdown({from: 1_000, to: 10_000})(series), []);
});

test("CEX legacy series preserves its synthetic Now fallback without exchange snapshots", () => {
  const series = buildCexSupplySeriesFromFixtures(
    [
      {key: "day_1", timestamp: 1_000, liquid: 100, wallets: 1},
      {key: "day_2", timestamp: 2_000, liquid: 160, wallets: 1},
    ],
    {total: 160, wallets: 1},
    10_000,
  );

  assert.deepEqual(series.points.map(point => point.key), ["day_1", "day_2", "now"]);
  assert.deepEqual(extractCexExchangeBreakdown({from: 1_000, to: 10_000})(series), []);
});

test("CEX details disclosure is keyboard-focusable and protects long exchange labels", () => {
  assert.match(preview, /<details class="cex-exchange-disclosure" id="cexSupplyDetails">/);
  assert.match(preview, /<summary>CEX details<\/summary>/);
  assert.match(preview, /id="cexSupplyExchangeBreakdown"/);
  assert.match(preview, /\.cex-exchange-disclosure summary:focus-visible\{[^}]*outline:1px solid var\(--gold\)/);
  assert.match(preview, /\.cex-exchange-name\{[^}]*min-width:0;[^}]*overflow:hidden;[^}]*text-overflow:ellipsis/);
});

test("CEX details exposes canonical wallet addresses with copy and DeBank actions", () => {
  assert.match(preview, /function cexWalletRowsForExchange\(/);
  assert.match(preview, /DoloWalletTableUX\.walletCellHtml\(/);
  assert.match(preview, /class="cex-wallet-disclosure"/);
  assert.match(preview, /data-cex-wallet-copy/);
  assert.match(preview, /https:\/\/debank\.com\/profile\//);
  assert.match(preview, /Current wallet snapshot/);
});

test("each CEX exchange row visibly advertises its expandable address list", () => {
  assert.match(preview, /class="cex-exchange-expand"/);
  assert.match(preview, /wallets\.length === 1 \? "address" : "addresses"/);
  assert.match(preview, /\.cex-exchange-name\{[^}]*font-size:14px;[^}]*font-weight:650/);
  assert.match(preview, /class="cex-exchange-value"[^>]*>[\s\S]*?class="cex-exchange-current"[\s\S]*?class="cex-exchange-change/);
  assert.match(preview, /\.cex-exchange-value\{[^}]*display:flex;[^}]*flex-direction:column;[^}]*align-items:flex-end/);
  assert.match(preview, /\.cex-exchange-expand\{[^}]*border:1px solid var\(--line-2\);[^}]*border-radius:999px/);
  assert.match(preview, /\.cex-wallet-disclosure\[open\] \.cex-exchange-expand svg\{transform:rotate\(180deg\)\}/);
});

test("holder distribution states both its included and excluded wallet scope", () => {
  const scopeRenderer = preview.slice(preview.indexOf("function holderScopeHtml"), preview.indexOf("function holderCexStatHtml"));
  assert.match(scopeRenderer, /Includes: Market \+ Team\/Investor/);
  assert.match(scopeRenderer, /Excludes: CEX, protocol &amp; custody\/MM/);
  assert.match(scopeRenderer, /potential custody\/MM and bot wallets remain excluded/);
});

test("holder bucket controls share the metric UX without an active gold dot", () => {
  const controlsCss = preview.slice(
    preview.indexOf(".holder-bucket-mode,.holder-metric-mode{"),
    preview.indexOf(".holder-chart-toggle.is-active{")
  );
  assert.match(controlsCss, /\.holder-bucket-mode button\.active,\s*\.holder-bucket-mode button:hover,\s*\.holder-bucket-mode button:focus-visible,/);
  assert.match(controlsCss, /\.holder-metric-mode button\.active,\s*\.holder-metric-mode button:hover,\s*\.holder-metric-mode button:focus-visible/);
  assert.doesNotMatch(controlsCss, /holder-exposure-mode/);
  assert.doesNotMatch(controlsCss, /\.holder-bucket-mode button\.active::before/);
  assert.match(preview, /document\.querySelectorAll\("\[data-holder-bucket-view\]"\)\.forEach\(item => \{\s*const active = item\.dataset\.holderBucketView === holderBucketView;\s*item\.classList\.toggle\("active", active\);\s*item\.setAttribute\("aria-pressed", String\(active\)\);/);
});

test("holder distribution clips its final row to the card's rounded lower corners", () => {
  assert.match(preview, /\.holder-chart-card > \.holder-chart-legend:last-child\{border-radius:0 0 var\(--r-xl\) var\(--r-xl\);overflow:hidden}/);
});

test("holder distribution fixes the visible chart audience to market plus allocation wallets", () => {
  assert.doesNotMatch(preview, /holder-audience-mode/);
  assert.doesNotMatch(preview, /data-holder-audience/);
  assert.match(preview, /const holderAudience = "holders"/);
  assert.match(preview, /function holderBelongsToAudience\(type, audience = holderAudience\)/);
  assert.match(preview, /root\?\.\[audience\]\?\.\[holderBucketView\]/);
  assert.match(preview, /source\?\.\[balanceKey\]\?\.\[holderAudience\]\?\.\[holderBucketView\]/);
  assert.match(preview, /if\(!holderBelongsToAudience\(type\)\) return false;/);
});

test("holder distribution guards relative change and renders a symmetric percent view", () => {
  assert.match(preview, /let holderDistributionMetric = "balance"/);
  assert.match(preview, /function holderMetricValue\(/);
  assert.match(preview, /if\(baseline <= 0\) return null/);
  assert.match(preview, /function holderMetricScale\(/);
  assert.match(preview, /return \{min:-max, max, zero:0, label:value => value\.toFixed/);
  assert.match(preview, /const zeroY = yAt\(metricScale\.zero\)/);
  assert.match(preview, /if\(holderDistributionMetric !== "balance" \|\| model\.points\.length < 2\)\{\s*areaPath\.setAttribute\("d", ""\);/);
});

test("holder distribution exposes a guarded change tooltip", () => {
  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );
  assert.match(preview, /New \/ no baseline/);
  assert.match(preview, /holderDistributionMetric === "changePct" \? `<span class="tt-change \$\{deltaClass\}">\$\{changeText\}<\/span> · \$\{fmtNum\(bucket\.total\)\} DOLO · \$\{fmtSignedHolder\(delta\)\} DOLO`/);
  assert.doesNotMatch(holderRenderer, /Change · \$\{holderRangeLabel\}/);
});

test("holder distribution uses separate semantic pin and Details controls", () => {
  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );
  const holderLegendMarkup = holderRenderer.slice(
    holderRenderer.indexOf("const legendHeader ="),
    holderRenderer.indexOf("const legendChangeHead =")
  );
  const holderLegendHoverHandlers = holderRenderer.slice(
    holderRenderer.indexOf('legend.querySelectorAll(".holder-chart-legend-item")'),
    holderRenderer.indexOf('legend.querySelectorAll(".holder-legend-pin")')
  );

  assert.match(holderRenderer, /function toggleHolderDistributionPin\(key\)/);
  assert.match(holderLegendMarkup, /<button class="holder-legend-cell holder-legend-pin holder-legend-main" type="button" data-column="group" data-pin-key="\$\{bucket\.key\}"/);
  assert.match(holderLegendMarkup, /data-column="balance" data-pin-key="\$\{bucket\.key\}"/);
  assert.match(holderLegendMarkup, /data-column="wallets" data-pin-key="\$\{bucket\.key\}"/);
  assert.match(holderLegendMarkup, /data-column="change" data-pin-key="\$\{bucket\.key\}"/);
  assert.match(holderLegendMarkup, /<div class="holder-legend-cell holder-legend-details" data-column="details">\s*<button class="holder-details-btn" type="button"/);
  assert.doesNotMatch(holderLegendMarkup, /Market holders/);
  assert.doesNotMatch(holderLegendMarkup, /holder-chart-legend-item\$\{active\}[^>]*role="button"/);
  assert.doesNotMatch(holderLegendMarkup, /holder-chart-legend-item\$\{active\}[^>]*tabindex="0"/);

  assert.match(holderRenderer, /lines\.querySelectorAll\("\.holder-chart-series-line\[role='button'\]"\)[\s\S]*toggleHolderDistributionPin\(line\.dataset\.key\)/);
  assert.match(holderRenderer, /legend\.querySelectorAll\("\.holder-legend-pin"\)[\s\S]*toggleHolderDistributionPin\(pin\.dataset\.pinKey\)/);
  assert.doesNotMatch(holderLegendHoverHandlers, /addEventListener\("click"/);
  assert.match(holderRenderer, /holder-details-btn"\)\.forEach\(btn => btn\.addEventListener\("click", event => \{\s*event\.stopPropagation\(\);/);
  assert.match(holderRenderer, /if\(holderDistributionActiveKey === key\) holderDistributionActiveKey = "";/);
  assert.match(holderRenderer, /if\(holderDistributionActiveKey && !bucketDefs\.some\(bucket => bucket\.key === holderDistributionActiveKey\)\)\{\s*holderDistributionActiveKey = "";/);
});

test("holder distribution keeps an open Details panel independent from its active pin", () => {
  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );

  assert.doesNotMatch(holderRenderer, /if\(holderWalletPanelKey\)\{\s*holderDistributionActiveKey = holderWalletPanelKey;\s*\}/);
  assert.match(holderRenderer, /else \{\s*holderWalletPanelKey = key;\s*holderDistributionActiveKey = key;\s*\}/);
});

test("holder distribution Details controls match the compact Assets pattern", () => {
  assert.match(preview, /\.holder-legend-head-action\{text-align:center\}/);
  assert.match(preview, /\.holder-details-btn\{\s*height:24px;width:100%;max-width:72px;min-width:0;padding:0 6px;[^}]*gap:3px;[^}]*overflow:hidden;/);
  assert.match(preview, /\.holder-details-btn span\{font-size:9px;font-weight:700;letter-spacing:\.5px;[^}]*overflow:hidden;text-overflow:ellipsis;white-space:nowrap}/);

  const distributionRenderer = preview.slice(preview.indexOf("function renderHolderDistributionChart"), preview.indexOf("function renderAllocationChart"));
  const allocationRenderer = preview.slice(preview.indexOf("function renderAllocationChart"), preview.indexOf("function renderCexSupplyChart"));
  assert.match(distributionRenderer, /<span class="holder-legend-head-action" data-column="details">Details<\/span>/);
  assert.match(allocationRenderer, /<span class="holder-legend-head-action">Details<\/span>/);
});

test("holder distribution matches the DOLO Holders table surface and typography", () => {
  assert.match(
    preview,
    /\.holder-distribution-legend\{--holder-layout-columns:50\.630228% 14\.756621% 10\.347846% 13\.807863% 10\.457442%\}/,
  );
  assert.match(
    preview,
    /\.holder-distribution-legend \.holder-legend-head,\.holder-distribution-legend \.holder-legend-row\{[\s\S]*grid-template-columns:var\(--holder-layout-columns\);/,
  );
  assert.match(
    preview,
    /\.holder-distribution-legend \[data-column\]\{min-width:0;order:var\(--holder-layout-order,0\)\}/,
  );
  assert.match(
    preview,
    /\.holder-distribution-legend \.holder-legend-metric\.primary\{[^}]*align-items:flex-end;text-align:right;/,
  );
  assert.match(
    preview,
    /\.holder-chart-legend\{[^}]*background:var\(--bg-2\)}/,
  );
  assert.match(
    preview,
    /\.holder-distribution-legend \.holder-legend-head\{[^}]*padding:12px 18px;[^}]*background:var\(--bg-1\);[^}]*font-family:var\(--sans\);font-size:10px;font-weight:600;letter-spacing:1\.6px;/,
  );
  assert.match(
    preview,
    /\.holder-distribution-legend \.holder-chart-legend-item \.bucket\{font-family:var\(--sans\);font-size:13px;font-weight:600;letter-spacing:-\.1px}/,
  );
  assert.match(
    preview,
    /\.holder-legend-number\{display:block;font-size:13px;font-weight:500;/,
  );
  assert.match(
    preview,
    /\.holder-distribution-legend \.holder-legend-metric\.primary \.holder-legend-number\{font-size:13px;color:var\(--fg-1\);font-weight:600;/,
  );
});

test("Bucket wallets keeps Change compact without repeating the DOLO unit", () => {
  const walletChange = preview.slice(
    preview.indexOf("function holderWalletRangeChange"),
    preview.indexOf("function zeroHolderRowFrom")
  );
  assert.doesNotMatch(walletChange, /main:\s*[^,}]*DOLO/);
});

test("Bucket wallets uses the same flat rows and hover contract as DOLO Holders", () => {
  const bucketTableCss = preview.slice(
    preview.indexOf(".holder-wallet-table-wrap{"),
    preview.indexOf(".holder-wallet-change{")
  );
  assert.match(bucketTableCss, /\.holder-wallet-table th\{[^}]*padding:12px 18px;[^}]*background:var\(--bg-1\);[^}]*font-size:10px;[^}]*font-weight:600;[^}]*letter-spacing:1\.6px;/);
  assert.match(bucketTableCss, /\.holder-wallet-table td\{[^}]*padding:12px 18px;[^}]*border-left:0;[^}]*border-right:0;/);
  assert.doesNotMatch(bucketTableCss, /tbody tr:nth-child\(even\)/);
  assert.match(bucketTableCss, /\.holder-wallet-table tbody tr:hover td\{background:var\(--bg-3\);color:var\(--fg-1\)\}/);
  assert.match(bucketTableCss, /\.holder-wallet-table tbody tr:hover td:first-child::before\{[^}]*width:2px;background:var\(--gold\);/);
});

test("Bucket wallets has a DOLO Holders background with one complete table contour", () => {
  const detailCss = preview.slice(
    preview.indexOf(".holder-wallet-detail-shell{"),
    preview.indexOf(".holder-wallet-change{")
  );
  assert.match(detailCss, /\.holder-wallet-detail-shell\{[^}]*background:var\(--bg-2\);/);
  assert.match(detailCss, /\.holder-wallet-table-wrap\{[^}]*margin:0 12px 14px 14px;[^}]*border:1px solid var\(--line-2\);[^}]*border-radius:8px;[^}]*background:var\(--bg-2\);/);
});

test("Bucket wallet Details panel has a restrained gold perimeter without a gold side treatment", () => {
  const panelCss = preview.slice(
    preview.indexOf(".holder-wallet-panel{"),
    preview.indexOf(".holder-wallet-panel-head{")
  );
  const tableCss = preview.slice(
    preview.indexOf(".holder-wallet-table-wrap{"),
    preview.indexOf(".holder-wallet-table{")
  );
  assert.match(panelCss, /\.holder-wallet-panel\{[^}]*background:transparent;/);
  assert.match(panelCss, /\.holder-wallet-panel-inline\{background:transparent}/);
  assert.match(panelCss, /\.holder-wallet-detail-shell\{[^}]*border:1px solid rgba\(201,162,39,\.22\);[^}]*background:var\(--bg-2\);[^}]*box-shadow:var\(--sh-card\);/);
  assert.match(panelCss, /\.holder-wallet-detail-shell::before\{display:none}/);
  assert.match(tableCss, /\.holder-wallet-table-wrap\{[^}]*border:1px solid var\(--line-2\);[^}]*box-shadow:none;/);
  assert.doesNotMatch(panelCss, /linear-gradient/);
});

test("holder distribution removes vertical dividers and centers the Details control", () => {
  const legendCss = preview.slice(
    preview.indexOf(".holder-distribution-legend{--holder-layout-columns"),
    preview.indexOf(".holder-wallet-panel{")
  );
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-head > \[data-column\]\{[^}]*border-left:0;/);
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-cell\{[^}]*border:0;/);
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-metric\{[^}]*border-left:0;/);
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-metric\.primary::before\{display:none}/);
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-head > \[data-column="details"\]\{text-align:center}/);
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-details\{[^}]*display:grid;[^}]*place-items:center;[^}]*padding:0 12px;/);
  assert.match(legendCss, /holder-legend-details \.holder-details-btn\{margin:0;max-width:72px;justify-self:center;}/);
  assert.doesNotMatch(legendCss, /\.holder-distribution-legend \.holder-legend-details \.holder-details-btn svg\{display:none}/);
});

test("holder Details replaces rank with Fresh-style Chain and a wallet search", () => {
  const walletPanel = preview.slice(
    preview.indexOf("function walletDrilldownPanelHtml"),
    preview.indexOf("function holderPanelDateText")
  );
  assert.match(walletPanel, /data-holder-details-search="\$\{escHtml\(searchKey\)\}"/);
  assert.match(walletPanel, /holder-wallet-panel-search-row/);
  assert.match(walletPanel, /\$\{meta \? `<div class="holder-wallet-panel-meta">\$\{escHtml\(meta\)\}<\/div>` : ""\}/);
  assert.match(walletPanel, /<th data-column="chain" aria-sort="none"><button class="holder-wallet-sort" type="button" data-holder-wallet-sort="chain"><span>Chain<\/span>/);
  assert.match(walletPanel, /<th data-column="address">Address<\/th>/);
  assert.match(walletPanel, /<td class="chain-cell" data-column="chain" data-label="Chain">\$\{holderWalletChainHtml\(row\)\}<\/td>/);
  assert.match(walletPanel, /<td data-column="address" data-label="Address">\$\{holderWalletAddressCell\(row\)\}<\/td>/);
  assert.doesNotMatch(walletPanel, /<th>#<\/th>/);
  assert.match(preview, /function holderWalletChainHtml\(row\)[\s\S]*return freshChainCell\(\{chains\}\);/);
  assert.match(preview, /function bindHolderWalletPanel\(panel\)[\s\S]*data-holder-details-search/);

  const bucketPanel = preview.slice(
    preview.indexOf("function holderWalletInlinePanelHtml"),
    preview.indexOf("function allocationWalletInlinePanelHtml")
  );
  assert.doesNotMatch(bucketPanel, /current\/prior wallets/);
});

test("empty chain cells use a neutral accessible state", () => {
  const chainRenderer = preview.slice(
    preview.indexOf("function freshChainCell(row)"),
    preview.indexOf("function freshExposureHtml")
  );

  assert.match(chainRenderer, /aria-label="No current chain balance">—<\/span>/);
  assert.doesNotMatch(chainRenderer, /escHtml\(chain\.label \|\| "Unknown"\)/);
});

test("Bucket wallet Details supports sorting by Chain, DOLO, and Change", () => {
  const walletPanel = preview.slice(
    preview.indexOf("function walletDrilldownPanelHtml"),
    preview.indexOf("function holderPanelDateText")
  );
  const walletBinder = preview.slice(
    preview.indexOf("function bindHolderWalletPanel"),
    preview.indexOf("function walletDrilldownPanelHtml")
  );
  assert.match(walletPanel, /data-holder-wallet-sort="chain"/);
  assert.match(walletPanel, /data-holder-wallet-sort="dolo"/);
  assert.match(walletPanel, /data-holder-wallet-sort="change"/);
  assert.match(walletPanel, /data-wallet-sort-chain="\$\{escHtml\(holderWalletChainSortValue\(row\)\)\}"/);
  assert.match(walletPanel, /data-wallet-sort-dolo="\$\{safeHolderNum\(row\.panelTotal\)\}"/);
  assert.match(walletPanel, /data-wallet-sort-change="\$\{safeHolderNum\(row\.rangeChange\?\.delta\)\}"/);
  assert.match(walletBinder, /const initialSortDirection = sortKey === "chain" \? "asc" : "desc";/);
  assert.match(walletBinder, /body\.append\(\.\.\.rows, empty\);/);
  assert.match(walletBinder, /button\.addEventListener\("click", \(\) => sortWalletRows\(button\.dataset\.holderWalletSort\)\)/);
});

test("holder distribution keeps mobile tooltips bounded and hides empty chart paths from keyboard users", () => {
  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );

  assert.match(preview, /\.holder-chart-tip\{[\s\S]*box-sizing:border-box;[\s\S]*max-width:calc\(100% - 16px\);[\s\S]*white-space:normal/);
  assert.match(holderRenderer, /const maxTipLeft = Math\.max\(8, wrap\.clientWidth - tipW - 8\);/);
  assert.match(holderRenderer, /aria-label="Pin \$\{bucket\.label\} DOLO series\. Balance \$\{fmtNum\(bucket\.total\)\} DOLO\. Wallets \$\{bucket\.wallets\.toLocaleString\("en-US"\)\}\. Change \$\{fmtSignedHolder\(delta\)\} DOLO \$\{deltaPct\}\."/);
  assert.match(holderRenderer, /const interactionAttrs = path \? ` role="button" tabindex="0"/);
  assert.match(holderRenderer, /: ` aria-hidden="true" pointer-events="none"`/);
  assert.match(holderRenderer, /lines\.querySelectorAll\("\.holder-chart-series-line\[role='button'\]"\)/);
});

test("historical holder rows prefer the canonical DOLO Holders wallet name", () => {
  const start = preview.indexOf("function canonicalHolderLabel");
  const end = preview.indexOf("function holderHistoricalWalletRowsAtPoint", start);
  assert.notEqual(start, -1, "canonicalHolderLabel helper is required");
  assert.notEqual(end, -1, "historical holder renderer must follow the canonical label helper");
  const helperSource = preview.slice(start, end);
  const canonicalHolderLabel = new Function(
    "sharedAddressInfo",
    `${helperSource}\nreturn canonicalHolderLabel;`
  )(() => ({label: "Canonical DOLO Holders name"}));

  assert.equal(
    canonicalHolderLabel("0x1111111111111111111111111111111111111111", "Stale snapshot name"),
    "Canonical DOLO Holders name"
  );
  assert.match(
    preview.slice(end, preview.indexOf("function holderWalletRowsAvailableAtPoint", end)),
    /label:canonicalHolderLabel\(addr, item\.label \|\| ""\)/
  );
  const availability = preview.slice(
    preview.indexOf("function holderWalletRowsAvailableAtPoint"),
    preview.indexOf("function holderWalletBaselinePointForRange")
  );
  assert.doesNotMatch(availability, /holderBalanceChangesFromPointToNow/);
  assert.match(availability, /return !!holderHistoricalWalletRowsAtPoint/);
  const rowsResolver = preview.slice(
    preview.indexOf("function holderRowsAtPoint"),
    preview.indexOf("function zeroHolderRowFrom")
  );
  assert.match(rowsResolver, /if\(sourcePoint\.key !== "now"\) return \[\];/);
  assert.doesNotMatch(rowsResolver, /holderBalanceChangesFromPointToNow/);
  assert.match(preview, /Loading exact Total exposure wallet details/);
  assert.match(preview, /Exact Total exposure wallet details are unavailable/);
});

test("holder hover marks the line nearest to the pointer", () => {
  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );

  assert.match(holderRenderer, /const py = \(event\.clientY - rect\.top\) \/ rect\.height \* H;/);
  assert.match(holderRenderer, /const hoveredSeries = seriesByBucket\.reduce\(/);
  assert.match(holderRenderer, /const focusIndex = hoveredSeries\?\.index \?\? defaultFocusIndex;/);
  assert.match(holderRenderer, /paintActive\(focusBucket\.key\);/);
  assert.match(holderRenderer, /hoverDot\.setAttribute\("fill", focusBucket\.color\);/);
});

test("holder and CEX charts use the card-meta status treatment and clipped CEX footer", () => {
  assert.match(preview, /<div class="card-head holder-distribution-head">/);
  assert.match(preview, /<div class="holder-distribution-title-row">/);
  assert.match(preview, /<div class="holder-distribution-toolbar">/);
  assert.match(preview, /<div class="card-meta holder-chart-meta" id="holder-chart-meta"/);
  assert.match(preview, /<div class="card-meta holder-chart-meta" id="cex-supply-meta"/);
  assert.match(preview, /\.holder-distribution-head\{[^}]*display:block;[^}]*border-bottom:0/);
  assert.match(preview, /\.holder-distribution-title-row\{[^}]*border-bottom:1px solid var\(--line-1\)/);
  assert.match(preview, /\.cex-supply-card \.holder-flow-stats\{[^}]*border-radius:0 0 var\(--r-xl\) var\(--r-xl\);[^}]*overflow:hidden/);
  assert.match(preview, /\.cex-supply-card \.holder-flow-stat\{border-bottom:0\}/);

  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );
  const cexRenderer = preview.slice(
    preview.indexOf("function renderCexSupplyChart(options = {})"),
    preview.indexOf("const COPY_ICO")
  );
  assert.match(holderRenderer, /metaEl\.innerHTML = `<span class="pulse"><\/span>\$\{holderScopeHtml\(\)\}`;/);
  assert.match(cexRenderer, /metaEl\.innerHTML = `<span class="pulse"><\/span><span>\$\{fullModel\.sourceLabel/);
});

test("holder distribution places scope above the divider and veDOLO at the toolbar edge", () => {
  const holderCard = preview.slice(
    preview.indexOf('<section class="card holder-chart-card" id="holder-distribution-card">'),
    preview.indexOf('<div class="holder-chart-wrap" id="holderChartWrap">')
  );

  assert.match(
    holderCard,
    /<div class="holder-distribution-title-row">[\s\S]*id="holder-chart-meta"[\s\S]*<\/div>\s*<div class="holder-distribution-toolbar">/,
  );
  assert.match(
    holderCard,
    /<div class="holder-chart-controls">[\s\S]*?<\/div>\s*<label class="holder-chart-toggle" for="holder-include-vedolo"[^>]*>/,
  );
  assert.match(preview, /\.holder-distribution-toolbar > \.holder-chart-toggle\{margin-left:auto}/);
});

test("holder distribution centers Details and presents change amount above its percentage", () => {
  const legendCss = preview.slice(
    preview.indexOf(".holder-distribution-legend{--holder-layout-columns"),
    preview.indexOf(".holder-wallet-panel{")
  );
  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );

  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-details\{[^}]*display:grid;[^}]*place-items:center;/);
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-details \.holder-details-btn\{[^}]*justify-self:center;/);
  assert.match(holderRenderer, /const deltaPct = firstTotal > 0 \? `\(\$\{fmtSignedHolderPct\(delta \/ firstTotal \* 100\)\}\)` : "New";/);
  assert.match(holderRenderer, /<strong class="holder-legend-number">\$\{fmtSignedHolder\(delta\)\}<\/strong><span class="holder-legend-percent">\$\{deltaPct\}<\/span>/);
  assert.match(preview, /\.holder-distribution-legend \.holder-legend-percent\{[^}]*font-size:10px;[^}]*line-height:1;/);
});

test("holder distribution hides its desktop header on mobile", () => {
  assert.match(
    preview,
    /@media \(max-width:640px\)\{[\s\S]*?\.holder-distribution-legend \.holder-legend-head\{display:none}/,
  );
});

test("holder distribution exposes the visible change percentage to assistive technology", () => {
  const holderRenderer = preview.slice(
    preview.indexOf("function renderHolderDistributionChart(options = {})"),
    preview.indexOf("function allocationPointFromSource")
  );

  assert.match(holderRenderer, /Change \$\{fmtSignedHolder\(delta\)\} DOLO \$\{deltaPct\}\."/);
  assert.match(holderRenderer, /series by change\. \$\{fmtSignedHolder\(delta\)\} DOLO \$\{deltaPct\}\."/);
});

test("holder mini-chart handles keep a wide resize cursor hitbox", () => {
  const brushCss = preview.slice(
    preview.indexOf(".holder-brush-wrap{"),
    preview.indexOf(".holder-brush-label{")
  );

  assert.match(brushCss, /\.holder-brush-handle\{[^}]*width:24px;[^}]*cursor:ew-resize;[^}]*z-index:2;/);
  assert.match(brushCss, /\.holder-brush-handle\.l\{left:-12px;/);
  assert.match(brushCss, /\.holder-brush-handle\.r\{right:-12px;/);
  assert.match(preview, /id="holderBrushHandleL"/);
  assert.match(preview, /id="cexSupplyBrushHandleL"/);
});

test("holder distribution centers the Details header with the button column", () => {
  assert.match(
    preview,
    /\.holder-distribution-legend \.holder-legend-head > \.holder-legend-head-action\[data-column="details"\]\{display:grid;place-items:center;text-align:center}/,
  );
});
