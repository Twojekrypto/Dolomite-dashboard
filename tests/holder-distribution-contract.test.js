import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const preview = fs.readFileSync("dolo-preview.html", "utf8");

test("holder distribution exposes accessible metric controls", () => {
  assert.match(preview, /id="holder-bucket-mode"/);
  assert.match(preview, /data-holder-bucket-view="whales" aria-pressed="true"/);
  assert.match(preview, /data-holder-bucket-view="smaller" aria-pressed="false"/);
  assert.match(preview, /id="holder-metric-mode"/);
  assert.match(preview, /data-holder-metric="balance"/);
  assert.match(preview, /data-holder-metric="changePct"/);
  assert.match(preview, /aria-pressed="true"/);
});

test("holder distribution explains scope and keeps the Change header concise", () => {
  assert.match(preview, /holder-source-exclusion/);
  assert.match(preview, /<span data-column="change">Change<\/span>/);
});

test("holder distribution excludes potential CEX/MM and bots from the chart", () => {
  const scopeRenderer = preview.slice(preview.indexOf("function holderScopeHtml"), preview.indexOf("function holderCexStatHtml"));
  assert.match(scopeRenderer, /CEX &amp; allocations excluded/);
  assert.match(scopeRenderer, /potential CEX\/MM or bot wallets/);
});

test("holder bucket controls share the metric UX without an active gold dot", () => {
  const controlsCss = preview.slice(
    preview.indexOf(".holder-bucket-mode,.holder-metric-mode{"),
    preview.indexOf(".holder-chart-toggle.is-active{")
  );
  assert.match(controlsCss, /\.holder-bucket-mode button\.active,\s*\.holder-bucket-mode button:hover,\s*\.holder-bucket-mode button:focus-visible,/);
  assert.doesNotMatch(controlsCss, /\.holder-bucket-mode button\.active::before/);
  assert.match(preview, /document\.querySelectorAll\("\[data-holder-bucket-view\]"\)\.forEach\(item => \{\s*const active = item\.dataset\.holderBucketView === holderBucketView;\s*item\.classList\.toggle\("active", active\);\s*item\.setAttribute\("aria-pressed", String\(active\)\);/);
});

test("holder distribution clips its final row to the card's rounded lower corners", () => {
  assert.match(preview, /\.holder-chart-card > \.holder-chart-legend:last-child\{border-radius:0 0 var\(--r-xl\) var\(--r-xl\);overflow:hidden}/);
});

test("holder distribution fixes the visible chart audience to market wallets", () => {
  assert.doesNotMatch(preview, /holder-audience-mode/);
  assert.doesNotMatch(preview, /data-holder-audience/);
  assert.match(preview, /const holderAudience = "market"/);
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
  assert.match(legendCss, /\.holder-distribution-legend \.holder-legend-details\{[^}]*justify-content:center;[^}]*padding:0 12px;/);
  assert.match(legendCss, /holder-legend-details \.holder-details-btn\{margin:0;max-width:72px}/);
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
  assert.match(holderRenderer, /aria-label="Pin \$\{bucket\.label\} DOLO series\. Balance \$\{fmtNum\(bucket\.total\)\} DOLO\. Wallets \$\{bucket\.wallets\.toLocaleString\(\)\}\. Change \$\{fmtSignedHolder\(delta\)\} DOLO\."/);
  assert.match(holderRenderer, /const interactionAttrs = path \? ` role="button" tabindex="0"/);
  assert.match(holderRenderer, /: ` aria-hidden="true" pointer-events="none"`/);
  assert.match(holderRenderer, /lines\.querySelectorAll\("\.holder-chart-series-line\[role='button'\]"\)/);
});
