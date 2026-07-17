import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const preview = fs.readFileSync("dolo-preview.html", "utf8");

test("holder distribution exposes accessible metric controls", () => {
  assert.match(preview, /id="holder-metric-mode"/);
  assert.match(preview, /data-holder-metric="balance"/);
  assert.match(preview, /data-holder-metric="changePct"/);
  assert.match(preview, /aria-pressed="true"/);
});

test("holder distribution explains scope and dynamic comparison period", () => {
  assert.match(preview, /holder-source-exclusion/);
  assert.match(preview, /holder-legend-change-head/);
});

test("holder distribution excludes potential CEX/MM and bots from the chart", () => {
  const scopeRenderer = preview.slice(preview.indexOf("function holderScopeHtml"), preview.indexOf("function holderCexStatHtml"));
  assert.match(scopeRenderer, /CEX, potential &amp; allocations excluded/);
  assert.match(scopeRenderer, /potential CEX\/MM or bot wallets/);
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

test("holder distribution exposes a guarded change tooltip and updates the legend range", () => {
  assert.match(preview, /New \/ no baseline/);
  assert.match(preview, /holderDistributionMetric === "changePct" \? `<span class="tt-change \$\{deltaClass\}">\$\{changeText\}<\/span> · \$\{fmtNum\(bucket\.total\)\} DOLO · \$\{fmtSignedHolder\(delta\)\} DOLO`/);
  assert.match(preview, /legendChangeHead\.textContent = `Change · \$\{holderRangeLabel\}`/);
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
  assert.match(holderLegendMarkup, /<button class="holder-legend-pin" type="button" data-pin-key="\$\{bucket\.key\}" aria-pressed="\$\{bucket\.key === holderDistributionActiveKey\}"/);
  assert.match(holderLegendMarkup, /<\/button>\s*<button class="holder-details-btn"/);
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
  assert.match(preview, /\.holder-legend-head-action\{width:72px;text-align:center\}/);
  assert.match(preview, /\.holder-details-btn\{\s*height:24px;width:100%;max-width:72px;min-width:0;padding:0 6px;[^}]*gap:3px;[^}]*overflow:hidden;/);
  assert.match(preview, /\.holder-details-btn span\{font-size:9px;font-weight:700;letter-spacing:\.5px;[^}]*overflow:hidden;text-overflow:ellipsis;white-space:nowrap}/);

  const distributionRenderer = preview.slice(preview.indexOf("function renderHolderDistributionChart"), preview.indexOf("function renderAllocationChart"));
  const allocationRenderer = preview.slice(preview.indexOf("function renderAllocationChart"), preview.indexOf("function renderCexSupplyChart"));
  assert.match(distributionRenderer, /<span class="holder-legend-head-action">Details<\/span>/);
  assert.match(allocationRenderer, /<span class="holder-legend-head-action">Details<\/span>/);
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
