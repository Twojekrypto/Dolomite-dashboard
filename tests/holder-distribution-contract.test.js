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

test("holder distribution supports pinned series from chart and legend", () => {
  assert.match(preview, /function toggleHolderDistributionPin\(key\)/);
  assert.match(preview, /role="button" tabindex="0"/);
  assert.match(preview, /aria-pressed=/);
  assert.match(preview, /addEventListener\("keydown"/);
});
