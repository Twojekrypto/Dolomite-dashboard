const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const preview = fs.readFileSync(path.join(__dirname, "..", "dolo-preview.html"), "utf8");
const freshSection = preview.slice(preview.indexOf('<table class="tbl fresh-wallets-table"'), preview.indexOf("<!-- HOLDER DISTRIBUTION CHART -->"));
const freshRenderer = preview.slice(preview.indexOf("function freshWalletStatus(row)"), preview.indexOf('\ndocument.getElementById("q-holders")'));
const freshStatusSource = preview.slice(preview.indexOf("const fmtPct = v =>"), preview.indexOf("const HOLDER_BUCKET_GROUPS")) + preview.slice(preview.indexOf("function freshWalletStatus(row)"), preview.indexOf("function freshChainCell(row)"));
const freshWalletStatus = new Function(`${freshStatusSource}\nreturn freshWalletStatus;`)();

test("fresh wallet table uses a Chain-first layout without a rank column", () => {
  assert.match(freshSection, /<col data-column="chain"/);
  assert.match(freshSection, /<th data-column="chain">Chain<\/th>/);
  assert.doesNotMatch(freshSection, /<th[^>]*>#<\/th>/);
  assert.doesNotMatch(freshRenderer, /<td class="rank">/);
});

test("fresh wallet status contains retention only and Chain is rendered as its own cell", () => {
  assert.match(freshRenderer, /function freshChainCell\(row\)/);
  assert.match(freshRenderer, /<td data-column="chain">\$\{freshChainCell\(row\)\}<\/td>/);
  assert.match(freshRenderer, /<td data-column="status">[\s\S]*fresh-status \$\{status\.tone\}[\s\S]*\$\{status\.value\}[\s\S]*\$\{status\.detail\}[\s\S]*<\/td>/);
});

test("fresh wallet status shows exact retention rather than a threshold label", () => {
  assert.deepEqual(
    freshWalletStatus({ received: 100, balance: 100, lockedBalance: 0 }),
    { main: "Fully retained", value: "100%", detail: "retained", tone: "up" }
  );
  assert.deepEqual(
    freshWalletStatus({ received: 100, balance: 97, lockedBalance: 0 }),
    { main: "Holding", value: "97%", detail: "retained", tone: "up" }
  );
  assert.deepEqual(
    freshWalletStatus({ received: 100, balance: 125, lockedBalance: 0 }),
    { main: "Increased", value: "125%", detail: "vs received", tone: "up" }
  );
});

test("production preview keeps the saved static table layout without editor assets", () => {
  assert.match(freshSection, /<col data-column="chain" style="width:11\.5%">/);
  assert.match(freshSection, /<col data-column="address" style="width:24%">/);
  assert.doesNotMatch(preview, /fresh-wallets-layout-editor/);
});
