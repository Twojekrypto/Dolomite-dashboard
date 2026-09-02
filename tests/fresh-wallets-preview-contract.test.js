const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const preview = fs.readFileSync(path.join(__dirname, "..", "dolo-preview.html"), "utf8");
const freshSection = preview.slice(preview.indexOf('<table class="tbl fresh-wallets-table"'), preview.indexOf("<!-- HOLDER DISTRIBUTION CHART -->"));
const freshRenderer = preview.slice(preview.indexOf("function freshWalletStatus(row)"), preview.indexOf('\ndocument.getElementById("q-holders")'));
const freshStatusSource = preview.slice(preview.indexOf("const fmtPct = v =>"), preview.indexOf("const HOLDER_BUCKET_GROUPS")) + preview.slice(preview.indexOf("function freshWalletStatus(row)"), preview.indexOf("function freshChainCell(row)"));
const freshWalletStatus = new Function(`${freshStatusSource}\nreturn freshWalletStatus;`)();
const freshPageModelSource = preview.match(/function freshPageModel\(rows, page, pageSize\)\{[\s\S]*?\n\}/)?.[0];
const freshBackendRowsSource = preview.match(/function buildFreshWalletRowsFromBackend\(period\)\{[\s\S]*?\n\}/)?.[0];

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
  assert.match(freshSection, /<col data-column="chain" style="width:9\.361%">/);
  assert.match(freshSection, /<col data-column="address" style="width:14\.253%">/);
  assert.match(freshSection, /<col data-column="created" style="width:9\.593%">/);
  assert.match(freshSection, /<col data-column="signal" style="width:24\.916%">/);
  assert.match(freshSection, /<col data-column="received" style="width:19\.26%">/);
  assert.match(freshSection, /<col data-column="exposure" style="width:10\.117%">/);
  assert.match(freshSection, /<col data-column="status" style="width:12\.5%">/);
  assert.doesNotMatch(preview, /fresh-wallets-layout-editor/);
});

test("fresh wallet pagination renders up to ten real rows without artificial spacer rows", () => {
  assert.match(preview, /freshPageSize:\s*10,/);
  assert.doesNotMatch(freshRenderer, /fresh-spacer-row/);
  assert.doesNotMatch(freshRenderer, /syncFlowSpacerHeight\(tbody,\s*"fresh"/);
  assert.ok(freshPageModelSource, "pure Fresh Wallet pagination model is required");
  const freshPageModel = new Function(`${freshPageModelSource}\nreturn freshPageModel;`)();
  const twoRows = freshPageModel(["a", "b"], 0, 10);
  const firstTwelve = freshPageModel(Array.from({length: 12}, (_, i) => i), 0, 10);
  const lastTwelve = freshPageModel(Array.from({length: 12}, (_, i) => i), 1, 10);
  assert.deepEqual(twoRows, {page: 0, totalPages: 1, start: 0, rows: ["a", "b"]});
  assert.deepEqual(firstTwelve, {page: 0, totalPages: 2, start: 0, rows: [0,1,2,3,4,5,6,7,8,9]});
  assert.deepEqual(lastTwelve, {page: 1, totalPages: 2, start: 10, rows: [10,11]});
});

test("fresh wallet backend renderer keeps only user-controlled wallet types", () => {
  assert.ok(freshBackendRowsSource, "Fresh Wallet backend row builder is required");
  const buildRows = new Function(
    "DOLO_FRESH_HOLDERS",
    "sharedAddressInfo",
    "holderDistributionType",
    "parseHolderTimestamp",
    "safeHolderNum",
    "freshWalletChainParts",
    "isSafeWallet",
    "freshWalletSignal",
    "FRESH_HOLDER_MIN_BALANCE",
    `${freshBackendRowsSource}\nreturn buildFreshWalletRowsFromBackend;`
  )(
    {
      "90d": [
        {address:"0x1111111111111111111111111111111111111111", type:"eoa", balance:20_000, received:20_000, wallet_created_timestamp:"2026-08-20T00:00:00Z"},
        {address:"0x2222222222222222222222222222222222222222", type:"multisig", balance:20_000, received:20_000, wallet_created_timestamp:"2026-08-20T00:00:00Z"},
        {address:"0x3333333333333333333333333333333333333333", type:"bot", balance:20_000, received:20_000, wallet_created_timestamp:"2026-08-20T00:00:00Z"},
        {address:"0x4444444444444444444444444444444444444444", type:"mm", balance:20_000, received:20_000, wallet_created_timestamp:"2026-08-20T00:00:00Z"},
        {address:"0x5555555555555555555555555555555555555555", type:"investor", balance:20_000, received:20_000, wallet_created_timestamp:"2026-08-20T00:00:00Z"},
      ],
    },
    () => null,
    (_address, row) => row.type,
    value => Math.floor(Date.parse(value) / 1000),
    value => Number(value) || 0,
    () => [],
    row => row.type === "multisig",
    () => ({label:"Received", cls:""}),
    10_000
  );

  assert.deepEqual(buildRows("90d").map(row => row.type), ["eoa", "multisig"]);
});
