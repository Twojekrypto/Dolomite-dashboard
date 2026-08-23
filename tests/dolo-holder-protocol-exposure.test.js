const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const preview = fs.readFileSync(path.join(__dirname, "..", "dolo-preview.html"), "utf8");

function loadExposureBuilder(){
  const start = preview.indexOf("function buildHolderExposureRows(");
  const end = preview.indexOf("\nfunction holdersView(", start);
  assert.ok(start >= 0 && end > start, "holder exposure builder should be present");
  const source = preview.slice(start, end);
  return new Function(
    "safeHolderNum",
    "VEDOLO_CONTRACT_ADDRESS",
    `${source}\nreturn buildHolderExposureRows;`,
  )(
    value => Number.isFinite(Number(value)) ? Number(value) : 0,
    "0xcb86b75ee6133d179a12d550b09fb3cdb1e141d4",
  );
}

function loadHolderAmountFormatter(){
  const start = preview.indexOf("function fmtHolderAmount(");
  const end = preview.indexOf("\nconst fmtPct", start);
  assert.ok(start >= 0 && end > start, "holder amount formatter should be present");
  const source = preview.slice(start, end);
  return new Function(`${source}\nreturn fmtHolderAmount;`)();
}

test("holder exposure attributes Dolomite positions to owners and removes custody double counting", () => {
  const buildRows = loadExposureBuilder();
  const custody = "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d";
  const alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const bob = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  const carol = "0xcccccccccccccccccccccccccccccccccccccccc";
  const resolveRow = address => ({addr:address, label:"", type:"eoa", balEth:0, balBera:0});

  const rows = buildRows(
    [
      {addr:alice, label:"Alice", type:"eoa", balEth:100, balBera:0, total:100},
      {addr:custody, label:"DolomiteMargin", type:"protocol", balEth:500, balBera:0, total:500},
    ],
    {
      [alice]: {eth:500, bera:0, total:500},
      [bob]: {eth:0, bera:250, total:250},
    },
    new Set([custody]),
    new Map([[alice, 25], [carol, 50]]),
    true,
    resolveRow,
  );

  const byAddress = new Map(rows.map(row => [row.addr, row]));
  assert.equal(byAddress.has(custody), false);
  assert.equal(byAddress.get(alice).inDolomite, 500);
  assert.equal(byAddress.get(alice).total, 625);
  assert.equal(byAddress.get(bob).inDolomiteBera, 250);
  assert.equal(byAddress.get(bob).total, 250);
  assert.equal(byAddress.get(carol).locked, 50);
  assert.equal(byAddress.get(carol).total, 50);
});

test("disabling veDOLO removes only locked exposure, not Dolomite positions", () => {
  const buildRows = loadExposureBuilder();
  const alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const veOnly = "0xcccccccccccccccccccccccccccccccccccccccc";

  const rows = buildRows(
    [{addr:alice, label:"Alice", type:"eoa", balEth:100, balBera:0, total:100}],
    {[alice]: {eth:500, bera:0, total:500}},
    new Set(),
    new Map([[alice, 25], [veOnly, 50]]),
    false,
    address => ({addr:address, label:"", type:"eoa", balEth:0, balBera:0}),
  );

  assert.deepEqual(rows.map(row => row.addr), [alice]);
  assert.equal(rows[0].locked, 0);
  assert.equal(rows[0].total, 600);
});

test("holder protocol balances promote rounded units and omit redundant zeroes", () => {
  const format = loadHolderAmountFormatter();

  assert.equal(format(1_000), "1K");
  assert.equal(format(1_500), "1.5K");
  assert.equal(format(999_950), "1M");
  assert.equal(format(1_000_000), "1M");
  assert.equal(format(1_250_000), "1.25M");
});

test("DOLO Holders ends with compact Total and does not render Exposure Share", () => {
  const tableStart = preview.indexOf('<table class="tbl" id="tbl-holders">');
  const tableEnd = preview.indexOf("</table>", tableStart);
  assert.ok(tableStart >= 0 && tableEnd > tableStart, "DOLO Holders table should be present");

  const table = preview.slice(tableStart, tableEnd);
  const header = table.slice(table.indexOf("<thead>"), table.indexOf("</thead>"));
  const headers = [...header.matchAll(/<th\b[\s\S]*?<\/th>/g)]
    .map(match => match[0].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim());

  assert.deepEqual(headers, ["#", "Address", "ETH", "BERA", "In Dolomite", "veDOLO", "Total ▼"]);
  assert.doesNotMatch(table, /Exposure Share|share-cell|share-val|share-bar/);
});
