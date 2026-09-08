const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const preview = fs.readFileSync(path.join(__dirname, "..", "dolo-preview.html"), "utf8");
const start = preview.indexOf("function flowBalanceCellHtml(row)");
const end = preview.indexOf("\nfunction effectiveFlowTx", start);

function loadRenderer(formatter = value => Number(value).toLocaleString("en-US", { maximumFractionDigits: 2 })){
  assert.ok(start >= 0 && end > start, "flow balance renderer should be present");
  const source = preview.slice(start, end);
  return new Function(
    "safeHolderNum",
    "fmtNum",
    `${source}\nreturn flowBalanceCellHtml;`,
  )(
    value => Number.isFinite(Number(value)) ? Number(value) : 0,
    formatter,
  );
}

test("wallet dust does not hide a material protocol balance behind a rounded zero", () => {
  const fmtStart = preview.indexOf("const fmtNum =");
  const fmtEnd = preview.indexOf("\n};", fmtStart) + 3;
  const formatter = new Function(`${preview.slice(fmtStart, fmtEnd)}; return fmtNum;`)();
  const render = loadRenderer(formatter);
  for(const balance of [0, 0.27, 0.49]){
    const html = render({ balance, dolomiteBalance: 1645166.005906 });
    assert.match(html, /flow-dolomite-primary">1\.65M</);
    assert.match(html, new RegExp(`Current wallet balance: ${balance} DOLO`));
    assert.doesNotMatch(html, /flow-balance-main">0</);
  }
  for(const balance of [0.5, 1, 100000]){
    assert.match(render({ balance, dolomiteBalance: 500000 }), /flow-balance-main/);
  }
  assert.match(render({ balance: 0.27, dolomiteBalance: 0 }), />0\.27</);
  assert.match(render({ balance: 0, dolomiteBalance: 0.01 }), />0\.01</);
});

test("a protocol-only DOLO balance replaces the misleading empty dash", () => {
  const render = loadRenderer();
  const html = render({ balance: 0, dolomiteBalance: 167945.37 });

  assert.match(html, /167,945\.37/);
  assert.match(html, /in Dolomite/);
  assert.match(html, /flow-dolomite-primary/);
  assert.doesNotMatch(html, />—</);
});

test("liquid and Dolomite balances stay visually separated", () => {
  const render = loadRenderer();
  const html = render({ balance: 25000, dolomiteBalance: 150000 });

  assert.match(html, />25,000</);
  assert.match(html, /\+150,000 in Dolomite/);
  assert.match(html, /Current wallet balance: 25,000 DOLO/);
  assert.match(html, /Current DOLO in Dolomite: 150,000 DOLO/);
});

test("an address with no current DOLO exposure keeps the muted dash", () => {
  const render = loadRenderer();
  const html = render({ balance: 0, dolomiteBalance: 0 });

  assert.match(html, /bal-zero/);
  assert.match(html, />—</);
  assert.doesNotMatch(html, /in Dolomite/);
});

test("current Dolomite balances are loaded once from the root address map", () => {
  assert.match(preview, /DOLOMITE_FLOW_BALANCES\s*=\s*flows\.dolomite_balances\s*\|\|\s*\{\}/);
  assert.match(preview, /DOLOMITE_FLOW_BALANCES\[key\]/);
  assert.doesNotMatch(preview, /item\.dolomite_balance/);
});
