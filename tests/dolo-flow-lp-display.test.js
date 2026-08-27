const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const preview = fs.readFileSync(path.join(__dirname, "..", "dolo-preview.html"), "utf8");

function loadLpBadgeBuilder(){
  const start = preview.indexOf("function flowLpBadgeHtml(");
  const end = preview.indexOf("\nfunction effectiveFlowTx(", start);
  assert.ok(start >= 0 && end > start, "LP flow badge builder should be present");
  const source = preview.slice(start, end);
  return new Function(
    "safeHolderNum",
    "fmtNum",
    "escHtml",
    `${source}\nreturn flowLpBadgeHtml;`,
  )(
    value => Number.isFinite(Number(value)) ? Number(value) : 0,
    value => Math.abs(Number(value) - 2_490_986.194008284) < 1
      ? "2.49M"
      : (Number(value) >= 1_300_000 ? "1.3M"
      : (Math.abs(Number(value)) < 1 ? Math.abs(Number(value)).toFixed(2) : "850K")),
    value => String(value)
      .replaceAll("&", "&amp;")
      .replaceAll('"', "&quot;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;"),
  );
}

test("verified LP deposit is shown as a secondary component without replacing net outflow", () => {
  const badge = loadLpBadgeBuilder()({
    direction: "deposit",
    amount: "1304943.547531365190891539",
    pair: "DOLO/USDC",
    adapter: "uniswap-v4",
    confidence: "verified_same_tx",
  }, "out");

  assert.match(badge, /1\.3M → LP/);
  assert.match(badge, /Verified LP deposit/);
  assert.match(badge, /Uniswap v4/);
  assert.match(badge, /DOLO\/USDC/);
  assert.match(badge, /not a sale/);
});

test("verified LP withdrawal is shown only for accumulator rows", () => {
  const activity = {
    direction: "withdrawal",
    amount: "850000",
    pair: "DOLO/WBERA",
    adapter: "kodiak-v3",
    confidence: "verified_same_tx",
  };

  const badge = loadLpBadgeBuilder();
  assert.match(badge(activity, "acc"), /850K ← LP/);
  assert.match(badge(activity, "acc"), /Verified LP withdrawal/);
  assert.equal(badge(activity, "out"), "");
});

test("period LP classification discloses the ordinary wallet transfer net", () => {
  const badge = loadLpBadgeBuilder()({
    direction: "deposit",
    amount: "1304943.547531365190891539",
    pair: "DOLO/USDC",
    adapter: "uniswap-v4",
    confidence: "verified_same_tx",
    period_wallet_net_flow: 0.31,
  }, "out");

  assert.match(badge, /Wallet transfer net: \+0\.31 DOLO/);
  assert.match(badge, /Main row remains the transfer-derived wallet net flow/);
});

test("period LP badge uses aggregate net deposit instead of latest transaction amount", () => {
  const badge = loadLpBadgeBuilder()({
    direction: "deposit",
    amount: "1304943.547531365190891539",
    period_lp_deposit: "3795930.051008284105982345",
    period_lp_withdrawal: "1304943.856999999999991187",
    period_net_lp_deposit: "2490986.194008284105991158",
    pair: "DOLO/USDC",
    adapter: "uniswap-v4",
    confidence: "verified_same_tx",
  }, "out");

  assert.match(badge, /2\.49M net → LP/);
  assert.match(badge, /Net added to LP during the selected period: 2\.49M DOLO/);
  assert.match(badge, /Latest verified LP deposit: 1\.3M DOLO/);
});

test("near-flat period LP churn renders a neutral rebalance badge", () => {
  const badge = loadLpBadgeBuilder()({
    direction: "deposit",
    amount: "1304943.547531365190891539",
    period_lp_deposit: "1304943.547531365190891539",
    period_lp_withdrawal: "1304943.856999999999991187",
    period_lp_rebalance: "1304943.547531365190891539",
    period_net_lp_withdrawal: "0.309468634809099648",
    pair: "DOLO/USDC",
    adapter: "uniswap-v4",
    confidence: "verified_same_tx",
  }, "acc");

  assert.match(badge, /LP rebalance · 1\.3M/);
  assert.match(badge, /1\.3M DOLO withdrawn from LP/);
  assert.match(badge, /1\.3M DOLO redeposited into LP/);
  assert.match(badge, /Net wallet change from LP activity: \+0\.31 DOLO/);
});

test("all-time LP net remains visible beside a small accumulator net", () => {
  const badge = loadLpBadgeBuilder()({
    direction: "deposit",
    amount: "1304943.547531365190891539",
    period_lp_deposit: "3795930.051008284105982345",
    period_lp_withdrawal: "1304943.856999999999991187",
    period_net_lp_deposit: "2490986.194008284105991158",
    pair: "DOLO/USDC",
    adapter: "uniswap-v4",
    confidence: "verified_same_tx",
  }, "acc");

  assert.match(badge, /2\.49M net → LP/);
});
