const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildPortfolioLiquidityModel,
  compactRawAmount,
  createPortfolioLiquidityController,
  roundedRawAmount,
} = require("../portfolio-dolo-liquidity.js");

const TARGET = "0x1111111111111111111111111111111111111111";
const OTHER = "0x2222222222222222222222222222222222222222";

function liquidityFixture(){
  return {
    generatedAt: "2026-08-31T08:00:00Z",
    pools: [
      { identifier: "pool-a", pair: "DOLO/USD1", pairedSymbol: "USD1", pairedDecimals: 18, liquidityUsd: 250000 },
      { identifier: "pool-small", pair: "DOLO/USDC", pairedSymbol: "USDC", pairedDecimals: 6, liquidityUsd: 9000 },
    ],
    activePositions: [
      { id: "owned", poolId: "pool-a", beneficialOwner: TARGET.toUpperCase(), custodian: OTHER, valueUsd: 120, doloRaw: "1500000000000000000000", pairedRaw: "80000000000000000000" },
      { id: "custody-only", poolId: "pool-a", beneficialOwner: null, custodian: TARGET, valueUsd: 80, doloRaw: "500000000000000000000", pairedRaw: "20000000000000000000" },
      { id: "other-owner", poolId: "pool-a", beneficialOwner: OTHER, custodian: TARGET, valueUsd: 999, doloRaw: "1", pairedRaw: "1" },
      { id: "small-pool", poolId: "pool-small", beneficialOwner: TARGET, custodian: TARGET, valueUsd: 15, doloRaw: "1", pairedRaw: "1" },
    ],
  };
}

test("Portfolio liquidity shows only attributable active positions from eligible pools", () => {
  const model = buildPortfolioLiquidityModel(liquidityFixture(), TARGET);

  assert.equal(model.visible, true);
  assert.deepEqual(model.positions.map(position => position.id), ["owned", "custody-only"]);
  assert.equal(model.totalValueUsd, 200);
  assert.equal(model.generatedAt, "2026-08-31T08:00:00Z");
  assert.equal(model.positions[0].pool.pair, "DOLO/USD1");
});

test("Portfolio liquidity stays hidden when the wallet has no attributable position", () => {
  const model = buildPortfolioLiquidityModel(liquidityFixture(), "0x3333333333333333333333333333333333333333");

  assert.equal(model.visible, false);
  assert.deepEqual(model.positions, []);
  assert.equal(model.totalValueUsd, 0);
});

test("Portfolio liquidity controller reveals the section only for a wallet with positions", async () => {
  const section = { hidden: false };
  const rendered = [];
  const controller = createPortfolioLiquidityController({
    section,
    loadData: async () => liquidityFixture(),
    render: model => rendered.push(model.positions.map(position => position.id)),
    clear: () => rendered.push([]),
  });

  await controller.showWallet(TARGET);
  assert.equal(section.hidden, false);
  assert.deepEqual(rendered.at(-1), ["owned", "custody-only"]);

  await controller.showWallet("0x3333333333333333333333333333333333333333");
  assert.equal(section.hidden, true);
  assert.deepEqual(rendered.at(-1), []);
});

test("Portfolio liquidity formats current token amounts without unsafe floating point conversion", () => {
  assert.equal(compactRawAmount("16387908718238867098667283", 18), "16.4M");
  assert.equal(roundedRawAmount("16387908718238867098667283", 18, 2), "16,387,908.72");
  assert.equal(compactRawAmount("0", 18), "0");
});
