import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const preview = fs.readFileSync("dolo-preview.html", "utf8");

test("custody inference evaluates one observed chain-period-role row at a time", () => {
  const start = preview.indexOf("function isPotentialCustodyObservation");
  const end = preview.indexOf("function learnAutoCexLabels", start);
  assert.notEqual(start, -1, "isPotentialCustodyObservation helper is required");
  assert.notEqual(end, -1, "custody learner must follow the observation helper");
  const helperSource = preview.slice(start, end);
  const isPotentialCustodyObservation = new Function(
    "safeNum",
    `${helperSource}\nreturn isPotentialCustodyObservation;`
  )(value => Number(value) || 0);

  // 0xc0bb… has these signals in different observations. Neither observation
  // independently proves custody, so they must never be merged into one match.
  assert.equal(isPotentialCustodyObservation({tx_count: 4, balance: 817036, net_flow: 721189}, "acc"), false);
  assert.equal(isPotentialCustodyObservation({tx_count: 296, balance: 817036, net_flow: 12650000}, "out"), false);

  // Preserve the existing high-confidence active accumulator threshold.
  assert.equal(isPotentialCustodyObservation({tx_count: 120, balance: 750000, net_flow: 180000}, "acc"), true);

  const learner = preview.slice(end, preview.indexOf("function syncPriceMeta", end));
  assert.match(learner, /if\(!isPotentialCustodyObservation\(item, role\)\) return;/);
  assert.doesNotMatch(learner, /const signals = \{\}/);
});

test("reviewed market wallets bypass custody labels while unrelated qualifiers remain detected", () => {
  const start = preview.indexOf("function isPotentialCustodyObservation");
  const end = preview.indexOf("function learnAutoCexLabels", start);
  const learnerEnd = preview.indexOf("function syncPriceMeta", end);
  const overrideStart = preview.indexOf("const REVIEWED_MARKET_WALLET_OVERRIDES = new Set([");
  const overrideEnd = preview.indexOf("]);", overrideStart);
  assert.notEqual(overrideStart, -1, "production reviewed-wallet override set is required");
  assert.notEqual(overrideEnd, -1, "production reviewed-wallet override set must be complete");
  const overrideSource = preview.slice(overrideStart, overrideEnd + 3);
  const actualOverrides = new Function(
    `${overrideSource}\nreturn [...REVIEWED_MARKET_WALLET_OVERRIDES];`
  )();
  const expectedOverrides = [
    "0x74d4138b742560802d8e10325f72d1b4e0b20882",
    "0x39c4210ed2286d56e979e8ad1fa64c12c2713904",
    "0x8c39b7cb82b9d791adcc9387098944b635c17b32",
    "0x32cd32b45277bf025c1df7bb4388e6a412b77fe5",
    "0x97d59a4cd39fec0756e067979b17207212ecd999",
  ];
  assert.equal(actualOverrides.length, 5);
  assert.equal(new Set(actualOverrides).size, 5);
  assert.deepEqual([...actualOverrides].sort(), [...expectedOverrides].sort());
  const helperSource = preview.slice(start, end);
  const learnerSource = preview.slice(end, learnerEnd);
  const isPotentialCustodyObservation = new Function(
    "safeNum",
    `${helperSource}\nreturn isPotentialCustodyObservation;`
  )(value => Number(value) || 0);
  const autoLabels = {};
  const learnAutoCexLabels = new Function(
    "ADDR_LABELS",
    "AUTO_CEX_LABELS",
    "REVIEWED_MARKET_WALLET_OVERRIDES",
    "lower",
    "isDelegatedEoa",
    "isPotentialCustodyObservation",
    `${learnerSource}\nreturn learnAutoCexLabels;`
  )({}, autoLabels, new Set(actualOverrides), address => String(address || "").toLowerCase(), () => false, isPotentialCustodyObservation);
  const reviewed = [...actualOverrides];
  const unrelated = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const observation = {tx_count: 120, balance: 750000, net_flow: 180000};
  const accumulators = reviewed.map((address, index) => ({
    address: index === 0 ? address.toUpperCase() : address,
    ...observation,
  }));
  accumulators.push({address: unrelated, ...observation});

  const learned = learnAutoCexLabels({periods: {recent: {eth: {accumulators, sellers: []}, bera: {accumulators: [], sellers: []}}}}, {holders: []});

  assert.equal(learned, 1);
  assert.deepEqual(autoLabels, {
    [unrelated]: {label: "Watchlist wallet", type: "watch", source: "heuristic-flow-pattern", confidence: "potential"},
  });
  reviewed.forEach(address => assert.equal(autoLabels[address], undefined));
});
