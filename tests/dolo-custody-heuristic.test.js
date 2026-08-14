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
