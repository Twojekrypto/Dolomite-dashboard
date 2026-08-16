const assert = require("node:assert/strict");
const { activeLockedDoloTotal } = require("../vedolo-locked-history.js");

const holders = [
  { token_details: [
    { id: 1, dolo: 125.25, end: 500 },
    { id: 2, dolo: 900, end: 399 },
  ] },
  { token_details: [{ id: 3, dolo: 74.75, end: 401 }] },
];

assert.equal(activeLockedDoloTotal(holders, 400), 200);
assert.equal(activeLockedDoloTotal(holders, 500), 0);
assert.equal(activeLockedDoloTotal([], 400), null);
assert.equal(activeLockedDoloTotal(null, 400), null);

console.log("veDOLO locked-history contracts: 4 passed");
