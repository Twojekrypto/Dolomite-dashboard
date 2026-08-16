const assert = require("node:assert/strict");
const {
  activeLockedDoloTotal,
  buildActiveLockedHistory,
} = require("../vedolo-locked-history.js");

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

const DAY = 86400;
const locks = [
  { depositType: 1, tokenId: 1, dolo: 100, timestamp: 10 * DAY + 100, locktime: 15 * DAY, block: 1 },
  { depositType: 1, tokenId: 2, dolo: 50, timestamp: 10 * DAY + 200, locktime: 20 * DAY, block: 2 },
  { depositType: 3, tokenId: 2, dolo: 0, timestamp: 11 * DAY + 100, locktime: 21 * DAY, block: 3 },
  { depositType: 4, tokenId: 1, sourceTokenId: 2, targetTokenId: 1, dolo: 50, timestamp: 11 * DAY + 200, locktime: 21 * DAY, block: 4 },
  { depositType: 5, tokenId: 3, sourceTokenId: 1, targetTokenId: 3, dolo: 40, timestamp: 12 * DAY + 100, locktime: 21 * DAY, block: 5 },
];
const unlocks = [
  { tokenId: 3, dolo: 40, timestamp: 13 * DAY + 100, block: 6 },
];
const history = buildActiveLockedHistory(locks, unlocks, 17 * DAY, 111);
const valueOnDay = day => history.find(point => point[0] === day * DAY)?.[1];

assert.equal(valueOnDay(10), 150, "create events add principal");
assert.equal(valueOnDay(11), 150, "extend and merge conserve locked principal");
assert.equal(valueOnDay(12), 150, "split conserves locked principal");
assert.equal(valueOnDay(13), 110, "early withdrawal removes active principal");
assert.equal(valueOnDay(16), 110, "merge carries the shorter target through the longer source end");
assert.equal(valueOnDay(17), 111, "holder snapshot anchors the final day only");

assert.throws(
  () => buildActiveLockedHistory([
    { depositType: 4, tokenId: 2, dolo: 10, timestamp: 10 * DAY, locktime: 20 * DAY },
  ], [], 17 * DAY, null),
  /merge transition is incomplete/,
);

const roundedDustSplit = buildActiveLockedHistory([
  { tokenId: 7, depositType: 1, dolo: 10, locktime: 20 * DAY, timestamp: DAY, block: 1 },
  { tokenId: 8, sourceTokenId: 7, targetTokenId: 8, depositType: 5, dolo: 0, locktime: 20 * DAY, timestamp: 2 * DAY, block: 2 },
], [], 3 * DAY, null);
assert.equal(roundedDustSplit.at(-1)[1], 10);

assert.throws(
  () => buildActiveLockedHistory([], [
    { tokenId: 99, dolo: 1, timestamp: DAY, block: 1 },
  ], 2 * DAY, null),
  /unlock source position #99 is missing/,
);
assert.throws(
  () => buildActiveLockedHistory([
    { tokenId: 99, depositType: 2, dolo: 1, locktime: 3 * DAY, timestamp: DAY, block: 1 },
  ], [], 2 * DAY, null),
  /increase source position #99 is missing/,
);
assert.throws(
  () => buildActiveLockedHistory([
    { tokenId: 2, depositType: 1, dolo: 50, locktime: 3 * DAY, timestamp: DAY, block: 1 },
    { tokenId: 1, sourceTokenId: 2, targetTokenId: 1, depositType: 4, dolo: 50, locktime: 3 * DAY, timestamp: DAY + 1, block: 2 },
  ], [], 2 * DAY, null),
  /merge target position #1 is missing/,
);

console.log("veDOLO locked-history contracts: 15 passed");
