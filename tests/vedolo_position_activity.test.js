const assert = require("node:assert/strict");
const {
  depositKind,
  isExternalLock,
  buildActivityRows,
} = require("../vedolo-position-activity.js");

const depositFixtures = [
  { depositType: 0, expectedKind: "deposit", external: true },
  { depositType: 1, expectedKind: "create", external: true },
  { depositType: 2, expectedKind: "increase", external: true },
  { depositType: 3, expectedKind: "extend", external: false },
  { depositType: 4, expectedKind: "merge", external: false },
  { depositType: 5, expectedKind: "split", external: false },
];

for (const fixture of depositFixtures) {
  const row = { depositType: fixture.depositType, dolo: 25 };
  assert.equal(depositKind(row), fixture.expectedKind);
  assert.equal(isExternalLock(row), fixture.external);
}
assert.equal(isExternalLock({ depositType: 1, dolo: 0 }), false);
assert.equal(depositKind({ depositType: 99 }), "unknown");

const locks = [
  { depositType: 1, tokenId: 1, timestamp: 100, dolo: 100, address: "0xcreate" },
  { depositType: 3, tokenId: 1, timestamp: 200, dolo: 0, locktime: 900, address: "0xextend", txHash: "0xext" },
  { depositType: 4, tokenId: 2, sourceTokenId: 1, targetTokenId: 2, timestamp: 300, dolo: 100, locktime: 900, address: "0xmerge", txHash: "0xmerge" },
  { depositType: 5, tokenId: 3, sourceTokenId: 2, targetTokenId: 3, timestamp: 400, dolo: 40, locktime: 900, address: "0xsplit", txHash: "0xsplit" },
];
const transfers = [
  { tokenId: 3, timestamp: 500, from: "0xfrom", to: "0xto", txHash: "0xtransfer" },
];

const activities = buildActivityRows(locks, transfers);
assert.deepEqual(activities.map(row => row.kind), ["transfer", "split", "merge", "extend"]);
assert.deepEqual(activities.map(row => row.timestamp), [500, 400, 300, 200]);
assert.equal(activities[0].address, "0xto");
assert.equal(activities[0].from, "0xfrom");
assert.equal(activities[1].sourceTokenId, 2);
assert.equal(activities[1].targetTokenId, 3);
assert.equal(activities[2].dolo, 100);
assert.equal(activities[3].locktime, 900);
assert.equal(activities.some(row => row.kind === "create"), false);

console.log("veDOLO position-activity contracts: 20 passed");
