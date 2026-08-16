const assert = require("node:assert/strict");
const {
  depositKind,
  isExternalLock,
  buildActivityRows,
  activityTouchesAddress,
  filterActivityRows,
  buildPortfolioActivityRows,
  buildHistoryActivityEvents,
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

const sharedActivityRows = buildActivityRows([
  { depositType: 3, tokenId: 7, timestamp: 100, address: "0xOWNER", txHash: "0xextend", block: 1 },
  { depositType: 4, sourceTokenId: 7, targetTokenId: 8, timestamp: 200, address: "0xOWNER", txHash: "0xmerge", block: 2 },
  { depositType: 5, sourceTokenId: 8, targetTokenId: 9, timestamp: 300, address: "0xOWNER", txHash: "0xsplit", block: 3 },
], [
  { tokenId: 9, timestamp: 400, from: "0xSENDER", to: "0xRECIPIENT", txHash: "0xtransfer", block: 4 },
]);

assert.equal(activityTouchesAddress(sharedActivityRows[0], "0xsender"), true);
assert.equal(activityTouchesAddress(sharedActivityRows[0], "0xRECIPIENT"), true);
assert.equal(activityTouchesAddress(sharedActivityRows[0], "0xowner"), false);
assert.equal(activityTouchesAddress(sharedActivityRows[1], "0xOWNER"), true);
assert.equal(activityTouchesAddress(sharedActivityRows[1], "0xrecipient"), false);
assert.equal(activityTouchesAddress(sharedActivityRows[1], "0xown"), false);

assert.deepEqual(
  filterActivityRows(sharedActivityRows, {
    address: "0xowner",
    startTimestamp: 200,
    endTimestamp: 300,
  }).map(row => row.kind),
  ["split", "merge"],
);
assert.deepEqual(
  filterActivityRows(sharedActivityRows, {
    address: "0xrecipient",
    kinds: ["transfer"],
    startTimestamp: 400,
    endTimestamp: 400,
  }).map(row => row.txHash),
  ["0xtransfer"],
);

const duplicateLocks = [
  { depositType: 4, sourceTokenId: 7, targetTokenId: 8, timestamp: 200, address: "0xOWNER", txHash: "0xmerge", block: 2 },
  { depositType: 4, sourceTokenId: 7, targetTokenId: 8, timestamp: 200, address: "0xOWNER", txHash: "0xMERGE", block: 2 },
  { depositType: 4, sourceTokenId: 10, targetTokenId: 8, timestamp: 200, address: "0xOWNER", txHash: "0xmerge", block: 2 },
];
const duplicateTransfers = [
  { tokenId: 9, timestamp: 400, from: "0xSENDER", to: "0xRECIPIENT", txHash: "0xtransfer", block: 4 },
  { tokenId: 9, timestamp: 400, from: "0xsender", to: "0xrecipient", txHash: "0xTRANSFER", block: 4 },
];

const portfolioRows = buildPortfolioActivityRows(duplicateLocks, duplicateTransfers, "0xrecipient");
assert.deepEqual(portfolioRows.map(row => row.route), ["transfer"]);
assert.deepEqual(
  Object.fromEntries(["action", "actionSub", "paid", "pairedDolo", "price", "principalDelta", "isPositionManagement"].map(key => [key, portfolioRows[0][key]])),
  {
    action: "Transfer veDOLO",
    actionSub: "Position management event; no new DOLO locked.",
    paid: 0,
    pairedDolo: 0,
    price: null,
    principalDelta: 0,
    isPositionManagement: true,
  },
);

const ownerPortfolioRows = buildPortfolioActivityRows(duplicateLocks, duplicateTransfers, "0xowner");
assert.deepEqual(ownerPortfolioRows.map(row => `${row.route}:${row.sourceTokenId}->${row.targetTokenId}`), ["merge:7->8", "merge:10->8"]);
assert.equal(ownerPortfolioRows.length, 2);

const historyEvents = buildHistoryActivityEvents(duplicateLocks, duplicateTransfers, "0xrecipient", {
  startTimestamp: 400,
  endTimestamp: 400,
});
assert.equal(historyEvents.length, 1);
assert.deepEqual(
  Object.fromEntries(["chainKey", "action", "role", "asset", "amount", "usd", "taxCategory", "reviewFlag", "reviewReason", "principalDelta", "isPositionManagement"].map(key => [key, historyEvents[0][key]])),
  {
    chainKey: "berachain",
    action: "vedoloTransfer",
    role: "neutral",
    asset: "veDOLO position",
    amount: "0",
    usd: 0,
    taxCategory: "vedolo_position_management",
    reviewFlag: "not_applicable",
    reviewReason: "",
    principalDelta: 0,
    isPositionManagement: true,
  },
);
assert.equal(Object.hasOwn(historyEvents[0], "usdValue"), false);
assert.equal(historyEvents[0].txHash, "0xtransfer");
assert.equal(historyEvents[0].blockNumber, "4");
assert.equal(historyEvents[0].serialId, "vedoloTransfer:9:9");
assert.equal(historyEvents[0].label, "Position #9");
assert.equal(historyEvents[0].sourceEntity, "vedoloFlowsRpcLogs");
assert.equal(historyEvents[0].sourceLabel, "Berachain veDOLO RPC log history");

const ownerHistoryEvents = buildHistoryActivityEvents(duplicateLocks, duplicateTransfers, "0xowner");
assert.deepEqual(
  ownerHistoryEvents.map(event => event.label),
  ["Position #7 -> #8", "Position #10 -> #8"],
);
assert.ok(ownerHistoryEvents.every(event => event.sourceEntity === "vedoloFlowsRpcLogs"));

console.log("veDOLO position-activity contracts: 48 passed");
