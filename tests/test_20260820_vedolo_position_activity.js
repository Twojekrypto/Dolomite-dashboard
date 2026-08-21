"use strict";

const assert = require("node:assert/strict");
const activity = require("../vedolo-position-activity.js");

const wallet = "0x1111111111111111111111111111111111111111";
const create = {
  depositType: 1,
  tokenId: 42,
  address: wallet,
  dolo: 1055.7,
  locktime: 1900000000,
  timestamp: 1800000000,
  block: 123,
  txHash: "0x" + "2".repeat(64),
};
const extend = {
  depositType: 3,
  tokenId: 42,
  address: wallet,
  dolo: 0,
  locktime: 1950000000,
  timestamp: 1800000100,
  block: 124,
  txHash: "0x" + "3".repeat(64),
};

assert.equal(activity.depositKind(create), "create");
assert.equal(activity.isExternalLock(create), true);

const rows = activity.buildActivityRows([create, extend], []);
assert.equal(rows.length, 2);
assert.equal(rows.find(row => row.kind === "create").isNewLock, true);

const portfolioRows = activity.buildPortfolioActivityRows([create, extend], [], wallet);
assert.equal(portfolioRows.some(row => row.route === "create"), false, "Portfolio already builds canonical new-lock rows from exercise/lock sources");
assert.equal(portfolioRows.some(row => row.route === "extend"), true, "Portfolio keeps semantic position-management activity");

const historyRows = activity.buildHistoryActivityEvents([create, extend], [], wallet);
assert.equal(historyRows.some(row => row.route === "create"), false, "History already receives the principal deposit event from its canonical source");
assert.equal(historyRows.some(row => row.route === "extend"), true, "position-management activity must remain in History details");

assert.equal(activity.routeLabel("create"), "New Lock");
assert.equal(activity.routeLabel("all"), "All Actions");
assert.notEqual(activity.routeIconSvg("direct"), activity.routeIconSvg("create"), "direct locks need a distinct action icon");
assert.match(activity.routeIconHtml("merge", true, "route-icon"), /route-icon active/);

const actionIcons = ["odolo", "pair", "airdrop", "direct", "transfer", "merge", "split", "extend"]
  .map(kind => activity.routeIconSvg(kind));
assert.equal(new Set(actionIcons).size, actionIcons.length, "every Portfolio action needs a distinct icon");

console.log("vedolo-position-activity tests passed");
