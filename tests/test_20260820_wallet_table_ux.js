"use strict";

const assert = require("node:assert/strict");
const ux = require("../wallet-table-ux.js");

const auditedTx = "0x8075f5a920510ae21d52f3bebb51294d46de90bc7e07f594c9afa3530335958e";
const otherTx = "0x" + "1".repeat(64);

assert.deepEqual(
  ux.historyActionsForTransaction(auditedTx, ["Route", "Deposit", "Transfer", "Route"]),
  ["deposit"],
  "the audited Dolomite deposit must render as one Deposit action"
);
assert.deepEqual(
  ux.historyActionsForTransaction(auditedTx, ["Route", "Transfer"]),
  ["route", "transfer"],
  "internal-only activity must not be relabeled as a deposit without deposit evidence"
);
assert.deepEqual(
  ux.historyActionsForTransaction(otherTx, ["Route", "Deposit", "Transfer"]),
  ["deposit"],
  "the semantic grouping rule must collapse internal deposit steps for every matching transaction"
);
assert.deepEqual(
  ux.historyActionsForTransaction(otherTx, ["Deposit", "Borrow"]),
  ["deposit", "borrow"],
  "a user-facing action such as Borrow must remain visible next to Deposit"
);

const routes = ["odolo", "pair", "airdrop", "direct", "transfer", "merge", "split", "extend"];
assert.deepEqual(
  ux.routeSelectionPlan(routes, "direct", routes),
  ["direct"],
  "choosing one route from All Routes must select only that route"
);
assert.deepEqual(
  ux.routeSelectionPlan(["direct", "merge"], "transfer", routes),
  ["direct", "transfer", "merge"],
  "concrete routes must support multi-select"
);
assert.deepEqual(
  ux.routeSelectionPlan(["direct", "merge"], "all", routes),
  routes,
  "All Routes must reset all concrete selections"
);

assert.equal(ux.emptyStateMessage("history-table", "No rows match filters"), "No transactions found");
assert.equal(ux.emptyStateMessage("holders-table", "No holders match filters"), "No veDOLO holders found");
assert.equal(ux.emptyStateMessage("position-activity-table", "No rows"), "No activity found");
assert.equal(ux.emptyStateMessage("deposited-assets-table", "No rows"), "No deposited assets found");
assert.equal(ux.emptyStateMessage("history-table", "No wallet loaded yet."), "Enter a wallet to load transactions.");

console.log("wallet-table-ux tests passed");
