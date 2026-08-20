"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const historyJs = fs.readFileSync(path.join(root, "history/history.js"), "utf8");
const historyIndex = fs.readFileSync(path.join(root, "history/index.html"), "utf8");
const tx = "0x8075f5a920510ae21d52f3bebb51294d46de90bc7e07f594c9afa3530335958e";

assert.ok(historyJs.includes(tx), "the audited router transaction must be explicitly grouped");
assert.match(historyJs, /AUDITED_INTERNAL_DEPOSIT_TXS\.has\(rowHash\)\) return \["deposit"\]/);
assert.ok(historyIndex.includes("wallet-table-ux.css?v=20260820-table-ux-v1"));
assert.ok(historyIndex.includes("wallet-table-ux.js?v=20260820-table-ux-v1"));
assert.ok(historyIndex.includes("history-20260820-grouped-actions-table-ux"));

console.log("history grouping contract tests passed");
