"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.resolve(__dirname, "..");
const historyIndex = fs.readFileSync(path.join(root, "history/index.html"), "utf8");
const historyJs = fs.readFileSync(path.join(root, "history/history.js"), "utf8");

test("Dolomite Transaction History exposes every veDOLO Position Activity action", () => {
  [
    ["vedoloDirect", "Direct veDOLO"],
    ["vedoloAirdrop", "Airdrop"],
    ["vedoloTransfer", "Transfer veDOLO"],
    ["vedoloMerge", "Merge veDOLO positions"],
    ["vedoloSplit", "Split veDOLO position"],
    ["vedoloExtend", "Extend veDOLO lock"],
    ["vestingPair", "Pair"],
    ["exercise", "Exercise"],
  ].forEach(([value, label]) => {
    assert.match(historyIndex, new RegExp(`<option value="${value}">${label}</option>`));
  });
});

test("History delegates veDOLO action icons to the shared Portfolio icon set", () => {
  assert.match(historyJs, /window\.VeDoloPositionActivity\?\.routeIconHtml/);
  assert.match(historyJs, /vedoloDirect:\s*"direct"/);
  assert.match(historyJs, /vedoloAirdrop:\s*"airdrop"/);
  assert.match(historyJs, /vestingPair:\s*"pair"/);
  assert.match(historyJs, /exercise:\s*"odolo"/);
});
