"use strict";

const assert = require("node:assert/strict");
const labelsPath = require.resolve("../dolo-address-labels.js");
const overridesPath = require.resolve("../dolo-address-overrides.js");

delete require.cache[labelsPath];
delete require.cache[overridesPath];
delete global.window;
delete global.DOLO_ADDRESS_LABELS;
delete global.DOLO_ADDR_LABELS;
global.window = {};

require("../dolo-address-labels.js");
require("../dolo-address-overrides.js");

const runtimeLabels = global.window.cloneDoloAddressLabels();

assert.equal(global.window.DOLO_ADDRESS_LABELS["0x4e5bc1cd2c421ecfef65395b3237f90a97178c55"].confidence, "high");
assert.equal(global.window.DOLO_ADDRESS_LABELS["0x7bd27a0103e48e25acdb131cc190314562171fde"].type, "bot");
assert.equal(global.window.DOLO_ADDRESS_LABELS["0x26c2448c0038874f68cc0d388d96f8d218af3bdf"].type, "bot");
assert.equal(runtimeLabels["0x4e5bc1cd2c421ecfef65395b3237f90a97178c55"].label, "BingX-Linked Operational Wallet");
assert.equal(runtimeLabels["0x7bd27a0103e48e25acdb131cc190314562171fde"].label, "Automated Trading / Arbitrage");
assert.equal(runtimeLabels["0x26c2448c0038874f68cc0d388d96f8d218af3bdf"].type, "bot");

console.log("dolo-address-overrides tests passed");
