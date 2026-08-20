"use strict";

const assert = require("node:assert/strict");
delete global.DOLO_ADDRESS_LABELS;
delete global.DOLO_ADDR_LABELS;
require("../dolo-address-overrides.js");

assert.equal(global.DOLO_ADDRESS_LABELS["0x4e5bc1cd2c421ecfef65395b3237f90a97178c55"].confidence, "high");
assert.equal(global.DOLO_ADDRESS_LABELS["0x7bd27a0103e48e25acdb131cc190314562171fde"].type, "trader");
assert.equal(global.DOLO_ADDRESS_LABELS["0x26c2448c0038874f68cc0d388d96f8d218af3bdf"].type, "bot");
assert.equal(global.DOLO_ADDR_LABELS["0x4e5bc1cd2c421ecfef65395b3237f90a97178c55"].label, "BingX-Linked Operational Wallet");

console.log("dolo-address-overrides tests passed");
