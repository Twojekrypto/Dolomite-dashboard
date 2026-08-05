const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const preview = fs.readFileSync(path.join(__dirname, "..", "dolo-preview.html"), "utf8");

const typesStart = preview.indexOf("const TYPE_LABELS =");
const typesEnd = preview.indexOf("const TYPE_TIPS =", typesStart);
assert.notEqual(typesStart, -1);
assert.notEqual(typesEnd, -1);
const typeModel = new Function(
  `${preview.slice(typesStart, typesEnd)}\nreturn { TYPE_LABELS, ADDRESS_TYPES };`,
)();

const mapTypeStart = preview.indexOf("function mapType(info, holder)");
const mapTypeEnd = preview.indexOf("function autoCexInfo", mapTypeStart);
assert.notEqual(mapTypeStart, -1);
assert.notEqual(mapTypeEnd, -1);
const mapType = new Function(
  `${preview.slice(mapTypeStart, mapTypeEnd)}\nreturn mapType;`,
)();

test("protocol labels stay independently filterable", () => {
  assert.equal(typeModel.ADDRESS_TYPES.includes("protocol"), true);
  assert.equal(typeModel.TYPE_LABELS.protocol, "Protocol");
  assert.equal(
    mapType({ type: "protocol" }, { contract_wallet_type: "safe" }),
    "protocol",
  );
});
