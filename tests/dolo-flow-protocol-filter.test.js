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
  `${preview.slice(typesStart, typesEnd)}\nreturn { TYPE_LABELS, TYPE_FILTER_LABELS, TYPE_FILTER_ALIAS, typeFilterKey, ADDRESS_TYPES };`,
)();

const mapTypeStart = preview.indexOf("function mapType(info, holder)");
const mapTypeEnd = preview.indexOf("function autoCexInfo", mapTypeStart);
assert.notEqual(mapTypeStart, -1);
assert.notEqual(mapTypeEnd, -1);
const mapType = new Function(
  `${preview.slice(mapTypeStart, mapTypeEnd)}\nreturn mapType;`,
)();

const holderTypeStart = preview.indexOf("function holderDistributionType(addr, source)");
const holderTypeEnd = preview.indexOf("// Pipeline emits some timestamps", holderTypeStart);
assert.notEqual(holderTypeStart, -1);
assert.notEqual(holderTypeEnd, -1);
const holderDistributionType = new Function(
  "sharedAddressInfo",
  `${preview.slice(holderTypeStart, holderTypeEnd)}\nreturn holderDistributionType;`,
)(() => null);

test("protocol labels stay independently filterable", () => {
  assert.equal(typeModel.ADDRESS_TYPES.includes("protocol"), true);
  assert.equal(typeModel.TYPE_LABELS.protocol, "Protocol");
  assert.equal(
    mapType({ type: "protocol" }, { contract_wallet_type: "safe" }),
    "protocol",
  );
});

test("EIP-7702 delegated accounts remain user wallets despite legacy contract flags", () => {
  const holder = { is_contract: true, contract_wallet_type: "delegated_eoa" };
  assert.equal(mapType(null, holder), "eoa");
  assert.equal(holderDistributionType("0xabc", holder), "eoa");
});

test("verified account-abstraction wallets remain users despite contract bytecode", () => {
  const holder = { is_contract: true, contract_wallet_type: "smart_account" };
  assert.equal(mapType(null, holder), "eoa");
  assert.equal(holderDistributionType("0xabc", holder), "eoa");
});

test("named user contract wallets use the Smart Wallet category instead of CA", () => {
  assert.equal(typeModel.TYPE_LABELS.multisig, "Smart Wallet");
  assert.equal(mapType({ type: "contract_wallet" }, { is_contract: true }), "multisig");
});

test("one User wallets filter includes EOA and smart wallets without changing row labels", () => {
  assert.equal(typeModel.TYPE_LABELS.eoa, "EOA");
  assert.equal(typeModel.TYPE_LABELS.multisig, "Smart Wallet");
  assert.equal(typeModel.TYPE_FILTER_LABELS.eoa, "User wallets");
  assert.equal(typeModel.TYPE_FILTER_ALIAS.multisig, "eoa");
  assert.equal(typeModel.typeFilterKey("multisig"), "eoa");
  assert.equal(typeModel.ADDRESS_TYPES.includes("multisig"), false);
  assert.equal(typeModel.ADDRESS_TYPES.length, 7);
});

test("uncertain behavioral matches use the neutral Watchlist filter label", () => {
  assert.equal(typeModel.TYPE_LABELS.watch, "Watchlist");
  assert.equal(typeModel.TYPE_FILTER_LABELS.watch, "Watchlist");
});

test("legacy trader labels remain visible through the Trading bots filter", () => {
  assert.equal(mapType({ type: "trader" }, {}), "bot");
  assert.equal(holderDistributionType("0xabc", { type: "trader" }), "bot");
});

test("DOLO Flows type filter lists Investors immediately before Protocol", () => {
  const filterStart = preview.indexOf('<div class="dd" id="dd-flows-types">');
  const filterEnd = preview.indexOf('      <div class="tb-right">', filterStart);
  assert.notEqual(filterStart, -1);
  assert.notEqual(filterEnd, -1);

  const flowTypeFilter = preview.slice(filterStart, filterEnd);
  assert.ok(
    flowTypeFilter.indexOf('data-type="investor"') <
      flowTypeFilter.indexOf('data-type="protocol"'),
  );
});

test("holder and flow type filters expose an explicit All types option", () => {
  for (const id of ["dd-types", "dd-flows-types"]) {
    const filterStart = preview.indexOf(`<div class="dd" id="${id}">`);
    const filterEnd = preview.indexOf('</div>\n        </div>', filterStart);
    assert.notEqual(filterStart, -1);
    assert.notEqual(filterEnd, -1);
    const filter = preview.slice(filterStart, filterEnd);
    assert.match(filter, /data-type="all"/);
    assert.match(filter, />All types</);
    assert.match(filter, /data-type="eoa"[\s\S]*?>User wallets</);
    assert.doesNotMatch(filter, /data-type="multisig"/);
    assert.equal((filter.match(/class="rw-type-icon"/g) || []).length, 8);
  }
});

test("type filters switch from All to one type, then allow a multi-selection", () => {
  const helperStart = preview.indexOf("function nextAddressTypeSelection(");
  const helperEnd = preview.indexOf("// Address Type filter", helperStart);
  assert.notEqual(helperStart, -1);
  assert.notEqual(helperEnd, -1);
  const nextAddressTypeSelection = new Function(
    "ADDRESS_TYPES",
    `${preview.slice(helperStart, helperEnd)}\nreturn nextAddressTypeSelection;`,
  )(typeModel.ADDRESS_TYPES);

  let selected = new Set(typeModel.ADDRESS_TYPES);
  selected = nextAddressTypeSelection(selected, "ca");
  assert.deepEqual([...selected], ["ca"]);

  selected = nextAddressTypeSelection(selected, "cex");
  assert.deepEqual([...selected], ["ca", "cex"]);

  selected = nextAddressTypeSelection(selected, "ca");
  assert.deepEqual([...selected], ["cex"]);

  selected = nextAddressTypeSelection(selected, "all");
  assert.deepEqual([...selected], typeModel.ADDRESS_TYPES);
});

test("combined DOLO Flows consumes the generator's pre-ranked all-chain rows", () => {
  const start = preview.indexOf("  function flowRowsForPeriod(){");
  const end = preview.indexOf("  renderFlows = function(){", start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  const rows = [];
  const flowRowsForPeriod = new Function(
    "liveFlowsData",
    "state",
    "FLOWS",
    "HOLDERS",
    "DOLOMITE_FLOW_BALANCES",
    "lower",
    "addressInfo",
    "labelFor",
    "mapType",
    "isSafeWallet",
    "safeNum",
    `${preview.slice(start, end)}\nreturn flowRowsForPeriod;`,
  )(
    {
      periods: {
        "7d": {
          all: {
            accumulators: [{ address: "0xall", net_flow: 100 }],
            sellers: [],
          },
          eth: {
            accumulators: [{ address: "0xeth", net_flow: 60 }],
            sellers: [],
          },
          bera: {
            accumulators: [{ address: "0xbera", net_flow: 60 }],
            sellers: [],
          },
        },
      },
    },
    { flowsPeriod: "7d", flowsChain: "all" },
    rows,
    [],
    {},
    value => String(value || "").toLowerCase(),
    () => ({}),
    value => value,
    () => "eoa",
    () => false,
    value => Number(value || 0),
  );

  flowRowsForPeriod.call(null);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].addr, "0xall");
  assert.equal(rows[0].chgAll, 100);

  const effectiveStart = preview.indexOf("function effectiveChg(r){");
  const effectiveEnd = preview.indexOf("function effectiveGrossInflow", effectiveStart);
  const effectiveChg = new Function(
    "state",
    `${preview.slice(effectiveStart, effectiveEnd)}\nreturn effectiveChg;`,
  )({ flowsChain: "all" });
  assert.equal(effectiveChg(rows[0]), 100);
});

test("DOLO Flows exact-address search reaches verified rows below the Top 100", () => {
  const start = preview.indexOf("  function flowRowsForPeriod(){");
  const end = preview.indexOf("  renderFlows = function(){", start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  const rows = [];
  const searchedAddress = "0xa3aef439e6b69125cdbfd946ab1d8a9d012e1c46";
  const flowRowsForPeriod = new Function(
    "liveFlowsData",
    "state",
    "FLOWS",
    "HOLDERS",
    "DOLOMITE_FLOW_BALANCES",
    "lower",
    "addressInfo",
    "labelFor",
    "mapType",
    "isSafeWallet",
    "safeNum",
    `${preview.slice(start, end)}\nreturn flowRowsForPeriod;`,
  )(
    {
      periods: {
        all: {
          all: {
            accumulators: [{ address: "0xtop", net_flow: 100_000 }],
            sellers: [],
            search_accumulators: [{ address: searchedAddress, net_flow: 24_678.68 }],
            search_sellers: [],
          },
        },
      },
    },
    { flowsPeriod: "all", flowsChain: "all", qFlows: searchedAddress },
    rows,
    [{ addr: searchedAddress, total: 24_678.684, contract_wallet_type: "delegated_eoa" }],
    {},
    value => String(value || "").toLowerCase(),
    () => ({}),
    value => value,
    () => "eoa",
    () => false,
    value => Number(value || 0),
  );

  flowRowsForPeriod.call(null);
  assert.deepEqual(rows.map(row => row.addr), [searchedAddress]);
  assert.equal(rows[0].chgAll, 24_678.68);
  assert.equal(rows[0].balance, 24_678.684);
});

test("DOLO Flows type filters use the complete flow index without a search query", () => {
  const start = preview.indexOf("  function flowRowsForPeriod(){");
  const end = preview.indexOf("  renderFlows = function(){", start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  const rows = [];
  const investorAddress = "0x2222222222222222222222222222222222222222";
  const flowRowsForPeriod = new Function(
    "liveFlowsData",
    "state",
    "FLOWS",
    "HOLDERS",
    "DOLOMITE_FLOW_BALANCES",
    "lower",
    "addressInfo",
    "labelFor",
    "mapType",
    "isSafeWallet",
    "safeNum",
    "ADDRESS_TYPES",
    `${preview.slice(start, end)}\nreturn flowRowsForPeriod;`,
  )(
    {
      periods: {
        all: {
          all: {
            accumulators: [{ address: "0xtop", net_flow: 100_000 }],
            sellers: [],
            search_accumulators: [
              { address: "0xtop", net_flow: 100_000 },
              { address: investorAddress, net_flow: 250 },
            ],
            search_sellers: [],
          },
        },
      },
    },
    {
      flowsPeriod: "all",
      flowsChain: "all",
      qFlows: "",
      flowsTypes: new Set(["investor"]),
    },
    rows,
    [],
    {},
    value => String(value || "").toLowerCase(),
    address => ({ type: address === investorAddress ? "investor" : "eoa" }),
    value => value,
    info => info.type,
    () => false,
    value => Number(value || 0),
    typeModel.ADDRESS_TYPES,
  );

  flowRowsForPeriod.call(null);
  assert.deepEqual(rows.map(row => row.addr), ["0xtop", investorAddress]);
});

test("DOLO Flows labels the combined scope as All chains", () => {
  const filterStart = preview.indexOf('<div class="dd" id="dd-flows-chain">');
  const filterEnd = preview.indexOf('<div class="dd" id="dd-flows-types">', filterStart);
  assert.notEqual(filterStart, -1);
  assert.notEqual(filterEnd, -1);
  const filter = preview.slice(filterStart, filterEnd);
  assert.match(filter, />All chains</);
  assert.doesNotMatch(filter, /Ethereum \+ Berachain/);
});
