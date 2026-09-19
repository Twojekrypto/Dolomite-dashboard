"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const test = require("node:test");
const shared = require("../vedolo-position-activity.js");

const wallet = "0xc08d4f72c199bfda10bfd26a625d881fcfe20593";
const other = "0x1111111111111111111111111111111111111111";
const escrow = "0xcb86b75ee6133d179a12d550b09fb3cdb1e141d4";
const dolo = "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654";
const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const depositTopic = "0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624";
const withdrawTopic = "0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94";
const word = value => BigInt(value).toString(16).padStart(64, "0");
const topicAddress = value => `0x${value.slice(2).padStart(64, "0")}`;
const log = (address, topics, values, index) => ({ address, topics, data: `0x${values.map(word).join("")}`, logIndex: `0x${index.toString(16)}` });
const transfer = (from, to, amount, index) => log(dolo, [transferTopic, topicAddress(from), topicAddress(to)], [amount], index);
const deposit = (tokenId, amount, type, index, provider = wallet) => log(escrow, [depositTopic, topicAddress(provider), `0x${word(1800000000)}`], [tokenId, amount, type, 1788721232], index);
const withdraw = (tokenId, amount, index, provider = wallet) => log(escrow, [withdrawTopic, topicAddress(provider)], [tokenId, amount, 1788837896], index);
const sample = { address: wallet, txHash: `0x${"a".repeat(64)}`, tokenId: 18031, dolo: 1825.3456, timestamp: 1788837896, logIndex: 36 };
const receipt = logs => ({ status: "0x1", from: wallet, gasUsed: "0x5208", effectiveGasPrice: "0x1", logs });

function harness(storage = new Map()) {
  const source = fs.readFileSync(require.resolve("../history/history.js"), "utf8");
  const marker = '\n  if (document.readyState === "loading") {';
  const exposed = `\n globalThis.api = { state, els, fetchGas, groupEvents, fetchVedoloHistoryActivity, reportExportReadiness, reportCompletenessForRows, ensureFullReportReady, historyCompletionStatusMessage, cleanReportActionLabel, cleanTransactionAssetFlow, evidenceRowPayload, cleanHistoryReportCsvRows, cleanHistoryReportHeaders, setSelectedActionsFromValues, selectAllActions, fetchRewardClaimEvents, detailEventFlowLabel, rowHtml, cleanTransactionAction, displayActionsForRow, actionDisplayLabel, configure(rpc, price) { rpcRequest = rpc; if (price) historicalPrice = price; } };`;
  const sandbox = { console, URL, URLSearchParams, Blob, Set, Map, Date, Math, Intl,
    document: { readyState: "loading", addEventListener() {} },
    window: { VeDoloPositionActivity: shared, location: { href: "http://localhost/history/" }, localStorage: { getItem: key => storage.get(key), setItem: (key, value) => storage.set(key, value) } },
  };
  vm.runInNewContext(source.replace(marker, exposed + marker), sandbox);
  const api = sandbox.api;
  api.els.status = { textContent: "", classList: { toggle() {} } };
  api.els.action = { options: ["all", "deposit", "claim", "vedoloDirect", "vedoloWithdraw", "vedoloEarlyExit"].map(value => ({value, textContent: value})) };
  Object.assign(api.state, { address: wallet, loading: false, filtersDirty: false, warnings: [], selectedChains: new Set(["berachain"]), earn: { status: "ready", warnings: [], ledgers: {}, rewards: {}, prices: {} } });
  api.selectAllActions();
  return { api, sandbox, storage };
}

async function enrich(api, events, logs, payer = wallet, price = 0.027) {
  let receiptReads = 0;
  api.configure(async (_chain, method) => {
    if (method === "eth_getTransactionReceipt") { receiptReads++; return receipt(logs); }
    return { from: payer, gasPrice: "0x1", input: "0x" };
  }, async () => price);
  const row = api.groupEvents(events)[0];
  row.gas = await api.fetchGas(row, wallet);
  assert.equal(receiptReads, 1, "receipt enrichment must reuse the gas read");
  return row;
}

test("withdrawals are wallet/date scoped and wait for exact evidence", () => {
  const events = shared.buildHistoryActivityEvents([], [], wallet, { unlocks: [sample, {...sample, txHash: "0xother", address: other}], start: sample.timestamp, end: sample.timestamp });
  assert.equal(events.length, 1);
  assert.equal(events[0].action, "vedoloWithdraw");
  assert.equal(events[0].vedoloEvidenceStatus, "pending");
  assert.equal(events[0].usd, null);
});

test("receipt proves early-exit net, original principal and penalty without double legs", async () => {
  const { api } = harness();
  const events = shared.buildHistoryActivityEvents([], [], wallet, { unlocks: [sample, {...sample}] });
  assert.equal(events.length, 1);
  const row = await enrich(api, events, [
    transfer(escrow, "0x0000000000000000000000000000000000000000", "112472657819795000000", 28),
    transfer(escrow, other, "311634929615181715708", 32),
    transfer(escrow, wallet, "1825345568960923284292", 35),
    withdraw(18031, "1825345568960923284292", 36),
  ]);
  const event = row.events[0];
  assert.equal(event.action, "vedoloEarlyExit");
  assert.equal(event.amount, "1825.345568960923284292");
  assert.equal(event.vedoloEvidence.originalLocked, "2249.4531563959");
  assert.equal(event.vedoloEvidence.penalty, "424.107587434976715708");
  assert.equal(event.legs.length, 1);
  assert.equal(event.legs[0].direction, "in");
  assert.equal(event.legs[0].amount, "1825.345568960923284292");
  assert.equal(api.reportExportReadiness([row], []).canFullReport, true);
  assert.match(api.cleanTransactionAssetFlow(row), /1825\.345568960923284292/);
  assert.equal(api.evidenceRowPayload(row).events[0].vedoloEvidence.penalty, "424.107587434976715708");
});

test("new locks and increases preserve receipt precision and cache replay", async () => {
  const { api, storage } = harness();
  const locks = [{...sample, logIndex: 2, depositType: 2, tokenId: 21386, dolo: 64.2798}];
  const events = shared.buildHistoryActivityEvents(locks, [], wallet);
  assert.equal(events[0].isNewLock, false);
  const row = await enrich(api, events, [transfer(wallet, escrow, "64279781348969389954", 1), deposit(21386, "64279781348969389954", 2, 2)]);
  assert.equal(row.events[0].amount, "64.279781348969389954");
  assert.equal(row.events[0].legs[0].direction, "out");
  assert.match(api.cleanReportActionLabel(row.events[0]), /Increase/);
  const fresh = harness(storage).api;
  fresh.configure(async () => { throw new Error("unexpected second receipt fetch"); }, async () => { throw new Error("unexpected second price fetch"); });
  const cachedRow = fresh.groupEvents(shared.buildHistoryActivityEvents(locks, [], wallet))[0];
  cachedRow.gas = await fresh.fetchGas(cachedRow, wallet);
  assert.equal(cachedRow.events[0].amount, "64.279781348969389954");
  assert.equal(cachedRow.events[0].vedoloEvidenceStatus, "complete");
  assert.equal(fresh.reportExportReadiness([cachedRow], []).canFullReport, true);
});

test("deposit-for locks principal but never invents a wallet outgoing transfer", async () => {
  const { api } = harness();
  const events = shared.buildHistoryActivityEvents([{...sample, logIndex: 2, depositType: 1, dolo: 500}], [], wallet);
  const row = await enrich(api, events, [transfer(other, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 1, 2, other)], other);
  assert.equal(row.events[0].isNewLock, true);
  assert.equal(row.events[0].amount, "500");
  assert.equal(row.events[0].legs.length, 0);
  assert.equal(row.gas.status, "not-payer");
  assert.equal(api.reportCompletenessForRows([row]).receiptChecked, 1);
});

test("missing receipt, wrong emitter and missing price keep report blocked", async () => {
  for (const status of ["error", "missing", "pending", "price-missing", "ready", undefined]) {
    const { api } = harness();
    const row = { chainKey: "berachain", events: [], gas: {status} };
    api.state.gasChecked = 1;
    assert.equal(api.reportExportReadiness([row], []).canFullReport, false, status);
    assert.equal(api.ensureFullReportReady([row], [], "CSV"), false, status);
    assert.equal(api.reportCompletenessForRows([row]).receiptChecked, status === "price-missing" ? 1 : 0, status);
    api.state.filteredRows = [row];
    assert.doesNotMatch(api.historyCompletionStatusMessage(1, 1, 0, true), /Reports ready/);
  }
  for (const wrongEmitter of [false, true]) {
    const { api } = harness();
    const events = shared.buildHistoryActivityEvents([{...sample, logIndex: 2, depositType: 1, dolo: 500}], [], wallet);
    const depositLog = deposit(18031, "500000000000000000000", 1, 2);
    if (wrongEmitter) depositLog.address = other;
    const row = await enrich(api, events, [transfer(wallet, escrow, "500000000000000000000", 1), depositLog], wallet, null);
    assert.equal(api.reportExportReadiness([row], []).canFullReport, false);
    assert.equal(row.events[0].usd, null);
    assert.equal(row.events[0].vedoloEvidenceStatus, wrongEmitter ? "missing" : "complete");
  }
});

test("selected claim-source failure blocks All even with no rows on that chain", () => {
  const { api } = harness();
  api.state.selectedChains = new Set(["berachain", "arbitrum"]);
  api.state.warnings = ["Arbitrum reward claim index warning: RPC limit."];
  const rows = [{chainKey: "berachain", gas: {status: "ok"}, events: []}];
  assert.equal(api.reportExportReadiness(rows, []).canFullReport, false);
  api.setSelectedActionsFromValues(["deposit"]);
  assert.equal(api.reportExportReadiness(rows, []).canFullReport, true);
  api.selectAllActions();
  api.state.selectedChains = new Set(["berachain"]);
  assert.equal(api.reportExportReadiness(rows, []).canFullReport, true);
});

test("all seven audited receipts reconcile exact amounts and withdrawal penalties", async () => {
  const fixtures = require("./fixtures/history-vedolo-receipts.json");
  const expected = [
    ["1825.345568960923284292", "2249.4531563959", "424.107587434976715708"],
    ["1938.749024689973786783", "2542.140431244792424486", "603.391406554818637703"],
    ["20000", "20000", "0"],
    ["12.996098024241998856", "17.068253130363722753", "4.072155106121723897"],
    ["7.252507783427308222", "9.525129040431190022", "2.2726212570038818"],
    ["500"], ["64.279781348969389954"],
  ];
  for (const [index, fixture] of fixtures.entries()) {
    const { api } = harness();
    const sample = fixture.sample;
    const isLock = sample.depositType != null;
    const events = shared.buildHistoryActivityEvents(isLock ? [sample] : [], [], sample.address, {unlocks:isLock ? [] : [sample]});
    const row = api.groupEvents(events)[0];
    api.configure(async (_chain, method) => method === "eth_getTransactionReceipt" ? fixture.receipt : {from:fixture.receipt.from, input:"0x"}, async () => 0.027);
    row.gas = await api.fetchGas(row, sample.address);
    assert.equal(row.events[0].vedoloEvidenceStatus, "complete", sample.txHash);
    assert.equal(row.events[0].amount, expected[index][0]);
    assert.equal(row.events[0].legs.length, 1);
    assert.equal(row.events[0].legs[0].amount, expected[index][0]);
    if (!isLock) {
      assert.equal(row.events[0].vedoloEvidence.originalLocked, expected[index][1]);
      assert.equal(row.events[0].vedoloEvidence.penalty, expected[index][2]);
      assert.equal(row.events[0].action, index === 2 ? "vedoloWithdraw" : "vedoloEarlyExit");
    }
    assert.equal(api.reportExportReadiness([row], []).canFullReport, true);
    const csv = api.cleanHistoryReportCsvRows([row], []);
    assert.equal(csv.length, 1);
    assert.ok(csv[0].join(",").includes(expected[index][0]), "CSV preserves exact DOLO");
  }
});

test("multiple withdrawals segment penalties per Withdraw and reject net mismatch", async () => {
  const { api } = harness();
  const events = shared.buildHistoryActivityEvents([], [], wallet, {unlocks:[{...sample, logIndex:2}, {...sample, tokenId:24974, logIndex:5}]});
  const row = await enrich(api, events, [
    transfer(escrow, wallet, "1000000000000000000", 1), withdraw(18031, "1000000000000000000", 2),
    transfer(escrow, other, "500000000000000000", 3), transfer(escrow, wallet, "2000000000000000000", 4), withdraw(24974, "2000000000000000000", 5),
  ]);
  assert.deepEqual(Array.from(row.events, event => event.vedoloEvidence.penalty), ["0", "0.5"]);
  const broken = harness().api;
  const badRow = await enrich(broken, shared.buildHistoryActivityEvents([], [], wallet, {unlocks:[{...sample, logIndex:2}]}), [transfer(escrow, wallet, "1000000000000000000", 1), withdraw(18031, "2000000000000000000", 2)]);
  assert.equal(badRow.events[0].vedoloEvidenceStatus, "missing");
  assert.equal(broken.reportExportReadiness([badRow], []).canFullReport, false);
});

test("missing claim-source metadata is not a complete empty selected source", async () => {
  for (const payload of [{events:[]}, {events:[], chains:{xlayer:{}}}]) {
    const { api, sandbox } = harness();
    sandbox.fetch = async () => ({ok:true, status:200, json:async () => payload});
    api.state.selectedChains = new Set(["arbitrum"]);
    const result = await api.fetchRewardClaimEvents("arbitrum", wallet, {start:1788000000, end:1789000000});
    assert.equal(result.events.length, 0);
    assert.ok(result.warnings.some(message => message.includes("index is missing")));
    api.state.warnings = result.warnings;
    assert.equal(api.reportExportReadiness([], []).dataWarnings, 1);
  }
});

test("table, details and exports distinguish increase, penalty and unavailable valuation", async () => {
  const { api } = harness();
  const events = shared.buildHistoryActivityEvents([{...sample, depositType:2, logIndex:2}], [], wallet);
  const row = await enrich(api, events, [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 2, 2)], wallet, null);
  assert.match(api.cleanTransactionAction(row), /Increase veDOLO/);
  assert.match(api.displayActionsForRow(row).map(action => api.actionDisplayLabel(action)).join(","), /Increase veDOLO/);
  assert.match(api.rowHtml(row, false), /volume-td">Unavailable/);
  assert.match(api.detailEventFlowLabel(row.events[0]), /historical USD unavailable/);
  const exitApi = harness().api;
  const fixture = require("./fixtures/history-vedolo-receipts.json")[0];
  const exitRow = await enrich(exitApi, shared.buildHistoryActivityEvents([], [], wallet, {unlocks:[sample]}), fixture.receipt.logs);
  assert.match(exitApi.detailEventFlowLabel(exitRow.events[0]), /original locked 2249\.4531563959 DOLO; penalty 424\.107587434976715708/);
});

test("failed receipt lookups can retry and never persist a successful gas-only legacy entry", async () => {
  const { api } = harness();
  const row = api.groupEvents(shared.buildHistoryActivityEvents([{...sample, depositType:1, logIndex:2}], [], wallet))[0];
  api.configure(async () => {throw new Error("temporary RPC failure");}, async () => 1);
  assert.equal((await api.fetchGas(row, wallet)).status, "error");
  const logs = [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 1, 2)];
  api.configure(async (_chain, method) => method === "eth_getTransactionReceipt" ? receipt(logs) : {from:wallet, input:"0x"}, async () => 1);
  assert.equal((await api.fetchGas(row, wallet)).status, "ok");
  assert.equal(row.events[0].amount, "500");
});

test("unconfirmed receipt and unknown payer cannot become checked not-payer evidence", async () => {
  for (const incomplete of [{gasUsed:"0x5208", logs:[]}, {status:"0x1", gasUsed:"0x5208", logs:[]}]) {
    const { api } = harness();
    const row = {chainKey:"berachain", txHash:sample.txHash, events:[], timestamp:sample.timestamp};
    api.configure(async (_chain, method) => method === "eth_getTransactionReceipt" ? incomplete : {gasPrice:"0x1"}, async () => 1);
    row.gas = await api.fetchGas(row, wallet);
    assert.equal(api.reportCompletenessForRows([row]).receiptChecked, 0);
    assert.equal(api.reportExportReadiness([row], []).canFullReport, false);
  }
});

test("legacy and indexed source overlap yields one exact event while separate receipt logs survive", async () => {
  const legacy = {...sample, depositType:2, dolo:500, logIndex:undefined};
  for (const sourceRows of [[legacy, {...legacy, logIndex:2}], [{...legacy, logIndex:2}, legacy]]) {
    const { api } = harness();
    const events = shared.buildHistoryActivityEvents(sourceRows, [], wallet);
    assert.equal(events.length, 1);
    const row = await enrich(api, events, [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 2, 2)]);
    assert.equal(row.events[0].legs[0].amount, "500");
    assert.equal(api.reportExportReadiness([row], []).canFullReport, true);
  }
  const { api } = harness();
  const events = shared.buildHistoryActivityEvents([legacy, {...legacy, logIndex:2}, {...legacy, logIndex:4}], [], wallet);
  assert.equal(events.length, 2);
  const row = await enrich(api, events, [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 2, 2), transfer(wallet, escrow, "250000000000000000000", 3), deposit(18031, "250000000000000000000", 2, 4)]);
  assert.deepEqual(Array.from(row.events, event => event.legs[0].amount), ["500", "250"]);
  assert.equal(api.reportExportReadiness([row], []).canFullReport, true);
});

test("one receipt identity cannot enrich duplicated semantic rows even after source reconciliation", async () => {
  const { api } = harness();
  const event = shared.buildHistoryActivityEvents([{...sample, depositType:2, logIndex:2}], [], wallet)[0];
  const row = await enrich(api, [event, {...event, serialId:"other-source:18031"}], [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 2, 2)]);
  assert.equal(row.events.filter(event => event.vedoloEvidenceStatus === "complete").length, 1);
  assert.equal(row.events.flatMap(event => event.legs || []).length, 1);
  assert.equal(api.reportExportReadiness([row], []).canFullReport, false);
  // A cache produced before this repair must not reintroduce the same receipt twice.
  row.gas.vedoloEvidence.push({...row.gas.vedoloEvidence[0], serialId:"other-source:18031"});
  api.configure(async () => {throw new Error("unexpected receipt refetch");}, async () => {throw new Error("unexpected price refetch");});
  const replay = api.groupEvents([{...event}, {...event, serialId:"other-source:18031"}])[0];
  replay.gas = await api.fetchGas(replay, wallet);
  assert.equal(replay.events.filter(event => event.vedoloEvidenceStatus === "complete").length, 1);
  assert.equal(replay.events.flatMap(event => event.legs || []).length, 1);
  assert.equal(api.reportExportReadiness([replay], []).canFullReport, false);
});

test("cached exact receipt evidence recovers missing DOLO valuation without any receipt refetch", async () => {
  const locks = [{...sample, depositType:1, dolo:500, logIndex:2}];
  const logs = [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 1, 2)];
  for (const payer of [wallet, other]) {
    for (const cacheKind of ["memory", "storage"]) {
      const original = harness();
      let recovered = false;
      let rpcReads = 0;
      let doloPriceReads = 0;
      let nativePriceReads = 0;
      const rpc = async (_chain, method) => {
        rpcReads++;
        return method === "eth_getTransactionReceipt" ? receipt(logs) : {from:payer, input:"0x"};
      };
      const priceFetch = async url => {
        const priceId = decodeURIComponent(String(url).split("/").pop());
        if (priceId.includes(dolo)) doloPriceReads++;
        else nativePriceReads++;
        return {ok:true, json:async () => ({coins:priceId.includes(dolo) && !recovered ? {} : {[priceId]:{price:0.02}}})};
      };
      original.sandbox.fetch = priceFetch;
      original.api.configure(rpc);
      const first = original.api.groupEvents(shared.buildHistoryActivityEvents(locks, [], wallet))[0];
      first.gas = await original.api.fetchGas(first, wallet);
      assert.equal(first.gas.status, payer === wallet ? "ok" : "not-payer");
      assert.equal(first.events[0].amount, "500");
      assert.equal(first.events[0].valuationStatus, "unavailable");
      assert.equal(original.api.reportExportReadiness([first], []).canFullReport, false);
      assert.equal(rpcReads, 2);
      assert.equal(doloPriceReads, 1);
      const retry = cacheKind === "memory" ? original : harness(original.storage);
      retry.sandbox.fetch = priceFetch;
      retry.api.configure(rpc);
      recovered = true;
      const second = retry.api.groupEvents(shared.buildHistoryActivityEvents(locks, [], wallet))[0];
      second.gas = await retry.api.fetchGas(second, wallet);
      assert.equal(second.events[0].valuationStatus, "historical", `${payer}/${cacheKind}`);
      assert.equal(second.events[0].usd, 10);
      assert.equal(second.events[0].legs[0].amount, "500");
      assert.equal(retry.api.reportExportReadiness([second], []).canFullReport, true);
      assert.equal(rpcReads, 2, "confirmed receipt and transaction must not be refetched");
      assert.equal(doloPriceReads, 2, "only missing DOLO price is retried");
      assert.equal(nativePriceReads, payer === wallet ? 1 : 0, "confirmed native gas valuation stays cached");
      const persisted = harness(original.storage);
      persisted.api.configure(async () => {throw new Error("unexpected RPC");}, async () => {throw new Error("unexpected price");});
      const third = persisted.api.groupEvents(shared.buildHistoryActivityEvents(locks, [], wallet))[0];
      third.gas = await persisted.api.fetchGas(third, wallet);
      assert.equal(third.events[0].usd, 10, "recovered valuation is persisted");
    }
  }
});

test("cached gas discovers new veDOLO candidates with only one additional receipt read", async () => {
  for (const cacheKind of ["memory", "storage"]) {
    const original = harness();
    let receiptReads = 0;
    let txReads = 0;
    const logs = [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 1, 2)];
    const rpc = async (_chain, method) => {
      if (method === "eth_getTransactionReceipt") { receiptReads++; return receipt(logs); }
      txReads++;
      return {from:wallet, input:"0x"};
    };
    original.api.configure(rpc, async () => 0.02);
    const first = {chainKey:"berachain", txHash:sample.txHash, timestamp:sample.timestamp, events:[]};
    first.gas = await original.api.fetchGas(first, wallet);
    assert.equal(first.gas.status, "ok");
    const next = cacheKind === "memory" ? original : harness(original.storage);
    next.api.configure(rpc, async () => 0.02);
    const locks = [{...sample, depositType:1, dolo:500, logIndex:2}];
    const row = next.api.groupEvents(shared.buildHistoryActivityEvents(locks, [], wallet))[0];
    row.gas = await next.api.fetchGas(row, wallet);
    assert.equal(row.events[0].vedoloEvidenceStatus, "complete", cacheKind);
    assert.equal(row.events[0].legs[0].amount, "500");
    assert.equal(next.api.reportExportReadiness([row], []).canFullReport, true);
    assert.equal(receiptReads, 2);
    assert.equal(txReads, 1, "confirmed gas metadata must be reused");
    const persisted = harness(original.storage);
    persisted.api.configure(async () => {throw new Error("unexpected RPC");}, async () => {throw new Error("unexpected price");});
    const replay = persisted.api.groupEvents(shared.buildHistoryActivityEvents(locks, [], wallet))[0];
    replay.gas = await persisted.api.fetchGas(replay, wallet);
    assert.equal(replay.events[0].amount, "500");
    assert.equal(persisted.api.reportExportReadiness([replay], []).canFullReport, true);
  }
});

test("legacy-to-indexed source identity reuses exact cached receipt evidence without RPC or pricing", async () => {
  for (const cacheKind of ["memory", "storage"]) {
    const original = harness();
    const legacy = {...sample, depositType:2, dolo:500, logIndex:undefined};
    const first = await enrich(original.api, shared.buildHistoryActivityEvents([legacy], [], wallet), [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 2, 2)]);
    assert.equal(first.events[0].vedoloEvidenceStatus, "complete");
    const next = cacheKind === "memory" ? original : harness(original.storage);
    let rpcReads = 0;
    let priceReads = 0;
    next.api.configure(async () => {rpcReads++; throw new Error("unexpected RPC");}, async () => {priceReads++; throw new Error("unexpected price");});
    const row = next.api.groupEvents(shared.buildHistoryActivityEvents([{...legacy, logIndex:2}], [], wallet))[0];
    assert.notEqual(row.events[0].serialId, first.events[0].serialId);
    row.gas = await next.api.fetchGas(row, wallet);
    assert.equal(row.events[0].vedoloEvidenceStatus, "complete", cacheKind);
    assert.equal(row.events[0].legs[0].amount, "500");
    assert.equal(row.events[0].isNewLock, false);
    assert.equal(next.api.reportExportReadiness([row], []).canFullReport, true);
    assert.equal(rpcReads, 0);
    assert.equal(priceReads, 0);
  }
});

test("candidate refresh preserves separate receipt logs and never matches an unrelated token by serial", async () => {
  const original = harness();
  const firstLock = {...sample, depositType:2, dolo:500, logIndex:2};
  const logs = [transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 2, 2), transfer(wallet, escrow, "250000000000000000000", 3), deposit(18031, "250000000000000000000", 2, 4)];
  await enrich(original.api, shared.buildHistoryActivityEvents([firstLock], [], wallet), logs);
  let refreshedReceipts = 0;
  original.api.configure(async (_chain, method) => {
    assert.equal(method, "eth_getTransactionReceipt");
    refreshedReceipts++;
    return receipt(logs);
  }, async () => 0.02);
  const row = original.api.groupEvents(shared.buildHistoryActivityEvents([firstLock, {...firstLock, dolo:250, logIndex:4}], [], wallet))[0];
  row.gas = await original.api.fetchGas(row, wallet);
  assert.deepEqual(Array.from(row.events, event => event.legs[0].amount), ["500", "250"]);
  assert.equal(original.api.reportExportReadiness([row], []).canFullReport, true);
  assert.equal(refreshedReceipts, 1);
  const unrelated = shared.buildHistoryActivityEvents([{...firstLock, tokenId:99999}], [], wallet)[0];
  unrelated.serialId = row.events[0].serialId;
  const wrongRow = original.api.groupEvents([unrelated])[0];
  wrongRow.gas = await original.api.fetchGas(wrongRow, wallet);
  assert.equal(wrongRow.events[0].vedoloEvidenceStatus, "missing");
  assert.equal(wrongRow.events[0].legs.length, 0);
  assert.equal(wrongRow.events[0].amount, "");
  assert.equal(original.api.reportExportReadiness([wrongRow], []).canFullReport, false);
  assert.equal(refreshedReceipts, 2);
});

test("unavailable refreshed receipt retains confirmed gas but blocks new candidates until retry succeeds", async () => {
  const {api} = harness();
  api.configure(async (_chain, method) => method === "eth_getTransactionReceipt" ? receipt([]) : {from:wallet, input:"0x"}, async () => 0.02);
  const first = {chainKey:"berachain", txHash:sample.txHash, timestamp:sample.timestamp, events:[]};
  first.gas = await api.fetchGas(first, wallet);
  const locks = [{...sample, depositType:1, dolo:500, logIndex:2}];
  const row = api.groupEvents(shared.buildHistoryActivityEvents(locks, [], wallet))[0];
  api.configure(async (_chain, method) => {assert.equal(method, "eth_getTransactionReceipt"); return null;}, async () => 0.02);
  row.gas = await api.fetchGas(row, wallet);
  assert.equal(row.gas.status, "ok");
  assert.equal(api.reportCompletenessForRows([row]).receiptChecked, 1);
  assert.equal(row.events[0].vedoloEvidenceStatus, "missing");
  assert.equal(api.reportExportReadiness([row], []).canFullReport, false);
  api.configure(async (_chain, method) => {assert.equal(method, "eth_getTransactionReceipt"); return receipt([transfer(wallet, escrow, "500000000000000000000", 1), deposit(18031, "500000000000000000000", 1, 2)]);}, async () => 0.02);
  row.gas = await api.fetchGas(row, wallet);
  assert.equal(row.events[0].amount, "500");
  assert.equal(api.reportExportReadiness([row], []).canFullReport, true);
});
