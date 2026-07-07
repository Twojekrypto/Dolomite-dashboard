import os
import re
import unittest
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# Run instrumented history.js under a fixed locale so Number.toLocaleString() output
# (thousands/decimal separators) is deterministic regardless of the developer's or
# CI runner's system locale. The assertions pin en-US formatting (e.g. "67,055.74").
NODE_ENV = {**os.environ, "LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"}
HISTORY_JS = ROOT / "history" / "history.js"
HISTORY_HTML = ROOT / "history" / "index.html"
HISTORY_CSS = ROOT / "history" / "history.css"
ODOLO_CLAIM_GENERATOR = ROOT / "generate_odolo_claim_events.py"
REWARD_CLAIM_GENERATOR = ROOT / "generate_reward_claim_events.py"


class HistoryTaxExportContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = HISTORY_JS.read_text()
        cls.html = HISTORY_HTML.read_text()
        cls.css = HISTORY_CSS.read_text()
        cls.claim_generator = ODOLO_CLAIM_GENERATOR.read_text()
        cls.reward_claim_generator = REWARD_CLAIM_GENERATOR.read_text()

    def test_history_version_is_cache_busted_consistently(self):
        # Pin consistency, not a specific value: whatever HISTORY_VERSION history.js
        # declares must be the cache-busting query string used for both the JS and CSS
        # in index.html. This stays green across deliberate version bumps as long as the
        # three references agree.
        match = re.search(r'const HISTORY_VERSION = "([^"]+)"', self.source)
        self.assertIsNotNone(match, "HISTORY_VERSION constant missing from history.js")
        version = match.group(1)
        self.assertIn(f"history/history.js?v={version}", self.html)
        self.assertIn(f"history/history.css?v={version}", self.html)

    def test_history_graph_queries_can_use_fail_fast_options(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { graphQuery };" + marker);
const attempts = [];
const timers = [];
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  AbortController,
  setTimeout(callback, ms) {
    timers.push(ms);
    callback();
    return timers.length;
  },
  clearTimeout() {},
  fetch(_endpoint, options) {
    attempts.push({ aborted: !!options.signal?.aborted });
    const error = new Error("aborted");
    error.name = "AbortError";
    return Promise.reject(error);
  },
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
(async () => {
  let message = "";
  try {
    await sandbox.__historyTest.graphQuery("https://example.invalid/graphql", "{ ping }", { timeoutMs: 5, attempts: 1 });
  } catch (error) {
    message = error.message || String(error);
  }
  const results = { attempts: attempts.length, timers, message };
  if (results.attempts !== 1) throw new Error(JSON.stringify(results));
  if (results.timers[0] !== 5) throw new Error(JSON.stringify(results));
  if (results.message !== "Subgraph timeout") throw new Error(JSON.stringify(results));
})().catch(error => {
  console.error(error);
  process.exit(1);
});
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_history_finalize_can_timeout_without_waiting_for_receipts_forever(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { waitForHistoryFinalize };" + marker);
const timers = [];
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  setTimeout(callback, ms) {
    timers.push(ms);
    callback();
    return timers.length;
  },
  clearTimeout() {},
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
(async () => {
  const completed = await sandbox.__historyTest.waitForHistoryFinalize([new Promise(() => {})], 9);
  const results = { completed, timers };
  if (results.completed !== false) throw new Error(JSON.stringify(results));
  if (results.timers[0] !== 9) throw new Error(JSON.stringify(results));
})().catch(error => {
  console.error(error);
  process.exit(1);
});
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_action_filter_options_are_reportable(self):
        options = re.findall(r'<option value="([^"]+)">([^<]+)</option>', self.html)
        action_values = [value for value, _label in options if value != "all"]
        labels_match = re.search(r"const ACTION_LABELS = \{(?P<body>.*?)\n  \};", self.source, re.S)
        self.assertIsNotNone(labels_match, "ACTION_LABELS block missing")
        labels_block = labels_match.group("body")
        for value in action_values:
            if value == "swap":
                continue
            self.assertRegex(labels_block, rf"\b{re.escape(value)}:", f"{value} filter has no report/table label")

        filter_match = re.search(r"function rowMatchesActionFilter\(row, action\) \{(?P<body>.*?)\n  \}", self.source, re.S)
        self.assertIsNotNone(filter_match, "rowMatchesActionFilter missing")
        filter_block = filter_match.group("body")
        for custom_value in ["swap", "vestingPair", "vestingClaim", "claim", "addCollateral"]:
            self.assertIn(f'action === "{custom_value}"', filter_block)

    def test_borrow_position_actions_have_distinct_readable_colors(self):
        self.assertIn(".action-chip.borrow{color:#fb923c", self.css)
        self.assertIn(".action-chip.openBorrow{color:#fbbf24", self.css)
        self.assertIn(".action-chip.repay{color:#86efac", self.css)
        self.assertIn(".action-chip.closeBorrow{color:#5eead4", self.css)

    def test_history_is_positioned_as_transaction_history_first(self):
        self.assertIn("<h1>Dolomite Transaction History</h1>", self.html)
        self.assertIn("<h2>Transaction results</h2>", self.html)
        self.assertNotIn("<h2>Dolomite Transaction History</h2>", self.html)
        self.assertIn('<span class="beta-badge">BETA</span>', self.html)
        self.assertIn("Best-effort Dolomite history. Verify exports before accounting or tax filing.", self.html)
        self.assertIn("Download report", self.html)
        self.assertIn("One Dolomite history report in CSV, JSON or printable format.", self.html)
        self.assertIn("Dolomite Transaction History", self.html)
        self.assertIn('aria-label="Dolomite transaction results"', self.html)
        self.assertNotIn("History overview", self.html)
        self.assertNotIn("Dolomite transaction ledger", self.html)
        self.assertIn("Dolomite History Report", self.source)
        self.assertIn("dolomite-history-report", self.source)

    def test_csv_report_is_clean_and_json_keeps_full_evidence(self):
        for column in [
            '"row_type"',
            '"date_utc"',
            '"network"',
            '"tx_hash"',
            '"action"',
            '"asset_flow"',
            '"usd_value_at_time"',
            '"gas_asset"',
            '"gas_amount"',
            '"gas_usd_at_time"',
            '"gas_status"',
            '"review_status"',
            '"review_reason"',
            '"user_note"',
            '"source"',
            '"source_entity"',
            '"report_scope"',
            '"gas_coverage"',
            '"warnings_count"',
            '"report_generated_at"',
        ]:
            self.assertIn(column, self.source)
        for removed_column in [
            '"earn_source"',
            '"earn_market_id"',
            '"earn_status"',
            '"earn_method"',
            '"earn_snapshot_date"',
            '"earn_coverage"',
            '"earn_period_source"',
            '"activity_group"',
            '"tx_event_count"',
            '"fee_allocation"',
            '"claim_proof_status"',
            '"reward_accumulated_amount"',
            '"reward_claimed_estimate_amount"',
            '"reward_unclaimed_amount"',
            '"external_cost_basis_included"',
            '"scope_notes"',
            '"event_index"',
            '"account_number"',
        ]:
            self.assertNotIn(removed_column, self.source)
        self.assertIn("exportHistoryReportCsv", self.source)
        self.assertIn("cleanHistoryReportCsvRows", self.source)
        self.assertIn("cleanHistoryTransactionCsvRow", self.source)
        self.assertIn("cleanHistoryEarnCsvRow", self.source)
        self.assertIn("cleanHistorySourceEntities(row)", self.source)
        self.assertIn("sourceEntity: event.sourceEntity || \"\"", self.source)
        self.assertIn("sourceEntities: cleanHistorySourceEntities(row)", self.source)
        self.assertIn("cleanEarnActionLabel(entry)", self.source)
        self.assertIn("cleanTransactionAction(row)", self.source)
        self.assertIn("cleanHistoryReviewStatus", self.source)
        self.assertIn("cleanEarnAssetFlow", self.source)
        self.assertIn('downloadCsvFile(', self.source)
        self.assertIn("function downloadTextFile", self.source)
        self.assertIn('typeof URL.createObjectURL === "function"', self.source)
        self.assertIn("data:${mimeType}", self.source)
        self.assertIn("downloadTextFile(filename, JSON.stringify(payload, null, 2)", self.source)
        self.assertIn("dolomite-history-report-${state.address || \"wallet\"}-${state.year}-${HISTORY_VERSION}.csv", self.source)
        self.assertNotIn("exportTaxCsv", self.source)
        self.assertNotIn("dolomite-tax-ledger", self.source)
        self.assertNotIn("dolomite-tax-tool-generic", self.source)
        self.assertIn("Dolomite protocol activity only", self.source)
        self.assertIn("Excludes acquisition cost basis", self.source)
        self.assertIn("earn-verified-ledger", self.source)
        self.assertIn("earn-merkl-rewards", self.source)
        self.assertIn("external_reward_candidate", self.source)
        self.assertIn("reward_period_unverified", self.source)
        self.assertIn("estimated_from_accumulated_minus_unclaimed", self.source)
        self.assertIn("transaction_level_fee_recorded_once", self.source)
        self.assertIn("shared_tx_fee_not_repeated", self.source)
        self.assertIn("routeEvidence", self.source)
        self.assertIn("routeTokenPath", self.source)
        self.assertIn("routeMatchConfidence", self.source)
        self.assertIn("routeHopCount", self.source)
        self.assertIn('routeEvidence: "token_path_unverified"', self.source)
        self.assertIn('routeMatchConfidence: "none"', self.source)
        self.assertIn("rawAmount: String(amount ?? \"\").trim()", self.source)
        self.assertIn("ODOLO_CLAIM_EVENTS_URL", self.source)
        self.assertIn("REWARD_CLAIM_EVENTS_URL", self.source)
        self.assertIn("REWARD_CLAIM_EVENTS_BASE", self.source)
        self.assertIn("rewardClaimEventsPromises = new Map()", self.source)
        self.assertIn("loadRewardClaimPayload(chainKey)", self.source)
        self.assertIn("fetchOptionalJson(`${REWARD_CLAIM_EVENTS_BASE}/${chainKey}.json`)", self.source)
        self.assertIn("if (!claimFilterRelevant || !REWARD_CLAIM_INDEX_CHAIN_KEYS.has(chainKey))", self.source)
        self.assertIn('REWARD_CLAIM_INDEX_CHAIN_KEYS = new Set(["berachain", "arbitrum", "mantle", "xlayer"])', self.source)
        self.assertIn('REWARD_CLAIM_INDEX_CHAIN_KEYS.has(chainKey) && !hasChainMeta', self.source)
        self.assertIn('coverageStatus !== "complete"', self.source)
        self.assertIn("reward claim index is missing", self.source)
        self.assertIn("eventFromOdoloClaim", self.source)
        self.assertIn("eventFromRewardClaim", self.source)
        self.assertIn("eventFromRewardLevelUpdate", self.source)
        self.assertIn("odoloRewardClaimEvents", self.source)
        self.assertIn("rewardClaimEvents", self.source)
        self.assertIn("liquidityMiningLevelUpdateRequests", self.source)
        self.assertIn("Dolomite RewardClaimed log", self.source)

    def test_clean_csv_uses_transaction_rows_without_event_spam(self):
        self.assertIn('"transaction"', self.source)
        self.assertIn('"earn_candidate"', self.source)
        self.assertIn("row.events.map(taxProfileForEvent)", self.source)
        self.assertIn("cleanTransactionAssetFlow(row)", self.source)
        self.assertIn("compactTransactionAssetPreview(row)", self.source)
        self.assertIn("compactVestingEventsPreview(row)", self.source)
        self.assertIn("vestingEventsForRow(row).map(compactVestingTableFlow)", self.source)
        self.assertIn('compactTransferTableFlow(transfer, "table")', self.source)
        self.assertIn("formatRoundedTokenAmount(payment, \"USDC\")", self.source)
        self.assertIn("primaryTransactionEvent(row.events)", self.source)
        self.assertIn("cleanPrimaryEventFlow(primary)", self.source)
        self.assertIn("cleanFlowFromLegs(event.legs, mode)", self.source)
        self.assertIn("cleanSwapOutcomeFlow(event, mode)", self.source)
        self.assertIn("cleanSwapOutcomeFlow(primary, \"table\")", self.source)
        self.assertIn("cleanSwapOutcomeFlow(event, \"detail\")", self.source)
        self.assertIn("parseSignedSwapLabel(event?.label, cleanReportActionLabel(event), mode)", self.source)
        self.assertIn("cleanLegGroup(legs, \"out\", mode)", self.source)
        self.assertIn("cleanLegGroup(legs, \"in\", mode)", self.source)
        self.assertIn("return `${cleanReportActionLabel(event)}: ${flow}`;", self.source)
        self.assertIn('return "Zap";', self.source)
        self.assertIn('if (event.taxCategory === "swap") return "AMM Trade";', self.source)
        self.assertIn('if (event?.action === "trade" || event?.taxCategory === "swap") return "Trade";', self.source)
        self.assertIn("cleanVestingEventFlow(event)", self.source)
        self.assertIn("vestingPositionLegs(position, flowLabel, direction)", self.source)
        self.assertIn("vestingPositionAssetLabel(position, flowLabel)", self.source)
        self.assertIn("vestingPositionAmountLabel(position, flowLabel)", self.source)
        self.assertIn('label: `${vestingActionLabel(flowLabel)}${positionId}${amountLabel ? ` (${amountLabel})` : ""}`', self.source)
        self.assertIn('return "Pair oDOLO + DOLO";', self.source)
        self.assertIn('return "Claim veDOLO";', self.source)
        self.assertIn("oDOLO/DOLO vesting pair", self.source)
        self.assertIn("USDC exercise cost", self.source)
        self.assertIn("vestingTaxCategory(flowLabel)", self.source)
        self.assertIn('return "odolo_dolo_pair";', self.source)
        self.assertIn('return "vedolo_claim";', self.source)
        self.assertNotIn("reward_or_vesting_income", self.source)
        self.assertNotIn('return "vesting_reward";', self.source)
        self.assertIn("activityGroup: activityGroupForEvent(event)", self.source)
        self.assertIn("actionLabel: cleanReportActionLabel(event)", self.source)
        self.assertIn("formatVestingStatus(event.vestingStatus)", self.source)
        self.assertIn('where: `toEffectiveUser: "${user}", ${timeFilter}`', self.source)
        self.assertIn("sameEffectiveVestingUser(row) ? [] : eventFromVesting", self.source)
        self.assertIn("function sameEffectiveVestingUser", self.source)
        self.assertIn('return "Moved internally"', self.source)
        self.assertIn('return "Move veDOLO position"', self.source)
        self.assertIn('return "vesting_internal_move"', self.source)
        self.assertIn("function vestingFlowIsInternal", self.source)
        self.assertIn('"Transferred out"', self.source)
        self.assertIn("return `${direction}${id}${amount}${status}`;", self.source)
        self.assertIn("exercise cost", self.source)
        self.assertNotIn("paired/locked DOLO", self.source)
        self.assertNotIn("DOLO payment amount", self.source)
        self.assertNotIn("Sent ${event.vestingKind", self.source)
        self.assertIn("Pair oDOLO + DOLO", self.html)
        self.assertIn("Claim veDOLO", self.html)
        self.assertIn("Pair / Claim veDOLO", self.source)
        self.assertIn("Claim veDOLO", self.source)
        self.assertNotIn("Vesting Transfer", self.html)
        self.assertNotIn("Vesting Transfer", self.source)
        self.assertIn("summarizeUniqueCsvLabels(summaries, 4)", self.source)
        self.assertIn("reconcileZapEvents(row)", self.source)
        self.assertIn("matchedTradeFlowForZap", self.source)
        self.assertIn("matchedTradeRouteFlowForZap", self.source)
        self.assertIn("routeEndpointCandidate", self.source)
        self.assertIn("routeMatchCandidate", self.source)
        self.assertIn("canonicalDecimalString(a?.rawAmount ?? a?.amount)", self.source)
        self.assertIn("legUsdMatches", self.source)
        self.assertIn("legIdentityMatches", self.source)
        self.assertIn("legIdentityScore", self.source)
        self.assertIn("aAddress === bAddress ? 6 : 0", self.source)
        self.assertIn("tokenPath { ${PAIR_TOKEN_FIELDS} }", self.source)
        self.assertIn("paired_trade_reconciled", self.source)
        self.assertIn("Zap token labels were reconciled against the paired trade event", self.source)
        self.assertIn("Reward candidate", self.source)
        self.assertIn("Review-only reward candidate", self.source)
        self.assertIn("Yield candidate", self.source)
        self.assertIn("Review-only yield candidate", self.source)
        self.assertIn("EARN ledger/snapshot + historical price", self.source)
        self.assertIn("EARN rewards cache + historical price", self.source)
        self.assertNotIn("joinLegField", self.source)
        self.assertNotIn("genericTaxToolRows", self.source)
        self.assertNotIn("genericTaxToolType", self.source)

    def test_clean_csv_keeps_source_entity_and_action_labels_aligned(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { cleanHistoryReportHeaders, cleanHistoryReportCsvRows, cleanReportActionLabel, displayActionsForRow, positionLifecycleForRows, state };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyTest;
api.state.year = "custom";
api.state.dateFrom = "2026-01-01";
api.state.dateTo = "2026-05-26";
api.state.selectedChains = new Set(["arbitrum"]);
api.state.earn = { status: "ready", warnings: [], ledgers: {}, rewards: {}, prices: {} };
api.state.warnings = [];
const row = {
  chainKey: "arbitrum",
  txHash: "0xabc",
  blockNumber: "1",
  timestamp: 1768210081,
  usdVolume: 100,
  gas: { status: "ok", paidByWallet: true, nativeSymbol: "ETH", nativeAmountExact: "0.001", gasUsd: "2.5", historicalPrice: 2500 },
  actions: new Set(["trade", "amm"]),
  semanticActions: new Set(),
  events: [
    { action: "trade", sourceEntity: "trades", taxCategory: "swap", reviewFlag: "possible_taxable_disposal", account: "0", label: "-100 USDC / +0.04 ETH", usd: 100, legs: [{ direction: "out", symbol: "USDC", amount: "100", usd: "100" }, { direction: "in", symbol: "ETH", amount: "0.04", usd: "100" }] },
    { action: "amm", sourceEntity: "ammMints", taxCategory: "liquidity_deposit", reviewFlag: "needs_review", reviewReason: "amm_liquidity_review", account: "external", label: "Add liquidity: +10 USDC", usd: 10, legs: [{ direction: "out", symbol: "USDC", amount: "10", usd: "10" }] },
  ],
};
const earn = { source: "earn-merkl-rewards", timestamp: 1768210081, chainKey: "arbitrum", chainName: "Arbitrum", action: "EARN reward summary", assetInAmount: "12.5", assetInSymbol: "DOLO", usd: "1.2", reviewFlag: "needs_review", reviewReason: "reward_period_unverified" };
const headers = api.cleanHistoryReportHeaders();
const rows = api.cleanHistoryReportCsvRows([row], [earn]);
const sourceEntityIndex = headers.indexOf("source_entity");
if (sourceEntityIndex < 0) throw new Error("missing source_entity");
if (rows.some(item => item.length !== headers.length)) throw new Error(JSON.stringify({ headers: headers.length, rows: rows.map(item => item.length) }));
if (rows[0][sourceEntityIndex] !== "trades; ammMints") throw new Error(JSON.stringify(rows[0]));
if (rows[1][6] !== "Reward / Claim") throw new Error(JSON.stringify(rows[1]));
if (api.cleanReportActionLabel(row.events[1]) !== "Add Liquidity") throw new Error(api.cleanReportActionLabel(row.events[1]));
if (api.displayActionsForRow(row).map(item => typeof item === "string" ? item : item.label).join("|") !== "trade|Add LP") throw new Error("bad AMM chips");
const lifecycle = api.positionLifecycleForRows([row], [earn]);
if (!lifecycle.some(item => item.sourceEntityLabel === "trades")) throw new Error(JSON.stringify(lifecycle));
if (!lifecycle.some(item => item.sourceEntityLabel === "ammMints")) throw new Error(JSON.stringify(lifecycle));
if (!lifecycle.some(item => item.sourceEntityLabel === "earn-merkl-rewards")) throw new Error(JSON.stringify(lifecycle));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_odolo_reward_claims_are_dated_transaction_rows(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyClaimTest = { eventFromOdoloClaim, groupEvents, rowMatchesActionFilter, cleanHistoryReportHeaders, cleanHistoryReportCsvRows, cleanTransactionAction, cleanTransactionAssetFlow, displayActionsForRow, state };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyClaimTest;
api.state.year = "custom";
api.state.dateFrom = "2026-06-01";
api.state.dateTo = "2026-06-10";
api.state.selectedChains = new Set(["berachain"]);
api.state.earn = { status: "ready", warnings: [], ledgers: {}, rewards: {}, prices: {} };
api.state.warnings = [];
const event = api.eventFromOdoloClaim("berachain", {
  txHash: "0xd9cc0f3511fed88a4acc086b98ef2c3b7d5903c5b5ba63975d906551123a5934",
  blockNumber: 21838308,
  timestamp: 1780691332,
  logIndex: 1,
  user: "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
  distributor: "0x79e6e932bf6686a4d357d7821e6e08835ba8a026",
  epoch: 56,
  amount: "282.340149793751586955",
  amountWei: "282340149793751586955",
});
const row = api.groupEvents([event])[0];
row.gas = { status: "ok", paidByWallet: true, nativeSymbol: "BERA", nativeAmountExact: "0.001", gasUsd: 1, historicalPrice: 2 };
if (!api.rowMatchesActionFilter(row, "claim")) throw new Error("Claim filter did not match oDOLO");
if (!api.rowMatchesActionFilter(row, "odoloClaim")) throw new Error("legacy Claim oDOLO filter did not match");
if (api.cleanTransactionAction(row) !== "Claim oDOLO") throw new Error(api.cleanTransactionAction(row));
if (api.cleanTransactionAssetFlow(row) !== "Claim oDOLO: Received 282.34014979 oDOLO") throw new Error(api.cleanTransactionAssetFlow(row));
if (api.displayActionsForRow(row).map(item => typeof item === "string" ? item : item.label).join("|") !== "odoloClaim") throw new Error("bad action chip");
const headers = api.cleanHistoryReportHeaders();
const rows = api.cleanHistoryReportCsvRows([row], []);
const actionIdx = headers.indexOf("action");
const sourceIdx = headers.indexOf("source");
const sourceEntityIdx = headers.indexOf("source_entity");
if (rows[0][actionIdx] !== "Claim oDOLO") throw new Error(JSON.stringify(rows[0]));
if (!rows[0][sourceIdx].includes("RewardClaimed")) throw new Error(JSON.stringify(rows[0]));
if (rows[0][sourceEntityIdx] !== "odoloRewardClaimEvents") throw new Error(JSON.stringify(rows[0]));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_odolo_claim_generator_discovers_all_reward_distributors(self):
        self.assertIn("from generate_reward_claim_events import main", self.claim_generator)
        self.assertIn("CHAIN_CONFIGS", self.reward_claim_generator)
        self.assertIn("BERA_ODOLO_DISTRIBUTORS", self.reward_claim_generator)
        self.assertIn("fetch_claim_distributors", self.reward_claim_generator)
        self.assertIn("liquidityMiningClaims", self.reward_claim_generator)
        self.assertIn('for direction in ("asc", "desc")', self.reward_claim_generator)
        self.assertIn("existing_distributors", self.reward_claim_generator)
        self.assertIn("event_distributors", self.reward_claim_generator)
        self.assertIn("resolve_distributor_tokens", self.reward_claim_generator)
        self.assertIn("apply_distributor_token_metadata", self.reward_claim_generator)
        self.assertIn("ARB_MIN_DISTRIBUTOR", self.reward_claim_generator)
        self.assertIn("ARB_OARB_DISTRIBUTOR", self.reward_claim_generator)
        self.assertIn('set((config.get("knownDistributorTokens") or {}).keys())', self.reward_claim_generator)
        self.assertIn("distributor_topics = [topic_address(distributor) for distributor in distributors]", self.reward_claim_generator)
        self.assertIn("topic_batches = [", self.reward_claim_generator)
        self.assertIn('"topics": [REWARD_CLAIMED_TOPIC, topic_batch]', self.reward_claim_generator)
        self.assertIn('distributor = decode_topic_address(topics[1])', self.reward_claim_generator)
        self.assertIn("CHAIN_OUTPUT_DIR", self.reward_claim_generator)
        self.assertIn("reward_claim_manifest", self.reward_claim_generator)
        self.assertIn("reward_claim_chain_payload", self.reward_claim_generator)
        self.assertIn("eventsShardedByChain", self.reward_claim_generator)
        self.assertIn('f"{chain_key}.json"', self.reward_claim_generator)
        self.assertIn('"chains": chains_payload', self.reward_claim_generator)
        self.assertIn("build_legacy_odolo_payload", self.reward_claim_generator)
        self.assertNotIn("REWARDS_DISTRIBUTOR =", self.reward_claim_generator)

    def test_reward_claim_generator_resolves_distributor_tokens(self):
        import generate_reward_claim_events as generator

        original_rpc = generator.rpc_request
        distributor = "0x1111111111111111111111111111111111111111"
        token = "0x2222222222222222222222222222222222222222"

        def word(value):
            return "0x" + value.rjust(64, "0")

        def bytes32(value):
            return "0x" + value.encode("utf-8").hex().ljust(64, "0")

        def fake_rpc(_rpc_urls, method, params, timeout=30):
            self.assertEqual(method, "eth_call")
            call = params[0]
            to = call["to"].lower()
            data = call["data"]
            if to == distributor and data == "0xfc0c546a":
                return word(token[2:])
            if to == token and data == generator.ERC20_SYMBOL_SELECTOR:
                return bytes32("MIN")
            if to == token and data == generator.ERC20_DECIMALS_SELECTOR:
                return word("c")
            if to == token and data == generator.ERC20_NAME_SELECTOR:
                return bytes32("Mineral")
            return "0x"

        try:
            generator.rpc_request = fake_rpc
            resolved = generator.resolve_distributor_token({"name": "Arbitrum", "rpcUrls": ["mock"], "token": {"symbol": "Reward", "address": "", "decimals": 18}}, distributor)
        finally:
            generator.rpc_request = original_rpc

        self.assertEqual(resolved["symbol"], "MIN")
        self.assertEqual(resolved["address"], token)
        self.assertEqual(resolved["decimals"], 12)

    def test_reward_claim_generator_resolves_oarb_selector(self):
        import generate_reward_claim_events as generator

        original_rpc = generator.rpc_request
        distributor = "0x3333333333333333333333333333333333333333"
        token = "0x4444444444444444444444444444444444444444"

        def word(value):
            return "0x" + value.rjust(64, "0")

        def bytes32(value):
            return "0x" + value.encode("utf-8").hex().ljust(64, "0")

        def fake_rpc(_rpc_urls, method, params, timeout=30):
            self.assertEqual(method, "eth_call")
            call = params[0]
            to = call["to"].lower()
            data = call["data"]
            if to == distributor and data == "0xfc0c546a":
                return "0x"
            if to == distributor and data == "0xe8616b24":
                return word(token[2:])
            if to == token and data == generator.ERC20_SYMBOL_SELECTOR:
                return bytes32("oARB")
            if to == token and data == generator.ERC20_DECIMALS_SELECTOR:
                return word("c")
            if to == token and data == generator.ERC20_NAME_SELECTOR:
                return bytes32("oARB Token")
            return "0x"

        try:
            generator.rpc_request = fake_rpc
            resolved = generator.resolve_distributor_token({"name": "Arbitrum", "rpcUrls": ["mock"], "token": {"symbol": "Reward", "address": "", "decimals": 18}}, distributor)
        finally:
            generator.rpc_request = original_rpc

        self.assertEqual(resolved["symbol"], "oARB")
        self.assertEqual(resolved["address"], token)
        self.assertEqual(resolved["decimals"], 12)

    def test_multi_chain_reward_claims_are_transaction_rows(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyRewardClaimTest = { eventFromRewardClaim, groupEvents, rowMatchesActionFilter, cleanHistoryReportHeaders, cleanHistoryReportCsvRows, cleanTransactionAction, cleanTransactionAssetFlow, displayActionsForRow, state };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyRewardClaimTest;
api.state.year = "custom";
api.state.dateFrom = "2024-05-10";
api.state.dateTo = "2024-05-11";
api.state.selectedChains = new Set(["arbitrum"]);
api.state.earn = { status: "ready", warnings: [], ledgers: {}, rewards: {}, prices: {} };
api.state.warnings = [];
const event = api.eventFromRewardClaim("arbitrum", {
  txHash: "0x226727970c33d916ae038af5268dc0b3135b6234a763a9430e8e07f6e967af8e",
  blockNumber: 209021381,
  timestamp: 1715359919,
  logIndex: 99,
  user: "0x6775bb983d384d60d4a1e390d36d53fe3d4c1d37",
  distributor: "0x2e3d10cc42227af0ce908f00c76ffe1de1728b4b",
  epoch: 0,
  amount: "31.720527777776524265",
  amountWei: "31720527777776524265",
  tokenSymbol: "MIN",
  tokenAddress: "0x946f4a316e8ae3c7fdcdf86e84496c3ee3fbf26d",
  tokenDecimals: 18,
});
const row = api.groupEvents([event])[0];
row.gas = { status: "ok", paidByWallet: true, nativeSymbol: "ETH", nativeAmountExact: "0.0001", gasUsd: 0.2, historicalPrice: 2000 };
if (!api.rowMatchesActionFilter(row, "claim")) throw new Error("Claim filter did not match reward claim");
if (!api.rowMatchesActionFilter(row, "rewardClaim")) throw new Error("legacy Claim Rewards filter did not match");
if (api.cleanTransactionAction(row) !== "Claim Rewards") throw new Error(api.cleanTransactionAction(row));
if (api.cleanTransactionAssetFlow(row) !== "Claim Rewards: Received 31.72052777 MIN") throw new Error(api.cleanTransactionAssetFlow(row));
if (api.displayActionsForRow(row).map(item => typeof item === "string" ? item : item.label).join("|") !== "rewardClaim") throw new Error("bad action chip");
const headers = api.cleanHistoryReportHeaders();
const rows = api.cleanHistoryReportCsvRows([row], []);
const actionIdx = headers.indexOf("action");
const sourceIdx = headers.indexOf("source");
const sourceEntityIdx = headers.indexOf("source_entity");
const reviewIdx = headers.indexOf("review_status");
if (rows[0][actionIdx] !== "Claim Rewards") throw new Error(JSON.stringify(rows[0]));
if (!rows[0][sourceIdx].includes("RewardClaimed")) throw new Error(JSON.stringify(rows[0]));
if (rows[0][sourceEntityIdx] !== "rewardClaimEvents") throw new Error(JSON.stringify(rows[0]));
if (rows[0][reviewIdx] !== "income_candidate") throw new Error(JSON.stringify(rows[0]));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_reward_level_updates_are_transaction_rows(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyRewardLevelTest = { eventFromRewardLevelUpdate, groupEvents, rowMatchesActionFilter, cleanHistoryReportHeaders, cleanHistoryReportCsvRows, cleanTransactionAction, cleanTransactionAssetFlow, displayActionsForRow, state };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyRewardLevelTest;
api.state.year = "custom";
api.state.dateFrom = "2025-03-26";
api.state.dateTo = "2025-03-27";
api.state.selectedChains = new Set(["arbitrum"]);
api.state.earn = { status: "ready", warnings: [], ledgers: {}, rewards: {}, prices: {} };
api.state.warnings = [];
const event = api.eventFromRewardLevelUpdate("arbitrum", {
  id: "999",
  requestId: "999",
  level: 7,
  isFulfilled: true,
  user: { id: "0x46c38155354791da32e8ca26fad5d84ff1bde945" },
  initiateTransaction: {
    id: "0x0bc171bf22018315556183ad21253d9c0fccbb1c9208dc01e7e95297084a6702",
    timestamp: "1743036427",
    blockNumber: "318191936",
  },
  fulfilmentTransaction: {
    id: "0x42341f9e5bb891ea1bdfe5204a7b5d1453b5806a270c6078495684d1e94698f0",
    timestamp: "1743036437",
    blockNumber: "318191976",
  },
}, "initiated", "initiateTransaction");
event.sourceEntity = "liquidityMiningLevelUpdateRequests";
const row = api.groupEvents([event])[0];
row.gas = { status: "ok", paidByWallet: true, nativeSymbol: "ETH", nativeAmountExact: "0.0001", gasUsd: 0.2, historicalPrice: 2000 };
if (!api.rowMatchesActionFilter(row, "rewardLevelUpdate")) throw new Error("Reward level filter did not match");
if (api.cleanTransactionAction(row) !== "Reward Level Update") throw new Error(api.cleanTransactionAction(row));
if (api.cleanTransactionAssetFlow(row) !== "Reward level 7 requested") throw new Error(api.cleanTransactionAssetFlow(row));
if (api.displayActionsForRow(row).map(item => typeof item === "string" ? item : item.label).join("|") !== "rewardLevelUpdate") throw new Error("bad action chip");
const headers = api.cleanHistoryReportHeaders();
const rows = api.cleanHistoryReportCsvRows([row], []);
const actionIdx = headers.indexOf("action");
const sourceEntityIdx = headers.indexOf("source_entity");
const reviewIdx = headers.indexOf("review_status");
if (rows[0][actionIdx] !== "Reward Level Update") throw new Error(JSON.stringify(rows[0]));
if (rows[0][sourceEntityIdx] !== "liquidityMiningLevelUpdateRequests") throw new Error(JSON.stringify(rows[0]));
if (rows[0][reviewIdx] !== "ok") throw new Error(JSON.stringify(rows[0]));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_detail_rows_hide_wallet_noise_and_strip_repeated_action_labels(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyDetailTest = { detailHtml, detailDisplayEventsForRow, detailEventFlowLabel, eventMetaHtml, state };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyDetailTest;
api.state.address = "0x28da3dde285d8f1f87b2d858f89961bb8b9af180";
api.state.year = "custom";
const row = {
  key: "berachain:0x658938af8085375fc9f354dfa0f55191b961fcbb3c5ce4522058146487d50d7d",
  chainKey: "berachain",
  txHash: "0x658938af8085375fc9f354dfa0f55191b961fcbb3c5ce4522058146487d50d7d",
  events: [
    {
      action: "transfer",
      role: "out",
      label: "-45.86206606 DOLO",
      account: "0",
      fromAccount: "9275123456804",
      toAccount: "0",
      isSelfTransfer: true,
      counterparty: "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
    },
    {
      action: "zap",
      label: "Zap: -4.10137745 WBERA / +45.86206606 DOLO",
      account: "0",
      legs: [{ direction: "out", symbol: "WBERA", amount: "4.10137745" }, { direction: "in", symbol: "DOLO", amount: "45.86206606" }],
    },
  ],
  gas: {
    status: "ok",
    nativeAmount: "0.0000002",
    nativeAmountExact: "0.0000002",
    nativeSymbol: "BERA",
    historicalPrice: 0.3834,
    gasUsd: 0.00000008,
    from: "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
  },
};
const displayEvents = api.detailDisplayEventsForRow(row);
if (displayEvents.length !== 1 || displayEvents[0].action !== "zap") throw new Error(JSON.stringify(displayEvents));
const html = api.detailHtml(row);
if (!html.includes("What happened") || !html.includes("Gas fee")) throw new Error(html);
if (html.includes("Counterparty")) throw new Error(html);
if (html.includes("0x28da")) throw new Error(html);
if (html.includes("Tx sender:")) throw new Error(html);
if (html.includes("This wallet")) throw new Error(html);
if (html.includes("Fee allocation") || html.includes("Dolomite events")) throw new Error(html);
if (html.includes("Acct ")) throw new Error(html);
if (html.includes("<strong>Transfer</strong>")) throw new Error(html);
if (!html.includes("<strong>Zap</strong>")) throw new Error(html);
if (!html.includes("event-action-stack")) throw new Error(html);
if (!html.includes("event-open-link icon-only")) throw new Error(html);
if (html.includes("<span>Tx</span>")) throw new Error(html);
if (html.includes("event-detail-label\">Dolomite account</span>")) throw new Error(html);
if (html.includes("Main account (0)")) throw new Error(html);
if (!html.includes("<span class=\"event-flow\">Paid 4.1014 WBERA -&gt; Received 45.8621 DOLO</span>")) throw new Error(html);
if (html.includes("Zap:")) throw new Error(html);
const transferOnlyHtml = api.detailHtml({ ...row, events: [row.events[0]] });
if (!transferOnlyHtml.includes("event-detail-label\">Internal route</span>")) throw new Error(transferOnlyHtml);
if (!transferOnlyHtml.includes("Subaccount 9275...6804 -&gt; Main account (0)")) throw new Error(transferOnlyHtml);
if (transferOnlyHtml.includes("Acct ")) throw new Error(transferOnlyHtml);
const externalMeta = api.eventMetaHtml(row, {
  action: "transfer",
  role: "out",
  account: "2485123454437",
  counterparty: "0x1111111111111111111111111111111111111111",
});
if (!externalMeta.includes("event-detail-label\">To</span>")) throw new Error(externalMeta);
if (!externalMeta.includes("https://berascan.com/address/0x1111111111111111111111111111111111111111")) throw new Error(externalMeta);
const externalSenderHtml = api.detailHtml({
  ...row,
  gas: { ...row.gas, from: "0x1111111111111111111111111111111111111111" },
});
if (!externalSenderHtml.includes("Tx sender:</b>")) throw new Error(externalSenderHtml);
if (!externalSenderHtml.includes("https://berascan.com/address/0x1111111111111111111111111111111111111111")) throw new Error(externalSenderHtml);
const claimFlow = api.detailEventFlowLabel({
  action: "vesting",
  vestingFlowLabel: "Claimed veDOLO",
  vestingPositionId: "3903",
  vestingPaymentAmount: "1071.7999454",
  vestingOTokenAmount: "67055.73715564",
  label: "Claim veDOLO #3903 (paid 1071.7999454 USDC)",
});
if (!claimFlow.startsWith("veDOLO #3903 · paid")) throw new Error(claimFlow);
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_clean_csv_asset_flow_prefers_primary_execution(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { cleanTransactionAction, cleanTransactionAssetFlow, compactTransactionAssetPreview, cleanHistoryReportHeaders, cleanHistoryReportCsvRows, cleanEarnAssetFlow, assetActivitySummaryForRows, eventFromZap, eventFromVesting, groupEvents, displayActionsForRow, rowMatchesActionFilter, activityGroupForEvent, cleanReportActionLabel, state };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const tx = {
  actions: new Set(["transfer", "deposit", "trade", "zap"]),
  events: [
    { action: "transfer", role: "out", label: "-29324.71667819 USD1", legs: [{ direction: "out", symbol: "USD1", amount: "29324.71667819" }] },
    { action: "deposit", label: "-29331.40307 USDC", legs: [{ direction: "out", symbol: "USDC", amount: "29331.40307" }] },
    { action: "trade", taxCategory: "swap", label: "-29331.40307 USDC / +29324.71667819 USD1", legs: [{ direction: "out", symbol: "USDC", amount: "29331.40307" }, { direction: "in", symbol: "USD1", amount: "29324.71667819" }] },
    { action: "zap", taxCategory: "zap", label: "Zap: -29331.40307 USDC / +29324.71667819 USD1", legs: [{ direction: "out", symbol: "USDC", amount: "29331.40307" }, { direction: "in", symbol: "USD1", amount: "29324.71667819" }] },
  ],
};
const transferOut = {
  actions: new Set(["transfer"]),
  events: [
    { action: "transfer", role: "out", label: "-29324.71667819 USD1", legs: [{ direction: "out", symbol: "USD1", amount: "29324.71667819" }] },
  ],
};
const transferIn = {
  actions: new Set(["transfer"]),
  events: [
    { action: "transfer", role: "in", label: "+45.86206606 DOLO", legs: [{ direction: "in", symbol: "DOLO", amount: "45.86206606" }] },
  ],
};
const duplicateZapEvents = [
  { chainKey: "arbitrum", txHash: "0xf444", timestamp: 1, blockNumber: "420533702", action: "zap", taxCategory: "zap", usd: 3.11424138421, legs: [{ direction: "out", symbol: "WETH", amount: "0.001" }, { direction: "in", symbol: "DAI", amount: "3.110365522148733691" }] },
  { chainKey: "arbitrum", txHash: "0xf444", timestamp: 1, blockNumber: "420533702", action: "trade", taxCategory: "swap", usd: 3.11424138421, legs: [{ direction: "out", symbol: "WETH", amount: "0.001" }, { direction: "in", symbol: "DAI", amount: "3.110365522148733691" }] },
  { chainKey: "arbitrum", txHash: "0xf444", timestamp: 1, blockNumber: "420533702", action: "transfer", usd: 3.109142433114459144, legs: [{ direction: "in", symbol: "DAI", amount: "3.110365522148733691" }] },
  { chainKey: "arbitrum", txHash: "0xf444", timestamp: 1, blockNumber: "420533702", action: "transfer", usd: 3.11424138421, legs: [{ direction: "out", symbol: "WETH", amount: "0.001" }] },
];
const reversedPathZapEvents = [
  { chainKey: "arbitrum", txHash: "0xe694", timestamp: 1, blockNumber: "420533831", action: "zap", taxCategory: "zap", asset: "WETH / DAI", usd: 1.122568792975754457, taxNote: "Routed zap candidate.", legs: [{ direction: "out", symbol: "WETH", amount: "1.110365179408656327", usd: "1.109928550509157461" }, { direction: "in", symbol: "DAI", amount: "0.000360463000288759", usd: "1.122568792975754457" }] },
  { chainKey: "arbitrum", txHash: "0xe694", timestamp: 1, blockNumber: "420533831", action: "trade", taxCategory: "swap", usd: 1.109928550509157461, legs: [{ direction: "out", symbol: "DAI", amount: "1.110365179408656327", usd: "1.109928550509157461" }, { direction: "in", symbol: "WETH", amount: "0.000360463000288759", usd: "1.122568792975754457" }] },
];
const multiHopZapEvents = [
  { chainKey: "arbitrum", txHash: "0xmulti", timestamp: 1, blockNumber: "420533999", action: "zap", taxCategory: "zap", asset: "WETH / USDC", usd: 100, taxNote: "Routed zap candidate.", legs: [{ direction: "out", symbol: "WETH", amount: "100", usd: "100" }, { direction: "in", symbol: "USDC", amount: "0.03", usd: "99.7" }] },
  { chainKey: "arbitrum", txHash: "0xmulti", timestamp: 1, blockNumber: "420533999", action: "trade", taxCategory: "swap", usd: 100, legs: [{ direction: "out", symbol: "USDC", amount: "100", usd: "100" }, { direction: "in", symbol: "DAI", amount: "99.8", usd: "99.8" }] },
  { chainKey: "arbitrum", txHash: "0xmulti", timestamp: 1, blockNumber: "420533999", action: "trade", taxCategory: "swap", usd: 99.7, legs: [{ direction: "out", symbol: "DAI", amount: "99.8", usd: "99.8" }, { direction: "in", symbol: "WETH", amount: "0.03", usd: "99.7" }] },
];
const noUsdZapEvents = [
  { chainKey: "arbitrum", txHash: "0xnousd", timestamp: 1, blockNumber: "420534000", action: "zap", taxCategory: "zap", asset: "WETH / DAI", usd: 0, taxNote: "Routed zap candidate.", legs: [{ direction: "out", symbol: "WETH", tokenAddress: "0x1111111111111111111111111111111111111111", amount: "0.5", usd: "" }, { direction: "in", symbol: "DAI", tokenAddress: "0x2222222222222222222222222222222222222222", amount: "1550", usd: "" }] },
  { chainKey: "arbitrum", txHash: "0xnousd", timestamp: 1, blockNumber: "420534000", action: "trade", taxCategory: "swap", usd: 0, legs: [{ direction: "out", symbol: "WETH", tokenAddress: "0x1111111111111111111111111111111111111111", amount: "0.5", usd: "" }, { direction: "in", symbol: "DAI", tokenAddress: "0x2222222222222222222222222222222222222222", amount: "1550", usd: "" }] },
];
const labelOnlyZap = {
  actions: new Set(["zap"]),
  events: [{ action: "zap", taxCategory: "zap", label: "Zap: -1 USDC/+0.1 WETH" }],
};
const zapFromSubgraph = sandbox.__historyTest.eventFromZap("arbitrum", {
  id: "0xf444-19",
  transaction: { id: "0xf444", timestamp: "1768210081", blockNumber: "420533702" },
  marginAccount: { accountNumber: "1" },
  tokenPath: [{ symbol: "WETH" }, { symbol: "DAI" }],
  amountInToken: "0.001",
  amountOutToken: "3.110365522148733691",
  amountInUSD: "3.11424138421",
  amountOutUSD: "3.109142433114459144",
});
const groupedZap = sandbox.__historyTest.groupEvents(duplicateZapEvents)[0];
const groupedCorrectedZap = sandbox.__historyTest.groupEvents(reversedPathZapEvents)[0];
const correctedZapEvent = groupedCorrectedZap.events.find(event => event.action === "zap");
const groupedMultiHopZap = sandbox.__historyTest.groupEvents(multiHopZapEvents)[0];
const multiHopZapEvent = groupedMultiHopZap.events.find(event => event.action === "zap");
const groupedNoUsdZap = sandbox.__historyTest.groupEvents(noUsdZapEvents)[0];
const noUsdZapEvent = groupedNoUsdZap.events.find(event => event.action === "zap");
const deposit = {
  actions: new Set(["deposit"]),
  events: [{ action: "deposit", label: "+0.00160549 WBTC", legs: [{ direction: "out", symbol: "WBTC", amount: "0.00160549" }] }],
};
const reward = { action: "EARN reward summary", assetInAmount: "12.50000000", assetInSymbol: "DOLO" };
const reviewOnlyYield = { action: "EARN yield summary", reviewFlag: "needs_review", assetInAmount: "5.00000000", assetInSymbol: "USDC" };
const vesting = {
  actions: new Set(["vesting"]),
    events: [{
    action: "vesting",
    role: "in",
    vestingFlowLabel: "Opened",
    vestingPositionId: "3903",
    vestingKind: "oDOLO/DOLO vesting pair",
    vestingStatus: "ACTIVE",
    vestingOTokenAmount: "67055.73715564",
    vestingPaymentAmount: "120.5",
    legs: [
      { direction: "in", symbol: "oDOLO/DOLO vesting pair", amount: "67055.73715564" },
    ],
  }],
};
const vestingOut = {
  actions: new Set(["vesting"]),
    events: [{
    action: "vesting",
    role: "out",
    vestingFlowLabel: "Claimed veDOLO",
    vestingPositionId: "3903",
    vestingKind: "oDOLO/DOLO vesting pair",
    vestingStatus: "CLOSED",
    vestingOTokenAmount: "67055.73715564",
    vestingPaymentAmount: "1071.799454",
    legs: [
      { direction: "out", symbol: "oDOLO/DOLO vesting pair", amount: "67055.73715564" },
    ],
  }],
};
const mixedVesting = {
  actions: new Set(["trade", "vesting"]),
  events: [
    { action: "trade", taxCategory: "swap", label: "-1 USDC / +1 DAI", legs: [{ direction: "out", symbol: "USDC", amount: "1" }, { direction: "in", symbol: "DAI", amount: "1" }] },
    vesting.events[0],
  ],
};
const mixedVestingChip = sandbox.__historyTest.displayActionsForRow(mixedVesting).find(action => action && typeof action === "object");
const pairFromSubgraph = sandbox.__historyTest.eventFromVesting("berachain", {
  serialId: "pair-1",
  transaction: { id: "0xpair", timestamp: "1768210081", blockNumber: "123" },
  fromEffectiveUser: null,
  toEffectiveUser: { id: "0xabc" },
  vestingPosition: { positionId: "3903", oTokenAmount: "67055.73715564", paymentAmountWei: "1071.799454", status: "ACTIVE" },
}, "in");
const claimFromSubgraph = sandbox.__historyTest.eventFromVesting("berachain", {
  serialId: "claim-1",
  transaction: { id: "0xclaim", timestamp: "1768210081", blockNumber: "124" },
  fromEffectiveUser: { id: "0xabc" },
  toEffectiveUser: null,
  vestingPosition: { positionId: "3903", oTokenAmount: "67055.73715564", paymentAmountWei: "1071.799454", status: "CLOSED" },
}, "out");
const internalMoveFromSubgraph = sandbox.__historyTest.eventFromVesting("berachain", {
  serialId: "move-1",
  transaction: { id: "0xmove", timestamp: "1768210081", blockNumber: "125" },
  fromEffectiveUser: { id: "0xabc" },
  toEffectiveUser: { id: "0xABC" },
  vestingPosition: { positionId: "3904", oTokenAmount: "1000", paymentAmountWei: "0", status: "ACTIVE" },
}, "out");
const combinedVesting = {
  actions: new Set(["vesting"]),
  events: [pairFromSubgraph, claimFromSubgraph],
};
const combinedVestingChipLabels = sandbox.__historyTest.displayActionsForRow(combinedVesting)
  .filter(action => action && typeof action === "object")
  .map(action => action.label)
  .join("|");
const results = {
  txAction: sandbox.__historyTest.cleanTransactionAction(tx),
  txFlow: sandbox.__historyTest.cleanTransactionAssetFlow(tx),
  transferOutFlow: sandbox.__historyTest.cleanTransactionAssetFlow(transferOut),
  transferOutTablePreview: sandbox.__historyTest.compactTransactionAssetPreview(transferOut),
  transferInTablePreview: sandbox.__historyTest.compactTransactionAssetPreview(transferIn),
  depositFlow: sandbox.__historyTest.cleanTransactionAssetFlow(deposit),
  vestingAction: sandbox.__historyTest.cleanTransactionAction(vesting),
  vestingOutAction: sandbox.__historyTest.cleanTransactionAction(vestingOut),
  mixedVestingAction: sandbox.__historyTest.cleanTransactionAction(mixedVesting),
  vestingFlow: sandbox.__historyTest.cleanTransactionAssetFlow(vesting),
  vestingOutFlow: sandbox.__historyTest.cleanTransactionAssetFlow(vestingOut),
  vestingTablePreview: sandbox.__historyTest.compactTransactionAssetPreview(vesting),
  vestingOutTablePreview: sandbox.__historyTest.compactTransactionAssetPreview(vestingOut),
  vestingChipLabel: sandbox.__historyTest.displayActionsForRow(vesting)[0].label,
  vestingOutChipLabel: sandbox.__historyTest.displayActionsForRow(vestingOut)[0].label,
  mixedVestingChipLabel: mixedVestingChip?.label,
  pairFromSubgraphLabel: sandbox.__historyTest.cleanReportActionLabel(pairFromSubgraph),
  pairFromSubgraphTaxCategory: pairFromSubgraph.taxCategory,
  pairFromSubgraphActivityGroup: sandbox.__historyTest.activityGroupForEvent(pairFromSubgraph),
  pairFromSubgraphLegs: pairFromSubgraph.legs.map(leg => `${leg.direction}:${leg.symbol}:${leg.amount}`).join("|"),
  claimFromSubgraphLabel: sandbox.__historyTest.cleanReportActionLabel(claimFromSubgraph),
  claimFromSubgraphTaxCategory: claimFromSubgraph.taxCategory,
  claimFromSubgraphActivityGroup: sandbox.__historyTest.activityGroupForEvent(claimFromSubgraph),
  claimFromSubgraphLegs: claimFromSubgraph.legs.map(leg => `${leg.direction}:${leg.symbol}:${leg.amount}`).join("|"),
  internalMoveLabel: sandbox.__historyTest.cleanReportActionLabel(internalMoveFromSubgraph),
  internalMoveFlow: sandbox.__historyTest.cleanTransactionAssetFlow({ actions: new Set(["vesting"]), events: [internalMoveFromSubgraph] }),
  internalMoveTaxCategory: internalMoveFromSubgraph.taxCategory,
  internalMoveReviewReason: internalMoveFromSubgraph.reviewReason,
  combinedVestingAction: sandbox.__historyTest.cleanTransactionAction(combinedVesting),
  combinedVestingFlow: sandbox.__historyTest.cleanTransactionAssetFlow(combinedVesting),
  combinedVestingTablePreview: sandbox.__historyTest.compactTransactionAssetPreview(combinedVesting),
  combinedVestingChipLabels,
  vestingPairFilter: sandbox.__historyTest.rowMatchesActionFilter(vesting, "vestingPair"),
  vestingClaimFilter: sandbox.__historyTest.rowMatchesActionFilter(vestingOut, "vestingClaim"),
  vestingPairDoesNotMatchClaim: sandbox.__historyTest.rowMatchesActionFilter(vesting, "vestingClaim"),
  rewardFlow: sandbox.__historyTest.cleanEarnAssetFlow(reward),
  reviewOnlyYieldFlow: sandbox.__historyTest.cleanEarnAssetFlow(reviewOnlyYield),
  zapLabel: zapFromSubgraph.label,
  zapFlow: sandbox.__historyTest.cleanTransactionAssetFlow({ actions: new Set(["zap"]), events: [zapFromSubgraph] }),
  groupedZapUsd: groupedZap.usdVolume,
  correctedZapLabel: correctedZapEvent.label,
  correctedZapFlow: sandbox.__historyTest.cleanTransactionAssetFlow(groupedCorrectedZap),
  correctedZapRouteEvidence: correctedZapEvent.routeEvidence,
  correctedZapRouteConfidence: correctedZapEvent.routeMatchConfidence,
  correctedZapRouteHopCount: correctedZapEvent.routeHopCount,
  correctedZapRouteTokenPath: correctedZapEvent.routeTokenPath,
  correctedZapNote: correctedZapEvent.taxNote,
  correctedZapUsd: groupedCorrectedZap.usdVolume,
  multiHopZapFlow: sandbox.__historyTest.cleanTransactionAssetFlow(groupedMultiHopZap),
  multiHopZapRouteEvidence: multiHopZapEvent.routeEvidence,
  multiHopZapRouteConfidence: multiHopZapEvent.routeMatchConfidence,
  multiHopZapRouteHopCount: multiHopZapEvent.routeHopCount,
  noUsdZapFlow: sandbox.__historyTest.cleanTransactionAssetFlow(groupedNoUsdZap),
  labelOnlyZapFlow: sandbox.__historyTest.cleanTransactionAssetFlow(labelOnlyZap),
  txTablePreview: sandbox.__historyTest.compactTransactionAssetPreview(tx),
  correctedZapTablePreview: sandbox.__historyTest.compactTransactionAssetPreview(groupedCorrectedZap),
  noUsdZapRouteEvidence: noUsdZapEvent.routeEvidence,
  noUsdZapRouteConfidence: noUsdZapEvent.routeMatchConfidence,
  swapFilterMatchesTradeZap: sandbox.__historyTest.rowMatchesActionFilter(tx, "swap"),
  legacyTradeFilterMatchesTradeZap: sandbox.__historyTest.rowMatchesActionFilter(tx, "trade"),
  legacyZapFilterMatchesTradeZap: sandbox.__historyTest.rowMatchesActionFilter(tx, "zap"),
};
if (results.txAction !== "Zap") throw new Error(JSON.stringify(results));
if (results.txFlow !== "Paid 29331.40307 USDC -> Received 29324.71667819 USD1") throw new Error(JSON.stringify(results));
if (results.txFlow.includes(";") || results.txFlow.includes(" / ")) throw new Error(JSON.stringify(results));
if (results.txTablePreview !== "29331.40 USDC -> 29324.72 USD1") throw new Error(JSON.stringify(results));
if (results.transferOutFlow !== "Transfer out: 29324.71667819 USD1") throw new Error(JSON.stringify(results));
if (results.transferOutTablePreview !== "-29324.72 USD1") throw new Error(JSON.stringify(results));
if (results.transferInTablePreview !== "+45.8621 DOLO") throw new Error(JSON.stringify(results));
if (results.zapLabel !== "Zap: -0.001 WETH / +3.11036552 DAI") throw new Error(JSON.stringify(results));
if (results.zapFlow !== "Paid 0.001 WETH -> Received 3.11036552 DAI") throw new Error(JSON.stringify(results));
if (Math.abs(results.groupedZapUsd - 3.11424138421) > 0.0000001) throw new Error(JSON.stringify(results));
if (results.correctedZapLabel !== "Zap: -1.11036517 DAI / +0.00036046 WETH") throw new Error(JSON.stringify(results));
if (results.correctedZapFlow !== "Paid 1.11036517 DAI -> Received 0.00036046 WETH") throw new Error(JSON.stringify(results));
if (results.correctedZapTablePreview !== "1.11 DAI -> 0.00036046 WETH") throw new Error(JSON.stringify(results));
if (results.correctedZapRouteEvidence !== "paired_trade_reconciled") throw new Error(JSON.stringify(results));
if (results.correctedZapRouteConfidence !== "high") throw new Error(JSON.stringify(results));
if (results.correctedZapRouteHopCount !== 1) throw new Error(JSON.stringify(results));
if (results.correctedZapRouteTokenPath !== "WETH / DAI") throw new Error(JSON.stringify(results));
if (!results.correctedZapNote.includes("reconciled against the paired trade event")) throw new Error(JSON.stringify(results));
if (Math.abs(results.correctedZapUsd - 1.122568792975754457) > 0.0000001) throw new Error(JSON.stringify(results));
if (results.multiHopZapFlow !== "Paid 100 USDC -> Received 0.03 WETH") throw new Error(JSON.stringify(results));
if (results.multiHopZapRouteEvidence !== "paired_trade_reconciled") throw new Error(JSON.stringify(results));
if (results.multiHopZapRouteConfidence !== "high") throw new Error(JSON.stringify(results));
if (results.multiHopZapRouteHopCount !== 2) throw new Error(JSON.stringify(results));
if (results.noUsdZapFlow !== "Paid 0.5 WETH -> Received 1550 DAI") throw new Error(JSON.stringify(results));
if (results.labelOnlyZapFlow !== "Paid 1 USDC -> Received 0.1 WETH") throw new Error(JSON.stringify(results));
if (results.noUsdZapRouteEvidence !== "paired_trade_verified") throw new Error(JSON.stringify(results));
if (results.noUsdZapRouteConfidence !== "medium") throw new Error(JSON.stringify(results));
if (!results.swapFilterMatchesTradeZap || !results.legacyTradeFilterMatchesTradeZap || !results.legacyZapFilterMatchesTradeZap) throw new Error(JSON.stringify(results));
if (results.depositFlow !== "0.00160549 WBTC") throw new Error(JSON.stringify(results));
if (results.vestingAction !== "Pair oDOLO + DOLO") throw new Error(JSON.stringify(results));
if (results.vestingOutAction !== "Claim veDOLO") throw new Error(JSON.stringify(results));
if (results.mixedVestingAction !== "Trade; Pair oDOLO + DOLO") throw new Error(JSON.stringify(results));
if (results.vestingFlow !== "Pair oDOLO + DOLO #3903 (paired 67055.73715564 oDOLO + 67055.73715564 DOLO), current status: Active") throw new Error(JSON.stringify(results));
if (results.vestingOutFlow !== "Claim veDOLO #3903 (paid 1071.799454 USDC; used 67055.73715564 paired oDOLO/DOLO; received veDOLO lock), current status: Closed") throw new Error(JSON.stringify(results));
if (results.vestingTablePreview !== "67,055.74 oDOLO + DOLO -> Position #3903") throw new Error(JSON.stringify(results));
if (results.vestingOutTablePreview !== "veDOLO #3903 · paid 1,071.80 USDC") throw new Error(JSON.stringify(results));
if (results.vestingChipLabel !== "PAIR") throw new Error(JSON.stringify(results));
if (results.vestingOutChipLabel !== "CLAIM") throw new Error(JSON.stringify(results));
if (results.mixedVestingChipLabel !== "PAIR") throw new Error(JSON.stringify(results));
if (results.pairFromSubgraphLabel !== "Pair oDOLO + DOLO") throw new Error(JSON.stringify(results));
if (results.pairFromSubgraphTaxCategory !== "odolo_dolo_pair") throw new Error(JSON.stringify(results));
if (results.pairFromSubgraphActivityGroup !== "odolo_dolo_pair") throw new Error(JSON.stringify(results));
if (results.pairFromSubgraphLegs !== "out:oDOLO:67055.73715564|out:DOLO:67055.73715564") throw new Error(JSON.stringify(results));
if (results.claimFromSubgraphLabel !== "Claim veDOLO") throw new Error(JSON.stringify(results));
if (results.claimFromSubgraphTaxCategory !== "vedolo_claim") throw new Error(JSON.stringify(results));
if (results.claimFromSubgraphActivityGroup !== "vedolo_claim") throw new Error(JSON.stringify(results));
if (results.claimFromSubgraphLegs !== "out:oDOLO/DOLO vesting pair:67055.73715564|in:veDOLO lock:67055.73715564|out:USDC exercise cost:1071.799454") throw new Error(JSON.stringify(results));
if (results.internalMoveLabel !== "Move veDOLO position") throw new Error(JSON.stringify(results));
if (results.internalMoveFlow !== "Move veDOLO position #3904 (vesting pair 1000 oDOLO/DOLO), current status: Active") throw new Error(JSON.stringify(results));
if (results.internalMoveTaxCategory !== "vesting_internal_move") throw new Error(JSON.stringify(results));
if (results.internalMoveReviewReason !== "odolo_vedolo_internal_move_review") throw new Error(JSON.stringify(results));
if (results.combinedVestingAction !== "Pair oDOLO + DOLO; Claim veDOLO") throw new Error(JSON.stringify(results));
if (!results.combinedVestingFlow.includes("Pair oDOLO + DOLO #3903") || !results.combinedVestingFlow.includes("Claim veDOLO #3903")) throw new Error(JSON.stringify(results));
if (!results.combinedVestingTablePreview.includes("Position #3903") || !results.combinedVestingTablePreview.includes("veDOLO #3903")) throw new Error(JSON.stringify(results));
if (results.combinedVestingChipLabels !== "PAIR|CLAIM") throw new Error(JSON.stringify(results));
if (!results.vestingPairFilter || !results.vestingClaimFilter || results.vestingPairDoesNotMatchClaim) throw new Error(JSON.stringify(results));
if (results.rewardFlow !== "Reward candidate: 12.5 DOLO") throw new Error(JSON.stringify(results));
if (results.reviewOnlyYieldFlow !== "Review-only yield candidate: 5 USDC") throw new Error(JSON.stringify(results));
const reportApi = sandbox.__historyTest;
reportApi.state.year = "custom";
reportApi.state.dateFrom = "2026-01-01";
reportApi.state.dateTo = "2026-05-26";
reportApi.state.selectedChains = new Set(["ethereum"]);
reportApi.state.earn = { status: "ready", warnings: [], ledgers: {}, rewards: {}, prices: {} };
reportApi.state.warnings = [];
const groupedTx = sandbox.__historyTest.groupEvents(tx.events.map((event, index) => ({
  ...event,
  chainKey: "ethereum",
  txHash: "0xzapcsv",
  timestamp: 1768210081,
  blockNumber: "123",
  sourceEntity: event.action === "zap" ? "zaps" : event.action === "trade" ? "trades" : event.action === "deposit" ? "deposits" : "transfers",
})))[0];
groupedTx.gas = { status: "ok", paidByWallet: true, nativeSymbol: "ETH", nativeAmountExact: "0.001", gasUsd: "2.5", historicalPrice: 2500 };
const headers = reportApi.cleanHistoryReportHeaders();
const csvRows = reportApi.cleanHistoryReportCsvRows([groupedTx], []);
const csv = Object.fromEntries(headers.map((header, index) => [header, csvRows[0][index]]));
if (csvRows.length !== 1) throw new Error(JSON.stringify(csvRows));
if (csv.action !== "Zap") throw new Error(JSON.stringify(csv));
if (csv.asset_flow !== "Paid 29331.40307 USDC -> Received 29324.71667819 USD1") throw new Error(JSON.stringify(csv));
if (csv.source_entity !== "transfers; deposits; trades; zaps") throw new Error(JSON.stringify(csv));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_borrow_and_repay_are_classified_from_account_balance_replay(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { groupEvents, displayActionsForRow, rowMatchesActionFilter, cleanTransactionAction, cleanReportActionLabel, activityGroupForEvent, compactTransactionAssetPreview, parBalanceToTokenBalance, scaledBigIntToDecimal, decimalToScaledBigInt };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const scale = 10n ** 18n;
const key = "arbitrum:1:0xusdc";
const debtKey = "arbitrum:1:0xdebt";
const withdraw = amount => ({
  chainKey: "arbitrum",
  txHash: `0xwithdraw${amount}`,
  timestamp: Number(amount),
  blockNumber: "1",
  action: "withdraw",
  role: "user",
  account: "1",
  legs: [{ direction: "in", symbol: "USDC", tokenAddress: "0xusdc", amount: String(amount), rawAmount: String(amount) }],
});
const deposit = amount => ({
  chainKey: "arbitrum",
  txHash: `0xdeposit${amount}`,
  timestamp: Number(amount),
  blockNumber: "2",
  action: "deposit",
  role: "user",
  account: "1",
  legs: [{ direction: "out", symbol: "USDC", tokenAddress: "0xusdc", amount: String(amount), rawAmount: String(amount) }],
});
const selfTransfer = (amount, fromAccount, toAccount, timestamp) => ({
  chainKey: "arbitrum",
  txHash: `0xtransfer${fromAccount}${toAccount}${amount}`,
  timestamp,
  blockNumber: String(timestamp),
  action: "transfer",
  role: "out",
  account: fromAccount,
  fromAccount,
  toAccount,
  isSelfTransfer: true,
  legs: [{ direction: "out", symbol: "USDC", tokenAddress: "0xusdc", amount: String(amount), rawAmount: String(amount) }],
});
const selfTransferAsset = (amount, fromAccount, toAccount, timestamp, symbol, tokenAddress) => ({
  ...selfTransfer(amount, fromAccount, toAccount, timestamp),
  legs: [{ direction: "out", symbol, tokenAddress, amount: String(amount), rawAmount: String(amount) }],
});
const openBorrow = sandbox.__historyTest.groupEvents([withdraw(10)], {
  currentBalanceReplay: true,
  currentBalances: new Map([[key, -10n * scale]]),
})[0];
const increaseBorrow = sandbox.__historyTest.groupEvents([withdraw(5)], {
  currentBalanceReplay: true,
  currentBalances: new Map([[key, -15n * scale]]),
})[0];
const repay = sandbox.__historyTest.groupEvents([deposit(5)], {
  currentBalanceReplay: true,
  currentBalances: new Map([[key, -5n * scale]]),
})[0];
const closeBorrow = sandbox.__historyTest.groupEvents([deposit(10)], {
  currentBalanceReplay: true,
  currentBalances: new Map([[key, 0n]]),
})[0];
const addCollateral = sandbox.__historyTest.groupEvents([deposit(5)], {
  currentBalanceReplay: true,
  currentBalances: new Map([[key, 5n * scale], [debtKey, -20n * scale]]),
})[0];
const openBorrowTransfer = sandbox.__historyTest.groupEvents([selfTransfer(10, "1", "0", 30)], {
  currentBalanceReplay: true,
  currentBalances: new Map([[key, -10n * scale]]),
})[0];
const closeBorrowTransfer = sandbox.__historyTest.groupEvents([selfTransfer(10, "0", "1", 40)], {
  currentBalanceReplay: true,
  currentBalances: new Map([[key, 0n]]),
})[0];
const routeAccount = "53264417164200931625493956231501517131794473210820855151423986246727225441839";
const borrowPositionOpenMarker = (account, txHash = "0xopenposition", timestamp = 45) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "borrowPositionOpen",
  role: "user",
  account,
  label: "Borrow position opened",
  asset: "Borrow position",
  legs: [],
});
const borrowPositionCloseMarker = (account, txHash = "0xcloseposition", timestamp = 65) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "borrowPositionClose",
  role: "user",
  account,
  label: "Borrow position closed",
  asset: "Borrow position",
  legs: [],
});
const openBorrowPositionWithCollateral = sandbox.__historyTest.groupEvents([
  selfTransferAsset("0.01", "0", routeAccount, 45, "WETH", "0xweth"),
  borrowPositionOpenMarker(routeAccount, "0xtransfer0" + routeAccount + "0.01", 45),
], {
  currentBalanceReplay: true,
  currentBalances: new Map([
    ["arbitrum:" + routeAccount + ":0xusdc", -2n * scale],
    ["arbitrum:" + routeAccount + ":0xweth", 10_000_000_000_000_000n],
  ]),
})[0];
const openBorrowRouteTransfer = sandbox.__historyTest.groupEvents([selfTransfer("0.001", routeAccount, "0", 50)], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xusdc", -1_000_000_000_000_000n]]),
})[0];
const increaseBorrowRouteTransfer = sandbox.__historyTest.groupEvents([selfTransfer("0.001", routeAccount, "0", 55)], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xusdc", -3_000_000_000_000_000n]]),
})[0];
const withdrawCollateralRouteTransfer = sandbox.__historyTest.groupEvents([selfTransferAsset("0.005", routeAccount, "0", 58, "WETH", "0xweth")], {
  currentBalanceReplay: true,
  currentBalances: new Map([
    ["arbitrum:" + routeAccount + ":0xusdc", -2n * scale],
    ["arbitrum:" + routeAccount + ":0xweth", 5_000_000_000_000_000n],
  ]),
})[0];
const closeBorrowRouteTransfer = sandbox.__historyTest.groupEvents([selfTransfer("0.001", "0", routeAccount, 60)], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xusdc", 0n]]),
})[0];
const closeBorrowRouteWithMarker = sandbox.__historyTest.groupEvents([
  selfTransfer("0.001", "0", routeAccount, 65),
  borrowPositionCloseMarker(routeAccount, "0xtransfer0" + routeAccount + "0.001", 65),
], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xusdc", 0n]]),
})[0];
const zapWithBorrow = {
  actions: new Set(["zap", "withdraw"]),
  semanticActions: new Set(["openBorrow"]),
  events: [{ action: "zap", taxCategory: "zap", legs: [{ direction: "out", symbol: "USDC", amount: "10" }, { direction: "in", symbol: "WETH", amount: "0.003" }] }],
};
const vestingClaimRow = {
  actions: new Set(["vesting"]),
  semanticActions: new Set(),
  events: [{ action: "vesting", vestingFlowLabel: "Claimed veDOLO" }],
};
const results = {
  openAction: sandbox.__historyTest.cleanTransactionAction(openBorrow),
  openChip: sandbox.__historyTest.displayActionsForRow(openBorrow).join("|"),
  openFilterBorrow: sandbox.__historyTest.rowMatchesActionFilter(openBorrow, "borrow"),
  openFilterWithdraw: sandbox.__historyTest.rowMatchesActionFilter(openBorrow, "withdraw"),
  openEventLabel: sandbox.__historyTest.cleanReportActionLabel(openBorrow.events[0]),
  openGroup: sandbox.__historyTest.activityGroupForEvent(openBorrow.events[0]),
  openBefore: openBorrow.events[0].borrowBalanceBefore,
  openAfter: openBorrow.events[0].borrowBalanceAfter,
  increaseAction: sandbox.__historyTest.cleanTransactionAction(increaseBorrow),
  repayAction: sandbox.__historyTest.cleanTransactionAction(repay),
  repayChip: sandbox.__historyTest.displayActionsForRow(repay).join("|"),
  repayFilter: sandbox.__historyTest.rowMatchesActionFilter(repay, "repay"),
  repayDepositFilter: sandbox.__historyTest.rowMatchesActionFilter(repay, "deposit"),
  repayEventLabel: sandbox.__historyTest.cleanReportActionLabel(repay.events[0]),
  repayBefore: repay.events[0].borrowBalanceBefore,
  repayAfter: repay.events[0].borrowBalanceAfter,
  closeAction: sandbox.__historyTest.cleanTransactionAction(closeBorrow),
  closeChip: sandbox.__historyTest.displayActionsForRow(closeBorrow).join("|"),
  closeFilter: sandbox.__historyTest.rowMatchesActionFilter(closeBorrow, "repay"),
  closeGroup: sandbox.__historyTest.activityGroupForEvent(closeBorrow.events[0]),
  closePreview: sandbox.__historyTest.compactTransactionAssetPreview(closeBorrow),
  openCollateralPositionAction: sandbox.__historyTest.cleanTransactionAction(openBorrowPositionWithCollateral),
  openCollateralPositionChip: sandbox.__historyTest.displayActionsForRow(openBorrowPositionWithCollateral).join("|"),
  openCollateralPositionBorrowFilter: sandbox.__historyTest.rowMatchesActionFilter(openBorrowPositionWithCollateral, "borrow"),
  openCollateralPositionAddCollateralFilter: sandbox.__historyTest.rowMatchesActionFilter(openBorrowPositionWithCollateral, "addCollateral"),
  openCollateralPositionPreview: sandbox.__historyTest.compactTransactionAssetPreview(openBorrowPositionWithCollateral),
  addCollateralAction: sandbox.__historyTest.cleanTransactionAction(addCollateral),
  addCollateralChip: sandbox.__historyTest.displayActionsForRow(addCollateral).join("|"),
  addCollateralFilter: sandbox.__historyTest.rowMatchesActionFilter(addCollateral, "addCollateral"),
  addCollateralDepositFilter: sandbox.__historyTest.rowMatchesActionFilter(addCollateral, "deposit"),
  addCollateralEventLabel: sandbox.__historyTest.cleanReportActionLabel(addCollateral.events[0]),
  addCollateralGroup: sandbox.__historyTest.activityGroupForEvent(addCollateral.events[0]),
  addCollateralBefore: addCollateral.events[0].borrowBalanceBefore,
  addCollateralAfter: addCollateral.events[0].borrowBalanceAfter,
  openTransferAction: sandbox.__historyTest.cleanTransactionAction(openBorrowTransfer),
  openTransferChip: sandbox.__historyTest.displayActionsForRow(openBorrowTransfer).join("|"),
  openTransferBorrowFilter: sandbox.__historyTest.rowMatchesActionFilter(openBorrowTransfer, "borrow"),
  openTransferTransferFilter: sandbox.__historyTest.rowMatchesActionFilter(openBorrowTransfer, "transfer"),
  openTransferEventLabel: sandbox.__historyTest.cleanReportActionLabel(openBorrowTransfer.events[0]),
  openTransferGroup: sandbox.__historyTest.activityGroupForEvent(openBorrowTransfer.events[0]),
  openTransferBefore: openBorrowTransfer.events[0].borrowBalanceBefore,
  openTransferAfter: openBorrowTransfer.events[0].borrowBalanceAfter,
  closeTransferAction: sandbox.__historyTest.cleanTransactionAction(closeBorrowTransfer),
  closeTransferChip: sandbox.__historyTest.displayActionsForRow(closeBorrowTransfer).join("|"),
  closeTransferRepayFilter: sandbox.__historyTest.rowMatchesActionFilter(closeBorrowTransfer, "repay"),
  closeTransferTransferFilter: sandbox.__historyTest.rowMatchesActionFilter(closeBorrowTransfer, "transfer"),
  closeTransferGroup: sandbox.__historyTest.activityGroupForEvent(closeBorrowTransfer.events[0]),
  openRouteTransferAction: sandbox.__historyTest.cleanTransactionAction(openBorrowRouteTransfer),
  openRouteTransferChip: sandbox.__historyTest.displayActionsForRow(openBorrowRouteTransfer).join("|"),
  openRouteTransferBorrowFilter: sandbox.__historyTest.rowMatchesActionFilter(openBorrowRouteTransfer, "borrow"),
  openRouteTransferTransferFilter: sandbox.__historyTest.rowMatchesActionFilter(openBorrowRouteTransfer, "transfer"),
  openRouteTransferPreview: sandbox.__historyTest.compactTransactionAssetPreview(openBorrowRouteTransfer),
  increaseRouteTransferAction: sandbox.__historyTest.cleanTransactionAction(increaseBorrowRouteTransfer),
  increaseRouteTransferChip: sandbox.__historyTest.displayActionsForRow(increaseBorrowRouteTransfer).join("|"),
  increaseRouteTransferBorrowFilter: sandbox.__historyTest.rowMatchesActionFilter(increaseBorrowRouteTransfer, "borrow"),
  increaseRouteTransferRepayFilter: sandbox.__historyTest.rowMatchesActionFilter(increaseBorrowRouteTransfer, "repay"),
  withdrawCollateralAction: sandbox.__historyTest.cleanTransactionAction(withdrawCollateralRouteTransfer),
  withdrawCollateralChip: sandbox.__historyTest.displayActionsForRow(withdrawCollateralRouteTransfer).join("|"),
  withdrawCollateralFilter: sandbox.__historyTest.rowMatchesActionFilter(withdrawCollateralRouteTransfer, "withdrawCollateral"),
  withdrawCollateralTransferFilter: sandbox.__historyTest.rowMatchesActionFilter(withdrawCollateralRouteTransfer, "transfer"),
  withdrawCollateralPreview: sandbox.__historyTest.compactTransactionAssetPreview(withdrawCollateralRouteTransfer),
  closeRouteTransferAction: sandbox.__historyTest.cleanTransactionAction(closeBorrowRouteTransfer),
  closeRouteTransferChip: sandbox.__historyTest.displayActionsForRow(closeBorrowRouteTransfer).join("|"),
  closeRouteTransferRepayFilter: sandbox.__historyTest.rowMatchesActionFilter(closeBorrowRouteTransfer, "repay"),
  closeRouteTransferTransferFilter: sandbox.__historyTest.rowMatchesActionFilter(closeBorrowRouteTransfer, "transfer"),
  closeRouteTransferPreview: sandbox.__historyTest.compactTransactionAssetPreview(closeBorrowRouteTransfer),
  closeRouteWithMarkerAction: sandbox.__historyTest.cleanTransactionAction(closeBorrowRouteWithMarker),
  closeRouteWithMarkerChip: sandbox.__historyTest.displayActionsForRow(closeBorrowRouteWithMarker).join("|"),
  closeRouteWithMarkerRepayFilter: sandbox.__historyTest.rowMatchesActionFilter(closeBorrowRouteWithMarker, "repay"),
  indexedDebt: sandbox.__historyTest.scaledBigIntToDecimal(sandbox.__historyTest.parBalanceToTokenBalance("-5", { borrowIndex: "2", supplyIndex: "1" })),
  indexedSupply: sandbox.__historyTest.scaledBigIntToDecimal(sandbox.__historyTest.parBalanceToTokenBalance("3", { borrowIndex: "2", supplyIndex: "1.5" })),
  scientificSmall: sandbox.__historyTest.scaledBigIntToDecimal(sandbox.__historyTest.decimalToScaledBigInt("1e-6")),
  scientificLarge: sandbox.__historyTest.scaledBigIntToDecimal(sandbox.__historyTest.decimalToScaledBigInt("1.2e3")),
  zapBorrowAction: sandbox.__historyTest.cleanTransactionAction(zapWithBorrow),
  zapBorrowChips: sandbox.__historyTest.displayActionsForRow(zapWithBorrow).join("|"),
  vestingClaimSpecificFilter: sandbox.__historyTest.rowMatchesActionFilter(vestingClaimRow, "vestingClaim"),
  vestingClaimGenericFilter: sandbox.__historyTest.rowMatchesActionFilter(vestingClaimRow, "claim"),
};
if (results.openAction !== "Open Borrow") throw new Error(JSON.stringify(results));
if (results.openChip !== "openBorrow") throw new Error(JSON.stringify(results));
if (!results.openFilterBorrow || results.openFilterWithdraw) throw new Error(JSON.stringify(results));
if (results.openEventLabel !== "Open Borrow") throw new Error(JSON.stringify(results));
if (results.openGroup !== "borrow") throw new Error(JSON.stringify(results));
if (results.openBefore !== "0" || results.openAfter !== "-10") throw new Error(JSON.stringify(results));
if (results.increaseAction !== "Borrow") throw new Error(JSON.stringify(results));
if (results.repayAction !== "Repay") throw new Error(JSON.stringify(results));
if (results.repayChip !== "repay") throw new Error(JSON.stringify(results));
if (!results.repayFilter || results.repayDepositFilter) throw new Error(JSON.stringify(results));
if (results.repayEventLabel !== "Repay") throw new Error(JSON.stringify(results));
if (results.repayBefore !== "-10" || results.repayAfter !== "-5") throw new Error(JSON.stringify(results));
if (results.closeAction !== "Repay") throw new Error(JSON.stringify(results));
if (results.closeChip !== "repay") throw new Error(JSON.stringify(results));
if (!results.closeFilter || results.closeGroup !== "repay") throw new Error(JSON.stringify(results));
if (results.closePreview !== "10 USDC") throw new Error(JSON.stringify(results));
if (results.openCollateralPositionAction !== "Open Borrow") throw new Error(JSON.stringify(results));
if (results.openCollateralPositionChip !== "openBorrow") throw new Error(JSON.stringify(results));
if (!results.openCollateralPositionBorrowFilter || results.openCollateralPositionAddCollateralFilter) throw new Error(JSON.stringify(results));
if (results.openCollateralPositionPreview !== "0.01 WETH") throw new Error(JSON.stringify(results));
if (results.addCollateralAction !== "Add Collateral") throw new Error(JSON.stringify(results));
if (results.addCollateralChip !== "addCollateral") throw new Error(JSON.stringify(results));
if (!results.addCollateralFilter || results.addCollateralDepositFilter) throw new Error(JSON.stringify(results));
if (results.addCollateralEventLabel !== "Add Collateral") throw new Error(JSON.stringify(results));
if (results.addCollateralGroup !== "dolomite_in") throw new Error(JSON.stringify(results));
if (results.addCollateralBefore !== "0" || results.addCollateralAfter !== "5") throw new Error(JSON.stringify(results));
if (results.openTransferAction !== "Open Borrow") throw new Error(JSON.stringify(results));
if (results.openTransferChip !== "openBorrow") throw new Error(JSON.stringify(results));
if (!results.openTransferBorrowFilter || results.openTransferTransferFilter) throw new Error(JSON.stringify(results));
if (results.openTransferEventLabel !== "Open Borrow") throw new Error(JSON.stringify(results));
if (results.openTransferGroup !== "borrow") throw new Error(JSON.stringify(results));
if (results.openTransferBefore !== "0" || results.openTransferAfter !== "-10") throw new Error(JSON.stringify(results));
if (results.closeTransferAction !== "Repay") throw new Error(JSON.stringify(results));
if (results.closeTransferChip !== "repay") throw new Error(JSON.stringify(results));
if (!results.closeTransferRepayFilter || results.closeTransferTransferFilter) throw new Error(JSON.stringify(results));
if (results.closeTransferGroup !== "repay") throw new Error(JSON.stringify(results));
if (results.openRouteTransferAction !== "Open Borrow") throw new Error(JSON.stringify(results));
if (results.openRouteTransferChip !== "openBorrow") throw new Error(JSON.stringify(results));
if (!results.openRouteTransferBorrowFilter || results.openRouteTransferTransferFilter) throw new Error(JSON.stringify(results));
if (results.openRouteTransferPreview !== "0.001 USDC") throw new Error(JSON.stringify(results));
if (results.increaseRouteTransferAction !== "Borrow") throw new Error(JSON.stringify(results));
if (results.increaseRouteTransferChip !== "borrow") throw new Error(JSON.stringify(results));
if (!results.increaseRouteTransferBorrowFilter || results.increaseRouteTransferRepayFilter) throw new Error(JSON.stringify(results));
if (results.withdrawCollateralAction !== "Withdraw Collateral") throw new Error(JSON.stringify(results));
if (results.withdrawCollateralChip !== "withdrawCollateral") throw new Error(JSON.stringify(results));
if (!results.withdrawCollateralFilter || results.withdrawCollateralTransferFilter) throw new Error(JSON.stringify(results));
if (results.withdrawCollateralPreview !== "0.005 WETH") throw new Error(JSON.stringify(results));
if (results.closeRouteTransferAction !== "Repay") throw new Error(JSON.stringify(results));
if (results.closeRouteTransferChip !== "repay") throw new Error(JSON.stringify(results));
if (!results.closeRouteTransferRepayFilter || results.closeRouteTransferTransferFilter) throw new Error(JSON.stringify(results));
if (results.closeRouteTransferPreview !== "0.001 USDC") throw new Error(JSON.stringify(results));
if (results.closeRouteWithMarkerAction !== "Close Borrow") throw new Error(JSON.stringify(results));
if (results.closeRouteWithMarkerChip !== "closeBorrow") throw new Error(JSON.stringify(results));
if (!results.closeRouteWithMarkerRepayFilter) throw new Error(JSON.stringify(results));
if (results.indexedDebt !== "-10" || results.indexedSupply !== "4.5") throw new Error(JSON.stringify(results));
if (results.scientificSmall !== "0.000001" || results.scientificLarge !== "1200") throw new Error(JSON.stringify(results));
if (results.zapBorrowAction !== "Zap; Open Borrow" || results.zapBorrowChips !== "zap|openBorrow") throw new Error(JSON.stringify(results));
if (!results.vestingClaimSpecificFilter || !results.vestingClaimGenericFilter) throw new Error(JSON.stringify(results));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_borrow_position_receipt_logs_reclassify_missing_subgraph_lifecycle_rows(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyReceiptTest = { groupEvents, displayActionsForRow, rowMatchesActionFilter, cleanTransactionAction, compactTransactionAssetPreview, applyBorrowReceiptSemanticsForRow };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyReceiptTest;
const wallet = "0x28da3dde285d8f1f87b2d858f89961bb8b9af180";
const routeAccount = "53264947907898141993957179932880619725268172546596567937770629149662327972903";
const abiWord = value => BigInt(value).toString(16).padStart(64, "0");
const topicAddress = address => "0x" + String(address).toLowerCase().replace(/^0x/, "").padStart(64, "0");
const topicUint = value => "0x" + abiWord(value);
const openBorrowInput = "0xbb0a6fa5" + abiWord(0) + abiWord(routeAccount) + abiWord(0) + abiWord(1_000_000_000_000_000n) + abiWord(1);
const openBorrowLog = () => ({
  address: "0xe43638797513ef7a6d326a95e8647d86d2f5a099",
  topics: [
    "0xfd9156bd20ce24a786c761efe71a3931de038c1f2620c1bb4720609bc742b58e",
    topicAddress(wallet),
    topicUint(routeAccount),
  ],
  data: "0x",
});
const closeBorrowLog = () => ({
  address: "0x6bd780e7fdf01d77e4d475c821f1e7ae05409072",
  topics: [
    "0x21281f8d59117d0399dc467dbdd321538ceffe3225e80e2bd4de6f1b3355cbc7",
    topicAddress(wallet),
    topicAddress(wallet),
  ],
  data: "0x" + abiWord(0) + abiWord(routeAccount) + abiWord(0) + abiWord(1_000_000_000_000_000n) + abiWord(1),
});
const makeTransferToRoute = () => ({
  chainKey: "arbitrum",
  txHash: "0xdd6ce55745394eddfe23b0ba0543ccd323e7ce4622785f352f5db985f65b3d2c",
  timestamp: 1782243862,
  blockNumber: "476606014",
  action: "transfer",
  role: "out",
  account: "0",
  fromAccount: "0",
  toAccount: routeAccount,
  isSelfTransfer: true,
  legs: [{ direction: "out", symbol: "WETH", tokenAddress: "0xweth", amount: "0.001", rawAmount: "0.001" }],
});
const transferToRoute = makeTransferToRoute();
const row = api.groupEvents([transferToRoute], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xweth", 1_000_000_000_000_000n]]),
})[0];
row.receiptClassificationPending = true;
const before = {
  action: api.cleanTransactionAction(row),
  chips: api.displayActionsForRow(row).join("|"),
  transferFilter: api.rowMatchesActionFilter(row, "transfer"),
};
api.applyBorrowReceiptSemanticsForRow(row, {
  logs: [openBorrowLog()],
}, {
  from: wallet,
  to: "0xe43638797513ef7a6d326a95e8647d86d2f5a099",
  input: "0xbb0a6fa5" + "0".repeat(320),
}, wallet);
row.gas = { status: "ok" };
const after = {
  before,
  action: api.cleanTransactionAction(row),
  chips: api.displayActionsForRow(row).join("|"),
  borrowFilter: api.rowMatchesActionFilter(row, "borrow"),
  transferFilter: api.rowMatchesActionFilter(row, "transfer"),
  preview: api.compactTransactionAssetPreview(row),
  semantic: Array.from(row.semanticActions || []),
};
if (after.before.chips !== "classificationPending" || after.before.transferFilter) throw new Error(JSON.stringify(after));
if (after.action !== "Open Borrow") throw new Error(JSON.stringify(after));
if (after.chips !== "openBorrow") throw new Error(JSON.stringify(after));
if (!after.borrowFilter || after.transferFilter) throw new Error(JSON.stringify(after));
if (after.preview !== "0.001 WETH") throw new Error(JSON.stringify(after));
if (after.semantic.join("|") !== "openBorrow") throw new Error(JSON.stringify(after));
const calldataOnlyRow = api.groupEvents([makeTransferToRoute()], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xweth", 1_000_000_000_000_000n]]),
})[0];
api.applyBorrowReceiptSemanticsForRow(calldataOnlyRow, { logs: [] }, {
  from: wallet,
  to: "0xe43638797513ef7a6d326a95e8647d86d2f5a099",
  input: openBorrowInput,
}, wallet);
calldataOnlyRow.gas = { status: "ok" };
const calldataOnly = {
  action: api.cleanTransactionAction(calldataOnlyRow),
  chips: api.displayActionsForRow(calldataOnlyRow).join("|"),
  borrowFilter: api.rowMatchesActionFilter(calldataOnlyRow, "borrow"),
  transferFilter: api.rowMatchesActionFilter(calldataOnlyRow, "transfer"),
  semantic: Array.from(calldataOnlyRow.semanticActions || []),
};
if (calldataOnly.action !== "Open Borrow") throw new Error(JSON.stringify(calldataOnly));
if (calldataOnly.chips !== "openBorrow") throw new Error(JSON.stringify(calldataOnly));
if (!calldataOnly.borrowFilter || calldataOnly.transferFilter) throw new Error(JSON.stringify(calldataOnly));
if (calldataOnly.semantic.join("|") !== "openBorrow") throw new Error(JSON.stringify(calldataOnly));
const mixedLifecycleRow = api.groupEvents([makeTransferToRoute()], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xweth", 1_000_000_000_000_000n]]),
})[0];
api.applyBorrowReceiptSemanticsForRow(mixedLifecycleRow, { logs: [openBorrowLog(), closeBorrowLog()] }, {
  from: wallet,
  to: "0xe43638797513ef7a6d326a95e8647d86d2f5a099",
  input: openBorrowInput,
}, wallet);
mixedLifecycleRow.gas = { status: "ok" };
const mixedLifecycle = {
  action: api.cleanTransactionAction(mixedLifecycleRow),
  chips: api.displayActionsForRow(mixedLifecycleRow).join("|"),
  borrowFilter: api.rowMatchesActionFilter(mixedLifecycleRow, "borrow"),
  repayFilter: api.rowMatchesActionFilter(mixedLifecycleRow, "repay"),
  semantic: Array.from(mixedLifecycleRow.semanticActions || []),
};
if (mixedLifecycle.action !== "Open Borrow") throw new Error(JSON.stringify(mixedLifecycle));
if (mixedLifecycle.chips !== "openBorrow") throw new Error(JSON.stringify(mixedLifecycle));
if (!mixedLifecycle.borrowFilter || mixedLifecycle.repayFilter) throw new Error(JSON.stringify(mixedLifecycle));
if (mixedLifecycle.semantic.join("|") !== "openBorrow") throw new Error(JSON.stringify(mixedLifecycle));
const internalZapRouteRow = {
  chainKey: "ethereum",
  txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
  timestamp: 1776672875,
  blockNumber: "24919843",
  actions: new Set(["zap"]),
  semanticActions: new Set(),
  events: [
    {
      chainKey: "ethereum",
      txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
      timestamp: 1776672875,
      blockNumber: "24919843",
      action: "zap",
      role: "user",
      account: "0",
      legs: [
        { direction: "out", symbol: "USDC", tokenAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount: "29331.40307", rawAmount: "29331.40307" },
        { direction: "in", symbol: "USD1", tokenAddress: "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", amount: "29324.716678195844195284", rawAmount: "29324.716678195844195284" },
      ],
    },
    {
      chainKey: "ethereum",
      txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
      timestamp: 1776672875,
      blockNumber: "24919843",
      action: "transfer",
      role: "out",
      account: "0",
      fromAccount: "0",
      toAccount: routeAccount,
      isSelfTransfer: true,
      legs: [{ direction: "out", symbol: "USDC", tokenAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount: "29331.40307", rawAmount: "29331.40307" }],
    },
  ],
};
api.applyBorrowReceiptSemanticsForRow(internalZapRouteRow, { logs: [closeBorrowLog()] }, {
  from: wallet,
  to: "0xd6c1b15716742689c5b33c19c78d9d2a1494bf33",
  input: "0x5f974be9" + "0".repeat(128),
}, wallet);
internalZapRouteRow.gas = { status: "ok" };
const internalZapRoute = {
  action: api.cleanTransactionAction(internalZapRouteRow),
  chips: api.displayActionsForRow(internalZapRouteRow).join("|"),
  borrowFilter: api.rowMatchesActionFilter(internalZapRouteRow, "borrow"),
  repayFilter: api.rowMatchesActionFilter(internalZapRouteRow, "repay"),
  swapFilter: api.rowMatchesActionFilter(internalZapRouteRow, "swap"),
  semantic: Array.from(internalZapRouteRow.semanticActions || []),
};
if (internalZapRoute.action !== "Zap") throw new Error(JSON.stringify(internalZapRoute));
if (internalZapRoute.chips !== "zap") throw new Error(JSON.stringify(internalZapRoute));
if (!internalZapRoute.swapFilter || internalZapRoute.borrowFilter || internalZapRoute.repayFilter) throw new Error(JSON.stringify(internalZapRoute));
if (internalZapRoute.semantic.length !== 0) throw new Error(JSON.stringify(internalZapRoute));
const makeInternalSwapRouteEvents = () => [
  {
    chainKey: "ethereum",
    txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
    timestamp: 1776672875,
    blockNumber: "24919843",
    action: "transfer",
    role: "out",
    account: "0",
    fromAccount: "0",
    toAccount: routeAccount,
    isSelfTransfer: true,
    legs: [{ direction: "out", symbol: "USDC", tokenAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount: "29331.40307", rawAmount: "29331.40307" }],
  },
  {
    chainKey: "ethereum",
    txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
    timestamp: 1776672875,
    blockNumber: "24919843",
    action: "trade",
    role: "taker",
    account: routeAccount,
    legs: [
      { direction: "out", symbol: "USDC", tokenAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount: "29331.40307", rawAmount: "29331.40307" },
      { direction: "in", symbol: "USD1", tokenAddress: "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", amount: "29324.716678195844195284", rawAmount: "29324.716678195844195284" },
    ],
  },
  {
    chainKey: "ethereum",
    txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
    timestamp: 1776672875,
    blockNumber: "24919843",
    action: "transfer",
    role: "out",
    account: routeAccount,
    fromAccount: routeAccount,
    toAccount: "0",
    isSelfTransfer: true,
    legs: [{ direction: "out", symbol: "USD1", tokenAddress: "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", amount: "29324.716678195844195284", rawAmount: "29324.716678195844195284" }],
  },
  {
    chainKey: "ethereum",
    txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
    timestamp: 1776672875,
    blockNumber: "24919843",
    action: "zap",
    role: "user",
    account: "0",
    legs: [
      { direction: "out", symbol: "USDC", tokenAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount: "29331.40307", rawAmount: "29331.40307" },
      { direction: "in", symbol: "USD1", tokenAddress: "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", amount: "29324.716678195844195284", rawAmount: "29324.716678195844195284" },
    ],
  },
];
const groupedInternalSwapRoute = api.groupEvents(makeInternalSwapRouteEvents(), {
  currentBalanceReplay: true,
  currentBalances: new Map([["ethereum:0:0xexistingdebt", -1_000_000_000_000_000_000n]]),
})[0];
groupedInternalSwapRoute.gas = { status: "ok" };
const groupedInternalSwap = {
  action: api.cleanTransactionAction(groupedInternalSwapRoute),
  chips: api.displayActionsForRow(groupedInternalSwapRoute).join("|"),
  borrowFilter: api.rowMatchesActionFilter(groupedInternalSwapRoute, "borrow"),
  repayFilter: api.rowMatchesActionFilter(groupedInternalSwapRoute, "repay"),
  addCollateralFilter: api.rowMatchesActionFilter(groupedInternalSwapRoute, "addCollateral"),
  swapFilter: api.rowMatchesActionFilter(groupedInternalSwapRoute, "swap"),
  semantic: Array.from(groupedInternalSwapRoute.semanticActions || []),
};
if (groupedInternalSwap.action !== "Zap") throw new Error(JSON.stringify(groupedInternalSwap));
if (groupedInternalSwap.chips !== "zap") throw new Error(JSON.stringify(groupedInternalSwap));
if (!groupedInternalSwap.swapFilter || groupedInternalSwap.borrowFilter || groupedInternalSwap.repayFilter || groupedInternalSwap.addCollateralFilter) throw new Error(JSON.stringify(groupedInternalSwap));
if (groupedInternalSwap.semantic.length !== 0) throw new Error(JSON.stringify(groupedInternalSwap));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_golden_transaction_classification_matrix_has_sources(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyGoldenTest = { groupEvents, displayActionsForRow, rowMatchesActionFilter, cleanTransactionAction, compactTransactionAssetPreview, applyBorrowReceiptSemanticsForRow, classificationSourceForRow, eventMetaBlockHtml, evidenceRowPayload };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyGoldenTest;
const wallet = "0x28da3dde285d8f1f87b2d858f89961bb8b9af180";
const scale = 10n ** 18n;
const routeAccount = "53264947907898141993957179932880619725268172546596567937770629149662327972903";
const abiWord = value => BigInt(value).toString(16).padStart(64, "0");
const topicAddress = address => "0x" + String(address).toLowerCase().replace(/^0x/, "").padStart(64, "0");
const topicUint = value => "0x" + abiWord(value);
const openBorrowInput = "0xbb0a6fa5" + abiWord(0) + abiWord(routeAccount) + abiWord(0) + abiWord(1_000_000_000_000_000n) + abiWord(1);
const openBorrowLog = () => ({
  topics: [
    "0xfd9156bd20ce24a786c761efe71a3931de038c1f2620c1bb4720609bc742b58e",
    topicAddress(wallet),
    topicUint(routeAccount),
  ],
  data: "0x",
});
const transferToRoute = (txHash, symbol = "WETH", amount = "0.001", tokenAddress = "0xweth", timestamp = 1782243862) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "transfer",
  role: "out",
  account: "0",
  fromAccount: "0",
  toAccount: routeAccount,
  isSelfTransfer: true,
  legs: [{ direction: "out", symbol, tokenAddress, amount, rawAmount: amount }],
});
const deposit = (txHash, amount = "5", timestamp = 1785000000) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "deposit",
  role: "user",
  account: "1",
  legs: [{ direction: "out", symbol: "USDC", tokenAddress: "0xusdc", amount, rawAmount: amount }],
});
const routeDeposit = (txHash, amount = "0.002", timestamp = 1768210000) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "deposit",
  role: "user",
  account: routeAccount,
  legs: [{ direction: "out", symbol: "WETH", tokenAddress: "0xweth", amount, rawAmount: amount }],
});
const routeWithdraw = (txHash, amount = "0.00000000001671", timestamp = 1768210300) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "withdraw",
  role: "user",
  account: routeAccount,
  legs: [{ direction: "in", symbol: "WETH", tokenAddress: "0xweth", amount, rawAmount: amount }],
});
const borrowPositionOpenMarker = (txHash, timestamp = 1782243862) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "borrowPositionOpen",
  role: "user",
  account: routeAccount,
  label: "Borrow position opened",
  asset: "Borrow position",
  legs: [],
});
const borrowPositionCloseMarker = (txHash, timestamp = 1787000000) => ({
  chainKey: "arbitrum",
  txHash,
  timestamp,
  blockNumber: String(timestamp),
  action: "borrowPositionClose",
  role: "user",
  account: routeAccount,
  label: "Borrow position closed",
  asset: "Borrow position",
  legs: [],
});
const internalZapRouteEvents = () => [
  {
    chainKey: "ethereum",
    txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
    timestamp: 1776672875,
    blockNumber: "24919843",
    action: "trade",
    role: "taker",
    account: routeAccount,
    taxCategory: "swap",
    legs: [
      { direction: "out", symbol: "USDC", tokenAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount: "29331.40307", rawAmount: "29331.40307" },
      { direction: "in", symbol: "USD1", tokenAddress: "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", amount: "29324.716678195844195284", rawAmount: "29324.716678195844195284" },
    ],
  },
  {
    chainKey: "ethereum",
    txHash: "0xedb1d9bb02182e5238bb40ee8e8aadc6f1f51c91b3700c801081b8972d5ff9e7",
    timestamp: 1776672875,
    blockNumber: "24919843",
    action: "zap",
    role: "user",
    account: "0",
    legs: [
      { direction: "out", symbol: "USDC", tokenAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount: "29331.40307", rawAmount: "29331.40307" },
      { direction: "in", symbol: "USD1", tokenAddress: "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", amount: "29324.716678195844195284", rawAmount: "29324.716678195844195284" },
    ],
  },
];
const zapThenReceiptBorrowEvents = () => [
  {
    chainKey: "arbitrum",
    txHash: "0xzapreceiptborrow",
    timestamp: 1782243862,
    blockNumber: "1782243862",
    action: "zap",
    role: "user",
    account: "0",
    legs: [
      { direction: "out", symbol: "USDC", tokenAddress: "0xusdc", amount: "2", rawAmount: "2" },
      { direction: "in", symbol: "WETH", tokenAddress: "0xweth", amount: "0.001", rawAmount: "0.001" },
    ],
  },
  transferToRoute("0xzapreceiptborrow", "WETH", "0.001", "0xweth", 1782243862),
];
const rows = [];
const zapRoute = api.groupEvents(internalZapRouteEvents(), {
  currentBalanceReplay: true,
  currentBalances: new Map([["ethereum:0:0xexistingdebt", -1n * scale]]),
})[0];
rows.push({
  name: "internal routed swap stays zap",
  row: zapRoute,
  expectedAction: "Zap",
  expectedChips: "zap",
  expectedSource: "Swap route; no borrow lifecycle signal",
  filters: { swap: true, borrow: false, repay: false, addCollateral: false },
});
const receiptOpen = api.groupEvents([transferToRoute("0xdd6ce55745394eddfe23b0ba0543ccd323e7ce4622785f352f5db985f65b3d2c")], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xweth", 1_000_000_000_000_000n]]),
})[0];
api.applyBorrowReceiptSemanticsForRow(receiptOpen, { logs: [openBorrowLog()] }, {
  from: wallet,
  to: "0xe43638797513ef7a6d326a95e8647d86d2f5a099",
  input: openBorrowInput,
}, wallet);
rows.push({
  name: "receipt fallback opens routed borrow",
  row: receiptOpen,
  expectedAction: "Open Borrow",
  expectedChips: "openBorrow",
  expectedSource: "Borrow position receipt",
  filters: { borrow: true, transfer: false, repay: false, addCollateral: false },
});
const zapReceiptBorrow = api.groupEvents(zapThenReceiptBorrowEvents(), {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xweth", 1_000_000_000_000_000n]]),
})[0];
api.applyBorrowReceiptSemanticsForRow(zapReceiptBorrow, { logs: [openBorrowLog()] }, {
  from: wallet,
  to: "0xe43638797513ef7a6d326a95e8647d86d2f5a099",
  input: openBorrowInput,
}, wallet);
rows.push({
  name: "zap row source prefers borrow receipt over swap route",
  row: zapReceiptBorrow,
  expectedAction: "Zap; Open Borrow",
  expectedChips: "zap|openBorrow",
  expectedSource: "Borrow position receipt",
  expectedMetaSource: "Swap route; no borrow lifecycle signal",
  filters: { swap: true, borrow: true, transfer: false, addCollateral: false },
});
const openWithCollateral = api.groupEvents([
  transferToRoute("0x2aa642553a3fb144e0c432df278bfae1be80082e826ca47bc98b4e8ea55d7bc0", "WETH", "0.01", "0xweth", 1783000000),
  borrowPositionOpenMarker("0x2aa642553a3fb144e0c432df278bfae1be80082e826ca47bc98b4e8ea55d7bc0", 1783000000),
], {
  currentBalanceReplay: true,
  currentBalances: new Map([
    ["arbitrum:" + routeAccount + ":0xusdc", -2n * scale],
    ["arbitrum:" + routeAccount + ":0xweth", 10_000_000_000_000_000n],
  ]),
})[0];
rows.push({
  name: "open borrow with collateral remains open borrow",
  row: openWithCollateral,
  expectedAction: "Open Borrow",
  expectedChips: "openBorrow",
  expectedSource: "Borrow position lifecycle",
  expectedPreview: "0.01 WETH",
  filters: { borrow: true, addCollateral: false, transfer: false },
});
const partialRepay = api.groupEvents([deposit("0x38ae9fd3b916b4b4ed7bbeda5ff18d3f0987440d804de61b33f5cdc3ddb1822f", "5", 1785000000)], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:1:0xusdc", -5n * scale]]),
})[0];
rows.push({
  name: "partial repayment remains repay",
  row: partialRepay,
  expectedAction: "Repay",
  expectedChips: "repay",
  expectedSource: "Current balance replay",
  filters: { repay: true, deposit: false, borrow: false },
});
const openLifecycleDeposit = api.groupEvents([
  routeDeposit("0xc91e13e21d2a7c656dd49d27b7cb35b7b89710cec191b7716261daac196b5763"),
  borrowPositionOpenMarker("0xc91e13e21d2a7c656dd49d27b7cb35b7b89710cec191b7716261daac196b5763", 1768210000),
], {
  currentBalanceReplay: true,
  currentBalances: new Map([
    ["arbitrum:" + routeAccount + ":0xusdc", -2n * scale],
    ["arbitrum:" + routeAccount + ":0xweth", 2_000_000_000_000_000n],
  ]),
})[0];
rows.push({
  name: "open lifecycle deposit is open borrow not add collateral",
  row: openLifecycleDeposit,
  expectedAction: "Open Borrow",
  expectedChips: "openBorrow",
  expectedSource: "Borrow position lifecycle",
  expectedPreview: "0.002 WETH",
  filters: { borrow: true, addCollateral: false, deposit: false },
});
const closeWithMarker = api.groupEvents([
  transferToRoute("0xc4aa236e7fc1b9c74494cf7412f377ba25bd3ed10627675bb007b1c9842d3a93", "USDC", "0.001", "0xusdc", 1787000000),
  borrowPositionCloseMarker("0xc4aa236e7fc1b9c74494cf7412f377ba25bd3ed10627675bb007b1c9842d3a93", 1787000000),
], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xusdc", 0n]]),
})[0];
rows.push({
  name: "explicit close lifecycle remains close borrow",
  row: closeWithMarker,
  expectedAction: "Close Borrow",
  expectedChips: "closeBorrow",
  expectedSource: "Borrow position lifecycle",
  filters: { repay: true, transfer: false, borrow: false },
});
const closeLifecycleWithdraw = api.groupEvents([
  routeWithdraw("0x7184a10c65b6727564b5a861f5f3c495a9e8239694c49c9f6e889f13c6ebf064"),
  borrowPositionCloseMarker("0x7184a10c65b6727564b5a861f5f3c495a9e8239694c49c9f6e889f13c6ebf064", 1768210300),
], {
  currentBalanceReplay: true,
  currentBalances: new Map([["arbitrum:" + routeAccount + ":0xweth", 0n]]),
})[0];
rows.push({
  name: "close lifecycle withdraw is close borrow not withdraw",
  row: closeLifecycleWithdraw,
  expectedAction: "Close Borrow",
  expectedChips: "closeBorrow",
  expectedSource: "Borrow position lifecycle",
  expectedPreview: "0.00000000001671 WETH",
  filters: { repay: true, withdraw: false, borrow: false },
});
rows.forEach(fixture => {
  const action = api.cleanTransactionAction(fixture.row);
  const chips = api.displayActionsForRow(fixture.row).join("|");
  const source = api.classificationSourceForRow(fixture.row);
  const preview = api.compactTransactionAssetPreview(fixture.row);
  const expectedMetaSource = fixture.expectedMetaSource || fixture.expectedSource;
  const metaHtml = api.eventMetaBlockHtml(fixture.row, fixture.row.events.find(event => !["borrowPositionOpen", "borrowPositionClose"].includes(event.action)) || fixture.row.events[0]);
  const payload = api.evidenceRowPayload(fixture.row);
  const filterResults = Object.fromEntries(Object.keys(fixture.filters).map(key => [key, api.rowMatchesActionFilter(fixture.row, key)]));
  const result = { name: fixture.name, action, chips, source, preview, filterResults, metaHtml, payloadClassificationSource: payload.classificationSource };
  if (action !== fixture.expectedAction) throw new Error(JSON.stringify(result));
  if (chips !== fixture.expectedChips) throw new Error(JSON.stringify(result));
  if (source !== fixture.expectedSource) throw new Error(JSON.stringify(result));
  if (fixture.expectedPreview && preview !== fixture.expectedPreview) throw new Error(JSON.stringify(result));
  Object.entries(fixture.filters).forEach(([key, expected]) => {
    if (filterResults[key] !== expected) throw new Error(JSON.stringify(result));
  });
  if (!metaHtml.includes("Classification") || !metaHtml.includes(expectedMetaSource)) throw new Error(JSON.stringify(result));
  if (payload.classificationSource !== fixture.expectedSource) throw new Error(JSON.stringify(result));
  if (!payload.events.some(event => event.classificationSource === fixture.expectedSource)) throw new Error(JSON.stringify(result));
});
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_review_only_earn_does_not_enter_activity_net(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { assetActivitySummaryForRows };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const rows = [];
const earnEntries = [
  { source: "earn-verified-ledger", trusted: true, reviewFlag: "income_candidate", chainKey: "arbitrum", assetInSymbol: "USDC", assetInAmount: "10", usd: 10 },
  { source: "earn-verified-ledger", trusted: false, reviewFlag: "needs_review", reviewReason: "status_mismatch", chainKey: "arbitrum", assetInSymbol: "USDC", assetInAmount: "5", usd: 5 },
  { source: "earn-merkl-rewards", trusted: true, reviewFlag: "needs_review", claimProofStatus: "estimated_from_accumulated_minus_unclaimed", chainKey: "arbitrum", assetInSymbol: "USDC", assetInAmount: "2", usd: 2 },
];
const summary = sandbox.__historyTest.assetActivitySummaryForRows(rows, earnEntries).find(item => item.symbol === "USDC");
if (!summary) throw new Error("missing USDC summary");
if (summary.earnAmount !== 10) throw new Error(JSON.stringify(summary));
if (summary.earnReviewAmount !== 7) throw new Error(JSON.stringify(summary));
if (summary.netAmount !== 10) throw new Error(JSON.stringify(summary));
if (summary.reviewCount !== 2) throw new Error(JSON.stringify(summary));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_complex_dolomite_actions_have_explicit_review_guardrails(self):
        for reason in [
            "zap_route_review",
            "amm_swap_review",
            "amm_liquidity_review",
            "odolo_vedolo_pairing_review",
            "odolo_vedolo_exercise_review",
            "odolo_vedolo_transfer_review",
            "async_position_timing",
            "liquidation_forced_settlement",
            "vaporization_debt_absorption",
        ]:
            self.assertIn(reason, self.source)
        for label in [
            "Zap route review",
            "AMM trade review",
            "AMM liquidity review",
            "oDOLO/veDOLO exercise review",
            "Async timing review",
            "Forced liquidation review",
            "Debt settlement review",
        ]:
            self.assertIn(label, self.source)
        self.assertIn("reviewReason: event.reviewReason || \"\"", self.source)
        self.assertIn("const explicitReason = String(profile.reviewReason || \"\").trim()", self.source)

    def test_earn_rows_are_review_only_when_ledger_is_stale_or_untrusted(self):
        self.assertIn("__historySnapshotStale", self.source)
        self.assertIn("stale_snapshot:", self.source)
        self.assertIn("function earnLedgerEvidenceLevel", self.source)
        self.assertIn("function canUseEarnLedgerBaselineEntry", self.source)
        self.assertIn('const evidenceLevel = ledgerStale ? "review" : earnLedgerEvidenceLevel(market)', self.source)
        self.assertIn('const trusted = evidenceLevel === "verified"', self.source)
        self.assertIn('reviewFlag: positive && trusted ? "income_candidate" : "needs_review"', self.source)
        self.assertIn('"earn_yield_inferred"', self.source)
        self.assertIn('"inferred_yield_review"', self.source)
        self.assertIn("isVerifiedEarnEntry", self.source)
        self.assertIn("earnReviewAmount", self.source)
        self.assertIn("earnReviewUsd", self.source)
        self.assertIn("Review-only yield candidate", self.source)
        self.assertIn("EARN verification incomplete", self.source)
        self.assertNotIn("EARN mismatch", self.source)

    def test_precise_wei_helpers_are_used_instead_of_parse_float(self):
        self.assertNotIn("parseFloat", self.source)
        self.assertIn("safeBigInt(yearly.cumulativeYield)", self.source)
        self.assertIn("formatUnits(yieldWei, decimals, decimals)", self.source)

    def test_clean_amount_keeps_tiny_nonzero_values_visible(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { cleanAmount };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const cleanAmount = sandbox.__historyTest.cleanAmount;
if (cleanAmount("123.450000") !== "123.45") throw new Error(cleanAmount("123.450000"));
if (cleanAmount("0.123456789") !== "0.12345678") throw new Error(cleanAmount("0.123456789"));
if (cleanAmount("0.000000009296098168") === "0.00000000") throw new Error("tiny amount rounded to displayed zero");
if (cleanAmount("0.000000009296098168") !== "0.000000009296") throw new Error(cleanAmount("0.000000009296098168"));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_earn_year_yield_prefers_reusable_ledger_before_snapshot_fallback(self):
        self.assertIn("EARN_SNAPSHOT_BASE", self.source)
        self.assertIn("fetchEarnYearYields", self.source)
        self.assertIn('method: "snapshot-series-year"', self.source)
        self.assertIn("resolveEarnMarketYield", self.source)
        self.assertIn("earnLedgerYieldFromMarket", self.source)
        self.assertIn("canUseEarnLedgerBaselineEntry(market) && earnLedgerOverlapsBounds", self.source)
        self.assertIn('yearly.source === "earn-verified-ledger"', self.source)
        self.assertIn('yearly.source === "earn-snapshot-series"', self.source)

    def test_earn_candidates_use_strict_confidence_contract(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { state, getBounds, earnLedgerTaxEntries, resolveEarnMarketYield, earnLedgerEvidenceLevel, cleanEarnReviewStatus, cleanEarnReviewReason };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const state = sandbox.__historyTest.state;
state.action = "all";
state.year = "2026";
state.selectedChains = new Set(["arbitrum"]);
state.earn = {
  status: "ready",
  ledgers: {
    arbitrum: {
      snapshotDate: "2026-05-21",
      markets: {
        "1": { symbol: "USDC", decimals: 6, cumulativeYield: "1000", firstDate: "2026-01-21", lastDate: "2026-05-21", strictStatus: "verified", status: "verified", strictMethod: "interest-ledger", method: "interest-ledger", canonicalHistoryCoverageStatus: "fresh" },
        "2": { symbol: "USDC", decimals: 6, cumulativeYield: "2000", firstDate: "2026-01-21", lastDate: "2026-05-21", strictStatus: "inferred", status: "pre_snapshot_carry", strictMethod: "netflow+pre-snapshot-carry", method: "netflow+pre-snapshot-carry", canonicalHistoryCoverageStatus: "fresh" },
        "3": { symbol: "USDC", decimals: 6, cumulativeYield: "3000", firstDate: "2026-01-21", lastDate: "2026-05-21", strictStatus: "mismatch", status: "mismatch", strictMethod: "canonical-history-mismatch", method: "snapshot-fallback", canonicalHistoryCoverageStatus: "fresh" },
      },
    },
  },
  yearYields: {
    arbitrum: {
      "1": { source: "earn-snapshot-series", method: "snapshot-series-year", cumulativeYield: "9999", symbol: "USDC", decimals: 6, firstDate: "2026-01-21", lastDate: "2026-05-21" },
      "2": { source: "earn-snapshot-series", method: "snapshot-series-year", cumulativeYield: "8888", symbol: "USDC", decimals: 6, firstDate: "2026-01-21", lastDate: "2026-05-21" },
      "3": { source: "earn-snapshot-series", method: "snapshot-series-year", cumulativeYield: "7777", symbol: "USDC", decimals: 6, firstDate: "2026-01-21", lastDate: "2026-05-21" },
    },
  },
  rewards: {},
  prices: {},
};
const entries = sandbox.__historyTest.earnLedgerTaxEntries(sandbox.__historyTest.getBounds("2026"));
const byMarket = Object.fromEntries(entries.map(entry => [entry.marketId, entry]));
if (sandbox.__historyTest.earnLedgerEvidenceLevel(state.earn.ledgers.arbitrum.markets["1"]) !== "verified") throw new Error("verified level failed");
if (sandbox.__historyTest.earnLedgerEvidenceLevel(state.earn.ledgers.arbitrum.markets["2"]) !== "inferred") throw new Error("inferred level failed");
if (sandbox.__historyTest.earnLedgerEvidenceLevel(state.earn.ledgers.arbitrum.markets["3"]) !== "review") throw new Error("review level failed");
if (byMarket["1"].assetInAmount !== "0.001") throw new Error(JSON.stringify(byMarket["1"]));
if (byMarket["1"].reviewFlag !== "income_candidate") throw new Error(JSON.stringify(byMarket["1"]));
if (byMarket["1"].earnPeriodSource !== "earn-verified-ledger") throw new Error(JSON.stringify(byMarket["1"]));
if (byMarket["2"].assetInAmount !== "0.002") throw new Error(JSON.stringify(byMarket["2"]));
if (byMarket["2"].taxCategory !== "earn_yield_inferred") throw new Error(JSON.stringify(byMarket["2"]));
if (byMarket["2"].reviewFlag !== "needs_review") throw new Error(JSON.stringify(byMarket["2"]));
if (!byMarket["2"].reviewReason.includes("inferred_yield_review")) throw new Error(JSON.stringify(byMarket["2"]));
if (sandbox.__historyTest.cleanEarnReviewStatus(byMarket["2"]) !== "needs_review") throw new Error(JSON.stringify(byMarket["2"]));
if (!sandbox.__historyTest.cleanEarnReviewReason(byMarket["2"]).includes("EARN inferred yield")) throw new Error(JSON.stringify(byMarket["2"]));
if (byMarket["3"].assetInAmount !== "0.007777") throw new Error(JSON.stringify(byMarket["3"]));
if (byMarket["3"].earnPeriodSource !== "earn-snapshot-series") throw new Error(JSON.stringify(byMarket["3"]));
if (!byMarket["3"].reviewReason.includes("status_mismatch")) throw new Error(JSON.stringify(byMarket["3"]));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_history_filter_dirty_and_custom_range_runtime_contract(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyTest = { state, els, getBounds, rowsMatchingCurrentFilters, reportExportReadiness, earnTaxEntriesForCurrentView, selectedChainKeys };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyTest;
const state = api.state;
state.year = "custom";
state.dateFrom = "2026-02-03";
state.dateTo = "2026-02-04";
const customBounds = api.getBounds("custom");
if (customBounds.label !== "2026-02-03 to 2026-02-04") throw new Error(JSON.stringify(customBounds));
if (customBounds.start !== Math.floor(Date.UTC(2026, 1, 3, 0, 0, 0) / 1000)) throw new Error(JSON.stringify(customBounds));
if (customBounds.end !== Math.floor(Date.UTC(2026, 1, 4, 23, 59, 59) / 1000)) throw new Error(JSON.stringify(customBounds));
state.dateTo = "2026-02-02";
let reversedRejected = false;
try {
  api.getBounds("custom");
} catch (error) {
  reversedRejected = String(error.message || error).includes("end after the From date");
}
if (!reversedRejected) throw new Error("custom range did not reject reversed dates");
state.dateTo = "2026-02-04";
state.rows = [
  { chainKey: "berachain", actions: new Set(["deposit"]), events: [{ action: "deposit" }] },
  { chainKey: "arbitrum", actions: new Set(["withdraw"]), events: [{ action: "withdraw" }] },
];
state.filteredRows = state.rows;
state.selectedChains = new Set(["berachain"]);
api.els.action = { options: [{ value: "all" }, { value: "deposit" }, { value: "withdraw" }] };
state.selectedActions = new Set(["deposit"]);
state.filtersDirty = false;
let filteredRows = api.rowsMatchingCurrentFilters(state.rows);
if (filteredRows.length !== 1 || filteredRows[0].chainKey !== "berachain") throw new Error(JSON.stringify(filteredRows));
if (api.selectedChainKeys().join(",") !== "berachain") throw new Error(api.selectedChainKeys().join(","));
state.earn = { status: "ready", ledgers: {}, rewards: {}, prices: {} };
state.filtersDirty = true;
if (api.reportExportReadiness(filteredRows, []).canFullReport) throw new Error("dirty filters allowed export");
if (api.earnTaxEntriesForCurrentView().length !== 0) throw new Error("dirty filters allowed earn export rows");
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_earn_summary_stays_in_exports_not_first_screen(self):
        for element_id in [
            "history-earn-status",
            "history-earn-yield",
            "history-earn-trusted",
            "history-earn-rewards",
            "history-earn-coverage",
        ]:
            self.assertNotIn(element_id, self.html)
            self.assertNotIn(element_id, self.source)
        self.assertIn("earnSummaryForCurrentView", self.source)
        self.assertIn("summary: earnSummaryForCurrentView()", self.source)
        self.assertNotIn(".earn-grid", self.css)

    def test_review_reasons_stay_in_exports_not_page_detail(self):
        for element_id in [
            "history-review-status",
            "history-review-list",
            "history-review-queue-status",
            "history-review-queue-list",
        ]:
            self.assertNotIn(element_id, self.html)
            self.assertNotIn(element_id, self.source)
        self.assertIn("reviewSummaryForCurrentView", self.source)
        self.assertIn("reviewQueueForRows", self.source)
        self.assertIn("reviewQueue: reviewQueueForRows(rows, earnEntries)", self.source)
        self.assertIn("const includeEarnReview = actionFilterAllSelected();", self.source)
        self.assertNotIn(".review-strip", self.css)
        self.assertNotIn(".review-pill", self.css)
        self.assertNotIn(".review-queue-panel", self.css)
        self.assertNotIn("Review reasons", self.html)

    def test_dolomite_activity_summary_stays_in_exports_not_first_screen(self):
        for element_id in [
            "history-activity-in",
            "history-activity-out",
            "history-activity-trades",
            "history-activity-accounts",
        ]:
            self.assertNotIn(element_id, self.html)
            self.assertNotIn(element_id, self.source)
        self.assertIn("dolomiteActivitySummaryForRows", self.source)
        self.assertIn("activityGroupForEvent", self.source)
        self.assertIn("const activitySummary = dolomiteActivitySummaryForRows(rows)", self.source)
        self.assertIn("activity: activitySummary", self.source)
        self.assertNotIn(".activity-grid", self.css)

    def test_history_overview_consolidates_duplicate_summary_metrics(self):
        for element_id in [
            "history-total-tx",
            "history-gas-usd",
            "history-coverage",
            "history-tax-review",
        ]:
            self.assertNotIn(element_id, self.html)
            self.assertNotIn(element_id, self.source)
        for removed in [
            "History overview",
            "summary-strip",
            "history-tax-disposals",
            "history-tax-income",
            "history-tax-fees",
            "Transactions",
            "Gas fees",
            "Tax evidence",
            "Evidence rows",
            "Gas spent",
            "Report rows",
            "EARN rows",
            "Export rows",
        ]:
            self.assertNotIn(removed, self.html)
        for removed_source in [
            "history-tax-disposals",
            "history-tax-income",
            "history-tax-fees",
        ]:
            self.assertNotIn(removed_source, self.source)
        self.assertNotIn(".overview-grid", self.css)
        self.assertNotIn(".overview-cell", self.css)
        self.assertNotIn(".tax-panel", self.css)
        self.assertIn(".history-table{width:100%;min-width:1040px", self.css)
        self.assertIn("els.count.textContent = historyCountLabel(rows, earnEntries)", self.source)
        self.assertIn("transaction${rows.length === 1 ? \"\" : \"s\"}", self.source)
        self.assertIn("0 transactions", self.html)
        self.assertIn(".hero-topline h1{font-size:32px;line-height:1.08", self.css)
        self.assertIn("font-size:10px;text-transform:uppercase;letter-spacing:1.25px;color:var(--gold-hi);font-weight:800", self.css)
        self.assertIn(".beta-badge{display:inline-flex;align-items:center;height:22px", self.css)
        self.assertIn(".history-beta-note{margin-top:6px;color:var(--fg-4);font-size:11.5px", self.css)
        self.assertIn(".report-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;padding:14px 18px", self.css)
        self.assertIn(".history-table td{padding:12px", self.css)
        self.assertIn(".history-table col.col-chain{width:12%}", self.css)
        self.assertIn(".history-table col.col-date{width:13%}", self.css)
        self.assertIn(".history-table col.col-action{width:14%}", self.css)
        self.assertIn(".history-table col.col-assets{width:27%}", self.css)
        self.assertIn(".history-table col.col-details{width:12%}", self.css)
        self.assertIn(".history-table td.num{text-align:right;font-family:var(--mono);font-weight:500}", self.css)
        self.assertIn(".history-table tbody tr[data-row-key]:hover td{background:rgba(255,255,255,.026);color:var(--fg-1)}", self.css)
        self.assertIn(".history-table tbody tr[data-row-key]:hover td:first-child::before", self.css)
        self.assertNotIn(".history-table tbody tr:hover td{background:linear-gradient", self.css)
        self.assertIn("border-radius:0 2px 2px 0", self.css)
        self.assertNotIn("border-right", self.css)
        self.assertIn('<th>Details</th>', self.html)
        self.assertIn('<th>Chain</th>', self.html)
        self.assertIn('<th class="num">Value</th>', self.html)
        self.assertNotIn('<th class="num">Volume</th>', self.html)
        self.assertNotIn('<th>Network</th>', self.html)
        self.assertIn('<col class="col-details">', self.html)
        self.assertIn('td colspan="7"', self.html)
        self.assertIn("HISTORY_TABLE_COLSPAN = 7", self.source)
        self.assertIn("historyDetailToggleHtml", self.source)
        self.assertIn("displayActionsForRow", self.source)
        self.assertIn(".action-td{overflow:hidden}", self.css)
        self.assertIn(".action-chip{min-width:0;justify-content:center;text-align:center;line-height:1.08;white-space:normal;overflow-wrap:anywhere}", self.css)
        self.assertIn(".date-main{font-size:11px}", self.css)
        self.assertIn(".action-chip{max-width:82px;padding:5px 7px;letter-spacing:.5px}", self.css)
        self.assertNotIn(".action-chip{display:inline-flex;align-items:center;gap:7px;max-width:100%;border-radius:8px;font-size:10px;text-transform:uppercase;letter-spacing:.65px;font-weight:800;padding:5px 8px;border:1px solid rgba(255,255,255,.075);background:rgba(255,255,255,.028);color:var(--fg-2);white-space:nowrap", self.css)
        self.assertNotIn("eventCountLabel", self.source)
        self.assertNotIn("asset-sub", self.source)
        self.assertNotIn(".asset-sub", self.css)
        self.assertNotIn("Dolomite events; CSV records it once", self.source)
        self.assertNotIn("source event", self.source)
        self.assertIn("rowMatchesActionFilter", self.source)
        self.assertIn("vestingEventsForRow", self.source)
        self.assertIn('if (mergedActions.includes("zap")) {', self.source)
        self.assertIn('return ["zap", ...semanticZapActions, ...vestingChips]', self.source)
        self.assertIn("date-tx", self.source)
        self.assertIn(".date-top", self.css)
        self.assertIn(".date-tx{width:18px;height:18px", self.css)
        self.assertIn("formatHistoryDate(row.timestamp)", self.source)
        self.assertIn("formatRelativeTime(row.timestamp))} · ${escapeHtml(formatClockTime(row.timestamp))", self.source)
        self.assertIn("function formatRelativeTime(timestamp)", self.source)
        self.assertIn("function formatClockTime(timestamp)", self.source)
        self.assertIn("color:var(--fg-4);opacity:.68", self.css)
        self.assertNotIn("Block ${escapeHtml(row.blockNumber", self.source)
        self.assertNotIn("<th>Tx</th>", self.html)
        self.assertNotIn("col-tx", self.html)
        self.assertNotIn("col-tx", self.css)
        self.assertNotIn("tx-link", self.source)
        self.assertNotIn("tx-link", self.css)
        self.assertNotIn("tx-td", self.source)
        self.assertNotIn("tx-td", self.css)
        self.assertIn("data-history-detail-toggle", self.source)
        self.assertIn("eventTransactionLinkHtml(row, true)", self.source)
        self.assertIn("explorerTxUrl(row)", self.source)
        self.assertIn("explorerAddressUrl(chainKey, address)", self.source)
        self.assertIn("const senderLink = sameHistoryWalletAddress(gas.from) ? \"\" : addressExplorerLinkHtml(row.chainKey, gas.from)", self.source)
        self.assertIn("sameHistoryWalletAddress(event?.counterparty)", self.source)
        self.assertIn("eventMetaHtml(row, event)", self.source)
        self.assertIn("detailEventFlowLabel(event)", self.source)
        self.assertIn("detailDisplayEventsForRow(row)", self.source)
        self.assertIn("const displayEvents = detailDisplayEventsForRow(row)", self.source)
        self.assertIn("if (primary) return [primary]", self.source)
        self.assertIn("isSwapLikeEvent(event)", self.source)
        self.assertIn("cleanSwapOutcomeFlow(event, \"detail\")", self.source)
        self.assertIn("compactVestingTableFlow(event) || stripDetailActionPrefix", self.source)
        self.assertIn("eventAccountHtml(event)", self.source)
        self.assertIn("eventMetaBlockHtml(row, event)", self.source)
        self.assertIn("eventCounterpartyHtml(row, event)", self.source)
        self.assertIn('const label = event?.role === "in" ? "From" : event?.role === "out" ? "To" : "Address";', self.source)
        self.assertIn("const isInternalRoute = event?.isSelfTransfer && fromAccount && toAccount && fromAccount !== toAccount", self.source)
        self.assertIn("`${fromAccount} -> ${toAccount}`", self.source)
        self.assertIn("accountDisplayName(event?.fromAccount)", self.source)
        self.assertIn("accountDisplayName(event?.toAccount)", self.source)
        self.assertIn('account && account !== "0" ? formatAccountValue(account) : ""', self.source)
        self.assertIn('if (value === "0") return "Main account (0)";', self.source)
        self.assertIn("return `Subaccount ${formatAccountValue(value)}`;", self.source)
        self.assertNotIn("formatAccountChip", self.source)
        self.assertNotIn("`Acct ${account}`", self.source)
        self.assertIn("explorerAddress: \"https://berascan.com/address/\"", self.source)
        self.assertIn("event-action-stack", self.source)
        self.assertIn("<strong>${escapeHtml(cleanReportActionLabel(event))}</strong>", self.source)
        self.assertIn("${value.slice(0, 4)}...${value.slice(-4)}", self.source)
        self.assertIn(".history-detail-toggle", self.css)
        self.assertIn(".details-cell", self.css)
        self.assertIn(".detail-box{position:sticky;left:0", self.css)
        self.assertIn(".event-row > *{min-width:0}", self.css)
        self.assertIn(".event-action-stack", self.css)
        self.assertIn(".event-open-link", self.css)
        self.assertIn(".address-open-link", self.css)
        self.assertNotIn(".wallet-context-chip", self.css)
        self.assertIn(".event-meta{display:flex;align-items:center;flex-wrap:wrap", self.css)
        self.assertIn(".event-detail-item{display:inline-flex;align-items:center", self.css)
        self.assertIn(".event-detail-label", self.css)
        self.assertNotIn(".event-account-chip", self.css)
        self.assertIn("overflow-wrap:anywhere", self.css)
        self.assertNotIn(".detail-open-link", self.css)
        self.assertNotIn(".event-row span:last-child{text-align:right", self.css)
        self.assertNotIn("tx-sub", self.source)
        self.assertNotIn("tx-hash", self.source)
        self.assertNotIn('class="gas-sub"', self.source)
        self.assertNotIn("gas-sub", self.css)
        self.assertIn('return `<div class="gas-cell"><div class="gas-main good">${formatUsd(gas.gasUsd)}</div></div>`', self.source)
        self.assertIn("Price missing</div></div>`", self.source)
        self.assertIn('class="chain-chip" title="${escapeAttr(chain.name)}"', self.source)
        self.assertIn('<span class="chain-name">${escapeHtml(chain.name)}</span>', self.source)
        self.assertNotIn("chain-short", self.source)
        self.assertIn(".chain-name{display:inline}", self.css)
        self.assertNotIn(".summary-strip", self.css)
        self.assertLess(self.html.index('aria-label="Dolomite history report download"'), self.html.index('<div class="table-wrap">'))

    def test_loading_progress_ux_is_rendered(self):
        for element_id in [
            "history-loading-panel",
            "history-loading-title",
            "history-loading-sub",
            "history-loading-eta",
            "history-loading-percent",
            "history-loading-clock",
            "history-loading-bar",
            "history-step-subgraphs",
            "history-step-receipts",
            "history-step-evidence",
            "history-step-reports",
        ]:
            self.assertIn(element_id, self.html)
            self.assertIn(element_id, self.source)
        self.assertIn("renderLoadingPanel", self.source)
        self.assertIn("loadingProgressPercent", self.source)
        self.assertIn("loadingClockText", self.source)
        self.assertIn("syncLoadingTicker", self.source)
        self.assertIn("syncAddressFieldValue", self.source)
        self.assertIn("loadingEtaText", self.source)
        self.assertIn('!["idle", "done"].includes(phase)', self.source)
        self.assertIn("els.loadingPanel.hidden = !visible", self.source)
        self.assertNotIn("new Event(", self.source)
        self.assertIn(".loading-eta strong", self.css)
        self.assertIn('role="progressbar"', self.html)
        self.assertIn('id="history-loading-panel" aria-label="Transaction history loading progress" aria-live="polite" hidden', self.html)
        self.assertIn(".loading-panel", self.css)
        self.assertIn(".loading-step", self.css)

    def test_history_completion_status_stays_compact_while_evidence_finishes(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyStatusTest = { state, historyCompletionStatusMessage };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  Date,
  Math,
  Intl,
  setInterval() { return 1; },
  clearInterval() {},
  setTimeout() { return 1; },
  clearTimeout() {},
  fetch() { return Promise.reject(new Error("fetch disabled")); },
  localStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
  location: { search: "", pathname: "/history/", origin: "https://example.test" },
  history: { replaceState() {} },
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} }, getElementById() { return null; }, querySelectorAll() { return []; } },
  window: {},
};
sandbox.window = sandbox;
vm.createContext(sandbox);
vm.runInContext(instrumented, sandbox);
const api = sandbox.__historyStatusTest;
api.state.fastMode = true;
api.state.warnings = [
  "Berachain reward claim index is current through 07 Jul 2026; newer reward claims may be missing until the next workflow refresh.",
  "Arbitrum reward claim index is current through 07 Jul 2026; newer reward claims may be missing until the next workflow refresh.",
  "Mantle reward claim index is current through 07 Jul 2026; newer reward claims may be missing until the next workflow refresh.",
];
api.state.earn = { warnings: ["EARN candidate evidence is partial."] };
const pending = api.historyCompletionStatusMessage(80, 80, 0, false);
const complete = api.historyCompletionStatusMessage(80, 12, 3, true);
if (pending.length > 150) throw new Error(`pending status too long: ${pending}`);
if (pending.includes("Berachain reward claim index")) throw new Error(`warning detail leaked: ${pending}`);
if (!pending.includes("progress panel")) throw new Error(`pending status should point to progress panel: ${pending}`);
if (!pending.includes("4 data warnings")) throw new Error(`warning count missing: ${pending}`);
if (!complete.includes("12 match current filters")) throw new Error(`filter summary missing: ${complete}`);
if (!complete.includes("3 evidence rows")) throw new Error(`evidence summary missing: ${complete}`);
"""
        result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, env=NODE_ENV)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("Partial data warning: ${shortWarnings()}", self.source)
        self.assertNotIn("Gas/evidence is still finishing in the background", self.source)

    def test_history_filters_use_assets_style_dropdowns(self):
        for element_id in [
            "history-year-button",
            "history-year-menu",
            "history-action-button",
            "history-action-menu",
            "history-network-button",
            "history-network-icon",
            "history-network-menu",
        ]:
            self.assertIn(element_id, self.html)
            self.assertIn(element_id, self.source)
        for generated_element_id in [
            "history-date-range",
            "history-date-from",
            "history-date-to",
        ]:
            self.assertNotIn(generated_element_id, self.html)
            self.assertIn(generated_element_id, self.source)
        for text in [
            "All Chains",
            "All actions",
            "Chain",
            "Date",
            "Action",
            "Borrow",
            "Repay",
            "Trade / Zap",
            "Debt Settlement",
            "Delayed Deposit",
            "Delayed Withdraw",
            "AMM / Liquidity",
            "Claim",
        ]:
            self.assertIn(text, self.html)
        self.assertIn("fetchBorrowReplayBalances", self.source)
        self.assertIn("fetchCurrentInterestIndexes", self.source)
        self.assertIn("parBalanceToTokenBalance", self.source)
        self.assertIn("multiplyScaledDecimal", self.source)
        self.assertIn("expandScientificDecimal", self.source)
        self.assertIn("borrowSemanticForBalanceTransition", self.source)
        self.assertIn("current_balance_replay", self.source)
        self.assertIn("Open Borrow", self.source)
        self.assertIn("Close Borrow", self.source)
        self.assertIn("normalizeActionFilter", self.source)
        self.assertIn('if (action === "swap")', self.source)
        self.assertIn('if (action === "trade" || action === "zap") return "swap";', self.source)
        self.assertIn('if (action === "odoloClaim" || action === "rewardClaim") return "claim";', self.source)
        self.assertNotIn('<option value="trade">Trade</option>', self.html)
        self.assertNotIn('<option value="zap">Zap</option>', self.html)
        self.assertNotIn('<option value="odoloClaim">Claim oDOLO</option>', self.html)
        self.assertNotIn('<option value="rewardClaim">Claim Rewards</option>', self.html)
        self.assertNotIn("Deposit / Repay", self.html)
        self.assertNotIn("Withdraw / Borrow", self.html)
        for generated_text in [
            "Custom range only",
            "From",
            "To",
            "Select exact range, then Load history",
            "Applied after Load history",
        ]:
            self.assertIn(generated_text, self.source)
        self.assertNotIn("All since", self.source)
        self.assertNotIn("history-date-quick-list", self.source)
        self.assertNotIn("history-date-quick-list", self.css)
        self.assertNotIn("data-history-year", self.source)
        self.assertNotIn("<span>Year</span>", self.html)
        self.assertNotIn('id="history-year-count">Year</span>', self.html)
        for fn in [
            "buildActionDropdown",
            "syncNetworkDropdown",
            "handleNetworkDropdownClick",
            "markHistoryFiltersDirty",
            "syncDateRangeControls",
            "ensureCustomRangeDefaults",
            "dateDropdownPanelHtml",
            "cacheDateRangeElements",
            "defaultCustomRange",
            "dateRangeIsDefault",
            "dateFilterButtonLabel",
            "formatDateInputShort",
            "closeHistoryDropdowns",
        ]:
            self.assertIn(fn, self.source)
        self.assertIn("state.filtersDirty", self.source)
        self.assertIn("Load required", self.source)
        self.assertIn("Filters changed. Click Load history", self.source)
        self.assertIn("els.run.classList.toggle(\"pending\"", self.source)
        self.assertIn("selectedChainKeys()", self.source)
        self.assertIn("const chainKeys = selectedChainKeys();", self.source)
        self.assertIn("rowsMatchingCurrentFilters", self.source)
        self.assertIn("match current filters", self.source)
        self.assertIn("dateInputToUnixStart", self.source)
        self.assertIn("dateInputToUnixEnd", self.source)
        self.assertIn("validDateInput", self.source)
        self.assertIn("dateRange: meta.period", self.source)
        self.assertNotIn("if (state.rows.length) lookup();", self.source)
        self.assertIn("globeIconHtml", self.source)
        self.assertIn("CHAIN_FILTER_ORDER", self.source)
        self.assertIn("clearHistoryFilter", self.source)
        self.assertIn("networkButtonIconHtml", self.source)
        self.assertIn('!allSelected && state.selectedChains.has(chain)', self.source)
        self.assertIn(".history-dd-btn", self.css)
        self.assertIn(".history-dd-panel", self.css)
        self.assertIn(".history-dd-clear", self.css)
        self.assertIn(".history-dd-icon img", self.css)
        self.assertIn(".history-date-range", self.css)
        self.assertIn(".history-dd-panel-date", self.css)
        self.assertIn(".history-date-fields", self.css)
        self.assertIn(".history-date-field input", self.css)
        self.assertIn(".primary-btn.pending", self.css)
        self.assertIn("#history-year-control{width:176px}", self.css)
        self.assertIn("#history-action-control{width:190px}", self.css)
        self.assertIn("#history-network-control{width:210px}", self.css)
        self.assertIn("min-width:220px", self.css)
        self.assertIn("text-align:left", self.css)
        self.assertIn('data-history-clear="network"', self.html)
        self.assertIn('data-history-clear="action"', self.html)
        self.assertIn('data-history-clear="year"', self.html)
        self.assertNotIn("history-chain-filters", self.html)
        self.assertNotIn("history-all-chains", self.html)
        self.assertNotIn('id="history-year-control" for=', self.html)
        self.assertNotIn('id="history-action-control" for=', self.html)

    def test_history_covers_legacy_trade_tab_and_routed_swaps(self):
        for query_contract in [
            'entity: "trades"',
            'where: `takerEffectiveUser: "${user}", ${timeFilter}`',
            'where: `makerEffectiveUser: "${user}", takerEffectiveUser_not: "${user}", ${timeFilter}`',
            'map: row => eventFromTrade(chainKey, row, "taker")',
            'map: row => eventFromTrade(chainKey, row, "maker")',
            'entity: "zaps"',
            'map: row => eventFromZap(chainKey, row)',
            'entity: "ammTrades"',
            'entity: "ammMints"',
            'entity: "ammBurns"',
        ]:
            self.assertIn(query_contract, self.source)
        self.assertIn('...eventBase(chainKey, row, "trade", side)', self.source)
        self.assertIn("eventsWithSourceEntity(spec.map(row), spec.entity)", self.source)
        self.assertIn('"Dolomite Trade-tab swap candidate; taxability depends on jurisdiction."', self.source)
        self.assertIn('if (event?.action === "trade" || event?.taxCategory === "swap") return "Trade";', self.source)
        self.assertIn('label: ACTION_TABLE_LABELS[key] || cleanReportActionLabel(event)', self.source)
        self.assertIn('ammAddLiquidity: "Add LP"', self.source)
        self.assertIn('ammRemoveLiquidity: "Remove LP"', self.source)
        self.assertIn('return "ammTrade";', self.source)
        self.assertIn('if (action === "swap")', self.source)
        self.assertIn('row.actions.has("trade")', self.source)
        self.assertIn('row.actions.has("zap")', self.source)
        self.assertIn('event?.taxCategory === "swap" || event?.taxCategory === "zap"', self.source)
        self.assertIn('<option value="swap">Trade / Zap</option>', self.html)
        self.assertNotIn('<option value="trade">Trade</option>', self.html)

    def test_history_report_is_consolidated_with_format_choices(self):
        for element_id in [
            "history-report-status",
            "history-tax-export",
            "history-report-json",
            "history-report-print",
        ]:
            self.assertIn(element_id, self.html)
            self.assertIn(element_id, self.source)
        for element_id in [
            "history-methodology-panel",
            "history-methodology-status",
            "history-methodology-scope",
            "history-methodology-prices",
            "history-methodology-balances",
        ]:
            self.assertNotIn(element_id, self.html)
            self.assertNotIn(element_id, self.source)
        for element_id in [
            "history-report-tax",
            "history-report-tax-tool",
            "history-completeness-receipts",
            "history-completeness-prices",
            "history-completeness-earn",
            "history-completeness-rows",
            "history-report-ledger",
            "history-report-statement",
        ]:
            self.assertNotIn(element_id, self.html)
            self.assertNotIn(element_id, self.source)
        for text in [
            "Download report",
            "Dolomite history report",
            "All Dolomite transactions, gas fees and tax evidence.",
        ]:
            self.assertIn(text, self.html)
        for removed_text in [
            "Transactions",
            "Gas fees",
            "Tax evidence",
            "Evidence rows",
            "Report includes",
        ]:
            self.assertNotIn(removed_text, self.html)
        for removed_text in [
            "Activity statement",
            "Yearly summary for accountant review",
            "Evidence report",
            "Dolomite actions, gas and EARN evidence",
            "Detailed Dolomite rows for accountant or tax software",
            "Transaction timeline with gas evidence",
        ]:
            self.assertNotIn(removed_text, self.html)
        self.assertNotIn("Activity statement print", self.source)
        self.assertNotIn("Dolomite Transaction History Statement", self.source)
        self.assertIn("Printable report", self.source)
        self.assertNotIn("Evidence report", self.html)
        self.assertNotIn("Dolomite actions, gas and EARN evidence", self.html)
        self.assertIn("Tax evidence JSON", self.source)
        for removed_text in [
            "Evidence JSON",
            "Tax tool CSV",
            "Print statement",
            "Raw proof pack",
            "Generic import rows",
            "Printable statement",
            "Priced events",
            "Export rows",
        ]:
            self.assertNotIn(removed_text, self.html)
        for fn in [
            "renderReportFiles",
            "exportHistoryReportCsv",
            "exportEvidenceJson",
            "printAnnualStatement",
            "reportCompletenessForRows",
            "assetActivitySummaryForRows",
            "positionLifecycleForRows",
            "lifecycleFlowLabel",
            "annualStatementPayload",
            "evidencePayload",
            "downloadJson",
            "reportMethodology",
            "reviewQueueForRows",
        ]:
            self.assertIn(fn, self.source)
        self.assertIn('aria-label="Dolomite history report download"', self.html)
        self.assertIn(".ledger-report-panel", self.css)
        self.assertIn(".ledger-report-head", self.css)
        self.assertIn(".report-grid-single", self.css)
        self.assertIn(".report-card-wide", self.css)
        self.assertNotIn(".report-included-list", self.css)
        self.assertIn(".report-card", self.css)
        self.assertIn(".report-format-row", self.css)
        self.assertIn(".report-format-btn", self.css)
        self.assertEqual(self.html.count("report-format-btn"), 3)
        self.assertNotIn("history-export", self.html)
        self.assertNotIn("history-export", self.source)
        self.assertNotIn("els.export", self.source)
        self.assertIn("history-scope-info", self.html)
        self.assertIn('aria-expanded="false"', self.html)
        self.assertIn('id="history-scope-tooltip"', self.html)
        self.assertIn('aria-hidden="true"', self.html)
        self.assertIn("Complete Dolomite history for the selected wallet and date range", self.html)
        self.assertIn("Borrow rows show borrow/repay activity, not final tax PnL", self.html)
        self.assertIn(".history-scope-tooltip", self.css)
        self.assertIn("toggleHistoryScopeInfo", self.source)
        self.assertIn("closeHistoryScopeInfo", self.source)
        self.assertIn("closeHistoryScopeInfo();\n    closeHistoryDropdowns();", self.source)
        self.assertIn("els.scopeInfo.querySelector(\".history-scope-tooltip\")?.setAttribute(\"aria-hidden\", \"false\")", self.source)
        self.assertNotIn("els.reportLedger", self.source)
        self.assertNotIn("els.reportStatement", self.source)
        self.assertNotIn("canLedger", self.source)
        self.assertNotIn('aria-label="Dolomite report center"', self.html)
        self.assertNotIn(".report-center", self.css)
        self.assertNotIn(".report-head", self.css)
        self.assertNotIn(".report-download-mark", self.css)
        self.assertNotIn("report-download-mark", self.html)
        self.assertNotIn(".completeness-grid", self.css)
        self.assertNotIn(".methodology-panel", self.css)
        self.assertNotIn(".methodology-grid", self.css)
        self.assertNotIn(".methodology-cell", self.css)
        self.assertNotIn(".position-lifecycle-row", self.css)
        self.assertNotIn("position-lifecycle-panel", self.css)
        self.assertNotIn("history-position-lifecycle-list", self.html)
        self.assertNotIn("history-position-lifecycle-list", self.source)
        self.assertNotIn("renderPositionLifecycle", self.source)
        self.assertNotIn(".review-queue-panel", self.css)
        for removed_text in [
            "yield/rewards",
            "EARN candidates",
            "Annual asset summary",
            "Deposits, withdrawals, routes and EARN candidates by asset",
        ]:
            self.assertNotIn(removed_text, self.html)
        self.assertNotIn("history-asset-summary-list", self.html)
        self.assertNotIn("history-asset-summary-list", self.source)
        self.assertNotIn("renderAssetActivitySummary", self.source)

    def test_history_keeps_methodology_in_exports_not_page_ui(self):
        self.assertNotIn("history-advanced-evidence", self.html)
        self.assertNotIn("history-more-exports", self.html)
        for text in [
            "Report methodology",
            "Scope, prices and balance limitations",
        ]:
            self.assertNotIn(text, self.html)
        self.assertNotIn("Methodology", self.html)
        self.assertNotIn("history-methodology-panel", self.html)
        self.assertIn("function reportMethodology()", self.source)
        self.assertIn("methodology: reportMethodology()", self.source)
        self.assertNotIn("<details", self.html)
        self.assertNotIn("<summary", self.html)
        self.assertNotIn(".simple-disclosure", self.css)
        self.assertNotIn("Report details", self.html)
        self.assertNotIn("Position timeline and methodology", self.html)
        self.assertNotIn("Asset summary, position timeline and methodology", self.html)
        self.assertNotIn("Advanced evidence", self.html)
        self.assertNotIn("More export formats", self.html)
        for removed_text in [
            "Year summary",
            "Evidence summary",
            "Swaps / sells",
            "Yield / rewards",
            "Report quality",
            "Activity direction",
            "EARN enrichment",
            "Annual asset summary",
            "Review reasons",
        ]:
            self.assertNotIn(removed_text, self.html)
        self.assertNotIn("Full examples are included in Evidence CSV/JSON.", self.source)

    def test_history_has_accountant_grade_activity_views_without_balance_guessing(self):
        for text in [
            "Position lifecycle",
            "exact open/closed state needs balance snapshots",
        ]:
            self.assertNotIn(text, self.html)
        for text in [
            "assetSummary: assetActivitySummaryForRows(rows, earnEntries)",
            "positionLifecycle: positionLifecycleForRows(rows, earnEntries)",
            "open/closed not inferred",
            "lifecycleFlowLabel(item)",
        ]:
            self.assertIn(text, self.source)
        self.assertNotIn("assetMovement: assetMovementForRows(rows)", self.source)
        self.assertNotIn("history-asset-movement-list", self.html)

    def test_review_notes_are_local_and_exported(self):
        for fn in [
            "loadReviewNotes",
            "saveReviewNotes",
            "reviewNoteForRow",
            "reviewNotesForExport",
        ]:
            self.assertIn(fn, self.source)
        self.assertIn("dolomite-history-review-notes", self.source)
        self.assertIn("data-review-note", self.source)
        self.assertIn(".review-note-box", self.css)

    def test_history_gas_receipts_are_cached_and_gateway_can_be_preferred(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyRpcTest = { fetchGas, gasCache, gasCacheKey, readStoredGasResult, writeStoredGasResult, rpcUrlsForChain };" + marker);
const storage = new Map();
let fetchCalls = 0;
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  AbortController,
  setTimeout(callback) { callback(); return 1; },
  clearTimeout() {},
  fetch() {
    fetchCalls += 1;
    return Promise.reject(new Error("fetch should not run for a stored receipt"));
  },
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: {
    __DOLO_RPC_GATEWAY: { arbitrum: "https://gateway.example/arbitrum" },
    location: { href: "http://127.0.0.1/history/" },
    history: { replaceState() {} },
    localStorage: {
      getItem(key) { return storage.has(key) ? storage.get(key) : null; },
      setItem(key, value) { storage.set(key, String(value)); },
      removeItem(key) { storage.delete(key); },
    },
  },
};
vm.runInNewContext(instrumented, sandbox);
(async () => {
  const api = sandbox.__historyRpcTest;
  const wallet = "0x28da3dde285d8f1f87b2d858f89961bb8b9af180";
  const row = {
    chainKey: "arbitrum",
    txHash: "0xabc",
    timestamp: 1768210081,
    events: [{ action: "transfer", isSelfTransfer: true, fromAccount: "0", toAccount: "3224015000000001" }],
  };
  const cacheKey = api.gasCacheKey(row, wallet);
  api.writeStoredGasResult(cacheKey, {
    status: "not-payer",
    paidByWallet: false,
    from: "0x1111111111111111111111111111111111111111",
    nativeSymbol: "ETH",
    nativeAmountExact: "0.001",
    borrowLifecycleSemantics: [{ action: "openBorrow", account: "3224015000000001", source: "receipt_log" }],
  });
  api.gasCache.clear();
  const cached = await api.fetchGas(row, wallet);
  if (fetchCalls !== 0) throw new Error(`unexpected fetch calls: ${fetchCalls}`);
  if (cached.status !== "not-payer") throw new Error(JSON.stringify(cached));
  if (row.events[0].borrowSemanticAction !== "openBorrow") throw new Error(JSON.stringify(row.events[0]));
  const stored = api.readStoredGasResult(cacheKey);
  if (!stored || stored.status !== "not-payer") throw new Error(JSON.stringify(stored));
  const urls = api.rpcUrlsForChain("arbitrum");
  if (urls[0] !== "https://gateway.example/arbitrum") throw new Error(JSON.stringify(urls));
  if (!urls.includes("https://arb1.arbitrum.io/rpc")) throw new Error(JSON.stringify(urls));
})().catch(error => {
  console.error(error);
  process.exit(1);
});
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_history_prioritizes_classification_receipts_and_fast_mode_skips_full_gas(self):
        self.assertIn('id="history-fast-mode"', self.html)
        self.assertIn("els.fastMode", self.source)
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyPriorityTest = { historyRowsForGasPriority };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyPriorityTest;
const plain = { txHash: "0xplain", gas: { status: "pending" } };
const classified = { txHash: "0xclassified", gas: { status: "pending" }, receiptClassificationPending: true };
const full = api.historyRowsForGasPriority([plain, classified], false);
if (full.classificationRows.map(row => row.txHash).join(",") !== "0xclassified") throw new Error(JSON.stringify(full));
if (full.backgroundRows.map(row => row.txHash).join(",") !== "0xplain") throw new Error(JSON.stringify(full));
if (full.skippedRows.length !== 0) throw new Error(JSON.stringify(full));
const fast = api.historyRowsForGasPriority([plain, classified], true);
if (fast.classificationRows.map(row => row.txHash).join(",") !== "0xclassified") throw new Error(JSON.stringify(fast));
if (fast.backgroundRows.length !== 0) throw new Error(JSON.stringify(fast));
if (fast.skippedRows.map(row => row.txHash).join(",") !== "0xplain") throw new Error(JSON.stringify(fast));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_loaded_all_history_can_apply_action_and_subset_chain_filters_without_rescan(self):
        script = r"""
const fs = require("fs");
const vm = require("vm");
const source = fs.readFileSync("history/history.js", "utf8");
const marker = "\n  if (document.readyState === \"loading\") {";
const instrumented = source.replace(marker, "\n  globalThis.__historyFilterReuseTest = { state, els, setSelectedActionsFromValues, filtersCanReuseLoadedRows };" + marker);
const sandbox = {
  console,
  URL,
  URLSearchParams,
  Blob,
  Set,
  Map,
  document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} } },
  window: { location: { href: "http://127.0.0.1/history/" }, history: { replaceState() {} }, localStorage: { getItem() { return null; }, setItem() {} } },
};
vm.runInNewContext(instrumented, sandbox);
const api = sandbox.__historyFilterReuseTest;
const wallet = "0x28da3dde285d8f1f87b2d858f89961bb8b9af180";
api.els.action = { value: "all", options: [
  { value: "all" },
  { value: "deposit" },
  { value: "withdraw" },
  { value: "claim" },
] };
api.state.address = wallet;
api.state.year = "custom";
api.state.dateFrom = "2026-01-01";
api.state.dateTo = "2026-07-07";
api.state.rows = [{ chainKey: "arbitrum" }, { chainKey: "ethereum" }];
api.state.loadedScope = {
  address: wallet,
  year: "custom",
  dateFrom: "2026-01-01",
  dateTo: "2026-07-07",
  action: "all",
  chains: ["arbitrum", "ethereum"],
};
api.state.selectedChains = new Set(["arbitrum"]);
api.setSelectedActionsFromValues(["deposit"]);
if (!api.filtersCanReuseLoadedRows()) throw new Error("deposit after all should filter locally");
api.state.selectedChains = new Set(["arbitrum", "berachain"]);
if (api.filtersCanReuseLoadedRows()) throw new Error("adding an unloaded chain should require reload");
api.state.selectedChains = new Set(["arbitrum"]);
api.state.loadedScope.action = "deposit";
api.setSelectedActionsFromValues(["claim"]);
if (api.filtersCanReuseLoadedRows()) throw new Error("claim after a deposit-only scan should require reload");
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True, env=NODE_ENV)

    def test_lessons_capture_history_report_direction(self):
        lessons = (ROOT / "lessons.md").read_text()
        self.assertIn("History Report Direction", lessons)
        self.assertIn("Dolomite Transaction History", lessons)
        self.assertIn("History TX first", lessons)
        self.assertIn("Opening/closing balances need real snapshots", lessons)
        self.assertIn("Tax-tool exports should be generic/mappable", lessons)


if __name__ == "__main__":
    unittest.main()
