import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_CORE = ROOT / "dashboard-core.html"
# The dashboard was split into markup + extracted stylesheet/script files;
# contracts apply to the combined source.
DASHBOARD_CORE_CSS = ROOT / "dashboard-core.css"
DASHBOARD_CORE_JS = ROOT / "dashboard-core.js"
ASSETS_LIVE_BUILDER = ROOT / "scripts" / "build_assets_live.mjs"
ASSETS_LIVE_WORKFLOW = ROOT / ".github" / "workflows" / "update-assets-live.yml"
PAGES_WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"
SECRET_GUARD_WORKFLOW = ROOT / ".github" / "workflows" / "secret-guard.yml"
ETHEREUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-ethereum-canonical-history.yml"
ARBITRUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-arbitrum-canonical-history.yml"
BERACHAIN_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-canonical-history.yml"
SECONDARY_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-secondary-canonical-history.yml"
BERACHAIN_NETFLOW_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-netflow.yml"
BERACHAIN_BORROW_ROUTE_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-borrow-route-history.yml"
EARN_FRESHNESS_WORKFLOW = ROOT / ".github" / "workflows" / "monitor-earn-freshness.yml"
EARN_COVERAGE_BACKFILL_WORKFLOW = ROOT / ".github" / "workflows" / "backfill-earn-canonical-coverage.yml"
EARN_WATCHDOG_DISPATCH_PLANNER = ROOT / "scripts" / "plan_earn_watchdog_dispatch.py"
EARN_SNAPSHOTS_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-snapshots.yml"
EARN_MERKL_REWARDS_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-merkl-rewards.yml"
EARN_FRESHNESS_SCRIPT = ROOT / "update_earn_freshness_status.py"
EARN_COMMIT_HELPER = ROOT / "scripts" / "commit_with_fresh_earn_status.sh"
EARN_RPC_POLICY = ROOT / "earn" / "earn-rpc-policy.js"
GLOBAL_PRIORITY_ADDRESSES = ROOT / "config" / "earn_canonical_priority_addresses.txt"
BERACHAIN_PRIORITY_ADDRESSES = ROOT / "config" / "earn_berachain_canonical_hot_addresses.txt"
EARN_COVERAGE_REPORT = ROOT / "report_earn_subaccount_history_coverage.py"
CANONICAL_REFRESH_RUNNER = ROOT / "run_earn_canonical_history_refresh.py"
NETFLOW_SCANNER = ROOT / "scan_earn_netflow.py"
SUBACCOUNT_EVENT_SCANNER = ROOT / "scan_earn_subaccount_history_events.py"
SUBACCOUNT_HISTORY_BUILDER = ROOT / "build_earn_subaccount_history.py"
NETFLOW_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-netflow.yml"
LIQUIDATION_PREVIEW = ROOT / "liquidation-preview.html"


class EarnDashboardContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = DASHBOARD_CORE.read_text(encoding="utf-8")
        for extracted in (DASHBOARD_CORE_CSS, DASHBOARD_CORE_JS):
            if extracted.exists():
                cls.source += "\n" + extracted.read_text(encoding="utf-8")

    def test_borrow_positions_prefer_replay_ledger_for_open_debt_cost(self):
        self.assertIn("function earn_getOpenDebtYieldForAccount", self.source)
        self.assertIn("const replayOpenDebtYieldWei = earn_getOpenDebtYieldForAccount", self.source)
        self.assertIn("source: replayDebtCostWei !== null ? 'replay-ledger' : 'index-estimate'", self.source)
        self.assertNotIn("const accruedTokens = actualTokens - absPar; // cost accrued", self.source)

    def test_dedicated_earn_route_skips_unrelated_dashboard_warmups(self):
        self.assertIn("function earn_isDedicatedRoutePage()", self.source)
        self.assertIn(
            "if (!earn_isDedicatedRoutePage()) {\n"
            "            fetch('vedolo_expiry.json'",
            self.source,
        )
        self.assertIn(
            "if (!earn_isDedicatedRoutePage()) {\n"
            "            fetch('vesting_investors.json'",
            self.source,
        )
        self.assertIn(
            "if (!earn_isDedicatedRoutePage()) {\n"
            "                assets_setupWarmupTriggers();",
            self.source,
        )

    def test_arbitrum_and_berachain_head_workflows_do_not_block_on_missing_histories(self):
        workflows = {
            "update-earn-arbitrum-canonical-history.yml": (
                "Select Arbitrum canonical head wallets",
                "Restore Arbitrum canonical runtime cache",
            ),
            "update-earn-berachain-canonical-history.yml": (
                "Select Berachain hot wallets",
                "Restore Berachain canonical runtime cache",
            ),
            "update-earn-secondary-canonical-history.yml": (
                "Select hot wallets",
                "Restore canonical runtime cache",
            ),
        }
        for filename, (selection_step, restore_step) in workflows.items():
            workflow = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
            self.assertIn("--existing-history-only", workflow)
            self.assertIn("STEADY_HOT_LIMIT", workflow)
            self.assertIn("--max-new-backfill-workers", workflow)
            self.assertLess(workflow.find(selection_step), workflow.find(restore_step))
            self.assertIn('history_path="data/earn-subaccount-history/${CHAIN}/${address}.json"', workflow)
            self.assertIn('git add -f "$history_path"', workflow)
            self.assertIn('done < "/tmp/earn-${CHAIN}-canonical-hot-addresses.txt"', workflow)
            isolate_cache = 'git stash push --keep-index --include-untracked --message "${CHAIN}-canonical-runtime"'
            sync_manifest = 'python3 scripts/sync_earn_subaccount_manifest.py --chain "$CHAIN"'
            self.assertIn(isolate_cache, workflow)
            self.assertIn(sync_manifest, workflow)
            self.assertLess(workflow.find(isolate_cache), workflow.find(sync_manifest))
            self.assertLess(workflow.find(isolate_cache), workflow.find("scripts/commit_with_fresh_earn_status.sh"))

    def test_verified_ledger_fetch_falls_back_to_address_prefix_shard(self):
        self.assertIn("VERIFIED_LEDGER_SHARD_BASE", self.source)
        self.assertIn("addr.slice(2, 4)", self.source)
        self.assertIn("shard.ledgers[addr]", self.source)
        self.assertLess(
            self.source.index("`${VERIFIED_LEDGER_SHARD_BASE}/${chainId}/${prefix}.json`"),
            self.source.index("`${VERIFIED_LEDGER_BASE}/${chainId}/${addr}.json`"),
        )

    def test_liquidation_risk_fetch_uses_wallet_shard(self):
        self.assertIn("LIQUIDATION_RISK_SHARD_BASE", self.source)
        self.assertIn("earn_fetchLiqRiskForWallet", self.source)
        self.assertNotIn("fetch('liquidation_risk.json')", self.source)

    def test_summary_keeps_historical_pnl_separate_from_current_price_total_yield(self):
        self.assertIn("Historical Yield P&amp;L", self.source)
        self.assertIn("historicalYieldUsd", self.source)
        self.assertIn("daily-snapshot-constant-par", self.source)

    def test_liquidation_risk_payload_is_only_loaded_for_current_borrow_debt(self):
        self.assertNotIn(
            "// Prefetch liquidation_risk.json in parallel with balance lookup",
            self.source,
        )
        self.assertIn(
            "const currentAccounts = await earn_fetchCurrentAccountsFromSubgraph(addr).catch(() => null);",
            self.source,
        )
        self.assertIn("const hasCurrentBorrow = currentAccounts.positions.some", self.source)
        self.assertIn("if (currentAccounts && sgEndpoint) {", self.source)
        self.assertIn("if (!hasCurrentBorrow) {", self.source)

    def test_yield_summary_distinguishes_current_market_checks_and_current_usd_value(self):
        self.assertIn('<div class="earn-summary-label">Current Markets Check</div>', self.source)
        self.assertNotIn('<div class="earn-summary-label">Ledger Check</div>', self.source)
        self.assertIn("' at current token prices'", self.source)

    def test_non_strict_yield_quality_can_override_verified_balance_badge(self):
        self.assertIn("function earn_getYieldQualityPresentation", self.source)
        self.assertIn("if (fallbackStatus === 'verified')", self.source)
        self.assertIn("const yieldQualityPresentation = earn_getYieldQualityPresentation(yieldCalc)", self.source)
        self.assertIn("method === 'all-netflow-verified'", self.source)
        self.assertIn("'Inferred Carry'", self.source)
        self.assertIn("label: 'Fallback'", self.source)
        self.assertIn("label: 'Inferred'", self.source)

    def test_public_netflow_matches_are_not_rendered_as_strict_verified(self):
        self.assertIn("rawLabel: 'Netflow Match'", self.source)
        self.assertIn("'This yield reconciles through public netflow plus snapshot history, but it is not strict replay verification.'", self.source)
        self.assertIn("? (canonicalHistoryCoverageIncomplete ? 'coverage_incomplete' : 'inferred')", self.source)

    def test_fresh_snapshot_netflow_inference_is_usable_but_not_strict(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
for (const name of [
  'earn_isTrustedReplayYieldStatus',
  'earn_canUseVerifiedLedgerMarketEntry',
]) {
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
  if (start < 0 || end < 0) throw new Error(`missing ${name}`);
  eval(source.slice(start, end));
}
const entry = {
  strictStatus: 'inferred',
  status: 'verified',
  strictMethod: 'netflow+snapshot',
  method: 'netflow+snapshot',
  canonicalHistoryCoverageStatus: 'fresh',
};
if (!earn_canUseVerifiedLedgerMarketEntry(entry)) {
  throw new Error('fresh snapshot/netflow inference was not reusable');
}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_inferred_ledger_does_not_receive_a_verified_historical_header(self):
        self.assertIn("let strictLedgerMarkets = 0;", self.source)
        self.assertIn("strictLedgerMarkets++;", self.source)
        self.assertIn("'Total Yield Earned · Historical evidence'", self.source)

    def test_verified_resolved_interest_ledger_is_validated_and_hydrated_as_fast_path(self):
        self.assertIn(
            "resolvedInterestLedger: shardEntry[2]",
            self.source,
        )
        self.assertIn("function earn_validatePublishedResolvedInterestLedger", self.source)
        self.assertIn("function earn_applyPublishedResolvedInterestLedger", self.source)
        self.assertIn(
            "earn_applyPublishedResolvedInterestLedger(verifiedLedger)",
            self.source,
        )
        self.assertIn(
            "if (earn_replayVerificationReady || earn_replayStatus === 'ready') return false;",
            self.source,
        )
        self.assertIn(
            "String(market.strictStatus || '').toLowerCase() !== 'verified'",
            self.source,
        )

    def test_published_resolved_ledger_precedes_background_replay_and_latest_balance_read(self):
        static_promise = self.source.index("const publishedResolvedLedgerPromise")
        replay_block = self.source.index("const replayBlockTagPromise", static_promise)
        static_apply = self.source.index("earn_applyPublishedResolvedInterestLedger(verifiedLedger)", static_promise)
        balance_read = self.source.index("earn_fetchActiveSupplyBalances(addr, { blockTag: 'latest' })", replay_block)
        first_render = self.source.index("earn_renderResults(enriched", balance_read)
        self.assertLess(static_promise, static_apply)
        self.assertLess(static_apply, replay_block)
        self.assertLess(balance_read, first_render)
        self.assertNotIn(
            "const replayBlockTag = await replayBlockTagPromise;\n"
            "                    balances = await earn_fetchActiveSupplyBalances",
            self.source,
        )

    def test_resolved_interest_fast_path_rejects_omitted_counted_market(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
for (const name of [
  'earn_parseBlockNumberLike',
  'earn_validatePublishedResolvedInterestLedger',
]) {
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
  if (start < 0 || end < 0) throw new Error(`missing ${name}`);
  eval(source.slice(start, end));
}
const verified = {
  status: 'verified', counted: true, canVerify: true,
  rawVerified: true, snapshotIncomplete: false,
};
const market = {
  earnYield: '1', settledYield: '0', settledSupplyYield: '0', settledBorrowYield: '0',
  openBorrowYield: '0', openSupplyYield: '1', openCollateralYield: '0',
  currentBorrowPar: '0', currentSupplyPar: '1', currentCollateralSupplyPar: '0',
  strictStatus: 'verified', strictMethod: 'interest-ledger',
};
const ledger = {
  snapshotDate: '2026-07-18',
  resolvedInterestLedger: {
    snapshotDate: '2026-07-18', comparisonBlock: 12345,
    strictStatus: 'verified', strictMethod: 'interest-ledger',
    canonicalHistoryCoverageStatus: 'fresh',
    markets: { '1': market },
    replayVerificationData: { '1': verified, '2': verified },
  },
};
if (earn_validatePublishedResolvedInterestLedger(ledger) !== null) {
  throw new Error('incomplete resolved ledger was accepted');
}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_reward_summaries_start_after_first_positions_render(self):
        self.assertIn("function earn_startDeferredSummaryFetches", self.source)
        self.assertNotIn("const summaryPrefetchTasks = [", self.source)
        first_render = self.source.index(
            "earn_renderResults(enriched, { skipSummary: false, softRefresh: !!cachedLookup });"
        )
        deferred_start = self.source.index(
            "earn_startDeferredSummaryFetches(addr, chainId, lookupRunId);",
            first_render,
        )
        self.assertGreater(deferred_start, first_render)
        final_batch_start = self.source.index("// Batch background data", deferred_start)
        final_batch_end = self.source.index("]).then(() => {", final_batch_start)
        self.assertNotIn("summaryPrefetchPromise", self.source[final_batch_start:final_batch_end])

    def test_live_balance_adjusted_replay_is_diagnostic_not_trusted_for_total_yield(self):
        self.assertIn("function earn_isTrustedReplayYieldMethod(method)", self.source)
        self.assertIn("method === 'interest-ledger-live-balance-adjusted'", self.source)
        self.assertIn("function earn_isTrustedReplayYieldStatus(status)", self.source)
        self.assertNotIn("|| status === 'live_balance_adjusted';", self.source)
        self.assertNotIn("|| method === 'interest-ledger-live-balance-adjusted';", self.source)
        self.assertIn("earn_isTrustedReplayYieldStatus(strictVerificationStatus)", self.source)
        self.assertIn("earn_isTrustedInterestLedgerYieldCalc(itemYieldCalc)", self.source)
        self.assertIn("trustedHistoryMethods.has(histMethod)", self.source)

    def test_aligned_live_balance_drift_is_kept_out_of_strict_yield(self):
        self.assertIn(
            "const adjustedInterestYieldCandidate = interestYieldCandidate !== null && alignedReplayWeiDrift\n"
            "                ? interestYieldCandidate + alignedReplayWeiDrift.totalYieldCorrection\n"
            "                : interestYieldCandidate;",
            self.source,
        )
        self.assertIn(
            "method === 'interest-ledger-live-balance-adjusted'\n"
            "                    ? 'coverage_incomplete'",
            self.source,
        )

    def test_decimal_parser_preserves_scientific_notation_exactly(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
const start = source.indexOf('function earn_decimalToBigInt(');
const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
if (start < 0 || end < 0) throw new Error('earn_decimalToBigInt not found');
eval(source.slice(start, end));
const cases = [
  ['1e-7', 18, '100000000000'],
  ['1.234567e1', 6, '12345670'],
  ['-2.5e-3', 6, '-2500'],
  ['1', Infinity, '1'],
];
for (const [input, decimals, expected] of cases) {
  const actual = earn_decimalToBigInt(input, decimals).toString();
  if (actual !== expected) throw new Error(`${input}: expected ${expected}, got ${actual}`);
}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_yield_calculation_release_invalidates_prior_lookup_cache(self):
        self.assertIn("const EARN_LOOKUP_CACHE_VERSION = 15;", self.source)
        self.assertIn("parsed.version !== EARN_LOOKUP_CACHE_VERSION", self.source)

    def test_earn_rpc_policy_is_loaded_before_dashboard_runtime(self):
        html = DASHBOARD_CORE.read_text(encoding="utf-8")
        policy_pos = html.index('src="earn/earn-rpc-policy.js')
        runtime_pos = html.index('src="dashboard-core.js')
        self.assertLess(policy_pos, runtime_pos)
        builder = (ROOT / "build_earn_bundle.py").read_text(encoding="utf-8")
        self.assertIn('<script src="earn/earn-rpc-policy.js', builder)

    def test_earn_rpc_requests_use_lookup_scoped_endpoint_health(self):
        self.assertTrue(EARN_RPC_POLICY.exists())
        self.assertIn("EarnRpcPolicy.create", self.source)
        self.assertIn("earn_rpcPolicy.recordFailure", self.source)
        self.assertIn("earn_rpcPolicy.recordSuccess", self.source)
        self.assertIn("earn_resetRpcPolicy();", self.source)

    def test_lookup_cache_preserves_trusted_markets_during_rpc_degradation(self):
        html = DASHBOARD_CORE.read_text(encoding="utf-8")
        cache_policy_pos = html.index('src="earn/earn-cache-policy.js')
        runtime_pos = html.index('src="dashboard-core.js')
        self.assertLess(cache_policy_pos, runtime_pos)
        self.assertIn("EarnCachePolicy.mergeTrustedLookupSnapshot", self.source)
        self.assertIn("preservedTrustedMarketIds", self.source)

    def test_earn_freshness_separates_live_recency_from_historical_coverage(self):
        self.assertIn("Live data", self.source)
        self.assertIn("Historical verification", self.source)
        self.assertIn("earn-freshness-separator", self.source)
        self.assertNotIn("recencyFresh ? 'Fresh chain data' : 'Chain data syncing'", self.source)

    def test_canonical_coverage_has_a_separate_growing_backfill_workflow(self):
        self.assertTrue(EARN_COVERAGE_BACKFILL_WORKFLOW.exists())
        workflow = EARN_COVERAGE_BACKFILL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("--coverage-backfill", workflow)
        self.assertIn("run_earn_data_correctness_pipeline.py", workflow)
        self.assertIn("cron: '11 */2 * * *'", workflow)
        self.assertIn("max-parallel: 3", workflow)
        self.assertIn("group: ${{ matrix.concurrency_group }}", workflow)
        self.assertIn("earn-arbitrum-canonical-history", workflow)
        self.assertIn("earn-berachain-canonical-history", workflow)
        self.assertIn("earn-secondary-canonical-history-mantle", workflow)
        self.assertNotIn("--existing-history-only", workflow)
        self.assertIn("build_earn_verified_ledger_shards.py", workflow)
        self.assertLess(
            workflow.index("build_earn_resolved_interest_ledger.py"),
            workflow.index("build_earn_verified_ledger.py"),
        )
        self.assertIn("data/earn-resolved-interest-ledger", workflow)

    def test_snapshot_workflow_refreshes_existing_resolved_ledgers_before_public_ledgers(self):
        workflow = (ROOT / ".github/workflows/update-earn-snapshots.yml").read_text(encoding="utf-8")
        self.assertIn("build_earn_resolved_interest_ledger.py", workflow)
        self.assertIn("--existing-addresses", workflow)
        self.assertLess(
            workflow.index("build_earn_resolved_interest_ledger.py"),
            workflow.index("build_earn_verified_ledger.py"),
        )
        self.assertIn("git add -f data/earn-resolved-interest-ledger/", workflow)

    def test_earn_commit_helper_resynchronizes_resolved_manifest_after_rebase(self):
        helper = (ROOT / "scripts/commit_with_fresh_earn_status.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/sync_earn_resolved_manifest.py", helper)
        self.assertIn("data/earn-resolved-interest-ledger/manifest.json", helper)

    def test_truncated_subgraph_replay_is_never_strictly_verified(self):
        self.assertIn("const replayTruncated = [", self.source)
        self.assertIn("verificationIncomplete: replayTruncated", self.source)
        self.assertIn("earn_replayTruncatedSubgraphMarkets = replay.verificationIncomplete", self.source)
        self.assertIn(
            "const subgraphReplayTruncated = earn_replaySubgraphHistoryIncomplete\n"
            "                    || earn_replayTruncatedSubgraphMarkets.has(String(mid));",
            self.source,
        )
        self.assertIn("|| subgraphReplayTruncated\n                    || replayStateAdjusted;", self.source)

    def test_incomplete_subgraph_replay_withholds_strict_status_for_every_market(self):
        self.assertIn("let earn_replaySubgraphHistoryIncomplete = false;", self.source)
        self.assertIn("earn_replaySubgraphHistoryIncomplete = !!replay.verificationIncomplete;", self.source)
        self.assertIn(
            "const subgraphReplayTruncated = earn_replaySubgraphHistoryIncomplete\n"
            "                    || earn_replayTruncatedSubgraphMarkets.has(String(mid));",
            self.source,
        )

    def test_truncated_subgraph_replay_explains_the_actual_verification_gap(self):
        self.assertIn("subgraphReplayTruncated", self.source)
        self.assertIn("Subgraph fallback returned only part of this market", self.source)

    def _assert_strict_status_for_incomplete_replay(self, provenance_flag):
        script = f"""
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
for (const name of [
  'earn_absBigInt',
  'earn_getStrictVerificationStatus',
]) {{
  const start = source.indexOf(`function ${{name}}(`);
  const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
  if (start < 0 || end < 0) throw new Error(`missing ${{name}}`);
  eval(source.slice(start, end));
}}
const entry = {{
  counted: true,
  canVerify: true,
  snapshotIncomplete: true,
  {provenance_flag}: true,
  rawVerified: true,
  usdDriftVerified: true,
  maxUsdDrift: 0,
  actualSupplyPar: 100n,
  actualSupplyWei: 110n,
  actualCollateralPar: 0n,
  actualCollateralWei: 0n,
  actualBorrowPar: 0n,
  actualBorrowWei: 0n,
  expectedSupplyPar: 100n,
  expectedSupplyWei: 110n,
  expectedCollateralPar: 0n,
  expectedCollateralWei: 0n,
  expectedBorrowPar: 0n,
  expectedBorrowWei: 0n,
  supplyParDiff: 0n,
  supplyWeiDiff: 0n,
  collateralParDiff: 0n,
  collateralWeiDiff: 0n,
  borrowParDiff: 0n,
  borrowWeiDiff: 0n,
  parTolerance: 0n,
  supplyWeiTolerance: 0n,
  collateralWeiTolerance: 0n,
  borrowWeiTolerance: 0n,
}};
const actual = earn_getStrictVerificationStatus(entry);
if (actual !== 'coverage_incomplete') {{
  throw new Error(`expected coverage_incomplete, got ${{actual}}`);
}}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_truncated_replay_cannot_be_strict_verified(self):
        self._assert_strict_status_for_incomplete_replay("subgraphReplayTruncated")

    def test_snapshot_supplemented_replay_cannot_be_strict_verified(self):
        self._assert_strict_status_for_incomplete_replay("snapshotSupplemented")

    def test_reconciled_replay_state_cannot_be_strict_verified(self):
        self._assert_strict_status_for_incomplete_replay("replayStateAdjusted")

    def test_live_index_refresh_overrides_stale_subgraph_indexes(self):
        refresh_start = self.source.index("async function earn_refreshReplayWithLiveCurrentIndexes(")
        refresh_end = self.source.index("\n        function ", refresh_start + 1)
        self.assertGreater(refresh_start, -1)
        self.assertGreater(refresh_end, refresh_start)
        refresh_source = self.source[refresh_start:refresh_end]
        self.assertIn(
            "earn_mergeReplayCurrentIndexMap(\n"
            "                    liveCurrentIndexMap,\n"
            "                    replay.currentIndexMap || {}\n"
            "                )",
            refresh_source,
        )

    def test_replay_par_to_wei_matches_protocol_half_up_rounding(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
const start = source.indexOf('function earn_parToWeiRoundHalfUp(');
const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
if (start < 0 || end < 0) throw new Error('earn_parToWeiRoundHalfUp not found');
eval(source.slice(start, end));
const scale = 10n ** 18n;
if (earn_parToWeiRoundHalfUp(1n, scale + scale / 2n) !== 2n) {
  throw new Error('half-up boundary was truncated');
}
if (earn_parToWeiRoundHalfUp(1n, scale + scale / 2n - 1n) !== 1n) {
  throw new Error('value below half-up boundary rounded too early');
}
const par = 1517885155478936933n;
const index = 1350894139995677699n;
if (earn_parToWeiRoundHalfUp(par, index) !== 2050502161722924040n) {
  throw new Error('protocol fixture did not reproduce getAccountWei');
}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def _assert_non_exact_replay_is_mismatch(self, overrides):
        script = f"""
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
for (const name of [
  'earn_absBigInt',
  'earn_shouldTrustVisibleSupplyReplayMismatch',
  'earn_getAlignedReplayWeiDriftAdjustment',
  'earn_shouldTrustAlignedReplayWeiDrift',
  'earn_getStrictVerificationStatus',
]) {{
  const start = source.indexOf(`function ${{name}}(`);
  if (start < 0) continue;
  const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
  if (end < 0) throw new Error(`incomplete ${{name}}`);
  eval(source.slice(start, end));
}}
const entry = {{
  counted: true,
  canVerify: true,
  snapshotIncomplete: false,
  subgraphReplayTruncated: false,
  replayStateAdjusted: false,
  rawVerified: false,
  actualSupplyPar: 100n,
  expectedSupplyPar: 100n,
  actualSupplyWei: 100n,
  expectedSupplyWei: 100n,
  actualCollateralPar: 0n,
  expectedCollateralPar: 0n,
  actualCollateralWei: 0n,
  expectedCollateralWei: 0n,
  actualBorrowPar: 0n,
  expectedBorrowPar: 0n,
  actualBorrowWei: 0n,
  expectedBorrowWei: 0n,
  supplyParDiff: 0n,
  supplyWeiDiff: 0n,
  collateralParDiff: 0n,
  collateralWeiDiff: 0n,
  borrowParDiff: 0n,
  borrowWeiDiff: 0n,
  parTolerance: 0n,
  supplyWeiTolerance: 0n,
  collateralWeiTolerance: 0n,
  borrowWeiTolerance: 0n,
  {overrides}
}};
const actual = earn_getStrictVerificationStatus(entry);
if (actual !== 'mismatch') {{
  throw new Error(`expected mismatch, got ${{actual}}`);
}}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_aligned_wei_drift_cannot_be_strict_verified(self):
        self._assert_non_exact_replay_is_mismatch(
            "actualSupplyWei: 200n, supplyWeiDiff: 100n,"
        )

    def test_ghost_borrow_cannot_be_strict_verified(self):
        self._assert_non_exact_replay_is_mismatch(
            "expectedBorrowPar: 500n, expectedBorrowWei: 600n, borrowParDiff: -500n, borrowWeiDiff: -600n,"
        )

    def test_render_yield_cache_calculates_each_asset_once(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
const start = source.indexOf('function earn_createVerifiedYieldCalcCache(');
const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
if (start < 0 || end < 0) throw new Error('earn_createVerifiedYieldCalcCache not found');
let calls = 0;
let receivedOpts = null;
globalThis.earn_calculateYield = (position, opts) => {
  calls += 1;
  receivedOpts = opts;
  return { position, calls };
};
eval(source.slice(start, end));
const getVerifiedYieldCalc = earn_createVerifiedYieldCalcCache();
const asset = { marketId: '7' };
const first = getVerifiedYieldCalc(asset);
const second = getVerifiedYieldCalc(asset);
if (calls !== 1) throw new Error(`expected one calculation, got ${calls}`);
if (first !== second) throw new Error('cached result identity changed');
if (!receivedOpts || receivedOpts.requireVerifiedInterest !== true) {
  throw new Error('cache did not request verified interest');
}
const renderStart = source.indexOf('function earn_renderResults(');
const renderEnd = source.indexOf(String.fromCharCode(10) + '        function ', renderStart + 1);
const renderSource = source.slice(renderStart, renderEnd);
if (!renderSource.includes('const getVerifiedYieldCalc = earn_createVerifiedYieldCalcCache();')) {
  throw new Error('render does not create a verified-yield cache');
}
if ((renderSource.match(/getVerifiedYieldCalc\(a\)/g) || []).length < 2) {
  throw new Error('render does not reuse the verified-yield cache in table and summary paths');
}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_summary_does_not_label_missing_yield_as_verified_zero(self):
        self.assertIn(
            "const hasVerifiedTotalYield = trustedYieldAssetCount > 0;",
            self.source,
        )
        self.assertIn(
            "(earn_totalYieldStatus === 'loading' || earn_replayStatus === 'loading')",
            self.source,
        )
        self.assertIn("'No verified yield data'", self.source)
        self.assertIn("hasVerifiedTotalYield ? `${yieldSign}${earn_formatUSD(Math.abs(totalYieldUsd))}` : '—'", self.source)
        self.assertIn("hasBorrowOnlyEarnContext ? '—' : isTotalYieldLoading ? 'Loading...'", self.source)

    def test_unverified_yield_is_never_rendered_as_a_numeric_result(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
for (const name of [
  'earn_isTrustedReplayYieldMethod',
  'earn_isTrustedReplayYieldStatus',
  'earn_isTrustedInterestLedgerYieldCalc',
]) {
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
  if (start < 0 || end < 0) throw new Error(`missing ${name}`);
  eval(source.slice(start, end));
}
const cases = [
  [{ hasData: true, trustedForTotal: true, verificationStatus: 'verified', method: 'interest-ledger' }, true],
  [{ hasData: true, trustedForTotal: true, verificationStatus: 'verified', method: 'interest-ledger-override' }, true],
  [{ hasData: true, trustedForTotal: false, verificationStatus: 'fallback', method: 'all-netflow' }, false],
  [{ hasData: true, trustedForTotal: false, verificationStatus: 'coverage_incomplete', method: 'interest-ledger' }, false],
  [{ hasData: true, trustedForTotal: false, verificationStatus: 'mismatch', method: 'interest-ledger' }, false],
  [{ hasData: true, trustedForTotal: true, verificationStatus: 'verified', method: 'all-netflow-verified' }, false],
  [{ hasData: false, trustedForTotal: true, verificationStatus: 'verified', method: 'interest-ledger' }, false],
];
for (const [value, expected] of cases) {
  const actual = earn_isTrustedInterestLedgerYieldCalc(value);
  if (actual !== expected) throw new Error(`${JSON.stringify(value)}: expected ${expected}, got ${actual}`);
}
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        self.assertIn(
            "const hasVerifiedYield = earn_isTrustedInterestLedgerYieldCalc(yieldCalc);",
            self.source,
        )
        self.assertIn(
            "item.yieldTrustedForDisplay = earn_isTrustedInterestLedgerYieldCalc(calc);",
            self.source,
        )
        self.assertIn(
            "resolvedTrustedForTotal = earn_isTrustedInterestLedgerYieldCalc(yieldCalc);",
            self.source,
        )
        self.assertIn(
            "histMeta.resolvedTrustedForTotal === true",
            self.source,
        )
        self.assertIn(
            "if (earn_isTrustedInterestLedgerYieldCalc(yieldCalc))",
            self.source,
        )

    def test_replay_keeps_closed_borrow_cost_out_of_earn_yield(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
for (const name of [
  'earn_normalizeMarketId',
  'earn_addSettledYield',
  'earn_settleReducedExposureYield',
  'earn_summarizeReplayAccountStates',
]) {
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf(String.fromCharCode(10) + '        function ', start + 1);
  if (start < 0 || end < 0) throw new Error(`missing ${name}`);
  eval(source.slice(start, end));
}

const supplyState = {
  par: 100n,
  liveYield: 10n,
  settledYield: 0n,
  settledSupplyYield: 0n,
  settledBorrowYield: 0n,
};
earn_settleReducedExposureYield(supplyState, 0n);
if (supplyState.settledSupplyYield !== 10n || supplyState.settledBorrowYield !== 0n) {
  throw new Error('closed supply yield was not routed to settledSupplyYield');
}

const borrowState = {
  par: -100n,
  liveYield: -5n,
  settledYield: 0n,
  settledSupplyYield: 0n,
  settledBorrowYield: 0n,
};
earn_settleReducedExposureYield(borrowState, 0n);
if (borrowState.settledSupplyYield !== 0n || borrowState.settledBorrowYield !== -5n) {
  throw new Error('closed borrow cost was not routed to settledBorrowYield');
}

const result = earn_summarizeReplayAccountStates({
  '0|1': {
    account: '0', marketId: '1', par: '0', settledYield: '10',
    settledSupplyYield: '10', settledBorrowYield: '0', liveYield: '0',
    hadSupply: true, hadBorrow: false,
  },
  '1|1': {
    account: '1', marketId: '1', par: '0', settledYield: '-5',
    settledSupplyYield: '0', settledBorrowYield: '-5', liveYield: '0',
    hadSupply: false, hadBorrow: true,
  },
}, {});
const market = result.interestYieldData['1'];
if (market.earnYield !== '10') throw new Error(`earn yield included borrow cost: ${market.earnYield}`);
if (market.settledYield !== '10') throw new Error(`settled supply yield is ${market.settledYield}`);
if (market.settledBorrowYield !== '-5') throw new Error(`settled borrow cost is ${market.settledBorrowYield}`);
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("const hasSupplyYield = interestMeta", self.source)
        self.assertNotIn("const hasSettledYield = interestMeta", self.source)

    def test_subgraph_replay_paginates_with_cursor_instead_of_a_hard_offset_cap(self):
        self.assertIn("orderBy: id, orderDirection: asc", self.source)
        self.assertIn("id_gt:", self.source)
        self.assertNotIn("if (skip > 10000 && hasMore)", self.source)

    def test_subgraph_cursor_paginator_collects_each_page_without_offset_loss(self):
        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
const start = source.indexOf('async function earn_subgraphPaginateAll(');
const end = source.indexOf(String.fromCharCode(10) + '        async function ', start + 1);
if (start < 0 || end < 0) throw new Error('earn_subgraphPaginateAll not found');
eval(source.slice(start, end));

const makePage = (start, count) => Array.from({ length: count }, (_, index) => ({
  id: `event-${String(start + index).padStart(6, '0')}`,
}));
const pages = [makePage(0, 1000), makePage(1000, 1000), makePage(2000, 1)];
const queries = [];
globalThis.earn_subgraphQuery = async (_endpoint, query) => {
  queries.push(query);
  return { events: pages.shift() || [] };
};

(async () => {
  const rows = await earn_subgraphPaginateAll('', 'events', '{ owner: "0xabc" }', 'serialId');
  if (rows.length !== 2001) throw new Error(`expected 2001 rows, got ${rows.length}`);
  if (rows.truncated) throw new Error('complete cursor pagination was marked truncated');
  if (!queries[1].includes('id_gt: "event-000999"')) throw new Error('second page did not use the first page cursor');
  if (!queries[2].includes('id_gt: "event-001999"')) throw new Error('third page did not use the second page cursor');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_earn_shows_verified_background_refresh_status(self):
        self.assertIn("earn-data-freshness-pill", self.source)
        self.assertIn("EARN_FRESHNESS_STATUS_URL", self.source)
        self.assertIn("Fresh data · refreshing in background", self.source)
        self.assertIn("earn_loadFreshnessStatus().then(earn_updateFreshnessPill)", self.source)
        self.assertIn("chainStatus.canonical?.refreshMode", self.source)
        self.assertIn("chainStatus.netflow?.refreshMode", self.source)
        self.assertIn("freshnessModes.includes('background')", self.source)
        self.assertIn("earn_formatCanonicalCoverageLabel", self.source)
        self.assertIn("Live data", self.source)
        self.assertIn("Historical verification", self.source)
        self.assertIn("earn_formatFreshnessAge", self.source)
        self.assertNotIn("canonical coverage syncing", self.source)
        self.assertIn("chainStatus?.canonical?.coverageCatchup === true", self.source)
        self.assertIn("chainStatus?.canonical?.coverageBacklog === true", self.source)
        self.assertIn("backfill pending", self.source)
        self.assertIn("coverage?.backfilledWalletCount", self.source)
        self.assertIn("coverage?.headFreshWalletCount ?? coverage?.freshWalletCount", self.source)
        self.assertIn(
            "earn_formatCanonicalCoverageLabel(canonicalCoverage, canonicalCoverageBacklog)",
            self.source,
        )

    def test_earn_chain_filter_keeps_archived_networks_last_and_labeled(self):
        start = self.source.index("const EARN_CHAINS = {")
        end = self.source.index("// oDOLO token address", start)
        chains = self.source[start:end]

        self.assertLess(chains.index("xlayer: {"), chains.index("polygonzkevm: {"))
        self.assertLess(chains.index("polygonzkevm: {"), chains.index("botanix: {"))
        self.assertEqual(2, chains.count("earnLifecycle: 'archived'"))
        self.assertEqual(2, chains.count("earnLifecycleLabel: 'Archived'"))
        self.assertIn("earn-chain-option-copy", self.source)
        self.assertIn("earn-chain-option-status", self.source)

    def test_earn_freshness_pill_wraps_inside_mobile_width(self):
        self.assertIn(".earn-data-freshness-pill", self.source)
        start = self.source.index(".earn-data-freshness-pill {")
        end = self.source.index(".earn-data-freshness-pill.visible", start)
        rules = self.source[start:end]
        self.assertIn("max-width: 100%;", rules)
        self.assertIn("white-space: normal;", rules)
        self.assertIn("overflow-wrap: anywhere;", rules)

    def test_earn_monitor_publishes_quality_status(self):
        workflow = EARN_FRESHNESS_WORKFLOW.read_text(encoding="utf-8")
        helper = EARN_COMMIT_HELPER.read_text(encoding="utf-8")
        self.assertIn("build_earn_quality_status.py --output data/earn-quality/status.json", workflow)
        self.assertIn("git add data/earn-freshness/status.json data/earn-quality/status.json", workflow)
        self.assertIn("quality_output=\"${EARN_QUALITY_STATUS_OUTPUT:-data/earn-quality/status.json}\"", helper)
        self.assertIn("python3 build_earn_quality_status.py --output \"$quality_output\"", helper)

    def test_assets_live_builder_keeps_cached_rate_rows_when_chain_api_is_empty(self):
        source = ASSETS_LIVE_BUILDER.read_text(encoding="utf-8")
        self.assertIn('import { readFile, writeFile } from "node:fs/promises";', source)
        self.assertIn("async function loadPreviousAssetsSnapshot()", source)
        self.assertIn("function cachedRateRowsForChain", source)
        self.assertIn("fetchRateRowsForChainWithFallback", source)
        self.assertIn("rateFallbackSourceGeneratedAt", source)
        self.assertIn("rateFallbackMaxAgeMinutes", source)
        self.assertIn("cached-rate-fallback", source)
        self.assertIn('const RETIRED_CHAIN_KEYS = new Set(["botanix", "polygonzkevm"]);', source)
        self.assertIn("function activeChainKeys()", source)
        self.assertIn("retiredChains", source)

    def test_archived_chains_are_not_scheduled_for_live_earn_refreshes(self):
        for workflow_path in (
            EARN_SNAPSHOTS_WORKFLOW,
            SECONDARY_CANONICAL_WORKFLOW,
            NETFLOW_WORKFLOW,
            EARN_MERKL_REWARDS_WORKFLOW,
            EARN_FRESHNESS_WORKFLOW,
        ):
            workflow = workflow_path.read_text(encoding="utf-8")
            self.assertNotIn("botanix", workflow, workflow_path.name)
            self.assertNotIn("polygonzkevm", workflow, workflow_path.name)

    def test_assets_live_workflow_warns_on_stale_cached_rate_fallback(self):
        workflow = ASSETS_LIVE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("Check assets fallback freshness", workflow)
        self.assertIn("Assets rate fallback", workflow)
        self.assertIn("Stale assets rate fallback", workflow)
        self.assertIn("rateFallbackMaxAgeMinutes", workflow)
        self.assertIn("ASSETS_RATE_FALLBACK_MAX_AGE_MINUTES: '360'", workflow)
        self.assertIn(
            "process.env.ASSETS_RATE_FALLBACK_MAX_AGE_MINUTES || 360",
            workflow,
        )
        self.assertIn("for i in $(seq 1 12)", workflow)
        self.assertIn("Failed to push after 12 attempts.", workflow)

    def test_pages_workflow_keeps_earn_deploys_out_of_workflow_run(self):
        workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("push:", workflow)
        self.assertIn("- master", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("- Update EARN Snapshots", workflow)
        self.assertNotIn("- Update Earn snapshots", workflow)
        self.assertNotIn("- Update Earn freshness and quality status", workflow)
        self.assertNotIn("- Update oDOLO Contract Data", workflow)

    def test_pages_deployment_checks_generated_bundle_and_representative_markets(self):
        workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("python3 build_earn_bundle.py --check", workflow)
        self.assertIn("python3 build_earn_representative_audit.py --check", workflow)

    def test_earn_refresh_workflows_publish_compact_ledger_shards(self):
        for path in (
            ARBITRUM_CANONICAL_WORKFLOW,
            BERACHAIN_CANONICAL_WORKFLOW,
            ETHEREUM_CANONICAL_WORKFLOW,
            SECONDARY_CANONICAL_WORKFLOW,
            EARN_SNAPSHOTS_WORKFLOW,
        ):
            workflow = path.read_text(encoding="utf-8")
            self.assertIn("build_earn_verified_ledger_shards.py", workflow, path.name)
            self.assertIn("data/earn-verified-ledger-shards", workflow, path.name)

    def test_snapshot_workflow_refreshes_historical_prices_before_ledgers(self):
        workflow = EARN_SNAPSHOTS_WORKFLOW.read_text(encoding="utf-8")
        self.assertLess(
            workflow.index("build_earn_historical_prices.py"),
            workflow.index("build_earn_verified_ledger.py"),
        )
        self.assertIn("data/earn-historical-prices", workflow)

    def test_earn_commit_helper_dispatches_pages_after_action_token_push(self):
        helper = EARN_COMMIT_HELPER.read_text(encoding="utf-8")
        self.assertIn("EARN_DISPATCH_PAGES_AFTER_PUSH", helper)
        self.assertIn("gh workflow run pages.yml --ref \"$git_branch\"", helper)
        self.assertIn("Skipping GitHub Pages deploy dispatch", helper)
        for workflow_path in (
            EARN_SNAPSHOTS_WORKFLOW,
            ETHEREUM_CANONICAL_WORKFLOW,
            ARBITRUM_CANONICAL_WORKFLOW,
            BERACHAIN_CANONICAL_WORKFLOW,
            SECONDARY_CANONICAL_WORKFLOW,
            BERACHAIN_NETFLOW_WORKFLOW,
            BERACHAIN_BORROW_ROUTE_WORKFLOW,
            NETFLOW_WORKFLOW,
            EARN_MERKL_REWARDS_WORKFLOW,
            EARN_FRESHNESS_WORKFLOW,
        ):
            workflow = workflow_path.read_text(encoding="utf-8")
            self.assertIn("actions: write", workflow, workflow_path.name)
            self.assertIn("GH_TOKEN: ${{ github.token }}", workflow, workflow_path.name)
            self.assertIn("github.actor == 'github-actions[bot]'", workflow, workflow_path.name)

    def test_earn_workflows_use_shallow_checkout(self):
        for workflow_path in (
            EARN_SNAPSHOTS_WORKFLOW,
            ETHEREUM_CANONICAL_WORKFLOW,
            ARBITRUM_CANONICAL_WORKFLOW,
            BERACHAIN_CANONICAL_WORKFLOW,
            SECONDARY_CANONICAL_WORKFLOW,
            BERACHAIN_NETFLOW_WORKFLOW,
            BERACHAIN_BORROW_ROUTE_WORKFLOW,
            NETFLOW_WORKFLOW,
            EARN_MERKL_REWARDS_WORKFLOW,
            EARN_FRESHNESS_WORKFLOW,
        ):
            workflow = workflow_path.read_text(encoding="utf-8")
            self.assertNotIn("fetch-depth: 0", workflow, workflow_path.name)
            if "fetch-depth:" in workflow:
                self.assertIn("fetch-depth: 1", workflow, workflow_path.name)

    def test_secret_guard_uses_shallow_checkout(self):
        workflow = SECRET_GUARD_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("fetch-depth: 0", workflow)
        self.assertIn("fetch-depth: 1", workflow)
        self.assertIn("ensure_commit", workflow)

    def test_earn_commit_helper_regenerates_freshness_after_rebase(self):
        helper = EARN_COMMIT_HELPER.read_text(encoding="utf-8")
        rebase_pos = helper.index('git pull --rebase -X theirs "$git_remote" "$git_branch"')
        manifest_pos = helper.index("sync_earn_verified_manifest.py --base-ref")
        refresh_pos = helper.index("update_earn_freshness_status.py --output")
        amend_pos = helper.index("git commit --amend --no-edit")
        push_pos = helper.index('git push "$git_remote" "HEAD:$git_branch"')
        self.assertLess(rebase_pos, refresh_pos)
        self.assertLess(rebase_pos, manifest_pos)
        self.assertLess(manifest_pos, refresh_pos)
        self.assertLess(refresh_pos, amend_pos)
        self.assertLess(amend_pos, push_pos)
        self.assertIn("data/earn-verified-ledger/", helper)
        self.assertIn("git add -f data/earn-verified-ledger/manifest.json", helper)
        self.assertIn("ledger_manifest_staged", helper)
        self.assertIn("ledger_chains", helper)
        self.assertIn("ledger_sync_all", helper)
        self.assertIn("--all-chains", helper)
        self.assertIn('CHAIN:-', helper)
        self.assertIn("git rebase --abort", helper)
        self.assertIn("EARN_FRESHNESS_ACTIONS_OUTPUT", helper)
        self.assertIn('EARN_GIT_REMOTE:-origin', helper)
        self.assertIn('EARN_PUSH_ATTEMPTS:-40', helper)
        self.assertIn('EARN_PUSH_RETRY_SLEEP_SECONDS:-5', helper)
        self.assertIn('EARN_PUSH_MAX_RETRY_SLEEP_SECONDS:-30', helper)
        self.assertIn("RANDOM % 4", helper)
        self.assertIn("Failed to push after $attempts attempts.", helper)

    def test_rewards_card_has_merkl_unavailable_state(self):
        self.assertIn("merklUnavailable: false", self.source)
        self.assertIn("WLFI source did not respond", self.source)
        self.assertIn("earn-summary-mini-pill is-warning", self.source)

    def test_merkl_wlfi_rewards_are_named_and_attributed_per_asset(self):
        self.assertIn("'576387d3d84237f5': '0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d'", self.source)
        self.assertIn("'2ed15ca6f6a47991': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'", self.source)
        self.assertIn("function earn_getMerklRewardSymbolForPart", self.source)
        self.assertIn("const merklRatesReady = earn_fetchMarketRates(chainId).catch", self.source)
        self.assertIn("earn_resolveMerklReasonTokenAddress(chainId, reasonKey, reward, campaignTokenMap)", self.source)
        self.assertIn("assetSymbol: symbol", self.source)
        self.assertIn("earn-lend-token-source", self.source)
        self.assertIn("allocation:", self.source)
        self.assertIn("unclaimedPerToken", self.source)
        self.assertIn("function earn_getRewardUnclaimedForToken", self.source)
        self.assertIn("earn-reward-state-chip", self.source)
        self.assertIn("Claimed est.", self.source)
        self.assertIn("earn_odoloAmountPerNetwork", self.source)
        self.assertIn("function earn_getOdoloEstimateForPosition", self.source)
        self.assertIn("generated: ~", self.source)
        self.assertIn("earn_renderResults(earn_cachedAssets, { skipSummary: true, softRefresh: true })", self.source)
        self.assertIn("const rewardSymbol = String(ys.rewardSymbol || '').trim()", self.source)
        self.assertIn("EARN_MERKL_REWARDS_BASE", self.source)
        self.assertIn("earn_fetchCachedMerklRewards", self.source)

    def test_merkl_live_rewards_use_v4_and_count_pending_without_cross_market_attribution(self):
        self.assertIn("/v4/users/${encodeURIComponent(addr)}/protocols/dolomite/rewards?chainId=${numericId}", self.source)
        self.assertNotIn("api.merkl.xyz/v3/rewards", self.source)
        self.assertIn("function earn_parseMerklV4Rewards", self.source)
        self.assertIn("earn_getMerklV4AccumulatedRaw", self.source)
        self.assertIn("earn_getMerklV4UnclaimedRaw", self.source)
        self.assertIn("earn_getMerklOpportunityTokenMap", self.source)

        script = """
const fs = require('fs');
const source = fs.readFileSync('dashboard-core.js', 'utf8');
const start = source.indexOf('function earn_createRewardBucket(');
const end = source.indexOf(String.fromCharCode(10) + '        function earn_applyMerklRewardsResult', start + 1);
if (start < 0 || end < 0) throw new Error('Merkl v4 parser not found');
globalThis.earn_bigIntToDecimalNumber = (value, decimals) => Number(value) / (10 ** decimals);
globalThis.earn_resolveMerklReasonTokenAddress = (_chain, reason) => {
  if (reason.startsWith('Dolomite_')) return '0xusd1';
  if (reason.startsWith('MultiLogPerAdditionalParam_')) return '0xusdc';
  return '';
};
eval(source.slice(start, end));

const payload = [{
  chain: { id: 1 },
  rewards: [{
    token: { symbol: 'WLFI', decimals: 6, address: '0xreward' },
    breakdowns: [
      { reason: 'Dolomite_0xmarket', amount: '1500000', claimed: '500000', pending: '250000' },
      { reason: 'MultiLogPerAdditionalParam_accountNumber_0_1', amount: '2000000', claimed: '2000000', pending: '0' },
      { reason: 'not-dolomite', amount: '999999999', claimed: '0', pending: '0' },
    ],
  }],
}];
const rewards = earn_parseMerklV4Rewards('ethereum', 1, payload, {});
const wlfi = rewards.WLFI;
if (!wlfi) throw new Error('expected WLFI bucket');
if (wlfi.accumulated !== 3.75) throw new Error(`wrong accumulated ${wlfi.accumulated}`);
if (wlfi.unclaimed !== 1.25) throw new Error(`wrong unclaimed ${wlfi.unclaimed}`);
if (wlfi.perToken['0xusd1'] !== 1.75 || wlfi.perToken['0xusdc'] !== 2) throw new Error(JSON.stringify(wlfi.perToken));
if (wlfi.assignedPerToken['0xusdc'] !== 2 || wlfi.perAccountToken['0']['0xusdc'] !== 2) throw new Error('account 0 attribution missing');
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_ethereum_canonical_workflow_rebuilds_verified_ledger_on_fresh_history(self):
        workflow = ETHEREUM_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '12,42 * * * *'", workflow)
        self.assertIn("timeout-minutes: 90", workflow)
        self.assertIn("default: '120'", workflow)
        self.assertIn("default: '1200'", workflow)
        self.assertIn("HOT_LIMIT: ${{ github.event.inputs.hot_limit || '120' }}", workflow)
        self.assertIn("CHECKPOINT_STEPS: ${{ github.event.inputs.checkpoint_steps || '1200' }}", workflow)
        for env_name in (
            "ALCHEMY_ETHEREUM_RPC_KAT",
            "ALCHEMY_ETHEREUM_RPC_DAN",
            "ALCHEMY_ETHEREUM_RPC_ZEN",
        ):
            self.assertIn(env_name, workflow)
        self.assertNotIn("--existing-history-only", workflow)
        self.assertIn("--prefer-stale-history", workflow)
        self.assertIn("Select missing and oldest Ethereum canonical wallets", workflow)
        self.assertIn("MAX_RESUME_TARGET_LAG_BLOCKS: '600'", workflow)
        self.assertIn("CHECKPOINT_SLEEP_SECONDS: '2'", workflow)
        self.assertIn("MAX_DELTA_SCAN_BLOCKS_PER_TASK: '1000'", workflow)
        self.assertIn("EARN_RPC_GETLOGS_TIMEOUT_SECONDS: '30'", workflow)
        self.assertIn("--max-incremental-scan-workers 12", workflow)
        self.assertIn("--max-incremental-apply-workers 12", workflow)
        self.assertIn('--max-resume-target-lag-blocks "$MAX_RESUME_TARGET_LAG_BLOCKS"', workflow)
        self.assertIn('--max-delta-scan-blocks-per-task "$MAX_DELTA_SCAN_BLOCKS_PER_TASK"', workflow)
        self.assertIn("Build Ethereum verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)
        self.assertIn("git add -f data/earn-subaccount-history/manifest.json data/earn-subaccount-history/ethereum", workflow)
        self.assertIn("git add -f data/earn-verified-ledger/manifest.json data/earn-verified-ledger/ethereum", workflow)

    def test_snapshot_workflow_skips_archived_verified_ledger_caches(self):
        workflow = EARN_SNAPSHOTS_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("Build active-chain verified ledger caches", workflow)
        self.assertIn("Publish active-chain verified ledger shards", workflow)
        for chain in ("ethereum", "arbitrum", "berachain", "mantle", "xlayer"):
            self.assertIn(f"--chain {chain}", workflow)
        for chain in ("botanix", "polygonzkevm"):
            self.assertNotIn(chain, workflow)

    def test_coverage_report_can_resolve_live_target_block(self):
        source = EARN_COVERAGE_REPORT.read_text(encoding="utf-8")
        self.assertIn("from scan_earn_netflow import CHAINS, get_block_number", source)
        self.assertIn("def _resolve_live_chain_block", source)
        self.assertIn("live_target = _resolve_live_chain_block(chain)", source)
        self.assertLess(
            source.find("live_target = _resolve_live_chain_block(chain)"),
            source.find("progress_target = _active_scan_progress_target(events_dir, chain)"),
        )

    def test_arbitrum_canonical_workflow_targets_one_hour_freshness(self):
        workflow = ARBITRUM_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '18,48 * * * *'", workflow)
        self.assertIn("timeout-minutes: 75", workflow)
        self.assertIn("default: '0'", workflow)
        self.assertIn("STEADY_HOT_LIMIT: ${{ github.event.inputs.hot_limit || '0' }}", workflow)
        self.assertIn("CHECKPOINT_STEPS: ${{ github.event.inputs.checkpoint_steps || '24' }}", workflow)
        self.assertIn("COMMAND_TIMEOUT_SECONDS: '180'", workflow)
        self.assertIn("secrets.ALCHEMY_ARBITRUM_RPC_ZEN", workflow)
        self.assertIn("secrets.ALCHEMY_ARBITRUM_RPC_KAT", workflow)
        self.assertIn("secrets.ALCHEMY_ARBITRUM_RPC_DAN", workflow)
        self.assertIn("--existing-history-only", workflow)
        self.assertIn("--prefer-stale-history", workflow)
        self.assertIn("MAX_RESUME_TARGET_LAG_BLOCKS: '28800'", workflow)
        self.assertIn('--max-steps "$CHECKPOINT_STEPS"', workflow)
        self.assertIn('--command-timeout-seconds "$COMMAND_TIMEOUT_SECONDS"', workflow)
        self.assertIn('--max-resume-target-lag-blocks "$MAX_RESUME_TARGET_LAG_BLOCKS"', workflow)
        self.assertIn("Build Arbitrum verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)

    def test_berachain_canonical_workflow_runs_in_checkpointed_chunks(self):
        workflow = BERACHAIN_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '7,37 * * * *'", workflow)
        self.assertIn("timeout-minutes: 90", workflow)
        self.assertIn("default: '0'", workflow)
        self.assertIn("BOOTSTRAP_HOT_LIMIT: '180'", workflow)
        self.assertIn("STEADY_HOT_LIMIT: ${{ github.event.inputs.hot_limit || '0' }}", workflow)
        self.assertIn("default: '150'", workflow)
        self.assertIn("CHECKPOINT_STEPS: ${{ github.event.inputs.checkpoint_steps || '150' }}", workflow)
        self.assertIn("CHECKPOINT_SLEEP_SECONDS: '20'", workflow)
        self.assertIn("COMMAND_TIMEOUT_SECONDS: '180'", workflow)
        self.assertIn("has_public_baseline", workflow)
        self.assertIn("--existing-history-only", workflow)
        self.assertIn("--prefer-stale-history", workflow)
        self.assertIn("MAX_RESUME_TARGET_LAG_BLOCKS: '3600'", workflow)
        self.assertIn("MAX_DELTA_SCAN_BLOCKS_PER_TASK: '1000'", workflow)
        self.assertIn("config/earn_berachain_canonical_hot_addresses.txt", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN: ${{ secrets.DRPC_BERACHAIN_RPC_ZEN }}", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2 DRPC_BERACHAIN_RPC_ZEN ALCHEMY_BERACHAIN_RPC_2", workflow)
        self.assertIn("Check Berachain RPC redundancy", workflow)
        self.assertIn("--allow-checkpoint-incomplete", workflow)
        self.assertIn('--command-timeout-seconds "$COMMAND_TIMEOUT_SECONDS"', workflow)
        self.assertIn('--max-resume-target-lag-blocks "$MAX_RESUME_TARGET_LAG_BLOCKS"', workflow)
        self.assertIn('--max-delta-scan-blocks-per-task "$MAX_DELTA_SCAN_BLOCKS_PER_TASK"', workflow)
        self.assertIn("Build Berachain verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)

    def test_berachain_canonical_priority_file_pins_valid_hot_wallets(self):
        addresses = [
            raw.strip()
            for raw in BERACHAIN_PRIORITY_ADDRESSES.read_text(encoding="utf-8").splitlines()
            if raw.strip() and not raw.strip().startswith("#")
        ]
        self.assertGreaterEqual(len(addresses), 20)
        self.assertEqual(len(addresses), len(set(addresses)))
        for address in addresses:
            self.assertTrue(address.startswith("0x"), address)
            self.assertEqual(42, len(address), address)
        self.assertIn("0x66322a0f0ef69afb3f9d41b4f6ea657592578330", addresses)
        self.assertIn("0xdac2c5d760ff866bc796ddb88dffec3d9a90b7e5", addresses)

    def test_global_earn_priority_file_pins_user_reported_golden_wallets(self):
        addresses = [
            raw.strip()
            for raw in GLOBAL_PRIORITY_ADDRESSES.read_text(encoding="utf-8").splitlines()
            if raw.strip() and not raw.strip().startswith("#")
        ]
        self.assertEqual(len(addresses), len(set(addresses)))
        for address in addresses:
            self.assertTrue(address.startswith("0x"), address)
            self.assertEqual(42, len(address), address)
        self.assertIn("0xffe4e3986d18333402564ea64f3a83fcc1907b52", addresses)
        self.assertIn("0x615b12d8de9d8c649de8b5813e23ba11b3f15aff", addresses)
        self.assertIn("0xda33e6230ecb4872d7f073cfc704b8279e18fab3", addresses)

    def test_secondary_canonical_workflow_targets_secondary_chains(self):
        workflow = SECONDARY_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '23,53 * * * *'", workflow)
        # XLayer runs twice hourly like primary chains — a single hourly slot
        # left it stale for days after one failed completion (2026-06 audit).
        self.assertIn("cron: '8,38 * * * *'", workflow)
        self.assertIn("type: choice", workflow)
        self.assertIn("          - mantle", workflow)
        self.assertIn("          - xlayer", workflow)
        self.assertIn("timeout-minutes: 75", workflow)
        self.assertIn("run-name: Refresh Secondary Canonical EARN History", workflow)
        self.assertIn("'23,53 * * * *' && 'mantle'", workflow)
        self.assertIn("'8,38 * * * *' && 'xlayer'", workflow)
        self.assertIn("Plan selected secondary chains", workflow)
        self.assertIn("needs: plan-secondary-canonical", workflow)
        self.assertIn("matrix: ${{ fromJson(needs.plan-secondary-canonical.outputs.matrix) }}", workflow)
        self.assertIn("group: earn-secondary-canonical-history-${{ matrix.chain }}", workflow)
        self.assertIn('"xlayer": {', workflow)
        self.assertIn('"bootstrap_hot_limit": 160', workflow)
        self.assertIn('"bootstrap_hot_limit": 300', workflow)
        self.assertIn('"steady_hot_limit": nonnegative_int(hot_limit_override, 0)', workflow)
        self.assertIn('"checkpoint_steps": positive_int(checkpoint_steps_override, 30)', workflow)
        self.assertIn('"max_resume_target_lag_blocks": 3600', workflow)
        self.assertIn('"max_resume_target_lag_blocks": 7200', workflow)
        self.assertIn("CHECKPOINT_STEPS: ${{ matrix.checkpoint_steps }}", workflow)
        self.assertIn("MAX_RESUME_TARGET_LAG_BLOCKS: ${{ matrix.max_resume_target_lag_blocks }}", workflow)
        self.assertIn("MAX_DELTA_SCAN_BLOCKS_PER_TASK: '10000'", workflow)
        self.assertIn("MAX_INCREMENTAL_SCAN_WORKER_RUNTIME_SECONDS: '300'", workflow)
        self.assertIn("COMMAND_TIMEOUT_SECONDS: '180'", workflow)
        self.assertIn("--existing-history-only", workflow)
        self.assertIn("--prefer-stale-history", workflow)
        self.assertIn("XLAYER_RPC_QUICKNODE_TWOJE: ${{ secrets.XLAYER_RPC_QUICKNODE_TWOJE }}", workflow)
        self.assertIn("ALCHEMY_XLAYER_RPC_ZEN: ${{ secrets.ALCHEMY_XLAYER_RPC_ZEN }}", workflow)
        self.assertIn("DRP_XLAYER_RPC_TWO: ${{ secrets.DRP_XLAYER_RPC_TWO }}", workflow)
        self.assertIn("Check secondary RPC redundancy", workflow)
        self.assertIn("QUICKNODE_MANTLE_RPC_2: ${{ secrets.QUICKNODE_MANTLE_RPC_2 }}", workflow)
        self.assertIn("DRPC_MANTLE_RPC: ${{ secrets.DRPC_MANTLE_RPC }}", workflow)
        self.assertNotIn("botanix", workflow)
        self.assertNotIn("polygonzkevm", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)
        self.assertIn('--max-steps "$CHECKPOINT_STEPS"', workflow)
        self.assertIn('--command-timeout-seconds "$COMMAND_TIMEOUT_SECONDS"', workflow)
        self.assertIn('--max-resume-target-lag-blocks "$MAX_RESUME_TARGET_LAG_BLOCKS"', workflow)
        self.assertIn('--max-delta-scan-blocks-per-task "$MAX_DELTA_SCAN_BLOCKS_PER_TASK"', workflow)
        self.assertIn('--max-incremental-scan-worker-runtime-seconds "$MAX_INCREMENTAL_SCAN_WORKER_RUNTIME_SECONDS"', workflow)
        self.assertNotIn("Resolve selected chain", workflow)
        self.assertNotIn("Skipping $CHAIN for this trigger.", workflow)
        self.assertNotIn("max-parallel: 1", workflow)

    def test_hot_wallet_selector_prioritizes_scored_wallets_before_missing_backlog(self):
        import select_earn_canonical_hot_addresses as selector

        chain = "arbitrum"
        cold_a = "0x" + "0" * 39 + "1"
        cold_b = "0x" + "0" * 39 + "2"
        active = "0x" + "f" * 40

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot_dir = root / "snapshots"
            netflow_dir = root / "netflow"
            history_dir = root / "history"
            snapshot_dir.mkdir()
            netflow_dir.mkdir()
            history_dir.mkdir()
            (snapshot_dir / "manifest.json").write_text(
                json.dumps({"dates": ["2026-05-11"], "chains": {"2026-05-11": [chain]}}),
                encoding="utf-8",
            )
            (snapshot_dir / "2026-05-11.json").write_text(
                json.dumps({
                    "snapshots": {
                        chain: {
                            active: {"markets": {"0": {"par": "123456789"}}},
                        },
                    },
                }),
                encoding="utf-8",
            )
            (netflow_dir / f"{chain}.json").write_text(json.dumps({"netflows": {}}), encoding="utf-8")
            (history_dir / "manifest.json").write_text(
                json.dumps({"chains": {chain: {"lastBlock": 123}}}),
                encoding="utf-8",
            )

            with mock.patch.object(selector, "SNAPSHOT_DIR", snapshot_dir):
                with mock.patch.object(selector, "NETFLOW_DIR", netflow_dir):
                    with mock.patch.object(selector, "_load_known_addresses", return_value=[cold_a, cold_b, active]):
                        selected, metadata = selector.build_selection(
                            chain,
                            limit=2,
                            priority_files=[],
                            include_priority_even_if_unknown=False,
                            history_dir=history_dir,
                            prefer_stale_history=True,
                        )

        self.assertEqual(active, selected[0])
        self.assertEqual(2, len(selected))
        self.assertEqual(3, metadata["missingHistoryAddressCount"])

    def test_hot_wallet_selector_prioritizes_active_wallet_before_oldest_cold_watermark(self):
        import select_earn_canonical_hot_addresses as selector

        chain = "berachain"
        cold_stale = "0x" + "0" * 39 + "1"
        active_stale = "0x" + "f" * 40

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot_dir = root / "snapshots"
            netflow_dir = root / "netflow"
            history_dir = root / "history"
            chain_history_dir = history_dir / chain
            snapshot_dir.mkdir()
            netflow_dir.mkdir()
            chain_history_dir.mkdir(parents=True)
            (snapshot_dir / "manifest.json").write_text(
                json.dumps({"dates": ["2026-05-11"], "chains": {"2026-05-11": [chain]}}),
                encoding="utf-8",
            )
            (snapshot_dir / "2026-05-11.json").write_text(
                json.dumps({
                    "snapshots": {
                        chain: {
                            active_stale: {"markets": {"0": {"par": "987654321"}}},
                        },
                    },
                }),
                encoding="utf-8",
            )
            (netflow_dir / f"{chain}.json").write_text(json.dumps({"netflows": {}}), encoding="utf-8")
            (history_dir / "manifest.json").write_text(
                json.dumps({"chains": {chain: {"lastBlock": 1_000}}}),
                encoding="utf-8",
            )
            (chain_history_dir / f"{cold_stale}.json").write_text(
                json.dumps({"lastScannedBlock": 1}),
                encoding="utf-8",
            )
            (chain_history_dir / f"{active_stale}.json").write_text(
                json.dumps({"lastScannedBlock": 999}),
                encoding="utf-8",
            )

            with mock.patch.object(selector, "SNAPSHOT_DIR", snapshot_dir):
                with mock.patch.object(selector, "NETFLOW_DIR", netflow_dir):
                    with mock.patch.object(selector, "_load_known_addresses", return_value=[cold_stale, active_stale]):
                        selected, metadata = selector.build_selection(
                            chain,
                            limit=1,
                            priority_files=[],
                            include_priority_even_if_unknown=False,
                            history_dir=history_dir,
                            coverage_backfill=True,
                        )

        self.assertEqual([active_stale], selected)
        self.assertEqual(2, metadata["staleHistoryAddressCount"])
        self.assertEqual(1, metadata["activeStaleHistoryAddressCount"])
        self.assertEqual("active-first-then-cold-watermark", metadata["selectionPolicy"])

    def test_berachain_netflow_workflow_runs_frequent_chain_only_scan(self):
        workflow = BERACHAIN_NETFLOW_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '27,57 * * * *'", workflow)
        self.assertIn("group: earn-berachain-netflow-data", workflow)
        self.assertIn("timeout-minutes: 75", workflow)
        self.assertIn("Check Berachain RPC redundancy", workflow)
        self.assertIn("scan_earn_netflow.py berachain --max-runtime-seconds 3300", workflow)
        self.assertIn("data/earn-netflow/berachain.json", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN: ${{ secrets.DRPC_BERACHAIN_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_BERACHAIN_RPC_3: ${{ secrets.ALCHEMY_BERACHAIN_RPC_3 }}", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)

    def test_berachain_borrow_route_workflow_checks_rpc_redundancy(self):
        workflow = BERACHAIN_BORROW_ROUTE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '17 */6 * * *'", workflow)
        self.assertIn("group: earn-berachain-canonical-history", workflow)
        self.assertIn("timeout-minutes: 75", workflow)
        self.assertIn("BORROW_ROUTE_HOT_LIMIT: '250'", workflow)
        self.assertIn("BORROW_ROUTE_CHECKPOINT_STEPS: '90'", workflow)
        self.assertIn("BORROW_ROUTE_CHECKPOINT_SLEEP_SECONDS: '20'", workflow)
        self.assertIn("Select Berachain borrow-route and hot wallets", workflow)
        self.assertIn("select_earn_borrow_route_history_addresses.py", workflow)
        self.assertIn("select_earn_canonical_hot_addresses.py", workflow)
        self.assertIn("/tmp/earn-${CHAIN}-borrow-route-refresh-addresses.txt", workflow)
        self.assertIn("--selection-address-file \"/tmp/earn-${CHAIN}-borrow-route-refresh-addresses.txt\"", workflow)
        self.assertNotIn("--selection-address-file \"/tmp/earn-${CHAIN}-all-known-addresses.txt\"", workflow)
        self.assertIn("--max-steps \"$BORROW_ROUTE_CHECKPOINT_STEPS\"", workflow)
        self.assertIn("Check Berachain RPC redundancy", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN: ${{ secrets.DRPC_BERACHAIN_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_BERACHAIN_RPC_3: ${{ secrets.ALCHEMY_BERACHAIN_RPC_3 }}", workflow)
        self.assertIn("git add -f data/earn-subaccount-history/manifest.json", workflow)
        self.assertIn("git add -f data/earn-verified-ledger/manifest.json", workflow)
        self.assertIn("git stash push --keep-index --include-untracked --message \"berachain-borrow-route-runtime\"", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)

    def test_berachain_watchdog_refreshes_after_thirty_minutes(self):
        source = EARN_FRESHNESS_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('"refreshAfterMinutes": 30', source)
        self.assertIn('policy.get("refreshAfterMinutes", REFRESH_AFTER_MINUTES)', source)
        self.assertIn('"netflowWorkflow": "update-earn-berachain-netflow.yml"', source)

    def test_earn_canonical_lookup_verified_window_matches_two_hours_by_chain(self):
        self.assertIn("ethereum: 600n, // ~2h at 12s blocks", self.source)
        self.assertIn("berachain: 3600n, // ~2h at 2s blocks", self.source)
        self.assertIn("arbitrum: 28800n, // ~2h at ~0.25s blocks", self.source)
        self.assertIn("botanix: 1200n, // ~2h at 6s blocks", self.source)
        self.assertIn("mantle: 3600n, // ~2h at 2s blocks", self.source)
        self.assertIn("polygonzkevm: 2250n, // ~2h at ~3.2s blocks", self.source)
        self.assertIn("xlayer: 7200n, // ~2h at 1s blocks", self.source)
        self.assertNotIn("ethereum: 900n", self.source)
        self.assertNotIn("polygonzkevm: 5400n", self.source)
        self.assertNotIn("xlayer: 3600n", self.source)

    def test_earn_freshness_watchdog_runs_and_can_dispatch_refreshes(self):
        workflow = EARN_FRESHNESS_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '*/15 * * * *'", workflow)
        self.assertIn("actions: write", workflow)
        self.assertIn("update_earn_freshness_status.py", workflow)
        self.assertIn("data/earn-freshness/status.json", workflow)
        # Budget must cover all simultaneously-stale chains (arbitrum, mantle,
        # xlayer + ethereum coverage catchup) with headroom — see 2026-06 audit.
        self.assertIn("EARN_WATCHDOG_MAX_DISPATCHES: '6'", workflow)
        self.assertIn("scripts/plan_earn_watchdog_dispatch.py", workflow)
        self.assertIn("dispatched_count=0", workflow)
        self.assertIn("max_dispatches=\"${EARN_WATCHDOG_MAX_DISPATCHES:-4}\"", workflow)
        self.assertIn('if [ "${chain:-}" = "__all__" ]; then', workflow)
        self.assertIn("Deferring $job_key", workflow)
        self.assertIn("displayTitle,name,status", workflow)
        self.assertIn("covers_requested_chain", workflow)
        self.assertIn('chain_label_pattern = re.compile(r"\\[([^\\]]+)\\]")', workflow)
        self.assertIn('return chain in labels or "all" in labels', workflow)
        self.assertNotIn('return "[" not in title', workflow)
        self.assertIn("gh workflow run", workflow)
        self.assertIn("earn-refresh-jobs.tsv", workflow)
        self.assertIn("earn-refresh-dispatched.tsv", workflow)
        self.assertIn("Skipping duplicate refresh request", workflow)
        self.assertIn('-f "chain=$chain"', workflow)
        self.assertIn("scripts/check_earn_sla.py", workflow)
        self.assertIn("$GITHUB_STEP_SUMMARY", workflow)
        planner = EARN_WATCHDOG_DISPATCH_PLANNER.read_text(encoding="utf-8")
        self.assertIn("refreshJobs", planner)
        self.assertIn("priority", planner)
        self.assertIn("mode", planner)
        self.assertIn('ALL_CHAINS_SENTINEL = "__all__"', planner)
        self.assertIn("def build_dispatch_rows", planner)
        self.assertIn("def write_dispatch_tsv", planner)

    def test_freshness_routes_secondary_refreshes_with_chain_inputs(self):
        source = EARN_FRESHNESS_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('"canonicalWorkflowInputs": {"chain": "mantle"}', source)
        self.assertIn('"canonicalWorkflowInputs": {"chain": "xlayer"}', source)
        self.assertIn('ARCHIVED_CHAINS = {"botanix", "polygonzkevm"}', source)
        self.assertNotIn('"canonicalWorkflowInputs": {"chain": "botanix"}', source)
        self.assertNotIn('"canonicalWorkflowInputs": {"chain": "polygonzkevm"}', source)
        self.assertIn('"xlayer": {', source)
        self.assertIn('"verifiedBlockLag": 7200', source)
        self.assertIn('"backgroundRefreshRecommended"', source)
        self.assertIn('"chainReport"', source)
        self.assertIn('"refreshJobs"', source)
        self.assertIn("def _refresh_job_priority", source)
        self.assertIn('"priority": int(priority)', source)
        self.assertIn('"mode": normalized_mode', source)
        self.assertIn('job_key = f"{workflow}:chain={normalized_inputs[\'chain\']}"', source)

    def test_canonical_refresh_runner_keeps_json_stdout_clean(self):
        runner = CANONICAL_REFRESH_RUNNER.read_text(encoding="utf-8")
        self.assertIn("stderr=subprocess.PIPE", runner)
        self.assertNotIn("stderr=subprocess.STDOUT", runner)
        scanner = NETFLOW_SCANNER.read_text(encoding="utf-8")
        self.assertIn("file=sys.stderr", scanner)

    def test_canonical_refresh_runner_times_out_stuck_continue_commands_as_checkpoints(self):
        runner = CANONICAL_REFRESH_RUNNER.read_text(encoding="utf-8")
        self.assertIn("class CommandTimedOut", runner)
        self.assertIn("start_new_session=True", runner)
        self.assertIn("proc.communicate(timeout=timeout_seconds", runner)
        self.assertIn("commandTimedOut", runner)
        self.assertIn("--command-timeout-seconds", runner)
        self.assertIn("allow_checkpoint_incomplete", runner)

    def test_netflow_workflow_runs_chain_aware_non_berachain_scans(self):
        workflow = NETFLOW_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '17 * * * *'", workflow)
        self.assertIn("run-name: Update Earn Netflow Data", workflow)
        self.assertIn("name: Update netflow [${{ matrix.chain }}]", workflow)
        self.assertIn("timeout-minutes: 200", workflow)
        self.assertIn("group: earn-netflow-${{ matrix.chain }}", workflow)
        self.assertIn("Plan requested chains", workflow)
        self.assertIn("matrix: ${{ fromJson(needs.plan-netflow.outputs.matrix) }}", workflow)
        self.assertIn("--partial-output-interval-seconds \"$PARTIAL_OUTPUT_INTERVAL_SECONDS\"", workflow)
        self.assertIn('"maxRuntimeSeconds": chains[chain]["maxRuntimeSeconds"]', workflow)
        self.assertIn('"partialOutputIntervalSeconds": chains[chain]["partialOutputIntervalSeconds"]', workflow)
        self.assertIn('"arbitrum": {"maxRuntimeSeconds": 3300, "partialOutputIntervalSeconds": 0}', workflow)
        self.assertIn('"mantle": {"maxRuntimeSeconds": 7200, "partialOutputIntervalSeconds": 1800}', workflow)
        self.assertIn('"xlayer": {"maxRuntimeSeconds": 3300, "partialOutputIntervalSeconds": 600}', workflow)
        self.assertIn("path: data/.netflow-progress/${{ matrix.chain }}.json", workflow)
        self.assertIn('git add "data/earn-netflow/${CHAIN}.json"', workflow)
        self.assertIn("X Layer netflow did not make progress past block 0", workflow)
        for chain in ("arbitrum", "ethereum", "mantle", "xlayer"):
            self.assertIn(f'"{chain}"', workflow)
        self.assertNotIn("botanix", workflow)
        self.assertNotIn("polygonzkevm", workflow)
        self.assertNotIn("scan_earn_netflow.py arbitrum,ethereum,mantle,botanix,polygonzkevm --max-runtime-seconds 19800", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_ZEN: ${{ secrets.ALCHEMY_ARBITRUM_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_KAT: ${{ secrets.ALCHEMY_ARBITRUM_RPC_KAT }}", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_DAN: ${{ secrets.ALCHEMY_ARBITRUM_RPC_DAN }}", workflow)
        self.assertIn("ALCHEMY_ETHEREUM_RPC_ZEN: ${{ secrets.ALCHEMY_ETHEREUM_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_ETHEREUM_RPC_KAT: ${{ secrets.ALCHEMY_ETHEREUM_RPC_KAT }}", workflow)
        self.assertIn("ALCHEMY_ETHEREUM_RPC_DAN: ${{ secrets.ALCHEMY_ETHEREUM_RPC_DAN }}", workflow)
        self.assertIn("XLAYER_RPC_QUICKNODE_TWOJE: ${{ secrets.XLAYER_RPC_QUICKNODE_TWOJE }}", workflow)
        self.assertIn("ALCHEMY_XLAYER_RPC_ZEN: ${{ secrets.ALCHEMY_XLAYER_RPC_ZEN }}", workflow)
        self.assertIn("DRP_XLAYER_RPC_TWO: ${{ secrets.DRP_XLAYER_RPC_TWO }}", workflow)
        self.assertIn("Check secondary RPC redundancy", workflow)
        self.assertIn("QUICKNODE_MANTLE_RPC_2: ${{ secrets.QUICKNODE_MANTLE_RPC_2 }}", workflow)
        self.assertIn("DRPC_MANTLE_RPC: ${{ secrets.DRPC_MANTLE_RPC }}", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)
        self.assertNotIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)

    def test_earn_freshness_workflow_exposes_all_dedicated_arbitrum_and_ethereum_rpc_secrets(self):
        workflow = EARN_FRESHNESS_WORKFLOW.read_text(encoding="utf-8")

        for env_name in (
            "ALCHEMY_ARBITRUM_RPC_KAT",
            "ALCHEMY_ARBITRUM_RPC_DAN",
            "ALCHEMY_ARBITRUM_RPC_ZEN",
            "ALCHEMY_ETHEREUM_RPC_KAT",
            "ALCHEMY_ETHEREUM_RPC_DAN",
            "ALCHEMY_ETHEREUM_RPC_ZEN",
        ):
            self.assertIn(f"{env_name}: ${{{{ secrets.{env_name} }}}}", workflow)
        scanner = NETFLOW_SCANNER.read_text(encoding="utf-8")
        self.assertIn('os.environ.get("MANTLE_RPC")', scanner)
        self.assertIn('os.environ.get("QUICKNODE_MANTLE_RPC")', scanner)
        self.assertIn('os.environ.get("QUICKNODE_MANTLE_RPC_2")', scanner)
        self.assertIn('os.environ.get("MANTLE_RPC_QUICKNODE_TWOJE")', scanner)
        self.assertIn('os.environ.get("DRPC_MANTLE_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("DRPC_MANTLE_RPC")', scanner)
        self.assertIn('os.environ.get("ALCHEMY_MANTLE_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("QUICKNODE_BOTANIX_RPC")', scanner)
        self.assertIn('os.environ.get("DRPC_BOTANIX_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("ALCHEMY_BOTANIX_RPC_ZEN")', scanner)
        self.assertIn('"https://mantle.api.onfinality.io/public"', scanner)
        self.assertIn('"max_block_chunk": 1_800', scanner)
        self.assertIn('os.environ.get("QUICKNODE_BERACHAIN_RPC_2")', scanner)
        self.assertIn('os.environ.get("DRPC_BERACHAIN_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("ALCHEMY_POLYGONZKEVM_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("DRP_POLYGONZKEVM_RPC_TWO")', scanner)
        self.assertIn('os.environ.get("XLAYER_RPC_QUICKNODE_TWOJE")', scanner)
        self.assertIn('"https://mainnet.xlayer-rpc.com"', scanner)
        self.assertIn('"max_block_chunk": 9_999', scanner)
        self.assertIn('"start_block": 859_455', scanner)
        self.assertIn("max_chunk_size = int(chain_config.get(\"max_block_chunk\") or BLOCK_CHUNK)", scanner)
        self.assertIn("chunk_size = min(max_chunk_size, chunk_size * 2)", scanner)
        self.assertIn("soft_runtime_no_progress", scanner)
        self.assertIn("consecutive_failures_at_block", scanner)
        self.assertIn('os.environ.get("ALCHEMY_XLAYER_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("DRP_XLAYER_RPC_TWO")', scanner)
        self.assertIn("raise SystemExit(main())", scanner)

    def test_earn_snapshot_workflow_refreshes_freshness_status(self):
        workflow = EARN_SNAPSHOTS_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)

    def test_subaccount_scanners_respect_chain_specific_rpc_block_chunks(self):
        event_scanner = SUBACCOUNT_EVENT_SCANNER.read_text(encoding="utf-8")
        history_builder = SUBACCOUNT_HISTORY_BUILDER.read_text(encoding="utf-8")
        self.assertIn('max_chunk_size = int(config.get("max_block_chunk") or BLOCK_CHUNK)', event_scanner)
        self.assertIn("adaptive_chunk_size = min(max_chunk_size, max(1, int(chunk_size)), max(1, range_span))", event_scanner)
        self.assertIn("adaptive_chunk_size = min(max_chunk_size, adaptive_chunk_size * 2)", event_scanner)
        self.assertIn('CHAINS[args.chain].get("max_block_chunk")', event_scanner)
        self.assertIn('config.get("canonical_max_block_chunk")', history_builder)
        self.assertIn("--checkpoint-file", history_builder)

    def test_lending_toolbar_filters_always_open_downward(self):
        source = LIQUIDATION_PREVIEW.read_text(encoding="utf-8")
        start = source.index("function positionLendingToolbarMenu")
        end = source.index("function clearChainFilterOnly", start)
        helper = source[start:end]
        self.assertIn("const top = rect.bottom + 6", helper)
        self.assertIn("max-height", helper)
        self.assertIn("overflow-y", helper)
        self.assertNotIn("aboveTop", helper)
        self.assertNotIn("rect.top -", helper)
        start = source.index("function positionPopoverFixed")
        end = source.index("function initColFilterPopover", start)
        popover_helper = source[start:end]
        self.assertIn("const top = rect.bottom + 6", popover_helper)
        self.assertIn("availableBelow", popover_helper)
        self.assertNotIn("preferAbove", popover_helper)

    def test_liquidation_tables_keep_address_and_expand_contracts(self):
        source = LIQUIDATION_PREVIEW.read_text(encoding="utf-8")
        self.assertIn("body.route-liquidation #positions-table colgroup col:nth-child(2) { width: 19% !important; }", source)
        self.assertIn("body.route-liquidation #liquidation-history-table colgroup col:nth-child(2) { width: 19% !important; }", source)
        self.assertIn("body.route-liquidation #liquidation-history-table colgroup col:nth-child(3) { width: 39% !important; }", source)
        self.assertIn("body.route-liquidation #liquidation-history-table colgroup col:nth-child(4) { width: 0% !important; }", source)
        self.assertIn("body.route-liquidation #liquidation-history-table colgroup col:nth-child(5) { width: 18.2% !important; }", source)
        self.assertIn("body.route-liquidation #liquidation-history-table colgroup col:nth-child(6) { width: 16% !important; }", source)
        start = source.index('<table class="liquidation-history-table" id="liquidation-history-table"')
        end = source.index('<tbody id="liquidation-history-body"', start)
        history_head = source[start:end]
        self.assertIn("<col><col><col><col><col><col>", history_head)
        self.assertLess(history_head.index("<th>Chain</th>"), history_head.index("<th>Liquidated wallet</th>"))
        self.assertLess(history_head.index("<th>Liquidated wallet</th>"), history_head.index("<th>Date</th>"))
        self.assertLess(history_head.index("<th>Date</th>"), history_head.index('class="col-spacer"'))
        self.assertIn("body.route-liquidation #liquidation-history-table thead th.col-spacer,\n        body.route-liquidation #liquidation-history-table tbody td.col-spacer {\n            width: 0 !important;", source)
        self.assertLess(history_head.index("<th>Liquidated wallet</th>"), history_head.index("Collateral seized"))
        self.assertLess(history_head.index("Collateral seized"), history_head.index("Debt repaid"))
        self.assertIn("body.route-liquidation #positions-table tbody tr,\n        body.route-liquidation #liquidation-history-table tbody tr {\n            height: 86px !important;", source)
        self.assertIn("max-width: 132px !important;", source)
        self.assertIn("const chainLabel = CHAIN_DISPLAY_NAMES[normalizeDisplayChainKey(chain)] || chain || '—';", source)
        self.assertIn("function setPositionRowExpanded(row, expanded)", source)
        self.assertIn("token-pill-extra-wrap", source)
        self.assertIn("renderTokenPills(p.collateralTokens, { limit: 3, expanded: isExpanded, collapsible: hasTokenOverflow })", source)
        start = source.index("document.getElementById('table-body')?.addEventListener('click'")
        end = source.index("// ─── Keyboard Shortcuts", start)
        toggle_block = source[start:end]
        self.assertIn("togglePositionRowExpanded(row);", toggle_block)
        self.assertNotIn("renderTable();", toggle_block)


if __name__ == "__main__":
    unittest.main()
