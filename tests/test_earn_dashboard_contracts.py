import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_CORE = ROOT / "dashboard-core.html"
ETHEREUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-ethereum-canonical-history.yml"
ARBITRUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-arbitrum-canonical-history.yml"
BERACHAIN_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-canonical-history.yml"
SECONDARY_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-secondary-canonical-history.yml"
BERACHAIN_NETFLOW_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-netflow.yml"
BERACHAIN_BORROW_ROUTE_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-borrow-route-history.yml"
EARN_FRESHNESS_WORKFLOW = ROOT / ".github" / "workflows" / "monitor-earn-freshness.yml"
EARN_FRESHNESS_SCRIPT = ROOT / "update_earn_freshness_status.py"
BERACHAIN_PRIORITY_ADDRESSES = ROOT / "config" / "earn_berachain_canonical_hot_addresses.txt"
EARN_COVERAGE_REPORT = ROOT / "report_earn_subaccount_history_coverage.py"
CANONICAL_REFRESH_RUNNER = ROOT / "run_earn_canonical_history_refresh.py"
NETFLOW_SCANNER = ROOT / "scan_earn_netflow.py"
NETFLOW_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-netflow.yml"
LIQUIDATION_PREVIEW = ROOT / "liquidation-preview.html"


class EarnDashboardContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = DASHBOARD_CORE.read_text(encoding="utf-8")

    def test_borrow_positions_prefer_replay_ledger_for_open_debt_cost(self):
        self.assertIn("function earn_getOpenDebtYieldForAccount", self.source)
        self.assertIn("const replayOpenDebtYieldWei = earn_getOpenDebtYieldForAccount", self.source)
        self.assertIn("source: replayDebtCostWei !== null ? 'replay-ledger' : 'index-estimate'", self.source)
        self.assertNotIn("const accruedTokens = actualTokens - absPar; // cost accrued", self.source)

    def test_non_strict_yield_quality_can_override_verified_balance_badge(self):
        self.assertIn("function earn_getYieldQualityPresentation", self.source)
        self.assertIn("if (fallbackStatus === 'verified')", self.source)
        self.assertIn("const yieldQualityPresentation = earn_getYieldQualityPresentation(yieldCalc)", self.source)
        self.assertIn("method === 'all-netflow-verified'", self.source)
        self.assertIn("label: 'Inferred Carry'", self.source)
        self.assertIn("label: 'Fallback'", self.source)
        self.assertIn("label: 'Inferred'", self.source)

    def test_public_netflow_matches_are_not_rendered_as_strict_verified(self):
        self.assertIn("rawLabel: 'Netflow Match'", self.source)
        self.assertIn("'This yield reconciles through public netflow plus snapshot history, but it is not strict replay verification.'", self.source)
        self.assertIn("? (canonicalHistoryCoverageIncomplete ? 'coverage_incomplete' : 'inferred')", self.source)

    def test_rewards_card_has_merkl_unavailable_state(self):
        self.assertIn("merklUnavailable: false", self.source)
        self.assertIn("WLFI source did not respond", self.source)
        self.assertIn("earn-summary-mini-pill is-warning", self.source)

    def test_ethereum_canonical_workflow_rebuilds_verified_ledger_on_fresh_history(self):
        workflow = ETHEREUM_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '12,42 * * * *'", workflow)
        self.assertIn("Build Ethereum verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)
        self.assertIn("git add -f data/earn-subaccount-history/manifest.json data/earn-subaccount-history/ethereum", workflow)
        self.assertIn("git add -f data/earn-verified-ledger/manifest.json data/earn-verified-ledger/ethereum", workflow)

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
        self.assertIn("secrets.ALCHEMY_ARBITRUM_RPC_ZEN", workflow)
        self.assertIn("Build Arbitrum verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)

    def test_berachain_canonical_workflow_runs_in_checkpointed_chunks(self):
        workflow = BERACHAIN_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '7,37 * * * *'", workflow)
        self.assertIn("timeout-minutes: 90", workflow)
        self.assertIn("BOOTSTRAP_HOT_LIMIT: '500'", workflow)
        self.assertIn("STEADY_HOT_LIMIT: '500'", workflow)
        self.assertIn("CHECKPOINT_STEPS: '150'", workflow)
        self.assertIn("CHECKPOINT_SLEEP_SECONDS: '20'", workflow)
        self.assertIn("has_public_baseline", workflow)
        self.assertIn("config/earn_berachain_canonical_hot_addresses.txt", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN: ${{ secrets.DRPC_BERACHAIN_RPC_ZEN }}", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2 DRPC_BERACHAIN_RPC_ZEN ALCHEMY_BERACHAIN_RPC_2", workflow)
        self.assertIn("Check Berachain RPC redundancy", workflow)
        self.assertIn("--allow-checkpoint-incomplete", workflow)
        self.assertIn("Build Berachain verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)

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

    def test_secondary_canonical_workflow_targets_mantle_and_botanix_only(self):
        workflow = SECONDARY_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '23,53 * * * *'", workflow)
        self.assertIn("cron: '28,58 * * * *'", workflow)
        self.assertIn("type: choice", workflow)
        self.assertIn("          - mantle", workflow)
        self.assertIn("          - botanix", workflow)
        self.assertIn("timeout-minutes: 120", workflow)
        self.assertIn("run-name: Refresh Secondary Canonical EARN History", workflow)
        self.assertIn("'23,53 * * * *' && 'mantle'", workflow)
        self.assertIn("'28,58 * * * *' && 'botanix'", workflow)
        self.assertIn("--max-steps 360", workflow)
        self.assertIn("Resolve selected chain", workflow)
        self.assertIn("Skipping $CHAIN for this trigger.", workflow)
        self.assertNotIn("chain: polygonzkevm", workflow)
        self.assertNotIn("chain: xlayer", workflow)

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

    def test_berachain_borrow_route_workflow_checks_rpc_redundancy(self):
        workflow = BERACHAIN_BORROW_ROUTE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("timeout-minutes: 150", workflow)
        self.assertIn("--max-steps 240", workflow)
        self.assertIn("Check Berachain RPC redundancy", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN: ${{ secrets.DRPC_BERACHAIN_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_BERACHAIN_RPC_3: ${{ secrets.ALCHEMY_BERACHAIN_RPC_3 }}", workflow)

    def test_berachain_watchdog_refreshes_after_one_hour(self):
        source = EARN_FRESHNESS_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('"refreshAfterMinutes": 60', source)
        self.assertIn('policy.get("refreshAfterMinutes", REFRESH_AFTER_MINUTES)', source)
        self.assertIn('"netflowWorkflow": "update-earn-berachain-netflow.yml"', source)

    def test_earn_canonical_lookup_verified_window_matches_three_hours_by_chain(self):
        self.assertIn("ethereum: 900n, // ~3h at 12s blocks", self.source)
        self.assertIn("berachain: 5400n, // ~3h at 2s blocks", self.source)
        self.assertIn("arbitrum: 43200n, // ~3h at ~0.25s blocks", self.source)
        self.assertIn("botanix: 1800n, // ~3h at 6s blocks", self.source)
        self.assertIn("mantle: 5400n, // ~3h at 2s blocks", self.source)
        self.assertIn("polygonzkevm: 3400n, // ~3h at ~3.2s blocks", self.source)
        self.assertIn("xlayer: 10800n, // ~3h at 1s blocks", self.source)
        self.assertNotIn("polygonzkevm: 5400n", self.source)
        self.assertNotIn("xlayer: 3600n", self.source)

    def test_earn_freshness_watchdog_runs_and_can_dispatch_refreshes(self):
        workflow = EARN_FRESHNESS_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '*/15 * * * *'", workflow)
        self.assertIn("actions: write", workflow)
        self.assertIn("update_earn_freshness_status.py", workflow)
        self.assertIn("data/earn-freshness/status.json", workflow)
        self.assertIn("refreshJobs", workflow)
        self.assertIn("displayTitle,name,status", workflow)
        self.assertIn("covers_requested_chain", workflow)
        self.assertIn('f"[{chain}]" in title or "[all]" in title', workflow)
        self.assertIn("gh workflow run", workflow)
        self.assertIn("earn-refresh-jobs.tsv", workflow)
        self.assertIn('-f "chain=$chain"', workflow)

    def test_freshness_routes_secondary_refreshes_with_chain_inputs(self):
        source = EARN_FRESHNESS_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('"canonicalWorkflowInputs": {"chain": "mantle"}', source)
        self.assertIn('"canonicalWorkflowInputs": {"chain": "botanix"}', source)
        self.assertIn('"supportMode": "snapshot-first"', source)
        self.assertIn('"backgroundRefreshRecommended"', source)
        self.assertIn('"chainReport"', source)
        self.assertIn('"refreshJobs"', source)
        self.assertIn('merged_inputs["chain"] = "all"', source)

    def test_canonical_refresh_runner_keeps_json_stdout_clean(self):
        runner = CANONICAL_REFRESH_RUNNER.read_text(encoding="utf-8")
        self.assertIn("stderr=subprocess.PIPE", runner)
        self.assertNotIn("stderr=subprocess.STDOUT", runner)
        scanner = NETFLOW_SCANNER.read_text(encoding="utf-8")
        self.assertIn("file=sys.stderr", scanner)

    def test_netflow_workflow_runs_chain_aware_non_berachain_scans(self):
        workflow = NETFLOW_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '17 * * * *'", workflow)
        self.assertIn("run-name: Update Earn Netflow Data", workflow)
        self.assertIn("name: Update netflow [${{ matrix.chain }}]", workflow)
        self.assertIn("timeout-minutes: 200", workflow)
        self.assertIn("group: earn-netflow-${{ matrix.chain }}", workflow)
        self.assertIn("run: python3 scan_earn_netflow.py \"$CHAIN\" --max-runtime-seconds \"$MAX_RUNTIME_SECONDS\"", workflow)
        self.assertIn("maxRuntimeSeconds: 3300", workflow)
        self.assertIn("maxRuntimeSeconds: 10800", workflow)
        self.assertIn("path: data/.netflow-progress/${{ matrix.chain }}.json", workflow)
        self.assertIn('git add "data/earn-netflow/${CHAIN}.json"', workflow)
        for chain in ("arbitrum", "ethereum", "mantle", "botanix", "polygonzkevm"):
            self.assertIn(f"- chain: {chain}", workflow)
        self.assertNotIn("scan_earn_netflow.py arbitrum,ethereum,mantle,botanix,polygonzkevm --max-runtime-seconds 19800", workflow)
        self.assertNotIn("scan_earn_netflow.py arbitrum,ethereum,mantle,botanix,polygonzkevm,xlayer", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_ZEN: ${{ secrets.ALCHEMY_ARBITRUM_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_POLYGONZKEVM_RPC_ZEN: ${{ secrets.ALCHEMY_POLYGONZKEVM_RPC_ZEN }}", workflow)
        self.assertIn("DRP_POLYGONZKEVM_RPC_TWO: ${{ secrets.DRP_POLYGONZKEVM_RPC_TWO }}", workflow)
        self.assertNotIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
        self.assertNotIn("ALCHEMY_XLAYER_RPC_ZEN: ${{ secrets.ALCHEMY_XLAYER_RPC_ZEN }}", workflow)
        scanner = NETFLOW_SCANNER.read_text(encoding="utf-8")
        self.assertIn('os.environ.get("QUICKNODE_BERACHAIN_RPC_2")', scanner)
        self.assertIn('os.environ.get("DRPC_BERACHAIN_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("ALCHEMY_POLYGONZKEVM_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("DRP_POLYGONZKEVM_RPC_TWO")', scanner)
        self.assertIn('os.environ.get("ALCHEMY_XLAYER_RPC_ZEN")', scanner)
        self.assertIn('os.environ.get("DRP_XLAYER_RPC_TWO")', scanner)

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


if __name__ == "__main__":
    unittest.main()
