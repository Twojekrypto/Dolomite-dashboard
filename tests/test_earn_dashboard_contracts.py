import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_CORE = ROOT / "dashboard-core.html"
ASSETS_LIVE_BUILDER = ROOT / "scripts" / "build_assets_live.mjs"
ASSETS_LIVE_WORKFLOW = ROOT / ".github" / "workflows" / "update-assets-live.yml"
PAGES_WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"
ETHEREUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-ethereum-canonical-history.yml"
ARBITRUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-arbitrum-canonical-history.yml"
BERACHAIN_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-canonical-history.yml"
SECONDARY_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-secondary-canonical-history.yml"
BERACHAIN_NETFLOW_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-netflow.yml"
BERACHAIN_BORROW_ROUTE_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-berachain-borrow-route-history.yml"
EARN_FRESHNESS_WORKFLOW = ROOT / ".github" / "workflows" / "monitor-earn-freshness.yml"
EARN_WATCHDOG_DISPATCH_PLANNER = ROOT / "scripts" / "plan_earn_watchdog_dispatch.py"
EARN_SNAPSHOTS_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-snapshots.yml"
EARN_FRESHNESS_SCRIPT = ROOT / "update_earn_freshness_status.py"
EARN_COMMIT_HELPER = ROOT / "scripts" / "commit_with_fresh_earn_status.sh"
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

    def test_earn_shows_verified_background_refresh_status(self):
        self.assertIn("earn-data-freshness-pill", self.source)
        self.assertIn("EARN_FRESHNESS_STATUS_URL", self.source)
        self.assertIn("Verified · refreshing in background", self.source)
        self.assertIn("earn_loadFreshnessStatus().then(earn_updateFreshnessPill)", self.source)
        self.assertIn("chainStatus.canonical?.refreshMode", self.source)
        self.assertIn("chainStatus.netflow?.refreshMode", self.source)
        self.assertIn("freshnessModes.includes('background')", self.source)
        self.assertIn("earn_formatCanonicalCoverageLabel", self.source)
        self.assertIn("canonical coverage syncing", self.source)
        self.assertIn("Fresh chain data", self.source)
        self.assertIn("chainStatus?.canonical?.coverageCatchup === true", self.source)

    def test_assets_live_builder_keeps_cached_rate_rows_when_chain_api_is_empty(self):
        source = ASSETS_LIVE_BUILDER.read_text(encoding="utf-8")
        self.assertIn('import { readFile, writeFile } from "node:fs/promises";', source)
        self.assertIn("async function loadPreviousAssetsSnapshot()", source)
        self.assertIn("function cachedRateRowsForChain", source)
        self.assertIn("fetchRateRowsForChainWithFallback", source)
        self.assertIn("rateFallbackSourceGeneratedAt", source)
        self.assertIn("rateFallbackMaxAgeMinutes", source)
        self.assertIn("cached-rate-fallback", source)

    def test_assets_live_workflow_warns_on_stale_cached_rate_fallback(self):
        workflow = ASSETS_LIVE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("Check assets fallback freshness", workflow)
        self.assertIn("Assets rate fallback", workflow)
        self.assertIn("Stale assets rate fallback", workflow)
        self.assertIn("rateFallbackMaxAgeMinutes", workflow)
        self.assertIn("FALLBACK_MAX_AGE_MINUTES = 120", workflow)
        self.assertIn("for i in $(seq 1 12)", workflow)
        self.assertIn("Failed to push after 12 attempts.", workflow)

    def test_pages_workflow_redeploys_after_successful_data_workflows(self):
        workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_run:", workflow)
        for workflow_name in (
            "Update Assets Live Data",
            "Update veDOLO Data",
            "Update DOLO Flows Data",
            "Update Earn Snapshots",
            "Update Earn Netflow Data",
            "Update Berachain Earn Netflow Data",
            "Monitor EARN Freshness",
            "Refresh Ethereum Canonical EARN History",
            "Refresh Arbitrum Canonical EARN History",
            "Refresh Berachain Canonical EARN History",
            "Refresh Secondary Canonical EARN History",
            "Refresh Berachain Borrow-Route EARN History",
            "Update Liquidation Risk Data",
            "Update oDOLO Data",
            "Update oDOLO Flows",
            "Update veDOLO Flows",
        ):
            self.assertIn(f"- {workflow_name}", workflow)
        self.assertIn("github.event.workflow_run.conclusion == 'success'", workflow)
        self.assertNotIn("- Update Data", workflow)
        self.assertNotIn("- Update DOLO Flows\n", workflow)
        self.assertNotIn("- Update EARN Snapshots", workflow)
        self.assertNotIn("- Update oDOLO Contract Data", workflow)

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
            EARN_FRESHNESS_WORKFLOW,
        ):
            workflow = workflow_path.read_text(encoding="utf-8")
            self.assertNotIn("fetch-depth: 0", workflow, workflow_path.name)
            if "fetch-depth:" in workflow:
                self.assertIn("fetch-depth: 1", workflow, workflow_path.name)

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

    def test_ethereum_canonical_workflow_rebuilds_verified_ledger_on_fresh_history(self):
        workflow = ETHEREUM_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '12,42 * * * *'", workflow)
        self.assertIn("Build Ethereum verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)
        self.assertIn("git add -f data/earn-subaccount-history/manifest.json data/earn-subaccount-history/ethereum", workflow)
        self.assertIn("git add -f data/earn-verified-ledger/manifest.json data/earn-verified-ledger/ethereum", workflow)

    def test_snapshot_workflow_builds_polygon_verified_ledger_cache(self):
        workflow = EARN_SNAPSHOTS_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("Build Ethereum verified ledger cache", workflow)
        self.assertIn("Build Polygon zkEVM verified ledger cache", workflow)
        self.assertIn("--chain polygonzkevm", workflow)
        self.assertIn("data/earn-verified-ledger/polygonzkevm/", workflow)

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
        self.assertIn("HOT_LIMIT: '50'", workflow)
        self.assertIn("CHECKPOINT_STEPS: '60'", workflow)
        self.assertIn("COMMAND_TIMEOUT_SECONDS: '180'", workflow)
        self.assertIn("secrets.ALCHEMY_ARBITRUM_RPC_ZEN", workflow)
        self.assertIn("--prefer-stale-history", workflow)
        self.assertIn('--max-steps "$CHECKPOINT_STEPS"', workflow)
        self.assertIn('--command-timeout-seconds "$COMMAND_TIMEOUT_SECONDS"', workflow)
        self.assertIn("Build Arbitrum verified ledger cache", workflow)
        self.assertIn("build_earn_verified_ledger.py", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)

    def test_berachain_canonical_workflow_runs_in_checkpointed_chunks(self):
        workflow = BERACHAIN_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '7,37 * * * *'", workflow)
        self.assertIn("timeout-minutes: 75", workflow)
        self.assertIn("BOOTSTRAP_HOT_LIMIT: '500'", workflow)
        self.assertIn("STEADY_HOT_LIMIT: '100'", workflow)
        self.assertIn("CHECKPOINT_STEPS: '60'", workflow)
        self.assertIn("CHECKPOINT_SLEEP_SECONDS: '20'", workflow)
        self.assertIn("COMMAND_TIMEOUT_SECONDS: '180'", workflow)
        self.assertIn("has_public_baseline", workflow)
        self.assertNotIn("--existing-history-only", workflow)
        self.assertIn("--prefer-stale-history", workflow)
        self.assertIn("config/earn_berachain_canonical_hot_addresses.txt", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN: ${{ secrets.DRPC_BERACHAIN_RPC_ZEN }}", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2 DRPC_BERACHAIN_RPC_ZEN ALCHEMY_BERACHAIN_RPC_2", workflow)
        self.assertIn("Check Berachain RPC redundancy", workflow)
        self.assertIn("--allow-checkpoint-incomplete", workflow)
        self.assertIn('--command-timeout-seconds "$COMMAND_TIMEOUT_SECONDS"', workflow)
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

    def test_secondary_canonical_workflow_targets_secondary_chains(self):
        workflow = SECONDARY_CANONICAL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("cron: '23,53 * * * *'", workflow)
        self.assertIn("cron: '28,58 * * * *'", workflow)
        self.assertIn("cron: '33 * * * *'", workflow)
        self.assertIn("cron: '38 * * * *'", workflow)
        self.assertIn("type: choice", workflow)
        self.assertIn("          - mantle", workflow)
        self.assertIn("          - botanix", workflow)
        self.assertIn("          - polygonzkevm", workflow)
        self.assertIn("          - xlayer", workflow)
        self.assertIn("timeout-minutes: 75", workflow)
        self.assertIn("run-name: Refresh Secondary Canonical EARN History", workflow)
        self.assertIn("'23,53 * * * *' && 'mantle'", workflow)
        self.assertIn("'28,58 * * * *' && 'botanix'", workflow)
        self.assertIn("'33 * * * *' && 'polygonzkevm'", workflow)
        self.assertIn("'38 * * * *' && 'xlayer'", workflow)
        self.assertIn("Plan selected secondary chains", workflow)
        self.assertIn("needs: plan-secondary-canonical", workflow)
        self.assertIn("matrix: ${{ fromJson(needs.plan-secondary-canonical.outputs.matrix) }}", workflow)
        self.assertIn("group: earn-secondary-canonical-history-${{ matrix.chain }}", workflow)
        self.assertIn('"polygonzkevm": {', workflow)
        self.assertIn('"xlayer": {', workflow)
        self.assertIn('"hot_limit": 750', workflow)
        self.assertIn('"hot_limit": 500', workflow)
        self.assertIn('"hot_limit": 100', workflow)
        self.assertIn('"hot_limit": 300', workflow)
        self.assertIn('"checkpoint_steps": 60', workflow)
        self.assertIn('"checkpoint_steps": 90', workflow)
        self.assertIn("CHECKPOINT_STEPS: ${{ matrix.checkpoint_steps }}", workflow)
        self.assertIn("COMMAND_TIMEOUT_SECONDS: '180'", workflow)
        self.assertIn("--prefer-stale-history", workflow)
        self.assertIn("ALCHEMY_POLYGONZKEVM_RPC_ZEN: ${{ secrets.ALCHEMY_POLYGONZKEVM_RPC_ZEN }}", workflow)
        self.assertIn("DRP_POLYGONZKEVM_RPC_TWO: ${{ secrets.DRP_POLYGONZKEVM_RPC_TWO }}", workflow)
        self.assertIn("XLAYER_RPC_QUICKNODE_TWOJE: ${{ secrets.XLAYER_RPC_QUICKNODE_TWOJE }}", workflow)
        self.assertIn("ALCHEMY_XLAYER_RPC_ZEN: ${{ secrets.ALCHEMY_XLAYER_RPC_ZEN }}", workflow)
        self.assertIn("DRP_XLAYER_RPC_TWO: ${{ secrets.DRP_XLAYER_RPC_TWO }}", workflow)
        self.assertIn("Check secondary RPC redundancy", workflow)
        self.assertIn("QUICKNODE_MANTLE_RPC_2: ${{ secrets.QUICKNODE_MANTLE_RPC_2 }}", workflow)
        self.assertIn("DRPC_MANTLE_RPC: ${{ secrets.DRPC_MANTLE_RPC }}", workflow)
        self.assertIn("QUICKNODE_BOTANIX_RPC: ${{ secrets.QUICKNODE_BOTANIX_RPC }}", workflow)
        self.assertIn("DRPC_BOTANIX_RPC_ZEN: ${{ secrets.DRPC_BOTANIX_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_BOTANIX_RPC_ZEN: ${{ secrets.ALCHEMY_BOTANIX_RPC_ZEN }}", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)
        self.assertIn('--max-steps "$CHECKPOINT_STEPS"', workflow)
        self.assertIn('--command-timeout-seconds "$COMMAND_TIMEOUT_SECONDS"', workflow)
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

    def test_hot_wallet_selector_prioritizes_scored_wallets_before_cold_stale_backlog(self):
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
                            prefer_stale_history=True,
                        )

        self.assertEqual([active_stale], selected)
        self.assertEqual(2, metadata["staleHistoryAddressCount"])

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
        self.assertIn("EARN_WATCHDOG_MAX_DISPATCHES: '4'", workflow)
        self.assertIn("scripts/plan_earn_watchdog_dispatch.py", workflow)
        self.assertIn("dispatched_count=0", workflow)
        self.assertIn("max_dispatches=\"${EARN_WATCHDOG_MAX_DISPATCHES:-4}\"", workflow)
        self.assertIn('if [ "${chain:-}" = "__all__" ]; then', workflow)
        self.assertIn("Deferring $job_key", workflow)
        self.assertIn("displayTitle,name,status", workflow)
        self.assertIn("covers_requested_chain", workflow)
        self.assertIn('f"[{chain}]" in title or "[all]" in title', workflow)
        self.assertIn("gh workflow run", workflow)
        self.assertIn("earn-refresh-jobs.tsv", workflow)
        self.assertIn("earn-refresh-dispatched.tsv", workflow)
        self.assertIn("Skipping duplicate refresh request", workflow)
        self.assertIn('-f "chain=$chain"', workflow)
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
        self.assertIn('"canonicalWorkflowInputs": {"chain": "botanix"}', source)
        self.assertIn('"canonicalWorkflowInputs": {"chain": "polygonzkevm"}', source)
        self.assertIn('"canonicalWorkflowInputs": {"chain": "xlayer"}', source)
        self.assertIn('"xlayer": {', source)
        self.assertIn('"verifiedBlockLag": 10800', source)
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
        self.assertIn('"polygonzkevm": {"maxRuntimeSeconds": 3300, "partialOutputIntervalSeconds": 600}', workflow)
        self.assertIn('"xlayer": {"maxRuntimeSeconds": 3300, "partialOutputIntervalSeconds": 600}', workflow)
        self.assertIn("path: data/.netflow-progress/${{ matrix.chain }}.json", workflow)
        self.assertIn('git add "data/earn-netflow/${CHAIN}.json"', workflow)
        self.assertIn("X Layer netflow did not make progress past block 0", workflow)
        for chain in ("arbitrum", "ethereum", "mantle", "botanix", "polygonzkevm", "xlayer"):
            self.assertIn(f'"{chain}"', workflow)
        self.assertNotIn("scan_earn_netflow.py arbitrum,ethereum,mantle,botanix,polygonzkevm --max-runtime-seconds 19800", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_ZEN: ${{ secrets.ALCHEMY_ARBITRUM_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_POLYGONZKEVM_RPC_ZEN: ${{ secrets.ALCHEMY_POLYGONZKEVM_RPC_ZEN }}", workflow)
        self.assertIn("DRP_POLYGONZKEVM_RPC_TWO: ${{ secrets.DRP_POLYGONZKEVM_RPC_TWO }}", workflow)
        self.assertIn("XLAYER_RPC_QUICKNODE_TWOJE: ${{ secrets.XLAYER_RPC_QUICKNODE_TWOJE }}", workflow)
        self.assertIn("ALCHEMY_XLAYER_RPC_ZEN: ${{ secrets.ALCHEMY_XLAYER_RPC_ZEN }}", workflow)
        self.assertIn("DRP_XLAYER_RPC_TWO: ${{ secrets.DRP_XLAYER_RPC_TWO }}", workflow)
        self.assertIn("Check secondary RPC redundancy", workflow)
        self.assertIn("QUICKNODE_MANTLE_RPC_2: ${{ secrets.QUICKNODE_MANTLE_RPC_2 }}", workflow)
        self.assertIn("DRPC_MANTLE_RPC: ${{ secrets.DRPC_MANTLE_RPC }}", workflow)
        self.assertIn("QUICKNODE_BOTANIX_RPC: ${{ secrets.QUICKNODE_BOTANIX_RPC }}", workflow)
        self.assertIn("DRPC_BOTANIX_RPC_ZEN: ${{ secrets.DRPC_BOTANIX_RPC_ZEN }}", workflow)
        self.assertIn("ALCHEMY_BOTANIX_RPC_ZEN: ${{ secrets.ALCHEMY_BOTANIX_RPC_ZEN }}", workflow)
        self.assertIn("scripts/commit_with_fresh_earn_status.sh", workflow)
        self.assertNotIn("QUICKNODE_BERACHAIN_RPC_2: ${{ secrets.QUICKNODE_BERACHAIN_RPC_2 }}", workflow)
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
        self.assertIn("adaptive_chunk_size = min(max_chunk_size, max(1_000, int(chunk_size)))", event_scanner)
        self.assertIn("adaptive_chunk_size = min(max_chunk_size, adaptive_chunk_size * 2)", event_scanner)
        self.assertIn('CHAINS[args.chain].get("max_block_chunk")', event_scanner)
        self.assertIn('default_block_chunk = int(config.get("max_block_chunk") or DEFAULT_ADDRESS_SCAN_BLOCK_CHUNK)', history_builder)

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
