import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_CORE = ROOT / "dashboard-core.html"
ETHEREUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-ethereum-canonical-history.yml"
ARBITRUM_CANONICAL_WORKFLOW = ROOT / ".github" / "workflows" / "update-earn-arbitrum-canonical-history.yml"
EARN_COVERAGE_REPORT = ROOT / "report_earn_subaccount_history_coverage.py"
CANONICAL_REFRESH_RUNNER = ROOT / "run_earn_canonical_history_refresh.py"
NETFLOW_SCANNER = ROOT / "scan_earn_netflow.py"


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

    def test_canonical_refresh_runner_keeps_json_stdout_clean(self):
        runner = CANONICAL_REFRESH_RUNNER.read_text(encoding="utf-8")
        self.assertIn("stderr=subprocess.PIPE", runner)
        self.assertNotIn("stderr=subprocess.STDOUT", runner)
        scanner = NETFLOW_SCANNER.read_text(encoding="utf-8")
        self.assertIn("file=sys.stderr", scanner)


if __name__ == "__main__":
    unittest.main()
