import unittest
from datetime import datetime, timezone
from pathlib import Path

from audit_dolomite_revenue_onchain import (
    audit_window_for_date,
    build_audit_report,
    classify_chain_result,
    default_target_date,
)


ROOT = Path(__file__).resolve().parents[1]


class AuditDolomiteRevenueOnchainTest(unittest.TestCase):
    def test_default_target_date_uses_t_minus_two_closed_day(self):
        now = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)

        self.assertEqual(default_target_date(now), "2026-06-21")

    def test_audit_window_replays_the_named_utc_day(self):
        start, end = audit_window_for_date("2026-06-21")

        self.assertEqual(start, 1782000000)
        self.assertEqual(end, 1782086400)

    def test_chain_result_warns_when_usd_diff_exceeds_tolerance(self):
        result = classify_chain_result(
            "Arbitrum",
            defillama={"feesUSD": 1_000.0, "revenueUSD": 200.0},
            onchain={"feesUSD": 1_030.0, "revenueUSD": 206.0, "protocolCut": 0.2},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "warn")
        self.assertGreater(result["revenueDiffPct"], 0.02)
        self.assertGreater(result["feesDiffPct"], 0.02)

    def test_chain_result_passes_inside_tolerance(self):
        result = classify_chain_result(
            "Ethereum",
            defillama={"feesUSD": 10_000.0, "revenueUSD": 2_000.0},
            onchain={"feesUSD": 10_100.0, "revenueUSD": 2_015.0, "protocolCut": 0.1995},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "pass")

    def test_missing_defillama_chain_total_is_zero_baseline(self):
        result = classify_chain_result(
            "Mantle",
            defillama=None,
            onchain={"feesUSD": 0.0, "revenueUSD": 0.0, "protocolCut": 0.0},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["defillamaChainMissing"])

    def test_missing_defillama_chain_total_warns_on_onchain_revenue(self):
        result = classify_chain_result(
            "Mantle",
            defillama=None,
            onchain={"feesUSD": 10.0, "revenueUSD": 2.0, "protocolCut": 0.2},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "warn")
        self.assertIsNone(result["revenueDiffPct"])
        self.assertTrue(result["revenueDiffUnbounded"])

    def test_report_is_partial_when_only_some_chains_are_audited(self):
        report = build_audit_report(
            target_date="2026-06-21",
            target_timestamp=1782000000,
            window_start_timestamp=1781913600,
            chain_results={
                "Arbitrum": {"status": "pass"},
                "Ethereum": {"status": "missing", "error": "archive RPC unavailable"},
            },
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(report["status"], "partial")
        self.assertEqual(report["summary"]["auditedChainCount"], 1)
        self.assertEqual(report["summary"]["missingChainCount"], 1)

    def test_report_warns_if_any_chain_warns(self):
        report = build_audit_report(
            target_date="2026-06-21",
            target_timestamp=1782000000,
            window_start_timestamp=1781913600,
            chain_results={
                "Arbitrum": {"status": "pass"},
                "Ethereum": {"status": "warn"},
            },
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(report["status"], "warn")

    def test_report_keeps_finite_max_when_another_diff_is_unbounded(self):
        report = build_audit_report(
            target_date="2026-06-21",
            target_timestamp=1782000000,
            window_start_timestamp=1782000000,
            chain_results={
                "Ethereum": {
                    "status": "warn",
                    "feesDiffPct": 0.022,
                    "revenueDiffPct": 0.031,
                    "feesDiffUnbounded": False,
                    "revenueDiffUnbounded": False,
                },
                "Mantle": {
                    "status": "warn",
                    "feesDiffPct": None,
                    "revenueDiffPct": None,
                    "feesDiffUnbounded": True,
                    "revenueDiffUnbounded": True,
                },
            },
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(report["summary"]["maxRevenueDiffPct"], 0.031)
        self.assertTrue(report["summary"]["revenueDiffUnbounded"])

    def test_workflow_runs_audit_and_commits_output(self):
        workflow = (ROOT / ".github/workflows/audit-dolomite-revenue-onchain.yml").read_text(encoding="utf-8")

        self.assertIn("python3 audit_dolomite_revenue_onchain.py", workflow)
        self.assertIn("data/dolomite-revenue-onchain-audit.json", workflow)
        self.assertIn("git add data/dolomite-revenue-onchain-audit.json dolomite_revenue.json", workflow)

    def test_pages_redeploys_after_revenue_audit_workflow(self):
        workflow = (ROOT / ".github/workflows/pages.yml").read_text(encoding="utf-8")

        self.assertIn("Audit Dolomite Revenue Onchain", workflow)


if __name__ == "__main__":
    unittest.main()
