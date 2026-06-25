import unittest
import contextlib
import io
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fetch_dolomite_revenue import build_output, onchain_audit_assurance
from validate_data import (
    RULES,
    ValidationResult,
    _dolomite_revenue_chain_windows_valid,
    _dolomite_revenue_totals_valid,
    _dolomite_revenue_window_totals_valid,
    validate_file,
)


START_TS = 1_700_000_000
DAY_SECONDS = 86_400
ROOT = Path(__file__).resolve().parents[1]


def metric_payload(total24h, latest_value, step):
    chart_rows = []
    breakdown_rows = []
    for index in range(31):
        timestamp = START_TS + index * DAY_SECONDS
        value = latest_value if index == 30 else (index + 1) * step
        chart_rows.append([timestamp, value])
        breakdown_rows.append([timestamp, {"Ethereum": {"interest": value}}])

    return {
        "total24h": total24h,
        "total48hto24h": step,
        "total7d": step * 7,
        "total30d": step * 30,
        "totalAllTime": step * 300,
        "totalDataChart": chart_rows,
        "totalDataChartBreakdown": breakdown_rows,
    }


class FetchDolomiteRevenueTest(unittest.TestCase):
    def test_daily_totals_follow_latest_series_when_total24h_lags(self):
        revenue_data = metric_payload(total24h=9_999, latest_value=100, step=2)
        fees_data = metric_payload(total24h=8_888, latest_value=500, step=10)

        output = build_output(revenue_data, fees_data, onchain_audit=None)

        expected_revenue_7d = 100 + sum((index + 1) * 2 for index in range(24, 30))
        expected_fees_7d = 500 + sum((index + 1) * 10 for index in range(24, 30))
        expected_revenue_30d = 100 + sum((index + 1) * 2 for index in range(1, 30))
        expected_fees_30d = 500 + sum((index + 1) * 10 for index in range(1, 30))

        self.assertEqual(output["latest"]["revenueUSD"], 100)
        self.assertEqual(output["latest"]["feesUSD"], 500)
        self.assertNotIn("liquidatorEarningsUSD", output["latest"])
        self.assertEqual(output["totals"]["dailyRevenueUSD"], 100)
        self.assertEqual(output["totals"]["dailyFeesUSD"], 500)
        self.assertNotIn("dailyLiquidatorEarningsUSD", output["totals"])
        self.assertEqual(output["totals"]["dailySupplySideRevenueUSD"], 400)
        self.assertEqual(output["totals"]["previousDailyRevenueUSD"], 60)
        self.assertEqual(output["totals"]["previousDailyFeesUSD"], 300)
        self.assertEqual(output["totals"]["revenue7dUSD"], expected_revenue_7d)
        self.assertEqual(output["totals"]["fees7dUSD"], expected_fees_7d)
        self.assertNotIn("liquidatorEarnings7dUSD", output["totals"])
        self.assertEqual(output["totals"]["revenue30dUSD"], expected_revenue_30d)
        self.assertEqual(output["totals"]["fees30dUSD"], expected_fees_30d)
        self.assertNotIn("liquidatorEarnings30dUSD", output["totals"])
        self.assertNotIn("liquidatorEarningsAllTimeUSD", output["totals"])
        self.assertEqual(output["assurance"]["classification"], "adapter-estimated protocol borrow-interest revenue")
        self.assertTrue(any("veDOLO borrow-fee rebates are not netted" in item for item in output["methodology"]["sourceLimitations"]))
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_onchain_audit_assurance_marks_stale_target_date(self):
        assurance = onchain_audit_assurance(
            {
                "status": "warn",
                "generatedAt": "2026-06-24T10:04:22Z",
                "targetDate": "2026-06-22",
                "summary": {"maxRevenueDiffPct": 0.031, "revenueDiffUnbounded": True},
            },
            now=datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(assurance["onchainAuditStatus"], "stale")
        self.assertEqual(assurance["onchainAuditRawStatus"], "warn")
        self.assertEqual(assurance["onchainAuditTargetDate"], "2026-06-22")
        self.assertEqual(assurance["onchainAuditExpectedTargetDate"], "2026-06-23")
        self.assertTrue(assurance["onchainAuditStale"])

    def test_build_output_embeds_latest_onchain_audit_status(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={
                "status": "warn",
                "generatedAt": "2026-06-25T09:00:00Z",
                "targetDate": "2026-06-23",
                "summary": {"maxRevenueDiffPct": 0.031, "revenueDiffUnbounded": True},
                "chains": {
                    "Berachain": {
                        "status": "warn",
                        "feesUSD": 100.0,
                        "revenueUSD": 15.0,
                        "defillamaFeesUSD": 90.0,
                        "defillamaRevenueUSD": 13.5,
                        "revenueDiffPct": 0.11111111,
                        "feesDiffPct": 0.11111111,
                        "warnReasons": ["revenue_diff_exceeds_tolerance"],
                    }
                },
            },
            now=datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(output["assurance"]["onchainAuditStatus"], "warn")
        self.assertEqual(output["assurance"]["onchainAuditTargetDate"], "2026-06-23")
        self.assertEqual(output["assurance"]["onchainAuditMaxRevenueDiffPct"], 0.031)
        self.assertTrue(output["assurance"]["onchainAuditRevenueDiffUnbounded"])
        self.assertEqual(output["assurance"]["onchainAuditChains"]["Berachain"]["status"], "warn")
        self.assertEqual(output["assurance"]["onchainAuditChains"]["Berachain"]["warnReasons"], ["revenue_diff_exceeds_tolerance"])

    def test_build_output_embeds_berachain_borrow_fee_rebate_metadata_without_netting(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={},
            borrow_fee_rebate_metadata={
                "veDoloStartTimestamp": 1779321600,
                "veDoloHoldingFactor": "5.000000000000000000",
                "currentEpochIndex": 6,
                "currentEpochStartTimestamp": 1782345600,
                "onchainFeeRebateEpochIndexMap": {"80094": 4},
                "onchainRollingClaimsEpochIndexMap": {"80094": 4},
                "allChainRebateInfo": {
                    "80094": {
                        "startEpoch": 1,
                        "claimsEnabled": False,
                        "rebatePercentage": "0.100000000000000006",
                        "marketToRebateInfo": {
                            "0": {"startEpoch": 1, "endEpoch": None},
                            "1": {"startEpoch": 1, "endEpoch": None},
                        },
                    }
                },
            },
        )

        self.assertEqual(output["borrowFeeRebates"]["status"], "active")
        self.assertEqual(output["borrowFeeRebates"]["chains"]["Berachain"]["rebatePercentage"], 0.1)
        self.assertEqual(output["borrowFeeRebates"]["chains"]["Berachain"]["marketCount"], 2)
        self.assertEqual(output["assurance"]["borrowFeeRebateStatus"], "active_pre_rebate_not_netted")
        self.assertIn("borrowFeeRebates", output["sourceUrls"])

    def test_validator_does_not_depend_on_liquidation_history_for_revenue(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)
        output = build_output(revenue_data, fees_data, onchain_audit=None)

        with tempfile.TemporaryDirectory() as tmp:
            previous_cwd = os.getcwd()
            try:
                os.chdir(tmp)
                with open("dolomite_revenue.json", "w") as f:
                    json.dump(output, f)
                with open("liquidation_history.json", "w") as f:
                    json.dump({
                        "liquidationHistory": [{
                            "timestamp": START_TS + 30 * DAY_SECONDS,
                            "liquidationRewardUSD": 4.5,
                        }]
                    }, f)

                rules = {**RULES["dolomite_revenue.json"], "min_bytes": 0}
                result = ValidationResult()
                with contextlib.redirect_stdout(io.StringIO()):
                    validate_file("dolomite_revenue.json", rules, result)
            finally:
                os.chdir(previous_cwd)

        self.assertEqual(result.failed, 0)

    def test_liquidation_workflow_no_longer_refreshes_revenue(self):
        workflow = (ROOT / ".github/workflows/update-liquidation-risk.yml").read_text()

        self.assertNotIn("python3 fetch_dolomite_revenue.py", workflow)
        self.assertNotIn("dolomite_revenue.json", workflow)

    def test_revenue_ui_surfaces_per_chain_audit_status(self):
        html = (ROOT / "revenue-preview.html").read_text(encoding="utf-8")

        self.assertIn("auditStatusForChain", html)
        self.assertIn("onchainAuditChains", html)
        self.assertIn("onchain audit STALE", html)


if __name__ == "__main__":
    unittest.main()
