import unittest
import contextlib
import io
import json
import os
import tempfile
from pathlib import Path

from fetch_dolomite_revenue import build_output
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

        output = build_output(
            revenue_data,
            fees_data,
            {"2023-12-14": 4.5},
        )

        expected_revenue_7d = 100 + sum((index + 1) * 2 for index in range(24, 30))
        expected_fees_7d = 500 + sum((index + 1) * 10 for index in range(24, 30))
        expected_revenue_30d = 100 + sum((index + 1) * 2 for index in range(1, 30))
        expected_fees_30d = 500 + sum((index + 1) * 10 for index in range(1, 30))

        self.assertEqual(output["latest"]["revenueUSD"], 100)
        self.assertEqual(output["latest"]["feesUSD"], 500)
        self.assertEqual(output["latest"]["liquidatorEarningsUSD"], 4.5)
        self.assertEqual(output["totals"]["dailyRevenueUSD"], 100)
        self.assertEqual(output["totals"]["dailyFeesUSD"], 500)
        self.assertEqual(output["totals"]["dailyLiquidatorEarningsUSD"], 4.5)
        self.assertEqual(output["totals"]["dailySupplySideRevenueUSD"], 400)
        self.assertEqual(output["totals"]["previousDailyRevenueUSD"], 60)
        self.assertEqual(output["totals"]["previousDailyFeesUSD"], 300)
        self.assertEqual(output["totals"]["revenue7dUSD"], expected_revenue_7d)
        self.assertEqual(output["totals"]["fees7dUSD"], expected_fees_7d)
        self.assertEqual(output["totals"]["liquidatorEarnings7dUSD"], 4.5)
        self.assertEqual(output["totals"]["revenue30dUSD"], expected_revenue_30d)
        self.assertEqual(output["totals"]["fees30dUSD"], expected_fees_30d)
        self.assertEqual(output["totals"]["liquidatorEarnings30dUSD"], 4.5)
        self.assertEqual(output["totals"]["liquidatorEarningsAllTimeUSD"], 4.5)
        self.assertEqual(output["assurance"]["classification"], "adapter-estimated protocol borrow-interest revenue plus liquidation rewards earned by liquidators")
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_validator_rejects_stale_liquidator_earnings_snapshot(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)
        output = build_output(revenue_data, fees_data, {})

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

        self.assertGreater(result.failed, 0)
        self.assertTrue(any("liquidator earnings" in error for error in result.errors))

    def test_liquidation_workflow_refreshes_revenue_when_history_changes(self):
        workflow = (ROOT / ".github/workflows/update-liquidation-risk.yml").read_text()

        self.assertIn("git diff --quiet -- liquidation_history.json", workflow)
        self.assertIn("python3 fetch_dolomite_revenue.py", workflow)
        self.assertIn("dolomite_revenue.json", workflow)


if __name__ == "__main__":
    unittest.main()
