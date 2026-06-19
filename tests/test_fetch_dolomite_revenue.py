import unittest

from fetch_dolomite_revenue import build_output
from validate_data import (
    _dolomite_revenue_chain_windows_valid,
    _dolomite_revenue_totals_valid,
    _dolomite_revenue_window_totals_valid,
)


START_TS = 1_700_000_000
DAY_SECONDS = 86_400


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
        self.assertEqual(output["latest"]["liquidationFeesUSD"], 4.5)
        self.assertEqual(output["totals"]["dailyRevenueUSD"], 100)
        self.assertEqual(output["totals"]["dailyFeesUSD"], 500)
        self.assertEqual(output["totals"]["dailyLiquidationFeesUSD"], 4.5)
        self.assertEqual(output["totals"]["dailySupplySideRevenueUSD"], 400)
        self.assertEqual(output["totals"]["previousDailyRevenueUSD"], 60)
        self.assertEqual(output["totals"]["previousDailyFeesUSD"], 300)
        self.assertEqual(output["totals"]["revenue7dUSD"], expected_revenue_7d)
        self.assertEqual(output["totals"]["fees7dUSD"], expected_fees_7d)
        self.assertEqual(output["totals"]["liquidationFees7dUSD"], 4.5)
        self.assertEqual(output["totals"]["revenue30dUSD"], expected_revenue_30d)
        self.assertEqual(output["totals"]["fees30dUSD"], expected_fees_30d)
        self.assertEqual(output["totals"]["liquidationFees30dUSD"], 4.5)
        self.assertEqual(output["totals"]["liquidationFeesAllTimeUSD"], 4.5)
        self.assertEqual(output["assurance"]["classification"], "adapter-estimated protocol borrow-interest revenue plus confirmed liquidation fee-agent transfers")
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))


if __name__ == "__main__":
    unittest.main()
