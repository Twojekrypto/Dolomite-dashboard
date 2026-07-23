import unittest

import fetch_defillama
import validate_data
from validate_data import RULES


class FetchDefillamaHistoryTest(unittest.TestCase):
    def history_builder(self):
        self.assertTrue(
            hasattr(fetch_defillama, "build_history_series"),
            "fetch_defillama.build_history_series is missing",
        )
        return fetch_defillama.build_history_series

    def test_build_history_series_preserves_net_tvl_and_adds_borrowed_supply(self):
        net, supply = self.history_builder()(
            [
                {"date": 10, "totalLiquidityUSD": 100},
                {"date": 20, "totalLiquidityUSD": 120},
            ],
            [
                {"date": 10, "totalLiquidityUSD": 40},
                {"date": 20, "totalLiquidityUSD": 50},
            ],
        )

        self.assertEqual(
            net,
            [
                {"date": 10, "totalLiquidityUSD": 100},
                {"date": 20, "totalLiquidityUSD": 120},
            ],
        )
        self.assertEqual(
            supply,
            [
                {"date": 10, "totalLiquidityUSD": 140},
                {"date": 20, "totalLiquidityUSD": 170},
            ],
        )

    def test_build_history_series_aligns_union_and_rejects_invalid_points(self):
        net, supply = self.history_builder()(
            [
                {"date": "bad", "totalLiquidityUSD": 999},
                {"date": 20, "totalLiquidityUSD": 120},
                {"date": 10, "totalLiquidityUSD": 100},
                {"date": 30, "totalLiquidityUSD": -1},
            ],
            [
                {"date": 10, "totalLiquidityUSD": 40},
                {"date": 15, "totalLiquidityUSD": 5},
                {"date": 20, "totalLiquidityUSD": float("nan")},
            ],
        )

        self.assertEqual(
            net,
            [
                {"date": 10, "totalLiquidityUSD": 100},
                {"date": 20, "totalLiquidityUSD": 120},
            ],
        )
        self.assertEqual(
            supply,
            [
                {"date": 10, "totalLiquidityUSD": 140},
                {"date": 15, "totalLiquidityUSD": 5},
                {"date": 20, "totalLiquidityUSD": 120},
            ],
        )

    def test_defillama_validation_requires_both_history_series(self):
        rules = RULES["defillama_data.json"]

        self.assertIn("tvl", rules["required_keys"])
        self.assertIn("totalSupply", rules["required_keys"])
        descriptions = [description for description, _ in rules["checks"]]
        self.assertIn("TVL history must be sorted and populated", descriptions)
        self.assertIn("Total Supply history must be sorted and populated", descriptions)

    def test_defillama_validation_rejects_non_finite_and_boolean_values(self):
        valid_rows = [
            {"date": index + 1, "totalLiquidityUSD": index + 1}
            for index in range(1000)
        ]
        invalid_rows = (
            valid_rows[:999]
            + [{"date": 1000, "totalLiquidityUSD": float("nan")}],
            valid_rows[:999]
            + [{"date": 1000, "totalLiquidityUSD": float("inf")}],
            valid_rows[:999]
            + [{"date": 1000, "totalLiquidityUSD": True}],
            valid_rows[:999]
            + [{"date": True, "totalLiquidityUSD": 1000}],
        )

        for rows in invalid_rows:
            with self.subTest(last_row=rows[-1]):
                self.assertFalse(
                    validate_data._defillama_history_series_valid(
                        {"tvl": rows},
                        "tvl",
                    )
                )


if __name__ == "__main__":
    unittest.main()
