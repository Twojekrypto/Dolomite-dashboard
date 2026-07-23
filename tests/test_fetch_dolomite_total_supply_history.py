import importlib
import unittest

from validate_data import RULES


class DolomiteTotalSupplyHistoryTest(unittest.TestCase):
    def history_module(self):
        try:
            return importlib.import_module("fetch_dolomite_total_supply_history")
        except ModuleNotFoundError:
            self.fail("fetch_dolomite_total_supply_history.py is missing")

    def test_aggregates_complete_official_market_history(self):
        module = self.history_module()
        aggregate = module.aggregate_market_histories(
            [
                {
                    "marketKey": "ethereum:usdc",
                    "currentSupplyUsd": 100,
                    "points": {
                        10: 80,
                        20: 90,
                    },
                },
                {
                    "marketKey": "arbitrum:weth",
                    "currentSupplyUsd": 50,
                    "points": {
                        10: 40,
                        20: 50,
                    },
                },
                {
                    "marketKey": "ethereum:inactive",
                    "currentSupplyUsd": 0,
                    "points": {
                        10: 5,
                    },
                },
            ]
        )

        self.assertEqual(
            aggregate,
            [
                {"date": 10, "totalLiquidityUSD": 125},
                {"date": 20, "totalLiquidityUSD": 140},
            ],
        )

    def test_merges_official_window_and_anchors_latest_to_current_supply(self):
        module = self.history_module()
        merged = module.merge_total_supply_histories(
            [
                {"date": 1, "totalLiquidityUSD": 100},
                {"date": 5, "totalLiquidityUSD": 120},
                {"date": 10, "totalLiquidityUSD": 140},
            ],
            [
                {"date": 10, "totalLiquidityUSD": 200},
                {"date": 20, "totalLiquidityUSD": 220},
            ],
            current_timestamp=25,
            current_supply=215,
        )

        self.assertEqual(
            merged,
            [
                {"date": 1, "totalLiquidityUSD": 100},
                {"date": 5, "totalLiquidityUSD": 120},
                {"date": 10, "totalLiquidityUSD": 200},
                {"date": 20, "totalLiquidityUSD": 220},
                {"date": 25, "totalLiquidityUSD": 215},
            ],
        )

    def test_excludes_active_market_with_stale_metrics_from_recent_window(self):
        module = self.history_module()
        try:
            aggregate = module.aggregate_market_histories(
                [
                    {
                        "marketKey": "ethereum:fresh",
                        "currentSupplyUsd": 100,
                        "points": {
                            1_000_000: 80,
                            1_086_400: 90,
                        },
                    },
                    {
                        "marketKey": "xlayer:stale",
                        "currentSupplyUsd": 50,
                        "points": {
                            1_000: 50,
                            87_400: 50,
                        },
                    },
                ]
            )
        except ValueError as exc:
            self.fail(f"stale market should not erase recent coverage: {exc}")

        self.assertEqual(
            aggregate,
            [
                {"date": 1_000_000, "totalLiquidityUSD": 80},
                {"date": 1_086_400, "totalLiquidityUSD": 90},
            ],
        )

    def test_validator_requires_official_total_supply_history(self):
        rules = RULES.get("dolomite_total_supply_history.json")

        self.assertIsNotNone(rules)
        self.assertIn("totalSupply", rules["required_keys"])
        self.assertIn("currentSupply", rules["required_keys"])
        descriptions = [description for description, _ in rules["checks"]]
        self.assertIn(
            "latest Total Supply history must match current supply",
            descriptions,
        )


if __name__ == "__main__":
    unittest.main()
