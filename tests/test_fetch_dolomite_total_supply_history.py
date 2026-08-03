import importlib
import unittest
from decimal import Decimal
from unittest import mock

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

    def test_short_new_market_does_not_collapse_established_history(self):
        module = self.history_module()
        aggregate = module.aggregate_market_histories(
            [
                {
                    "marketKey": "arbitrum:weth",
                    "currentSupplyUsd": 100,
                    "points": {
                        10: 80,
                        20: 90,
                        30: 100,
                    },
                },
                {
                    "marketKey": "arbitrum:new-market",
                    "currentSupplyUsd": 10,
                    "points": {
                        30: 10,
                    },
                },
            ]
        )

        self.assertEqual(
            aggregate,
            [
                {"date": 10, "totalLiquidityUSD": 80},
                {"date": 20, "totalLiquidityUSD": 90},
                {"date": 30, "totalLiquidityUSD": 110},
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

    def test_aggregates_net_tvl_as_supply_minus_borrowed(self):
        module = self.history_module()

        self.assertTrue(
            hasattr(module, "aggregate_net_tvl_histories"),
            "official Net TVL history aggregator is missing",
        )
        aggregate = module.aggregate_net_tvl_histories(
            [
                {
                    "marketKey": "ethereum:usdc",
                    "currentSupplyUsd": 100,
                    "points": {10: 80, 20: 90},
                    "borrowPoints": {10: 20, 20: 25},
                },
                {
                    "marketKey": "arbitrum:weth",
                    "currentSupplyUsd": 50,
                    "points": {10: 40, 20: 50},
                    "borrowPoints": {10: 10, 20: 15},
                },
                {
                    "marketKey": "ethereum:inactive",
                    "currentSupplyUsd": 0,
                    "points": {10: 5},
                    "borrowPoints": {10: 1},
                },
            ]
        )

        self.assertEqual(
            aggregate,
            [
                {"date": 10, "totalLiquidityUSD": 94},
                {"date": 20, "totalLiquidityUSD": 100},
            ],
        )

    def test_net_tvl_allows_negative_market_component_when_protocol_total_is_positive(self):
        module = self.history_module()

        try:
            aggregate = module.aggregate_net_tvl_histories(
                [
                    {
                        "marketKey": "berachain:virtual-liquidity",
                        "currentSupplyUsd": 10,
                        "points": {10: 10},
                        "borrowPoints": {10: 20},
                    },
                    {
                        "marketKey": "ethereum:collateral",
                        "currentSupplyUsd": 50,
                        "points": {10: 50},
                        "borrowPoints": {10: 0},
                    },
                ]
            )
        except ValueError as exc:
            self.fail(f"protocol-level Net TVL should remain valid: {exc}")

        self.assertEqual(
            aggregate,
            [{"date": 10, "totalLiquidityUSD": 40}],
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

    def test_transient_failure_for_immaterial_market_is_marked_stale(self):
        module = self.history_module()
        markets = [
            {
                "marketKey": "mantle:major",
                "currentSupplyUsd": Decimal("814000000"),
            },
            {
                "marketKey": "mantle:tiny",
                "currentSupplyUsd": Decimal("12"),
            },
        ]

        def fake_fetch(market):
            if market["marketKey"] == "mantle:tiny":
                raise RuntimeError("HTTP 500")
            return {
                **market,
                "points": {1_000_000: Decimal("800000000")},
                "borrowPoints": {1_000_000: Decimal("300000000")},
            }

        with mock.patch.object(module, "_fetch_metric_series", side_effect=fake_fetch):
            histories = module._fetch_all_market_histories(markets)

        recent, stale = module.split_recent_market_histories(histories)
        stale_supply = module.validate_official_snapshot_coverage(
            Decimal("814000012"),
            Decimal("514000000"),
            stale,
        )

        self.assertEqual(["mantle:major"], [market["marketKey"] for market in recent])
        self.assertEqual(["mantle:tiny"], [market["marketKey"] for market in stale])
        self.assertEqual("HTTP 500", stale[0]["fetchError"])
        self.assertEqual(Decimal("12"), stale_supply)

    def test_material_missing_market_share_still_fails(self):
        module = self.history_module()

        with self.assertRaisesRegex(ValueError, "stale market coverage"):
            module.validate_official_snapshot_coverage(
                Decimal("814000000"),
                Decimal("514000000"),
                [{"marketKey": "mantle:material", "currentSupplyUsd": Decimal("1000000")}],
            )

    def test_material_stale_market_is_refetched_before_validation(self):
        module = self.history_module()
        histories = [
            {
                "marketKey": "ethereum:fresh",
                "currentSupplyUsd": Decimal("800000000"),
                "points": {1_000_000: Decimal("790000000")},
                "borrowPoints": {1_000_000: Decimal("280000000")},
            },
            {
                "marketKey": "xlayer:material",
                "currentSupplyUsd": Decimal("12000000"),
                "points": {1_000: Decimal("11000000")},
                "borrowPoints": {1_000: Decimal("1000000")},
            },
        ]
        refreshed_market = {
            **histories[1],
            "points": {1_000_000: Decimal("12000000")},
            "borrowPoints": {1_000_000: Decimal("1000000")},
        }

        with mock.patch.object(module, "_fetch_metric_series", return_value=refreshed_market) as fetch:
            refreshed = module.retry_stale_market_histories(histories)

        fetch.assert_called_once_with(histories[1])
        recent, stale = module.split_recent_market_histories(refreshed)
        self.assertEqual([], stale)
        self.assertEqual(
            {"ethereum:fresh", "xlayer:material"},
            {row["marketKey"] for row in recent},
        )

    def test_failed_stale_market_retry_keeps_strict_failure(self):
        module = self.history_module()
        histories = [
            {
                "marketKey": "ethereum:fresh",
                "currentSupplyUsd": Decimal("800000000"),
                "points": {1_000_000: Decimal("790000000")},
                "borrowPoints": {1_000_000: Decimal("280000000")},
            },
            {
                "marketKey": "xlayer:material",
                "currentSupplyUsd": Decimal("12000000"),
                "points": {},
                "borrowPoints": {},
            },
        ]

        with mock.patch.object(module, "_fetch_metric_series", side_effect=RuntimeError("still stale")):
            refreshed = module.retry_stale_market_histories(histories)

        _, stale = module.split_recent_market_histories(refreshed)
        with self.assertRaisesRegex(ValueError, "stale market coverage"):
            module.validate_official_snapshot_coverage(
                Decimal("812000000"),
                Decimal("519000000"),
                stale,
            )

    def test_validator_requires_official_total_supply_history(self):
        rules = RULES.get("dolomite_total_supply_history.json")

        self.assertIsNotNone(rules)
        self.assertIn("totalSupply", rules["required_keys"])
        self.assertIn("currentSupply", rules["required_keys"])
        self.assertIn("tvl", rules["required_keys"])
        self.assertIn("currentTvl", rules["required_keys"])
        descriptions = [description for description, _ in rules["checks"]]
        self.assertIn(
            "latest Total Supply history must match current supply",
            descriptions,
        )
        self.assertIn(
            "latest Net TVL history must match current Net TVL",
            descriptions,
        )
        self.assertIn(
            "Net TVL history must reconcile with Total Supply history",
            descriptions,
        )


if __name__ == "__main__":
    unittest.main()
