import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import fetch_dolomite_tvl as tvl
import generate_earn_snapshots as earn
import generate_supply_history as supply_history
import generate_supply_activity as supply_activity
import generate_supply_health as supply_health
import fetch_dolomite_total_supply_history as total_history
import fetch_liquidation_risk as risk
import fetch_dolomite_revenue as revenue
from update_earn_freshness_status import build_status
from test_tvl_preview_contracts import run_tvl_js_probe


class RetiredNetworksTest(unittest.TestCase):
    def test_revenue_output_and_windows_share_retirement_scope(self):
        start = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp())
        def metric(multiplier):
            return {
                "totalAllTime": 3300 * multiplier,
                "totalDataChart": [[start + day * 86400, 110 * multiplier] for day in range(30)],
                "totalDataChartBreakdown": [[start + day * 86400, {
                    "Ethereum": {"interest": 100 * multiplier},
                    "Mantle": {"interest": 10 * multiplier},
                }] for day in range(30)],
            }
        output = revenue.build_output(metric(1), metric(2), onchain_audit={},
                                      onchain_revenue_overrides={}, borrow_fee_rebate_metadata={},
                                      borrow_fee_rebate_data={})
        self.assertEqual(110, output["series"][0]["revenueUSD"])
        self.assertEqual(100, output["totals"]["dailyRevenueUSD"])
        self.assertEqual(700, output["totals"]["revenue7dUSD"])
        self.assertEqual(3180, output["totals"]["revenue30dUSD"])
        self.assertEqual(3180, output["totals"]["revenueAllTimeUSD"])
        self.assertEqual(6360, output["totals"]["feesAllTimeUSD"])
        self.assertNotIn("Mantle", output["chainTotals7d"])

    def test_revenue_retirement_preserves_past_and_rebates(self):
        active = {"revenueUSD": 90, "grossRevenueUSD": 100, "borrowFeeRebateUSD": 10,
                  "feesUSD": 200, "supplySideRevenueUSD": 100}
        retired = {"revenueUSD": 5, "grossRevenueUSD": 5, "borrowFeeRebateUSD": 0,
                   "feesUSD": 10, "supplySideRevenueUSD": 5}
        row = {key: active[key] + retired[key] for key in active}
        row.update(date="2026-09-20", chains={"Ethereum": active, "Mantle": retired})
        past = dict(row, date="2026-09-18")
        result = revenue.apply_active_network_scope([past, row])
        self.assertEqual(past, result[0])
        self.assertEqual(active["revenueUSD"], result[1]["revenueUSD"])
        self.assertEqual(10, result[1]["borrowFeeRebateUSD"])
        self.assertEqual({"Ethereum": active}, result[1]["chains"])
        self.assertEqual(result, revenue.apply_active_network_scope(result))

    def test_pre_retirement_history_is_preserved_exactly(self):
        cutoff = int(datetime(2026, 9, 19, tzinfo=timezone.utc).timestamp())
        old = [{"date": cutoff - 86400, "totalLiquidityUSD": 125.123},
               {"date": cutoff, "totalLiquidityUSD": 126}]
        new = [{"date": cutoff - 86400, "totalLiquidityUSD": 100},
               {"date": cutoff, "totalLiquidityUSD": 101}]
        self.assertEqual([old[0], new[1]], total_history.preserve_archived_history(new, old))

    def test_archived_liquidations_survive_active_scanner_updates(self):
        old = [{"chain": "mantle", "id": "old1"}, {"chain": "xlayer", "id": "old2"},
               {"chain": "ethereum", "id": "old3"}]
        self.assertEqual(old[:2], risk.retired_liquidation_rows({"liquidationHistory": old}))

    def test_default_scanners_only_use_active_deployments(self):
        active = {"ethereum", "berachain", "arbitrum"}
        self.assertEqual(active, set(earn.DEFAULT_CHAINS))
        for module in (supply_history, supply_activity, supply_health):
            self.assertEqual(active, set(module.DEFAULT_GRAPH_CHAINS))

    def test_cached_retired_tvl_is_excluded_without_rescaling_history(self):
        result = run_tvl_js_probe("""
            const old = {
              currentChainTvls: {Ethereum: 100, "Ethereum-borrowed": 20, Mantle: 10, "X Layer": 5},
              chainTokensInUsd: {Ethereum: {USDC: 120}, Mantle: {MNT: 10}, "X Layer": {OKB: 5}},
              tvl: [{date: 10, totalLiquidityUSD: 105}, {date: 20, totalLiquidityUSD: 104}],
              totalSupply: [{date: 10, totalLiquidityUSD: 135}, {date: 20, totalLiquidityUSD: 135}],
              totalTvl: 115, totalBorrowed: 20, supplyLiquidity: 135
            };
            const snapshot = buildTvlSnapshot(old, null);
            process.stdout.write(JSON.stringify(snapshot));
        """)
        self.assertEqual(["ethereum"], [row["key"] for row in result["chains"]])
        self.assertEqual([[10, 105], [20, 104]], result["history"])
        self.assertEqual(100, result["totalTvl"])
        self.assertEqual(120, result["totalSupply"])

    def test_missing_retired_sources_do_not_block_active_tvl_publication(self):
        failures, missing = tvl.blocking_tvl_failures(
            ["Mantle", "X Layer"],
            {"Ethereum": {}, "Berachain": {}, "Arbitrum": {}},
        )
        self.assertEqual([], failures)
        self.assertEqual([], missing)

    def test_freshness_does_not_dispatch_retired_chain_repairs(self):
        with tempfile.TemporaryDirectory() as tmp:
            status = build_status(data_dir=Path(tmp), live_blocks={},
                                  now=datetime(2026, 9, 19, tzinfo=timezone.utc))
        self.assertEqual({"ethereum", "berachain", "arbitrum"}, set(status["chains"]))


if __name__ == "__main__":
    unittest.main()
