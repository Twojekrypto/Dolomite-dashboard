import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import generate_dolo_flows as flows


COINBASE_10 = "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43"
COINBASE = "0xdf3100ac6c20c4a9443ece6c639c4ee91d592062"
SOURCE = "0xabe44baf180ac426565503bbc3ecf71a0459456e"
SILENTLY_MISSED_BLOCK = 25_482_810
UNLABELED_CONTRACT = "0xcccccccccccccccccccccccccccccccccccccccc"
OUTSIDE = "0xdddddddddddddddddddddddddddddddddddddddd"
STRATEGIC_INVESTOR_CLAIMS = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
INVESTOR_CLAIMS = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"
EARLY_ONLY = "0x1111111111111111111111111111111111111111"
INVESTOR_ONLY = "0x2222222222222222222222222222222222222222"
OVERLAP = "0x3333333333333333333333333333333333333333"


class GenerateDoloFlowsIntegrityTests(unittest.TestCase):
    def test_vesting_investors_are_classified_by_official_claim_contract(self):
        payload = flows.extract_vesting_investors({
            "eth": [
                (STRATEGIC_INVESTOR_CLAIMS, OUTSIDE, 99 * 10**18, 101),
            ],
            "bera": [
                (STRATEGIC_INVESTOR_CLAIMS, EARLY_ONLY, 2 * 10**18, 102),
                (INVESTOR_CLAIMS, INVESTOR_ONLY, 3 * 10**18, 103),
                (STRATEGIC_INVESTOR_CLAIMS, OVERLAP, 4 * 10**18, 201),
                (INVESTOR_CLAIMS, OVERLAP, 5 * 10**18, 202),
                (STRATEGIC_INVESTOR_CLAIMS, OVERLAP, 10**18 // 2, 203),
            ],
        })

        self.assertEqual(payload["schemaVersion"], 2)
        self.assertEqual(payload["early_investors"], [EARLY_ONLY, OVERLAP])
        self.assertEqual(payload["investors"], [INVESTOR_ONLY, OVERLAP])
        self.assertEqual(payload["team"], [])
        self.assertNotIn(OUTSIDE, payload["early_investors"])

        records = {row["address"]: row for row in payload["wallets"]}
        self.assertEqual(records[EARLY_ONLY]["label"], "Early Investor")
        self.assertEqual(records[EARLY_ONLY]["sourceChains"], ["bera"])
        self.assertEqual(records[INVESTOR_ONLY]["label"], "Investor")
        self.assertEqual(records[OVERLAP]["label"], "Early Investor")
        self.assertEqual(
            records[OVERLAP]["claimSources"],
            ["strategic_investor_claims", "investor_claims"],
        )
        self.assertEqual(records[OVERLAP]["transferCount"], 3)
        self.assertEqual(records[OVERLAP]["firstTransferBlock"], 201)
        self.assertEqual(records[OVERLAP]["lastTransferBlock"], 203)
        self.assertEqual(records[OVERLAP]["receivedDolo"], "9.5")
        self.assertTrue(records[OVERLAP]["alsoReceivedLongTermTranche"])

    def test_investor_claim_recipients_are_never_derived_as_core_team(self):
        payload = flows.extract_vesting_investors({
            "eth": [],
            "bera": [(INVESTOR_CLAIMS, INVESTOR_ONLY, 10**18, 300)],
        })

        self.assertEqual(payload["investors"], [INVESTOR_ONLY])
        self.assertEqual(payload["team"], [])
        self.assertEqual(payload["methodology"]["team"], "not-derived-from-investor-claims")

    def test_single_block_range_still_has_scan_work(self):
        self.assertTrue(hasattr(flows, "block_range_has_work"))
        self.assertTrue(flows.block_range_has_work(100, 100))
        self.assertTrue(flows.block_range_has_work(100, 101))
        self.assertFalse(flows.block_range_has_work(101, 100))

    def test_recent_authoritative_rescan_covers_silent_coinbase_block(self):
        self.assertTrue(hasattr(flows, "incremental_refresh_start"))
        last_block = 25_500_000

        refresh_start = flows.incremental_refresh_start(
            last_block,
            21_500_000,
            50_000,
        )

        self.assertEqual(refresh_start, 25_450_001)
        self.assertLessEqual(refresh_start, SILENTLY_MISSED_BLOCK)

    def test_authoritative_refresh_replaces_overlap_without_duplicates(self):
        self.assertTrue(hasattr(flows, "replace_transfer_range"))
        before = (SOURCE, OUTSIDE, 1 * 10**18, 99)
        stale = (SOURCE, COINBASE_10, 2 * 10**18, 105)
        after = (COINBASE_10, OUTSIDE, 3 * 10**18, 111)
        authoritative = (SOURCE, COINBASE_10, 12_500_000 * 10**18, 105)

        replaced = flows.replace_transfer_range(
            [before, stale, after],
            [authoritative],
            100,
            110,
        )

        self.assertEqual(replaced, [before, authoritative, after])

    def test_scan_tip_cannot_rewind_when_rpc_is_unavailable_or_stale(self):
        self.assertTrue(hasattr(flows, "validated_scan_end"))
        self.assertEqual(flows.validated_scan_end("eth", 25_500_010, 5, 25_500_000), 25_500_005)
        with self.assertRaisesRegex(RuntimeError, "current block"):
            flows.validated_scan_end("eth", 0, 5, 25_500_000)
        with self.assertRaisesRegex(RuntimeError, "rewind"):
            flows.validated_scan_end("eth", 25_499_000, 5, 25_500_000)

    def test_labeled_cex_contract_remains_visible_in_flow_rows(self):
        self.assertTrue(hasattr(flows, "select_dynamic_flow_exclusions"))

        excluded = flows.select_dynamic_flow_exclusions(
            {COINBASE_10, UNLABELED_CONTRACT},
            {COINBASE_10: {"label": "Coinbase 10", "type": "cex"}},
        )

        self.assertNotIn(COINBASE_10, excluded)
        self.assertIn(UNLABELED_CONTRACT, excluded)

    def test_directional_components_reconcile_coinbase_net_flow(self):
        self.assertTrue(hasattr(flows, "calculate_flow_components"))
        transfers = [
            (SOURCE, COINBASE_10, 12_500_000 * 10**18, 100),
            (COINBASE_10, COINBASE, 6_284_454 * 10**18, 101),
        ]

        components = flows.calculate_flow_components(transfers)[COINBASE_10]

        self.assertEqual(components["gross_inflow"], 12_500_000)
        self.assertEqual(components["gross_outflow"], 6_284_454)
        self.assertEqual(components["net_flow"], 6_215_546)

    def test_successful_gap_repair_replaces_range_and_clears_gap(self):
        self.assertTrue(hasattr(flows, "repair_skipped_ranges"))
        old_in_gap = (SOURCE, COINBASE_10, 1 * 10**18, 105)
        outside_gap = (SOURCE, OUTSIDE, 2 * 10**18, 99)
        repaired = (SOURCE, COINBASE_10, 12_500_000 * 10**18, 105)
        state = {"skipped_ranges_eth": [[100, 110]]}

        with patch.object(
            flows,
            "fetch_transfer_logs",
            return_value=([repaired], 0, 1),
        ) as fetch:
            transfers, unresolved, repaired_count = flows.repair_skipped_ranges(
                "eth",
                [outside_gap, old_in_gap],
                state,
                90,
                120,
            )

        fetch.assert_called_once_with("eth", 100, 110)
        self.assertEqual(transfers, [outside_gap, repaired])
        self.assertEqual(unresolved, [])
        self.assertEqual(repaired_count, 1)
        self.assertEqual(state["skipped_ranges_eth"], [])

    def test_failed_gap_repair_discards_partial_rows_and_keeps_gap(self):
        self.assertTrue(hasattr(flows, "repair_skipped_ranges"))
        outside_gap = (SOURCE, OUTSIDE, 2 * 10**18, 99)
        partial = (SOURCE, COINBASE_10, 3 * 10**18, 105)
        state = {"skipped_ranges_eth": [[100, 110]]}

        with patch.object(
            flows,
            "fetch_transfer_logs",
            return_value=([partial], 1, 2),
        ):
            transfers, unresolved, repaired_count = flows.repair_skipped_ranges(
                "eth",
                [outside_gap],
                state,
                90,
                120,
            )

        self.assertEqual(transfers, [outside_gap])
        self.assertEqual(unresolved, [[100, 110]])
        self.assertEqual(repaired_count, 0)
        self.assertEqual(state["skipped_ranges_eth"], [[100, 110]])

    def test_unresolved_history_gap_blocks_publication(self):
        self.assertTrue(hasattr(flows, "require_complete_flow_history"))

        with self.assertRaisesRegex(RuntimeError, "Ethereum.*100-110"):
            flows.require_complete_flow_history({"eth": [[100, 110]], "bera": []})

        flows.require_complete_flow_history({"eth": [], "bera": []})

    def test_rate_limit_waits_before_retrying_the_same_log_range(self):
        rate_limited = Mock(
            status_code=429,
            headers={"Retry-After": "3"},
        )
        rate_limited.json.return_value = {"error": {"message": "rate limit"}}
        success = Mock(status_code=200, headers={})
        success.json.return_value = {
            "result": [{
                "topics": [
                    flows.TRANSFER_TOPIC,
                    "0x" + "0" * 24 + SOURCE[2:],
                    "0x" + "0" * 24 + COINBASE_10[2:],
                ],
                "data": hex(10**18),
                "blockNumber": hex(100),
            }]
        }
        chain = {
            "eth": {
                "name": "Ethereum",
                "rpcs": ["https://rpc.example"],
                "chunk_size": 1_000,
                "deploy_block": 1,
            }
        }

        with patch.object(flows, "CHAINS", chain), patch.object(
            flows.requests, "post", side_effect=[rate_limited, success]
        ), patch.object(flows.time, "sleep") as sleep:
            transfers, failed, _ = flows.fetch_transfer_logs("eth", 100, 100)

        self.assertEqual(failed, 0)
        self.assertEqual(transfers, [(SOURCE, COINBASE_10, 10**18, 100)])
        sleep.assert_any_call(3.0)

    def test_dashboard_net_flow_hover_reconciles_gross_directions(self):
        source = (Path(__file__).resolve().parents[1] / "dolo-preview.html").read_text()

        self.assertIn("grossInEth", source)
        self.assertIn("grossOutEth", source)
        self.assertIn("effectiveGrossInflow", source)
        self.assertIn("effectiveGrossOutflow", source)
        self.assertIn("In ${fmtNum(grossIn)}", source)
        self.assertIn("Out ${fmtNum(grossOut)}", source)

    def test_holder_audiences_separate_verified_market_from_potential_and_bots(self):
        market = "0x1111111111111111111111111111111111111111"
        watch = "0x2222222222222222222222222222222222222222"
        bot = "0x3333333333333333333333333333333333333333"
        cex = "0x4444444444444444444444444444444444444444"
        holder_rows = {address: {} for address in (market, watch, bot, cex)}
        labels = {
            watch: {"type": "watch"},
            bot: {"type": "bot"},
            cex: {"type": "cex"},
        }
        balances = {market: 120_000, watch: 220_000, bot: 320_000, cex: 420_000}
        buckets = [{"key": "all", "min": 0, "max": float("inf")}]

        verified = flows.build_bucket_model(
            balances, {}, holder_rows, labels, buckets, audience="market"
        )
        potential = flows.build_bucket_model(
            balances, {}, holder_rows, labels, buckets, audience="potential"
        )

        self.assertEqual(verified["trackedWallets"], 1)
        self.assertEqual(verified["trackedTotal"], 120_000)
        self.assertEqual(verified["excludedPotentialWallets"], 2)
        self.assertEqual(verified["excludedPotentialTotal"], 540_000)
        self.assertEqual(potential["trackedWallets"], 2)
        self.assertEqual(potential["trackedTotal"], 540_000)
        self.assertEqual(potential["excludedCexWallets"], 1)


if __name__ == "__main__":
    unittest.main()
