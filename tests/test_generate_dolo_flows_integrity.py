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
RECIPIENT = "0x3e5041d44c9ad13f661bcb49bf37e44eca973c5d"
DOLO_DEPOSIT_ROUTER = "0xf8b2c637a68cf6a17b1df9f8992eebeff63d2dff"
SHARED_DOLOMITE_MARGIN = "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d"
STRATEGIC_INVESTOR_CLAIMS = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
INVESTOR_CLAIMS = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"
EARLY_ONLY = "0x1111111111111111111111111111111111111111"
INVESTOR_ONLY = "0x2222222222222222222222222222222222222222"
OVERLAP = "0x3333333333333333333333333333333333333333"
DOLOMITE_GNOSIS_SAFE = "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4"
CHAINLINK_REWARDS_CLAIM = "0x2f41d42de3eab9e75f3d417259f24421771fb700"
ECOSYSTEM_INCENTIVES_2 = "0x06265db7ecd9c5724a97bd4909146625d2e2619c"
CROSS_CHAIN_WALLET = "0x15762db764826c219f1385c028e7e043a27e1891"
ENSO_AGGREGATOR_TRADER = "0x40e816361e9eceb4ded402def58cc77e9f097914"
AUTOMATED_TRADER = "0x7bd27a0103e48e25acdb131cc190314562171fde"
KNOWN_BERA_FLOW_SOURCE = "0x52256ef863a713ef349ae6e97a7e8f35785145de"
KNOWN_BERA_FLOW_RECIPIENT = "0xb490d2a5d857c0357a8c2ac23c30ba0e6e02f909"
KNOWN_BERA_FLOW_TX = "0xcc41fb29534dc8adb1440454087a6a738fdfbeaaa2d20880d2265edbbc8997b3"
VERIFIED_USER_SAFE = "0x4cedf88d4fdefa1e460bbed10bdfef105c62fd68"
BROWNFI_DOLO_BUSD_POOL = "0x16b3a5e95db753fe5195244fa208301e38beae2a"
KNOWN_TRADING_BOTS = {
    "0x5a6f918fcda24e9b5143f3a1b77e63df6de30f74",
    "0x6a2383cff0d46d2b7d29759f17c26fba726f3ea3",
    "0x278d858f05b94576c1e6f73285886876ff6ef8d2",
    "0xf10f81795b359f8a72682cc2a39444bf818ef4ca",
}


class GenerateDoloFlowsIntegrityTests(unittest.TestCase):
    def test_searchable_rows_keep_verified_wallets_below_leaderboard_cutoff(self):
        flows_by_address = {
            "0x1111111111111111111111111111111111111111": 100_000.0,
            "0xa3aef439e6b69125cdbfd946ab1d8a9d012e1c46": 24_678.68,
            "0x51b85c18b8a94081cd5af25cb3c4bce5750a0b19": 25_335.15,
        }
        build_rows = getattr(flows, "build_searchable_flow_rows", lambda *_: ([], []))
        accumulators, sellers = build_rows(
            flows_by_address,
            {},
            {},
            set(),
        )

        self.assertEqual(
            [row["address"] for row in accumulators],
            [
                "0x1111111111111111111111111111111111111111",
                "0x51b85c18b8a94081cd5af25cb3c4bce5750a0b19",
                "0xa3aef439e6b69125cdbfd946ab1d8a9d012e1c46",
            ],
        )
        self.assertEqual(sellers, [])

    def test_brownfi_pool_uses_the_shared_verified_lp_label(self):
        label = flows.load_address_labels()[BROWNFI_DOLO_BUSD_POOL]

        self.assertEqual(label["label"], "BrownFi DOLO/BUSD LP Pool")
        self.assertEqual(label["type"], "lp")

    def test_combined_ranking_happens_after_complete_chain_merge(self):
        target = "0x9999999999999999999999999999999999999999"
        eth = {
            f"0x{index:040x}": float(10_000 - index)
            for index in range(1, 102)
        }
        bera = {
            f"0x{index + 1_000:040x}": float(10_000 - index)
            for index in range(1, 102)
        }
        eth[target] = 9_000.0
        bera[target] = 9_000.0

        eth_top = flows.get_top(eth, {}, 100)
        bera_top = flows.get_top(bera, {}, 100)
        self.assertNotIn(target, {row["address"] for row in eth_top})
        self.assertNotIn(target, {row["address"] for row in bera_top})

        combined = flows.merge_chain_flow_maps({"eth": eth, "bera": bera})
        combined_top = flows.get_top(combined, {}, 100)

        self.assertEqual(combined_top[0]["address"], target)
        self.assertEqual(combined_top[0]["net_flow"], 18_000.0)

    def test_equal_opposing_chain_legs_cancel_before_combined_ranking(self):
        eth = {CROSS_CHAIN_WALLET: -283_000.05056654825}
        bera = {CROSS_CHAIN_WALLET: 283_000.05056654825}

        combined = flows.merge_chain_flow_maps({"eth": eth, "bera": bera})

        self.assertAlmostEqual(combined[CROSS_CHAIN_WALLET], 0)
        self.assertEqual(flows.get_top(combined, {}, 100, "accumulator"), [])
        self.assertEqual(flows.get_top(combined, {}, 100, "seller"), [])

    def test_verified_safe_holder_contract_remains_visible_in_flows(self):
        holder_rows = {
            VERIFIED_USER_SAFE: {
                "address": VERIFIED_USER_SAFE,
                "balance": 20_000_000,
                "is_contract": True,
                "contract_wallet_type": "safe",
            }
        }
        verified_user_contracts = flows.verified_user_contract_addresses(holder_rows)
        excluded = flows.select_dynamic_flow_exclusions(
            {VERIFIED_USER_SAFE},
            {},
            verified_user_contracts,
        )

        self.assertIn(VERIFIED_USER_SAFE, verified_user_contracts)
        self.assertNotIn(VERIFIED_USER_SAFE, excluded)

    def test_known_trading_bots_remain_visible_to_the_trading_bots_filter(self):
        labels = {address: {"type": "bot"} for address in KNOWN_TRADING_BOTS}
        excluded = flows.select_dynamic_flow_exclusions(
            KNOWN_TRADING_BOTS,
            labels,
            set(),
        )

        self.assertTrue(KNOWN_TRADING_BOTS.isdisjoint(flows.EXCLUDED_ADDRS))
        self.assertEqual(excluded, set())

    def test_bridge_audit_separates_canonical_and_legacy_cancellations(self):
        exact = "0x1111111111111111111111111111111111111111"
        legacy = "0x2222222222222222222222222222222222222222"
        raw = {
            "eth": {exact: 0.0, legacy: 0.0},
            "bera": {exact: -100.0, legacy: 0.0},
        }
        bridge = {
            "eth": {exact: 100.0, legacy: 75.0},
            "bera": {exact: -100.0, legacy: -75.0},
        }
        adapter_outflows = {
            "eth": {},
            "bera": {exact: 100.0},
        }

        _neutralized, audit, _cancellations = (
            flows.neutralize_raw_and_bridge_flows_with_audit(
                raw,
                bridge,
                adapter_outflows,
            )
        )

        self.assertEqual(audit["canonicalAdapter"]["addressCount"], 1)
        self.assertEqual(audit["canonicalAdapter"]["dolo"], 100.0)
        self.assertEqual(audit["legacyHeuristic"]["addressCount"], 1)
        self.assertEqual(audit["legacyHeuristic"]["dolo"], 75.0)
        self.assertEqual(audit["total"]["addressCount"], 2)
        self.assertEqual(audit["total"]["dolo"], 175.0)

    def test_wallet_receipt_remains_market_inflow_after_ethereum_protocol_deposit(self):
        amount = 283_000.05056654825
        amount_wei = int(amount * 10**18)
        transfers = [
            (SOURCE, RECIPIENT, amount_wei, 25_806_192),
            (RECIPIENT, DOLO_DEPOSIT_ROUTER, amount_wei, 25_806_203),
            (DOLO_DEPOSIT_ROUTER, SHARED_DOLOMITE_MARGIN, amount_wei, 25_806_203),
        ]
        raw = flows.calculate_flows(transfers, set())
        components = flows.calculate_flow_components(transfers)

        market, market_components = flows.neutralize_protocol_custody_transfers(
            raw,
            components,
            transfers,
            "eth",
        )

        self.assertAlmostEqual(market[RECIPIENT], amount)
        self.assertAlmostEqual(market[SOURCE], -amount)
        self.assertAlmostEqual(market_components[RECIPIENT]["gross_inflow"], amount)
        self.assertAlmostEqual(market_components[RECIPIENT]["gross_outflow"], 0)
        self.assertAlmostEqual(market_components[RECIPIENT]["protocol_deposit"], amount)
        self.assertAlmostEqual(market.get(DOLO_DEPOSIT_ROUTER, 0), 0)
        self.assertAlmostEqual(market.get(SHARED_DOLOMITE_MARGIN, 0), 0)
        self.assertAlmostEqual(sum(market.values()), 0)

    def test_berachain_protocol_deposit_uses_the_same_verified_router(self):
        amount_wei = 125_000 * 10**18
        transfers = [
            (RECIPIENT, DOLO_DEPOSIT_ROUTER, amount_wei, 25_000_000),
            (DOLO_DEPOSIT_ROUTER, SHARED_DOLOMITE_MARGIN, amount_wei, 25_000_000),
        ]
        raw = flows.calculate_flows(transfers, set())
        components = flows.calculate_flow_components(transfers)

        market, market_components = flows.neutralize_protocol_custody_transfers(
            raw,
            components,
            transfers,
            "bera",
        )

        self.assertAlmostEqual(market.get(RECIPIENT, 0), 0)
        self.assertAlmostEqual(market_components[RECIPIENT]["gross_outflow"], 0)
        self.assertAlmostEqual(market_components[RECIPIENT]["protocol_deposit"], 125_000)
        self.assertAlmostEqual(market.get(DOLO_DEPOSIT_ROUTER, 0), 0)
        self.assertAlmostEqual(market.get(SHARED_DOLOMITE_MARGIN, 0), 0)
        self.assertAlmostEqual(sum(market.values()), 0)

    def test_protocol_withdrawal_does_not_create_a_false_market_accumulator(self):
        amount_wei = 40_000 * 10**18
        transfers = [
            (SHARED_DOLOMITE_MARGIN, DOLO_DEPOSIT_ROUTER, amount_wei, 25_000_001),
            (DOLO_DEPOSIT_ROUTER, RECIPIENT, amount_wei, 25_000_001),
        ]
        raw = flows.calculate_flows(transfers, set())
        components = flows.calculate_flow_components(transfers)

        market, market_components = flows.neutralize_protocol_custody_transfers(
            raw,
            components,
            transfers,
            "eth",
        )

        self.assertAlmostEqual(market.get(RECIPIENT, 0), 0)
        self.assertAlmostEqual(market_components[RECIPIENT]["gross_inflow"], 0)
        self.assertAlmostEqual(market_components[RECIPIENT]["protocol_withdrawal"], 40_000)
        self.assertAlmostEqual(market.get(DOLO_DEPOSIT_ROUTER, 0), 0)
        self.assertAlmostEqual(market.get(SHARED_DOLOMITE_MARGIN, 0), 0)
        self.assertAlmostEqual(sum(market.values()), 0)

    def test_unknown_contract_transfer_remains_a_real_market_flow(self):
        amount_wei = 5_000 * 10**18
        transfers = [(RECIPIENT, UNLABELED_CONTRACT, amount_wei, 25_000_002)]
        raw = flows.calculate_flows(transfers, set())
        components = flows.calculate_flow_components(transfers)

        market, market_components = flows.neutralize_protocol_custody_transfers(
            raw,
            components,
            transfers,
            "eth",
        )

        self.assertEqual(market[RECIPIENT], -5_000)
        self.assertEqual(market[UNLABELED_CONTRACT], 5_000)
        self.assertEqual(market_components[RECIPIENT]["gross_outflow"], 5_000)

    def test_arbitrum_token_presence_is_not_treated_as_an_active_dolo_market(self):
        self.assertEqual(flows.DOLO_MARKET_IDS, {"eth": 16, "bera": 35})
        self.assertNotIn("arb", flows.CHAINS)

    def test_berachain_ccip_bridge_then_ethereum_outflow_counts_once(self):
        bridge_amount = 283_000.05056654825
        prior_bera_inflow = 74_597.85219008963
        other_bera_outflow = 877.0
        peer = "0x4444444444444444444444444444444444444444"
        bridge_adapter = flows.BERACHAIN_DOLO_CCIP_ADAPTER
        transfers = {
            "eth": [
                (flows.ZERO, CROSS_CHAIN_WALLET, int(bridge_amount * 10**18), 200),
                (CROSS_CHAIN_WALLET, peer, int(bridge_amount * 10**18), 201),
            ],
            "bera": [
                (peer, CROSS_CHAIN_WALLET, int(prior_bera_inflow * 10**18), 90),
                (CROSS_CHAIN_WALLET, peer, int(other_bera_outflow * 10**18), 95),
                (CROSS_CHAIN_WALLET, bridge_adapter, int(bridge_amount * 10**18), 100),
                (bridge_adapter, flows.ZERO, int(bridge_amount * 10**18), 100),
            ],
        }
        raw_flows = {
            chain: flows.calculate_flows(rows, flows.EXCLUDED_ADDRS)
            for chain, rows in transfers.items()
        }
        bridge_flows = {
            chain: flows.calculate_bridge_flows(rows)
            for chain, rows in transfers.items()
        }
        adapter_outflows = {
            chain: flows.calculate_bridge_adapter_outflows(rows)
            for chain, rows in transfers.items()
        }

        neutralized = flows.neutralize_raw_and_bridge_flows(
            raw_flows,
            bridge_flows,
            adapter_outflows,
        )

        self.assertAlmostEqual(neutralized["eth"][CROSS_CHAIN_WALLET], -bridge_amount)
        self.assertAlmostEqual(
            neutralized["bera"][CROSS_CHAIN_WALLET],
            prior_bera_inflow - other_bera_outflow,
        )
        self.assertAlmostEqual(
            sum(chain[CROSS_CHAIN_WALLET] for chain in neutralized.values()),
            prior_bera_inflow - other_bera_outflow - bridge_amount,
        )

    def test_gross_outflow_excludes_the_neutralized_source_bridge_leg(self):
        bridge_amount = 283_000.05056654825
        other_bera_outflow = 877.0
        peer = "0x4444444444444444444444444444444444444444"
        bridge_adapter = flows.BERACHAIN_DOLO_CCIP_ADAPTER

        transfers = {
            "eth": [
                (flows.ZERO, CROSS_CHAIN_WALLET, int(bridge_amount * 10**18), 200),
            ],
            "bera": [
                (CROSS_CHAIN_WALLET, bridge_adapter, int(bridge_amount * 10**18), 100),
                (bridge_adapter, flows.ZERO, int(bridge_amount * 10**18), 100),
                (CROSS_CHAIN_WALLET, peer, int(other_bera_outflow * 10**18), 101),
            ],
        }
        raw_flows = {
            chain: flows.calculate_flows(rows, flows.EXCLUDED_ADDRS)
            for chain, rows in transfers.items()
        }
        bridge_flows = {
            chain: flows.calculate_bridge_flows(rows)
            for chain, rows in transfers.items()
        }
        adapter_outflows = {
            chain: flows.calculate_bridge_adapter_outflows(rows)
            for chain, rows in transfers.items()
        }
        _, _, _, cancellations = flows.neutralize_raw_and_bridge_flows_with_stats(
            raw_flows,
            bridge_flows,
            adapter_outflows,
        )
        components = flows.apply_bridge_outflow_cancellations(
            {
                chain: flows.calculate_flow_components(rows)
                for chain, rows in transfers.items()
            },
            cancellations,
        )

        self.assertAlmostEqual(
            components["bera"][CROSS_CHAIN_WALLET]["gross_outflow"],
            other_bera_outflow,
        )

    def test_bridge_to_a_different_recipient_remains_a_source_wallet_outflow(self):
        amount = 251_764.0
        source_wallet = "0x70c69520eb6595d102bfd8aed8fc58428489c4e4"
        destination_wallet = "0x26c2448c0038874f68cc0d388d96f8d218af3bdf"
        bridge_adapter = flows.BERACHAIN_DOLO_CCIP_ADAPTER
        transfers = {
            "eth": [
                (flows.ZERO, destination_wallet, int(amount * 10**18), 200),
            ],
            "bera": [
                (source_wallet, bridge_adapter, int(amount * 10**18), 100),
                (bridge_adapter, flows.ZERO, int(amount * 10**18), 100),
            ],
        }
        raw_flows = {
            chain: flows.calculate_flows(rows, flows.EXCLUDED_ADDRS)
            for chain, rows in transfers.items()
        }
        bridge_flows = {
            chain: flows.calculate_bridge_flows(rows)
            for chain, rows in transfers.items()
        }
        adapter_outflows = {
            chain: flows.calculate_bridge_adapter_outflows(rows)
            for chain, rows in transfers.items()
        }

        neutralized = flows.neutralize_raw_and_bridge_flows(
            raw_flows,
            bridge_flows,
            adapter_outflows,
        )

        self.assertAlmostEqual(neutralized["bera"][source_wallet], -amount)

    def test_holder_audience_includes_team_and_investors_in_balance_ranges(self):
        balances = {
            EARLY_ONLY: 2_000_000,
            INVESTOR_ONLY: 700_000,
            OUTSIDE: 200_000,
        }
        labels = {
            EARLY_ONLY: {"label": "Core Team 1", "type": "protocol"},
            INVESTOR_ONLY: {"label": "Long-term Investor", "type": "investor"},
        }
        model = flows.build_bucket_model(
            balances, {}, {}, labels, flows.HOLDER_BUCKET_GROUPS["whales"],
            include_allocations=True, audience="holders",
        )

        self.assertEqual(model["trackedWallets"], 3)
        self.assertEqual(model["allocationWallets"], 2)
        self.assertEqual(model["teamWallets"], 1)
        self.assertEqual(model["investorWallets"], 1)
        self.assertEqual(model["buckets"][0]["teamWallets"], 1)
        self.assertEqual(model["buckets"][1]["investorWallets"], 1)

    def test_exact_latest_flow_metadata_uses_direction_and_last_log_index(self):
        wallet = "0x1111111111111111111111111111111111111111"
        peer = "0x2222222222222222222222222222222222222222"
        rows = [{"address": wallet, "net_flow": 10}]
        transfers = [
            (peer, wallet, 1, 100),
            (wallet, peer, 1, 101),
            (peer, wallet, 1, 102),
        ]
        logs = [
            {"from": peer, "to": wallet, "transactionHash": "0x" + "a" * 64, "logIndex": "0x1"},
            {"from": peer, "to": wallet, "transactionHash": "0x" + "b" * 64, "logIndex": "0x2"},
        ]

        flows.attach_latest_flow_metadata(
            rows, transfers, "inbound", "ethereum",
            lambda blocks: {102: {"timestamp": 1_786_406_400, "logs": logs}},
        )

        self.assertEqual(rows[0]["latest_tx_hash"], "0x" + "b" * 64)
        self.assertEqual(rows[0]["latest_tx_timestamp"], 1_786_406_400)
        self.assertEqual(rows[0]["latest_tx_chain"], "ethereum")

    def test_incomplete_latest_flow_evidence_fails_closed(self):
        wallet = "0x1111111111111111111111111111111111111111"
        peer = "0x2222222222222222222222222222222222222222"
        rows = [{"address": wallet, "net_flow": -10}]
        transfers = [(wallet, peer, 1, 100)]

        flows.attach_latest_flow_metadata(
            rows, transfers, "outbound", "berachain",
            lambda blocks: {100: {"timestamp": 0, "logs": []}},
        )

        self.assertNotIn("latest_tx_hash", rows[0])
        self.assertNotIn("latest_tx_timestamp", rows[0])
        self.assertNotIn("latest_tx_chain", rows[0])

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
                (STRATEGIC_INVESTOR_CLAIMS, DOLOMITE_GNOSIS_SAFE, 166_667 * 10**18, 204),
                (INVESTOR_CLAIMS, DOLOMITE_GNOSIS_SAFE, 806_667 * 10**18, 205),
            ],
        })

        self.assertEqual(payload["schemaVersion"], 3)
        self.assertEqual(payload["strategic_investors"], [EARLY_ONLY, OVERLAP])
        self.assertEqual(payload["early_investors"], [EARLY_ONLY, OVERLAP])
        self.assertEqual(payload["investors"], [INVESTOR_ONLY, OVERLAP])
        self.assertEqual(payload["team"], [])
        self.assertNotIn(OUTSIDE, payload["early_investors"])
        self.assertNotIn(DOLOMITE_GNOSIS_SAFE, payload["early_investors"])
        self.assertNotIn(DOLOMITE_GNOSIS_SAFE, payload["investors"])

        records = {row["address"]: row for row in payload["wallets"]}
        self.assertNotIn(DOLOMITE_GNOSIS_SAFE, records)
        self.assertEqual(records[EARLY_ONLY]["label"], "Strategic Investor")
        self.assertEqual(records[EARLY_ONLY]["sourceChains"], ["bera"])
        self.assertEqual(
            records[EARLY_ONLY]["roundAttribution"],
            {
                "key": "2024-strategic-900k",
                "label": "2024 strategic round · $900K",
                "status": "high-confidence-onchain-attribution",
            },
        )
        self.assertIsNone(records[EARLY_ONLY]["vestingSchedule"])
        self.assertEqual(records[INVESTOR_ONLY]["label"], "Long-term Investor")
        self.assertEqual(
            records[INVESTOR_ONLY]["vestingSchedule"],
            "3-year vesting · 1-year cliff",
        )
        self.assertEqual(records[OVERLAP]["label"], "Strategic Investor")
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
        self.assertEqual(payload["methodology"]["overlapPriority"], "strategic-investor")

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

    def test_berachain_authoritative_rescan_uses_bounded_verified_overlap(self):
        last_block = 25_238_666
        cutoff = flows.CHAINS["bera"]["deploy_block"]

        refresh_start = flows.incremental_refresh_start(
            last_block,
            cutoff,
            flows.RECENT_RESCAN_BLOCKS["bera"],
        )

        self.assertEqual(refresh_start, last_block + 1 - 100_000)

    def test_failed_quorum_refresh_cannot_replace_active_cache(self):
        cached = [(SOURCE, COINBASE_10, 10**18, 100)]
        partial = [(SOURCE, OUTSIDE, 2 * 10**18, 100)]

        with self.assertRaises(flows.TransferLogQuorumError):
            flows.merge_verified_transfer_scan(
                cached,
                partial,
                100,
                110,
                failed_chunks=1,
            )

        self.assertEqual(cached, [(SOURCE, COINBASE_10, 10**18, 100)])

    def test_verified_scan_staging_resumes_without_touching_active_cache(self):
        state = {
            "bera_transfers": [[SOURCE, COINBASE_10, 10**18, 90]],
            "bera_last_block": 90,
            "verified_scan_staging": {
                "bera": {
                    "startBlock": 100,
                    "endBlock": 200,
                    "nextBlock": 151,
                    "transfers": [[SOURCE, OUTSIDE, 2 * 10**18, 120]],
                    "verification": "independent-rpc-exact-quorum",
                }
            },
        }

        transfers, next_block = flows.load_verified_scan_staging(
            state, "bera", 100, 200
        )

        self.assertEqual(transfers, [(SOURCE, OUTSIDE, 2 * 10**18, 120)])
        self.assertEqual(next_block, 151)
        self.assertEqual(state["bera_last_block"], 90)
        self.assertEqual(state["bera_transfers"], [[SOURCE, COINBASE_10, 10**18, 90]])

    def test_verified_scan_staging_resumes_when_chain_tip_advances(self):
        state = {
            "verified_scan_staging": {
                "eth": {
                    "startBlock": 100,
                    "endBlock": 200,
                    "nextBlock": 151,
                    "transfers": [[SOURCE, OUTSIDE, 2 * 10**18, 120]],
                    "verification": "independent-rpc-exact-quorum",
                }
            },
        }

        transfers, next_block = flows.load_verified_scan_staging(
            state, "eth", 100, 250
        )

        self.assertEqual(transfers, [(SOURCE, OUTSIDE, 2 * 10**18, 120)])
        self.assertEqual(next_block, 151)

    def test_completed_verified_chain_can_resume_during_full_backfill(self):
        deploy_block = flows.CHAINS["eth"]["deploy_block"]
        last_block = deploy_block + 1_000
        state = {
            "flow_log_integrity": {
                "version": flows.FLOW_LOG_INTEGRITY_VERSION,
                "status": "building",
                "verification": "independent-rpc-exact-quorum",
                "unresolvedGapCount": 0,
                "chains": {
                    "eth": {
                        "deployBlock": deploy_block,
                        "coverageStartBlock": deploy_block,
                        "verifiedThroughBlock": last_block,
                        "lastPublishedBlock": last_block,
                        "verification": "independent-rpc-exact-quorum",
                        "lastVerificationProof": {
                            "minimumMatchingProviderFamilies": 2,
                        },
                    },
                },
            },
            "eth_history_start_block": deploy_block,
            "eth_last_block": last_block,
            "eth_transfers": [[SOURCE, OUTSIDE, 2 * 10**18, deploy_block + 10]],
        }

        resumable = flows.completed_verified_backfill_chains(state)

        self.assertEqual(resumable, {"eth"})

    def test_incomplete_or_unproven_chain_is_not_resumed_as_verified(self):
        deploy_block = flows.CHAINS["eth"]["deploy_block"]
        state = {
            "flow_log_integrity": {
                "version": flows.FLOW_LOG_INTEGRITY_VERSION,
                "status": "building",
                "verification": "independent-rpc-exact-quorum",
                "unresolvedGapCount": 0,
                "chains": {
                    "eth": {
                        "deployBlock": deploy_block,
                        "coverageStartBlock": deploy_block,
                        "verifiedThroughBlock": deploy_block + 1_000,
                        "lastPublishedBlock": deploy_block + 1_000,
                        "verification": "independent-rpc-exact-quorum",
                        "lastVerificationProof": {
                            "minimumMatchingProviderFamilies": 1,
                        },
                    },
                },
            },
            "eth_history_start_block": deploy_block,
            "eth_last_block": deploy_block + 1_000,
            "eth_transfers": [[SOURCE, OUTSIDE, 2 * 10**18, deploy_block + 10]],
        }

        self.assertEqual(flows.completed_verified_backfill_chains(state), set())

    def test_failed_quorum_checkpoints_all_verified_chunks_before_stopping(self):
        state = {}
        log = {
            "address": flows.DOLO_CONTRACT,
            "topics": [
                flows.TRANSFER_TOPIC,
                "0x" + "0" * 24 + SOURCE[2:],
                "0x" + "0" * 24 + OUTSIDE[2:],
            ],
            "data": hex(2 * 10**18),
            "blockNumber": hex(100),
            "transactionHash": "0x" + "a" * 64,
            "logIndex": "0x0",
        }
        chain = {
            "eth": {
                "name": "Ethereum",
                "rpcs": ["https://rpc-one.example", "https://rpc-two.example"],
                "chunk_size": 1,
                "deploy_block": 1,
            }
        }

        with patch.object(flows, "CHAINS", chain), patch.object(
            flows,
            "_request_transfer_logs",
            side_effect=[[log], [log], None, None],
        ), patch.object(flows, "save_state"), patch.object(flows.time, "sleep"):
            _transfers, failed, _attempted = flows.fetch_transfer_logs(
                "eth", 100, 101, state=state
            )

        self.assertEqual(failed, 1)
        staged = state["verified_scan_staging"]["eth"]
        self.assertEqual(staged["nextBlock"], 101)
        self.assertEqual(staged["transfers"], [[SOURCE, OUTSIDE, 2 * 10**18, 100]])

    def test_verified_coverage_requires_full_baseline_then_advances_incrementally(self):
        state = {}

        with self.assertRaises(flows.TransferLogQuorumError):
            flows.mark_verified_chain_coverage(
                state, "bera", 25_000_000, 25_100_000, full_baseline=False
            )

        flows.mark_verified_chain_coverage(
            state,
            "bera",
            flows.CHAINS["bera"]["deploy_block"],
            25_000_000,
            full_baseline=True,
        )
        flows.mark_verified_chain_coverage(
            state, "bera", 24_900_000, 25_100_000, full_baseline=False
        )

        coverage = state["flow_log_integrity"]["chains"]["bera"]
        self.assertEqual(coverage["coverageStartBlock"], flows.CHAINS["bera"]["deploy_block"])
        self.assertEqual(coverage["verifiedThroughBlock"], 25_100_000)
        self.assertEqual(coverage["verification"], "independent-rpc-exact-quorum")
        self.assertFalse(flows.has_complete_verified_baseline(state))

        flows.mark_verified_chain_coverage(
            state,
            "eth",
            flows.CHAINS["eth"]["deploy_block"],
            25_500_000,
            full_baseline=True,
        )
        self.assertTrue(flows.has_complete_verified_baseline(state))

    def test_flow_workflow_exposes_explicit_full_verified_backfill(self):
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "update-dolo-flows.yml"
        ).read_text()

        self.assertIn("full_verified_backfill:", workflow)
        self.assertIn("DOLO_FLOWS_FULL_VERIFIED_BACKFILL:", workflow)

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

    def test_contract_detection_candidates_include_balanced_custody_intermediary(self):
        amount_wei = 1_041_767 * 10**18
        transfers = [
            (SOURCE, ENSO_AGGREGATOR_TRADER, amount_wei, 100),
            (ENSO_AGGREGATOR_TRADER, SHARED_DOLOMITE_MARGIN, amount_wei, 100),
        ]
        raw_flows = flows.calculate_flows(transfers, set())

        candidates = flows.contract_detection_candidates(
            transfers,
            "eth",
            raw_flows,
            top_n=1,
        )

        self.assertAlmostEqual(raw_flows.get(ENSO_AGGREGATOR_TRADER, 0), 0)
        self.assertIn(ENSO_AGGREGATOR_TRADER, candidates)

    def test_verified_enso_aggregator_is_always_excluded_from_market_flows(self):
        self.assertIn(ENSO_AGGREGATOR_TRADER, flows.EXCLUDED_ADDRS)

        amount_wei = 1_041_767 * 10**18
        transfers = [
            (SOURCE, ENSO_AGGREGATOR_TRADER, amount_wei, 100),
            (ENSO_AGGREGATOR_TRADER, SHARED_DOLOMITE_MARGIN, amount_wei, 100),
        ]
        raw = flows.calculate_flows(transfers, flows.EXCLUDED_ADDRS)
        components = flows.calculate_flow_components(transfers)
        market, _ = flows.neutralize_protocol_custody_transfers(
            raw,
            components,
            transfers,
            "eth",
        )

        rows = (
            flows.get_top(market, {}, 10, "accumulator", flows.EXCLUDED_ADDRS)
            + flows.get_top(market, {}, 10, "seller", flows.EXCLUDED_ADDRS)
        )
        self.assertNotIn(ENSO_AGGREGATOR_TRADER, {row["address"] for row in rows})

    def test_protocol_transfer_keeps_both_visible_flow_sides(self):
        excluded = flows.select_dynamic_flow_exclusions(
            {CHAINLINK_REWARDS_CLAIM, ECOSYSTEM_INCENTIVES_2},
            {
                CHAINLINK_REWARDS_CLAIM: {
                    "label": "Chainlink Rewards Claim",
                    "type": "protocol",
                },
                ECOSYSTEM_INCENTIVES_2: {
                    "label": "Ecosystem Incentives 2",
                    "type": "protocol",
                },
            },
        )
        net = flows.calculate_flows(
            [(
                CHAINLINK_REWARDS_CLAIM,
                ECOSYSTEM_INCENTIVES_2,
                2_213_363 * 10**18,
                100,
            )],
            excluded,
        )

        sellers = flows.get_top(net, {}, 10, "seller", excluded)
        accumulators = flows.get_top(net, {}, 10, "accumulator", excluded)

        self.assertEqual(
            [row["address"] for row in sellers],
            [CHAINLINK_REWARDS_CLAIM],
        )
        self.assertEqual(
            [row["address"] for row in accumulators],
            [ECOSYSTEM_INCENTIVES_2],
        )

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
                "address": flows.DOLO_CONTRACT,
                "topics": [
                    flows.TRANSFER_TOPIC,
                    "0x" + "0" * 24 + SOURCE[2:],
                    "0x" + "0" * 24 + COINBASE_10[2:],
                ],
                "data": hex(10**18),
                "blockNumber": hex(100),
                "transactionHash": "0x" + "a" * 64,
                "logIndex": "0x0",
            }]
        }
        chain = {
            "eth": {
                "name": "Ethereum",
                "rpcs": ["https://rpc-one.example", "https://rpc-two.example"],
                "chunk_size": 1_000,
                "deploy_block": 1,
            }
        }

        with patch.object(flows, "CHAINS", chain), patch.object(
            flows.requests, "post", side_effect=[rate_limited, success, success]
        ), patch.object(flows.time, "sleep") as sleep:
            transfers, failed, _ = flows.fetch_transfer_logs("eth", 100, 100)

        self.assertEqual(failed, 0)
        self.assertEqual(transfers, [(SOURCE, COINBASE_10, 10**18, 100)])
        sleep.assert_any_call(3.0)

    def test_fetch_transfer_logs_uses_independent_quorum_over_silent_empty_rpc(self):
        log = {
            "address": flows.DOLO_CONTRACT,
            "topics": [
                flows.TRANSFER_TOPIC,
                "0x" + "0" * 24 + KNOWN_BERA_FLOW_SOURCE[2:],
                "0x" + "0" * 24 + KNOWN_BERA_FLOW_RECIPIENT[2:],
            ],
            "data": hex(21_100 * 10**18),
            "blockNumber": hex(24_990_784),
            "transactionHash": KNOWN_BERA_FLOW_TX,
            "logIndex": "0x0",
        }

        def response(logs):
            result = Mock(status_code=200, headers={})
            result.json.return_value = {"result": logs}
            return result

        chain = {
            "bera": {
                "name": "Berachain",
                "rpcs": [
                    "https://berachain-rpc.publicnode.com/",
                    "https://rpc.berachain.com/",
                    "https://berachain.drpc.org/",
                ],
                "chunk_size": 1_000,
                "deploy_block": 1,
            }
        }
        responses = {
            "publicnode.com": response([]),
            "berachain.com": response([log]),
            "drpc.org": response([log]),
        }

        with patch.object(flows, "CHAINS", chain), patch.object(
            flows.requests,
            "post",
            side_effect=lambda url, **_kwargs: responses[flows.rpc_provider_family(url)],
        ), patch.object(flows.time, "sleep"):
            transfers, failed, _ = flows.fetch_transfer_logs(
                "bera", 24_990_784, 24_990_784
            )

        self.assertEqual(failed, 0)
        self.assertEqual(
            transfers,
            [(KNOWN_BERA_FLOW_SOURCE, KNOWN_BERA_FLOW_RECIPIENT, 21_100 * 10**18, 24_990_784)],
        )

    def test_fetch_transfer_logs_regrows_reduced_chunks_only_after_stable_successes(self):
        requested_ranges = []
        chain = {
            "bera": {
                "name": "Berachain",
                "rpcs": ["https://rpc-one.example", "https://rpc-two.example"],
                "chunk_size": 2_000,
                "deploy_block": 1,
            }
        }

        def request_logs(_endpoint, _cfg, start_block, end_block):
            requested_ranges.append((start_block, end_block))
            if (start_block, end_block) == (100, 2_099):
                return None
            return [{
                "address": flows.DOLO_CONTRACT,
                "topics": [
                    flows.TRANSFER_TOPIC,
                    "0x" + "0" * 24 + SOURCE[2:],
                    "0x" + "0" * 24 + OUTSIDE[2:],
                ],
                "data": hex(10**18),
                "blockNumber": hex(start_block),
                "transactionHash": "0x" + f"{start_block:064x}",
                "logIndex": "0x0",
            }]

        with patch.object(flows, "CHAINS", chain), patch.object(
            flows, "_request_transfer_logs", side_effect=request_logs
        ), patch.object(
            flows, "RPC_LOG_CHUNK_REGROW_SUCCESS_THRESHOLD", 3
        ), patch.object(flows.time, "sleep"):
            _transfers, failed, _ = flows.fetch_transfer_logs("bera", 100, 5_099)

        self.assertEqual(failed, 0)
        self.assertEqual(
            list(dict.fromkeys(requested_ranges)),
            [
                (100, 2_099),
                (100, 1_099),
                (1_100, 2_099),
                (2_100, 3_099),
                (3_100, 5_099),
            ],
        )

    def test_dashboard_net_flow_hover_reconciles_gross_directions(self):
        source = (Path(__file__).resolve().parents[1] / "dolo-preview.html").read_text()

        self.assertIn("grossInEth", source)
        self.assertIn("grossOutEth", source)
        self.assertIn("effectiveGrossInflow", source)
        self.assertIn("effectiveGrossOutflow", source)
        self.assertIn("In ${fmtNum(grossIn)}", source)
        self.assertIn("Out ${fmtNum(grossOut)}", source)

    def test_dashboard_flow_hover_explains_protocol_custody_adjustments(self):
        source = (Path(__file__).resolve().parents[1] / "dolo-preview.html").read_text()

        self.assertIn("protocolDepositEth", source)
        self.assertIn("protocolWithdrawalBera", source)
        self.assertIn("Deposited to Dolomite", source)
        self.assertIn("Withdrawn from Dolomite", source)

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

    def test_behavioral_trader_override_is_loaded_as_a_bot(self):
        labels = flows.load_address_labels()

        self.assertEqual(labels[AUTOMATED_TRADER]["type"], "bot")
        self.assertEqual(
            flows.holder_distribution_type(AUTOMATED_TRADER, {}, labels),
            "bot",
        )


if __name__ == "__main__":
    unittest.main()
