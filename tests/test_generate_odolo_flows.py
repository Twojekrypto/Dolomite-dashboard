import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import generate_odolo_flows as odolo_flows


class GenerateOdoloFlowsTests(unittest.TestCase):
    def test_exact_latest_flow_metadata_is_optional_and_directional(self):
        wallet = "0x1111111111111111111111111111111111111111"
        peer = "0x2222222222222222222222222222222222222222"
        rows = [{"address": wallet, "net_flow": 10}]
        transfers = [(peer, wallet, 1, 99), (peer, wallet, 1, 100)]
        logs = [{
            "from": peer,
            "to": wallet,
            "transactionHash": "0x" + "c" * 64,
            "logIndex": "0x0",
        }]

        odolo_flows.attach_latest_flow_metadata(
            rows, transfers, "inbound", "berachain",
            lambda blocks: {100: {"timestamp": 1_786_406_400, "logs": logs}},
        )

        self.assertEqual(rows[0]["latest_tx_hash"], "0x" + "c" * 64)
        self.assertEqual(rows[0]["latest_tx_timestamp"], 1_786_406_400)
        self.assertEqual(rows[0]["latest_tx_chain"], "berachain")

    def test_current_block_must_be_valid_before_cutoffs(self):
        with self.assertRaises(RuntimeError):
            odolo_flows.build_cutoff_blocks(0)

        with self.assertRaises(RuntimeError):
            odolo_flows.build_cutoff_blocks(odolo_flows.DEPLOY_BLOCK - 1)

    def test_cutoff_blocks_keep_all_as_deploy_block(self):
        current_block = odolo_flows.DEPLOY_BLOCK + 2_000_000
        cutoffs = odolo_flows.build_cutoff_blocks(current_block)

        self.assertEqual(cutoffs["all"], odolo_flows.DEPLOY_BLOCK)
        self.assertGreater(cutoffs["1d"], cutoffs["7d"])
        self.assertGreater(cutoffs["7d"], cutoffs["30d"])
        self.assertGreaterEqual(cutoffs["180d"], cutoffs["all"])

    def test_future_rewards_wallet_is_not_a_top_flow_row(self):
        user = "0x1111111111111111111111111111111111111111"
        rows = odolo_flows.get_top(
            {
                odolo_flows.REWARDS_CONTRACT: 1000,
                user: 100,
            },
            tx_counts={odolo_flows.REWARDS_CONTRACT: 5, user: 1},
            n=10,
            excluded=odolo_flows.EXCLUDED_ADDRS,
        )

        self.assertEqual([row["address"] for row in rows], [user])

    def test_transfer_log_fetch_refuses_partial_chunk(self):
        response = Mock()
        response.json.return_value = {"error": {"message": "temporary rpc failure"}}

        with patch.object(odolo_flows, "RPC_URLS", ["https://rpc.example"]), \
             patch.object(odolo_flows.requests, "post", return_value=response), \
             patch.object(odolo_flows.time, "sleep"):
            with self.assertRaises(RuntimeError):
                odolo_flows.fetch_transfer_logs(100, 200)

    def test_transfer_log_fetch_includes_single_block_range(self):
        response = Mock()
        response.json.return_value = {"result": []}

        with patch.object(odolo_flows, "RPC_URLS", ["https://rpc.example"]), \
             patch.object(odolo_flows.requests, "post", return_value=response) as post, \
             patch.object(odolo_flows.time, "sleep"):
            transfers = odolo_flows.fetch_transfer_logs(100, 100)

        self.assertEqual(transfers, [])
        post.assert_called_once()
        request = post.call_args.kwargs["json"]
        self.assertEqual(request["params"][0]["fromBlock"], hex(100))
        self.assertEqual(request["params"][0]["toBlock"], hex(100))

    def test_recent_rescan_replaces_cached_range_authoritatively(self):
        old_only = ("0x" + "1" * 40, "0x" + "2" * 40, 10, 99)
        stale = ("0x" + "3" * 40, "0x" + "4" * 40, 20, 100)
        refreshed = ("0x" + "5" * 40, "0x" + "6" * 40, 30, 101)

        merged = odolo_flows.replace_transfer_range(
            [old_only, stale], [refreshed], 100, 101
        )

        self.assertEqual(merged, [old_only, refreshed])

    def test_fetch_odolo_balances_uses_multicall_fast_path(self):
        addr = "0x" + "c" * 40

        def fake_multicall(rpcs, addrs):
            return {a.lower(): 1234 * 10**18 for a in addrs}, []

        with patch.object(odolo_flows, "RPC_URLS", ["https://rpc.example"]), \
             patch.object(odolo_flows, "_multicall_odolo_balances", side_effect=fake_multicall), \
             patch.object(odolo_flows.requests, "post") as post, \
             patch.object(odolo_flows.time, "sleep"):
            balances = odolo_flows.fetch_odolo_balances([addr])

        post.assert_not_called()  # Multicall3 resolved everyone; no per-address calls
        self.assertEqual(balances[addr.lower()], 1234.0)

    def test_fetch_odolo_balances_falls_back_when_multicall_unresolved(self):
        addr = "0x" + "d" * 40
        word = "0x" + hex(50 * 10**18)[2:].zfill(64)

        def fake_post(url, json=None, **kwargs):
            resp = Mock()
            if isinstance(json, list):
                resp.json.return_value = [
                    {"jsonrpc": "2.0", "id": item["id"], "result": word} for item in json
                ]
            else:
                resp.json.return_value = {"jsonrpc": "2.0", "id": json["id"], "result": word}
            return resp

        with patch.object(odolo_flows, "RPC_URLS", ["https://rpc.example"]), \
             patch.object(odolo_flows, "_multicall_odolo_balances",
                          side_effect=lambda rpcs, addrs: ({}, list(addrs))), \
             patch.object(odolo_flows.requests, "post", side_effect=fake_post), \
             patch.object(odolo_flows.time, "sleep"):
            balances = odolo_flows.fetch_odolo_balances([addr])

        self.assertEqual(balances[addr.lower()], 50.0)

    def test_multicall_odolo_balances_without_web3_defers_to_fallback(self):
        import sys
        a, b = "0x" + "e" * 40, "0x" + "f" * 40
        with patch.dict(sys.modules, {"web3": None}):
            resolved, unresolved = odolo_flows._multicall_odolo_balances(["https://rpc"], [a, b])
        self.assertEqual(resolved, {})
        self.assertEqual(unresolved, [a, b])

    def test_detect_contracts_uses_batch_results(self):
        contract = "0x" + "a" * 40
        wallet = "0x" + "b" * 40

        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            for payload in payloads:
                request_id = payload["id"]
                result = "0x6000" if payload["params"][0] == contract else "0x"
                out[request_id] = {"jsonrpc": "2.0", "id": request_id, "result": result}
            return out, []

        with patch.object(odolo_flows, "RPC_URLS", ["https://rpc.example"]), \
             patch.object(odolo_flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(odolo_flows, "rpc_single_request") as single:
            contracts = odolo_flows.detect_contracts_batch([contract, wallet])

        single.assert_not_called()
        self.assertEqual(contracts, {contract})

    def test_existing_flow_snapshot_addresses_are_reconciliation_candidates(self):
        acc = "0x" + "1" * 40
        claimer = "0x" + "2" * 40
        payload = {
            "periods": {
                "all": {
                    "accumulators": [{"address": acc}],
                    "sellers": [],
                }
            },
            "claimer_behavior": {
                "all_claimers": [{"address": claimer}],
            },
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            candidates = odolo_flows.load_existing_flow_candidates(path)
        finally:
            os.unlink(path)

        self.assertIn(acc, candidates)
        self.assertIn(claimer, candidates)
        self.assertEqual(candidates[acc], {"previous_odolo_flows"})
        self.assertEqual(candidates[claimer], {"previous_odolo_claimers"})

    def test_reward_claim_events_self_heal_missing_transfer_claimers(self):
        wallet = "0x" + "3" * 40
        payload = {
            "token": {"symbol": "oDOLO"},
            "events": [
                {
                    "user": wallet,
                    "distributor": odolo_flows.REWARDS_CONTRACT,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "amountWei": str(42 * 10**18),
                }
            ],
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            event_claims = odolo_flows.load_reward_claims(path)
        finally:
            os.unlink(path)

        merged, stats = odolo_flows.merge_claim_sources({}, event_claims)

        self.assertEqual(merged[wallet], 42.0)
        self.assertEqual(stats["added"], 1)
        self.assertEqual(stats["updated"], 0)

    def test_official_claim_to_flow_excluded_contract_still_counts_as_claimed(self):
        wallet = "0x089b95152253b6af73e7f7267d749058d56ce231"
        self.assertIn(wallet, odolo_flows.EXCLUDED_ADDRS)
        payload = {
            "events": [
                {
                    "user": wallet,
                    "distributor": odolo_flows.REWARDS_CONTRACT,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "amountWei": str(42 * 10**18),
                }
            ],
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            event_claims = odolo_flows.load_reward_claims(path)
        finally:
            os.unlink(path)

        self.assertEqual(event_claims, {wallet: 42.0})

    def test_reward_claim_events_can_be_filtered_by_period_block(self):
        old_wallet = "0x" + "6" * 40
        recent_wallet = "0x" + "7" * 40
        payload = {
            "token": {"symbol": "oDOLO"},
            "events": [
                {
                    "user": old_wallet,
                    "distributor": odolo_flows.REWARDS_CONTRACT,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "amountWei": str(10 * 10**18),
                    "blockNumber": 100,
                },
                {
                    "user": recent_wallet,
                    "distributor": odolo_flows.REWARDS_CONTRACT,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "amountWei": str(20 * 10**18),
                    "blockNumber": 200,
                },
            ],
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            recent = odolo_flows.load_reward_claims(path, min_block=150)
        finally:
            os.unlink(path)

        self.assertEqual(recent, {recent_wallet: 20.0})

    def test_reward_claim_loader_requires_official_distributor_and_token(self):
        wallet = "0x" + "3" * 40
        payload = {
            "events": [
                {
                    "user": wallet,
                    "distributor": "0x" + "4" * 40,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "tokenSymbol": "oDOLO",
                    "amountWei": str(100 * 10**18),
                },
                {
                    "user": wallet,
                    "distributor": odolo_flows.REWARDS_CONTRACT,
                    "tokenAddress": "0x" + "5" * 40,
                    "tokenSymbol": "oDOLO",
                    "amountWei": str(200 * 10**18),
                },
                {
                    "user": wallet,
                    "distributor": odolo_flows.REWARDS_CONTRACT,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "tokenSymbol": "oDOLO",
                    "amountWei": str(30 * 10**18),
                },
            ]
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            claims = odolo_flows.load_reward_claims(path)
        finally:
            os.unlink(path)

        self.assertEqual(claims, {wallet: 30.0})

    def test_flow_components_keep_gross_outflow_separate_from_net_flow(self):
        wallet = "0x" + "8" * 40
        peer = "0x" + "9" * 40
        transfers = [
            (peer, wallet, 100 * 10**18, 1),
            (wallet, peer, 40 * 10**18, 2),
        ]

        components = odolo_flows.calculate_flow_components(transfers)

        self.assertEqual(components[wallet]["gross_inflow"], 100.0)
        self.assertEqual(components[wallet]["gross_outflow"], 40.0)
        self.assertEqual(components[wallet]["net_flow"], 60.0)

    def test_labeled_custody_contract_stays_visible_in_flows(self):
        custody = "0x" + "a" * 40
        router = "0x" + "b" * 40

        excluded = odolo_flows.select_dynamic_flow_exclusions(
            {custody, router},
            {custody: {"type": "cex"}, router: {"type": "contract"}},
        )

        self.assertNotIn(custody, excluded)
        self.assertIn(router, excluded)

    def test_merge_claim_sources_keeps_larger_total_instead_of_double_counting(self):
        wallet = "0x" + "4" * 40

        merged, stats = odolo_flows.merge_claim_sources({wallet: 100.0}, {wallet: 80.0})
        self.assertEqual(merged[wallet], 100.0)
        self.assertEqual(stats["updated"], 0)

        merged, stats = odolo_flows.merge_claim_sources({wallet: 100.0}, {wallet: 125.0})
        self.assertEqual(merged[wallet], 125.0)
        self.assertEqual(stats["updated"], 1)

    def test_canonical_claim_totals_ignore_unmatched_post_index_transfers(self):
        historical_wallet = "0x" + "1" * 40
        canonical_wallet = "0x" + "2" * 40
        transfers = [
            (
                odolo_flows.REWARDS_CONTRACT,
                historical_wallet,
                10 * 10**18,
                90,
            ),
            (
                odolo_flows.REWARDS_CONTRACT,
                canonical_wallet,
                100 * 10**18,
                110,
            ),
        ]

        claims, stats = odolo_flows.build_canonical_claim_totals(
            transfers,
            {canonical_wallet: 30.0},
            first_canonical_event_block=100,
        )

        self.assertEqual(claims, {
            historical_wallet: 10.0,
            canonical_wallet: 30.0,
        })
        self.assertEqual(stats["historical_transfer_claimed"], 10.0)
        self.assertEqual(stats["canonical_event_claimed"], 30.0)
        self.assertEqual(stats["historical_transfer_wallets"], 1)
        self.assertEqual(stats["canonical_event_wallets"], 1)
        self.assertEqual(stats["canonical_event_added"], 1)
        self.assertEqual(stats["canonical_event_updated"], 0)
        self.assertEqual(stats["ignored_post_index_transfer_count"], 1)
        self.assertEqual(stats["ignored_post_index_transfer_amount"], 70.0)

    def test_claim_coverage_uses_only_exact_canonical_events(self):
        payload = {
            "events": [
                {
                    "blockNumber": 100,
                    "user": "0x" + "1" * 40,
                    "distributor": "0x" + "4" * 40,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "amountWei": str(10 * 10**18),
                },
                {
                    "blockNumber": 200,
                    "user": "0x" + "2" * 40,
                    "distributor": odolo_flows.REWARDS_CONTRACT,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "amountWei": str(20 * 10**18),
                },
            ],
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            first_block = odolo_flows.load_first_canonical_claim_block([path])
        finally:
            os.unlink(path)

        self.assertEqual(first_block, 200)

    def test_claim_sources_deduplicate_event_ids_and_keep_disjoint_events(self):
        wallet = "0x" + "3" * 40
        event_a = {
            "txHash": "0x" + "a" * 64,
            "logIndex": 1,
            "blockNumber": 200,
            "user": wallet,
            "distributor": odolo_flows.REWARDS_CONTRACT,
            "tokenAddress": odolo_flows.ODOLO_CONTRACT,
            "amountWei": str(10 * 10**18),
        }
        event_b = {
            "txHash": "0x" + "b" * 64,
            "logIndex": 2,
            "blockNumber": 201,
            "user": wallet,
            "distributor": odolo_flows.REWARDS_CONTRACT,
            "tokenAddress": odolo_flows.ODOLO_CONTRACT,
            "amountWei": str(20 * 10**18),
        }
        paths = []
        try:
            for events in ([event_a], [event_a], [event_b]):
                with tempfile.NamedTemporaryFile("w", delete=False) as f:
                    json.dump({"events": events}, f)
                    paths.append(f.name)

            claims = odolo_flows.load_reward_claims_from_sources(paths)
        finally:
            for path in paths:
                os.unlink(path)

        self.assertEqual(claims, {wallet: 30.0})

    def test_claim_sources_ignore_events_above_snapshot_block(self):
        wallet = "0x" + "4" * 40
        events = [
            {
                "txHash": "0x" + "c" * 64,
                "logIndex": 1,
                "blockNumber": 200,
                "user": wallet,
                "distributor": odolo_flows.REWARDS_CONTRACT,
                "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                "amountWei": str(10 * 10**18),
            },
            {
                "txHash": "0x" + "d" * 64,
                "logIndex": 2,
                "blockNumber": 201,
                "user": wallet,
                "distributor": odolo_flows.REWARDS_CONTRACT,
                "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                "amountWei": str(20 * 10**18),
            },
        ]
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump({"events": events}, f)
            path = f.name
        try:
            claims = odolo_flows.load_reward_claims_from_sources(
                [path],
                max_block=200,
            )
        finally:
            os.unlink(path)

        self.assertEqual(claims, {wallet: 10.0})

    def test_canonical_claim_coverage_must_be_complete(self):
        payload = {
            "chains": {
                "berachain": {
                    "coverageStatus": "complete",
                    "fromBlock": odolo_flows.DEPLOY_BLOCK,
                    "toBlock": odolo_flows.DEPLOY_BLOCK + 1_000,
                },
            },
            "events": [],
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            coverage = odolo_flows.load_canonical_claim_coverage([path])
            self.assertEqual(coverage["to_block"], odolo_flows.DEPLOY_BLOCK + 1_000)

            payload["chains"]["berachain"]["coverageStatus"] = "partial"
            with open(path, "w") as f:
                json.dump(payload, f)
            with self.assertRaises(RuntimeError):
                odolo_flows.load_canonical_claim_coverage([path])
        finally:
            os.unlink(path)

    def test_flow_snapshot_is_capped_to_fresh_claim_coverage(self):
        confirmed_head = odolo_flows.DEPLOY_BLOCK + 10_000
        chain_head = confirmed_head + odolo_flows.REORG_BUFFER_BLOCKS

        snapshot = odolo_flows.select_flow_snapshot_block(
            chain_head,
            claim_coverage_to_block=confirmed_head - 40,
        )

        self.assertEqual(snapshot, confirmed_head - 40)

        with self.assertRaises(RuntimeError):
            odolo_flows.select_flow_snapshot_block(
                chain_head,
                claim_coverage_to_block=(
                    confirmed_head - odolo_flows.CLAIM_COVERAGE_MAX_LAG_BLOCKS - 1
                ),
            )

    def test_claimer_summary_uses_row_totals_not_rounded_percentages(self):
        rows = [
            {
                "claimed": 60.01,
                "exercised": 20.01,
                "outflow": 10.0,
                "claim_remaining": 30.0,
                "held": 45.25,
                "bought_extra": 0,
            },
            {
                "claimed": 40.03,
                "exercised": 20.0,
                "outflow": 20.02,
                "claim_remaining": 0.01,
                "held": 4.75,
                "bought_extra": 2.5,
            },
        ]

        summary = odolo_flows.summarize_claimer_rows(rows)

        self.assertEqual(summary["total_claimed"], 100.04)
        self.assertEqual(summary["total_exercised"], 40.01)
        self.assertEqual(summary["total_outflow"], 30.02)
        self.assertEqual(summary["total_claim_remaining"], 30.01)
        self.assertEqual(summary["total_held"], 50.0)
        self.assertEqual(summary["count_bought_extra"], 1)

    def test_current_holder_rows_include_self_healing_balance_candidates(self):
        wallet = "0x" + "5" * 40
        candidates = {}
        odolo_flows.add_candidate(candidates, wallet, "previous_odolo_claimers")

        rows = odolo_flows.build_current_holder_rows({wallet: 12.345}, candidates)

        self.assertEqual(rows, [{
            "address": wallet,
            "balance": 12.35,
            "sources": ["previous_odolo_claimers"],
            "rank": 1,
        }])


if __name__ == "__main__":
    unittest.main()
