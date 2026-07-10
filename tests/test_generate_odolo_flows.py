import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import generate_odolo_flows as odolo_flows


class GenerateOdoloFlowsTests(unittest.TestCase):
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

    def test_reward_claim_events_can_be_filtered_by_period_block(self):
        old_wallet = "0x" + "6" * 40
        recent_wallet = "0x" + "7" * 40
        payload = {
            "token": {"symbol": "oDOLO"},
            "events": [
                {
                    "user": old_wallet,
                    "tokenAddress": odolo_flows.ODOLO_CONTRACT,
                    "amountWei": str(10 * 10**18),
                    "blockNumber": 100,
                },
                {
                    "user": recent_wallet,
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
