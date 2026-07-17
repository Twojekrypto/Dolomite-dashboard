import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_dolo_holders as holders


ALICE = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BOB = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
CHARLIE = "0xcccccccccccccccccccccccccccccccccccccccc"


def uint256_hex(value):
    return "0x" + hex(int(value))[2:].zfill(64)


class GenerateDoloHoldersRpcBatchTests(unittest.TestCase):
    def test_verify_top_balances_uses_batch_results(self):
        rows = [
            {"address": ALICE, "balance": 1000, "balance_eth": 1000, "balance_bera": 0, "chains": ["eth"]},
            {"address": BOB, "balance": 0, "balance_eth": 0, "balance_bera": 0, "chains": []},
        ]
        eth_balances = {ALICE: 1000}
        bera_balances = {}
        batch_sizes = []

        def fake_batch(_rpcs, payloads, **_kwargs):
            batch_sizes.append(len(payloads))
            out = {}
            for payload in payloads:
                request_id = payload["id"]
                if request_id == "eth:0":
                    amount = 1200 * 10**18
                elif request_id == "eth:1":
                    amount = 150 * 10**18
                else:
                    amount = 0
                out[request_id] = {"jsonrpc": "2.0", "id": request_id, "result": uint256_hex(amount)}
            return out, []

        # Force the per-address fallback (multicall resolves nothing) so this
        # test exercises the JSON-RPC batch path exactly as before.
        with patch.object(holders, "_multicall_dolo_balances",
                          side_effect=lambda rpcs, addrs: ({}, list(addrs))), \
             patch.object(holders, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(holders, "rpc_single_request") as single:
            out_rows, out_eth, _out_bera = holders.verify_top_balances(
                rows,
                eth_balances,
                bera_balances,
                forced_addrs=set(),
                max_check=2,
            )

        self.assertGreaterEqual(max(batch_sizes), 2)
        single.assert_not_called()
        self.assertEqual(out_rows[0]["balance_eth"], 1200)
        self.assertEqual(out_eth[ALICE], 1200)
        self.assertEqual(out_rows[1]["balance_eth"], 150)
        self.assertIn("eth", out_rows[1]["chains"])

    def test_verify_top_balances_uses_multicall_fast_path(self):
        # When Multicall3 resolves the holders, the per-address eth_call path
        # is never touched (the compute-unit win).
        rows = [{"address": ALICE, "balance": 1000, "balance_eth": 1000,
                 "balance_bera": 0, "chains": ["eth"]}]

        def fake_multicall(rpcs, addrs):
            return {a.lower(): 1200 * 10**18 for a in addrs}, []

        with patch.object(holders, "_multicall_dolo_balances", side_effect=fake_multicall), \
             patch.object(holders, "rpc_batch_requests") as batch, \
             patch.object(holders, "rpc_single_request") as single:
            out_rows, out_eth, _ = holders.verify_top_balances(
                rows, {ALICE: 1000}, {}, forced_addrs=set(), max_check=1,
            )

        batch.assert_not_called()
        single.assert_not_called()
        self.assertEqual(out_rows[0]["balance_eth"], 1200)
        self.assertEqual(out_eth[ALICE], 1200)

    def test_verify_top_balances_falls_back_when_multicall_unresolved(self):
        # Holders Multicall3 cannot resolve fall through to the per-address path.
        rows = [{"address": ALICE, "balance": 1000, "balance_eth": 1000,
                 "balance_bera": 0, "chains": ["eth"]}]

        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            for payload in payloads:
                rid = payload["id"]
                amount = 1200 * 10**18 if rid.startswith("eth") else 0
                out[rid] = {"jsonrpc": "2.0", "id": rid, "result": uint256_hex(amount)}
            return out, []

        with patch.object(holders, "_multicall_dolo_balances",
                          side_effect=lambda rpcs, addrs: ({}, list(addrs))), \
             patch.object(holders, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(holders, "rpc_single_request") as single:
            out_rows, out_eth, _ = holders.verify_top_balances(
                rows, {ALICE: 1000}, {}, forced_addrs=set(), max_check=1,
            )

        single.assert_not_called()
        self.assertEqual(out_rows[0]["balance_eth"], 1200)

    def test_multicall_dolo_balances_without_web3_defers_to_fallback(self):
        # No web3 -> fast path is a no-op, every address returned for fallback.
        with patch.dict(sys.modules, {"web3": None}):
            resolved, unresolved = holders._multicall_dolo_balances(["https://rpc"], [ALICE, BOB])
        self.assertEqual(resolved, {})
        self.assertEqual(unresolved, [ALICE, BOB])

    def test_flow_reconciliation_candidates_read_nested_flow_addresses(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as f:
            f.write(
                '{"periods":{"7d":{"bera":{"accumulators":['
                '{"address":"%s","balance":1969.67}]}}}}' % ALICE
            )
            f.flush()

            candidates = holders.load_flow_reconciliation_candidates(f.name)

        self.assertEqual(candidates, {ALICE})

    def test_reconcile_candidate_balances_adds_missing_flow_holder(self):
        def fake_fetch(chain, addresses):
            if chain == "bera":
                return {ALICE: 1969.6699}
            return {ALICE: 0}

        with patch.object(holders, "fetch_chain_dolo_balances", side_effect=fake_fetch):
            eth_balances, bera_balances, stats = holders.reconcile_candidate_balances(
                {},
                {},
                {ALICE},
            )

        rows = holders.merge_holders(eth_balances, bera_balances, forced_addrs=set())
        self.assertEqual(stats["added"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["address"], ALICE)
        self.assertEqual(rows[0]["balance_bera"], 1969.6699)
        self.assertEqual(rows[0]["chains"], ["bera"])

    def test_detect_contracts_batches_code_and_safe_storage(self):
        singleton = "0x" + "1" * 40
        rows = [
            {"address": ALICE},
            {"address": BOB},
        ]

        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            for payload in payloads:
                request_id = payload["id"]
                method = payload["method"]
                if method == "eth_getCode":
                    result = "0x6000" if request_id.endswith(":0") else "0x"
                elif method == "eth_getStorageAt":
                    result = "0x" + ("0" * 24) + singleton[2:]
                else:
                    result = "0x"
                out[request_id] = {"jsonrpc": "2.0", "id": request_id, "result": result}
            return out, []

        with patch.object(holders, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(holders, "rpc_single_request") as single, \
             patch.object(holders, "SAFE_SINGLETON_ADDRS", {singleton}):
            out = holders.detect_contracts(rows, max_check=2)

        single.assert_not_called()
        self.assertTrue(out[0]["is_contract"])
        self.assertEqual(out[0]["contract_wallet_type"], "safe")
        self.assertNotIn("is_contract", out[1])

    def test_detect_contracts_checks_every_holder_in_chart_range(self):
        rows = [
            {"address": ALICE, "balance": 1_000},
            {"address": BOB, "balance": 999.99},
            {"address": CHARLIE, "balance": 5_000},
        ]
        code_batch_sizes = []

        def fake_batch(_rpcs, payloads, **_kwargs):
            code_batch_sizes.append(len(payloads))
            return {
                payload["id"]: {"jsonrpc": "2.0", "id": payload["id"], "result": "0x"}
                for payload in payloads
            }, []

        with patch.object(holders, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(holders, "rpc_single_request") as single:
            holders.detect_contracts(rows, max_check=None, min_balance=1_000)

        self.assertEqual(code_batch_sizes, [2, 2])
        single.assert_not_called()

    def test_holder_chart_contract_coverage_checkpoints_until_complete(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = os.path.join(tmp_dir, "dolo_holders.json")
            with open(output_path, "w") as f:
                json.dump({
                    "timestamp": "2026-07-17T00:00:00",
                    "stats": {},
                    "holders": [
                        {"address": ALICE, "balance": 1_000},
                        {"address": BOB, "balance": 2_000},
                        {"address": CHARLIE, "balance": 3_000},
                    ],
                }, f)

            with patch.object(holders, "OUTPUT_JSON", output_path), \
                 patch.object(holders, "HOLDER_CHART_CONTRACT_VERIFY_BATCH_SIZE", 2), \
                 patch.object(holders, "detect_contracts", side_effect=lambda rows, **_kwargs: rows) as detect:
                holders.verify_holder_chart_contracts_only()
                with open(output_path) as f:
                    partial = json.load(f)
                holders.verify_holder_chart_contracts_only()
                with open(output_path) as f:
                    complete = json.load(f)

        self.assertEqual([len(call.args[0]) for call in detect.call_args_list], [2, 1])
        self.assertEqual(partial["holder_chart_contract_coverage"]["status"], "in_progress")
        self.assertEqual(partial["holder_chart_contract_coverage"]["verifiedWallets"], 2)
        self.assertEqual(complete["holder_chart_contract_coverage"]["status"], "complete")
        self.assertEqual(complete["holder_chart_contract_coverage"]["verifiedWallets"], 3)


if __name__ == "__main__":
    unittest.main()
