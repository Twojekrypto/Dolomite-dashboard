import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_dolo_holders as holders


ALICE = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BOB = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


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

        with patch.object(holders, "rpc_batch_requests", side_effect=fake_batch), \
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


if __name__ == "__main__":
    unittest.main()
