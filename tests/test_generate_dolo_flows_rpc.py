import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_dolo_flows as flows


ALICE = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BOB = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def uint256_hex(value):
    return "0x" + hex(int(value))[2:].zfill(64)


class GenerateDoloFlowsRpcTests(unittest.TestCase):
    def test_detect_contracts_batch_uses_batch_results(self):
        contract = ALICE
        wallet = BOB

        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            for payload in payloads:
                request_id = payload["id"]
                result = "0x6000" if payload["params"][0] == contract else "0x"
                out[request_id] = {"jsonrpc": "2.0", "id": request_id, "result": result}
            return out, []

        with patch.object(flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(flows, "rpc_single_request") as single:
            contracts = flows.detect_contracts_batch([contract, wallet], "eth")

        single.assert_not_called()
        self.assertEqual(contracts, {contract})

    def test_fetch_dolo_balances_batches_by_chain_and_sums(self):
        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            missing = []
            for payload in payloads:
                request_id = payload["id"]
                chain, idx = request_id.split(":")
                if chain == "eth" and idx == "0":
                    amount = 100 * 10**18
                elif chain == "bera" and idx == "0":
                    amount = 25 * 10**18
                elif chain == "eth" and idx == "1":
                    amount = 7 * 10**18
                else:
                    amount = 0
                out[request_id] = {"jsonrpc": "2.0", "id": request_id, "result": uint256_hex(amount)}
            return out, missing

        with patch.object(flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(flows, "rpc_single_request") as single:
            balances, failures, failed = flows.fetch_dolo_balances([ALICE, BOB])

        single.assert_not_called()
        self.assertEqual(failures, 0)
        self.assertEqual(failed, set())
        self.assertEqual(balances[ALICE], 125)
        self.assertEqual(balances[BOB], 7)

    def test_fetch_dolo_balances_falls_back_for_missing_batch_item(self):
        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            missing = []
            for payload in payloads:
                request_id = payload["id"]
                if request_id == "eth:0":
                    missing.append(request_id)
                    continue
                out[request_id] = {"jsonrpc": "2.0", "id": request_id, "result": uint256_hex(0)}
            return out, missing

        def fake_single(_rpcs, payload, **_kwargs):
            if payload["id"] == "eth:0":
                return {"jsonrpc": "2.0", "id": payload["id"], "result": uint256_hex(42 * 10**18)}
            return {"jsonrpc": "2.0", "id": payload["id"], "result": uint256_hex(0)}

        with patch.object(flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(flows, "rpc_single_request", side_effect=fake_single):
            balances, failures, failed = flows.fetch_dolo_balances([ALICE])

        self.assertEqual(failures, 0)
        self.assertEqual(failed, set())
        self.assertEqual(balances[ALICE], 42)


if __name__ == "__main__":
    unittest.main()
