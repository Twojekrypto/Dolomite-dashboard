import os
import sys
import unittest
from unittest.mock import Mock, patch

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

        # Force the per-address fallback (multicall resolves nothing) so this
        # test exercises the JSON-RPC batch path exactly as before.
        with patch.object(flows, "_multicall_dolo_balances",
                          side_effect=lambda rpcs, addrs: ({}, list(addrs))), \
             patch.object(flows, "rpc_batch_requests", side_effect=fake_batch), \
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

        with patch.object(flows, "_multicall_dolo_balances",
                          side_effect=lambda rpcs, addrs: ({}, list(addrs))), \
             patch.object(flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(flows, "rpc_single_request", side_effect=fake_single):
            balances, failures, failed = flows.fetch_dolo_balances([ALICE])

        self.assertEqual(failures, 0)
        self.assertEqual(failed, set())
        self.assertEqual(balances[ALICE], 42)

    def test_fetch_dolo_balances_uses_multicall_fast_path(self):
        # When Multicall3 resolves every address, the per-address eth_call path
        # is never touched (the compute-unit win).
        def fake_multicall(rpcs, addrs):
            return {a.lower(): 10 * 10**18 for a in addrs}, []

        with patch.object(flows, "_multicall_dolo_balances", side_effect=fake_multicall), \
             patch.object(flows, "rpc_batch_requests") as batch, \
             patch.object(flows, "rpc_single_request") as single:
            balances, failures, failed = flows.fetch_dolo_balances([ALICE])

        batch.assert_not_called()
        single.assert_not_called()
        self.assertEqual(failures, 0)
        self.assertEqual(failed, set())
        self.assertEqual(balances[ALICE], 20)  # 10 DOLO per chain x 2 chains

    def test_fetch_dolo_balances_falls_back_when_multicall_unresolved(self):
        # Addresses Multicall3 cannot resolve fall through to the per-address path.
        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            for payload in payloads:
                rid = payload["id"]
                out[rid] = {"jsonrpc": "2.0", "id": rid, "result": uint256_hex(5 * 10**18)}
            return out, []

        with patch.object(flows, "_multicall_dolo_balances",
                          side_effect=lambda rpcs, addrs: ({}, list(addrs))), \
             patch.object(flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(flows, "rpc_single_request") as single:
            balances, failures, failed = flows.fetch_dolo_balances([ALICE])

        single.assert_not_called()
        self.assertEqual(failures, 0)
        self.assertEqual(balances[ALICE], 10)  # 5 per chain x 2 chains

    def test_multicall_dolo_balances_decodes_success_and_flags_failures(self):
        import types

        fake_results = [
            (True, (100 * 10**18).to_bytes(32, "big")),  # ALICE -> resolved
            (False, b""),                                # BOB -> unresolved (failed)
        ]

        class _Fns:
            def aggregate3(self, calls):
                self.calls = calls
                return Mock(call=Mock(return_value=fake_results))

        class _Contract:
            functions = _Fns()

        class _Eth:
            def contract(self, address=None, abi=None):
                return _Contract()

        class _W3:
            eth = _Eth()

            def __init__(self, provider):
                pass

            @staticmethod
            def HTTPProvider(url, request_kwargs=None):
                return url

            @staticmethod
            def to_checksum_address(addr):
                return addr

        # Inject a fake `web3` module so the test is hermetic (web3 is only
        # installed in CI via requirements.txt, not in every environment).
        fake_web3 = types.ModuleType("web3")
        fake_web3.Web3 = _W3
        with patch.dict(sys.modules, {"web3": fake_web3}):
            resolved, unresolved = flows._multicall_dolo_balances(["https://rpc"], [ALICE, BOB])

        self.assertEqual(resolved, {ALICE: 100 * 10**18})
        self.assertEqual(unresolved, [BOB])

    def test_multicall_dolo_balances_without_web3_defers_to_fallback(self):
        # When web3 isn't importable, the fast path is a no-op and every address
        # is returned for the per-address fallback (graceful degradation).
        with patch.dict(sys.modules, {"web3": None}):
            resolved, unresolved = flows._multicall_dolo_balances(["https://rpc"], [ALICE, BOB])
        self.assertEqual(resolved, {})
        self.assertEqual(unresolved, [ALICE, BOB])


if __name__ == "__main__":
    unittest.main()
