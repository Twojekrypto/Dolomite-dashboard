import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_dolo_flows as flows
from rpc_client import PUBLIC_ENDPOINTS


ALICE = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BOB = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def uint256_hex(value):
    return "0x" + hex(int(value))[2:].zfill(64)


def transfer_log(block, tx_hash, log_index, amount=1):
    return {
        "address": flows.DOLO_CONTRACT,
        "blockNumber": hex(block),
        "transactionHash": tx_hash,
        "logIndex": hex(log_index),
        "topics": [
            flows.TRANSFER_TOPIC,
            "0x" + "0" * 24 + ALICE[2:],
            "0x" + "0" * 24 + BOB[2:],
        ],
        "data": uint256_hex(amount),
    }


class GenerateDoloFlowsRpcTests(unittest.TestCase):
    def test_ethereum_public_fallbacks_include_two_archive_capable_families(self):
        endpoints = PUBLIC_ENDPOINTS["ethereum"]

        self.assertIn("https://eth.drpc.org/", endpoints)
        self.assertIn("https://mainnet.gateway.tenderly.co", endpoints)
        self.assertLess(
            endpoints.index("https://mainnet.gateway.tenderly.co"),
            endpoints.index("https://ethereum-rpc.publicnode.com/"),
        )

    def test_rpc_provider_family_does_not_count_two_alchemy_keys_twice(self):
        self.assertEqual(
            flows.rpc_provider_family("https://eth-mainnet.g.alchemy.com/v2/key-one"),
            flows.rpc_provider_family("https://eth-mainnet.g.alchemy.com/v2/key-two"),
        )
        self.assertNotEqual(
            flows.rpc_provider_family("https://rpc.berachain.com/"),
            flows.rpc_provider_family("https://berachain.drpc.org/"),
        )

    def test_transfer_log_digest_is_order_and_hex_format_independent(self):
        first = transfer_log(100, "0x" + "1" * 64, 0, amount=7)
        second = transfer_log(101, "0x" + "2" * 64, 1, amount=9)
        equivalent_first = dict(first, blockNumber="0x064", logIndex="0x00")

        self.assertEqual(
            flows.transfer_log_digest([first, second]),
            flows.transfer_log_digest([second, equivalent_first]),
        )

    def test_log_quorum_rejects_silent_empty_provider_and_duplicate_vendor_votes(self):
        expected = [transfer_log(100, "0x" + "1" * 64, 0, amount=21_100 * 10**18)]
        results = [
            ("https://bera-mainnet.g.alchemy.com/v2/key-one", expected),
            ("https://bera-mainnet.g.alchemy.com/v2/key-two", expected),
            ("https://berachain-rpc.publicnode.com/", []),
        ]

        with self.assertRaises(flows.TransferLogQuorumError):
            flows.select_transfer_log_quorum(results)

        selected, proof = flows.select_transfer_log_quorum([
            *results,
            ("https://berachain.drpc.org/", expected),
        ])

        self.assertEqual(selected, expected)
        self.assertEqual(proof["matchingProviderFamilies"], 2)
        self.assertEqual(proof["logCount"], 1)
        self.assertIn("publicnode.com", proof["disagreeingProviderFamilies"])

    def test_fetch_dolomite_dolo_balances_aggregates_subaccounts_and_chains(self):
        class Response:
            status_code = 200

            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        payloads = {
            "https://subgraph.example/eth": {
                "data": {
                    "_meta": {"block": {"number": 123, "timestamp": 1_700_000_000}},
                    "interestIndexes": [{
                        "id": flows.DOLO_CONTRACT,
                        "supplyIndex": "1.25",
                        "token": {"id": flows.DOLO_CONTRACT, "symbol": "DOLO", "marketId": "16"},
                    }],
                    "marginAccountTokenValues": [
                        {"valuePar": "10", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
                        {"valuePar": "5", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
                        {"valuePar": "3", "marginAccount": {"effectiveUser": {"id": BOB}, "user": {"id": BOB}}},
                    ],
                }
            },
            "https://subgraph.example/bera": {
                "data": {
                    "_meta": {"block": {"number": 456, "timestamp": 1_700_000_100}},
                    "interestIndexes": [{
                        "id": flows.DOLO_CONTRACT,
                        "supplyIndex": "1",
                        "token": {"id": flows.DOLO_CONTRACT, "symbol": "DOLO", "marketId": "35"},
                    }],
                    "marginAccountTokenValues": [
                        {"valuePar": "2", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
                    ],
                }
            },
        }

        def request(url, **_kwargs):
            return Response(payloads[url])

        subgraphs = {
            "eth": {"name": "Ethereum", "url": "https://subgraph.example/eth"},
            "bera": {"name": "Berachain", "url": "https://subgraph.example/bera"},
        }
        balances, metadata = flows.fetch_dolomite_dolo_balances(
            [ALICE, BOB], request_fn=request, subgraphs=subgraphs,
            attempts=1, now_ts=1_700_000_200,
        )

        self.assertEqual(balances[ALICE]["eth"], 18.75)
        self.assertEqual(balances[ALICE]["bera"], 2)
        self.assertEqual(balances[ALICE]["total"], 20.75)
        self.assertEqual(balances[BOB]["total"], 3.75)
        self.assertEqual(metadata["status"], "complete")
        self.assertEqual(metadata["chains"]["eth"]["blockNumber"], 123)

    def test_fetch_dolomite_dolo_balances_without_wallet_filter_returns_every_positive_owner(self):
        class Response:
            status_code = 200

            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        def payload(block_number, timestamp, supply_index, rows):
            return {
                "data": {
                    "_meta": {"block": {"number": block_number, "timestamp": timestamp}},
                    "interestIndexes": [{
                        "id": flows.DOLO_CONTRACT,
                        "supplyIndex": supply_index,
                        "token": {"id": flows.DOLO_CONTRACT, "symbol": "DOLO", "marketId": "16"},
                    }],
                    "marginAccountTokenValues": rows,
                }
            }

        payloads = {
            "https://subgraph.example/eth": payload(123, 1_700_000_000, "1.25", [
                {"valuePar": "10", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
                {"valuePar": "3", "marginAccount": {"effectiveUser": {"id": BOB}, "user": {"id": BOB}}},
            ]),
            "https://subgraph.example/bera": payload(456, 1_700_000_100, "1", [
                {"valuePar": "2", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
            ]),
        }

        def request(url, **_kwargs):
            return Response(payloads[url])

        subgraphs = {
            "eth": {"name": "Ethereum", "url": "https://subgraph.example/eth"},
            "bera": {"name": "Berachain", "url": "https://subgraph.example/bera"},
        }
        balances, metadata = flows.fetch_dolomite_dolo_balances(
            None, request_fn=request, subgraphs=subgraphs,
            attempts=1, now_ts=1_700_000_200,
        )

        self.assertEqual(set(balances), {ALICE, BOB})
        self.assertEqual(balances[ALICE], {"eth": 12.5, "bera": 2.0, "total": 14.5})
        self.assertEqual(balances[BOB], {"eth": 3.75, "bera": 0.0, "total": 3.75})
        self.assertEqual(metadata["scope"], "all-positive-effective-users")
        self.assertEqual(
            metadata["chains"]["eth"]["custodyAddress"],
            flows.DOLOMITE_MARGIN_ADDRS["eth"],
        )

    def test_fetch_dolomite_dolo_balances_does_not_publish_partial_totals(self):
        class Response:
            status_code = 200

            def json(self):
                return {
                    "data": {
                        "_meta": {"block": {"number": 123, "timestamp": 1_700_000_000}},
                        "interestIndexes": [{
                            "id": flows.DOLO_CONTRACT,
                            "supplyIndex": "1",
                            "token": {"id": flows.DOLO_CONTRACT, "symbol": "DOLO", "marketId": "16"},
                        }],
                        "marginAccountTokenValues": [
                            {"valuePar": "10", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
                        ],
                    }
                }

        def request(url, **_kwargs):
            if url.endswith("/bera"):
                raise OSError("subgraph unavailable")
            return Response()

        subgraphs = {
            "eth": {"name": "Ethereum", "url": "https://subgraph.example/eth"},
            "bera": {"name": "Berachain", "url": "https://subgraph.example/bera"},
        }
        balances, metadata = flows.fetch_dolomite_dolo_balances(
            [ALICE], request_fn=request, subgraphs=subgraphs,
            attempts=1, now_ts=1_700_000_200,
        )

        self.assertEqual(balances, {})
        self.assertEqual(metadata["status"], "unavailable")
        self.assertEqual(metadata["failedChains"], ["bera"])

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

    def test_detect_contracts_batch_keeps_eip7702_account_visible(self):
        delegate = "0x" + "1" * 40
        designation = "0xef0100" + delegate[2:]

        def fake_batch(_rpcs, payloads, **_kwargs):
            return {
                payload["id"]: {
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": designation,
                }
                for payload in payloads
            }, []

        with patch.object(flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(flows, "rpc_single_request") as single:
            contracts = flows.detect_contracts_batch([ALICE], "eth")

        single.assert_not_called()
        self.assertEqual(contracts, set())

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
