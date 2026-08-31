import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_dolo_flows as flows
from rpc_client import PUBLIC_ENDPOINTS, get_endpoints


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
    def test_new_ethereum_drpc_secret_is_preferred_before_public_fallbacks(self):
        endpoint = "https://lb.drpc.org/ogrpc?network=ethereum&dkey=test-secret"
        with patch.dict(os.environ, {"DRPC_ETHEREUM_RPC_2_JEFF": endpoint}, clear=True):
            endpoints = get_endpoints("ethereum")

        self.assertEqual(endpoints[0], endpoint)

    def test_exact_period_cutoff_uses_irregular_block_timestamps(self):
        timestamps = {
            100: 1_000,
            101: 1_007,
            102: 1_021,
            103: 1_028,
            104: 1_041,
        }

        selected = flows.find_first_block_at_or_after_timestamp(
            100,
            104,
            1_010,
            timestamps.__getitem__,
        )

        self.assertEqual(selected, 102)
        self.assertEqual(timestamps[selected], 1_021)

    def test_exact_period_cutoff_pins_to_deploy_when_target_is_older(self):
        timestamps = {100: 1_000, 101: 1_007, 102: 1_021}

        selected = flows.find_first_block_at_or_after_timestamp(
            100,
            102,
            900,
            timestamps.__getitem__,
        )

        self.assertEqual(selected, 100)

    def test_exact_period_cutoff_fails_closed_for_future_target(self):
        timestamps = {100: 1_000, 101: 1_007, 102: 1_021}

        with self.assertRaises(ValueError):
            flows.find_first_block_at_or_after_timestamp(
                100,
                102,
                1_022,
                timestamps.__getitem__,
            )

    def test_period_timestamp_lookup_skips_log_only_endpoints_and_rate_limited_first_choice(self):
        response = {
            "result": {"timestamp": hex(1_700_000_000)},
        }
        with patch.object(flows, "rpc_single_request", return_value=response) as request:
            timestamp = flows.load_block_timestamp("eth", 21_500_000, {})

        endpoints = request.call_args.args[0]
        self.assertEqual(timestamp, 1_700_000_000)
        self.assertEqual(endpoints[0], "https://eth.drpc.org/")
        self.assertTrue(all(endpoint.startswith("http") for endpoint in endpoints))

    def test_ethereum_public_fallbacks_include_two_archive_capable_families(self):
        endpoints = PUBLIC_ENDPOINTS["ethereum"]

        self.assertIn("https://eth.drpc.org/", endpoints)
        self.assertIn("https://eth.api.onfinality.io/public", endpoints)
        self.assertIn("https://mainnet.gateway.tenderly.co", endpoints)
        self.assertLess(
            endpoints.index("https://eth.api.onfinality.io/public"),
            endpoints.index("https://mainnet.gateway.tenderly.co"),
        )
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
        self.assertEqual(
            flows.rpc_provider_family("etherscan://logs"),
            "etherscan.io",
        )
        self.assertEqual(
            flows.rpc_provider_family("blockscout://logs"),
            "blockscout.com",
        )

    def test_ethereum_log_quorum_prefers_independent_archive_fallbacks(self):
        families = flows._rpc_families([
            "https://eth-mainnet.g.alchemy.com/v2/key-one",
            "https://eth-mainnet.g.alchemy.com/v2/key-two",
            "https://eth.drpc.org/",
            "https://mainnet.gateway.tenderly.co",
            "https://rpc.mevblocker.io",
        ], chain_key="eth")

        self.assertEqual(
            [family for family, _endpoints in families[:3]],
            ["mainnet.gateway.tenderly.co", "rpc.mevblocker.io", "drpc.org"],
        )

    def test_berachain_log_quorum_prefers_fast_independent_public_sources(self):
        paid_drpc = "https://lb.drpc.org/ogrpc?network=berachain&dkey=test-key"
        public_drpc = "https://berachain.drpc.org/"
        families = flows._rpc_families([
            paid_drpc,
            "https://example.quiknode.pro/test-key/",
            "https://berachain-mainnet.g.alchemy.com/v2/test-key",
            "https://rpc.berachain.com/",
            "https://berachain-rpc.publicnode.com/",
            public_drpc,
        ], chain_key="bera")

        self.assertEqual(
            [family for family, _endpoints in families[:2]],
            ["berachain.com", "drpc.org"],
        )
        self.assertEqual(families[1][1][0], public_drpc)

    def test_berachain_log_chunks_stay_within_verified_provider_limit(self):
        self.assertLessEqual(flows.CHAINS["bera"]["chunk_size"], 6_250)

    def test_etherscan_log_source_paginates_without_truncating_results(self):
        first_page = [
            transfer_log(100 + index // 100, "0x" + f"{index:064x}", index)
            for index in range(1_000)
        ]
        second_page = [transfer_log(110, "0x" + "f" * 64, 1_000)]

        def response(rows):
            result = Mock(status_code=200, headers={})
            result.json.return_value = {
                "status": "1",
                "message": "OK",
                "result": rows,
            }
            return result

        with patch.object(
            flows.requests,
            "get",
            side_effect=[response(first_page), response(second_page)],
        ) as request_get, patch.object(flows.time, "sleep"):
            logs = flows._request_etherscan_transfer_logs(
                {"name": "Ethereum"}, 100, 110, api_key="test-key"
            )

        self.assertEqual(len(logs), 1_001)
        self.assertEqual(request_get.call_count, 2)
        self.assertEqual(request_get.call_args_list[0].kwargs["params"]["page"], 1)
        self.assertEqual(request_get.call_args_list[1].kwargs["params"]["page"], 2)
        self.assertEqual(request_get.call_args_list[0].kwargs["params"]["offset"], 1_000)

    def test_blockscout_log_source_splits_capped_ranges_until_complete(self):
        capped_parent = [
            transfer_log(100 + index // 500, "0x" + f"{index:064x}", index)
            for index in range(1_000)
        ]
        left = [
            transfer_log(100, "0x" + f"{index + 2_000:064x}", index)
            for index in range(600)
        ]
        right = [
            transfer_log(101, "0x" + f"{index + 3_000:064x}", index)
            for index in range(500)
        ]

        def response(rows):
            result = Mock(status_code=200, headers={})
            result.json.return_value = {
                "status": "1",
                "message": "OK",
                "result": [
                    dict(row, topics=list(row["topics"]) + ["none"])
                    for row in rows
                ],
            }
            return result

        with patch.object(
            flows.requests,
            "get",
            side_effect=[response(capped_parent), response(left), response(right)],
        ) as request_get, patch.object(flows.time, "sleep"):
            logs = flows._request_blockscout_transfer_logs(
                {"name": "Ethereum"}, 100, 101
            )

        self.assertEqual(logs, left + right)
        self.assertEqual(request_get.call_count, 3)
        requested_ranges = [
            (call.kwargs["params"]["fromBlock"], call.kwargs["params"]["toBlock"])
            for call in request_get.call_args_list
        ]
        self.assertEqual(requested_ranges, [(100, 101), (100, 100), (101, 101)])

    def test_blockscout_log_source_rejects_a_capped_single_block(self):
        capped = [
            transfer_log(100, "0x" + f"{index:064x}", index)
            for index in range(1_000)
        ]
        response = Mock(status_code=200, headers={})
        response.json.return_value = {
            "status": "1",
            "message": "OK",
            "result": capped,
        }

        with patch.object(flows.requests, "get", return_value=response), patch.object(
            flows.time, "sleep"
        ):
            logs = flows._request_blockscout_transfer_logs(
                {"name": "Ethereum"}, 100, 100
            )

        self.assertIsNone(logs)

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

    def test_fetch_dolomite_dolo_balances_pins_historical_query_to_requested_block(self):
        class Response:
            status_code = 200

            def json(self):
                return {
                    "data": {
                        "_meta": {"block": {"number": 123, "timestamp": 1_600_000_000}},
                        "interestIndexes": [{
                            "id": flows.DOLO_CONTRACT,
                            "supplyIndex": "1.25",
                            "token": {"id": flows.DOLO_CONTRACT, "symbol": "DOLO", "marketId": "16"},
                        }],
                        "marginAccountTokenValues": [
                            {"valuePar": "10", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
                            {"valuePar": "5", "marginAccount": {"effectiveUser": {"id": ALICE}, "user": {"id": ALICE}}},
                        ],
                    }
                }

        queries = []

        def request(_url, **kwargs):
            queries.append(kwargs["json"]["query"])
            return Response()

        balances, metadata = flows.fetch_dolomite_dolo_balances(
            None,
            request_fn=request,
            subgraphs={"eth": {"name": "Ethereum", "url": "https://subgraph.example/eth"}},
            attempts=1,
            now_ts=1_800_000_000,
            block_numbers={"eth": 123},
        )

        self.assertEqual(balances[ALICE]["eth"], 18.75)
        self.assertEqual(metadata["status"], "complete")
        self.assertEqual(metadata["chains"]["eth"]["requestedBlock"], 123)
        self.assertTrue(queries)
        self.assertIn("block: { number: 123 }", queries[0])

    def test_holder_dolomite_history_cache_reuses_complete_points(self):
        state = {}
        points = [
            {"key": "hist_20260829", "timestamp": "2026-08-29T00:00:00Z", "ts": 1_777_075_200},
            {"key": "hist_20260830", "timestamp": "2026-08-30T00:00:00Z", "ts": 1_777_161_600},
        ]
        subgraphs = {
            "eth": {"name": "Ethereum", "url": "https://subgraph.example/eth"},
            "bera": {"name": "Berachain", "url": "https://subgraph.example/bera"},
        }
        calls = []

        def fetch(_addresses, **kwargs):
            chain_key = next(iter(kwargs["subgraphs"]))
            block = kwargs["block_numbers"][chain_key]
            calls.append((chain_key, block))
            return {
                ALICE: {chain_key: 10.0 if chain_key == "eth" else 2.0, "total": 10.0 if chain_key == "eth" else 2.0}
            }, {
                "status": "complete",
                "failedChains": [],
                "chains": {chain_key: {
                    "requestedBlock": block,
                    "blockNumber": block,
                    "blockTimestamp": block * 10,
                    "matchedWallets": 1,
                    "custodyAddress": flows.DOLOMITE_MARGIN_ADDRS[chain_key],
                }},
            }

        with patch.object(flows, "fetch_dolomite_dolo_balances", side_effect=fetch):
            snapshots, metadata = flows.load_holder_dolomite_history_snapshots(
                state,
                points,
                {"eth": 2_000, "bera": 4_000},
                1_777_248_000,
                subgraphs=subgraphs,
                checkpoint_fn=lambda _state: None,
                request_delay_seconds=0,
            )

        self.assertEqual(len(calls), 4)
        self.assertEqual(snapshots["hist_20260829"][ALICE], {"eth": 10.0, "bera": 2.0, "total": 12.0})
        self.assertEqual(metadata["status"], "complete")
        self.assertEqual(metadata["pointCount"], 2)

        with patch.object(
            flows,
            "fetch_dolomite_dolo_balances",
            side_effect=AssertionError("complete cached points must not refetch"),
        ):
            cached, cached_metadata = flows.load_holder_dolomite_history_snapshots(
                state,
                points,
                {"eth": 2_000, "bera": 4_000},
                1_777_248_000,
                subgraphs=subgraphs,
                checkpoint_fn=lambda _state: None,
                request_delay_seconds=0,
            )

        self.assertEqual(cached, snapshots)
        self.assertEqual(cached_metadata["cachedChainSnapshots"], 4)

    def test_holder_dolomite_history_treats_verified_pre_market_points_as_zero(self):
        state = {}
        points = [
            {"key": "hist_before_market", "timestamp": "2025-04-24T00:00:00Z", "ts": 100},
            {"key": "hist_after_market", "timestamp": "2025-08-01T00:00:00Z", "ts": 900},
        ]
        subgraphs = {
            "eth": {
                "name": "Ethereum",
                "url": "https://subgraph.example/eth",
                "marketStartBlock": 500,
            },
        }
        calls = []

        def fetch(_addresses, **kwargs):
            block = kwargs["block_numbers"]["eth"]
            calls.append(block)
            return {}, {
                "status": "complete",
                "failedChains": [],
                "chains": {"eth": {
                    "requestedBlock": block,
                    "blockNumber": block,
                    "blockTimestamp": 900,
                    "matchedWallets": 0,
                    "custodyAddress": flows.DOLOMITE_MARGIN_ADDRS["eth"],
                }},
            }

        with patch.object(flows, "holder_history_cutoff_block", side_effect=[400, 800]), \
             patch.object(flows, "fetch_dolomite_dolo_balances", side_effect=fetch):
            snapshots, metadata = flows.load_holder_dolomite_history_snapshots(
                state,
                points,
                {"eth": 1_000},
                1_000,
                subgraphs=subgraphs,
                checkpoint_fn=lambda _state: None,
                request_delay_seconds=0,
            )

        self.assertEqual(calls, [800])
        self.assertEqual(snapshots["hist_before_market"], {})
        self.assertEqual(snapshots["hist_after_market"], {})
        cached = state["holder_dolomite_history"]["points"]["hist_before_market"]["chains"]["eth"]
        self.assertEqual(cached["requestedBlock"], 400)
        self.assertEqual(cached["marketStartBlock"], 500)
        self.assertEqual(cached["evidence"], "verified-zero-before-dolo-market")
        self.assertEqual(metadata["preMarketZeroChainSnapshots"], 1)

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
