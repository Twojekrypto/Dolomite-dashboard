import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, call, patch

import fetch_dolomite_tvl


class FetchDolomiteTvlTest(unittest.TestCase):
    def test_token_api_retries_transient_server_errors(self):
        transient_failure = Mock()
        transient_failure.status_code = 500
        transient_failure.raise_for_status.side_effect = fetch_dolomite_tvl.requests.HTTPError(
            "temporary upstream failure",
            response=transient_failure,
        )
        success = Mock()
        success.raise_for_status.return_value = None
        success.json.return_value = {"tokens": [{"symbol": "USDC"}]}

        with patch.object(
            fetch_dolomite_tvl.requests,
            "get",
            side_effect=[transient_failure, transient_failure, success],
        ) as request_get, patch.object(fetch_dolomite_tvl, "time", create=True) as time_mock:
            try:
                result = fetch_dolomite_tvl.fetch_token_liquidity_payload("Berachain")
            except fetch_dolomite_tvl.requests.HTTPError:
                result = None

        self.assertEqual([{"symbol": "USDC"}], result)
        self.assertEqual(3, request_get.call_count)
        self.assertEqual([call(2), call(4)], time_mock.sleep.call_args_list)

    def test_token_api_retries_transient_forbidden_response(self):
        transient_failure = Mock()
        transient_failure.status_code = 403
        transient_failure.raise_for_status.side_effect = fetch_dolomite_tvl.requests.HTTPError(
            "temporary upstream access denial",
            response=transient_failure,
        )
        success = Mock()
        success.raise_for_status.return_value = None
        success.json.return_value = {"tokens": [{"symbol": "DOLO"}]}

        with patch.object(
            fetch_dolomite_tvl.requests,
            "get",
            side_effect=[transient_failure, success],
        ) as request_get, patch.object(fetch_dolomite_tvl.time, "sleep") as sleep_mock:
            result = fetch_dolomite_tvl.fetch_token_liquidity_payload("Ethereum")

        self.assertEqual([{"symbol": "DOLO"}], result)
        self.assertEqual(2, request_get.call_count)
        self.assertEqual([call(2)], sleep_mock.call_args_list)

    def test_subgraph_payload_retries_incomplete_metadata(self):
        incomplete = Mock()
        incomplete.raise_for_status.return_value = None
        incomplete.json.return_value = {
            "data": {
                "tokens": [],
                "_meta": {
                    "block": {
                        "number": 123,
                        "hash": None,
                        "timestamp": 1_783_728_000,
                    },
                    "deployment": "dolomite-arbitrum",
                },
            }
        }
        complete_data = {
            "tokens": [],
            "_meta": {
                "block": {
                    "number": 124,
                    "hash": "0xabc",
                    "timestamp": 1_783_728_010,
                },
                "deployment": "dolomite-arbitrum",
            },
        }
        complete = Mock()
        complete.raise_for_status.return_value = None
        complete.json.return_value = {"data": complete_data}

        with patch.object(
            fetch_dolomite_tvl.requests,
            "post",
            side_effect=[incomplete, complete],
        ), patch.object(fetch_dolomite_tvl.time, "sleep") as sleep_mock:
            result = fetch_dolomite_tvl.fetch_subgraph_payload(
                "Arbitrum",
                "https://example.test/subgraph",
            )

        self.assertEqual(complete_data, result)
        self.assertEqual([call(2)], sleep_mock.call_args_list)

    def test_subgraph_payload_refuses_persistently_incomplete_metadata(self):
        incomplete = Mock()
        incomplete.raise_for_status.return_value = None
        incomplete.json.return_value = {
            "data": {
                "tokens": [],
                "_meta": {
                    "block": {
                        "number": 123,
                        "hash": "",
                        "timestamp": None,
                    },
                    "deployment": "",
                },
            }
        }

        with patch.object(
            fetch_dolomite_tvl.requests,
            "post",
            return_value=incomplete,
        ), patch.object(fetch_dolomite_tvl.time, "sleep"):
            with self.assertRaisesRegex(
                RuntimeError,
                r"Arbitrum subgraph metadata incomplete: "
                r"block\.timestamp, block\.hash, deployment",
            ):
                fetch_dolomite_tvl.fetch_subgraph_payload(
                    "Arbitrum",
                    "https://example.test/subgraph",
                )

    def test_archived_chain_failures_do_not_block_active_tvl_snapshot(self):
        archived = {"Polygon zkEVM", "Botanix"}
        payloads = {
            chain: {}
            for chain in fetch_dolomite_tvl.ASSETS_CHAINS
            if chain not in archived
        }

        failed, missing = fetch_dolomite_tvl.blocking_tvl_failures(sorted(archived), payloads)

        self.assertEqual([], failed)
        self.assertEqual([], missing)
        self.assertTrue(archived.issubset(fetch_dolomite_tvl.RETIRED_ASSETS_CHAINS))
        self.assertFalse(archived & set(fetch_dolomite_tvl.ACTIVE_ASSETS_CHAINS))

    def test_active_chain_failure_still_blocks_tvl_snapshot(self):
        payloads = {
            chain: {}
            for chain in fetch_dolomite_tvl.ASSETS_CHAINS
            if chain != "Arbitrum"
        }

        failed, missing = fetch_dolomite_tvl.blocking_tvl_failures(["Arbitrum"], payloads)

        self.assertEqual(["Arbitrum"], failed)
        self.assertEqual(["Arbitrum"], missing)

    def test_snapshot_uses_official_token_amounts_and_price_map(self):
        payloads = {
            "Arbitrum": {
                "tokens": [
                    {
                        "id": "0x505582242757f16d72f8c4462a616e388ca1b074",
                        "marketId": "33",
                        "symbol": "dGM",
                        "supplyLiquidityUSD": "100",
                        "borrowLiquidityUSD": "0",
                    },
                    {
                        "id": "0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef",
                        "marketId": "32",
                        "symbol": "dGM",
                        "supplyLiquidityUSD": "50.5",
                        "borrowLiquidityUSD": "10",
                    },
                    {
                        "id": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
                        "marketId": "5",
                        "symbol": "USDT",
                        "supplyLiquidityUSD": "0.5",
                        "borrowLiquidityUSD": "0",
                    },
                ],
                "_meta": {
                    "block": {
                        "number": 123,
                        "hash": "0xabc",
                        "timestamp": 1_783_728_000,
                    },
                    "deployment": "dolomite-arbitrum",
                },
            }
        }
        api_tokens = {
            "Arbitrum": [
                {
                    "id": "0x505582242757f16d72f8c4462a616e388ca1b074",
                    "marketId": "33",
                    "symbol": "dGM",
                    "cleanSymbol": "gmETH-USD",
                    "supplyLiquidity": "2",
                    "borrowLiquidity": "0",
                },
                {
                    "id": "0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef",
                    "marketId": "32",
                    "symbol": "dGM",
                    "cleanSymbol": "gmBTC-USD",
                    "supplyLiquidity": "1",
                    "borrowLiquidity": "0.2",
                },
                {
                    "id": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
                    "marketId": "5",
                    "symbol": "USDT",
                    "cleanSymbol": "USDT",
                    "supplyLiquidity": "0.5",
                    "borrowLiquidity": "0",
                },
            ]
        }
        prices = {
            "Arbitrum": {
                "0x505582242757f16d72f8c4462a616e388ca1b074": "40",
                "0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef": "50",
                "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "1",
            }
        }

        snapshot = fetch_dolomite_tvl.build_snapshot_from_official_liquidity(
            payloads,
            api_tokens,
            prices,
            now=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(130.5, snapshot["supplyLiquidity"])
        self.assertEqual(120.5, snapshot["totalTvl"])
        self.assertEqual(10.0, snapshot["totalBorrowed"])
        self.assertEqual(120.5, snapshot["currentChainTvls"]["Arbitrum"])
        self.assertEqual(10.0, snapshot["currentChainTvls"]["Arbitrum-borrowed"])

        arbitrum_tokens = snapshot["chainTokensInUsd"]["Arbitrum"]
        self.assertEqual(80.0, arbitrum_tokens["gmETH-USD"])
        self.assertEqual(50.0, arbitrum_tokens["gmBTC-USD"])
        self.assertNotIn("dGM", arbitrum_tokens)

        token_totals = snapshot["tokensInUsd"][0]["tokens"]
        self.assertEqual(80.0, token_totals["gmETH-USD"])
        self.assertEqual(50.0, token_totals["gmBTC-USD"])
        self.assertNotIn("dGM", token_totals)

    def test_clean_symbol_falls_back_to_raw_symbol_when_missing(self):
        payloads = {
            "Berachain": {
                "tokens": [
                    {
                        "id": "0x779ded0c9e1022225f8e0630b35a9b54be713736",
                        "marketId": "5",
                        "symbol": "USD₮0",
                        "supplyLiquidityUSD": "20",
                        "borrowLiquidityUSD": "4",
                    }
                ],
                "_meta": {
                    "block": {"number": 456, "hash": "0xdef", "timestamp": 1_783_728_000},
                    "deployment": "dolomite-berachain",
                },
            }
        }
        api_tokens = {
            "Berachain": [
                {
                    "id": "0x779ded0c9e1022225f8e0630b35a9b54be713736",
                    "marketId": "5",
                    "symbol": "USD₮0",
                    "supplyLiquidity": "20",
                    "borrowLiquidity": "4",
                }
            ]
        }
        prices = {
            "Berachain": {
                "0x779ded0c9e1022225f8e0630b35a9b54be713736": "1",
            }
        }

        snapshot = fetch_dolomite_tvl.build_snapshot_from_official_liquidity(
            payloads,
            api_tokens,
            prices,
            now=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(20.0, snapshot["chainTokensInUsd"]["Berachain"]["USD₮0"])

    def test_berachain_stbtc_uses_official_exchange_rate_price(self):
        stbtc = "0xf6718b2701d4a6498ef77d7c152b2137ab28b8a3"
        payloads = {
            "Berachain": {
                "tokens": [
                    {
                        "id": stbtc,
                        "marketId": "11",
                        "symbol": "stBTC",
                        "supplyLiquidityUSD": "25741782.384404124",
                        "borrowLiquidityUSD": "0",
                    }
                ],
                "_meta": {
                    "block": {"number": 789, "hash": "0xghi", "timestamp": 1_783_728_000},
                    "deployment": "dolomite-berachain",
                },
            }
        }
        api_tokens = {
            "Berachain": [
                {
                    "id": stbtc,
                    "marketId": "11",
                    "symbol": "stBTC",
                    "cleanSymbol": "stBTC",
                    "supplyLiquidity": "247.229050532267004551",
                    "borrowLiquidity": "0",
                }
            ]
        }
        prices = {
            "Berachain": {
                stbtc: "62320.603136",
            }
        }

        snapshot = fetch_dolomite_tvl.build_snapshot_from_official_liquidity(
            payloads,
            api_tokens,
            prices,
            now=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )

        self.assertAlmostEqual(
            15407463.541911501,
            snapshot["chainTokensInUsd"]["Berachain"]["stBTC"],
            places=6,
        )
        self.assertLess(snapshot["chainTokensInUsd"]["Berachain"]["stBTC"], 16000000)


if __name__ == "__main__":
    unittest.main()
