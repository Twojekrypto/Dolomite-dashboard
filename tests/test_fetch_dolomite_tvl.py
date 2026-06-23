import unittest
from datetime import datetime, timezone

import fetch_dolomite_tvl


class FetchDolomiteTvlTest(unittest.TestCase):
    def test_snapshot_uses_usd_token_liquidity_and_clean_symbols(self):
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
        clean_symbols = {
            "Arbitrum": {
                "0x505582242757f16d72f8c4462a616e388ca1b074": "gmETH-USD",
                "33": "gmETH-USD",
                "0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef": "gmBTC-USD",
                "32": "gmBTC-USD",
            }
        }

        snapshot = fetch_dolomite_tvl.build_snapshot_from_token_liquidity(
            payloads,
            clean_symbols,
            now=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(151.0, snapshot["supplyLiquidity"])
        self.assertEqual(141.0, snapshot["totalTvl"])
        self.assertEqual(10.0, snapshot["totalBorrowed"])
        self.assertEqual(141.0, snapshot["currentChainTvls"]["Arbitrum"])
        self.assertEqual(10.0, snapshot["currentChainTvls"]["Arbitrum-borrowed"])

        arbitrum_tokens = snapshot["chainTokensInUsd"]["Arbitrum"]
        self.assertEqual(100.0, arbitrum_tokens["gmETH-USD"])
        self.assertEqual(50.5, arbitrum_tokens["gmBTC-USD"])
        self.assertNotIn("dGM", arbitrum_tokens)

        token_totals = snapshot["tokensInUsd"][0]["tokens"]
        self.assertEqual(100.0, token_totals["gmETH-USD"])
        self.assertEqual(50.5, token_totals["gmBTC-USD"])
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

        snapshot = fetch_dolomite_tvl.build_snapshot_from_token_liquidity(
            payloads,
            {},
            now=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(20.0, snapshot["chainTokensInUsd"]["Berachain"]["USD₮0"])


if __name__ == "__main__":
    unittest.main()
