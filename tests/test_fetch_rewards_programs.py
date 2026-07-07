import json
import tempfile
import unittest
from pathlib import Path

import fetch_rewards_programs as rewards


class RewardsProgramSupplyHistoryTest(unittest.TestCase):
    def _write_supply_history(self, root: Path) -> dict:
        history_path = root / "data" / "supply-history" / "arbitrum-usdc.json"
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(json.dumps({
            "points": [
                {"timestamp": 100, "value": "10", "tokenValue": "10"},
                {"timestamp": 200, "value": "20", "tokenValue": "20"},
                {"timestamp": 300, "value": "30", "tokenValue": "30"},
            ]
        }), encoding="utf-8")
        return {
            "chains": [{
                "chain": "arbitrum",
                "tokens": [{
                    "tokenId": "0xusdc",
                    "symbol": "USDC",
                    "path": "data/supply-history/arbitrum-usdc.json",
                }],
            }],
        }

    def test_ended_program_supply_range_uses_token_supply_history_not_tvl(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = self._write_supply_history(root)
            index = rewards.build_supply_history_index(manifest)
            program = {
                "chain": "arbitrum",
                "explorerAddress": "0xUSDC",
                "marketTokens": ["USDC"],
                "campaignStart": 90,
                "campaignEnd": 250,
                "tvlUsd": 1234567,
            }

            rewards.enrich_program_supply_ranges([program], index, root)

            self.assertEqual(program["supplyStartToken"], 10.0)
            self.assertEqual(program["supplyStartTimestamp"], 100)
            self.assertEqual(program["supplyEndToken"], 20.0)
            self.assertEqual(program["supplyEndTimestamp"], 200)
            self.assertEqual(program["supplySymbol"], "USDC")
            self.assertEqual(program["supplyHistorySource"], "static-subgraph-replay")
            self.assertEqual(program["tvlUsd"], 1234567)

    def test_missing_campaign_dates_fall_back_to_first_and_latest_supply_points(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = self._write_supply_history(root)
            index = rewards.build_supply_history_index(manifest)
            program = {
                "chain": "arbitrum",
                "tokenId": "0xUSDC",
                "marketTokens": ["USDC"],
            }

            rewards.enrich_program_supply_ranges([program], index, root)

            self.assertEqual(program["supplyStartToken"], 10.0)
            self.assertEqual(program["supplyStartTimestamp"], 100)
            self.assertEqual(program["supplyEndToken"], 30.0)
            self.assertEqual(program["supplyEndTimestamp"], 300)

    def test_campaign_bounds_use_first_start_and_last_end(self):
        start, end = rewards.campaign_bounds_from_campaigns([
            {"startTimestamp": 300, "endTimestamp": 400},
            {"startTimestamp": 100, "endTimestamp": 250},
            {"startTimestamp": None, "endTimestamp": "500"},
        ])

        self.assertEqual(start, 100)
        self.assertEqual(end, 500)


if __name__ == "__main__":
    unittest.main()
