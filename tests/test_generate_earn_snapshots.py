import unittest
from unittest.mock import patch

import generate_earn_snapshots as snapshots


class GenerateEarnSnapshotsTests(unittest.TestCase):
    def test_default_chain_list_skips_archived_chains(self):
        for chain in ("polygonzkevm", "botanix"):
            self.assertIn(chain, snapshots.CHAINS)
            self.assertIn(chain, snapshots.RETIRED_CHAINS)
            self.assertNotIn(chain, snapshots.DEFAULT_CHAINS)

    def test_par_to_wei_uses_contract_round_half_up(self):
        self.assertEqual(1, snapshots._par_to_wei_round_half_up(1, 500_000_000_000_000_000))
        self.assertEqual(0, snapshots._par_to_wei_round_half_up(1, 499_999_999_999_999_999))

    def test_chain_snapshot_is_pinned_to_meta_block_and_publishes_indexes(self):
        address = "0x1111111111111111111111111111111111111111"
        token = "0x2222222222222222222222222222222222222222"
        responses = [
            {"_meta": {"block": {"number": 123}}},
            {"interestIndexes": [{"id": token, "supplyIndex": "1.5", "borrowIndex": "2"}]},
            {
                "marginAccounts": [{
                    "id": "account-1",
                    "effectiveUser": {"id": address},
                    "tokenValues": [{
                        "token": {"id": token, "marketId": "7", "symbol": "TEST", "decimals": 0},
                        "valuePar": "1",
                    }],
                }],
            },
        ]

        with patch.object(snapshots, "_graphql_query", side_effect=responses) as query:
            result = snapshots._aggregate_chain_snapshot("arbitrum", "https://example.test", 250)

        self.assertEqual(123, result["metadata"]["blockNumber"])
        self.assertEqual("1500000000000000000", result["metadata"]["interestIndexes"]["7"]["supplyIndex"])
        self.assertEqual("2000000000000000000", result["metadata"]["interestIndexes"]["7"]["borrowIndex"])
        self.assertEqual("2", result["snapshots"][address]["markets"]["7"]["wei"])
        margin_variables = query.call_args_list[2].args[2]
        self.assertEqual(123, margin_variables["block"])

    def test_merge_preserves_metadata_for_chain_not_refreshed(self):
        existing = {
            "snapshots": {"ethereum": {"0x1": {"markets": {}}}},
            "chainMetadata": {"ethereum": {"blockNumber": 10}},
        }
        merged = snapshots._merge_snapshot_payload(
            existing,
            "2026-07-18",
            "2026-07-18T12:00:00Z",
            {"arbitrum": {}},
            {"arbitrum": {"blockNumber": 20}},
        )

        self.assertEqual(10, merged["chainMetadata"]["ethereum"]["blockNumber"])
        self.assertEqual(20, merged["chainMetadata"]["arbitrum"]["blockNumber"])

    def test_missing_market_index_never_becomes_strict_snapshot_metadata(self):
        address = "0x1111111111111111111111111111111111111111"
        token = "0x2222222222222222222222222222222222222222"
        responses = [
            {"_meta": {"block": {"number": 123}}},
            {"interestIndexes": []},
            {"marginAccounts": [{
                "id": "account-1",
                "effectiveUser": {"id": address},
                "tokenValues": [{
                    "token": {"id": token, "marketId": "7", "symbol": "TEST", "decimals": 0},
                    "valuePar": "1",
                }],
            }]},
        ]

        with patch.object(snapshots, "_graphql_query", side_effect=responses):
            result = snapshots._aggregate_chain_snapshot("arbitrum", "https://example.test", 250)

        self.assertEqual("1", result["snapshots"][address]["markets"]["7"]["wei"])
        self.assertNotIn("7", result["metadata"]["interestIndexes"])


if __name__ == "__main__":
    unittest.main()
