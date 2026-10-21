import io
import json
import unittest
from http.client import IncompleteRead, RemoteDisconnected
from unittest.mock import MagicMock, patch

import generate_earn_snapshots as snapshots


class GenerateEarnSnapshotsTests(unittest.TestCase):
    def test_graphql_query_retries_interrupted_response_with_identical_pinned_request(self):
        endpoint = "https://example.test/subgraph"
        query = snapshots.MARGIN_ACCOUNTS_QUERY
        variables = {"first": 250, "lastId": "account-250", "block": 123}
        partial = b'{"data":{"marginAccounts":[]}}'
        complete = b'{"data":{"marginAccounts":[{"id":"account-251"}]}}'

        for error in (IncompleteRead(partial), RemoteDisconnected("Connection closed")):
            with self.subTest(error=type(error).__name__):
                interrupted = MagicMock()
                interrupted.__enter__.return_value.read.side_effect = error
                with patch.object(snapshots, "urlopen", side_effect=[interrupted, io.BytesIO(complete)]) as request:
                    with patch.object(snapshots.time, "sleep"):
                        try:
                            result = snapshots._graphql_query(endpoint, query, variables, timeout=7)
                        except (IncompleteRead, RemoteDisconnected) as exc:
                            self.fail(f"Interrupted response escaped the retry loop: {exc}")

                self.assertEqual({"marginAccounts": [{"id": "account-251"}]}, result)
                self.assertEqual(2, request.call_count)
                for call in request.call_args_list:
                    self.assertEqual(endpoint, call.args[0].full_url)
                    self.assertEqual("POST", call.args[0].get_method())
                    self.assertEqual({"query": query, "variables": variables}, json.loads(call.args[0].data))
                    self.assertEqual(7, call.kwargs["timeout"])

    def test_graphql_query_rejects_partial_data_after_three_interrupted_responses(self):
        partial = b'{"data":{"marginAccounts":[]}}'
        responses = []
        for _ in range(3):
            interrupted = MagicMock()
            interrupted.__enter__.return_value.read.side_effect = IncompleteRead(partial)
            responses.append(interrupted)

        with patch.object(snapshots, "urlopen", side_effect=responses) as request:
            with patch.object(snapshots.time, "sleep") as sleep:
                try:
                    with self.assertRaisesRegex(RuntimeError, "GraphQL request failed.*IncompleteRead"):
                        snapshots._graphql_query("https://example.test/subgraph", snapshots.MARGIN_ACCOUNTS_QUERY)
                except IncompleteRead as exc:
                    self.fail(f"Interrupted response escaped before the retry budget was exhausted: {exc}")

        self.assertEqual(3, request.call_count)
        self.assertEqual(2, sleep.call_count)

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
