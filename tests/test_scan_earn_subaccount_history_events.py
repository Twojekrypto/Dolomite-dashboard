import unittest
from unittest.mock import patch

import scan_earn_subaccount_history_events as scanner
from scan_earn_subaccount_history_events import _initial_rpc_index


class ScanEarnSubaccountHistoryEventsTest(unittest.TestCase):
    def test_initial_rpc_index_spreads_progress_keys_across_endpoints(self):
        rpcs = ["rpc-a", "rpc-b", "rpc-c", "rpc-d"]

        indexes = [_initial_rpc_index(rpcs, f"d{idx}of16") for idx in range(1, 17)]

        self.assertTrue(all(0 <= idx < len(rpcs) for idx in indexes))
        self.assertGreater(len(set(indexes)), 1)
        self.assertEqual(indexes, [_initial_rpc_index(rpcs, f"d{idx}of16") for idx in range(1, 17)])

    def test_initial_rpc_index_defaults_to_first_endpoint_without_progress_key(self):
        self.assertEqual(_initial_rpc_index(["rpc-a", "rpc-b"], None), 0)
        self.assertEqual(_initial_rpc_index([], "d1of4"), 0)

    def test_get_logs_falls_back_to_single_topics_when_combined_topic_query_fails(self):
        calls = []

        def fake_get_logs(rpcs, rpc_idx, contract, topics, from_block, to_block):
            calls.append(topics)
            if topics == [scanner.ALL_EVENTS]:
                raise RuntimeError("query returned more than 10000 results")
            topic = topics[0][0]
            return [{
                "transactionHash": f"0x{len(calls):064x}",
                "logIndex": "0x0",
                "blockNumber": hex(from_block),
                "topics": [topic],
            }]

        with patch.object(scanner, "get_logs", side_effect=fake_get_logs):
            logs = scanner._get_logs_with_topic_fallback(
                ["rpc-a"],
                [0],
                "0xContract",
                scanner.ALL_EVENTS,
                100,
                124,
            )

        self.assertEqual(calls[0], [scanner.ALL_EVENTS])
        self.assertEqual(calls[1:], [[[topic]] for topic in scanner.ALL_EVENTS])
        self.assertEqual(len(logs), len(scanner.ALL_EVENTS))


if __name__ == "__main__":
    unittest.main()
