import unittest

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


if __name__ == "__main__":
    unittest.main()
