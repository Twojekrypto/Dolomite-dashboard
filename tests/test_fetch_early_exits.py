import unittest
from unittest.mock import patch

import fetch_early_exits


class FetchEarlyExitsTest(unittest.TestCase):
    def test_withdraw_log_fetch_reduces_oversized_windows(self):
        calls = []

        def fake_rpc(method, params):
            if method == "eth_blockNumber":
                return hex(10)
            [payload] = params
            start = int(payload["fromBlock"], 16)
            end = int(payload["toBlock"], 16)
            calls.append((start, end))
            if end - start + 1 > 2:
                return None
            return []

        with patch.object(fetch_early_exits, "LOG_INITIAL_STEP", 16), \
             patch.object(fetch_early_exits, "LOG_MIN_STEP", 2), \
             patch.object(fetch_early_exits.time, "sleep", return_value=None), \
             patch.object(fetch_early_exits, "rpc_call", side_effect=fake_rpc):
            logs, latest = fetch_early_exits.fetch_withdraw_events(start_block=1)

        self.assertEqual(logs, [])
        self.assertEqual(latest, 10)
        self.assertIn((1, 2), calls)
        self.assertEqual(calls[-1], (9, 10))
        self.assertLessEqual(
            len([1 for start, end in calls if end - start + 1 > 2]),
            4,
        )


if __name__ == "__main__":
    unittest.main()
