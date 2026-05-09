import unittest
from unittest.mock import patch

import scan_earn_netflow
from scan_earn_netflow import MIN_BLOCK_CHUNK, _is_chunk_too_large_error, _reduced_chunk_size


class ScanEarnNetflowTest(unittest.TestCase):
    def test_detects_payload_size_errors_from_rpc_tail(self):
        self.assertTrue(_is_chunk_too_large_error("HTTP Error 413: Request Entity Too Large"))
        self.assertTrue(_is_chunk_too_large_error("All RPCs failed; recent errors: payload too large"))
        self.assertTrue(_is_chunk_too_large_error("invalid block range params"))
        self.assertTrue(_is_chunk_too_large_error("query returned more than 10000 results"))

    def test_reduced_chunk_size_never_goes_below_minimum(self):
        self.assertEqual(_reduced_chunk_size(49_999), 24_999)
        self.assertEqual(_reduced_chunk_size(500), MIN_BLOCK_CHUNK)

    def test_main_returns_failure_when_chain_scan_fails(self):
        fake_chains = {
            "mantle": {
                "margin": "0x0000000000000000000000000000000000000000",
                "rpcs": ["https://example.invalid"],
                "start_block": 1,
            }
        }
        with patch.object(scan_earn_netflow, "CHAINS", fake_chains), \
             patch.object(scan_earn_netflow, "scan_chain", return_value={"completed": False, "reason": "rpc_failed"}), \
             patch("sys.argv", ["scan_earn_netflow.py", "mantle"]):
            self.assertEqual(scan_earn_netflow.main(), 1)


if __name__ == "__main__":
    unittest.main()
