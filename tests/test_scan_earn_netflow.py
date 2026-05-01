import unittest

from scan_earn_netflow import MIN_BLOCK_CHUNK, _is_chunk_too_large_error, _reduced_chunk_size


class ScanEarnNetflowTest(unittest.TestCase):
    def test_detects_payload_size_errors_from_rpc_tail(self):
        self.assertTrue(_is_chunk_too_large_error("HTTP Error 413: Request Entity Too Large"))
        self.assertTrue(_is_chunk_too_large_error("All RPCs failed; recent errors: payload too large"))
        self.assertTrue(_is_chunk_too_large_error("query returned more than 10000 results"))

    def test_reduced_chunk_size_never_goes_below_minimum(self):
        self.assertEqual(_reduced_chunk_size(49_999), 24_999)
        self.assertEqual(_reduced_chunk_size(500), MIN_BLOCK_CHUNK)


if __name__ == "__main__":
    unittest.main()
