import os
import unittest
from unittest.mock import patch

import build_earn_subaccount_history as history_builder


class BuildEarnSubaccountHistoryTest(unittest.TestCase):
    def test_address_scan_block_chunk_env_splits_backfill_ranges(self):
        ranges = []

        def fake_fetch_logs(rpcs, rpc_idx, contract, addresses, from_block, to_block):
            ranges.append((from_block, to_block))
            return []

        chain_config = {
            "margin": "0x1111111111111111111111111111111111111111",
            "rpcs": ["https://example.invalid"],
            "start_block": 1,
        }

        with (
            patch.dict(history_builder.CHAINS, {"testchain": chain_config}, clear=False),
            patch.dict(os.environ, {"EARN_SUBACCOUNT_HISTORY_BLOCK_CHUNK": "10"}, clear=False),
            patch("build_earn_subaccount_history.get_block_number", return_value=99),
            patch("build_earn_subaccount_history._get_latest_snapshot_date", return_value="2026-04-29"),
            patch("build_earn_subaccount_history._get_netflow_last_block", return_value=25),
            patch("build_earn_subaccount_history._fetch_logs_for_addresses", side_effect=fake_fetch_logs),
            patch("builtins.print"),
        ):
            histories = history_builder.build_history_for_addresses_in_block_range(
                "testchain",
                ["0x2222222222222222222222222222222222222222"],
                from_block=1,
                to_block=25,
            )

        self.assertEqual(ranges, [(1, 10), (11, 20), (21, 25)])
        history = histories["0x2222222222222222222222222222222222222222"]
        self.assertEqual(history["lastScannedBlock"], 25)
        self.assertEqual(history["scanRange"], {"fromBlock": 1, "toBlock": 25})


if __name__ == "__main__":
    unittest.main()
