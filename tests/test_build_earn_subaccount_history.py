import os
import tempfile
import unittest
from pathlib import Path
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

    def test_address_scan_reduces_rejected_rpc_range_and_retries_same_block(self):
        ranges = []

        def fake_fetch_logs(rpcs, rpc_idx, contract, addresses, from_block, to_block):
            ranges.append((from_block, to_block))
            if to_block - from_block + 1 > 4:
                raise RuntimeError("block range exceeds provider limit")
            return []

        chain_config = {
            "margin": "0x1111111111111111111111111111111111111111",
            "rpcs": ["https://example.invalid"],
            "start_block": 1,
            "min_block_chunk": 2,
        }

        with (
            patch.dict(history_builder.CHAINS, {"testchain": chain_config}, clear=False),
            patch.dict(os.environ, {"EARN_SUBACCOUNT_HISTORY_BLOCK_CHUNK": "8"}, clear=False),
            patch("build_earn_subaccount_history.get_block_number", return_value=99),
            patch("build_earn_subaccount_history._get_latest_snapshot_date", return_value="2026-04-29"),
            patch("build_earn_subaccount_history._get_netflow_last_block", return_value=12),
            patch("build_earn_subaccount_history._fetch_logs_for_addresses", side_effect=fake_fetch_logs),
            patch("builtins.print"),
        ):
            history_builder.build_history_for_addresses_in_block_range(
                "testchain",
                ["0x2222222222222222222222222222222222222222"],
                from_block=1,
                to_block=12,
            )

        self.assertEqual(ranges, [(1, 8), (1, 4), (5, 8), (9, 12)])

    def test_address_scan_resumes_from_persisted_block_checkpoint(self):
        chain_config = {
            "margin": "0x1111111111111111111111111111111111111111",
            "rpcs": ["https://example.invalid"],
            "start_block": 1,
        }
        address = "0x2222222222222222222222222222222222222222"
        first_ranges = []
        second_ranges = []

        def interrupted_fetch(rpcs, rpc_idx, contract, addresses, from_block, to_block):
            first_ranges.append((from_block, to_block))
            if from_block == 5:
                raise KeyboardInterrupt("workflow checkpoint")
            return []

        def resumed_fetch(rpcs, rpc_idx, contract, addresses, from_block, to_block):
            second_ranges.append((from_block, to_block))
            return []

        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = Path(tmp) / "history-checkpoint.json"
            with (
                patch.dict(history_builder.CHAINS, {"testchain": chain_config}, clear=False),
                patch.dict(os.environ, {"EARN_SUBACCOUNT_HISTORY_BLOCK_CHUNK": "4"}, clear=False),
                patch("build_earn_subaccount_history.get_block_number", return_value=99),
                patch("build_earn_subaccount_history._get_latest_snapshot_date", return_value="2026-04-29"),
                patch("build_earn_subaccount_history._get_netflow_last_block", return_value=8),
                patch(
                    "build_earn_subaccount_history._fetch_logs_for_addresses",
                    side_effect=interrupted_fetch,
                ),
                patch("builtins.print"),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    history_builder.build_history_for_addresses_in_block_range(
                        "testchain",
                        [address],
                        from_block=1,
                        to_block=8,
                        checkpoint_path=checkpoint,
                    )

            self.assertTrue(checkpoint.exists())

            with (
                patch.dict(history_builder.CHAINS, {"testchain": chain_config}, clear=False),
                patch.dict(os.environ, {"EARN_SUBACCOUNT_HISTORY_BLOCK_CHUNK": "4"}, clear=False),
                patch("build_earn_subaccount_history.get_block_number", return_value=99),
                patch("build_earn_subaccount_history._get_latest_snapshot_date", return_value="2026-04-29"),
                patch("build_earn_subaccount_history._get_netflow_last_block", return_value=8),
                patch(
                    "build_earn_subaccount_history._fetch_logs_for_addresses",
                    side_effect=resumed_fetch,
                ),
                patch("builtins.print"),
            ):
                histories = history_builder.build_history_for_addresses_in_block_range(
                    "testchain",
                    [address],
                    from_block=1,
                    to_block=8,
                    checkpoint_path=checkpoint,
                )

        self.assertEqual(first_ranges, [(1, 4), (5, 8)])
        self.assertEqual(second_ranges, [(5, 8)])
        self.assertEqual(histories[address]["lastScannedBlock"], 8)


if __name__ == "__main__":
    unittest.main()
