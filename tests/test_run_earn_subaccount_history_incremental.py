import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from plan_earn_subaccount_history_incremental import build_incremental_plan
from run_earn_subaccount_history_incremental import _should_build_fresh_plan


class RunEarnSubaccountHistoryIncrementalTest(unittest.TestCase):
    def test_continue_does_not_auto_refresh_completed_cycle(self):
        self.assertFalse(
            _should_build_fresh_plan(
                status={"complete": True},
                refresh_plan=False,
            )
        )

    def test_refresh_plan_explicitly_starts_new_cycle(self):
        self.assertTrue(
            _should_build_fresh_plan(
                status={"complete": False},
                refresh_plan=True,
            )
        )

    def test_selection_address_file_limits_incremental_plan_scope(self):
        selected = "0x1111111111111111111111111111111111111111"
        other = "0x2222222222222222222222222222222222222222"
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            history_dir = root / "history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"arbitrum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{selected}.json").write_text(
                '{"lastScannedBlock":100}',
                encoding="utf-8",
            )
            selection_file = root / "selection.txt"
            selection_file.write_text(f"{selected}\n", encoding="utf-8")

            with patch(
                "plan_earn_subaccount_history_incremental._load_known_addresses",
                return_value=[selected, other],
            ), patch(
                "plan_earn_subaccount_history_incremental._resolve_target_block",
                return_value=105,
            ):
                plan = build_incremental_plan(
                    "arbitrum",
                    events_dir=root / "events",
                    history_dir=history_dir,
                    plan_dir=root / "plans",
                    to_block=None,
                    max_scan_workers=4,
                    max_apply_workers=4,
                    max_new_backfill_workers=4,
                    selection_address_file=selection_file,
                )

        self.assertEqual(plan["selectionAddressCount"], 1)
        self.assertEqual(plan["trackedAddressCount"], 1)
        self.assertEqual(plan["newAddressCount"], 0)
        self.assertEqual(len(plan["scanTasks"]), 3)
        self.assertEqual(len(plan["applyTasks"]), 1)

    def test_stale_selected_histories_are_backfilled_to_target(self):
        selected = "0x1111111111111111111111111111111111111111"
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            history_dir = root / "history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"arbitrum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{selected}.json").write_text(
                '{"lastScannedBlock":90}',
                encoding="utf-8",
            )
            selection_file = root / "selection.txt"
            selection_file.write_text(f"{selected}\n", encoding="utf-8")

            with patch(
                "plan_earn_subaccount_history_incremental._load_known_addresses",
                return_value=[selected],
            ), patch(
                "plan_earn_subaccount_history_incremental._resolve_target_block",
                return_value=105,
            ):
                plan = build_incremental_plan(
                    "arbitrum",
                    events_dir=root / "events",
                    history_dir=history_dir,
                    plan_dir=root / "plans",
                    to_block=None,
                    max_scan_workers=4,
                    max_apply_workers=4,
                    max_new_backfill_workers=4,
                    selection_address_file=selection_file,
                )

        self.assertEqual(plan["trackedAddressCount"], 1)
        self.assertEqual(plan["freshTrackedAddressCount"], 0)
        self.assertEqual(plan["staleTrackedAddressCount"], 1)
        self.assertEqual(plan["newAddressCount"], 0)
        self.assertEqual(plan["backfillAddressCount"], 1)
        self.assertEqual(len(plan["scanTasks"]), 0)
        self.assertEqual(len(plan["applyTasks"]), 0)
        self.assertEqual(len(plan["newAddressTasks"]), 1)
        self.assertIn("--to-block 105", plan["newAddressTasks"][0]["command"])


if __name__ == "__main__":
    unittest.main()
