import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import run_earn_subaccount_history_incremental as incremental_runner
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

    def test_complete_stages_with_incomplete_coverage_start_new_cycle(self):
        self.assertTrue(
            _should_build_fresh_plan(
                status={
                    "complete": False,
                    "scan": {"complete": True},
                    "newAddressBackfill": {"complete": True},
                    "apply": {"complete": True},
                    "coverage": {"complete": False},
                },
                refresh_plan=False,
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

    def test_orphaned_histories_do_not_block_incremental_cycle(self):
        active = "0x1111111111111111111111111111111111111111"
        orphan = "0x2222222222222222222222222222222222222222"
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            history_dir = root / "history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"arbitrum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{active}.json").write_text(
                '{"lastScannedBlock":100}',
                encoding="utf-8",
            )
            (chain_dir / f"{orphan}.json").write_text(
                '{"lastScannedBlock":90}',
                encoding="utf-8",
            )

            with patch(
                "plan_earn_subaccount_history_incremental._load_known_addresses",
                return_value=[active],
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
                    selection_address_file=None,
                )

        self.assertEqual(plan["trackedAddressCount"], 1)
        self.assertEqual(plan["orphanedHistoryCount"], 1)
        self.assertEqual(plan["staleTrackedAddressCount"], 0)
        self.assertEqual(plan["backfillAddressCount"], 0)
        self.assertEqual(len(plan["newAddressTasks"]), 0)

    def test_stale_selected_histories_are_caught_up_by_delta(self):
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
        self.assertEqual(plan["deltaTrackedAddressCount"], 1)
        self.assertEqual(plan["deltaFromBlock"], 91)
        self.assertEqual(plan["newAddressCount"], 0)
        self.assertEqual(plan["backfillAddressCount"], 0)
        self.assertGreater(len(plan["scanTasks"]), 0)
        self.assertEqual(len(plan["applyTasks"]), 1)
        self.assertEqual(len(plan["newAddressTasks"]), 0)
        self.assertIn("--from-block 91", plan["scanTasks"][0]["command"])

    def test_scan_plan_splits_large_delta_into_bounded_block_tasks(self):
        selected = "0x1111111111111111111111111111111111111111"
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            history_dir = root / "history"
            chain_dir = history_dir / "ethereum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"ethereum":{"lastBlock":100}}}',
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
                return_value=[selected],
            ), patch(
                "plan_earn_subaccount_history_incremental._resolve_target_block",
                return_value=1_100,
            ):
                plan = build_incremental_plan(
                    "ethereum",
                    events_dir=root / "events",
                    history_dir=history_dir,
                    plan_dir=root / "plans",
                    to_block=None,
                    max_scan_workers=2,
                    max_apply_workers=2,
                    max_new_backfill_workers=2,
                    selection_address_file=selection_file,
                    max_scan_blocks_per_task=250,
                )

        self.assertEqual(plan["maxScanWorkers"], 2)
        self.assertEqual(plan["maxScanBlocksPerTask"], 250)
        self.assertEqual(len(plan["scanTasks"]), 4)
        self.assertLessEqual(
            max(task["toBlock"] - task["fromBlock"] + 1 for task in plan["scanTasks"]),
            250,
        )

    def test_scan_launcher_caps_parallel_tasks_for_multi_task_plan(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cycle_root = root / "cycle"
            plan = {
                "chain": "ethereum",
                "cycleId": "ethereum-f1-t1000",
                "cycleRoot": str(cycle_root),
                "deltaEventsDir": str(cycle_root / "events"),
                "targetBlock": 1_000,
                "maxScanWorkers": 2,
                "scanTasks": [
                    {
                        "progressKey": f"d{idx}of5",
                        "fromBlock": idx * 100,
                        "toBlock": idx * 100 + 99,
                        "addressFile": str(root / "addresses.txt"),
                    }
                    for idx in range(1, 6)
                ],
            }
            (root / "addresses.txt").write_text(
                "0x1111111111111111111111111111111111111111\n",
                encoding="utf-8",
            )

            with patch.object(incremental_runner, "_start_task", side_effect=[101, 102]) as start_task:
                result = incremental_runner._ensure_scan_running(plan, "ethereum")

        self.assertEqual(start_task.call_count, 2)
        self.assertEqual([row["progressKey"] for row in result["started"]], ["d1of5", "d2of5"])
        queued = [row for row in result["skipped"] if row["reason"] == "queued"]
        self.assertEqual(len(queued), 3)


if __name__ == "__main__":
    unittest.main()
