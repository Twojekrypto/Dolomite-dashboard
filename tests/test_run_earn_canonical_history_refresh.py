import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from run_earn_canonical_history_refresh import (
    RefreshIncomplete,
    _has_complete_baseline,
    _incomplete_cycle_exceeds_scan_task_span,
    _incomplete_cycle_requires_scan_task_upgrade,
    _target_lag_exceeds_resume_budget,
    _status_payload,
    _write_status_output,
)


class RunEarnCanonicalHistoryRefreshTest(unittest.TestCase):
    def test_target_lag_guard_refreshes_old_incomplete_incremental_cycle(self):
        self.assertTrue(
            _target_lag_exceeds_resume_budget(
                {"targetBlock": 1_000},
                current_target_block=1_601,
                max_resume_target_lag_blocks=600,
            )
        )
        self.assertFalse(
            _target_lag_exceeds_resume_budget(
                {"targetBlock": 1_000},
                current_target_block=1_600,
                max_resume_target_lag_blocks=600,
            )
        )
        self.assertFalse(
            _target_lag_exceeds_resume_budget(
                {"targetBlock": 1_000},
                current_target_block=1_601,
                max_resume_target_lag_blocks=0,
            )
        )

    def test_scan_task_span_guard_refreshes_old_large_incremental_plan(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            plan_path = root / "plans" / "ethereum-f1-t1000" / "incremental-plan.json"
            plan_path.parent.mkdir(parents=True)
            plan_path.write_text(
                json.dumps(
                    {
                        "maxScanBlocksPerTask": 10,
                        "scanTasks": [
                            {"fromBlock": 1, "toBlock": 1_000},
                            {"fromBlock": 1_001, "toBlock": 2_000},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            self.assertTrue(
                _incomplete_cycle_exceeds_scan_task_span(
                    plan_path,
                    max_delta_scan_blocks_per_task=500,
                )
            )
            self.assertFalse(
                _incomplete_cycle_exceeds_scan_task_span(
                    plan_path,
                    max_delta_scan_blocks_per_task=1_000,
                )
            )
            self.assertTrue(
                _incomplete_cycle_requires_scan_task_upgrade(
                    plan_path,
                    max_delta_scan_blocks_per_task=1_000,
                )
            )
            self.assertFalse(
                _incomplete_cycle_requires_scan_task_upgrade(
                    plan_path,
                    max_delta_scan_blocks_per_task=10,
                )
            )

    def test_scan_task_span_guards_ignore_non_object_plan_payload(self):
        with TemporaryDirectory() as tmpdir:
            plan_path = Path(tmpdir) / "incremental-plan.json"
            plan_path.write_text("[]", encoding="utf-8")

            self.assertFalse(
                _incomplete_cycle_exceeds_scan_task_span(
                    plan_path,
                    max_delta_scan_blocks_per_task=500,
                )
            )
            self.assertFalse(
                _incomplete_cycle_requires_scan_task_upgrade(
                    plan_path,
                    max_delta_scan_blocks_per_task=1_000,
                )
            )

    def test_has_complete_baseline_requires_each_selected_wallet_at_manifest_block(self):
        selected = "0x1111111111111111111111111111111111111111"
        stale = "0x2222222222222222222222222222222222222222"
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
            (chain_dir / f"{stale}.json").write_text(
                '{"lastScannedBlock":99}',
                encoding="utf-8",
            )

            self.assertTrue(_has_complete_baseline(history_dir, "arbitrum", [selected]))
            self.assertFalse(_has_complete_baseline(history_dir, "arbitrum", [selected, stale]))

    def test_incomplete_status_payload_summarizes_progress_without_marking_complete(self):
        incomplete = RefreshIncomplete(
            chain="arbitrum",
            phase="incremental",
            max_steps=720,
            payload={
                "status": {
                    "cycleId": "arbitrum-f1-t2",
                    "targetBlock": 2,
                    "complete": False,
                    "scan": {"complete": True, "completedWorkerCount": 4, "workerCount": 4},
                    "newAddressBackfill": {"complete": False, "completedWorkerCount": 0, "workerCount": 1},
                    "apply": {"complete": False, "completedWorkerCount": 0, "workerCount": 4},
                    "coverage": None,
                }
            },
        )

        payload = _status_payload(
            chain="arbitrum",
            phase="incremental",
            complete=False,
            selected_addresses=["0x1111111111111111111111111111111111111111"],
            incomplete=incomplete,
        )

        self.assertFalse(payload["complete"])
        self.assertEqual(payload["selectedAddressCount"], 1)
        self.assertEqual(payload["progress"]["scan"]["completedWorkerCount"], 4)
        self.assertFalse(payload["progress"]["newAddressBackfill"]["complete"])

    def test_write_status_output_creates_parent_directory(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nested" / "status.json"
            _write_status_output(path, {"complete": True, "chain": "xlayer"})

            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["chain"], "xlayer")


if __name__ == "__main__":
    unittest.main()
