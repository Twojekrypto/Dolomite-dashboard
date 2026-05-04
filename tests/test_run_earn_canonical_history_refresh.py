import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from run_earn_canonical_history_refresh import (
    RefreshIncomplete,
    _has_complete_baseline,
    _status_payload,
    _write_status_output,
)


class RunEarnCanonicalHistoryRefreshTest(unittest.TestCase):
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
