import json
import tempfile
import unittest
from pathlib import Path

from materialize_earn_subaccount_history import _infer_scan_bounds, materialize_histories


class MaterializeEarnSubaccountHistoryTest(unittest.TestCase):
    def test_infer_scan_bounds_uses_manifest_global_target_for_sparse_shards(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            events_dir = Path(tmpdir) / "events"
            chain_dir = events_dir / "mantle"
            chain_dir.mkdir(parents=True)
            (chain_dir / "000000000100-000000000150.json").write_text(
                json.dumps({"owners": {}}),
                encoding="utf-8",
            )
            (events_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "chains": {
                            "mantle": {
                                "globalFromBlock": 50,
                                "globalToBlock": 200,
                                "scanRanges": [
                                    {"fromBlock": 50, "toBlock": 99},
                                    {"fromBlock": 100, "toBlock": 200},
                                ],
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(_infer_scan_bounds(events_dir, "mantle"), (50, 200))

    def test_materialize_histories_stamps_sparse_target_block(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_dir = root / "events"
            chain_dir = events_dir / "mantle"
            chain_dir.mkdir(parents=True)
            (chain_dir / "000000000100-000000000150.json").write_text(
                json.dumps({"owners": {}}),
                encoding="utf-8",
            )
            (events_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "chains": {
                            "mantle": {
                                "globalFromBlock": 50,
                                "globalToBlock": 200,
                                "scanRanges": [{"fromBlock": 50, "toBlock": 200}],
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            output_dir = root / "history"
            address = "0x0000000000000000000000000000000000000001"

            payload = materialize_histories(
                "mantle",
                addresses=[address],
                events_dir=events_dir,
                output_dir=output_dir,
                no_skip_existing=True,
                progress_key="test",
            )

            history = json.loads((output_dir / "mantle" / f"{address}.json").read_text(encoding="utf-8"))
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["targetBlock"], 200)
            self.assertEqual(history["lastScannedBlock"], 200)
            self.assertEqual(manifest["chains"]["mantle"]["lastBlock"], 200)


if __name__ == "__main__":
    unittest.main()
