import importlib.util
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SYNC_SCRIPT = ROOT / "scripts" / "sync_earn_verified_manifest.py"
spec = importlib.util.spec_from_file_location("sync_earn_verified_manifest", SYNC_SCRIPT)
sync_earn_verified_manifest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync_earn_verified_manifest)


class EarnVerifiedManifestSyncTest(unittest.TestCase):
    def test_sync_merges_touched_chain_without_reverting_remote_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            ledger_dir = tmp_path / "earn-verified-ledger"
            xlayer_dir = ledger_dir / "xlayer"
            xlayer_dir.mkdir(parents=True)
            (xlayer_dir / "0x0000000000000000000000000000000000000001.json").write_text(
                '{"snapshotDate":"2026-05-11","canonicalHistory":{"comparisonBlock":59705507}}',
                encoding="utf-8",
            )
            (xlayer_dir / "0x0000000000000000000000000000000000000002.json").write_text(
                '{"snapshotDate":"2026-05-10","canonicalHistory":{"comparisonBlock":59685830}}',
                encoding="utf-8",
            )
            manifest_path = ledger_dir / "manifest.json"
            manifest_path.write_text(
                (
                    '{"version":2,"generatedAt":"old-local","chains":{'
                    '"ethereum":{"snapshotDate":"2026-05-10","lastNetflowBlock":1,"addressCount":10},'
                    '"xlayer":{"snapshotDate":"2026-05-10","lastNetflowBlock":59685830,"addressCount":2}'
                    "}}"
                ),
                encoding="utf-8",
            )
            base_manifest = {
                "version": 2,
                "generatedAt": "remote",
                "chains": {
                    "ethereum": {
                        "snapshotDate": "2026-05-11",
                        "lastNetflowBlock": 25070593,
                        "addressCount": 2761,
                    },
                    "xlayer": {
                        "snapshotDate": "2026-05-10",
                        "lastNetflowBlock": 59685830,
                        "addressCount": 2,
                    },
                },
            }

            payload = sync_earn_verified_manifest.sync_manifest(
                manifest_path=manifest_path,
                ledger_dir=ledger_dir,
                chains=["xlayer"],
                base_manifest=base_manifest,
                now=datetime(2026, 5, 11, 8, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(base_manifest["chains"]["ethereum"], payload["chains"]["ethereum"])
            self.assertEqual(
                {
                    "snapshotDate": "2026-05-11",
                    "lastNetflowBlock": 59705507,
                    "addressCount": 1,
                },
                payload["chains"]["xlayer"],
            )

    def test_sync_preserves_current_netflow_block_for_latest_local_entry(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            ledger_dir = tmp_path / "earn-verified-ledger"
            chain_dir = ledger_dir / "polygonzkevm"
            chain_dir.mkdir(parents=True)
            (chain_dir / "0x0000000000000000000000000000000000000001.json").write_text(
                '{"snapshotDate":"2026-05-11","canonicalHistory":{"comparisonBlock":31934190}}',
                encoding="utf-8",
            )
            manifest_path = ledger_dir / "manifest.json"
            manifest_path.write_text(
                (
                    '{"version":2,"generatedAt":"local","chains":{'
                    '"polygonzkevm":{"snapshotDate":"2026-05-11","lastNetflowBlock":31951093,"addressCount":7171}'
                    "}}"
                ),
                encoding="utf-8",
            )

            payload = sync_earn_verified_manifest.sync_manifest(
                manifest_path=manifest_path,
                ledger_dir=ledger_dir,
                chains=["polygonzkevm"],
                base_manifest={"version": 2, "generatedAt": "remote", "chains": {}},
                now=datetime(2026, 5, 11, 8, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(31951093, payload["chains"]["polygonzkevm"]["lastNetflowBlock"])


if __name__ == "__main__":
    unittest.main()
