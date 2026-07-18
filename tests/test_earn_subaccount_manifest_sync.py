import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SYNC_SCRIPT = ROOT / "scripts" / "sync_earn_subaccount_manifest.py"
spec = importlib.util.spec_from_file_location("sync_earn_subaccount_manifest", SYNC_SCRIPT)
sync_earn_subaccount_manifest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync_earn_subaccount_manifest)


class EarnSubaccountManifestSyncTest(unittest.TestCase):
    def test_sync_counts_only_valid_published_wallet_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            history_dir = root / "earn-subaccount-history"
            chain_dir = history_dir / "berachain"
            chain_dir.mkdir(parents=True)
            for suffix in ("1", "2"):
                address = f"0x{suffix.zfill(40)}"
                (chain_dir / f"{address}.json").write_text("{}", encoding="utf-8")
            (chain_dir / "manifest.json").write_text("{}", encoding="utf-8")
            manifest_path = history_dir / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "chains": {
                            "berachain": {
                                "addressCount": 612,
                                "selectionAddressCount": 612,
                                "lastBlock": 123,
                            },
                            "arbitrum": {"addressCount": 500, "lastBlock": 456},
                        }
                    }
                ),
                encoding="utf-8",
            )

            payload = sync_earn_subaccount_manifest.sync_manifest(
                manifest_path=manifest_path,
                history_dir=history_dir,
                chains=["berachain"],
            )

            self.assertEqual(2, payload["chains"]["berachain"]["addressCount"])
            self.assertEqual(612, payload["chains"]["berachain"]["selectionAddressCount"])
            self.assertEqual(123, payload["chains"]["berachain"]["lastBlock"])
            self.assertEqual({"addressCount": 500, "lastBlock": 456}, payload["chains"]["arbitrum"])

    def test_sync_rejects_invalid_manifest_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            history_dir = root / "earn-subaccount-history"
            history_dir.mkdir(parents=True)
            manifest_path = history_dir / "manifest.json"
            manifest_path.write_text("{", encoding="utf-8")

            with self.assertRaises(ValueError):
                sync_earn_subaccount_manifest.sync_manifest(
                    manifest_path=manifest_path,
                    history_dir=history_dir,
                    chains=["berachain"],
                )


if __name__ == "__main__":
    unittest.main()
