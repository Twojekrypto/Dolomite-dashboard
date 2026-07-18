import json
import tempfile
import unittest
from pathlib import Path

from scripts.sync_earn_resolved_manifest import sync_manifest


class SyncEarnResolvedManifestTests(unittest.TestCase):
    def test_rebuilds_chain_counts_after_parallel_rebase(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for chain, count in (("arbitrum", 2), ("berachain", 1)):
                chain_dir = root / chain
                chain_dir.mkdir(parents=True)
                for index in range(count):
                    address = "0x" + f"{index + 1:040x}"
                    (chain_dir / f"{address}.json").write_text(
                        json.dumps({"snapshotDate": "2026-07-18"}),
                        encoding="utf-8",
                    )

            manifest = sync_manifest(root, chains=["arbitrum", "berachain"], generated_at="2026-07-18T12:00:00Z")

        self.assertEqual(2, manifest["chains"]["arbitrum"]["addressCount"])
        self.assertEqual(1, manifest["chains"]["berachain"]["addressCount"])
        self.assertEqual("2026-07-18", manifest["chains"]["arbitrum"]["snapshotDate"])
