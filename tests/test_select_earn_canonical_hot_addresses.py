import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from select_earn_canonical_hot_addresses import build_selection


class SelectEarnCanonicalHotAddressesTest(unittest.TestCase):
    def test_existing_history_only_keeps_steady_refresh_on_public_baseline(self):
        existing = "0x1111111111111111111111111111111111111111"
        new_hot = "0x2222222222222222222222222222222222222222"

        with tempfile.TemporaryDirectory() as tmp:
            history_dir = Path(tmp) / "earn-subaccount-history"
            chain_dir = history_dir / "berachain"
            chain_dir.mkdir(parents=True)
            (chain_dir / f"{existing}.json").write_text("{}", encoding="utf-8")

            def score_snapshot(_chain, scores):
                scores[existing] = 10
                scores[new_hot] = 20

            with (
                patch("select_earn_canonical_hot_addresses._load_known_addresses", return_value=[existing, new_hot]),
                patch("select_earn_canonical_hot_addresses._score_snapshot_wallets", side_effect=score_snapshot),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, metadata = build_selection(
                    "berachain",
                    limit=10,
                    priority_files=[],
                    include_priority_even_if_unknown=True,
                    history_dir=history_dir,
                    existing_history_only=True,
                )

        self.assertEqual(selected, [existing])
        self.assertTrue(metadata["existingHistoryOnly"])
        self.assertEqual(metadata["existingHistoryAddressCount"], 1)
        self.assertEqual(metadata["selectedAddressCount"], 1)


if __name__ == "__main__":
    unittest.main()
