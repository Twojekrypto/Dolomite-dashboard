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

    def test_prefer_stale_history_prioritizes_scored_stale_wallets(self):
        fresh = "0x1111111111111111111111111111111111111111"
        stale = "0x2222222222222222222222222222222222222222"

        with tempfile.TemporaryDirectory() as tmp:
            history_dir = Path(tmp) / "earn-subaccount-history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"arbitrum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{fresh}.json").write_text('{"lastScannedBlock":100}', encoding="utf-8")
            (chain_dir / f"{stale}.json").write_text('{"lastScannedBlock":90}', encoding="utf-8")

            def score_snapshot(_chain, scores):
                scores[fresh] = 100
                scores[stale] = 10

            with (
                patch("select_earn_canonical_hot_addresses._load_known_addresses", return_value=[fresh, stale]),
                patch("select_earn_canonical_hot_addresses._score_snapshot_wallets", side_effect=score_snapshot),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, metadata = build_selection(
                    "arbitrum",
                    limit=2,
                    priority_files=[],
                    include_priority_even_if_unknown=True,
                    history_dir=history_dir,
                    prefer_stale_history=True,
                )

        self.assertEqual(selected, [stale, fresh])
        self.assertEqual(metadata["staleHistoryAddressCount"], 1)

    def test_prefer_stale_history_selects_oldest_wallet_before_higher_score(self):
        oldest = "0x1111111111111111111111111111111111111111"
        recent = "0x2222222222222222222222222222222222222222"

        with tempfile.TemporaryDirectory() as tmp:
            history_dir = Path(tmp) / "earn-subaccount-history"
            chain_dir = history_dir / "ethereum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"ethereum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{oldest}.json").write_text('{"lastScannedBlock":10}', encoding="utf-8")
            (chain_dir / f"{recent}.json").write_text('{"lastScannedBlock":90}', encoding="utf-8")

            def score_snapshot(_chain, scores):
                scores[oldest] = 1
                scores[recent] = 1_000

            with (
                patch("select_earn_canonical_hot_addresses._load_known_addresses", return_value=[oldest, recent]),
                patch("select_earn_canonical_hot_addresses._score_snapshot_wallets", side_effect=score_snapshot),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, _metadata = build_selection(
                    "ethereum",
                    limit=1,
                    priority_files=[],
                    include_priority_even_if_unknown=True,
                    history_dir=history_dir,
                    prefer_stale_history=True,
                )

        self.assertEqual(selected, [oldest])

    def test_prefer_stale_history_selects_missing_history_before_stale(self):
        stale = "0x1111111111111111111111111111111111111111"
        missing = "0x2222222222222222222222222222222222222222"

        with tempfile.TemporaryDirectory() as tmp:
            history_dir = Path(tmp) / "earn-subaccount-history"
            chain_dir = history_dir / "ethereum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"ethereum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{stale}.json").write_text('{"lastScannedBlock":10}', encoding="utf-8")

            def score_snapshot(_chain, scores):
                scores[stale] = 1_000
                scores[missing] = 1

            with (
                patch("select_earn_canonical_hot_addresses._load_known_addresses", return_value=[stale, missing]),
                patch("select_earn_canonical_hot_addresses._score_snapshot_wallets", side_effect=score_snapshot),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, _metadata = build_selection(
                    "ethereum",
                    limit=1,
                    priority_files=[],
                    include_priority_even_if_unknown=True,
                    history_dir=history_dir,
                    prefer_stale_history=True,
                )

        self.assertEqual(selected, [missing])


if __name__ == "__main__":
    unittest.main()
