import tempfile
import unittest
import inspect
from pathlib import Path
from unittest.mock import patch

from select_earn_canonical_hot_addresses import _active_strict_quality, build_selection


class SelectEarnCanonicalHotAddressesTest(unittest.TestCase):
    def test_strict_quality_requires_resolved_proof_for_every_active_market(self):
        address = "0x1111111111111111111111111111111111111111"
        with tempfile.TemporaryDirectory() as tmp:
            ledger_dir = Path(tmp) / "earn-verified-ledger"
            (ledger_dir / "arbitrum").mkdir(parents=True)
            (ledger_dir / "arbitrum" / f"{address}.json").write_text(
                '{"markets":{"1":{"strictStatus":"inferred"},'
                '"2":{"strictStatus":"inferred"}},'
                '"resolvedInterestLedger":{"strictStatus":"verified",'
                '"markets":{"1":{"strictStatus":"verified"}}}}',
                encoding="utf-8",
            )
            with patch(
                "select_earn_canonical_hot_addresses._latest_snapshot_payload",
                return_value={
                    address: {
                        "markets": {
                            "1": {"par": "10"},
                            "2": {"par": "20"},
                        },
                    },
                },
            ):
                quality = _active_strict_quality("arbitrum", {address}, ledger_dir)

        self.assertEqual("inferred", quality[address])

    def test_strict_remediation_prioritizes_active_mismatch_and_skips_verified_wallet(self):
        if "strict_remediation" not in inspect.signature(build_selection).parameters:
            self.fail("build_selection must expose strict_remediation mode")

        cold_missing = "0x1111111111111111111111111111111111111111"
        active_mismatch = "0x2222222222222222222222222222222222222222"
        active_verified = "0x3333333333333333333333333333333333333333"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            history_dir = root / "earn-subaccount-history"
            ledger_dir = root / "earn-verified-ledger"
            (history_dir / "arbitrum").mkdir(parents=True)
            (ledger_dir / "arbitrum").mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"arbitrum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            for address in (active_mismatch, active_verified):
                (history_dir / "arbitrum" / f"{address}.json").write_text(
                    '{"lastScannedBlock":100}',
                    encoding="utf-8",
                )
            (ledger_dir / "arbitrum" / f"{active_mismatch}.json").write_text(
                '{"markets":{"1":{"strictStatus":"mismatch"}}}',
                encoding="utf-8",
            )
            (ledger_dir / "arbitrum" / f"{active_verified}.json").write_text(
                '{"markets":{"1":{"strictStatus":"inferred"}},'
                '"resolvedInterestLedger":{"strictStatus":"verified",'
                '"markets":{"1":{"strictStatus":"verified"}}}}',
                encoding="utf-8",
            )
            snapshots = {
                active_mismatch: {"markets": {"1": {"par": "10"}}},
                active_verified: {"markets": {"1": {"par": "20"}}},
            }

            with (
                patch(
                    "select_earn_canonical_hot_addresses._load_known_addresses",
                    return_value=[cold_missing, active_mismatch, active_verified],
                ),
                patch(
                    "select_earn_canonical_hot_addresses._latest_snapshot_payload",
                    return_value=snapshots,
                ),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, metadata = build_selection(
                    "arbitrum",
                    limit=1,
                    priority_files=[],
                    include_priority_even_if_unknown=False,
                    history_dir=history_dir,
                    ledger_dir=ledger_dir,
                    strict_remediation=True,
                )

        self.assertEqual([active_mismatch], selected)
        self.assertEqual(1, metadata["activeStrictBlockingAddressCount"])
        self.assertEqual(1, metadata["activeMismatchAddressCount"])
        self.assertEqual(1, metadata["activeStrictVerifiedAddressCount"])

    def test_coverage_backfill_adds_missing_wallet_after_public_baseline(self):
        if "coverage_backfill" not in inspect.signature(build_selection).parameters:
            self.fail("build_selection must expose coverage_backfill mode")

        existing = "0x1111111111111111111111111111111111111111"
        missing = "0x2222222222222222222222222222222222222222"
        with tempfile.TemporaryDirectory() as tmp:
            history_dir = Path(tmp) / "earn-subaccount-history"
            chain_dir = history_dir / "mantle"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"mantle":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{existing}.json").write_text('{"lastScannedBlock":100}', encoding="utf-8")

            def score_snapshot(_chain, scores):
                scores[existing] = 100
                scores[missing] = 10

            with (
                patch("select_earn_canonical_hot_addresses._load_known_addresses", return_value=[existing, missing]),
                patch("select_earn_canonical_hot_addresses._score_snapshot_wallets", side_effect=score_snapshot),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, metadata = build_selection(
                    "mantle",
                    limit=1,
                    priority_files=[],
                    include_priority_even_if_unknown=True,
                    history_dir=history_dir,
                    existing_history_only=True,
                    coverage_backfill=True,
                )

        self.assertEqual(selected, [missing])
        self.assertTrue(metadata["coverageBackfill"])
        self.assertFalse(metadata["existingHistoryOnly"])

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

    def test_unbounded_existing_history_selection_includes_unscored_wallets(self):
        scored = "0x1111111111111111111111111111111111111111"
        cold = "0x2222222222222222222222222222222222222222"

        with tempfile.TemporaryDirectory() as tmp:
            history_dir = Path(tmp) / "earn-subaccount-history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            for address in (scored, cold):
                (chain_dir / f"{address}.json").write_text("{}", encoding="utf-8")

            def score_snapshot(_chain, scores):
                scores[scored] = 10

            with (
                patch("select_earn_canonical_hot_addresses._load_known_addresses", return_value=[scored, cold]),
                patch("select_earn_canonical_hot_addresses._score_snapshot_wallets", side_effect=score_snapshot),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, metadata = build_selection(
                    "arbitrum",
                    limit=0,
                    priority_files=[],
                    include_priority_even_if_unknown=True,
                    history_dir=history_dir,
                    existing_history_only=True,
                )

        self.assertEqual(selected, [scored, cold])
        self.assertEqual(metadata["existingHistoryAddressCount"], 2)
        self.assertEqual(metadata["selectedAddressCount"], 2)

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

    def test_coverage_backfill_refreshes_active_stale_wallet_before_cold_missing_wallet(self):
        active_stale = "0x1111111111111111111111111111111111111111"
        cold_missing = "0x2222222222222222222222222222222222222222"

        with tempfile.TemporaryDirectory() as tmp:
            history_dir = Path(tmp) / "earn-subaccount-history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            (history_dir / "manifest.json").write_text(
                '{"chains":{"arbitrum":{"lastBlock":100}}}',
                encoding="utf-8",
            )
            (chain_dir / f"{active_stale}.json").write_text(
                '{"lastScannedBlock":99}',
                encoding="utf-8",
            )

            with (
                patch(
                    "select_earn_canonical_hot_addresses._load_known_addresses",
                    return_value=[active_stale, cold_missing],
                ),
                patch(
                    "select_earn_canonical_hot_addresses._active_snapshot_wallets",
                    return_value={active_stale},
                    create=True,
                ),
                patch("select_earn_canonical_hot_addresses._score_snapshot_wallets", return_value=None),
                patch("select_earn_canonical_hot_addresses._score_netflow_wallets", return_value=None),
            ):
                selected, metadata = build_selection(
                    "arbitrum",
                    limit=1,
                    priority_files=[],
                    include_priority_even_if_unknown=True,
                    history_dir=history_dir,
                    coverage_backfill=True,
                )

        self.assertEqual([active_stale], selected)
        self.assertEqual(1, metadata["activeSnapshotAddressCount"])
        self.assertEqual(1, metadata["activeStaleHistoryAddressCount"])
        self.assertEqual(1, metadata["coldMissingHistoryAddressCount"])


if __name__ == "__main__":
    unittest.main()
