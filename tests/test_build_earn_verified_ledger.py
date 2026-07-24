import json
import tempfile
import unittest
from pathlib import Path

import build_earn_verified_ledger as verified_ledger
from build_earn_verified_ledger import (
    _calculate_historical_yield_pnl,
    _derive_strict_verification,
    _select_best_baseline,
    _validate_resolved_interest_ledger,
    _write_or_remove_ledger,
)


class BuildEarnVerifiedLedgerTest(unittest.TestCase):
    def test_rejects_truncated_or_adjusted_resolved_replay(self):
        address = "0x1111111111111111111111111111111111111111"
        base = {
            "chain": "arbitrum",
            "address": address,
            "snapshotDate": "2026-07-18",
            "comparisonBlock": 12345,
            "strictStatus": "verified",
            "strictMethod": "interest-ledger",
            "canonicalHistoryCoverageStatus": "fresh",
            "markets": {"1": {
                "earnYield": "1",
                "settledYield": "1",
                "settledSupplyYield": "1",
                "settledBorrowYield": "0",
                "openBorrowYield": "0",
                "openSupplyYield": "0",
                "openCollateralYield": "0",
                "currentBorrowPar": "0",
                "currentSupplyPar": "0",
                "currentCollateralSupplyPar": "0",
                "strictStatus": "verified",
                "strictMethod": "interest-ledger",
            }},
            "replayVerificationData": {"1": {
                "status": "verified",
                "counted": True,
                "canVerify": True,
                "rawVerified": True,
                "snapshotIncomplete": False,
                "subgraphReplayTruncated": False,
                "replayStateAdjusted": False,
            }},
        }

        for flag in ("subgraphReplayTruncated", "replayStateAdjusted"):
            with self.subTest(flag=flag):
                artifact = json.loads(json.dumps(base))
                artifact["replayVerificationData"]["1"][flag] = True
                self.assertIsNone(_validate_resolved_interest_ledger(
                    artifact,
                    chain="arbitrum",
                    address=address,
                    latest_date="2026-07-18",
                    canonical_history={"lastScannedBlock": 12345},
                    snapshot_comparison_block=12345,
                ))

    def test_recent_cycle_baseline_requires_exact_zero_proof(self):
        helper = getattr(verified_ledger, "_trusted_recent_cycle_netflow", None)
        self.assertIsNotNone(helper)
        self.assertIsNone(helper({"recentNetFlow": "100", "resetPar": "0"}))
        self.assertIsNone(helper({
            "recentNetFlow": "100",
            "resetPar": "10",
            "cycleStartProof": "heuristic",
        }))
        self.assertEqual(
            helper({
                "recentNetFlow": "100",
                "resetPar": "0",
                "cycleStartProof": "exact-zero",
            }),
            100,
        )

    def test_accepts_verified_resolved_interest_ledger_at_canonical_block(self):
        address = "0x1111111111111111111111111111111111111111"
        artifact = {
            "version": 1,
            "chain": "arbitrum",
            "address": address,
            "snapshotDate": "2026-07-18",
            "comparisonBlock": 12345,
            "generatedAt": "2026-07-18T12:00:00Z",
            "strictStatus": "verified",
            "strictMethod": "interest-ledger",
            "canonicalHistoryCoverageStatus": "fresh",
            "markets": {
                "1": {
                    "earnYield": "123",
                    "settledYield": "23",
                    "settledSupplyYield": "23",
                    "settledBorrowYield": "0",
                    "openBorrowYield": "0",
                    "openSupplyYield": "100",
                    "openCollateralYield": "0",
                    "currentBorrowPar": "0",
                    "currentSupplyPar": "1000",
                    "currentCollateralSupplyPar": "0",
                    "hadSupply": True,
                    "hadBorrow": False,
                    "strictStatus": "verified",
                    "strictMethod": "interest-ledger",
                }
            },
            "replayVerificationData": {
                "1": {
                    "status": "verified",
                    "counted": True,
                    "canVerify": True,
                    "rawVerified": True,
                    "snapshotIncomplete": False,
                }
            },
        }

        validated = _validate_resolved_interest_ledger(
            artifact,
            chain="arbitrum",
            address=address,
            latest_date="2026-07-18",
            canonical_history={"lastScannedBlock": 12345},
            snapshot_comparison_block=12345,
        )

        self.assertEqual(validated["comparisonBlock"], 12345)
        self.assertEqual(validated["markets"]["1"]["earnYield"], "123")

    def test_accepts_resolved_interest_ledger_when_canonical_head_is_later(self):
        address = "0x1111111111111111111111111111111111111111"
        base = {
            "chain": "arbitrum",
            "address": address,
            "snapshotDate": "2026-07-18",
            "comparisonBlock": 12344,
            "strictStatus": "verified",
            "strictMethod": "interest-ledger",
            "canonicalHistoryCoverageStatus": "fresh",
            "markets": {"1": {
                "earnYield": "1",
                "settledYield": "1",
                "settledSupplyYield": "1",
                "settledBorrowYield": "0",
                "openBorrowYield": "0",
                "openSupplyYield": "0",
                "openCollateralYield": "0",
                "currentBorrowPar": "0",
                "currentSupplyPar": "0",
                "currentCollateralSupplyPar": "0",
                "strictStatus": "verified",
                "strictMethod": "interest-ledger",
            }},
            "replayVerificationData": {"1": {
                "status": "verified",
                "counted": True,
                "canVerify": True,
                "rawVerified": True,
                "snapshotIncomplete": False,
            }},
        }

        validated = _validate_resolved_interest_ledger(
            base,
            chain="arbitrum",
            address=address,
            latest_date="2026-07-18",
            canonical_history={"lastScannedBlock": 12345},
            snapshot_comparison_block=12344,
        )
        self.assertEqual(12344, validated["comparisonBlock"])

    def test_rejects_resolved_interest_ledger_from_different_snapshot_block(self):
        address = "0x1111111111111111111111111111111111111111"
        market = {
            "earnYield": "1", "settledYield": "1", "settledSupplyYield": "1",
            "settledBorrowYield": "0", "openBorrowYield": "0", "openSupplyYield": "0",
            "openCollateralYield": "0", "currentBorrowPar": "0", "currentSupplyPar": "0",
            "currentCollateralSupplyPar": "0", "strictStatus": "verified", "strictMethod": "interest-ledger",
        }
        artifact = {
            "chain": "arbitrum", "address": address, "snapshotDate": "2026-07-18",
            "comparisonBlock": 12344, "strictStatus": "verified", "strictMethod": "interest-ledger",
            "canonicalHistoryCoverageStatus": "fresh", "markets": {"1": market},
            "replayVerificationData": {"1": {"status": "verified", "counted": True, "canVerify": True, "rawVerified": True, "snapshotIncomplete": False}},
        }

        self.assertIsNone(_validate_resolved_interest_ledger(
            artifact,
            chain="arbitrum",
            address=address,
            latest_date="2026-07-18",
            canonical_history={"lastScannedBlock": 12345},
            snapshot_comparison_block=12345,
        ))

    def test_rejects_future_or_inferred_resolved_interest_ledger(self):
        address = "0x1111111111111111111111111111111111111111"
        base = {
            "chain": "arbitrum",
            "address": address,
            "snapshotDate": "2026-07-18",
            "comparisonBlock": 12346,
            "strictStatus": "verified",
            "strictMethod": "interest-ledger",
            "canonicalHistoryCoverageStatus": "fresh",
            "markets": {"1": {"earnYield": "1", "settledYield": "1", "settledSupplyYield": "1", "settledBorrowYield": "0", "openBorrowYield": "0", "openSupplyYield": "0", "openCollateralYield": "0", "currentBorrowPar": "0", "currentSupplyPar": "0", "currentCollateralSupplyPar": "0", "strictStatus": "verified", "strictMethod": "interest-ledger"}},
            "replayVerificationData": {"1": {"status": "verified", "counted": True, "canVerify": True, "rawVerified": True, "snapshotIncomplete": False}},
        }

        self.assertIsNone(_validate_resolved_interest_ledger(
            base,
            chain="arbitrum",
            address=address,
            latest_date="2026-07-18",
            canonical_history={"lastScannedBlock": 12345},
        ))
        inferred = json.loads(json.dumps(base))
        inferred["comparisonBlock"] = 12345
        inferred["markets"]["1"]["strictStatus"] = "inferred"
        self.assertIsNone(_validate_resolved_interest_ledger(
            inferred,
            chain="arbitrum",
            address=address,
            latest_date="2026-07-18",
            canonical_history={"lastScannedBlock": 12345},
        ))

    def test_rejects_resolved_interest_ledger_with_omitted_counted_market(self):
        address = "0x1111111111111111111111111111111111111111"
        market = {
            "earnYield": "123",
            "settledYield": "23",
            "settledSupplyYield": "23",
            "settledBorrowYield": "0",
            "openBorrowYield": "0",
            "openSupplyYield": "100",
            "openCollateralYield": "0",
            "currentBorrowPar": "0",
            "currentSupplyPar": "1000",
            "currentCollateralSupplyPar": "0",
            "strictStatus": "verified",
            "strictMethod": "interest-ledger",
        }
        verification = {
            "status": "verified",
            "counted": True,
            "canVerify": True,
            "rawVerified": True,
            "snapshotIncomplete": False,
        }
        artifact = {
            "chain": "arbitrum",
            "address": address,
            "snapshotDate": "2026-07-18",
            "comparisonBlock": 12345,
            "strictStatus": "verified",
            "strictMethod": "interest-ledger",
            "canonicalHistoryCoverageStatus": "fresh",
            "markets": {"1": market},
            "replayVerificationData": {"1": verification, "2": verification},
        }

        self.assertIsNone(_validate_resolved_interest_ledger(
            artifact,
            chain="arbitrum",
            address=address,
            latest_date="2026-07-18",
            canonical_history={"lastScannedBlock": 12345},
        ))

    def test_historical_pnl_prices_only_constant_positive_principal_intervals(self):
        result = _calculate_historical_yield_pnl(
            [
                {"date": "2026-07-01", "par": 1_000_000, "wei": 1_000_000},
                {"date": "2026-07-02", "par": 1_000_000, "wei": 1_010_000},
                {"date": "2026-07-03", "par": 2_000_000, "wei": 2_020_000},
                {"date": "2026-07-04", "par": 2_000_000, "wei": 2_030_000},
            ],
            {"2026-07-02": "2", "2026-07-04": "3"},
            decimals=6,
            symbol="TEST",
        )

        self.assertEqual(result["historicalYieldUsd"], "0.05")
        self.assertEqual(result["historicalYieldEligibleIntervals"], 2)
        self.assertEqual(result["historicalYieldPricedIntervals"], 2)
        self.assertEqual(result["historicalYieldSkippedFlowIntervals"], 1)
        self.assertEqual(result["historicalYieldValuationStatus"], "complete")

    def test_historical_pnl_reports_missing_non_stable_price(self):
        result = _calculate_historical_yield_pnl(
            [
                {"date": "2026-07-01", "par": 10**18, "wei": 10**18},
                {"date": "2026-07-02", "par": 10**18, "wei": 2 * 10**18},
            ],
            {},
            decimals=18,
            symbol="WETH",
        )

        self.assertEqual(result["historicalYieldUsd"], "0")
        self.assertEqual(result["historicalYieldMissingPriceIntervals"], 1)
        self.assertEqual(result["historicalYieldValuationStatus"], "unavailable")

    def test_select_best_baseline_prefers_closest_exact_match(self):
        chosen = _select_best_baseline(
            [
                {"name": "all-time-netflow", "diff": 5, "pre_snapshot_meta": None},
                {"name": "recent-cycle", "diff": 1, "pre_snapshot_meta": None},
            ],
            tolerance=5,
        )
        self.assertEqual(chosen["name"], "recent-cycle")

    def test_select_best_baseline_prefers_stronger_carry_window(self):
        chosen = _select_best_baseline(
            [
                {
                    "name": "all-time-netflow",
                    "diff": 15,
                    "pre_snapshot_meta": {
                        "residual": 0,
                        "tinyParDriftWindow": True,
                    },
                },
                {
                    "name": "recent-cycle",
                    "diff": 15,
                    "pre_snapshot_meta": {
                        "residual": 2,
                        "tinyParDriftWindow": False,
                    },
                },
            ],
            tolerance=1,
        )
        self.assertEqual(chosen["name"], "recent-cycle")

    def test_snapshot_netflow_match_is_inferred_even_with_fresh_canonical_history(self):
        strict_status, strict_method, strict_reason = _derive_strict_verification(
            "verified",
            "netflow+snapshot",
            canonical_coverage_status="fresh",
            canonical_consistency_status="match",
        )
        self.assertEqual(strict_status, "inferred")
        self.assertEqual(strict_method, "netflow+snapshot")
        self.assertEqual(strict_reason, "snapshot_netflow_match_requires_inference")

    def test_existing_refresh_removes_ledger_with_no_current_markets(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            path = output_dir / "arbitrum" / "0x1111111111111111111111111111111111111111.json"
            path.parent.mkdir(parents=True)
            path.write_text('{"markets":{"0":{"strictStatus":"verified"}}}', encoding="utf-8")

            wrote = _write_or_remove_ledger(
                output_dir,
                "arbitrum",
                "0x1111111111111111111111111111111111111111",
                None,
                remove_stale=True,
            )

            self.assertFalse(wrote)
            self.assertFalse(path.exists())

    def test_equivalent_existing_ledger_keeps_its_generated_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            address = "0x2222222222222222222222222222222222222222"
            path = output_dir / "arbitrum" / f"{address}.json"
            path.parent.mkdir(parents=True)
            existing = {
                "generatedAt": "2026-07-01T00:00:00Z",
                "markets": {"0": {"strictStatus": "inferred"}},
            }
            path.write_text(json.dumps(existing), encoding="utf-8")
            rebuilt = {
                "generatedAt": "2026-07-11T00:00:00Z",
                "markets": {"0": {"strictStatus": "inferred"}},
            }

            wrote = _write_or_remove_ledger(
                output_dir,
                "arbitrum",
                address,
                rebuilt,
                remove_stale=True,
            )

            self.assertTrue(wrote)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), existing)


if __name__ == "__main__":
    unittest.main()
