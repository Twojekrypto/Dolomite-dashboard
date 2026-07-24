import json
import tempfile
import unittest
from pathlib import Path

from build_earn_quality_status import _ledger_market_quality, build_quality_status


class EarnQualityStatusTest(unittest.TestCase):
    def _write_json(self, root: Path, rel: str, payload: dict) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_exact_nested_replay_overrides_quality_without_rewriting_raw_forensics(self):
        wallet = "0x1111111111111111111111111111111111111111"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = {
                "markets": {
                    "7": {
                        "strictStatus": "mismatch",
                        "strictMethod": "canonical-history-mismatch",
                        "strictReason": "canonical_history_mismatch",
                    },
                },
                "resolvedInterestLedger": {
                    "strictStatus": "verified",
                    "strictMethod": "interest-ledger",
                    "markets": {
                        "7": {
                            "strictStatus": "verified",
                            "strictMethod": "interest-ledger",
                        },
                    },
                    "replayVerificationData": {
                        "7": {
                            "status": "verified",
                            "rawVerified": True,
                            "snapshotIncomplete": False,
                            "subgraphReplayTruncated": False,
                            "replayStateAdjusted": False,
                        },
                    },
                },
            }
            ledger_path = root / f"earn-verified-ledger/arbitrum/{wallet}.json"
            self._write_json(
                root,
                f"earn-verified-ledger/arbitrum/{wallet}.json",
                ledger,
            )

            quality = _ledger_market_quality(root, "arbitrum", wallet, "7")
            persisted = json.loads(ledger_path.read_text(encoding="utf-8"))

        self.assertEqual("verified", quality["status"])
        self.assertEqual("interest-ledger", quality["method"])
        self.assertEqual("exact_replay_reconciled", quality["reason"])
        self.assertEqual("mismatch", persisted["markets"]["7"]["strictStatus"])
        self.assertEqual(
            "canonical_history_mismatch",
            persisted["markets"]["7"]["strictReason"],
        )

    def test_adjusted_nested_replay_does_not_override_raw_mismatch(self):
        wallet = "0x2222222222222222222222222222222222222222"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                f"earn-verified-ledger/arbitrum/{wallet}.json",
                {
                    "markets": {"7": {"strictStatus": "mismatch"}},
                    "resolvedInterestLedger": {
                        "strictStatus": "verified",
                        "strictMethod": "interest-ledger",
                        "markets": {
                            "7": {
                                "strictStatus": "verified",
                                "strictMethod": "interest-ledger",
                            },
                        },
                        "replayVerificationData": {
                            "7": {
                                "rawVerified": True,
                                "snapshotIncomplete": False,
                                "subgraphReplayTruncated": False,
                                "replayStateAdjusted": True,
                            },
                        },
                    },
                },
            )

            quality = _ledger_market_quality(root, "arbitrum", wallet, "7")

        self.assertEqual("mismatch", quality["status"])

    def test_active_snapshot_positions_are_counted_against_strict_ledger_status(self):
        wallet_a = "0x1111111111111111111111111111111111111111"
        wallet_b = "0x2222222222222222222222222222222222222222"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                "earn-snapshots/manifest.json",
                {"dates": ["2026-05-16"], "chains": {"2026-05-16": ["arbitrum"]}},
            )
            self._write_json(
                root,
                "earn-snapshots/2026-05-16.json",
                {
                    "date": "2026-05-16",
                    "snapshots": {
                        "arbitrum": {
                            wallet_a: {"markets": {"0": {"symbol": "WETH"}, "1": {"symbol": "USDC"}}},
                            wallet_b: {"markets": {"2": {"symbol": "USD1"}}},
                        }
                    },
                },
            )
            self._write_json(
                root,
                f"earn-verified-ledger/arbitrum/{wallet_a}.json",
                {
                    "markets": {
                        "0": {"strictStatus": "verified"},
                        "1": {
                            "strictStatus": "coverage_incomplete",
                            "strictMethod": "insufficient-history",
                            "strictReason": "snapshot_missing_market",
                        },
                    }
                },
            )

            status = build_quality_status(data_dir=root)

        chain = status["chains"]["arbitrum"]
        self.assertEqual(chain["activeAddressCount"], 2)
        self.assertEqual(chain["activeMarketCount"], 3)
        self.assertEqual(chain["strictVerifiedMarketCount"], 1)
        self.assertEqual(chain["nonStrictMarketCount"], 2)
        self.assertEqual(chain["blockingMarketCount"], 2)
        self.assertEqual(chain["actionableBlockingMarketCount"], 0)
        self.assertEqual(chain["coverageBacklogMarketCount"], 1)
        self.assertEqual(chain["sourceGapMarketCount"], 1)
        self.assertEqual(chain["coverageIncompleteMarketCount"], 1)
        self.assertEqual(
            chain["marketStatusCounts"],
            {"verified": 1, "coverage_incomplete": 1, "missing_ledger": 1},
        )
        self.assertEqual(chain["marketReasonCounts"]["missing_ledger"], 1)
        self.assertEqual(chain["marketReasonCounts"]["snapshot_missing_market"], 1)
        self.assertEqual(chain["marketReasonCounts"]["unknown"], 1)
        self.assertEqual(chain["qualityTier"], "partial")
        self.assertEqual(status["summary"]["strictVerifiedMarketRatio"], 0.333333)
        self.assertEqual(status["summary"]["actionableBlockingMarketCount"], 0)

    def test_live_balance_adjusted_status_remains_non_strict_quality(self):
        wallet = "0x3333333333333333333333333333333333333333"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                "earn-snapshots/manifest.json",
                {"dates": ["2026-05-16"], "chains": {"2026-05-16": ["arbitrum"]}},
            )
            self._write_json(
                root,
                "earn-snapshots/2026-05-16.json",
                {
                    "date": "2026-05-16",
                    "snapshots": {
                        "arbitrum": {
                            wallet: {"markets": {"0": {"symbol": "WETH"}}},
                        }
                    },
                },
            )
            self._write_json(
                root,
                f"earn-verified-ledger/arbitrum/{wallet}.json",
                {
                    "markets": {
                        "0": {"strictStatus": "live_balance_adjusted"},
                    }
                },
            )

            status = build_quality_status(data_dir=root)

        chain = status["chains"]["arbitrum"]
        self.assertEqual(chain["strictVerifiedMarketCount"], 0)
        self.assertEqual(chain["nonStrictMarketCount"], 1)
        self.assertEqual(chain["blockingMarketCount"], 0)
        self.assertEqual(chain["actionableBlockingMarketCount"], 0)
        self.assertEqual(chain["inferredMarketCount"], 1)
        self.assertEqual(chain["marketStatusCounts"], {"inferred": 1})

    def test_snapshot_netflow_strict_status_is_reclassified_as_inferred(self):
        wallet = "0x5555555555555555555555555555555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                "earn-snapshots/manifest.json",
                {"dates": ["2026-05-16"], "chains": {"2026-05-16": ["arbitrum"]}},
            )
            self._write_json(
                root,
                "earn-snapshots/2026-05-16.json",
                {"snapshots": {"arbitrum": {wallet: {"markets": {"0": {"symbol": "USDC"}}}}}},
            )
            self._write_json(
                root,
                f"earn-verified-ledger/arbitrum/{wallet}.json",
                {
                    "markets": {
                        "0": {
                            "strictStatus": "verified",
                            "strictMethod": "netflow+snapshot",
                            "strictReason": "exact_snapshot_netflow_match",
                            "status": "verified",
                            "method": "netflow+snapshot",
                            "canonicalHistoryCoverageStatus": "fresh",
                        }
                    }
                },
            )

            status = build_quality_status(data_dir=root)

        chain = status["chains"]["arbitrum"]
        self.assertEqual(chain["strictVerifiedMarketCount"], 0)
        self.assertEqual(chain["inferredMarketCount"], 1)
        self.assertEqual(chain["marketStatusCounts"], {"inferred": 1})
        self.assertEqual(chain["marketReasonCounts"], {"snapshot_netflow_match_requires_inference": 1})

    def test_mismatch_is_actionable_blocking_but_inference_is_not(self):
        wallet = "0x4444444444444444444444444444444444444444"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                "earn-snapshots/manifest.json",
                {"dates": ["2026-05-16"], "chains": {"2026-05-16": ["arbitrum"]}},
            )
            self._write_json(
                root,
                "earn-snapshots/2026-05-16.json",
                {
                    "date": "2026-05-16",
                    "snapshots": {
                        "arbitrum": {
                            wallet: {"markets": {"0": {"symbol": "WETH"}, "1": {"symbol": "USDC"}}},
                        }
                    },
                },
            )
            self._write_json(
                root,
                f"earn-verified-ledger/arbitrum/{wallet}.json",
                {
                    "markets": {
                        "0": {"strictStatus": "mismatch", "strictReason": "canonical_history_mismatch"},
                        "1": {"strictStatus": "inferred", "strictReason": "pre_snapshot_carry_requires_inference"},
                    }
                },
            )

            status = build_quality_status(data_dir=root)

        chain = status["chains"]["arbitrum"]
        self.assertEqual(chain["blockingMarketCount"], 1)
        self.assertEqual(chain["actionableBlockingMarketCount"], 1)
        self.assertEqual(chain["inferredMarketCount"], 1)
        self.assertEqual(chain["mismatchMarketCount"], 1)


if __name__ == "__main__":
    unittest.main()
