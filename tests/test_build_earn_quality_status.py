import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import build_earn_quality_status as quality_module
from build_earn_quality_status import _ledger_market_quality, build_quality_status


class EarnQualityStatusTest(unittest.TestCase):
    def _write_json(self, root: Path, rel: str, payload: dict) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _write_snapshot(self, root: Path, wallets: dict) -> None:
        self._write_json(root, "earn-snapshots/manifest.json", {
            "dates": ["2026-10-20"], "chains": {"2026-10-20": ["ethereum"]},
        })
        self._write_json(root, "earn-snapshots/2026-10-20.json", {
            "snapshots": {"ethereum": wallets},
        })

    def test_diagnostics_keep_exact_mismatch_evidence_and_separate_non_strict_causes(self):
        wallet = "0x1111111111111111111111111111111111111111"
        missing_wallet = "0x2222222222222222222222222222222222222222"
        exact = "900719925474099312345678901234567890"
        raw_market = {
            "symbol": "USDC", "decimals": 6,
            "strictStatus": "mismatch", "strictMethod": "snapshot-fallback",
            "strictReason": "snapshot_netflow_mismatch",
            "status": "mismatch", "method": "snapshot-fallback",
            "snapshotYield": exact, "netflowYield": exact + "1", "diff": "56820297",
            "recentCycleDiff": "-" + exact,
            "firstDate": "2026-08-31", "firstPar": exact, "firstWei": exact,
            "lastDate": "2026-10-20", "lastPar": exact, "lastWei": exact,
            "canonicalHistoryCoverageStatus": "fresh",
            "canonicalHistoryComparisonBlock": 26233099,
            "canonicalHistoryLastScannedBlock": 26233172,
            "canonicalHistoryConsistencyStatus": "match",
            "canonicalHistoryNetflowDiffWei": "0",
        }
        verification = {
            "rawVerified": False, "snapshotIncomplete": False,
            "subgraphReplayTruncated": False, "replayStateAdjusted": False,
            "expectedSupplyWei": exact, "actualSupplyWei": exact + "1",
        }
        ledger = {
            "snapshotDate": "2026-10-20", "generatedAt": "2026-10-20T10:25:57Z",
            "canonicalHistory": {
                "coverageStatus": "fresh", "lastScannedBlock": 26233172,
                "comparisonBlock": 26233099,
            },
            "markets": {
                "0": {"strictStatus": "verified"},
                "1": raw_market,
                "2": {"strictStatus": "inferred", "strictReason": "pre_snapshot_carry_requires_inference"},
                "3": {"strictStatus": "coverage_incomplete", "strictMethod": "insufficient-history",
                      "strictReason": "snapshot_missing_market"},
                "4": {"strictStatus": "coverage_incomplete", "strictMethod": "canonical-history-coverage",
                      "strictReason": "canonical_history_not_fresh", "canonicalHistoryCoverageStatus": "stale"},
            },
            "resolvedInterestLedger": {
                "strictStatus": "verified", "strictMethod": "interest-ledger",
                "comparisonBlock": 26233099,
                "markets": {"1": {"strictStatus": "verified", "strictMethod": "interest-ledger"}},
                "replayVerificationData": {"1": verification},
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_snapshot(root, {
                wallet: {"markets": {str(i): {"symbol": "USDC"} for i in range(6)}},
                missing_wallet: {"markets": {"0": {"symbol": "WETH"}}},
            })
            self._write_json(root, f"earn-verified-ledger/ethereum/{wallet}.json", ledger)
            ledger_before = (root / f"earn-verified-ledger/ethereum/{wallet}.json").read_text()
            with patch.object(quality_module, "_read_json", wraps=quality_module._read_json) as read_json:
                status = build_quality_status(data_dir=root)
            ledger_after = (root / f"earn-verified-ledger/ethereum/{wallet}.json").read_text()

        chain = status["chains"]["ethereum"]
        diagnostics = chain["nonStrictDiagnostics"]
        groups = diagnostics["byReason"]
        sample = groups["snapshot_netflow_mismatch"]["samples"][0]
        self.assertEqual(1, chain["strictVerifiedMarketCount"])
        self.assertEqual(1, chain["mismatchMarketCount"])
        self.assertEqual({"hasVerifiedMarket": 1, "hasNonStrictMarket": 2}, chain["addressStatusCounts"])
        self.assertEqual({"mismatch": 1, "inferred": 1, "source_missing": 3, "coverage_backlog": 1},
                         diagnostics["categoryCounts"])
        self.assertEqual(6, diagnostics["sampleCount"])
        self.assertEqual(wallet, sample["address"])
        self.assertEqual("1", sample["marketId"])
        self.assertEqual("mismatch", sample["category"])
        self.assertEqual("mismatch", sample["status"])
        self.assertEqual("56820297", sample["marketData"]["diff"])
        self.assertEqual("0", sample["marketData"]["canonicalHistoryNetflowDiffWei"])
        self.assertEqual(exact, sample["marketData"]["firstPar"])
        self.assertEqual("-" + exact, sample["marketData"]["recentCycleDiff"])
        self.assertEqual(26233099, sample["source"]["canonicalHistory"]["comparisonBlock"])
        self.assertEqual(26233172, sample["marketData"]["canonicalHistoryLastScannedBlock"])
        self.assertEqual(26233099, sample["source"]["resolvedInterestLedger"]["comparisonBlock"])
        self.assertEqual(verification, sample["source"]["resolvedInterestLedger"]["replayVerificationData"])
        self.assertEqual("missing_ledger", groups["missing_ledger"]["samples"][0]["status"])
        self.assertEqual({}, groups["missing_ledger"]["samples"][0]["marketData"])
        self.assertEqual({}, groups["missing_market"]["samples"][0]["marketData"])
        self.assertEqual("stale", groups["canonical_history_not_fresh"]["samples"][0]["coverage"])
        ledger_reads = [call for call in read_json.call_args_list if "earn-verified-ledger" in str(call.args[0])]
        self.assertEqual(2, len(ledger_reads))
        self.assertEqual(ledger_before, ledger_after)
        self.assertEqual(ledger, json.loads(ledger_after))

    def test_diagnostic_samples_are_bounded_deterministic_and_mismatch_first(self):
        wallet = "0x1111111111111111111111111111111111111111"
        markets = {}
        for reason_id in range(12):
            for sample_id in range(6):
                mid = str(reason_id * 6 + sample_id)
                markets[mid] = {
                    "strictStatus": "mismatch" if reason_id == 11 else "inferred",
                    "strictReason": f"reason_{reason_id:02d}",
                }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_snapshot(root, {wallet: {"markets": dict(reversed(list(markets.items())))}})
            self._write_json(root, f"earn-verified-ledger/ethereum/{wallet}.json", {"markets": markets})
            first = build_quality_status(data_dir=root)["chains"]["ethereum"]
            self._write_snapshot(root, {wallet: {"markets": markets}})
            second = build_quality_status(data_dir=root)["chains"]["ethereum"]

        diagnostics = first["nonStrictDiagnostics"]
        self.assertEqual(diagnostics, second["nonStrictDiagnostics"])
        self.assertEqual(50, diagnostics["sampleLimit"])
        self.assertEqual(5, diagnostics["samplesPerReasonLimit"])
        self.assertEqual(50, diagnostics["sampleCount"])
        self.assertEqual(22, diagnostics["omittedMarketCount"])
        self.assertEqual(6, first["mismatchMarketCount"])
        self.assertEqual(66, first["inferredMarketCount"])
        self.assertEqual(0, first["strictVerifiedMarketCount"])
        groups = diagnostics["byReason"]
        self.assertEqual(12, len(groups))
        self.assertEqual(["66", "67", "68", "69", "70"],
                         [sample["marketId"] for sample in groups["reason_11"]["samples"]])
        for reason, group in groups.items():
            self.assertEqual(6, group["marketCount"])
            self.assertLessEqual(len(group["samples"]), 5)
            self.assertEqual(6 - len(group["samples"]), group["omittedMarketCount"])
            self.assertEqual(6, first["marketReasonCounts"][reason])
        self.assertEqual(50, sum(len(group["samples"]) for group in groups.values()))

    def test_verified_only_chain_has_empty_diagnostics(self):
        wallet = "0x1111111111111111111111111111111111111111"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_snapshot(root, {wallet: {"markets": {"0": {}}}})
            self._write_json(root, f"earn-verified-ledger/ethereum/{wallet}.json", {
                "markets": {"0": {"strictStatus": "verified"}},
            })
            chain = build_quality_status(data_dir=root)["chains"]["ethereum"]
        self.assertEqual(1, chain["strictVerifiedMarketCount"])
        self.assertEqual({}, chain["nonStrictDiagnostics"]["byReason"])
        self.assertEqual({}, chain["nonStrictDiagnostics"]["categoryCounts"])
        self.assertEqual(0, chain["nonStrictDiagnostics"]["sampleCount"])
        self.assertEqual(0, chain["nonStrictDiagnostics"]["omittedMarketCount"])

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
