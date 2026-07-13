import json
import tempfile
import unittest
from pathlib import Path

from build_earn_representative_audit import build_representative_audit


class EarnRepresentativeAuditTest(unittest.TestCase):
    def test_covers_every_market_in_latest_chain_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshots = root / "snapshots"
            shards = root / "shards"
            snapshots.mkdir()
            (snapshots / "manifest.json").write_text(json.dumps({
                "dates": ["2026-07-12"],
                "chains": {"2026-07-12": ["arbitrum"]},
            }), encoding="utf-8")
            wallet = "0xab11111111111111111111111111111111111111"
            (snapshots / "2026-07-12.json").write_text(json.dumps({
                "snapshots": {"arbitrum": {wallet: {"markets": {
                    "1": {"token": "0x01", "symbol": "USDC", "decimals": 6, "par": "10", "wei": "11"},
                    "2": {"token": "0x02", "symbol": "WETH", "decimals": 18, "par": "2", "wei": "3"},
                }}}}
            }), encoding="utf-8")
            (shards / "arbitrum").mkdir(parents=True)
            (shards / "manifest.json").write_text(json.dumps({
                "version": 1,
                "prefixLength": 2,
                "chains": {"arbitrum": {"snapshotDate": "2026-07-12", "shards": {"ab": {"path": "arbitrum/ab.json"}}}},
            }), encoding="utf-8")
            markets = {
                mid: {
                    "token": token,
                    "symbol": symbol,
                    "decimals": decimals,
                    "cumulativeYield": "1",
                    "strictStatus": "inferred",
                    "canonicalHistoryCoverageStatus": "fresh",
                    "historicalYieldUsd": "0.01",
                    "historicalYieldValuationStatus": "complete",
                    "historicalYieldEligibleIntervals": 1,
                    "historicalYieldPricedIntervals": 1,
                    "historicalYieldSkippedFlowIntervals": 0,
                    "historicalYieldMissingPriceIntervals": 0,
                    "lastDate": "2026-07-12",
                    "lastPar": "10" if mid == "1" else "2",
                    "lastWei": "11" if mid == "1" else "3",
                    "isLatestSnapshot": True,
                }
                for mid, token, symbol, decimals in [
                    ("1", "0x01", "USDC", 6),
                    ("2", "0x02", "WETH", 18),
                ]
            }
            (shards / "arbitrum" / "ab.json").write_text(json.dumps({
                "version": 1,
                "chain": "arbitrum",
                "prefix": "ab",
                "snapshotDate": "2026-07-12",
                "ledgers": {wallet: {"snapshotDate": "2026-07-12", "markets": markets}},
            }), encoding="utf-8")

            report = build_representative_audit(snapshots, shards, representatives_per_market=1)

            self.assertEqual(report["status"], "pass")
            self.assertEqual(report["summary"]["marketCount"], 2)
            self.assertEqual(report["summary"]["failedMarketCount"], 0)
            self.assertEqual({row["marketId"] for row in report["markets"]}, {"1", "2"})

    def test_stale_or_mismatched_published_snapshot_is_a_deployment_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshots = root / "snapshots"
            shards = root / "shards"
            snapshots.mkdir()
            (shards / "arbitrum").mkdir(parents=True)
            wallet = "0xab11111111111111111111111111111111111111"
            (snapshots / "manifest.json").write_text(json.dumps({
                "dates": ["2026-07-12"],
                "chains": {"2026-07-12": ["arbitrum"]},
            }), encoding="utf-8")
            snapshot_market = {"token": "0x01", "symbol": "USDC", "decimals": 6, "par": "10", "wei": "11"}
            (snapshots / "2026-07-12.json").write_text(json.dumps({
                "snapshots": {"arbitrum": {wallet: {"markets": {"1": snapshot_market}}}},
            }), encoding="utf-8")
            (shards / "manifest.json").write_text(json.dumps({"version": 1, "prefixLength": 2}), encoding="utf-8")
            (shards / "arbitrum" / "ab.json").write_text(json.dumps({
                "chain": "arbitrum",
                "prefix": "ab",
                "ledgers": {wallet: {"snapshotDate": "2026-07-11", "markets": {"1": {
                    **snapshot_market,
                    "cumulativeYield": "1",
                    "lastDate": "2026-07-11",
                    "lastPar": "10",
                    "lastWei": "9",
                    "isLatestSnapshot": False,
                    "strictStatus": "inferred",
                    "canonicalHistoryCoverageStatus": "fresh",
                    "historicalYieldUsd": "0",
                    "historicalYieldValuationStatus": "unavailable",
                    "historicalYieldEligibleIntervals": 0,
                    "historicalYieldPricedIntervals": 0,
                    "historicalYieldSkippedFlowIntervals": 0,
                    "historicalYieldMissingPriceIntervals": 0,
                }}}},
            }), encoding="utf-8")

            report = build_representative_audit(snapshots, shards, representatives_per_market=1)

            self.assertEqual(report["status"], "fail")
            errors = report["markets"][0]["representatives"][0]["errors"]
            self.assertIn("stale_published_ledger", errors)
            self.assertIn("last_snapshot_date_mismatch", errors)
            self.assertIn("last_wei_mismatch", errors)
            self.assertIn("not_latest_snapshot", errors)

    def test_missing_published_ledger_is_a_deployment_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshots = root / "snapshots"
            shards = root / "shards"
            snapshots.mkdir()
            shards.mkdir()
            wallet = "0xcd11111111111111111111111111111111111111"
            (snapshots / "manifest.json").write_text(json.dumps({"dates": ["2026-07-12"], "chains": {"2026-07-12": ["berachain"]}}), encoding="utf-8")
            (snapshots / "2026-07-12.json").write_text(json.dumps({"snapshots": {"berachain": {wallet: {"markets": {"7": {"token": "0x07", "symbol": "HONEY", "decimals": 18, "par": "1", "wei": "1"}}}}}}), encoding="utf-8")
            (shards / "manifest.json").write_text(json.dumps({"version": 1, "prefixLength": 2, "chains": {}}), encoding="utf-8")

            report = build_representative_audit(snapshots, shards, representatives_per_market=1)

            self.assertEqual(report["status"], "fail")
            self.assertEqual(report["summary"]["failedMarketCount"], 1)


if __name__ == "__main__":
    unittest.main()
