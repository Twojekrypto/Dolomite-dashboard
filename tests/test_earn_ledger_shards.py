import json
import tempfile
import unittest
from pathlib import Path

from build_earn_verified_ledger_shards import build_chain_shards, decode_compact_ledger


class EarnLedgerShardsTest(unittest.TestCase):
    def test_legacy_markets_get_explicit_unavailable_historical_pnl(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source" / "arbitrum"
            output = root / "public"
            source.mkdir(parents=True)
            address = "0xab11111111111111111111111111111111111111"
            (source / f"{address}.json").write_text(json.dumps({
                "snapshotDate": "2026-07-11",
                "markets": {
                    "1": {
                        "symbol": "dGM",
                        "decimals": 18,
                        "cumulativeYield": "0",
                    }
                },
            }), encoding="utf-8")

            build_chain_shards(source.parent, output, "arbitrum")

            shard = json.loads((output / "arbitrum" / "ab.json").read_text(encoding="utf-8"))
            market = decode_compact_ledger(shard, shard["ledgers"][address])["markets"]["1"]
            self.assertEqual(market["historicalYieldUsd"], "0")
            self.assertEqual(market["historicalYieldEligibleIntervals"], 0)
            self.assertEqual(market["historicalYieldPricedIntervals"], 0)
            self.assertEqual(market["historicalYieldMissingPriceIntervals"], 0)
            self.assertEqual(market["historicalYieldValuationStatus"], "unavailable")
            self.assertEqual(market["historicalYieldValuationMethod"], "daily-snapshot-constant-par")

    def test_builds_address_prefix_shards_with_compact_runtime_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source" / "arbitrum"
            output = root / "public"
            source.mkdir(parents=True)
            address = "0xab11111111111111111111111111111111111111"
            (source / f"{address}.json").write_text(json.dumps({
                "version": 2,
                "chain": "arbitrum",
                "address": address,
                "snapshotDate": "2026-07-12",
                "generatedAt": "2026-07-13T00:00:00Z",
                "summary": {"verified": 1},
                "strictSummary": {"inferred": 1},
                "canonicalHistory": {"coverageStatus": "fresh"},
                "markets": {
                    "1": {
                        "symbol": "USDC",
                        "decimals": 6,
                        "cumulativeYield": "123",
                        "strictStatus": "inferred",
                        "strictMethod": "netflow+snapshot",
                        "canonicalHistoryCoverageStatus": "fresh",
                        "historicalYieldUsd": "1.25",
                        "historicalYieldValuationStatus": "complete",
                        "historicalYieldEligibleIntervals": 2,
                        "historicalYieldPricedIntervals": 2,
                        "debugOnly": "drop-me",
                    }
                },
            }), encoding="utf-8")

            meta = build_chain_shards(root / "source", output, "arbitrum")

            shard = json.loads((output / "arbitrum" / "ab.json").read_text(encoding="utf-8"))
            ledger = decode_compact_ledger(shard, shard["ledgers"][address])
            self.assertEqual(meta["addressCount"], 1)
            self.assertEqual(meta["prefixLength"], 2)
            self.assertEqual(ledger["snapshotDate"], "2026-07-12")
            self.assertEqual(ledger["markets"]["1"]["historicalYieldUsd"], "1.25")
            self.assertNotIn("debugOnly", ledger["markets"]["1"])

    def test_incremental_rebuild_preserves_unselected_published_ledgers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source" / "arbitrum"
            output = root / "public"
            source.mkdir(parents=True)
            (output / "arbitrum").mkdir(parents=True)
            old_address = "0xab11111111111111111111111111111111111111"
            new_address = "0xab22222222222222222222222222222222222222"
            old_ledger = {"snapshotDate": "2026-07-11", "markets": {"1": {"symbol": "USDC", "decimals": 6, "cumulativeYield": "1"}}}
            (output / "arbitrum" / "ab.json").write_text(json.dumps({"ledgers": {old_address: old_ledger}}), encoding="utf-8")
            (source / f"{new_address}.json").write_text(json.dumps({"snapshotDate": "2026-07-12", "markets": {"2": {"symbol": "WETH", "decimals": 18, "cumulativeYield": "2"}}}), encoding="utf-8")

            build_chain_shards(source.parent, output, "arbitrum", updated_addresses={new_address})

            shard = json.loads((output / "arbitrum" / "ab.json").read_text(encoding="utf-8"))
            self.assertEqual(shard["version"], 2)
            self.assertEqual(set(shard["ledgers"]), {old_address, new_address})
            self.assertEqual(decode_compact_ledger(shard, shard["ledgers"][new_address])["markets"]["2"]["symbol"], "WETH")

    def test_same_snapshot_legacy_source_does_not_downgrade_published_pnl(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source" / "arbitrum"
            output = root / "public"
            source.mkdir(parents=True)
            (output / "arbitrum").mkdir(parents=True)
            address = "0xab11111111111111111111111111111111111111"
            published = {
                "snapshotDate": "2026-07-13",
                "markets": {
                    "1": {
                        "symbol": "USDC",
                        "decimals": 6,
                        "cumulativeYield": "123",
                        "historicalYieldUsd": "4.25",
                        "historicalYieldEligibleIntervals": 3,
                        "historicalYieldPricedIntervals": 3,
                        "historicalYieldSkippedFlowIntervals": 1,
                        "historicalYieldMissingPriceIntervals": 0,
                        "historicalYieldValuationStatus": "complete",
                        "historicalYieldValuationMethod": "daily-snapshot-constant-par",
                    }
                },
            }
            (output / "arbitrum" / "ab.json").write_text(json.dumps({"ledgers": {address: published}}), encoding="utf-8")
            (source / f"{address}.json").write_text(json.dumps({
                "snapshotDate": "2026-07-13",
                "markets": {"1": {"symbol": "USDC", "decimals": 6, "cumulativeYield": "1"}},
            }), encoding="utf-8")

            build_chain_shards(source.parent, output, "arbitrum")

            shard = json.loads((output / "arbitrum" / "ab.json").read_text(encoding="utf-8"))
            market = decode_compact_ledger(shard, shard["ledgers"][address])["markets"]["1"]
            self.assertEqual(market["cumulativeYield"], "123")
            self.assertEqual(market["historicalYieldUsd"], "4.25")
            self.assertEqual(market["historicalYieldValuationStatus"], "complete")

    def test_older_source_snapshot_does_not_replace_newer_published_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source" / "arbitrum"
            output = root / "public"
            source.mkdir(parents=True)
            (output / "arbitrum").mkdir(parents=True)
            address = "0xab11111111111111111111111111111111111111"
            published = {"snapshotDate": "2026-07-13", "markets": {"1": {"symbol": "USDC", "cumulativeYield": "123"}}}
            (output / "arbitrum" / "ab.json").write_text(json.dumps({"ledgers": {address: published}}), encoding="utf-8")
            (source / f"{address}.json").write_text(json.dumps({
                "snapshotDate": "2026-07-11",
                "markets": {"1": {"symbol": "USDC", "cumulativeYield": "1"}},
            }), encoding="utf-8")

            build_chain_shards(source.parent, output, "arbitrum")

            shard = json.loads((output / "arbitrum" / "ab.json").read_text(encoding="utf-8"))
            ledger = decode_compact_ledger(shard, shard["ledgers"][address])
            self.assertEqual(ledger["snapshotDate"], "2026-07-13")
            self.assertEqual(ledger["markets"]["1"]["cumulativeYield"], "123")


if __name__ == "__main__":
    unittest.main()
