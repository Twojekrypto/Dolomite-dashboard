import json
import tempfile
import unittest
from pathlib import Path

from build_earn_historical_prices import collect_chain_requirements, collect_requirements_for_chains, update_chain_prices


class BuildEarnHistoricalPricesTest(unittest.TestCase):
    def test_collects_multiple_chains_in_one_snapshot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            snapshots = Path(tmp) / "snapshots"
            snapshots.mkdir()
            (snapshots / "manifest.json").write_text(json.dumps({
                "dates": ["2026-07-01"],
                "chains": {"2026-07-01": ["arbitrum", "berachain"]},
            }), encoding="utf-8")
            wallet = "0x1111111111111111111111111111111111111111"
            (snapshots / "2026-07-01.json").write_text(json.dumps({"snapshots": {
                "arbitrum": {wallet: {"markets": {"1": {"token": "0xarb", "symbol": "ARB", "decimals": 18}}}},
                "berachain": {wallet: {"markets": {"2": {"token": "0xbera", "symbol": "BERA", "decimals": 18}}}},
            }}), encoding="utf-8")

            requirements = collect_requirements_for_chains(snapshots, {"arbitrum", "berachain"})

            self.assertEqual(set(requirements), {"arbitrum", "berachain"})
            self.assertEqual(requirements["arbitrum"]["0xarb"]["dates"], {"2026-07-01"})
            self.assertEqual(requirements["berachain"]["0xbera"]["dates"], {"2026-07-01"})

    def test_collects_unique_token_dates_and_updates_incrementally(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshots = root / "snapshots"
            snapshots.mkdir()
            (snapshots / "manifest.json").write_text(json.dumps({
                "dates": ["2026-07-01", "2026-07-02"],
                "chains": {"2026-07-01": ["arbitrum"], "2026-07-02": ["arbitrum"]},
            }), encoding="utf-8")
            wallet = "0x1111111111111111111111111111111111111111"
            for date in ("2026-07-01", "2026-07-02"):
                (snapshots / f"{date}.json").write_text(json.dumps({"snapshots": {"arbitrum": {wallet: {"markets": {
                    "1": {"token": "0xabc", "symbol": "WETH", "decimals": 18, "par": "1", "wei": "1"},
                }}}}}), encoding="utf-8")

            requirements = collect_chain_requirements(snapshots, "arbitrum")
            calls = []

            def fetcher(chain, date, tokens):
                calls.append((chain, date, tuple(tokens)))
                return {token: "123.45" for token in tokens}

            output = root / "arbitrum.json"
            first = update_chain_prices("arbitrum", requirements, output, fetcher=fetcher)
            second = update_chain_prices("arbitrum", requirements, output, fetcher=fetcher)

            self.assertEqual(len(calls), 2)
            self.assertEqual(first["prices"]["0xabc"]["2026-07-01"], "123.45")
            self.assertEqual(second["prices"], first["prices"])
            self.assertEqual(second["coverage"]["missingPriceCount"], 0)

    def test_yield_bearing_stable_wrapper_requires_a_market_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "berachain.json"
            calls = []

            def fetcher(chain, date, tokens):
                calls.append((chain, date, tuple(tokens)))
                return {tokens[0]: "1.087"}

            payload = update_chain_prices(
                "berachain",
                {"0xabc": {"symbol": "sUSDe", "decimals": 18, "dates": {"2026-07-01"}}},
                output,
                fetcher=fetcher,
            )

            self.assertEqual(calls, [("berachain", "2026-07-01", ("0xabc",))])
            self.assertEqual(payload["prices"]["0xabc"]["2026-07-01"], "1.087")
            self.assertEqual(payload["sources"]["0xabc"]["2026-07-01"], "defillama")


if __name__ == "__main__":
    unittest.main()
