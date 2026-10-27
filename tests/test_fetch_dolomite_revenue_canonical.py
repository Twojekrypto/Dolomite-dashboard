import copy
import json
import tempfile
import unittest
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import fetch_dolomite_revenue as revenue
import validate_data as validation


def canonical_history():
    chains = {}
    for name, chain_id, start in (("Berachain", 80094, 1), ("Arbitrum", 42161, 10)):
        rows = []
        for offset, delta in enumerate((100, -20)):
            rows.append({
                "epoch": start + offset, "ts": offset * 2 + 1,
                "endTimestamp": offset * 2 + 3, "status": "allocated" if delta > 0 else "correction",
                "deltaRaw": {"2": str(delta)}, "cumulativeUSD": 999999,
                "officialInterest": {
                    "status": "verified", "periodStartTimestamp": offset * 2 + 1,
                    "periodEndTimestamp": offset * 2 + 3,
                    "eligibleBorrowInterestUSD": 1000, "unscaledMaximumRebateUSD": 100,
                    "maximumRebateUSD": 100, "marketPrices": {"2": str(10 ** 36 * (offset + 1))},
                    "marketRevenueFactors": {"2": 1}, "marketBorrowInterestUSD": {"2": 1000},
                    "marketMaximumRebateUSD": {"2": 100},
                    "sourceUrl": f"https://raw.githubusercontent.com/dolomite-exchange/liquidity-mining-data/master/finalized/{chain_id}/borrow-interest/epoch-{start + offset}-output.json", "sha256": "a" * 64,
                },
            })
        chains[name] = {
            "chainId": chain_id, "startEpoch": start, "publishedEpoch": start + 1,
            "reconciled": True, "missingEpochs": [], "epochs": rows,
            "totalsRaw": {"2": "80"}, "referencePrices": {"2": str(10 ** 40)},
        }
    return {"generatedAt": "2026-10-26T00:00:00Z", "chains": chains}


def series():
    return [{
        "timestamp": day, "feesUSD": 600, "revenueUSD": 200,
        "supplySideRevenueUSD": 400,
        "chains": {name: {"feesUSD": 300, "revenueUSD": 100, "supplySideRevenueUSD": 200}
                   for name in ("Berachain", "Arbitrum")},
    } for day in range(1, 5)]


class CanonicalRebateTest(unittest.TestCase):
    def test_raw_signed_deltas_conserve_both_chains_and_use_each_epoch_closing_price(self):
        data = revenue.canonical_borrow_fee_rebate_data(canonical_history())
        self.assertEqual(set(data["chains"]), {"Berachain", "Arbitrum"})
        for chain in data["chains"].values():
            self.assertEqual(chain["totalsRaw"], {"2": "80"})
            self.assertEqual([row["rebateUSD"] for row in chain["epochRebates"]], [100, -40])
            self.assertEqual(chain["totalRebateUSD"], 60)
            self.assertEqual(sum(int(row["markets"][0]["deltaRaw"]) for row in chain["epochRebates"]), 80)

    def test_rejects_missing_chain_unreconciled_raw_totals_and_duplicate_epochs(self):
        for mutation in (
            lambda p: p["chains"].pop("Arbitrum"),
            lambda p: p["chains"]["Berachain"].update(reconciled=False),
            lambda p: p["chains"]["Berachain"]["totalsRaw"].update({"2": "81"}),
            lambda p: p["chains"]["Berachain"]["epochs"].append(copy.deepcopy(p["chains"]["Berachain"]["epochs"][0])),
            lambda p: p["chains"]["Berachain"].update(missingEpochs=[1]),
        ):
            payload = canonical_history()
            mutation(payload)
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                revenue.canonical_borrow_fee_rebate_data(payload)

    def test_rejects_missing_price_or_unverified_official_interest(self):
        for mutation in (
            lambda row: row.pop("officialInterest"),
            lambda row: row["officialInterest"].update(status="missing"),
            lambda row: row["officialInterest"]["marketPrices"].clear(),
            lambda row: row["deltaRaw"].update({"2": 1.5}),
            lambda row: row["deltaRaw"].update({"2": True}),
        ):
            payload = canonical_history()
            mutation(payload["chains"]["Berachain"]["epochs"][0])
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                revenue.canonical_borrow_fee_rebate_data(payload)

    def test_preserves_official_source_anomaly_and_market_coverage_mismatch(self):
        payload = canonical_history()
        official = payload["chains"]["Berachain"]["epochs"][0]["officialInterest"]
        official["maximumRebateUSD"] = 1
        official["marketMaximumRebateUSD"] = {"3": 1}
        data = revenue.canonical_borrow_fee_rebate_data(payload)
        row = data["chains"]["Berachain"]["epochRebates"][0]
        self.assertEqual(row["maxRebateAuditStatus"], "source_anomaly")
        self.assertEqual(row["maxRebateCoverageStatus"], "mismatch")
        self.assertEqual(row["maxRebatePublishedOnlyMarketIds"], ["2"])
        self.assertEqual(row["maxRebateOfficialOnlyMarketIds"], ["3"])

    def test_live_metadata_rejects_stale_canonical_history(self):
        data = revenue.canonical_borrow_fee_rebate_data(canonical_history())
        metadata = {"onchainFeeRebateEpochIndexMap": {"80094": 3, "42161": 11}}
        with self.assertRaisesRegex(ValueError, "stale"):
            revenue.normalized_borrow_fee_rebate_metadata(metadata, data)

    def test_signed_daily_allocation_preserves_gross_and_supply(self):
        rows = series()
        before = copy.deepcopy(rows)
        data = revenue.canonical_borrow_fee_rebate_data(canonical_history())
        out = revenue.apply_borrow_fee_rebates(rows, data)
        self.assertEqual([row["borrowFeeRebateUSD"] for row in out], [100, 100, -40, -40])
        self.assertEqual(sum(Decimal(str(row["borrowFeeRebateUSD"])) for row in out), 120)
        for original, row in zip(before, out):
            self.assertEqual(row["feesUSD"], original["feesUSD"])
            self.assertEqual(row["grossRevenueUSD"], original["revenueUSD"])
            self.assertEqual(row["supplySideRevenueUSD"], original["supplySideRevenueUSD"])
            for name, chain in row["chains"].items():
                self.assertEqual(chain["grossRevenueUSD"], original["chains"][name]["revenueUSD"])
                self.assertEqual(chain["supplySideRevenueUSD"], original["chains"][name]["supplySideRevenueUSD"])
                self.assertEqual(chain["revenueUSD"], chain["grossRevenueUSD"] - chain["borrowFeeRebateUSD"])

    def test_signed_corrections_require_explicit_canonical_mode(self):
        with self.assertRaises(ValueError):
            revenue.apply_borrow_fee_rebates(series(), {"chains": {"Berachain": {"epochRebates": [{
                "periodStartTimestamp": 1, "periodEndTimestamp": 3, "rebateUSD": -1,
            }]}}})

    def test_missing_allocation_window_refuses_to_lose_negative_correction(self):
        data = revenue.canonical_borrow_fee_rebate_data(canonical_history())
        with self.assertRaisesRegex(ValueError, "daily rows"):
            revenue.apply_borrow_fee_rebates(series()[:2], data)

    def test_normalization_keeps_canonical_chains_when_metadata_is_unavailable(self):
        data = revenue.canonical_borrow_fee_rebate_data(canonical_history())
        normalized = revenue.normalized_borrow_fee_rebate_metadata(None, data, {"chains": {}})
        self.assertEqual(set(normalized["chains"]), {"Berachain", "Arbitrum"})
        self.assertEqual(normalized["dataStatus"], "canonical_reconciled")

    def test_all_time_totals_equal_saved_series_after_overrides(self):
        rows = revenue.apply_borrow_fee_rebates(series(), revenue.canonical_borrow_fee_rebate_data(canonical_history()))
        totals = revenue.metric_totals({"totalAllTime": 99999}, {"totalAllTime": 88888}, rows)
        self.assertEqual(totals["grossRevenueAllTimeUSD"], 800)
        self.assertEqual(totals["borrowFeeRebateAllTimeUSD"], 120)
        self.assertEqual(totals["revenueAllTimeUSD"], 680)
        self.assertEqual(totals["feesAllTimeUSD"], 2400)

    def test_missing_canonical_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises((OSError, ValueError)):
                revenue.load_canonical_borrow_fee_rebate_data(Path(tmp) / "absent.json")

    def test_build_output_uses_canonical_netting_for_both_chains(self):
        fees = {"totalDataChart": [[day, 600] for day in range(1, 32)],
                "totalDataChartBreakdown": [[day, {"Berachain": {"interest": 300}, "Arbitrum": {"interest": 300}}]
                                            for day in range(1, 32)]}
        gross = {"totalDataChart": [[day, 200] for day in range(1, 32)],
                 "totalDataChartBreakdown": [[day, {"Berachain": {"interest": 100}, "Arbitrum": {"interest": 100}}]
                                             for day in range(1, 32)]}
        out = revenue.build_output(gross, fees, onchain_audit={}, onchain_revenue_overrides={},
                                   borrow_fee_rebate_data=revenue.canonical_borrow_fee_rebate_data(canonical_history()))
        self.assertEqual(out["totals"]["grossRevenueAllTimeUSD"], 6200)
        self.assertEqual(out["totals"]["revenueAllTimeUSD"], 6080)
        self.assertEqual(out["totals"]["borrowFeeRebateAllTimeUSD"], 120)
        self.assertEqual(set(out["borrowFeeRebates"]["chains"]), {"Berachain", "Arbitrum"})

    def test_retired_zero_chains_do_not_clamp_post_retirement_signed_corrections(self):
        start = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp())
        history = canonical_history()
        for chain in history["chains"].values():
            for offset, row in enumerate(chain["epochs"]):
                row["ts"] = start + offset * 7 * 86400
                row["endTimestamp"] = start + (offset + 1) * 7 * 86400
                row["officialInterest"]["periodStartTimestamp"] = row["ts"]
                row["officialInterest"]["periodEndTimestamp"] = row["endTimestamp"]
        gross, fees = {}, {}
        for payload, value in ((gross, 100), (fees, 300)):
            retired_value = 1 if payload is fees else 0
            payload["totalDataChart"] = [[start + day * 86400, 2 * value + 2 * retired_value] for day in range(31)]
            payload["totalDataChartBreakdown"] = [[start + day * 86400, {
                "Berachain": {"interest": value}, "Arbitrum": {"interest": value},
                "Mantle": {"interest": retired_value}, "X Layer": {"interest": retired_value},
            }] for day in range(31)]
        out = revenue.build_output(gross, fees, onchain_audit={}, onchain_revenue_overrides={},
                                   borrow_fee_rebate_data=revenue.canonical_borrow_fee_rebate_data(history))
        self.assertEqual(out["totals"]["borrowFeeRebateAllTimeUSD"], 120)
        self.assertEqual(out["totals"]["revenueAllTimeUSD"], 6080)
        for row in out["series"][7:14]:
            self.assertLess(row["borrowFeeRebateUSD"], 0)
            self.assertNotIn("Mantle", row["chains"])
            self.assertNotIn("X Layer", row["chains"])
            self.assertEqual(Decimal(str(row["borrowFeeRebateUSD"])), sum(Decimal(str(chain["borrowFeeRebateUSD"])) for chain in row["chains"].values()))
            self.assertEqual(Decimal(str(row["grossRevenueUSD"])) - Decimal(str(row["borrowFeeRebateUSD"])), Decimal(str(row["revenueUSD"])))
        self.assertTrue(validation._dolomite_revenue_borrow_fee_rebate_max_audits_valid(out))
        self.assertTrue(validation._dolomite_revenue_series_valid(out))

    def test_retired_zero_payloads_preserve_signed_rebate_in_daily_scope(self):
        row = {"date": "2026-10-01", "feesUSD": 500, "grossRevenueUSD": 100,
               "borrowFeeRebateUSD": -20, "revenueUSD": 120, "supplySideRevenueUSD": 400,
               "chains": {"Berachain": {"feesUSD": 500, "grossRevenueUSD": 100,
                           "borrowFeeRebateUSD": -20, "revenueUSD": 120, "supplySideRevenueUSD": 400},
                          "Mantle": {"feesUSD": 0, "grossRevenueUSD": 0, "borrowFeeRebateUSD": 0,
                                     "revenueUSD": 0, "supplySideRevenueUSD": 0}, "X Layer": {}}}
        scoped = revenue.apply_active_network_scope([row])[0]
        self.assertEqual(scoped["borrowFeeRebateUSD"], -20)
        self.assertEqual(scoped["revenueUSD"], 120)
        self.assertEqual(set(scoped["chains"]), {"Berachain"})

    def test_main_never_fetches_legacy_netting_or_writes_when_canonical_missing(self):
        with mock.patch.object(revenue, "fetch_metric", return_value={}), \
             mock.patch.object(revenue, "fetch_borrow_fee_rebate_metadata", return_value=None), \
             mock.patch.object(revenue, "load_canonical_borrow_fee_rebate_data", side_effect=ValueError("missing canonical")), \
             mock.patch.object(revenue, "fetch_borrow_fee_rebate_data") as legacy, \
             mock.patch.object(revenue, "write_onchain_revenue_overrides") as write:
            with self.assertRaisesRegex(ValueError, "missing canonical"):
                revenue.main()
            legacy.assert_not_called()
            write.assert_not_called()

    def test_validator_accepts_signed_canonical_corrections(self):
        rebates = revenue.normalized_borrow_fee_rebate_metadata(None, revenue.canonical_borrow_fee_rebate_data(canonical_history()))
        rows = revenue.apply_borrow_fee_rebates(series(), rebates)
        data = {"borrowFeeRebates": rebates, "series": rows, "latest": rows[-1],
                "totals": revenue.metric_totals({}, {}, rows)}
        self.assertTrue(validation._dolomite_revenue_borrow_fee_rebate_max_audits_valid(data))
        self.assertTrue(validation._dolomite_revenue_totals_valid(data))

    def test_validator_rejects_raw_drift_prices_provenance_and_missing_chain(self):
        baseline = json.loads((Path(__file__).resolve().parents[1] / "dolomite_revenue.json").read_text())
        self.assertTrue(validation._dolomite_revenue_borrow_fee_rebate_max_audits_valid(baseline))
        for mutation in (
            lambda p: p["borrowFeeRebates"]["chains"].pop("Arbitrum"),
            lambda p: p["borrowFeeRebates"]["chains"]["Berachain"]["totalsRaw"].update({"2": "249426065"}),
            lambda p: p["borrowFeeRebates"]["chains"]["Berachain"]["epochRebates"][0]["markets"][0].update(encodedPrice="1"),
            lambda p: p["borrowFeeRebates"]["chains"]["Berachain"]["epochRebates"][0].update(maxRebateSourceSha256=""),
            lambda p: p["borrowFeeRebates"]["chains"]["Berachain"]["epochRebates"][0].update(maxRebateSourceUrl="https://wrong.example/epoch.json"),
            lambda p: p["series"][-1]["chains"]["Arbitrum"].update(borrowFeeRebateUSD=1),
        ):
            payload = copy.deepcopy(baseline)
            mutation(payload)
            with self.subTest(mutation=mutation):
                self.assertFalse(validation._dolomite_revenue_borrow_fee_rebate_max_audits_valid(payload))

    def test_validator_rejects_all_time_aggregate_drift(self):
        data = json.loads((Path(__file__).resolve().parents[1] / "dolomite_revenue.json").read_text())
        data["totals"]["revenueAllTimeUSD"] += 2
        self.assertFalse(validation._dolomite_revenue_window_totals_valid(data))

    def test_next_generation_history_immutable_urls_survive_production_netting_and_validation(self):
        root = Path(__file__).resolve().parents[1]
        history = json.loads((root / "veborrow_rebate_history.json").read_text())
        for index, chain in enumerate(history["chains"].values(), start=1):
            revision = str(index) * 40
            chain["officialInterestSourceCommit"] = revision
            for row in chain["epochs"]:
                row["officialInterest"]["sourceUrl"] = f"https://raw.githubusercontent.com/dolomite-exchange/liquidity-mining-data/{revision}/finalized/{chain['chainId']}/borrow-interest/epoch-{row['epoch']}-output.json"
        rebates = revenue.normalized_borrow_fee_rebate_metadata(None, revenue.canonical_borrow_fee_rebate_data(history))
        data = json.loads((root / "dolomite_revenue.json").read_text())
        for row in data["series"]:
            for payload in [row, *row["chains"].values()]:
                payload["revenueUSD"] = payload["grossRevenueUSD"]
                payload.pop("borrowFeeRebateCalculationMode", None)
        data["series"] = revenue.apply_borrow_fee_rebates(data["series"], rebates)
        data["borrowFeeRebates"] = rebates
        self.assertTrue(validation._dolomite_revenue_borrow_fee_rebate_max_audits_valid(data))
        for bad_revision in ("1" * 39, "1" * 41, "z" * 40, "main"):
            tampered = copy.deepcopy(data)
            row = tampered["borrowFeeRebates"]["chains"]["Berachain"]["epochRebates"][0]
            row["maxRebateSourceUrl"] = f"https://raw.githubusercontent.com/dolomite-exchange/liquidity-mining-data/{bad_revision}/finalized/80094/borrow-interest/epoch-{row['epoch']}-output.json"
            with self.subTest(revision=bad_revision):
                self.assertFalse(validation._dolomite_revenue_borrow_fee_rebate_max_audits_valid(tampered))

    def test_immutable_source_url_requires_the_correct_chain_and_epoch(self):
        data = json.loads((Path(__file__).resolve().parents[1] / "dolomite_revenue.json").read_text())
        row = data["borrowFeeRebates"]["chains"]["Berachain"]["epochRebates"][0]
        valid_url = f"https://raw.githubusercontent.com/dolomite-exchange/liquidity-mining-data/{'a' * 40}/finalized/80094/borrow-interest/epoch-{row['epoch']}-output.json"
        for bad_url in (valid_url.replace("/80094/", "/42161/"), valid_url.replace("epoch-1-", "epoch-2-"), valid_url + "?ref=master"):
            row["maxRebateSourceUrl"] = bad_url
            with self.subTest(url=bad_url):
                self.assertFalse(validation._dolomite_revenue_borrow_fee_rebate_max_audits_valid(data))
