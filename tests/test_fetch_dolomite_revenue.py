import unittest
import contextlib
import io
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fetch_dolomite_revenue import (
    build_output,
    expected_onchain_audit_target_date,
    fee_rebate_epoch_from_transaction_input,
    onchain_audit_assurance,
    preserve_previous_borrow_fee_rebate_data,
)
from web3 import Web3
from validate_data import (
    RULES,
    ValidationResult,
    _dolomite_revenue_chain_windows_valid,
    _dolomite_revenue_totals_valid,
    _dolomite_revenue_window_totals_valid,
    validate_file,
)


START_TS = 1_700_000_000
DAY_SECONDS = 86_400
ROOT = Path(__file__).resolve().parents[1]


def metric_payload(total24h, latest_value, step):
    chart_rows = []
    breakdown_rows = []
    for index in range(31):
        timestamp = START_TS + index * DAY_SECONDS
        value = latest_value if index == 30 else (index + 1) * step
        chart_rows.append([timestamp, value])
        breakdown_rows.append([timestamp, {"Ethereum": {"interest": value}}])

    return {
        "total24h": total24h,
        "total48hto24h": step,
        "total7d": step * 7,
        "total30d": step * 30,
        "totalAllTime": step * 300,
        "totalDataChart": chart_rows,
        "totalDataChartBreakdown": breakdown_rows,
    }


def metric_payload_with_berachain(total24h, latest_value, step, latest_berachain_value):
    payload = metric_payload(total24h, latest_value, step)
    for index, row in enumerate(payload["totalDataChart"]):
        value = row[1]
        berachain_value = latest_berachain_value if index == 30 else min(value, latest_berachain_value)
        ethereum_value = max(value - berachain_value, 0)
        payload["totalDataChartBreakdown"][index][1] = {
            "Berachain": {"interest": berachain_value},
            "Ethereum": {"interest": ethereum_value},
        }
    return payload


def borrow_fee_rebate_metadata():
    return {
        "veDoloStartTimestamp": 1779321600,
        "veDoloHoldingFactor": "5.000000000000000000",
        "currentEpochIndex": 6,
        "currentEpochStartTimestamp": 1782345600,
        "onchainFeeRebateEpochIndexMap": {"80094": 4},
        "onchainRollingClaimsEpochIndexMap": {"80094": 4},
        "allChainRebateInfo": {
            "80094": {
                "startEpoch": 1,
                "claimsEnabled": False,
                "rebatePercentage": "0.100000000000000006",
                "marketToRebateInfo": {
                    "0": {"startEpoch": 1, "endEpoch": None},
                    "1": {"startEpoch": 1, "endEpoch": None},
                },
            }
        },
    }


class FetchDolomiteRevenueTest(unittest.TestCase):
    def test_fee_rebate_epoch_uses_handler_expected_epoch_from_calldata(self):
        w3 = Web3()
        contract = w3.eth.contract(abi=[{
            "inputs": [
                {"internalType": "uint256[]", "name": "_marketIds", "type": "uint256[]"},
                {"internalType": "bytes32[]", "name": "_merkleRoots", "type": "bytes32[]"},
                {"internalType": "uint256[]", "name": "_totalAmounts", "type": "uint256[]"},
                {"internalType": "uint256", "name": "_expectedEpoch", "type": "uint256"},
                {"internalType": "bool", "name": "_incrementEpoch", "type": "bool"},
            ],
            "name": "handlerSetMerkleRoots",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        }])
        calldata = contract.encode_abi(
            "handlerSetMerkleRoots",
            args=[
                [0, 1],
                [b"\x11" * 32, b"\x22" * 32],
                [123, 456],
                5,
                True,
            ],
        )

        self.assertEqual(fee_rebate_epoch_from_transaction_input(w3, calldata), 5)

    def test_daily_totals_follow_latest_series_when_total24h_lags(self):
        revenue_data = metric_payload(total24h=9_999, latest_value=100, step=2)
        fees_data = metric_payload(total24h=8_888, latest_value=500, step=10)

        output = build_output(revenue_data, fees_data, onchain_audit=None)

        expected_revenue_7d = 100 + sum((index + 1) * 2 for index in range(24, 30))
        expected_fees_7d = 500 + sum((index + 1) * 10 for index in range(24, 30))
        expected_revenue_30d = 100 + sum((index + 1) * 2 for index in range(1, 30))
        expected_fees_30d = 500 + sum((index + 1) * 10 for index in range(1, 30))

        self.assertEqual(output["latest"]["revenueUSD"], 100)
        self.assertEqual(output["latest"]["feesUSD"], 500)
        self.assertNotIn("liquidatorEarningsUSD", output["latest"])
        self.assertEqual(output["totals"]["dailyRevenueUSD"], 100)
        self.assertEqual(output["totals"]["dailyFeesUSD"], 500)
        self.assertNotIn("dailyLiquidatorEarningsUSD", output["totals"])
        self.assertEqual(output["totals"]["dailySupplySideRevenueUSD"], 400)
        self.assertEqual(output["totals"]["previousDailyRevenueUSD"], 60)
        self.assertEqual(output["totals"]["previousDailyFeesUSD"], 300)
        self.assertEqual(output["totals"]["revenue7dUSD"], expected_revenue_7d)
        self.assertEqual(output["totals"]["fees7dUSD"], expected_fees_7d)
        self.assertNotIn("liquidatorEarnings7dUSD", output["totals"])
        self.assertEqual(output["totals"]["revenue30dUSD"], expected_revenue_30d)
        self.assertEqual(output["totals"]["fees30dUSD"], expected_fees_30d)
        self.assertNotIn("liquidatorEarnings30dUSD", output["totals"])
        self.assertNotIn("liquidatorEarningsAllTimeUSD", output["totals"])
        self.assertEqual(output["assurance"]["classification"], "hybrid adapter/current-index protocol borrow-interest revenue")
        self.assertEqual(output["assurance"]["berachainRevenueSource"], "current-index onchain audit for audited daily rows; DeFiLlama adapter fallback outside audited coverage")
        self.assertEqual(output["assurance"]["onchainOverrideRevenueSource"], "current-index onchain audit for audited Ethereum/Berachain/Mantle daily rows; DeFiLlama adapter fallback outside audited coverage")
        self.assertTrue(any("Ethereum, Berachain, and Mantle use the independent current-index onchain audit" in item for item in output["methodology"]["sourceLimitations"]))
        self.assertTrue(any("Current unfinalized rebate epochs remain gross until the weekly claim data is published" in item for item in output["methodology"]["sourceLimitations"]))
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_onchain_audit_assurance_marks_stale_target_date(self):
        assurance = onchain_audit_assurance(
            {
                "status": "warn",
                "generatedAt": "2026-06-24T10:04:22Z",
                "targetDate": "2026-06-22",
                "summary": {"maxRevenueDiffPct": 0.031, "revenueDiffUnbounded": True},
            },
            now=datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(assurance["onchainAuditStatus"], "stale")
        self.assertEqual(assurance["onchainAuditRawStatus"], "warn")
        self.assertEqual(assurance["onchainAuditTargetDate"], "2026-06-22")
        self.assertEqual(assurance["onchainAuditExpectedTargetDate"], "2026-06-23")
        self.assertTrue(assurance["onchainAuditStale"])

    def test_expected_onchain_audit_target_allows_morning_workflow_delay(self):
        self.assertEqual(
            expected_onchain_audit_target_date(datetime(2026, 6, 26, 8, 45, tzinfo=timezone.utc)),
            "2026-06-23",
        )
        self.assertEqual(
            expected_onchain_audit_target_date(datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)),
            "2026-06-24",
        )

    def test_onchain_audit_assurance_keeps_yesterdays_audit_fresh_during_grace_window(self):
        assurance = onchain_audit_assurance(
            {
                "status": "warn",
                "generatedAt": "2026-06-25T15:31:35Z",
                "targetDate": "2026-06-23",
                "summary": {"maxRevenueDiffPct": 0.031, "revenueDiffUnbounded": True},
            },
            now=datetime(2026, 6, 26, 8, 45, tzinfo=timezone.utc),
        )

        self.assertEqual(assurance["onchainAuditStatus"], "warn")
        self.assertEqual(assurance["onchainAuditTargetDate"], "2026-06-23")
        self.assertEqual(assurance["onchainAuditExpectedTargetDate"], "2026-06-23")
        self.assertFalse(assurance["onchainAuditStale"])

    def test_build_output_embeds_latest_onchain_audit_status(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={
                "status": "warn",
                "generatedAt": "2026-06-25T09:00:00Z",
                "targetDate": "2026-06-23",
                "summary": {"maxRevenueDiffPct": 0.031, "revenueDiffUnbounded": True},
                "chains": {
                    "Berachain": {
                        "status": "warn",
                        "feesUSD": 100.0,
                        "revenueUSD": 15.0,
                        "defillamaFeesUSD": 90.0,
                        "defillamaRevenueUSD": 13.5,
                        "revenueDiffPct": 0.11111111,
                        "feesDiffPct": 0.11111111,
                        "warnReasons": ["revenue_diff_exceeds_tolerance"],
                    }
                },
            },
            now=datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(output["assurance"]["onchainAuditStatus"], "warn")
        self.assertEqual(output["assurance"]["onchainAuditTargetDate"], "2026-06-23")
        self.assertEqual(output["assurance"]["onchainAuditMaxRevenueDiffPct"], 0.031)
        self.assertTrue(output["assurance"]["onchainAuditRevenueDiffUnbounded"])
        self.assertEqual(output["assurance"]["onchainAuditChains"]["Berachain"]["status"], "warn")
        self.assertEqual(output["assurance"]["onchainAuditChains"]["Berachain"]["warnReasons"], ["revenue_diff_exceeds_tolerance"])

    def test_build_output_uses_berachain_current_index_audit_values(self):
        revenue_data = metric_payload_with_berachain(
            total24h=100,
            latest_value=100,
            step=2,
            latest_berachain_value=20,
        )
        fees_data = metric_payload_with_berachain(
            total24h=500,
            latest_value=500,
            step=10,
            latest_berachain_value=100,
        )
        target_timestamp = START_TS + 30 * DAY_SECONDS
        target_date = datetime.fromtimestamp(target_timestamp, tz=timezone.utc).strftime("%Y-%m-%d")

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={
                "status": "warn",
                "generatedAt": "2026-06-25T09:00:00Z",
                "targetDate": target_date,
                "chains": {
                    "Berachain": {
                        "status": "warn",
                        "feesUSD": 80.0,
                        "revenueUSD": 16.0,
                        "defillamaFeesUSD": 100.0,
                        "defillamaRevenueUSD": 20.0,
                        "revenueDiffPct": 0.2,
                        "feesDiffPct": 0.2,
                        "warnReasons": ["fees_diff_exceeds_tolerance", "revenue_diff_exceeds_tolerance"],
                    }
                },
            },
        )

        bera = output["latest"]["chains"]["Berachain"]
        self.assertEqual(bera["feesUSD"], 80.0)
        self.assertEqual(bera["grossRevenueUSD"], 16.0)
        self.assertEqual(bera["revenueUSD"], 16.0)
        self.assertEqual(bera["defillamaFeesUSD"], 100.0)
        self.assertEqual(bera["defillamaGrossRevenueUSD"], 20.0)
        self.assertEqual(bera["source"], "onchain-current-index-audit")
        self.assertEqual(output["latest"]["feesUSD"], 480.0)
        self.assertEqual(output["latest"]["grossRevenueUSD"], 96.0)
        self.assertEqual(output["latest"]["revenueUSD"], 96.0)
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_build_output_uses_ethereum_current_index_audit_values(self):
        revenue_data = metric_payload_with_berachain(
            total24h=100,
            latest_value=100,
            step=2,
            latest_berachain_value=20,
        )
        fees_data = metric_payload_with_berachain(
            total24h=500,
            latest_value=500,
            step=10,
            latest_berachain_value=100,
        )
        target_timestamp = START_TS + 30 * DAY_SECONDS
        target_date = datetime.fromtimestamp(target_timestamp, tz=timezone.utc).strftime("%Y-%m-%d")

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={
                "status": "warn",
                "generatedAt": "2026-06-25T09:00:00Z",
                "targetDate": target_date,
                "chains": {
                    "Ethereum": {
                        "status": "warn",
                        "feesUSD": 360.0,
                        "revenueUSD": 72.0,
                        "defillamaFeesUSD": 400.0,
                        "defillamaRevenueUSD": 80.0,
                        "revenueDiffPct": 0.1,
                        "feesDiffPct": 0.1,
                        "warnReasons": ["fees_diff_exceeds_tolerance", "revenue_diff_exceeds_tolerance"],
                    }
                },
            },
        )

        eth = output["latest"]["chains"]["Ethereum"]
        self.assertEqual(eth["feesUSD"], 360.0)
        self.assertEqual(eth["grossRevenueUSD"], 72.0)
        self.assertEqual(eth["revenueUSD"], 72.0)
        self.assertEqual(eth["defillamaFeesUSD"], 400.0)
        self.assertEqual(eth["defillamaGrossRevenueUSD"], 80.0)
        self.assertEqual(eth["source"], "onchain-current-index-audit")
        self.assertEqual(output["latest"]["feesUSD"], 460.0)
        self.assertEqual(output["latest"]["grossRevenueUSD"], 92.0)
        self.assertEqual(output["latest"]["revenueUSD"], 92.0)
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_build_output_uses_saved_berachain_onchain_override_history(self):
        revenue_data = metric_payload_with_berachain(
            total24h=100,
            latest_value=100,
            step=2,
            latest_berachain_value=20,
        )
        fees_data = metric_payload_with_berachain(
            total24h=500,
            latest_value=500,
            step=10,
            latest_berachain_value=100,
        )
        target_timestamp = START_TS + 29 * DAY_SECONDS
        target_date = datetime.fromtimestamp(target_timestamp, tz=timezone.utc).strftime("%Y-%m-%d")

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={},
            onchain_revenue_overrides={
                "schemaVersion": 1,
                "overrides": [{
                    "chain": "Berachain",
                    "date": target_date,
                    "feesUSD": 70.0,
                    "revenueUSD": 14.0,
                    "defillamaFeesUSD": 100.0,
                    "defillamaGrossRevenueUSD": 20.0,
                    "source": "onchain-current-index-audit",
                }],
            },
        )

        bera = output["series"][29]["chains"]["Berachain"]
        self.assertEqual(bera["feesUSD"], 70.0)
        self.assertEqual(bera["grossRevenueUSD"], 14.0)
        self.assertEqual(bera["source"], "onchain-current-index-audit")
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_build_output_uses_saved_ethereum_onchain_override_history(self):
        revenue_data = metric_payload_with_berachain(
            total24h=100,
            latest_value=100,
            step=2,
            latest_berachain_value=20,
        )
        fees_data = metric_payload_with_berachain(
            total24h=500,
            latest_value=500,
            step=10,
            latest_berachain_value=100,
        )
        target_timestamp = START_TS + 29 * DAY_SECONDS
        target_date = datetime.fromtimestamp(target_timestamp, tz=timezone.utc).strftime("%Y-%m-%d")

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={},
            onchain_revenue_overrides={
                "schemaVersion": 1,
                "overrides": [{
                    "chain": "Ethereum",
                    "date": target_date,
                    "feesUSD": 360.0,
                    "revenueUSD": 72.0,
                    "defillamaFeesUSD": 400.0,
                    "defillamaGrossRevenueUSD": 80.0,
                    "source": "onchain-current-index-audit",
                }],
            },
        )

        eth = output["series"][29]["chains"]["Ethereum"]
        self.assertEqual(eth["feesUSD"], 360.0)
        self.assertEqual(eth["grossRevenueUSD"], 72.0)
        self.assertEqual(eth["source"], "onchain-current-index-audit")
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))


    def test_build_output_embeds_berachain_borrow_fee_rebate_metadata(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={},
            borrow_fee_rebate_metadata=borrow_fee_rebate_metadata(),
        )

        self.assertEqual(output["borrowFeeRebates"]["status"], "active")
        self.assertEqual(output["borrowFeeRebates"]["chains"]["Berachain"]["rebatePercentage"], 0.1)
        self.assertEqual(output["borrowFeeRebates"]["chains"]["Berachain"]["marketCount"], 2)
        self.assertEqual(output["assurance"]["borrowFeeRebateStatus"], "active_no_closed_epoch_rebates")
        self.assertIn("borrowFeeRebates", output["sourceUrls"])

    def test_build_output_nets_berachain_borrow_fee_rebates_from_closed_epochs(self):
        revenue_data = metric_payload_with_berachain(
            total24h=100,
            latest_value=100,
            step=2,
            latest_berachain_value=20,
        )
        fees_data = metric_payload_with_berachain(
            total24h=500,
            latest_value=500,
            step=10,
            latest_berachain_value=100,
        )
        latest_ts = START_TS + 30 * DAY_SECONDS

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={},
            borrow_fee_rebate_metadata=borrow_fee_rebate_metadata(),
            borrow_fee_rebate_data={
                "status": "ok",
                "chains": {
                    "Berachain": {
                        "status": "ok",
                        "source": "FeeRebateRollingClaims.MarketIdToMerkleRootSet",
                        "epochRebates": [{
                            "epoch": 1,
                            "periodStartTimestamp": latest_ts,
                            "periodEndTimestamp": latest_ts + DAY_SECONDS,
                            "rebateUSD": 5.0,
                            "marketCount": 2,
                        }],
                    }
                },
            },
        )

        self.assertEqual(output["latest"]["grossRevenueUSD"], 100)
        self.assertEqual(output["latest"]["borrowFeeRebateUSD"], 5)
        self.assertEqual(output["latest"]["revenueUSD"], 95)
        self.assertEqual(output["latest"]["supplySideRevenueUSD"], 400)
        self.assertEqual(output["latest"]["chains"]["Berachain"]["grossRevenueUSD"], 20)
        self.assertEqual(output["latest"]["chains"]["Berachain"]["borrowFeeRebateUSD"], 5)
        self.assertEqual(output["latest"]["chains"]["Berachain"]["revenueUSD"], 15)
        self.assertEqual(output["totals"]["dailyGrossRevenueUSD"], 100)
        self.assertEqual(output["totals"]["dailyBorrowFeeRebateUSD"], 5)
        self.assertEqual(output["totals"]["dailyRevenueUSD"], 95)
        self.assertEqual(output["totals"]["dailySupplySideRevenueUSD"], 400)
        self.assertEqual(output["borrowFeeRebates"]["netting"], "netted_closed_epochs")
        self.assertEqual(output["borrowFeeRebates"]["chains"]["Berachain"]["totalRebateUSD"], 5)
        self.assertEqual(output["assurance"]["borrowFeeRebateStatus"], "active_netted_closed_epochs")
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_borrow_fee_rebates_are_allocated_by_borrow_interest_share(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)
        day_29_ts = START_TS + 29 * DAY_SECONDS
        day_30_ts = START_TS + 30 * DAY_SECONDS
        for payload, day_29_total, day_29_bera, day_30_total, day_30_bera in (
            (revenue_data, 140, 120, 160, 80),
            (fees_data, 1000, 300, 1000, 700),
        ):
            payload["totalDataChart"][29][1] = day_29_total
            payload["totalDataChart"][30][1] = day_30_total
            payload["totalDataChartBreakdown"][29][1] = {
                "Berachain": {"interest": day_29_bera},
                "Ethereum": {"interest": day_29_total - day_29_bera},
            }
            payload["totalDataChartBreakdown"][30][1] = {
                "Berachain": {"interest": day_30_bera},
                "Ethereum": {"interest": day_30_total - day_30_bera},
            }

        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={},
            borrow_fee_rebate_metadata=borrow_fee_rebate_metadata(),
            borrow_fee_rebate_data={
                "status": "ok",
                "chains": {
                    "Berachain": {
                        "status": "ok",
                        "source": "FeeRebateRollingClaims.MarketIdToMerkleRootSet",
                        "epochRebates": [{
                            "epoch": 1,
                            "periodStartTimestamp": day_29_ts,
                            "periodEndTimestamp": day_30_ts + DAY_SECONDS,
                            "rebateUSD": 100.0,
                            "marketCount": 2,
                        }],
                    }
                },
            },
        )

        day_29 = output["series"][29]["chains"]["Berachain"]
        day_30 = output["series"][30]["chains"]["Berachain"]
        self.assertAlmostEqual(day_29["borrowFeeRebateUSD"], 30.0, places=6)
        self.assertAlmostEqual(day_30["borrowFeeRebateUSD"], 70.0, places=6)
        self.assertAlmostEqual(day_29["revenueUSD"], 90.0, places=6)
        self.assertAlmostEqual(day_30["revenueUSD"], 10.0, places=6)
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_missing_rebate_fetch_preserves_previous_closed_epochs(self):
        revenue_data = metric_payload_with_berachain(
            total24h=100,
            latest_value=100,
            step=2,
            latest_berachain_value=20,
        )
        fees_data = metric_payload_with_berachain(
            total24h=500,
            latest_value=500,
            step=10,
            latest_berachain_value=100,
        )
        latest_ts = START_TS + 30 * DAY_SECONDS
        previous_output = {
            "generatedAt": "2026-06-20T12:00:00Z",
            "borrowFeeRebates": {
                "status": "active",
                "netting": "netted_closed_epochs",
                "dataGeneratedAt": "2026-06-20T11:59:00Z",
                "chains": {
                    "Berachain": {
                        "status": "active",
                        "source": "FeeRebateRollingClaims.MarketIdToMerkleRootSet",
                        "eventCount": 3,
                        "pricedEventCount": 3,
                        "missingPriceCount": 0,
                        "priceFallbackCount": 0,
                        "latestRebateDate": datetime.fromtimestamp(latest_ts + DAY_SECONDS, tz=timezone.utc).strftime("%Y-%m-%d"),
                        "epochRebates": [{
                            "epoch": 1,
                            "periodStartTimestamp": latest_ts,
                            "periodEndTimestamp": latest_ts + DAY_SECONDS,
                            "rebateUSD": 5.0,
                            "marketCount": 2,
                        }],
                    }
                },
            },
        }

        rebate_data = preserve_previous_borrow_fee_rebate_data(
            {
                "status": "missing",
                "error": "TimeoutError: RPC unavailable",
                "chains": {"Berachain": {"status": "missing", "error": "TimeoutError: RPC unavailable"}},
            },
            previous_output,
        )
        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit={},
            borrow_fee_rebate_metadata=borrow_fee_rebate_metadata(),
            borrow_fee_rebate_data=rebate_data,
        )

        self.assertEqual(output["borrowFeeRebates"]["dataStatus"], "fallback_previous_closed_epochs")
        self.assertEqual(output["borrowFeeRebates"]["chains"]["Berachain"]["totalRebateUSD"], 5)
        self.assertEqual(output["latest"]["borrowFeeRebateUSD"], 5)
        self.assertEqual(output["latest"]["revenueUSD"], 95)
        self.assertEqual(output["assurance"]["borrowFeeRebateStatus"], "active_netted_closed_epochs")
        self.assertTrue(_dolomite_revenue_totals_valid(output))
        self.assertTrue(_dolomite_revenue_window_totals_valid(output))
        self.assertTrue(_dolomite_revenue_chain_windows_valid(output))

    def test_current_rebate_fetch_preserves_previous_epoch_max_rebate_audit(self):
        latest_ts = START_TS + 30 * DAY_SECONDS
        current_rebate_data = {
            "status": "ok",
            "chains": {
                "Berachain": {
                    "status": "ok",
                    "source": "FeeRebateRollingClaims.MarketIdToMerkleRootSet",
                    "epochRebates": [{
                        "epoch": 1,
                        "periodStartTimestamp": latest_ts,
                        "periodEndTimestamp": latest_ts + DAY_SECONDS,
                        "rebateUSD": 5.0,
                        "marketCount": 2,
                    }],
                }
            },
        }
        previous_output = {
            "borrowFeeRebates": {
                "chains": {
                    "Berachain": {
                        "epochRebates": [{
                            "epoch": 1,
                            "periodStartTimestamp": latest_ts,
                            "periodEndTimestamp": latest_ts + DAY_SECONDS,
                            "rebateUSD": 5.0,
                            "marketCount": 2,
                            "maxRebateUSD": 9.75,
                            "maxRebateMethod": "eligible_market_daily_current_index",
                        }],
                    }
                },
            },
        }

        rebate_data = preserve_previous_borrow_fee_rebate_data(current_rebate_data, previous_output)

        epoch = rebate_data["chains"]["Berachain"]["epochRebates"][0]
        self.assertEqual(epoch["maxRebateUSD"], 9.75)
        self.assertEqual(epoch["maxRebateMethod"], "eligible_market_daily_current_index")

    def test_current_revenue_file_has_audited_max_rebate_for_every_closed_epoch(self):
        data = json.loads((ROOT / "dolomite_revenue.json").read_text())
        rows = (
            (data.get("borrowFeeRebates") or {})
            .get("chains", {})
            .get("Berachain", {})
            .get("epochRebates", [])
        )
        missing = [
            row.get("epoch")
            for row in rows
            if row.get("rebateUSD", 0) > 0
            and (
                not isinstance(row.get("maxRebateUSD"), (int, float))
                or row.get("maxRebateUSD") <= 0
                or row.get("maxRebateMethod") != "eligible_market_daily_current_index"
                or row.get("maxRebateSource") != "onchain-current-index-audit"
            )
        ]

        self.assertEqual(missing, [])

    def test_validator_does_not_depend_on_liquidation_history_for_revenue(self):
        revenue_data = metric_payload(total24h=100, latest_value=100, step=2)
        fees_data = metric_payload(total24h=500, latest_value=500, step=10)
        output = build_output(revenue_data, fees_data, onchain_audit=None)

        with tempfile.TemporaryDirectory() as tmp:
            previous_cwd = os.getcwd()
            try:
                os.chdir(tmp)
                with open("dolomite_revenue.json", "w") as f:
                    json.dump(output, f)
                with open("liquidation_history.json", "w") as f:
                    json.dump({
                        "liquidationHistory": [{
                            "timestamp": START_TS + 30 * DAY_SECONDS,
                            "liquidationRewardUSD": 4.5,
                        }]
                    }, f)

                rules = {**RULES["dolomite_revenue.json"], "min_bytes": 0}
                result = ValidationResult()
                with contextlib.redirect_stdout(io.StringIO()):
                    validate_file("dolomite_revenue.json", rules, result)
            finally:
                os.chdir(previous_cwd)

        self.assertEqual(result.failed, 0)

    def test_liquidation_workflow_no_longer_refreshes_revenue(self):
        workflow = (ROOT / ".github/workflows/update-liquidation-risk.yml").read_text()

        self.assertNotIn("python3 fetch_dolomite_revenue.py", workflow)
        self.assertNotIn("dolomite_revenue.json", workflow)

    def test_revenue_workflows_pass_berachain_rpc_for_rebate_netting(self):
        update_tvl = (ROOT / ".github/workflows/update-tvl-data.yml").read_text()
        audit = (ROOT / ".github/workflows/audit-dolomite-revenue-onchain.yml").read_text()

        for workflow in (update_tvl, audit):
            self.assertIn("python3 fetch_dolomite_revenue.py", workflow)
            self.assertIn("data/dolomite-revenue-onchain-overrides.json", workflow)
            self.assertIn("ALCHEMY_BERACHAIN_RPC", workflow)
            self.assertIn("QUICKNODE_BERACHAIN_RPC_2", workflow)
            self.assertIn("DRPC_BERACHAIN_RPC_ZEN", workflow)
            self.assertIn("ALCHEMY_BERACHAIN_RPC_2", workflow)
            self.assertIn("ALCHEMY_BERACHAIN_RPC_3", workflow)
        self.assertIn("python3 fetch_veborrow_simulation.py", update_tvl)
        self.assertIn("veborrow_simulation.json", update_tvl)

    def test_revenue_ui_surfaces_per_chain_audit_status(self):
        html = (ROOT / "revenue-preview.html").read_text(encoding="utf-8")

        self.assertIn("auditStatusForChain", html)
        self.assertIn("onchainAuditChains", html)
        self.assertIn('text.textContent = `Data updated · ${dataAgeLabel(revenueData.generatedAt)}`;', html)
        self.assertNotIn('text.textContent = "Updated " + dateLabel(revenueData.generatedAt);', html)
        self.assertNotIn("DeFiLlama gross + rebate netting", html)
        self.assertNotIn("onchain audit STALE", html)
        self.assertIn("Net Borrow Revenue", html)
        self.assertIn("7D Total Revenue", html)
        self.assertIn("All-Time Total Revenue", html)
        self.assertIn("net borrow + oDOLO exercised", html)
        self.assertIn("all-time net borrow + oDOLO exercised", html)
        self.assertIn("totalRevenueWindowUSD", html)
        self.assertIn("n(row.revenueUSD) + n(row.odoloUsdcUSD)", html)
        self.assertIn('id="revenueAllTime"', html)
        self.assertIn('setMetricText("revenueAllTime", usd(totalRevenueWindowUSD(0)));', html)
        self.assertNotIn("30D Total Revenue", html)
        self.assertNotIn('id="revenue30d"', html)
        self.assertNotIn('setText("revenue30d"', html)
        self.assertNotIn("rolling net protocol revenue", html)
        self.assertNotIn("net retained by protocol", html)
        self.assertIn('dolomite_revenue.json?v=revenue-20260708-veborrow-max-rebate', html)
        self.assertNotIn('dolomite_revenue.json?v=revenue-20260625-borrow-fee-weighted-rebate', html)
        route_html = (ROOT / "revenue/index.html").read_text(encoding="utf-8")
        self.assertIn("network-filter-20260805-fast-wallet-sort-20260805-strategic-round-20260805", route_html)
        self.assertLess(
            html.index("Protocol Revenue by Chain"),
            html.index("Dolomite Revenue Over Time"),
        )
        self.assertNotIn("<th>Protocol cut</th>", html)
        self.assertNotIn("<th>% of protocol total</th>", html)
        self.assertIn("Supply payments", html)
        self.assertNotIn("Lender payments", html)
        self.assertIn('data-chain-sort="revenue"', html)
        self.assertIn('data-chain-sort="fees"', html)
        self.assertIn('data-chain-sort="supply"', html)
        self.assertIn('class="revenue-chain-sort active"', html)
        self.assertIn("function setChainSort(key)", html)
        self.assertIn("renderChainSortHeaders", html)
        self.assertIn("chainLifecycleMeta", html)
        self.assertIn("chainLifecycleStatusHtml", html)
        self.assertIn("Archived network", html)
        self.assertIn("Shutting down Jul 09", html)
        self.assertIn(".chain-status.archived", html)
        self.assertIn(".chain-status.shutting-down", html)
        self.assertIn('class="chain-status ${esc(lifecycle.className)}"', html)
        self.assertIn('!detail.startsWith("target ")', html)
        self.assertIn('<tr><td colspan="4" class="empty-state">Loading protocol revenue...</td></tr>', html)
        self.assertIn('No protocol revenue available for selected range.', html)
        self.assertIn('Protocol revenue data could not be loaded.', html)
        self.assertNotIn('<div class="num-sub">gross</div>', html)
        self.assertNotIn('<div class="num-sub">lender share</div>', html)
        self.assertNotIn('<div class="num-sub">net retained</div>', html)
        self.assertNotIn('<div class="num-sub">of protocol total</div>', html)
        self.assertNotIn('chainSub = `${abbr} · Dolomite revenue`', html)
        self.assertIn(".revenue-primary{font-family:var(--mono);font-size:14px;font-weight:900;color:#75b87b", html)
        self.assertIn('id="veBorrowPanel"', html)
        self.assertIn('id="veBorrowChart"', html)
        self.assertIn('id="veBorrowBrushWrap"', html)
        self.assertIn('data-brush-key="veBorrow"', html)
        self.assertIn('data-veborrow-mode="daily"', html)
        self.assertIn('data-veborrow-mode="cumulative"', html)
        self.assertNotIn('id="veBorrowRows"', html)
        self.assertNotIn("renderVeBorrowTable", html)
        self.assertNotIn("veborrow-table", html)
        self.assertIn("MIN_CHARTED_VEBORROW_REBATE_USD", html)
        self.assertIn("veBorrowRewardStartTimestamp", html)
        self.assertIn("veBorrowPublishedThroughTimestamp", html)
        self.assertIn("veBorrowPendingDataRow", html)
        self.assertIn("veborrow-chart-bar pending", html)
        self.assertIn("Pending rebate data", html)
        self.assertIn("Striped bars = pending veBorrow rebate data, not zero savings", html)
        self.assertIn("renderVeBorrowChart", html)
        self.assertIn("hasOnchainRevenueOverrideForChain", html)
        self.assertIn("isDustOnlyAuditWarn", html)
        self.assertNotIn("current-index onchain", html)
        self.assertIn("Dolomite revenue", html)
        self.assertIn("borrowFeeRebateCumulativeUSD", html)
        self.assertIn("borrowFeeRebateUSD", html)
        self.assertIn('<div class="tt-row active"><span>Borrow interest</span>', html)
        self.assertIn('<div class="tt-row"><span>Users saved</span>', html)
        self.assertIn('id="veBorrowSimToggle"', html)
        self.assertIn('id="veBorrowSimulation"', html)
        self.assertIn("renderVeBorrowSimulation", html)
        self.assertIn("veborrow_simulation.json", html)
        self.assertIn("Current veDOLO", html)
        self.assertIn("Current veDOLO saved", html)
        self.assertIn("Max theoretical", html)
        self.assertIn("Max rebate", html)
        self.assertIn("Utilization", html)
        self.assertIn("veBorrowMaxRebateForRows", html)
        self.assertIn("maxRebateUSD", html)
        self.assertIn("publishedSelected", html)
        self.assertIn("!veBorrowPendingDataRow(row)", html)
        self.assertIn("tooltipMaxRebate", html)
        self.assertIn("<span>Max rebate</span>", html)
        self.assertIn("pctFine(utilization)", html)
        self.assertNotIn("<span>All published</span>", html)
        self.assertIn("currentVeDoloSavedUSD", html)
        self.assertIn("renderVeBorrowSimulationMode", html)
        self.assertIn('const isVeBorrowCumulative = veBorrowChartMode === "cumulative"', html)
        self.assertIn('<div class="tt-row active"><span>Cumulative saved</span><b>${usdFull(row.borrowFeeRebateCumulativeUSD)}</b></div>', html)
        self.assertIn("isVeBorrowCumulative ? cumulativeRows", html)
        self.assertIn("Required locked DOLO", html)
        self.assertIn("Simulation only", html)
        self.assertIn("simulate ETH + ARB + BERA", html)
        veborrow_panel = html[html.index('id="veBorrowPanel"'):html.index('id="veBorrowDiscountUsersPanel"')]
        self.assertLess(
            veborrow_panel.index('id="veBorrowSimToggle"'),
            veborrow_panel.index('class="revenue-mode-toggles veborrow-mode-toggles"'),
        )
        self.assertIn("Ethereum + Arbitrum + Berachain veBorrow Simulation", html)
        self.assertIn('const VEBORROW_SIM_CHAINS = ["Ethereum", "Arbitrum", "Berachain"]', html)
        self.assertIn("veBorrowDisplaySimulationChains", html)
        self.assertIn("activeRebateChains", html)
        self.assertIn("rebatePercentagesByChain", html)
        self.assertIn("veBorrowRebatePercentageForChain", html)
        self.assertIn("chainInterest * veBorrowRebatePercentageForChain(chain, config)", html)
        self.assertIn('displayChainLabel || "ETH + ARB + BERA"', html)
        self.assertIn("payout estimate", html)
        self.assertIn("Berachain is active today", html)
        self.assertNotIn("Ethereum + Arbitrum veBorrow Simulation", html)
        self.assertIn("veBorrowSimulationRows", html)
        self.assertIn("requiredLockedDolo", html)
        self.assertIn("dolo_price.json", html)
        self.assertIn("veBorrowWalletPage", html)
        self.assertIn("veBorrowWalletPageSize = 10", html)
        self.assertIn('id="veBorrowDiscountUsersPanel"', html)
        self.assertIn('id="veBorrowDiscountUsersChart"', html)
        self.assertIn('id="veBorrowDiscountUsersChartTip"', html)
        self.assertIn('id="veBorrowDiscountUsersBrushWrap"', html)
        self.assertIn('id="veBorrowDiscountUsersBrushSvg"', html)
        self.assertIn('data-brush-key="veBorrowDiscountUsers"', html)
        self.assertIn("renderVeBorrowDiscountUsersChart", html)
        self.assertIn("renderVeBorrowDiscountUsersBrush", html)
        self.assertIn("veBorrowDiscountDailyRows", html)
        self.assertIn("discountUserVoteWeight", html)
        self.assertIn("Discount vote weight", html)
        self.assertNotIn('data-veborrow-discount-mode', html)
        self.assertNotIn("veBorrowDiscountUsersMode", html)
        self.assertNotIn("setVeBorrowDiscountUsersMode", html)
        self.assertNotIn("Cumulative wallet-days", html)
        self.assertNotIn("cumulative wallet-days", html)
        self.assertIn('document.querySelectorAll("[data-veborrow-mode]")', html)
        self.assertIn("discountUserCount", html)
        self.assertIn("Current discount", html)
        self.assertIn("Daily Berachain wallets using discount", html)
        self.assertIn("Berachain active discount", html)
        self.assertIn('id="veBorrowCurrentWalletsPanel"', html)
        self.assertIn("renderVeBorrowWalletPager", html)
        self.assertIn("goVeBorrowWalletPage", html)
        self.assertIn('id="veBorrowWalletPager"', html)
        self.assertIn('id="veBorrowWalletInfo"', html)
        self.assertIn('id="veBorrowWalletCount"', html)
        self.assertIn("renderVeBorrowCurrentWalletTable", html)
        self.assertIn("veborrow-wallet-card-head", html)
        self.assertIn("veborrow-wallet-foot", html)
        self.assertIn("veborrow-wallet-table-wrap", html)
        self.assertIn("veborrow-wallet-panel", html)
        self.assertIn("veborrow-wallet-table", html)
        self.assertIn("veborrow-wallet-spacer-row", html)
        self.assertIn('<section class="veborrow-wallet-panel" id="veBorrowCurrentWalletsPanel">', html)
        self.assertIn('</section>\n\n  <section class="veborrow-wallet-panel" id="veBorrowCurrentWalletsPanel">', html)
        self.assertIn('<table class="veborrow-wallet-table">', html)
        self.assertNotIn('<table class="revenue-table veborrow-wallet-table">', html)

        self.assertIn(".veborrow-wallet-table tbody tr:nth-child(even) td{background:transparent}", html)
        self.assertNotIn('id="veBorrowSimulationWallets"', html)
        simulation_start = html.index('id="veBorrowSimulation"')
        wallet_panel_start = html.index('id="veBorrowCurrentWalletsPanel"')
        self.assertLess(simulation_start, wallet_panel_start)
        self.assertLess(html.index("</section>", simulation_start), wallet_panel_start)
        simulation_markup = html[simulation_start:wallet_panel_start]
        self.assertNotIn("Top users saved with current veDOLO", simulation_markup)
        address_header_index = html.index('data-veborrow-wallet-sort="address"')
        borrow_header_index = html.index('data-veborrow-wallet-sort="borrowInterest"')
        self.assertLess(address_header_index, borrow_header_index)
        self.assertIn("veBorrowWalletSortKey", html)
        self.assertIn("sortVeBorrowWallets", html)
        self.assertNotIn('data-veborrow-wallet-sort="chain"', html)
        self.assertIn("data-veborrow-wallet-sort=\"address\"", html)
        self.assertIn("data-veborrow-wallet-sort=\"borrowInterest\"", html)
        self.assertIn("data-veborrow-wallet-sort=\"vedolo\"", html)
        self.assertIn("data-veborrow-wallet-sort=\"saved\"", html)
        self.assertIn("data-veborrow-wallet-sort=\"missing\"", html)
        self.assertIn('<td colspan="5" class="empty-state">No wallets match selected filters.</td>', html)
        self.assertNotIn('<span class="veborrow-metric-sub">selected range</span>', html)
        self.assertNotIn("ETH max", html)
        self.assertNotIn("all eligible chains", html)
        self.assertIn("https://debank.com/profile/", html)
        self.assertIn("copyVeBorrowAddress", html)
        self.assertIn("addr-cell", html)
        self.assertIn("addr-line top", html)
        self.assertIn("addr-mono addr-tooltip-wrap", html)
        self.assertIn("addr-copy", html)
        self.assertIn("addr-debank", html)
        self.assertIn("dolo-address-labels.js?v=dolo-labels-20260805-strategic-round", html)
        self.assertIn("VEBORROW_WALLET_LABELS", html)
        self.assertIn("veBorrowWalletDisplayName", html)
        self.assertIn("addr-name addr-generic", html)
        self.assertIn("Wallet", html)
        self.assertNotIn("type-tag ${tagClass}", html)
        self.assertNotIn(".veborrow-wallet-table .type-tag.type-veborrow", html)
        self.assertNotIn(".veborrow-wallet-table .type-tag.type-borrower", html)
        self.assertNotIn("copy-addr-icon", html)
        self.assertNotIn("debank-icon", html)
        self.assertIn("encodeURIComponent(address)", html)
        self.assertNotIn("veborrow-wallet-address-name", html)
        self.assertNotIn("Current veDOLO saver", html)
        self.assertIn('class="pg-btn" type="button" data-veborrow-wallet-page="first"', html)
        self.assertIn('<span class="pg-current">${veBorrowWalletPage} <span class="pg-sep">/</span> ${pages}</span>', html)
        self.assertIn('id="veBorrowWalletSearch"', html)
        self.assertIn("Search address...", html)
        self.assertIn('id="veBorrowWalletNetworkControl"', html)
        self.assertIn('id="veBorrowWalletNetworkButton"', html)
        self.assertIn('id="veBorrowWalletNetworkMenu"', html)
        self.assertIn('{ key: "all", name: "All Chains" }', html)
        self.assertIn('data-veborrow-wallet-chain="${esc(chain.key)}"', html)
        self.assertIn("veBorrowWalletDefaultChainKeys", html)
        self.assertIn("veBorrowWalletSelectedChains", html)
        self.assertIn("setVeBorrowWalletSelectedChains", html)
        self.assertIn("toggleVeBorrowWalletNetworkMenu", html)
        self.assertIn("syncVeBorrowWalletNetworkDropdown", html)
        self.assertIn('const VEBORROW_WALLET_CHAIN_ORDER = ["ethereum", "berachain", "arbitrum", "mantle", "xlayer"]', html)
        self.assertNotIn('lifecycle: "archived"', html)
        self.assertNotIn('lifecycle: "shuttingDown"', html)
        self.assertNotIn("veBorrowWalletAvailableChainKeys", html)
        self.assertIn("veBorrowWalletSearchQuery", html)
        self.assertIn("filterVeBorrowWallets", html)
        self.assertIn("clearVeBorrowWalletSearch", html)
        self.assertIn("veBorrowWalletSearchQuery = input.value", html)
        self.assertIn("veBorrowWalletDisplayName(row).name", html)
        self.assertIn("rowChains.has(chainKey)", html)
        self.assertIn('event.target.closest(".veborrow-wallet-chain-dd")', html)
        self.assertIn("veBorrowWalletAriaSort", html)
        self.assertIn("veBorrowWalletThClass", html)
        self.assertNotIn('aria-sort="${veBorrowWalletAriaSort("chain")}"', html)
        borrow_interest_index = html.index('<div class="tt-row active"><span>Borrow interest</span>')
        user_saved_index = html.index('<div class="tt-row"><span>Users saved</span>')
        cumulative_index = html.index('<div class="tt-row"><span>Cumulative saved</span>')
        self.assertLess(borrow_interest_index, user_saved_index)
        self.assertLess(user_saved_index, cumulative_index)
        self.assertNotIn("Net Berachain revenue", html)

    def test_current_vedolo_wallet_filter_uses_active_chains_and_opens_without_chart_render(self):
        script = r'''
const assert = require("assert");
const fs = require("fs");
const source = fs.readFileSync("revenue-preview.html", "utf8");

function between(start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from);
  if (from < 0 || to < 0) throw new Error(`Missing production source boundary: ${start} -> ${end}`);
  return source.slice(from, to);
}

const chainConstants = between(
  "const VEBORROW_WALLET_CHAIN_ORDER",
  "const VEBORROW_WALLET_LABELS",
);
const chainFunctions = between(
  "function normalizeVeBorrowWalletChainKey",
  "function veBorrowWalletChainSelectionIsDefault",
);
const chainKeys = new Function(`
  ${chainConstants}
  ${chainFunctions}
  return veBorrowWalletFilterChains([]).map(chain => chain.key);
`)();
assert.deepStrictEqual(chainKeys, ["ethereum", "berachain", "arbitrum", "mantle", "xlayer"]);

const toggleFunction = between(
  "function toggleVeBorrowWalletNetworkMenu",
  "window.toggleVeBorrowWalletNetworkMenu",
);
const toggleResult = new Function(`
  let veBorrowWalletNetworkOpen = false;
  let chartRenderCount = 0;
  let dropdownSyncCount = 0;
  function renderVeBorrowChart() { chartRenderCount += 1; }
  function syncVeBorrowWalletNetworkDropdown() { dropdownSyncCount += 1; }
  ${toggleFunction}
  toggleVeBorrowWalletNetworkMenu({ stopPropagation() {} });
  return { veBorrowWalletNetworkOpen, chartRenderCount, dropdownSyncCount };
`)();
assert.deepStrictEqual(toggleResult, {
  veBorrowWalletNetworkOpen: true,
  chartRenderCount: 0,
  dropdownSyncCount: 1,
});
'''
        subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_current_vedolo_wallet_sort_rerenders_only_cached_table_rows(self):
        script = r'''
const assert = require("assert");
const fs = require("fs");
const source = fs.readFileSync("revenue-preview.html", "utf8");

function between(start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from);
  if (from < 0 || to < 0) throw new Error(`Missing production source boundary: ${start} -> ${end}`);
  return source.slice(from, to);
}

const sortFunctions = between(
  "const veBorrowWalletAddressCollator",
  "window.setVeBorrowWalletSort",
);
const result = new Function(`
  function n(value) { return Number(value) || 0; }
  let displayNameCalls = 0;
  function veBorrowWalletDisplayName(row) {
    displayNameCalls += 1;
    return { name: row.label || "Wallet" };
  }
  let veBorrowWalletSortKey = "saved";
  let veBorrowWalletSortDesc = true;
  let veBorrowWalletPage = 4;
  const veBorrowWalletRowsCache = [
    { address: "0xbbb", label: "Beta", currentVeDoloSavedUSD: 2 },
    { address: "0xaaa", label: "Alpha", currentVeDoloSavedUSD: 1 },
    { address: "0xccc", label: "Alpha", currentVeDoloSavedUSD: 3 },
  ];
  let chartRenderCount = 0;
  let tableRenderCount = 0;
  let renderedRows = null;
  function renderVeBorrowChart() { chartRenderCount += 1; }
  function renderVeBorrowCurrentWalletTable(rows) {
    tableRenderCount += 1;
    renderedRows = rows;
  }
  ${sortFunctions}
  setVeBorrowWalletSort("address");
  const firstOrder = sortVeBorrowWallets(veBorrowWalletRowsCache).map(row => row.address);
  sortVeBorrowWallets(veBorrowWalletRowsCache);
  return {
    chartRenderCount,
    tableRenderCount,
    renderedRows,
    firstOrder,
    displayNameCalls,
    page: veBorrowWalletPage,
  };
`)();

assert.strictEqual(result.chartRenderCount, 0);
assert.strictEqual(result.tableRenderCount, 1);
assert.strictEqual(result.renderedRows.length, 3);
assert.deepStrictEqual(result.firstOrder, ["0xaaa", "0xccc", "0xbbb"]);
assert.strictEqual(result.displayNameCalls, 3);
assert.strictEqual(result.page, 1);
'''
        subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_revenue_panel_headers_use_the_holders_table_divider(self):
        html = (ROOT / "revenue-preview.html").read_text(encoding="utf-8")

        self.assertEqual(html.count('class="panel-head revenue-section-head'), 5)
        self.assertEqual(html.count('class="revenue-section-primary"'), 5)
        self.assertEqual(html.count('class="revenue-section-copy"'), 5)
        self.assertEqual(html.count('class="revenue-section-title-line"'), 5)
        self.assertEqual(
            html.count('class="revenue-section-secondary revenue-section-tools"'),
            4,
        )
        self.assertEqual(html.count(" data-revenue-updated>"), 5)
        self.assertRegex(
            html,
            r'<div class="panel-title">Current discount</div>\s*<div class="veborrow-discount-meta" id="veBorrowDiscountUsersMeta">',
        )
        self.assertNotIn('<div class="panel-title">Current discount users</div>', html)
        self.assertIn(
            '${Math.round(latestDaily).toLocaleString("en-US")} Berachain wallets · ${model.length.toLocaleString("en-US")}D',
            html,
        )
        self.assertNotIn(
            '${Math.round(latestDaily).toLocaleString("en-US")} Berachain wallets · ${model.length.toLocaleString("en-US")}D range',
            html,
        )
        self.assertEqual(
            html.count(r'pattern="\d{1,2}(?:\.|/|-)\d{1,2}(?:\.|/|-)\d{4}"'),
            2,
        )
        self.assertNotIn(r'pattern="\d{1,2}[./-]\d{1,2}[./-]\d{4}"', html)
        self.assertIn(
            'document.querySelectorAll("[data-revenue-updated]")',
            html,
        )
        self.assertIn(
            "Data updated · ${dataAgeLabel(revenueData.generatedAt)}",
            html,
        )

    def test_protocol_revenue_chain_range_uses_borrow_interest_brush_only(self):
        html = (ROOT / "revenue-preview.html").read_text(encoding="utf-8")

        self.assertIn("let chainRangeBrushKey = BORROW_INTEREST_CHART_KEY;", html)
        self.assertNotIn("let chainRangeBrushKey = STREAMS_CHART_KEY;", html)
        self.assertIn("if (key === BORROW_INTEREST_CHART_KEY) chainRangeBrushKey = key;", html)
        self.assertNotIn("if (key !== VEBORROW_CHART_KEY) chainRangeBrushKey = key;", html)

    def test_protocol_revenue_column_has_no_inline_share_chart(self):
        html = (ROOT / "revenue-preview.html").read_text(encoding="utf-8")

        self.assertNotIn("revenue-share-track", html)
        self.assertNotIn("shareWidth", html)
        self.assertIn(
            '<td class="revenue-col"><div class="revenue-col-inner"><div class="revenue-primary">${usdFull(row.revenueUSD)}</div></div></td>',
            html,
        )


if __name__ == "__main__":
    unittest.main()
