import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

from audit_dolomite_revenue_onchain import (
    audit_window_for_date,
    build_audit_report,
    check_endpoint_budget,
    classify_chain_result,
    default_target_date,
    env_positive_int,
    parse_args,
    revenue_chain_totals,
    resolve_token_price,
    selected_market_ids,
    standard_market_state_from_getters,
)
from rpc_client import CHAIN_ENV_KEYS


ROOT = Path(__file__).resolve().parents[1]


class _FakeCall:
    def __init__(self, value):
        self.value = value
        self.blocks = []

    def call(self, block_identifier=None):
        self.blocks.append(block_identifier)
        return self.value


class _FakeMarketFunctions:
    def __init__(self):
        self.start_index = (1_000, 2_000, 100)
        self.end_index = (1_100, 2_100, 200)
        self.with_info_called = False

    def getMarket(self, market_id):
        market = [
            "0x0000000000000000000000000000000000000001",
            False,
            (50, 500),
            (999, 999, 0),
            "0x0000000000000000000000000000000000000000",
            "0x0000000000000000000000000000000000000000",
            (0,),
            (0,),
            (False, 0),
            (False, 0),
            (750_000_000_000_000_000,),
        ]
        return _FakeCall(market)

    def getMarketTokenAddress(self, market_id):
        return _FakeCall("0x0000000000000000000000000000000000000002")

    def getMarketTotalPar(self, market_id):
        return _FakeCall((123, 456))

    def getMarketCurrentIndex(self, market_id):
        class _IndexCall:
            def call(_, block_identifier=None):
                return self.start_index if block_identifier == 100 else self.end_index
        return _IndexCall()

    def getMarketWithInfo(self, market_id):
        self.with_info_called = True
        raise AssertionError("getMarketWithInfo should not be used for revenue audit state")


class _FakeContract:
    def __init__(self):
        self.functions = _FakeMarketFunctions()


class AuditDolomiteRevenueOnchainTest(unittest.TestCase):
    def test_default_target_date_uses_t_minus_two_closed_day(self):
        now = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)

        self.assertEqual(default_target_date(now), "2026-06-21")

    def test_default_audit_skips_archived_polygon_zkevm(self):
        with mock.patch("sys.argv", ["audit_dolomite_revenue_onchain.py"]):
            args = parse_args()

        self.assertNotIn("polygon_zkevm", args.chains.split(","))
        workflow = (ROOT / ".github" / "workflows" / "audit-dolomite-revenue-onchain.yml").read_text(encoding="utf-8")
        self.assertNotIn("POLYGONZKEVM", workflow)

    def test_audit_window_replays_the_named_utc_day(self):
        start, end = audit_window_for_date("2026-06-21")

        self.assertEqual(start, 1782000000)
        self.assertEqual(end, 1782086400)

    def test_revenue_chain_totals_uses_gross_revenue_for_audit_baseline(self):
        totals = revenue_chain_totals(
            {
                "series": [{
                    "date": "2026-06-21",
                    "chains": {
                        "Berachain": {
                            "feesUSD": 100.0,
                            "grossRevenueUSD": 20.0,
                            "borrowFeeRebateUSD": 5.0,
                            "revenueUSD": 15.0,
                            "supplySideRevenueUSD": 80.0,
                        }
                    },
                }]
            },
            "2026-06-21",
        )

        self.assertEqual(totals["Berachain"]["revenueUSD"], 20.0)
        self.assertEqual(totals["Berachain"]["netRevenueUSD"], 15.0)
        self.assertEqual(totals["Berachain"]["borrowFeeRebateUSD"], 5.0)

    def test_chain_result_warns_when_usd_diff_exceeds_tolerance(self):
        result = classify_chain_result(
            "Arbitrum",
            defillama={"feesUSD": 1_000.0, "revenueUSD": 200.0},
            onchain={"feesUSD": 1_030.0, "revenueUSD": 206.0, "protocolCut": 0.2},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "warn")
        self.assertGreater(result["revenueDiffPct"], 0.02)
        self.assertGreater(result["feesDiffPct"], 0.02)
        self.assertIn("fees_diff_exceeds_tolerance", result["warnReasons"])
        self.assertIn("revenue_diff_exceeds_tolerance", result["warnReasons"])

    def test_chain_result_passes_inside_tolerance(self):
        result = classify_chain_result(
            "Ethereum",
            defillama={"feesUSD": 10_000.0, "revenueUSD": 2_000.0},
            onchain={"feesUSD": 10_100.0, "revenueUSD": 2_015.0, "protocolCut": 0.1995, "priceFallbackCount": 1},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["priceFallbackCount"], 1)

    def test_missing_defillama_chain_total_is_zero_baseline(self):
        result = classify_chain_result(
            "Mantle",
            defillama=None,
            onchain={"feesUSD": 0.0, "revenueUSD": 0.0, "protocolCut": 0.0},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["defillamaChainMissing"])

    def test_missing_defillama_chain_total_warns_on_onchain_revenue(self):
        result = classify_chain_result(
            "Mantle",
            defillama=None,
            onchain={"feesUSD": 10.0, "revenueUSD": 2.0, "protocolCut": 0.2},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "warn")
        self.assertIsNone(result["revenueDiffPct"])
        self.assertTrue(result["revenueDiffUnbounded"])
        self.assertIn("defillama_chain_missing_onchain_nonzero", result["warnReasons"])

    def test_missing_defillama_chain_total_is_info_when_onchain_revenue_is_immaterial(self):
        result = classify_chain_result(
            "Mantle",
            defillama=None,
            onchain={"feesUSD": 2.75, "revenueUSD": 0.41, "protocolCut": 0.14909091},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "pass")
        self.assertIsNone(result["revenueDiffPct"])
        self.assertTrue(result["revenueDiffUnbounded"])
        self.assertIn("defillama_chain_missing_onchain_immaterial", result["infoReasons"])

    def test_dust_scale_diff_is_info_when_both_sources_are_below_caps(self):
        # Real Mantle case: DeFiLlama rounds chain rows to whole USD ($1 fees,
        # $0 revenue) while the onchain replay measures $3.80 / $0.57.
        result = classify_chain_result(
            "Mantle",
            defillama={"feesUSD": 1.0, "revenueUSD": 0.0},
            onchain={"feesUSD": 3.796781, "revenueUSD": 0.569517, "protocolCut": 0.15},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "pass")
        self.assertIn("dust_scale_diff_immaterial", result["infoReasons"])
        self.assertNotIn("warnReasons", result)

    def test_dust_scale_policy_does_not_apply_when_onchain_is_material(self):
        result = classify_chain_result(
            "Mantle",
            defillama={"feesUSD": 1.0, "revenueUSD": 0.0},
            onchain={"feesUSD": 50.0, "revenueUSD": 7.5, "protocolCut": 0.15},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "warn")
        self.assertIn("fees_diff_exceeds_tolerance", result["warnReasons"])

    def test_dust_scale_policy_does_not_apply_when_defillama_is_material(self):
        result = classify_chain_result(
            "Mantle",
            defillama={"feesUSD": 40.0, "revenueUSD": 8.0},
            onchain={"feesUSD": 3.0, "revenueUSD": 0.5, "protocolCut": 0.15},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "warn")
        self.assertIn("fees_diff_exceeds_tolerance", result["warnReasons"])

    def test_chain_result_warns_when_immaterial_tokens_are_omitted(self):
        result = classify_chain_result(
            "Arbitrum",
            defillama={"feesUSD": 100.0, "revenueUSD": 20.0},
            onchain={"feesUSD": 100.0, "revenueUSD": 20.0, "protocolCut": 0.2, "priceOmissionCount": 1},
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(result["status"], "warn")
        self.assertEqual(result["priceOmissionCount"], 1)
        self.assertIn("unpriced_tokens_omitted_immaterial", result["warnReasons"])

    def test_standard_market_state_uses_getters_without_price_oracle_info(self):
        contract = _FakeContract()

        state = standard_market_state_from_getters(
            contract,
            market_id=6,
            from_block=100,
            to_block=200,
            default_earnings_rate=850_000_000_000_000_000,
        )

        self.assertFalse(contract.functions.with_info_called)
        self.assertEqual(state["token"], "0x0000000000000000000000000000000000000002")
        self.assertEqual(state["borrowPar"], 123)
        self.assertEqual(state["startBorrowIndex"], 1_000)
        self.assertEqual(state["endBorrowIndex"], 1_100)
        self.assertEqual(state["earningsRate"], 750_000_000_000_000_000)

    def test_selected_market_ids_preserves_market_zero_for_filtered_audits(self):
        self.assertEqual(selected_market_ids(6, [0, "1", 1, 9, None, "bad"]), [0, 1])

    def test_stable_symbol_price_fallback_is_explicit(self):
        price, source = resolve_token_price({"symbol": "USDa"}, None)

        self.assertEqual(str(price), "1")
        self.assertEqual(source, "stable-symbol-fallback")

    def test_non_stable_missing_price_stays_missing(self):
        price, source = resolve_token_price({"symbol": "WBERA"}, None)

        self.assertIsNone(price)
        self.assertIsNone(source)

    def test_immaterial_missing_price_token_is_omitted_explicitly(self):
        price, source = resolve_token_price(
            {"symbol": "DPX", "protocolRevenueAmount": Decimal("0.0000085667284406504")},
            None,
        )

        self.assertEqual(price, Decimal("0"))
        self.assertEqual(source, "omitted-immaterial-missing-price")

    def test_material_missing_price_token_stays_missing(self):
        price, source = resolve_token_price(
            {"symbol": "DPX", "protocolRevenueAmount": Decimal("0.01")},
            None,
        )

        self.assertIsNone(price)
        self.assertIsNone(source)

    def test_endpoint_budget_marks_slow_rpc_as_failed_endpoint(self):
        with mock.patch("audit_dolomite_revenue_onchain.REVENUE_AUDIT_ENDPOINT_BUDGET_SECONDS", 10), \
             mock.patch("audit_dolomite_revenue_onchain.time.monotonic", return_value=25):
            with self.assertRaises(TimeoutError) as exc:
                check_endpoint_budget(endpoint_started_at=10, stage="reading market 7")

        self.assertIn("endpoint budget exceeded", str(exc.exception))
        self.assertIn("reading market 7", str(exc.exception))

    def test_env_positive_int_ignores_invalid_values(self):
        with mock.patch.dict("os.environ", {"REVENUE_AUDIT_RPC_TIMEOUT_SECONDS": "oops"}):
            self.assertEqual(env_positive_int("REVENUE_AUDIT_RPC_TIMEOUT_SECONDS", 20), 20)

    def test_rpc_client_includes_revenue_audit_archive_secret_fallbacks(self):
        self.assertLess(
            CHAIN_ENV_KEYS["arbitrum"].index("ALCHEMY_ARBITRUM_RPC_ZEN"),
            CHAIN_ENV_KEYS["arbitrum"].index("ALCHEMY_ARBITRUM_RPC"),
        )
        self.assertLess(
            CHAIN_ENV_KEYS["ethereum"].index("ALCHEMY_ETHEREUM_RPC_ZEN"),
            CHAIN_ENV_KEYS["ethereum"].index("ALCHEMY_ETHEREUM_RPC"),
        )
        self.assertIn("ALCHEMY_ARBITRUM_RPC_ZEN", CHAIN_ENV_KEYS["arbitrum"])
        self.assertIn("ALCHEMY_ARBITRUM_RPC_KAT", CHAIN_ENV_KEYS["arbitrum"])
        self.assertIn("ALCHEMY_ARBITRUM_RPC_DAN", CHAIN_ENV_KEYS["arbitrum"])
        self.assertIn("ALCHEMY_ETHEREUM_RPC_ZEN", CHAIN_ENV_KEYS["ethereum"])
        self.assertIn("ALCHEMY_ETHEREUM_RPC_KAT", CHAIN_ENV_KEYS["ethereum"])
        self.assertIn("ALCHEMY_ETHEREUM_RPC_DAN", CHAIN_ENV_KEYS["ethereum"])
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2", CHAIN_ENV_KEYS["berachain"])
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN", CHAIN_ENV_KEYS["berachain"])

    def test_report_is_partial_when_only_some_chains_are_audited(self):
        report = build_audit_report(
            target_date="2026-06-21",
            target_timestamp=1782000000,
            window_start_timestamp=1781913600,
            chain_results={
                "Arbitrum": {"status": "pass"},
                "Ethereum": {"status": "missing", "error": "archive RPC unavailable"},
            },
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(report["status"], "partial")
        self.assertEqual(report["summary"]["auditedChainCount"], 1)
        self.assertEqual(report["summary"]["missingChainCount"], 1)

    def test_report_warns_if_any_chain_warns(self):
        report = build_audit_report(
            target_date="2026-06-21",
            target_timestamp=1782000000,
            window_start_timestamp=1781913600,
            chain_results={
                "Arbitrum": {"status": "pass"},
                "Ethereum": {"status": "warn"},
            },
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(report["status"], "warn")

    def test_report_keeps_finite_max_when_another_diff_is_unbounded(self):
        report = build_audit_report(
            target_date="2026-06-21",
            target_timestamp=1782000000,
            window_start_timestamp=1782000000,
            chain_results={
                "Ethereum": {
                    "status": "warn",
                    "feesDiffPct": 0.022,
                    "revenueDiffPct": 0.031,
                    "feesDiffUnbounded": False,
                    "revenueDiffUnbounded": False,
                },
                "Mantle": {
                    "status": "warn",
                    "feesDiffPct": None,
                    "revenueDiffPct": None,
                    "feesDiffUnbounded": True,
                    "revenueDiffUnbounded": True,
                },
            },
            tolerance_pct=0.02,
            protocol_cut_tolerance=0.002,
        )

        self.assertEqual(report["summary"]["maxRevenueDiffPct"], 0.031)
        self.assertTrue(report["summary"]["revenueDiffUnbounded"])

    def test_workflow_runs_audit_and_commits_output(self):
        workflow = (ROOT / ".github/workflows/audit-dolomite-revenue-onchain.yml").read_text(encoding="utf-8")

        self.assertGreaterEqual(workflow.count("cron:"), 3)
        self.assertIn("cron: '37 1 * * *'", workflow)
        self.assertIn("python3 audit_dolomite_revenue_onchain.py", workflow)
        self.assertIn("data/dolomite-revenue-onchain-audit.json", workflow)
        self.assertIn("git add data/dolomite-revenue-onchain-audit.json data/dolomite-revenue-onchain-overrides.json dolomite_revenue.json", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_ZEN", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_KAT", workflow)
        self.assertIn("ALCHEMY_ARBITRUM_RPC_DAN", workflow)
        self.assertIn("ALCHEMY_ETHEREUM_RPC_ZEN", workflow)
        self.assertIn("ALCHEMY_ETHEREUM_RPC_KAT", workflow)
        self.assertIn("ALCHEMY_ETHEREUM_RPC_DAN", workflow)
        self.assertIn("QUICKNODE_BERACHAIN_RPC_2", workflow)
        self.assertIn("DRPC_BERACHAIN_RPC_ZEN", workflow)

    def test_pages_redeploys_after_revenue_audit_workflow(self):
        workflow = (ROOT / ".github/workflows/pages.yml").read_text(encoding="utf-8")

        self.assertIn("Audit Dolomite Revenue Onchain", workflow)


if __name__ == "__main__":
    unittest.main()
