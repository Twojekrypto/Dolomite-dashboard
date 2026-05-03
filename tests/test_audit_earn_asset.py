import unittest
from argparse import Namespace

from audit_earn_asset import (
    build_live_audit_js,
    build_live_js_defaults_for_args,
    normalize_live_row_category,
    parse_live_row_pattern,
    summarize_live_results,
)


class AuditEarnAssetTest(unittest.TestCase):
    def test_live_audit_js_selects_input_chain_not_hardcoded_arbitrum(self):
        js = build_live_audit_js()
        self.assertNotIn("earnChainSelect('arbitrum')", js)
        self.assertIn("function buildAuditSource(address, chain)", js)
        self.assertIn("buildAuditSource(address, CHAIN)", js)
        self.assertIn("earnChainSelect(chain)", js)

    def test_normalize_live_row_category_canonicalizes_legacy_aliases(self):
        self.assertEqual(normalize_live_row_category({"category": "verified_other"}), "verified_nonstandard")
        self.assertEqual(normalize_live_row_category({"category": "borrow_only"}), "non_active_borrow_route")

    def test_normalize_live_row_category_marks_replay_verified(self):
        row = {
            "positionKind": "visible_supply",
            "marketRow": {"verifyLabel": "VERIFIED", "sourceLabel": "Replay Ledger"},
            "focusMarket": {
                "resolvedSource": "replay-ledger",
                "resolvedVerificationStatus": "verified",
            },
        }
        self.assertEqual(normalize_live_row_category(row), "replay_verified")

    def test_normalize_live_row_category_prefers_verified_over_missing_position(self):
        row = {
            "positionKind": "missing",
            "marketRow": {"verifyLabel": "", "sourceLabel": ""},
            "focusMarket": {
                "resolvedSource": "replay-ledger",
                "resolvedVerificationStatus": "verified",
                "verificationData": {
                    "status": "verified",
                    "canVerify": True,
                    "counted": True,
                    "supplyWeiDiff": "0",
                    "collateralWeiDiff": "0",
                    "borrowWeiDiff": "0",
                },
            },
        }
        self.assertEqual(normalize_live_row_category(row), "replay_verified")

    def test_normalize_live_row_category_marks_timed_out_missing_as_timeout(self):
        row = {
            "positionKind": "missing",
            "timedOut": True,
            "marketRow": {"verifyLabel": "", "sourceLabel": "", "yieldCell": "Loading..."},
            "focusMarket": {},
        }
        self.assertEqual(normalize_live_row_category(row), "timeout_other")

    def test_parse_live_row_pattern_detects_exact_match_non_strict_inferred(self):
        row = {
            "positionKind": "visible_supply",
            "marketRow": {"verifyLabel": "", "sourceLabel": "", "balanceCell": "≈ $123.45", "yieldCell": "$1.23"},
            "focusMarket": {
                "resolvedVerificationStatus": "inferred",
                "resolvedCanonicalHistoryCoverageStatus": "fresh",
                "verificationData": {
                    "status": "coverage_incomplete",
                    "supplyWeiDiff": "0",
                    "collateralWeiDiff": "0",
                    "borrowWeiDiff": "0",
                    "maxUsdDrift": 0,
                },
                "calc": {"hasData": True, "verificationStatus": "coverage_incomplete", "trustedForTotal": False},
            },
        }
        self.assertEqual(normalize_live_row_category(row), "has_data_other")
        self.assertEqual(
            parse_live_row_pattern(row),
            ("exact_match_non_strict_inferred", "low"),
        )

    def test_parse_live_row_pattern_marks_non_active_borrow_route_as_info(self):
        row = {
            "positionKind": "borrow_only",
            "marketRow": {"verifyLabel": "", "sourceLabel": ""},
            "focusMarket": {},
        }
        self.assertEqual(normalize_live_row_category(row), "non_active_borrow_route")
        self.assertEqual(parse_live_row_pattern(row), ("non_active_borrow_route", "info"))

    def test_parse_live_row_pattern_detects_hidden_collateral_coverage_gap(self):
        row = {
            "positionKind": "hidden_collateral",
            "visiblePosition": None,
            "collateralPosition": {"accountNumber": "7"},
            "borrowPosition": None,
            "marketRow": {"verifyLabel": "", "sourceLabel": "", "balanceCell": "≈ $4.20", "yieldCell": "$0.00"},
            "focusMarket": {
                "resolvedSource": "",
                "resolvedMethod": "",
                "resolvedCanonicalHistoryCoverageStatus": "fresh",
                "verificationData": {
                    "status": "unverified",
                    "actualSupplyWei": "0",
                    "expectedSupplyWei": "0",
                    "actualCollateralWei": "123",
                    "expectedCollateralWei": "0",
                    "actualBorrowWei": "0",
                    "expectedBorrowWei": "0",
                    "maxUsdDrift": 0.5,
                },
                "calc": {"hasData": False},
            },
        }
        self.assertEqual(normalize_live_row_category(row), "hidden_collateral_other")
        self.assertEqual(
            parse_live_row_pattern(row),
            ("hidden_collateral_coverage_gap", "low"),
        )

    def test_parse_live_row_pattern_detects_exact_mixed_overlap_as_informational(self):
        row = {
            "positionKind": "visible_supply",
            "visiblePosition": {"accountNumber": "0"},
            "collateralPosition": {"accountNumber": "1"},
            "borrowPosition": {"accountNumber": "2"},
            "marketRow": {"verifyLabel": "", "sourceLabel": "", "balanceCell": "≈ $2.46", "yieldCell": "$11.09"},
            "focusMarket": {
                "resolvedVerificationStatus": "inferred",
                "resolvedCanonicalHistoryCoverageStatus": "fresh",
                "verificationData": {
                    "status": "coverage_incomplete",
                    "expectedSupplyWei": "10",
                    "actualSupplyWei": "10",
                    "expectedCollateralWei": "20",
                    "actualCollateralWei": "20",
                    "expectedBorrowWei": "30",
                    "actualBorrowWei": "30",
                    "supplyWeiDiff": "0",
                    "collateralWeiDiff": "0",
                    "borrowWeiDiff": "0",
                    "maxUsdDrift": 0,
                },
                "calc": {"hasData": True, "verificationStatus": "coverage_incomplete", "trustedForTotal": False},
            },
        }
        self.assertEqual(
            parse_live_row_pattern(row),
            ("exact_match_non_strict_inferred", "low"),
        )

    def test_parse_live_row_pattern_detects_visible_hidden_overlap(self):
        row = {
            "positionKind": "visible_supply",
            "visiblePosition": {"accountNumber": "0"},
            "collateralPosition": {"accountNumber": "7"},
            "borrowPosition": None,
            "marketRow": {"verifyLabel": "", "sourceLabel": "", "balanceCell": "≈ $12.75", "yieldCell": "$0.65"},
            "focusMarket": {
                "resolvedVerificationStatus": "inferred",
                "resolvedCanonicalHistoryCoverageStatus": "fresh",
                "verificationData": {
                    "status": "coverage_incomplete",
                    "expectedSupplyWei": "10",
                    "actualSupplyWei": "10",
                    "expectedCollateralWei": "0",
                    "actualCollateralWei": "5",
                    "expectedBorrowWei": "0",
                    "actualBorrowWei": "0",
                    "supplyWeiDiff": "0",
                    "collateralWeiDiff": "5",
                    "borrowWeiDiff": "0",
                    "maxUsdDrift": 2.13,
                },
                "calc": {"hasData": True, "verificationStatus": "coverage_incomplete", "trustedForTotal": False},
            },
        }
        self.assertEqual(
            parse_live_row_pattern(row),
            ("mixed_visible_hidden_overlap", "medium"),
        )

    def test_parse_live_row_pattern_downgrades_tiny_non_strict_drift(self):
        row = {
            "positionKind": "visible_supply",
            "visiblePosition": {"accountNumber": "0"},
            "marketRow": {"verifyLabel": "", "sourceLabel": "", "balanceCell": "≈ $0.08", "yieldCell": "$0.00"},
            "focusMarket": {
                "resolvedVerificationStatus": "inferred",
                "resolvedCanonicalHistoryCoverageStatus": "fresh",
                "verificationData": {
                    "status": "coverage_incomplete",
                    "expectedSupplyWei": "100",
                    "actualSupplyWei": "99",
                    "expectedCollateralWei": "0",
                    "actualCollateralWei": "0",
                    "expectedBorrowWei": "0",
                    "actualBorrowWei": "0",
                    "supplyWeiDiff": "-1",
                    "collateralWeiDiff": "0",
                    "borrowWeiDiff": "0",
                    "maxUsdDrift": 0.0000001,
                },
                "calc": {"hasData": True, "verificationStatus": "coverage_incomplete", "trustedForTotal": False},
            },
        }
        self.assertEqual(
            parse_live_row_pattern(row),
            ("tiny_non_strict_inferred_drift", "low"),
        )

    def test_summarize_live_results_splits_blocking_vs_informational_tail(self):
        verified = {
            "address": "0x1",
            "positionKind": "visible_supply",
            "marketRow": {"verifyLabel": "VERIFIED", "sourceLabel": "Replay Ledger", "balanceCell": "≈ $50.00", "yieldCell": "$1.00"},
            "focusMarket": {"resolvedSource": "replay-ledger", "resolvedVerificationStatus": "verified"},
        }
        info_row = {
            "address": "0x2",
            "positionKind": "visible_supply",
            "marketRow": {"verifyLabel": "", "sourceLabel": "", "balanceCell": "≈ $10.00", "yieldCell": "$0.05"},
            "focusMarket": {
                "resolvedVerificationStatus": "fallback",
                "resolvedCanonicalHistoryCoverageStatus": "fresh",
                "verificationData": {
                    "status": "coverage_incomplete",
                    "supplyWeiDiff": "0",
                    "collateralWeiDiff": "0",
                    "borrowWeiDiff": "0",
                    "maxUsdDrift": 0,
                },
                "calc": {"hasData": True, "verificationStatus": "coverage_incomplete", "trustedForTotal": False},
            },
        }
        blocking_row = {
            "address": "0x3",
            "positionKind": "visible_supply",
            "marketRow": {"verifyLabel": "SNAPSHOT ONLY", "sourceLabel": "Snapshot", "balanceCell": "≈ $100.00", "yieldCell": "$2.00"},
            "focusMarket": {
                "resolvedMethod": "snapshot-series",
                "verificationData": {"maxUsdDrift": 4},
            },
        }
        timeout_row = {
            "address": "0x4",
            "category": "eval_timeout",
            "positionKind": "visible_supply",
            "marketRow": {"verifyLabel": "PENDING", "yieldCell": "Loading..."},
            "focusMarket": {"resolvedVerificationStatus": "pending"},
            "timedOut": True,
        }
        payload = {
            "chain": "arbitrum",
            "marketId": "0",
            "symbol": "WETH",
            "inputCount": 4,
            "completed": 4,
            "results": [verified, info_row, blocking_row, timeout_row],
        }
        summary = summarize_live_results(payload)
        self.assertEqual(summary["verifiedChecked"], 1)
        self.assertEqual(summary["informationalRealNonVerifiedChecked"], 1)
        self.assertEqual(summary["blockingRealNonVerifiedChecked"], 1)
        self.assertEqual(summary["timeouts"], 1)

    def test_build_live_audit_js_embeds_config_values(self):
        js = build_live_audit_js(
            live_defaults={"workers": 6},
            live_js_defaults={
                "pageTargetPollMs": 111,
                "pageReadyPollMs": 112,
                "pageReadyMaxWaitMs": 113,
                "settlePollMs": 114,
                "settleStablePolls": 5,
                "maxWaitMs": 117,
                "lateReplayGraceMs": 118,
                "timeoutFinalSnapshotDelayMs": 115,
                "snapshotFlushEveryResults": 7,
                "snapshotFlushMaxDelayMs": 116,
                "snapshotFetchTimeoutMs": 119,
            },
        )
        self.assertIn("const WORKERS = Math.max(1, Number(process.argv[6] || 6));", js)
        self.assertIn("const PAGE_TARGET_POLL_MS = 111;", js)
        self.assertIn("const MAX_WAIT_MS = 117;", js)
        self.assertIn("const LATE_REPLAY_GRACE_MS = 118;", js)
        self.assertIn("const SNAPSHOT_FETCH_TIMEOUT_MS = 119;", js)
        self.assertIn("const EVALUATION_TIMEOUT_MS = MAX_WAIT_MS + LATE_REPLAY_GRACE_MS + TIMEOUT_FINAL_SNAPSHOT_DELAY_MS + 10000;", js)
        self.assertIn("const SNAPSHOT_FLUSH_EVERY_RESULTS = 7;", js)
        self.assertIn("function buildEndpointPairs(baseUrls, debugJsonUrls)", js)
        self.assertIn("function distributeWorkers(totalWorkers, endpointCount)", js)
        self.assertIn("const shouldExtendForLateReplay = (snap) => {", js)
        self.assertIn("globalThis.__EARN_SNAPSHOT_FETCH_TIMEOUT_OVERRIDE__ = ${SNAPSHOT_FETCH_TIMEOUT_MS};", js)
        self.assertIn("usedLateReplayGrace = true;", js)
        self.assertIn("if (replayTrusted || exactVerified) return 'replay_verified';", js)

    def test_build_live_js_defaults_for_args_applies_preset_and_cli_overrides(self):
        live_js_defaults = build_live_js_defaults_for_args(
            Namespace(
                _live_js_defaults={
                    "maxWaitMs": 240000,
                    "lateReplayGraceMs": 120000,
                    "snapshotFetchTimeoutMs": 30000,
                },
                max_wait_ms=360000,
                late_replay_grace_ms=None,
            )
        )
        self.assertEqual(live_js_defaults["maxWaitMs"], 360000)
        self.assertEqual(live_js_defaults["lateReplayGraceMs"], 120000)
        self.assertEqual(live_js_defaults["snapshotFetchTimeoutMs"], 30000)

    def test_build_live_audit_js_guards_target_creation_loop(self):
        js = build_live_audit_js()
        self.assertIn("function samePageSurface(targetUrl, desiredUrl)", js)
        self.assertIn("const maxAttempts = Math.max(count * 3, 8);", js)
        self.assertIn("Unable to provision", js)


if __name__ == "__main__":
    unittest.main()
