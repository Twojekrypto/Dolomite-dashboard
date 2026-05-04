import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from run_earn_chain_live_rerun import (
    INFORMATIONAL_TAIL_RETRY_PRESET,
    build_combined_market_report,
    build_informational_retry_payload,
    build_live_plan,
    build_run_summary,
    detect_external_live_audits,
    live_audit_command,
    live_phase_payload_is_complete,
    run_market_cycle,
)


class RunEarnChainLiveRerunTest(unittest.TestCase):
    def test_build_live_plan_creates_output_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rerun_chain_root = root / "reruns" / "arbitrum" / "example-run"
            reports_dir = rerun_chain_root / "reports"
            unresolved_dir = rerun_chain_root / "unresolved"
            reports_dir.mkdir(parents=True, exist_ok=True)
            unresolved_dir.mkdir(parents=True, exist_ok=True)

            summary_path = rerun_chain_root / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "runId": "example-run",
                        "snapshotDate": "2026-04-18",
                        "markets": [
                            {
                                "marketId": "0",
                                "symbol": "WETH",
                                "holderCount": 10,
                                "resolvedCount": 4,
                                "unresolvedCount": 6,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (reports_dir / "0_WETH.json").write_text("{}", encoding="utf-8")
            (unresolved_dir / "0_WETH.json").write_text(
                json.dumps(
                    {
                        "chain": "arbitrum",
                        "marketId": "0",
                        "symbol": "WETH",
                        "inputCount": 6,
                        "unresolved": [{"wallet": f"0x{i}"} for i in range(6)],
                    }
                ),
                encoding="utf-8",
            )

            live_root = root / "live"
            plan = build_live_plan(
                "arbitrum",
                summary_path=summary_path,
                live_root=live_root,
                history_dir=root / "history",
                netflow_dir=root / "netflow",
                canonical_target_block=123,
                localhost_url="http://127.0.0.1:8921/index.html?cb=a,http://127.0.0.1:8922/index.html?cb=b",
                debug_json_url="http://127.0.0.1:9555/json,http://127.0.0.1:9666/json",
                workers_per_market=4,
                retry_workers_per_market=6,
                max_markets=1,
                min_unresolved_count=1,
                market_ids=[],
                live_preset="dual-sharded",
            )

            run_root = Path(plan["runRoot"])
            for relative in (
                "inputs",
                "results",
                "summaries",
                "retry-inputs",
                "forensic",
                "tail-explain",
                "reports",
            ):
                self.assertTrue((run_root / relative).is_dir(), relative)

            self.assertEqual(len(plan["endpointPairs"]), 2)
            self.assertEqual(
                plan["localhostUrls"],
                [
                    "http://127.0.0.1:8921/index.html?cb=a",
                    "http://127.0.0.1:8922/index.html?cb=b",
                ],
            )
            self.assertEqual(
                plan["debugJsonUrls"],
                [
                    "http://127.0.0.1:9555/json",
                    "http://127.0.0.1:9666/json",
                ],
            )
            self.assertEqual(plan["localhostUrl"], "http://127.0.0.1:8921/index.html?cb=a")
            self.assertEqual(plan["debugJsonUrl"], "http://127.0.0.1:9555/json")
            self.assertEqual(plan["livePreset"], "dual-sharded")
            self.assertEqual(plan["informationalRetryPreset"], INFORMATIONAL_TAIL_RETRY_PRESET)

            market = plan["markets"][0]
            self.assertTrue(market["informationalRetryInputPath"].endswith("0_WETH__informational.json"))
            self.assertTrue(market["informationalRetryOutputPath"].endswith("0_WETH__informational-retry.json"))
            self.assertTrue(market["informationalRetrySummaryPath"].endswith("0_WETH__informational-retry.json"))

    def test_live_audit_command_passes_plan_preset_to_asset_auditor(self):
        plan = {
            "chain": "arbitrum",
            "canonicalTargetBlock": 123,
            "livePreset": "targeted-slow-retry",
            "endpointPairs": [
                {
                    "localhostUrl": "http://127.0.0.1:8921/earn/",
                    "debugJsonUrl": "http://127.0.0.1:9555/json",
                }
            ],
        }
        market = {"marketId": "0", "symbol": "WETH"}

        cmd = live_audit_command(
            plan,
            market,
            input_path=Path("/tmp/weth-input.json"),
            output_path=Path("/tmp/weth-output.json"),
            phase="timeout-retry",
        )

        preset_index = cmd.index("--live-preset")
        self.assertEqual(cmd[preset_index + 1], "targeted-slow-retry")

    def test_live_audit_command_uses_slow_preset_for_informational_retry_tail(self):
        plan = {
            "chain": "mantle",
            "canonicalTargetBlock": 94857811,
            "livePreset": "dual-sharded",
            "endpointPairs": [
                {
                    "localhostUrl": "http://127.0.0.1:8921/earn/",
                    "debugJsonUrl": "http://127.0.0.1:9555/json",
                },
                {
                    "localhostUrl": "http://127.0.0.1:8921/earn/",
                    "debugJsonUrl": "http://127.0.0.1:9666/json",
                },
            ],
            "workersPerMarket": 12,
            "retryWorkersPerMarket": 12,
        }
        market = {"marketId": "1", "symbol": "WMNT"}

        cmd = live_audit_command(
            plan,
            market,
            input_path=Path("/tmp/wmnt-info-input.json"),
            output_path=Path("/tmp/wmnt-info-output.json"),
            phase="informational-retry",
        )

        preset_index = cmd.index("--live-preset")
        workers_index = cmd.index("--workers")
        self.assertEqual(cmd[preset_index + 1], "targeted-slow-retry")
        self.assertEqual(cmd[workers_index + 1], "2")
        self.assertIn("informational-retry", cmd[cmd.index("--localhost-url") + 1])

    def test_build_informational_retry_payload_extracts_low_severity_tail(self):
        payload = {
            "chain": "mantle",
            "marketId": "1",
            "symbol": "WMNT",
            "snapshotDate": "2026-05-03",
            "results": [
                {
                    "address": "0xinfo",
                    "category": "no_data",
                    "positionKind": "visible_supply",
                    "marketRow": {
                        "balanceCell": "0.000001WMNT \u2248 $0.0000",
                        "verifyLabel": "",
                        "sourceLabel": "",
                        "yieldCell": "\u2014",
                    },
                    "visiblePosition": {"accountNumber": "0", "wei": "1", "par": "1"},
                    "collateralPosition": {"accountNumber": "123", "wei": "1000", "par": "1000"},
                    "focusMarket": {"calc": {"hasData": False}, "verificationData": None},
                    "staticStatus": "inferred",
                    "staticMethod": "recent-cycle+pre-snapshot-carry",
                    "staticReason": "historical:pre_snapshot_carry",
                },
                {
                    "address": "0xverified",
                    "category": "verified_nonstandard",
                    "positionKind": "visible_supply",
                    "marketRow": {"verifyLabel": "VERIFIED"},
                    "focusMarket": {},
                },
            ],
        }

        retry_payload = build_informational_retry_payload(payload)

        self.assertEqual(retry_payload["inputCount"], 1)
        self.assertEqual(retry_payload["unresolved"][0]["wallet"], "0xinfo")

    def test_live_phase_payload_is_not_complete_when_output_is_partial(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "input.json"
            input_path.write_text(
                json.dumps(
                    {
                        "inputCount": 3,
                        "unresolved": [
                            {"wallet": "0x1"},
                            {"wallet": "0x2"},
                            {"wallet": "0x3"},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            self.assertFalse(live_phase_payload_is_complete({"completed": 2, "results": [{}, {}]}, input_path))
            self.assertTrue(live_phase_payload_is_complete({"completed": 3, "results": [{}, {}, {}]}, input_path))

    def test_build_live_plan_filters_known_missing_wallets_from_previous_live_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rerun_chain_root = root / "reruns" / "arbitrum" / "example-run"
            reports_dir = rerun_chain_root / "reports"
            unresolved_dir = rerun_chain_root / "unresolved"
            reports_dir.mkdir(parents=True, exist_ok=True)
            unresolved_dir.mkdir(parents=True, exist_ok=True)

            summary_path = rerun_chain_root / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "runId": "example-run",
                        "snapshotDate": "2026-04-18",
                        "markets": [
                            {
                                "marketId": "0",
                                "symbol": "WETH",
                                "holderCount": 10,
                                "resolvedCount": 4,
                                "unresolvedCount": 2,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (reports_dir / "0_WETH.json").write_text("{}", encoding="utf-8")
            (unresolved_dir / "0_WETH.json").write_text(
                json.dumps(
                    {
                        "chain": "arbitrum",
                        "marketId": "0",
                        "symbol": "WETH",
                        "inputCount": 2,
                        "unresolved": [
                            {"wallet": "0xmissing", "status": "inferred", "method": "netflow+pre-snapshot-carry"},
                            {"wallet": "0xkeep", "status": "mismatch", "method": "snapshot-fallback"},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            live_root = root / "live"
            prior_results_dir = live_root / "arbitrum" / "prior-run" / "results"
            prior_results_dir.mkdir(parents=True, exist_ok=True)
            (prior_results_dir / "0_WETH__merged.json").write_text(
                json.dumps(
                    {
                        "chain": "arbitrum",
                        "marketId": "0",
                        "symbol": "WETH",
                        "inputCount": 2,
                        "completed": 2,
                        "results": [
                            {
                                "address": "0xmissing",
                                "positionKind": "missing",
                                "category": "missing_position",
                                "focusMarket": {},
                            },
                            {
                                "address": "0xkeep",
                                "positionKind": "visible_supply",
                                "marketRow": {"verifyLabel": "VERIFIED", "sourceLabel": "Replay Ledger"},
                                "focusMarket": {
                                    "resolvedSource": "replay-ledger",
                                    "resolvedVerificationStatus": "verified",
                                },
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            plan = build_live_plan(
                "arbitrum",
                summary_path=summary_path,
                live_root=live_root,
                history_dir=root / "history",
                netflow_dir=root / "netflow",
                canonical_target_block=123,
                localhost_url="http://127.0.0.1:8921/index.html",
                debug_json_url="http://127.0.0.1:9555/json",
                workers_per_market=4,
                retry_workers_per_market=6,
                max_markets=1,
                min_unresolved_count=1,
                market_ids=[],
                live_preset="single-fast",
            )

            market = plan["markets"][0]
            self.assertEqual(market["sourceUnresolvedCount"], 2)
            self.assertEqual(market["unresolvedCount"], 1)
            self.assertEqual(market["skippedKnownMissingCount"], 1)

            filtered_input_path = Path(market["inputPath"])
            payload = json.loads(filtered_input_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["inputCount"], 1)
            self.assertEqual([row["wallet"] for row in payload["unresolved"]], ["0xkeep"])

    def test_detect_external_live_audits_requires_real_live_invocation(self):
        ps_output = "\n".join(
            [
                "100 /usr/bin/python3 /repo/audit_earn_asset.py live --chain arbitrum",
                "101 rg -n audit_earn_asset.py live .",
                "102 /usr/bin/python3 /repo/audit_earn_asset.py static --chain arbitrum",
                "103 /usr/bin/python3 run_earn_chain_live_rerun.py status --note 'audit_earn_asset.py live'",
                "104 /usr/bin/python3 /repo/audit_earn_asset.py extract-live --results out.json",
                "200 /usr/bin/python3 /repo/audit_earn_asset.py live --chain arbitrum",
            ]
        )
        fake_proc = SimpleNamespace(returncode=0, stdout=ps_output)

        with (
            patch("run_earn_chain_live_rerun.os.getpid", return_value=200),
            patch("run_earn_chain_live_rerun.subprocess.run", return_value=fake_proc),
        ):
            rows = detect_external_live_audits()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pid"], 100)
        self.assertIn("audit_earn_asset.py live", rows[0]["command"])

    def test_build_run_summary_separates_raw_diagnostics_from_final_verdict(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            summary_path = root / "summary.json"
            report_path = root / "report.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "completed": 4,
                        "timeouts": 1,
                        "missingPosition": 1,
                        "counts": {
                            "verified_nonstandard": 10,
                            "eval_timeout": 1,
                            "missing_position": 1,
                        },
                    }
                ),
                encoding="utf-8",
            )
            report_path.write_text(
                json.dumps(
                    {
                        "finalBlockingTailCount": 0,
                        "finalInformationalTailCount": 0,
                        "timeoutRetryInputCount": 2,
                        "timeoutRetryCompletedCount": 2,
                        "tailLikelyCauseCounts": {},
                    }
                ),
                encoding="utf-8",
            )
            plan = {
                "runId": "example-run",
                "chain": "berachain",
                "canonicalTargetBlock": 123,
                "sourceSummaryPath": str(root / "source-summary.json"),
                "sourceSnapshotDate": "2026-05-03",
                "markets": [
                    {
                        "marketId": "0",
                        "symbol": "WETH",
                        "unresolvedCount": 4,
                        "sourceUnresolvedCount": 4,
                        "skippedKnownMissingCount": 0,
                        "status": "completed",
                        "stage": "completed",
                        "outputPath": str(root / "merged.json"),
                        "summaryPath": str(summary_path),
                        "reportPath": str(report_path),
                    }
                ],
            }

            result = build_run_summary(plan)

            self.assertEqual(result["auditVerdict"]["status"], "pass")
            self.assertEqual(result["finalTailTotals"]["finalBlockingTailCount"], 0)
            self.assertEqual(result["finalTailTotals"]["timeoutRetryCompletedCount"], 2)
            self.assertEqual(result["rawDiagnosticCounts"], {"eval_timeout": 1, "missing_position": 1})
            self.assertEqual(result["aggregateCounts"]["verified_nonstandard"], 10)
            market = result["markets"][0]
            self.assertEqual(market["auditVerdict"]["status"], "pass")
            self.assertEqual(market["rawDiagnostics"]["total"], 2)
            self.assertFalse(market["rawDiagnostics"]["blocking"])

    def test_combined_market_report_includes_audit_verdict_and_raw_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            timeout_input = root / "timeout-input.json"
            timeout_input.write_text(json.dumps({"inputCount": 2}), encoding="utf-8")
            informational_input = root / "informational-input.json"
            informational_input.write_text(json.dumps({"inputCount": 1}), encoding="utf-8")
            plan = {
                "chain": "berachain",
                "canonicalTargetBlock": 123,
                "sourceSnapshotDate": "2026-05-03",
            }
            market = {
                "marketId": "0",
                "symbol": "WETH",
                "timeoutRetryInputPath": str(timeout_input),
                "forensicPath": str(root / "forensic.json"),
                "tailExplainPath": str(root / "tail.json"),
                "fullOutputPath": str(root / "full.json"),
                "timeoutRetryOutputPath": str(root / "retry.json"),
                "informationalRetryInputPath": str(informational_input),
                "informationalRetryOutputPath": str(root / "informational-retry.json"),
                "outputPath": str(root / "merged.json"),
            }

            report = build_combined_market_report(
                plan,
                market=market,
                static_report={"holderCount": 4, "resolvedCount": 2, "unresolvedCount": 2},
                full_summary={"counts": {"eval_timeout": 2}},
                timeout_retry_summary={"completed": 2},
                informational_retry_summary={"completed": 1},
                merged_summary={"counts": {"verified_nonstandard": 5, "eval_timeout": 1, "no_data": 1}},
                forensic_report={"blockingRows": [], "informationalRows": []},
                tail_report={"likelyCauseCounts": {}},
            )

            self.assertEqual(report["auditVerdict"]["status"], "pass")
            self.assertEqual(report["rawDiagnostics"]["counts"], {"eval_timeout": 1, "no_data": 1})
            self.assertEqual(report["timeoutRetryInputCount"], 2)
            self.assertEqual(report["timeoutRetryCompletedCount"], 2)
            self.assertEqual(report["informationalRetryInputCount"], 1)
            self.assertEqual(report["informationalRetryCompletedCount"], 1)

    def test_run_market_cycle_retries_informational_tail_before_final_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_root = root / "live" / "mantle" / "run"
            for name in ("inputs", "results", "summaries", "retry-inputs", "forensic", "tail-explain", "reports"):
                (run_root / name).mkdir(parents=True, exist_ok=True)

            static_path = run_root / "static.json"
            static_path.write_text(
                json.dumps({"holderCount": 1, "resolvedCount": 0, "unresolvedCount": 1}),
                encoding="utf-8",
            )
            input_path = run_root / "inputs" / "1_WMNT.json"
            input_path.write_text(
                json.dumps({"chain": "mantle", "marketId": "1", "symbol": "WMNT", "inputCount": 1, "unresolved": [{"wallet": "0xinfo"}]}),
                encoding="utf-8",
            )
            plan = {
                "runId": "run",
                "chain": "mantle",
                "runRoot": str(run_root),
                "canonicalTargetBlock": 94857811,
                "sourceSnapshotDate": "2026-05-03",
                "sourceSummaryPath": str(run_root / "source-summary.json"),
                "historyDir": str(root / "history"),
                "netflowDir": str(root / "netflow"),
                "endpointPairs": [{"localhostUrl": "http://127.0.0.1:8921/earn/", "debugJsonUrl": "http://127.0.0.1:9555/json"}],
                "workersPerMarket": 4,
                "retryWorkersPerMarket": 4,
                "markets": [],
            }
            market = {
                "marketId": "1",
                "symbol": "WMNT",
                "unresolvedCount": 1,
                "sourceUnresolvedCount": 1,
                "skippedKnownMissingCount": 0,
                "staticReportPath": str(static_path),
                "inputPath": str(input_path),
                "fullOutputPath": str(run_root / "results" / "1_WMNT__full.json"),
                "fullSummaryPath": str(run_root / "summaries" / "1_WMNT__full.json"),
                "timeoutRetryInputPath": str(run_root / "retry-inputs" / "1_WMNT__timeout.json"),
                "timeoutRetryOutputPath": str(run_root / "results" / "1_WMNT__timeout-retry.json"),
                "timeoutRetrySummaryPath": str(run_root / "summaries" / "1_WMNT__timeout-retry.json"),
                "informationalRetryInputPath": str(run_root / "retry-inputs" / "1_WMNT__informational.json"),
                "informationalRetryOutputPath": str(run_root / "results" / "1_WMNT__informational-retry.json"),
                "informationalRetrySummaryPath": str(run_root / "summaries" / "1_WMNT__informational-retry.json"),
                "outputPath": str(run_root / "results" / "1_WMNT__merged.json"),
                "summaryPath": str(run_root / "summaries" / "1_WMNT__merged.json"),
                "forensicPath": str(run_root / "forensic" / "1_WMNT.json"),
                "tailExplainPath": str(run_root / "tail-explain" / "1_WMNT.json"),
                "reportPath": str(run_root / "reports" / "1_WMNT.json"),
                "status": "pending",
                "stage": "pending",
                "startedAt": None,
                "completedAt": None,
                "error": None,
            }
            plan["markets"].append(market)

            informational_row = {
                "address": "0xinfo",
                "category": "no_data",
                "positionKind": "visible_supply",
                "marketRow": {
                    "balanceCell": "0.000001WMNT \u2248 $0.0000",
                    "verifyLabel": "",
                    "sourceLabel": "",
                    "yieldCell": "\u2014",
                },
                "visiblePosition": {"accountNumber": "0", "wei": "1", "par": "1"},
                "collateralPosition": {"accountNumber": "123", "wei": "1000", "par": "1000"},
                "focusMarket": {"calc": {"hasData": False}, "verificationData": None},
                "staticStatus": "inferred",
                "staticMethod": "recent-cycle+pre-snapshot-carry",
                "staticReason": "historical:pre_snapshot_carry",
            }
            verified_row = {
                "address": "0xinfo",
                "category": "verified_nonstandard",
                "positionKind": "visible_supply",
                "marketRow": {"verifyLabel": "VERIFIED", "sourceLabel": ""},
                "focusMarket": {},
            }

            def fake_live_payload(_plan, *, market, phase, input_path, output_path):
                if phase == "full":
                    payload = {
                        "chain": "mantle",
                        "marketId": "1",
                        "symbol": "WMNT",
                        "snapshotDate": "2026-05-03",
                        "inputCount": 1,
                        "completed": 1,
                        "results": [informational_row],
                    }
                elif phase == "informational-retry":
                    payload = {
                        "chain": "mantle",
                        "marketId": "1",
                        "symbol": "WMNT",
                        "snapshotDate": "2026-05-03",
                        "inputCount": 1,
                        "completed": 1,
                        "results": [verified_row],
                    }
                else:
                    payload = {"chain": "mantle", "marketId": "1", "symbol": "WMNT", "inputCount": 0, "completed": 0, "results": []}
                Path(output_path).write_text(json.dumps(payload), encoding="utf-8")
                return payload

            with patch("run_earn_chain_live_rerun.ensure_live_phase_payload", side_effect=fake_live_payload):
                run_market_cycle(plan, market)

            report = json.loads(Path(market["reportPath"]).read_text(encoding="utf-8"))
            self.assertEqual(report["auditVerdict"]["status"], "pass")
            self.assertEqual(report["finalInformationalTailCount"], 0)
            self.assertEqual(report["informationalRetryInputCount"], 1)
            self.assertEqual(report["informationalRetryCompletedCount"], 1)


if __name__ == "__main__":
    unittest.main()
