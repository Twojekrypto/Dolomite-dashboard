import json
import tempfile
import unittest
from pathlib import Path

from run_earn_chain_live_rerun import build_live_plan


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
                        "unresolved": [],
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


if __name__ == "__main__":
    unittest.main()
