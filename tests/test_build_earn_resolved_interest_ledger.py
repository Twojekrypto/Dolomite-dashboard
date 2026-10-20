import inspect
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import build_earn_resolved_interest_ledger as ledger_builder
from build_earn_resolved_interest_ledger import build_resolved_ledger


ADDRESS = "0x1111111111111111111111111111111111111111"
TOKEN = "0x2222222222222222222222222222222222222222"


def snapshot_payload(par="100", wei="120"):
    return {
        "date": "2026-07-18",
        "snapshots": {
            "arbitrum": {
                ADDRESS: {
                    "markets": {
                        "7": {"token": TOKEN, "symbol": "TEST", "decimals": 0, "par": par, "wei": wei},
                    }
                }
            }
        },
        "chainMetadata": {
            "arbitrum": {
                "blockNumber": 123,
                "interestIndexes": {
                    "7": {
                        "token": TOKEN,
                        "supplyIndex": "1200000000000000000",
                        "borrowIndex": "1300000000000000000",
                    }
                },
            }
        },
    }


def history_payload(events, *, last_scanned=130, has_borrow=False):
    return {
        "chain": "arbitrum",
        "address": ADDRESS,
        "lastScannedBlock": last_scanned,
        "scanRange": {"fromBlock": 1, "toBlock": last_scanned},
        "sourceMetadata": {"latestSnapshotDate": "2026-07-18"},
        "accounts": {
            "0": {
                "account": "0",
                "accountKnown": True,
                "hasBorrow": has_borrow,
                "markets": {
                    "7": {"events": events},
                },
            }
        },
    }


def strict_evidence(*, par="100", wei="120", **flags):
    evidence = {
        "comparisonBlock": 123,
        "protocolStartBlock": 1,
        "eventIndexes": {
            "100:1:2:0:7": "1000000000000000000",
        },
        "eventIndexPairs": {
            "100:1:2:0:7": {
                "supplyIndex": "1000000000000000000",
                "borrowIndex": "1000000000000000000",
            },
        },
        "currentIndexes": {
            "7": {
                "supplyIndex": "1200000000000000000",
                "borrowIndex": "1300000000000000000",
            },
        },
        "currentPositions": {
            "0|7": {"par": str(par), "wei": str(wei)},
        },
    }
    evidence.update(flags)
    return evidence


class BuildEarnResolvedInterestLedgerTests(unittest.TestCase):
    def _run_strict_cli(self, root, addresses, histories, *, fetch_side_effect=None, strict=True, extra_args=()):
        snapshot_dir = root / "snapshots"
        history_dir = root / "history"
        output_dir = root / "output"
        status_path = root / "status.json"
        address_file = root / "addresses.txt"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        history_dir.joinpath("arbitrum").mkdir(parents=True, exist_ok=True)
        snapshot = snapshot_payload()
        snapshot["snapshots"]["arbitrum"] = {
            address: snapshot_payload()["snapshots"]["arbitrum"][ADDRESS]
            for address in addresses
        }
        snapshot_dir.joinpath("manifest.json").write_text(json.dumps({
            "dates": ["2026-07-18"],
            "chains": {"2026-07-18": ["arbitrum"]},
        }), encoding="utf-8")
        snapshot_dir.joinpath("2026-07-18.json").write_text(
            json.dumps(snapshot), encoding="utf-8"
        )
        for address, history in histories.items():
            history_dir.joinpath("arbitrum", f"{address}.json").write_text(
                json.dumps(history), encoding="utf-8"
            )
        address_file.write_text("\n".join(addresses) + "\n", encoding="utf-8")
        rpc_calls = []

        def fake_fetch(chain, address, history, **kwargs):
            rpc_calls.append((chain, address, history, kwargs))
            if fetch_side_effect:
                return fetch_side_effect(chain, address, history, **kwargs)
            return strict_evidence()

        argv = [
            "build_earn_resolved_interest_ledger.py",
            "--chain", "arbitrum",
            "--address-file", str(address_file),
            "--output-dir", str(output_dir),
            "--status-output", str(status_path),
        ]
        if strict:
            argv.append("--fetch-strict-rpc-evidence")
        argv.extend(extra_args)
        with (
            patch.object(ledger_builder, "SNAPSHOT_DIR", snapshot_dir),
            patch.object(ledger_builder, "HISTORY_DIR", history_dir),
            patch.object(sys, "argv", argv),
            patch("earn_strict_rpc_evidence.fetch_strict_evidence", side_effect=fake_fetch),
        ):
            self.assertEqual(0, ledger_builder.main())
        return rpc_calls, json.loads(status_path.read_text(encoding="utf-8"))

    def test_regular_build_keeps_an_unchanged_exact_borrow_ledger(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        history = history_payload([event], has_borrow=True)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._run_strict_cli(root, [ADDRESS], {ADDRESS: history})
            path = root / "output" / "arbitrum" / f"{ADDRESS}.json"
            original = json.loads(path.read_text())
            calls, _ = self._run_strict_cli(root, [ADDRESS], {ADDRESS: history}, strict=False)
            self.assertTrue(path.exists(), "routine non-RPC refresh deleted exact borrow evidence")
            self.assertEqual(original, json.loads(path.read_text()))
            self.assertEqual([], calls)

    def test_reuse_keeps_exact_evidence_when_known_canonical_tail_advances(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        history = history_payload([event], has_borrow=True)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._run_strict_cli(root, [ADDRESS], {ADDRESS: history})
            path = root / "output" / "arbitrum" / f"{ADDRESS}.json"
            saved = json.loads(path.read_text())
            history["accounts"]["0"]["markets"]["7"]["events"].append({
                **event, "blockNumber": 135, "deltaWei": "20", "newPar": "120",
            })
            history["lastScannedBlock"] = 140
            history["scanRange"]["toBlock"] = 140
            calls, status = self._run_strict_cli(
                root, [ADDRESS], {ADDRESS: history}, extra_args=["--max-rpc-wallets", "0"],
            )
            self.assertEqual([], calls)
            self.assertEqual(saved, json.loads(path.read_text()))
            self.assertEqual("unchanged_pinned_evidence", status["chains"]["arbitrum"]["addresses"][ADDRESS]["reason"])
            self.assertEqual(0, status["chains"]["arbitrum"]["rpcWalletCount"])

    def test_daily_snapshot_rollover_invalidates_reuse_at_the_same_comparison_block(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        history = history_payload([event], has_borrow=True)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._run_strict_cli(root, [ADDRESS], {ADDRESS: history})
            saved = json.loads((root / "output" / "arbitrum" / f"{ADDRESS}.json").read_text())
            next_snapshot = snapshot_payload()
            next_snapshot["date"] = "2026-07-19"
            history["sourceMetadata"]["latestSnapshotDate"] = "2026-07-19"
            self.assertIsNotNone(ledger_builder.preflight_strict_rpc_evidence(
                "arbitrum", ADDRESS, "2026-07-19", next_snapshot, history,
            ))
            self.assertIsNone(ledger_builder._reuse_resolved_ledger(
                saved, "arbitrum", ADDRESS, "2026-07-19", next_snapshot, history,
            ))
            fresh = build_resolved_ledger(
                "arbitrum", ADDRESS, "2026-07-19", next_snapshot, history,
                strict_evidence=strict_evidence(),
            )
            self.assertEqual(saved["comparisonBlock"], fresh["comparisonBlock"])
            self.assertEqual("2026-07-19", fresh["snapshotDate"])

    def test_reuse_rejects_corrected_prefix_or_adjusted_verification(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        for change in ("prefix", "adjustment"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                history = history_payload([event], has_borrow=True)
                self._run_strict_cli(root, [ADDRESS], {ADDRESS: history})
                path = root / "output" / "arbitrum" / f"{ADDRESS}.json"
                if change == "prefix":
                    history["accounts"]["0"]["markets"]["7"]["events"] = [{**event, "deltaWei": "99"}]
                else:
                    saved = json.loads(path.read_text())
                    saved["replayVerificationData"]["7"]["replayStateAdjusted"] = True
                    path.write_text(json.dumps(saved))
                self._run_strict_cli(root, [ADDRESS], {ADDRESS: history}, strict=False)
                self.assertFalse(path.exists(), "changed or adjusted evidence must be invalidated")

    def test_reuse_rejects_unknown_accounts_with_empty_or_future_only_markets(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        for unknown_events in ([], [{**event, "blockNumber": 130}]):
            with self.subTest(unknown_events=unknown_events), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                history = history_payload([event], has_borrow=True)
                self._run_strict_cli(root, [ADDRESS], {ADDRESS: history})
                path = root / "output" / "arbitrum" / f"{ADDRESS}.json"
                saved = json.loads(path.read_text())
                history["accounts"]["unknown"] = {
                    "accountKnown": False,
                    "markets": {"7": {"events": unknown_events}},
                }
                diagnostics = {}
                self.assertIsNone(build_resolved_ledger(
                    "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history,
                    strict_evidence=strict_evidence(), diagnostics=diagnostics,
                ))
                self.assertEqual("coverage_incomplete", diagnostics["strictStatus"])
                self.assertIsNone(ledger_builder._reuse_resolved_ledger(
                    saved, "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history,
                ), "reuse must preserve the fresh replay's unknown-account gate")
                self._run_strict_cli(root, [ADDRESS], {ADDRESS: history}, strict=False)
                self.assertFalse(path.exists(), "unknown accounts must invalidate the saved exact artifact")

    def test_partial_rpc_results_keep_mismatch_diagnostics_and_retry_unresolved_markets(self):
        snapshot = snapshot_payload()
        markets = snapshot["snapshots"]["arbitrum"][ADDRESS]["markets"]
        markets["8"] = dict(markets["7"])
        indexes = snapshot["chainMetadata"]["arbitrum"]["interestIndexes"]
        indexes["8"] = dict(indexes["7"])
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        history = history_payload([event], has_borrow=True)
        history["accounts"]["0"]["markets"]["8"] = {"events": [{**event, "logIndex": 3}]}
        evidence = strict_evidence()
        evidence["currentIndexes"]["8"] = dict(evidence["currentIndexes"]["7"])
        evidence["currentPositions"]["0|8"] = {"par": "99", "wei": "120"}
        evidence["eventIndexPairs"]["100:1:3:0:8"] = dict(
            evidence["eventIndexPairs"]["100:1:2:0:7"]
        )

        with tempfile.TemporaryDirectory() as temp, patch(
            f"{__name__}.snapshot_payload", return_value=snapshot,
        ):
            root = Path(temp)
            runs = [self._run_strict_cli(
                root, [ADDRESS], {ADDRESS: history},
                fetch_side_effect=lambda *args, **kwargs: evidence,
            ) for _ in range(2)]
            for run_number, (calls, status) in enumerate(runs):
                with self.subTest(run=run_number):
                    self.assertEqual(1, len(calls), "strict repair must retry unresolved active markets")
                    self.assertEqual({"mismatch": 1}, status["chains"]["arbitrum"]["summary"])
                    diagnostics = status["chains"]["arbitrum"]["addresses"][ADDRESS]
                    self.assertEqual("mismatch", diagnostics["strictStatus"])
                    self.assertEqual("mismatch", diagnostics["markets"]["8"]["status"])
            path = root / "output" / "arbitrum" / f"{ADDRESS}.json"
            saved = json.loads(path.read_text())
            self.assertEqual({"7"}, set(saved["markets"]))
            calls, _ = self._run_strict_cli(root, [ADDRESS], {ADDRESS: history}, strict=False)
            self.assertEqual([], calls)
            self.assertEqual(saved, json.loads(path.read_text()), "routine refresh should retain exact partial proof")

    def test_rpc_budget_defers_other_borrow_wallets_without_marking_them_verified(self):
        second = "0x3333333333333333333333333333333333333333"
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        histories = {a: {**history_payload([event], has_borrow=True), "address": a} for a in [ADDRESS, second]}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls, status = self._run_strict_cli(root, [ADDRESS, second], histories, extra_args=["--max-rpc-wallets", "1"])
            self.assertEqual([ADDRESS], [call[1] for call in calls])
            self.assertFalse((root / "output" / "arbitrum" / f"{second}.json").exists())
            self.assertEqual("rpc_wallet_budget_deferred", status["chains"]["arbitrum"]["addresses"][second]["reason"])

    def test_failed_rpc_attempts_persist_bounded_private_rotation_state(self):
        second = "0x3333333333333333333333333333333333333333"
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2,
                 "deltaWei": "100", "newPar": "100", "accountKnown": True}
        histories = {
            address: {**history_payload([event], has_borrow=True), "address": address}
            for address in (ADDRESS, second)
        }

        def unavailable(*args, **kwargs):
            raise RuntimeError("archive RPC unavailable")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cache_path = root / "private-cache.json"
            calls, status = self._run_strict_cli(
                root, [ADDRESS, second], histories, fetch_side_effect=unavailable,
                extra_args=["--evidence-cache", str(cache_path), "--max-evidence-cache-entries", "1"],
            )
            self.assertEqual([ADDRESS, second], [call[1] for call in calls])
            attempts = json.loads(cache_path.read_text())["walletAttempts"]
            self.assertEqual({f"arbitrum:{second}"}, set(attempts))
            for address in (ADDRESS, second):
                diagnostics = status["chains"]["arbitrum"]["addresses"][address]
                self.assertEqual("rpc_evidence_error", diagnostics["reason"])
                self.assertEqual("coverage_incomplete", diagnostics["strictStatus"])
                attempted_at = diagnostics["attemptedAt"]
                self.assertTrue(attempted_at.endswith("Z"))
                self.assertIsNotNone(datetime.fromisoformat(attempted_at.replace("Z", "+00:00")).tzinfo)
                self.assertFalse((root / "output" / "arbitrum" / f"{address}.json").exists())
            self.assertEqual(attempts[f"arbitrum:{second}"], status["chains"]["arbitrum"]["addresses"][second]["attemptedAt"])

    def test_preflight_rejects_uncovered_scan_range_even_with_a_recent_last_block(self):
        history = history_payload([], last_scanned=130)
        history["scanRange"]["toBlock"] = 122
        diagnostics = {}
        self.assertIsNone(ledger_builder.preflight_strict_rpc_evidence(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history, diagnostics=diagnostics))
        self.assertEqual("history_scan_range_incomplete", diagnostics["reason"])

    def test_cli_exposes_strict_rpc_and_diagnostic_status_options(self):
        source = (
            Path(__file__).resolve().parents[1] / "build_earn_resolved_interest_ledger.py"
        ).read_text(encoding="utf-8")
        self.assertIn('"--fetch-strict-rpc-evidence"', source)
        self.assertIn('"--status-output"', source)
        self.assertIn("fetch_strict_evidence(", source)

    def test_builds_strict_ledger_from_exact_rpc_evidence(self):
        if "strict_evidence" not in inspect.signature(build_resolved_ledger).parameters:
            self.fail("build_resolved_ledger must accept strict_evidence")
        event = {
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }
        diagnostics = {}

        ledger = build_resolved_ledger(
            "arbitrum",
            ADDRESS,
            "2026-07-18",
            snapshot_payload(),
            history_payload([event]),
            generated_at="2026-07-18T12:00:00Z",
            strict_evidence=strict_evidence(),
            diagnostics=diagnostics,
        )

        self.assertEqual("verified", ledger["markets"]["7"]["strictStatus"])
        self.assertEqual("verified", diagnostics["markets"]["7"]["status"])
        self.assertEqual("0", ledger["replayVerificationData"]["7"]["supplyParDiff"])
        self.assertFalse(ledger["replayVerificationData"]["7"]["replayStateAdjusted"])

    def test_exact_rpc_mismatch_stays_out_of_strict_ledger_and_is_diagnostic(self):
        event = {
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }
        diagnostics = {}

        ledger = build_resolved_ledger(
            "arbitrum",
            ADDRESS,
            "2026-07-18",
            snapshot_payload(),
            history_payload([event]),
            strict_evidence=strict_evidence(par="99", wei="120"),
            diagnostics=diagnostics,
        )

        self.assertIsNone(ledger)
        self.assertEqual("mismatch", diagnostics["markets"]["7"]["status"])
        self.assertEqual("-1", diagnostics["markets"]["7"]["supplyParDiff"])

    def test_incomplete_or_adjusted_rpc_evidence_is_never_published(self):
        event = {
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }
        for flag in ("snapshotIncomplete", "subgraphReplayTruncated", "replayStateAdjusted"):
            with self.subTest(flag=flag):
                diagnostics = {}
                ledger = build_resolved_ledger(
                    "arbitrum",
                    ADDRESS,
                    "2026-07-18",
                    snapshot_payload(),
                    history_payload([event]),
                    strict_evidence=strict_evidence(**{flag: True}),
                    diagnostics=diagnostics,
                )
                self.assertIsNone(ledger)
                self.assertEqual("coverage_incomplete", diagnostics["strictStatus"])

    def test_builds_exact_open_supply_ledger_from_pinned_snapshot(self):
        history = history_payload([{
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }])

        ledger = build_resolved_ledger(
            "arbitrum",
            ADDRESS,
            "2026-07-18",
            snapshot_payload(),
            history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertEqual(123, ledger["comparisonBlock"])
        self.assertEqual("20", ledger["markets"]["7"]["earnYield"])
        self.assertEqual("20", ledger["markets"]["7"]["openSupplyYield"])
        self.assertEqual("0", ledger["markets"]["7"]["settledSupplyYield"])
        self.assertTrue(ledger["replayVerificationData"]["7"]["rawVerified"])

    def test_counts_fully_closed_cycle_as_settled_supply_yield(self):
        payload = snapshot_payload(par="50", wei="60")
        history = history_payload([
            {"blockNumber": 90, "transactionIndex": 1, "logIndex": 1, "deltaWei": "100", "newPar": "100", "accountKnown": True},
            {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "-110", "newPar": "0", "accountKnown": True},
            {"blockNumber": 110, "transactionIndex": 1, "logIndex": 3, "deltaWei": "50", "newPar": "50", "accountKnown": True},
        ])

        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", payload, history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertEqual("10", ledger["markets"]["7"]["settledSupplyYield"])
        self.assertEqual("10", ledger["markets"]["7"]["openSupplyYield"])
        self.assertEqual("20", ledger["markets"]["7"]["earnYield"])

    def test_rejects_partial_reduction_without_exact_event_interest_index(self):
        history = history_payload([
            {"blockNumber": 90, "transactionIndex": 1, "logIndex": 1, "deltaWei": "100", "newPar": "100", "accountKnown": True},
            {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "-55", "newPar": "50", "accountKnown": True},
        ])

        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(par="50", wei="60"), history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertIsNone(ledger)

    def test_rejects_borrow_route_and_history_behind_snapshot_block(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "100", "newPar": "100", "accountKnown": True}
        self.assertIsNone(build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history_payload([event], has_borrow=True),
            generated_at="2026-07-18T12:00:00Z",
        ))
        self.assertIsNone(build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history_payload([event], last_scanned=122),
            generated_at="2026-07-18T12:00:00Z",
        ))

    def test_rejects_snapshot_mismatch_instead_of_publishing_inference(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "100", "newPar": "99", "accountKnown": True}
        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history_payload([event]),
            generated_at="2026-07-18T12:00:00Z",
        )
        self.assertIsNone(ledger)

    def test_rejects_head_fresh_history_without_full_canonical_backfill(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "100", "newPar": "100", "accountKnown": True}
        history = history_payload([event])
        history["scanRange"]["fromBlock"] = 29_750_001

        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertIsNone(ledger)

    def test_strict_cli_skips_rpc_for_ineligible_histories_with_precise_diagnostics(self):
        event = {
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }
        cases = [
            ("history_snapshot_date_mismatch", {
                **history_payload([event]),
                "sourceMetadata": {"latestSnapshotDate": "2026-07-17"},
            }),
            ("history_starts_after_protocol_start", {
                **history_payload([event]),
                "scanRange": {"fromBlock": 99_999_999, "toBlock": 130},
            }),
            ("stale_comparison_block", history_payload([event], last_scanned=122)),
            ("missing_canonical_accounts", {
                **history_payload([event]),
                "accounts": {},
            }),
            ("invalid_input", []),
            ("invalid_input", {
                **history_payload([event]),
                "sourceMetadata": [],
            }),
        ]
        for expected_reason, history in cases:
            with self.subTest(reason=expected_reason), tempfile.TemporaryDirectory() as temp_dir:
                rpc_calls, status = self._run_strict_cli(
                    Path(temp_dir), [ADDRESS], {ADDRESS: history}
                )

                diagnostic = status["chains"]["arbitrum"]["addresses"][ADDRESS]
                self.assertEqual(expected_reason, diagnostic["reason"])
                self.assertEqual([], rpc_calls)

    def test_strict_cli_preserves_address_file_order_and_reuses_one_chain_client(self):
        first = "0xffffffffffffffffffffffffffffffffffffffff"
        second = "0x0000000000000000000000000000000000000001"
        event = {
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }
        histories = {}
        for address in (first, second):
            history = history_payload([event])
            history["address"] = address
            histories[address] = history
        clients = []

        def capture_evidence(_chain, _address, _history, **kwargs):
            clients.append(kwargs.get("client"))
            kwargs["diagnostics"].update({"hits": 1, "misses": 2, "skipped": 3})
            return strict_evidence()

        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            ledger_builder, "RpcClient", return_value=object(), create=True
        ) as client_factory:
            rpc_calls, status = self._run_strict_cli(
                Path(temp_dir), [first, second], histories,
                fetch_side_effect=capture_evidence,
            )

        self.assertEqual([first, second], [call[1] for call in rpc_calls])
        self.assertEqual(1, client_factory.call_count)
        self.assertEqual(2, len(clients))
        self.assertIsNotNone(clients[0])
        self.assertIs(clients[0], clients[1])
        self.assertEqual(
            {"hits": 2, "misses": 4, "skipped": 6},
            status["chains"]["arbitrum"]["rpcEvidenceCache"],
        )

    def test_exported_preflight_returns_rpc_context_without_mutating_verified_rules(self):
        preflight = getattr(ledger_builder, "preflight_strict_rpc_evidence", None)
        if preflight is None:
            self.fail("preflight_strict_rpc_evidence must be exported")
        event = {
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }
        diagnostics = {}

        context = preflight(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(),
            history_payload([event]), diagnostics=diagnostics,
        )

        self.assertEqual(123, context["comparisonBlock"])
        self.assertEqual({"7"}, set(context["snapshotMarkets"]))
        self.assertEqual({}, diagnostics)


if __name__ == "__main__":
    unittest.main()
