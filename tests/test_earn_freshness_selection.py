import inspect
import io
import json
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import select_earn_canonical_hot_addresses as selector
import materialize_earn_subaccount_history as materializer
from run_earn_data_correctness_pipeline import _ensure_plan, _materialize_task_argv
from scan_earn_netflow import CHAINS
from scripts.select_earn_publishable_histories import select_publishable_histories


class EarnFreshnessSelectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.history = self.root / "history"
        self.ledger = self.root / "ledger"
        self.snapshots = self.root / "snapshots"
        for directory in (self.history / "ethereum", self.ledger / "ethereum", self.snapshots):
            directory.mkdir(parents=True)
        self.addresses = ["0x" + str(i) * 40 for i in range(1, 7)]
        self.dust, self.large, self.small, self.unpriced, self.cold, self.missing = self.addresses
        self.date = "2026-09-07"
        self.payload = {
            "date": self.date,
            "chainMetadata": {"ethereum": {"blockNumber": 100, "interestIndexes": {}}},
            "snapshots": {"ethereum": {
                address: {"markets": {market: {"par": str(wei), "wei": str(wei), "decimals": 0}}}
                for address, market, wei in (
                    (self.dust, "1", 1), (self.large, "1", 1000),
                    (self.small, "1", 10), (self.unpriced, "2", 1), (self.missing, "1", 100),
                )
            }},
        }
        self.write(self.snapshots / "manifest.json", {"dates": [self.date], "chains": {self.date: ["ethereum"]}})
        self.write(self.snapshots / f"{self.date}.json", self.payload)
        self.write(self.history / "manifest.json", {"chains": {"ethereum": {"lastBlock": 100}}})
        self.assets = self.root / "assets.json"
        self.write(self.assets, {"rows": [{"chain": "ethereum", "marketId": "1", "price": "1"}]})
        for address, block in ((self.dust, 1), (self.large, 80), (self.small, 20), (self.unpriced, 10), (self.cold, 1)):
            self.write_history(address, block)
        for patched in (
            patch.object(selector, "SNAPSHOT_DIR", self.snapshots),
            patch.object(selector, "_load_known_addresses", return_value=self.addresses),
            patch.object(selector, "_score_netflow_wallets", return_value=None),
        ):
            patched.start()
            self.addCleanup(patched.stop)

    def write(self, path, payload):
        path.write_text(json.dumps(payload), encoding="utf-8")

    def write_history(self, address, block, date=None, *, from_block=1):
        self.write(self.history / "ethereum" / f"{address}.json", {
            "chain": "ethereum", "address": address, "lastScannedBlock": block,
            "scanRange": {"fromBlock": from_block, "toBlock": block},
            "sourceMetadata": {"latestSnapshotDate": date or self.date},
            "accounts": {"0": {"markets": {"1": {"events": []}}}},
        })

    def select(self, **kwargs):
        for key in kwargs:
            self.assertIn(key, inspect.signature(selector.build_selection).parameters,
                          f"selector must support opt-in {key}")
        options = dict(limit=1000, priority_files=[], include_priority_even_if_unknown=False,
                       history_dir=self.history, ledger_dir=self.ledger, assets_file=self.assets)
        options.update(kwargs)
        return selector.build_selection("ethereum", **options)

    def test_material_head_uses_oldest_within_tier_and_never_fills_with_dust(self):
        selected, _ = self.select(material_active_head=True)
        self.assertEqual(selected, [self.small, self.large, self.unpriced])

    def test_material_head_rotates_after_watermark_advances(self):
        selected, _ = self.select(material_active_head=True, limit=1)
        self.assertEqual(selected, [self.small])
        self.write_history(self.small, 100)
        self.write(self.history / "manifest.json", {"chains": {"ethereum": {"lastBlock": 120}}})
        selected, _ = self.select(material_active_head=True, limit=1)
        self.assertEqual(selected, [self.large])

    def test_actionable_background_returns_zero_when_complete(self):
        for address in self.addresses:
            self.write_history(address, 100)
        selected, _ = self.select(coverage_backfill=True, actionable_only=True)
        self.assertEqual(selected, [])
        legacy, _ = self.select(coverage_backfill=True)
        self.assertTrue(legacy, "legacy CLI behavior remains opt-in")

    def test_actionable_background_keeps_material_missing_and_stale_before_cold(self):
        selected, _ = self.select(coverage_backfill=True, actionable_only=True)
        self.assertEqual(selected[:3], [self.missing, self.large, self.small])
        self.assertLess(selected.index(self.unpriced), selected.index(self.dust))
        self.assertLess(selected.index(self.small), selected.index(self.cold))

    def test_actionable_background_keeps_head_fresh_incomplete_start_until_repaired(self):
        start = CHAINS["ethereum"]["start_block"]
        target = start + 100
        self.write(self.history / "manifest.json", {"chains": {"ethereum": {"lastBlock": target}}})
        for address in self.addresses:
            self.write_history(address, target, from_block=start)
        self.write_history(self.dust, target - 10, from_block=start)
        self.write_history(self.cold, target - 20, from_block=start)
        for incomplete_start in (start + 1, 0, None, "invalid"):
            with self.subTest(from_block=incomplete_start):
                self.write_history(self.small, target, from_block=incomplete_start)
                selected, metadata = self.select(coverage_backfill=True, actionable_only=True, limit=1)
                self.assertEqual(selected, [self.small], "material prefix gap must precede stale dust/cold")
                self.assertEqual(metadata["incompleteStartHistoryAddressCount"], 1)
        for address in (self.small, self.dust, self.cold):
            self.write_history(address, target, from_block=start)
        selected, metadata = self.select(coverage_backfill=True, actionable_only=True)
        self.assertEqual(selected, [])
        self.assertEqual(metadata["incompleteStartHistoryAddressCount"], 0)

    def test_late_start_priority_outside_known_universe_remains_actionable(self):
        start = CHAINS["ethereum"]["start_block"]
        target = start + 100
        self.write(self.history / "manifest.json", {"chains": {"ethereum": {"lastBlock": target}}})
        for address in self.addresses:
            self.write_history(address, target, from_block=start)
        pinned = "0x" + "7" * 40
        priority = self.root / "priority.txt"
        priority.write_text(pinned + "\n", encoding="utf-8")
        self.write_history(pinned, target, from_block=start + 1)
        selected, metadata = self.select(coverage_backfill=True, actionable_only=True,
                                         priority_files=[priority], include_priority_even_if_unknown=True)
        self.assertEqual(selected, [pinned])
        self.assertEqual(metadata["skippedFreshPriorityAddressCount"], 0)

    def test_selected_late_start_uses_full_one_pass_runtime_and_becomes_publishable(self):
        start = CHAINS["ethereum"]["start_block"]
        target = start + 100
        self.write(self.history / "manifest.json", {"chains": {"ethereum": {"lastBlock": target}}})
        for address in self.addresses:
            self.write_history(address, target, from_block=start)
        self.write_history(self.small, target, from_block=start + 50)
        selected, _ = self.select(coverage_backfill=True, actionable_only=True)
        self.assertEqual(selected, [self.small])
        selection_file = self.root / "coverage-addresses.txt"
        selection_file.write_text("\n".join(selected) + "\n", encoding="utf-8")

        # The coverage workflow uses a separate cohort runtime, never the public
        # late-start history as an incremental baseline or materializer output.
        events_dir = self.root / "coverage-runtime" / "events"
        runtime_history = self.root / "coverage-runtime" / "history"
        with patch("plan_earn_data_correctness.get_block_number", return_value=target):
            plan = _ensure_plan("ethereum", events_dir=events_dir, history_dir=runtime_history,
                                max_scan_workers=1, max_materialize_workers=1,
                                selection_address_file=selection_file, refresh=False)
        self.assertEqual(len(plan["scanTasks"]), 1)
        scan = plan["scanTasks"][0]
        self.assertEqual((scan["fromBlock"], scan["toBlock"]), (start, target))
        self.assertEqual(scan["addressFile"], str(selection_file))

        # Supply the scanner's boundary output, including an event before the
        # old history's start; use the real runner argv and materializer parser.
        (events_dir / "ethereum").mkdir(parents=True)
        event = {"owner": self.small, "account": "0", "market": "1",
                 "deltaWei": "10", "newPar": "10", "blockNumber": start + 1,
                 "transactionIndex": 0, "logIndex": 0, "transactionHash": "0x" + "a" * 64,
                 "event": "deposit", "flowType": "d", "accountKnown": True}
        self.write(events_dir / "ethereum" / f"{start:012d}-{target:012d}.json", {"owners": {self.small: [event]}})
        self.write(events_dir / "manifest.json", {"chains": {"ethereum": {
            "globalFromBlock": start, "globalToBlock": target,
            "scanRanges": [{"fromBlock": start, "toBlock": target}],
        }}})
        task = {**plan["materializeTasks"][0], "chain": "ethereum"}
        argv = _materialize_task_argv(task)
        with patch.object(sys, "argv", argv[1:]), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(materializer.main(), 0)
        self.assertIn('"materializedAddressCount": 1', output.getvalue())
        repaired_path = runtime_history / "ethereum" / f"{self.small}.json"
        repaired = json.loads(repaired_path.read_text(encoding="utf-8"))
        self.assertEqual(repaired["scanRange"]["fromBlock"], start)
        self.assertEqual(repaired["lastScannedBlock"], target)
        self.assertEqual(repaired["accounts"]["0"]["markets"]["1"]["events"][0]["blockNumber"], start + 1)
        publishable, rejected = select_publishable_histories(chain="ethereum", addresses=selected,
            history_dir=runtime_history, start_block=start, target_block=target)
        self.assertEqual((publishable, rejected), ([self.small], {}))
        shutil.copyfile(repaired_path, self.history / "ethereum" / f"{self.small}.json")
        self.assertEqual(self.select(coverage_backfill=True, actionable_only=True)[0], [])

    def test_strict_limit_counts_ready_wallets_and_reports_prerequisite_skips(self):
        self.write_history(self.large, 100, date="2026-09-06")
        self.write_history(self.small, 100)
        selected, metadata = self.select(strict_remediation=True, strict_rpc_ready_only=True, limit=1)
        self.assertEqual(selected, [self.small])
        reasons = metadata["skippedStrictPrerequisiteReasonCounts"]
        self.assertEqual(reasons["history_snapshot_date_mismatch"], 1)
        self.assertEqual(reasons["invalid_input"], 1)
        self.assertGreater(reasons["stale_comparison_block"], 0)
        self.assertEqual(metadata["activeStrictVerifiedAddressCount"], 0)

    def test_old_resolved_ledger_never_counts_as_latest_verified(self):
        ledger = {
            "snapshotDate": self.date,
            "markets": {"1": {"strictStatus": "inferred"}},
            "resolvedInterestLedger": {
                "snapshotDate": "2026-09-06", "comparisonBlock": 99,
                "strictStatus": "verified", "strictMethod": "interest-ledger",
                "markets": {"1": {"strictStatus": "verified", "strictMethod": "interest-ledger"}},
                "replayVerificationData": {"1": {"rawVerified": True}},
            },
        }
        path = self.ledger / "ethereum" / f"{self.small}.json"
        self.write(path, ledger)
        self.assertEqual(selector._active_strict_quality("ethereum", {self.small}, self.ledger)[self.small], "inferred")
        ledger["resolvedInterestLedger"].update(snapshotDate=self.date, comparisonBlock=100)
        self.write(path, ledger)
        self.assertEqual(selector._active_strict_quality("ethereum", {self.small}, self.ledger)[self.small], "verified")
