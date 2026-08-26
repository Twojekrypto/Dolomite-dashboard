import unittest
from pathlib import Path


try:
    from generate_vedolo_early_unlock import (
        SimulationDataError,
        build_simulation_payload,
    )
except ModuleNotFoundError:
    SimulationDataError = RuntimeError
    build_simulation_payload = None

try:
    from generate_vedolo_early_unlock import build_current_holders
except ImportError:
    build_current_holders = None

try:
    from validate_data import _vedolo_early_unlock_valid
except ImportError:
    _vedolo_early_unlock_valid = None


WAD = 10**18
WEEK = 7 * 24 * 60 * 60
NOW = 1_800_000_000


class GenerateVedoloEarlyUnlockTests(unittest.TestCase):
    def test_builds_current_wallets_from_pinned_owner_reads(self):
        self.assertIsNotNone(build_current_holders, "current owner grouping is not implemented")
        zero = "0x" + "0" * 40
        owner_a = "0x" + "a" * 40
        owner_b = "0x" + "b" * 40
        holders = build_current_holders(
            [1, 2, 3, 4],
            {1: owner_b, 2: zero, 3: owner_a, 4: owner_b},
        )
        self.assertEqual(holders, [
            {"address": owner_a, "token_ids": [3]},
            {"address": owner_b, "token_ids": [1, 4]},
        ])

    def test_aggregates_exact_position_quotes_and_dolo_weighted_remaining_time(self):
        self.assertIsNotNone(build_simulation_payload, "early-unlock generator is not implemented")

        holders = [
            {"address": "0x" + "a" * 40, "token_ids": [1, 2]},
            {"address": "0x" + "b" * 40, "token_ids": [3]},
        ]
        position_reads = {
            1: {"amount_wei": 100 * WAD, "end": NOW + 10 * WEEK, "vote_weight_wei": 40 * WAD},
            2: {"amount_wei": 50 * WAD, "end": NOW - WEEK, "vote_weight_wei": 0},
            3: {"amount_wei": 25 * WAD, "end": NOW - WEEK, "vote_weight_wei": 0},
        }
        fee_quotes = {
            1: {"burn_fee_wei": 5 * WAD, "recoup_fee_wei": 10 * WAD},
            2: {"burn_fee_wei": 0, "recoup_fee_wei": 0},
            3: {"burn_fee_wei": 0, "recoup_fee_wei": 0},
        }

        payload = build_simulation_payload(
            holders,
            position_reads,
            fee_quotes,
            snapshot_timestamp=NOW,
            source_block=12_345,
            fee_calculator="0x" + "f" * 40,
        )

        self.assertEqual(payload["schemaVersion"], 1)
        self.assertEqual(payload["sourceBlock"], 12_345)
        self.assertEqual(payload["walletCount"], 1)
        self.assertEqual(payload["positionCount"], 2)
        row = payload["wallets"][0]
        self.assertEqual(row["address"], "0x" + "a" * 40)
        self.assertEqual(row["lockedDoloWei"], str(150 * WAD))
        self.assertEqual(row["veDoloWei"], str(40 * WAD))
        self.assertEqual(row["availableAfterExitWei"], str(135 * WAD))
        self.assertEqual(row["penaltyWei"], str(15 * WAD))
        self.assertAlmostEqual(row["avgWeeksUntilUnlock"], 20 / 3, places=6)
        self.assertEqual([p["status"] for p in row["positions"]], ["early_exit", "expired"])
        self.assertEqual(row["positions"][0]["availableAfterExitWei"], str(85 * WAD))
        self.assertEqual(row["positions"][1]["availableAfterExitWei"], str(50 * WAD))

    def test_preserves_one_wei_recovery_without_float_arithmetic(self):
        self.assertIsNotNone(build_simulation_payload, "early-unlock generator is not implemented")

        payload = build_simulation_payload(
            [{"address": "0x" + "c" * 40, "token_ids": [7]}],
            {7: {"amount_wei": 3, "end": NOW + WEEK, "vote_weight_wei": 2}},
            {7: {"burn_fee_wei": 1, "recoup_fee_wei": 1}},
            snapshot_timestamp=NOW,
            source_block=99,
            fee_calculator="0x" + "d" * 40,
        )

        row = payload["wallets"][0]
        self.assertEqual(row["lockedDoloWei"], "3")
        self.assertEqual(row["availableAfterExitWei"], "1")
        self.assertEqual(row["positions"][0]["penaltyWei"], "2")

    def test_rejects_missing_position_quotes_instead_of_publishing_partial_wallets(self):
        self.assertIsNotNone(build_simulation_payload, "early-unlock generator is not implemented")

        with self.assertRaises(SimulationDataError):
            build_simulation_payload(
                [{"address": "0x" + "e" * 40, "token_ids": [11]}],
                {11: {"amount_wei": WAD, "end": NOW + WEEK, "vote_weight_wei": WAD}},
                {},
                snapshot_timestamp=NOW,
                source_block=100,
                fee_calculator="0x" + "f" * 40,
            )

    def test_validator_reconciles_wallet_and_position_wei_totals(self):
        self.assertIsNotNone(_vedolo_early_unlock_valid, "early-unlock validator is not implemented")
        payload = build_simulation_payload(
            [{"address": "0x" + "a" * 40, "token_ids": [1]}],
            {1: {"amount_wei": 100 * WAD, "end": NOW + WEEK, "vote_weight_wei": 40 * WAD}},
            {1: {"burn_fee_wei": 5 * WAD, "recoup_fee_wei": 10 * WAD}},
            snapshot_timestamp=NOW,
            source_block=12_345,
            fee_calculator="0x" + "f" * 40,
        )
        self.assertTrue(_vedolo_early_unlock_valid(payload))

        payload["wallets"][0]["availableAfterExitWei"] = str(86 * WAD)
        self.assertFalse(_vedolo_early_unlock_valid(payload))

    def test_update_workflow_generates_validates_and_commits_snapshot(self):
        workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "update-data.yml").read_text()
        generator = workflow.index("run: python3 generate_vedolo_early_unlock.py")
        validation = workflow.index("validate_data.py vedolo_holders.json")
        commit = workflow.index("git add vedolo_holders.json")
        self.assertLess(generator, validation)
        self.assertLess(validation, commit)
        self.assertIn("vedolo_early_unlock.json", workflow[validation:commit])
        self.assertIn("vedolo_early_unlock.json", workflow[commit:])


if __name__ == "__main__":
    unittest.main()
