import unittest

from validate_vedolo_locked_history import (
    LockedHistoryValidationError,
    active_locked_dolo_from_holders,
    reconstructed_active_locked_dolo,
    validate_locked_history,
)


class ValidateVedoloLockedHistoryTests(unittest.TestCase):
    def setUp(self):
        self.flows = {
            "timestamp": "2026-08-16T00:00:00+00:00",
            "total_locks": 2,
            "total_unlocks": 1,
            "total_transfers": 0,
            "locks": [
                {"txHash": "0x" + "1" * 64, "tokenId": 1, "block": 10, "timestamp": 100, "locktime": 500, "dolo": 1_000_000, "depositType": 1},
                {"txHash": "0x" + "2" * 64, "tokenId": 2, "block": 20, "timestamp": 120, "locktime": 300, "dolo": 500_000, "depositType": 1},
            ],
            "unlocks": [
                {"txHash": "0x" + "3" * 64, "tokenId": 2, "block": 30, "timestamp": 200, "dolo": 500_000},
            ],
            "transfers": [],
        }
        self.holders = {
            "timestamp": "1970-01-01T00:06:40+00:00",
            "holders": [
                {"address": "0x" + "a" * 40, "token_details": [{"id": 1, "dolo": 1_000_000, "end": 500}]},
                {"address": "0x" + "b" * 40, "token_details": [{"id": 2, "dolo": 500_000, "end": 300}]},
            ],
        }

    def test_reconciles_event_series_to_active_unexpired_holder_positions(self):
        self.assertEqual(reconstructed_active_locked_dolo(self.flows, 400), 1_000_000)
        self.assertEqual(active_locked_dolo_from_holders(self.holders, 400), 1_000_000)
        result = validate_locked_history(self.flows, self.holders, max_absolute_gap=1)
        self.assertEqual(result["gap"], 0)

    def test_rejects_material_event_history_gap(self):
        self.flows["locks"] = self.flows["locks"][1:]
        self.flows["total_locks"] = 1
        with self.assertRaisesRegex(LockedHistoryValidationError, "active balance mismatch"):
            validate_locked_history(self.flows, self.holders, max_absolute_gap=1)

    def test_default_validation_rejects_even_a_small_unexplained_gap(self):
        self.holders["holders"][0]["token_details"][0]["dolo"] += 2
        with self.assertRaisesRegex(LockedHistoryValidationError, "active balance mismatch"):
            validate_locked_history(self.flows, self.holders)

    def test_rejects_equal_total_when_active_token_ids_do_not_match(self):
        self.holders["holders"][0]["token_details"][0]["id"] = 999
        with self.assertRaisesRegex(LockedHistoryValidationError, "active position IDs mismatch"):
            validate_locked_history(self.flows, self.holders)

    def test_rejects_equal_total_when_per_position_amounts_do_not_match(self):
        self.flows["locks"].append({
            "txHash": "0x" + "6" * 64,
            "tokenId": 3,
            "block": 25,
            "timestamp": 150,
            "locktime": 500,
            "dolo": 100,
            "depositType": 1,
        })
        self.flows["total_locks"] = 3
        self.holders["holders"].append({
            "address": "0x" + "c" * 40,
            "token_details": [{"id": 3, "dolo": 200, "end": 500}],
        })
        self.holders["holders"][0]["token_details"][0]["dolo"] -= 100
        with self.assertRaisesRegex(LockedHistoryValidationError, "active position amount mismatch"):
            validate_locked_history(self.flows, self.holders)

    def test_rejects_orphan_unlock_and_increase_events(self):
        orphan_unlock = {
            "total_locks": 0,
            "total_unlocks": 1,
            "locks": [],
            "unlocks": [{
                "txHash": "0x" + "4" * 64,
                "tokenId": 44,
                "block": 1,
                "timestamp": 100,
                "dolo": 10,
            }],
        }
        with self.assertRaisesRegex(LockedHistoryValidationError, "unlock source position #44 is missing"):
            reconstructed_active_locked_dolo(orphan_unlock, 200)

        for deposit_type in (0, 2):
            with self.subTest(deposit_type=deposit_type):
                orphan_increase = {
                    "total_locks": 1,
                    "total_unlocks": 0,
                    "locks": [{
                        "txHash": "0x" + "5" * 64,
                        "tokenId": 55,
                        "block": 1,
                        "timestamp": 100,
                        "locktime": 500,
                        "dolo": 10,
                        "depositType": deposit_type,
                    }],
                    "unlocks": [],
                }
                with self.assertRaisesRegex(LockedHistoryValidationError, "increase source position #55 is missing"):
                    reconstructed_active_locked_dolo(orphan_increase, 200)

    def test_rejects_duplicate_event_identity(self):
        self.flows["locks"].append(dict(self.flows["locks"][0]))
        self.flows["total_locks"] = 3
        with self.assertRaisesRegex(LockedHistoryValidationError, "duplicate lock event"):
            validate_locked_history(self.flows, self.holders)

    def test_merge_split_and_early_withdraw_preserve_exact_active_principal(self):
        day = 86_400
        flows = {
            "total_locks": 5,
            "total_unlocks": 1,
            "locks": [
                {"txHash": "0x" + "1" * 64, "tokenId": 1, "block": 1, "timestamp": day, "locktime": 15 * day, "dolo": 100, "depositType": 1},
                {"txHash": "0x" + "2" * 64, "tokenId": 2, "block": 2, "timestamp": 2 * day, "locktime": 20 * day, "dolo": 50, "depositType": 1},
                {"txHash": "0x" + "3" * 64, "tokenId": 2, "block": 3, "timestamp": 3 * day, "locktime": 21 * day, "dolo": 0, "depositType": 3},
                {"txHash": "0x" + "4" * 64, "tokenId": 1, "sourceTokenId": 2, "targetTokenId": 1, "block": 4, "timestamp": 4 * day, "locktime": 21 * day, "dolo": 50, "depositType": 4},
                {"txHash": "0x" + "5" * 64, "tokenId": 3, "sourceTokenId": 1, "targetTokenId": 3, "block": 5, "timestamp": 5 * day, "locktime": 21 * day, "dolo": 40, "depositType": 5},
            ],
            "unlocks": [
                {"txHash": "0x" + "6" * 64, "tokenId": 3, "block": 6, "timestamp": 6 * day, "dolo": 40},
            ],
        }
        self.assertEqual(reconstructed_active_locked_dolo(flows, 17 * day), 110)

    def test_rejects_merge_without_exact_token_transition(self):
        self.flows["locks"].append({
            "txHash": "0x" + "4" * 64,
            "tokenId": 1,
            "block": 40,
            "timestamp": 250,
            "locktime": 600,
            "dolo": 500_000,
            "depositType": 4,
        })
        self.flows["total_locks"] = 3
        with self.assertRaisesRegex(LockedHistoryValidationError, "merge transition is incomplete"):
            reconstructed_active_locked_dolo(self.flows, 400)

    def test_rejects_merge_when_target_position_history_is_missing(self):
        flows = {
            "total_locks": 2,
            "total_unlocks": 0,
            "locks": [
                {"txHash": "0x" + "1" * 64, "tokenId": 2, "block": 1, "timestamp": 100, "locktime": 500, "dolo": 50, "depositType": 1},
                {"txHash": "0x" + "2" * 64, "tokenId": 1, "sourceTokenId": 2, "targetTokenId": 1, "block": 2, "timestamp": 200, "locktime": 500, "dolo": 50, "depositType": 4},
            ],
            "unlocks": [],
        }
        with self.assertRaisesRegex(LockedHistoryValidationError, "merge target position #1 is missing"):
            reconstructed_active_locked_dolo(flows, 300)

    def test_accepts_split_that_rounds_below_display_precision(self):
        day = 86_400
        flows = {
            "total_locks": 2,
            "total_unlocks": 0,
            "locks": [
                {"txHash": "0x" + "1" * 64, "tokenId": 7, "block": 1, "timestamp": day, "locktime": 20 * day, "dolo": 10, "depositType": 1},
                {"txHash": "0x" + "2" * 64, "tokenId": 8, "sourceTokenId": 7, "targetTokenId": 8, "block": 2, "timestamp": 2 * day, "locktime": 20 * day, "dolo": 0, "depositType": 5},
            ],
            "unlocks": [],
        }
        self.assertEqual(reconstructed_active_locked_dolo(flows, 3 * day), 10)


if __name__ == "__main__":
    unittest.main()
