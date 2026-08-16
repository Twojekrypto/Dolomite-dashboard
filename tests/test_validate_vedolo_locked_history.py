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
                {"txHash": "0x" + "1" * 64, "tokenId": 1, "block": 10, "timestamp": 100, "locktime": 500, "dolo": 1_000_000},
                {"txHash": "0x" + "2" * 64, "tokenId": 2, "block": 20, "timestamp": 120, "locktime": 300, "dolo": 500_000},
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

    def test_rejects_duplicate_event_identity(self):
        self.flows["locks"].append(dict(self.flows["locks"][0]))
        self.flows["total_locks"] = 3
        with self.assertRaisesRegex(LockedHistoryValidationError, "duplicate lock event"):
            validate_locked_history(self.flows, self.holders)


if __name__ == "__main__":
    unittest.main()
