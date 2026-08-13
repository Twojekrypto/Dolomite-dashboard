import copy
import unittest

import fetch_early_exits
import validate_data


class ValidateEarlyExitsTest(unittest.TestCase):
    @staticmethod
    def _payload():
        early_tx = "0x" + "11" * 32
        normal_tx = "0x" + "22" * 32
        events = [
            {
                "event_id": f"{early_tx}:4",
                "tx_hash": early_tx,
                "log_index": 4,
                "provider": "0x" + "33" * 20,
                "value_raw": "70",
                "token_id": 1,
                "timestamp": 1_700_000_004,
                "block": 3_000_000,
            },
            {
                "event_id": f"{normal_tx}:8",
                "tx_hash": normal_tx,
                "log_index": 8,
                "provider": "0x" + "44" * 20,
                "value_raw": "100",
                "token_id": 2,
                "timestamp": 1_700_000_008,
                "block": 3_000_001,
            },
        ]
        calculations = {
            events[0]["event_id"]: {
                "burn_fee_raw": "5",
                "recoup_fee_raw": "25",
                "total_penalty_raw": "30",
                "original_locked_raw": "100",
                "user_received_raw": "70",
                "is_early_exit": True,
            },
            events[1]["event_id"]: {
                "burn_fee_raw": "0",
                "recoup_fee_raw": "0",
                "total_penalty_raw": "0",
                "original_locked_raw": "100",
                "user_received_raw": "100",
                "is_early_exit": False,
            },
        }
        return fetch_early_exits.build_output(events, calculations, 3_000_001)

    def test_early_exit_rules_require_complete_exact_reconciliation(self):
        rules = validate_data.RULES["early_exits.json"]
        descriptions = {description for description, _ in rules["checks"]}

        self.assertIn("early exits must have complete event coverage", descriptions)
        self.assertIn("early exit rows and totals must reconcile exactly", descriptions)

    def test_exact_generated_payload_passes_both_checks(self):
        payload = self._payload()

        self.assertTrue(validate_data._early_exit_coverage_complete(payload))
        self.assertTrue(validate_data._early_exit_rows_reconcile(payload))

    def test_missing_event_or_wrong_total_fails_closed(self):
        payload = self._payload()
        payload["stats"]["total_withdrawals"] += 1
        self.assertFalse(validate_data._early_exit_coverage_complete(payload))

        payload = self._payload()
        payload["stats"]["total_recoup_fee_raw"] = "26"
        self.assertFalse(validate_data._early_exit_rows_reconcile(payload))

    def test_duplicate_event_id_and_decimal_rounding_fail_closed(self):
        payload = self._payload()
        duplicate = copy.deepcopy(payload["recent_exits"][0])
        payload["recent_exits"].append(duplicate)
        payload["stats"]["total_early_exits"] = 2
        self.assertFalse(validate_data._early_exit_rows_reconcile(payload))

        payload = self._payload()
        payload["recent_exits"][0]["total_penalty"] = "0"
        self.assertFalse(validate_data._early_exit_rows_reconcile(payload))


if __name__ == "__main__":
    unittest.main()
