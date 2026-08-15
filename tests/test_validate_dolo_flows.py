import copy
import unittest

import validate_data


def valid_cex_history_payload():
    return {
        "cex_supply_history": [
            {
                "key": "hist_20260814",
                "timestamp": "2026-08-14T00:00:00Z",
                "liquid": 30.01,
                "wallets": 3,
                "exchanges": [
                    {"name": "Alpha", "liquid": 20.0, "wallets": 2},
                    {"name": "Beta", "liquid": 10.01, "wallets": 1},
                ],
            }
        ]
    }


class ValidateDoloFlowsTest(unittest.TestCase):
    @staticmethod
    def _check(payload):
        checks = dict(validate_data.RULES["dolo_flows.json"]["checks"])
        return checks["CEX exchange history must reconcile exactly"](payload)

    def test_cex_exchange_history_is_required_by_the_dolo_flows_rule(self):
        rules = validate_data.RULES["dolo_flows.json"]

        self.assertIn("cex_supply_history", rules["required_keys"])
        self.assertIn("CEX exchange history must reconcile exactly", dict(rules["checks"]))

    def test_valid_cex_exchange_history_passes(self):
        self.assertTrue(self._check(valid_cex_history_payload()))

    def test_positive_cex_supply_requires_nonempty_exchange_rows(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"] = []

        self.assertFalse(self._check(payload))

    def test_cex_exchange_rows_require_unique_nonempty_names(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"][1]["name"] = "Alpha"
        self.assertFalse(self._check(payload))

        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"][0]["name"] = ""
        self.assertFalse(self._check(payload))

    def test_cex_exchange_rows_reject_nonfinite_negative_and_boolean_fields(self):
        malformed_rows = [
            {"name": "Alpha", "liquid": float("nan"), "wallets": 2},
            {"name": "Alpha", "liquid": -0.01, "wallets": 2},
            {"name": "Alpha", "liquid": 20.0, "wallets": True},
            {"name": "Alpha", "liquid": 20.0, "wallets": -1},
        ]
        for row in malformed_rows:
            with self.subTest(row=row):
                payload = valid_cex_history_payload()
                payload["cex_supply_history"][0]["exchanges"][0] = row
                self.assertFalse(self._check(payload))

    def test_cex_exchange_rows_must_be_sorted_by_balance_then_name(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"].reverse()

        self.assertFalse(self._check(payload))

    def test_cex_exchange_balances_must_match_point_total_in_cents(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["liquid"] = 30.02

        self.assertFalse(self._check(payload))

    def test_cex_exchange_wallets_must_match_point_wallet_count(self):
        payload = copy.deepcopy(valid_cex_history_payload())
        payload["cex_supply_history"][0]["wallets"] = 4

        self.assertFalse(self._check(payload))

    def test_optional_flow_transaction_metadata_is_all_or_none_and_exact(self):
        row = {
            "address": "0x" + "1" * 40,
            "latest_tx_hash": "0x" + "a" * 64,
            "latest_tx_timestamp": 1_786_406_400,
            "latest_tx_chain": "ethereum",
        }
        payload = {"periods": {"7d": {"eth": {"accumulators": [row], "sellers": []}}}}
        self.assertTrue(validate_data._flow_tx_metadata_is_valid(payload))

        for key, invalid in (
            ("latest_tx_hash", "0x1234"),
            ("latest_tx_timestamp", True),
            ("latest_tx_chain", "arbitrum"),
        ):
            with self.subTest(key=key):
                malformed = copy.deepcopy(payload)
                malformed["periods"]["7d"]["eth"]["accumulators"][0][key] = invalid
                self.assertFalse(validate_data._flow_tx_metadata_is_valid(malformed))

        partial = copy.deepcopy(payload)
        partial["periods"]["7d"]["eth"]["accumulators"][0].pop("latest_tx_timestamp")
        self.assertFalse(validate_data._flow_tx_metadata_is_valid(partial))

    def test_dolo_flow_rule_registers_transaction_metadata_guard(self):
        self.assertIn(
            "optional latest transaction metadata must be exact",
            dict(validate_data.RULES["dolo_flows.json"]["checks"]),
        )


if __name__ == "__main__":
    unittest.main()
