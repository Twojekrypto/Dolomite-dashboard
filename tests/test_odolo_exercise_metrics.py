import os
import sys
import json
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_exercisers
import calculate_avg_lock
import odolo_exercises
import validate_data


class TestOdoloExerciseMetrics(unittest.TestCase):
    @staticmethod
    def _routescan_response(rows, status="1"):
        class Response:
            def json(self):
                return {"status": status, "message": "OK", "result": rows}

        return Response()

    @staticmethod
    def _routescan_tx(tx_hash, block_number, transaction_index):
        seven_days_hex = hex(7 * 86400)[2:].zfill(64)
        return {
            "hash": tx_hash,
            "blockNumber": str(block_number),
            "transactionIndex": str(transaction_index),
            "methodId": odolo_exercises.EXERCISE_METHOD_DOLO,
            "input": odolo_exercises.EXERCISE_METHOD_DOLO + seven_days_hex + "0" * 128,
            "from": "0x" + "1" * 40,
            "to": generate_exercisers.VESTER_CONTRACT,
            "timeStamp": "1700000000",
            "isError": "0",
            "txreceipt_status": "1",
        }

    def test_routescan_pagination_retries_before_returning_incomplete_duplicates(self):
        tx_a = self._routescan_tx("0x" + "a" * 64, 100, 0)
        tx_b = self._routescan_tx("0x" + "b" * 64, 101, 0)
        tx_c = self._routescan_tx("0x" + "c" * 64, 102, 0)
        tx_d = self._routescan_tx("0x" + "d" * 64, 103, 0)
        responses = [
            self._routescan_response([tx_a, tx_b]),
            self._routescan_response([tx_b, tx_c]),
            self._routescan_response([], status="0"),
            self._routescan_response([tx_a, tx_b]),
            self._routescan_response([tx_c, tx_d]),
            self._routescan_response([], status="0"),
        ]

        with (
            patch.object(generate_exercisers, "PAGE_SIZE", 2),
            patch.object(generate_exercisers.requests, "get", side_effect=responses),
            patch.object(generate_exercisers.time, "sleep"),
        ):
            transactions = generate_exercisers.get_all_transactions()

        self.assertEqual(
            [tx["hash"] for tx in transactions],
            [tx_a["hash"], tx_b["hash"], tx_c["hash"], tx_d["hash"]],
        )

    def test_routescan_pagination_retries_invalid_exercise_calldata(self):
        tx_a = self._routescan_tx("0x" + "a" * 64, 100, 0)
        tx_b = self._routescan_tx("0x" + "b" * 64, 101, 0)
        invalid_tx_b = dict(
            tx_b,
            input=odolo_exercises.EXERCISE_METHOD_DOLO + "0" * 192,
        )
        responses = [
            self._routescan_response([tx_a, invalid_tx_b]),
            self._routescan_response([], status="0"),
            self._routescan_response([tx_a, tx_b]),
            self._routescan_response([], status="0"),
        ]

        with (
            patch.object(generate_exercisers, "PAGE_SIZE", 2),
            patch.object(generate_exercisers.requests, "get", side_effect=responses),
            patch.object(generate_exercisers.time, "sleep"),
        ):
            transactions = generate_exercisers.get_all_transactions()

        self.assertEqual(transactions, [tx_a, tx_b])

    def test_sub_day_lock_duration_is_not_rounded_to_zero(self):
        timestamp = 1_700_000_000
        lock_end = timestamp + 2_228
        tx = {
            "methodId": odolo_exercises.EXERCISE_METHOD_USDC,
            "timeStamp": str(timestamp),
            "input": odolo_exercises.EXERCISE_METHOD_USDC
                + "0" * 128
                + hex(lock_end)[2:].zfill(64),
        }

        self.assertEqual(odolo_exercises.extract_lock_duration_days(tx), 0.0258)

    def test_protocol_discount_matches_the_deployed_week_rounding_schedule(self):
        self.assertTrue(hasattr(odolo_exercises, "protocol_discount_pct"))
        discount = odolo_exercises.protocol_discount_pct
        self.assertAlmostEqual(discount(int(3.5 * 86400)), 2.5, places=9)
        self.assertAlmostEqual(discount(7 * 86400), 5, places=9)
        self.assertAlmostEqual(discount(14 * 86400), 5 + 45 / 103, places=9)
        self.assertAlmostEqual(discount(365 * 86400), 5 + 45 * 52 / 103, places=9)
        self.assertAlmostEqual(discount(int(721.1 * 86400)), 50, places=9)

    def test_average_discount_is_the_mean_of_exact_transaction_discounts(self):
        self.assertTrue(hasattr(calculate_avg_lock, "average_discount_pct"))
        durations = [int(3.5 * 86400), 14 * 86400]
        expected = (2.5 + 5 + 45 / 103) / 2
        self.assertAlmostEqual(calculate_avg_lock.average_discount_pct(durations), expected, places=9)

    def test_legacy_zero_usdc_receipt_is_refetched_once(self):
        legacy = {"paid_token": "USDC.e", "usdc": 0, "odolo": 0.00001}
        verified = {
            "paid_token": "USDC.e",
            "usdc": 0,
            "odolo": 0.00001,
            "receipt_version": generate_exercisers.RECEIPT_CACHE_VERSION,
        }

        self.assertTrue(generate_exercisers.cache_entry_needs_receipt_refresh(legacy))
        self.assertFalse(generate_exercisers.cache_entry_needs_receipt_refresh(verified))

    def test_round_amount_preserves_dust_exercises(self):
        self.assertEqual(generate_exercisers.round_amount(0.000673978684860832), 0.000674)
        self.assertGreater(generate_exercisers.round_amount(0.000673978684860832), 0)
        self.assertEqual(generate_exercisers.round_amount(1234.5678), 1234.57)

    def test_existing_output_seed_preserves_exact_lock_seconds(self):
        tx_hash = "0x" + "a" * 64
        payload = {
            "exercisers": [{
                "txs": [{
                    "hash": tx_hash,
                    "vedolo": 100,
                    "usdc": 10,
                    "lock_days": 14,
                    "lock_seconds": 14 * 86400,
                    "paid_token": "USDC.e",
                    "receipt_version": generate_exercisers.RECEIPT_CACHE_VERSION,
                }]
            }]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "exercisers_by_address.json")
            with open(output, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            with patch.object(generate_exercisers, "OUTPUT_FILE", output):
                cache = generate_exercisers.seed_cache_from_existing_output()

        self.assertEqual(cache[tx_hash]["lock_seconds"], 14 * 86400)

    def test_generator_splits_usdc_exercise_from_dolo_pairing(self):
        exercisers = [{
            "txs": [
                {"paid_token": "USDC.e", "vedolo": 1000.0, "usdc": 50.0},
                {"paid_token": "DOLO", "vedolo": 2500.0, "dolo_paid": 2500.0},
            ]
        }]

        totals = generate_exercisers.summarize_exercise_totals(exercisers)

        self.assertEqual(totals["total_vedolo"], 3500.0)
        self.assertEqual(totals["total_odolo_exercised"], 1000.0)
        self.assertEqual(totals["total_odolo_exercise_usdc_paid"], 50.0)
        self.assertEqual(totals["total_odolo_exercised_exercises"], 1)
        self.assertEqual(totals["total_dolo_pair_vedolo"], 2500.0)
        self.assertEqual(totals["total_dolo_pair_exercises"], 1)
        self.assertEqual(totals["total_dolo_paired"], 2500.0)

    def test_validation_rejects_all_methods_as_odolo_exercised(self):
        data = {
            "total_vedolo": 3500.0,
            "total_odolo_exercised": 1000.0,
            "total_odolo_exercise_usdc_paid": 50.0,
            "total_odolo_exercised_exercises": 1,
            "total_dolo_pair_vedolo": 2500.0,
            "total_dolo_pair_exercises": 1,
            "total_dolo_paired": 2500.0,
            "exercisers": [{
                "txs": [
                    {"paid_token": "USDC.e", "vedolo": 1000.0, "usdc": 50.0},
                    {"paid_token": "DOLO", "vedolo": 2500.0, "dolo_paid": 2500.0},
                ]
            }],
        }
        self.assertTrue(validate_data._odolo_exerciser_totals_reconcile(data))

        data["total_odolo_exercised"] = 3500.0
        self.assertFalse(validate_data._odolo_exerciser_totals_reconcile(data))

    def test_holder_reconciliation_keeps_current_locked_separate(self):
        totals = {
            "total_vedolo": 1444183.95,
            "total_odolo_exercised": 706038.35,
        }
        fields = generate_exercisers.holder_reconciliation_fields(
            totals,
            {"total_dolo": 1124773.26, "nft_count": 50, "total_vote_weight": 751718.697},
        )

        self.assertEqual(fields["current_vedolo_locked"], 1124773.26)
        self.assertEqual(fields["current_vedolo_positions"], 50)
        self.assertEqual(fields["current_vedolo_vote_weight"], 751718.697)
        self.assertEqual(fields["current_locked_delta_vs_usdc_exercise"], 418734.91)
        self.assertEqual(fields["current_locked_delta_vs_all_exercise_history"], -319410.69)
        self.assertFalse(fields["current_vedolo_route_breakdown_available"])

    def test_holder_reconciliation_splits_current_positions_by_route(self):
        totals = {
            "total_vedolo": 3000.0,
            "total_odolo_exercised": 2000.0,
        }
        holder = {
            "total_dolo": 2500.0,
            "nft_count": 2,
            "total_vote_weight": 1800.0,
            "token_ids": [101, 999],
            "token_amounts": {101: 1500.0, 999: 1000.0},
        }
        txs = [
            {"paid_token": "USDC.e", "vedolo": 1000.0, "token_ids": [101]},
            {"paid_token": "USDC.e", "vedolo": 1000.0, "token_ids": [102]},
            {"paid_token": "DOLO", "vedolo": 1000.0, "dolo_paid": 1000.0},
        ]

        fields = generate_exercisers.holder_reconciliation_fields(totals, holder, txs)

        self.assertTrue(fields["current_vedolo_route_breakdown_available"])
        self.assertEqual(fields["current_usdc_exercise_positions"], 1)
        self.assertEqual(fields["current_usdc_exercise_locked"], 1500.0)
        self.assertEqual(fields["current_other_vedolo_positions"], 1)
        self.assertEqual(fields["current_other_vedolo_locked"], 1000.0)
        self.assertEqual(fields["current_exercise_positions_missing_from_holder_snapshot"], 1)

    def test_validation_requires_per_address_reconciliation_fields(self):
        data = {
            "exercisers": [{
                "total_vedolo": 3500.0,
                "total_odolo_exercised": 1000.0,
                "total_odolo_exercise_usdc_paid": 50.0,
                "total_odolo_exercised_exercises": 1,
                "total_dolo_pair_vedolo": 2500.0,
                "total_dolo_pair_exercises": 1,
                "total_dolo_paired": 2500.0,
                "current_vedolo_locked": 4200.0,
                "current_vedolo_positions": 2,
                "current_vedolo_vote_weight": 3000.0,
                "current_locked_delta_vs_usdc_exercise": 3200.0,
                "current_locked_delta_vs_all_exercise_history": 700.0,
                "txs": [
                    {"paid_token": "USDC.e", "vedolo": 1000.0, "usdc": 50.0},
                    {"paid_token": "DOLO", "vedolo": 2500.0, "dolo_paid": 2500.0},
                ],
            }],
        }

        self.assertTrue(validate_data._odolo_exerciser_address_totals_reconcile(data))
        del data["exercisers"][0]["current_vedolo_locked"]
        self.assertFalse(validate_data._odolo_exerciser_address_totals_reconcile(data))

    def test_validation_rejects_duplicate_or_misdated_exercise_transactions(self):
        tx = {
            "hash": "0x" + "a" * 64,
            "date": "2023-11-14",
            "timestamp": 1_700_000_000,
            "paid_token": "USDC.e",
            "usdc": 10,
            "vedolo": 100,
            "lock_days": 7,
            "lock_seconds": 7 * 86400,
        }
        data = {"exercisers": [{"txs": [tx]}]}
        self.assertTrue(validate_data._odolo_exercise_transactions_are_valid(data))

        data["exercisers"][0]["txs"].append(dict(tx))
        self.assertFalse(validate_data._odolo_exercise_transactions_are_valid(data))

    def test_validation_requires_exact_lock_seconds_for_discount_math(self):
        tx = {
            "hash": "0x" + "b" * 64,
            "date": "2023-11-14",
            "timestamp": 1_700_000_000,
            "paid_token": "USDC.e",
            "usdc": 10,
            "vedolo": 100,
            "lock_days": 14,
            "lock_seconds": 14 * 86400,
        }
        data = {"exercisers": [{"txs": [tx]}]}
        self.assertTrue(validate_data._odolo_exercise_transactions_are_valid(data))
        del tx["lock_seconds"]
        self.assertFalse(validate_data._odolo_exercise_transactions_are_valid(data))


if __name__ == "__main__":
    unittest.main()
