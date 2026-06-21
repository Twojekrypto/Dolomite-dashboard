import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_exercisers
import validate_data


class TestOdoloExerciseMetrics(unittest.TestCase):
    def test_round_amount_preserves_dust_exercises(self):
        self.assertEqual(generate_exercisers.round_amount(0.000673978684860832), 0.000674)
        self.assertGreater(generate_exercisers.round_amount(0.000673978684860832), 0)
        self.assertEqual(generate_exercisers.round_amount(1234.5678), 1234.57)

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


if __name__ == "__main__":
    unittest.main()
