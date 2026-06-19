import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_exercisers
import validate_data


class TestOdoloExerciseMetrics(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
