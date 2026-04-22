import unittest

from build_earn_verified_ledger import _select_best_baseline


class BuildEarnVerifiedLedgerTest(unittest.TestCase):
    def test_select_best_baseline_prefers_closest_exact_match(self):
        chosen = _select_best_baseline(
            [
                {"name": "all-time-netflow", "diff": 5, "pre_snapshot_meta": None},
                {"name": "recent-cycle", "diff": 1, "pre_snapshot_meta": None},
            ],
            tolerance=5,
        )
        self.assertEqual(chosen["name"], "recent-cycle")

    def test_select_best_baseline_prefers_stronger_carry_window(self):
        chosen = _select_best_baseline(
            [
                {
                    "name": "all-time-netflow",
                    "diff": 15,
                    "pre_snapshot_meta": {
                        "residual": 0,
                        "tinyParDriftWindow": True,
                    },
                },
                {
                    "name": "recent-cycle",
                    "diff": 15,
                    "pre_snapshot_meta": {
                        "residual": 2,
                        "tinyParDriftWindow": False,
                    },
                },
            ],
            tolerance=1,
        )
        self.assertEqual(chosen["name"], "recent-cycle")


if __name__ == "__main__":
    unittest.main()
