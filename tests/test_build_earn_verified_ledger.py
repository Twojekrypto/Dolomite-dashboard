import unittest

from build_earn_verified_ledger import _derive_strict_verification, _select_best_baseline


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

    def test_snapshot_netflow_match_is_inferred_even_with_fresh_canonical_history(self):
        strict_status, strict_method, strict_reason = _derive_strict_verification(
            "verified",
            "netflow+snapshot",
            canonical_coverage_status="fresh",
            canonical_consistency_status="match",
        )
        self.assertEqual(strict_status, "inferred")
        self.assertEqual(strict_method, "netflow+snapshot")
        self.assertEqual(strict_reason, "snapshot_netflow_match_requires_inference")


if __name__ == "__main__":
    unittest.main()
