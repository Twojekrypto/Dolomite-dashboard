import unittest

import validate_data


def _payload(period_counts):
    return {
        "timestamp": "2026-06-20T21:00:00",
        "current_block": 22_500_000,
        "deploy_block": 3_500_000,
        "cutoff_blocks": {
            "1d": 22_456_800,
            "7d": 22_197_600,
            "30d": 21_204_000,
            "90d": 18_612_000,
            "180d": 14_724_000,
            "all": 3_500_000,
        },
        "transfer_coverage": {
            "oldest_needed_block": 3_500_000,
            "scanned_from_block": 3_500_000,
            "min_cached_block": 3_500_000,
            "max_cached_block": 22_499_900,
            "transfer_count": period_counts["all"],
        },
        "periods": {
            period: {"total_transfers": count}
            for period, count in period_counts.items()
        },
    }


class OdoloFlowsValidationTests(unittest.TestCase):
    def test_rejects_collapsed_period_windows(self):
        payload = _payload({
            "1d": 100_000,
            "7d": 100_000,
            "30d": 100_000,
            "90d": 100_000,
            "180d": 100_000,
            "all": 100_000,
        })

        self.assertFalse(validate_data._odolo_flow_windows_are_not_collapsed(payload))

    def test_accepts_monotonic_distinct_windows_with_full_coverage(self):
        payload = _payload({
            "1d": 100,
            "7d": 900,
            "30d": 4_000,
            "90d": 12_000,
            "180d": 30_000,
            "all": 100_000,
        })

        self.assertTrue(validate_data._odolo_flow_windows_are_monotonic(payload))
        self.assertTrue(validate_data._odolo_flow_windows_are_not_collapsed(payload))
        self.assertTrue(validate_data._odolo_flow_block_metadata_is_valid(payload))

    def test_rejects_missing_all_time_backfill_coverage(self):
        payload = _payload({
            "1d": 100,
            "7d": 900,
            "30d": 4_000,
            "90d": 12_000,
            "180d": 30_000,
            "all": 100_000,
        })
        payload["transfer_coverage"]["scanned_from_block"] = 4_409_129

        self.assertFalse(validate_data._odolo_flow_block_metadata_is_valid(payload))


if __name__ == "__main__":
    unittest.main()
