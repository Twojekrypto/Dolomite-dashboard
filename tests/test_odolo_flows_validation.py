import unittest

import validate_data


def _payload(period_counts):
    return {
        "timestamp": "2026-06-20T21:00:00",
        "current_block": 22_500_000,
        "chain_head": 22_500_020,
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
            "recent_rescan_blocks": 10_000,
            "reorg_buffer_blocks": 20,
            "state_schema_version": 2,
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

    def test_rejects_legacy_checkpoint_without_overlap_rescan(self):
        payload = _payload({
            "1d": 100,
            "7d": 900,
            "30d": 4_000,
            "90d": 12_000,
            "180d": 30_000,
            "all": 100_000,
        })
        payload["transfer_coverage"]["state_schema_version"] = 1
        payload["transfer_coverage"]["recent_rescan_blocks"] = 0

        self.assertFalse(validate_data._odolo_flow_block_metadata_is_valid(payload))

    def test_claimer_lifecycle_partition_must_reconcile(self):
        payload = {
            "claimer_behavior": {
                "total_claimed": 100,
                "all_claimers": [{
                    "address": "0x" + "1" * 40,
                    "claimed": 100,
                    "exercised": 40,
                    "outflow": 30,
                    "claim_remaining": 30,
                    "held": 80,
                }],
            },
            "claimer_periods": {},
        }
        self.assertTrue(validate_data._odolo_claimer_partitions_reconcile(payload))

        payload["claimer_behavior"]["all_claimers"][0]["claim_remaining"] = 40
        self.assertFalse(validate_data._odolo_claimer_partitions_reconcile(payload))

    def test_claimer_total_must_not_exceed_allocation(self):
        self.assertTrue(validate_data._odolo_claim_total_within_allocation({
            "claimer_behavior": {"total_claimed": 53_911_566}
        }))
        self.assertFalse(validate_data._odolo_claim_total_within_allocation({
            "claimer_behavior": {"total_claimed": 200_000_001}
        }))

    def test_legacy_odolo_events_require_canonical_identity(self):
        valid = {
            "events": [{
                "distributor": validate_data.ODOLO_CLAIMS_DISTRIBUTOR,
                "tokenAddress": validate_data.ODOLO_TOKEN_ADDRESS,
            }]
        }
        self.assertTrue(validate_data._odolo_claim_events_are_canonical(valid))

        valid["events"][0]["distributor"] = "0x" + "1" * 40
        self.assertFalse(validate_data._odolo_claim_events_are_canonical(valid))

    def test_flow_components_must_reconcile_gross_and_net(self):
        row = {
            "address": "0x" + "2" * 40,
            "gross_inflow": 100,
            "gross_outflow": 40,
            "net_flow": 60,
        }
        payload = {"periods": {"7d": {"accumulators": [row], "sellers": []}}}
        self.assertTrue(validate_data._odolo_flow_components_reconcile(payload))

        row["net_flow"] = 70
        self.assertFalse(validate_data._odolo_flow_components_reconcile(payload))


if __name__ == "__main__":
    unittest.main()
