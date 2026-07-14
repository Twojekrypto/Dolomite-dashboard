import unittest

from vedolo_vote_power import (
    CanonicalSnapshot,
    GlobalPoint,
    build_vote_power_payload,
    decode_global_point,
    decode_signed_word,
    evaluate_vote_power_at,
)


class VeDoloVotePowerTests(unittest.TestCase):
    def test_applies_weekly_slope_change_without_float_math(self):
        point = GlobalPoint(bias=1000, slope=10, timestamp=0, block=1)

        result = evaluate_vote_power_at(
            15,
            [point],
            {10: -5},
            week_seconds=10,
        )

        self.assertEqual(result, 875)

    def test_decodes_sign_extended_solidity_integer(self):
        self.assertEqual(decode_signed_word("f" * 64), -1)

    def test_decodes_global_point_from_four_32_byte_words(self):
        result = "".join(
            format(word, "064x")
            for word in (123, 7, 1_700_000_000, 42)
        )

        self.assertEqual(
            decode_global_point(result),
            GlobalPoint(bias=123, slope=7, timestamp=1_700_000_000, block=42),
        )

    def test_decodes_signed_bias_and_slope_in_global_point(self):
        negative_one = "f" * 64
        result = negative_one + negative_one + "0" * 128

        self.assertEqual(
            decode_global_point(result),
            GlobalPoint(bias=-1, slope=-1, timestamp=0, block=0),
        )

    def test_rejects_incomplete_global_point_response(self):
        with self.assertRaises(ValueError):
            decode_global_point("0" * 63)

    def test_uses_latest_point_by_timestamp_then_block(self):
        points = [
            GlobalPoint(bias=100, slope=0, timestamp=10, block=2),
            GlobalPoint(bias=200, slope=0, timestamp=10, block=3),
            GlobalPoint(bias=300, slope=0, timestamp=9, block=99),
        ]

        self.assertEqual(evaluate_vote_power_at(10, points, {}), 200)

    def test_payload_ends_with_exact_contract_total_supply(self):
        snapshot = CanonicalSnapshot(123, 15, 875, 1000, 1)
        payload = build_vote_power_payload(
            snapshot,
            [GlobalPoint(1000, 10, 0, 1)],
            {10: -5},
            day_seconds=10,
        )

        self.assertEqual(payload["lastPointWei"], "875")
        self.assertEqual(payload["points"][-1], [15, "0.000000000000000875"])

    def test_payload_rejects_mismatched_contract_total_supply(self):
        with self.assertRaisesRegex(ValueError, "totalSupply"):
            build_vote_power_payload(
                CanonicalSnapshot(123, 15, 876, 1000, 1),
                [GlobalPoint(1000, 10, 0, 1)],
                {10: -5},
                day_seconds=10,
            )

    def test_history_generator_imports_on_supported_python(self):
        import generate_vedolo_vote_power_history

        self.assertTrue(generate_vedolo_vote_power_history.PUBLIC_OUTPUT_PATH)

    def test_history_generator_decodes_compact_rpc_quantities(self):
        from generate_vedolo_vote_power_history import _decode_uint

        self.assertEqual(_decode_uint("0x1", "target block"), 1)


if __name__ == "__main__":
    unittest.main()
