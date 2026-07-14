import unittest

from vedolo_vote_power import (
    GlobalPoint,
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


if __name__ == "__main__":
    unittest.main()
