import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import validate_data

from vedolo_vote_power import (
    CanonicalSnapshot,
    GlobalPoint,
    build_vote_power_payload,
    decode_global_point,
    decode_signed_word,
    evaluate_vote_power_at,
)


def encode_global_point(bias, slope, timestamp, block):
    return "0x" + "".join(
        f"{value & ((1 << 256) - 1):064x}"
        for value in (bias, slope, timestamp, block)
    )


class VeDoloVotePowerTests(unittest.TestCase):
    def test_history_validation_rejects_booleans_in_exact_integer_fields(self):
        payload = {
            "schemaVersion": 1,
            "metric": "votePower",
            "chain": "berachain",
            "contract": "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4",
            "source": "global-point-history",
            "targetBlock": 1,
            "targetTimestamp": 2,
            "totalSupplyWei": "8",
            "lockedSupplyWei": "9",
            "lastPointWei": "8",
            "coverage": {"from": 1, "through": 2},
            "points": [[1, "0.000000000000000007"], [2, "0.000000000000000008"]],
        }
        replacements = {
            "schemaVersion": lambda value: {**value, "schemaVersion": True},
            "targetBlock": lambda value: {**value, "targetBlock": True},
            "targetTimestamp": lambda value: {**value, "targetTimestamp": True},
            "coverage.from": lambda value: {**value, "coverage": {"from": True, "through": 2}},
            "coverage.through": lambda value: {**value, "coverage": {"from": 1, "through": True}},
            "points[0].timestamp": lambda value: {**value, "points": [[True, value["points"][0][1]], value["points"][1]]},
            "points[1].timestamp": lambda value: {**value, "points": [value["points"][0], [True, value["points"][1][1]]]},
        }

        for field, replace in replacements.items():
            with self.subTest(field=field):
                self.assertFalse(validate_data._vedolo_vote_power_history_valid(replace(payload)))

    def test_history_validation_rejects_last_point_different_from_total_supply(self):
        payload = {
            "schemaVersion": 1,
            "metric": "votePower",
            "chain": "berachain",
            "contract": "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4",
            "source": "global-point-history",
            "targetBlock": 1,
            "targetTimestamp": 2,
            "totalSupplyWei": "8",
            "lockedSupplyWei": "9",
            "lastPointWei": "8",
            "coverage": {"from": 1, "through": 2},
            "points": [[1, "0.000000000000000007"], [2, "0.000000000000000008"]],
        }
        self.assertTrue(validate_data._vedolo_vote_power_history_valid(payload))
        self.assertFalse(validate_data._vedolo_vote_power_history_valid({
            **payload,
            "lastPointWei": "7",
        }))

    def test_history_validation_requires_canonical_history_contract(self):
        payload = {
            "schemaVersion": 1,
            "metric": "votePower",
            "chain": "berachain",
            "contract": "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4",
            "source": "global-point-history",
            "targetBlock": 1,
            "targetTimestamp": 2,
            "totalSupplyWei": "8",
            "lockedSupplyWei": "9",
            "lastPointWei": "8",
            "coverage": {"from": 1, "through": 2},
            "points": [[1, "0.000000000000000007"], [2, "0.000000000000000008"]],
        }
        self.assertTrue(validate_data._vedolo_vote_power_history_valid(payload))

        invalid_payloads = (
            {**payload, "schemaVersion": 2},
            {**payload, "chain": "ethereum"},
            {**payload, "contract": "0x0000000000000000000000000000000000000000"},
            {**payload, "coverage": {"from": 1, "through": 3}},
            {**payload, "points": [[1, "0.000000000000000007"], [1, "0.000000000000000008"]]},
            {**payload, "points": [[1, "0"], [2, "-0.000000000000000008"]]},
            {**payload, "lastPointWei": "7"},
        )
        for invalid_payload in invalid_payloads:
            with self.subTest(invalid_payload=invalid_payload):
                self.assertFalse(validate_data._vedolo_vote_power_history_valid(invalid_payload))

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
        self.assertEqual(decode_signed_word("0x" + "f" * 64), -1)

    def test_rejects_malformed_signed_word_outputs(self):
        malformed_words = (
            "f" * 64,
            "0x" + "f" * 63,
            "0x" + "f" * 65,
            "0x" + "g" * 64,
            None,
        )

        for word in malformed_words:
            with self.subTest(word=word):
                with self.assertRaisesRegex(ValueError, "exactly one ABI word"):
                    decode_signed_word(word)

    def test_decodes_global_point_from_four_32_byte_words(self):
        result = "0x" + "".join(
            format(word, "064x")
            for word in (123, 7, 1_700_000_000, 42)
        )

        self.assertEqual(
            decode_global_point(result),
            GlobalPoint(bias=123, slope=7, timestamp=1_700_000_000, block=42),
        )

    def test_decodes_signed_bias_and_slope_in_global_point(self):
        negative_one = "f" * 64
        result = "0x" + negative_one + negative_one + "0" * 128

        self.assertEqual(
            decode_global_point(result),
            GlobalPoint(bias=-1, slope=-1, timestamp=0, block=0),
        )

    def test_rejects_incomplete_global_point_response(self):
        with self.assertRaises(ValueError):
            decode_global_point("0" * 63)

    def test_requires_exact_prefixed_four_word_abi_result(self):
        malformed_results = (
            "0" * 256,
            "0x" + "0" * 255,
            "0x" + "0" * 257,
            None,
            "0x" + "0" * 255 + "g",
        )

        for result in malformed_results:
            with self.subTest(result=result):
                with self.assertRaises(ValueError):
                    decode_global_point(result)

    def test_uses_latest_epoch_for_points_in_the_same_block_and_timestamp(self):
        points = [
            GlobalPoint(bias=100, slope=0, timestamp=10, block=2),
            GlobalPoint(bias=200, slope=0, timestamp=10, block=2),
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

    def test_history_syncs_both_stats_files_from_its_single_snapshot(self):
        from generate_vedolo_vote_power_history import write_vote_power_history

        snapshot = CanonicalSnapshot(123, 0, 123_450_000_000_000_000_000, 0, 0)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_path = root / "history.json"
            state_path = root / "state.json"
            stats_path = root / "vedolo_stats.json"
            holders_path = root / "vedolo_holders.json"
            stats_path.write_text(json.dumps({
                "stats": {
                    "total_vote_weight": 101.25,
                    "total_vote_weight_holder_sum": 101.25,
                },
                "timestamp": "before",
            }))
            holders_path.write_text(json.dumps({
                "stats": {
                    "total_vote_weight": 202.5,
                    "total_vote_weight_holder_sum": 202.5,
                },
                "holders": [],
            }))

            with patch(
                "generate_vedolo_vote_power_history.fetch_canonical_snapshot",
                return_value=snapshot,
            ) as fetch_snapshot, patch(
                "generate_vedolo_vote_power_history._fetch_global_point_results",
                return_value=[encode_global_point(snapshot.total_supply_wei, 0, 0, 1)],
            ):
                payload = write_vote_power_history(
                    output_path,
                    state_path,
                    sync_stats=True,
                    stats_path=stats_path,
                    holders_path=holders_path,
                )

            self.assertEqual(fetch_snapshot.call_count, 1)
            self.assertEqual(payload["totalSupplyWei"], str(snapshot.total_supply_wei))
            self.assertEqual(payload["lastPointWei"], str(snapshot.total_supply_wei))

            stats_payload = json.loads(stats_path.read_text())
            holders_payload = json.loads(holders_path.read_text())
            for stats, holder_sum in (
                (stats_payload["stats"], 101.25),
                (holders_payload["stats"], 202.5),
            ):
                self.assertEqual(stats["total_vote_weight"], 123.45)
                self.assertEqual(stats["total_vote_weight_holder_sum"], holder_sum)
                self.assertEqual(stats["total_vote_weight_source"], "contract_totalSupply")
                self.assertEqual(stats["total_vote_weight_block"], snapshot.block_number)
                self.assertEqual(stats["total_vote_weight_timestamp"], snapshot.timestamp)

    def test_history_sync_preserves_existing_holders_field_order(self):
        from generate_vedolo_vote_power_history import write_vote_power_history

        snapshot = CanonicalSnapshot(123, 0, 10, 0, 0)
        original_holders = """{
  \"contract\": \"0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4\",
  \"network\": \"berachain\",
  \"timestamp\": \"before\",
  \"stats\": {\"total_vote_weight\": 1},
  \"holders\": [{\"address\": \"0x1\", \"nft_count\": 1}]
}\n"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stats_path = root / "vedolo_stats.json"
            holders_path = root / "vedolo_holders.json"
            stats_path.write_text(json.dumps({"stats": {"total_vote_weight": 1}}))
            holders_path.write_text(original_holders)

            with patch(
                "generate_vedolo_vote_power_history.fetch_canonical_snapshot",
                return_value=snapshot,
            ), patch(
                "generate_vedolo_vote_power_history._fetch_global_point_results",
                return_value=[encode_global_point(10, 0, 0, 1)],
            ):
                write_vote_power_history(
                    root / "history.json",
                    root / "state.json",
                    sync_stats=True,
                    stats_path=stats_path,
                    holders_path=holders_path,
                )

            saved = holders_path.read_text()

        self.assertLess(saved.index('"network"'), saved.index('"stats"'))
        self.assertLess(saved.index('"stats"'), saved.index('"holders"'))
        self.assertLess(saved.index('"address"'), saved.index('"nft_count"'))

    def test_history_generator_imports_on_supported_python(self):
        import generate_vedolo_vote_power_history

        self.assertTrue(generate_vedolo_vote_power_history.PUBLIC_OUTPUT_PATH)

    def test_history_generator_decodes_compact_rpc_quantities(self):
        from generate_vedolo_vote_power_history import _decode_uint

        self.assertEqual(_decode_uint("0x1", "target block"), 1)

    def test_invalid_cached_points_trigger_a_full_rebuild(self):
        from generate_vedolo_vote_power_history import fetch_global_points

        snapshot = CanonicalSnapshot(123, 30, 0, 0, 1)
        invalid_caches = {
            "bare ABI payload": [
                "0" * 256,
                encode_global_point(9, 1, 11, 2),
            ],
            "nonmonotonic timestamp": [
                encode_global_point(10, 1, 10, 1),
                encode_global_point(9, 1, 9, 2),
            ],
            "negative bias": [
                encode_global_point(-1, 1, 10, 1),
                encode_global_point(0, 1, 11, 2),
            ],
        }
        fresh_points = [
            encode_global_point(10, 1, 10, 1),
            encode_global_point(9, 1, 11, 2),
        ]

        for name, cached_points in invalid_caches.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                state_path = Path(directory) / "state.json"
                state_path.write_text(
                    json.dumps(
                        {
                            "schemaVersion": 1,
                            "epoch": 1,
                            "points": cached_points,
                        }
                    )
                )
                requested_indices = []

                def fetch_results(indices, block_tag):
                    requested_indices.extend(indices)
                    return fresh_points

                with patch(
                    "generate_vedolo_vote_power_history._fetch_global_point_results",
                    side_effect=fetch_results,
                ):
                    points = fetch_global_points(snapshot, state_path)

                self.assertEqual(requested_indices, [0, 1])
                self.assertEqual(
                    points,
                    [
                        GlobalPoint(10, 1, 10, 1),
                        GlobalPoint(9, 1, 11, 2),
                    ],
                )

    def test_cached_points_allow_same_block_and_timestamp_in_epoch_order(self):
        from generate_vedolo_vote_power_history import fetch_global_points

        snapshot = CanonicalSnapshot(123, 30, 0, 0, 1)
        cached_points = [
            encode_global_point(10, 1, 10, 1),
            encode_global_point(20, 2, 10, 1),
        ]

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            state_path.write_text(json.dumps({
                "schemaVersion": 1,
                "epoch": 1,
                "points": cached_points,
            }))
            with patch(
                "generate_vedolo_vote_power_history._fetch_global_point_results",
                return_value=[],
            ) as fetch:
                points = fetch_global_points(snapshot, state_path)

        self.assertEqual(list(fetch.call_args.args[0]), [])
        self.assertEqual(points[-1], GlobalPoint(20, 2, 10, 1))

    def test_invalid_fresh_points_abort_before_cache_or_publication(self):
        from generate_vedolo_vote_power_history import write_vote_power_history

        snapshot = CanonicalSnapshot(123, 30, 0, 0, 1)
        invalid_points = [
            encode_global_point(10, 1, 11, 1),
            encode_global_point(9, 1, 10, 1),
        ]

        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "public.json"
            state_path = Path(directory) / "state.json"
            with patch(
                "generate_vedolo_vote_power_history.fetch_canonical_snapshot",
                return_value=snapshot,
            ), patch(
                "generate_vedolo_vote_power_history._fetch_global_point_results",
                return_value=invalid_points,
            ):
                with self.assertRaisesRegex(RuntimeError, "nondecreasing"):
                    write_vote_power_history(output_path, state_path)

            self.assertFalse(state_path.exists())
            self.assertFalse(output_path.exists())

    def test_malformed_fresh_point_aborts_before_cache_or_publication(self):
        from generate_vedolo_vote_power_history import write_vote_power_history

        snapshot = CanonicalSnapshot(123, 30, 0, 0, 1)
        invalid_points = [
            "0" * 256,
            encode_global_point(9, 1, 11, 2),
        ]

        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "public.json"
            state_path = Path(directory) / "state.json"
            with patch(
                "generate_vedolo_vote_power_history.fetch_canonical_snapshot",
                return_value=snapshot,
            ), patch(
                "generate_vedolo_vote_power_history._fetch_global_point_results",
                return_value=invalid_points,
            ):
                with self.assertRaises(ValueError):
                    write_vote_power_history(output_path, state_path)

            self.assertFalse(state_path.exists())
            self.assertFalse(output_path.exists())

    def test_rejects_malformed_total_supply_eth_call_response(self):
        from generate_vedolo_vote_power_history import fetch_canonical_snapshot

        abi_word = "0x" + "0" * 63 + "1"

        def rpc_response(endpoints, payload, describe):
            method = payload["method"]
            if method == "eth_blockNumber":
                return {"result": "0x7b"}
            if method == "eth_getBlockByNumber":
                return {"result": {"timestamp": "0x1e"}}
            selector = payload["params"][0]["data"]
            result = "0x1" if selector == "0x18160ddd" else abi_word
            return {"result": result}

        with patch(
            "generate_vedolo_vote_power_history.rpc_single_request",
            side_effect=rpc_response,
        ), patch("generate_vedolo_vote_power_history.get_endpoints", return_value=[]):
            with self.assertRaisesRegex(RuntimeError, "exactly one ABI word"):
                fetch_canonical_snapshot()

    def test_rejects_malformed_slope_change_eth_call_response(self):
        from generate_vedolo_vote_power_history import WEEK_SECONDS, fetch_slope_changes

        snapshot = CanonicalSnapshot(123, WEEK_SECONDS, 0, 0, 0)
        responses = {"slope:%d" % WEEK_SECONDS: {"result": "f" * 64}}

        with patch(
            "generate_vedolo_vote_power_history.rpc_batch_requests",
            return_value=(responses, []),
        ), patch("generate_vedolo_vote_power_history.get_endpoints", return_value=[]):
            with self.assertRaisesRegex(ValueError, "exactly one ABI word"):
                fetch_slope_changes([GlobalPoint(0, 0, 0, 1)], snapshot)


if __name__ == "__main__":
    unittest.main()
