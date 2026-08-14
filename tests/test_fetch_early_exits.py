import json
import os
import tempfile
import unittest
from unittest.mock import patch

import fetch_early_exits


def _topic_address(address):
    return "0x" + ("0" * 24) + address.lower().removeprefix("0x")


def _transfer(log_index, recipient, amount_wei):
    return {
        "address": fetch_early_exits.DOLO_TOKEN,
        "logIndex": hex(log_index),
        "topics": [
            fetch_early_exits.TRANSFER_TOPIC,
            _topic_address(fetch_early_exits.VEDOLO_CONTRACT),
            _topic_address(recipient),
        ],
        "data": hex(amount_wei),
    }


def _event(tx_hash, log_index, provider, value_raw):
    return {
        "event_id": f"{tx_hash}:{log_index}",
        "tx_hash": tx_hash,
        "log_index": log_index,
        "provider": provider,
        "value_raw": str(value_raw),
        "token_id": log_index,
        "timestamp": 1_700_000_000 + log_index,
        "block": 100,
    }


class FetchEarlyExitsTest(unittest.TestCase):
    def _checkpoint_fixture(self):
        early_rows = []
        for index in range(7_999):
            tx_hash = f"0x{index + 1:064x}"
            audited_row = index == 0
            early_rows.append({
                "event_id": f"{tx_hash}:1",
                "address": "0x1111111111111111111111111111111111111111",
                "tx_hash": tx_hash,
                "log_index": 1,
                "block": 4_000_000 + index,
                "token_id": index,
                "timestamp": 1_700_000_000 + index,
                "date": "2023-11-14",
                "original_locked": "31244026.225122226305692208" if audited_row else "0",
                "original_locked_raw": "31244026225122226305692208" if audited_row else "0",
                "total_penalty": "15299061.818413516642511141" if audited_row else "0",
                "total_penalty_raw": "15299061818413516642511141" if audited_row else "0",
                "penalty_pct": "48.966358" if audited_row else "0",
                "burn_fee": "1562201.311256111315281866" if audited_row else "0",
                "burn_fee_raw": "1562201311256111315281866" if audited_row else "0",
                "recoup_fee": "13736860.507157405327229275" if audited_row else "0",
                "recoup_fee_raw": "13736860507157405327229275" if audited_row else "0",
                "user_received": "15944964.406708709663181067" if audited_row else "0",
                "user_received_raw": "15944964406708709663181067" if audited_row else "0",
            })
        return {
            "schemaVersion": fetch_early_exits.OUTPUT_SCHEMA_VERSION,
            "coverage": {
                "complete": True,
                "fromBlock": fetch_early_exits.VEDOLO_DEPLOYMENT_BLOCK,
                "toBlock": 24_819_325,
                "eventCount": 9_063,
                "uniqueTransactionCount": 9_060,
            },
            "stats": {
                "total_early_exits": 7_999,
                "total_normal_exits": 1_064,
                "total_withdrawals": 9_063,
                "unique_withdrawal_transactions": 9_060,
                "total_burn_fee_dolo": "1562201.311256111315281866",
                "total_burn_fee_raw": "1562201311256111315281866",
                "total_recoup_fee_dolo": "13736860.507157405327229275",
                "total_recoup_fee_raw": "13736860507157405327229275",
                "total_penalty_dolo": "15299061.818413516642511141",
                "total_penalty_raw": "15299061818413516642511141",
                "total_original_locked": "31244026.225122226305692208",
                "total_original_locked_raw": "31244026225122226305692208",
                "total_received_dolo": "15944964.406708709663181067",
                "total_received_raw": "15944964406708709663181067",
                "avg_penalty_pct": "48.966358",
                "last_updated": "2026-08-13T20:36:09Z",
            },
            "recent_exits": early_rows,
        }

    def test_incremental_merge_preserves_full_audited_history(self):
        previous = self._checkpoint_fixture()
        fresh_events = [
            _event(f"0x{10_000 + index:064x}", 1, "0x1111111111111111111111111111111111111111", 7)
            for index in range(3)
        ]
        for index, event in enumerate(fresh_events):
            event["block"] = previous["coverage"]["toBlock"] + index + 1
        fresh_calculations = {
            event["event_id"]: {
                "burn_fee_raw": "0",
                "recoup_fee_raw": "0",
                "total_penalty_raw": "0",
                "original_locked_raw": "7",
                "user_received_raw": "7",
                "is_early_exit": False,
            }
            for event in fresh_events
        }

        merged = fetch_early_exits.merge_incremental_output(
            previous,
            fresh_events,
            fresh_calculations,
            latest_block=previous["coverage"]["toBlock"] + 3,
            updated_at="2026-08-14T00:00:00Z",
        )

        self.assertEqual(merged["stats"]["total_withdrawals"], 9_066)
        self.assertEqual(merged["stats"]["total_early_exits"], 7_999)
        self.assertEqual(len(merged["recent_exits"]), 7_999)
        self.assertEqual(merged["coverage"]["toBlock"], previous["coverage"]["toBlock"] + 3)
        self.assertEqual(merged["stats"]["total_penalty_raw"], "15299061818413516642511141")
        self.assertEqual(merged["stats"]["total_original_locked_raw"], "31244026225122226305692208")

    def test_invalid_checkpoint_raises_instead_of_rebuilding_from_partial_history(self):
        previous = self._checkpoint_fixture()
        previous["coverage"]["eventCount"] -= 1
        event = _event("0x" + "ab" * 32, 1, "0x1111111111111111111111111111111111111111", 7)
        event["block"] = previous["coverage"]["toBlock"] + 1
        calculation = {
            event["event_id"]: {
                "burn_fee_raw": "0",
                "recoup_fee_raw": "0",
                "total_penalty_raw": "0",
                "original_locked_raw": "7",
                "user_received_raw": "7",
                "is_early_exit": False,
            }
        }

        with self.assertRaisesRegex(ValueError, "invalid early-exits checkpoint"):
            fetch_early_exits.merge_incremental_output(
                previous,
                [event],
                calculation,
                latest_block=event["block"],
            )

    def test_undersized_checkpoint_is_rejected_before_parse_or_rpc(self):
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".json") as handle:
            handle.write(b"{")
            handle.flush()
            with patch.object(fetch_early_exits, "OUTPUT_FILE", handle.name), \
                 patch.object(fetch_early_exits, "fetch_withdraw_events", side_effect=AssertionError("RPC must not run")):
                with self.assertRaisesRegex(SystemExit, "checkpoint is too small"):
                    fetch_early_exits.main()

    def test_coherent_two_withdrawal_checkpoint_is_rejected_before_rpc(self):
        first_tx = "0x" + "11" * 32
        second_tx = "0x" + "22" * 32
        partial = {
            "schemaVersion": 2,
            "coverage": {
                "complete": True,
                "fromBlock": 2_926_448,
                "toBlock": 24_819_325,
                "eventCount": 2,
                "uniqueTransactionCount": 2,
            },
            "stats": {
                "total_early_exits": 2,
                "total_normal_exits": 0,
                "total_withdrawals": 2,
                "unique_withdrawal_transactions": 2,
                "total_burn_fee_dolo": "0.000000000000000003",
                "total_burn_fee_raw": "3",
                "total_recoup_fee_dolo": "0.000000000000000005",
                "total_recoup_fee_raw": "5",
                "total_penalty_dolo": "0.000000000000000008",
                "total_penalty_raw": "8",
                "total_original_locked": "0.000000000000000022",
                "total_original_locked_raw": "22",
                "total_received_dolo": "0.000000000000000014",
                "total_received_raw": "14",
                "avg_penalty_pct": "36.363636",
                "last_updated": "2026-08-14T00:00:00Z",
            },
            "recent_exits": [
                {
                    "event_id": f"{first_tx}:1",
                    "address": "0x" + "33" * 20,
                    "tx_hash": first_tx,
                    "log_index": 1,
                    "block": 3_000_000,
                    "token_id": 1,
                    "timestamp": 1_700_000_001,
                    "date": "2023-11-14",
                    "original_locked": "0.000000000000000010",
                    "original_locked_raw": "10",
                    "total_penalty": "0.000000000000000003",
                    "total_penalty_raw": "3",
                    "penalty_pct": "30",
                    "burn_fee": "0.000000000000000001",
                    "burn_fee_raw": "1",
                    "recoup_fee": "0.000000000000000002",
                    "recoup_fee_raw": "2",
                    "user_received": "0.000000000000000007",
                    "user_received_raw": "7",
                },
                {
                    "event_id": f"{second_tx}:2",
                    "address": "0x" + "44" * 20,
                    "tx_hash": second_tx,
                    "log_index": 2,
                    "block": 3_000_001,
                    "token_id": 2,
                    "timestamp": 1_700_000_002,
                    "date": "2023-11-14",
                    "original_locked": "0.000000000000000012",
                    "original_locked_raw": "12",
                    "total_penalty": "0.000000000000000005",
                    "total_penalty_raw": "5",
                    "penalty_pct": "41.666667",
                    "burn_fee": "0.000000000000000002",
                    "burn_fee_raw": "2",
                    "recoup_fee": "0.000000000000000003",
                    "recoup_fee_raw": "3",
                    "user_received": "0.000000000000000007",
                    "user_received_raw": "7",
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as handle:
            json.dump(partial, handle)
            handle.flush()
            self.assertGreater(os.path.getsize(handle.name), 1_000)
            with patch.object(fetch_early_exits, "OUTPUT_FILE", handle.name), \
                 patch.object(fetch_early_exits, "fetch_withdraw_events", return_value=([], 24_819_325)) as fetch, \
                 patch.object(fetch_early_exits, "_atomic_dump"):
                with self.assertRaisesRegex(SystemExit, "audited baseline"):
                    fetch_early_exits.main()
            fetch.assert_not_called()

    def test_checkpoint_coverage_counters_reject_boolean_values(self):
        for field in ("eventCount", "uniqueTransactionCount"):
            with self.subTest(field=field):
                previous = self._checkpoint_fixture()
                previous["coverage"][field] = True
                with self.assertRaisesRegex(ValueError, f"coverage\\.{field} must be a strict non-negative integer"):
                    fetch_early_exits._validate_checkpoint(previous)

    def test_checkpoint_block_and_stat_counters_require_strict_integers(self):
        previous = self._checkpoint_fixture()
        previous["coverage"]["toBlock"] = "24819325"
        with self.assertRaisesRegex(ValueError, "coverage\\.toBlock must be a strict non-negative integer"):
            fetch_early_exits._validate_checkpoint(previous)

        for field in (
            "total_early_exits",
            "total_normal_exits",
            "total_withdrawals",
            "unique_withdrawal_transactions",
        ):
            with self.subTest(field=field):
                previous = self._checkpoint_fixture()
                previous["stats"][field] = str(previous["stats"][field])
                with self.assertRaisesRegex(ValueError, f"stats\\.{field} must be a strict non-negative integer"):
                    fetch_early_exits._validate_checkpoint(previous)

    def test_checkpoint_from_block_requires_strict_integer_and_rejects_before_rpc(self):
        for from_block in (2_926_448.0, True):
            with self.subTest(from_block=from_block):
                previous = self._checkpoint_fixture()
                previous["coverage"]["fromBlock"] = from_block
                with self.assertRaisesRegex(ValueError, "coverage must start at the deployment block"):
                    fetch_early_exits._validate_checkpoint(previous)

        previous = self._checkpoint_fixture()
        previous["coverage"]["fromBlock"] = 2_926_448.0
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as handle:
            json.dump(previous, handle)
            handle.flush()
            with patch.object(fetch_early_exits, "OUTPUT_FILE", handle.name), \
                 patch.object(fetch_early_exits, "rpc_call", side_effect=AssertionError("RPC must not run")) as rpc:
                with self.assertRaisesRegex(SystemExit, "coverage must start at the deployment block"):
                    fetch_early_exits.main()
            rpc.assert_not_called()

    def test_checkpoint_raw_values_require_exact_digit_strings(self):
        previous = self._checkpoint_fixture()
        previous["stats"]["total_burn_fee_raw"] = 1_562_201_311_256_111_315_281_866
        with self.assertRaisesRegex(ValueError, "stats\\.total_burn_fee_raw must be an exact digit string"):
            fetch_early_exits._validate_checkpoint(previous)

        previous = self._checkpoint_fixture()
        previous["recent_exits"][0]["burn_fee_raw"] = 1_562_201_311_256_111_315_281_866
        with self.assertRaisesRegex(ValueError, "burn_fee_raw must be an exact digit string"):
            fetch_early_exits._validate_checkpoint(previous)

    def test_fresh_calculation_raw_values_require_exact_digit_strings(self):
        calculation = {
            "is_early_exit": True,
            "burn_fee_raw": 1,
            "recoup_fee_raw": "2",
            "total_penalty_raw": "3",
            "original_locked_raw": "10",
            "user_received_raw": "7",
        }

        with self.assertRaisesRegex(ValueError, "fresh:1\\.burn_fee_raw must be an exact digit string"):
            fetch_early_exits._fresh_calculation(calculation, "fresh:1")

    def test_withdraw_log_fetch_reduces_oversized_windows(self):
        calls = []

        def fake_rpc(method, params):
            if method == "eth_blockNumber":
                return hex(10)
            [payload] = params
            start = int(payload["fromBlock"], 16)
            end = int(payload["toBlock"], 16)
            calls.append((start, end))
            if end - start + 1 > 2:
                return None
            return []

        with patch.object(fetch_early_exits, "LOG_INITIAL_STEP", 16), \
             patch.object(fetch_early_exits, "LOG_MIN_STEP", 2), \
             patch.object(fetch_early_exits.time, "sleep", return_value=None), \
             patch.object(fetch_early_exits, "rpc_call", side_effect=fake_rpc):
            logs, latest = fetch_early_exits.fetch_withdraw_events(start_block=1)

        self.assertEqual(logs, [])
        self.assertEqual(latest, 10)
        self.assertIn((1, 2), calls)
        self.assertEqual(calls[-1], (9, 10))
        self.assertLessEqual(
            len([1 for start, end in calls if end - start + 1 > 2]),
            4,
        )

    def test_unknown_protocol_recipient_is_recoup_not_user_received(self):
        tx_hash = "0x" + "aa" * 32
        provider = "0x1111111111111111111111111111111111111111"
        dao_safe = "0x6e939bbaceb45159982a2cac3a1fcbf7e93cf682"
        event = _event(tx_hash, 4, provider, 70 * 10**18)
        receipt = {
            "logs": [
                _transfer(1, fetch_early_exits.ZERO_ADDR, 5 * 10**18),
                _transfer(2, dao_safe, 25 * 10**18),
                _transfer(3, provider, 70 * 10**18),
            ]
        }

        result = fetch_early_exits.calculate_receipt_events(receipt, [event])[event["event_id"]]

        self.assertEqual(result["burn_fee_raw"], str(5 * 10**18))
        self.assertEqual(result["recoup_fee_raw"], str(25 * 10**18))
        self.assertEqual(result["user_received_raw"], str(70 * 10**18))

    def test_decoded_event_keeps_exact_wei_and_log_identity(self):
        tx_hash = "0x" + "ab" * 32
        value_wei = 12_345_678_901_234_567
        log = {
            "topics": [
                fetch_early_exits.WITHDRAW_TOPIC,
                _topic_address("0x2222222222222222222222222222222222222222"),
            ],
            "data": "0x" + f"{7:064x}{value_wei:064x}{1_700_000_000:064x}",
            "blockNumber": hex(123),
            "logIndex": hex(9),
            "transactionHash": tx_hash,
        }

        event = fetch_early_exits.decode_withdraw_event(log)

        self.assertIn("value_raw", event)
        self.assertEqual(event.get("value_raw"), str(value_wei))
        self.assertEqual(event.get("event_id"), f"{tx_hash}:9")

    def test_multiple_withdraws_in_one_transaction_are_scoped_by_log_index(self):
        tx_hash = "0x" + "cd" * 32
        first_provider = "0x1111111111111111111111111111111111111111"
        second_provider = "0x2222222222222222222222222222222222222222"
        events = [
            _event(tx_hash, 4, first_provider, 70),
            _event(tx_hash, 8, second_provider, 180),
        ]
        receipt = {
            "logs": [
                _transfer(1, fetch_early_exits.ZERO_ADDR, 5),
                _transfer(2, "0x3333333333333333333333333333333333333333", 25),
                _transfer(3, first_provider, 70),
                _transfer(5, fetch_early_exits.ZERO_ADDR, 10),
                _transfer(6, "0x4444444444444444444444444444444444444444", 10),
                _transfer(7, second_provider, 180),
            ]
        }

        result = fetch_early_exits.calculate_receipt_events(receipt, events)

        self.assertEqual(set(result), {f"{tx_hash}:4", f"{tx_hash}:8"})
        self.assertEqual(result[f"{tx_hash}:4"]["original_locked_raw"], "100")
        self.assertEqual(result[f"{tx_hash}:8"]["original_locked_raw"], "200")

    def test_provider_transfer_must_match_withdraw_event_value_exactly(self):
        tx_hash = "0x" + "ef" * 32
        provider = "0x1111111111111111111111111111111111111111"
        event = _event(tx_hash, 3, provider, 70)
        receipt = {"logs": [_transfer(1, fetch_early_exits.ZERO_ADDR, 5), _transfer(2, provider, 69)]}

        with self.assertRaisesRegex(ValueError, "provider received 69 wei"):
            fetch_early_exits.calculate_receipt_events(receipt, [event])

    def test_log_merge_preserves_multiple_events_in_same_transaction(self):
        tx_hash = "0x" + "12" * 32
        first = {"transactionHash": tx_hash, "logIndex": "0x1", "blockNumber": "0xa", "transactionIndex": "0x0"}
        second = {"transactionHash": tx_hash, "logIndex": "0x2", "blockNumber": "0xa", "transactionIndex": "0x0"}

        merged = fetch_early_exits.merge_event_logs([first], [first, second], rescan_from=10)

        self.assertEqual([log["logIndex"] for log in merged], ["0x1", "0x2"])

    def test_build_output_uses_event_count_and_exact_micro_values(self):
        tx_hash = "0x" + "34" * 32
        event = _event(tx_hash, 4, "0x1111111111111111111111111111111111111111", 1)
        calculation = {
            event["event_id"]: {
                "burn_fee_raw": "1",
                "recoup_fee_raw": "1",
                "total_penalty_raw": "2",
                "original_locked_raw": "3",
                "user_received_raw": "1",
                "is_early_exit": True,
            }
        }

        output = fetch_early_exits.build_output([event], calculation, 123, "2026-08-13T00:00:00Z")

        self.assertEqual(output["coverage"]["eventCount"], 1)
        self.assertEqual(output["stats"]["total_withdrawals"], 1)
        self.assertEqual(output["recent_exits"][0]["total_penalty"], "0.000000000000000002")
        self.assertEqual(output["recent_exits"][0]["total_penalty_raw"], "2")

    def test_legacy_cache_forces_full_history_rescan(self):
        import json
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as handle:
            json.dump({"_meta": {"last_scanned_block": 999}, "receipts": {}}, handle)
            handle.flush()
            withdraw_logs, transfer_logs, rescan_from = fetch_early_exits.load_cache(handle.name)

        self.assertEqual(withdraw_logs, [])
        self.assertEqual(transfer_logs, [])
        self.assertEqual(rescan_from, fetch_early_exits.VEDOLO_DEPLOYMENT_BLOCK)

    def test_cache_without_complete_transfer_log_coverage_forces_full_rescan(self):
        import json
        import tempfile

        payload = {
            "_meta": {
                "schema_version": fetch_early_exits.CACHE_SCHEMA_VERSION,
                "complete_from_block": fetch_early_exits.VEDOLO_DEPLOYMENT_BLOCK,
                "last_scanned_block": 4_000_000,
            },
            "withdraw_logs": [],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()
            withdraw_logs, transfer_logs, rescan_from = fetch_early_exits.load_cache(handle.name)

        self.assertEqual(withdraw_logs, [])
        self.assertEqual(transfer_logs, [])
        self.assertEqual(rescan_from, fetch_early_exits.VEDOLO_DEPLOYMENT_BLOCK)


if __name__ == "__main__":
    unittest.main()
