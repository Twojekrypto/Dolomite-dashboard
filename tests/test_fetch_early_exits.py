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
