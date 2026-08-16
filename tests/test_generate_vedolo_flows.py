import json
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import rpc_usage
import generate_vedolo_flows as vedolo_flows
from generate_vedolo_flows import (
    AUDITED_MISSING_DEPOSIT_BLOCKS,
    DEPOSIT_TOPIC,
    EventLogFetchError,
    ODOLO_EXERCISE_TOPIC,
    ODOLO_VESTER,
    TRANSFER_TOPIC,
    VEDOLO_CONTRACT,
    WITHDRAW_TOPIC,
    ZERO_TOPIC,
    _logs_with_topic0,
    check_odolo_exercise_batch,
    decode_transfer,
    extract_odolo_receipt_beneficiary,
    fetch_event_logs,
    recover_missing_deposit_events,
    remap_odolo_lock_beneficiaries,
)


class GenerateVedoloFlowsTests(unittest.TestCase):
    @staticmethod
    def _position_action_input(selector, first, second):
        return "0x" + selector + hex(first)[2:].zfill(64) + hex(second)[2:].zfill(64)

    def test_parses_merge_and_split_calldata_into_exact_token_transitions(self):
        merge = vedolo_flows.parse_position_action_calldata(
            {"depositType": 4, "tokenId": 22},
            self._position_action_input("d1c2babb", 11, 22),
        )
        split = vedolo_flows.parse_position_action_calldata(
            {"depositType": 5, "tokenId": 33},
            self._position_action_input("4b19becc", 22, 7 * 10**18),
        )

        self.assertEqual(merge, {"sourceTokenId": 11, "targetTokenId": 22})
        self.assertEqual(split, {"sourceTokenId": 22, "targetTokenId": 33})

    def test_position_action_calldata_rejects_wrong_selector_target_and_shape(self):
        lock = {"depositType": 4, "tokenId": 22}
        invalid_inputs = [
            self._position_action_input("4b19becc", 11, 22),
            self._position_action_input("d1c2babb", 11, 23),
            "0xd1c2babb01",
        ]
        for input_data in invalid_inputs:
            with self.subTest(input_data=input_data), self.assertRaises(EventLogFetchError):
                vedolo_flows.parse_position_action_calldata(lock, input_data)

    def test_position_action_annotation_uses_cache_and_fails_closed_when_unresolved(self):
        merge_tx = "0x" + "a" * 64
        split_tx = "0x" + "b" * 64
        locks = [
            {"depositType": 4, "tokenId": 22, "txHash": merge_tx},
            {"depositType": 5, "tokenId": 33, "txHash": split_tx},
        ]
        inputs = {
            merge_tx: self._position_action_input("d1c2babb", 11, 22),
            split_tx: self._position_action_input("4b19becc", 22, 7 * 10**18),
        }
        calls = []

        def fetcher(tx_hash):
            calls.append(tx_hash)
            return {"input": inputs[tx_hash]}

        state = {}
        self.assertEqual(
            vedolo_flows.annotate_position_action_tokens(locks, state, fetcher=fetcher),
            2,
        )
        self.assertEqual(calls, [merge_tx, split_tx])
        self.assertEqual(locks[0]["sourceTokenId"], 11)
        self.assertEqual(locks[1]["targetTokenId"], 33)

        replay_rows = [
            {"depositType": 4, "tokenId": 22, "txHash": merge_tx},
            {"depositType": 5, "tokenId": 33, "txHash": split_tx},
        ]
        self.assertEqual(
            vedolo_flows.annotate_position_action_tokens(
                replay_rows,
                state,
                fetcher=lambda _tx_hash: self.fail("cache should avoid RPC"),
            ),
            2,
        )

        with self.assertRaisesRegex(EventLogFetchError, "could not resolve merge transition"):
            vedolo_flows.annotate_position_action_tokens(
                [{"depositType": 4, "tokenId": 44, "txHash": "0x" + "c" * 64}],
                {},
                fetcher=lambda _tx_hash: None,
            )

    def test_fetch_event_logs_does_not_trust_one_empty_provider_when_a_peer_has_events(self):
        real_log = {
            "topics": [DEPOSIT_TOPIC],
            "blockNumber": "0x10",
            "transactionHash": "0x" + "a" * 64,
        }

        class _Resp:
            def __init__(self, result):
                self._result = result

            def json(self):
                return {"result": self._result}

        def _fake_post(url, **_kwargs):
            return _Resp([] if url.endswith("empty") else [real_log])

        with patch("generate_vedolo_flows.RPC_URLS", ["https://rpc.empty", "https://rpc.real"]), \
             patch("generate_vedolo_flows.requests.post", side_effect=_fake_post), \
             patch("generate_vedolo_flows.time.sleep", return_value=None):
            logs = fetch_event_logs(1, 100, DEPOSIT_TOPIC)

        self.assertEqual(logs, [real_log])

    def test_fetch_event_logs_checks_every_peer_before_confirming_empty(self):
        real_log = {
            "topics": [DEPOSIT_TOPIC],
            "blockNumber": "0x11",
            "transactionHash": "0x" + "b" * 64,
        }

        class _Resp:
            def __init__(self, result):
                self._result = result

            def json(self):
                return {"result": self._result}

        def _fake_post(url, **_kwargs):
            return _Resp([real_log] if url.endswith("real") else [])

        with patch(
            "generate_vedolo_flows.RPC_URLS",
            ["https://rpc.empty-one", "https://rpc.empty-two", "https://rpc.real"],
        ), patch("generate_vedolo_flows.requests.post", side_effect=_fake_post), patch(
            "generate_vedolo_flows.time.sleep", return_value=None
        ):
            logs = fetch_event_logs(1, 100, DEPOSIT_TOPIC)

        self.assertEqual(logs, [real_log])

    def test_fetch_event_logs_fails_closed_when_empty_cannot_be_confirmed(self):
        class _Resp:
            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        def _fake_post(url, **_kwargs):
            if url.endswith("empty"):
                return _Resp({"result": []})
            return _Resp({"error": {"message": "temporary RPC failure"}})

        with patch("generate_vedolo_flows.RPC_URLS", ["https://rpc.empty", "https://rpc.error"]), \
             patch("generate_vedolo_flows.requests.post", side_effect=_fake_post), \
             patch("generate_vedolo_flows.time.sleep", return_value=None):
            with self.assertRaisesRegex(EventLogFetchError, "unconfirmed empty response"):
                fetch_event_logs(1, 100, DEPOSIT_TOPIC)

    def test_rejects_candidate_that_drops_finalized_lock_or_unlock(self):
        previous = {
            "locks": [{"txHash": "0x" + "1" * 64, "tokenId": 7, "block": 100}],
            "unlocks": [{"txHash": "0x" + "2" * 64, "tokenId": 8, "block": 200}],
        }
        candidate = {
            "locks": list(previous["locks"]),
            "unlocks": [],
        }

        with self.assertRaisesRegex(EventLogFetchError, "dropped 1 finalized unlock"):
            vedolo_flows.assert_no_immutable_event_regression(
                previous,
                candidate,
                current_block=10_000,
                reorg_depth=256,
            )

    def test_allows_candidate_to_replace_only_recent_reorg_window_events(self):
        previous = {
            "locks": [{"txHash": "0x" + "1" * 64, "tokenId": 7, "block": 9_900}],
            "unlocks": [],
        }

        vedolo_flows.assert_no_immutable_event_regression(
            previous,
            {"locks": [], "unlocks": []},
            current_block=10_000,
            reorg_depth=256,
        )

    def test_reconciles_cached_state_with_published_history_before_incremental_scan(self):
        published = {
            "locks": [
                {"txHash": "0x" + "1" * 64, "tokenId": 1, "block": 100},
                {"txHash": "0x" + "2" * 64, "tokenId": 2, "block": 200},
            ],
            "unlocks": [{"txHash": "0x" + "3" * 64, "tokenId": 1, "block": 300}],
            "transfers": [],
        }
        stale_state = {
            "last_block": 1_000,
            "transfers_last_block": 1_000,
            "locks": [published["locks"][1]],
            "unlocks": [],
            "transfers": [],
        }

        repaired = vedolo_flows.reconcile_state_with_published_history(stale_state, published)

        self.assertEqual(len(repaired["locks"]), 2)
        self.assertEqual(len(repaired["unlocks"]), 1)
        self.assertEqual(repaired["last_block"], 1_000)

    def test_bootstraps_missing_cache_from_last_published_event_block(self):
        published = {
            "locks": [{"txHash": "0x" + "1" * 64, "tokenId": 1, "block": 777}],
            "unlocks": [],
            "transfers": [{"txHash": "0x" + "2" * 64, "tokenId": 1, "block": 700, "from": "0x1", "to": "0x2"}],
        }

        repaired = vedolo_flows.reconcile_state_with_published_history({}, published)

        self.assertEqual(repaired["last_block"], 777)
        self.assertEqual(repaired["transfers_last_block"], 700)
        self.assertEqual(repaired["locks"], published["locks"])

    def test_audited_recovery_covers_every_current_expired_position_without_a_route(self):
        root = Path(__file__).resolve().parents[1]
        flows = json.loads((root / "vedolo_flows.json").read_text(encoding="utf-8"))
        holders = json.loads((root / "vedolo_holders.json").read_text(encoding="utf-8"))["holders"]

        routed_lock_ids = {
            int(lock.get("tokenId") or 0)
            for lock in flows.get("locks", [])
            if int(lock.get("depositType") or 0) != 3
        }
        latest_transfers = {}
        for transfer in flows.get("transfers", []):
            token_id = int(transfer.get("tokenId") or 0)
            previous = latest_transfers.get(token_id)
            if not previous or int(transfer.get("timestamp") or 0) > int(previous.get("timestamp") or 0):
                latest_transfers[token_id] = transfer

        missing_route_ids = set()
        now = int(time.time())
        for holder in holders:
            owner = str(holder.get("address") or "").lower()
            for position in holder.get("token_details", []):
                if int(position.get("end") or 0) > now:
                    continue
                token_id = int(position.get("id") or 0)
                latest_transfer = latest_transfers.get(token_id)
                transferred_to_owner = (
                    latest_transfer
                    and str(latest_transfer.get("to") or "").lower() == owner
                )
                if token_id not in routed_lock_ids and not transferred_to_owner:
                    missing_route_ids.add(token_id)

        self.assertFalse(missing_route_ids - set(AUDITED_MISSING_DEPOSIT_BLOCKS))
        self.assertTrue({12724, 12726, 15001}.issubset(AUDITED_MISSING_DEPOSIT_BLOCKS))

    def test_audited_recovery_covers_the_active_positions_missed_by_incremental_scans(self):
        expected = {
            24481, 24482, 24483, 24484, 24485, 24486, 24487, 24488, 24489,
            24518, 24923, 24930, 24973,
        }
        self.assertTrue(expected.issubset(AUDITED_MISSING_DEPOSIT_BLOCKS))

    def test_recovers_only_the_exact_missing_deposit_for_an_audited_token_block(self):
        provider = "0x" + "a" * 40
        target_token_id = 22
        target_block = 12_345

        def deposit_log(token_id, block, tx_suffix):
            return {
                "topics": [
                    DEPOSIT_TOPIC,
                    "0x" + ("0" * 24) + provider[2:],
                    "0x" + hex(1_800_000_000)[2:].zfill(64),
                ],
                "data": "0x" + "".join([
                    hex(token_id)[2:].zfill(64),
                    hex(5 * 10**18)[2:].zfill(64),
                    hex(1)[2:].zfill(64),
                    hex(1_700_000_000)[2:].zfill(64),
                ]),
                "transactionHash": "0x" + tx_suffix * 64,
                "blockNumber": hex(block),
            }

        calls = []

        def fetcher(start_block, end_block, topic):
            calls.append((start_block, end_block, topic))
            return [
                deposit_log(target_token_id, target_block, "b"),
                deposit_log(999, target_block, "c"),
            ]

        existing = [{"tokenId": 11, "txHash": "0x" + "1" * 64}]
        merged, recovered = recover_missing_deposit_events(
            existing,
            {11: 100, target_token_id: target_block},
            fetcher=fetcher,
        )

        self.assertEqual(calls, [(target_block, target_block + 1, DEPOSIT_TOPIC)])
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]["tokenId"], target_token_id)
        self.assertEqual(recovered[0]["address"], provider)
        self.assertEqual(recovered[0]["depositType"], 1)
        self.assertEqual([row["tokenId"] for row in merged], [11, target_token_id])

    def test_missing_deposit_recovery_fails_closed_without_an_exact_token_event(self):
        wrong_token_log = {
            "topics": [
                DEPOSIT_TOPIC,
                "0x" + ("0" * 24) + ("a" * 40),
                "0x" + hex(1_800_000_000)[2:].zfill(64),
            ],
            "data": "0x" + "".join([
                hex(999)[2:].zfill(64),
                hex(10**18)[2:].zfill(64),
                hex(1)[2:].zfill(64),
                hex(1_700_000_000)[2:].zfill(64),
            ]),
            "transactionHash": "0x" + "d" * 64,
            "blockNumber": hex(12_345),
        }

        with self.assertRaisesRegex(EventLogFetchError, "token 22 at block 12,345"):
            recover_missing_deposit_events(
                [],
                {22: 12_345},
                fetcher=lambda *_args: [wrong_token_log],
            )

    def test_remaps_odolo_locks_to_exerciser_wallets(self):
        locks = [
            {
                "address": ODOLO_VESTER,
                "txHash": "0x" + "1" * 64,
                "isOdolo": True,
            },
            {
                "address": ODOLO_VESTER,
                "txHash": "0x" + "2" * 64,
                "isOdolo": True,
            },
            {
                "address": "0x" + "3" * 40,
                "txHash": "0x" + "4" * 64,
                "isOdolo": False,
            },
        ]

        resolved, unresolved = remap_odolo_lock_beneficiaries(
            locks,
            {
                "0x" + "1" * 64: "0x" + "a" * 40,
            },
        )

        self.assertEqual((resolved, unresolved), (1, 1))
        self.assertEqual(locks[0]["address"], "0x" + "a" * 40)
        self.assertEqual(locks[0]["beneficiaryAddress"], "0x" + "a" * 40)
        self.assertEqual(locks[0]["protocolAddress"], ODOLO_VESTER)
        self.assertEqual(locks[0]["addressSource"], "odolo-exerciser")

        self.assertEqual(locks[1]["address"], ODOLO_VESTER)
        self.assertIsNone(locks[1]["beneficiaryAddress"])
        self.assertEqual(locks[1]["addressSource"], "odolo-vester-fallback")

        self.assertNotIn("beneficiaryAddress", locks[2])

    def test_remaps_legacy_vester_locks_without_is_odolo_flag(self):
        locks = [
            {
                "address": ODOLO_VESTER,
                "txHash": "0x" + "5" * 64,
            },
            {
                "address": "0x" + "6" * 40,
                "txHash": "0x" + "7" * 64,
            },
        ]

        resolved, unresolved = remap_odolo_lock_beneficiaries(
            locks,
            {
                "0x" + "5" * 64: "0x" + "b" * 40,
            },
        )

        self.assertEqual((resolved, unresolved), (1, 0))
        self.assertTrue(locks[0]["isOdolo"])
        self.assertEqual(locks[0]["address"], "0x" + "b" * 40)
        self.assertEqual(locks[0]["protocolAddress"], ODOLO_VESTER)
        self.assertEqual(locks[0]["addressSource"], "odolo-exerciser")
        self.assertNotIn("isOdolo", locks[1])

    def test_normalizes_protocol_address_for_previously_remapped_locks(self):
        beneficiary = "0x" + "c" * 40
        locks = [
            {
                "address": beneficiary,
                "protocolAddress": beneficiary,
                "txHash": "0x" + "8" * 64,
                "isOdolo": True,
            },
        ]

        resolved, unresolved = remap_odolo_lock_beneficiaries(
            locks,
            {
                "0x" + "8" * 64: beneficiary,
            },
        )

        self.assertEqual((resolved, unresolved), (1, 0))
        self.assertEqual(locks[0]["address"], beneficiary)
        self.assertEqual(locks[0]["beneficiaryAddress"], beneficiary)
        self.assertEqual(locks[0]["protocolAddress"], ODOLO_VESTER)
        self.assertEqual(locks[0]["addressSource"], "odolo-exerciser")

    def test_uses_receipt_beneficiary_when_exerciser_lookup_misses_tx(self):
        beneficiary = "0x" + "d" * 40
        locks = [
            {
                "address": ODOLO_VESTER,
                "txHash": "0x" + "9" * 64,
                "isOdolo": True,
                "beneficiaryAddress": beneficiary,
            },
        ]

        resolved, unresolved = remap_odolo_lock_beneficiaries(locks, {})

        self.assertEqual((resolved, unresolved), (1, 0))
        self.assertEqual(locks[0]["address"], beneficiary)
        self.assertEqual(locks[0]["protocolAddress"], ODOLO_VESTER)
        self.assertEqual(locks[0]["addressSource"], "odolo-receipt")

    def test_extracts_beneficiary_from_vester_receipt_logs(self):
        beneficiary = "0x" + "e" * 40
        receipt = {
            "logs": [
                {
                    "address": ODOLO_VESTER,
                    "topics": [
                        ODOLO_EXERCISE_TOPIC,
                        "0x" + ("0" * 24) + beneficiary[2:],
                        ZERO_TOPIC,
                    ],
                },
            ],
        }

        self.assertEqual(extract_odolo_receipt_beneficiary(receipt), beneficiary)

    def test_extracts_beneficiary_from_vedolo_mint_receipt_logs(self):
        beneficiary = "0x" + "f" * 40
        receipt = {
            "logs": [
                {
                    "address": VEDOLO_CONTRACT,
                    "topics": [
                        ODOLO_EXERCISE_TOPIC,
                        ZERO_TOPIC,
                        "0x" + ("0" * 24) + beneficiary[2:],
                    ],
                },
            ],
        }

        self.assertEqual(extract_odolo_receipt_beneficiary(receipt), beneficiary)

    def test_check_odolo_exercise_batch_uses_lookup_without_rpc(self):
        tx_hash = "0x" + "a" * 64
        beneficiary = "0x" + "1" * 40

        exercise_txs, complete = check_odolo_exercise_batch(
            [tx_hash],
            exerciser_lookup={tx_hash: beneficiary},
            receipt_checks={},
        )

        self.assertTrue(complete)
        self.assertEqual(exercise_txs, {tx_hash: beneficiary})

    def test_check_odolo_exercise_batch_uses_cached_receipts(self):
        tx_hash = "0x" + "b" * 64
        beneficiary = "0x" + "2" * 40

        exercise_txs, complete = check_odolo_exercise_batch(
            [tx_hash],
            receipt_checks={
                tx_hash: {
                    "isOdolo": True,
                    "beneficiary": beneficiary,
                },
            },
        )

        self.assertTrue(complete)
        self.assertEqual(exercise_txs, {tx_hash: beneficiary})

    def test_decodes_wallet_to_wallet_vedolo_transfer(self):
        from_address = "0x" + "3" * 40
        to_address = "0x" + "4" * 40
        tx_hash = "0x" + "c" * 64
        row = decode_transfer(
            {
                "topics": [
                    TRANSFER_TOPIC,
                    "0x" + ("0" * 24) + from_address[2:],
                    "0x" + ("0" * 24) + to_address[2:],
                    "0x" + "7b".zfill(64),
                ],
                "transactionHash": tx_hash,
                "blockNumber": "0x2a",
            }
        )

        self.assertEqual(
            row,
            {
                "from": from_address,
                "to": to_address,
                "txHash": tx_hash,
                "tokenId": 123,
                "block": 42,
            },
        )

    def test_ignores_vedolo_mints_and_burns_as_transfers(self):
        to_address = "0x" + "5" * 40
        mint = {
            "topics": [
                TRANSFER_TOPIC,
                ZERO_TOPIC,
                "0x" + ("0" * 24) + to_address[2:],
                "0x" + "1".zfill(64),
            ],
            "transactionHash": "0x" + "d" * 64,
            "blockNumber": "0x2b",
        }
        burn = {
            "topics": [
                TRANSFER_TOPIC,
                "0x" + ("0" * 24) + to_address[2:],
                ZERO_TOPIC,
                "0x" + "1".zfill(64),
            ],
            "transactionHash": "0x" + "e" * 64,
            "blockNumber": "0x2c",
        }

        self.assertIsNone(decode_transfer(mint))
        self.assertIsNone(decode_transfer(burn))

    def test_fetch_event_logs_accepts_topic_list_as_or_filter(self):
        captured = {}

        class _Resp:
            def __init__(self, logs):
                self._logs = logs

            def json(self):
                return {"result": self._logs}

        w_log = {"topics": [WITHDRAW_TOPIC], "blockNumber": "0x1"}
        d_log = {"topics": [DEPOSIT_TOPIC], "blockNumber": "0x2"}

        def _fake_post(url, json=None, **kwargs):
            captured["topics"] = json["params"][0]["topics"]
            return _Resp([w_log, d_log])

        rpc_usage.reset_usage()
        with patch("generate_vedolo_flows.CHUNK_SIZE", 10_000_000), \
             patch("generate_vedolo_flows.RPC_URLS", ["https://rpc.example"]), \
             patch("generate_vedolo_flows.requests.post", side_effect=_fake_post), \
             patch("generate_vedolo_flows.time.sleep", return_value=None):
            logs = fetch_event_logs(1, 100, [WITHDRAW_TOPIC, DEPOSIT_TOPIC])

        # Both event types travel in ONE getLogs request (OR-matched topic0).
        self.assertEqual(captured["topics"], [[WITHDRAW_TOPIC, DEPOSIT_TOPIC]])
        self.assertEqual(rpc_usage.usage_summary()["by_method"].get("eth_getLogs"), 1)
        # Caller splits the merged result back into per-type lists (data-identical).
        self.assertEqual(_logs_with_topic0(logs, WITHDRAW_TOPIC), [w_log])
        self.assertEqual(_logs_with_topic0(logs, DEPOSIT_TOPIC), [d_log])

    def test_fetch_event_logs_single_topic_stays_exact_match(self):
        captured = {}

        class _Resp:
            def json(self):
                return {"result": []}

        def _fake_post(url, json=None, **kwargs):
            captured["topics"] = json["params"][0]["topics"]
            return _Resp()

        with patch("generate_vedolo_flows.CHUNK_SIZE", 10_000_000), \
             patch("generate_vedolo_flows.RPC_URLS", ["https://rpc.example"]), \
             patch("generate_vedolo_flows.requests.post", side_effect=_fake_post), \
             patch("generate_vedolo_flows.time.sleep", return_value=None):
            fetch_event_logs(1, 100, TRANSFER_TOPIC)

        self.assertEqual(captured["topics"], [[TRANSFER_TOPIC]])

    def test_fetch_event_logs_fails_instead_of_skipping_unreadable_chunk(self):
        class RpcErrorResponse:
            def json(self):
                return {"error": {"message": "temporary RPC failure"}}

        with patch("generate_vedolo_flows.RPC_URLS", ["https://rpc.example"]):
            with patch("generate_vedolo_flows.requests.post", return_value=RpcErrorResponse()):
                with patch("generate_vedolo_flows.time.sleep", return_value=None):
                    with self.assertRaisesRegex(EventLogFetchError, "from block 1 to 2"):
                        fetch_event_logs(1, 2, TRANSFER_TOPIC)


if __name__ == "__main__":
    unittest.main()
