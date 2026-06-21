import unittest
from unittest.mock import patch

from generate_vedolo_flows import (
    EventLogFetchError,
    ODOLO_EXERCISE_TOPIC,
    ODOLO_VESTER,
    TRANSFER_TOPIC,
    VEDOLO_CONTRACT,
    ZERO_TOPIC,
    check_odolo_exercise_batch,
    decode_transfer,
    extract_odolo_receipt_beneficiary,
    fetch_event_logs,
    remap_odolo_lock_beneficiaries,
)


class GenerateVedoloFlowsTests(unittest.TestCase):
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
