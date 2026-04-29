import unittest

from generate_vedolo_flows import (
    ODOLO_EXERCISE_TOPIC,
    ODOLO_VESTER,
    VEDOLO_CONTRACT,
    ZERO_TOPIC,
    extract_odolo_receipt_beneficiary,
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


if __name__ == "__main__":
    unittest.main()
