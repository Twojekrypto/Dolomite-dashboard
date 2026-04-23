import unittest

from generate_vedolo_flows import ODOLO_VESTER, remap_odolo_lock_beneficiaries


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


if __name__ == "__main__":
    unittest.main()
