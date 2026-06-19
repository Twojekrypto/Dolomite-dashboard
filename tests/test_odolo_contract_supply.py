import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fetch_odolo_contract
import validate_data


class TestOdoloContractSupply(unittest.TestCase):
    def test_future_rewards_wallet_is_tracked(self):
        self.assertEqual(
            fetch_odolo_contract.FUTURE_REWARDS_WALLET.lower(),
            validate_data.ODOLO_FUTURE_REWARDS_WALLET,
        )

    def test_circulating_supply_excludes_future_rewards_and_vester(self):
        metrics = fetch_odolo_contract.derive_supply_metrics(
            total_supply=150_000_000,
            in_vester_balance=3_000_000,
            future_rewards_reserve=119_000_000,
        )

        self.assertEqual(metrics["inCirculation"], 28_000_000)
        self.assertEqual(metrics["futureRewardsReserve"], 119_000_000)
        self.assertEqual(metrics["circulatingExclusions"]["vesterBalance"], 3_000_000)
        self.assertNotIn("availableTokens", metrics["circulatingMethodology"])

    def test_validation_reconciles_odolo_circulating_supply(self):
        self.assertTrue(validate_data._odolo_circulating_reconciles({
            "totalSupply": 150_000_000,
            "futureRewardsReserve": 119_000_000,
            "inVesterBalance": 3_000_000,
            "inCirculation": 28_000_000,
        }))

        self.assertFalse(validate_data._odolo_circulating_reconciles({
            "totalSupply": 150_000_000,
            "futureRewardsReserve": 119_000_000,
            "inVesterBalance": 3_000_000,
            "inCirculation": 136_000_000,
        }))


if __name__ == "__main__":
    unittest.main()
