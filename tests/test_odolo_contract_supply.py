import json
import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

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

    def test_allocation_metrics_derive_burned_supply_from_integer_units(self):
        metrics = fetch_odolo_contract.derive_allocation_metrics(
            147_113_292 * 10**18,
            18,
        )

        self.assertEqual(metrics["allocationSupply"], 200_000_000)
        self.assertEqual(metrics["redeemedAndBurned"], 52_886_708)
        self.assertEqual(
            metrics["allocationSupplyWei"],
            str(200_000_000 * 10**18),
        )
        self.assertEqual(
            metrics["redeemedAndBurnedWei"],
            str(52_886_708 * 10**18),
        )
        self.assertIn(
            "allocationSupply - totalSupply",
            metrics["allocationMethodology"],
        )

    def test_allocation_metrics_reject_total_supply_above_allocation(self):
        with self.assertRaises(ValueError):
            fetch_odolo_contract.derive_allocation_metrics(
                200_000_001 * 10**18,
                18,
            )

    def test_allocation_components_must_reconcile_to_200m(self):
        payload = {
            "allocationSupply": 200_000_000,
            "totalSupply": 147_000_000,
            "futureRewardsReserve": 126_000_000,
            "inVesterBalance": 3_000_000,
            "inCirculation": 18_000_000,
            "redeemedAndBurned": 53_000_000,
        }
        self.assertTrue(validate_data._odolo_allocation_reconciles(payload))

        payload["redeemedAndBurned"] = 52_000_000
        self.assertFalse(validate_data._odolo_allocation_reconciles(payload))

    def test_raw_supply_components_reconcile_exactly_in_wei(self):
        unit = 10**18
        payload = {
            "allocationSupply": 200_000_000,
            "totalSupply": 147_000_000,
            "futureRewardsReserve": 126_000_000,
            "inVesterBalance": 3_000_000,
            "inCirculation": 18_000_000,
            "redeemedAndBurned": 53_000_000,
            "allocationSupplyWei": str(200_000_000 * unit),
            "totalSupplyWei": str(147_000_000 * unit),
            "futureRewardsReserveWei": str(126_000_000 * unit),
            "inVesterBalanceWei": str(3_000_000 * unit),
            "inCirculationWei": str(18_000_000 * unit),
            "redeemedAndBurnedWei": str(53_000_000 * unit),
        }

        self.assertTrue(validate_data._odolo_allocation_reconciles(payload))

        payload["inCirculationWei"] = str(18_000_000 * unit + 1)
        self.assertFalse(validate_data._odolo_allocation_reconciles(payload))

    def test_display_supply_values_must_match_their_raw_wei_sources(self):
        unit = 10**18
        payload = {
            "allocationSupply": 200_000_000,
            "totalSupply": 147_000_000,
            "futureRewardsReserve": 126_000_001,
            "inVesterBalance": 3_000_000,
            "inCirculation": 18_000_000,
            "redeemedAndBurned": 53_000_000,
            "allocationSupplyWei": str(200_000_000 * unit),
            "totalSupplyWei": str(147_000_000 * unit),
            "futureRewardsReserveWei": str(126_000_000 * unit),
            "inVesterBalanceWei": str(3_000_000 * unit),
            "inCirculationWei": str(18_000_000 * unit),
            "redeemedAndBurnedWei": str(53_000_000 * unit),
        }

        self.assertFalse(validate_data._odolo_allocation_reconciles(payload))

    def test_raw_supply_metrics_preserve_integer_units(self):
        metrics = fetch_odolo_contract.derive_raw_supply_metrics(
            total_supply_wei=150_000_000 * 10**18 + 7,
            in_vester_balance_wei=3_000_000 * 10**18 + 2,
            future_rewards_reserve_wei=119_000_000 * 10**18 + 3,
        )

        self.assertEqual(metrics["totalSupplyWei"], str(150_000_000 * 10**18 + 7))
        self.assertEqual(metrics["inVesterBalanceWei"], str(3_000_000 * 10**18 + 2))
        self.assertEqual(metrics["futureRewardsReserveWei"], str(119_000_000 * 10**18 + 3))
        self.assertEqual(metrics["inCirculationWei"], str(28_000_000 * 10**18 + 2))

    def test_contract_snapshot_reads_every_value_at_one_block(self):
        unit = 10**18
        client = Mock()
        client.call.return_value = hex(12_345_678)
        client.eth_call_batch.side_effect = [
            [
                hex(150_000_000 * unit),
                hex(18),
                hex(3_000_000 * unit),
                hex(119_000_000 * unit),
            ],
            [hex(10_000_000 * unit), hex(7_000_000 * unit), hex(3_000_000 * unit)],
        ]
        client.last_endpoint = "https://rpc.example"

        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(fetch_odolo_contract, "OUTPUT_FILE", os.path.join(tmp, "snapshot.json")), \
             patch.object(fetch_odolo_contract, "RpcClient", return_value=client), \
             patch.object(fetch_odolo_contract, "get_holder_count", return_value=None):
            fetch_odolo_contract.main()
            with open(fetch_odolo_contract.OUTPUT_FILE) as snapshot_file:
                payload = json.load(snapshot_file)

        self.assertEqual(payload["blockNumber"], 12_345_678)
        self.assertEqual(client.eth_call_batch.call_count, 2)
        for call in client.eth_call_batch.call_args_list:
            self.assertEqual(call.kwargs["block"], hex(12_345_678))


if __name__ == "__main__":
    unittest.main()
