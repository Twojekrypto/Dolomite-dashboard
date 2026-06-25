import unittest

from fetch_veborrow_simulation import simulate_current_vedolo_rebates


class VeBorrowSimulationTest(unittest.TestCase):
    def test_current_vedolo_caps_user_savings_linearly_below_max_requirement(self):
        result = simulate_current_vedolo_rebates(
            current_debt_by_chain={
                "Ethereum": {
                    "0xabc": 100_000.0,
                },
            },
            selected_borrow_interest_by_chain={
                "Ethereum": 1_000.0,
            },
            ve_dolo_vote_weight_by_wallet={
                "0xabc": 12_500.0,
            },
            dolo_price_usd=0.02,
            ve_dolo_holding_factor=5.0,
            rebate_percentages={
                "Ethereum": 0.10,
            },
        )

        wallet = result["wallets"][0]
        chain = result["chains"]["Ethereum"]
        self.assertAlmostEqual(wallet["maxUsersSavedUSD"], 100.0)
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 50.0)
        self.assertAlmostEqual(wallet["eligibilityRatio"], 0.5)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 25_000.0)
        self.assertAlmostEqual(chain["currentVeDoloSavedUSD"], 50.0)
        self.assertAlmostEqual(result["summary"]["currentVeDoloSavedUSD"], 50.0)

    def test_current_vedolo_requirement_is_shared_across_simulated_networks(self):
        result = simulate_current_vedolo_rebates(
            current_debt_by_chain={
                "Ethereum": {"0xabc": 100_000.0},
                "Arbitrum": {"0xabc": 300_000.0},
            },
            selected_borrow_interest_by_chain={
                "Ethereum": 1_000.0,
                "Arbitrum": 2_000.0,
            },
            ve_dolo_vote_weight_by_wallet={
                "0xabc": 75_000.0,
            },
            dolo_price_usd=0.02,
            ve_dolo_holding_factor=5.0,
            rebate_percentages={
                "Ethereum": 0.10,
                "Arbitrum": 0.10,
            },
        )

        wallet = result["wallets"][0]
        self.assertAlmostEqual(wallet["maxUsersSavedUSD"], 300.0)
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 300.0)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 75_000.0)
        self.assertAlmostEqual(result["chains"]["Ethereum"]["currentVeDoloSavedUSD"], 100.0)
        self.assertAlmostEqual(result["chains"]["Arbitrum"]["currentVeDoloSavedUSD"], 200.0)


if __name__ == "__main__":
    unittest.main()
