import unittest

from fetch_veborrow_simulation import (
    DISPLAY_SIMULATION_CHAINS,
    ELIGIBILITY_CHAINS,
    encode_get_votes_call,
    fetch_onchain_vedolo_vote_weights,
    merge_vedolo_vote_sources,
    simulate_current_vedolo_rebates,
)


class VeBorrowSimulationTest(unittest.TestCase):
    def test_display_simulation_chains_include_active_berachain_baseline(self):
        self.assertEqual(DISPLAY_SIMULATION_CHAINS, ["Ethereum", "Arbitrum", "Berachain"])
        self.assertEqual(DISPLAY_SIMULATION_CHAINS, ELIGIBILITY_CHAINS)

    def test_encode_get_votes_call_uses_erc20_votes_selector_and_padded_address(self):
        call = encode_get_votes_call("0x000000000000000000000000000000000000dEaD")

        self.assertEqual(call[:10], "0x9ab24eb0")
        self.assertEqual(call[-40:], "000000000000000000000000000000000000dead")
        self.assertEqual(len(call), 74)

    def test_fetch_onchain_votes_skips_empty_wallet_set(self):
        votes, metadata = fetch_onchain_vedolo_vote_weights([])

        self.assertEqual(votes, {})
        self.assertEqual(metadata["status"], "skipped")
        self.assertEqual(metadata["requestedWallets"], 0)

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

    def test_current_vedolo_requirement_includes_active_eligibility_chains(self):
        result = simulate_current_vedolo_rebates(
            current_debt_by_chain={
                "Ethereum": {"0xabc": 100_000.0},
                "Berachain": {"0xabc": 100_000.0},
            },
            selected_borrow_interest_by_chain={
                "Ethereum": 1_000.0,
                "Berachain": 1_000.0,
            },
            ve_dolo_vote_weight_by_wallet={
                "0xabc": 25_000.0,
            },
            dolo_price_usd=0.02,
            ve_dolo_holding_factor=5.0,
            rebate_percentages={
                "Ethereum": 0.10,
                "Berachain": 0.10,
            },
            display_chains=["Ethereum"],
            eligibility_chains=["Ethereum", "Berachain"],
        )

        wallet = result["wallets"][0]
        self.assertEqual(set(result["chains"].keys()), {"Ethereum"})
        self.assertAlmostEqual(wallet["maxUsersSavedUSD"], 100.0)
        self.assertAlmostEqual(wallet["eligibilityMaxUsersSavedUSD"], 200.0)
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 50.0)
        self.assertAlmostEqual(wallet["eligibilityRatio"], 0.5)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 50_000.0)
        self.assertAlmostEqual(result["chains"]["Ethereum"]["currentVeDoloSavedUSD"], 50.0)
        self.assertAlmostEqual(result["summary"]["maxUsersSavedUSD"], 100.0)
        self.assertAlmostEqual(result["summary"]["eligibilityMaxUsersSavedUSD"], 200.0)

    def test_revenue_factor_scales_rebate_payout_but_not_vedolo_requirement(self):
        result = simulate_current_vedolo_rebates(
            current_debt_by_chain={
                "Ethereum": {"0xabc": 100_000.0},
            },
            selected_borrow_interest_by_chain={
                "Ethereum": 1_000.0,
            },
            ve_dolo_vote_weight_by_wallet={
                "0xabc": 25_000.0,
            },
            dolo_price_usd=0.02,
            ve_dolo_holding_factor=5.0,
            rebate_percentages={
                "Ethereum": 0.10,
            },
            chain_revenue_factors={
                "Ethereum": 0.50,
            },
        )

        wallet = result["wallets"][0]
        self.assertAlmostEqual(wallet["eligibilityMaxUsersSavedUSD"], 100.0)
        self.assertAlmostEqual(wallet["maxUsersSavedUSD"], 50.0)
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 50.0)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 25_000.0)
        self.assertAlmostEqual(result["chains"]["Ethereum"]["maxUsersSavedUSD"], 50.0)

    def test_onchain_vote_weight_overrides_snapshot_when_available(self):
        merged = merge_vedolo_vote_sources(
            {
                "voteWeightByWallet": {"0xabc": 100.0, "0xdef": 50.0},
                "lockedDoloByWallet": {"0xabc": 125.0, "0xdef": 75.0},
                "holderCount": 2,
            },
            {"0xabc": 90.0},
        )

        self.assertAlmostEqual(float(merged["voteWeightByWallet"]["0xabc"]), 90.0)
        self.assertAlmostEqual(float(merged["voteWeightByWallet"]["0xdef"]), 50.0)
        self.assertEqual(merged["voteWeightSourceByWallet"]["0xabc"], "onchain_getVotes")
        self.assertEqual(merged["voteWeightSourceByWallet"]["0xdef"], "holder_snapshot_fallback")
        self.assertEqual(merged["sourceCounts"]["onchain_getVotes"], 1)
        self.assertEqual(merged["sourceCounts"]["holder_snapshot_fallback"], 1)


if __name__ == "__main__":
    unittest.main()
