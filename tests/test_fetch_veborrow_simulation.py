import unittest
from decimal import Decimal

from fetch_veborrow_simulation import (
    DISPLAY_SIMULATION_CHAINS,
    ELIGIBILITY_CHAINS,
    active_rebate_chains_from_metadata,
    active_rebate_market_ids_by_chain,
    debt_usd_for_token_value,
    encode_get_votes_call,
    fetch_onchain_vedolo_vote_weights,
    merge_vedolo_vote_sources,
    simulate_current_vedolo_rebates,
)
from validate_data import _veborrow_simulation_valid


class VeBorrowSimulationTest(unittest.TestCase):
    def test_display_simulation_chains_include_active_berachain_baseline(self):
        self.assertEqual(DISPLAY_SIMULATION_CHAINS, ["Ethereum", "Arbitrum", "Berachain"])
        self.assertEqual(DISPLAY_SIMULATION_CHAINS, ELIGIBILITY_CHAINS)

    def test_checked_in_snapshot_display_config_matches_code(self):
        import json

        with open("veborrow_simulation.json", "r", encoding="utf-8") as handle:
            snapshot = json.load(handle)

        self.assertEqual(snapshot["config"]["displaySimulationChains"], DISPLAY_SIMULATION_CHAINS)
        self.assertEqual(snapshot["config"]["simulationChains"], DISPLAY_SIMULATION_CHAINS)
        self.assertEqual(snapshot["config"]["activeRebateChains"], ["Arbitrum", "Berachain"])
        self.assertEqual(snapshot["config"]["annualizationPeriods"], 52)
        self.assertEqual(snapshot["config"]["thresholdBasis"], "official_annualized_max_rebate")
        self.assertTrue(_veborrow_simulation_valid(snapshot))

    def test_active_rebate_chains_follow_current_official_market_metadata(self):
        metadata = {
            "currentEpochIndex": 16,
            "allChainRebateInfo": {
                "1": None,
                "42161": {
                    "startEpoch": 10,
                    "claimsEnabled": False,
                    "marketToRebateInfo": {
                        "0": {"startEpoch": 10, "endEpoch": None},
                    },
                },
                "80094": {
                    "startEpoch": 1,
                    "claimsEnabled": False,
                    "marketToRebateInfo": {
                        "0": {"startEpoch": 1, "endEpoch": None},
                    },
                },
            },
        }

        self.assertEqual(
            active_rebate_chains_from_metadata(metadata),
            ["Arbitrum", "Berachain"],
        )

    def test_active_rebate_market_ids_only_include_currently_enabled_markets(self):
        market_ids = active_rebate_market_ids_by_chain({
            "currentEpochIndex": 6,
            "allChainRebateInfo": {
                "80094": {
                    "startEpoch": 1,
                    "marketToRebateInfo": {
                        "0": {"startEpoch": 1, "endEpoch": None},
                        "6": {"startEpoch": 1, "endEpoch": 5},
                        "7": {"startEpoch": 7, "endEpoch": None},
                    },
                },
                "1": None,
            },
        })

        self.assertEqual(market_ids, {"Berachain": {"0"}})

    def test_debt_usd_for_token_value_skips_non_eligible_active_market(self):
        token_value = {
            "valuePar": "-100",
            "token": {
                "id": "0x0000000000000000000000000000000000000001",
                "marketId": "6",
            },
        }

        debt = debt_usd_for_token_value(
            token_value,
            {"0x0000000000000000000000000000000000000001": Decimal("1")},
            {"0x0000000000000000000000000000000000000001": Decimal("1")},
            eligible_market_ids={"0"},
        )

        self.assertEqual(debt, Decimal("0"))

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
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 0.961538, places=6)
        self.assertAlmostEqual(wallet["eligibilityRatio"], 0.00961538, places=8)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 1_300_000.0)
        self.assertAlmostEqual(chain["currentVeDoloSavedUSD"], 0.961538, places=6)
        self.assertAlmostEqual(result["summary"]["currentVeDoloSavedUSD"], 0.961538, places=6)

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
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 5.769231, places=6)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 3_900_000.0)
        self.assertAlmostEqual(result["chains"]["Ethereum"]["currentVeDoloSavedUSD"], 1.923077, places=6)
        self.assertAlmostEqual(result["chains"]["Arbitrum"]["currentVeDoloSavedUSD"], 3.846154, places=6)

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
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 0.961538, places=6)
        self.assertAlmostEqual(wallet["eligibilityRatio"], 0.00961538, places=8)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 2_600_000.0)
        self.assertAlmostEqual(result["chains"]["Ethereum"]["currentVeDoloSavedUSD"], 0.961538, places=6)
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
        self.assertAlmostEqual(wallet["currentVeDoloSavedUSD"], 0.961538, places=6)
        self.assertAlmostEqual(wallet["requiredVeDoloForMax"], 1_300_000.0)
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
