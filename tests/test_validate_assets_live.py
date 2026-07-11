import unittest

import validate_data


class AssetsLiveValidationTest(unittest.TestCase):
    def test_assets_live_accepts_active_chain_count_with_archived_chains_recorded(self):
        payload = {
            "chainCount": validate_data.EXPECTED_ASSETS_LIVE_CHAIN_COUNT,
            "chains": ["arbitrum", "ethereum", "berachain", "mantle", "xlayer"],
            "retiredChains": ["botanix", "polygonzkevm"],
        }

        self.assertTrue(validate_data._has_expected_assets_live_chains(payload))
        self.assertTrue(validate_data._has_expected_assets_live_retired_chains(payload))

    def test_assets_live_rejects_missing_archived_chain_record(self):
        payload = {
            "chainCount": validate_data.EXPECTED_ASSETS_LIVE_CHAIN_COUNT,
            "chains": ["arbitrum", "ethereum", "berachain", "mantle", "xlayer"],
            "retiredChains": ["polygonzkevm"],
        }

        self.assertFalse(validate_data._has_expected_assets_live_retired_chains(payload))

    def test_assets_live_allows_legacy_snapshot_with_polygon_still_active(self):
        payload = {
            "chainCount": 7,
            "chains": ["arbitrum", "ethereum", "berachain", "mantle", "botanix", "polygonzkevm", "xlayer"],
        }

        self.assertTrue(validate_data._has_expected_assets_live_chains(payload))
        self.assertTrue(validate_data._has_expected_assets_live_retired_chains(payload))

    def test_tvl_accepts_active_chains_with_archived_chains_recorded(self):
        payload = {
            "currentChainTvls": {
                "Ethereum": 1,
                "Berachain": 1,
                "Mantle": 1,
                "Arbitrum": 1,
                "X Layer": 1,
            },
            "retiredChains": ["Botanix", "Polygon zkEVM"],
        }

        self.assertTrue(validate_data._has_expected_tvl_chains(payload))
        self.assertTrue(validate_data._has_expected_tvl_retired_chains(payload))

    def test_tvl_allows_legacy_snapshot_with_polygon_still_active(self):
        payload = {
            "currentChainTvls": {
                "Ethereum": 1,
                "Berachain": 1,
                "Botanix": 1,
                "Polygon zkEVM": 1,
                "Mantle": 1,
                "Arbitrum": 1,
                "X Layer": 1,
            },
        }

        self.assertTrue(validate_data._has_expected_tvl_chains(payload))
        self.assertTrue(validate_data._has_expected_tvl_retired_chains(payload))

    def test_assets_live_rule_mentions_active_chains(self):
        check_names = [name for name, _ in validate_data.RULES["assets_live.json"]["checks"]]

        self.assertIn("all active configured chains must be present", check_names)
        self.assertIn("retired chains must be recorded", check_names)


if __name__ == "__main__":
    unittest.main()
