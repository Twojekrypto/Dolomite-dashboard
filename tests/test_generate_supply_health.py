import unittest

import generate_supply_health as health


class GenerateSupplyHealthTests(unittest.TestCase):
    def test_default_chain_list_skips_archived_chains(self):
        for chain in ("polygon_zkevm", "botanix"):
            self.assertIn(chain, health.GRAPH_ENDPOINTS)
            self.assertIn(chain, health.RETIRED_GRAPH_CHAINS)
            self.assertNotIn(chain, health.DEFAULT_GRAPH_CHAINS)


if __name__ == "__main__":
    unittest.main()
