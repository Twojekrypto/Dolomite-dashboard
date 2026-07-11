import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_supply_activity as gsa


class GenerateSupplyActivityTests(unittest.TestCase):
    def test_default_chain_list_skips_archived_chains(self):
        for chain in ("polygon_zkevm", "botanix"):
            self.assertIn(chain, gsa.GRAPH_ENDPOINTS)
            self.assertIn(chain, gsa.RETIRED_GRAPH_CHAINS)
            self.assertNotIn(chain, gsa.DEFAULT_GRAPH_CHAINS)


if __name__ == "__main__":
    unittest.main()
