import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_supply_activity as gsa


class GenerateSupplyActivityTests(unittest.TestCase):
    def test_default_chain_list_skips_retired_polygon_zkevm(self):
        self.assertIn("polygon_zkevm", gsa.GRAPH_ENDPOINTS)
        self.assertIn("polygon_zkevm", gsa.RETIRED_GRAPH_CHAINS)
        self.assertNotIn("polygon_zkevm", gsa.DEFAULT_GRAPH_CHAINS)


if __name__ == "__main__":
    unittest.main()
