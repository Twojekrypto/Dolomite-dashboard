import unittest

import generate_earn_snapshots as snapshots


class GenerateEarnSnapshotsTests(unittest.TestCase):
    def test_default_chain_list_skips_retired_polygon_zkevm(self):
        self.assertIn("polygonzkevm", snapshots.CHAINS)
        self.assertIn("polygonzkevm", snapshots.RETIRED_CHAINS)
        self.assertNotIn("polygonzkevm", snapshots.DEFAULT_CHAINS)


if __name__ == "__main__":
    unittest.main()
