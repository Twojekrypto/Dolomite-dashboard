import unittest

import generate_earn_snapshots as snapshots


class GenerateEarnSnapshotsTests(unittest.TestCase):
    def test_default_chain_list_skips_archived_chains(self):
        for chain in ("polygonzkevm", "botanix"):
            self.assertIn(chain, snapshots.CHAINS)
            self.assertIn(chain, snapshots.RETIRED_CHAINS)
            self.assertNotIn(chain, snapshots.DEFAULT_CHAINS)


if __name__ == "__main__":
    unittest.main()
