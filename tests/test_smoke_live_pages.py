import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.fspath(ROOT))

import scripts.smoke_live_pages as smoke


class SmokeLivePagesTests(unittest.TestCase):
    def test_earn_route_uses_dedicated_generated_bundle(self):
        route_html = (ROOT / "earn" / "index.html").read_text(encoding="utf-8")

        self.assertEqual(smoke.ROUTE_CHECKS["/earn/"], ("earn/earn-core.html", "Earn"))
        self.assertIn('"target": "../earn/earn-core.html"', route_html)
        self.assertIn("/earn/earn-core.js", smoke.ASSET_CHECKS)

    def test_earn_asset_check_matches_simplified_summary(self):
        expected = smoke.ASSET_CHECKS["/earn/earn-core.js"]

        self.assertIn("EARN TAB NAMESPACE", expected)
        self.assertIn("Total Yield Earned", expected)
        self.assertIn("Rewards", expected)
        self.assertNotIn("Historical Yield P&amp;L", expected)

    def test_liquidation_route_check_matches_route_shell(self):
        route_html = (ROOT / "liquidation" / "index.html").read_text(encoding="utf-8")

        self.assertEqual(smoke.ROUTE_CHECKS["/liquidation/"], ("liquidation-preview.html", "Liquidations"))
        for needle in smoke.ROUTE_CHECKS["/liquidation/"]:
            self.assertIn(needle, route_html)


if __name__ == "__main__":
    unittest.main()
