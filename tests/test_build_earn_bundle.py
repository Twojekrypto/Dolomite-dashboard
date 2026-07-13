import unittest
from pathlib import Path

from build_earn_bundle import render_earn_assets


ROOT = Path(__file__).resolve().parents[1]


class BuildEarnBundleTest(unittest.TestCase):
    def test_generated_bundle_contains_only_earn_view_and_runtime(self):
        html, javascript = render_earn_assets(
            ROOT / "dashboard-core.html",
            ROOT / "dashboard-core.js",
        )

        self.assertIn('<header class="site-header anim">', html)
        self.assertIn('<div id="view-earn" class="view-section active">', html)
        self.assertIn('id="earn-custom-date-overlay"', html)
        self.assertNotIn('id="view-treasury"', html)
        self.assertNotIn('id="view-dolo"', html)
        self.assertIn("const EARN_CHAINS", javascript)
        self.assertIn("function aprToApy", javascript)
        self.assertIn("function dolo_labelBadge", javascript)
        self.assertIn("UNIFIED PREMIUM TOOLTIP SYSTEM", javascript)
        self.assertNotIn("TREASURY NAMESPACE", javascript)
        self.assertNotIn("const HOLDER_DATA", javascript)

    def test_generated_bundle_is_materially_smaller_than_shared_sources(self):
        html, javascript = render_earn_assets(
            ROOT / "dashboard-core.html",
            ROOT / "dashboard-core.js",
        )

        self.assertLess(len(html), len((ROOT / "dashboard-core.html").read_text(encoding="utf-8")) * 0.25)
        self.assertLess(len(javascript), len((ROOT / "dashboard-core.js").read_text(encoding="utf-8")) * 0.70)

    def test_generated_bundle_trims_trailing_whitespace(self):
        html, javascript = render_earn_assets(
            ROOT / "dashboard-core.html",
            ROOT / "dashboard-core.js",
        )

        for generated in (html, javascript):
            self.assertEqual(generated, "\n".join(line.rstrip() for line in generated.splitlines()) + "\n")


if __name__ == "__main__":
    unittest.main()
