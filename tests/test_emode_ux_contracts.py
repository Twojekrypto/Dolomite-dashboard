import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO = ROOT / "portfolio-preview.html"
LIQUIDATION = ROOT / "liquidation-preview.html"
EARN_CSS = ROOT / "dashboard-core.css"
EARN_JS = ROOT / "dashboard-core.js"
EARN_HTML = ROOT / "dashboard-core.html"

EMODE_TIP = (
    "E-Mode applies special risk parameters for correlated assets in the same category. "
    "It can improve borrow efficiency, but liquidation still depends on the account's "
    "collateral, debt, and selected E-Mode category."
)
FLAME_HIGHLIGHT = (
    '<path d="M12.2 11.45c.1.93-.78 1.32-.78 2.22a.78.78 0 1 0 1.56 0'
    'c0-.74-.3-1.35-.78-2.22Z" fill="#fff1c1"/>'
)


def between(source, start, end):
    start_index = source.index(start)
    end_index = source.index(end, start_index + len(start))
    return source[start_index:end_index]


class EModeUxContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.portfolio = PORTFOLIO.read_text(encoding="utf-8")
        cls.liquidation = LIQUIDATION.read_text(encoding="utf-8")
        cls.earn_css = EARN_CSS.read_text(encoding="utf-8")
        cls.earn_js = EARN_JS.read_text(encoding="utf-8")
        cls.earn_html = EARN_HTML.read_text(encoding="utf-8")

    def test_portfolio_remains_the_emode_reference(self):
        self.assertIn(FLAME_HIGHLIGHT, self.portfolio)
        self.assertIn(EMODE_TIP, self.portfolio)
        self.assertIn('class="pf-emode-icon"', self.portfolio)
        self.assertIn('min-height:24px', self.portfolio)
        self.assertIn('.pf-emode-badge:hover{transform:scale(1.04)', self.portfolio)

    def test_liquidation_emode_matches_portfolio_badge(self):
        badge_css = between(self.liquidation, ".emode-badge {", "/* E-Mode row highlight */")
        self.assertIn(FLAME_HIGHLIGHT, self.liquidation)
        self.assertIn(EMODE_TIP, self.liquidation)
        self.assertIn('class="emode-icon" aria-hidden="true"', self.liquidation)
        self.assertIn("min-height: 24px", badge_css)
        self.assertIn("border-radius: 999px", badge_css)
        self.assertIn("width: 18px", badge_css)
        self.assertIn("height: 18px", badge_css)
        self.assertIn("transform: scale(1.04)", badge_css)
        self.assertIn("E-Mode</span>", self.liquidation)

    def test_changed_pages_have_fresh_cache_versions(self):
        route_versions = {
            "portfolio/index.html": "20260713-emode-expand",
            "liquidation/index.html": "20260713-emode",
            "borrow/index.html": "20260713-emode",
            "supply/index.html": "20260713-emode",
            "earn/index.html": "earn-core-20260723-premium-ux-2",
            "rewards/index.html": "20260713-supply-copy",
        }
        for path, version in route_versions.items():
            with self.subTest(path=path):
                self.assertIn(version, (ROOT / path).read_text(encoding="utf-8"))
        self.assertIn('dashboard-core.css?v=core-split-20260723-earn-premium-ux', self.earn_html)
        self.assertIn('dashboard-core.js?v=core-split-20260723-earn-premium-ux', self.earn_html)

    def test_both_earn_render_paths_share_full_emode_badge(self):
        badge_css = between(
            self.earn_css,
            ".earn-hf-emode-inline {",
            "/* ═══════ BORROW POSITIONS — MOBILE RESPONSIVE CARDS",
        )
        self.assertIn("const EARN_EMODE_TIP =", self.earn_js)
        self.assertIn("const EARN_EMODE_ICON =", self.earn_js)
        self.assertIn(FLAME_HIGHLIGHT, self.earn_js)
        self.assertIn(EMODE_TIP, self.earn_js)
        self.assertIn("function earn_emodeBadge(active)", self.earn_js)
        self.assertEqual(self.earn_js.count("earn_emodeBadge(p.eMode)"), 2)
        self.assertIn('class="earn-emode-icon" aria-hidden="true"', self.earn_js)
        self.assertIn("E-Mode</span>", self.earn_js)
        self.assertIn("min-height: 24px", badge_css)
        self.assertIn("border-radius: 999px", badge_css)
        self.assertIn("width: 18px", badge_css)
        self.assertIn("height: 18px", badge_css)
        self.assertIn("transform: scale(1.04)", badge_css)


if __name__ == "__main__":
    unittest.main()
