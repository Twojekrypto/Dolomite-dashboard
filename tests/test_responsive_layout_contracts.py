import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MOBILE_NAV = (ROOT / "mobile-nav.css").read_text(encoding="utf-8")
MOBILE_POLISH = (ROOT / "mobile-polish.css").read_text(encoding="utf-8")
ROUTE_LOADER = (ROOT / "route-loader.js").read_text(encoding="utf-8")
SUPPLY_HEALTH = (ROOT / "tvl" / "supply-health.css").read_text(encoding="utf-8")
EARN_CSS = (ROOT / "dashboard-core.css").read_text(encoding="utf-8")
VEDOLO = (ROOT / "vedolo-preview.html").read_text(encoding="utf-8")
HISTORY_ENTRY = (ROOT / "history" / "index.html").read_text(encoding="utf-8")
ROUTE_ENTRIES = [
    ROOT / "index.html",
    *sorted(path for path in ROOT.glob("*/index.html") if path.parent.name != "history"),
]


class ResponsiveLayoutContractsTest(unittest.TestCase):
    def test_shared_navigation_switches_before_desktop_tabs_clip(self):
        breakpoint = re.search(r"@media \(max-width: (\d+)px\)", MOBILE_NAV)
        self.assertIsNotNone(breakpoint)
        self.assertGreaterEqual(int(breakpoint.group(1)), 1180)

    def test_open_navigation_menu_respects_dynamic_viewport_height(self):
        panels = re.findall(
            r"\.mobile-nav-panel\s*\{(?P<body>.*?)\n\s*\}",
            MOBILE_NAV,
            re.DOTALL,
        )
        panel = next((body for body in panels if "max-height" in body), "")
        self.assertIn("100dvh", panel)
        self.assertIn("env(safe-area-inset-bottom", panel)
        self.assertIn("overflow-y: auto", panel)
        self.assertIn("responsive-20260801", ROUTE_LOADER)
        self.assertIn("mobile-nav-responsive-20260801", HISTORY_ENTRY)
        self.assertIn("mobile-polish-safari-details-20260805", HISTORY_ENTRY)

    def test_shared_details_controls_disable_webkit_native_clipping(self):
        compatibility = re.search(
            r"/\* Safari/WebKit table disclosure controls \*/(?P<selectors>.*?)\{(?P<rules>.*?)\}",
            MOBILE_POLISH,
            re.DOTALL,
        )
        self.assertIsNotNone(compatibility)
        selectors = compatibility.group("selectors")
        for selector in (
            ".asset-toggle",
            ".ex-toggle",
            ".holder-toggle",
            ".holder-details-btn",
            ".supply-health-row-toggle",
            ".earn-row-details-button",
            ".history-detail-toggle",
        ):
            with self.subTest(selector=selector):
                self.assertIn(selector, selectors)

        rules = compatibility.group("rules")
        self.assertIn("-webkit-appearance: none !important", rules)
        self.assertIn("appearance: none !important", rules)
        self.assertIn("overflow: visible !important", rules)
        self.assertIn("box-sizing: border-box !important", rules)
        self.assertIn("vertical-align: middle", rules)
        self.assertIn("mobile-polish-safari-details-20260805", ROUTE_LOADER)

    def test_all_route_entries_use_current_route_loader_cache_tag(self):
        cache_tag = "route-loader-table-ux-20260820"
        for path in ROUTE_ENTRIES:
            with self.subTest(path=path.relative_to(ROOT)):
                source = path.read_text(encoding="utf-8")
                self.assertIn(f"route-loader.js?v={cache_tag}", source)

    def test_vedolo_revenue_impact_table_has_an_internal_mobile_scroller(self):
        self.assertIn('class="exit-metric-scroll"', VEDOLO)
        self.assertRegex(
            VEDOLO,
            re.compile(
                r"@media \(max-width:720px\)\{.*?"
                r"\.revenue-impact-panel \.exit-metric-scroll\{[^}]*overflow-x:auto[^}]*\}.*?"
                r"\.revenue-impact-panel \.exit-metric-table\{[^}]*min-width:360px",
                re.DOTALL,
            ),
        )

    def test_mobile_shared_actions_meet_touch_target_contract(self):
        actions = re.search(
            r"body\.mobile-polished \.addr-copy,(?P<body>.*?)\{(?P<rules>.*?)\}",
            MOBILE_POLISH,
            re.DOTALL,
        )
        self.assertIsNotNone(actions)
        self.assertIn(".pg-btn", actions.group("body"))
        self.assertIn(".col-filter-btn", actions.group("body"))
        self.assertIn(".supply-health-row-toggle", actions.group("body"))
        self.assertIn(".earn-row-details-button", actions.group("body"))
        self.assertIn("min-width: 44px !important", actions.group("rules"))
        self.assertIn("min-height: 44px !important", actions.group("rules"))

    def test_route_owned_details_controls_allow_mobile_touch_height(self):
        self.assertIn(
            "#supply-health-card .supply-health-row-toggle {\n    min-height: 44px;",
            SUPPLY_HEALTH,
        )
        self.assertIn(
            ".earn-row-details-button {\n                min-height: 44px;",
            EARN_CSS,
        )


if __name__ == "__main__":
    unittest.main()
