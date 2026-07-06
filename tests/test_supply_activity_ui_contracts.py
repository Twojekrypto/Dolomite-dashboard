import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LIQUIDATION_PREVIEW = ROOT / "liquidation-preview.html"
SUPPLY_DRAFT_JS = ROOT / "supply" / "supply-draft.js"
SUPPLY_DRAFT_CSS = ROOT / "supply" / "supply-draft.css"
SUPPLY_INDEX = ROOT / "supply" / "index.html"


class SupplyActivityUiContractsTest(unittest.TestCase):
    def test_asset_activity_uses_dolo_holder_address_labels(self):
        html = LIQUIDATION_PREVIEW.read_text(encoding="utf-8")
        activity_renderer = re.search(
            r"const renderActivityAddressTools = address => \{(?P<body>.*?)\n\s+\};",
            html,
            re.S,
        )
        self.assertIsNotNone(activity_renderer)
        body = activity_renderer.group("body")
        self.assertIn("renderDoloAddressTools(address, debankUrl", body)
        self.assertIn("forceLabel: true", body)

    def test_asset_activity_period_filter_supports_all_time(self):
        js = SUPPLY_DRAFT_JS.read_text(encoding="utf-8")
        self.assertIn("{ key: 'all', short: 'All', label: 'All time', days: null }", js)
        self.assertIn("function isActivityAllTimePeriod(meta)", js)
        self.assertIn("if (isActivityAllTimePeriod(meta)) return null;", js)
        self.assertIn("supplyActivityTimeMin = cutoffTs;", js)
        self.assertIn("timeTrigger?.classList.toggle('has-active', cutoffTs != null);", js)
        self.assertIn("activityPeriodNeedsFullHistory(meta)", js)

    def test_activity_period_dropdown_is_not_clipped_by_empty_table(self):
        css = SUPPLY_DRAFT_CSS.read_text(encoding="utf-8")
        self.assertIn(
            "body.supply-draft-route #supply-activity-card .table-card-inner {\n  overflow: visible !important;\n}",
            css,
        )
        self.assertIn("body.supply-draft-route #supply-activity-card .supply-activity-toolbar", css)
        self.assertIn("z-index: 100;", css)

    def test_supply_route_cache_is_busted_for_activity_update(self):
        html = SUPPLY_INDEX.read_text(encoding="utf-8")
        self.assertIn("activity-labels-alltime-dropdown", html)


if __name__ == "__main__":
    unittest.main()
