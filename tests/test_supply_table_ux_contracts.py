import unittest
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TVL_VIEW = ROOT / "tvl-preview.html"
TVL_SCRIPT = ROOT / "tvl" / "supply-health.js"
TVL_STYLES = ROOT / "tvl" / "supply-health.css"
LIQUIDATION_VIEW = ROOT / "liquidation-preview.html"
SUPPLY_SCRIPT = ROOT / "supply" / "supply-draft.js"
SUPPLY_STYLES = ROOT / "supply" / "supply-draft.css"
DOLO_VIEW = ROOT / "dolo-preview.html"
PORTFOLIO_VIEW = ROOT / "portfolio-preview.html"


class SupplyTableUxContractsTest(unittest.TestCase):
    def assert_source_has(self, needle, source, surface):
        self.assertTrue(needle in source, f"{surface}: missing {needle!r}")

    def test_supply_health_groups_search_and_assets_style_network_filter(self):
        html = TVL_VIEW.read_text(encoding="utf-8")
        source = TVL_SCRIPT.read_text(encoding="utf-8")
        styles = TVL_STYLES.read_text(encoding="utf-8")

        self.assert_source_has('class="supply-health-filter-group"', html, "TVL markup")
        self.assertRegex(
            html,
            re.compile(
                r'class="supply-health-filter-group">[\s\S]*supply-health-search-shell[\s\S]*supply-health-chain-dropdown',
            ),
        )
        self.assert_source_has('tvl-dd-opt select-all', source, "TVL filter renderer")
        self.assert_source_has('dd-opt-check', source, "TVL filter renderer")
        self.assert_source_has("'All Chains'", source, "TVL filter renderer")
        self.assert_source_has('.supply-health-filter-group', styles, "TVL filter styles")

    def test_supply_health_multi_select_keeps_the_menu_open_after_rerender(self):
        source = TVL_SCRIPT.read_text(encoding="utf-8")
        handler_start = source.index("chainList.addEventListener('click', event => {")
        handler_end = source.index("card.querySelectorAll('th[data-health-sort]')", handler_start)
        handler = source[handler_start:handler_end]

        self.assert_source_has('event.stopPropagation();', handler, "TVL multi-select handler")

    def test_lending_positions_filter_excludes_retired_networks_only(self):
        source = LIQUIDATION_VIEW.read_text(encoding="utf-8")

        self.assert_source_has('const LENDING_POSITION_ACTIVE_CHAIN_KEYS', source, "Lending Positions")
        self.assert_source_has("'ethereum'", source, "Lending Positions")
        self.assert_source_has("'xlayer'", source, "Lending Positions")
        active_keys = source[source.index('const LENDING_POSITION_ACTIVE_CHAIN_KEYS'):source.index('const LENDING_POSITION_ACTIVE_CHAIN_KEYS') + 280]
        self.assertFalse("'base'" in active_keys, "retired Botanix key must not be active")
        self.assertFalse("'polygon_zkevm'" in active_keys, "retired Polygon zkEVM key must not be active")
        self.assert_source_has('sanitizeLendingPositionChainSelection()', source, "Lending Positions")

    def test_asset_activity_has_live_loading_copy_and_right_side_period_control(self):
        html = LIQUIDATION_VIEW.read_text(encoding="utf-8")
        source = SUPPLY_SCRIPT.read_text(encoding="utf-8")
        styles = SUPPLY_STYLES.read_text(encoding="utf-8")

        self.assert_source_has('aria-live="polite"', html, "Asset Activity markup")
        self.assertFalse("'30D ready'" in html, "static 30D-ready copy must not be rendered")
        self.assert_source_has('Loading latest 30D activity', html, "Asset Activity markup")
        self.assert_source_has('moveActivityPeriodToToolbarActions', source, "Asset Activity script")
        self.assert_source_has('supply-activity-toolbar-actions', source, "Asset Activity script")
        self.assert_source_has('supply-draft-activity-continuous-surface', styles, "Asset Activity styles")

    def test_fresh_and_portfolio_position_summaries_share_metric_rail_contract(self):
        dolo = DOLO_VIEW.read_text(encoding="utf-8")
        portfolio = PORTFOLIO_VIEW.read_text(encoding="utf-8")

        self.assert_source_has('fresh-stat selected-market-metric', dolo, "Fresh Wallet summary")
        self.assert_source_has('Fresh Wallets', dolo, "Fresh Wallet summary")
        self.assert_source_has('DOLO Received', dolo, "Fresh Wallet summary")
        self.assert_source_has('pf-exercise-summary-item selected-market-metric', portfolio, "Position Activity summary")
        self.assert_source_has('function renderExerciseSummary(rows)', portfolio, "Position Activity summary")


if __name__ == "__main__":
    unittest.main()
