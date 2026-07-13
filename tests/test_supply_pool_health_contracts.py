import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "supply" / "supply-draft.js"
STYLES = ROOT / "supply" / "supply-draft.css"
ROUTE = ROOT / "supply" / "index.html"
SUPPLY_VIEW = ROOT / "liquidation-preview.html"


class SupplyPoolHealthContractsTest(unittest.TestCase):
    def test_pool_health_refreshes_when_the_selected_supply_chain_changes(self):
        source = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("function syncSupplyHealthChain()", source)
        self.assertIn("supplyHealthExpandedKey = ''", source)
        self.assertIn("supplyChainSelect.addEventListener('change', syncSupplyHealthChain)", source)

    def test_pool_health_rows_expose_keyboard_and_expanded_state(self):
        source = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('tabindex="0"', source)
        self.assertIn('aria-expanded="${expanded ? \'true\' : \'false\'}"', source)
        self.assertIn("event.key === 'Enter' || event.key === ' '", source)

    def test_pool_health_uses_the_shared_dense_table_interaction_states(self):
        styles = STYLES.read_text(encoding="utf-8")

        self.assertIn(".supply-health-row:hover td", styles)
        self.assertIn(".supply-health-row:focus-visible td", styles)
        self.assertIn(".supply-health-row.expanded td", styles)
        self.assertIn(".supply-health-row td:first-child::before", styles)

    def test_supply_route_busts_the_polished_pool_health_assets(self):
        route = ROUTE.read_text(encoding="utf-8")

        self.assertIn("pool-health-ux-20260714-table-pager-icons", route)

    def test_supply_pool_health_uses_ten_row_pages_and_the_shared_pager_pattern(self):
        source = SCRIPT.read_text(encoding="utf-8")
        styles = STYLES.read_text(encoding="utf-8")

        self.assertIn("const SUPPLY_HEALTH_PAGE_SIZE = 10", source)
        self.assertIn('id="supply-health-pagination"', source)
        self.assertIn("function renderSupplyHealthPagination", source)
        self.assertIn("markets.slice(start, start + SUPPLY_HEALTH_PAGE_SIZE)", source)
        self.assertIn("function supplyHealthGoPage", source)
        self.assertIn("#supply-health-pagination", styles)

    def test_supply_tables_use_ten_rows_per_page(self):
        source = SUPPLY_VIEW.read_text(encoding="utf-8")

        self.assertIn("const SUPPLY_PER_PAGE = 10", source)
        self.assertIn("const SUPPLY_ACTIVITY_PAGE_SIZE = 10", source)

    def test_pool_health_uses_assets_tab_address_icons_for_isolation_markets(self):
        source = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("const supplyHealthAssetIconOverrides", source)
        self.assertIn("arbitrum:0x2c799166c9f0dbf9efc5004cbce4c5a37fa39329", source)
        self.assertIn("berachain:0xe946dd7d03f6f5c440f68c84808ca88d26475fc5", source)
        self.assertIn("getTokenIcon(token.symbol)", source)

    def test_pool_health_left_accent_is_explicitly_gold_only(self):
        styles = STYLES.read_text(encoding="utf-8")

        selector = "body.supply-draft-route .supply-health-row:hover td:first-child::before"
        start = styles.index(selector)
        block = styles[start:styles.index("}", start)]
        self.assertIn("background: var(--supply-gold)", block)
        self.assertNotIn("supply-green", block)


if __name__ == "__main__":
    unittest.main()
