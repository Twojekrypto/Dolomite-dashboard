import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TVL_SCRIPT = ROOT / "tvl" / "supply-health.js"
TVL_STYLES = ROOT / "tvl" / "supply-health.css"
TVL_VIEW = ROOT / "tvl-preview.html"
SUPPLY_SCRIPT = ROOT / "supply" / "supply-draft.js"
SUPPLY_STYLES = ROOT / "supply" / "supply-draft.css"
SUPPLY_VIEW = ROOT / "liquidation-preview.html"


class SupplyPoolHealthContractsTest(unittest.TestCase):
    def test_pool_health_is_directly_below_token_composition_on_tvl(self):
        html = TVL_VIEW.read_text(encoding="utf-8")

        token_position = html.index("<h2>Token Composition</h2>")
        health_position = html.index('id="supply-health-card"')
        protocol_position = html.index("<!-- PROTOCOL INFO")
        self.assertLess(token_position, health_position)
        self.assertLess(health_position, protocol_position)
        self.assertIn("tvl/supply-health.css?v=", html)
        self.assertIn("tvl/supply-health.js?v=", html)

    def test_pool_health_has_asset_search_and_independent_chain_filter(self):
        html = TVL_VIEW.read_text(encoding="utf-8")
        source = TVL_SCRIPT.read_text(encoding="utf-8")

        self.assertIn('id="supply-health-search"', html)
        self.assertIn('aria-label="Search asset"', html)
        self.assertIn('<div class="supply-health-search-shell">', html)
        self.assertIn('id="supply-health-search-clear"', html)
        self.assertIn('id="supply-health-chain-filter"', html)
        self.assertIn('id="supply-health-chain-panel"', html)
        self.assertIn("clearSupplyHealthSearch(searchInput)", source)
        self.assertIn("updateSupplyHealthFilters(state, { chains })", source)

    def test_pool_health_rows_expose_keyboard_and_expanded_state(self):
        source = TVL_SCRIPT.read_text(encoding="utf-8")

        self.assertIn('tabindex="0"', source)
        self.assertIn('aria-expanded="${expanded ? \'true\' : \'false\'}"', source)
        self.assertIn("event.key === 'Enter' || event.key === ' '", source)

    def test_pool_health_uses_white_rows_and_gold_interaction_rail(self):
        styles = TVL_STYLES.read_text(encoding="utf-8")

        self.assertIn("#supply-health-card .supply-health-table tbody td", styles)
        self.assertIn("color: var(--fg-1)", styles)
        self.assertIn(".supply-health-row:hover td", styles)
        self.assertIn(".supply-health-row:focus-visible td", styles)
        self.assertIn(".supply-health-row.expanded td", styles)

        selector = "#supply-health-card .supply-health-row:hover td:first-child::before"
        start = styles.index(selector)
        block = styles[start:styles.index("}", start)]
        self.assertIn("background: var(--gold)", block)

    def test_pool_health_header_uses_holders_style_data_freshness(self):
        html = TVL_VIEW.read_text(encoding="utf-8")
        source = TVL_SCRIPT.read_text(encoding="utf-8")
        styles = TVL_STYLES.read_text(encoding="utf-8")

        self.assertIn('id="supply-health-updated"', html)
        self.assertIn("function supplyHealthRelativeAge", source)
        self.assertIn("Data updated ·", source)
        self.assertIn(".supply-health-updated::before", styles)
        self.assertIn("background: var(--gold)", styles)

    def test_pool_health_uses_ten_row_pages_and_shared_pager_shape(self):
        html = TVL_VIEW.read_text(encoding="utf-8")
        source = TVL_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("const SUPPLY_HEALTH_PAGE_SIZE = 10", source)
        self.assertIn('id="supply-health-pagination"', html)
        self.assertIn("function renderSupplyHealthPagination", source)
        self.assertIn("paginateSupplyHealthMarkets(markets, state.page", source)
        self.assertIn("window.supplyHealthGoPage", source)

    def test_pool_health_footer_matches_holders_range_layout(self):
        html = TVL_VIEW.read_text(encoding="utf-8")

        self.assertNotIn(
            "Wallet counts are on-chain addresses:",
            html,
        )
        self.assertIn('id="supply-health-page-range"', html)
        self.assertIn('id="supply-health-pagination"', html)

    def test_supply_route_no_longer_mounts_pool_health(self):
        source = SUPPLY_SCRIPT.read_text(encoding="utf-8")
        styles = SUPPLY_STYLES.read_text(encoding="utf-8")

        self.assertNotIn("installSupplyHealthCard();", source)
        self.assertNotIn("fetchSupplyHealth();", source)
        self.assertNotIn("/* ---- Supply Pool Health card ---- */", styles)

    def test_asset_activity_summary_uses_four_semantic_metrics(self):
        source = SUPPLY_SCRIPT.read_text(encoding="utf-8")
        styles = SUPPLY_STYLES.read_text(encoding="utf-8")

        self.assertNotIn("label: `Net Flow · ${meta.short}`", source)
        self.assertIn("cls: 'deposit'", source)
        self.assertIn("cls: 'withdraw'", source)
        self.assertIn("cls: 'transfer'", source)
        self.assertIn(
            "grid-template-columns: repeat(4, minmax(0, 1fr))",
            styles,
        )
        self.assertIn(".supply-activity-stat.deposit .value", styles)
        self.assertIn(".supply-activity-stat.withdraw .value", styles)
        self.assertIn(".supply-activity-stat.transfer .value", styles)

    def test_existing_supply_tables_keep_ten_rows_per_page(self):
        source = SUPPLY_VIEW.read_text(encoding="utf-8")

        self.assertIn("const SUPPLY_PER_PAGE = 10", source)
        self.assertIn("const SUPPLY_ACTIVITY_PAGE_SIZE = 10", source)


if __name__ == "__main__":
    unittest.main()
