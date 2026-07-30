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
        self.assertIn(
            "event.target.closest('a, button, input, select, textarea, [role=\"button\"], [contenteditable=\"true\"]')",
            source,
        )

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

    def test_pool_health_uses_eight_columns_and_assets_style_details_controls(self):
        html = TVL_VIEW.read_text(encoding="utf-8")
        source = TVL_SCRIPT.read_text(encoding="utf-8")
        styles = TVL_STYLES.read_text(encoding="utf-8")

        self.assertIn('<th data-health-sort="chain"><span class="th-content">Chain', html)
        self.assertNotIn('data-health-sort="avgWalletUsd"', html)
        self.assertNotIn('data-health-sort="supply30dPct"', html)
        self.assertIn('<th class="supply-health-details-head">Details</th>', html)
        self.assertIn('class="supply-health-chain-cell"', source)
        self.assertIn('class="supply-health-details-cell"', source)
        self.assertIn('<td colspan="8">', source)
        self.assertNotIn('<td colspan="9">', source)
        self.assertIn("label: '30D Supply Change'", source)
        self.assertIn("value: formatHealthSignedPct(growth.supply30dPct)", source)
        self.assertIn("tone: healthSignedClass(growth.supply30dPct)", source)
        self.assertIn("featured: true", source)
        self.assertIn(
            'class="supply-health-detail-stat${stat.featured ? \' featured\' : \'\'}"',
            source,
        )
        self.assertIn('<span>${expanded ? \'Hide\' : \'Details\'}</span>', source)
        self.assertIn('<polyline points="6 9 12 15 18 9"/>', source)

        hover_start = styles.index(
            "#supply-health-card .supply-health-table thead th[data-health-sort]:hover"
        )
        hover_end = styles.index("}", hover_start)
        header_hover_block = styles[hover_start:hover_end + 1]
        self.assertNotIn('background: var(--bg-3);', header_hover_block)

        numeric_selector = "#supply-health-card .supply-health-table tbody td.num"
        numeric_start = styles.rindex(numeric_selector)
        numeric_block = styles[numeric_start:styles.index("}", numeric_start)]
        self.assertIn("font-size: 13px", numeric_block)
        self.assertIn("font-weight: 500", numeric_block)

        centered_columns = (
            "#supply-health-card .supply-health-table thead th:nth-child(4),\n"
            "#supply-health-card .supply-health-table thead th:nth-child(5),\n"
            "#supply-health-card .supply-health-table thead th:nth-child(6),\n"
            "#supply-health-card .supply-health-table thead th:nth-child(7),\n"
            "#supply-health-card .supply-health-table thead th:nth-child(8),\n"
            "#supply-health-card .supply-health-table tbody td:nth-child(4),\n"
            "#supply-health-card .supply-health-table tbody td:nth-child(5),\n"
            "#supply-health-card .supply-health-table tbody td:nth-child(6),\n"
            "#supply-health-card .supply-health-table tbody td:nth-child(7),\n"
            "#supply-health-card .supply-health-table tbody td:nth-child(8)"
        )
        centered_start = styles.index(centered_columns)
        centered_block = styles[centered_start:styles.index("}", centered_start)]
        self.assertIn("text-align: center", centered_block)

        self.assertNotIn(
            "#supply-health-card .supply-health-table thead th:nth-child(3),\n"
            "#supply-health-card .supply-health-table thead th:nth-child(4),\n"
            "#supply-health-card .supply-health-table thead th:nth-child(5)",
            styles,
        )

        narrow_hidden_columns = (
            "  #supply-health-card .supply-health-table thead th:nth-child(3),\n"
            "  #supply-health-card .supply-health-table thead th:nth-child(4),\n"
            "  #supply-health-card .supply-health-table thead th:nth-child(5),\n"
            "  #supply-health-card .supply-health-table thead th:nth-child(6),\n"
            "  #supply-health-card .supply-health-table tbody td:nth-child(3),\n"
            "  #supply-health-card .supply-health-table tbody td:nth-child(4),\n"
            "  #supply-health-card .supply-health-table tbody td:nth-child(5),\n"
            "  #supply-health-card .supply-health-table tbody td:nth-child(6)"
        )
        self.assertIn(narrow_hidden_columns + " {\n    display: none", styles)
        self.assertIn(
            "  #supply-health-card .supply-health-table thead th,\n"
            "  #supply-health-card .supply-health-table tbody td {\n"
            "    padding-right: 8px;\n"
            "    padding-left: 8px;",
            styles,
        )
        self.assertIn(
            "  #supply-health-card .supply-health-table thead th:nth-child(2) { width: 42%; }\n"
            "  #supply-health-card .supply-health-table thead th:nth-child(7) { width: 17%; }\n"
            "  #supply-health-card .supply-health-table thead th:nth-child(8) {\n"
            "    width: 13%;\n"
            "    padding: 0;",
            styles,
        )
        narrow_start = styles.index("@media (max-width: 840px)")
        mobile_start = styles.index("@media (max-width: 760px)")
        narrow_block = styles[narrow_start:mobile_start]
        self.assertIn(
            "  #supply-health-card .supply-health-detail {\n"
            "    grid-template-columns: 1fr;\n"
            "  }",
            narrow_block,
        )
        button_start = styles.index("#supply-health-card .supply-health-row-toggle {")
        button_block = styles[button_start:styles.index("}", button_start)]
        self.assertIn("height: 24px", button_block)
        self.assertIn("max-width: 72px", button_block)
        self.assertIn("border-radius: 999px", button_block)
        self.assertIn("background: var(--gold-wash)", button_block)

        featured_start = styles.index(
            "#supply-health-card .supply-health-detail-stat.featured"
        )
        featured_block = styles[featured_start:styles.index("}", featured_start)]
        self.assertIn("grid-column: 1 / -1", featured_block)

    def test_pool_health_details_use_market_dossier_and_assets_icon_frame(self):
        source = TVL_SCRIPT.read_text(encoding="utf-8")
        styles = TVL_STYLES.read_text(encoding="utf-8")

        self.assertIn('class="supply-health-detail-panel"', source)
        self.assertIn('class="supply-health-detail-head"', source)
        self.assertIn(
            'class="supply-health-token-icon ${iconPresentation.frameClass}"',
            source,
        )
        self.assertNotIn('class="supply-health-asset-icon"', source)
        self.assertIn(
            "#supply-health-card .supply-health-detail-panel::before",
            styles,
        )
        self.assertIn("#supply-health-card .supply-health-detail-head", styles)
        self.assertIn(
            "#supply-health-card .supply-health-token-icon.full-logo",
            styles,
        )
        self.assertIn("@media (max-width: 1100px)", styles)

    def test_pool_health_semantic_colors_are_card_scoped(self):
        source = TVL_SCRIPT.read_text(encoding="utf-8")
        styles = TVL_STYLES.read_text(encoding="utf-8")

        self.assertIn('class="num health-participation"', source)
        self.assertIn('class="num health-concentration"', source)
        self.assertIn(
            "formatHealthConcentrationTip('top10', market.top10Pct)",
            source,
        )
        self.assertIn(
            "formatHealthConcentrationTip('largest', market.largestPct)",
            source,
        )
        cell_selector = "#supply-health-card .supply-health-table tbody td"
        self.assertIn(f"{cell_selector}.health-participation", styles)
        self.assertIn(f"{cell_selector}.health-concentration", styles)
        self.assertNotIn("health-concentration-low", styles)
        self.assertNotIn("health-concentration-moderate", styles)
        self.assertNotIn("health-concentration-high", styles)

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

    def test_pool_health_keeps_a_fixed_scroll_viewport_through_expansion(self):
        styles = TVL_STYLES.read_text(encoding="utf-8")

        scroll_selector = "#supply-health-card .supply-health-scroll"
        scroll_start = styles.index(scroll_selector)
        scroll_block = styles[scroll_start:styles.index("}", scroll_start)]
        self.assertIn("height: 662px", scroll_block)
        self.assertIn("overflow-y: auto", scroll_block)

        narrow_start = styles.index("@media (max-width: 840px)")
        narrow_styles = styles[narrow_start:]
        self.assertIn(
            "#supply-health-card .supply-health-table thead th:nth-child(3),",
            narrow_styles,
        )
        self.assertIn(
            "#supply-health-card .supply-health-table tbody td:nth-child(6)",
            narrow_styles,
        )
        self.assertIn("display: none", narrow_styles)

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
