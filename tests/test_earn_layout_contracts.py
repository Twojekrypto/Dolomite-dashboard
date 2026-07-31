import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EarnLayoutContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (ROOT / 'earn/earn-core.html').read_text(encoding='utf-8')
        cls.js = (ROOT / 'earn/earn-core.js').read_text(encoding='utf-8')
        cls.css = (ROOT / 'dashboard-core.css').read_text(encoding='utf-8')
        cls.draft_css = (ROOT / 'earn/earn-draft.css').read_text(encoding='utf-8')
        cls.bundle_builder = (ROOT / 'build_earn_bundle.py').read_text(encoding='utf-8')

    def test_supply_schema_places_price_before_supply(self):
        expected = ['token', 'quality', 'price', 'supply', 'balance', 'yield', 'details']
        start = self.html.index('<table class="earn-asset-table" data-earn-layout-table="supply">')
        end = self.html.index('</table>', start)
        fragment = self.html[start:end]
        positions = [fragment.index(f'data-column="{key}"') for key in expected]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('Supply <span class="earn-sort-arrow">', fragment)
        self.assertNotIn('Supply APR', fragment)

    def test_supply_price_uses_existing_canonical_price_cache(self):
        self.assertIn("earn_sortAssets('price')", self.html)
        self.assertIn("key === 'price'", self.js)
        self.assertIn('earn_getUsdPrice(a.symbol, a.tokenAddr, cid)', self.js)
        self.assertIn('function earn_formatMarketPrice(price)', self.js)

    def test_supply_and_borrow_details_share_compact_assets_geometry(self):
        self.assertIn('.earn-row-details-button {', self.css)
        self.assertIn('max-width: 72px', self.css)
        self.assertIn('margin: 0 auto', self.css)
        self.assertIn('.earn-details-cell {', self.css)

    def test_css_uses_stable_column_keys_not_supply_nth_child_alignment(self):
        self.assertIn('[data-column="price"]', self.css)
        self.assertIn('[data-column="supply"]', self.css)
        self.assertIn('[data-column="yield"]', self.css)
        self.assertIn('body.earn-draft-route #earn-supply-section .earn-asset-table thead th[data-column="price"]', self.css)
        self.assertIn('colgroup col[data-column="price"]', self.draft_css)
        self.assertIn('.earn-supply-rate-cell .assets-apy-breakdown', self.draft_css)
        self.assertNotIn('.earn-asset-table:not(.earn-past-table) thead th:nth-child(2)', self.draft_css)

    def test_supply_quality_column_and_assets_rate_labels(self):
        self.assertIn('data-column="quality"', self.html)
        self.assertLess(self.html.index('data-column="token"'), self.html.index('data-column="quality"'))
        self.assertIn('function earn_renderSupplyQualityCell(', self.js)
        self.assertIn("l: 'Lending'", self.js)
        self.assertIn("l: 'oDOLO'", self.js)
        self.assertIn("l: 'Yield'", self.js)
        self.assertIn('earn-supply-apr-line', self.js)
        self.assertIn('earn-supply-apr-cell', self.css)
        self.assertIn('.earn-supply-rate-cell {\n            text-align: right;', self.css)
        self.assertIn('thead th[data-column="supply"],', self.draft_css)
        self.assertIn('tbody td[data-column="supply"],', self.draft_css)
        self.assertIn('.earn-quality-marker', self.css)

    def test_past_assets_move_quality_markers_out_of_token(self):
        expected = ['token', 'quality', 'yield', 'details']
        start = self.html.index('<table class="earn-asset-table earn-past-table"')
        end = self.html.index('</table>', start)
        past_table = self.html[start:end]
        positions = [past_table.index(f'data-column="{key}"') for key in expected]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('Quality', past_table)

        past_renderer_start = self.js.index('function earn_renderWithdrawnAssets()')
        past_renderer_end = self.js.index('function earn_togglePastPositions()', past_renderer_start)
        past_renderer = self.js[past_renderer_start:past_renderer_end]
        self.assertIn('const qualityCellHtml = earn_renderSupplyQualityCell(', past_renderer)
        self.assertIn('<td data-column="quality">${qualityCellHtml}</td>', past_renderer)
        self.assertIn('colSpan: 4', past_renderer)
        self.assertNotIn('earn_renderVerificationBadge(', past_renderer)
        self.assertNotIn('earn_renderYieldSourceBadge(', past_renderer)

        self.assertIn('.earn-past-table colgroup col[data-column="quality"]', self.draft_css)
        self.assertIn('.earn-past-table thead th[data-column="quality"]', self.draft_css)
        self.assertIn('.earn-past-table tbody td[data-column="details"]', self.draft_css)
        self.assertIn('padding-left: 6px !important;', self.draft_css)

    def test_published_past_layout_matches_saved_local_widths(self):
        self.assertIn('.earn-past-table colgroup col[data-column="token"] { width: 39%; }', self.css)
        self.assertIn('.earn-past-table colgroup col[data-column="details"] { width: 14%; }', self.css)
        self.assertIn('body.earn-draft-route .earn-past-table colgroup col[data-column="token"] { width: 39% !important; }', self.draft_css)
        self.assertIn('body.earn-draft-route .earn-past-table colgroup col[data-column="details"] { width: 14% !important; }', self.draft_css)

    def test_portfolio_value_has_no_redundant_earn_overview_eyebrow(self):
        self.assertNotIn('Earn Overview', self.js)
        self.assertIn('Portfolio Value${addrBadgeHtml}', self.js)
        self.assertNotIn('Earn Overview', (ROOT / 'dashboard-core.js').read_text(encoding='utf-8'))

    def test_bundle_builder_keeps_static_layout_cache_and_local_editor_guard(self):
        self.assertIn(
            "earn-core-20260724-mismatch-floor-straight-hover-filter-icon-parity-20260731",
            self.bundle_builder,
        )
        self.assertIn("new URLSearchParams(window.location.search).get('layoutEditor')", self.bundle_builder)
        self.assertIn("const loopback = isLocalhost", self.bundle_builder)

    def test_earn_sort_controls_match_the_assets_table_pattern(self):
        for table in ('earn-asset-table', 'earn-lending-table', 'earn-past-table'):
            self.assertIn(f'.{table} thead th[data-sort] .earn-sort-arrow', self.css)
        self.assertIn("arrow.textContent = isActive ? (ascending ? '\\u25b2' : '\\u25bc') : ''", self.js)
        self.assertIn('data-sort="token" aria-sort="none" onclick="earn_sortPastPositions(\'token\')"', self.html)

    def test_borrow_emode_column_and_compact_table_amounts(self):
        borrow_start = self.html.index('<table class="earn-data-table earn-lending-table"')
        borrow_end = self.html.index('</table>', borrow_start)
        borrow = self.html[borrow_start:borrow_end]
        expected = ['health', 'emode', 'collateral', 'debt', 'pnl', 'details']
        positions = [borrow.index(f'data-column="{key}"') for key in expected]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('data-sort="lend-emode"', borrow)
        self.assertIn('<td data-column="emode" class="earn-emode-cell">', self.js)
        self.assertIn('function earn_formatAmountOneDecimal(', self.js)
        self.assertIn('function earn_formatUSDOneDecimal(', self.js)
        self.assertIn('earn_formatAmountOneDecimal(yieldCalc.absYield, a.decimals)', self.js)
        self.assertIn('earn_formatAmountOneDecimal(item.yieldWei, item.decimals)', self.js)
        self.assertIn('.earn-merged-tokens {\n            display: flex;\n            flex-direction: column;', self.css)

    def test_supply_token_and_borrow_pnl_follow_compact_header_alignment(self):
        self.assertIn('<div class="earn-token-header">', self.html)
        self.assertIn('#earn-supply-section .earn-token-header {', self.css)
        past_start = self.html.index('<table class="earn-asset-table earn-past-table"')
        past_end = self.html.index('</table>', past_start)
        past_table = self.html[past_start:past_end]
        self.assertIn('<div class="earn-token-header">', past_table)
        self.assertNotIn('style="display:flex;align-items:center;gap:12px"', past_table)
        self.assertIn('#earn-past-section .earn-token-header {', self.css)
        self.assertIn('#earn-past-section .earn-past-table thead th[data-column="token"]', self.draft_css)
        self.assertIn('.earn-lending-table tbody td[data-column="pnl"] .earn-net-inline,', self.css)
        self.assertIn('td[data-column="pnl"] {\n            text-align: left;', self.css)
        self.assertIn('td[data-column="pnl"] {\n  text-align: left !important;', self.draft_css)
        self.assertIn('Balance <span class="earn-sort-arrow">', self.html)
        self.assertIn('<span>Total Yield</span>', self.html)
        self.assertNotIn('Total Yield Earned', self.html)
        self.assertNotIn('Current Balance', self.html)


if __name__ == '__main__':
    unittest.main()
