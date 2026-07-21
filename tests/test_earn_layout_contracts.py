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
        self.assertIn('.earn-lending-table tbody td[data-column="pnl"] .earn-net-inline,', self.css)
        self.assertIn('td[data-column="pnl"] {\n            text-align: left;', self.css)
        self.assertIn('td[data-column="pnl"] {\n  text-align: left !important;', self.draft_css)
        self.assertIn('Balance <span class="earn-sort-arrow">', self.html)
        self.assertIn('<span>Total Yield</span>', self.html)
        self.assertNotIn('Total Yield Earned', self.html)
        self.assertNotIn('Current Balance', self.html)


if __name__ == '__main__':
    unittest.main()
