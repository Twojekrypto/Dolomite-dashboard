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

    def test_supply_quality_column_and_compact_rate_labels(self):
        self.assertIn('data-column="quality"', self.html)
        self.assertLess(self.html.index('data-column="token"'), self.html.index('data-column="quality"'))
        self.assertIn('function earn_renderSupplyQualityCell(', self.js)
        self.assertIn("label: 'Interest'", self.js)
        self.assertIn("label: 'oDOLO'", self.js)
        self.assertIn('.earn-quality-marker', self.css)


if __name__ == '__main__':
    unittest.main()
