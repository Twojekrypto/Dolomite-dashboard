import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ODOLO_HTML = ROOT / "odolo-preview.html"
ODOLO_ROUTE = ROOT / "odolo" / "index.html"


class OdoloPreviewContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = ODOLO_HTML.read_text(encoding="utf-8")
        cls.route = ODOLO_ROUTE.read_text(encoding="utf-8")
        cls.latest_table = re.search(
            r'<table class="tbl latest-ex-tbl" id="tbl-latest-ex">(?P<body>.*?)</table>',
            cls.html,
            re.S,
        ).group("body")
        cls.latest_render = re.search(
            r'function renderLatestExercises\(\)\{(?P<body>.*?)\nfunction getLatestPairRows\(\)',
            cls.html,
            re.S,
        ).group("body")

    def test_latest_odolo_exercises_uses_price_instead_of_usdc_paid(self):
        self.assertIn("Latest oDOLO Exercises", self.html)
        self.assertIn('data-latest-sort="price" class="num" style="width:96px">Price', self.latest_table)
        self.assertNotIn('data-latest-sort="usdc"', self.latest_table)
        self.assertNotIn("USDC Paid", self.latest_table)
        self.assertNotIn('data-label="USDC Paid"', self.latest_render)
        self.assertNotIn("fmtUsdFull(tx.usdc)", self.latest_render)
        self.assertIn('data-label="Price"><span class="bal-val price">${fmtPrice(tx.price)}</span>', self.latest_render)
        self.assertIn('tbody.innerHTML = `<tr><td colspan="5"><div class="latest-empty">Loading latest oDOLO exercises…</div></td></tr>`;', self.latest_render)
        self.assertIn('tbody.innerHTML = `<tr><td colspan="5"><div class="latest-empty">${emptyLabel}</div></td></tr>`;', self.latest_render)

    def test_odolo_route_busts_cache_for_latest_exercise_price_column(self):
        self.assertIn("latest-ex-price-20260706", self.route)


if __name__ == "__main__":
    unittest.main()
