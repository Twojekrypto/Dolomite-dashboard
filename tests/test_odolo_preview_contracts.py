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

    def test_latest_odolo_exercises_keeps_usdc_paid_and_price_columns(self):
        self.assertIn("Latest oDOLO Exercises", self.html)
        self.assertIn('data-latest-sort="usdc" class="num" style="width:118px">USDC Paid', self.latest_table)
        self.assertIn('data-latest-sort="price" class="num" style="width:96px">Price', self.latest_table)
        self.assertIn('data-label="USDC Paid"><span class="bal-val usd">${fmtUsdFull(tx.usdc)}</span>', self.latest_render)
        self.assertIn('data-label="Price"><span class="bal-val price">${fmtPrice(tx.price)}</span>', self.latest_render)
        self.assertIn('tbody.innerHTML = `<tr><td colspan="6"><div class="latest-empty">Loading latest oDOLO exercises…</div></td></tr>`;', self.latest_render)
        self.assertIn('tbody.innerHTML = `<tr><td colspan="6"><div class="latest-empty">${emptyLabel}</div></td></tr>`;', self.latest_render)

    def test_odolo_route_no_longer_uses_mistaken_latest_price_cache_bust(self):
        self.assertNotIn("latest-ex-price-20260706", self.route)

    def test_discount_curve_uses_the_protocol_schedule_and_stays_within_zero_to_fifty_percent(self):
        curve = re.search(
            r'function syncLiveDiscountCurve\(\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        ).group("body")

        self.assertIn("const yMin = 0;", self.html)
        self.assertIn("const yMax = 50;", self.html)
        self.assertIn("const discount = theoreticalDiscount(days);", curve)
        self.assertIn("Protocol discount by lock duration", self.html)
        self.assertNotIn("dailyDoloPrice", self.html)
        self.assertNotIn('fetchJson("dolo_price_history.json")', self.html)

    def test_one_day_filters_compare_exact_transaction_timestamps(self):
        self.assertIn("return Date.now() - days * 86400000;", self.html)
        self.assertIn("if(cutoff && exerciseTimeMs(tx) < cutoff) return;", self.html)
        self.assertNotIn("if(cutoff && tx.date < cutoff) return;", self.html)

    def test_short_locks_render_in_hours_or_minutes(self):
        self.assertIn('if(days < 1/24) return Math.max(1, Math.round(days*1440)) + "m";', self.html)
        self.assertIn('if(days < 1) return (days*24).toFixed(1) + "h";', self.html)

    def test_claimer_behavior_donut_uses_non_overlapping_claim_partition(self):
        behavior = re.search(
            r'function syncLiveBehavior\(\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        ).group("body")

        self.assertIn("pct_claim_remaining", behavior)
        self.assertNotIn("pct_held", behavior)
        self.assertNotIn("pct_bought_extra", behavior)

    def test_latest_pair_controls_use_portfolio_green_interactions(self):
        self.assertIn('id="latest-pairs-section"', self.html)
        self.assertIn('--latest-pair-accent:#75b87b', self.html)
        self.assertIn('#latest-pairs-section .search:focus-within', self.html)
        self.assertIn('#latest-pairs-section #dd-pair-period .dd-btn.open', self.html)
        self.assertIn('#latest-pairs-section #dd-pair-period .dd-opt.active .dd-opt-check', self.html)
        self.assertIn('odolo-pair-green-controls-20260711', self.route)

    def test_latest_exercise_controls_match_the_pair_green_interactions(self):
        self.assertIn('id="latest-exercises-section"', self.html)
        self.assertIn('--latest-exercise-accent:#75b87b', self.html)
        self.assertIn('#latest-exercises-section .search:focus-within', self.html)
        self.assertIn('#latest-exercises-section #dd-latest-period .dd-btn.open', self.html)
        self.assertIn('#latest-exercises-section #dd-latest-period .dd-opt.active .dd-opt-check', self.html)
        self.assertIn('odolo-exercise-green-controls-20260711', self.route)


if __name__ == "__main__":
    unittest.main()
