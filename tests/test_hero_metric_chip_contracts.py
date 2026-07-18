import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class HeroMetricChipContractsTest(unittest.TestCase):
    def test_primary_dashboard_cards_share_the_gold_value_chip_contract(self):
        pages = {
            "dolo-preview.html": 'class="hero-chg hero-value-chip up"',
            "odolo-preview.html": 'class="hero-chg gold hero-value-chip"',
            "tvl-preview.html": 'class="hero-chg hero-value-chip up" id="heroChg"',
            "portfolio-preview.html": 'class="pf-sum-usd" id="pf-sum-total-usd"',
            "rewards-preview.html": 'class="hero-chg hero-value-chip"',
            "revenue-preview.html": 'class="hero-chg hero-value-chip" id="dailyChange"',
        }
        for filename, marker in pages.items():
            with self.subTest(page=filename):
                self.assertIn(marker, (ROOT / filename).read_text(encoding="utf-8"))

    def test_primary_dashboard_cards_align_value_chips_to_the_hero_baseline(self):
        for filename, selector in (
            ("dolo-preview.html", ".hero-price-row{display:flex;align-items:flex-end;"),
            ("odolo-preview.html", ".hero-price-row{display:flex;align-items:flex-end;"),
            ("tvl-preview.html", ".hero-headline{display:flex;align-items:flex-end;"),
            ("portfolio-preview.html", ".pf-sum-headline{display:flex;align-items:flex-end;"),
            ("rewards-preview.html", ".hero-price-row{display:flex;align-items:flex-end;"),
            ("revenue-preview.html", ".hero-headline{display:flex;align-items:flex-end;"),
        ):
            with self.subTest(page=filename):
                self.assertIn(selector, (ROOT / filename).read_text(encoding="utf-8"))

    def test_rewards_and_revenue_use_the_shared_count_up_metric(self):
        for filename, setter in (
            ("rewards-preview.html", "function setMetricText(id, value)"),
            ("revenue-preview.html", "function setMetricText(id, value)"),
        ):
            source = (ROOT / filename).read_text(encoding="utf-8")
            with self.subTest(page=filename):
                self.assertIn('src="count-up.js?v=20260427-countup"', source)
                self.assertIn(setter, source)
                self.assertIn("window.CountUpMetric", source)

    def test_value_chips_keep_the_short_context_label(self):
        portfolio = (ROOT / "portfolio-preview.html").read_text(encoding="utf-8")
        self.assertIn('<span class="lbl">wallet value</span>', portfolio)
        for filename, label in (
            ("dolo-preview.html", 'class="lbl">24h change</span>'),
            ("tvl-preview.html", 'class="lbl">7d change</span>'),
            ("rewards-preview.html", 'class="lbl">daily rewards</span>'),
            ("revenue-preview.html", 'class="lbl">24h change</span>'),
        ):
            with self.subTest(page=filename):
                self.assertIn(label, (ROOT / filename).read_text(encoding="utf-8"))

    def test_routes_bust_preview_cache_for_the_hero_value_chip_refresh(self):
        for filename in (
            "index.html",
            "dolo/index.html",
            "odolo/index.html",
            "tvl/index.html",
            "portfolio/index.html",
            "rewards/index.html",
            "revenue/index.html",
        ):
            with self.subTest(route=filename):
                self.assertIn("hero-value-chip-20260718", (ROOT / filename).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
