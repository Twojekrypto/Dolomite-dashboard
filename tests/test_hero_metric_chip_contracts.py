import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class HeroMetricChipContractsTest(unittest.TestCase):
    @staticmethod
    def css_rule(source, selector):
        match = re.search(re.escape(selector.rstrip("{")) + r"\s*\{([^}]*)\}", source)
        if not match:
            return ""
        return re.sub(r"\s+", "", match.group(1))

    def test_primary_dashboard_cards_share_the_gold_value_chip_contract(self):
        pages = {
            "dolo-preview.html": 'class="hero-chg hero-value-chip up"',
            "odolo-preview.html": 'class="hero-chg gold hero-value-chip"',
            "tvl-preview.html": 'class="hero-chg hero-value-chip up" id="heroChg"',
            "portfolio-preview.html": 'class="pf-sum-usd" id="pf-sum-total-usd"',
            "rewards-preview.html": 'class="hero-chg hero-value-chip"',
            "revenue-preview.html": 'class="hero-chg hero-value-chip" id="dailyChange"',
            "vedolo-preview.html": 'class="hero-chip"',
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
            ("vedolo-preview.html", ".hero-headline{display:flex;align-items:flex-end;"),
        ):
            with self.subTest(page=filename):
                self.assertIn(selector, (ROOT / filename).read_text(encoding="utf-8"))

    def test_primary_dashboard_cards_share_odolo_hero_value_typography(self):
        expected_value_type = "font-family:var(--mono);font-size:clamp(44px,6vw,62px);font-weight:700;letter-spacing:-2.5px"
        for filename, selector in (
            ("dolo-preview.html", ".hero-price{"),
            ("odolo-preview.html", ".hero-price{"),
            ("tvl-preview.html", ".hero-value{"),
            ("rewards-preview.html", ".hero-price{"),
            ("revenue-preview.html", ".hero-value{"),
            ("portfolio-preview.html", ".pf-sum-total-val{"),
            ("vedolo-preview.html", ".hero-value{"),
        ):
            with self.subTest(page=filename):
                source = (ROOT / filename).read_text(encoding="utf-8")
                self.assertIn(expected_value_type, self.css_rule(source, selector))

    def test_primary_dashboard_cards_share_odolo_value_chip_typography(self):
        expected_chip_type = "font-family:var(--mono);font-weight:600;font-size:14px;letter-spacing:.2px"
        expected_label_type = "font-size:10px;letter-spacing:1.2px;text-transform:uppercase;opacity:.75;margin-left:2px"
        selectors = {
            "dolo-preview.html": ".hero-chg{",
            "odolo-preview.html": ".hero-chg{",
            "tvl-preview.html": ".hero-chg{",
            "rewards-preview.html": ".hero-chg{",
            "revenue-preview.html": ".hero-chg{",
            "portfolio-preview.html": ".pf-sum-usd{",
            "vedolo-preview.html": ".hero-chip{",
        }
        for filename, selector in selectors.items():
            with self.subTest(page=filename):
                source = (ROOT / filename).read_text(encoding="utf-8")
                self.assertIn(expected_chip_type, self.css_rule(source, selector))
                label_selector = {
                    "portfolio-preview.html": ".pf-sum-usd .lbl{",
                    "vedolo-preview.html": ".hero-chip .lbl{",
                }.get(filename, ".hero-chg .lbl{")
                self.assertIn(expected_label_type, self.css_rule(source, label_selector))

    def test_primary_dashboard_value_chips_share_odolo_gold_treatment(self):
        selectors = {
            "dolo-preview.html": ".hero-chg.hero-value-chip{",
            "odolo-preview.html": ".hero-chg.gold{",
            "tvl-preview.html": ".hero-chg.hero-value-chip{",
            "portfolio-preview.html": ".pf-sum-usd{",
            "rewards-preview.html": ".hero-chg.hero-value-chip{",
            "revenue-preview.html": ".hero-chg.hero-value-chip{",
            "vedolo-preview.html": ".hero-chip{",
        }
        for filename, selector in selectors.items():
            with self.subTest(page=filename):
                source = (ROOT / filename).read_text(encoding="utf-8")
                rule = self.css_rule(source, selector)
                self.assertIn("color:var(--gold-hi)", rule)
                self.assertIn("background:var(--gold-wash)", rule)
                self.assertIn("var(--gold-line)", rule)

    def test_portfolio_unit_matches_the_odolo_main_value_scale(self):
        source = (ROOT / "portfolio-preview.html").read_text(encoding="utf-8")
        self.assertIn(".pf-sum-total-val .unit{font-size:.42em;font-weight:500;color:var(--fg-3);letter-spacing:-.5px;margin-left:8px", source)

    def test_vedolo_unit_matches_the_odolo_main_value_scale(self):
        source = (ROOT / "vedolo-preview.html").read_text(encoding="utf-8")
        self.assertIn(".hero-value .hero-unit{font-size:.42em;font-weight:500;color:var(--fg-3);letter-spacing:-.5px;margin-left:8px", source)

    def test_rewards_and_revenue_use_the_shared_count_up_metric(self):
        for filename, setter in (
            ("rewards-preview.html", "function setMetricText(id, value)"),
            ("revenue-preview.html", "function setMetricText(id, value)"),
        ):
            source = (ROOT / filename).read_text(encoding="utf-8")
            with self.subTest(page=filename):
                self.assertIn('src="count-up.js?v=20260821-smooth-resume-v1"', source)
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
            "vedolo/index.html",
        ):
            with self.subTest(route=filename):
                self.assertIn("hero-value-chip-20260718-typography", (ROOT / filename).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
