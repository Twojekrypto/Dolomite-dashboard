import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EarnPremiumUxContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.css = (ROOT / "dashboard-core.css").read_text(encoding="utf-8")
        cls.js = (ROOT / "dashboard-core.js").read_text(encoding="utf-8")
        cls.html = (ROOT / "dashboard-core.html").read_text(encoding="utf-8")
        cls.draft_css = (ROOT / "earn/earn-draft.css").read_text(encoding="utf-8")

    def test_borrow_rows_do_not_use_index_based_background_tint(self):
        self.assertNotIn(
            ".earn-lending-table tbody tr.earn-lend-row:nth-child(4n+1) td",
            self.css,
        )

    def test_quality_markers_are_focusable_signal_pills(self):
        self.assertIn(
            'class="earn-quality-marker ${marker.cls}" tabindex="0"',
            self.js,
        )
        quality_start = self.css.index(".earn-quality-marker {")
        quality_end = self.css.index(".earn-quality-empty {", quality_start)
        quality_css = self.css[quality_start:quality_end]
        self.assertIn("min-height: 24px", quality_css)
        self.assertIn("border-radius: 999px", quality_css)
        self.assertIn("text-transform: uppercase", quality_css)
        self.assertIn("cursor: default", quality_css)
        self.assertIn(".earn-quality-marker.verified", quality_css)
        self.assertIn("background:", quality_css)
        self.assertIn("border:", quality_css)

    def test_earn_apr_toggle_uses_assets_gold_switch_geometry(self):
        self.assertIn("#earn-supply-section #earn-apr-pill {", self.css)
        start = self.css.index("#earn-supply-section #earn-apr-pill {")
        end = self.css.index(".earn-asset-table {", start)
        switch_css = self.css[start:end]
        self.assertIn("height: 36px", switch_css)
        self.assertIn("border-radius: 10px", switch_css)
        self.assertIn("background: var(--earn-gold", switch_css)
        self.assertIn("height: 28px", switch_css)
        self.assertIn("border-radius: 8px", switch_css)
        self.assertIn(
            "#earn-supply-section #earn-apr-pill .apr-pill-opt.active",
            switch_css,
        )

    def test_dedicated_earn_route_keeps_the_gold_toggle_active_state(self):
        self.assertIn(
            "body.earn-draft-route #earn-supply-section #earn-apr-pill {",
            self.draft_css,
        )
        self.assertIn(
            "border-radius: 10px !important",
            self.draft_css,
        )
        self.assertIn(
            "background: var(--earn-gold) !important",
            self.draft_css,
        )
        self.assertIn(
            "color: var(--earn-bg-0) !important",
            self.draft_css,
        )

    def test_supply_rate_lines_have_source_aware_unified_tooltips(self):
        self.assertIn(
            "function earn_getSupplyAprSourceTip(sourceKey, symbol, rateData)",
            self.js,
        )
        self.assertIn("rateData?.yieldSources", self.js)
        self.assertIn("World Liberty Financial", self.js)
        self.assertIn("Merkl campaign", self.js)
        self.assertIn("Weekly oDOLO emissions", self.js)
        self.assertIn('data-tip="${earn_escapeHtml(part.tip)}"', self.js)
        self.assertIn('tabindex="0"', self.js)

    def test_unified_tooltip_supports_keyboard_focus(self):
        tooltip_start = self.js.index(
            "// ═══ UNIFIED PREMIUM TOOLTIP SYSTEM ═══"
        )
        tooltip_end = self.js.index(
            "// ===== TREASURY NAMESPACE =====",
            tooltip_start,
        )
        tooltip_js = self.js[tooltip_start:tooltip_end]
        self.assertIn("document.addEventListener('focusin'", tooltip_js)
        self.assertIn("document.addEventListener('focusout'", tooltip_js)

    def test_summary_contains_only_approved_metric_groups(self):
        summary_scope = self.js.index("const summaryRunId =")
        summary_start = self.js.index("summaryEl.innerHTML = `", summary_scope)
        summary_end = self.js.index(
            "summaryEl.classList.add('visible')",
            summary_start,
        )
        summary_js = self.js[summary_start:summary_end]
        for label in ("Portfolio Value", "Total Yield Earned", "Rewards"):
            self.assertIn(label, summary_js)
        for removed in (
            "Historical Yield P&amp;L",
            "Current Markets Check",
            "Total Debt",
            "Open Borrow Route P&amp;L",
            "Best Performer",
            "Borrow Route Yield",
        ):
            self.assertNotIn(removed, summary_js)
        self.assertIn('<div class="earn-summary-metrics">', summary_js)
        self.assertIn(
            "grid-template-columns: repeat(2, minmax(0, 1fr))",
            self.css,
        )

    def test_dedicated_earn_route_preserves_approved_summary_hierarchy(self):
        summary_start = self.draft_css.index(
            "body.earn-draft-route .earn-summary-card {"
        )
        summary_end = self.draft_css.index(
            "body.earn-draft-route .earn-summary-stat {",
            summary_start,
        )
        summary_css = self.draft_css[summary_start:summary_end]
        self.assertIn(
            "grid-template-columns: minmax(0, 1.2fr) minmax(360px, .8fr) !important",
            summary_css,
        )
        self.assertIn(
            "body.earn-draft-route .earn-summary-metrics {",
            summary_css,
        )
        self.assertIn(
            "grid-template-columns: repeat(2, minmax(0, 1fr)) !important",
            summary_css,
        )
        self.assertNotIn(".earn-summary-side", summary_css)
        self.assertNotIn(".earn-summary-primary-grid", summary_css)
        self.assertNotIn(".earn-summary-secondary-grid", summary_css)

    def test_dedicated_earn_bundle_uses_premium_ux_cache_version(self):
        version = "earn-core-20260723-premium-ux-2"
        builder = (ROOT / "build_earn_bundle.py").read_text(encoding="utf-8")
        route = (ROOT / "earn/index.html").read_text(encoding="utf-8")
        self.assertIn(version, builder)
        self.assertGreaterEqual(route.count(version), 2)

    def test_shared_dashboard_assets_use_premium_ux_cache_version(self):
        version = "core-split-20260723-earn-premium-ux"
        self.assertIn(f"dashboard-core.css?v={version}", self.html)
        self.assertIn(f"dashboard-core.js?v={version}", self.html)


if __name__ == "__main__":
    unittest.main()
