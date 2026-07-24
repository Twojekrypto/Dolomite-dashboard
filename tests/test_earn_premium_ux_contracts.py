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
        for rule in (
            "height: 36px",
            "padding: 3px",
            "border-radius: 10px",
            "width: 52px",
            "height: 28px",
            "display: inline-flex",
            "align-items: center",
            "padding: 0 14px",
            "line-height: 18px",
            "transform: translateX(52px)",
            "background: var(--earn-gold",
        ):
            self.assertIn(rule, switch_css)
        self.assertNotIn("min-width: 48px", switch_css)
        self.assertIn(
            "#earn-supply-section #earn-apr-pill .apr-pill-opt.active",
            switch_css,
        )

    def test_dedicated_earn_route_keeps_assets_switch_geometry(self):
        start = self.draft_css.index(
            "body.earn-draft-route #earn-supply-section #earn-apr-pill {"
        )
        end = self.draft_css.index(
            "body.earn-draft-route .earn-error",
            start,
        )
        switch_css = self.draft_css[start:end]
        for rule in (
            "height: 36px !important",
            "padding: 3px !important",
            "width: 52px !important",
            "height: 28px !important",
            "display: inline-flex !important",
            "align-items: center !important",
            "padding: 0 14px !important",
            "line-height: 18px !important",
            "transform: translateX(52px) !important",
        ):
            self.assertIn(rule, switch_css)

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

    def test_portfolio_value_uses_chips_without_duplicate_counter_sentence(self):
        summary_scope = self.js.index("const summaryRunId =")
        summary_start = self.js.index("summaryEl.innerHTML = `", summary_scope)
        summary_end = self.js.index(
            "summaryEl.classList.add('visible')",
            summary_start,
        )
        summary_js = self.js[summary_start:summary_end]
        debt_start = self.js.index("function earn_updateSummaryDebt(positions)")
        debt_end = self.js.index(
            "function earn_renderWithdrawnAssets",
            debt_start,
        )
        debt_js = self.js[debt_start:debt_end]

        self.assertNotIn("earn-summary-portfolio-sub", summary_js)
        self.assertNotIn("portfolioSub", summary_js)
        self.assertNotIn("earn-summary-portfolio-sub", debt_js)
        self.assertNotIn("portfolioSubEl", debt_js)
        self.assertIn('id="earn-summary-supply-chip"', summary_js)
        self.assertIn('id="earn-summary-borrow-chip"', summary_js)
        self.assertIn("summaryVerificationChip", summary_js)

    def test_shared_summary_uses_one_institutional_ledger_surface(self):
        start = self.css.index("/* Institutional Ledger */")
        end = self.css.index("/* ═══════ Filter Bar", start)
        ledger_css = self.css[start:end]
        for rule in (
            "grid-template-columns: minmax(0, 1.3fr) minmax(360px, 0.7fr)",
            "gap: 0",
            ".earn-summary-main::after",
            "width: 1px",
            "#c9a227",
            ".earn-summary-metrics .earn-summary-stat + .earn-summary-stat",
            "border-left: 1px solid",
            "background: transparent",
            "box-shadow: none",
        ):
            self.assertIn(rule, ledger_css)

    def test_dedicated_summary_uses_gold_rail_without_nested_metric_cards(self):
        start = self.draft_css.index(
            "body.earn-draft-route .earn-summary-card {"
        )
        end = self.draft_css.index(
            "body.earn-draft-route #earn-supply-section",
            start,
        )
        ledger_css = self.draft_css[start:end]
        for rule in (
            "gap: 0 !important",
            "padding: 0 !important",
            "body.earn-draft-route .earn-summary-main::after",
            "width: 1px !important",
            "background: transparent !important",
            "border-left: 1px solid var(--earn-line-2) !important",
            "box-shadow: none !important",
        ):
            self.assertIn(rule, ledger_css)

    def test_summary_rail_and_metrics_stack_without_mobile_overflow(self):
        mobile = self.draft_css[self.draft_css.index("@media (max-width: 980px)"):]
        self.assertIn("height: 1px !important", mobile)
        self.assertIn("width: auto !important", mobile)
        self.assertIn("border-top: 1px solid var(--earn-line-2) !important", mobile)
        self.assertIn("grid-template-columns: minmax(0, 1fr) !important", mobile)

    def test_dedicated_earn_route_preserves_approved_summary_hierarchy(self):
        summary_start = self.draft_css.index(
            "body.earn-draft-route .earn-summary-card {"
        )
        summary_end = self.draft_css.index(
            "body.earn-draft-route #earn-supply-section",
            summary_start,
        )
        summary_css = self.draft_css[summary_start:summary_end]
        self.assertIn(
            "grid-template-columns: minmax(0, 1.3fr) minmax(360px, .7fr) !important",
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
        version = "earn-core-20260724-portfolio-dedupe"
        builder = (ROOT / "build_earn_bundle.py").read_text(encoding="utf-8")
        route = (ROOT / "earn/index.html").read_text(encoding="utf-8")
        self.assertIn(version, builder)
        self.assertGreaterEqual(route.count(version), 2)

    def test_shared_dashboard_assets_use_premium_ux_cache_version(self):
        version = "core-split-20260724-portfolio-dedupe"
        self.assertIn(f"dashboard-core.css?v={version}", self.html)
        self.assertIn(f"dashboard-core.js?v={version}", self.html)


if __name__ == "__main__":
    unittest.main()
