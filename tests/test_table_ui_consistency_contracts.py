import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class TableUiConsistencyContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.assets = (ROOT / "assets-preview.html").read_text(encoding="utf-8")
        cls.portfolio = (ROOT / "portfolio-preview.html").read_text(encoding="utf-8")
        cls.dolo = (ROOT / "dolo-preview.html").read_text(encoding="utf-8")
        cls.odolo = (ROOT / "odolo-preview.html").read_text(encoding="utf-8")
        cls.vedolo = (ROOT / "vedolo-preview.html").read_text(encoding="utf-8")
        cls.revenue = (ROOT / "revenue-preview.html").read_text(encoding="utf-8")
        cls.liquidation = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
        cls.history_css = (ROOT / "history" / "history.css").read_text(encoding="utf-8")
        cls.history_js = (ROOT / "history" / "history.js").read_text(encoding="utf-8")
        cls.supply_css = (ROOT / "supply" / "supply-draft.css").read_text(encoding="utf-8")

    def test_decorative_brand_glow_cannot_expand_the_page_viewport(self):
        for source in (self.assets, self.portfolio):
            self.assertIn("html{overflow-x:clip}", source)

    def test_assets_scroll_viewport_clips_hover_highlight_to_rounded_shell(self):
        self.assertIn(
            ".assets-table-wrap{clip-path:inset(0 round 0 0 var(--r-xl) var(--r-xl))}",
            self.assets,
        )

    def test_history_scope_tooltip_is_clamped_to_the_viewport(self):
        self.assertIn("--history-scope-shift", self.history_css)
        self.assertIn("function clampHistoryScopeTooltip()", self.history_js)
        self.assertIn("clampHistoryScopeTooltip();", self.history_js)

    def test_mobile_expanded_details_reset_the_parent_table_scroll(self):
        self.assertIn("function resetExpandedTableScroll", self.odolo)
        self.assertIn("resetExpandedTableScroll(\"#tbl-ex\")", self.odolo)
        self.assertIn("function resetExpandedTableScroll", self.vedolo)
        self.assertIn("resetExpandedTableScroll(\"#holders-table\")", self.vedolo)

    def test_empty_search_states_keep_their_table_height(self):
        self.assertIn("function stableTableSpacerRowsHtml", self.odolo)
        self.assertIn("stableTableSpacerRowsHtml(cbState.pageSize - pageRows.length, 7)", self.odolo)
        self.assertIn("stableTableSpacerRowsHtml(state.latestPageSize - 1, 6)", self.odolo)
        self.assertIn("stableTableSpacerRowsHtml(state.pairPageSize - 1, 4)", self.odolo)
        self.assertIn(
            "state.freshPageSize - Math.max(pageRows.length, 1)",
            self.dolo,
        )
        self.assertIn("SUPPLY_ACTIVITY_PAGE_SIZE - 1", self.liquidation)
        self.assertIn(
            "veBorrowWalletPageSize - Math.max(pageRows.length, 1)",
            self.revenue,
        )

    def test_table_freshness_uses_the_shared_information_pattern(self):
        self.assertIn('"Data updated · " + ago(liveFlowTimestamp)', self.dolo)
        self.assertIn("Data updated · ${dataAgeLabel(revenueData?.generatedAt)}", self.revenue)

    def test_pagers_share_the_primary_table_dimensions(self):
        self.assertIn("width:30px;height:30px", self.history_css)
        self.assertIn("border-radius:6px", self.history_css)
        self.assertIn("gap:4px", self.history_css)
        self.assertIn("width: 30px !important;", self.supply_css)
        self.assertIn("border-radius: 6px !important;", self.supply_css)
        self.assertIn("const PAGER_ICON_FIRST", self.history_js)
        self.assertIn("buildSupplyTableFooter(", (ROOT / "supply" / "supply-draft.js").read_text(encoding="utf-8"))
        self.assertNotIn(">«</button>", self.history_js)
        self.assertNotIn(">«</button>", (ROOT / "supply" / "supply-draft.js").read_text(encoding="utf-8"))

    def test_clear_controls_have_accessible_names(self):
        clear_ids = (
            "search-clear",
            "hf-filter-clear",
            "collateral-filter-clear",
            "debt-filter-clear",
            "liquidation-history-search-clear",
            "lh-coll-filter-clear",
            "lh-debt-filter-clear",
            "supply-search-clear",
            "supply-activity-search-clear",
            "supply-activity-time-filter-clear",
            "supply-activity-amount-filter-clear",
            "supply-activity-usd-filter-clear",
        )
        for clear_id in clear_ids:
            self.assertRegex(
                self.liquidation,
                rf'<button[^>]*id="{clear_id}"[^>]*aria-label="Clear [^"]+"',
            )

    def test_visible_counts_and_dates_do_not_inherit_the_browser_locale(self):
        primary_sources = (
            self.assets,
            self.dolo,
            self.odolo,
            self.vedolo,
            self.liquidation,
            self.history_js,
            (ROOT / "dashboard-core.js").read_text(encoding="utf-8"),
        )
        for source in primary_sources:
            self.assertNotIn("toLocaleString()", source)
            self.assertIn('toLocaleString("en-US")', source)


if __name__ == "__main__":
    unittest.main()
