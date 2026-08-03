import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def source(path):
    return (ROOT / path).read_text(encoding="utf-8")


class TableAuditRemediationContractsTest(unittest.TestCase):
    def test_earn_mobile_clears_desktop_export_and_responds_to_resize(self):
        layout = source("earn/earn-static-layout.js")
        css = source("earn/earn-draft.css")
        self.assertIn("MOBILE_BREAKPOINT = 560", layout)
        self.assertIn("function clearDesktopLayout", layout)
        self.assertIn("function applyMobileLayout", layout)
        self.assertIn("table.classList.remove('earn-static-layout')", layout)
        self.assertIn("root.addEventListener('resize', schedule", layout)
        self.assertIn("max-width: 560px", css)
        self.assertIn("min-width: 760px", css)
        for path in ("earn/earn-core.html",):
            self.assertIn("earn-static-layout-20260803-mobile", source(path))

    def test_table_snapshot_metadata_uses_source_specific_language(self):
        assets = source("assets-preview.html")
        dolo = source("dolo-preview.html")
        rewards = source("rewards-preview.html")
        revenue = source("revenue-preview.html")
        history = source("history/index.html") + source("history/history.js")
        earn = source("earn/earn-core.html") + source("earn/earn-static-layout.js")
        self.assertIn("Data updated ·", assets)
        self.assertNotIn("Official data · updated", assets)
        self.assertIn('id="fresh-wallets-meta"', dolo)
        self.assertNotIn('metaEl.textContent = "First on-chain tx + 10K+ exposure"', dolo)
        self.assertIn('id="rwPastMeta"', rewards)
        self.assertIn('id="chainDataUpdated"', revenue)
        self.assertIn("Report generated ·", history)
        self.assertIn("Verified at ·", earn)

    def test_vedolo_pagers_use_shared_svg_accessible_contract(self):
        html = source("vedolo-preview.html")
        self.assertIn("PAGER_ICON_FIRST", html)
        self.assertIn('aria-label="First page"', html)
        self.assertIn("formatTableRange", html)
        self.assertNotIn('>«</button>', html)
        self.assertNotIn('>»</button>', html)
        self.assertRegex(html, r"\.pager button\{[^}]*min-width:30px[^}]*height:30px")

    def test_history_pager_has_left_range_and_30px_svg_buttons(self):
        js = source("history/history.js")
        css = source("history/history.css")
        self.assertIn("history-range", js)
        self.assertIn("PAGER_ICON_FIRST", js)
        self.assertIn("1fr auto 1fr", css)
        self.assertRegex(css, r"\.flow-pager-btn\{[^}]*width:30px[^}]*height:30px")

    def test_revenue_chain_table_uses_institutional_card_shell(self):
        html = source("revenue-preview.html")
        self.assertRegex(html, r"\.table-panel\{[^}]*border-radius:22px")
        self.assertRegex(html, r"\.table-panel .*\.panel-title\{[^}]*font-size:16px[^}]*font-weight:600")
        self.assertIn('id="chainDataUpdated"', html)

    def test_borrow_tables_use_ranges_and_tighter_primary_rows(self):
        html = source("liquidation-preview.html")
        self.assertIn("function formatBorrowTableRange", html)
        self.assertIn("--borrow-table-row-height: 74px", html)
        self.assertIn("--borrow-table-header-height: 42px", html)
        self.assertRegex(
            html,
            r"#positions-table thead th,[^}]+padding: 0 18px !important",
        )
        self.assertRegex(
            html,
            r"#positions-table tbody td \{[^}]*padding: 6px 18px !important",
        )
        self.assertRegex(
            html,
            r"#positions-table tbody td,\s*body\.route-liquidation #liquidation-history-table tbody td \{[^}]*font-weight: 500",
        )

    def test_cex_summary_workflow_reads_nested_report_schema(self):
        workflow = source(".github/workflows/audit-dolo-cex-labels.yml")
        self.assertIn('api = full.get("api") or {}', workflow)
        self.assertIn('"apiStatus": api.get("status")', workflow)
        self.assertIn('api.get("confirmedCexSuggestions", [])[:20]', workflow)


if __name__ == "__main__":
    unittest.main()
