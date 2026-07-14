import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FOOTER_STYLES = ROOT / "protocol-footer.css"
ROUTE_LOADER = ROOT / "route-loader.js"
ROUTE_ENTRIES = (
    ROOT / "index.html",
    *(ROOT / route / "index.html" for route in (
        "assets", "borrow", "dolo", "earn", "liquidation", "odolo", "portfolio",
        "revenue", "rewards", "supply", "tvl", "vedolo",
    )),
)


class TableSurfaceContractsTest(unittest.TestCase):
    def test_shared_styles_remove_only_table_shell_separators(self):
        styles = FOOTER_STYLES.read_text(encoding="utf-8")

        for selector in (
            "body .card > .card-head",
            "body .card > .toolbar",
            "body .panel > .panel-head",
            "body .table-card-outer .table-card-inner > .table-card-header",
            "body .history-table-toolbar",
            "body .pf-filters",
            "body .veborrow-wallet-card-head",
            "body .veborrow-wallet-toolbar",
            "body .supply-history-head",
            "body .supply-leaderboard-toolbar",
            "body.supply-draft-route .supply-intel-header",
            "body.supply-draft-route .supply-table-header",
            "body.supply-draft-route .supply-activity-header",
            "body.supply-draft-route .supply-activity-toolbar",
            "body.supply-draft-route .supply-health-header",
            "body.route-liquidation .liquidation-history-card .table-card-header",
            "body.route-liquidation .controls-row1",
            "body.earn-draft-route .earn-supply-header",
        ):
            self.assertIn(selector, styles)
        self.assertIn("border-bottom: 0 !important;", styles)
        self.assertIn("Continuous table surfaces", styles)

    def test_every_route_loads_the_separator_free_footer_version(self):
        version = "protocol-footer-20260714-table-surfaces"

        self.assertIn(version, ROUTE_LOADER.read_text(encoding="utf-8"))
        for entry in ROUTE_ENTRIES:
            self.assertIn("route-loader-20260714-table-surfaces", entry.read_text(encoding="utf-8"))
        history = (ROOT / "history" / "index.html").read_text(encoding="utf-8")
        self.assertIn(version, history)


if __name__ == "__main__":
    unittest.main()
