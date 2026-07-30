import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO = (ROOT / "portfolio-preview.html").read_text(encoding="utf-8")
VEDOLO = (ROOT / "vedolo-preview.html").read_text(encoding="utf-8")
BORROW = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
SUPPLY_HEALTH_CSS = (ROOT / "tvl" / "supply-health.css").read_text(encoding="utf-8")
SUPPLY_SCRIPT = (ROOT / "supply" / "supply-draft.js").read_text(encoding="utf-8")
SUPPLY_CSS = (ROOT / "supply" / "supply-draft.css").read_text(encoding="utf-8")


def final_rule(source, selector):
    start = source.rindex(selector)
    end = source.index("}", start)
    return source[start:end + 1]


def first_rule(source, selector):
    start = source.index(selector)
    end = source.index("}", start)
    return source[start:end + 1]


class InstitutionalTableMarketUxContracts(unittest.TestCase):
    def test_vedolo_activity_summary_continues_the_header_surface(self):
        rule = first_rule(
            PORTFOLIO,
            "#pf-exercises-section .pf-exercise-summary.selected-market-rail",
        )
        head = first_rule(PORTFOLIO, "#pf-exercises-section .card-head")
        expected_background = (
            "background:linear-gradient(180deg,rgba(117,184,123,.045),"
            "rgba(20,20,23,.18))"
        )
        self.assertIn(expected_background, head)
        self.assertIn(expected_background, rule)

    def test_expired_vedolo_headers_and_cells_share_alignment_lanes(self):
        for selector in (
            '#claimable-table th[data-sort="dolo"] .th-content',
            '#claimable-table th[data-sort="lock"] .th-content',
            '#claimable-table th[data-sort="price"] .th-content',
            '#claimable-table th[data-sort="expired"] .th-content',
        ):
            self.assertIn(selector, VEDOLO, f"missing aligned claimable selector: {selector}")
        self.assertIn("justify-content:flex-end", VEDOLO)
        self.assertIn(
            '#claimable-table th[data-sort="id"] .th-content',
            VEDOLO,
        )
        self.assertIn("justify-content:center", VEDOLO)

    def test_liquidation_history_exposes_active_only_sort_controls(self):
        history = BORROW[
            BORROW.index('id="liquidation-history-table"'):
            BORROW.index("</table>", BORROW.index('id="liquidation-history-table"'))
        ]
        for field in ("chain", "address", "date", "collateral", "debt"):
            self.assertIn(f'data-liquidation-sort="{field}"', history)
        self.assertGreaterEqual(history.count('class="sort-arrow"'), 5)
        self.assertIn("syncLiquidationHistorySortHeaders", BORROW)
        self.assertIn("closest('button, input, select, textarea, .col-filter-popover')", BORROW)

    def test_borrow_table_surface_and_typography_match_holders(self):
        selector = (
            "body.route-liquidation .liquidation-table-surface .table-card-inner"
        )
        self.assertIn(selector, BORROW)
        surface = final_rule(BORROW, selector)
        self.assertIn("background: var(--bg-2) !important", surface)
        footer_selector = "body.route-liquidation .liquidation-table-surface .table-footer"
        self.assertIn(footer_selector, BORROW, "missing DOLO-style table footer override")
        footer = final_rule(BORROW, footer_selector)
        self.assertIn("background: var(--bg-2) !important", footer)
        positions_footer = final_rule(BORROW, "body.route-liquidation #table-footer")
        self.assertIn("background: var(--bg-2) !important", positions_footer)
        cells = final_rule(
            BORROW,
            "body.route-liquidation #liquidation-history-table tbody td",
        )
        self.assertIn("font-family: var(--mono) !important", cells)
        self.assertIn("font-size: 12px !important", cells)
        self.assertIn("padding: 12px 18px !important", cells)

    def test_supply_health_details_are_edge_to_edge_without_an_inset_card(self):
        detail_cell = first_rule(
            SUPPLY_HEALTH_CSS,
            "#supply-health-card .supply-health-table .supply-health-detail-row td",
        )
        self.assertIn("padding: 0", detail_cell)
        panel = final_rule(
            SUPPLY_HEALTH_CSS,
            "#supply-health-card .supply-health-detail-panel",
        )
        self.assertIn("margin: 0", panel)
        self.assertIn("border: 0", panel)
        self.assertIn("border-radius: 0", panel)
        self.assertIn("box-shadow: none", panel)
        self.assertNotIn(
            "#supply-health-card .supply-health-detail-panel::before",
            SUPPLY_HEALTH_CSS,
        )

    def test_supply_selector_is_a_market_directory_with_search_clear(self):
        for contract in (
            "supply-draft-market-option",
            "supply-draft-option-name",
            "supply-draft-option-market",
            "supply-draft-option-address",
            "supply-asset-search-clear",
            "Active supply markets",
        ):
            self.assertIn(contract, SUPPLY_SCRIPT)
        self.assertIn(".supply-draft-market-option", SUPPLY_CSS)
        self.assertNotIn("shell.classList.add('supply-asset-search-shell', 'no-clear')", SUPPLY_SCRIPT)

    def test_supply_selection_updates_all_market_identity_icons(self):
        start = SUPPLY_SCRIPT.index("function setSelectorUi")
        end = SUPPLY_SCRIPT.index("function syncEmptyState", start)
        selector_ui = SUPPLY_SCRIPT[start:end]
        for icon_id in (
            "selected-asset-icon",
            "supply-intel-asset-icon",
            "supply-header-icon",
        ):
            self.assertIn(icon_id, selector_ui)


if __name__ == "__main__":
    unittest.main()
