import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
POSITION_HISTORY = json.loads(
    (ROOT / "data" / "liquidation-risk" / "position-count-history.json").read_text(encoding="utf-8")
)


class TestBorrowHeroUx(unittest.TestCase):
    def test_hero_has_source_backed_24h_position_change(self):
        for contract in (
            'id="stat-total-row"',
            'id="stat-total-change"',
            'id="stat-total-change-value"',
            'id="stat-total-change-unit"',
            'id="stat-total-change-percent"',
            'class="stat-total-change-copy"',
            "data/liquidation-risk/position-count-history.json",
            "renderPositionCount24h",
            "baselineCount",
            ".toFixed(2)",
            "% · 24h",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

    def test_24h_change_uses_a_two_line_count_and_percentage_badge(self):
        self.assertIn(
            "body.route-liquidation .stat-total-change-copy {\n"
            "            display: flex !important;\n"
            "            flex-direction: column !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation .stat-total-change-primary {\n"
            "            display: inline-flex !important;\n"
            "            align-items: baseline !important;",
            SOURCE,
        )
        self.assertNotIn('class="stat-total-change-label">24h change', SOURCE)

    def test_committed_position_history_is_internally_consistent(self):
        snapshots = POSITION_HISTORY["snapshots"]
        change = POSITION_HISTORY["change24h"]

        self.assertGreaterEqual(len(snapshots), 2)
        self.assertEqual(POSITION_HISTORY["generatedAt"], snapshots[-1]["timestamp"])
        self.assertEqual(change["currentCount"], snapshots[-1]["count"])
        self.assertEqual(
            change["change"],
            change["currentCount"] - change["baselineCount"],
        )

    def test_summary_value_colors_follow_requested_risk_hierarchy(self):
        self.assertIn(
            "body.route-liquidation .liquidation-stat-card.collateral .liquidation-stat-value {\n"
            "            color: var(--up) !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation .liquidation-stat-card.debt .liquidation-stat-value {\n"
            "            color: var(--down) !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation .liquidation-stat-card.risk .liquidation-stat-value,\n"
            "        body.route-liquidation .liquidation-stat-card.critical .liquidation-stat-value {\n"
            "            color: #e15f5f !important;",
            SOURCE,
        )


class TestBorrowTableHeaderUx(unittest.TestCase):
    def test_redundant_kickers_and_old_metadata_are_removed(self):
        self.assertNotIn("Position Monitor", SOURCE)
        self.assertNotIn("Liquidation Log", SOURCE)
        self.assertNotIn("Live ranking", SOURCE)
        self.assertNotIn("Debt repaid:", SOURCE)

    def test_table_titles_and_counts_share_an_aligned_row(self):
        self.assertGreaterEqual(SOURCE.count('class="liquidation-table-title-row"'), 2)
        self.assertIn(
            "body.route-liquidation .liquidation-table-title-row {\n"
            "            display: inline-flex !important;\n"
            "            align-items: baseline !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation .liquidation-table-title-row .header-count {\n"
            "            font-variant-numeric: tabular-nums !important;",
            SOURCE,
        )

    def test_both_tables_show_the_shared_relative_data_time(self):
        for contract in (
            'id="positions-data-updated"',
            'id="liquidation-history-data-updated"',
            "Data updated ·",
            "updateBorrowFreshnessLabels",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

    def test_all_freshness_labels_match_dolo_holders_ux(self):
        self.assertGreaterEqual(SOURCE.count("borrow-data-meta"), 3)
        self.assertGreaterEqual(SOURCE.count('class="borrow-data-pulse"'), 3)
        self.assertIn(
            "body.route-liquidation .borrow-data-meta {\n"
            "            font-size: 11px !important;\n"
            "            color: var(--fg-3) !important;\n"
            "            font-family: var(--mono) !important;\n"
            "            display: inline-flex !important;\n"
            "            align-items: center !important;\n"
            "            gap: 6px !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation .borrow-data-pulse {\n"
            "            width: 6px !important;\n"
            "            height: 6px !important;\n"
            "            border-radius: 50% !important;\n"
            "            background: var(--gold) !important;\n"
            "            box-shadow: 0 0 8px var(--gold) !important;",
            SOURCE,
        )
        self.assertNotIn("el.style.color = color", SOURCE)
        self.assertNotIn("dot.style.background = color", SOURCE)
        self.assertNotIn("<span>Updated</span>", SOURCE)


class TestBorrowSimulatorAddressUx(unittest.TestCase):
    def test_wallet_label_and_address_are_never_underlined(self):
        self.assertIn(
            "body.route-liquidation #sim-card .sim-atrisk-table .known-address-label,\n"
            "        body.route-liquidation #sim-card .sim-atrisk-table .known-address-sub a.addr-tooltip-wrap {\n"
            "            text-decoration: none !important;",
            SOURCE,
        )

    def test_chain_address_gap_grows_without_moving_money_columns(self):
        self.assertIn(
            "colgroup col:nth-child(1) { width: 9.2% !important; }",
            SOURCE,
        )
        self.assertIn(
            "colgroup col:nth-child(4) { width: 26.6% !important; }",
            SOURCE,
        )
        self.assertIn(
            "thead th:nth-child(2),\n"
            "        body.route-liquidation #sim-card .positions-table.sim-atrisk-table tbody td:nth-child(2) {\n"
            "            padding-left: 18px !important;",
            SOURCE,
        )
        self.assertIn(
            "colgroup col:nth-child(5) { width: 20% !important; }",
            SOURCE,
        )
        self.assertIn(
            "colgroup col:nth-child(6) { width: 16.3% !important; }",
            SOURCE,
        )


if __name__ == "__main__":
    unittest.main()
