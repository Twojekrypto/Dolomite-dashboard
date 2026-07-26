import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
POSITION_HISTORY = json.loads(
    (ROOT / "data" / "liquidation-risk" / "position-count-history.json").read_text(encoding="utf-8")
)
LIQUIDATION_TABLE_SOURCE = SOURCE[
    SOURCE.index('<table class="liquidation-history-table" id="liquidation-history-table"'):
    SOURCE.index("</table>", SOURCE.index('<table class="liquidation-history-table" id="liquidation-history-table"'))
]
LIQUIDATION_RENDER_SOURCE = SOURCE[
    SOURCE.index("function renderLiquidationHistoryTable()"):
    SOURCE.index("function liquidationHistoryGoPage", SOURCE.index("function renderLiquidationHistoryTable()"))
]


class TestBorrowHeroUx(unittest.TestCase):
    def test_position_change_provenance_uses_a_nameable_live_status(self):
        self.assertRegex(
            SOURCE,
            re.compile(
                r'<div id="stat-total-change" class="stat-total-change unavailable" '
                r'role="status" aria-label="Position change history unavailable">'
            ),
        )

    def test_hero_has_source_backed_truthful_position_change(self):
        for contract in (
            'id="stat-total-row"',
            'id="stat-total-change"',
            'id="stat-total-change-percent"',
            "data/liquidation-risk/position-count-history.json",
            "renderPositionCount24h",
            "fallbackChange",
            "windowSeconds",
            "formatPositionCountWindow",
            "24-hour baseline is still building",
            ".toFixed(2)",
            "const windowLabel",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

    def test_change_chip_contains_only_percentage_and_real_window(self):
        for obsolete in (
            'id="stat-total-change-value"',
            'id="stat-total-change-unit"',
            'class="stat-total-change-copy"',
            'class="stat-total-change-primary"',
        ):
            with self.subTest(obsolete=obsolete):
                self.assertNotIn(obsolete, SOURCE)
        self.assertIn(
            '<span id="stat-total-change-percent" class="stat-total-change-label">— · awaiting history</span>',
            SOURCE,
        )
        self.assertIn("percent.textContent = '— · awaiting history';", SOURCE)

    def test_change_chip_uses_compact_dolo_style(self):
        self.assertIn(
            "body.route-liquidation .stat-total-change {\n"
            "            display: inline-flex !important;\n"
            "            align-items: center !important;\n"
            "            gap: 6px !important;\n"
            "            min-height: 30px !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation .stat-total-change-label {\n"
            "            color: currentColor !important;\n"
            "            opacity: 1 !important;",
            SOURCE,
        )

    def test_committed_position_history_is_internally_consistent(self):
        snapshots = POSITION_HISTORY["snapshots"]
        exact_change = POSITION_HISTORY["change24h"]
        fallback_change = POSITION_HISTORY["fallbackChange"]

        self.assertGreaterEqual(len(snapshots), 2)
        self.assertEqual(POSITION_HISTORY["generatedAt"], snapshots[-1]["timestamp"])
        change = exact_change or fallback_change
        self.assertIsNotNone(change)
        self.assertEqual(change["currentCount"], snapshots[-1]["count"])
        self.assertEqual(
            change["change"],
            change["currentCount"] - change["baselineCount"],
        )
        if exact_change is None:
            self.assertEqual(
                fallback_change["windowSeconds"],
                POSITION_HISTORY["generatedAt"] - fallback_change["baselineAt"],
            )
            self.assertGreater(fallback_change["windowSeconds"], 0)

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
    def test_at_risk_table_exposes_dolo_style_sort_controls_for_risk_metrics(self):
        for contract in (
            'data-sim-risk-sort="hfAfter"',
            'data-sim-risk-sort="collateral"',
            'data-sim-risk-sort="debt"',
            'class="sim-atrisk-sort"',
            'class="sort" aria-hidden="true"',
            'aria-sort="ascending"',
            'body.route-liquidation #sim-card .sim-atrisk-sort {',
            'padding: 0 !important;\n            border: 0 !important;\n            background: transparent !important;\n            appearance: none !important;',
            'body.route-liquidation #sim-card .sim-atrisk-sort .sort {',
            'body.route-liquidation #sim-card .sim-atrisk-sortable[aria-sort="ascending"] .sort,',
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

    def test_at_risk_metric_sorting_reorders_full_result_set_and_resets_pagination(self):
        for contract in (
            'window.simRiskSort = function(sortKey) {',
            "const initialDirection = sortKey === 'hfAfter' ? 'asc' : 'desc';",
            'window._simRiskPage = 1;',
            "const sortKey = window._simRiskSortKey || 'hfAfter';",
            "const sortDirection = window._simRiskSortDirection || 'asc';",
            'const sortValues = {',
            'hfAfter: row => Number(row.newHF),',
            'collateral: row => Number(row.collateralUSD),',
            'debt: row => Number(row.debtUSD),',
            'return sortDirection === \'asc\' ? comparison : -comparison;',
            'renderSimRiskSortHeaders(sortKey, sortDirection);',
            "return sortDirection === 'asc' ? 'smallest collateral' : 'largest collateral';",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

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


class TestBorrowLiquidationHistoryUx(unittest.TestCase):
    def test_history_uses_five_real_columns_without_a_spacer(self):
        self.assertEqual(LIQUIDATION_TABLE_SOURCE.count("<col>"), 5)
        self.assertNotIn("col-spacer", LIQUIDATION_TABLE_SOURCE)
        self.assertNotIn("col-spacer", LIQUIDATION_RENDER_SOURCE)
        self.assertIn('colspan="5"', LIQUIDATION_RENDER_SOURCE)

    def test_history_amount_selectors_follow_the_five_column_layout(self):
        self.assertIn(
            "#liquidation-history-table colgroup col:nth-child(4) { width: 18.2% !important; }",
            SOURCE,
        )
        self.assertIn(
            "#liquidation-history-table colgroup col:nth-child(5) { width: 16% !important; }",
            SOURCE,
        )
        self.assertNotRegex(
            SOURCE,
            re.compile(r"(?:#|\.)liquidation-history-table[^\n]*nth-child\(6\)"),
        )
        self.assertNotIn("transform: translateX(-12px)", SOURCE)


class TestBorrowInstitutionalLiveImpactUx(unittest.TestCase):
    def test_live_simulation_clamps_every_row_value_before_consuming_it(self):
        run_source = SOURCE[
            SOURCE.index("function runMultiAssetSim()"):
            SOURCE.index("function updateBorrowFreshnessLabels", SOURCE.index("function runMultiAssetSim()"))
        ]
        self.assertIn(
            "const pctVal = clampMultiAssetPct("
            "row.querySelector('input[type=\"number\"]').value"
            ");",
            run_source,
        )
        self.assertIn(
            "const pct = clampMultiAssetPct("
            "row.querySelector('input[type=\"number\"]').value"
            ");",
            run_source,
        )
        self.assertNotIn(
            "parseFloat(row.querySelector('input[type=\"number\"]').value)",
            run_source,
        )

    def test_simulator_explains_the_causal_flow_without_redundant_status_copy(self):
        for contract in (
            "Build Scenario",
            "Price shock",
            "Scenario Result",
            'id="sim-impact-headline"',
            "positions cross HF 1.0",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)
        for obsolete in (
            'id="sim-impact-state"',
            'id="sim-risk-level"',
            'class="sim-impact-risk-row"',
            "Scenario active · Updated live as you edit",
            "Adjust a token to simulate impact",
            "Risk level",
            "High impact",
        ):
            with self.subTest(obsolete=obsolete):
                self.assertNotIn(obsolete, SOURCE)

    def test_scenario_rows_use_one_manual_input_without_presets(self):
        for obsolete in (
            'class="sim-multi-presets"',
            'class="sim-multi-preset"',
            'data-pct="-5"',
            'data-pct="-10"',
            'data-pct="-25"',
            "applyMultiAssetPreset",
        ):
            with self.subTest(obsolete=obsolete):
                self.assertNotIn(obsolete, SOURCE)
        self.assertIn('data-step="-1"', SOURCE)
        self.assertIn('data-step="1"', SOURCE)
        self.assertIn('step="any"', SOURCE)
        self.assertIn('class="sim-multi-shock-rail"', SOURCE)
        self.assertIn("function clampMultiAssetPct(value)", SOURCE)
        self.assertIn("'--sim-shock-left'", SOURCE)
        self.assertIn("'--sim-shock-width'", SOURCE)

    def test_builder_actions_live_in_header_and_result_is_announced(self):
        header = SOURCE[
            SOURCE.index('<div class="sim-multi-header">'):
            SOURCE.index('<div class="sim-multi-builder">', SOURCE.index('<div class="sim-multi-header">'))
        ]
        self.assertIn('class="sim-multi-header-actions"', header)
        self.assertIn('id="sim-multi-add"', header)
        self.assertIn("Add Asset", header)
        self.assertIn('id="sim-multi-reset"', header)
        self.assertIn("Reset Scenario", header)
        self.assertNotIn('class="sim-multi-summary-card"', SOURCE)
        self.assertIn(
            'class="liquidation-sim-metric liq sim-impact-primary" role="status" aria-live="polite" aria-atomic="true"',
            SOURCE,
        )

    def test_result_cards_fill_the_simplified_result_panel(self):
        self.assertIn(
            "grid-template-rows: auto minmax(140px, 1.1fr) minmax(132px, .9fr) !important;",
            SOURCE,
        )
        self.assertIn(
            "grid-template-columns: repeat(2, minmax(0, 1fr)) !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation #sim-card .sim-impact-secondary-grid > .liquidation-sim-metric",
            SOURCE,
        )
        self.assertIn(
            "@media (min-width: 981px) {\n"
            "            body.route-liquidation #sim-card .sim-multi-builder {\n"
            "                display: flex !important;\n"
            "                flex: 1 1 auto !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation #sim-card #sim-multi-rows {\n"
            "                display: grid !important;\n"
            "                flex: 1 1 auto !important;\n"
            "                grid-auto-rows: minmax(64px, 1fr) !important;",
            SOURCE,
        )

    def test_mobile_scenario_rows_do_not_reserve_empty_slots(self):
        self.assertIn(
            "const rowSlots = isCompact ? Math.min(rows.length, visibleRows) : visibleRows;",
            SOURCE,
        )
        self.assertIn(
            "height = (rowHeight * rowSlots) + (rowGap * Math.max(0, rowSlots - 1));",
            SOURCE,
        )

    def test_scenario_desk_uses_balanced_desktop_panels(self):
        for contract in (
            "grid-template-columns: repeat(2, minmax(0, 1fr)) !important;",
            "body.route-liquidation #sim-card .sim-multi-header-actions {",
            "body.route-liquidation #sim-card .sim-multi-shock-rail {",
            "body.route-liquidation #sim-card .sim-multi-shock-fill {",
            "left: var(--sim-shock-left, 50%) !important;",
            "width: var(--sim-shock-width, 0%) !important;",
            "body.route-liquidation #sim-card #sim-multi-panel,\n"
            "        body.route-liquidation #sim-card .liquidation-sim-metrics {\n"
            "            height: 100% !important;",
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, SOURCE)

    def test_scenario_desk_column_headers_align_with_desktop_rows_and_hide_when_stacked(self):
        self.assertIn(
            "body.route-liquidation #sim-card #sim-multi-panel .sim-multi-column-heads {\n"
            "            display: grid !important;\n"
            "            align-items: center !important;\n"
            "            padding: 0 9px 7px !important;\n"
            "            color: var(--fg-4) !important;\n"
            "            font-family: var(--mono) !important;\n"
            "            font-size: 9px !important;\n"
            "            font-weight: 650 !important;\n"
            "            letter-spacing: .72px !important;\n"
            "            line-height: 1 !important;\n"
            "            text-transform: uppercase !important;\n"
            "        }",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation #sim-card #sim-multi-panel .sim-multi-column-heads {\n"
            "                display: none !important;\n"
            "            }\n"
            "            body.route-liquidation #sim-card #sim-multi-panel .sim-multi-row {",
            SOURCE,
        )

    def test_scenario_desk_stacks_without_mobile_preset_space(self):
        self.assertIn(
            "@media (max-width: 980px) {\n"
            "            body.route-liquidation #sim-card.liquidation-sim-card {\n"
            "                grid-template-columns: 1fr !important;",
            SOURCE,
        )
        self.assertIn(
            "body.route-liquidation #sim-card .sim-multi-header-actions {\n"
            "                width: 100% !important;",
            SOURCE,
        )
        self.assertNotIn(".sim-multi-presets", SOURCE)
        self.assertNotIn(".sim-multi-preset", SOURCE)

    def test_mobile_scenario_input_remains_readable(self):
        self.assertIn(
            "body.route-liquidation #sim-card .sim-multi-row input[type=\"number\"] {\n"
            "                width: 64px !important;\n"
            "            }",
            SOURCE,
        )


if __name__ == "__main__":
    unittest.main()
