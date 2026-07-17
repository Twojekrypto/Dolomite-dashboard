import unittest
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VEDOLO_HTML = ROOT / "vedolo-preview.html"
VEDOLO_ROUTE = ROOT / "vedolo" / "index.html"


class VeDoloPreviewContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = VEDOLO_HTML.read_text(encoding="utf-8")
        cls.route = VEDOLO_ROUTE.read_text(encoding="utf-8")
        cls.claimable_table = re.search(
            r'<table class="tbl" id="claimable-table">(?P<body>.*?)</table>',
            cls.html,
            re.S,
        ).group("body")

    def test_lock_expiry_timeline_has_wallet_search(self):
        self.assertIn("Lock Expiry Timeline", self.html)
        self.assertIn('id="q-expiry"', self.html)
        self.assertIn('placeholder="Search wallet address…"', self.html)
        self.assertIn('state.expiryUi.q = e.target.value.trim(); renderExpiry();', self.html)
        self.assertIn("function buildExpiryView()", self.html)
        self.assertIn("holder.token_details", self.html)
        self.assertIn("expiryBucketLabel(position.end)", self.html)
        self.assertIn("No wallet expiry data.", self.html)
        self.assertIn("expirySearchHasClear", self.html)
        self.assertIn("expirySearchMatchesHolderWidth", self.html)

    def test_vedolo_route_busts_preview_cache_for_expiry_search(self):
        self.assertIn("expiry-search-20260610", self.route)

    def test_expired_claimable_vedolo_table_contract(self):
        self.assertIn("Expired veDOLO Ready to Claim", self.html)
        self.assertNotIn("<h2>Expired Claimable veDOLO</h2>", self.html)
        self.assertIn('id="q-claimable"', self.html)
        self.assertIn('id="claimable-table"', self.html)
        self.assertIn('id="claimable-body"', self.html)
        self.assertIn('id="claimable-info"', self.html)
        self.assertIn('id="claimable-pager"', self.html)
        self.assertIn("function buildExpiredClaimableRows()", self.html)
        self.assertIn("end <= nowSec", self.html)
        self.assertIn("function renderExpiredClaimable()", self.html)
        self.assertIn("renderExpiredClaimable();", self.html)
        self.assertIn('id="claimable-meta"', self.html)

    def test_expired_claimable_table_reuses_holder_wallet_ux_contracts(self):
        self.assertIn("#claimable-table{table-layout:fixed;min-width:1040px}", self.html)
        self.assertIn("#claimable-table .holder-wallet", self.html)
        self.assertIn('data-claimable-id="${esc(row.id)}"', self.html)
        self.assertIn("tableSpacerRows(st.perPage - Math.max(pageRows.length, 1), 8)", self.html)
        self.assertIn("syncSortHeader(\"#claimable-table\", st.sort, st.asc);", self.html)
        self.assertIn("claimable:state.claimable", self.html)
        self.assertIn('table === "claimable-table"', self.html)

    def test_expired_claimable_table_replaces_vote_and_details_with_route_and_exercise_price(self):
        self.assertIn("Route", self.claimable_table)
        self.assertIn("Exercise Price", self.claimable_table)
        self.assertIn('data-sort="route"', self.claimable_table)
        self.assertIn('data-sort="price"', self.claimable_table)
        self.assertNotIn("USDC.e Paid", self.claimable_table)
        self.assertNotIn("Vote Weight", self.claimable_table)
        self.assertNotIn("Details", self.claimable_table)
        self.assertNotIn("claimable-vote-col", self.claimable_table)
        self.assertNotIn("claimable-actions-col", self.claimable_table)

    def test_expired_claimable_route_column_is_center_aligned(self):
        self.assertIn("#claimable-table th[data-sort=\"route\"],#claimable-table .claimable-route-cell{text-align:center}", self.html)
        self.assertIn("#claimable-table th[data-sort=\"route\"] .th-content{justify-content:center}", self.html)
        self.assertIn("#claimable-table .claimable-route-cell .flow-source-tag{display:inline-flex;margin-inline:auto;justify-content:center;max-width:100%}", self.html)

    def test_expired_claimable_table_includes_lock_term_column(self):
        self.assertIn('<col class="claimable-lock-col">', self.claimable_table)
        self.assertIn("Lock Term", self.claimable_table)
        self.assertIn('data-sort="lock"', self.claimable_table)
        self.assertIn("function claimableLockTermDays(row)", self.html)
        self.assertIn("function claimableLockTermLabel(row)", self.html)
        self.assertIn("claimableLockTermLabel(row)", self.html)
        self.assertIn('if(sort === "lock"){ va = claimableLockTermDays(a); vb = claimableLockTermDays(b); }', self.html)
        self.assertIn('key === "lock"', self.html)
        self.assertIn('<td colspan="8">No expired claimable veDOLO positions match the current filters.</td>', self.html)

    def test_claimable_price_uses_exerciser_token_id_lookup(self):
        self.assertIn('fetchJson("exercisers_by_address.json").catch(() => null)', self.html)
        self.assertIn("exerciseEventsByToken:new Map()", self.html)
        self.assertIn("function buildExerciseRouteIndexes(exercisers)", self.html)
        self.assertIn("token_ids", self.html)
        self.assertIn("function claimableExercisePrice(row)", self.html)
        self.assertIn("function claimableExercisePriceLabel(row)", self.html)
        self.assertIn("row.exercise?.paid_token === \"USDC.e\"", self.html)
        self.assertNotIn("claimableUsdcPaidLabel", self.html)
        self.assertNotIn("USDC.e</span>", self.html)
        self.assertIn("flowSourceTag(row.route.kind, row.route.tooltip)", self.html)

    def test_vedolo_route_busts_preview_cache_for_claimable_table(self):
        self.assertIn("claimable-ready-title-20260706", self.route)

    def test_locked_chart_has_accessible_metric_switch(self):
        self.assertIn('id="lockedChartMode"', self.html)
        self.assertIn('data-locked-chart-mode="locked"', self.html)
        self.assertIn('data-locked-chart-mode="vote"', self.html)
        self.assertIn('id="lockedChartTitle"', self.html)
        self.assertIn('aria-pressed="true"', self.html)

    def test_vedolo_segmented_controls_use_holder_distribution_pill_ux(self):
        self.assertIn('class="locked-chart-mode vedolo-pill-segment"', self.html)
        self.assertIn('class="seg vedolo-pill-segment" id="flow-source"', self.html)
        self.assertIn('class="seg vedolo-pill-segment" id="duration-mode"', self.html)
        self.assertIn('.vedolo-pill-segment{', self.html)
        self.assertIn('border-radius:999px', self.html)
        self.assertIn('height:34px;min-height:34px', self.html)
        self.assertIn('.vedolo-pill-segment button.active,', self.html)
        self.assertIn('.vedolo-pill-segment button.active::after{display:none}', self.html)
        self.assertIn('#flow-source.vedolo-pill-segment{--vedolo-segment-count:3;', self.html)
        self.assertIn("vedolo-pill-controls-20260717", self.route)

    def test_locked_chart_places_zoom_meta_in_the_top_right(self):
        heading = re.search(
            r'<div class="locked-chart-heading">(?P<body>.*?)</div>\n    <div class="locked-chart-wrap"',
            self.html,
            re.S,
        ).group("body")
        self.assertIn('class="card-meta locked-chart-meta"', heading)
        title_pos = heading.index('class="card-title"')
        meta_pos = heading.index('class="card-meta locked-chart-meta"')
        controls_pos = heading.index('class="locked-chart-controls"')
        self.assertLess(title_pos, meta_pos)
        self.assertLess(meta_pos, controls_pos)
        self.assertIn('<div class="card-meta locked-chart-meta"><span class="pulse"></span>drag window below to zoom</div>', heading)
        self.assertIn('.locked-chart-heading{display:flex;align-items:flex-start;justify-content:space-between;', self.html)
        self.assertIn('.locked-chart-meta{margin-left:auto}', self.html)
        self.assertIn('.locked-chart-controls{display:flex;flex-basis:100%;align-items:flex-start}', self.html)
        self.assertIn('@media (max-width:560px){', self.html)
        self.assertIn('.locked-chart-heading{align-items:flex-start;flex-direction:column}', self.html)
        self.assertIn('.locked-chart-meta{margin-left:0}', self.html)

    def test_vote_power_availability_repeat_safe_setters_and_removals(self):
        self.assertIn('const LOCKED_CHART_VOTE_UNAVAILABLE_TIP = "Verified vote-power history is unavailable.";', self.html)
        self.assertIn('help.setAttribute("data-tip", LOCKED_CHART_VOTE_UNAVAILABLE_TIP);', self.html)
        self.assertIn('help.setAttribute("tabindex", "0");', self.html)
        self.assertIn('help.removeAttribute("data-tip");', self.html)
        self.assertIn('help.removeAttribute("tabindex");', self.html)
        self.assertNotIn('help.toggleAttribute(', self.html)

    def test_unavailable_vote_power_tooltip_uses_focusable_wrapper(self):
        match = re.search(
            r'(?P<wrapper><span class="locked-chart-vote-help"[^>]*>\s*<button[^>]*data-locked-chart-mode="vote".*?</button>\s*</span>)',
            self.html,
            re.S,
        )
        self.assertIsNotNone(match, "Vote Power unavailable state must use a focusable tooltip wrapper")
        if match is None:
            return
        vote = match.group("wrapper")
        self.assertIn('data-tip="Verified vote-power history is unavailable."', vote)
        self.assertIn('tabindex="0"', vote)
        self.assertIn('disabled', vote)
        self.assertNotIn('data-tip=', re.sub(r'<span[^>]*>', '', vote, count=1))
        self.assertIn('function syncLockedChartVoteAvailability()', self.html)
        self.assertIn('attributeFilter:["disabled"]', self.html)

    def test_vote_power_view_loads_static_history_only(self):
        self.assertIn('const votePowerPromise = fetchStaticJson("data/vedolo-vote-power-history.json", "vedolo-vote-power-history-20260714")', self.html)
        self.assertIn('async function fetchStaticJson(name, version)', self.html)
        self.assertNotIn('rpc.berachain.com', self.html)

    def test_locked_chart_uses_independent_vote_power_state_and_shared_brush(self):
        self.assertIn('votePowerHistory:[]', self.html)
        self.assertIn('lockedChartMode:"locked"', self.html)
        self.assertIn('function activeLockedChartSeries()', self.html)
        self.assertIn('function setLockedChartMode(mode)', self.html)
        self.assertIn('state.lockedBrush', self.html)

    def test_vote_power_parser_requires_canonical_static_contract(self):
        self.assertIn('function parseVotePowerHistory(payload)', self.html)
        self.assertIn('payload.schemaVersion !== 1', self.html)
        self.assertIn('payload.metric !== "votePower"', self.html)
        self.assertIn('payload.chain !== "berachain"', self.html)
        self.assertIn('payload.lastPointWei !== payload.totalSupplyWei', self.html)
        self.assertIn('timestamp <= previousTimestamp', self.html)
        self.assertIn('Number.isFinite(value)', self.html)
        self.assertIn('value < 0', self.html)

    def test_vote_power_parser_accepts_generator_compact_decimals(self):
        self.assertIn('^(0|[1-9]\\d*)(?:\\.\\d{1,18})?$', self.html)
        self.assertIn('fraction.padEnd(18, "0")', self.html)

    def test_locked_chart_switch_clears_previous_metric_hover(self):
        mode_switch = re.search(
            r'function setLockedChartMode\(mode\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        )
        self.assertIsNotNone(mode_switch)
        self.assertIn('clearLockedChartHover();', mode_switch.group('body'))

    def test_locked_brush_click_reads_active_series_bounds(self):
        click_handler = re.search(
            r'brushSvg\.addEventListener\("click", e => \{(?P<body>.*?)\n  \}\);',
            self.html,
            re.S,
        )
        self.assertIsNotNone(click_handler)
        self.assertIn('const {minX,maxX} = bounds();', click_handler.group('body'))

    def test_locked_chart_axis_uses_compact_value_without_metric_suffix(self):
        self.assertIn('>${fmtCompact(v)}</text>`', self.html)

    def test_locked_chart_switcher_is_mode_aware_and_clamps_shared_brush(self):
        self.assertIn('clampLockedBrushToActiveSeries();', self.html)
        self.assertIn('fmtCompact(value) + " Vote Power"', self.html)
        self.assertIn('activeLockedChartSeries().unit', self.html)
        self.assertIn('setVotePowerAvailability(state.votePowerHistory.length >= 2);', self.html)
        self.assertIn('function bindLockedBrushInteractions()', self.html)
        self.assertIn('state.lockedBrushListenersBound', self.html)

    def test_vedolo_route_busts_cache_for_vote_power_chart(self):
        self.assertIn('vedolo-vote-power-history-20260714', self.route)

    def test_vedolo_route_busts_preview_cache_for_stacked_chart_controls(self):
        self.assertIn('vedolo-chart-controls-stack-20260714', self.route)

    def test_vedolo_route_busts_preview_cache_for_top_right_chart_meta(self):
        self.assertIn('vedolo-chart-meta-right-20260715', self.route)

    def test_recent_early_exit_controls_use_red_interactions(self):
        self.assertIn('id="recent-early-exits-section"', self.html)
        self.assertIn('--recent-exit-accent:var(--down)', self.html)
        self.assertIn('#recent-early-exits-section .search:focus-within', self.html)
        self.assertIn('#recent-early-exits-section #dd-exit-period .dd-btn.open', self.html)
        self.assertIn('#recent-early-exits-section #dd-exit-period .dd-opt.active .dd-opt-check', self.html)
        self.assertIn('vedolo-exit-red-controls-20260711', self.route)

    def test_vedolo_tables_show_source_specific_data_update_metadata(self):
        for meta_id in (
            "recent-exits-meta",
            "holders-meta",
            "flows-meta",
            "claimable-meta",
            "duration-meta",
        ):
            self.assertIn(f'id="{meta_id}"', self.html)

        metadata_sync = re.search(
            r'function syncVedoloTableMetadata\(\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        ).group("body")
        self.assertIn("state.earlyStats?.last_updated", metadata_sync)
        self.assertIn("state.holdersTimestamp", metadata_sync)
        self.assertIn("state.flowsTimestamp", metadata_sync)
        self.assertIn('setText("recent-exits-meta",', metadata_sync)
        self.assertIn('setText("holders-meta",', metadata_sync)
        self.assertIn('setText("flows-meta",', metadata_sync)
        self.assertIn('setText("claimable-meta",', metadata_sync)
        self.assertIn('setText("duration-meta",', metadata_sync)
        self.assertIn("vedolo-table-update-metadata-20260717", self.route)


if __name__ == "__main__":
    unittest.main()
