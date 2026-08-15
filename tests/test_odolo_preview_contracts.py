import json
import re
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ODOLO_HTML = ROOT / "odolo-preview.html"
ODOLO_ROUTE = ROOT / "odolo" / "index.html"


class OdoloPreviewContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = ODOLO_HTML.read_text(encoding="utf-8")
        cls.route = ODOLO_ROUTE.read_text(encoding="utf-8")
        cls.latest_table = re.search(
            r'<table class="tbl latest-ex-tbl" id="tbl-latest-ex"[^>]*>(?P<body>.*?)</table>',
            cls.html,
            re.S,
        ).group("body")
        cls.latest_render = re.search(
            r'function renderLatestExercises\(\)\{(?P<body>.*?)\nfunction getLatestPairRows\(\)',
            cls.html,
            re.S,
        ).group("body")

    def test_latest_odolo_exercises_keeps_usdc_paid_and_price_columns(self):
        self.assertIn("Latest oDOLO Exercises", self.html)
        self.assertIn('data-latest-sort="usdc" class="num" style="width:118px">USDC Paid', self.latest_table)
        self.assertIn('data-latest-sort="price" class="num" style="width:96px">Price', self.latest_table)
        self.assertIn('data-label="USDC Paid"><span class="bal-val usd">${fmtUsdFull(tx.usdc)}</span>', self.latest_render)
        self.assertIn('data-label="Price"><span class="bal-val price">${fmtPrice(tx.price)}</span>', self.latest_render)
        self.assertIn('tbody.innerHTML = `<tr><td colspan="6"><div class="latest-empty">Loading latest oDOLO exercises…</div></td></tr>`;', self.latest_render)
        self.assertIn('tbody.innerHTML = `<tr><td colspan="6"><div class="latest-empty">${emptyLabel}</div></td></tr>` + stableTableSpacerRowsHtml', self.latest_render)

    def test_odolo_route_no_longer_uses_mistaken_latest_price_cache_bust(self):
        self.assertNotIn("latest-ex-price-20260706", self.route)

    def test_odolo_accuracy_artifacts_use_a_fresh_data_cache_key(self):
        self.assertIn('const DATA_VERSION = "odolo-discount-accuracy-20260814";', self.html)

    def test_discount_curve_uses_the_protocol_schedule_and_stays_within_zero_to_fifty_percent(self):
        curve = re.search(
            r'function syncLiveDiscountCurve\(\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        ).group("body")

        self.assertIn("const yMin = 0;", self.html)
        self.assertIn("const yMax = 50;", self.html)
        self.assertIn("const discount = theoreticalDiscount(days, lockSeconds);", curve)
        self.assertIn("Protocol discount by lock duration", self.html)
        self.assertNotIn("dailyDoloPrice", self.html)
        self.assertNotIn('fetchJson("dolo_price_history.json")', self.html)

    def test_discount_curve_rounds_post_week_locks_up_to_the_next_protocol_week(self):
        match = re.search(
            r'(function protocolDiscountFromDurationSeconds\(lockSeconds\)\{.*?\n\})\nfunction theoreticalDiscount',
            self.html,
            re.S,
        )
        self.assertIsNotNone(match, "exact protocol discount helper is required")
        script = f"""
{match.group(1)}
console.log(JSON.stringify([
  protocolDiscountFromDurationSeconds(3.5 * 86400),
  protocolDiscountFromDurationSeconds(7 * 86400),
  protocolDiscountFromDurationSeconds(14 * 86400),
  protocolDiscountFromDurationSeconds(365 * 86400),
  protocolDiscountFromDurationSeconds(721.1 * 86400)
]));
"""
        result = subprocess.run(["node", "-e", script], text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        values = json.loads(result.stdout)
        expected = [2.5, 5, 5 + 45 / 103, 5 + 45 * 52 / 103, 50]
        for actual, wanted in zip(values, expected):
            self.assertAlmostEqual(actual, wanted, places=9)

    def test_protocol_discount_line_is_drawn_as_exact_weekly_steps(self):
        discount_helper = re.search(
            r'(function protocolDiscountFromDurationSeconds\(lockSeconds\)\{.*?\n\})\nfunction theoreticalDiscount',
            self.html,
            re.S,
        ).group(1)
        points_helper = re.search(
            r'(function protocolDiscountCurvePoints\(maxDays = 760\)\{.*?\n\})',
            self.html,
            re.S,
        )
        self.assertIsNotNone(points_helper, "explicit staircase points are required")
        script = f"""
{discount_helper}
{points_helper.group(1)}
console.log(JSON.stringify(protocolDiscountCurvePoints()));
"""
        result = subprocess.run(["node", "-e", script], text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        points = json.loads(result.stdout)
        at_seven = [point["discount"] for point in points if point["days"] == 7]
        at_fourteen = [point["discount"] for point in points if point["days"] == 14]
        self.assertEqual(len(at_seven), 2)
        self.assertAlmostEqual(at_seven[0], 5, places=9)
        self.assertAlmostEqual(at_seven[1], 5 + 45 / 103, places=9)
        self.assertEqual(len(at_fourteen), 2)
        self.assertAlmostEqual(at_fourteen[0], 5 + 45 / 103, places=9)
        self.assertAlmostEqual(at_fourteen[1], 5 + 45 * 2 / 103, places=9)
        self.assertEqual(points[-1], {"days": 760, "discount": 50})
        self.assertIn("protocolDiscountCurvePoints().forEach", self.html)
        self.assertNotIn("for(let d=0; d<=760; d+=2)", self.html)

    def test_discount_curve_keeps_verified_zero_usdc_dust_exercises(self):
        curve_match = re.search(
            r'(function syncLiveDiscountCurve\(\)\{.*?\n\})\n\nfunction syncOdoloTableMetadata',
            self.html,
            re.S,
        )
        self.assertIsNotNone(curve_match)
        script = f"""
const LIVE = {{exercisers: {{exercisers: [{{txs: [
  {{paid_token:"USDC.e", usdc:0, vedolo:0.000674, lock_days:7, lock_seconds:604800}},
  {{paid_token:"USDC.e", usdc:10, vedolo:100, lock_days:14, lock_seconds:1209600}}
]}}]}}}};
const DISC_DATA = [];
const txUsd = tx => Number(tx.usdc);
const txVedolo = tx => Number(tx.vedolo);
const isUsdcExerciseTx = tx => tx.paid_token !== "DOLO";
const exerciseLockSeconds = tx => Number(tx.lock_seconds) || Number(tx.lock_days) * 86400;
const theoreticalDiscount = (days, seconds) => Number(seconds || days * 86400) / 86400;
const tierForUsd = () => "small";
const applyRows = (target, rows) => target.splice(0, target.length, ...rows);
const setMetricText = () => {{}};
{curve_match.group(1)}
syncLiveDiscountCurve();
console.log(JSON.stringify(DISC_DATA));
"""
        result = subprocess.run(["node", "-e", script], text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = json.loads(result.stdout)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["usd"], 0)

    def test_discount_curve_average_guide_is_derived_from_rendered_rows(self):
        match = re.search(
            r'(function discountCurveAverageLockDays\(rows\)\{.*?\n\})\n',
            self.html,
            re.S,
        )
        self.assertIsNotNone(match, "dynamic average-lock helper is required")
        script = f"""
{match.group(1)}
console.log(JSON.stringify([
  discountCurveAverageLockDays([{{days:7}}, {{days:21}}]),
  discountCurveAverageLockDays([
    {{days:7, lockSeconds:10 * 86400}},
    {{days:21, lockSeconds:10 * 86400}}
  ])
]));
"""
        result = subprocess.run(["node", "-e", script], text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), [14, 10])
        self.assertNotIn("avg lock 17.5mo", self.html)

    def test_top_exerciser_badge_counts_the_current_usdc_view_not_mixed_methods(self):
        renderer = re.search(
            r'function renderExercisers\(\)\{(?P<body>.*?)\nfunction renderFlows',
            self.html,
            re.S,
        ).group("body")
        self.assertIn("const totalAddresses = filterExercisers({includeSearch:false}).length;", renderer)
        self.assertIn("sum(usdcTxs, exerciseLockDays)", self.html)
        self.assertNotIn("LIVE.exercisers?.total_addresses", renderer)

    def test_one_day_filters_compare_exact_transaction_timestamps(self):
        self.assertIn("return Date.now() - days * 86400000;", self.html)
        self.assertIn("if(cutoff && exerciseTimeMs(tx) < cutoff) return;", self.html)
        self.assertNotIn("if(cutoff && tx.date < cutoff) return;", self.html)

    def test_short_locks_render_in_hours_or_minutes(self):
        self.assertIn('if(days < 1/24) return Math.max(1, Math.round(days*1440)) + "m";', self.html)
        self.assertIn('if(days < 1) return (days*24).toFixed(1) + "h";', self.html)

    def test_claimer_behavior_donut_uses_non_overlapping_claim_partition(self):
        behavior = re.search(
            r'function syncLiveBehavior\(\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        ).group("body")

        self.assertIn("pct_claim_remaining", behavior)
        self.assertNotIn("pct_held", behavior)
        self.assertNotIn("pct_bought_extra", behavior)

    def test_inline_javascript_is_syntax_valid(self):
        inline_scripts = []
        for opening, body in re.findall(
            r'(<script(?:\s[^>]*)?>)(.*?)</script>',
            self.html,
            re.S | re.I,
        ):
            if re.search(r'\bsrc\s*=', opening, re.I):
                continue
            inline_scripts.append(body)

        result = subprocess.run(
            ["node", "--check", "-"],
            input="\n".join(inline_scripts),
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_claimer_behavior_executes_with_exact_aggregate_amounts(self):
        behavior_fn = re.search(
            r'(function syncLiveBehavior\(\)\{.*?\n\})\n\nfunction syncLiveClaimersForPeriod',
            self.html,
            re.S,
        ).group(1)
        render_fn = re.search(
            r'(function renderDonuts\(\)\{.*?\n\})\n\n/\* ={20,}\n   DISCOUNT CURVE',
            self.html,
            re.S,
        ).group(1)
        script = f"""
const LIVE = {{
  contract: {{allocationSupply: 200000000}},
  flows: {{claimer_behavior: {{
    total_claimers: 2,
    total_claimed: 100.04,
    total_exercised: 40.01,
    total_outflow: 30.02,
    total_claim_remaining: 30.01,
    pct_exercised: 1,
    pct_outflow: 1,
    pct_claim_remaining: 98
  }}}}
}};
const BEHAV_SEGMENTS = [];
const DIST_SEGMENTS = [];
const donutCalls = [];
const finiteNum = (value, fallback = 0) => Number.isFinite(Number(value)) ? Number(value) : fallback;
const applyRows = (target, rows) => target.splice(0, target.length, ...rows);
const fmtNum = value => Number(value).toFixed(2);
const setText = () => {{}};
const setHtml = () => {{}};
const drawDonut = opts => donutCalls.push(opts);
{behavior_fn}
{render_fn}
syncLiveBehavior();
renderDonuts();
console.log(JSON.stringify({{segments: BEHAV_SEGMENTS, behaviorDonut: donutCalls[1]}}));
"""
        result = subprocess.run(
            ["node", "-e", script],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)

        self.assertEqual(
            [row["value"] for row in payload["segments"]],
            [40.01, 30.02, 30.01],
        )
        self.assertEqual(payload["behaviorDonut"]["totalLabel"], "40.0%")

    def test_latest_pair_controls_use_portfolio_green_interactions(self):
        self.assertIn('id="latest-pairs-section"', self.html)
        self.assertIn('--latest-pair-accent:#75b87b', self.html)
        self.assertIn('#latest-pairs-section .search:focus-within', self.html)
        self.assertIn('#latest-pairs-section #dd-pair-period .dd-btn.open', self.html)
        self.assertIn('#latest-pairs-section #dd-pair-period .dd-opt.active .dd-opt-check', self.html)
        self.assertIn('odolo-pair-green-controls-20260711', self.route)

    def test_latest_exercise_controls_match_the_pair_green_interactions(self):
        self.assertIn('id="latest-exercises-section"', self.html)
        self.assertIn('--latest-exercise-accent:#75b87b', self.html)
        self.assertIn('#latest-exercises-section .search:focus-within', self.html)
        self.assertIn('#latest-exercises-section #dd-latest-period .dd-btn.open', self.html)
        self.assertIn('#latest-exercises-section #dd-latest-period .dd-opt.active .dd-opt-check', self.html)
        self.assertIn('odolo-exercise-green-controls-20260711', self.route)

    def test_latest_exercises_offers_fourteen_day_period_without_changing_default(self):
        latest_section = re.search(
            r'<section class="card latest-activity-section" id="latest-exercises-section">(?P<body>.*?)</section>',
            self.html,
            re.S,
        ).group("body")

        self.assertIsNotNone(
            re.search(r'data-period="7d".*data-period="14d".*data-period="30d"', latest_section, re.S)
        )
        self.assertIn('{key:"14d",  short:"14D",  label:"Last 14 days"}', self.html)
        self.assertIn('"14d":14', self.html)
        self.assertIn('latestPeriod: "7d"', self.html)

    def test_latest_exercise_summary_precedes_toolbar_and_uses_period_rows(self):
        latest_section = re.search(
            r'<section class="card latest-activity-section" id="latest-exercises-section">(?P<body>.*?)</section>',
            self.html,
            re.S,
        ).group("body")

        self.assertIn('id="latest-exercise-stats"', latest_section)
        self.assertLess(latest_section.index('id="latest-exercise-stats"'), latest_section.index('class="toolbar"'))
        for label in ("Exercises", "veDOLO Received", "USDC Paid", "Avg Exercise Price", "Avg Lock"):
            self.assertIn(label, self.html)
        self.assertIn('const periodRows = getLatestExerciseRows();', self.latest_render)
        self.assertIn('renderLatestExerciseSummary(periodRows);', self.latest_render)
        self.assertIn('const rows = filterLatestExerciseRowsBySearch(periodRows, state.qLatest);', self.latest_render)
        self.assertIn('`${periodRows.length.toLocaleString("en-US")} · ${periodLabel}`', self.latest_render)

    def test_latest_exercise_summary_helper_and_route_are_cache_busted(self):
        self.assertIn(
            '<script src="odolo-exercise-summary.js?v=odolo-latest-summary-20260808"></script>',
            self.html,
        )
        self.assertIn('odolo-latest-exercise-summary-20260808', self.route)

    def test_odolo_tables_show_source_specific_data_update_metadata(self):
        for meta_id in (
            "claimer-breakdown-meta",
            "latest-ex-meta",
            "latest-pair-meta",
            "top-exercisers-meta",
            "flows-meta",
        ):
            self.assertIn(f'id="{meta_id}"', self.html)

        self.assertIn("function fmtDataUpdated(timestamp)", self.html)
        metadata_sync = re.search(
            r'function syncOdoloTableMetadata\(\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        ).group("body")
        self.assertIn("LIVE.flows?.timestamp", metadata_sync)
        self.assertIn("LIVE.exercisers?.updated", metadata_sync)
        self.assertIn('setCardMeta("claimer-breakdown-meta", flowUpdated)', metadata_sync)
        self.assertIn('setCardMeta("latest-ex-meta", exerciseUpdated)', metadata_sync)
        self.assertIn('setCardMeta("latest-pair-meta", exerciseUpdated)', metadata_sync)
        self.assertIn('setCardMeta("top-exercisers-meta", exerciseUpdated)', metadata_sync)
        self.assertIn('setCardMeta("flows-meta", flowUpdated)', metadata_sync)
        self.assertIn("odolo-table-update-metadata-20260717", self.route)

    def test_distribution_reconciles_full_allocation_with_burned_segment(self):
        self.assertIn('label:"Redeemed & burned"', self.html)
        self.assertIn('finiteNum(data.allocationSupply, 200e6)', self.html)
        self.assertIn('data.redeemedAndBurned', self.html)
        self.assertIn('centerLDefault:"Allocation"', self.html)
        self.assertIn('`${fmtNum(allocationSupply)} allocation`', self.html)
        self.assertIn("odolo-allocation-reconciliation-20260807", self.route)

    def test_claimer_behavior_displays_allocation_context(self):
        self.assertIn(
            'oDOLO claimed of ${fmtNum(allocationSupply)} allocation',
            self.html,
        )

    def test_claimer_breakdown_uses_shared_labels_with_badge_and_provenance(self):
        self.assertIn('<script src="odolo-address-meta.js?v=odolo-label-parity-20260807"></script>', self.html)
        self.assertIn("window.buildOdoloAddressMeta", self.html)
        self.assertNotIn("const ODOLO_SHARED_TYPE_MAP", self.html)
        self.assertIn("badgeLabel: meta.badgeLabel", self.html)
        self.assertIn("labelTooltip: meta.tooltip", self.html)
        self.assertIn("function odoloWalletCell(row, badgesHtml=\"\")", self.html)
        self.assertIn("DoloWalletTableUX.walletCellHtml", self.html)
        self.assertIn("labels:ADDRESS_META", self.html)
        self.assertIn('data-tooltip="${esc(r.labelTooltip || TYPE_TIPS[r.type] || "")}"', self.html)
        self.assertIn("odolo-claimer-label-parity-20260807", self.route)

    def test_donut_center_is_initialized_from_live_segment_values(self):
        draw_donut = re.search(
            r'function drawDonut\(opts\)\{(?P<body>.*?)\n\}\n\n\n/\* ={20,}',
            self.html,
            re.S,
        ).group("body")

        self.assertIn("setHover(-1);", draw_donut)


if __name__ == "__main__":
    unittest.main()
