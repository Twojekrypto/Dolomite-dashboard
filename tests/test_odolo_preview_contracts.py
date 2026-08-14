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

    def test_discount_curve_uses_the_protocol_schedule_and_stays_within_zero_to_fifty_percent(self):
        curve = re.search(
            r'function syncLiveDiscountCurve\(\)\{(?P<body>.*?)\n\}',
            self.html,
            re.S,
        ).group("body")

        self.assertIn("const yMin = 0;", self.html)
        self.assertIn("const yMax = 50;", self.html)
        self.assertIn("const discount = theoreticalDiscount(days);", curve)
        self.assertIn("Protocol discount by lock duration", self.html)
        self.assertNotIn("dailyDoloPrice", self.html)
        self.assertNotIn('fetchJson("dolo_price_history.json")', self.html)

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
        self.assertIn('<span class="addr-name">${esc(r.label)}</span>', self.html)
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
