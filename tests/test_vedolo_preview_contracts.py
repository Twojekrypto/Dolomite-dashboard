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
        self.assertIn("Ready to claim", self.html)

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


if __name__ == "__main__":
    unittest.main()
