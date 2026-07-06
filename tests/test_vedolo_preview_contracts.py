import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VEDOLO_HTML = ROOT / "vedolo-preview.html"
VEDOLO_ROUTE = ROOT / "vedolo" / "index.html"


class VeDoloPreviewContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = VEDOLO_HTML.read_text(encoding="utf-8")
        cls.route = VEDOLO_ROUTE.read_text(encoding="utf-8")

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
        self.assertIn("Expired Claimable veDOLO", self.html)
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

    def test_expired_claimable_table_reuses_holder_ux_contracts(self):
        self.assertIn("#claimable-table{table-layout:fixed;min-width:920px}", self.html)
        self.assertIn("#claimable-table .holder-wallet", self.html)
        self.assertIn("#claimable-table .holder-toggle", self.html)
        self.assertIn('data-claimable-id="${esc(row.id)}"', self.html)
        self.assertIn("tableSpacerRows(st.perPage - Math.max(pageRows.length, 1), 7)", self.html)
        self.assertIn("syncSortHeader(\"#claimable-table\", st.sort, st.asc);", self.html)
        self.assertIn("claimable:state.claimable", self.html)
        self.assertIn('table === "claimable-table"', self.html)

    def test_vedolo_route_busts_preview_cache_for_claimable_table(self):
        self.assertIn("expired-claimable-20260706", self.route)


if __name__ == "__main__":
    unittest.main()
