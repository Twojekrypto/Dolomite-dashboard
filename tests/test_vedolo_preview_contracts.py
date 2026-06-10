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


if __name__ == "__main__":
    unittest.main()
