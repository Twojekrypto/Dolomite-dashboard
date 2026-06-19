import os
import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.fspath(ROOT))


class ProtocolFooterContractsTest(unittest.TestCase):
    def test_footer_asset_version_is_shared_by_loader_and_history(self):
        footer_js = (ROOT / "protocol-footer.js").read_text(encoding="utf-8")
        match = re.search(r'const VERSION = "([^"]+)";', footer_js)
        self.assertIsNotNone(match)
        version = match.group(1)

        route_loader = (ROOT / "route-loader.js").read_text(encoding="utf-8")
        history_shell = (ROOT / "history" / "index.html").read_text(encoding="utf-8")

        self.assertIn(f'footer: "{version}"', route_loader)
        self.assertIn(f"protocol-footer.css?v={version}", history_shell)
        self.assertIn(f"protocol-footer.js?v={version}", history_shell)

    def test_route_shells_bust_route_loader_cache_for_footer_changes(self):
        shells = [
            ROOT / "index.html",
            *(path for path in sorted(ROOT.glob("*/index.html")) if path.parent.name != "history"),
        ]

        for shell in shells:
            html = shell.read_text(encoding="utf-8")
            self.assertIn("route-loader-20260619-footer-mobile", html, msg=str(shell.relative_to(ROOT)))
            self.assertNotIn("route-loader-20260611", html, msg=str(shell.relative_to(ROOT)))

    def test_mobile_footer_contract_address_layout_is_stable(self):
        css = (ROOT / "protocol-footer.css").read_text(encoding="utf-8")

        self.assertIn("@media (max-width:760px)", css)
        self.assertIn("grid-template-columns:minmax(0,1fr) 30px 30px", css)
        self.assertIn(".dolo-protocol-footer-hex", css)
        self.assertIn("max-width:100%", css)


if __name__ == "__main__":
    unittest.main()
