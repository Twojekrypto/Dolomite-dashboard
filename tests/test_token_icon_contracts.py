import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGET_FILES = [
    ROOT / "dashboard-core.js",
    ROOT / "assets-preview.html",
    ROOT / "tvl-preview.html",
    ROOT / "liquidation-preview.html",
]


class TokenIconContractsTest(unittest.TestCase):
    def test_usdt_symbols_do_not_reference_usdc_icon(self):
        offenders = []
        usdt_symbols = ("USDT", "USDT0", "USD₮0", "oUSDT")
        for path in TARGET_FILES:
            for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if "USDC.5f67b2404ca2e547fb00b70128d132cd.svg" not in line:
                    continue
                if any(re.search(rf"['\"]{re.escape(symbol)}['\"]", line) for symbol in usdt_symbols):
                    offenders.append(f"{path.name}:{line_no}:{line.strip()}")

        self.assertEqual([], offenders)

    def test_official_usdt_icon_asset_is_committed_and_used(self):
        icon = ROOT / "icons" / "usdt.png"
        self.assertTrue(icon.exists(), "icons/usdt.png must be committed")
        self.assertEqual(b"\x89PNG\r\n\x1a\n", icon.read_bytes()[:8])

        missing = [
            path.name
            for path in TARGET_FILES
            if "icons/usdt.png" not in path.read_text(encoding="utf-8")
        ]
        self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
