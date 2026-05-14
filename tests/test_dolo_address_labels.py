import json
import re
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LABELS_JS = ROOT / "dolo-address-labels.js"
ACTIVE_LABEL_CONSUMERS = (
    ROOT / "dolo-preview.html",
    ROOT / "dashboard-core.html",
    ROOT / "liquidation-preview.html",
)
ROUTE_SHELLS = (
    ROOT / "index.html",
    ROOT / "dolo" / "index.html",
    ROOT / "borrow" / "index.html",
    ROOT / "liquidation" / "index.html",
)


def load_labels():
    node_script = """
global.window = {};
require('./dolo-address-labels.js');
const labels = window.cloneDoloAddressLabels();
console.log(JSON.stringify({base: window.DOLO_ADDRESS_LABELS, labels}));
"""
    proc = subprocess.run(
        ["node", "-e", node_script],
        cwd=str(ROOT),
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(proc.stdout)


class DoloAddressLabelsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loaded = load_labels()
        cls.labels = cls.loaded["labels"]

    def test_label_script_exports_normalized_metadata(self):
        self.assertGreaterEqual(len(self.labels), 70)
        self.assertEqual(set(self.loaded["base"].keys()), set(self.labels.keys()))
        for address, info in self.labels.items():
            with self.subTest(address=address):
                self.assertRegex(address, r"^0x[a-f0-9]{40}$")
                self.assertTrue(info.get("label"))
                self.assertTrue(info.get("type"))
                self.assertTrue(info.get("source"))
                self.assertIn(info.get("confidence"), {"confirmed", "potential"})

    def test_coin_gecko_tokenomics_labels_are_confirmed(self):
        expected = {
            "0x185000fb4d98acea1a771db3714a431f7fe51cac": ("Core Team 1", "protocol"),
            "0xbf3c4e55a444ed489736c3d856b0cd0533fc2edd": ("Investor 2", "investor"),
            "0x6e939bbaceb45159982a2cac3a1fcbf7e93cf682": ("Ecosystem Incentives 1", "protocol"),
            "0x06265db7ecd9c5724a97bd4909146625d2e2619c": ("Ecosystem Incentives 2", "protocol"),
        }
        for address, (label, label_type) in expected.items():
            with self.subTest(address=address):
                info = self.labels[address]
                self.assertEqual(info["label"], label)
                self.assertEqual(info["type"], label_type)
                self.assertEqual(info["source"], "coingecko-tokenomics")
                self.assertEqual(info["confidence"], "confirmed")

    def test_potential_custody_labels_are_not_confirmed_cex(self):
        potential_rows = [info for info in self.labels.values() if "Potential" in info["label"] or info["type"] == "watch"]
        self.assertGreaterEqual(len(potential_rows), 1)
        for info in potential_rows:
            with self.subTest(label=info["label"]):
                self.assertEqual(info["type"], "watch")
                self.assertEqual(info["confidence"], "potential")
                self.assertEqual(info["source"], "heuristic-flow-pattern")
        for info in self.labels.values():
            if info["type"] == "cex":
                self.assertNotIn("Potential", info["label"])
                self.assertNotIn("CEX/MM", info["label"])
                self.assertEqual(info["confidence"], "confirmed")

    def test_active_pages_use_shared_label_source_only(self):
        inline_map_re = re.compile(r"const\s+(?:ADDR_LABELS|DOLO_ADDR_LABELS)\s*=\s*\{")
        for path in ACTIVE_LABEL_CONSUMERS:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertIn("dolo-address-labels.js", source)
                self.assertIn("cloneDoloAddressLabels", source)
                self.assertIsNone(inline_map_re.search(source))

    def test_route_shells_cache_bust_shared_label_cleanup(self):
        for path in ROUTE_SHELLS:
            with self.subTest(path=str(path.relative_to(ROOT))):
                self.assertIn("dolo-label-cleanup-20260514", path.read_text(encoding="utf-8"))

    def test_removed_legacy_index_preview_stays_removed(self):
        self.assertFalse((ROOT / "index_preview.html").exists())
        for path in ROOT.rglob("*"):
            if ".git" in path.parts or "__pycache__" in path.parts or not path.is_file():
                continue
            if path.suffix in {".pyc", ".pyo"}:
                continue
            if path.name == "test_dolo_address_labels.py":
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            self.assertNotIn("index_preview.html", text, str(path.relative_to(ROOT)))


if __name__ == "__main__":
    unittest.main()
