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


def tracked_repo_files():
    proc = subprocess.run(
        ["git", "ls-files"],
        cwd=str(ROOT),
        check=True,
        text=True,
        capture_output=True,
    )
    return [ROOT / line for line in proc.stdout.splitlines() if line]


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

    def test_public_explorer_labels_are_current(self):
        expected = {
            "0x000000000004444c5dc75cb358380d2e3de08a90": ("Uniswap V4 Pool Manager", "contract"),
            "0xf977814e90da44bfa03b6295a0616a897441acec": ("Binance Hot Wallet 20", "cex"),
            "0x21a31ee1afc51d94c2efccaa2092ad1028285549": ("Binance 15", "cex"),
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": ("Binance 16", "cex"),
            "0x0d0707963952f2fba59dd06f2b425ace40b492fe": ("Gate.io Deposit", "cex"),
            "0x1157a2076b9bb22a85cc2c162f20fab3898f4101": ("FalconX 1", "cex"),
        }
        for address, (label, label_type) in expected.items():
            with self.subTest(address=address):
                info = self.labels[address]
                self.assertEqual(info["label"], label)
                self.assertEqual(info["type"], label_type)
                self.assertEqual(info["source"], "etherscan-public-label")
                self.assertEqual(info["confidence"], "confirmed")

    def test_ens_reverse_names_are_identity_labels_not_entity_claims(self):
        expected = {
            "0xd6f631c796a56a5d448dd88a01f15058c4a0be52": "makeitback.eth",
            "0x7bfee91193d9df2ac0bfe90191d40f23c773c060": "7bfee.eth",
            "0x224f590a8b58f83bd2673348d6ac75a7b27f9b54": "mike-tyson.eth",
            "0x87db27ac8459ab6602f7a6155b48f6b184065da0": "atheon.eth",
        }
        for address, label in expected.items():
            with self.subTest(address=address):
                info = self.labels[address]
                self.assertEqual(info["label"], label)
                self.assertEqual(info["type"], "eoa")
                self.assertEqual(info["source"], "ens-reverse")
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

    def test_removed_legacy_preview_files_stay_removed(self):
        removed_files = ("index_preview.html", "add_investors.py")
        for filename in removed_files:
            self.assertFalse((ROOT / filename).exists(), filename)

        for path in tracked_repo_files():
            if path.name == "test_dolo_address_labels.py":
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for filename in removed_files:
                self.assertNotIn(filename, text, str(path.relative_to(ROOT)))


if __name__ == "__main__":
    unittest.main()
