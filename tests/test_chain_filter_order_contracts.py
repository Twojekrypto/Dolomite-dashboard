import re
import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ASSETS_CHAIN_ORDER = ["ethereum", "berachain", "arbitrum"]
CHAIN_ORDER = ["ethereum", "berachain", "arbitrum", "mantle", "botanix", "polygonzkevm", "xlayer"]
CHAIN_LABEL_ORDER = ["Ethereum", "Berachain", "Arbitrum"]
EARN_CHAIN_ORDER = ["ethereum", "berachain", "arbitrum", "mantle", "xlayer", "polygonzkevm", "botanix"]


class ChainFilterOrderContractsTest(unittest.TestCase):
    def test_assets_preview_network_filter_order_matches_total_supply_rank(self):
        text = (ROOT / "assets-preview.html").read_text(encoding="utf-8")
        panel = re.search(r'<div class="dd" id="dd-net">([\s\S]*?)</div>\s*</div>\s*</div>\s*</div>\s*<div class="tb-right">', text)
        self.assertIsNotNone(panel)
        keys = [
            key
            for key in re.findall(r'data-net="([^"]+)"', panel.group(1))
            if key != "all"
        ]
        self.assertEqual(ASSETS_CHAIN_ORDER, keys)

    def test_dashboard_assets_network_filter_order_matches_total_supply_rank(self):
        text = (ROOT / "dashboard-core.html").read_text(encoding="utf-8")
        panel = re.search(
            r'<div class="assets-network-grid">([\s\S]*?)</div>\s*</div>\s*</div>\s*</div>\s*<div class="assets-toolbar-right">',
            text,
        )
        self.assertIsNotNone(panel)
        keys = [
            key
            for key in re.findall(r'data-chain="([^"]+)"', panel.group(1))
            if key != "all"
        ]
        self.assertEqual(ASSETS_CHAIN_ORDER, keys)

    def test_earn_chain_menu_excludes_retired_networks(self):
        text = (ROOT / "dashboard-core.js").read_text(encoding="utf-8")
        block = re.search(r"const EARN_CHAINS = Object.fromEntries([\s\S]*?)\n        //", text)
        self.assertIsNotNone(block)
        result = subprocess.check_output(["node", "-e", block.group(0).rsplit("//", 1)[0] +
                                          "console.log(JSON.stringify(Object.keys(EARN_CHAINS)));"], text=True)
        self.assertEqual(ASSETS_CHAIN_ORDER, json.loads(result))

    def test_supply_chain_dropdown_order_matches_total_supply_rank(self):
        text = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
        block = re.search(r'<div id="chain-options-container">([\s\S]*?)</div>\s*</div>\s*<input type="hidden" id="supply-chain-select"', text)
        self.assertIsNotNone(block)
        labels = re.findall(r"<span>([^<]+)</span>", block.group(1))
        self.assertEqual(CHAIN_LABEL_ORDER, labels)

    def test_tvl_token_composition_dropdown_sorts_by_total_supply(self):
        text = (ROOT / "tvl-preview.html").read_text(encoding="utf-8")
        self.assertIn("function sortChainsByTotalSupply(chains)", text)
        self.assertIn("sortChainsByTotalSupply(CHAINS)", text)


if __name__ == "__main__":
    unittest.main()
