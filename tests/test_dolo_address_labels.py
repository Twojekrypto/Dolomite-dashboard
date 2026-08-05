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

    def test_all_core_team_wallets_are_independent_confirmed_labels(self):
        team_addresses = [
            "0x185000fb4d98acea1a771db3714a431f7fe51cac",
            "0x882e6d630a8c70a6a8d29a4d33f960b1267aa3d1",
            "0x6c97802a1815bc459004c686560244de28a2de75",
            "0x665b3ce67daeb5b19d4d14bbdb6297da0ffa5bf1",
            "0x3f1f155949f32fa3e688093d176a1b5a72c488eb",
            "0x871b0afcd3fd44f4c0071d4ade68ce40d0d6bbcc",
            "0xf2b42104b5ac0b3145a5e18b84aa3fd76d0fdeec",
            "0xb8ba44d5e46562ec011ade12603f353b18089a41",
        ]
        for index, address in enumerate(team_addresses, start=1):
            with self.subTest(address=address):
                info = self.labels[address]
                self.assertEqual(info["label"], f"Core Team {index}")
                self.assertEqual(info["type"], "protocol")
                self.assertTrue(info["treasury"])
                self.assertEqual(info["source"], "coingecko-tokenomics")
                self.assertEqual(info["confidence"], "confirmed")

        strategic_claims = self.labels["0x7efd088ae500598a19a242d6d48b9f7e0d061176"]
        self.assertEqual(strategic_claims["label"], "Strategic Investor Claims")
        self.assertEqual(strategic_claims["type"], "protocol")
        self.assertEqual(strategic_claims["source"], "dolomite-docs-module-dolo")

    def test_published_investor_payload_does_not_duplicate_investors_as_team(self):
        payload = json.loads((ROOT / "vesting_investors.json").read_text())
        self.assertGreater(len(payload["early_investors"]), 0)
        self.assertGreater(len(payload["investors"]), 0)
        self.assertEqual(payload["team"], [])

    def test_wallet_table_pages_use_the_shared_vesting_label_loader(self):
        expected_consumers = {
            "dashboard-core.js": "window.loadDoloVestingLabels(DOLO_ADDR_LABELS)",
            "dolo-preview.html": "window.loadDoloVestingLabels(ADDR_LABELS)",
            "liquidation-preview.html": "window.loadDoloVestingLabels(DOLO_ADDR_LABELS)",
            "odolo-preview.html": "window.loadDoloVestingLabels(ADDRESS_META)",
            "revenue-preview.html": "window.loadDoloVestingLabels(VEBORROW_WALLET_LABELS)",
            "vedolo-preview.html": "window.loadDoloVestingLabels(VEDOLO_ADDRESS_LABELS)",
        }
        for filename, marker in expected_consumers.items():
            with self.subTest(filename=filename):
                self.assertIn(marker, (ROOT / filename).read_text())

    def test_protocol_safe_keeps_dolomite_label(self):
        info = self.labels["0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4"]
        self.assertEqual(info["label"], "Dolomite Gnosis Safe")
        self.assertEqual(info["type"], "protocol")
        self.assertTrue(info["treasury"])
        self.assertTrue(info["safe"])
        self.assertEqual(info["source"], "dolomite-docs-core-proxies")
        self.assertEqual(info["confidence"], "confirmed")
        for row in self.labels.values():
            if row["type"] == "protocol":
                self.assertNotEqual(row["label"], "Gnosis Safe Multisig")

    def test_holder_distribution_chart_does_not_append_mismatched_live_bucket(self):
        html = (ROOT / "dolo-preview.html").read_text()
        self.assertNotIn("|| bucketMismatch", html)
        self.assertIn("&& !bucketMismatch", html)
        self.assertIn("false end-of-chart jump", html)

    def test_holder_details_can_use_exact_history_wallet_snapshots(self):
        html = (ROOT / "dolo-preview.html").read_text()
        generator = (ROOT / "generate_dolo_flows.py").read_text()
        self.assertIn("holder_wallet_history", html)
        self.assertIn("holderHistoricalWalletRowsAtPoint", html)
        self.assertIn("holderWalletRowsAvailableAtPoint", html)
        self.assertIn("if(changes === null) return [];", html)
        self.assertIn("holderWalletBaselinePointForRange(rawBaselinePoint", html)
        self.assertIn("holder_wallet_history", generator)
        self.assertIn("HOLDER_WALLET_HISTORY_VIEWS", generator)

    def test_dolo_hero_uses_price_freshness_not_holder_timestamp(self):
        html = (ROOT / "dolo-preview.html").read_text()
        self.assertIn("Price updating", html)
        self.assertIn("function syncPriceMeta", html)
        self.assertIn("Price updated · ", html)
        self.assertIn("Data updated · ", html)
        self.assertIn("syncPriceMeta(data.last_updated)", html)
        self.assertIn("syncLiveMeta(data.timestamp)", html)
        sync_body = re.search(r"function syncLiveMeta\(timestamp\)\{(?P<body>.*?)\n  \}", html, re.S)
        self.assertIsNotNone(sync_body)
        self.assertNotIn(".hero-live", sync_body.group("body"))

    def test_flow_and_fresh_meta_do_not_duplicate_visible_period_filters(self):
        dolo_html = (ROOT / "dolo-preview.html").read_text()
        odolo_html = (ROOT / "odolo-preview.html").read_text()
        self.assertIn('id="flows-meta">Flow data</span>', dolo_html)
        self.assertIn('id="flows-meta"><span class="pulse"></span>Loading data update…</div>', odolo_html)
        self.assertIn('id="fresh-wallets-meta">Loading data update…</span>', dolo_html)
        self.assertIn('<div class="sub">10K+ exposure</div>', dolo_html)
        self.assertNotIn('id="flows-meta">Period:', dolo_html)
        self.assertNotIn('id="flows-meta">Period:', odolo_html)
        self.assertNotIn('Period: " + (meta ? meta.short', dolo_html)
        self.assertNotIn('Period: " + (meta ? meta.short', odolo_html)
        self.assertNotIn('`${meta.short} · first on-chain tx', dolo_html)
        self.assertNotIn('metaEl.textContent = "First on-chain tx + 10K+ exposure"', dolo_html)
        self.assertIn('setCardMeta("flows-meta", flowUpdated)', odolo_html)

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

    def test_behavioral_bot_labels_are_confirmed(self):
        expected = {
            "0xb05ba67c11fc0b92a7bcb18e50cba87ce2d15109": "Bot / MM",
            "0x5a6f918fcda24e9b5143f3a1b77e63df6de30f74": "Bot / MM",
            "0x6a2383cff0d46d2b7d29759f17c26fba726f3ea3": "Bot / MM",
            "0x601d9ad1b431577e3635a23b7eb3bcc46bcc648b": "Bot / MM",
            "0xf10f81795b359f8a72682cc2a39444bf818ef4ca": "Bot / MM",
            "0x278d858f05b94576c1e6f73285886876ff6ef8d2": "Bot (CA)",
            "0x9cb677f2a8daa9511ae79c2ba56395552b5d030d": "Active Trader/Bot",
        }
        for address, label in expected.items():
            with self.subTest(address=address):
                info = self.labels[address]
                self.assertEqual(info["label"], label)
                self.assertEqual(info["type"], "bot")
                self.assertEqual(info["source"], "behavioral-label")
                self.assertEqual(info["confidence"], "confirmed")

        generator = (ROOT / "generate_dolo_flows.py").read_text()
        self.assertIn("0xf10f81795b359f8a72682cc2a39444bf818ef4ca", generator)

    def test_public_explorer_labels_are_current(self):
        expected = {
            "0x06fd4ba7973a0d39a91734bbc35bc2bcaa99e3b0": ("Binance Deposit", "cex"),
            "0x000000000004444c5dc75cb358380d2e3de08a90": ("Uniswap V4 Pool Manager", "contract"),
            "0xf977814e90da44bfa03b6295a0616a897441acec": ("Binance Hot Wallet 20", "cex"),
            "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": ("Coinbase 10", "cex"),
            "0xf89d7b9c864f589bbf53a82105107622b35eaa40": ("Bybit Hot Wallet", "cex"),
            "0x0529ea5885702715e83923c59746ae8734c553b7": ("Bitpanda 18", "cex"),
            "0xd2dd7b597fd2435b6db61ddf48544fd931e6869f": ("Kraken 246", "cex"),
            "0x21a31ee1afc51d94c2efccaa2092ad1028285549": ("Binance 15", "cex"),
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": ("Binance 16", "cex"),
            "0x0d0707963952f2fba59dd06f2b425ace40b492fe": ("Gate.io Deposit", "cex"),
            "0x1157a2076b9bb22a85cc2c162f20fab3898f4101": ("FalconX 1", "cex"),
            "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23": ("Bitget 6", "cex"),
            "0xe302dc0c3b74ae06da002fc66c2ee8a40cce256d": ("Gate.io Deposit", "cex"),
            "0xb46a0f71fded6ceb00b1c9278e0ac7e22c242df5": ("Binance Deposit", "cex"),
            "0x2933782b5a8d72f2754103d1489614f29bfa4625": ("KuCoin Wallet", "cex"),
            "0xdb861e302ef7b7578a448e951aede06302936c28": ("Phemex 1", "cex"),
            "0xf2067abfab8bc621211935431519d41825d2f344": ("Coinstore 3", "cex"),
            "0xcd531ae9efcce479654c4926dec5f6209531ca7b": ("Coinbase Prime 1", "cex"),
            "0x43684d03d81d3a4c70da68febdd61029d426f042": ("Binance 117", "cex"),
            "0x28c6c06298d514db089934071355e5743bf21d60": ("Binance 14", "cex"),
            "0x98adef6f2ac8572ec48965509d69a8dd5e8bba9d": ("Binance 93", "cex"),
            "0x2b3bf74b29f59fb8dda41cf3d6a8da28cf8e7921": ("BingX 29", "cex"),
            "0xb8001c3ec9aa1985f6c747e25c28324e4a361ec1": ("Cobo Custody 1", "cex"),
            "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": ("Binance 7", "cex"),
            "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": ("Binance 17", "cex"),
            "0x9696f59e4d72e237be84ffd425dcad154bf96976": ("Binance 18", "cex"),
            "0x9642b23ed1e01df1092b92641051881a322f5d4e": ("MEXC 16", "cex"),
            "0x035ae7d933dcbfe617ffba194a88af0c2867b90c": ("Gate.io Deposit", "cex"),
            "0x8782163068c7cd74d2510768a61135c1e4eb07b3": ("Gate.io Deposit", "cex"),
            "0x478271c960137418c9d90082dddbf68aa72e4367": ("Gate.io Deposit", "cex"),
            "0x753f43cca3dbd2bbe963609f8d25a833433a10c9": ("Gate.io Deposit", "cex"),
            "0x4ed6cf63bd9c009d247ee51224fc1c7041f517f1": ("Ceffu 6", "cex"),
            "0xef7960eb0b732f70007f7c05daf6bb597aa742d6": ("Binance Deposit", "cex"),
            "0x7ff8bbf9c8ab106db589e7863fb100525f61cce5": ("BloFin Hot Wallet 1", "cex"),
            "0x9b0c45d46d386cedd98873168c36efd0dcba8d46": ("Revolut 3", "cex"),
            "0xb8dbf21f34761347ccf1ec0c3bd8cb3fd41e155f": ("Gate.io Deposit", "cex"),
            "0x5576132eb74518fb27de2123ed3e6b1d43e0dac7": ("Gate.io Deposit", "cex"),
            "0x3e6f0c06dcca325f8f9175c1802927073394c67c": ("Gate.io Deposit", "cex"),
            "0x039ac6fbcebf02b54259bc690e13540693d9eb8d": ("Gate.io Deposit", "cex"),
            "0xf6012b3d6e669f0fdb58d1f62c2e6fe56c1b1625": ("Gate.io Deposit", "cex"),
            "0x850c198d2469b569091211fb5f62ff5d5627fbf0": ("Gate.io Deposit", "cex"),
            "0xb2655ac91bb3536bcfa0993069da6affabadc33d": ("Gate.io Deposit", "cex"),
            "0xb2b99d9879dd29b4b8590087ae991eaf3808984f": ("Gate.io Deposit", "cex"),
            "0xc17a40852e4bfe04bc81af355fdf132c539ba753": ("Binance Deposit", "cex"),
        }
        for address, (label, label_type) in expected.items():
            with self.subTest(address=address):
                info = self.labels[address]
                self.assertEqual(info["label"], label)
                self.assertEqual(info["type"], label_type)
                self.assertEqual(info["source"], "etherscan-public-label")
                self.assertEqual(info["confidence"], "confirmed")

    def test_dolo_search_surfaces_include_label_type(self):
        html = (ROOT / "dolo-preview.html").read_text()
        self.assertIn('${h.type || ""} ${TYPE_LABELS[h.type] || ""}', html)
        self.assertIn('${r.type || ""} ${TYPE_LABELS[r.type] || ""}', html)
        self.assertIn('${row.type || ""} ${TYPE_LABELS[row.type] || ""}', html)

    def test_mexc_flow_audit_wallet_is_cex_not_market_holder(self):
        info = self.labels["0xd5c342acbeedef81ab8e6072323bfda76172d05f"]
        self.assertEqual(info["label"], "MEXC Wallet")
        self.assertEqual(info["type"], "cex")
        self.assertEqual(info["source"], "flow-audit")
        self.assertEqual(info["confidence"], "confirmed")

    def test_mexc_wallet_is_not_in_holder_wallet_history(self):
        address = "0xd5c342acbeedef81ab8e6072323bfda76172d05f"
        with (ROOT / "dolo_flows.json").open() as f:
            flows = json.load(f)
        rows = []
        for point_key, point in (flows.get("holder_wallet_history") or {}).items():
            for view in ("liquid", "with_vedolo"):
                for group, group_rows in (point.get(view) or {}).items():
                    for row in group_rows:
                        if str(row.get("address", "")).lower() == address:
                            rows.append((point_key, view, group, row.get("label"), row.get("type")))
        self.assertEqual(rows, [])

    def test_active_berachain_strategy_label_is_potential(self):
        info = self.labels["0x0fb6bac552b7a29a21b4e595b1ef5c371cda4f9d"]
        self.assertEqual(info["label"], "Potential Berachain Strategy/MM")
        self.assertEqual(info["type"], "watch")
        self.assertEqual(info["source"], "heuristic-flow-pattern")
        self.assertEqual(info["confidence"], "potential")

    def test_chainlink_rewards_claim_contract_is_protocol_reward(self):
        info = self.labels["0x2f41d42de3eab9e75f3d417259f24421771fb700"]
        self.assertEqual(info["label"], "Chainlink Rewards Claim")
        self.assertEqual(info["type"], "protocol")
        self.assertTrue(info["treasury"])
        self.assertEqual(info["source"], "etherscan-buildclaim")
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
            if path.name == "dashboard-core.html":
                # Main dashboard script was extracted to dashboard-core.js.
                extracted = ROOT / "dashboard-core.js"
                if extracted.exists():
                    source += "\n" + extracted.read_text(encoding="utf-8")
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
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for filename in removed_files:
                self.assertNotIn(filename, text, str(path.relative_to(ROOT)))


if __name__ == "__main__":
    unittest.main()
