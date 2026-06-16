import json
import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TVL_PREVIEW = ROOT / "tvl-preview.html"
TVL_ROUTE = ROOT / "tvl" / "index.html"


def run_tvl_js_probe(probe):
    script = f"""
    const fs = require("fs");
    const vm = require("vm");
    const html = fs.readFileSync({json.dumps(str(TVL_PREVIEW))}, "utf8");
    const match = html.match(/<script>([\\s\\S]*)<\\/script>\\s*<\\/body>\\s*<\\/html>\\s*$/);
    if (!match) throw new Error("main TVL script not found");
    const source = match[1].replace(/\\nbootTvlDashboard\\(\\);\\s*$/, "\\n");
    const context = {{
      console: {{ log() {{}}, warn() {{}}, error() {{}} }},
      Date,
      JSON,
      Math,
      Number,
      Object,
      Array,
      Set,
      Map,
      String,
      window: {{}},
    }};
    vm.createContext(context);
    vm.runInContext(source, context, {{ filename: "tvl-preview.html" }});
    if (typeof context.buildTvlSnapshot !== "function") {{
      throw new Error("buildTvlSnapshot was not exposed");
    }}
    const buildTvlSnapshot = context.buildTvlSnapshot;
    {probe}
    """
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr or result.stdout)
    return json.loads(result.stdout)


class TvlPreviewContractsTest(unittest.TestCase):
    def test_tvl_route_busts_preview_cache_for_wlfi_fallback_fix(self):
        route = TVL_ROUTE.read_text(encoding="utf-8")
        self.assertIn("wlfi-fallback-20260616", route)

    def test_missing_wlfi_in_defillama_does_not_replace_official_supply(self):
        result = run_tvl_js_probe(
            textwrap.dedent(
                """
                const officialData = {
                  currentChainTvls: {
                    Ethereum: 350000000,
                    "Ethereum-borrowed": 50000000,
                    borrowed: 50000000,
                  },
                  chainTokensInUsd: {
                    Ethereum: {
                      WLFI: 296000000,
                      USD1: 104000000,
                    },
                  },
                  tokensInUsd: [{ tokens: { WLFI: 296000000, USD1: 104000000 } }],
                  chainMeta: { Ethereum: { blockTimestamp: 1 } },
                  freshnessMaxAgeSeconds: 1,
                  staleChains: ["Ethereum"],
                  supplyLiquidity: 400000000,
                  totalTvl: 350000000,
                  totalBorrowed: 50000000,
                  last_updated: "2026-06-01T00:00:00Z",
                };
                const llamaData = {
                  currentChainTvls: {
                    Ethereum: 100000000,
                    "Ethereum-borrowed": 50000000,
                    borrowed: 50000000,
                  },
                  chainTokensInUsd: {
                    Ethereum: { USD1: 150000000 },
                  },
                  tokensInUsd: [{ tokens: { USD1: 150000000 } }],
                  tvl: [],
                  last_updated: "2026-06-01T01:00:00Z",
                };
                const snapshot = buildTvlSnapshot(llamaData, officialData);
                const eth = snapshot.chains.find(chain => chain.name === "Ethereum");
                process.stdout.write(JSON.stringify({
                  totalSupply: snapshot.totalSupply,
                  hasWlfi: snapshot.tokens.some(token => token.sym === "WLFI" && token.usd === 296000000),
                  ethSupply: eth && eth.tvl + eth.borrowed,
                }));
                """
            )
        )
        self.assertEqual(result["totalSupply"], 400000000)
        self.assertTrue(result["hasWlfi"])
        self.assertEqual(result["ethSupply"], 400000000)

    def test_complete_defillama_stale_fallback_still_works(self):
        result = run_tvl_js_probe(
            textwrap.dedent(
                """
                const officialData = {
                  currentChainTvls: {
                    Arbitrum: 100000000,
                    "Arbitrum-borrowed": 10000000,
                    borrowed: 10000000,
                  },
                  chainTokensInUsd: {
                    Arbitrum: { USDC: 90000000, WETH: 20000000 },
                  },
                  tokensInUsd: [{ tokens: { USDC: 90000000, WETH: 20000000 } }],
                  chainMeta: { Arbitrum: { blockTimestamp: 1 } },
                  freshnessMaxAgeSeconds: 1,
                  staleChains: ["Arbitrum"],
                  supplyLiquidity: 110000000,
                  totalTvl: 100000000,
                  totalBorrowed: 10000000,
                  last_updated: "2026-06-01T00:00:00Z",
                };
                const llamaData = {
                  currentChainTvls: {
                    Arbitrum: 105000000,
                    "Arbitrum-borrowed": 10000000,
                    borrowed: 10000000,
                  },
                  chainTokensInUsd: {
                    Arbitrum: { USDC: 95000000, WETH: 20000000 },
                  },
                  tokensInUsd: [{ tokens: { USDC: 95000000, WETH: 20000000 } }],
                  tvl: [],
                  last_updated: "2026-06-01T01:00:00Z",
                };
                const snapshot = buildTvlSnapshot(llamaData, officialData);
                const arb = snapshot.chains.find(chain => chain.name === "Arbitrum");
                process.stdout.write(JSON.stringify({
                  totalSupply: snapshot.totalSupply,
                  usdc: snapshot.tokens.find(token => token.sym === "USDC")?.usd,
                  arbSupply: arb && arb.tvl + arb.borrowed,
                }));
                """
            )
        )
        self.assertEqual(result["totalSupply"], 115000000)
        self.assertEqual(result["usdc"], 95000000)
        self.assertEqual(result["arbSupply"], 115000000)


if __name__ == "__main__":
    unittest.main()
