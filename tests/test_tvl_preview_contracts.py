import json
import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TVL_PREVIEW = ROOT / "tvl-preview.html"
TVL_ROUTE = ROOT / "tvl" / "index.html"
TVL_WORKFLOW = ROOT / ".github" / "workflows" / "update-tvl-data.yml"


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
    def test_tvl_workflow_has_redundant_schedule_and_push_retry_budget(self):
        workflow = TVL_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("ref: master", workflow)
        self.assertIn("fetch-depth: 1", workflow)
        self.assertIn("cron: '13 * * * *'", workflow)
        self.assertIn("cron: '43 * * * *'", workflow)
        self.assertIn("for i in $(seq 1 12)", workflow)
        self.assertIn("Failed to push after 12 attempts.", workflow)

    def test_tvl_route_busts_preview_cache_for_wlfi_fallback_fix(self):
        route = TVL_ROUTE.read_text(encoding="utf-8")
        self.assertIn("wlfi-fallback-20260616", route)

    def test_snapshot_keeps_net_tvl_and_total_supply_histories_distinct(self):
        result = run_tvl_js_probe(
            textwrap.dedent(
                """
                const officialData = {
                  currentChainTvls: {
                    Arbitrum: 200,
                    "Arbitrum-borrowed": 50,
                    borrowed: 50,
                  },
                  chainTokensInUsd: { Arbitrum: { USDC: 250 } },
                  tokensInUsd: [{ tokens: { USDC: 250 } }],
                  chainMeta: { Arbitrum: { blockTimestamp: Date.now() / 1000 } },
                  freshnessMaxAgeSeconds: 21600,
                  staleChains: [],
                  supplyLiquidity: 250,
                  totalTvl: 200,
                  totalBorrowed: 50,
                  last_updated: "2026-07-23T00:00:00Z",
                };
                const llamaData = {
                  currentChainTvls: {
                    Arbitrum: 200,
                    "Arbitrum-borrowed": 50,
                    borrowed: 50,
                  },
                  tvl: [
                    { date: 10, totalLiquidityUSD: 100 },
                    { date: 20, totalLiquidityUSD: 200 },
                  ],
                  totalSupply: [
                    { date: 10, totalLiquidityUSD: 140 },
                    { date: 20, totalLiquidityUSD: 250 },
                  ],
                  chainTokensInUsd: { Arbitrum: { USDC: 250 } },
                  tokensInUsd: [{ tokens: { USDC: 250 } }],
                  last_updated: "2026-07-23T00:00:00Z",
                };
                const snapshot = buildTvlSnapshot(llamaData, officialData);
                process.stdout.write(JSON.stringify({
                  tvlHistory: snapshot.history,
                  supplyHistory: snapshot.supplyHistory,
                }));
                """
            )
        )
        self.assertEqual(result["tvlHistory"], [[10, 100], [20, 200]])
        self.assertIn("supplyHistory", result)
        self.assertEqual(result["supplyHistory"], [[10, 140], [20, 250]])

    def test_snapshot_prefers_full_official_total_supply_history(self):
        result = run_tvl_js_probe(
            textwrap.dedent(
                """
                const officialData = {
                  currentChainTvls: {
                    Arbitrum: 200,
                    "Arbitrum-borrowed": 100,
                    borrowed: 100,
                  },
                  chainTokensInUsd: { Arbitrum: { USDC: 300 } },
                  tokensInUsd: [{ tokens: { USDC: 300 } }],
                  chainMeta: { Arbitrum: { blockTimestamp: Date.now() / 1000 } },
                  freshnessMaxAgeSeconds: 21600,
                  staleChains: [],
                  supplyLiquidity: 300,
                  totalTvl: 200,
                  totalBorrowed: 100,
                  last_updated: "2026-07-23T00:00:00Z",
                };
                const llamaData = {
                  currentChainTvls: {
                    Arbitrum: 200,
                    "Arbitrum-borrowed": 100,
                    borrowed: 100,
                  },
                  tvl: [
                    { date: 10, totalLiquidityUSD: 100 },
                    { date: 20, totalLiquidityUSD: 200 },
                  ],
                  totalSupply: [
                    { date: 10, totalLiquidityUSD: 140 },
                    { date: 20, totalLiquidityUSD: 250 },
                  ],
                  chainTokensInUsd: { Arbitrum: { USDC: 250 } },
                  tokensInUsd: [{ tokens: { USDC: 250 } }],
                  last_updated: "2026-07-23T00:00:00Z",
                };
                const fullSupplyData = {
                  totalSupply: [
                    { date: 10, totalLiquidityUSD: 180 },
                    { date: 20, totalLiquidityUSD: 300 },
                  ],
                  currentSupply: 300,
                  last_updated: "2026-07-23T00:00:00Z",
                };
                const snapshot = buildTvlSnapshot(
                  llamaData,
                  officialData,
                  fullSupplyData,
                );
                process.stdout.write(JSON.stringify({
                  tvlHistory: snapshot.history,
                  supplyHistory: snapshot.supplyHistory,
                }));
                """
            )
        )
        self.assertEqual(result["tvlHistory"], [[10, 100], [20, 200]])
        self.assertEqual(result["supplyHistory"], [[10, 180], [20, 300]])

    def test_legacy_history_payload_keeps_net_tvl_fallback_distinct(self):
        result = run_tvl_js_probe(
            textwrap.dedent(
                """
                const officialData = {
                  currentChainTvls: {
                    Arbitrum: 200,
                    "Arbitrum-borrowed": 50,
                    borrowed: 50,
                  },
                  chainTokensInUsd: { Arbitrum: { USDC: 250 } },
                  tokensInUsd: [{ tokens: { USDC: 250 } }],
                  chainMeta: { Arbitrum: { blockTimestamp: Date.now() / 1000 } },
                  freshnessMaxAgeSeconds: 21600,
                  staleChains: [],
                  supplyLiquidity: 250,
                  totalTvl: 200,
                  totalBorrowed: 50,
                  last_updated: "2026-07-23T00:00:00Z",
                };
                const legacyLlamaData = {
                  currentChainTvls: {
                    Arbitrum: 200,
                    "Arbitrum-borrowed": 50,
                    borrowed: 50,
                  },
                  tvl: [
                    { date: 10, totalLiquidityUSD: 140 },
                    { date: 20, totalLiquidityUSD: 250 },
                  ],
                  chainTokensInUsd: { Arbitrum: { USDC: 250 } },
                  tokensInUsd: [{ tokens: { USDC: 250 } }],
                  last_updated: "2026-07-23T00:00:00Z",
                };
                const legacySnapshot = buildTvlSnapshot(legacyLlamaData, officialData);
                const unavailableSnapshot = buildTvlSnapshot({}, officialData);
                process.stdout.write(JSON.stringify({
                  legacyNet: legacySnapshot.history,
                  legacySupply: legacySnapshot.supplyHistory,
                  unavailableNet: unavailableSnapshot.history,
                  unavailableSupply: unavailableSnapshot.supplyHistory,
                }));
                """
            )
        )
        self.assertGreater(len(result["legacyNet"]), 100)
        self.assertNotEqual(result["legacyNet"], result["legacySupply"])
        self.assertEqual(result["legacySupply"], [[10, 140], [20, 250]])
        self.assertEqual(result["unavailableNet"], result["legacyNet"])
        self.assertNotEqual(
            result["unavailableSupply"],
            result["unavailableNet"],
        )

    def test_total_supply_chart_precedes_tvl_with_independent_brush_contract(self):
        text = TVL_PREVIEW.read_text(encoding="utf-8")

        self.assertIn("<h2>Total Supply Over Time</h2>", text)
        self.assertLess(
            text.index("<h2>Total Supply Over Time</h2>"),
            text.index("<h2>TVL Over Time</h2>"),
        )
        for element_id in (
            "supplyRangeBadge",
            "supplyChartWrap",
            "supplyChart",
            "supplyChartLine",
            "supplyChartArea",
            "supplyChartTip",
            "supplyBrushSvg",
            "supplyBrushLine",
            "supplyBrushArea",
            "supplyBrushOverlay",
            "supplyBrushWindow",
            "supplyBrushHandleL",
            "supplyBrushHandleR",
            "supplyBrushLabel",
        ):
            self.assertIn(f'id="{element_id}"', text)
        self.assertIn("function createHistoryChart(config)", text)
        self.assertIn('history: ()=>TOTAL_SUPPLY_HISTORY', text)
        self.assertIn('history: ()=>TVL_HISTORY', text)
        self.assertIn('rangeBadge: "supplyRangeBadge"', text)
        self.assertIn('rangeBadge: "rangeBadge"', text)

    def test_tvl_route_busts_preview_cache_for_dual_history_charts(self):
        route = TVL_ROUTE.read_text(encoding="utf-8")
        self.assertIn("dual-history-20260723", route)

    def test_tvl_route_and_workflow_publish_full_total_supply_history(self):
        route = TVL_ROUTE.read_text(encoding="utf-8")
        workflow = TVL_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("full-total-supply-20260723", route)
        self.assertIn("python3 fetch_dolomite_total_supply_history.py", workflow)
        self.assertIn("dolomite_total_supply_history.json", workflow)

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

    def test_all_chains_filter_leaves_individual_chains_unchecked(self):
        text = TVL_PREVIEW.read_text(encoding="utf-8")
        self.assertIn("let tokenState = { chains: new Set(), focus: null };", text)
        self.assertIn('const checked = !allActive && tokenState.chains.has(c.key);', text)
        self.assertNotIn("(allActive && n>0)", text)
        self.assertIn('tokenState.chains = new Set();', text)


if __name__ == "__main__":
    unittest.main()
