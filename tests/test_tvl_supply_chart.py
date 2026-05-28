import json
import subprocess
import sys
import unittest
from decimal import Decimal
from pathlib import Path

import fetch_dolomite_tvl
import validate_data


ROOT = Path(__file__).resolve().parents[1]
TVL_PREVIEW = ROOT / "tvl-preview.html"
TVL_ROUTE = ROOT / "tvl" / "index.html"
TVL_FETCHER = ROOT / "fetch_dolomite_tvl.py"
DOLOMITE_TVL = ROOT / "dolomite_tvl.json"
VALIDATOR = ROOT / "validate_data.py"
LIVE_SMOKE = ROOT / "scripts" / "smoke_live_pages.py"
STATIC_TVL_GUARD = ROOT / "scripts" / "verify_tvl_static_guards.py"
PAGES_WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"
EARN_AUDIT_WORKFLOW = ROOT / ".github" / "workflows" / "earn-audit-checks.yml"
EARN_AUDIT_RUNNER = ROOT / "run_earn_audit_checks.py"
UPDATE_TVL_WORKFLOW = ROOT / ".github" / "workflows" / "update-tvl-data.yml"
UPDATE_DATA_WORKFLOW = ROOT / ".github" / "workflows" / "update-data.yml"


class TvlSupplyChartContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preview_source = TVL_PREVIEW.read_text(encoding="utf-8")
        cls.fetcher_source = TVL_FETCHER.read_text(encoding="utf-8")
        cls.validator_source = VALIDATOR.read_text(encoding="utf-8")
        cls.smoke_source = LIVE_SMOKE.read_text(encoding="utf-8")
        cls.static_guard_source = STATIC_TVL_GUARD.read_text(encoding="utf-8")
        cls.pages_workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")
        cls.earn_audit_workflow = EARN_AUDIT_WORKFLOW.read_text(encoding="utf-8")
        cls.earn_audit_runner = EARN_AUDIT_RUNNER.read_text(encoding="utf-8")
        cls.update_tvl_workflow = UPDATE_TVL_WORKFLOW.read_text(encoding="utf-8")
        cls.update_data_workflow = UPDATE_DATA_WORKFLOW.read_text(encoding="utf-8")

    def test_chart_is_labeled_as_supply_and_anchors_current_snapshot(self):
        self.assertIn("<h2>Supply Over Time</h2>", self.preview_source)
        self.assertNotIn("TVL Over Time", self.preview_source)
        self.assertIn("function appendCurrentSupplyPoint", self.preview_source)
        self.assertIn("const history = buildHistory(llamaData, supplyLiquidity, officialData);", self.preview_source)
        self.assertIn("The final point is always anchored to Dolomite's current supply snapshot.", self.preview_source)

    def test_tvl_route_cache_busts_embedded_preview(self):
        route_source = TVL_ROUTE.read_text(encoding="utf-8")
        self.assertIn("supply-chart-20260527", route_source)

    def test_fetcher_uses_same_current_liquidity_source_as_dolomite_stats(self):
        self.assertIn('DOLOMITE_API_SERVER_URL = "https://api.dolomite.io"', self.fetcher_source)
        self.assertIn('f"{DOLOMITE_API_SERVER_URL}/tokens/{chain_id}"', self.fetcher_source)
        self.assertIn('f"{DOLOMITE_API_SERVER_URL}/tokens/{chain_id}/prices"', self.fetcher_source)
        self.assertIn('token.get("cleanSymbol")', self.fetcher_source)
        self.assertNotIn("totalPars(first: 1000)", self.fetcher_source)
        self.assertNotIn("oraclePrices(first: 1000)", self.fetcher_source)

    def test_generated_snapshot_reconciles_net_borrow_and_supply(self):
        data = json.loads(DOLOMITE_TVL.read_text(encoding="utf-8"))
        current = data["currentChainTvls"]
        chain_names = [
            key for key, value in current.items()
            if isinstance(value, (int, float))
            and "-" not in key
            and key.lower() != "borrowed"
        ]
        net_tvl = sum(float(current[chain]) for chain in chain_names)
        borrowed = sum(float(current[f"{chain}-borrowed"]) for chain in chain_names)
        supply = net_tvl + borrowed

        self.assertAlmostEqual(net_tvl, float(data["totalTvl"]), delta=5)
        self.assertAlmostEqual(borrowed, float(data["totalBorrowed"]), delta=5)
        self.assertAlmostEqual(supply, float(data["supplyLiquidity"]), delta=5)

        for chain in chain_names:
            chain_supply = float(current[chain]) + float(current[f"{chain}-borrowed"])
            token_supply = sum(float(value) for value in data["chainTokensInUsd"][chain].values())
            self.assertAlmostEqual(chain_supply, token_supply, delta=max(5, chain_supply * 1e-8))

    def test_wlfi_is_visible_as_a_top_token_composition_asset(self):
        data = json.loads(DOLOMITE_TVL.read_text(encoding="utf-8"))
        tokens = data["tokensInUsd"][-1]["tokens"]
        ranked = sorted(tokens.items(), key=lambda item: item[1], reverse=True)

        self.assertIn("WLFI", tokens)
        self.assertGreater(tokens["WLFI"], 100_000_000)
        self.assertEqual("WLFI", ranked[0][0])
        self.assertIn('"WLFI": DOLO_CDN + "WLFI.', self.preview_source)

    def test_stale_metadata_does_not_hide_official_token_composition(self):
        self.assertIn("const hasOfficialChain = Number(current[name]) > 0;", self.preview_source)
        self.assertIn("const useLlama = !hasOfficialChain && Number(llamaCurrent[name]) > 0;", self.preview_source)
        self.assertIn("const tokens = officialTokens[chain] || llamaTokens[chain];", self.preview_source)
        self.assertNotIn("staleKeys.has(chainKey(chain)) && llamaTokens[chain]", self.preview_source)

    def test_tvl_data_pipeline_refuses_missing_wlfi(self):
        data = json.loads(DOLOMITE_TVL.read_text(encoding="utf-8"))
        global_tokens = {
            symbol: Decimal(str(value))
            for symbol, value in data["tokensInUsd"][-1]["tokens"].items()
        }
        chain_tokens = {
            chain: {
                symbol: Decimal(str(value))
                for symbol, value in tokens.items()
            }
            for chain, tokens in data["chainTokensInUsd"].items()
        }
        fetch_dolomite_tvl.validate_expected_token_guards(global_tokens, chain_tokens)
        self.assertTrue(validate_data._dolomite_expected_tokens_present(data))

        broken_global = dict(global_tokens)
        broken_global.pop("WLFI", None)
        with self.assertRaises(RuntimeError):
            fetch_dolomite_tvl.validate_expected_token_guards(broken_global, chain_tokens)

        broken_data = json.loads(DOLOMITE_TVL.read_text(encoding="utf-8"))
        broken_data["tokensInUsd"][-1]["tokens"].pop("WLFI", None)
        self.assertFalse(validate_data._dolomite_expected_tokens_present(broken_data))

    def test_live_pages_smoke_checks_tvl_wlfi_composition(self):
        self.assertIn("def assert_tvl_composition_live", self.smoke_source)
        self.assertIn("stale-token-fix-20260528", self.smoke_source)
        self.assertIn('const tokens = officialTokens[chain] || llamaTokens[chain];', self.smoke_source)
        self.assertIn('latest.get("WLFI", 0)', self.smoke_source)
        self.assertIn('chain_tokens.get("Ethereum", {}).get("WLFI", 0)', self.smoke_source)
        self.assertIn("assert_tvl_composition_live(base_url)", self.smoke_source)

    def test_static_tvl_guard_blocks_bad_pages_artifacts_before_upload(self):
        self.assertIn("WLFI_MIN_USD = 10_000_000", self.static_guard_source)
        self.assertIn("stale-token-fix-20260528", self.static_guard_source)
        self.assertIn("const tokens = officialTokens[chain] || llamaTokens[chain];", self.static_guard_source)
        self.assertIn("staleKeys.has(chainKey(chain)) && llamaTokens[chain]", self.static_guard_source)

        guard_index = self.pages_workflow.index("Validate TVL static guard")
        upload_index = self.pages_workflow.index("Upload Pages artifact")
        self.assertLess(guard_index, upload_index)
        self.assertIn("python3 scripts/verify_tvl_static_guards.py", self.pages_workflow)

        proc = subprocess.run(
            [sys.executable, str(STATIC_TVL_GUARD)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_tvl_files_trigger_audit_workflow(self):
        for path in (
            ".github/workflows/pages.yml",
            ".github/workflows/update-data.yml",
            ".github/workflows/update-tvl-data.yml",
            "fetch_defillama.py",
            "fetch_dolomite_tvl.py",
            "validate_data.py",
            "tvl-preview.html",
            "tvl/index.html",
            "scripts/verify_tvl_static_guards.py",
        ):
            self.assertIn(f"- '{path}'", self.earn_audit_workflow)

        for path in (
            'ROOT / "fetch_defillama.py"',
            'ROOT / "fetch_dolomite_tvl.py"',
            'ROOT / "validate_data.py"',
            'ROOT / "scripts" / "verify_tvl_static_guards.py"',
        ):
            self.assertIn(path, self.earn_audit_runner)

    def test_tvl_update_workflows_fail_if_push_never_succeeds(self):
        for workflow in (self.update_tvl_workflow, self.update_data_workflow):
            self.assertIn("pushed=false", workflow)
            self.assertIn("for i in $(seq 1 12)", workflow)
            self.assertIn("git pull --rebase -X theirs origin master && git push", workflow)
            self.assertIn("Failed to push after 12 attempts.", workflow)
            self.assertIn('if [ "$pushed" != "true" ]; then', workflow)
            self.assertIn("exit 1", workflow)


if __name__ == "__main__":
    unittest.main()
