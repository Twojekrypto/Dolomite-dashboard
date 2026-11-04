import unittest
import io
import json
import tempfile
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from unittest.mock import patch

import generate_supply_health as health


class GenerateSupplyHealthTests(unittest.TestCase):
    def test_arbitrum_bundle_uses_active_deployment(self):
        active = "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn"
        def urlopen(req, timeout):
            data = {"data": {"tokens": [{"id": "token"}], "interestIndexes": [], "oraclePrices": []}} if req.full_url == active else {"errors": [{"message": "deployment does not exist"}]}
            return io.BytesIO(json.dumps(data).encode())
        with patch.object(health.request, "urlopen", side_effect=urlopen), patch.object(health.time, "sleep"):
            tokens, _, _ = health.fetch_bundle(health.GRAPH_ENDPOINTS["arbitrum"])
        self.assertEqual(tokens, [{"id": "token"}])

    def test_failed_chain_does_not_publish_partial_health_as_fresh(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            for name in ("latest.json", "history.json"):
                (out/name).write_text('{"generatedAt":"2026-11-03T00:00:00Z"}\n')
            before = {p.name: p.read_bytes() for p in out.iterdir()}
            def bundle(endpoint):
                if endpoint == health.GRAPH_ENDPOINTS["arbitrum"]:
                    raise RuntimeError("deployment does not exist")
                return ([{"id": "token", "symbol": "USD1", "supplyLiquidityUSD": "1000", "supplyLiquidity": "1000"}], {"token": 1}, {"token": 1})
            with patch("sys.argv", ["generate_supply_health.py", "--out-dir", directory, "--chains", "ethereum,arbitrum"]), patch.object(health, "fetch_bundle", side_effect=bundle), patch.object(health, "fetch_wallet_balances", return_value={"wallet": 1000}), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                result = health.main()
            self.assertEqual(result, 1)
            self.assertEqual({p.name: p.read_bytes() for p in out.iterdir()}, before)

    def test_default_chain_list_skips_archived_chains(self):
        for chain in ("polygon_zkevm", "botanix"):
            self.assertIn(chain, health.GRAPH_ENDPOINTS)
            self.assertIn(chain, health.RETIRED_GRAPH_CHAINS)
            self.assertNotIn(chain, health.DEFAULT_GRAPH_CHAINS)


if __name__ == "__main__":
    unittest.main()
