import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_supply_history as gsh


class GenerateSupplyHistoryTests(unittest.TestCase):
    def test_default_chain_list_skips_retired_polygon_zkevm(self):
        self.assertIn("polygon_zkevm", gsh.GRAPH_ENDPOINTS)
        self.assertIn("polygon_zkevm", gsh.RETIRED_GRAPH_CHAINS)
        self.assertNotIn("polygon_zkevm", gsh.DEFAULT_GRAPH_CHAINS)

    def test_fetch_error_reuses_existing_static_history(self):
        token_id = "0x912ce59144191c1204e64559fe8253a0e49e6548"
        token = {
            "id": token_id,
            "symbol": "ARB",
            "name": "Arbitrum",
            "marketId": "7",
            "supplyLiquidity": "100",
            "supplyLiquidityUSD": "151071.11711705",
            "totalPar": {"supplyPar": "100"},
        }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "data" / "supply-history"
            history_path = out_dir / "arbitrum" / f"{token_id}.json"
            history_path.parent.mkdir(parents=True)
            history_path.write_text(
                json.dumps(
                    {
                        "generatedAt": "2026-06-15T00:00:00Z",
                        "points": [
                            {"timestamp": 1, "value": "1", "tokenValue": "1"},
                            {"timestamp": 2, "value": "2", "tokenValue": "2"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            existing_chain = {
                "chain": "arbitrum",
                "tokens": [
                    {
                        "tokenId": token_id,
                        "symbol": "ARB",
                        "points": 2,
                        "path": history_path.as_posix(),
                    }
                ],
                "skippedTokens": [],
            }

            with patch.object(gsh, "fetch_bundle", return_value=([token], {token_id: Decimal("1")})), patch.object(
                gsh,
                "paginate_entity",
                side_effect=RuntimeError("Graph request failed after 3 attempts: temporary DNS failure"),
            ):
                result = gsh.generate_chain(
                    "arbitrum",
                    "unused-endpoint",
                    out_dir,
                    0,
                    set(),
                    set(),
                    False,
                    600,
                    datetime(2026, 6, 16, tzinfo=timezone.utc),
                    existing_chain,
                )

        self.assertEqual(result["tokensWritten"], 1)
        self.assertEqual(result["tokensSkipped"], 0)
        self.assertEqual(result["skippedTokens"], [])
        self.assertEqual(result["tokens"][0]["tokenId"], token_id)
        self.assertEqual(result["tokens"][0]["points"], 2)
        self.assertTrue(result["tokens"][0]["reusedStaticHistory"])
        self.assertIn("temporary DNS failure", result["tokens"][0]["lastRefreshError"])

    def test_fetch_error_without_existing_history_still_skips_token(self):
        token_id = "0x10393c20975cf177a3513071bc110f7962cd67da"
        token = {
            "id": token_id,
            "symbol": "JONES",
            "marketId": "25",
            "supplyLiquidityUSD": "40.44261352",
        }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "data" / "supply-history"
            with patch.object(gsh, "fetch_bundle", return_value=([token], {token_id: Decimal("1")})), patch.object(
                gsh,
                "paginate_entity",
                side_effect=RuntimeError("Graph request failed after 3 attempts: temporary DNS failure"),
            ):
                result = gsh.generate_chain(
                    "arbitrum",
                    "unused-endpoint",
                    out_dir,
                    0,
                    set(),
                    set(),
                    False,
                    600,
                    datetime(2026, 6, 16, tzinfo=timezone.utc),
                    None,
                )

        self.assertEqual(result["tokensWritten"], 0)
        self.assertEqual(result["tokensSkipped"], 1)
        self.assertEqual(result["skippedTokens"][0]["reason"], "fetch_error")


if __name__ == "__main__":
    unittest.main()
