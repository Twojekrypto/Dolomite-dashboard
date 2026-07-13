import os
import sys
import unittest
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fetch_liquidation_risk as flr


def tv(symbol, value_par, token_id=None, decimals="18", market_id="1"):
    return {
        "token": {
            "id": token_id or f"0x{symbol.lower()}",
            "symbol": symbol,
            "decimals": decimals,
            "marketId": market_id,
        },
        "valuePar": value_par,
    }


class TestSafeFloat(unittest.TestCase):
    def test_parses_strings(self):
        self.assertEqual(flr.safe_float("1.5"), 1.5)

    def test_default_on_garbage(self):
        self.assertEqual(flr.safe_float(None), 0.0)
        self.assertEqual(flr.safe_float("abc", 7.0), 7.0)


class TestPositionShards(unittest.TestCase):
    def test_writes_chain_and_address_prefix_shards(self):
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = {
                "generatedAtISO": "2026-07-13T00:00:00Z",
                "chainStats": {"arbitrum": {"positions": 2}},
                "positions": [
                    {"chain": "arbitrum", "address": "0xab11111111111111111111111111111111111111", "debtUSD": 10},
                    {"chain": "arbitrum", "address": "0xcd11111111111111111111111111111111111111", "debtUSD": 20},
                ],
            }

            manifest = flr.write_liquidation_risk_shards(payload, root)

            self.assertEqual(manifest["chains"]["arbitrum"]["positionCount"], 2)
            self.assertTrue((root / "arbitrum" / "ab.json").is_file())
            shard = json.loads((root / "arbitrum" / "ab.json").read_text(encoding="utf-8"))
            self.assertEqual(len(shard["positions"]), 1)
            self.assertEqual(shard["positions"][0]["debtUSD"], 10)


class TestComputeHealthFactor(unittest.TestCase):
    def test_simple_position(self):
        # 300 USD collateral, 100 USD debt, liq ratio 1.15 → HF = 300 / 115
        result = flr.compute_health_factor(
            token_values=[tv("WETH", "300"), tv("USDC", "-100", token_id="0xusdc")],
            oracle_prices={"0xweth": "1", "0xusdc": "1"},
            interest_indices={},
            market_risk_infos={},
            liquidation_ratio="1.15",
        )
        self.assertAlmostEqual(result["healthFactor"], 300 / 115, places=12)
        self.assertAlmostEqual(result["collateralUSD"], 300.0)
        self.assertAlmostEqual(result["debtUSD"], 100.0)
        self.assertEqual([t["symbol"] for t in result["collateralTokens"]], ["WETH"])
        self.assertEqual([t["symbol"] for t in result["debtTokens"]], ["USDC"])

    def test_interest_indices_scale_par(self):
        # collateral wei = par * supplyIndex, debt wei = |par| * borrowIndex
        result = flr.compute_health_factor(
            token_values=[tv("WETH", "100"), tv("USDC", "-100", token_id="0xusdc")],
            oracle_prices={"0xweth": "1", "0xusdc": "1"},
            interest_indices={
                "0xweth": {"supplyIndex": "1.10", "borrowIndex": "1"},
                "0xusdc": {"supplyIndex": "1", "borrowIndex": "1.20"},
            },
            market_risk_infos={},
            liquidation_ratio="1",
        )
        self.assertAlmostEqual(result["collateralUSD"], 110.0)
        self.assertAlmostEqual(result["debtUSD"], 120.0)
        self.assertAlmostEqual(result["healthFactor"], 110 / 120, places=12)

    def test_margin_premium_discounts_collateral_and_amplifies_debt(self):
        result = flr.compute_health_factor(
            token_values=[tv("ALT", "200"), tv("USDC", "-100", token_id="0xusdc")],
            oracle_prices={"0xalt": "1", "0xusdc": "1"},
            interest_indices={},
            market_risk_infos={
                "0xalt": {"marginPremium": "0.25"},
                "0xusdc": {"marginPremium": "0.10"},
            },
            liquidation_ratio="1",
        )
        # collateral weight 1/1.25 = 0.8 → 160; debt weight 1.10 → 110
        self.assertAlmostEqual(result["healthFactor"], 160 / 110, places=12)
        # reported USD values stay unweighted
        self.assertAlmostEqual(result["collateralUSD"], 160.0)
        self.assertAlmostEqual(result["debtUSD"], 110.0)

    def test_emode_override_zeroes_premiums_and_sets_ratio(self):
        result = flr.compute_health_factor(
            token_values=[tv("ALT", "200"), tv("USDC", "-100", token_id="0xusdc")],
            oracle_prices={"0xalt": "1", "0xusdc": "1"},
            interest_indices={},
            market_risk_infos={
                "0xalt": {"marginPremium": "0.25"},
                "0xusdc": {"marginPremium": "0.10"},
            },
            liquidation_ratio="1.15",
            margin_ratio_override=0.05,
        )
        # premiums ignored, liq ratio = 1.05 → HF = 200 / (100*1.05)
        self.assertAlmostEqual(result["healthFactor"], 200 / 105, places=12)

    def test_no_debt_returns_none(self):
        result = flr.compute_health_factor(
            token_values=[tv("WETH", "300")],
            oracle_prices={"0xweth": "1"},
            interest_indices={},
            market_risk_infos={},
            liquidation_ratio="1.15",
        )
        self.assertIsNone(result["healthFactor"])

    def test_zero_price_token_skipped(self):
        result = flr.compute_health_factor(
            token_values=[tv("GHOST", "1000"), tv("WETH", "50"),
                          tv("USDC", "-25", token_id="0xusdc")],
            oracle_prices={"0xweth": "1", "0xusdc": "1"},  # GHOST has no price
            interest_indices={},
            market_risk_infos={},
            liquidation_ratio="1",
        )
        self.assertAlmostEqual(result["collateralUSD"], 50.0)


class TestClassifyRisk(unittest.TestCase):
    def test_standard_thresholds(self):
        self.assertEqual(flr.classify_risk(None), "UNKNOWN")
        self.assertEqual(flr.classify_risk(flr.HF_CRITICAL - 1e-9), "CRITICAL")
        self.assertEqual(flr.classify_risk(flr.HF_CRITICAL), "DANGER")
        self.assertEqual(flr.classify_risk(flr.HF_DANGER), "WARNING")
        self.assertEqual(flr.classify_risk(flr.HF_WARNING), "SAFE")

    def test_emode_thresholds(self):
        self.assertEqual(flr.classify_risk(flr.HF_EMODE_CRITICAL - 1e-9, emode=True), "CRITICAL")
        self.assertEqual(flr.classify_risk(flr.HF_EMODE_CRITICAL, emode=True), "DANGER")
        self.assertEqual(flr.classify_risk(flr.HF_EMODE_DANGER, emode=True), "WARNING")
        self.assertEqual(flr.classify_risk(flr.HF_EMODE_WARNING, emode=True), "SAFE")


class TestHistoryStats(unittest.TestCase):
    def test_empty(self):
        stats = flr.build_liquidation_history_stats([])
        self.assertEqual(stats["total"], 0)
        self.assertEqual(stats["byChain"], {})

    def test_aggregation_and_rounding(self):
        rows = [
            {"chain": "berachain", "debtRepaidUSD": "10.005", "collateralSeizedUSD": 11,
             "liquidationRewardUSD": 1, "protocolFeeUSD": 0.1},
            {"chain": "berachain", "debtRepaidUSD": 5, "collateralSeizedUSD": "5.5",
             "liquidationRewardUSD": "bad-value", "protocolFeeUSD": "bad-value"},
            {"chain": "arbitrum", "debtRepaidUSD": 1, "collateralSeizedUSD": 2,
             "liquidationRewardUSD": 0.5, "protocolFeeUSD": 0.05},
        ]
        stats = flr.build_liquidation_history_stats(rows)
        self.assertEqual(stats["total"], 3)
        self.assertEqual(stats["byChain"]["berachain"]["count"], 2)
        self.assertAlmostEqual(stats["byChain"]["berachain"]["debtRepaidUSD"], 15.01)
        self.assertAlmostEqual(stats["byChain"]["berachain"]["liquidationRewardUSD"], 1.0)
        self.assertAlmostEqual(stats["byChain"]["berachain"]["protocolFeeUSD"], 0.1)
        self.assertAlmostEqual(stats["debtRepaidUSD"], 16.01)
        self.assertAlmostEqual(stats["protocolFeeUSD"], 0.15)

    def test_protocol_fee_transfers_match_same_tx_held_token_after_liquidation(self):
        rows = [
            {
                "txHash": "0xabc",
                "serialId": "10",
                "heldToken": {"id": "0xheld"},
                "liquidationRewardUSD": 100,
            },
            {
                "txHash": "0xabc",
                "serialId": "12",
                "heldToken": {"id": "0xother"},
                "liquidationRewardUSD": 50,
            },
        ]
        transfers = [
            {
                "id": "0xabc-11",
                "serialId": "11",
                "transaction": {"id": "0xabc"},
                "token": {"id": "0xheld"},
                "amountDeltaWei": "1.25",
                "amountUSDDeltaWei": "10",
            },
            {
                "id": "0xabc-9",
                "serialId": "9",
                "transaction": {"id": "0xabc"},
                "token": {"id": "0xheld"},
                "amountDeltaWei": "9",
                "amountUSDDeltaWei": "90",
            },
            {
                "id": "0xabc-13",
                "serialId": "13",
                "transaction": {"id": "0xabc"},
                "token": {"id": "0xother"},
                "amountDeltaWei": "0.5",
                "amountUSDDeltaWei": "5",
            },
        ]

        flr.attach_protocol_liquidation_fees(rows, transfers)

        self.assertEqual(rows[0]["protocolFeeTransferId"], "0xabc-11")
        self.assertEqual(rows[0]["protocolFeeSource"], "feeAgentTransfer")
        self.assertAlmostEqual(rows[0]["protocolFeeUSD"], 10.0)
        self.assertEqual(rows[1]["protocolFeeTransferId"], "0xabc-13")
        self.assertAlmostEqual(rows[1]["protocolFeeUSD"], 5.0)


class TestIndexSerialization(unittest.TestCase):
    def test_decimal_index_is_exact(self):
        raw = 1052103456789012345678  # 18-decimal index ≈ 1052.10…
        text = format(Decimal(raw).scaleb(-18), "f")
        self.assertEqual(text, "1052.103456789012345678")
        self.assertNotIn("E", text.upper())


if __name__ == "__main__":
    unittest.main()
