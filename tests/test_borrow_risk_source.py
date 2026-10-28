import json
import unittest
import urllib.error
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

import fetch_liquidation_risk as risk


class RiskEarningsSourceTests(unittest.TestCase):
    def test_identity_and_exact_principal_are_additive_to_risk_output(self):
        supply = "0x" + "A" * 40
        debt = "0x" + "b" * 40
        result = risk.compute_health_factor(
            [{"token": {"id": supply, "symbol": "SAME", "marketId": "7", "decimals": "18"}, "valuePar": "100.000000000000000001"},
             {"token": {"id": debt, "symbol": "SAME", "marketId": "8", "decimals": "6"}, "valuePar": "-20.000001"}],
            {supply: "2", debt: "1"},
            {supply: {"supplyIndex": "1.25", "borrowIndex": "1"}, debt: {"supplyIndex": "1", "borrowIndex": "1.1"}},
            {supply: {"marginPremium": "0.25"}, debt: {"marginPremium": "0.1"}},
            "1.15",
        )
        collateral = result["collateralTokens"][0]
        liability = result["debtTokens"][0]
        self.assertEqual(collateral.get("tokenAddress"), supply.lower())
        self.assertEqual(liability.get("tokenAddress"), debt)
        self.assertEqual((collateral.get("marketId"), liability.get("marketId")), ("7", "8"))
        self.assertEqual(collateral.get("amount"), "125.00000000000000000125")
        self.assertEqual(collateral.get("usdExact"), "250.00000000000000000250")
        self.assertEqual(liability.get("amount"), "22.0000011")
        self.assertEqual(liability.get("usdExact"), "22.0000011")
        original_fields = {"symbol", "usd", "marginPremium"}
        stripped = dict(result)
        self.assertIs(stripped.pop("earningsComplete", None), True)
        stripped["collateralTokens"] = [{k: v for k, v in collateral.items() if k in original_fields}]
        stripped["debtTokens"] = [{k: v for k, v in liability.items() if k in original_fields}]
        self.assertEqual(stripped, {
            "healthFactor": 7.186489040603683,
            "collateralUSD": 200.0,
            "debtUSD": 24.20000121,
            "collateralTokens": [{"symbol": "SAME", "usd": 250.0, "marginPremium": 0.25}],
            "debtTokens": [{"symbol": "SAME", "usd": 22.0000011, "marginPremium": 0.1}],
        })

    def test_emode_financial_fields_remain_unchanged(self):
        result = risk.compute_health_factor(
            [{"token": {"id": "0xaaa", "symbol": "A", "marketId": "1"}, "valuePar": "200"},
             {"token": {"id": "0xbbb", "symbol": "B", "marketId": "2"}, "valuePar": "-100"}],
            {"0xaaa": "1", "0xbbb": "1"}, {},
            {"0xaaa": {"marginPremium": "0.5"}, "0xbbb": {"marginPremium": "0.5"}},
            "1.15", margin_ratio_override=0.1,
        )
        self.assertEqual(result["healthFactor"], 1.8181818181818181)
        self.assertEqual((result["collateralUSD"], result["debtUSD"]), (200.0, 100.0))
        self.assertEqual(result["collateralTokens"][0]["marginPremium"], 0.0)
        self.assertEqual(result["debtTokens"][0]["marginPremium"], 0.0)
        self.assertEqual(result["collateralTokens"][0].get("usdExact"), "200")

    def test_nonzero_collateral_and_debt_require_oracle_prices(self):
        tokens = [
            {"token": {"id": "0xsupply", "symbol": "S"}, "valuePar": "200"},
            {"token": {"id": "0xdebt", "symbol": "D"}, "valuePar": "-100"},
        ]
        indices = {key: {"supplyIndex": "1", "borrowIndex": "1"} for key in ("0xsupply", "0xdebt")}
        for missing in ("0xsupply", "0xdebt"):
            with self.subTest(missing=missing):
                prices = {key: "1" for key in indices if key != missing}
                result = risk.compute_health_factor(tokens, prices, indices, {}, "1.15")
                self.assertIs(result.get("earningsComplete"), False)
                self.assertEqual(result["collateralUSD"], 0.0 if missing == "0xsupply" else 200.0)
                self.assertEqual(result["debtUSD"], 0.0 if missing == "0xdebt" else 100.0)

    def test_missing_index_flags_defaulted_principal_as_incomplete(self):
        for amount in ("200", "-100"):
            with self.subTest(amount=amount):
                result = risk.compute_health_factor(
                    [{"token": {"id": "0xtoken", "symbol": "T"}, "valuePar": amount}],
                    {"0xtoken": "1"}, {}, {}, "1.15",
                )
                self.assertIs(result.get("earningsComplete"), False)
                self.assertEqual(result["collateralUSD"] + result["debtUSD"], abs(float(amount)))

    def test_nonpositive_and_nonfinite_active_index_or_price_flags_incomplete(self):
        token = [{"token": {"id": "0xtoken", "symbol": "T"}, "valuePar": "2"}]
        for price, supply_index in (("0", "1"), ("-1", "1"), ("Infinity", "1"), ("1", "0"), ("1", "-1"), ("1", "Infinity")):
            with self.subTest(price=price, supply_index=supply_index):
                result = risk.compute_health_factor(
                    token, {"0xtoken": price},
                    {"0xtoken": {"supplyIndex": supply_index, "borrowIndex": "1"}}, {}, "1.15",
                )
                self.assertIs(result.get("earningsComplete"), False)

    def test_live_position_carries_completeness_from_all_nonzero_balances(self):
        supply = {"token": {"id": "0xsupply", "symbol": "S", "marketId": "1"}, "valuePar": "200"}
        debt = {"token": {"id": "0xdebt", "symbol": "D", "marketId": "2"}, "valuePar": "-100"}
        unknown = {"token": {"id": "0xmissing", "symbol": "M", "marketId": "3"}, "valuePar": "1"}
        responses = {
            risk.QUERY_DOLOMITE_MARGIN: {"dolomiteMargins": [{"id": "0xmargin", "liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 3}]},
            risk.QUERY_MARKET_RISK_INFO: {"marketRiskInfos": []},
            risk.QUERY_ORACLE_PRICES: {"oraclePrices": [{"token": token["token"], "price": "1"} for token in (supply, debt)]},
            risk.QUERY_INTEREST_INDICES: {"interestIndexes": [{"token": token["token"], "supplyIndex": "1", "borrowIndex": "1"} for token in (supply, debt)]},
            risk.QUERY_TOTAL_PARS: {"totalPars": []},
            risk.QUERY_MARGIN_ACCOUNTS: {"marginAccounts": [
                {"id": "complete", "effectiveUser": {"id": "0xwallet"}, "accountNumber": "0", "tokenValues": [supply, debt]},
                {"id": "partial", "effectiveUser": {"id": "0xwallet"}, "accountNumber": "1", "tokenValues": [supply, debt, unknown]},
            ]},
        }
        with patch.object(risk, "graphql_request", side_effect=lambda url, query, **kwargs: responses[query]), redirect_stdout(StringIO()):
            result = risk.fetch_chain_data("arbitrum", {"label": "Arbitrum", "subgraph_name": "test", "explorer": "https://example.invalid/", "rpc": ""})
        completeness = {row["accountId"]: row.get("earningsComplete") for row in result["positions"]}
        self.assertEqual(completeness, {"complete": True, "partial": False})

    def test_empty_data_object_is_not_a_successful_page(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"data":{}}'

        with patch.object(risk.urllib.request, "urlopen", return_value=Response()):
            with self.assertRaises(RuntimeError):
                risk.graphql_request("https://example.invalid", "query")

    def test_transport_exhaustion_cannot_look_like_an_empty_terminal_page(self):
        with patch.object(risk.urllib.request, "urlopen", side_effect=urllib.error.URLError("offline")), patch.object(risk.time, "sleep"):
            with self.assertRaises(RuntimeError):
                risk.graphql_request("https://example.invalid", "query", retries=2)

    def test_graphql_partial_errors_cannot_publish_incomplete_chain(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({"data": {"marginAccounts": []}, "errors": [{"message": "indexing failure"}]}).encode()

        with patch.object(risk.urllib.request, "urlopen", return_value=Response()):
            with self.assertRaises(RuntimeError):
                risk.graphql_request("https://example.invalid", "query")

    def test_missing_graphql_data_cannot_be_an_empty_terminal_page(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"data":null}'

        with patch.object(risk.urllib.request, "urlopen", return_value=Response()):
            with self.assertRaises(RuntimeError):
                risk.graphql_request("https://example.invalid", "query")

    def test_null_account_page_is_not_a_successful_terminal_page(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"data":{"marginAccounts":null}}'

        with patch.object(risk.urllib.request, "urlopen", return_value=Response()):
            with self.assertRaises(RuntimeError):
                risk.graphql_request("https://example.invalid", "query")


if __name__ == "__main__":
    unittest.main()
