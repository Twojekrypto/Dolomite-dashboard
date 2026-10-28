import json
import sys
import types
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

import fetch_liquidation_risk as risk


def accounts(count):
    return [{"id": f"account-{index}", "accountNumber": str(index),
             "user": {"id": f"0x{index + 1:040x}"},
             "effectiveUser": {"id": "0x" + "f" * 40}}
            for index in range(count)]


def fake_web3(outcomes, requests, providers, individual_calls):
    class Provider:
        def __init__(self, url, request_kwargs=None, exception_retry_configuration="default"):
            self.url = url
            providers.append((url, request_kwargs, exception_retry_configuration))

    class EncodedCall:
        def __init__(self, account):
            self.account = account

        def _encode_transaction_data(self):
            return "0x" + json.dumps(self.account).encode().hex()

        def call(self):
            individual_calls.append(self.account)
            raise RuntimeError("individual calls are forbidden")

    class AggregateCall:
        def __init__(self, url, calls):
            self.url, self.calls = url, calls

        def call(self):
            owners = [json.loads(call[2].decode()) for call in self.calls]
            requests.append((self.url, owners))
            response = outcomes[self.url]
            if isinstance(response, Exception):
                raise response
            return response(len(self.calls))

    class Functions:
        def __init__(self, url):
            self.url = url

        def getAccountRiskOverride(self, account):
            return EncodedCall(account)

        def aggregate3(self, calls):
            return AggregateCall(self.url, calls)

    class Eth:
        def __init__(self, url):
            self.url = url

        def contract(self, **kwargs):
            return types.SimpleNamespace(functions=Functions(self.url))

    class Web3:
        HTTPProvider = Provider
        to_checksum_address = staticmethod(lambda address: address)

        def __init__(self, provider):
            self.eth = Eth(provider.url)

    module = types.ModuleType("web3")
    module.Web3 = Web3
    return module


def valid_results(count):
    return [(True, (10**17).to_bytes(32, "big") + bytes(32)) for _ in range(count)]


class RiskRpcFallbackTests(unittest.TestCase):
    def run_overrides(self, outcomes, count=1, unresolved=None, fallbacks=None):
        requests, providers, individual = [], [], []
        module = fake_web3(outcomes, requests, providers, individual)
        with patch.dict(sys.modules, {"web3": module}), patch.object(risk.time, "sleep"), redirect_stdout(StringIO()):
            kwargs = {} if unresolved is None else {"unresolved_accounts": unresolved}
            result = risk.fetch_risk_overrides("https://primary", "0x" + "a" * 40, accounts(count), rpc_fallbacks=["https://healthy"] if fallbacks is None else fallbacks, **kwargs)
        return result, requests, providers, individual

    def test_failed_primary_uses_whole_batch_fallback_then_keeps_healthy_provider(self):
        result, requests, providers, individual = self.run_overrides({
            "https://primary": RuntimeError("offline"), "https://healthy": valid_results,
        }, count=51)
        self.assertEqual([url for url, _ in requests], ["https://primary", "https://healthy", "https://healthy"])
        self.assertEqual([len(owners) for _, owners in requests], [50, 50, 1])
        self.assertEqual(result, {f"account-{index}": 0.1 for index in range(51)})
        self.assertEqual(individual, [])
        self.assertTrue(all(options["timeout"] <= 30 and retry is None for _, options, retry in providers))
        self.assertEqual(requests[1][1][0], ["0x" + "0" * 39 + "1", 0])

    def test_all_providers_failing_raises_instead_of_fabricating_no_emode(self):
        with self.assertRaises(RuntimeError):
            self.run_overrides({"https://primary": RuntimeError("offline"), "https://healthy": RuntimeError("offline")})

    def test_incomplete_aggregate_results_retry_whole_batch_on_next_provider(self):
        for bad in (lambda n: [], lambda n: valid_results(n + 1), lambda n: [(False, bytes(64))] * n, lambda n: [(True, bytes(32))] * n):
            with self.subTest(response=bad):
                result, requests, _, individual = self.run_overrides({"https://primary": bad, "https://healthy": valid_results})
                self.assertEqual(result, {"account-0": 0.1})
                self.assertEqual([url for url, _ in requests], ["https://primary", "https://healthy"])
                self.assertEqual(individual, [])

    def test_successful_zero_override_is_a_known_standard_account(self):
        result, requests, _, individual = self.run_overrides({
            "https://primary": lambda count: [(True, bytes(64))] * count,
            "https://healthy": valid_results,
        })
        self.assertEqual(result, {})
        self.assertEqual([url for url, _ in requests], ["https://primary"])
        self.assertEqual(individual, [])

    def test_incomplete_results_from_every_provider_raise(self):
        with self.assertRaises(RuntimeError):
            self.run_overrides({"https://primary": lambda n: [], "https://healthy": lambda n: [(False, b"")] * n})

    def test_one_unknown_account_preserves_other_49_with_explicit_uncertainty(self):
        unresolved = set()
        result, requests, _, individual = self.run_overrides({
            "https://primary": lambda n: valid_results(n - 1) + [(False, b"")],
            "https://healthy": RuntimeError("offline"),
        }, count=50, unresolved=unresolved)
        self.assertEqual(result, {f"account-{index}": 0.1 for index in range(49)})
        self.assertEqual(unresolved, {"account-49"})
        self.assertEqual([len(owners) for _, owners in requests], [50, 1, 1, 1])
        self.assertEqual(individual, [])

    def test_next_provider_receives_only_unresolved_subset(self):
        unresolved = set()
        result, requests, _, individual = self.run_overrides({
            "https://primary": lambda n: valid_results(n - 1) + [(False, b"")],
            "https://healthy": valid_results,
        }, count=50, unresolved=unresolved)
        self.assertEqual(result, {f"account-{index}": 0.1 for index in range(50)})
        self.assertEqual(unresolved, set())
        self.assertEqual([len(owners) for _, owners in requests], [50, 1])
        self.assertEqual(individual, [])

    def test_single_provider_has_one_bounded_subset_retry(self):
        unresolved = set()
        result, requests, _, individual = self.run_overrides({
            "https://primary": lambda n: valid_results(n - 1) + [(False, b"")] if n > 1 else valid_results(n),
        }, count=50, unresolved=unresolved, fallbacks=[])
        self.assertEqual(len(result), 50)
        self.assertEqual([len(owners) for _, owners in requests], [50, 1])
        self.assertEqual(unresolved, set())
        self.assertEqual(individual, [])

    def test_known_zero_overrides_survive_alongside_one_unknown(self):
        unresolved = set()
        result, requests, _, individual = self.run_overrides({
            "https://primary": lambda n: [(True, bytes(64))] * (n - 1) + [(False, b"")],
        }, count=50, unresolved=unresolved, fallbacks=[])
        self.assertEqual(result, {})
        self.assertEqual(unresolved, {"account-49"})
        self.assertEqual([len(owners) for _, owners in requests], [50, 1])
        self.assertEqual(individual, [])

    def test_all_accounts_unknown_still_fails_chain(self):
        with self.assertRaises(RuntimeError):
            self.run_overrides({"https://primary": lambda n: [(False, b"")] * n}, unresolved=set(), fallbacks=[])

    def test_default_caller_remains_strict_on_partial_account_results(self):
        with self.assertRaises(RuntimeError):
            self.run_overrides({"https://primary": lambda n: valid_results(n - 1) + [(False, b"")]}, count=2, fallbacks=[])

    def test_unknown_override_excludes_only_that_emitted_account(self):
        token = {"id": "0xtoken", "symbol": "T", "marketId": "1"}
        chain_accounts = accounts(2)
        for account in chain_accounts:
            account["tokenValues"] = [{"token": token, "valuePar": "-100"}]
        responses = {
            risk.QUERY_DOLOMITE_MARGIN: {"dolomiteMargins": [{"id": "0xmargin", "liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 1, "defaultAccountRiskOverrideSetter": "0x" + "a" * 40}]},
            risk.QUERY_MARKET_RISK_INFO: {"marketRiskInfos": []},
            risk.QUERY_ORACLE_PRICES: {"oraclePrices": [{"token": token, "price": "1"}]},
            risk.QUERY_INTEREST_INDICES: {"interestIndexes": [{"token": token, "supplyIndex": "1", "borrowIndex": "1"}]},
            risk.QUERY_TOTAL_PARS: {"totalPars": []},
            risk.QUERY_MARGIN_ACCOUNTS: {"marginAccounts": chain_accounts},
        }
        module = fake_web3({"https://primary": lambda n: valid_results(n - 1) + [(False, b"")]}, [], [], [])
        with patch.dict(sys.modules, {"web3": module}), patch.object(risk.time, "sleep"), redirect_stdout(StringIO()), \
             patch.object(risk, "graphql_request", side_effect=lambda url, query, **kwargs: responses[query]), \
             patch.object(risk, "fetch_live_oracle_prices", side_effect=lambda *args, **kwargs: args[2]), \
             patch.object(risk, "fetch_live_interest_indices", side_effect=lambda *args, **kwargs: args[2]):
            result = risk.fetch_chain_data("arbitrum", {"label": "Arbitrum", "subgraph_name": "test", "rpc": "https://primary", "explorer": "https://example.invalid/"})
        positions = {row["accountId"]: row for row in result["positions"]}
        self.assertIs(positions["account-0"]["earningsComplete"], True)
        self.assertIs(positions["account-1"]["earningsComplete"], False)
        self.assertIs(positions["account-0"].get("riskOverrideVerified"), True)
        self.assertIs(positions["account-1"].get("riskOverrideVerified"), False)
        self.assertEqual(positions["account-0"]["debtUSD"], 100.0)
        self.assertEqual(positions["account-1"]["debtUSD"], 100.0)



if __name__ == "__main__":
    unittest.main()
