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
    def run_overrides(self, outcomes, count=1):
        requests, providers, individual = [], [], []
        module = fake_web3(outcomes, requests, providers, individual)
        with patch.dict(sys.modules, {"web3": module}), patch.object(risk.time, "sleep"), redirect_stdout(StringIO()):
            result = risk.fetch_risk_overrides("https://primary", "0x" + "a" * 40, accounts(count), rpc_fallbacks=["https://healthy"])
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



if __name__ == "__main__":
    unittest.main()
