import io
import json
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import report_rpc_provider_health as health


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


class RpcProviderHealthTests(unittest.TestCase):
    def test_reports_each_secret_by_safe_name_without_url_or_key(self):
        env = {
            "ALCHEMY_BERACHAIN_RPC_2_JEFF": "https://bera.example/v2/BERA_SECRET",
            "ALCHEMY_MANTLE_RPC_2_JEFF": "https://mantle.example/v2/MANTLE_SECRET",
        }
        responses = [
            _Response({"jsonrpc": "2.0", "id": 1, "result": "0x123"}),
            _Response({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": 429, "message": "Monthly capacity limit exceeded"},
            }),
        ]
        output = io.StringIO()

        with patch.dict(os.environ, env, clear=False), \
             patch.object(health, "urlopen", side_effect=responses), \
             redirect_stdout(output):
            result = health.main([
                "--provider", "ALCHEMY_BERACHAIN_RPC_2_JEFF",
                "--provider", "ALCHEMY_MANTLE_RPC_2_JEFF",
            ])

        text = output.getvalue()
        self.assertEqual(result, 1)
        self.assertIn("OK ALCHEMY_BERACHAIN_RPC_2_JEFF [bera.example]", text)
        self.assertIn("FAIL ALCHEMY_MANTLE_RPC_2_JEFF [mantle.example]: rate_limited", text)
        self.assertNotIn("BERA_SECRET", text)
        self.assertNotIn("MANTLE_SECRET", text)

    def test_missing_secret_is_skipped_without_failing_diagnostic(self):
        with patch.dict(os.environ, {"NOT_CONFIGURED_RPC": ""}, clear=False):
            output = io.StringIO()
            with redirect_stdout(output):
                result = health.main(["--provider", "NOT_CONFIGURED_RPC"])

        self.assertEqual(result, 0)
        self.assertIn("SKIP NOT_CONFIGURED_RPC: not configured", output.getvalue())


if __name__ == "__main__":
    unittest.main()
