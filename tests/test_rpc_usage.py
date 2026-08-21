import io
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rpc_usage


class RpcUsageOutputStreamTests(unittest.TestCase):
    """Regression guard: parents such as run_earn_canonical_history_refresh.py
    capture a child's stdout and json.loads() it. The end-of-run usage summary
    must go to stderr only — a line on stdout corrupts that JSON ("Extra data")
    and fails the workflow with exit code 1."""

    def test_summary_goes_to_stderr_not_stdout(self):
        rpc_usage.reset_usage()
        rpc_usage.record_request("eth_getLogs", 2)
        out, err = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {"RPC_USAGE_QUIET": "", "RPC_USAGE_LOG": ""}), \
             redirect_stdout(out), redirect_stderr(err):
            rpc_usage.emit_usage_summary()
        rpc_usage.reset_usage()
        self.assertEqual(out.getvalue(), "")            # stdout MUST stay clean
        self.assertIn("RPC usage", err.getvalue())      # summary lives on stderr

    def test_summary_silent_when_no_requests(self):
        rpc_usage.reset_usage()
        out, err = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {"RPC_USAGE_QUIET": "", "RPC_USAGE_LOG": ""}), \
             redirect_stdout(out), redirect_stderr(err):
            rpc_usage.emit_usage_summary()
        self.assertEqual(out.getvalue(), "")
        self.assertEqual(err.getvalue(), "")

    def test_provider_failover_summary_tracks_host_without_leaking_secret(self):
        rpc_usage.reset_usage()
        secret_url = "https://berachain-mainnet.g.alchemy.com/v2/VERY_SECRET_KEY"
        rpc_usage.record_request("eth_getLogs", 3)
        rpc_usage.record_provider_failure(secret_url, rate_limited=True)
        rpc_usage.record_provider_success("https://berachain.drpc.org/key", served_methods=3)

        summary = rpc_usage.usage_summary()

        self.assertEqual(
            summary["by_provider"]["berachain-mainnet.g.alchemy.com"],
            {"http_success": 0, "http_failure": 1, "rate_limited": 1, "served_methods": 0},
        )
        self.assertEqual(
            summary["by_provider"]["berachain.drpc.org"],
            {"http_success": 1, "http_failure": 0, "rate_limited": 0, "served_methods": 3},
        )

        out, err = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {"RPC_USAGE_QUIET": "", "RPC_USAGE_LOG": ""}), \
             redirect_stdout(out), redirect_stderr(err):
            rpc_usage.emit_usage_summary()
        self.assertIn("berachain.drpc.org", err.getvalue())
        self.assertIn("429 1", err.getvalue())
        self.assertNotIn("VERY_SECRET_KEY", err.getvalue())
        rpc_usage.reset_usage()


if __name__ == "__main__":
    unittest.main()
