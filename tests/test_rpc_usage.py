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


if __name__ == "__main__":
    unittest.main()
