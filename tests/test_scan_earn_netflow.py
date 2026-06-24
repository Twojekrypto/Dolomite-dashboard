import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import patch

import scan_earn_netflow
from scan_earn_netflow import MIN_BLOCK_CHUNK, _is_chunk_too_large_error, _reduced_chunk_size

ROOT = Path(__file__).resolve().parents[1]


class ScanEarnNetflowTest(unittest.TestCase):
    def test_detects_payload_size_errors_from_rpc_tail(self):
        self.assertTrue(_is_chunk_too_large_error("HTTP Error 413: Request Entity Too Large"))
        self.assertTrue(_is_chunk_too_large_error("All RPCs failed; recent errors: payload too large"))
        self.assertTrue(_is_chunk_too_large_error("invalid block range params"))
        self.assertTrue(_is_chunk_too_large_error("query returned more than 10000 results"))

    def test_reduced_chunk_size_never_goes_below_minimum(self):
        self.assertEqual(_reduced_chunk_size(49_999), 24_999)
        self.assertEqual(_reduced_chunk_size(100), MIN_BLOCK_CHUNK)

    def test_ethereum_scans_full_chunk_with_per_endpoint_1rpc_cap(self):
        # Ethereum no longer throttles the whole chain to 1rpc's 50-block limit;
        # it defaults to the full BLOCK_CHUNK, and 1rpc is capped per-endpoint.
        self.assertNotIn("max_block_chunk", scan_earn_netflow.CHAINS["ethereum"])
        effective = int(scan_earn_netflow.CHAINS["ethereum"].get("max_block_chunk")
                        or scan_earn_netflow.BLOCK_CHUNK)
        self.assertEqual(effective, scan_earn_netflow.BLOCK_CHUNK)
        # Only the Ethereum 1rpc path is capped; keyed providers are not, and the
        # cap must NOT leak to 1rpc on other chains (that regressed berachain EARN).
        self.assertEqual(scan_earn_netflow._endpoint_block_cap("https://1rpc.io/eth"), 50)
        self.assertIsNone(
            scan_earn_netflow._endpoint_block_cap("https://eth-mainnet.g.alchemy.com/v2/key")
        )
        self.assertIsNone(scan_earn_netflow._endpoint_block_cap("https://1rpc.io/berachain"))
        self.assertIsNone(scan_earn_netflow._endpoint_block_cap("https://1rpc.io/arb"))

    def test_scanner_reads_all_dedicated_arbitrum_and_ethereum_rpc_secrets(self):
        source = (ROOT / "scan_earn_netflow.py").read_text(encoding="utf-8")

        for env_name in (
            "ALCHEMY_ARBITRUM_RPC_KAT",
            "ALCHEMY_ARBITRUM_RPC_DAN",
            "ALCHEMY_ARBITRUM_RPC_ZEN",
            "ALCHEMY_ETHEREUM_RPC_KAT",
            "ALCHEMY_ETHEREUM_RPC_DAN",
            "ALCHEMY_ETHEREUM_RPC_ZEN",
        ):
            self.assertIn(env_name, source)

    def test_rpc_call_skips_capped_endpoint_for_oversized_getlogs(self):
        # A getLogs range wider than an endpoint's cap must rotate past that
        # endpoint (never silently truncated); a small range may still use it.
        rpcs = ["https://1rpc.io/eth", "https://strong.example/v2/key"]
        seen = []

        class _Resp:
            def __init__(self, payload):
                self._p = payload

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return json.dumps(self._p).encode()

        def _fake(req, timeout=None):
            seen.append(req.full_url)
            body = json.loads(req.data)
            return _Resp({"jsonrpc": "2.0", "id": body["id"], "result": []})

        with patch.object(scan_earn_netflow, "urlopen", side_effect=_fake):
            # Wide range while 1rpc is preferred -> skip 1rpc, serve via strong endpoint.
            scan_earn_netflow.get_logs(rpcs, [0], "0xC", [scan_earn_netflow.ALL_EVENTS], 0, 49_998)
            # Small range -> 1rpc is allowed to serve it.
            scan_earn_netflow.get_logs(rpcs, [0], "0xC", [scan_earn_netflow.ALL_EVENTS], 100, 129)

        self.assertEqual(seen, ["https://strong.example/v2/key", "https://1rpc.io/eth"])

    def test_getlogs_failover_tries_each_endpoint_once_with_short_timeout(self):
        rpcs = ["https://rpc-a.example", "https://rpc-b.example", "https://rpc-c.example"]
        seen = []
        timeouts = []

        def _fake(req, timeout=None):
            seen.append(req.full_url)
            timeouts.append(timeout)
            raise TimeoutError("slow getLogs")

        with patch.object(scan_earn_netflow, "urlopen", side_effect=_fake):
            with self.assertRaisesRegex(Exception, "All RPCs failed after 3 attempts"):
                scan_earn_netflow.get_logs(rpcs, [0], "0xC", [scan_earn_netflow.ALL_EVENTS], 100, 124)

        self.assertEqual(seen, rpcs)
        self.assertEqual(timeouts, [15, 15, 15])

    def test_rpc_call_includes_http_error_body_in_recent_errors(self):
        def _fake(req, timeout=None):
            raise HTTPError(
                req.full_url,
                400,
                "Bad Request",
                hdrs=None,
                fp=BytesIO(b'{"error":{"message":"Log response size exceeded"}}'),
            )

        with patch.object(scan_earn_netflow, "urlopen", side_effect=_fake):
            with self.assertRaisesRegex(Exception, "Log response size exceeded"):
                scan_earn_netflow.get_logs(
                    ["https://rpc-a.example"],
                    [0],
                    "0xC",
                    [scan_earn_netflow.ALL_EVENTS],
                    100,
                    124,
                )

    def test_main_returns_failure_when_chain_scan_fails(self):
        fake_chains = {
            "testchain": {
                "margin": "0x0000000000000000000000000000000000000000",
                "rpcs": ["https://example.invalid"],
                "start_block": 1,
            }
        }
        stdout = StringIO()
        stderr = StringIO()
        with patch.object(scan_earn_netflow, "CHAINS", fake_chains), \
             patch.object(scan_earn_netflow, "scan_chain", return_value={"completed": False, "reason": "rpc_failed"}), \
             patch("sys.argv", ["scan_earn_netflow.py", "testchain"]), \
             redirect_stdout(stdout), \
             redirect_stderr(stderr):
                self.assertEqual(scan_earn_netflow.main(), 1)
        self.assertIn("Scan failed for testchain: rpc_failed", stderr.getvalue())

    def test_main_treats_no_progress_with_existing_output_as_checkpoint(self):
        fake_chains = {
            "testchain": {
                "margin": "0x0000000000000000000000000000000000000000",
                "rpcs": ["https://example.invalid"],
                "start_block": 1,
            }
        }
        stdout = StringIO()
        with patch.object(scan_earn_netflow, "CHAINS", fake_chains), \
             patch.object(scan_earn_netflow, "scan_chain", return_value={
                 "completed": False,
                 "reason": "soft_runtime_no_progress",
                 "hasExistingOutput": True,
             }), \
             patch("sys.argv", ["scan_earn_netflow.py", "testchain"]), \
             redirect_stdout(stdout):
            self.assertEqual(scan_earn_netflow.main(), 0)
        self.assertIn("preserving the existing public netflow file", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
