import os
import sys
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_reward_claim_events as rce

ROOT = Path(__file__).resolve().parents[1]
KNOWN_BERA_ODOLO_CLAIM_TX = "0xe2d1621747cafc1f7d7d18b41a2cc1204369c91edb89eea59628bdd23d8340b2"
RECENT_BERA_ODOLO_CLAIM_TX = "0xf4dd6748b08850a1c871046fdad619acce76a9999038fd6a712ac2efa368cea6"


def _log(block):
    return {
        "topics": ["0x" + "9" * 64, "0x" + "1" * 64, "0x" + "2" * 64],
        "data": "0x",
        "blockNumber": hex(block),
        "logIndex": "0x0",
        "transactionHash": "0x" + "a" * 64,
    }


class RewardClaimCheckpointTests(unittest.TestCase):
    def test_xlayer_scan_discovers_claims_without_a_fresh_subgraph_distributor_list(self):
        cfg = {"name": "X Layer", "eventEmitter": "0x" + "1"*40,
               "rpcUrls": ["https://rpc.example"], "chunkSize": 100,
               "scanAllDistributors": True}
        log = _log(1000)
        def rpc(urls, method, params, **kwargs):
            self.assertEqual(params[0]["topics"], [rce.REWARD_CLAIMED_TOPIC])
            return [log]
        with tempfile.TemporaryDirectory() as tmp, patch.object(rce, "rpc_request", side_effect=rpc), patch.object(rce.time, "sleep"):
            rows = rce.fetch_reward_claimed_logs("xlayer", cfg, 1000, 1099, [], checkpoint_path=os.path.join(tmp, "xlayer.json"))
        self.assertEqual(rows, [log])

    def test_bounded_backfill_resumes_verified_prefix_as_head_moves(self):
        cfg = {"name": "X Layer", "eventEmitter": "0x" + "1"*40,
               "rpcUrls": ["https://rpc.example"], "chunkSize": 100, "maxLogChunks": 2}
        distributor = "0x" + "2"*40
        ranges = []
        def rpc(urls, method, params, **kwargs):
            start, end = int(params[0]["fromBlock"], 16), int(params[0]["toBlock"], 16)
            ranges.append((start, end))
            return [_log(start)]
        with tempfile.TemporaryDirectory() as tmp, patch.object(rce, "rpc_request", side_effect=rpc), patch.object(rce.time, "sleep"):
            checkpoint = os.path.join(tmp, "xlayer.json")
            with self.assertRaisesRegex(RuntimeError, "checkpoint"):
                rce.fetch_reward_claimed_logs("xlayer", cfg, 1000, 1299, [distributor], checkpoint_path=checkpoint)
            saved = json.loads(Path(checkpoint).read_text())
            self.assertEqual(saved["nextBlock"], 1200)
            rows = rce.fetch_reward_claimed_logs("xlayer", cfg, 1000, 1399, [distributor], checkpoint_path=checkpoint)
        self.assertEqual(ranges, [(1000, 1099), (1100, 1199), (1200, 1299), (1300, 1399)])
        self.assertEqual([int(row["blockNumber"], 16) for row in rows], [1000, 1100, 1200, 1300])

    def test_changed_distributors_cannot_reuse_checkpoint_coverage(self):
        cfg = {"name": "X Layer", "eventEmitter": "0x" + "1"*40,
               "rpcUrls": ["https://rpc.example"], "chunkSize": 100, "maxLogChunks": 1}
        ranges = []
        def rpc(urls, method, params, **kwargs):
            ranges.append(int(params[0]["fromBlock"], 16))
            return []
        with tempfile.TemporaryDirectory() as tmp, patch.object(rce, "rpc_request", side_effect=rpc), patch.object(rce.time, "sleep"):
            checkpoint = os.path.join(tmp, "xlayer.json")
            for distributor in ("0x" + "2"*40, "0x" + "3"*40):
                with self.assertRaisesRegex(RuntimeError, "checkpoint"):
                    rce.fetch_reward_claimed_logs("xlayer", cfg, 1000, 1299, [distributor], checkpoint_path=checkpoint)
        self.assertEqual(ranges, [1000, 1000])

    def test_existing_quicknode_secret_is_accepted_for_claim_scans(self):
        with patch.dict(os.environ, {"XLAYER_RPC_QUICKNODE_TWOJE": "https://quicknode.example/key"}, clear=True):
            self.assertTrue(rce.has_configured_rpc("xlayer"))


class RewardClaimTimestampReuseTests(unittest.TestCase):
    """Block timestamps are immutable, so already-resolved ones must be reused
    instead of re-fetched from the chain (data-identical, fewer RPC calls)."""

    def _patches(self, fake_fetch):
        return [
            patch.object(rce, "decode_claim_data", return_value=(1, 1000)),
            patch.object(rce, "decode_topic_address", side_effect=lambda t: "0x" + "3" * 40),
            patch.object(rce, "is_address", return_value=True),
            patch.object(rce, "token_for_distributor",
                         return_value={"decimals": 18, "symbol": "X", "address": "0x" + "4" * 40}),
            patch.object(rce, "fetch_block_timestamps", side_effect=fake_fetch),
        ]

    def test_reuses_known_timestamps_and_fetches_only_unknown(self):
        fetch_calls = []

        def fake_fetch(chain_key, config, blocks):
            fetch_calls.append(sorted(blocks))
            return {b: 5000 + b for b in blocks}

        patches = self._patches(fake_fetch)
        for p in patches:
            p.start()
        try:
            events = rce.claim_events_from_logs(
                "eth", {"name": "Ethereum"}, [_log(100), _log(200)], {},
                known_timestamps={100: 9999},
            )
        finally:
            for p in patches:
                p.stop()

        # Block 100 already known -> reused; only block 200 is fetched.
        self.assertEqual(fetch_calls, [[200]])
        by_block = {e["blockNumber"]: e["timestamp"] for e in events}
        self.assertEqual(by_block[100], 9999)
        self.assertEqual(by_block[200], 5200)

    def test_fetches_all_when_no_known_timestamps(self):
        fetch_calls = []

        def fake_fetch(chain_key, config, blocks):
            fetch_calls.append(sorted(blocks))
            return {b: 7000 + b for b in blocks}

        patches = self._patches(fake_fetch)
        for p in patches:
            p.start()
        try:
            events = rce.claim_events_from_logs("eth", {"name": "Ethereum"}, [_log(300)], {})
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(fetch_calls, [[300]])
        self.assertEqual(events[0]["timestamp"], 7300)

    def test_legacy_odolo_payload_excludes_non_odolo_distributors(self):
        official = rce.ODOLO_CLAIMS_DISTRIBUTOR
        option_airdrop = rce.OPTION_AIRDROP_DISTRIBUTOR
        payload = {
            "generatedAt": "2026-08-07T00:00:00Z",
            "chains": {"berachain": {"eventEmitter": "0x" + "9" * 40}},
            "events": [
                {
                    "chainKey": "berachain",
                    "distributor": official,
                    "tokenAddress": rce.ODOLO_CONTRACT,
                    "tokenSymbol": "oDOLO",
                    "blockNumber": 100,
                    "timestamp": 1000,
                    "user": "0x" + "1" * 40,
                    "amountWei": str(10 * 10**18),
                },
                {
                    "chainKey": "berachain",
                    "distributor": option_airdrop,
                    "tokenAddress": rce.DOLO_CONTRACT,
                    "tokenSymbol": "DOLO",
                    "blockNumber": 101,
                    "timestamp": 1001,
                    "user": "0x" + "2" * 40,
                    "amountWei": str(20 * 10**18),
                },
            ],
        }

        legacy = rce.build_legacy_odolo_payload(payload)

        self.assertEqual(legacy["distributor"], official)
        self.assertEqual(legacy["distributors"], [official])
        self.assertEqual(len(legacy["events"]), 1)
        self.assertEqual(legacy["events"][0]["user"], "0x" + "1" * 40)
        self.assertEqual(legacy["token"]["address"], rce.ODOLO_CONTRACT)

    def test_documented_dolo_claim_contracts_are_not_odolo(self):
        known = rce.CHAIN_CONFIGS["berachain"]["knownDistributorTokens"]
        for distributor in rce.BERA_DOLO_DISTRIBUTORS:
            with self.subTest(distributor=distributor):
                self.assertEqual(known[distributor]["symbol"], "DOLO")
                self.assertEqual(known[distributor]["address"], rce.DOLO_CONTRACT)

    def test_reward_claim_scanner_reads_all_dedicated_arbitrum_rpc_secrets(self):
        source = (ROOT / "generate_reward_claim_events.py").read_text(encoding="utf-8")

        for env_name in (
            "ALCHEMY_ARBITRUM_RPC_KAT",
            "ALCHEMY_ARBITRUM_RPC_DAN",
            "ALCHEMY_ARBITRUM_RPC_ZEN",
        ):
            self.assertIn(env_name, source)

    def test_reward_claim_scanner_reads_xlayer_zen_rpc_secret(self):
        workflow = (ROOT / ".github" / "workflows" / "update-reward-claim-events.yml").read_text(encoding="utf-8")

        self.assertIn("ALCHEMY_XLAYER_RPC_ZEN: ${{ secrets.ALCHEMY_XLAYER_RPC_ZEN }}", workflow)
        with patch.dict(os.environ, {"ALCHEMY_XLAYER_RPC_ZEN": "https://xlayer.example"}, clear=True):
            self.assertTrue(rce.has_configured_rpc("xlayer"))
            self.assertEqual(rce.get_endpoints("xlayer")[0], "https://xlayer.example")

    def test_reward_claim_scanner_reads_xlayer_rpc_alias(self):
        alias = "https://configured-xlayer.example"
        env = os.environ.copy()
        for name in ("ALCHEMY_XLAYER_RPC_ZEN", "ALCHEMY_XLAYER_RPC", "XLAYER_RPC"):
            env.pop(name, None)
        env["XLAYER_RPC"] = alias

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import json, generate_reward_claim_events as rce; "
                    "print(json.dumps(rce.CHAIN_CONFIGS['xlayer']['rpcUrls']))"
                ),
            ],
            cwd=ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        rpc_urls = json.loads(result.stdout)
        self.assertEqual(rpc_urls[0], alias)
        self.assertIn("https://rpc.xlayer.tech", [url.rstrip("/") for url in rpc_urls[1:]])

    def test_xlayer_rpc_alias_satisfies_configured_gate(self):
        with patch.dict(os.environ, {"XLAYER_RPC": "https://configured-xlayer.example"}, clear=True):
            self.assertTrue(rce.has_configured_rpc("xlayer"))

    def test_cross_chain_workflow_passes_xlayer_rpc_alias(self):
        workflow = (
            ROOT / ".github" / "workflows" / "update-reward-claim-events.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("XLAYER_RPC: ${{ secrets.XLAYER_RPC }}", workflow)

    def test_odolo_flow_workflow_refreshes_berachain_claims_before_flows(self):
        workflow = (ROOT / ".github" / "workflows" / "update-odolo-flows.yml").read_text(encoding="utf-8")

        self.assertIn("fetch-depth: 1", workflow)
        self.assertNotIn("fetch-depth: 0", workflow)
        self.assertIn("REWARD_CLAIM_CHAINS: berachain", workflow)
        self.assertLess(
            workflow.index("Generate Berachain reward claim events"),
            workflow.index("Generate oDOLO flows"),
        )
        self.assertNotIn("ALCHEMY_XLAYER_RPC_ZEN", workflow)

    def test_cross_chain_workflow_uses_sparse_shallow_checkout_and_contract_tests(self):
        workflow = (
            ROOT / ".github" / "workflows" / "update-reward-claim-events.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("fetch-depth: 1", workflow)
        self.assertNotIn("fetch-depth: 0", workflow)
        self.assertIn("sparse-checkout-cone-mode: false", workflow)
        for required_path in (
            "requirements.txt",
            "generate_reward_claim_events.py",
            "validate_data.py",
            "vedolo_vote_power.py",
            "tests/test_generate_reward_claim_events.py",
            ".github/workflows/update-reward-claim-events.yml",
            ".github/workflows/update-odolo-flows.yml",
            "data/reward-claim-events.json",
            "data/odolo-claim-events.json",
            "data/reward-claim-events",
        ):
            with self.subTest(required_path=required_path):
                self.assertIn(required_path, workflow)
        self.assertIn(
            "python3 -m unittest tests.test_generate_reward_claim_events",
            workflow,
        )
        self.assertIn("REWARD_CLAIM_MAX_LOG_CHUNKS_XLAYER: '2000'", workflow)

    def test_reward_claim_log_scan_fails_closed_on_an_unserved_chunk(self):
        config = {
            "name": "X Layer",
            "rpcUrls": ["https://xlayer.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 250_000,
        }

        with (
            patch.object(
                rce,
                "rpc_request",
                side_effect=RuntimeError("block range greater than 100 max"),
            ),
            patch.object(rce.time, "sleep"),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "X Layer claim-log chunk.*could not be scanned",
            ):
                rce.fetch_reward_claimed_logs(
                    "xlayer",
                    config,
                    850_676,
                    851_675,
                    ["0x" + "2" * 40],
                )

    def test_reward_claim_log_scan_learns_provider_cap_without_skipping_blocks(self):
        config = {
            "name": "X Layer",
            "rpcUrls": ["https://xlayer.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 1_000,
        }
        boundary_blocks = (1_000, 1_099, 1_100, 1_199, 1_200, 1_250)
        successful_ranges = []

        def fake_rpc(_rpc_urls, method, params, timeout=30):
            self.assertEqual(method, "eth_getLogs")
            self.assertEqual(timeout, 35)
            request = params[0]
            start = int(request["fromBlock"], 16)
            end = int(request["toBlock"], 16)
            if end - start + 1 > 100:
                raise RuntimeError("eth_getLogs failed: block range greater than 100 max")
            successful_ranges.append((start, end))
            return [_log(block) for block in boundary_blocks if start <= block <= end]

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            logs = rce.fetch_reward_claimed_logs(
                "xlayer",
                config,
                1_000,
                1_250,
                ["0x" + "2" * 40],
            )

        self.assertEqual(successful_ranges, [(1_000, 1_099), (1_100, 1_199), (1_200, 1_250)])
        self.assertEqual([int(log["blockNumber"], 16) for log in logs], list(boundary_blocks))

    def test_reward_claim_log_scan_adapts_when_provider_omits_numeric_cap(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 1_024,
        }
        successful_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            start = int(request["fromBlock"], 16)
            end = int(request["toBlock"], 16)
            if end - start + 1 > 100:
                raise RuntimeError("requested block range is too wide")
            successful_ranges.append((start, end))
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            logs = rce.fetch_reward_claimed_logs(
                "test", config, 1_000, 1_250, ["0x" + "2" * 40]
            )

        self.assertEqual(logs, [])
        self.assertEqual(successful_ranges[0][0], 1_000)
        self.assertEqual(successful_ranges[-1][1], 1_250)
        self.assertTrue(all(end - start + 1 <= 100 for start, end in successful_ranges))
        self.assertTrue(all(left[1] + 1 == right[0] for left, right in zip(successful_ranges, successful_ranges[1:])))

    def test_reward_claim_log_scan_parses_ranges_over_provider_cap(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 100_000,
        }
        successful_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            start = int(request["fromBlock"], 16)
            end = int(request["toBlock"], 16)
            if end - start + 1 > 10_000:
                raise RuntimeError("ranges over 10000 blocks are not supported on free plan")
            successful_ranges.append((start, end))
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            rce.fetch_reward_claimed_logs(
                "test", config, 1_000, 11_050, ["0x" + "2" * 40]
            )

        self.assertEqual(successful_ranges, [(1_000, 10_999), (11_000, 11_050)])

    def test_reward_claim_log_scan_respects_explicit_small_chunk_size(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://primary.example", "https://fallback.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 3,
        }
        requested_ranges = []

        def fake_rpc(rpc_urls, _method, params, timeout=30):
            self.assertEqual(rpc_urls, config["rpcUrls"])
            request = params[0]
            requested_ranges.append((int(request["fromBlock"], 16), int(request["toBlock"], 16)))
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            logs = rce.fetch_reward_claimed_logs(
                "test", config, 10, 15, ["0x" + "2" * 40]
            )

        self.assertEqual(logs, [])
        self.assertEqual(requested_ranges, [(10, 12), (13, 15)])

    def test_reward_claim_log_scan_keeps_normal_large_chunks(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 1_000,
        }
        requested_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            requested_ranges.append((int(request["fromBlock"], 16), int(request["toBlock"], 16)))
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            logs = rce.fetch_reward_claimed_logs(
                "test", config, 1_000, 2_500, ["0x" + "2" * 40]
            )

        self.assertEqual(logs, [])
        self.assertEqual(requested_ranges, [(1_000, 1_999), (2_000, 2_500)])

    def test_reward_claim_log_scan_retries_transient_failure_without_shrinking(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 100,
        }
        requested_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            requested_ranges.append((int(request["fromBlock"], 16), int(request["toBlock"], 16)))
            if len(requested_ranges) == 1:
                raise RuntimeError("429 Client Error: Too Many Requests")
            return [_log(100)]

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            logs = rce.fetch_reward_claimed_logs(
                "test", config, 100, 199, ["0x" + "2" * 40]
            )

        self.assertEqual(requested_ranges, [(100, 199), (100, 199)])
        self.assertEqual([int(log["blockNumber"], 16) for log in logs], [100])

    def test_reward_claim_log_scan_fails_closed_when_one_block_cannot_be_served(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 100,
        }

        with (
            patch.object(
                rce,
                "rpc_request",
                side_effect=RuntimeError("block range limit exceeded"),
            ) as rpc,
            patch.object(rce.time, "sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "claim-log chunk 777-777.*could not be scanned"):
                rce.fetch_reward_claimed_logs(
                    "test", config, 777, 777, ["0x" + "2" * 40]
                )
        self.assertEqual(rpc.call_count, 1)

    def test_reward_claim_log_scan_returns_immediately_for_empty_interval(self):
        with patch.object(rce, "rpc_request") as rpc:
            logs = rce.fetch_reward_claimed_logs(
                "test", {}, 10, 9, ["0x" + "2" * 40]
            )

        self.assertEqual(logs, [])
        rpc.assert_not_called()

    def test_reward_claim_log_scan_fails_closed_at_configured_chunk_budget(self):
        config = {
            "name": "X Layer",
            "rpcUrls": ["https://xlayer.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 100,
            "maxLogChunks": 2,
        }
        requested_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            requested_ranges.append((int(request["fromBlock"], 16), int(request["toBlock"], 16)))
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "cannot complete within remaining chunk budget"):
                rce.fetch_reward_claimed_logs(
                    "xlayer", config, 1_000, 1_250, ["0x" + "2" * 40]
                )

        self.assertEqual(requested_ranges, [(1_000, 1_099)])

    def test_reward_claim_log_scan_aborts_huge_infeasible_range_after_learning_cap(self):
        config = {
            "name": "X Layer",
            "rpcUrls": ["https://xlayer.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 1_000,
            "maxLogChunks": 2_000,
        }
        requested_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            start = int(request["fromBlock"], 16)
            end = int(request["toBlock"], 16)
            requested_ranges.append((start, end))
            if end - start + 1 > 100:
                raise RuntimeError("block range greater than 100 max")
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "cannot complete within remaining chunk budget"):
                rce.fetch_reward_claimed_logs(
                    "xlayer", config, 1_000, 1_000_000, ["0x" + "2" * 40]
                )

        self.assertEqual(requested_ranges, [(1_000, 1_999), (1_000, 1_099)])

    def test_reward_claim_log_scan_allows_exact_chunk_budget_across_topic_batches(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 100,
            "distributorBatchSize": 1,
            "maxLogChunks": 2,
        }
        requested_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            requested_ranges.append((int(request["fromBlock"], 16), int(request["toBlock"], 16)))
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            logs = rce.fetch_reward_claimed_logs(
                "test",
                config,
                1_000,
                1_099,
                ["0x" + "2" * 40, "0x" + "3" * 40],
            )

        self.assertEqual(logs, [])
        self.assertEqual(requested_ranges, [(1_000, 1_099), (1_000, 1_099)])

    def test_reward_claim_log_scan_retains_learned_cap_across_topic_batches(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 1_000,
            "distributorBatchSize": 1,
        }
        requested_ranges = []

        def fake_rpc(_rpc_urls, _method, params, timeout=30):
            request = params[0]
            start = int(request["fromBlock"], 16)
            end = int(request["toBlock"], 16)
            requested_ranges.append((start, end))
            if end - start + 1 > 100:
                raise RuntimeError("block range greater than 100 max")
            return []

        with patch.object(rce, "rpc_request", side_effect=fake_rpc), patch.object(rce.time, "sleep"):
            logs = rce.fetch_reward_claimed_logs(
                "test",
                config,
                1_000,
                1_150,
                ["0x" + "2" * 40, "0x" + "3" * 40],
            )

        self.assertEqual(logs, [])
        self.assertEqual(
            requested_ranges,
            [
                (1_000, 1_150),
                (1_000, 1_099),
                (1_100, 1_150),
                (1_000, 1_099),
                (1_100, 1_150),
            ],
        )

    def test_reward_claim_log_scan_rejects_null_rpc_result(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 100,
        }

        with (
            patch.object(rce, "rpc_request", return_value=None) as rpc,
            patch.object(rce.time, "sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "malformed eth_getLogs result.*NoneType"):
                rce.fetch_reward_claimed_logs(
                    "test", config, 1_000, 1_099, ["0x" + "2" * 40]
                )

        self.assertEqual(rpc.call_count, 2)

    def test_reward_claim_log_scan_rejects_non_list_rpc_result(self):
        config = {
            "name": "Test Chain",
            "rpcUrls": ["https://rpc.example"],
            "eventEmitter": "0x" + "1" * 40,
            "chunkSize": 100,
        }

        with (
            patch.object(rce, "rpc_request", return_value={"logs": []}) as rpc,
            patch.object(rce.time, "sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "malformed eth_getLogs result.*dict"):
                rce.fetch_reward_claimed_logs(
                    "test", config, 1_000, 1_099, ["0x" + "2" * 40]
                )

        self.assertEqual(rpc.call_count, 2)

    def test_sharded_manifest_events_are_reloaded_before_incremental_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            chain_dir = root / "data" / "reward-claim-events"
            chain_dir.mkdir(parents=True)
            manifest_path = root / "data" / "reward-claim-events.json"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)

            manifest_path.write_text(json.dumps({
                "schemaVersion": rce.SCHEMA_VERSION,
                "events": [],
                "eventsShardedByChain": True,
                "chainEventFiles": {
                    "berachain": "data/reward-claim-events/berachain.json",
                },
                "chains": {
                    "berachain": {"eventCount": 1, "toBlock": 100},
                },
            }), encoding="utf-8")
            chain_event = {
                "chainKey": "berachain",
                "txHash": "0x" + "b" * 64,
                "logIndex": 7,
                "blockNumber": 99,
                "timestamp": 12345,
                "user": "0x" + "1" * 40,
                "distributor": "0x" + "2" * 40,
            }
            (chain_dir / "berachain.json").write_text(json.dumps({
                "schemaVersion": rce.SCHEMA_VERSION,
                "chains": {"berachain": {"eventCount": 1, "toBlock": 100}},
                "events": [chain_event],
            }), encoding="utf-8")

            with patch.object(rce, "ROOT_DIR", str(root)), \
                    patch.object(rce, "OUTPUT_JSON", str(manifest_path)), \
                    patch.object(rce, "CHAIN_OUTPUT_DIR", str(chain_dir)):
                payload = rce.load_existing_reward_claim_payload()

        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["txHash"], chain_event["txHash"])

    def test_known_berachain_odolo_claim_is_indexed(self):
        payload = json.loads((ROOT / "data" / "reward-claim-events" / "berachain.json").read_text(encoding="utf-8"))
        matches = [
            event for event in payload.get("events", [])
            if str(event.get("txHash", "")).lower() == KNOWN_BERA_ODOLO_CLAIM_TX
        ]

        self.assertEqual(len(matches), 1)
        event = matches[0]
        self.assertEqual(event.get("user"), "0x28da3dde285d8f1f87b2d858f89961bb8b9af180")
        self.assertEqual(event.get("distributor"), "0x79e6e932bf6686a4d357d7821e6e08835ba8a026")
        self.assertEqual(event.get("blockNumber"), 23198982)
        self.assertEqual(event.get("epoch"), 60)
        self.assertEqual(event.get("amountWei"), "21180233137303023902")
        self.assertEqual(event.get("tokenSymbol"), "oDOLO")

    def test_recent_berachain_odolo_claim_is_indexed(self):
        payload = json.loads((ROOT / "data" / "reward-claim-events" / "berachain.json").read_text(encoding="utf-8"))
        matches = [
            event for event in payload.get("events", [])
            if str(event.get("txHash", "")).lower() == RECENT_BERA_ODOLO_CLAIM_TX
        ]

        self.assertEqual(len(matches), 1)
        event = matches[0]
        self.assertEqual(event.get("user"), "0x28da3dde285d8f1f87b2d858f89961bb8b9af180")
        self.assertEqual(event.get("distributor"), "0x79e6e932bf6686a4d357d7821e6e08835ba8a026")
        self.assertEqual(event.get("blockNumber"), 23283680)
        self.assertEqual(event.get("epoch"), 61)
        self.assertEqual(event.get("amountWei"), "19417570675568485919")
        self.assertEqual(event.get("tokenSymbol"), "oDOLO")


if __name__ == "__main__":
    unittest.main()
