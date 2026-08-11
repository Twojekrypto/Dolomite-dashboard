import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from unittest.mock import Mock

from eth_abi import encode

import generate_dolo_liquidity as liquidity


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "data" / "dolo-liquidity-pools.json"
FIXTURES = ROOT / "tests" / "fixtures" / "dolo-liquidity"

DOLO = "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654"
QUOTE = "0x1111111111111111111111111111111111111111"

PRIMARY_IDENTIFIERS = {
    (
        "ethereum",
        "uniswap-v4",
        "0x2d97d14362ae5a19a15adb230cf8840ee7e133bf942fd8efd754ae4d078727ea",
    ),
    (
        "ethereum",
        "uniswap-v3",
        "0x003896387666c5c11458eeb3f927b72a11b19783",
    ),
    (
        "ethereum",
        "uniswap-v4",
        "0x6f6f24b5a1cd819382379eb032466b8bac7ea0697cfcf31b7350b55ff4f1c472",
    ),
    (
        "ethereum",
        "uniswap-v4",
        "0x728e6e3b736e28f6b52f72ecec16a056b8ac6d9e05736a84e6b6128df9b1a12a",
    ),
    (
        "berachain",
        "kodiak-v3",
        "0xd5980e98a89e2d2361b3be657e8a003c6d3514e3",
    ),
}

SECONDARY_IDENTIFIERS = {
    (
        "berachain",
        "bulla-v3",
        "0x8991017b74f9f8070bff5b322802dd26e05e0cc7",
    ),
    (
        "berachain",
        "kodiak-v3",
        "0x8194ed4d6701b7a1b40e48431de37047f0248b0b",
    ),
}


class RegistryContractTests(unittest.TestCase):
    def test_production_registry_contains_approved_primary_and_secondary_pools(self):
        registry = liquidity.load_registry(REGISTRY)
        actual_primary = {
            (row["chainKey"], row["adapter"], row["identifier"])
            for row in registry["pools"]
            if row["primary"]
        }
        actual_secondary = {
            (row["chainKey"], row["adapter"], row["identifier"])
            for row in registry["pools"]
            if not row["primary"] and row.get("priority", 999) <= 20
        }

        self.assertTrue(PRIMARY_IDENTIFIERS.issubset(actual_primary))
        self.assertTrue(SECONDARY_IDENTIFIERS.issubset(actual_secondary))
        self.assertEqual(registry["display"]["hideBelowLiquidityUsd"], 1000)
        bulla = next(row for row in registry["pools"] if row["adapter"] == "bulla-v3")
        self.assertEqual(bulla["version"], "Algebra Integral")

    def test_production_registry_uses_official_manager_and_factory_addresses(self):
        registry = liquidity.load_registry(REGISTRY)
        ethereum = registry["chains"]["ethereum"]
        berachain = registry["chains"]["berachain"]

        self.assertEqual(ethereum["discoveryStartBlock"], 21_500_000)
        self.assertEqual(
            ethereum["adapters"]["uniswap-v3"]["factory"],
            "0x1f98431c8ad98523631ae4a59f267346ea31f984",
        )
        self.assertEqual(
            ethereum["adapters"]["uniswap-v3"]["positionManager"],
            "0xc36442b4a4522e871399cd717abdd847ab11fe88",
        )
        self.assertEqual(
            ethereum["adapters"]["uniswap-v4"]["poolManager"],
            "0x000000000004444c5dc75cb358380d2e3de08a90",
        )
        self.assertEqual(
            ethereum["adapters"]["uniswap-v4"]["positionManager"],
            "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e",
        )
        self.assertEqual(berachain["discoveryStartBlock"], 2_900_000)
        self.assertEqual(
            berachain["adapters"]["kodiak-v3"]["factory"],
            "0xd84cbf0b02636e7f53db9e5e45a616e05d710990",
        )
        self.assertEqual(
            berachain["adapters"]["kodiak-v3"]["positionManager"],
            "0xfe5e8c83ffe4d9627a75eaa7fee864768db989bd",
        )
        self.assertEqual(
            berachain["custody"]["kodiakIslandFactory"],
            "0x5261c5a5f08818c08ed0eb036d9575ba1e02c1d6",
        )
        self.assertEqual(
            berachain["custody"]["kodiakFarmFactory"],
            "0xaeaa563d9110f833fa3fb1ff9a35dfba11b0c9cf",
        )

    def test_registry_rejects_duplicate_pool_identity(self):
        payload = json.loads((FIXTURES / "registry-minimal.json").read_text())
        payload["pools"].append(dict(payload["pools"][0]))

        with self.assertRaisesRegex(ValueError, "duplicate pool identity"):
            self._load_payload(payload)

    def test_registry_rejects_wrong_identifier_type_or_width(self):
        payload = json.loads((FIXTURES / "registry-minimal.json").read_text())
        payload["pools"][0]["identifierType"] = "contract"
        with self.assertRaisesRegex(ValueError, "poolId"):
            self._load_payload(payload)

        payload = json.loads((FIXTURES / "registry-minimal.json").read_text())
        payload["pools"][0]["identifier"] = "0x1234"
        with self.assertRaisesRegex(ValueError, "bytes32"):
            self._load_payload(payload)

    def test_registry_rejects_unknown_adapter_missing_block_and_bad_threshold(self):
        payload = json.loads((FIXTURES / "registry-minimal.json").read_text())
        payload["pools"][0]["adapter"] = "guess-dex"
        with self.assertRaisesRegex(ValueError, "unknown adapter"):
            self._load_payload(payload)

        payload = json.loads((FIXTURES / "registry-minimal.json").read_text())
        del payload["chains"]["ethereum"]["discoveryStartBlock"]
        with self.assertRaisesRegex(ValueError, "discoveryStartBlock"):
            self._load_payload(payload)

        payload = json.loads((FIXTURES / "registry-minimal.json").read_text())
        payload["display"]["hideBelowLiquidityUsd"] = 0
        with self.assertRaisesRegex(ValueError, "hideBelowLiquidityUsd"):
            self._load_payload(payload)

    def _load_payload(self, payload):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "registry.json"
            path.write_text(json.dumps(payload))
            return liquidity.load_registry(path)


class ExactLiquidityMathTests(unittest.TestCase):
    def test_event_key_normalizes_chain_hash_and_log_index(self):
        self.assertEqual(
            liquidity.event_key(" Ethereum ", "0x" + "AB" * 32, "0x0f"),
            "ethereum:0x" + "ab" * 32 + ":15",
        )
        with self.assertRaisesRegex(ValueError, "transaction hash"):
            liquidity.event_key("ethereum", "0x1234", 1)

    def test_tick_price_is_normalized_as_paired_token_per_one_dolo(self):
        getcontext().prec = 90
        expected = Decimal("1.0001") ** 100

        direct = liquidity.tick_to_paired_per_dolo(
            100, DOLO, QUOTE, 18, 18, DOLO
        )
        reversed_pair = liquidity.tick_to_paired_per_dolo(
            -100, QUOTE, DOLO, 18, 18, DOLO
        )
        decimal_adjusted = liquidity.tick_to_paired_per_dolo(
            0, DOLO, QUOTE, 18, 6, DOLO
        )

        self.assertEqual(direct, expected)
        self.assertEqual(reversed_pair, expected)
        self.assertEqual(decimal_adjusted, Decimal(10) ** 12)

    def test_tick_price_rejects_pair_without_dolo(self):
        with self.assertRaisesRegex(ValueError, "DOLO"):
            liquidity.tick_to_paired_per_dolo(
                0,
                "0x2222222222222222222222222222222222222222",
                QUOTE,
                18,
                18,
                DOLO,
            )

    def test_concentrated_liquidity_amounts_are_exact_below_inside_and_above(self):
        q96 = 1 << 96

        self.assertEqual(
            liquidity.amounts_for_liquidity(1000, q96 // 4, q96 // 2, q96 * 2),
            (1500, 0),
        )
        self.assertEqual(
            liquidity.amounts_for_liquidity(1000, q96, q96 // 2, q96 * 2),
            (500, 500),
        )
        self.assertEqual(
            liquidity.amounts_for_liquidity(1000, q96 * 3, q96 // 2, q96 * 2),
            (0, 1500),
        )
        self.assertEqual(
            liquidity.amounts_for_liquidity(1000, q96 // 2, q96 // 2, q96 * 2),
            (1500, 0),
        )

    def test_concentrated_liquidity_rejects_invalid_range(self):
        q96 = 1 << 96
        with self.assertRaisesRegex(ValueError, "sqrt range"):
            liquidity.amounts_for_liquidity(1, q96, q96, q96)
        with self.assertRaisesRegex(ValueError, "liquidity"):
            liquidity.amounts_for_liquidity(-1, q96, q96 // 2, q96 * 2)

    def test_v2_underlying_uses_precise_integer_share(self):
        self.assertEqual(
            liquidity.v2_underlying(25, 100, 1001, 2003),
            (250, 500),
        )
        with self.assertRaisesRegex(ValueError, "total supply"):
            liquidity.v2_underlying(1, 0, 100, 100)

    def test_range_classification_has_explicit_boundaries_and_unavailable(self):
        self.assertEqual(liquidity.classify_range(10, 10, 20), "in_range")
        self.assertEqual(liquidity.classify_range(19, 10, 20), "in_range")
        self.assertEqual(liquidity.classify_range(20, 10, 20), "out_of_range")
        self.assertEqual(liquidity.classify_range(9, 10, 20), "out_of_range")
        self.assertEqual(liquidity.classify_range(None, 10, 20), "unavailable")


class IncrementalReplayTests(unittest.TestCase):
    def setUp(self):
        self.tx_a = "0x" + "aa" * 32
        self.tx_b = "0x" + "bb" * 32
        self.address = "0x" + "12" * 20
        self.topic = "0x" + "34" * 32

    def _rpc_log(self, block, transaction_index, log_index, tx_hash=None, **overrides):
        row = {
            "address": self.address,
            "blockNumber": hex(block),
            "transactionIndex": hex(transaction_index),
            "logIndex": hex(log_index),
            "transactionHash": tx_hash or self.tx_a,
            "blockHash": "0x" + f"{block:064x}",
            "data": "0x",
            "topics": [self.topic],
            "removed": False,
        }
        row.update(overrides)
        return row

    def test_resume_block_uses_inclusive_128_block_overlap(self):
        self.assertEqual(
            liquidity.resume_block({"lastScannedBlock": 1000}, 100, overlap=128),
            873,
        )
        self.assertEqual(
            liquidity.resume_block({"lastScannedBlock": 150}, 100, overlap=128),
            100,
        )
        self.assertEqual(liquidity.resume_block(None, 100, overlap=128), 100)

    def test_block_ranges_cover_boundaries_once(self):
        self.assertEqual(
            list(liquidity.block_ranges(10, 20, 4)),
            [(10, 13), (14, 17), (18, 20)],
        )
        self.assertEqual(list(liquidity.block_ranges(20, 20, 4)), [(20, 20)])
        self.assertEqual(list(liquidity.block_ranges(21, 20, 4)), [])

    def test_scan_logs_sorts_provider_order_and_advances_after_all_chunks(self):
        calls = []

        def fake_rpc(_endpoints, payload, **_kwargs):
            calls.append(payload["params"][0])
            start = int(payload["params"][0]["fromBlock"], 16)
            if start == 10:
                rows = [
                    self._rpc_log(11, 1, 4, tx_hash=self.tx_b),
                    self._rpc_log(10, 2, 9, tx_hash=self.tx_a),
                ]
            else:
                rows = [self._rpc_log(12, 0, 0, tx_hash="0x" + "cc" * 32)]
            return {"jsonrpc": "2.0", "id": payload["id"], "result": rows}

        rows, cursor = liquidity.scan_logs(
            "ethereum",
            self.address,
            [self.topic],
            10,
            12,
            2,
            rpc=fake_rpc,
            endpoints=["https://rpc.invalid"],
        )

        self.assertEqual([(row["blockNumber"], row["logIndex"]) for row in rows], [(10, 9), (11, 4), (12, 0)])
        self.assertEqual(cursor, 12)
        self.assertEqual(
            [(int(call["fromBlock"], 16), int(call["toBlock"], 16)) for call in calls],
            [(10, 11), (12, 12)],
        )

    def test_scan_failure_does_not_mutate_or_advance_previous_source(self):
        previous = {"lastScannedBlock": 50, "status": "complete"}
        calls = 0

        def fake_rpc(_endpoints, payload, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("provider failed")
            return {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": [self._rpc_log(51, 0, 0)],
            }

        with self.assertRaisesRegex(RuntimeError, "provider failed"):
            liquidity.scan_logs(
                "ethereum",
                self.address,
                [self.topic],
                51,
                55,
                3,
                rpc=fake_rpc,
                endpoints=["https://rpc.invalid"],
            )

        self.assertEqual(previous, {"lastScannedBlock": 50, "status": "complete"})

    def test_scan_rejects_removed_or_malformed_logs(self):
        for row, message in [
            (self._rpc_log(10, 0, 0, removed=True), "removed log"),
            (self._rpc_log(10, 0, 0, transactionHash="0x1234"), "transaction hash"),
        ]:
            with self.subTest(message=message):
                fake = Mock(return_value={"jsonrpc": "2.0", "id": 1, "result": [row]})
                with self.assertRaisesRegex(ValueError, message):
                    liquidity.scan_logs(
                        "ethereum",
                        self.address,
                        [self.topic],
                        10,
                        10,
                        1,
                        rpc=fake,
                        endpoints=["https://rpc.invalid"],
                    )

    def test_dedupe_accepts_exact_duplicates_and_rejects_key_collisions(self):
        first = liquidity.normalize_rpc_log(self._rpc_log(10, 0, 1))
        duplicate = dict(first)
        unique = liquidity.normalize_rpc_log(self._rpc_log(10, 0, 2))

        self.assertEqual(
            liquidity.dedupe_logs("ethereum", [first, duplicate, unique]),
            [first, unique],
        )

        collision = dict(first, address="0x" + "99" * 20)
        with self.assertRaisesRegex(ValueError, "event-key collision"):
            liquidity.dedupe_logs("ethereum", [first, collision])

    def test_stale_fallback_preserves_only_failed_adapter_rows_and_cursor(self):
        previous = {
            "sources": [
                {"key": "ethereum:uniswap-v3", "status": "complete", "lastScannedBlock": 99},
                {"key": "berachain:kodiak-v3", "status": "complete", "lastScannedBlock": 77},
            ],
            "pools": [
                {"id": "pool-a", "sourceKey": "ethereum:uniswap-v3", "quality": "verified"},
                {"id": "pool-b", "sourceKey": "berachain:kodiak-v3", "quality": "verified"},
            ],
            "activePositions": [
                {"id": "position-a", "sourceKey": "ethereum:uniswap-v3", "quality": "verified"},
                {"id": "position-b", "sourceKey": "berachain:kodiak-v3", "quality": "verified"},
            ],
            "history": [
                {"id": "history-a", "sourceKey": "ethereum:uniswap-v3", "quality": "partial"},
                {"id": "history-b", "sourceKey": "berachain:kodiak-v3", "quality": "verified"},
            ],
        }

        fragment = liquidity.preserve_stale_adapter(
            previous,
            "ethereum:uniswap-v3",
            RuntimeError("RPC https://secret.example/v2/0123456789abcdef failed"),
            "2026-08-11T18:00:00Z",
        )

        self.assertEqual(fragment["source"]["lastScannedBlock"], 99)
        self.assertEqual(fragment["source"]["status"], "stale")
        self.assertNotIn("0123456789abcdef", fragment["source"]["errors"][0])
        self.assertEqual([row["id"] for row in fragment["pools"]], ["pool-a"])
        self.assertEqual(fragment["pools"][0]["quality"], "stale")
        self.assertEqual(fragment["activePositions"][0]["quality"], "stale")
        self.assertEqual(fragment["history"][0]["quality"], "partial")

    def test_load_previous_artifact_accepts_missing_and_rejects_corrupt(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "artifact.json"
            self.assertEqual(liquidity.load_previous_artifact(path), {})
            path.write_text("not json")
            with self.assertRaisesRegex(ValueError, "previous artifact"):
                liquidity.load_previous_artifact(path)

    def test_block_timestamp_batch_uses_cache_and_fails_closed_on_missing(self):
        cache = {("ethereum", 10): 1000}

        def fake_batch(_endpoints, payloads, **_kwargs):
            self.assertEqual([payload["params"][0] for payload in payloads], [hex(11)])
            return {
                "block:11": {
                    "jsonrpc": "2.0",
                    "id": "block:11",
                    "result": {"number": hex(11), "timestamp": hex(1100)},
                }
            }, []

        actual = liquidity.fetch_block_timestamps(
            "ethereum",
            [10, 11, 10],
            endpoints=["https://rpc.invalid"],
            rpc_batch=fake_batch,
            cache=cache,
        )
        self.assertEqual(actual, {10: 1000, 11: 1100})

        def missing_batch(_endpoints, payloads, **_kwargs):
            return {}, [payloads[0]["id"]]

        with self.assertRaisesRegex(RuntimeError, "timestamp"):
            liquidity.fetch_block_timestamps(
                "ethereum",
                [12],
                endpoints=["https://rpc.invalid"],
                rpc_batch=missing_batch,
                cache={},
            )


class V2AdapterTests(unittest.TestCase):
    ZERO = "0x" + "00" * 20
    ALICE = "0x" + "aa" * 20
    BOB = "0x" + "bb" * 20
    CAROL_CONTRACT = "0x" + "cc" * 20
    ROUTER = "0x" + "dd" * 20
    POOL = "0x8991017b74f9f8070bff5b322802dd26e05e0cc7"
    HONEY = "0xfcbd14dc51f0a4d49d5e53c2e0950e0bc26d0dce"

    @staticmethod
    def _topic_address(address):
        return "0x" + address[2:].lower().rjust(64, "0")

    def _log(self, signature, indexed, abi_types, values, block, log_index, tx_byte):
        return {
            "address": self.POOL,
            "blockNumber": block,
            "transactionIndex": 0,
            "logIndex": log_index,
            "transactionHash": "0x" + tx_byte * 64,
            "blockHash": "0x" + f"{block:064x}",
            "data": "0x" + encode(abi_types, values).hex(),
            "topics": [liquidity.event_topic(signature)] + indexed,
            "removed": False,
            "timestamp": 1_700_000_000 + block,
        }

    def _transfer(self, sender, recipient, value, block, log_index, tx_byte):
        return self._log(
            "Transfer(address,address,uint256)",
            [self._topic_address(sender), self._topic_address(recipient)],
            ["uint256"],
            [value],
            block,
            log_index,
            tx_byte,
        )

    def _mint(self, sender, amount0, amount1, block, log_index, tx_byte):
        return self._log(
            "Mint(address,uint256,uint256)",
            [self._topic_address(sender)],
            ["uint256", "uint256"],
            [amount0, amount1],
            block,
            log_index,
            tx_byte,
        )

    def _burn(self, sender, amount0, amount1, recipient, block, log_index, tx_byte):
        return self._log(
            "Burn(address,uint256,uint256,address)",
            [self._topic_address(sender), self._topic_address(recipient)],
            ["uint256", "uint256"],
            [amount0, amount1],
            block,
            log_index,
            tx_byte,
        )

    def test_decode_v2_events_uses_exact_indexed_and_data_fields(self):
        transfer = liquidity.decode_v2_log(
            self._transfer(self.ALICE, self.BOB, 25, 101, 3, "1")
        )
        mint = liquidity.decode_v2_log(
            self._mint(self.ROUTER, 1000, 2000, 102, 4, "2")
        )
        burn = liquidity.decode_v2_log(
            self._burn(self.ROUTER, 200, 400, self.BOB, 103, 5, "3")
        )

        self.assertEqual(
            (transfer["kind"], transfer["from"], transfer["to"], transfer["valueRaw"]),
            ("transfer", self.ALICE, self.BOB, 25),
        )
        self.assertEqual(
            (mint["kind"], mint["sender"], mint["amount0Raw"], mint["amount1Raw"]),
            ("mint", self.ROUTER, 1000, 2000),
        )
        self.assertEqual(
            (burn["kind"], burn["to"], burn["amount0Raw"], burn["amount1Raw"]),
            ("burn", self.BOB, 200, 400),
        )

    def test_replay_v2_tracks_current_wallets_and_only_liquidity_actions(self):
        logs = [
            self._transfer(self.ZERO, self.ZERO, 1000, 100, 0, "1"),
            self._transfer(self.ZERO, self.ALICE, 100, 100, 1, "1"),
            self._mint(self.ROUTER, 1000, 2000, 100, 2, "1"),
            self._transfer(self.ALICE, self.BOB, 25, 101, 0, "2"),
            self._transfer(self.BOB, self.POOL, 25, 102, 0, "3"),
            self._transfer(self.POOL, self.ZERO, 25, 102, 1, "3"),
            self._burn(self.ROUTER, 200, 400, self.BOB, 102, 2, "3"),
            self._transfer(self.ZERO, self.BOB, 10, 103, 0, "4"),
            self._mint(self.ROUTER, 100, 200, 103, 1, "4"),
            self._transfer(self.ZERO, self.CAROL_CONTRACT, 20, 104, 0, "5"),
            self._mint(self.ROUTER, 300, 600, 104, 1, "5"),
        ]
        latest = {
            "token0": DOLO,
            "token1": self.HONEY,
            "decimals0": 18,
            "decimals1": 18,
            "totalSupply": 1105,
            "reserve0": 11050,
            "reserve1": 22100,
            "balances": {
                self.ALICE: 75,
                self.BOB: 10,
                self.CAROL_CONTRACT: 20,
            },
        }
        pool = {
            "chainKey": "berachain",
            "adapter": "bulla-v2",
            "identifier": self.POOL,
            "identifierType": "contract",
            "pair": "DOLO/HONEY",
        }

        result = liquidity.replay_v2_pool(
            pool,
            logs,
            latest,
            contract_addresses={self.CAROL_CONTRACT},
        )

        rows = {row["custodian"]: row for row in result["activePositions"]}
        self.assertEqual(set(rows), {self.ALICE, self.BOB, self.CAROL_CONTRACT})
        self.assertEqual(rows[self.ALICE]["lpBalanceRaw"], "75")
        self.assertEqual(rows[self.ALICE]["amount0Raw"], "750")
        self.assertEqual(rows[self.ALICE]["amount1Raw"], "1500")
        self.assertEqual(rows[self.ALICE]["doloRaw"], "750")
        self.assertEqual(rows[self.ALICE]["pairedRaw"], "1500")
        self.assertEqual(rows[self.ALICE]["rangeStatus"], "full_range")
        self.assertEqual(rows[self.ALICE]["beneficialOwner"], self.ALICE)
        self.assertEqual(rows[self.ALICE]["attributionPath"], "direct")
        self.assertIsNone(rows[self.CAROL_CONTRACT]["beneficialOwner"])
        self.assertEqual(rows[self.CAROL_CONTRACT]["positionStatus"], "custodied_unresolved")
        self.assertEqual(rows[self.CAROL_CONTRACT]["quality"], "unavailable")

        history = result["history"]
        self.assertEqual([row["action"] for row in history], ["Added", "Closed", "Added", "Added"])
        self.assertEqual([row["blockNumber"] for row in history], [100, 102, 103, 104])
        self.assertNotIn(101, [row["blockNumber"] for row in history])
        self.assertEqual(history[1]["beneficialOwner"], self.BOB)
        self.assertEqual(history[1]["doloRaw"], "200")
        self.assertEqual(history[1]["pairedRaw"], "400")
        self.assertEqual(result["sourceStatus"], "complete")

    def test_replay_v2_marks_balance_mismatch_partial_without_rewriting_ledger(self):
        logs = [
            self._transfer(self.ZERO, self.ALICE, 100, 100, 0, "1"),
            self._mint(self.ROUTER, 1000, 2000, 100, 1, "1"),
        ]
        result = liquidity.replay_v2_pool(
            {
                "chainKey": "berachain",
                "adapter": "bulla-v2",
                "identifier": self.POOL,
                "identifierType": "contract",
                "pair": "DOLO/HONEY",
            },
            logs,
            {
                "token0": DOLO,
                "token1": self.HONEY,
                "decimals0": 18,
                "decimals1": 18,
                "totalSupply": 100,
                "reserve0": 1000,
                "reserve1": 2000,
                "balances": {self.ALICE: 99},
            },
        )

        row = result["activePositions"][0]
        self.assertEqual(row["lpBalanceRaw"], "100")
        self.assertEqual(row["onchainLpBalanceRaw"], "99")
        self.assertEqual(row["quality"], "partial")
        self.assertEqual(result["sourceStatus"], "partial")
        self.assertEqual(result["mismatches"][0]["address"], self.ALICE)


class V3AdapterTests(unittest.TestCase):
    ZERO = "0x" + "00" * 20
    ALICE = "0x" + "aa" * 20
    BOB = "0x" + "bb" * 20
    ROUTER = "0x" + "dd" * 20
    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    OTHER0 = "0x" + "11" * 20
    OTHER1 = "0x" + "22" * 20
    ETH_POOL = "0x003896387666c5c11458eeb3f927b72a11b19783"
    OTHER_POOL = "0x" + "33" * 20
    ETH_NPM = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
    ETH_FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"

    @staticmethod
    def _topic_address(address):
        return "0x" + address[2:].lower().rjust(64, "0")

    @staticmethod
    def _topic_uint(value):
        return "0x" + f"{value:064x}"

    def _log(self, address, signature, indexed, abi_types, values, block, log_index, tx_byte):
        return {
            "address": address,
            "blockNumber": block,
            "transactionIndex": 0,
            "logIndex": log_index,
            "transactionHash": "0x" + tx_byte * 64,
            "blockHash": "0x" + f"{block:064x}",
            "data": "0x" + encode(abi_types, values).hex(),
            "topics": [liquidity.event_topic(signature)] + indexed,
            "removed": False,
            "timestamp": 1_700_000_000 + block,
        }

    def _pool_created(self, token0, token1, fee, tick_spacing, pool, block, log_index):
        return self._log(
            self.ETH_FACTORY,
            "PoolCreated(address,address,uint24,int24,address)",
            [self._topic_address(token0), self._topic_address(token1), self._topic_uint(fee)],
            ["int24", "address"],
            [tick_spacing, pool],
            block,
            log_index,
            "1",
        )

    def _npm_transfer(self, sender, recipient, token_id, block, log_index, tx_byte):
        return self._log(
            self.ETH_NPM,
            "Transfer(address,address,uint256)",
            [self._topic_address(sender), self._topic_address(recipient), self._topic_uint(token_id)],
            [],
            [],
            block,
            log_index,
            tx_byte,
        )

    def _npm_liquidity(self, kind, token_id, amount, amount0, amount1, block, log_index, tx_byte):
        signature = (
            "IncreaseLiquidity(uint256,uint128,uint256,uint256)"
            if kind == "increase"
            else "DecreaseLiquidity(uint256,uint128,uint256,uint256)"
        )
        return self._log(
            self.ETH_NPM,
            signature,
            [self._topic_uint(token_id)],
            ["uint128", "uint256", "uint256"],
            [amount, amount0, amount1],
            block,
            log_index,
            tx_byte,
        )

    def test_v3_tick_math_matches_canonical_extremes(self):
        self.assertEqual(liquidity.sqrt_ratio_at_tick(0), 1 << 96)
        self.assertEqual(liquidity.sqrt_ratio_at_tick(-887272), 4295128739)
        self.assertEqual(
            liquidity.sqrt_ratio_at_tick(887272),
            1461446703485210103287273052203988822378723970342,
        )
        with self.assertRaisesRegex(ValueError, "tick"):
            liquidity.sqrt_ratio_at_tick(887273)

    def test_v3_factory_discovery_filters_both_token_orders_for_dolo(self):
        logs = [
            self._pool_created(DOLO, self.USDC, 3000, 60, self.ETH_POOL, 100, 0),
            self._pool_created(self.OTHER0, self.OTHER1, 500, 10, self.OTHER_POOL, 101, 0),
            self._pool_created(self.USDC, DOLO, 10000, 200, "0x" + "44" * 20, 102, 0),
        ]

        pools = liquidity.discover_v3_dolo_pools(logs, DOLO)

        self.assertEqual([row["pool"] for row in pools], [self.ETH_POOL, "0x" + "44" * 20])
        self.assertEqual(pools[0]["fee"], 3000)
        self.assertEqual(pools[0]["tickSpacing"], 60)
        self.assertEqual(pools[1]["token1"], DOLO)

    def test_v3_decoder_and_archive_mapping_fail_closed(self):
        logs = [
            self._npm_transfer(self.ZERO, self.ALICE, 1, 110, 0, "2"),
            self._npm_liquidity("increase", 1, 1000, 100, 200, 110, 1, "2"),
            self._npm_liquidity("increase", 99, 500, 50, 100, 111, 0, "3"),
        ]
        snapshots = {
            1: {
                "pool": self.ETH_POOL,
                "token0": DOLO,
                "token1": self.USDC,
                "fee": 3000,
                "tickLower": -100,
                "tickUpper": 100,
                "snapshotBlock": 110,
            }
        }

        mapped = liquidity.map_v3_events_to_pools(
            logs,
            snapshots,
            {self.ETH_POOL},
            DOLO,
        )

        self.assertEqual(len(mapped["eventsByPool"][self.ETH_POOL]), 2)
        increase = mapped["eventsByPool"][self.ETH_POOL][1]
        self.assertEqual(
            (increase["kind"], increase["tokenId"], increase["liquidityRaw"], increase["amount0Raw"]),
            ("increase", 1, 1000, 100),
        )
        self.assertEqual(len(mapped["unresolved"]), 1)
        self.assertEqual(mapped["unresolved"][0]["tokenId"], 99)
        self.assertEqual(mapped["unresolved"][0]["quality"], "partial")
        self.assertIn("archive position snapshot unavailable", mapped["unresolved"][0]["reason"])

    def test_v3_lifecycle_preserves_closed_history_and_uses_current_principal_only(self):
        logs = [
            self._npm_transfer(self.ZERO, self.ALICE, 1, 120, 0, "4"),
            self._npm_liquidity("increase", 1, 1000, 100, 200, 120, 1, "4"),
            self._npm_liquidity("increase", 1, 200, 20, 40, 121, 0, "5"),
            self._npm_transfer(self.ALICE, self.BOB, 1, 122, 0, "6"),
            self._npm_liquidity("decrease", 1, 500, 50, 100, 123, 0, "7"),
            self._npm_liquidity("decrease", 1, 700, 70, 140, 124, 0, "8"),
            self._npm_transfer(self.BOB, self.ZERO, 1, 124, 1, "8"),
            self._npm_transfer(self.ZERO, self.ALICE, 2, 125, 0, "9"),
            self._npm_liquidity("increase", 2, 1_000_000, 1000, 2000, 125, 1, "9"),
            self._npm_transfer(self.ALICE, self.BOB, 2, 126, 0, "a"),
        ]
        snapshot = {
            "pool": self.ETH_POOL,
            "token0": DOLO,
            "token1": self.USDC,
            "fee": 3000,
            "tickLower": -100,
            "tickUpper": 100,
            "snapshotBlock": 120,
        }
        mapped = liquidity.map_v3_events_to_pools(
            logs,
            {1: snapshot, 2: dict(snapshot, snapshotBlock=125)},
            {self.ETH_POOL},
            DOLO,
        )
        pool = {
            "chainKey": "ethereum",
            "adapter": "uniswap-v3",
            "identifier": self.ETH_POOL,
            "identifierType": "contract",
            "pair": "DOLO/USDC",
        }
        latest = {
            2: {
                **snapshot,
                "liquidity": 1_000_000,
                "owner": self.BOB,
                "tokensOwed0": 999_999_999,
                "tokensOwed1": 888_888_888,
            }
        }
        pool_state = {
            "sqrtPriceX96": 1 << 96,
            "currentTick": 0,
            "decimals0": 18,
            "decimals1": 6,
        }

        result = liquidity.build_v3_rows(
            pool,
            mapped["eventsByPool"][self.ETH_POOL],
            latest,
            pool_state,
        )

        self.assertEqual(
            [row["action"] for row in result["history"]],
            ["Added", "Increased", "Removed", "Closed", "Added"],
        )
        self.assertEqual(result["history"][2]["beneficialOwner"], self.BOB)
        self.assertEqual(result["history"][3]["beneficialOwner"], self.BOB)
        self.assertEqual(result["history"][3]["doloRaw"], "70")
        self.assertEqual(len(result["activePositions"]), 1)
        row = result["activePositions"][0]
        self.assertEqual(row["positionId"], "2")
        self.assertEqual(row["beneficialOwner"], self.BOB)
        self.assertEqual(row["rangeStatus"], "in_range")
        expected_amounts = liquidity.amounts_for_liquidity(
            1_000_000,
            1 << 96,
            liquidity.sqrt_ratio_at_tick(-100),
            liquidity.sqrt_ratio_at_tick(100),
        )
        self.assertEqual((row["amount0Raw"], row["amount1Raw"]), tuple(map(str, expected_amounts)))
        self.assertNotEqual(row["amount0Raw"], str(latest[2]["tokensOwed0"]))
        self.assertNotEqual(row["amount1Raw"], str(latest[2]["tokensOwed1"]))

    def test_kodiak_v3_uses_separate_adapter_identity(self):
        fixture = json.loads((FIXTURES / "v3-kodiak.json").read_text())
        self.assertEqual(fixture["factory"], "0xd84cbf0b02636e7f53db9e5e45a616e05d710990")
        self.assertEqual(fixture["positionManager"], "0xfe5e8c83ffe4d9627a75eaa7fee864768db989bd")
        self.assertNotEqual(fixture["positionManager"], self.ETH_NPM)


class V4AdapterTests(unittest.TestCase):
    ZERO = "0x" + "00" * 20
    ALICE = "0x" + "aa" * 20
    BOB = "0x" + "bb" * 20
    OTHER_SENDER = "0x" + "cc" * 20
    USD1 = "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d"
    POOL_ID = "0x2d97d14362ae5a19a15adb230cf8840ee7e133bf942fd8efd754ae4d078727ea"
    OTHER_POOL_ID = "0x" + "77" * 32
    POOL_MANAGER = "0x000000000004444c5dc75cb358380d2e3de08a90"
    POSITION_MANAGER = "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e"

    @staticmethod
    def _topic_address(address):
        return "0x" + address[2:].lower().rjust(64, "0")

    @staticmethod
    def _topic_uint(value):
        return "0x" + f"{value:064x}"

    def _log(self, address, signature, indexed, abi_types, values, block, log_index, tx_byte):
        return {
            "address": address,
            "blockNumber": block,
            "transactionIndex": 0,
            "logIndex": log_index,
            "transactionHash": "0x" + tx_byte * 64,
            "blockHash": "0x" + f"{block:064x}",
            "data": "0x" + encode(abi_types, values).hex(),
            "topics": [liquidity.event_topic(signature)] + indexed,
            "removed": False,
            "timestamp": 1_700_100_000 + block,
        }

    def _initialize(self, pool_id, currency0, currency1, block, log_index, tx_byte="1"):
        return self._log(
            self.POOL_MANAGER,
            "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)",
            [pool_id, self._topic_address(currency0), self._topic_address(currency1)],
            ["uint24", "int24", "address", "uint160", "int24"],
            [3000, 60, self.ZERO, 1 << 96, 0],
            block,
            log_index,
            tx_byte,
        )

    def _pool_modify(self, token_id, delta, block, log_index, tx_byte, sender=None):
        return self._log(
            self.POOL_MANAGER,
            "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)",
            [self.POOL_ID, self._topic_address(sender or self.POSITION_MANAGER)],
            ["int24", "int24", "int256", "bytes32"],
            [-100, 100, delta, bytes.fromhex(f"{token_id:064x}")],
            block,
            log_index,
            tx_byte,
        )

    def _position_modify(self, token_id, delta, block, log_index, tx_byte):
        return self._log(
            self.POSITION_MANAGER,
            "ModifyPosition(bytes32,address,int24,int24,int256,bytes32)",
            [self.POOL_ID, self._topic_address(self.ALICE)],
            ["int24", "int24", "int256", "bytes32"],
            [-100, 100, delta, bytes.fromhex(f"{token_id:064x}")],
            block,
            log_index,
            tx_byte,
        )

    def _transfer(self, sender, recipient, token_id, block, log_index, tx_byte):
        return self._log(
            self.POSITION_MANAGER,
            "Transfer(address,address,uint256)",
            [self._topic_address(sender), self._topic_address(recipient), self._topic_uint(token_id)],
            [],
            [],
            block,
            log_index,
            tx_byte,
        )

    def test_v4_initialize_discovers_dolo_and_keeps_pool_id_typed(self):
        logs = [
            self._initialize(self.POOL_ID, DOLO, self.USD1, 200, 0),
            self._initialize(self.OTHER_POOL_ID, self.OTHER_SENDER, self.USD1, 201, 0),
        ]

        pools = liquidity.discover_v4_dolo_pools(logs, DOLO)

        self.assertEqual(len(pools), 1)
        self.assertEqual(pools[0]["poolId"], self.POOL_ID)
        self.assertEqual(pools[0]["identifierType"], "poolId")
        self.assertEqual(pools[0]["sqrtPriceX96"], 1 << 96)
        self.assertEqual(pools[0]["tickSpacing"], 60)

    def test_v4_build_requires_canonical_sender_and_exact_modify_pair(self):
        pool_logs = [
            self._initialize(self.POOL_ID, DOLO, self.USD1, 210, 0),
            self._pool_modify(1, 1000, 211, 1, "2"),
            self._pool_modify(1, 200, 212, 0, "3"),
            self._pool_modify(1, -500, 214, 0, "5"),
            self._pool_modify(1, -700, 215, 0, "6"),
            self._pool_modify(2, 1000, 216, 1, "7"),
            self._pool_modify(3, 500, 216, 2, "7"),
            self._pool_modify(4, 100, 217, 0, "8", sender=self.OTHER_SENDER),
        ]
        position_logs = [
            self._transfer(self.ZERO, self.ALICE, 1, 211, 0, "2"),
            self._position_modify(1, 1000, 211, 2, "2"),
            self._position_modify(1, 200, 212, 1, "3"),
            self._transfer(self.ALICE, self.BOB, 1, 213, 0, "4"),
            self._position_modify(1, -500, 214, 1, "5"),
            self._position_modify(1, -700, 215, 1, "6"),
            self._transfer(self.BOB, self.ZERO, 1, 215, 2, "6"),
            self._transfer(self.ZERO, self.ALICE, 2, 216, 0, "7"),
            self._transfer(self.ZERO, self.BOB, 3, 216, 3, "7"),
        ]
        pool = {
            "chainKey": "ethereum",
            "adapter": "uniswap-v4",
            "identifier": self.POOL_ID,
            "identifierType": "poolId",
            "pair": "DOLO/USD1",
            "sourceUrl": "https://dexscreener.com/ethereum/" + self.POOL_ID,
        }
        latest = {
            2: {
                "poolId": self.POOL_ID,
                "currency0": DOLO,
                "currency1": self.USD1,
                "fee": 3000,
                "tickSpacing": 60,
                "hooks": self.ZERO,
                "tickLower": -100,
                "tickUpper": 100,
                "liquidity": 1000,
                "owner": self.ALICE,
            },
            3: {
                "poolId": self.POOL_ID,
                "currency0": DOLO,
                "currency1": self.USD1,
                "fee": 3000,
                "tickSpacing": 60,
                "hooks": self.ZERO,
                "tickLower": -100,
                "tickUpper": 100,
                "liquidity": 500,
                "owner": self.BOB,
            },
        }

        result = liquidity.build_v4_rows(
            pool,
            pool_logs,
            position_logs,
            latest,
            {
                "sqrtPriceX96": 1 << 96,
                "currentTick": 0,
                "decimals0": 18,
                "decimals1": 18,
            },
            pool_manager=self.POOL_MANAGER,
            position_manager=self.POSITION_MANAGER,
        )

        self.assertEqual(
            [row["action"] for row in result["history"]],
            ["Added", "Increased", "Removed", "Closed", "Added", "Added"],
        )
        self.assertEqual(result["history"][2]["beneficialOwner"], self.BOB)
        self.assertEqual(result["history"][3]["beneficialOwner"], self.BOB)
        self.assertTrue(all(row["amountStatus"] == "verified" for row in result["history"][:4]))
        self.assertTrue(all(row["amountStatus"] == "unavailable" for row in result["history"][4:]))
        self.assertTrue(all(row["doloRaw"] is None for row in result["history"][4:]))
        self.assertTrue(all(row["quality"] == "partial" for row in result["history"][4:]))
        self.assertEqual(len(result["unresolved"]), 1)
        self.assertIn("non-canonical sender", result["unresolved"][0]["reason"])
        self.assertEqual(len(result["activePositions"]), 2)
        self.assertTrue(all(row["poolIdentifierType"] == "poolId" for row in result["activePositions"]))
        self.assertTrue(all(row["poolExplorerUrl"] is None for row in result["activePositions"]))
        self.assertTrue(all(row["dexscreenerUrl"].endswith(self.POOL_ID) for row in result["activePositions"]))

    def test_v4_archive_price_can_prove_action_when_no_prior_swap_or_initialize(self):
        pool = {
            "chainKey": "ethereum",
            "adapter": "uniswap-v4",
            "identifier": self.POOL_ID,
            "identifierType": "poolId",
            "pair": "DOLO/USD1",
        }
        result = liquidity.build_v4_rows(
            pool,
            [self._pool_modify(5, 1000, 220, 1, "9")],
            [
                self._transfer(self.ZERO, self.ALICE, 5, 220, 0, "9"),
                self._position_modify(5, 1000, 220, 2, "9"),
            ],
            {},
            {
                "sqrtPriceX96": 1 << 96,
                "currentTick": 0,
                "decimals0": 18,
                "decimals1": 18,
                "currency0": DOLO,
                "currency1": self.USD1,
            },
            pool_manager=self.POOL_MANAGER,
            position_manager=self.POSITION_MANAGER,
            archive_sqrt_prices={(220, 0): 1 << 96},
        )

        self.assertEqual(result["history"][0]["amountStatus"], "verified")
        self.assertIsNotNone(result["history"][0]["doloRaw"])
        self.assertEqual(result["history"][0]["priceEvidence"], "archive_state")


class BeneficialOwnerTests(unittest.TestCase):
    ZERO = "0x" + "00" * 20
    ALICE = "0x" + "aa" * 20
    BOB = "0x" + "bb" * 20
    CAROL = "0x" + "cc" * 20
    FARM = "0x" + "dd" * 20
    UNKNOWN_VAULT = "0x" + "ee" * 20
    MANAGER = "0x" + "12" * 20
    IMPLEMENTATION = "0x" + "13" * 20
    ISLAND = "0x" + "14" * 20
    POOL = "0xd5980e98a89e2d2361b3be657e8a003c6d3514e3"
    WBERA = "0x6969696969696969696969696969696969696969"
    FACTORY = "0x5261c5a5f08818c08ed0eb036d9575ba1e02c1d6"

    @staticmethod
    def _topic_address(address):
        return "0x" + address[2:].lower().rjust(64, "0")

    def test_island_created_discovery_requires_underlying_dolo_pool(self):
        log = {
            "address": self.FACTORY,
            "blockNumber": 300,
            "transactionIndex": 0,
            "logIndex": 1,
            "transactionHash": "0x" + "ab" * 32,
            "blockHash": "0x" + f"{300:064x}",
            "data": "0x" + encode(["address"], [self.IMPLEMENTATION]).hex(),
            "topics": [
                liquidity.event_topic("IslandCreated(address,address,address,address)"),
                self._topic_address(self.POOL),
                self._topic_address(self.MANAGER),
                self._topic_address(self.ISLAND),
            ],
            "removed": False,
            "timestamp": 1_700_200_300,
        }

        discovered = liquidity.discover_kodiak_islands(
            [log],
            {self.POOL: (DOLO, self.WBERA)},
            DOLO,
        )

        self.assertEqual(len(discovered), 1)
        self.assertEqual(discovered[0]["island"], self.ISLAND)
        self.assertEqual(discovered[0]["underlyingPool"], self.POOL)
        self.assertEqual(discovered[0]["implementation"], self.IMPLEMENTATION)

        self.assertEqual(
            liquidity.discover_kodiak_islands(
                [log],
                {self.POOL: ("0x" + "22" * 20, self.WBERA)},
                DOLO,
            ),
            [],
        )

    def test_island_and_farm_allocation_reconciles_exact_underlying(self):
        underlying = {
            "id": "berachain:kodiak-v3:pool:77",
            "sourceKey": "berachain:kodiak-v3",
            "poolId": self.POOL,
            "poolIdentifierType": "contract",
            "chainKey": "berachain",
            "adapter": "kodiak-v3",
            "pair": "DOLO/WBERA",
            "positionId": "77",
            "amount0Raw": "1001",
            "amount1Raw": "2003",
            "doloRaw": "1001",
            "pairedRaw": "2003",
            "rangeStatus": "in_range",
            "valueUsd": None,
            "custodian": self.ISLAND,
            "beneficialOwner": None,
            "quality": "unavailable",
        }
        island = {
            "address": self.ISLAND,
            "totalShares": 100,
            "balances": {
                self.ALICE: 30,
                self.FARM: 50,
                self.UNKNOWN_VAULT: 20,
            },
        }
        farms = [
            {
                "address": self.FARM,
                "stakingToken": self.ISLAND,
                "custodyBalance": 50,
                "stakedBalances": {self.BOB: 30, self.CAROL: 20},
            }
        ]

        rows = liquidity.allocate_kodiak_island_position(
            underlying,
            island,
            farms,
            contract_addresses={self.FARM, self.UNKNOWN_VAULT, self.ISLAND},
        )

        self.assertEqual(len(rows), 4)
        self.assertNotIn(underlying["id"], [row["id"] for row in rows])
        self.assertEqual(sum(int(row["amount0Raw"]) for row in rows), 1001)
        self.assertEqual(sum(int(row["amount1Raw"]) for row in rows), 2003)
        self.assertEqual(sum(int(row["shareBalanceRaw"]) for row in rows), 100)
        by_owner = {row["beneficialOwner"]: row for row in rows}
        self.assertEqual(by_owner[self.ALICE]["attributionPath"], "kodiak_island")
        self.assertEqual(by_owner[self.BOB]["attributionPath"], "kodiak_island_farm")
        self.assertEqual(by_owner[self.BOB]["custodian"], self.FARM)
        self.assertEqual(by_owner[self.CAROL]["shareBalanceRaw"], "20")
        unresolved = by_owner[None]
        self.assertEqual(unresolved["custodian"], self.UNKNOWN_VAULT)
        self.assertEqual(unresolved["positionStatus"], "custodied_unresolved")
        self.assertEqual(unresolved["quality"], "unavailable")

    def test_unsupported_or_mismatched_farm_stays_unresolved(self):
        underlying = {
            "id": "position-1",
            "sourceKey": "berachain:kodiak-v3",
            "poolId": self.POOL,
            "poolIdentifierType": "contract",
            "chainKey": "berachain",
            "adapter": "kodiak-v3",
            "pair": "DOLO/WBERA",
            "positionId": "77",
            "amount0Raw": "1000",
            "amount1Raw": "2000",
            "doloRaw": "1000",
            "pairedRaw": "2000",
            "rangeStatus": "in_range",
            "valueUsd": None,
        }
        island = {
            "address": self.ISLAND,
            "totalShares": 100,
            "balances": {self.ALICE: 50, self.FARM: 50},
        }
        farms = [
            {
                "address": self.FARM,
                "stakingToken": "0x" + "99" * 20,
                "custodyBalance": 50,
                "stakedBalances": {self.BOB: 50},
            }
        ]

        rows = liquidity.allocate_kodiak_island_position(
            underlying,
            island,
            farms,
            contract_addresses={self.FARM, self.ISLAND},
        )

        self.assertEqual(len(rows), 2)
        self.assertNotIn(self.BOB, [row["beneficialOwner"] for row in rows])
        unresolved = next(row for row in rows if row["beneficialOwner"] is None)
        self.assertEqual(unresolved["custodian"], self.FARM)
        self.assertIn("staking token mismatch", unresolved["attributionReason"])

    def test_island_history_ignores_rebalances_and_staking_custody_moves(self):
        base = {
            "chainKey": "berachain",
            "adapter": "kodiak-v3",
            "identifier": self.POOL,
            "identifierType": "contract",
            "pair": "DOLO/WBERA",
            "token0": DOLO,
            "token1": self.WBERA,
        }
        actions = [
            {
                "kind": "deposit",
                "owner": self.ALICE,
                "shareDeltaRaw": 30,
                "amount0Raw": 100,
                "amount1Raw": 200,
                "blockNumber": 400,
                "timestamp": 1_700_300_400,
                "txHash": "0x" + "41" * 32,
                "logIndex": 1,
            },
            {"kind": "rebalance", "owner": self.MANAGER},
            {"kind": "stake", "owner": self.ALICE, "custodian": self.FARM},
            {"kind": "unstake", "owner": self.ALICE, "custodian": self.FARM},
            {"kind": "share_transfer", "owner": self.ALICE, "to": self.BOB},
            {
                "kind": "withdraw",
                "owner": self.ALICE,
                "shareDeltaRaw": 10,
                "amount0Raw": 30,
                "amount1Raw": 60,
                "blockNumber": 401,
                "timestamp": 1_700_300_401,
                "txHash": "0x" + "42" * 32,
                "logIndex": 1,
            },
            {
                "kind": "withdraw",
                "owner": self.ALICE,
                "shareDeltaRaw": 20,
                "amount0Raw": 70,
                "amount1Raw": 140,
                "blockNumber": 402,
                "timestamp": 1_700_300_402,
                "txHash": "0x" + "43" * 32,
                "logIndex": 1,
            },
        ]

        rows = liquidity.build_kodiak_island_history(base, self.ISLAND, actions)

        self.assertEqual([row["action"] for row in rows], ["Added", "Removed", "Closed"])
        self.assertEqual([row["doloRaw"] for row in rows], ["100", "30", "70"])
        self.assertTrue(all(row["attributionPath"] == "kodiak_island" for row in rows))
        self.assertNotIn("rebalance", [row.get("kind") for row in rows])


class ArtifactAssemblyTests(unittest.TestCase):
    def test_dexscreener_enrichment_derives_quote_price_without_owning_attribution(self):
        pool = {
            "chainKey": "ethereum",
            "adapter": "uniswap-v3",
            "identifier": "0x003896387666c5c11458eeb3f927b72a11b19783",
            "identifierType": "contract",
            "pair": "DOLO/USDC",
        }
        pair = {
            "chainId": "ethereum",
            "pairAddress": pool["identifier"],
            "baseToken": {"address": DOLO, "symbol": "DOLO"},
            "quoteToken": {"address": "0x" + "11" * 20, "symbol": "USDC"},
            "priceUsd": "0.05",
            "priceNative": "0.04",
            "liquidity": {"usd": 170000},
            "volume": {"h24": 25000},
            "url": "https://dexscreener.com/ethereum/example",
        }

        metadata = liquidity.derive_dexscreener_pool_metadata(pool, pair, Decimal("0.05"))

        self.assertEqual(metadata["doloPriceUsd"], 0.05)
        self.assertEqual(metadata["pairedPriceUsd"], 1.25)
        self.assertEqual(metadata["liquidityUsd"], 170000.0)
        self.assertEqual(metadata["volume24hUsd"], 25000.0)
        self.assertNotIn("owner", metadata)
        self.assertNotIn("beneficialOwner", metadata)

    def test_position_valuation_uses_raw_decimals_and_keeps_missing_price_null(self):
        valued = liquidity.value_position_row(
            {
                "doloRaw": str(10 * 10**18),
                "pairedRaw": str(2 * 10**6),
            },
            dolo_decimals=18,
            paired_decimals=6,
            dolo_price_usd=Decimal("0.05"),
            paired_price_usd=Decimal("1"),
        )
        missing = liquidity.value_position_row(
            {"doloRaw": str(10**18), "pairedRaw": str(10**6)},
            dolo_decimals=18,
            paired_decimals=6,
            dolo_price_usd=Decimal("0.05"),
            paired_price_usd=None,
        )

        self.assertEqual(valued["valueUsd"], 2.5)
        self.assertEqual(valued["valueStatus"], "verified")
        self.assertIsNone(missing["valueUsd"])
        self.assertEqual(missing["valueStatus"], "unavailable")

    def test_assemble_artifact_sorts_rows_and_reconciles_summary(self):
        registry = liquidity.load_registry(REGISTRY)
        generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        pools = [
            {
                "id": row["identifier"],
                "sourceKey": f"{row['chainKey']}:{row['adapter']}",
                **row,
                "liquidityUsd": None,
                "liquidityStatus": "unavailable",
            }
            for row in registry["pools"]
        ]
        active = [
            {
                "id": "row-b",
                "sourceKey": "berachain:bulla-v3",
                "poolId": pools[-2]["identifier"],
                "poolIdentifierType": "contract",
                "beneficialOwner": "0x" + "bb" * 20,
                "custodian": "0x" + "bb" * 20,
                "quality": "verified",
                "rangeStatus": "out_of_range",
                "valueUsd": None,
                "valueStatus": "unavailable",
                "doloRaw": "10",
                "pairedRaw": "20",
            },
            {
                "id": "row-a",
                "sourceKey": "ethereum:uniswap-v3",
                "poolId": pools[1]["identifier"],
                "poolIdentifierType": "contract",
                "beneficialOwner": "0x" + "aa" * 20,
                "custodian": "0x" + "aa" * 20,
                "quality": "verified",
                "rangeStatus": "in_range",
                "valueUsd": 50.25,
                "valueStatus": "verified",
                "doloRaw": "10",
                "pairedRaw": "20",
            },
        ]
        history = [
            {
                "id": "ethereum:0x" + "12" * 32 + ":2",
                "sourceKey": "ethereum:uniswap-v3",
                "poolId": pools[1]["identifier"],
                "poolIdentifierType": "contract",
                "blockNumber": 10,
                "logIndex": 2,
                "timestamp": 1000,
                "action": "Added",
                "quality": "verified",
                "doloRaw": "10",
                "pairedRaw": "20",
                "valueUsd": 1.0,
                "valueStatus": "verified",
            }
        ]
        sources = [
            {
                "key": "ethereum:uniswap-v3",
                "chainKey": "ethereum",
                "adapter": "uniswap-v3",
                "status": "complete",
                "lastScannedBlock": 10,
                "latestChainBlock": 10,
                "errors": [],
            },
            {
                "key": "berachain:bulla-v3",
                "chainKey": "berachain",
                "adapter": "bulla-v3",
                "status": "complete",
                "lastScannedBlock": 20,
                "latestChainBlock": 20,
                "errors": [],
            },
        ]

        artifact = liquidity.assemble_artifact(
            registry,
            sources,
            pools,
            active,
            history,
            generated_at,
        )

        self.assertEqual([row["id"] for row in artifact["activePositions"]], ["row-a", "row-b"])
        self.assertEqual(artifact["summary"]["activeLiquidityUsd"], 50.25)
        self.assertEqual(artifact["summary"]["lpWallets"], 2)
        self.assertEqual(artifact["summary"]["activePositions"], 2)
        self.assertEqual(artifact["summary"]["outOfRange"], 1)
        self.assertEqual(artifact["quality"]["verifiedActivePositions"], 2)
        self.assertEqual(artifact["quality"]["unresolvedCustody"], 0)

    def test_atomic_writer_rejects_artifact_at_two_megabytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "artifact.json"
            with self.assertRaisesRegex(ValueError, "2,000,000"):
                liquidity.write_artifact_atomic(
                    output,
                    {"payload": "x" * 2_000_000},
                    max_bytes=2_000_000,
                )
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
