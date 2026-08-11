import json
import tempfile
import unittest
from decimal import Decimal, getcontext
from pathlib import Path

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
        "bulla-v2",
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


if __name__ == "__main__":
    unittest.main()
