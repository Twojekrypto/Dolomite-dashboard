import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import validate_data


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = json.loads(
    (ROOT / "data" / "dolo-liquidity-pools.json").read_text(encoding="utf-8")
)


def valid_payload():
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    pools = [
        {
            "id": row["identifier"].lower(),
            "sourceKey": f"{row['chainKey']}:{row['adapter']}",
            "chainKey": row["chainKey"],
            "adapter": row["adapter"],
            "identifier": row["identifier"].lower(),
            "identifierType": row["identifierType"],
            "pair": row["pair"],
            "primary": row["primary"],
            "priority": row["priority"],
            "liquidityUsd": None,
            "liquidityStatus": "unavailable",
            "quality": "unavailable",
        }
        for row in REGISTRY["pools"]
    ]
    owner = "0x" + "aa" * 20
    active = [
        {
            "id": "active-1",
            "sourceKey": "ethereum:uniswap-v3",
            "poolId": "0x003896387666c5c11458eeb3f927b72a11b19783",
            "poolIdentifierType": "contract",
            "beneficialOwner": owner,
            "custodian": owner,
            "attributionPath": "direct",
            "positionStatus": "active",
            "quality": "verified",
            "rangeStatus": "in_range",
            "doloRaw": "1000000000000000000",
            "pairedRaw": "1000000",
            "valueUsd": 1.05,
            "valueStatus": "verified",
        }
    ]
    history = [
        {
            "id": "ethereum:0x" + "12" * 32 + ":1",
            "sourceKey": "ethereum:uniswap-v3",
            "poolId": "0x003896387666c5c11458eeb3f927b72a11b19783",
            "poolIdentifierType": "contract",
            "blockNumber": 10,
            "logIndex": 1,
            "timestamp": 1000,
            "action": "Added",
            "beneficialOwner": owner,
            "custodian": owner,
            "quality": "verified",
            "doloRaw": "1000000000000000000",
            "pairedRaw": "1000000",
            "valueUsd": 1.05,
            "valueStatus": "verified",
        }
    ]
    return {
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "summary": {
            "activeLiquidityUsd": 1.05,
            "lpWallets": 1,
            "activePositions": 1,
            "outOfRange": 0,
        },
        "sources": [
            {
                "key": "ethereum:uniswap-v3",
                "chainKey": "ethereum",
                "adapter": "uniswap-v3",
                "status": "complete",
                "lastScannedBlock": 10,
                "latestChainBlock": 10,
                "errors": [],
            }
        ],
        "pools": pools,
        "activePositions": active,
        "history": history,
        "quality": {
            "verifiedActivePositions": 1,
            "partialActivePositions": 0,
            "staleActivePositions": 0,
            "unavailableActivePositions": 0,
            "unresolvedCustody": 0,
        },
    }


class DoloLiquidityValidatorTests(unittest.TestCase):
    def test_liquidity_freshness_accepts_seven_hours_and_rejects_nine(self):
        freshness = dict(validate_data.RULES["dolo-liquidity.json"]["checks"])[
            "generatedAt must be fresh"
        ]
        now = datetime.now(timezone.utc)

        self.assertTrue(freshness({"generatedAt": (now - timedelta(hours=7)).isoformat()}))
        self.assertFalse(freshness({"generatedAt": (now - timedelta(hours=9)).isoformat()}))

    def test_valid_payload_passes_strict_contract(self):
        self.assertTrue(validate_data._dolo_liquidity_valid(valid_payload()))

    def test_duplicate_rows_and_history_event_keys_fail(self):
        payload = valid_payload()
        payload["activePositions"].append(copy.deepcopy(payload["activePositions"][0]))
        payload["summary"]["activePositions"] = 2
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

        payload = valid_payload()
        payload["history"].append(copy.deepcopy(payload["history"][0]))
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

    def test_missing_acceptance_pool_or_wrong_v4_identifier_type_fails(self):
        payload = valid_payload()
        payload["pools"] = payload["pools"][:-1]
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

        payload = valid_payload()
        v4 = next(row for row in payload["pools"] if row["adapter"] == "uniswap-v4")
        v4["identifierType"] = "contract"
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

    def test_negative_raw_or_fake_zero_unavailable_value_fails(self):
        payload = valid_payload()
        payload["activePositions"][0]["doloRaw"] = "-1"
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

        payload = valid_payload()
        payload["activePositions"][0]["valueStatus"] = "unavailable"
        payload["activePositions"][0]["valueUsd"] = 0
        payload["summary"]["activeLiquidityUsd"] = 0
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

    def test_summary_and_source_cursor_mismatch_fail(self):
        payload = valid_payload()
        payload["summary"]["lpWallets"] = 99
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

        payload = valid_payload()
        payload["sources"][0]["lastScannedBlock"] = 11
        payload["sources"][0]["latestChainBlock"] = 10
        self.assertFalse(validate_data._dolo_liquidity_valid(payload))

    def test_island_allocation_must_reconcile_and_cannot_keep_aggregate(self):
        payload = valid_payload()
        base = payload["activePositions"].pop()
        payload["activePositions"] = [
            {
                **base,
                "id": "share-a",
                "positionType": "kodiak_island_share",
                "allocationGroup": "island-1",
                "allocationTotalDoloRaw": "10",
                "allocationTotalPairedRaw": "20",
                "doloRaw": "4",
                "pairedRaw": "8",
            },
            {
                **base,
                "id": "share-b",
                "positionType": "kodiak_island_share",
                "allocationGroup": "island-1",
                "allocationTotalDoloRaw": "10",
                "allocationTotalPairedRaw": "20",
                "doloRaw": "6",
                "pairedRaw": "12",
            },
        ]
        payload["summary"]["activePositions"] = 2
        payload["summary"]["activeLiquidityUsd"] = 2.1
        payload["quality"]["verifiedActivePositions"] = 2
        self.assertTrue(validate_data._dolo_liquidity_valid(payload))

        broken = copy.deepcopy(payload)
        broken["activePositions"][1]["doloRaw"] = "5"
        self.assertFalse(validate_data._dolo_liquidity_valid(broken))

        duplicate_aggregate = copy.deepcopy(payload)
        duplicate_aggregate["activePositions"].append(
            {
                **base,
                "id": "aggregate",
                "allocationGroup": "island-1",
                "positionType": "concentrated_nft",
            }
        )
        duplicate_aggregate["summary"]["activePositions"] = 3
        duplicate_aggregate["summary"]["activeLiquidityUsd"] = 3.15
        duplicate_aggregate["quality"]["verifiedActivePositions"] = 3
        self.assertFalse(validate_data._dolo_liquidity_valid(duplicate_aggregate))


if __name__ == "__main__":
    unittest.main()
