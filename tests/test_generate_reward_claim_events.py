import os
import sys
import json
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

    def test_reward_claim_scanner_reads_all_dedicated_arbitrum_rpc_secrets(self):
        source = (ROOT / "generate_reward_claim_events.py").read_text(encoding="utf-8")

        for env_name in (
            "ALCHEMY_ARBITRUM_RPC_KAT",
            "ALCHEMY_ARBITRUM_RPC_DAN",
            "ALCHEMY_ARBITRUM_RPC_ZEN",
        ):
            self.assertIn(env_name, source)

    def test_reward_claim_scanner_reads_xlayer_zen_rpc_secret(self):
        source = (ROOT / "generate_reward_claim_events.py").read_text(encoding="utf-8")
        workflow = (ROOT / ".github" / "workflows" / "update-reward-claim-events.yml").read_text(encoding="utf-8")

        self.assertIn("ALCHEMY_XLAYER_RPC_ZEN", source)
        self.assertIn("ALCHEMY_XLAYER_RPC_ZEN: ${{ secrets.ALCHEMY_XLAYER_RPC_ZEN }}", workflow)
        with patch.dict(os.environ, {"ALCHEMY_XLAYER_RPC_ZEN": "https://xlayer.example"}, clear=True):
            self.assertTrue(rce.has_configured_rpc("xlayer"))

    def test_odolo_flow_workflow_refreshes_berachain_claims_before_flows(self):
        workflow = (ROOT / ".github" / "workflows" / "update-odolo-flows.yml").read_text(encoding="utf-8")

        self.assertIn("REWARD_CLAIM_CHAINS: berachain", workflow)
        self.assertLess(
            workflow.index("Generate Berachain reward claim events"),
            workflow.index("Generate oDOLO flows"),
        )
        self.assertNotIn("ALCHEMY_XLAYER_RPC_ZEN", workflow)

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
