import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_reward_claim_events as rce


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


if __name__ == "__main__":
    unittest.main()
