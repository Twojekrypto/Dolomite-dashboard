import unittest
from unittest.mock import patch

import flow_tx_metadata


class FlowTransactionMetadataTests(unittest.TestCase):
    def test_missing_batch_half_is_retried_individually(self):
        wallet = "0x" + "1" * 40
        peer = "0x" + "2" * 40
        tx_hash = "0x" + "a" * 64
        block = 123
        log = {
            "from": peer,
            "to": wallet,
            "transactionHash": tx_hash,
            "logIndex": "0x0",
        }

        def fake_batch(_endpoints, _payloads, **_kwargs):
            return {
                f"logs:{block}": {"jsonrpc": "2.0", "id": f"logs:{block}", "result": [log]},
            }, [f"block:{block}"]

        def fake_single(_endpoints, payload, **_kwargs):
            self.assertEqual(payload["method"], "eth_getBlockByNumber")
            return {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {"timestamp": hex(1_786_406_400)},
            }

        with patch.object(flow_tx_metadata, "rpc_single_request", fake_single, create=True):
            result = flow_tx_metadata.fetch_token_block_evidence(
                ["https://rpc.example"],
                "0x" + "3" * 40,
                {block},
                fake_batch,
                retries_per_endpoint=1,
            )

        self.assertEqual(result, {block: {"timestamp": 1_786_406_400, "logs": [log]}})


if __name__ == "__main__":
    unittest.main()
