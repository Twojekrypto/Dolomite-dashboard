import unittest
from unittest.mock import patch

import flow_tx_metadata


class FlowTransactionMetadataTests(unittest.TestCase):
    DOLO = "0x" + "d" * 40
    WALLET = "0x" + "1" * 40
    V4_POOL_MANAGER = "0x" + "2" * 40
    V4_POSITION_MANAGER = "0x" + "3" * 40
    V3_POSITION_MANAGER = "0x" + "4" * 40
    V3_POOL = "0x" + "5" * 40
    V4_POOL_ID = "0x" + "a" * 64

    @staticmethod
    def _topic_address(address):
        return "0x" + "0" * 24 + address[2:].lower()

    @staticmethod
    def _word(value):
        if value < 0:
            value = (1 << 256) + value
        return f"{value:064x}"

    def _registry(self):
        return {
            "chains": {
                "ethereum": {
                    "adapters": {
                        "uniswap-v4": {
                            "poolManager": self.V4_POOL_MANAGER,
                            "positionManager": self.V4_POSITION_MANAGER,
                        },
                        "uniswap-v3": {
                            "positionManager": self.V3_POSITION_MANAGER,
                        },
                    },
                },
            },
            "pools": [
                {
                    "chainKey": "ethereum",
                    "adapter": "uniswap-v4",
                    "identifierType": "poolId",
                    "identifier": self.V4_POOL_ID,
                    "pair": "DOLO/USDC",
                },
                {
                    "chainKey": "ethereum",
                    "adapter": "uniswap-v3",
                    "identifierType": "contract",
                    "identifier": self.V3_POOL,
                    "pair": "DOLO/USDC",
                },
            ],
        }

    def test_v4_modify_liquidity_receipt_verifies_exact_wallet_lp_deposit(self):
        amount_wei = 1_304_943_547531365190891539
        receipt = {
            "status": "0x1",
            "transactionHash": "0x" + "b" * 64,
            "logs": [
                {
                    "address": self.DOLO,
                    "topics": [
                        flow_tx_metadata.TRANSFER_TOPIC,
                        self._topic_address(self.WALLET),
                        self._topic_address(self.V4_POOL_MANAGER),
                    ],
                    "data": hex(amount_wei),
                },
                {
                    "address": self.V4_POOL_MANAGER,
                    "topics": [
                        flow_tx_metadata.V4_MODIFY_LIQUIDITY_TOPIC,
                        self.V4_POOL_ID,
                        self._topic_address(self.V4_POSITION_MANAGER),
                    ],
                    "data": "0x" + self._word(-100) + self._word(100) + self._word(99) + self._word(374940),
                },
            ],
        }

        activity = flow_tx_metadata.classify_lp_receipt(
            receipt,
            self.WALLET,
            "ethereum",
            self._registry(),
            self.DOLO,
        )

        self.assertEqual(activity, {
            "direction": "deposit",
            "amount": "1304943.547531365190891539",
            "pair": "DOLO/USDC",
            "adapter": "uniswap-v4",
            "confidence": "verified_same_tx",
            "tx_hash": "0x" + "b" * 64,
        })

    def test_v4_swap_like_transfer_without_position_event_is_not_labeled_lp(self):
        receipt = {
            "status": "0x1",
            "transactionHash": "0x" + "c" * 64,
            "logs": [{
                "address": self.DOLO,
                "topics": [
                    flow_tx_metadata.TRANSFER_TOPIC,
                    self._topic_address(self.WALLET),
                    self._topic_address(self.V4_POOL_MANAGER),
                ],
                "data": hex(100 * 10**18),
            }],
        }

        self.assertIsNone(flow_tx_metadata.classify_lp_receipt(
            receipt,
            self.WALLET,
            "ethereum",
            self._registry(),
            self.DOLO,
        ))

    def test_v3_decrease_with_pool_transfer_verifies_lp_withdrawal(self):
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "e" * 64,
            "logs": [
                {
                    "address": self.DOLO,
                    "topics": [
                        flow_tx_metadata.TRANSFER_TOPIC,
                        self._topic_address(self.V3_POOL),
                        self._topic_address(self.WALLET),
                    ],
                    "data": hex(850_000 * 10**18),
                },
                {
                    "address": self.V3_POSITION_MANAGER,
                    "topics": [flow_tx_metadata.V3_DECREASE_LIQUIDITY_TOPIC],
                    "data": "0x",
                },
            ],
        }

        activity = flow_tx_metadata.classify_lp_receipt(
            receipt,
            self.WALLET,
            "ethereum",
            self._registry(),
            self.DOLO,
        )

        self.assertEqual(activity["direction"], "withdrawal")
        self.assertEqual(activity["amount"], "850000")
        self.assertEqual(activity["pair"], "DOLO/USDC")
        self.assertEqual(activity["adapter"], "uniswap-v3")

    def test_brownfi_pool_mint_receipt_verifies_exact_lp_deposit(self):
        pool = "0x" + "6" * 40
        registry = {
            "chains": {
                "berachain": {
                    "adapters": {
                        "brownfi-v3": {
                            "factory": "0x" + "7" * 40,
                        },
                    },
                },
            },
            "pools": [{
                "chainKey": "berachain",
                "adapter": "brownfi-v3",
                "identifierType": "contract",
                "identifier": pool,
                "pair": "DOLO/BUSD",
            }],
        }
        receipt = {
            "status": "0x1",
            "transactionHash": "0x" + "9" * 64,
            "logs": [
                {
                    "address": self.DOLO,
                    "topics": [
                        flow_tx_metadata.TRANSFER_TOPIC,
                        self._topic_address(self.WALLET),
                        self._topic_address(pool),
                    ],
                    "data": hex(12_500 * 10**18),
                },
                {
                    "address": pool,
                    "topics": [flow_tx_metadata.BROWNFI_MINT_TOPIC],
                    "data": "0x",
                },
            ],
        }

        activity = flow_tx_metadata.classify_lp_receipt(
            receipt,
            self.WALLET,
            "berachain",
            registry,
            self.DOLO,
        )

        self.assertEqual(activity["direction"], "deposit")
        self.assertEqual(activity["amount"], "12500")
        self.assertEqual(activity["pair"], "DOLO/BUSD")
        self.assertEqual(activity["adapter"], "brownfi-v3")

    def test_latest_lp_metadata_fails_closed_without_a_verified_receipt(self):
        tx_hash = "0x" + "f" * 64
        rows = [{"address": self.WALLET, "latest_tx_hash": tx_hash}]

        flow_tx_metadata.attach_latest_lp_metadata(
            rows,
            "ethereum",
            self._registry(),
            self.DOLO,
            lambda hashes: {tx_hash: {"status": "0x0", "logs": []}},
        )

        self.assertNotIn("latest_lp_activity", rows[0])

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
