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

    def test_verified_lp_activity_index_finds_deposit_before_flow_ranking(self):
        block_number = 123
        tx_hash = "0x" + "b" * 64
        amount_wei = 1_304_943_547531365190891539
        transfer_log = {
            "address": self.DOLO,
            "from": self.WALLET,
            "to": self.V4_POOL_MANAGER,
            "transactionHash": tx_hash,
            "logIndex": "0x1",
            "topics": [
                flow_tx_metadata.TRANSFER_TOPIC,
                self._topic_address(self.WALLET),
                self._topic_address(self.V4_POOL_MANAGER),
            ],
            "data": hex(amount_wei),
        }
        receipt = {
            "status": "0x1",
            "transactionHash": tx_hash,
            "logs": [
                transfer_log,
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
        transfers = [[
            self.WALLET,
            self.V4_POOL_MANAGER,
            amount_wei,
            block_number,
        ]]

        activities = flow_tx_metadata.collect_verified_lp_activities(
            transfers,
            "ethereum",
            self._registry(),
            self.DOLO,
            lambda blocks: {
                block_number: {
                    "timestamp": 1_787_000_091,
                    "logs": [transfer_log],
                }
            },
            lambda hashes: {tx_hash: receipt},
        )

        self.assertEqual(activities[self.WALLET]["deposit"], "1304943.547531365190891539")
        self.assertEqual(activities[self.WALLET]["withdrawal"], "0")
        self.assertEqual(activities[self.WALLET]["latest"]["tx_hash"], tx_hash)
        self.assertEqual(activities[self.WALLET]["latest"]["timestamp"], 1_787_000_091)

    def test_verified_lp_activity_index_only_fetches_near_flat_pass_through_wallets(self):
        other_wallet = "0x" + "6" * 40
        excluded_router = "0x" + "7" * 40
        requested_blocks = []

        activities = flow_tx_metadata.collect_verified_lp_activities(
            [
                [self.WALLET, self.V4_POOL_MANAGER, 1_304_943_550_000_000_000_000_000, 123],
                [other_wallet, self.V4_POOL_MANAGER, 50_000_000_000_000_000_000_000, 124],
                [excluded_router, self.V4_POOL_MANAGER, 75_000_000_000_000_000_000_000, 125],
            ],
            "ethereum",
            self._registry(),
            self.DOLO,
            lambda blocks: requested_blocks.extend(sorted(blocks)) or {},
            lambda hashes: {},
            market_flows={
                self.WALLET: 0.31,
                other_wallet: -40_000.0,
            },
        )

        self.assertEqual(activities, {})
        self.assertEqual(requested_blocks, [123])

    def test_embedded_transfer_hash_fetches_block_evidence_only_after_lp_receipt_verification(self):
        swap_wallet = "0x" + "6" * 40
        legacy_wallet = "0x" + "7" * 40
        lp_tx_hash = "0x" + "b" * 64
        swap_tx_hash = "0x" + "c" * 64
        lp_amount = 1_304_943_547531365190891539
        swap_amount = 50_000 * 10**18
        requested_blocks = []

        def transfer_log(wallet, tx_hash, amount):
            return {
                "address": self.DOLO,
                "topics": [
                    flow_tx_metadata.TRANSFER_TOPIC,
                    self._topic_address(wallet),
                    self._topic_address(self.V4_POOL_MANAGER),
                ],
                "data": hex(amount),
                "transactionHash": tx_hash,
                "logIndex": "0x1",
            }

        lp_log = transfer_log(self.WALLET, lp_tx_hash, lp_amount)
        swap_log = transfer_log(swap_wallet, swap_tx_hash, swap_amount)
        lp_receipt = {
            "status": "0x1",
            "transactionHash": lp_tx_hash,
            "logs": [
                lp_log,
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
        swap_receipt = {
            "status": "0x1",
            "transactionHash": swap_tx_hash,
            "logs": [swap_log],
        }

        def load_evidence(blocks):
            requested_blocks.extend(sorted(blocks))
            return {
                123: {"timestamp": 1_787_000_091, "logs": [lp_log]},
                124: {"timestamp": 1_787_000_092, "logs": [swap_log]},
            }

        activities = flow_tx_metadata.collect_verified_lp_activities(
            [
                [self.WALLET, self.V4_POOL_MANAGER, lp_amount, 123, lp_tx_hash, 1],
                [swap_wallet, self.V4_POOL_MANAGER, swap_amount, 124, swap_tx_hash, 1],
                [legacy_wallet, self.V4_POOL_MANAGER, 10_000 * 10**18, 125],
            ],
            "ethereum",
            self._registry(),
            self.DOLO,
            load_evidence,
            lambda hashes: {
                lp_tx_hash: lp_receipt,
                swap_tx_hash: swap_receipt,
            },
            market_flows={self.WALLET: 0.31, swap_wallet: 0.2, legacy_wallet: 0.1},
        )

        self.assertEqual(requested_blocks, [123])
        self.assertEqual(set(activities), {self.WALLET})
        self.assertEqual(activities[self.WALLET]["latest"]["timestamp"], 1_787_000_091)

    def test_ranked_wallet_filter_collects_full_period_lp_net(self):
        other_wallet = "0x" + "6" * 40
        deposit_hash = "0x" + "b" * 64
        withdrawal_hash = "0x" + "c" * 64
        other_hash = "0x" + "d" * 64
        deposit_amount = 3_000_000 * 10**18
        withdrawal_amount = 500_000 * 10**18
        requested_hashes = set()

        def receipt(tx_hash, wallet, amount, direction):
            from_addr, to_addr = (
                (wallet, self.V4_POOL_MANAGER)
                if direction == "deposit"
                else (self.V4_POOL_MANAGER, wallet)
            )
            transfer_log = {
                "address": self.DOLO,
                "topics": [
                    flow_tx_metadata.TRANSFER_TOPIC,
                    self._topic_address(from_addr),
                    self._topic_address(to_addr),
                ],
                "data": hex(amount),
                "transactionHash": tx_hash,
                "logIndex": "0x1",
            }
            liquidity_delta = 1 if direction == "deposit" else -1
            return {
                "status": "0x1",
                "transactionHash": tx_hash,
                "logs": [
                    transfer_log,
                    {
                        "address": self.V4_POOL_MANAGER,
                        "topics": [
                            flow_tx_metadata.V4_MODIFY_LIQUIDITY_TOPIC,
                            self.V4_POOL_ID,
                            self._topic_address(self.V4_POSITION_MANAGER),
                        ],
                        "data": "0x" + self._word(-100) + self._word(100) + self._word(liquidity_delta) + self._word(1),
                    },
                ],
            }

        receipts = {
            deposit_hash: receipt(deposit_hash, self.WALLET, deposit_amount, "deposit"),
            withdrawal_hash: receipt(withdrawal_hash, self.WALLET, withdrawal_amount, "withdrawal"),
            other_hash: receipt(other_hash, other_wallet, 1_000 * 10**18, "deposit"),
        }

        def load_receipts(hashes):
            requested_hashes.update(hashes)
            return {tx_hash: receipts[tx_hash] for tx_hash in hashes}

        activities = flow_tx_metadata.collect_verified_lp_activities(
            [
                [self.WALLET, self.V4_POOL_MANAGER, deposit_amount, 200, deposit_hash, 1],
                [self.V4_POOL_MANAGER, self.WALLET, withdrawal_amount, 190, withdrawal_hash, 1],
                [other_wallet, self.V4_POOL_MANAGER, 1_000 * 10**18, 210, other_hash, 1],
            ],
            "ethereum",
            self._registry(),
            self.DOLO,
            lambda blocks: {
                block: {"timestamp": 1_787_000_000 + block, "logs": []}
                for block in blocks
            },
            load_receipts,
            wallet_filter={self.WALLET},
        )

        self.assertEqual(requested_hashes, {deposit_hash, withdrawal_hash})
        self.assertEqual(activities[self.WALLET]["deposit"], "3000000")
        self.assertEqual(activities[self.WALLET]["withdrawal"], "500000")
        self.assertEqual(activities[self.WALLET]["latest"]["tx_hash"], deposit_hash)

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
