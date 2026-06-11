import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import update_data

ZERO = "0x0000000000000000000000000000000000000000"
ALICE = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BOB = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def tx(token_id, from_addr, to_addr, block, index=0):
    return {
        "tokenID": str(token_id),
        "from": from_addr,
        "to": to_addr,
        "blockNumber": str(block),
        "transactionIndex": str(index),
    }


class TestBuildOwnership(unittest.TestCase):
    def test_mint_assigns_owner(self):
        holders, stats = update_data.build_ownership([tx(1, ZERO, ALICE, 100)])
        self.assertEqual(stats, {
            "total_minted": 1, "total_burned": 0,
            "active_nfts": 1, "unique_holders": 1,
        })
        self.assertEqual(holders, [{"address": ALICE, "nft_count": 1, "token_ids": [1]}])

    def test_transfer_moves_ownership(self):
        holders, stats = update_data.build_ownership([
            tx(1, ZERO, ALICE, 100),
            tx(1, ALICE, BOB, 200),
        ])
        self.assertEqual(stats["unique_holders"], 1)
        self.assertEqual(holders[0]["address"], BOB)

    def test_burn_excluded_from_holders(self):
        holders, stats = update_data.build_ownership([
            tx(1, ZERO, ALICE, 100),
            tx(2, ZERO, ALICE, 101),
            tx(1, ALICE, ZERO, 200),
        ])
        self.assertEqual(stats["total_minted"], 2)
        self.assertEqual(stats["total_burned"], 1)
        self.assertEqual(stats["active_nfts"], 1)
        self.assertEqual(holders, [{"address": ALICE, "nft_count": 1, "token_ids": [2]}])

    def test_out_of_order_input_sorted_by_block_and_index(self):
        # Transfer events arrive out of order; final owner must follow
        # (blockNumber, transactionIndex) ordering, not input order.
        holders, _ = update_data.build_ownership([
            tx(1, ALICE, BOB, 200, index=1),
            tx(1, ZERO, ALICE, 100),
            tx(1, BOB, ALICE, 200, index=0),  # same block, earlier index
        ])
        self.assertEqual(holders[0]["address"], BOB)

    def test_multiple_tokens_sorted_per_holder(self):
        holders, stats = update_data.build_ownership([
            tx(5, ZERO, ALICE, 100),
            tx(2, ZERO, ALICE, 101),
            tx(9, ZERO, BOB, 102),
        ])
        by_addr = {h["address"]: h for h in holders}
        self.assertEqual(by_addr[ALICE]["token_ids"], [2, 5])
        self.assertEqual(by_addr[ALICE]["nft_count"], 2)
        self.assertEqual(stats["unique_holders"], 2)

    def test_remint_like_sequence_keeps_latest_owner(self):
        holders, _ = update_data.build_ownership([
            tx(1, ZERO, ALICE, 100),
            tx(1, ALICE, BOB, 150),
            tx(1, BOB, ALICE, 300),
        ])
        self.assertEqual(holders[0]["address"], ALICE)


class TestRpcEndpoints(unittest.TestCase):
    def test_shared_endpoint_source(self):
        # update_data must use the shared endpoint list (berachain publics
        # always present; env-injected keys would be prepended in CI).
        self.assertIn("https://rpc.berachain.com/", update_data.RPC_URLS)
        self.assertGreaterEqual(len(update_data.RPC_URLS), 3)


if __name__ == "__main__":
    unittest.main()
