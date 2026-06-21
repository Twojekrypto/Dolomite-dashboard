import os
import sys
import json
import tempfile
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


class TestBoundedHolderRefresh(unittest.TestCase):
    def test_recent_flow_fallbacks_sum_new_lock_rows(self):
        now_ts = 1782040000
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "vedolo_flows.json")
            with open(path, "w") as f:
                json.dump({
                    "locks": [
                        {"tokenId": 24556, "timestamp": now_ts - 60, "dolo": 10.5, "locktime": 1844640000},
                        {"tokenId": "24556", "timestamp": now_ts - 30, "dolo": 25.0, "locktime": 1844650000},
                        {"tokenId": 1, "timestamp": now_ts - 999999, "dolo": 99, "locktime": 1844640000},
                    ]
                }, f)

            fallback = update_data.load_recent_flow_lock_fallbacks(
                path=path,
                now_ts=now_ts,
                lookback_seconds=3600,
            )

        self.assertEqual(set(fallback), {24556})
        self.assertEqual(fallback[24556]["amount"], 35.5)
        self.assertEqual(fallback[24556]["end"], 1844650000)
        self.assertEqual(fallback[24556]["source"], "vedolo_flows_recent_lock")

    def test_locked_refresh_prioritizes_missing_and_defers_bulk_stale(self):
        originals = {
            "CACHE_FILE": update_data.CACHE_FILE,
            "LOCKED_STALE_REFRESH_LIMIT": update_data.LOCKED_STALE_REFRESH_LIMIT,
            "LOCKED_FAILED_RETRY_LIMIT": update_data.LOCKED_FAILED_RETRY_LIMIT,
            "LOCKED_ZERO_RETRY_LIMIT": update_data.LOCKED_ZERO_RETRY_LIMIT,
            "make_batch_call": update_data.make_batch_call,
            "fetch_contract_dolo_balance": update_data.fetch_contract_dolo_balance,
        }
        seen_batches = []

        def fake_batch(token_ids):
            seen_batches.append(list(token_ids))
            out = {}
            failed = []
            for tid in token_ids:
                if tid == 7:
                    failed.append(tid)
                else:
                    out[tid] = {"amount": tid * 10, "end": 1800000000 + tid}
            return out, failed

        with tempfile.TemporaryDirectory() as tmp:
            try:
                update_data.CACHE_FILE = os.path.join(tmp, "locked_cache.json")
                update_data.LOCKED_STALE_REFRESH_LIMIT = 2
                update_data.LOCKED_FAILED_RETRY_LIMIT = 0
                update_data.LOCKED_ZERO_RETRY_LIMIT = 0
                update_data.make_batch_call = fake_batch
                update_data.fetch_contract_dolo_balance = lambda: 0
                stale_cache = {
                    str(tid): {"amount": tid, "end": 1700000000 + tid, "fetched_at": 1}
                    for tid in range(1, 6)
                }
                update_data.save_cache(stale_cache)

                cache = update_data.fetch_locked_dolo(
                    [1, 2, 3, 4, 5, 6, 7],
                    priority_token_ids=[5, 7],
                    fallback_locks={
                        7: {
                            "amount": 35.5,
                            "end": 1844650000,
                            "fetched_at": 1782040000,
                            "source": "vedolo_flows_recent_lock",
                        }
                    },
                )
            finally:
                for name, value in originals.items():
                    setattr(update_data, name, value)

        fetched_ids = {tid for batch in seen_batches for tid in batch}
        self.assertIn(6, fetched_ids)  # missing token
        self.assertIn(7, fetched_ids)  # missing priority token
        self.assertIn(5, fetched_ids)  # recent-flow priority stale token
        self.assertEqual(len(fetched_ids.intersection({1, 2, 3, 4})), 2)
        self.assertEqual(cache["7"]["amount"], 35.5)
        self.assertEqual(cache["7"]["source"], "vedolo_flows_recent_lock")


if __name__ == "__main__":
    unittest.main()
