import os
import sys
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import update_data
from vedolo_vote_power import CanonicalSnapshot

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

    def test_token_zero_transfers_are_ignored(self):
        holders, stats = update_data.build_ownership([
            tx(0, ZERO, update_data.VEDOLO_CONTRACT, 100),
            tx(1, ZERO, ALICE, 101),
        ])

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

    def test_nft_transfer_coverage_stats_record_count_and_block_range(self):
        stats = update_data.nft_transfer_coverage_stats([
            tx(1, ZERO, ALICE, 100),
            tx(2, ZERO, BOB, 250),
        ])

        self.assertEqual(stats, {
            "nft_transfer_count": 2,
            "nft_transfer_min_block": 100,
            "nft_transfer_max_block": 250,
        })

    def test_ownership_snapshot_guard_rejects_impossible_drops(self):
        previous = {
            "stats": {
                "total_minted": 10,
                "total_burned": 4,
                "nft_transfer_count": 20,
                "nft_transfer_max_block": 500,
            }
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(previous, f)
            path = f.name
        try:
            issues = update_data.ownership_snapshot_issues({
                "total_minted": 9,
                "total_burned": 4,
                "nft_transfer_count": 19,
                "nft_transfer_max_block": 499,
            }, path=path)
        finally:
            os.unlink(path)

        self.assertEqual(len(issues), 3)
        self.assertTrue(any("minted veDOLO positions decreased" in issue for issue in issues))
        self.assertTrue(any("NFT transfer events decreased" in issue for issue in issues))
        self.assertTrue(any("latest NFT transfer block decreased" in issue for issue in issues))


class TestNftTransferPagination(unittest.TestCase):
    def test_fetches_every_explorer_page_before_advancing_block_window(self):
        calls = []
        responses = [
            [
                {**tx(1, ZERO, ALICE, 1), "hash": "0x01"},
                {**tx(2, ZERO, ALICE, 2), "hash": "0x02"},
            ],
            [
                {**tx(3, ZERO, ALICE, 3), "hash": "0x03"},
                {**tx(4, ZERO, ALICE, 4), "hash": "0x04"},
            ],
            [
                {**tx(4, ZERO, ALICE, 4), "hash": "0x04"},
                {**tx(5, ZERO, ALICE, 5), "hash": "0x05"},
            ],
            [
                {**tx(6, ZERO, ALICE, 6), "hash": "0x06"},
            ],
        ]

        class Response:
            def __init__(self, result):
                self.result = result

            def json(self):
                return {"status": "1", "result": self.result}

        def fake_get(_url, *, params, timeout):
            calls.append((params["startblock"], params["page"], params["offset"], timeout))
            return Response(responses.pop(0))

        with (
            patch.object(update_data, "API_KEY", "test-key"),
            patch.object(update_data, "NFT_TRANSFER_PAGE_SIZE", 2),
            patch.object(update_data, "NFT_TRANSFER_MAX_PAGES", 2),
            patch.object(update_data.requests, "get", side_effect=fake_get),
            patch.object(update_data.time, "sleep"),
        ):
            transfers = update_data.fetch_all_nft_transfers()

        self.assertEqual([row["tokenID"] for row in transfers], ["1", "2", "3", "4", "5", "6"])
        self.assertEqual(calls, [
            (0, 1, 2, 30),
            (0, 2, 2, 30),
            (4, 1, 2, 30),
            (4, 2, 2, 30),
        ])

    def test_preserved_incomplete_snapshot_fails_workflow(self):
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "vedolo_holders.json")
            Path(output).write_text("{}", encoding="utf-8")
            with patch.object(update_data, "OUTPUT_JSON", output):
                with self.assertRaises(SystemExit) as raised:
                    update_data.preserve_existing_vedolo_snapshot("partial explorer result")

        self.assertEqual(raised.exception.code, 1)


class TestRpcEndpoints(unittest.TestCase):
    def test_shared_endpoint_source(self):
        # update_data must use the shared endpoint list (berachain publics
        # always present; env-injected keys would be prepended in CI).
        self.assertIn("https://rpc.berachain.com/", update_data.RPC_URLS)
        self.assertGreaterEqual(len(update_data.RPC_URLS), 3)

    def test_update_workflow_preserves_vedolo_history_deployment_contract(self):
        workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "update-data.yml").read_text()

        cache_position = workflow.index("path: vedolo_vote_power_history_state.json")
        update_position = workflow.index("run: python update_data.py")
        generator_position = workflow.index(
            "run: python3 generate_vedolo_vote_power_history.py --sync-stats"
        )
        validation_position = workflow.index(
            "validate_data.py vedolo_holders.json vedolo_stats.json vedolo_expiry.json "
            "data/vedolo-vote-power-history.json"
        )
        locked_history_validation_position = workflow.index(
            "validate_vedolo_locked_history.py --flows vedolo_flows.json "
            "--holders vedolo_holders.json"
        )
        git_add_position = workflow.index(
            "git add vedolo_holders.json vedolo_holders.csv vedolo_stats.json "
            "vedolo_expiry.json data/vedolo-vote-power-history.json"
        )

        self.assertLess(cache_position, update_position)
        self.assertLess(update_position, generator_position)
        self.assertLess(generator_position, validation_position)
        self.assertLess(validation_position, locked_history_validation_position)
        self.assertLess(locked_history_validation_position, git_add_position)
        self.assertLess(validation_position, git_add_position)

    def test_update_workflow_saves_vedolo_history_state_after_a_failed_generation(self):
        workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "update-data.yml").read_text()

        restore_position = workflow.index("uses: actions/cache/restore@v5")
        generator_position = workflow.index(
            "run: python3 generate_vedolo_vote_power_history.py --sync-stats"
        )
        save_position = workflow.index("- name: Save veDOLO vote power history state")
        save_block = workflow[save_position:save_position + 260]

        self.assertLess(restore_position, generator_position)
        self.assertLess(generator_position, save_position)
        self.assertIn("if: always()", save_block)
        self.assertIn("uses: actions/cache/save@v5", save_block)


class TestCanonicalVoteWeight(unittest.TestCase):
    def test_canonical_snapshot_replaces_only_aggregate_vote_weight(self):
        stats = {"total_vote_weight": 100.0}

        out = update_data.apply_canonical_vote_weight(
            stats,
            CanonicalSnapshot(44, 55, 123_450_000_000_000_000_000, 0, 3),
        )

        self.assertEqual(out["total_vote_weight"], 123.45)
        self.assertEqual(out["total_vote_weight_holder_sum"], 100.0)
        self.assertEqual(out["total_vote_weight_source"], "contract_totalSupply")
        self.assertEqual(out["total_vote_weight_block"], 44)
        self.assertEqual(out["total_vote_weight_timestamp"], 55)

    def test_update_aborts_before_writing_when_canonical_snapshot_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            original_output = update_data.OUTPUT_JSON
            temporary_output = os.path.join(directory, "vedolo_holders.json")
            update_data.OUTPUT_JSON = temporary_output
            try:
                with patch.object(update_data, "fetch_all_nft_transfers", return_value=[tx(1, ZERO, ALICE, 100)]), \
                     patch.object(update_data, "ownership_snapshot_issues", return_value=[]), \
                     patch.object(update_data, "load_recent_flow_lock_fallbacks", return_value={}), \
                     patch.object(update_data, "fetch_locked_dolo", return_value={}), \
                     patch.object(update_data, "fetch_vote_weights", return_value={1: 100.0}), \
                     patch.object(update_data, "fetch_canonical_snapshot", side_effect=RuntimeError("pinned read failed")):
                    with self.assertRaisesRegex(RuntimeError, "pinned read failed"):
                        update_data.main()
            finally:
                update_data.OUTPUT_JSON = original_output

            self.assertFalse(os.path.exists(temporary_output))


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


class TestVedoloMulticall(unittest.TestCase):
    def test_make_batch_call_uses_multicall_fast_path(self):
        # locked() returns (int128 amount, uint end). Positive + a negative
        # int128 (sign handling must mirror the existing hex decode).
        pos = (100 * 10**18).to_bytes(32, "big") + (1700000000).to_bytes(32, "big")
        neg = (2**128 - 50 * 10**18).to_bytes(32, "big") + (0).to_bytes(32, "big")

        with patch.object(update_data, "_multicall_vedolo_reads", return_value={7: pos, 9: neg}), \
             patch.object(update_data.requests, "Session") as session:
            out, failed = update_data.make_batch_call([7, 9])

        session.assert_not_called()  # Multicall3 resolved everyone; no per-call batch
        self.assertEqual(failed, [])
        self.assertEqual(out[7], {"amount": 100.0, "end": 1700000000})
        self.assertEqual(out[9]["amount"], -50.0)

    def test_make_vote_batch_call_uses_multicall_fast_path(self):
        with patch.object(update_data, "_multicall_vedolo_reads",
                          return_value={7: (42 * 10**18).to_bytes(32, "big")}), \
             patch.object(update_data.requests, "Session") as session:
            out, failed = update_data.make_vote_batch_call([7])

        session.assert_not_called()
        self.assertEqual(failed, [])
        self.assertEqual(out[7], 42.0)

    def test_multicall_vedolo_reads_without_web3_returns_empty(self):
        with patch.dict(sys.modules, {"web3": None}):
            result = update_data._multicall_vedolo_reads([1, 2], update_data.LOCKED_SELECTOR)
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
