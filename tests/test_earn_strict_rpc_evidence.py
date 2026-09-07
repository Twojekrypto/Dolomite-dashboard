import json
import tempfile
import unittest
from pathlib import Path

import earn_strict_rpc_evidence as evidence_module
from earn_strict_rpc_evidence import fetch_strict_evidence
from scan_earn_netflow import CHAINS as EARN_CHAIN_CONFIG


ADDRESS = "0x1111111111111111111111111111111111111111"
SECOND_ADDRESS = "0x3333333333333333333333333333333333333333"
INDEX_SCALE = 10**18
ARBITRUM_MARGIN = EARN_CHAIN_CONFIG["arbitrum"]["margin"].lower()
BLOCK_HASH = "0x" + "ab" * 32


def load_evidence_cache(*args, **kwargs):
    function = getattr(evidence_module, "load_evidence_cache", None)
    if function is None:
        raise AssertionError("load_evidence_cache must be exported")
    return function(*args, **kwargs)


def save_evidence_cache(*args, **kwargs):
    function = getattr(evidence_module, "save_evidence_cache", None)
    if function is None:
        raise AssertionError("save_evidence_cache must be exported")
    return function(*args, **kwargs)


def word(value):
    return f"{int(value):064x}"


def signed_words(value):
    value = int(value)
    return word(1 if value >= 0 else 0), word(abs(value))


def encode_current_index(borrow_index, supply_index):
    return "0x" + word(borrow_index) + word(supply_index) + word(123)


def encode_account_balances(market_id, par, wei):
    market_offset = 4 * 32
    token_offset = market_offset + 2 * 32
    par_offset = token_offset + 2 * 32
    wei_offset = par_offset + 3 * 32
    par_sign, par_value = signed_words(par)
    wei_sign, wei_value = signed_words(wei)
    return "0x" + "".join((
        word(market_offset),
        word(token_offset),
        word(par_offset),
        word(wei_offset),
        word(1),
        word(market_id),
        word(1),
        word(int("22" * 20, 16)),
        word(1),
        par_sign,
        par_value,
        word(1),
        wei_sign,
        wei_value,
    ))


def index_log(*, market=7, borrow=12 * INDEX_SCALE // 10, supply=12 * INDEX_SCALE // 10,
              transaction_index=0, log_index=1, block_hash=BLOCK_HASH,
              address=ARBITRUM_MARGIN, topic=None):
    return {
        "address": address,
        "topics": [
            topic or "0xf4626fd1187f91e6761ffb8a6ac3e8d9235a4a92da54e43feb0c57c4a4a322ab",
            "0x" + word(market),
        ],
        "data": "0x" + word(borrow) + word(supply),
        "blockNumber": "0x64",
        "blockHash": block_hash,
        "transactionIndex": hex(transaction_index),
        "logIndex": hex(log_index),
    }


def history_payload():
    return {
        "chain": "arbitrum",
        "address": ADDRESS,
        "lastScannedBlock": 130,
        "scanRange": {"fromBlock": 1, "toBlock": 130},
        "accounts": {
            "5": {
                "account": "5",
                "accountKnown": True,
                "markets": {
                    "7": {
                        "events": [{
                            "eventKey": "e1",
                            "blockNumber": 100,
                            "transactionIndex": 1,
                            "logIndex": 2,
                            "deltaWei": "-60",
                            "newPar": "-50",
                            "accountKnown": True,
                        }],
                    },
                },
            },
        },
    }


class FakeRpcClient:
    def __init__(
        self,
        logs,
        *,
        block_hashes=None,
        balance_weis=None,
        current_supply_indexes=None,
    ):
        self.logs = logs
        self.block_hashes = list(block_hashes or [BLOCK_HASH])
        self.balance_weis = list(balance_weis or [-60])
        self.current_supply_indexes = list(
            current_supply_indexes or [12 * INDEX_SCALE // 10]
        )
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_getBlockByNumber":
            block_hash = self.block_hashes.pop(0) if len(self.block_hashes) > 1 else self.block_hashes[0]
            return {"number": params[0], "hash": block_hash}
        if method == "eth_getLogs":
            if isinstance(self.logs, Exception):
                raise self.logs
            if isinstance(self.logs, dict):
                return list(self.logs.get(params[0].get("blockHash"), []))
            return list(self.logs)
        raise AssertionError(f"Unexpected RPC method: {method}")

    def eth_call_batch(self, calls, block="latest"):
        self.calls.append(("eth_call_batch", calls, block))
        results = []
        for _to, data in calls:
            if data.startswith("0x56ea84b2"):
                supply_index = (
                    self.current_supply_indexes.pop(0)
                    if len(self.current_supply_indexes) > 1
                    else self.current_supply_indexes[0]
                )
                results.append(encode_current_index(
                    supply_index,
                    supply_index,
                ))
            elif data.startswith("0x6a8194e7"):
                balance_wei = self.balance_weis.pop(0) if len(self.balance_weis) > 1 else self.balance_weis[0]
                results.append(encode_account_balances(7, -50, balance_wei))
            else:
                raise AssertionError(f"Unexpected calldata: {data}")
        return results


class EarnStrictRpcEvidenceTest(unittest.TestCase):
    def test_fetches_exact_event_index_and_pinned_signed_position(self):
        fake_client = FakeRpcClient([index_log()])

        evidence = fetch_strict_evidence(
            "arbitrum",
            ADDRESS,
            history_payload(),
            comparison_block=123,
            client=fake_client,
        )

        self.assertEqual(str(12 * INDEX_SCALE // 10), evidence["eventIndexes"]["e1"])
        self.assertEqual(str(12 * INDEX_SCALE // 10), evidence["eventIndexPairs"]["e1"]["supplyIndex"])
        self.assertEqual("-50", evidence["currentPositions"]["5|7"]["par"])
        self.assertEqual("-60", evidence["currentPositions"]["5|7"]["wei"])
        self.assertEqual("0x7b", next(
            call[2] for call in fake_client.calls if call[0] == "eth_call_batch"
        ))
        self.assertNotIn("rpcUrl", json.dumps(evidence))
        self.assertNotIn("endpoint", json.dumps(evidence).lower())

    def test_missing_index_log_before_action_is_explicit_and_never_ratio_derived(self):
        fake_client = FakeRpcClient([
            index_log(transaction_index=1, log_index=3),
        ])
        payload = history_payload()
        payload["accounts"]["5"]["markets"]["7"]["events"][0]["deltaWei"] = "-60"
        payload["accounts"]["5"]["markets"]["7"]["events"][0]["newPar"] = "-50"

        evidence = fetch_strict_evidence(
            "arbitrum",
            ADDRESS,
            payload,
            comparison_block=123,
            client=fake_client,
        )

        self.assertNotIn("e1", evidence["eventIndexes"])
        self.assertEqual("missing_event_index", evidence["errors"]["e1"])
        self.assertNotEqual(str(12 * INDEX_SCALE // 10), evidence["eventIndexes"].get("e1"))

    def test_reuses_successful_hash_pinned_log_query_but_refreshes_current_state(self):
        cache = {}
        first_diagnostics = {}
        second_diagnostics = {}
        fake_client = FakeRpcClient(
            [index_log()],
            block_hashes=[BLOCK_HASH, BLOCK_HASH],
            balance_weis=[-60, -61],
            current_supply_indexes=[12 * INDEX_SCALE // 10, 13 * INDEX_SCALE // 10],
        )

        first = fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=fake_client, evidence_cache=cache, diagnostics=first_diagnostics,
        )
        second = fetch_strict_evidence(
            "arbitrum", SECOND_ADDRESS, history_payload(), comparison_block=123,
            client=fake_client, evidence_cache=cache, diagnostics=second_diagnostics,
        )

        log_calls = [call for call in fake_client.calls if call[0] == "eth_getLogs"]
        block_calls = [call for call in fake_client.calls if call[0] == "eth_getBlockByNumber"]
        balance_calls = [call for call in fake_client.calls if call[0] == "eth_call_batch"]
        self.assertEqual(1, len(log_calls))
        self.assertEqual(2, len(block_calls))
        self.assertEqual({"blockHash", "address", "topics"}, set(log_calls[0][1][0]))
        self.assertEqual(2, len(balance_calls))
        self.assertEqual(["0x7b", "0x7b"], [call[2] for call in balance_calls])
        self.assertEqual("-60", first["currentPositions"]["5|7"]["wei"])
        self.assertEqual("-61", second["currentPositions"]["5|7"]["wei"])
        self.assertEqual(str(12 * INDEX_SCALE // 10), first["currentIndexes"]["7"]["supplyIndex"])
        self.assertEqual(str(13 * INDEX_SCALE // 10), second["currentIndexes"]["7"]["supplyIndex"])
        self.assertEqual({"hits": 0, "misses": 1, "skipped": 0}, first_diagnostics)
        self.assertEqual({"hits": 1, "misses": 0, "skipped": 0}, second_diagnostics)

    def test_changed_block_hash_refetches_and_cache_survives_disk_round_trip(self):
        next_hash = "0x" + "cd" * 32
        cache = {}
        first_client = FakeRpcClient([index_log()], block_hashes=[BLOCK_HASH])
        first = fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=first_client, evidence_cache=cache,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "strict-evidence-cache.json"
            save_evidence_cache(path, cache)
            restored = load_evidence_cache(path)
        second_client = FakeRpcClient(
            {next_hash: [index_log(block_hash=next_hash)]},
            block_hashes=[next_hash],
        )

        second = fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=second_client, evidence_cache=restored,
        )

        self.assertEqual(first["eventIndexes"], second["eventIndexes"])
        self.assertEqual(1, len([call for call in second_client.calls if call[0] == "eth_getLogs"]))

    def test_malformed_or_failed_log_evidence_is_never_cached(self):
        malformed_cases = [
            [index_log(block_hash="0x" + "ff" * 32)],
            [index_log(address="0x" + "33" * 20)],
            [index_log(topic="0x" + "44" * 32)],
            [{**index_log(), "transactionIndex": "-1"}],
            [{**index_log(), "logIndex": "-1"}],
            RuntimeError("provider failed"),
        ]
        for rows in malformed_cases:
            with self.subTest(rows=type(rows).__name__):
                cache = {}
                diagnostics = {}
                client = FakeRpcClient(rows)
                with self.assertRaises((RuntimeError, ValueError)):
                    fetch_strict_evidence(
                        "arbitrum", ADDRESS, history_payload(), comparison_block=123,
                        client=client, evidence_cache=cache, diagnostics=diagnostics,
                    )
                self.assertEqual({}, cache.get("entries", {}))

    def test_cache_identity_isolated_by_chain_and_market_topic_coverage(self):
        cache = {}
        client = FakeRpcClient([index_log()])
        fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=client, evidence_cache=cache,
        )
        market_eight = history_payload()
        market_eight["accounts"]["5"]["markets"] = {
            "8": {
                "events": [{
                    "eventKey": "e2", "blockNumber": 100,
                    "transactionIndex": 1, "logIndex": 2,
                    "deltaWei": "50", "newPar": "40", "accountKnown": True,
                }],
            }
        }
        second_client = FakeRpcClient([index_log(market=8)])

        fetch_strict_evidence(
            "arbitrum", ADDRESS, market_eight, comparison_block=123,
            client=second_client, evidence_cache=cache,
        )

        xlayer_margin = EARN_CHAIN_CONFIG["xlayer"]["margin"].lower()
        default_topic = "0x247e2f5b851dd23ef755d9ad527e801ee202c4097acd70c21e82dc5602cdd879"
        xlayer_client = FakeRpcClient([
            index_log(address=xlayer_margin, topic=default_topic),
        ])
        fetch_strict_evidence(
            "xlayer", ADDRESS, history_payload(), comparison_block=123,
            client=xlayer_client, evidence_cache=cache,
        )

        self.assertEqual(1, len([call for call in second_client.calls if call[0] == "eth_getLogs"]))
        self.assertEqual(1, len([call for call in xlayer_client.calls if call[0] == "eth_getLogs"]))
        self.assertEqual(3, len(cache["entries"]))

    def test_invalid_cached_log_provenance_is_skipped_and_refetched(self):
        cache = {}
        first_client = FakeRpcClient([index_log()])
        fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=first_client, evidence_cache=cache,
        )
        only_entry = next(iter(cache["entries"].values()))
        only_entry["logs"][0]["blockHash"] = "0x" + "ff" * 32
        diagnostics = {}
        second_client = FakeRpcClient([index_log()])

        evidence = fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=second_client, evidence_cache=cache, diagnostics=diagnostics,
        )

        self.assertIn("e1", evidence["eventIndexes"])
        self.assertEqual(1, len([call for call in second_client.calls if call[0] == "eth_getLogs"]))
        self.assertEqual({"hits": 0, "misses": 1, "skipped": 1}, diagnostics)

    def test_negative_cached_log_position_is_rejected_and_refetched(self):
        for field in ("transactionIndex", "logIndex"):
            with self.subTest(field=field):
                cache = {}
                fetch_strict_evidence(
                    "arbitrum", ADDRESS, history_payload(), comparison_block=123,
                    client=FakeRpcClient([index_log()]), evidence_cache=cache,
                )
                only_entry = next(iter(cache["entries"].values()))
                only_entry["logs"][0][field] = "-1"
                diagnostics = {}
                second_client = FakeRpcClient([index_log()])

                evidence = fetch_strict_evidence(
                    "arbitrum", ADDRESS, history_payload(), comparison_block=123,
                    client=second_client, evidence_cache=cache, diagnostics=diagnostics,
                )

                self.assertIn("e1", evidence["eventIndexes"])
                self.assertEqual(
                    1,
                    len([call for call in second_client.calls if call[0] == "eth_getLogs"]),
                )
                self.assertEqual({"hits": 0, "misses": 1, "skipped": 1}, diagnostics)

    def test_cache_prunes_oldest_successful_queries_to_configured_bound(self):
        cache = {}
        for market in (7, 8, 9):
            payload = history_payload()
            payload["accounts"]["5"]["markets"] = {
                str(market): {
                    "events": [{
                        "eventKey": f"e{market}", "blockNumber": 100,
                        "transactionIndex": 1, "logIndex": 2,
                        "deltaWei": "50", "newPar": "40", "accountKnown": True,
                    }],
                }
            }
            fetch_strict_evidence(
                "arbitrum", ADDRESS, payload, comparison_block=123,
                client=FakeRpcClient([index_log(market=market)]),
                evidence_cache=cache, max_cache_entries=2,
            )

        self.assertEqual(2, len(cache["entries"]))

    def test_loading_over_cap_cache_discards_old_malformed_entries_without_crashing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "strict-evidence-cache.json"
            path.write_text(json.dumps({
                "version": 1,
                "clock": 2,
                "entries": {"old": "malformed", "new": {"used": 2}},
            }), encoding="utf-8")

            restored = load_evidence_cache(path, max_entries=1)

        self.assertEqual({"new": {"used": 2}}, restored["entries"])


if __name__ == "__main__":
    unittest.main()
