import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import earn_strict_rpc_evidence as evidence_module
from earn_strict_rpc_evidence import fetch_strict_evidence
from earn_strict_replay import build_strict_replay
from rpc_client import RpcClient, RpcError
from requests.exceptions import HTTPError
from scan_earn_netflow import CHAINS as EARN_CHAIN_CONFIG


ADDRESS = "0x1111111111111111111111111111111111111111"
SECOND_ADDRESS = "0x3333333333333333333333333333333333333333"
INDEX_SCALE = 10**18
ARBITRUM_MARGIN = EARN_CHAIN_CONFIG["arbitrum"]["margin"].lower()
BLOCK_HASH = "0x" + "ab" * 32
ENDPOINTS = ["https://provider-a.invalid", "https://provider-b.invalid"]


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


def successful_rpc_response(payload):
    if isinstance(payload, list):
        return [successful_rpc_response(entry) for entry in payload]
    if payload["method"] == "eth_getBlockByNumber":
        result = {"number": payload["params"][0], "hash": BLOCK_HASH}
    elif payload["method"] == "eth_getLogs":
        result = [index_log()]
    elif payload["method"] == "eth_call":
        data = payload["params"][0]["data"]
        result = (encode_current_index(12 * INDEX_SCALE // 10, 12 * INDEX_SCALE // 10)
                  if data.startswith("0x56ea84b2") else encode_account_balances(7, -50, -60))
    else:
        raise AssertionError(f"Unexpected RPC method: {payload['method']}")
    return {"jsonrpc": "2.0", "id": payload["id"], "result": result}


def rejected_rpc_response(payload):
    return {"jsonrpc": "2.0", "id": 1 if isinstance(payload, list) else payload["id"],
            "error": {"code": -32602, "message": "valid pinned query rejected by provider"}}


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
    def _fetch_with_transport(self, transport):
        client = RpcClient(endpoints=ENDPOINTS, timeout=4, retries_per_endpoint=3, quiet=True)
        with patch.object(RpcClient, "_post", autospec=True, side_effect=transport), patch("rpc_client.time.sleep"):
            return fetch_strict_evidence(
                "arbitrum", ADDRESS, history_payload(), comparison_block=123, client=client,
            )

    def test_pinned_reads_retry_json_rpc_rejection_on_a_distinct_provider(self):
        for rejected_method in ("eth_getBlockByNumber", "eth_getLogs"):
            with self.subTest(rejected_method=rejected_method):
                attempts = []

                def transport(client, endpoint, payload):
                    if not isinstance(payload, list) and payload["method"] == rejected_method:
                        attempts.append((endpoint, payload["params"]))
                        if endpoint == ENDPOINTS[0]:
                            return rejected_rpc_response(payload)
                    return successful_rpc_response(payload)

                evidence = self._fetch_with_transport(transport)
                self.assertEqual([ENDPOINTS[0], ENDPOINTS[1]], [row[0] for row in attempts])
                self.assertEqual(attempts[0][1], attempts[1][1])
                if rejected_method == "eth_getBlockByNumber":
                    self.assertEqual(["0x64", False], attempts[1][1])
                else:
                    self.assertEqual(BLOCK_HASH, attempts[1][1][0]["blockHash"])
                    self.assertNotIn("fromBlock", attempts[1][1][0])
                self.assertEqual("-60", evidence["currentPositions"]["5|7"]["wei"])

    def test_rejected_snapshot_batch_recovers_with_single_calls_at_the_same_block(self):
        batches = []
        singles = []

        def transport(client, endpoint, payload):
            if isinstance(payload, list):
                batches.append(payload)
                return rejected_rpc_response(payload)
            if payload["method"] == "eth_call":
                singles.append(payload)
            return successful_rpc_response(payload)

        evidence = self._fetch_with_transport(transport)
        self.assertEqual(1, len(batches))
        self.assertEqual(2, len(singles))
        self.assertEqual(["0x7b", "0x7b"], [row["params"][1] for row in singles])
        self.assertEqual([row["params"] for row in batches[0]], [row["params"] for row in singles])
        self.assertEqual("-50", evidence["currentPositions"]["5|7"]["par"])
        self.assertEqual({}, evidence["errors"])

    def test_successful_pinned_provider_is_reused_for_later_event_reads(self):
        history = history_payload()
        events = history["accounts"]["5"]["markets"]["7"]["events"]
        events.append({**events[0], "eventKey": "e2", "blockNumber": 101, "deltaWei": "0"})
        next_hash = "0x" + "cd" * 32
        log_attempts = []
        block_attempts = []

        def transport(client, endpoint, payload):
            if not isinstance(payload, list) and payload["method"] == "eth_getBlockByNumber":
                block_attempts.append(endpoint)
                result = successful_rpc_response(payload)
                if payload["params"][0] == "0x65":
                    result["result"]["hash"] = next_hash
                return result
            if not isinstance(payload, list) and payload["method"] == "eth_getLogs":
                log_attempts.append(endpoint)
                if endpoint == ENDPOINTS[0]:
                    return rejected_rpc_response(payload)
                result = successful_rpc_response(payload)
                if payload["params"][0]["blockHash"] == next_hash:
                    result["result"][0].update({"blockHash": next_hash, "blockNumber": "0x65"})
                return result
            return successful_rpc_response(payload)

        client = RpcClient(endpoints=ENDPOINTS, timeout=4, retries_per_endpoint=3, quiet=True)
        with patch.object(RpcClient, "_post", autospec=True, side_effect=transport), patch("rpc_client.time.sleep"):
            evidence = fetch_strict_evidence(
                "arbitrum", ADDRESS, history, comparison_block=123, client=client,
            )
        self.assertEqual([ENDPOINTS[0], ENDPOINTS[1], ENDPOINTS[1]], log_attempts)
        self.assertEqual(ENDPOINTS, block_attempts)
        self.assertEqual({"e1", "e2"}, set(evidence["eventIndexes"]))
        self.assertEqual(ENDPOINTS[1], client.last_endpoint)

    def test_empty_index_log_result_retries_distinct_provider_without_changing_query(self):
        attempts = []

        def transport(client, endpoint, payload):
            result = successful_rpc_response(payload)
            if not isinstance(payload, list) and payload["method"] == "eth_getLogs":
                attempts.append((endpoint, payload["params"]))
                if endpoint == ENDPOINTS[0]:
                    result["result"] = []
            return result

        evidence = self._fetch_with_transport(transport)
        self.assertEqual(ENDPOINTS, [row[0] for row in attempts])
        self.assertEqual(attempts[0][1], attempts[1][1])
        self.assertEqual(BLOCK_HASH, attempts[1][1][0]["blockHash"])
        self.assertEqual(str(12 * INDEX_SCALE // 10), evidence["eventIndexes"]["e1"])
        self.assertEqual({}, evidence["errors"])

    def test_capacity_exhausted_provider_stays_disabled_across_later_pinned_reads(self):
        attempts = []

        def transport(client, endpoint, payload):
            result = successful_rpc_response(payload)
            if not isinstance(payload, list) and payload["method"] == "eth_getLogs":
                attempts.append(endpoint)
                if endpoint == ENDPOINTS[0]:
                    return {"jsonrpc": "2.0", "id": payload["id"],
                            "error": {"code": -32005, "message": "monthly capacity exceeded"}}
                if attempts.count(ENDPOINTS[1]) > 1:
                    result["result"] = []
            return result

        client = RpcClient(endpoints=ENDPOINTS, timeout=4, retries_per_endpoint=3, quiet=True)
        with patch.object(RpcClient, "_post", autospec=True, side_effect=transport), patch("rpc_client.time.sleep"):
            first = fetch_strict_evidence(
                "arbitrum", ADDRESS, history_payload(), comparison_block=123, client=client,
            )
            second = fetch_strict_evidence(
                "arbitrum", ADDRESS, history_payload(), comparison_block=123, client=client,
            )
        self.assertIn("e1", first["eventIndexes"])
        self.assertEqual("missing_event_index", second["errors"]["e1"])
        self.assertEqual([ENDPOINTS[0], ENDPOINTS[1], ENDPOINTS[1]], attempts)
        self.assertEqual({ENDPOINTS[0]}, client._disabled_endpoints)

    def test_capacity_rejected_batch_disables_provider_before_single_call_recovery(self):
        attempts = []

        def transport(client, endpoint, payload):
            if isinstance(payload, list) or payload["method"] == "eth_call":
                attempts.append((endpoint, isinstance(payload, list)))
                if endpoint == ENDPOINTS[0]:
                    return {"jsonrpc": "2.0", "id": 1 if isinstance(payload, list) else payload["id"],
                            "error": {"code": -32005, "message": "monthly capacity exceeded"}}
            return successful_rpc_response(payload)

        evidence = self._fetch_with_transport(transport)
        self.assertEqual([(ENDPOINTS[0], True), (ENDPOINTS[1], False), (ENDPOINTS[1], False)], attempts)
        self.assertEqual("-60", evidence["currentPositions"]["5|7"]["wei"])
        self.assertEqual({}, evidence["errors"])

    def test_empty_logs_and_unavailable_alternates_keep_missing_index_fail_closed(self):
        for alternate_empty in (True, False):
            with self.subTest(alternate_empty=alternate_empty):
                attempts = []

                def transport(client, endpoint, payload):
                    result = successful_rpc_response(payload)
                    if not isinstance(payload, list) and payload["method"] == "eth_getLogs":
                        attempts.append(endpoint)
                        if endpoint == ENDPOINTS[1] and not alternate_empty:
                            return rejected_rpc_response(payload)
                        result["result"] = []
                    return result

                evidence = self._fetch_with_transport(transport)
                self.assertEqual(ENDPOINTS, attempts)
                self.assertEqual({}, evidence["eventIndexes"])
                self.assertEqual("missing_event_index", evidence["errors"]["e1"])
                replay = build_strict_replay(history_payload(), evidence)
                self.assertEqual("coverage_incomplete", replay["strictStatus"])
                self.assertEqual({}, replay["markets"])

    def test_missing_or_undecodable_batch_balance_retries_only_that_pinned_read(self):
        for missing in (None, "0x", "0x0"):
            with self.subTest(missing=missing):
                singles = []

                def transport(client, endpoint, payload):
                    result = successful_rpc_response(payload)
                    if isinstance(payload, list):
                        result[1]["result"] = missing
                    elif payload["method"] == "eth_call":
                        singles.append(payload)
                    return result

                evidence = self._fetch_with_transport(transport)
                self.assertEqual(1, len(singles))
                self.assertTrue(singles[0]["params"][0]["data"].startswith("0x6a8194e7"))
                self.assertEqual("0x7b", singles[0]["params"][1])
                self.assertEqual("-60", evidence["currentPositions"]["5|7"]["wei"])
                self.assertEqual({}, evidence["errors"])

    def test_all_providers_rejecting_snapshot_balance_remains_coverage_incomplete(self):
        attempts = []

        def transport(client, endpoint, payload):
            if isinstance(payload, list):
                results = successful_rpc_response(payload)
                results[1]["result"] = None
                return results
            if payload["method"] == "eth_call":
                attempts.append((endpoint, payload["params"]))
                return rejected_rpc_response(payload)
            return successful_rpc_response(payload)

        evidence = self._fetch_with_transport(transport)
        self.assertEqual([ENDPOINTS[0], ENDPOINTS[1]], [row[0] for row in attempts])
        self.assertEqual(attempts[0][1], attempts[1][1])
        self.assertEqual("0x7b", attempts[0][1][1])
        self.assertEqual({}, evidence["currentPositions"])
        self.assertEqual("missing_current_positions", evidence["errors"]["currentPositions:5"])
        replay = build_strict_replay(history_payload(), evidence)
        self.assertEqual("coverage_incomplete", replay["strictStatus"])
        self.assertEqual({}, replay["markets"])

    def test_http_failures_try_each_distinct_provider_once_without_nested_rotation(self):
        attempts = []

        def transport(client, endpoint, payload):
            if not isinstance(payload, list) and payload["method"] == "eth_getLogs":
                attempts.append(endpoint)
                raise HTTPError("provider unavailable")
            return successful_rpc_response(payload)

        with self.assertRaises(RpcError):
            self._fetch_with_transport(transport)
        self.assertEqual(ENDPOINTS, attempts)

    def test_caller_without_endpoint_metadata_keeps_missing_batch_evidence_fail_closed(self):
        client = FakeRpcClient([index_log()])
        with patch.object(client, "eth_call_batch", return_value=[encode_current_index(INDEX_SCALE, INDEX_SCALE), None]):
            evidence = fetch_strict_evidence(
                "arbitrum", ADDRESS, history_payload(), comparison_block=123, client=client,
            )
        self.assertEqual({}, evidence["currentPositions"])
        self.assertEqual("missing_current_positions", evidence["errors"]["currentPositions:5"])
        self.assertFalse(any(call[0] == "eth_call" for call in client.calls))

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

    def test_empty_index_logs_are_not_persisted_as_successful_evidence(self):
        cache = {}
        first = fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=FakeRpcClient([]), evidence_cache=cache,
        )
        self.assertEqual("missing_event_index", first["errors"]["e1"])
        self.assertEqual({}, cache.get("entries", {}))
        diagnostics = {}
        recovered = fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=FakeRpcClient([index_log()]), evidence_cache=cache, diagnostics=diagnostics,
        )
        self.assertIn("e1", recovered["eventIndexes"])
        self.assertEqual({"hits": 0, "misses": 1, "skipped": 0}, diagnostics)

    def test_legacy_cached_empty_index_logs_are_evicted_and_refetched(self):
        cache = {}
        fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=FakeRpcClient([index_log()]), evidence_cache=cache,
        )
        next(iter(cache["entries"].values()))["logs"] = []
        diagnostics = {}
        client = FakeRpcClient([index_log()])
        recovered = fetch_strict_evidence(
            "arbitrum", ADDRESS, history_payload(), comparison_block=123,
            client=client, evidence_cache=cache, diagnostics=diagnostics,
        )
        self.assertIn("e1", recovered["eventIndexes"])
        self.assertEqual(1, len([call for call in client.calls if call[0] == "eth_getLogs"]))
        self.assertEqual({"hits": 0, "misses": 1, "skipped": 1}, diagnostics)

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
