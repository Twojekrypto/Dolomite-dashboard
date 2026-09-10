import os
import unittest
from unittest.mock import patch

import requests

import calculate_avg_lock
import generate_exercisers
import generate_dolo_liquidity as liquidity
import update_exercised_usd


def response(payload):
    result = requests.Response()
    result.status_code = 200
    import json
    result._content = json.dumps(payload).encode()
    return result


UNSUPPORTED = {"status": "0", "message": "chain not supported", "result": None}


class ExplorerOutageTests(unittest.TestCase):
    def test_rejection_reports_provider_reason_without_echoing_credentials(self):
        from explorer_api import explorer_get
        with patch.dict(os.environ, {"ETHERSCAN_API_KEY": "private-test-key"}), patch(
            "requests.get", side_effect=[response(UNSUPPORTED), response({
                "status": "0", "message": "NOTOK",
                "result": "Invalid API Key: private-test-key",
            })]
        ):
            with self.assertRaises(RuntimeError) as raised:
                explorer_get(calculate_avg_lock.ROUTESCAN_API, params={}, timeout=10)
        self.assertIn("Invalid API Key", str(raised.exception))
        self.assertNotIn("private-test-key", str(raised.exception))

    def test_kodiak_retries_a_transient_timeout_without_changing_the_page(self):
        position = {"id": "7", "owner": "0x" + "4" * 40, "liquidity": "123",
                    "tickLower": {"tickIdx": "-10"}, "tickUpper": {"tickIdx": "10"},
                    "pool": {"id": "0x" + "1" * 40, "feeTier": "3000", "tick": "0", "sqrtPrice": "1",
                             "token0": {"id": "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654", "decimals": "18"},
                             "token1": {"id": "0x" + "3" * 40, "decimals": "18"}}}
        with patch("requests.Session.post", side_effect=[
            requests.Timeout("temporary upstream timeout"), response({"data": {"positions": [position]}})
        ]) as post, patch("time.sleep"):
            indexed = liquidity._kodiak_position_index("0x" + "1" * 40)
        self.assertEqual(indexed[7]["indexedLiquidity"], 123)
        self.assertEqual(post.call_args_list[0], post.call_args_list[1])

    def test_missing_batch_receipt_recovers_via_individual_rpc(self):
        tx = "0x" + "a" * 64
        raw = {"address": "0x" + "1" * 40, "blockNumber": "0xa",
               "blockHash": "0x" + "b" * 64, "transactionHash": tx,
               "transactionIndex": "0x0", "logIndex": "0x",
               "topics": ["0x" + "2" * 64], "data": "0x", "timeStamp": "0x64"}
        with patch("requests.Session.get", return_value=response({"status": "1", "result": [raw]})), patch.object(
            liquidity, "rpc_batch_requests", return_value=({}, [f"receipt:{tx}"])
        ), patch.object(liquidity, "rpc_single_request", return_value={"result": {
            "status": "0x1", "logs": [{**raw, "logIndex": "0x7"}]
        }}):
            rows = liquidity._routescan_logs(80094, raw["address"], raw["topics"][0], 10, 10)
        self.assertEqual([(row["logIndex"], row["timestamp"]) for row in rows], [(7, 100)])

    def test_failed_fallback_does_not_expose_api_key_or_return_empty(self):
        with patch.dict(os.environ, {"ETHERSCAN_API_KEY": "private-test-key"}), patch(
            "requests.get", side_effect=[response(UNSUPPORTED), response(
                {"status": "0", "message": "NOTOK", "result": []}
            )]
        ):
            with self.assertRaises(RuntimeError) as raised:
                update_exercised_usd.get_new_transactions(100)
        self.assertNotIn("private-test-key", str(raised.exception))

    def test_average_lock_recovers_from_unsupported_chain(self):
        tx = {"methodId": "0xa88f8139", "isError": "0", "txreceipt_status": "1"}
        with patch.dict(os.environ, {"ETHERSCAN_API_KEY": "test-key"}), patch(
            "requests.get", side_effect=[response(UNSUPPORTED), response(
                {"status": "1", "message": "OK", "result": [tx]}
            )]
        ) as get:
            self.assertEqual(calculate_avg_lock.get_all_exercise_txs(), [tx])
        self.assertEqual(get.call_args.args[0], "https://api.etherscan.io/v2/api")
        self.assertEqual(get.call_args.kwargs["params"]["chainid"], "80094")
        self.assertEqual(get.call_args.kwargs["params"]["apikey"], "test-key")

    def test_exerciser_pagination_error_never_returns_partial_history(self):
        with patch("requests.get", side_effect=[
            response({"status": "1", "result": [{"hash": "one"}]}),
            response({"status": "0", "message": "NOTOK", "result": "Max rate limit reached"}),
        ]), patch.object(generate_exercisers, "PAGE_SIZE", 1), patch("time.sleep"):
            with self.assertRaises(RuntimeError):
                generate_exercisers._get_all_transactions_once()

    def test_usd_scanner_does_not_treat_api_error_as_no_new_transactions(self):
        with patch("requests.get", return_value=response(UNSUPPORTED)), patch.dict(
            os.environ, {"ETHERSCAN_API_KEY": "", "BERASCAN_API_KEY": ""}
        ):
            with self.assertRaises(RuntimeError):
                update_exercised_usd.get_new_transactions(100)

    def test_liquidity_single_block_error_does_not_become_empty_logs(self):
        with patch("requests.Session.get", return_value=response(UNSUPPORTED)), patch.dict(
            os.environ, {"ETHERSCAN_API_KEY": "", "BERASCAN_API_KEY": ""}
        ):
            with self.assertRaises(RuntimeError):
                liquidity._routescan_logs(80094, "0x" + "1" * 40, "0x" + "2" * 64, 100, 100)

    def test_liquidity_preserves_indexed_filters_on_fallback(self):
        with patch.dict(os.environ, {"ETHERSCAN_API_KEY": "test-key"}), patch(
            "requests.Session.get", side_effect=[response(UNSUPPORTED), response(
                {"status": "0", "message": "No records found", "result": []}
            )]
        ) as get:
            self.assertEqual(liquidity._routescan_logs(
                80094, "0x" + "1" * 40, "0x" + "2" * 64, 100, 100,
                indexed_topics={2: "0x" + "3" * 64},
            ), [])
        self.assertEqual(get.call_args.args[0], "https://api.etherscan.io/v2/api")
        self.assertEqual(get.call_args.kwargs["params"]["topic2"], "0x" + "3" * 64)
        self.assertEqual(get.call_args.kwargs["params"]["fromBlock"], 100)


if __name__ == "__main__":
    unittest.main()
