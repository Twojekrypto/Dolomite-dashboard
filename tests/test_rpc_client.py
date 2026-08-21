import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rpc_client
from rpc_client import (
    RpcClient,
    RpcError,
    decode_uint256,
    get_endpoints,
    rpc_batch_requests,
    rpc_single_request,
    safe_host,
)


def _response(json_data, status=200):
    resp = mock.Mock()
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


class TestEndpoints(unittest.TestCase):
    def test_shared_rpc_client_reads_new_jeff_provider_secrets(self):
        self.assertIn(
            "ALCHEMY_BERACHAIN_RPC_2_JEFF",
            rpc_client.CHAIN_ENV_KEYS["berachain"],
        )
        self.assertIn(
            "ALCHEMY_MANTLE_RPC_2_JEFF",
            rpc_client.CHAIN_ENV_KEYS["mantle"],
        )

    def test_new_berachain_capacity_precedes_exhausted_primary_alchemy(self):
        with mock.patch.dict(os.environ, {
            "DRPC_BERACHAIN_RPC_ZEN": "https://drpc.example/key",
            "ALCHEMY_BERACHAIN_RPC_2_JEFF": "https://jeff.example/v2/key",
            "ALCHEMY_BERACHAIN_RPC": "https://old.example/v2/key",
        }):
            eps = get_endpoints("berachain")

        self.assertEqual(eps[0], "https://drpc.example/key")
        self.assertLess(
            eps.index("https://jeff.example/v2/key"),
            eps.index("https://old.example/v2/key"),
        )

    def test_strict_replay_reads_existing_mantle_and_xlayer_secret_names(self):
        for env_name in (
            "MANTLE_RPC",
            "MANTLE_RPC_2",
            "QUICKNODE_MANTLE_RPC",
            "QUICKNODE_MANTLE_RPC_2",
            "MANTLE_RPC_QUICKNODE_TWOJE",
            "DRPC_MANTLE_RPC",
            "DRPC_MANTLE_RPC_ZEN",
            "ALCHEMY_MANTLE_RPC_ZEN",
            "ALCHEMY_MANTLE_RPC_DANU",
            "ALCHEMY_MANTLE_RPC_3",
        ):
            self.assertIn(env_name, rpc_client.CHAIN_ENV_KEYS["mantle"])
        for env_name in (
            "XLAYER_RPC_QUICKNODE_TWOJE",
            "XLAYER_RPC",
            "XLAYER_RPC_2",
            "ALCHEMY_XLAYER_RPC_ZEN",
            "DRP_XLAYER_RPC_TWO",
        ):
            self.assertIn(env_name, rpc_client.CHAIN_ENV_KEYS["xlayer"])

    def test_env_endpoint_first(self):
        with mock.patch.dict(os.environ, {
            "ALCHEMY_BERACHAIN_RPC": "https://x.example/v2/key",
            "DRPC_BERACHAIN_RPC_ZEN": "https://drpc.example/key",
        }):
            eps = get_endpoints("berachain")
        self.assertEqual(eps[0], "https://drpc.example/key")
        self.assertLess(eps.index("https://drpc.example/key"), eps.index("https://x.example/v2/key"))
        self.assertIn("https://rpc.berachain.com/", eps)

    def test_unknown_chain_raises(self):
        with self.assertRaises(ValueError):
            get_endpoints("nochain")

    def test_safe_host_strips_key(self):
        self.assertEqual(safe_host("https://berachain-mainnet.g.alchemy.com/v2/SECRET"),
                         "berachain-mainnet.g.alchemy.com")


class TestDecode(unittest.TestCase):
    def test_decode_empty(self):
        self.assertEqual(decode_uint256("0x"), 0)
        self.assertEqual(decode_uint256(""), 0)
        self.assertEqual(decode_uint256(None), 0)

    def test_decode_value(self):
        self.assertEqual(decode_uint256("0x" + "1".zfill(64)), 1)


class TestRpcClient(unittest.TestCase):
    def _client(self, endpoints=("https://a.example", "https://b.example")):
        return RpcClient(endpoints=list(endpoints), quiet=True, retries_per_endpoint=2)

    def test_call_success(self):
        client = self._client()
        with mock.patch.object(rpc_client.requests, "post",
                               return_value=_response({"jsonrpc": "2.0", "id": 1, "result": "0x5"})) as post:
            self.assertEqual(client.call("eth_blockNumber", []), "0x5")
        self.assertEqual(post.call_count, 1)
        self.assertEqual(client.last_endpoint, "https://a.example")
        # timeout always passed
        self.assertIn("timeout", post.call_args.kwargs)

    def test_rotation_on_failure(self):
        client = self._client()
        ok = _response({"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        with mock.patch.object(rpc_client.requests, "post",
                               side_effect=[Exception("boom"), ok]) as post:
            self.assertEqual(client.call("eth_blockNumber", []), "0x1")
        self.assertEqual(post.call_count, 2)
        self.assertEqual(client.last_endpoint, "https://b.example")
        # sticky: next call starts from the endpoint that worked
        with mock.patch.object(rpc_client.requests, "post", return_value=ok) as post2:
            client.call("eth_blockNumber", [])
        self.assertEqual(post2.call_args.args[0], "https://b.example")

    def test_all_fail_raises(self):
        client = self._client()
        with mock.patch.object(rpc_client.requests, "post", side_effect=Exception("down")), \
             mock.patch.object(rpc_client.time, "sleep"):
            with self.assertRaises(RpcError):
                client.call("eth_blockNumber", [])

    def test_rpc_error_payload_raises(self):
        client = self._client()
        with mock.patch.object(rpc_client.requests, "post",
                               return_value=_response({"jsonrpc": "2.0", "id": 1,
                                                       "error": {"code": -32000, "message": "x"}})):
            with self.assertRaises(RpcError):
                client.call("eth_call", [])

    def test_monthly_capacity_error_rotates_and_stays_off_exhausted_endpoint(self):
        client = self._client()
        exhausted = _response({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": 429, "message": "Monthly capacity limit exceeded"},
        })
        ok = _response({"jsonrpc": "2.0", "id": 1, "result": "0x7"})

        with mock.patch.object(rpc_client.requests, "post", side_effect=[exhausted, ok, ok]) as post:
            self.assertEqual("0x7", client.call("eth_blockNumber", []))
            self.assertEqual("0x7", client.call("eth_blockNumber", []))

        self.assertEqual(3, post.call_count)
        self.assertEqual("https://b.example", post.call_args.args[0])

    def test_batch_preserves_order(self):
        client = self._client()
        shuffled = [
            {"jsonrpc": "2.0", "id": 2, "result": "0x2"},
            {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
        ]
        with mock.patch.object(rpc_client.requests, "post", return_value=_response(shuffled)):
            out = client.eth_call_batch([("0xT", "0xd1"), ("0xT", "0xd2")])
        self.assertEqual(out, ["0x1", "0x2"])

    def test_batch_error_entry_raises(self):
        client = self._client()
        bad = [
            {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
            {"jsonrpc": "2.0", "id": 2, "error": {"code": -32000, "message": "revert"}},
        ]
        with mock.patch.object(rpc_client.requests, "post", return_value=_response(bad)):
            with self.assertRaises(RpcError):
                client.eth_call_batch([("0xT", "0xd1"), ("0xT", "0xd2")])

    def test_generic_batch_returns_missing_ids_for_partial_response(self):
        payloads = [
            {"jsonrpc": "2.0", "method": "eth_call", "params": [], "id": "a"},
            {"jsonrpc": "2.0", "method": "eth_call", "params": [], "id": "b"},
        ]
        with mock.patch.object(
            rpc_client.requests,
            "post",
            return_value=_response([{"jsonrpc": "2.0", "id": "a", "result": "0x1"}]),
        ):
            responses, missing = rpc_batch_requests(
                ["https://a.example"],
                payloads,
                quiet=True,
                retries_per_endpoint=1,
            )

        self.assertEqual(responses["a"]["result"], "0x1")
        self.assertEqual(missing, ["b"])

    def test_generic_batch_records_successful_methods(self):
        payloads = [
            {"jsonrpc": "2.0", "method": "eth_call", "params": [], "id": "a"},
            {"jsonrpc": "2.0", "method": "eth_getLogs", "params": [], "id": "b"},
        ]
        response = _response([
            {"jsonrpc": "2.0", "id": "a", "result": "0x1"},
            {"jsonrpc": "2.0", "id": "b", "result": "0x2"},
        ])
        with mock.patch.object(rpc_client.requests, "post", return_value=response), \
             mock.patch.object(rpc_client.rpc_usage, "record_methods") as record_methods, \
             mock.patch.object(rpc_client.time, "sleep"):
            responses, missing = rpc_batch_requests(
                ["https://a.example"],
                payloads,
                quiet=True,
                retries_per_endpoint=1,
            )

        self.assertEqual(set(responses), {"a", "b"})
        self.assertEqual(missing, [])
        record_methods.assert_called_once_with(["eth_call", "eth_getLogs"])

    def test_generic_batch_retries_a_json_rpc_error_entry(self):
        payloads = [
            {"jsonrpc": "2.0", "method": "eth_call", "params": [], "id": "a"},
        ]
        rate_limited = _response([
            {"jsonrpc": "2.0", "id": "a", "error": {"code": 429, "message": "rate limit"}},
        ])
        recovered = _response([
            {"jsonrpc": "2.0", "id": "a", "result": "0x1"},
        ])

        with mock.patch.object(
            rpc_client.requests, "post", side_effect=[rate_limited, recovered]
        ) as post, mock.patch.object(rpc_client.time, "sleep") as sleep:
            responses, missing = rpc_batch_requests(
                ["https://a.example"],
                payloads,
                quiet=True,
                retries_per_endpoint=2,
            )

        self.assertEqual(responses, {"a": {"jsonrpc": "2.0", "id": "a", "result": "0x1"}})
        self.assertEqual(missing, [])
        self.assertEqual(post.call_count, 2)
        sleep.assert_any_call(rpc_client.BACKOFF_BASE_SECONDS)

    def test_generic_batch_honors_retry_after_for_http_429(self):
        payloads = [
            {"jsonrpc": "2.0", "method": "eth_call", "params": [], "id": "a"},
        ]
        rate_limited = _response([])
        http_error = rpc_client.requests.HTTPError("429")
        http_error.response = mock.Mock(headers={"Retry-After": "3"}, status_code=429)
        rate_limited.raise_for_status.side_effect = http_error
        recovered = _response([
            {"jsonrpc": "2.0", "id": "a", "result": "0x1"},
        ])

        with mock.patch.object(
            rpc_client.requests, "post", side_effect=[rate_limited, recovered]
        ), mock.patch.object(rpc_client.time, "sleep") as sleep:
            responses, missing = rpc_batch_requests(
                ["https://a.example"],
                payloads,
                quiet=True,
                retries_per_endpoint=2,
            )

        self.assertEqual(responses["a"]["result"], "0x1")
        self.assertEqual(missing, [])
        sleep.assert_any_call(3.0)

    def test_generic_single_request_rotates_to_next_endpoint(self):
        ok = _response({"jsonrpc": "2.0", "id": 1, "result": "0x7"})
        with mock.patch.object(rpc_client.requests, "post", side_effect=[Exception("down"), ok]):
            out = rpc_single_request(
                ["https://a.example", "https://b.example"],
                {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
                quiet=True,
                retries_per_endpoint=1,
            )

        self.assertEqual(out["result"], "0x7")


if __name__ == "__main__":
    unittest.main()
