import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rpc_client
from rpc_client import RpcClient, RpcError, decode_uint256, get_endpoints, safe_host


def _response(json_data, status=200):
    resp = mock.Mock()
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


class TestEndpoints(unittest.TestCase):
    def test_env_endpoint_first(self):
        with mock.patch.dict(os.environ, {"ALCHEMY_BERACHAIN_RPC": "https://x.example/v2/key"}):
            eps = get_endpoints("berachain")
        self.assertEqual(eps[0], "https://x.example/v2/key")
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


if __name__ == "__main__":
    unittest.main()
