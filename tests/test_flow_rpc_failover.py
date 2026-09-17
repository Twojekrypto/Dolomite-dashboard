"""Provider range limits must not starve healthy independent witnesses."""
import unittest
from unittest.mock import Mock, patch

import requests
import generate_dolo_flows as dolo
import generate_vedolo_flows as vedolo
import rpc_client


class FlowRpcFailoverTests(unittest.TestCase):
    def test_dolo_range_error_does_not_block_healthy_quorum(self):
        chain = {"bera": {"name": "Berachain", "chunk_size": 2000,
                          "rpcs": ["https://limited.test", "https://one.test", "https://two.test"]}}

        def request(endpoint, _cfg, _start, _end):
            if endpoint == "https://limited.test":
                raise dolo.TransferLogRangeError("range limit")
            return []

        with patch.object(dolo, "CHAINS", chain), patch.object(
            dolo, "_request_transfer_logs", side_effect=request
        ) as fetch, patch.object(dolo.time, "sleep"):
            _, failed, _ = dolo.fetch_transfer_logs("bera", 1, 2000)
        self.assertEqual(failed, 0)
        self.assertEqual(fetch.call_count, 3)
        self.assertTrue(all(call.args[2:] == (1, 2000) for call in fetch.call_args_list))

    def test_http_400_range_error_is_classified_without_generic_retries(self):
        response = Mock(status_code=400)
        response.json.return_value = {"error": {"code": 35, "message": "ranges over 10000 blocks are not supported"}}
        response.raise_for_status.side_effect = requests.HTTPError("400")
        with patch.object(dolo.requests, "post", return_value=response) as fetch, patch.object(dolo.time, "sleep"):
            with self.assertRaises(dolo.TransferLogRangeError):
                dolo._request_transfer_logs("https://limited.test", {"name": "Berachain"}, 1, 1000)
        self.assertEqual(fetch.call_count, 1)

    def test_vedolo_tries_healthy_peer_before_shrinking(self):
        event = {"topics": [vedolo.DEPOSIT_TOPIC], "blockNumber": "0x10"}

        def request(endpoint, **_kwargs):
            response = Mock()
            response.json.return_value = ({"error": {"message": "range limit"}}
                                          if endpoint.endswith("limited.test") else {"result": [event]})
            return response

        with patch.object(vedolo, "RPC_URLS", ["https://limited.test", "https://healthy.test"]), patch.object(
            vedolo.requests, "post", side_effect=request
        ) as fetch, patch.object(vedolo.time, "sleep"):
            self.assertEqual(vedolo.fetch_event_logs(1, 2000, vedolo.DEPOSIT_TOPIC), [event])
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(fetch.call_args.kwargs["json"]["params"][0]["toBlock"], hex(2000))

    def test_two_keys_from_one_vendor_cannot_confirm_empty_vedolo_range(self):
        endpoints = ["https://bera.g.alchemy.com/v2/one", "https://bera.g.alchemy.com/v2/two", "https://failed.test"]

        def request(endpoint, **_kwargs):
            response = Mock()
            response.json.return_value = ({"error": {"message": "unavailable"}}
                                          if endpoint.endswith("failed.test") else {"result": []})
            return response

        with patch.object(vedolo, "RPC_URLS", endpoints), patch.object(
            vedolo.requests, "post", side_effect=request
        ), patch.object(vedolo.time, "sleep"):
            with self.assertRaisesRegex(vedolo.EventLogFetchError, "unconfirmed empty"):
                vedolo.fetch_event_logs(1, 100, vedolo.DEPOSIT_TOPIC)

    def test_official_berachain_alias_does_not_add_an_independent_vote(self):
        self.assertEqual(dolo.rpc_provider_family("https://rpc.berachain-apis.com"),
                         dolo.rpc_provider_family("https://rpc.berachain.com"))

    def test_verified_berachain_alias_is_available_as_fallback(self):
        self.assertIn("https://rpc.berachain-apis.com/", rpc_client.PUBLIC_ENDPOINTS["berachain"])


if __name__ == "__main__":
    unittest.main()
