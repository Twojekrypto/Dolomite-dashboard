import unittest
from unittest.mock import Mock, patch

import generate_odolo_flows as odolo_flows


class GenerateOdoloFlowsTests(unittest.TestCase):
    def test_current_block_must_be_valid_before_cutoffs(self):
        with self.assertRaises(RuntimeError):
            odolo_flows.build_cutoff_blocks(0)

        with self.assertRaises(RuntimeError):
            odolo_flows.build_cutoff_blocks(odolo_flows.DEPLOY_BLOCK - 1)

    def test_cutoff_blocks_keep_all_as_deploy_block(self):
        current_block = odolo_flows.DEPLOY_BLOCK + 2_000_000
        cutoffs = odolo_flows.build_cutoff_blocks(current_block)

        self.assertEqual(cutoffs["all"], odolo_flows.DEPLOY_BLOCK)
        self.assertGreater(cutoffs["1d"], cutoffs["7d"])
        self.assertGreater(cutoffs["7d"], cutoffs["30d"])
        self.assertGreaterEqual(cutoffs["180d"], cutoffs["all"])

    def test_future_rewards_wallet_is_not_a_top_flow_row(self):
        user = "0x1111111111111111111111111111111111111111"
        rows = odolo_flows.get_top(
            {
                odolo_flows.REWARDS_CONTRACT: 1000,
                user: 100,
            },
            tx_counts={odolo_flows.REWARDS_CONTRACT: 5, user: 1},
            n=10,
            excluded=odolo_flows.EXCLUDED_ADDRS,
        )

        self.assertEqual([row["address"] for row in rows], [user])

    def test_transfer_log_fetch_refuses_partial_chunk(self):
        response = Mock()
        response.json.return_value = {"error": {"message": "temporary rpc failure"}}

        with patch.object(odolo_flows, "RPC_URLS", ["https://rpc.example"]), \
             patch.object(odolo_flows.requests, "post", return_value=response), \
             patch.object(odolo_flows.time, "sleep"):
            with self.assertRaises(RuntimeError):
                odolo_flows.fetch_transfer_logs(100, 200)

    def test_detect_contracts_uses_batch_results(self):
        contract = "0x" + "a" * 40
        wallet = "0x" + "b" * 40

        def fake_batch(_rpcs, payloads, **_kwargs):
            out = {}
            for payload in payloads:
                request_id = payload["id"]
                result = "0x6000" if payload["params"][0] == contract else "0x"
                out[request_id] = {"jsonrpc": "2.0", "id": request_id, "result": result}
            return out, []

        with patch.object(odolo_flows, "RPC_URLS", ["https://rpc.example"]), \
             patch.object(odolo_flows, "rpc_batch_requests", side_effect=fake_batch), \
             patch.object(odolo_flows, "rpc_single_request") as single:
            contracts = odolo_flows.detect_contracts_batch([contract, wallet])

        single.assert_not_called()
        self.assertEqual(contracts, {contract})


if __name__ == "__main__":
    unittest.main()
