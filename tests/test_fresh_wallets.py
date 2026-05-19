import unittest
from unittest.mock import Mock, patch

import generate_dolo_flows as flows


class _ExplorerResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FreshWalletTests(unittest.TestCase):
    def test_requires_etherscan_key_for_multichain_activity(self):
        with patch.object(flows, "ETHERSCAN_API_KEY", ""):
            with self.assertRaises(RuntimeError):
                flows.validate_fresh_wallet_activity_config()

    def test_first_activity_uses_oldest_tracked_chain(self):
        base_ts = 2_000_000_000
        old_ts = base_ts - flows.PERIODS["90d"] - 100
        fresh_ts = base_ts - flows.PERIODS["7d"] + 100

        def fake_get(_url, params=None, timeout=None):
            chainid = params.get("chainid")
            if chainid == 1:
                return _ExplorerResponse({
                    "status": "1",
                    "result": [{
                        "timeStamp": str(fresh_ts),
                        "blockNumber": "123",
                        "hash": "0xfresh",
                    }],
                })
            if chainid == 42161:
                return _ExplorerResponse({
                    "status": "1",
                    "result": [{
                        "timeStamp": str(old_ts),
                        "blockNumber": "456",
                        "hash": "0xold",
                    }],
                })
            return _ExplorerResponse({"status": "0", "message": "No transactions found", "result": []})

        session = Mock()
        session.get.side_effect = fake_get
        sources = [
            {"key": "eth", "name": "Ethereum", "provider": "etherscan", "chainid": 1},
            {"key": "arb", "name": "Arbitrum", "provider": "etherscan", "chainid": 42161},
        ]

        with patch.object(flows, "ETHERSCAN_API_KEY", "test"), \
             patch.object(flows, "FRESH_WALLET_ACTIVITY_SOURCES", sources), \
             patch.object(flows, "FRESH_ETHERSCAN_REQUEST_DELAY_SECONDS", 0):
            first = flows.wallet_first_activity("0x1111111111111111111111111111111111111111", {}, session, base_ts)

        self.assertTrue(first["verified"])
        self.assertEqual(first["chain"], "arb")
        self.assertEqual(first["first_timestamp"], old_ts)

    def test_prior_outgoing_dolo_transfer_excludes_fresh_candidate(self):
        candidate = "0x1111111111111111111111111111111111111111"
        other = "0x2222222222222222222222222222222222222222"
        source = "0x3333333333333333333333333333333333333333"
        all_transfers = {
            "eth": [
                (candidate, other, int(5_000 * 10**18), 50),
                (source, candidate, int(20_000 * 10**18), 150),
            ],
            "bera": [],
        }
        cutoffs = {
            chain: {period: 100 for period in flows.FRESH_HOLDER_PERIODS}
            for chain in flows.CHAINS
        }
        current_blocks = {"eth": 200, "bera": 200}
        neutralized = {
            period: {"eth": {candidate: 15_000}, "bera": {}}
            for period in flows.FRESH_HOLDER_PERIODS
        }

        with patch.object(flows, "load_current_holder_rows", return_value={}), \
             patch.object(flows, "load_address_labels", return_value={}), \
             patch.object(flows, "load_current_vedolo_locks", return_value={}), \
             patch.object(flows, "wallet_first_activity") as first_activity:
            rows, audit = flows.build_fresh_holders(
                all_transfers,
                cutoffs,
                current_blocks,
                neutralized,
                2_000_000_000,
                {},
            )

        self.assertEqual(rows["90d"], [])
        first_activity.assert_not_called()
        self.assertGreaterEqual(audit["90d"]["candidateWallets"], 1)

    def test_vedolo_locked_balance_counts_toward_fresh_exposure(self):
        candidate = "0x1111111111111111111111111111111111111111"
        source = "0x3333333333333333333333333333333333333333"
        vester = "0x3e9b9a16743551da49b5e136c716bba7932d2cec"
        all_transfers = {
            "eth": [
                (source, candidate, int(20_000 * 10**18), 150),
                (candidate, vester, int(20_000 * 10**18), 160),
            ],
            "bera": [],
        }
        cutoffs = {
            chain: {period: 100 for period in flows.FRESH_HOLDER_PERIODS}
            for chain in flows.CHAINS
        }
        current_blocks = {"eth": 200, "bera": 200}
        neutralized = {
            period: {"eth": {candidate: 0}, "bera": {}}
            for period in flows.FRESH_HOLDER_PERIODS
        }
        first_activity = {
            "verified": True,
            "status": "ok",
            "chain": "eth",
            "first_timestamp": 2_000_000_000 - flows.PERIODS["90d"] + 10,
            "first_block": 140,
            "first_tx": "0xabc",
            "source": "normal_tx",
        }

        with patch.object(flows, "load_current_holder_rows", return_value={}), \
             patch.object(flows, "load_address_labels", return_value={}), \
             patch.object(flows, "load_current_vedolo_locks", return_value={candidate: 20_000}), \
             patch.object(flows, "wallet_first_activity", return_value=first_activity):
            rows, _audit = flows.build_fresh_holders(
                all_transfers,
                cutoffs,
                current_blocks,
                neutralized,
                2_000_000_000,
                {},
            )

        self.assertEqual(len(rows["90d"]), 1)
        row = rows["90d"][0]
        self.assertEqual(row["address"], candidate)
        self.assertEqual(row["liquid_balance"], 0)
        self.assertEqual(row["locked_balance"], 20_000)
        self.assertEqual(row["exposure"], 20_000)
        self.assertEqual(row["balance"], 20_000)


if __name__ == "__main__":
    unittest.main()
