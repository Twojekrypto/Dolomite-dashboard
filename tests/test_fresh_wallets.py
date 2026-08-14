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

    def test_parse_debank_rendered_wallet_age(self):
        html = '''
        <div class="db-user-tag is-age"><img alt="">88 days</div>
        <div><div><div>TVF</div></div></div>
        '''
        days, raw = flows.parse_debank_age_days(html)
        range_days, max_days, range_raw = flows.parse_debank_age_range_days(html)

        self.assertEqual(days, 88)
        self.assertEqual(raw, "88 days")
        self.assertEqual(range_days, 88)
        self.assertEqual(max_days, 89)
        self.assertEqual(range_raw, "88 days")

    def test_parse_debank_age_range_is_conservative_for_months(self):
        html = '<div class="is-age db-user-tag">3 months</div>'
        days, max_days, raw = flows.parse_debank_age_range_days(html)

        self.assertEqual(days, 90)
        self.assertEqual(max_days, 120)
        self.assertEqual(raw, "3 months")

    def test_debank_age_fallback_verifies_explorer_unverified_wallet(self):
        base_ts = 2_000_000_000

        def fake_get(_url, params=None, timeout=None):
            return _ExplorerResponse({"status": "0", "message": "NOTOK", "result": "rate limit"})

        session = Mock()
        session.get.side_effect = fake_get
        sources = [
            {"key": "base", "name": "Base", "provider": "etherscan", "chainid": 8453},
        ]
        fallback = {
            "verified": True,
            "status": "ok",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": base_ts - 12 * 86400,
            "first_block": 0,
            "first_tx": "",
            "source": "debank_age",
            "debank_age_days": 12,
        }

        with patch.object(flows, "ETHERSCAN_API_KEY", "test"), \
             patch.object(flows, "FRESH_WALLET_ACTIVITY_SOURCES", sources), \
             patch.object(flows, "FRESH_ETHERSCAN_REQUEST_DELAY_SECONDS", 0), \
             patch.object(flows, "fetch_debank_first_activity", return_value=fallback):
            first = flows.wallet_first_activity("0x1111111111111111111111111111111111111111", {}, session, base_ts)

        self.assertTrue(first["verified"])
        self.assertEqual(first["source"], "debank_age")
        self.assertEqual(first["debank_age_days"], 12)
        self.assertEqual(first["explorer_errors"][0]["chain"], "base")

    def test_debank_crosscheck_can_age_tracked_fresh_wallet(self):
        base_ts = 2_000_000_000
        tracked_fresh_ts = base_ts - 12 * 86400

        def fake_get(_url, params=None, timeout=None):
            return _ExplorerResponse({
                "status": "1",
                "result": [{
                    "timeStamp": str(tracked_fresh_ts),
                    "blockNumber": "123",
                    "hash": "0xfresh",
                }],
            })

        session = Mock()
        session.get.side_effect = fake_get
        sources = [
            {"key": "eth", "name": "Ethereum", "provider": "etherscan", "chainid": 1},
        ]
        debank_older = {
            "verified": True,
            "status": "ok",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": base_ts - 613 * 86400,
            "first_block": 0,
            "first_tx": "",
            "source": "debank_age",
            "debank_age_days": 613,
            "debank_age_max_days": 614,
        }

        with patch.object(flows, "ETHERSCAN_API_KEY", "test"), \
             patch.object(flows, "FRESH_WALLET_ACTIVITY_SOURCES", sources), \
             patch.object(flows, "FRESH_ETHERSCAN_REQUEST_DELAY_SECONDS", 0), \
             patch.object(flows, "fetch_debank_first_activity", return_value=debank_older):
            first = flows.wallet_first_activity("0x1111111111111111111111111111111111111111", {}, session, base_ts)

        self.assertTrue(first["verified"])
        self.assertEqual(first["source"], "debank_age")
        self.assertEqual(first["debank_age_days"], 613)
        self.assertEqual(first["explorer_first_activity"]["source"], "normal_tx")

    def test_tracked_fresh_wallet_requires_debank_crosscheck(self):
        base_ts = 2_000_000_000
        tracked_fresh_ts = base_ts - 12 * 86400

        def fake_get(_url, params=None, timeout=None):
            return _ExplorerResponse({
                "status": "1",
                "result": [{
                    "timeStamp": str(tracked_fresh_ts),
                    "blockNumber": "123",
                    "hash": "0xfresh",
                }],
            })

        session = Mock()
        session.get.side_effect = fake_get
        sources = [
            {"key": "eth", "name": "Ethereum", "provider": "etherscan", "chainid": 1},
        ]
        debank_missing = {
            "verified": False,
            "status": "debank_age_missing",
            "chain": "debank",
            "first_timestamp": 0,
            "source": "debank_age",
        }

        with patch.object(flows, "ETHERSCAN_API_KEY", "test"), \
             patch.object(flows, "FRESH_WALLET_ACTIVITY_SOURCES", sources), \
             patch.object(flows, "FRESH_ETHERSCAN_REQUEST_DELAY_SECONDS", 0), \
             patch.object(flows, "fetch_debank_first_activity", return_value=debank_missing):
            first = flows.wallet_first_activity("0x1111111111111111111111111111111111111111", {}, session, base_ts)

        self.assertFalse(first["verified"])
        self.assertEqual(first["status"], "debank_crosscheck_unverified")
        self.assertEqual(first["explorer_first_activity"]["source"], "normal_tx")

    def test_debank_coarse_month_age_is_excluded_from_90d_fresh(self):
        candidate = "0x1111111111111111111111111111111111111111"
        source = "0x3333333333333333333333333333333333333333"
        all_transfers = {
            "eth": [
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
            period: {"eth": {candidate: 20_000}, "bera": {}}
            for period in flows.FRESH_HOLDER_PERIODS
        }
        first_activity = {
            "verified": True,
            "status": "ok",
            "chain": "debank",
            "first_timestamp": 2_000_000_000 - flows.PERIODS["90d"],
            "first_block": 0,
            "first_tx": "",
            "source": "debank_age",
            "debank_age_days": 90,
            "debank_age_max_days": 120,
        }

        with patch.object(flows, "load_current_holder_rows", return_value={}), \
             patch.object(flows, "load_address_labels", return_value={}), \
             patch.object(flows, "load_current_vedolo_locks", return_value={}), \
             patch.object(flows, "wallet_first_activity", return_value=first_activity):
            rows, audit = flows.build_fresh_holders(
                all_transfers,
                cutoffs,
                current_blocks,
                neutralized,
                2_000_000_000,
                {},
            )

        self.assertEqual(rows["90d"], [])
        self.assertGreaterEqual(audit["90d"]["oldWalletsExcluded"], 1)

    def test_cached_debank_age_advances_with_elapsed_time(self):
        base_ts = 2_000_000_000
        first_activity = {
            "verified": True,
            "status": "ok",
            "chain": "debank",
            "first_timestamp": base_ts - 91 * 86400,
            "first_block": 0,
            "first_tx": "",
            "source": "debank_age",
            "debank_age_days": 85,
            "debank_age_max_days": 86,
            "checked_at": base_ts - 6 * 86400,
        }

        self.assertFalse(flows._fresh_activity_within_period(first_activity, "90d", base_ts))

    def test_fresh_holder_reports_current_cached_debank_age(self):
        base_ts = 2_000_000_000
        candidate = "0x1111111111111111111111111111111111111111"
        source = "0x3333333333333333333333333333333333333333"
        all_transfers = {
            "eth": [(source, candidate, int(20_000 * 10**18), 150)],
            "bera": [],
        }
        cutoffs = {
            chain: {period: 100 for period in flows.FRESH_HOLDER_PERIODS}
            for chain in flows.CHAINS
        }
        current_blocks = {"eth": 200, "bera": 200}
        neutralized = {
            period: {"eth": {candidate: 20_000}, "bera": {}}
            for period in flows.FRESH_HOLDER_PERIODS
        }
        first_activity = {
            "verified": True,
            "status": "ok",
            "chain": "debank",
            "first_timestamp": base_ts - 8 * 86400,
            "first_block": 0,
            "first_tx": "",
            "source": "debank_age",
            "debank_age_days": 0.0417,
            "debank_age_max_days": 0.0833,
            "checked_at": base_ts - 8 * 86400,
        }

        with patch.object(flows, "load_current_holder_rows", return_value={}), \
             patch.object(flows, "load_address_labels", return_value={}), \
             patch.object(flows, "load_current_vedolo_locks", return_value={}), \
             patch.object(flows, "wallet_first_activity", return_value=first_activity):
            rows, _audit = flows.build_fresh_holders(
                all_transfers,
                cutoffs,
                current_blocks,
                neutralized,
                base_ts,
                {},
            )

        row = rows["90d"][0]
        self.assertAlmostEqual(row["wallet_age_days"], 8.0, places=4)
        self.assertAlmostEqual(row["wallet_age_max_days"], 8.0416, places=4)

    def test_fresh_holder_keeps_explorer_created_tx_when_debank_crosschecks_age(self):
        base_ts = 2_000_000_000
        candidate = "0x1111111111111111111111111111111111111111"
        source = "0x3333333333333333333333333333333333333333"
        explorer_ts = base_ts - 8 * 86400
        all_transfers = {
            "eth": [(source, candidate, int(20_000 * 10**18), 150)],
            "bera": [],
        }
        cutoffs = {
            chain: {period: 100 for period in flows.FRESH_HOLDER_PERIODS}
            for chain in flows.CHAINS
        }
        neutralized = {
            period: {"eth": {candidate: 20_000}, "bera": {}}
            for period in flows.FRESH_HOLDER_PERIODS
        }
        first_activity = {
            "verified": True,
            "status": "ok",
            "chain": "debank",
            "first_timestamp": base_ts - 9 * 86400,
            "first_block": 0,
            "first_tx": "",
            "source": "debank_age",
            "debank_age_days": 9,
            "debank_age_max_days": 10,
            "explorer_first_activity": {
                "chain": "eth",
                "first_timestamp": explorer_ts,
                "first_block": 321,
                "first_tx": "0xexplorer",
                "source": "normal_tx",
            },
        }

        with patch.object(flows, "load_current_holder_rows", return_value={}), \
             patch.object(flows, "load_address_labels", return_value={}), \
             patch.object(flows, "load_current_vedolo_locks", return_value={}), \
             patch.object(flows, "wallet_first_activity", return_value=first_activity):
            rows, _audit = flows.build_fresh_holders(
                all_transfers,
                cutoffs,
                {"eth": 200, "bera": 200},
                neutralized,
                base_ts,
                {},
            )

        row = rows["90d"][0]
        self.assertEqual(row["wallet_created_chain"], "eth")
        self.assertEqual(row["wallet_created_block"], 321)
        self.assertEqual(row["wallet_created_tx"], "0xexplorer")
        self.assertEqual(row["wallet_created_source"], "normal_tx")
        self.assertEqual(row["verification_source"], "normal_tx+debank_age")
        self.assertEqual(row["wallet_age_verification_source"], "debank_age")
        self.assertEqual(row["wallet_created_timestamp"], "2033-05-10T03:33:20Z")

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

    def test_safe_contract_wallet_counts_as_fresh_holder(self):
        candidate = "0x1111111111111111111111111111111111111111"
        source = "0x3333333333333333333333333333333333333333"
        all_transfers = {
            "eth": [
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
            period: {"eth": {candidate: 20_000}, "bera": {}}
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

        with patch.object(flows, "load_current_holder_rows", return_value={
            candidate: {"is_contract": True, "contract_wallet_type": "safe"},
        }), \
             patch.object(flows, "load_address_labels", return_value={}), \
             patch.object(flows, "load_current_vedolo_locks", return_value={}), \
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
        self.assertEqual(rows["90d"][0]["address"], candidate)
        self.assertEqual(rows["90d"][0]["type"], "multisig")

    def test_named_protocol_safe_keeps_protocol_classification(self):
        candidate = "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4"
        holder_rows = {
            candidate: {"is_contract": True, "contract_wallet_type": "safe"},
        }
        labels = {
            candidate: {"label": "Dolomite Gnosis Safe", "type": "protocol"},
        }

        self.assertEqual(
            flows.holder_distribution_type(candidate, holder_rows, labels),
            "ca",
        )

    def test_unlabeled_safe_keeps_multisig_classification(self):
        candidate = "0x1111111111111111111111111111111111111111"
        holder_rows = {
            candidate: {"is_contract": True, "contract_wallet_type": "safe"},
        }

        self.assertEqual(
            flows.holder_distribution_type(candidate, holder_rows, {}),
            "multisig",
        )

    def test_eip7702_delegated_account_is_not_classified_as_ca(self):
        candidate = "0x1111111111111111111111111111111111111111"
        holder_rows = {
            candidate: {
                "is_contract": True,
                "contract_wallet_type": "delegated_eoa",
            },
        }

        self.assertEqual(
            flows.holder_distribution_type(candidate, holder_rows, {}),
            "eoa",
        )

    def test_cex_supply_point_groups_canonical_exchanges(self):
        binance_numbered = "0x1111111111111111111111111111111111111111"
        binance_deposit = "0x2222222222222222222222222222222222222222"
        coinbase_hot = "0x3333333333333333333333333333333333333333"
        non_cex = "0x4444444444444444444444444444444444444444"
        point = flows.build_cex_supply_point(
            {
                binance_numbered: 100,
                binance_deposit: 50,
                coinbase_hot: 25,
                non_cex: 900,
            },
            {},
            {
                binance_numbered: {"label": "Binance 14", "type": "cex"},
                binance_deposit: {"label": "Binance Deposit", "type": "cex"},
                coinbase_hot: {"label": "Coinbase Hot Wallet", "type": "cex"},
                non_cex: {"label": "MEXC Wallet", "type": "protocol"},
            },
        )

        self.assertEqual(point["liquid"], 175.0)
        self.assertEqual(point["wallets"], 3)
        self.assertEqual(point["exchanges"], [
            {"name": "Binance", "liquid": 150.0, "wallets": 2},
            {"name": "Coinbase", "liquid": 25.0, "wallets": 1},
        ])

    def test_canonical_cex_name_groups_verified_real_world_variants(self):
        cases = [
            ("MEXC Wallet", "MEXC"),
            ("MEXC 16", "MEXC"),
            ("BingX-linked", "BingX"),
            ("BingX 29", "BingX"),
            ("Gate.io Routing Wallet", "Gate.io"),
            ("Gate.io Deposit", "Gate.io"),
            ("KuCoin Wallet", "KuCoin"),
            ("Coinbase Prime 1", "Coinbase Prime"),
            ("CEX Distributor", "CEX Distributor"),
        ]

        for label, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(flows.canonical_cex_name(label), expected)

    def test_cex_supply_point_reconciles_rounding_residual_to_name_tiebreak(self):
        alpha = "0x1111111111111111111111111111111111111111"
        beta = "0x2222222222222222222222222222222222222222"

        point = flows.build_cex_supply_point(
            {alpha: 10.004, beta: 10.004},
            {},
            {
                alpha: {"label": "Alpha 1", "type": "cex"},
                beta: {"label": "Beta 1", "type": "cex"},
            },
        )

        self.assertEqual(point, {
            "wallets": 2,
            "liquid": 20.01,
            "exchanges": [
                {"name": "Alpha", "liquid": 10.01, "wallets": 1},
                {"name": "Beta", "liquid": 10.0, "wallets": 1},
            ],
        })

    def test_cex_supply_point_negative_residual_never_makes_an_exchange_negative(self):
        addresses = {
            "Alpha": "0x1111111111111111111111111111111111111111",
            "Beta": "0x2222222222222222222222222222222222222222",
            "Gamma": "0x3333333333333333333333333333333333333333",
            "Delta": "0x4444444444444444444444444444444444444444",
        }

        point = flows.build_cex_supply_point(
            {address: 0.0051 for address in addresses.values()},
            {},
            {
                address: {"label": f"{name} 1", "type": "cex"}
                for name, address in addresses.items()
            },
        )

        self.assertEqual(point, {
            "wallets": 4,
            "liquid": 0.02,
            "exchanges": [
                {"name": "Delta", "liquid": 0.01, "wallets": 1},
                {"name": "Gamma", "liquid": 0.01, "wallets": 1},
                {"name": "Alpha", "liquid": 0.0, "wallets": 1},
                {"name": "Beta", "liquid": 0.0, "wallets": 1},
            ],
        })

    def test_holder_wallet_history_rows_include_safe_user_wallets(self):
        safe_wallet = "0x1111111111111111111111111111111111111111"
        cex_wallet = "0x2222222222222222222222222222222222222222"
        holder_rows = {
            safe_wallet: {"contract_wallet_type": "safe"},
            cex_wallet: {},
        }
        labels = {
            cex_wallet: {"label": "Exchange", "type": "cex"},
        }
        rows = flows.build_bucket_wallet_history_rows(
            {
                "eth": {safe_wallet: 600_000, cex_wallet: 2_000_000},
                "bera": {safe_wallet: 500_000},
            },
            {},
            holder_rows,
            labels,
            [{"key": "1mplus", "min": 1_000_000, "max": float("inf")}],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["address"], safe_wallet)
        self.assertEqual(rows[0]["balance"], 1_100_000)
        self.assertEqual(rows[0]["balance_eth"], 600_000)
        self.assertEqual(rows[0]["balance_bera"], 500_000)
        self.assertEqual(rows[0]["type"], "multisig")
        self.assertTrue(rows[0]["safe"])

    def test_historical_holder_chain_balances_reverse_net_flows(self):
        address = "0x1111111111111111111111111111111111111111"
        removed_address = "0x2222222222222222222222222222222222222222"
        by_chain = flows.historical_liquid_by_chain(
            {
                "eth": {address: 1_000_000, removed_address: 10},
                "bera": {address: 500_000},
            },
            {
                "eth": {address: 250_000, removed_address: 20},
                "bera": {address: -100_000},
            },
        )

        self.assertEqual(by_chain["eth"][address], 750_000)
        self.assertEqual(by_chain["bera"][address], 600_000)
        self.assertNotIn(removed_address, by_chain["eth"])

    def test_plain_contract_wallet_still_excluded_from_fresh_holder(self):
        candidate = "0x1111111111111111111111111111111111111111"
        source = "0x3333333333333333333333333333333333333333"
        all_transfers = {
            "eth": [
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
            period: {"eth": {candidate: 20_000}, "bera": {}}
            for period in flows.FRESH_HOLDER_PERIODS
        }

        with patch.object(flows, "load_current_holder_rows", return_value={
            candidate: {"is_contract": True},
        }), \
             patch.object(flows, "load_address_labels", return_value={}), \
             patch.object(flows, "load_current_vedolo_locks", return_value={}), \
             patch.object(flows, "wallet_first_activity") as first_activity:
            rows, _audit = flows.build_fresh_holders(
                all_transfers,
                cutoffs,
                current_blocks,
                neutralized,
                2_000_000_000,
                {},
            )

        self.assertEqual(rows["90d"], [])
        first_activity.assert_not_called()


if __name__ == "__main__":
    unittest.main()
