import copy
import unittest

import validate_data


def valid_cex_history_payload():
    return {
        "cex_supply_history": [
            {
                "key": "hist_20260814",
                "timestamp": "2026-08-14T00:00:00Z",
                "liquid": 30.01,
                "wallets": 3,
                "exchanges": [
                    {"name": "Alpha", "liquid": 20.0, "wallets": 2},
                    {"name": "Beta", "liquid": 10.01, "wallets": 1},
                ],
            }
        ]
    }


class ValidateDoloFlowsTest(unittest.TestCase):
    @staticmethod
    def _reconciliation_payload():
        periods = ("1d", "7d", "30d", "90d", "180d", "all")
        empty = {"accumulators": [], "sellers": [], "total_transfers": 0}
        return {
            "schemaVersion": 3,
            "tracked_flow_chains": ["ethereum", "berachain"],
            "periods": {
                period: {
                    "eth": copy.deepcopy(empty),
                    "bera": copy.deepcopy(empty),
                    "all": copy.deepcopy(empty),
                }
                for period in periods
            },
            "period_boundaries": {
                chain: {
                    period: {
                        "targetTimestamp": 1_000,
                        "startBlock": 100,
                        "startTimestamp": 1_001,
                        "endBlock": 200,
                        "endTimestamp": 2_000,
                    }
                    for period in periods
                }
                for chain in ("eth", "bera")
            },
            "bridge_neutralization_audit": {
                period: {
                    "canonicalAdapter": {"addressCount": 1, "dolo": 10.0},
                    "legacyHeuristic": {"addressCount": 2, "dolo": 5.0},
                    "total": {"addressCount": 3, "dolo": 15.0},
                }
                for period in periods
            },
        }

    def test_v3_combined_flow_contract_passes_when_complete(self):
        self.assertTrue(
            validate_data._flow_reconciliation_v3_is_valid(
                self._reconciliation_payload()
            )
        )

    def test_v4_search_rows_allow_complete_address_lookup_beyond_top_100(self):
        payload = self._reconciliation_payload()
        payload["schemaVersion"] = 4
        searched = "0xa3aef439e6b69125cdbfd946ab1d8a9d012e1c46"
        for period_data in payload["periods"].values():
            for scope in ("eth", "bera", "all"):
                period_data[scope]["search_accumulators"] = [
                    {
                        "address": searched,
                        "net_flow": 24_678.68,
                        "tx_count": 2,
                        "gross_inflow": 25_000.0,
                        "gross_outflow": 321.32,
                        "protocol_deposit": 0.0,
                        "protocol_withdrawal": 0.0,
                    }
                ]
                period_data[scope]["search_sellers"] = []

        self.assertTrue(validate_data._flow_reconciliation_v3_is_valid(payload))

    def test_v5_rejects_directional_components_that_do_not_equal_net_flow(self):
        payload = self._reconciliation_payload()
        payload["schemaVersion"] = 5
        payload["timestamp"] = "1970-01-01T00:33:20Z"
        payload["generatedAt"] = "1970-01-01T00:34:00Z"
        for period, period_data in payload["periods"].items():
            for scope in ("eth", "bera", "all"):
                period_data[scope]["search_accumulators"] = []
                period_data[scope]["search_sellers"] = []
            payload["bridge_neutralization_audit"][period]["legacyHeuristic"] = {
                "addressCount": 0,
                "dolo": 0.0,
            }
            payload["bridge_neutralization_audit"][period]["legacyHeuristicObserved"] = {
                "addressCount": 0,
                "dolo": 0.0,
            }
            payload["bridge_neutralization_audit"][period]["total"] = {
                "addressCount": 1,
                "dolo": 10.0,
            }
        payload["periods"]["7d"]["eth"]["accumulators"] = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "net_flow": 15.0,
                "gross_inflow": 20.0,
                "gross_outflow": 5.0,
                "protocol_deposit": 0.0,
                "protocol_withdrawal": 0.0,
            }
        ]
        payload["periods"]["7d"]["eth"]["search_accumulators"] = copy.deepcopy(
            payload["periods"]["7d"]["eth"]["accumulators"]
        )

        self.assertTrue(validate_data._flow_reconciliation_v3_is_valid(payload))
        payload["periods"]["7d"]["eth"]["accumulators"][0]["net_flow"] = 10.0
        payload["periods"]["7d"]["eth"]["search_accumulators"][0]["net_flow"] = 10.0
        self.assertFalse(validate_data._flow_reconciliation_v3_is_valid(payload))

    def test_v5_requires_data_timestamp_to_match_oldest_chain_head(self):
        payload = self._reconciliation_payload()
        payload["schemaVersion"] = 5
        payload["timestamp"] = "1970-01-01T00:33:20Z"
        payload["generatedAt"] = "1970-01-01T00:34:00Z"
        for period, period_data in payload["periods"].items():
            for scope in ("eth", "bera", "all"):
                period_data[scope]["search_accumulators"] = []
                period_data[scope]["search_sellers"] = []
            payload["bridge_neutralization_audit"][period]["legacyHeuristic"] = {
                "addressCount": 0,
                "dolo": 0.0,
            }
            payload["bridge_neutralization_audit"][period]["legacyHeuristicObserved"] = {
                "addressCount": 0,
                "dolo": 0.0,
            }
            payload["bridge_neutralization_audit"][period]["total"] = {
                "addressCount": 1,
                "dolo": 10.0,
            }

        self.assertTrue(validate_data._flow_reconciliation_v3_is_valid(payload))
        payload["timestamp"] = "1970-01-01T00:34:00Z"
        self.assertFalse(validate_data._flow_reconciliation_v3_is_valid(payload))

    def test_v6_requires_complete_pinned_dolomite_trade_coverage(self):
        payload = self._reconciliation_payload()
        payload["schemaVersion"] = 6
        payload["timestamp"] = "1970-01-01T00:33:20Z"
        payload["generatedAt"] = "1970-01-01T00:34:00Z"
        for period, period_data in payload["periods"].items():
            for scope in ("eth", "bera", "all"):
                period_data[scope]["search_accumulators"] = []
                period_data[scope]["search_sellers"] = []
            payload["bridge_neutralization_audit"][period]["legacyHeuristic"] = {
                "addressCount": 0,
                "dolo": 0.0,
            }
            payload["bridge_neutralization_audit"][period]["legacyHeuristicObserved"] = {
                "addressCount": 0,
                "dolo": 0.0,
            }
            payload["bridge_neutralization_audit"][period]["total"] = {
                "addressCount": 1,
                "dolo": 10.0,
            }
        payload["dolomite_trade_meta"] = {
            "status": "complete",
            "source": "official-dolomite-subgraph-pinned-block",
            "chains": {
                chain: {
                    "status": "complete",
                    "source": "official-dolomite-subgraph-pinned-block",
                    "blockNumber": 200,
                    "blockTimestamp": 2_000,
                    "eventCount": 0,
                }
                for chain in ("eth", "bera")
            },
        }

        self.assertTrue(validate_data._flow_reconciliation_v3_is_valid(payload))
        payload["dolomite_trade_meta"]["chains"]["bera"]["blockNumber"] = 199
        self.assertFalse(validate_data._flow_reconciliation_v3_is_valid(payload))

    def test_v3_combined_flow_contract_rejects_missing_combined_rows(self):
        payload = self._reconciliation_payload()
        payload["periods"]["180d"].pop("all")

        self.assertFalse(validate_data._flow_reconciliation_v3_is_valid(payload))

    def test_v3_combined_flow_contract_rejects_bad_boundaries_and_audit_totals(self):
        payload = self._reconciliation_payload()
        payload["period_boundaries"]["eth"]["180d"]["startTimestamp"] = 999
        self.assertFalse(validate_data._flow_reconciliation_v3_is_valid(payload))

        payload = self._reconciliation_payload()
        payload["bridge_neutralization_audit"]["7d"]["total"]["dolo"] = 16.0
        self.assertFalse(validate_data._flow_reconciliation_v3_is_valid(payload))

    def test_dolo_flow_rule_registers_v3_reconciliation_guard(self):
        self.assertIn(
            "v3-v6 combined flow rows, complete search index, exact boundaries, bridge/trade audit and freshness must reconcile",
            dict(validate_data.RULES["dolo_flows.json"]["checks"]),
        )

    @staticmethod
    def _check(payload):
        checks = dict(validate_data.RULES["dolo_flows.json"]["checks"])
        return checks["CEX exchange history must reconcile exactly"](payload)

    def test_cex_exchange_history_is_required_by_the_dolo_flows_rule(self):
        rules = validate_data.RULES["dolo_flows.json"]

        self.assertIn("cex_supply_history", rules["required_keys"])
        self.assertIn("CEX exchange history must reconcile exactly", dict(rules["checks"]))

    def test_valid_cex_exchange_history_passes(self):
        self.assertTrue(self._check(valid_cex_history_payload()))

    def test_positive_cex_supply_requires_nonempty_exchange_rows(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"] = []

        self.assertFalse(self._check(payload))

    def test_cex_exchange_rows_require_unique_nonempty_names(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"][1]["name"] = "Alpha"
        self.assertFalse(self._check(payload))

        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"][0]["name"] = ""
        self.assertFalse(self._check(payload))

    def test_cex_exchange_rows_reject_nonfinite_negative_and_boolean_fields(self):
        malformed_rows = [
            {"name": "Alpha", "liquid": float("nan"), "wallets": 2},
            {"name": "Alpha", "liquid": -0.01, "wallets": 2},
            {"name": "Alpha", "liquid": 20.0, "wallets": True},
            {"name": "Alpha", "liquid": 20.0, "wallets": -1},
        ]
        for row in malformed_rows:
            with self.subTest(row=row):
                payload = valid_cex_history_payload()
                payload["cex_supply_history"][0]["exchanges"][0] = row
                self.assertFalse(self._check(payload))

    def test_cex_exchange_rows_must_be_sorted_by_balance_then_name(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["exchanges"].reverse()

        self.assertFalse(self._check(payload))

    def test_cex_exchange_balances_must_match_point_total_in_cents(self):
        payload = valid_cex_history_payload()
        payload["cex_supply_history"][0]["liquid"] = 30.02

        self.assertFalse(self._check(payload))

    def test_cex_exchange_wallets_must_match_point_wallet_count(self):
        payload = copy.deepcopy(valid_cex_history_payload())
        payload["cex_supply_history"][0]["wallets"] = 4

        self.assertFalse(self._check(payload))

    def test_optional_flow_transaction_metadata_is_all_or_none_and_exact(self):
        row = {
            "address": "0x" + "1" * 40,
            "latest_tx_hash": "0x" + "a" * 64,
            "latest_tx_timestamp": 1_786_406_400,
            "latest_tx_chain": "ethereum",
        }
        payload = {"periods": {"7d": {"eth": {"accumulators": [row], "sellers": []}}}}
        self.assertTrue(validate_data._flow_tx_metadata_is_valid(payload))

        for key, invalid in (
            ("latest_tx_hash", "0x1234"),
            ("latest_tx_timestamp", True),
            ("latest_tx_chain", "arbitrum"),
        ):
            with self.subTest(key=key):
                malformed = copy.deepcopy(payload)
                malformed["periods"]["7d"]["eth"]["accumulators"][0][key] = invalid
                self.assertFalse(validate_data._flow_tx_metadata_is_valid(malformed))

        partial = copy.deepcopy(payload)
        partial["periods"]["7d"]["eth"]["accumulators"][0].pop("latest_tx_timestamp")
        self.assertFalse(validate_data._flow_tx_metadata_is_valid(partial))

    def test_dolo_flow_rule_registers_transaction_metadata_guard(self):
        self.assertIn(
            "optional latest transaction metadata must be exact",
            dict(validate_data.RULES["dolo_flows.json"]["checks"]),
        )

    def test_optional_lp_activity_metadata_is_verified_and_transaction_bound(self):
        tx_hash = "0x" + "a" * 64
        row = {
            "address": "0x" + "1" * 40,
            "latest_tx_hash": tx_hash,
            "latest_tx_timestamp": 1_786_406_400,
            "latest_tx_chain": "ethereum",
            "latest_lp_activity": {
                "direction": "deposit",
                "amount": "1304943.547531365190891539",
                "pair": "DOLO/USDC",
                "adapter": "uniswap-v4",
                "confidence": "verified_same_tx",
                "tx_hash": tx_hash,
            },
        }
        payload = {"periods": {"7d": {"eth": {"accumulators": [], "sellers": [row]}}}}
        self.assertTrue(validate_data._flow_lp_metadata_is_valid(payload))

        for field, invalid in (
            ("direction", "swap"),
            ("amount", "-1"),
            ("pair", ""),
            ("adapter", ""),
            ("confidence", "inferred"),
            ("tx_hash", "0x" + "b" * 64),
        ):
            with self.subTest(field=field):
                malformed = copy.deepcopy(payload)
                malformed["periods"]["7d"]["eth"]["sellers"][0]["latest_lp_activity"][field] = invalid
                self.assertFalse(validate_data._flow_lp_metadata_is_valid(malformed))

    def test_dolo_flow_rule_registers_lp_activity_guard(self):
        self.assertIn(
            "optional LP activity metadata must be exact",
            dict(validate_data.RULES["dolo_flows.json"]["checks"]),
        )

    @staticmethod
    def _dolomite_balance_payload(status="complete"):
        address = "0x" + "1" * 40
        balances = {
            address: {"total": 20.75, "eth": 18.75, "bera": 2.0},
        }
        meta = {
            "status": status,
            "failedChains": [],
            "chains": {
                "eth": {"blockNumber": 1, "blockTimestamp": 1_786_406_400, "matchedWallets": 1, "custodyAddress": "0x" + "a" * 40},
                "bera": {"blockNumber": 2, "blockTimestamp": 1_786_406_401, "matchedWallets": 1, "custodyAddress": "0x" + "a" * 40},
            },
            "scope": "all-positive-effective-users",
        }
        if status == "unavailable":
            balances = {}
            meta.update({"failedChains": ["bera"], "chains": {"eth": meta["chains"]["eth"]}})
        return {
            "dolomite_balance_meta": meta,
            "dolomite_balances": balances,
            "periods": {"7d": {"all": {"accumulators": [{"address": address}], "sellers": []}}},
        }

    def test_dolomite_protocol_balance_snapshot_reconciles(self):
        self.assertTrue(
            validate_data._flow_dolomite_balances_are_valid(
                self._dolomite_balance_payload()
            )
        )

    def test_complete_dolomite_snapshot_requires_verified_custody_addresses(self):
        payload = self._dolomite_balance_payload()
        payload["dolomite_balance_meta"]["chains"]["eth"].pop("custodyAddress")

        self.assertFalse(validate_data._flow_dolomite_balances_are_valid(payload))

    def test_dolomite_protocol_balance_rejects_partial_or_invalid_amounts(self):
        for key, value in (
            ("dolomite_balance", 20.74),
            ("dolomite_balance_eth", -1),
            ("dolomite_balance_bera", float("nan")),
            ("dolomite_balance", True),
        ):
            with self.subTest(key=key, value=value):
                payload = self._dolomite_balance_payload()
                address = "0x" + "1" * 40
                payload["dolomite_balances"][address][key.replace("dolomite_balance_", "").replace("dolomite_balance", "total")] = value
                self.assertFalse(validate_data._flow_dolomite_balances_are_valid(payload))

    def test_unavailable_dolomite_snapshot_cannot_publish_partial_balances(self):
        payload = self._dolomite_balance_payload("unavailable")
        self.assertTrue(validate_data._flow_dolomite_balances_are_valid(payload))

        payload["dolomite_balances"]["0x" + "1" * 40] = {"total": 18.75, "eth": 18.75, "bera": 0}
        self.assertFalse(validate_data._flow_dolomite_balances_are_valid(payload))

    def test_dolo_flow_rule_requires_and_validates_dolomite_balance_snapshot(self):
        rules = validate_data.RULES["dolo_flows.json"]

        self.assertIn("dolomite_balance_meta", rules["required_keys"])
        self.assertIn("dolomite_balances", rules["required_keys"])
        self.assertIn(
            "Dolomite DOLO balances must be complete and reconcile",
            dict(rules["checks"]),
        )

    def test_holder_total_exposure_history_requires_complete_sources_and_current_point(self):
        def model(protocol=0):
            return {
                "trackedTotal": 100 + protocol,
                "trackedLiquid": 100,
                "trackedProtocol": protocol,
                "trackedLocked": 0,
                "buckets": [],
            }

        def point(key, timestamp):
            return {
                "key": key,
                "timestamp": timestamp,
                "liquid": {"holders": {"whales": model()}},
                "with_vedolo": {"holders": {"whales": model()}},
                "total_exposure": {"holders": {"whales": model(20)}},
                "total_exposure_with_vedolo": {"holders": {"whales": model(20)}},
            }

        payload = {
            "holder_history_schema": "audience-exposure-v3",
            "holder_dolomite_history_meta": {
                "status": "complete",
                "schemaVersion": 1,
                "pointCount": 1,
                "chainCount": 2,
                "chains": ["eth", "bera"],
            },
            "holder_history_points": [
                {"key": "hist_20260830", "timestamp": "2026-08-30T00:00:00Z"},
                {"key": "now", "timestamp": "2026-08-31T00:00:00Z"},
            ],
            "holder_bucket_history": [
                point("hist_20260830", "2026-08-30T00:00:00Z"),
                point("now", "2026-08-31T00:00:00Z"),
            ],
        }

        self.assertTrue(validate_data._holder_dolomite_exposure_history_is_valid(payload))
        payload["holder_bucket_history"][0].pop("total_exposure")
        self.assertFalse(validate_data._holder_dolomite_exposure_history_is_valid(payload))
        self.assertIn(
            "Holder total-exposure history must have complete Dolomite coverage",
            dict(validate_data.RULES["dolo_flows.json"]["checks"]),
        )

    def test_flow_history_integrity_requires_verified_full_chain_coverage(self):
        payload = {
            "flow_history_integrity": {
                "version": 2,
                "status": "complete",
                "verification": "independent-rpc-exact-quorum",
                "unresolvedGapCount": 0,
                "chains": {
                    "eth": {
                        "coverageStartBlock": 21_500_000,
                        "deployBlock": 21_500_000,
                        "verifiedThroughBlock": 25_500_000,
                        "lastPublishedBlock": 25_500_000,
                        "verification": "independent-rpc-exact-quorum",
                        "lastVerificationProof": {
                            "startBlock": 25_450_000,
                            "endBlock": 25_500_000,
                            "verifiedChunkCount": 2,
                            "minimumMatchingProviderFamilies": 2,
                            "providerFamilies": ["alchemy.com", "drpc.org"],
                        },
                    },
                    "bera": {
                        "coverageStartBlock": 2_900_000,
                        "deployBlock": 2_900_000,
                        "verifiedThroughBlock": 25_200_000,
                        "lastPublishedBlock": 25_200_000,
                        "verification": "independent-rpc-exact-quorum",
                        "lastVerificationProof": {
                            "startBlock": 25_100_000,
                            "endBlock": 25_200_000,
                            "verifiedChunkCount": 1,
                            "minimumMatchingProviderFamilies": 2,
                            "providerFamilies": ["berachain.com", "drpc.org"],
                        },
                    },
                },
            }
        }

        self.assertTrue(validate_data._flow_history_integrity_is_valid(payload))

        payload["flow_history_integrity"]["chains"]["bera"]["coverageStartBlock"] += 1
        self.assertFalse(validate_data._flow_history_integrity_is_valid(payload))

    def test_dolo_flow_rule_registers_verified_history_guard(self):
        rules = validate_data.RULES["dolo_flows.json"]

        self.assertIn("flow_history_integrity", rules["required_keys"])
        self.assertIn(
            "Transfer history must have independent RPC quorum coverage",
            dict(rules["checks"]),
        )

    def test_holder_wallet_history_has_a_pre_push_blob_size_guard(self):
        rules = validate_data.RULES["dolo_holder_wallet_history.json"]

        self.assertLess(rules["max_bytes"], 100_000_000)


if __name__ == "__main__":
    unittest.main()
