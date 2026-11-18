import copy
import tempfile
import unittest
from unittest.mock import patch

from eth_abi import encode

import operating_income_treasury as treasury


CORE = "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4"
DAO = "0x6e939bbaceb45159982a2cac3a1fcbf7e93cf682"
OTHER = "0x1111111111111111111111111111111111111111"
FEE = "0x4d5f0344d245f1d13607e5b61dd317de3b3178b8"
TOKEN = {"id": "0x2222222222222222222222222222222222222222", "marketId": "2", "symbol": "USDC", "decimals": "6"}
KEY = (CORE, "0", "2")
TX = "0x" + "a" * 64
HASH = "0x" + "b" * 64


def account(owner=CORE, number="0"):
    return {"id": owner + "-" + number, "accountNumber": number, "user": {"id": owner}}


def transfer(owner=CORE, number="0", delta="100", new_par=200_000_000):
    event = {
        "id": TX + "-4", "logIndex": "4", "serialId": "1",
        "transaction": {"id": TX, "timestamp": "150", "blockNumber": "15"},
        "fromMarginAccount": account(OTHER), "toMarginAccount": account(owner, number),
        "token": dict(TOKEN), "fromAmountDeltaPar": "-100", "toAmountDeltaPar": "100",
    }
    words = [0, int(number), 2, 0, 100_000_000, 0, 100_000_000, 1, int(delta) * 10**6, 1, new_par]
    log = {
        "address": OTHER, "transactionHash": TX, "blockHash": HASH, "blockNumber": "0xf",
        "transactionIndex": "0x0", "logIndex": "0x4", "removed": False,
        "topics": ["0x21281f8d59117d0399dc467dbdd321538ceffe3225e80e2bd4de6f1b3355cbc7",
                   "0x" + OTHER[2:].rjust(64, "0"), "0x" + owner[2:].rjust(64, "0")],
        "data": "0x" + "".join(f"{word:064x}" for word in words),
    }
    receipt = {"transactionHash": TX, "blockHash": HASH, "blockNumber": "0xf", "status": "0x1", "logs": [log]}
    return event, receipt


class TreasuryMathTests(unittest.TestCase):
    def rows(self, start, end, legs=(), price=10**30):
        return treasury._reconcile_rows({KEY: start}, {KEY: end}, list(legs), {"2": TOKEN}, {"2": price})

    def test_excludes_principal_and_preserves_amounts_above_float_precision(self):
        principal = 10**30 + 17
        rows = self.rows({"par": principal, "wei": principal},
                         {"par": principal + 100, "wei": principal + 107},
                         [{"owner": CORE, "account": "0", "market": "2", "deltaWei": 100, "deltaPar": 100, "newPar": principal + 100}])
        self.assertEqual("7", rows[0]["interestWei"])
        self.assertEqual("0.000007", rows[0]["netInterestUSD"])

    def test_signed_debt_cost_and_repayment_are_not_income(self):
        rows = self.rows({"par": -100, "wei": -110}, {"par": -50, "wei": -62},
                         [{"owner": CORE, "account": "0", "market": "2", "deltaWei": 55, "deltaPar": 50, "newPar": -50}])
        self.assertEqual("-7", rows[0]["interestWei"])

    def test_closed_position_retains_accrued_interest(self):
        rows = self.rows({"par": 100, "wei": 110}, {"par": 0, "wei": 0},
                         [{"owner": CORE, "account": "0", "market": "2", "deltaWei": -120, "deltaPar": -100, "newPar": 0}])
        self.assertEqual("10", rows[0]["interestWei"])

    def test_mismatched_intermediate_or_final_principal_fails_closed(self):
        for new_par, end_par in [(201, 200), (200, 201)]:
            with self.subTest(new_par=new_par, end_par=end_par), self.assertRaises(ValueError):
                self.rows({"par": 100, "wei": 100}, {"par": end_par, "wei": 210},
                          [{"owner": CORE, "account": "0", "market": "2", "deltaWei": 100, "deltaPar": 100, "newPar": new_par}])

    def test_missing_or_nonpositive_price_never_becomes_zero_income(self):
        for prices in [{}, {"2": 0}]:
            with self.subTest(prices=prices), self.assertRaises(ValueError):
                treasury._reconcile_rows({KEY: {"par": 100, "wei": 100}}, {KEY: {"par": 100, "wei": 110}}, [], {"2": TOKEN}, prices)


class TreasurySourceTests(unittest.TestCase):
    def test_pagination_rejects_cap_and_nonadvancing_pages(self):
        with self.assertRaises(ValueError):
            treasury._paginate(lambda cursor: [{"id": "a"}], page_size=1, max_pages=1)
        with self.assertRaises(ValueError):
            treasury._paginate(lambda cursor: [{"id": "a"}], page_size=1, max_pages=3)
        self.assertEqual([{"id": "a"}, {"id": "b"}], treasury._paginate(
            lambda cursor: [{"id": "a"}, {"id": "b"}] if not cursor else [], page_size=2, max_pages=2))

    def test_exact_boundary_excludes_every_block_at_cutoff(self):
        class Source:
            def rpc(self, method, params, **kwargs):
                number = int(params[0], 16)
                return {"number": hex(number), "timestamp": hex([0, 90, 100, 100, 110][number]), "hash": HASH}
        self.assertEqual(1, treasury._find_boundary(Source(), 100, 4)["number"])

    def test_receipts_prove_raw_owned_flow_and_large_subaccounts(self):
        number = str(2**255 + 17)
        event, receipt = transfer(number=number)
        legs = treasury._verified_legs({"transfers": [event]}, {TX: receipt}, OTHER, 100, 200)
        self.assertEqual(1, len(legs))
        self.assertEqual(number, legs[0]["account"])
        self.assertEqual(100_000_000, legs[0]["deltaWei"])
        self.assertEqual(100_000_000, legs[0]["deltaPar"])

    def test_duplicate_entities_do_not_double_count_but_conflicts_fail(self):
        event, receipt = transfer()
        self.assertEqual(1, len(treasury._verified_legs({"transfers": [event, copy.deepcopy(event)]}, {TX: receipt}, OTHER, 100, 200)))
        bad = copy.deepcopy(event)
        bad["toAmountDeltaPar"] = "101"
        with self.assertRaises(ValueError):
            treasury._verified_legs({"transfers": [event, bad]}, {TX: receipt}, OTHER, 100, 200)

    def test_missing_failed_wrong_block_or_missing_log_receipts_fail(self):
        event, receipt = transfer()
        broken = [{}, {TX: None}, {TX: {**receipt, "status": "0x0"}},
                  {TX: {**receipt, "blockNumber": "0x10"}}, {TX: {**receipt, "logs": []}}]
        for receipts in broken:
            with self.subTest(receipts=receipts), self.assertRaises(ValueError):
                treasury._verified_legs({"transfers": [event]}, receipts, OTHER, 100, 200)

    def test_source_event_on_exclusive_end_is_rejected(self):
        event, receipt = transfer()
        event["transaction"]["timestamp"] = "200"
        with self.assertRaises(ValueError):
            treasury._verified_legs({"transfers": [event]}, {TX: receipt}, OTHER, 100, 200)

    def test_receipt_log_must_belong_to_its_claimed_transaction_and_block(self):
        event, receipt = transfer()
        for field, wrong in [("transactionHash", "0x" + "c" * 64), ("blockNumber", "0x10")]:
            broken = copy.deepcopy(receipt)
            broken["logs"][0][field] = wrong
            with self.subTest(field=field), self.assertRaises(ValueError):
                treasury._verified_legs({"transfers": [event]}, {TX: broken}, OTHER, 100, 200)

    def test_fee_agent_is_not_treated_as_owned_treasury(self):
        event, receipt = transfer(owner=FEE)
        with self.assertRaises(ValueError):
            treasury._verified_legs({"transfers": [event]}, {TX: receipt}, OTHER, 100, 200)

    def test_immutable_rpc_cache_avoids_repeat_network_reads(self):
        with tempfile.TemporaryDirectory() as cache:
            source = treasury._Source("ethereum", {"chainId": 1, "subgraph": "https://example.invalid", "margin": OTHER},
                                      {"cacheDir": cache, "requestAttempts": 1})
            with patch.object(source, "_rpc_uncached", return_value={"value": "123"}) as network:
                self.assertEqual({"value": "123"}, source.rpc("eth_getTransactionReceipt", [TX], immutable=True))
                self.assertEqual({"value": "123"}, source.rpc("eth_getTransactionReceipt", [TX], immutable=True))
            self.assertEqual(1, network.call_count)


class FakeSource:
    """Controlled external RPC/subgraph responses; collector logic stays real."""
    stale = False
    missing_receipt = False
    chain_id = "0x1"
    rpc_head_hash = HASH
    deployment_changed = False
    token_count = 1
    historical_metadata_omitted = False
    indexing_errors = False

    def __init__(self, chain, config, options):
        self.config = config
        self.meta_calls = 0

    def graphql(self, query):
        if "_meta" in query:
            self.meta_calls += 1
            deployment = "changed" if self.deployment_changed and self.meta_calls > 1 else "original"
            block = {"number": 30, "timestamp": 300 if not self.stale else 1, "hash": HASH}
            if self.historical_metadata_omitted and "_meta(block:" in query:
                block.update(hash=None, timestamp=None)
            return {"_meta": {"deployment": deployment, "block": block, "hasIndexingErrors": self.indexing_errors}}
        import re
        entity = re.search(r"\{(\w+)\(", query).group(1)
        if entity == "marginAccounts":
            start = "block:{number:9}" in query
            return {entity: [{**account(), "tokenValues": [{"valuePar": "100" if start else "200", "token": TOKEN}] * self.token_count}]}
        return {entity: [transfer()[0]] if entity == "transfers" else []}

    def rpc(self, method, params, **kwargs):
        if method == "eth_chainId":
            return self.chain_id
        if method == "eth_getTransactionReceipt":
            return None if self.missing_receipt else transfer()[1]
        number = int(params[0], 16)
        return {"number": hex(number), "timestamp": hex(number * 10), "hash": self.rpc_head_hash if number == 30 else HASH}

    def calls(self, calls, block):
        values = []
        for _, calldata in calls:
            selector = calldata[:10]
            if selector == treasury._encode("getMarketPrice(uint256)", ["uint256"], [2])[:10]:
                values.append("0x" + encode(["uint256"], [10**30]).hex())
            else:
                par_selector = treasury._encode("getAccountPar((address,uint256),uint256)", ["(address,uint256)", "uint256"], [(CORE, 0), 2])[:10]
                is_par = selector == par_selector
                amount = (100 if block == 9 else 200) if is_par else (110 if block == 9 else 230)
                values.append("0x" + encode(["bool", "uint128" if is_par else "uint256"], [True, amount * 10**6]).hex())
        return values


class TreasuryCollectorTests(unittest.TestCase):
    config = {"ethereum": {"chainId": 1, "margin": OTHER, "subgraph": "https://example.invalid"}}

    def collect(self):
        with patch.object(treasury, "_Source", FakeSource), patch.object(treasury.time, "time", return_value=300):
            return treasury.collect_treasury_income(self.config, 100, 200, options={"maxSourceAgeSeconds": 100})

    def test_collects_pinned_receipt_proven_income_without_partner_haircut(self):
        result = self.collect()
        self.assertTrue(result["complete"])
        self.assertEqual({"startTimestamp": 100, "endTimestamp": 200}, result["period"])
        self.assertEqual("20", result["chains"]["ethereum"]["netInterestUSD"])
        self.assertFalse(result["partnerShareApplied"])
        self.assertEqual([CORE, DAO], [wallet["address"] for wallet in result["chains"]["ethereum"]["wallets"]])

    def test_stale_source_rpc_chain_mismatch_and_missing_receipt_fail(self):
        for field, value in [("stale", True), ("chain_id", "0x2"), ("missing_receipt", True)]:
            with self.subTest(field=field), patch.object(FakeSource, field, value), self.assertRaises(ValueError):
                self.collect()

    def test_invalid_period_empty_chains_or_pagination_limit_rejected(self):
        for config, start, end, options in [({}, 100, 200, {}), (self.config, 200, 100, {}),
                                            (self.config, 100, 200, {"pageSize": 1001})]:
            with self.subTest(config=config, start=start), self.assertRaises(ValueError):
                treasury.collect_treasury_income(config, start, end, options=options)

    def test_head_fork_deployment_change_and_nested_token_cap_fail_closed(self):
        for field, value in [("rpc_head_hash", "0x" + "c" * 64), ("deployment_changed", True), ("token_count", 1000)]:
            with self.subTest(field=field), patch.object(FakeSource, field, value), self.assertRaises(ValueError):
                self.collect()

    def test_historical_metadata_omissions_use_independently_verified_rpc_pin(self):
        with patch.object(FakeSource, "historical_metadata_omitted", True):
            self.assertEqual("20", self.collect()["chains"]["ethereum"]["netInterestUSD"])

    def test_unknown_indexing_health_is_not_complete(self):
        with patch.object(FakeSource, "indexing_errors", None), self.assertRaises(ValueError):
            self.collect()


if __name__ == "__main__":
    unittest.main()
