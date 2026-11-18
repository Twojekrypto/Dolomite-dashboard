import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from eth_abi import encode

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import wlfi_markets as wlfi
import generate_wlfi_markets as producer


TOKEN = '0x' + 'a' * 40
OWNER = '0x' + 'b' * 40
NOW = datetime(2026, 11, 18, 5, tzinfo=timezone.utc)


def evidence():
    number = str(wlfi.MINIMUM)
    account_id = OWNER + '-' + number
    head = {'number': 123, 'hash': '0x' + 'c' * 64, 'timestamp': int(NOW.timestamp())}
    state = [{'id': TOKEN, 'marketId': '0', 'symbol': 'USDC', 'decimals': '6',
              'totalPar': {'supplyPar': '10', 'borrowPar': '2'}}]
    indexed = [{'id': account_id, 'accountNumber': number, 'user': {'id': OWNER},
                'effectiveUser': {'id': OWNER},
                'tokenValues': [{'token': {'id': TOKEN}, 'valuePar': '1.000001'}]}]
    markets = [{'token': TOKEN, 'marketId': '0', 'supplyIndex': '1500000000000000000',
                'borrowIndex': '2000000000000000000', 'price': str(10**30),
                'supplyPar': '10000000', 'borrowPar': '2000000',
                'supplyWei': '15000000', 'borrowWei': '4000000'}]
    accounts = [{'id': account_id, 'owner': OWNER, 'accountNumber': number,
                 'balances': [{'marketId': '0', 'token': TOKEN, 'par': '1000001', 'wei': '1500002'}]}]
    return head, state, indexed, markets, accounts


class ClassificationTests(unittest.TestCase):
    def test_inclusive_ethereum_range_without_float_rounding(self):
        for value in (wlfi.MINIMUM, wlfi.MAXIMUM):
            self.assertEqual(wlfi.account_origin('ethereum', str(value)), 'wlfi')
        for value in (wlfi.MINIMUM - 1, wlfi.MAXIMUM + 1, 0):
            self.assertEqual(wlfi.account_origin('eth', str(value)), 'other')

    def test_missing_malformed_and_owner_only_are_unknown(self):
        for value in (None, '', '1.0', '-1', 1.0, True, '0x47', str(2**256),
                      {'owner': OWNER}, OWNER, OWNER + '-garbage'):
            self.assertEqual(wlfi.account_origin('ethereum', value), 'unknown', repr(value))
        self.assertEqual(wlfi.account_origin('unknown', str(wlfi.MINIMUM)), 'unknown')

    def test_unbounded_decimal_input_remains_unknown(self):
        self.assertEqual(wlfi.account_origin('ethereum', '9' * 5000), 'unknown')

    def test_valid_other_chains_and_actual_account_ids(self):
        self.assertEqual(wlfi.account_origin('arbitrum', None), 'other')
        self.assertEqual(wlfi.account_origin(80094, str(wlfi.MINIMUM)), 'other')
        self.assertEqual(wlfi.account_origin(1, {'account': OWNER + '-' + str(wlfi.MINIMUM)}), 'wlfi')
        self.assertIsNone(wlfi.account_number({'accountNumber': None, 'id': OWNER + '-' + str(wlfi.MINIMUM)}))


class SnapshotTests(unittest.TestCase):
    def build(self, values=None):
        return producer.build_snapshot(*(values or evidence()), now=NOW, max_source_age_seconds=1800)

    def test_precise_wei_half_up_current_index_and_same_block_denominator(self):
        snapshot = self.build()
        self.assertEqual(snapshot['schemaVersion'], 1)
        self.assertEqual(snapshot['source']['blockHash'], '0x' + 'c' * 64)
        self.assertEqual(snapshot['totals']['supplyUsd'], '1.500002')
        self.assertEqual(snapshot['totals']['netTvlUsd'], '1.500002')
        self.assertEqual(snapshot['totals']['borrowedUsd'], '0')
        self.assertEqual(snapshot['markets'][0]['protocolSupplyUsd'], '15')
        self.assertEqual(snapshot['markets'][0]['supplierWallets'], 1)
        self.assertEqual(snapshot['totals']['activeWallets'], 1)

    def test_missing_partial_duplicate_par_and_wei_mismatches_fail_closed(self):
        for mutate in (
            lambda x: x[4].clear(),
            lambda x: x[3].clear(),
            lambda x: x[4].append(copy.deepcopy(x[4][0])),
            lambda x: x[4][0]['balances'][0].update(par='1000000'),
            lambda x: x[4][0]['balances'][0].update(wei='1500001'),
            lambda x: x[3][0].update(supplyPar='9999999'),
            lambda x: x[3][0].update(price='0'),
            lambda x: x[2][0].update(accountNumber='0'),
            lambda x: x[0].update(timestamp=int(NOW.timestamp()) - 1801),
        ):
            values = evidence()
            mutate(values)
            with self.assertRaises(ValueError):
                self.build(values)

    def test_borrow_debt_and_distinct_effective_wallet_count(self):
        values = evidence()
        second = copy.deepcopy(values[2][0]); second['accountNumber'] = str(wlfi.MINIMUM + 1)
        second['id'] = OWNER + '-' + second['accountNumber']; second['tokenValues'][0]['valuePar'] = '-0.5'
        values[2].append(second)
        values[4].append({'id': second['id'], 'owner': OWNER, 'accountNumber': second['accountNumber'],
                          'balances': [{'marketId': '0', 'token': TOKEN, 'par': '-500000', 'wei': '-1000000'}]})
        snapshot = self.build(values)
        self.assertEqual(snapshot['totals']['borrowedUsd'], '1')
        self.assertEqual(snapshot['totals']['netTvlUsd'], '0.500002')
        self.assertEqual(snapshot['totals']['activeWallets'], 1)
        self.assertEqual(snapshot['markets'][0]['borrowerWallets'], 1)

    def test_atomic_writer_preserves_previous_good_on_validation_failure(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'latest.json'; path.write_text('previous good\n')
            with self.assertRaises(ValueError):
                producer.write_snapshot(path, {'schemaVersion': 1})
            self.assertEqual(path.read_text(), 'previous good\n')
            producer.write_snapshot(path, self.build())
            self.assertEqual(json.loads(path.read_text())['schemaVersion'], 1)


class RpcCompletenessTests(unittest.TestCase):
    def test_failed_and_truncated_multicalls_are_rejected(self):
        config = json.loads((Path(producer.__file__).parent / 'config/wlfi_markets.json').read_text())
        class Rpc:
            def __init__(self, entries):
                self.entries = entries
            def call(self, method, params):
                return '0x' + encode(['(bool,bytes)[]'], [self.entries]).hex()
        for entries in ([], [(False, b'')], [(True, b'')]):
            reader = producer.SnapshotReader(config, rpc=Rpc(entries))
            with self.assertRaises(ValueError):
                reader.multicall([b'1234'], 123)

    def test_unhealthy_subgraph_fails_before_any_chain_read(self):
        config = json.loads((Path(producer.__file__).parent / 'config/wlfi_markets.json').read_text())
        class Reader(producer.SnapshotReader):
            def graph(self, query):
                return {'_meta': {'hasIndexingErrors': True}}
        with self.assertRaisesRegex(ValueError, 'Unhealthy'):
            Reader(config, rpc=object()).collect()


if __name__ == '__main__':
    unittest.main()
