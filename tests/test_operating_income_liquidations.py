"""Protocol rake totals must preserve exact dollars and source completeness."""
import copy
import importlib
import importlib.util
import unittest
from unittest.mock import patch


MODULE = 'operating_income_liquidations'
COLLECTOR = importlib.import_module(MODULE) if importlib.util.find_spec(MODULE) else None
TX = '0x' + 'a' * 64
OWNER = '0x' + '1' * 40
TOKEN = '0x' + '2' * 40
AGENT = '0x4d5f0344d245f1d13607e5b61dd317de3b3178b8'
START, END, NOW = 1792368000, 1794960000, 1794960200
BLOCK = 666666666
HASH = '0x' + 'b' * 64


def liquidation(index=10, *, tx=TX, timestamp=START, token=TOKEN, owner=OWNER, account='0'):
    return {'id': tx + '-' + str(index), 'logIndex': str(index), 'serialId': str(index),
            'transaction': {'id': tx, 'timestamp': str(timestamp), 'blockNumber': str(BLOCK)},
            'heldToken': {'id': token, 'marketId': '1', 'decimals': '18'},
            'heldTokenAmountDeltaWei': '10', 'heldTokenAmountUSD': '1',
            'solidMarginAccount': {'accountNumber': account, 'user': {'id': owner}}}


def transfer(index=11, *, tx=TX, timestamp=START, usd='1.000000000000000001', amount='0.5', token=TOKEN, owner=OWNER, account='0'):
    return {'id': tx + '-' + str(index), 'logIndex': str(index), 'serialId': str(index),
            'transaction': {'id': tx, 'timestamp': str(timestamp), 'blockNumber': str(BLOCK)},
            'token': {'id': token, 'marketId': '1', 'decimals': '18'},
            'fromMarginAccount': {'accountNumber': account, 'user': {'id': owner}},
            'toMarginAccount': {'accountNumber': '0', 'user': {'id': AGENT}},
            'toEffectiveUser': {'id': AGENT}, 'amountDeltaWei': amount, 'amountUSDDeltaWei': usd}


class CollectorTest(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(COLLECTOR, 'The new protocol liquidation income collector is missing')


class ReducerTests(CollectorTest):
    def reduce(self, liquidations, transfers):
        return COLLECTOR.summarize_fee_transfers('ethereum', liquidations, transfers, START, END)

    def test_keeps_gross_ethereum_fee_exact_without_partner_cut(self):
        result = self.reduce([liquidation(), liquidation(20)], [transfer(), transfer(21, usd='2.999999999999999999')])
        self.assertEqual(result['grossProtocolFeeUSD'], '4')
        self.assertEqual(result['feeTransferCount'], 2)
        self.assertFalse(result['valuation']['independentReceiptAudit'])

    def test_global_multirow_matching_counts_each_transfer_once(self):
        liqs = [liquidation(12), liquidation(10)]
        fees = [transfer(13, usd='7'), transfer(11, usd='2')]
        result = self.reduce(liqs + [copy.deepcopy(liqs[0])], fees + [copy.deepcopy(fees[0])])
        self.assertEqual(result['grossProtocolFeeUSD'], '9')
        self.assertEqual(result['feeTransferCount'], 2)
        self.assertEqual(result['duplicateEventsRemoved'], 2)

    def test_zero_is_only_known_from_a_present_numeric_source_amount(self):
        result = self.reduce([liquidation()], [transfer(usd='0', amount='0.000000000000000003')])
        self.assertEqual(result['grossProtocolFeeUSD'], '0')
        self.assertEqual(result['zeroUSDTransferCount'], 1)
        self.assertEqual(self.reduce([liquidation()], [])['liquidationsWithoutFeeTransferCount'], 1)
        bad = transfer(); del bad['amountUSDDeltaWei']
        with self.assertRaises(ValueError): self.reduce([liquidation()], [bad])

    def test_invalid_decimal_or_fractional_raw_wei_never_becomes_zero(self):
        for usd in ['NaN', 'Infinity', '-1', '', None, 0.5]:
            with self.subTest(usd=usd), self.assertRaises(ValueError):
                self.reduce([liquidation()], [transfer(usd=usd)])
        with self.assertRaises(ValueError):
            self.reduce([liquidation()], [transfer(amount='0.0000000000000000001')])

    def test_wrong_raw_account_token_transaction_or_order_is_rejected(self):
        invalid = [transfer(account='1'), transfer(token='0x'+'3'*40),
                   transfer(tx='0x'+'c'*64), transfer(9)]
        for fee in invalid:
            with self.subTest(fee=fee['id']), self.assertRaises(ValueError): self.reduce([liquidation()], [fee])

    def test_wrong_recipient_or_nonzero_recipient_account_is_rejected(self):
        for field, value in [('accountNumber', '1'), ('user', {'id': OWNER})]:
            fee = transfer(); fee['toMarginAccount'][field] = value
            with self.assertRaises(ValueError): self.reduce([liquidation()], [fee])

    def test_boundary_filter_is_start_inclusive_end_exclusive(self):
        self.assertEqual(self.reduce([liquidation(timestamp=START)], [transfer(timestamp=START)])['feeTransferCount'], 1)
        for stamp in [START-1, END]:
            with self.subTest(stamp=stamp), self.assertRaises(ValueError):
                self.reduce([liquidation(timestamp=stamp)], [transfer(timestamp=stamp)])

    def test_conflicting_duplicate_or_multiple_fees_for_one_liquidation_is_rejected(self):
        with self.assertRaises(ValueError): self.reduce([liquidation()], [transfer(), transfer(usd='2')])
        with self.assertRaises(ValueError): self.reduce([liquidation()], [transfer(), transfer(12)])

    def test_missing_metadata_and_timestamp_disagreement_are_not_complete(self):
        for fee in [transfer(timestamp=START+1), transfer()]:
            if fee['transaction']['timestamp'] == str(START): del fee['token']['decimals']
            with self.assertRaises(ValueError): self.reduce([liquidation()], [fee])

    def test_zero_usd_for_a_material_token_amount_is_not_a_known_zero_price(self):
        with self.assertRaises(ValueError): self.reduce([liquidation()], [transfer(usd='0', amount='1')])
        row = liquidation(); del row['heldTokenAmountUSD']
        with self.assertRaises(ValueError): self.reduce([row], [transfer(usd='0', amount='0.000000000000000003')])


class PaginationTests(CollectorTest):
    def test_full_page_requires_an_explicit_terminal_page(self):
        pages = {'': [{'id': 'a'}, {'id': 'b'}], 'b': []}
        rows, count = COLLECTOR.paginate(lambda cursor: pages[cursor], 2, 2)
        self.assertEqual([row['id'] for row in rows], ['a', 'b'])
        self.assertEqual(count, 2)

    def test_limit_unsorted_or_repeated_cursor_fails_closed(self):
        cases = [(lambda _: [{'id':'a'}], 1, 1),
                 (lambda _: [{'id':'b'}, {'id':'a'}], 2, 2),
                 (lambda _: [{'id':'a'}, {'id':'a'}], 2, 2)]
        for fetch, size, limit in cases:
            with self.assertRaises(ValueError): COLLECTOR.paginate(fetch, size, limit)


class HeaderFallbackTests(CollectorTest):
    def header(self, values, calls=None):
        endpoints = ['https://lagging.invalid', 'https://ready.invalid']
        rpc = COLLECTOR.RpcClient(endpoints=endpoints, timeout=1, retries_per_endpoint=1, quiet=True)
        if calls is None: calls = []
        def post(client, endpoint, payload):
            calls.append(endpoint)
            return {'jsonrpc':'2.0', 'id':1, 'result':values[endpoint]}
        with patch.object(COLLECTOR.RpcClient, '_post', post):
            result = COLLECTOR._header(rpc, {'blockNumber':BLOCK, 'blockHash':HASH})
        return result, calls

    def valid(self):
        return {'number':hex(BLOCK), 'hash':HASH, 'timestamp':hex(NOW-10)}

    def test_null_header_rotates_to_existing_endpoint_with_same_pin(self):
        try:
            stamp, calls = self.header({'https://lagging.invalid':None, 'https://ready.invalid':self.valid()})
        except ValueError as error:
            self.fail('A lagging null header must try the next configured endpoint: ' + str(error))
        self.assertEqual(stamp, NOW-10)
        self.assertEqual(calls, ['https://lagging.invalid', 'https://ready.invalid'])

    def test_malformed_header_rotates_without_relaxing_requested_block(self):
        try:
            stamp, _ = self.header({'https://lagging.invalid':{'number':hex(BLOCK)}, 'https://ready.invalid':self.valid()})
        except ValueError as error:
            self.fail('An incomplete header must try the next configured endpoint: ' + str(error))
        self.assertEqual(stamp, NOW-10)

    def test_all_null_headers_fail_after_bounded_existing_endpoints(self):
        calls = []
        with self.assertRaises(ValueError):
            self.header({'https://lagging.invalid':None, 'https://ready.invalid':None}, calls)
        self.assertEqual(calls, ['https://lagging.invalid', 'https://ready.invalid'])

    def test_conflicting_canonical_hash_fails_even_if_another_endpoint_agrees(self):
        wrong = dict(self.valid(), hash='0x'+'c'*64)
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            self.header({'https://lagging.invalid':wrong, 'https://ready.invalid':self.valid()})


class SourceTests(CollectorTest):
    def configs(self):
        return {chain: {'chainId': chain_id, 'subgraph':'https://example.invalid/'+chain,
                        'margin': margin, 'multicall':'0x'+'9'*40}
                for chain, chain_id, margin in [('ethereum',1,'0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d'),
                    ('arbitrum',42161,'0x6bd780e7fdf01d77e4d475c821f1e7ae05409072'),
                    ('berachain',80094,'0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d')]}

    def collect(self, *, meta_change=None, header_change=None, graph_error=None, historical_meta_change=None):
        configs = self.configs()
        meta = {'block':{'number':BLOCK,'hash':HASH},'deployment':'deployment-1','hasIndexingErrors':False}
        class RPC:
            def __init__(self, chain, **kwargs): self.chain = chain
            def call(self, method, params):
                if method == 'eth_chainId': return hex(configs[self.chain]['chainId'])
                if method != 'eth_getBlockByNumber': raise AssertionError(method)
                header = {'number':hex(BLOCK),'hash':HASH,'timestamp':hex(NOW-10)}
                if header_change: header.update(header_change)
                return header
        def graph(url, query, variables, settings):
            if graph_error: raise graph_error
            if '_meta' in query:
                result = copy.deepcopy(meta)
                if meta_change: result.update(meta_change)
                if variables and historical_meta_change: result.update(historical_meta_change)
                return {'_meta':result}
            if variables['block'] != BLOCK or variables['start'] != str(START) or variables['end'] != str(END):
                raise AssertionError('Missing source pin or exact period')
            # The real query must bind block and range variables; the fake models a pinned Graph endpoint.
            if 'block:{number:$block}' not in query or 'timestamp_lt:$end' not in query:
                raise AssertionError('Query omitted pin or exclusive cutoff')
            family = 'transfers' if 'transfers(' in query else 'liquidations'
            return {family: [transfer()] if family == 'transfers' else [liquidation()]}
        with patch.object(COLLECTOR, 'RpcClient', RPC), patch.object(COLLECTOR, '_graphql', graph), patch.object(COLLECTOR.time, 'time', return_value=NOW):
            return COLLECTOR.collect_liquidation_income(configs, START, END, options={'maxSourceAgeSeconds':60})

    def test_complete_source_keeps_exact_period_gross_and_honest_valuation(self):
        result = self.collect()
        self.assertTrue(result['complete'])
        self.assertEqual(result['period'], {'startTimestamp':START,'endTimestamp':END})
        self.assertEqual(result['chains']['ethereum']['grossProtocolFeeUSD'], '1.000000000000000001')
        self.assertTrue(result['chains']['ethereum']['coverage']['allPagesReached'])
        self.assertFalse(result['chains']['ethereum']['valuation']['independentReceiptAudit'])

    def test_indexing_errors_stale_pin_and_missing_source_fail(self):
        with self.assertRaises(ValueError): self.collect(meta_change={'hasIndexingErrors':True})
        with self.assertRaises(ValueError): self.collect(header_change={'timestamp':hex(NOW-61)})
        with self.assertRaises(ValueError): self.collect(header_change={'hash':'0x'+'c'*64})
        with self.assertRaises(RuntimeError): self.collect(graph_error=RuntimeError('Source unavailable'))

    def test_wrong_chain_scope_or_contract_is_rejected_before_fetching(self):
        configs = self.configs(); del configs['arbitrum']
        with self.assertRaises(ValueError): COLLECTOR.collect_liquidation_income(configs, START, END)
        configs = self.configs(); configs['ethereum']['margin'] = OWNER
        with self.assertRaises(ValueError): COLLECTOR.collect_liquidation_income(configs, START, END)

    def test_historical_graph_metadata_may_omit_hash_when_rpc_rechecks_pin(self):
        try:
            result = self.collect(historical_meta_change={'block':{'number':BLOCK,'hash':None}})
        except ValueError as error:
            self.fail('Historical Graph hash omission must use the verified RPC pin: ' + str(error))
        self.assertTrue(result['complete'])
        self.assertEqual(result['chains']['ethereum']['source']['blockHash'], HASH)

    def test_historical_deployment_change_or_wrong_block_is_rejected(self):
        for change in [{'deployment':'new-deployment'}, {'block':{'number':BLOCK-1,'hash':None}},
                       {'block':{'number':BLOCK,'hash':'0x'+'c'*64}}]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.collect(historical_meta_change=change)


if __name__ == '__main__':
    unittest.main()
