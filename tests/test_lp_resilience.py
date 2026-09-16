import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from eth_abi import encode

import explorer_api
import generate_dolo_liquidity as lp


ADDRESS = '0x' + '11' * 20
ALICE = '0x' + '22' * 20
BOB = '0x' + '33' * 20
TOPIC = lp.event_topic('Transfer(address,address,uint256)')
URL = 'https://api.routescan.io/v2/network/mainnet/evm/1/etherscan/api'


def response(payload, retry_after=None):
    result = requests.Response()
    result.status_code = 200
    result._content = json.dumps(payload).encode()
    if retry_after is not None:
        result.headers['Retry-After'] = retry_after
    return result


def log(block, amount=100, sender=lp.ZERO_ADDRESS, recipient=ALICE, tx='aa'):
    return {'address': ADDRESS, 'blockNumber': hex(block), 'blockHash': '0x' + 'bb' * 32,
            'transactionHash': '0x' + tx * 32, 'transactionIndex': '0x0', 'logIndex': '0x0',
            'topics': [TOPIC, lp._topic_for_address(sender), lp._topic_for_address(recipient)],
            'data': '0x' + encode(['uint256'], [amount]).hex(), 'removed': False}


class ExplorerResilienceTests(unittest.TestCase):
    def test_http_date_retry_after_is_not_shortened_to_default_backoff(self):
        session = Mock()
        header = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
        session.get.side_effect = [response({'status': '0', 'message': 'NOTOK', 'result': 'Max rate limit reached'}, header),
                                   response({'status': '1', 'message': 'OK', 'result': []})]
        with patch.object(lp.time, 'sleep') as sleep:
            result = lp._routescan_request(session, URL, params={}, timeout=10)
        self.assertEqual(result.json()['status'], '1')
        self.assertTrue(any(28 <= call.args[0] <= 30 for call in sleep.call_args_list))

    def test_invalid_json_is_sanitized_and_not_retried(self):
        session = Mock()
        broken = response(None)
        broken._content = b'private-test-key not JSON'
        session.get.return_value = broken
        with patch.object(lp.time, 'sleep'), self.assertRaisesRegex(RuntimeError, 'malformed JSON') as raised:
            lp._routescan_request(session, URL, params={'apikey': 'private-test-key'}, timeout=10)
        self.assertNotIn('private-test-key', str(raised.exception))
        self.assertEqual(session.get.call_count, 1)

    def test_json_throttle_then_success_keeps_query_and_honors_retry_after(self):
        query = {'module': 'logs', 'action': 'getLogs', 'page': 3, 'topic2': '0xabc'}
        session = Mock()
        session.get.side_effect = [response({'status': '0', 'message': 'NOTOK',
                                            'result': 'Max rate limit reached'}, '9'),
                                   response({'status': '1', 'message': 'OK', 'result': []})]
        with patch.object(lp.time, 'sleep') as sleep:
            result = lp._routescan_request(session, URL, params=query, timeout=10, max_attempts=3)
        self.assertEqual(result.json()['result'], [])
        self.assertEqual([c.kwargs['params'] for c in session.get.call_args_list], [query, query])
        self.assertTrue(any(call.args[0] >= 9 for call in sleep.call_args_list))

    def test_persistent_json_throttle_is_bounded_and_does_not_rotate_keys(self):
        session = Mock()
        session.get.return_value = response({'status': '0', 'message': 'NOTOK',
                                            'result': 'Max rate limit reached'})
        with patch.object(lp.time, 'sleep'), patch.dict('os.environ', {
            'ETHERSCAN_API_KEY': 'private-primary', 'BERASCAN_API_KEY': 'private-backup'}):
            with self.assertRaises(RuntimeError):
                lp._routescan_request(session, URL, params={}, timeout=10, max_attempts=3)
        self.assertEqual(session.get.call_count, 3)
        self.assertTrue(all('apikey' not in c.kwargs['params'] for c in session.get.call_args_list))

    def test_permanent_and_malformed_failures_are_not_retried_or_empty(self):
        for payload in [None, {'result': []}, {'status': '1', 'result': 'garbage'},
                        {'status': '0', 'message': 'NOTOK', 'result': []},
                        {'status': '0', 'message': 'NOTOK', 'result': 'Max rate limit reached; No records found'},
                        {'status': '0', 'message': 'NOTOK', 'result': 'Invalid API Key'},
                        {'status': '0', 'message': 'NOTOK', 'result': 'This endpoint requires a PRO subscription'}]:
            with self.subTest(payload=payload):
                session = Mock()
                session.get.return_value = response(payload)
                with patch.object(lp.time, 'sleep'), self.assertRaises(RuntimeError):
                    lp._routescan_request(session, URL, params={'module': 'logs', 'action': 'getLogs'}, timeout=10)
                self.assertEqual(session.get.call_count, 1)

    def test_genuine_empty_result_is_accepted(self):
        for payload in [{'status': '0', 'message': 'No records found', 'result': []},
                        {'status': '0', 'message': 'No records found', 'result': 'No records found'},
                        {'status': '1', 'message': 'OK', 'result': []}]:
            session = Mock()
            session.get.return_value = response(payload)
            with patch.object(lp.time, 'sleep'):
                self.assertEqual(lp._routescan_request(session, URL, params={}, timeout=10).json(), payload)


class RPCResilienceTests(unittest.TestCase):
    def test_confirmed_empty_prefers_nonempty_peer_and_does_not_count_same_family_twice(self):
        seen = []
        def rpc(endpoints, payload, **kwargs):
            seen.extend(endpoints)
            return {'result': [log(10)] if 'independent' in endpoints[0] else []}
        rows, cursor = lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 11, 2, rpc=rpc,
                                    endpoints=['https://eth-mainnet.alchemy.com/v2/private-a',
                                               'https://eth-mainnet.alchemy.com/v2/private-b',
                                               'https://independent.invalid'])
        self.assertEqual([r['blockNumber'] for r in rows], [10])
        self.assertEqual(len(seen), 2)
        self.assertEqual(cursor, 11)

    def test_reorg_overlap_removes_old_events_instead_of_unioning_them(self):
        with tempfile.TemporaryDirectory() as directory:
            lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 12, 3,
                         rpc=lambda *a, **k: {'result': [log(10), log(12, tx='cc')]},
                         endpoints=['https://rpc.invalid'], cache_dir=directory, overlap=2)
            starts = []
            def empty_rpc(endpoints, payload, **kwargs):
                starts.append(int(payload['params'][0]['fromBlock'], 16))
                return {'result': []}
            rows, cursor = lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 13, 3,
                                       rpc=empty_rpc, endpoints=['https://one.invalid', 'https://two.invalid'],
                                       cache_dir=directory, overlap=2)
            self.assertEqual([r['blockNumber'] for r in rows], [10])
            self.assertEqual(starts, [11, 11])
            self.assertEqual(cursor, 13)

    def test_corrupt_filter_coverage_and_digest_never_promote_cache(self):
        for change in ('filter', 'gap', 'digest'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 12, 2,
                             rpc=lambda endpoints, payload, **k: {'result': [log(int(payload['params'][0]['fromBlock'], 16), tx=f'{int(payload["params"][0]["fromBlock"], 16):02x}')]},
                             endpoints=['https://rpc.invalid'], cache_dir=directory)
                checkpoint = next(Path(directory).glob('*.json'))
                cached = json.loads(checkpoint.read_text())
                if change == 'filter':
                    cached['filters']['address'] = BOB
                elif change == 'gap':
                    cached['ranges'][1][0] += 1
                else:
                    cached['logs'][0]['data'] = '0x'
                checkpoint.write_text(json.dumps(cached))
                with self.assertRaisesRegex(RuntimeError, 'invalid LP scan checkpoint'):
                    lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 13, 2,
                                 rpc=lambda *a, **k: self.fail('invalid cache reached RPC'),
                                 endpoints=['https://rpc.invalid'], cache_dir=directory)

    def test_single_block_result_cap_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'required log chunk'):
            lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 10, 1,
                         rpc=lambda *a, **k: {'result': [log(10)] * 1000},
                         endpoints=['https://rpc.invalid'])

    def test_generic_explorer_preflight_error_falls_back_with_every_filter(self):
        row = log(10)
        session = Mock()
        session.get.return_value = response({'status': '0', 'message': 'NOTOK', 'result': 'Error!'})
        calls = []
        def rpc(_endpoints, payload, **kwargs):
            calls.append(payload)
            if payload['method'] == 'eth_getLogs':
                return {'result': [row]}
            return {'result': {'number': '0xa', 'timestamp': '0x64'}}
        with patch.object(lp, 'rpc_single_request', side_effect=rpc), patch.object(lp, 'rpc_batch_requests', return_value=({'block:10': {'result': {'number': '0xa', 'timestamp': '0x64'}}}, [])), patch.object(lp.time, 'sleep'):
            rows = lp._routescan_logs(1, ADDRESS, TOPIC, 10, 11, session=session,
                                      indexed_topics={2: lp._topic_for_address(ALICE)})
        self.assertEqual([(r['blockNumber'], r['timestamp']) for r in rows], [(10, 100)])
        self.assertEqual(calls[0]['params'][0], {'address': ADDRESS,
                         'topics': [TOPIC, None, lp._topic_for_address(ALICE)],
                         'fromBlock': '0xa', 'toBlock': '0xb'})

    def test_rpc_range_limit_shrinks_without_gaps(self):
        requests_seen = []
        def rpc(_endpoints, payload, **kwargs):
            q = payload['params'][0]
            start, end = int(q['fromBlock'], 16), int(q['toBlock'], 16)
            requests_seen.append((start, end))
            if end - start + 1 > 2:
                return {'error': {'code': -32005, 'message': 'block range too wide'}}
            return {'result': [log(start, tx=f'{start:02x}')]}
        rows, cursor = lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 13, 4,
                                    rpc=rpc, endpoints=['https://rpc.invalid'])
        self.assertEqual(cursor, 13)
        self.assertEqual([r['blockNumber'] for r in rows], [10, 12])
        self.assertEqual(requests_seen, [(10, 13), (10, 11), (12, 13)])

    def test_single_unconfirmed_rpc_empty_cannot_be_complete(self):
        with self.assertRaises(RuntimeError):
            lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 11, 2,
                         rpc=lambda *a, **k: {'result': []}, endpoints=['https://rpc.invalid'])

    def test_rpc_log_outside_range_or_filter_is_rejected(self):
        for row in [log(12), {**log(10), 'address': BOB}, {**log(10), 'topics': ['0x' + 'cc' * 32]}]:
            with self.subTest(row=row), self.assertRaises((ValueError, RuntimeError)):
                lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 11, 2,
                             rpc=lambda *a, **k: {'result': [row]}, endpoints=['https://rpc.invalid'])

    def test_checkpoint_resumes_successful_prefix_and_replaces_reorg_overlap(self):
        with tempfile.TemporaryDirectory() as directory:
            served = []
            def failed_rpc(_endpoints, payload, **kwargs):
                start = int(payload['params'][0]['fromBlock'], 16)
                served.append(start)
                if start >= 12:
                    raise RuntimeError('missing chunk')
                return {'result': [log(start)]}
            with self.assertRaisesRegex(RuntimeError, 'missing chunk'):
                lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 15, 2, rpc=failed_rpc,
                             endpoints=['https://rpc.invalid'], cache_dir=directory, overlap=1)
            resumed = []
            def good_rpc(_endpoints, payload, **kwargs):
                start = int(payload['params'][0]['fromBlock'], 16)
                resumed.append(start)
                return {'result': [log(start, tx=f'{start:02x}')]}
            rows, cursor = lp.scan_logs('ethereum', ADDRESS, [TOPIC], 10, 16, 2, rpc=good_rpc,
                                       endpoints=['https://rpc.invalid'], cache_dir=directory, overlap=1)
            self.assertEqual(resumed, [11, 13, 15])
            self.assertEqual(cursor, 16)
            self.assertEqual([r['blockNumber'] for r in rows], [10, 11, 13, 15])


class PinnedHolderTests(unittest.TestCase):
    def test_island_build_uses_full_transfer_replay_and_preserves_farm_claims(self):
        island = BOB
        factory = '0x' + '44' * 20
        farm = '0x' + '55' * 20
        carol = '0x' + '66' * 20
        pool = {'chainKey': 'ethereum', 'adapter': 'kodiak-v3', 'identifier': ADDRESS, 'pair': 'DOLO/PAIR'}
        registry = {'chains': {'ethereum': {'chainId': 1, 'discoveryStartBlock': 1,
                                            'custody': {'kodiakIslandFactory': factory}}}}
        creation = lp.normalize_rpc_log({**log(10), 'address': factory,
                        'topics': [lp.event_topic(lp.KODIAK_ISLAND_CREATED_SIGNATURE),
                                   lp._topic_for_address(ADDRESS), lp._topic_for_address(factory), lp._topic_for_address(island)],
                        'data': '0x' + encode(['address'], [factory]).hex()})
        transfers = [lp.normalize_rpc_log({**log(10), 'address': island}),
                     lp.normalize_rpc_log({**log(11, 30, ALICE, farm, tx='cc'), 'address': island})]
        def logs(chain_id, address, topic, start, end, **kwargs):
            if topic == lp.event_topic(lp.KODIAK_ISLAND_CREATED_SIGNATURE):
                return [creation]
            self.assertEqual((address, topic, start, end), (island, TOPIC, 10, 12))
            return transfers
        def onchain(chain, address, signature, types):
            return {'pool()': (ADDRESS,), 'token0()': (lp.DOLO_ADDRESS,), 'token1()': (ALICE,),
                    'islandFactory()': (factory,), 'totalSupply()': (100,),
                    'getUnderlyingBalances()': (101, 203), 'lowerTick()': (-100,), 'upperTick()': (100,)}[signature]
        def balances(chain, calls):
            return {call['id']: (70 if call['args'][0] == ALICE else 30,) for call in calls}, {}
        farm_state = {'address': farm, 'stakingToken': island, 'custodyBalance': 30, 'stakedBalances': {carol: 30}}
        with patch.object(lp, '_routescan_logs', side_effect=logs), patch.object(lp, '_eth_call', side_effect=onchain), patch.object(lp, '_batch_eth_call_args', side_effect=balances), patch.object(lp, '_kodiak_farms_for_island', return_value=[farm_state]), patch.object(lp, '_contract_owners', side_effect=lambda chain, owners: set(owners) & {island, farm}), patch.object(lp, '_routescan_token_holder_candidates', side_effect=AssertionError('paid holder endpoint used')):
            result = lp._build_kodiak_island_rows(registry, pool, {'token0': lp.DOLO_ADDRESS,
                      'token1': ALICE, 'decimals0': 18, 'decimals1': 18, 'currentTick': 0}, 12)
        self.assertEqual({r['beneficialOwner'] for r in result['activePositions']}, {ALICE, carol})
        self.assertEqual(sum(int(r['doloRaw']) for r in result['activePositions']), 101)
        self.assertEqual(sum(int(r['pairedRaw']) for r in result['activePositions']), 203)
        self.assertTrue(any(r['attributionPath'] == 'kodiak_island_farm' for r in result['activePositions']))

    def test_batch_code_storage_and_other_chain_keep_snapshot_scope(self):
        seen = []
        call = {'id': 'one', 'address': ADDRESS, 'signature': 'totalSupply()', 'outputTypes': ['uint256']}
        def rpc(endpoints, payload, **kwargs):
            seen.append((payload['method'], payload['params'][-1]))
            if payload['method'] == 'eth_getCode':
                return {'result': '0x'}
            if payload['method'] == 'eth_getStorageAt':
                return {'result': '0x' + '00' * 12 + '11' * 20}
            return {'result': '0x' + encode(['uint256'], [5]).hex()}
        def batch(endpoints, payloads, **kwargs):
            seen.extend((p['method'], p['params'][-1]) for p in payloads)
            return {'one': {'result': '0x' + encode(['uint256'], [5]).hex()}}, []
        with patch.object(lp, 'rpc_single_request', side_effect=rpc), patch.object(lp, 'rpc_batch_requests', side_effect=batch):
            with lp.pinned_snapshot('ethereum', 12):
                lp._eth_code('ethereum', ADDRESS)
                lp._safe_singleton_address('ethereum', ADDRESS)
                lp._batch_eth_call_args('ethereum', [call])
                lp._eth_call('berachain', ADDRESS, 'totalSupply()', ['uint256'])
            lp._batch_eth_call_args('ethereum', [call])
        self.assertEqual(seen, [('eth_getCode', '0xc'), ('eth_getStorageAt', '0xc'),
                                ('eth_call', '0xc'), ('eth_call', 'latest'), ('eth_call', 'latest')])

    def test_transfer_replay_matches_each_current_balance_not_just_supply(self):
        transfers = [lp.normalize_rpc_log(log(10)), lp.normalize_rpc_log(log(11, 30, ALICE, BOB, tx='cc')),
                     lp.normalize_rpc_log(log(12, 5, BOB, lp.ZERO_ADDRESS, tx='dd'))]
        def balances(_chain, calls):
            return {call['id']: (70 if call['args'][0] == ALICE else 25,) for call in calls}, {}
        with patch.object(lp, '_routescan_logs', return_value=transfers), patch.object(lp, '_batch_eth_call_args', side_effect=balances):
            result = lp._reconcile_transfer_holder_balances('ethereum', 1, ADDRESS, 10, 12, 95)
        self.assertEqual(result, {ALICE: 70, BOB: 25})
        with patch.object(lp, '_routescan_logs', return_value=transfers), patch.object(lp, '_batch_eth_call_args', return_value=({f'balance:{ALICE}': (69,), f'balance:{BOB}': (26,)}, {})), self.assertRaisesRegex(RuntimeError, 'replay'):
            lp._reconcile_transfer_holder_balances('ethereum', 1, ADDRESS, 10, 12, 95)

    def test_registered_source_pins_defaults_and_resets_after_failure(self):
        observed = []
        def rpc(_endpoints, payload, **kwargs):
            observed.append(payload['params'][-1])
            return {'result': '0x' + encode(['uint256'], [5]).hex()}
        def builder(registry, pool, target):
            lp._eth_call('ethereum', ADDRESS, 'totalSupply()', ['uint256'])
            lp._eth_call_args('ethereum', ADDRESS, 'totalSupply()', ['uint256'], block=7)
            raise RuntimeError('intentional failed source')
        pool = {'chainKey': 'ethereum', 'adapter': 'uniswap-v3', 'identifier': ADDRESS}
        with patch.object(lp, 'rpc_single_request', side_effect=rpc):
            with self.assertRaises(RuntimeError):
                lp.build_registered_source({}, 'ethereum:uniswap-v3', [pool], 12,
                                           builders={'uniswap-v3': builder})
            lp._eth_call('ethereum', ADDRESS, 'totalSupply()', ['uint256'])
        self.assertEqual(observed, ['0xc', '0x7', 'latest'])
