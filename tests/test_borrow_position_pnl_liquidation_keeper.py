import copy
import json
from pathlib import Path
import unittest

import fetch_borrow_position_pnl as producer
from build_borrow_position_pnl import from_usd36, performance, validate_capital_proofs

FIXTURE = json.loads((Path(__file__).parent / 'fixtures/borrow_pnl_liquidation_keeper.json').read_text())


class Source:
    margin = FIXTURE['margin']

    def __init__(self, fixture):
        self.fixture = fixture

    def rpc(self, method, params, anchor):
        assert params == [self.fixture['transaction']['hash']]
        assert anchor == self.fixture['transaction']['blockHash']
        return self.fixture['transaction' if method == 'eth_getTransactionByHash' else 'receipt']


class LiquidationKeeperTests(unittest.TestCase):
    def inputs(self, fixture=FIXTURE):
        logs = [log for log in fixture['receipt']['logs'] if log['address'] == fixture['margin']]
        events = [producer.decode_event(log) for log in logs if log['topics'][0] in producer.decoder.ALL_EVENTS]
        return events, producer.operation_context(logs)

    def proof(self, fixture=FIXTURE, opening=None, events=None, contexts=None):
        owner, number = fixture['accountId'].split('-')
        original_events, original_contexts = self.inputs(fixture)
        return producer.liquidation_keeper_proof(Source(fixture), opening or {}, events if events is not None else original_events,
            owner, number, contexts if contexts is not None else original_contexts)

    def test_original_successful_closed_keeper_has_verified_dollar_profit(self):
        proof = self.proof()
        self.assertIsNotNone(proof)
        owner, number = FIXTURE['accountId'].split('-')
        events, contexts = self.inputs()
        recovered, certified = producer.recover_capital(Source(FIXTURE), {}, events, owner, number, contexts, {events[0]['txHash']})
        self.assertEqual(recovered, [proof])
        self.assertEqual(certified, {events[0]['txHash']})
        flows = producer.row_flows(events, owner, number, contexts, recovered)
        result = performance(0, 0, flows, coverage=True, replay=True)
        self.assertEqual(result['pnl'], dict(status='verified', valueUsdExact=FIXTURE['expectedPnlUsdExact'], reasonCodes=[]))
        self.assertEqual(result['return']['reasonCodes'], ['nonpositive_opening_equity'])
        self.assertEqual(result['netInflowUsdExact'], '-' + FIXTURE['expectedPnlUsdExact'])
        row = dict(address=owner, accountNumber=number, openingBalances=[], closingBalances=[], eventCount=3,
            capitalProof=[proof], capitalFlowProof=producer.public_capital_flows(flows), externalFlowCount=1,
            totalInflowUsdExact='0', totalOutflowUsdExact=FIXTURE['expectedPnlUsdExact'], **result)
        validate_capital_proofs(row, FIXTURE['margin'])

    def test_malformed_economic_routes_remain_unverified(self):
        events, contexts = self.inputs()
        cases = []
        for index, key, value in [(0, 'new_par', 1), (1, 'delta', -1), (2, 'new_par', 1), (2, 'delta', -1)]:
            candidate = copy.deepcopy(events); candidate[index]['legs'][0][key] = value; cases.append(candidate)
        candidate = copy.deepcopy(events); candidate[0]['legs'][2]['delta'] += 1; cases.append(candidate)
        candidate = copy.deepcopy(events); candidate[0]['legs'][2]['owner'] = events[0]['legs'][0]['owner']; candidate[0]['legs'][2]['account'] = '1234'; cases.append(candidate)
        candidate = copy.deepcopy(events); candidate[2]['routeAddress'] = '0x' + 'f' * 40; cases.append(candidate)
        candidate = copy.deepcopy(events); candidate[1]['kind'] = 'transfer'; cases.append(candidate)
        cases.extend([events[:-1], events + [events[-1]]])
        for candidate in cases:
            with self.subTest(events=candidate):
                self.assertIsNone(self.proof(events=candidate))
        self.assertIsNone(self.proof(opening={'17': 1}))
        context = copy.deepcopy(contexts); context[(events[1]['txHash'], events[1]['logIndex'])] = copy.deepcopy(context[(events[1]['txHash'], events[1]['logIndex'])]); context[(events[1]['txHash'], events[1]['logIndex'])]['prices']['17'] = 1
        self.assertIsNone(self.proof(contexts=context))

    def test_unproven_transaction_receipt_or_omitted_balance_event_is_rejected(self):
        cases = []
        for target, key, value in [('receipt', 'status', '0x0'), ('receipt', 'blockHash', '0x'+'f'*64), ('transaction', 'to', '0x'+'f'*40), ('transaction', 'value', '0x1')]:
            fixture = copy.deepcopy(FIXTURE); fixture[target][key] = value; cases.append(fixture)
        fixture = copy.deepcopy(FIXTURE); extra = copy.deepcopy(fixture['receipt']['logs'][6]); extra['logIndex'] = '0x32'; fixture['receipt']['logs'].append(extra); cases.append(fixture)
        for fixture in cases:
            self.assertIsNone(self.proof(fixture))

    def test_public_validator_rejects_tampered_proof(self):
        from build_borrow_position_pnl import validate_liquidation_keeper
        proof = self.proof()
        owner, number = FIXTURE['accountId'].split('-')
        with self.assertRaises(ValueError): validate_liquidation_keeper(proof, owner, '1235')
        with self.assertRaises(ValueError): validate_liquidation_keeper(proof, '0x'+'f'*40, number)
        for field, value in [('receiptStatus', '0x0'), ('transactionTo', '0x'+'f'*40), ('marginBalanceEventCount', 4)]:
            candidate = copy.deepcopy(proof); candidate[field] = value
            with self.assertRaises(ValueError): validate_liquidation_keeper(candidate, owner, number)
        candidate = copy.deepcopy(proof); candidate['events'][1]['legs'][0]['newParRawSigned'] = '1'
        with self.assertRaises(ValueError): validate_liquidation_keeper(candidate, owner, number)


if __name__ == '__main__':
    unittest.main()
