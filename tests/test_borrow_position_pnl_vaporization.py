import copy
import json
from pathlib import Path
import unittest

import fetch_borrow_position_pnl as producer

FIXTURES = json.loads((Path(__file__).parent / 'fixtures/borrow_pnl_vaporization_keepers.json').read_text())

class VaporizationKeeperTests(unittest.TestCase):
    def proof(self, fixture, opening=None):
        owner, number = fixture['accountId'].split('-')
        contexts = {(c['tx'], c['logIndex']): c['context'] for c in fixture['contexts']}
        return producer.vaporization_keeper_proof(opening or {}, fixture['events'], owner, number, contexts)

    def test_all_fourteen_source_routes_have_complete_zero_start_zero_end_keeper_proofs(self):
        for fixture in FIXTURES:
            with self.subTest(chain=fixture['chain'], account=fixture['accountId']):
                proof = self.proof(fixture)
                self.assertIsNotNone(proof)
                self.assertEqual(proof['kind'], 'vaporization_keeper_boundary')
                self.assertEqual(len(proof['events']), len(fixture['events']))
                contexts = {(c['tx'], c['logIndex']): c['context'] for c in fixture['contexts']}
                owner, number = fixture['accountId'].split('-')
                flows = producer.row_flows(fixture['events'], owner, number, contexts, [proof])
                self.assertTrue(all(f['usd36'] < 0 for f in flows))
                recovered, certified = producer.recover_capital(None, {}, fixture['events'], owner, number, contexts, {proof['transactionHash']})
                self.assertEqual(recovered, [proof])
                self.assertEqual(certified, {proof['transactionHash']})
                from build_borrow_position_pnl import validate_capital_proofs, from_usd36
                row = dict(chain=fixture['chain'], address=owner, accountNumber=number, openingBalances=[], closingBalances=[], eventCount=len(fixture['events']), externalFlowCount=len(flows), capitalProof=[proof], capitalFlowProof=producer.public_capital_flows(flows), pnl={'status': 'verified'}, netInflowUsdExact=from_usd36(sum(f['usd36'] for f in flows)), totalInflowUsdExact='0', totalOutflowUsdExact=from_usd36(-sum(f['usd36'] for f in flows)))
                validate_capital_proofs(row, 'unused-for-keeper')
                self.assertEqual(sum(f['usd36'] for f in flows), -sum(-int(e['legs'][0]['delta']) * contexts[(e['txHash'], e['logIndex'])]['prices'][e['legs'][0]['market']] for e in fixture['events'] if e['kind'] == 'withdraw'))

    def test_public_proof_rejects_fabricated_intermediate_principal_and_operation_marks(self):
        from build_borrow_position_pnl import validate_vaporization_keeper
        proof = self.proof(FIXTURES[1])
        owner, number = FIXTURES[1]['accountId'].split('-')
        changes = []
        candidate = copy.deepcopy(proof); candidate['events'][0]['legs'][0]['newParRawSigned'] = '1'; changes.append(candidate)
        candidate = copy.deepcopy(proof); candidate['events'][1]['legs'][0]['oraclePriceRaw'] = '1'; changes.append(candidate)
        candidate = copy.deepcopy(proof); candidate['events'][1]['legs'][0]['supplyIndexRaw'] = '1'; changes.append(candidate)
        candidate = copy.deepcopy(proof); candidate['events'][-1]['operationRef'] = proof['events'][0]['operationRef']; changes.append(candidate)
        candidate = copy.deepcopy(proof); candidate['events'][1]['topic0'] = '0x' + 'a' * 64; changes.append(candidate)
        candidate = copy.deepcopy(proof); candidate['events'][0]['vaporAccount']['deltaWeiRawSigned'] = '0'; changes.append(candidate)
        for candidate in changes:
            with self.assertRaises(ValueError):
                validate_vaporization_keeper(candidate, owner, number)

    def test_unsupported_or_incomplete_routes_stay_unproved(self):
        original = FIXTURES[1]
        cases = []
        for kind in ('deposit', 'transfer', 'liquidation'):
            fixture = copy.deepcopy(original); fixture['events'][0]['kind'] = kind; cases.append(fixture)
        fixture = copy.deepcopy(original); fixture['events'].pop(0); cases.append(fixture)
        fixture = copy.deepcopy(original); fixture['events'][-1]['legs'][0]['new_par'] = 1; cases.append(fixture)
        fixture = copy.deepcopy(original); fixture['events'][-1]['legs'][0]['delta'] -= 1; cases.append(fixture)
        fixture = copy.deepcopy(original); fixture['contexts'][-1]['context']['operator'] = '0x' + 'f' * 40; cases.append(fixture)
        fixture = copy.deepcopy(original); fixture['events'][0]['legs'][0]['owner'] = '0x' + 'f' * 40; cases.append(fixture)
        for fixture in cases:
            self.assertIsNone(self.proof(fixture))
        self.assertIsNone(self.proof(original, {'25': 1}))

if __name__ == '__main__':
    unittest.main()
