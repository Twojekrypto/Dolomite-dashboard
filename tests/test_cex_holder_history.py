import unittest
from unittest.mock import patch

import generate_dolo_flows as flows
import validate_data


class CexHolderHistoryTest(unittest.TestCase):
    def test_cex_wallet_validation_allows_existing_cent_residual_policy(self):
        balances = {f'0x{i:040x}':10.004 for i in range(1,11)}
        labels = {a:{'type':'cex','label':f'Exchange {chr(65+i)}'} for i,a in enumerate(balances)}
        point = flows.build_cex_supply_point(balances, {}, labels)
        point['walletBalances'] = [{'address':a, 'balance':b, 'exchange':labels[a]['label'],
                                    'evidenceStatus':'review_needed'} for a,b in balances.items()]
        self.assertTrue(validate_data._dolo_cex_supply_history_valid({'cex_supply_history':[point]}))

    def test_generated_cex_details_reconcile_and_validator_rejects_wrong_wallet_amount(self):
        point = {'liquid':150, 'wallets':1,
                 'exchanges':[{'name':'Binance','liquid':150,'wallets':1}],
                 'walletBalances':[{'address':'0x1111111111111111111111111111111111111111',
                                    'exchange':'Binance','balance':150,'evidenceStatus':'review_needed'}]}
        payload = {'cex_supply_history':[point]}
        self.assertTrue(validate_data._dolo_cex_supply_history_valid(payload))
        point['walletBalances'][0]['balance'] = 151
        self.assertFalse(validate_data._dolo_cex_supply_history_valid(payload))

    def test_known_trading_identity_wins_over_user_wallet_structure(self):
        address = "0xb7131fc8cdc43060a6210257f537dba5fcae6aed"
        for identity, expected in [('liquidator', 'bot'), ('bot', 'bot'),
                                   ('trader', 'bot'), ('mm', 'mm'), ('watch', 'watch')]:
            for structure in ['safe', 'multisig', 'delegated_eoa', 'smart_account']:
                with self.subTest(identity=identity, structure=structure):
                    self.assertEqual(flows.holder_distribution_type(
                        address, {address: {'is_contract': True, 'contract_wallet_type': structure}},
                        {address: {'type': identity, 'label': 'Trading wallet'}}), expected)

    def test_cex_history_details_use_the_same_reconstructed_balances(self):
        address = '0x1111111111111111111111111111111111111111'
        other = '0x2222222222222222222222222222222222222222'
        holders = {address: {'balance': 150}, other: {'balance': 0}}
        labels = {address: {'type': 'cex', 'label': 'Binance 1'},
                  other: {'type': 'cex', 'label': 'Binance 2'}}
        points = [{'key':'before','timestamp':'2026-09-01T00:00:00Z','ts':100},
                  {'key':'after','timestamp':'2026-09-02T00:00:00Z','ts':200}]
        cutoffs = {'before': {'eth': 10, 'bera': 10}, 'after': {'eth': 30, 'bera': 30}}
        with patch.object(flows, 'load_current_holder_rows', return_value=holders), \
             patch.object(flows, 'load_address_labels', return_value=labels):
            history = flows.calculate_cex_supply_history(
                {'eth': [(other, address, 50 * 10**18, 20)], 'bera': []},
                points, {'eth':30,'bera':30}, 200, cutoff_blocks_by_point=cutoffs)
        self.assertEqual(history[0]['liquid'], 150)
        self.assertEqual({r['address']:r['balance'] for r in history[0]['walletBalances']},
                         {address:100, other:50})
        self.assertEqual({r['address']:r['balance'] for r in history[1]['walletBalances']},
                         {address:150})
        self.assertTrue(all(r['evidenceStatus']=='review_needed'
                            for r in history[0]['walletBalances']))

    def test_cex_evidence_does_not_treat_behavior_or_missing_source_as_proof(self):
        for info in [{'confidence':'confirmed'},
                     {'source':'flow-audit','confidence':'confirmed'},
                     {'source':'etherscan-public-label','confidence':'potential'}]:
            with self.subTest(info=info):
                self.assertEqual(flows.cex_label_evidence_status(info),'review_needed')
        self.assertEqual(flows.cex_label_evidence_status(
            {'source':'etherscan-public-label'}),'public_label')


if __name__ == '__main__':
    unittest.main()
