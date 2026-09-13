import unittest
from unittest.mock import patch

import generate_dolo_flows as flows
import validate_data


class CexHolderHistoryTest(unittest.TestCase):
    def test_cex_flow_uses_raw_custody_and_excludes_future_and_internal_transfers(self):
        a,b,user = ['0x'+c*40 for c in ('1','2','3')]
        transfers={'eth':[(user,a,100*10**18,10),(a,b,40*10**18,11),(b,user,5*10**18,12),
                          (user,a,999*10**18,21)],'bera':[]}
        boundaries={key:{'1d':{'startBlock':10,'endBlock':20,'startTimestamp':100,'endTimestamp':200}} for key in flows.CHAINS}
        with patch.object(flows,'load_address_labels',return_value={a:{'type':'cex'},b:{'type':'cex'}}):
            result=flows.calculate_cex_flow_summary(transfers,boundaries)
        self.assertEqual(result['1d']['net'],95)

    def test_cached_cex_rebuild_pins_each_chain_to_published_verified_block(self):
        output = {'period_boundaries':{key:{'all':{'endBlock':100}} for key in flows.CHAINS}}
        state = {'flow_log_integrity':{'chains':{key:{'verifiedThroughBlock':99} for key in flows.CHAINS}}}
        for key in flows.CHAINS:
            state[key+'_last_block'] = 110
            state[key+'_transfers'] = []
            state[key+'_history_start_block'] = flows.CHAINS[key]['deploy_block']
            state['flow_log_integrity']['chains'][key]['lastVerificationProof'] = {'minimumMatchingProviderFamilies':2}
        with patch.object(flows,'has_complete_verified_baseline',return_value=True):
            with self.assertRaisesRegex(RuntimeError,'verified'):
                flows.cex_published_blocks(output,state)
            for key in flows.CHAINS:
                state['flow_log_integrity']['chains'][key]['verifiedThroughBlock'] = 110
            self.assertEqual(flows.cex_published_blocks(output,state), {key:100 for key in flows.CHAINS})
            state['flow_log_integrity']['chains']['eth']['lastVerificationProof'] = {}
            with self.assertRaisesRegex(RuntimeError,'verified'):
                flows.cex_published_blocks(output,state)

    def test_cex_history_replays_raw_transfers_not_unsynchronised_holders(self):
        address = '0x1111111111111111111111111111111111111111'
        zero = '0x' + '0' * 40
        points = [{'key':'before','timestamp':'2026-09-01T00:00:00Z','ts':100},
                  {'key':'now','timestamp':'2026-09-02T00:00:00Z','ts':200}]
        with patch.object(flows, 'load_current_holder_rows', return_value={address:{'balance':999}}), \
             patch.object(flows, 'load_address_labels', return_value={address:{'type':'cex','label':'MEXC'}}):
            history = flows.calculate_cex_supply_history(
                {'eth': [(zero,address,100 * 10**18,20), (address,zero,50 * 10**18,30)], 'bera': []},
                points, {'eth':20,'bera':20}, 200,
                cutoff_blocks_by_point={'before':{'eth':10,'bera':10}, 'now':{'eth':21,'bera':21}})
        self.assertEqual([p['liquid'] for p in history], [0,100])

    def test_cex_history_rejects_missing_incoming_leg_instead_of_clamping(self):
        address = '0x1111111111111111111111111111111111111111'
        with patch.object(flows, 'load_current_holder_rows', return_value={}), \
             patch.object(flows, 'load_address_labels', return_value={address:{'type':'cex','label':'MEXC'}}):
            with self.assertRaisesRegex(ValueError, 'Negative CEX'):
                flows.calculate_cex_supply_history(
                    {'eth':[(address,'0x'+'0'*40,10**18,10)],'bera':[]},
                    [{'key':'now','timestamp':'2026-09-01T00:00:00Z','ts':100}],
                    {'eth':20,'bera':20},100,cutoff_blocks_by_point={'now':{'eth':21,'bera':21}})

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
                {'eth': [('0x'+'0'*40,address,100 * 10**18,1),
                         ('0x'+'0'*40,other,50 * 10**18,1),
                         (other, address, 50 * 10**18, 20)], 'bera': []},
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
