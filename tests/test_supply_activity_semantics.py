import unittest
import importlib.util
from pathlib import Path


class SemanticsTests(unittest.TestCase):
    def test_netzero_deposit_withdraw_retains_technical_actions(self):
        common=dict(account='a',wallet='w',index={'supplyIndex':'1','borrowIndex':'1'},txHash='tx',timestamp=1,blockNumber=1)
        legs=[dict(common,id='d',sourceId='deposit-1',type='deposit',deltaPar='5',logIndex=1),dict(common,id='w',sourceId='withdraw-2',type='withdraw',deltaPar='-5',logIndex=2)]
        row=self.api().replay(legs,{'a':'0'},1)[0]
        self.assertEqual(row['semantics']['actions'],['deposit','withdraw'])
        self.assertEqual(row['amount'],'0')

    def api(self):
        path = Path(__file__).resolve().parents[1] / 'supply_activity_semantics.py'
        self.assertTrue(path.exists(), 'semantic replay implementation missing')
        spec = importlib.util.spec_from_file_location('semantics', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_sign_transitions_and_precise_index_conversion(self):
        api = self.api()
        for before, after, supply, debt in [('0','10','11','0'),('-10','-4','0','-7.2'),('0','-3','0','3.6'),('2','-3','-2.2','3.6'),('-3','2','2.2','-3.6')]:
            self.assertEqual(api.effects(before, after, {'supplyIndex':'1.1','borrowIndex':'1.2'}), (supply,debt))

    def test_missing_index_is_not_guessed(self):
        self.assertEqual(self.api().effects('-2','0',None),(None,None))

    def test_pure_repayment_is_not_a_deposit_and_amount_is_net_effect(self):
        leg=dict(id='x',sourceId='deposit-1',type='deposit',account='a',wallet='w',deltaPar='6',index={'supplyIndex':'1','borrowIndex':'1.2'},txHash='tx',timestamp=1,blockNumber=1,logIndex=1,amount='999')
        row=self.api().replay([leg],{'a':'-4'},1)[0]
        self.assertEqual(row['semantics']['actions'],['repay'])
        self.assertEqual(row['amount'],'7.2')

    def test_effects_round_each_balance_half_up_to_token_precision(self):
        self.assertEqual(self.api().effects('0.5','1',{'supplyIndex':'1.000001','borrowIndex':'1'},decimals=6),('0.5','0'))

    def test_full_replay_requires_zero_initial_unambiguous_balance(self):
        with self.assertRaisesRegex(RuntimeError,'initial'):
            self.api().replay([],{'a':'1'},42,require_zero=True)

    def test_selected_trade_and_vaporization_legs_use_real_signed_fields(self):
        api=self.api()
        self.assertTrue(hasattr(api,'event_legs'),'event adapter missing')
        common=dict(id='event',serialId='1',logIndex='2',transaction={'id':'tx','timestamp':'3','blockNumber':'4','zaps':[]})
        trade=dict(common,takerToken={'id':'selected'},makerToken={'id':'other'},takerMarginAccount={'id':'a'},takerEffectiveUser={'id':'w'},makerMarginAccount=None,takerInputTokenDeltaPar='-2',takerOutputTokenDeltaPar='99',takerInterestIndex={'supplyIndex':'1','borrowIndex':'1'})
        legs=api.event_legs('trade',trade,'selected')
        self.assertEqual([(x['account'],x['deltaPar']) for x in legs],[('a','-2')])
        vapor=dict(common,borrowedToken={'id':'selected'},heldToken={'id':'other'},vaporMarginAccount={'id':'v'},vaporEffectiveUser={'id':'vw'},solidMarginAccount={'id':'s'},solidEffectiveUser={'id':'sw'},vaporBorrowedTokenAmountDeltaPar='3',solidBorrowedTokenAmountDeltaPar='-3',solidHeldTokenAmountDeltaPar='7')
        self.assertEqual([(x['account'],x['deltaPar']) for x in api.event_legs('vaporization',vapor,'selected')],[('s','-3'),('v','3')])
        self.assertEqual([(x['account'],x['deltaPar']) for x in api.event_legs('vaporization',vapor,'other')],[('s','7')])

    def test_reverse_replay_groups_internal_movement_without_double_counting(self):
        api = self.api()
        legs = [dict(id='x-from', sourceId='transfer-1', type='transfer', account='a', wallet='w', deltaPar='-5', index={'supplyIndex':'1','borrowIndex':'1'}, txHash='tx', timestamp=1, blockNumber=1, logIndex=1),dict(id='x-to', sourceId='transfer-1', type='transfer', account='b', wallet='w', deltaPar='5', index={'supplyIndex':'1','borrowIndex':'1'}, txHash='tx', timestamp=1, blockNumber=1, logIndex=1)]
        rows = api.replay(legs, {'a':'5','b':'5'}, 1)
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['semantics']['supplyChange'],'0')
        self.assertEqual(rows[0]['semantics']['debtChange'],'0')
        self.assertEqual(len(rows[0]['semantics']['legs']),2)

    def test_duplicate_and_missing_baseline_fail_closed(self):
        api = self.api()
        leg=dict(id='x',sourceId='deposit-1',type='deposit',account='a',wallet='w',deltaPar='1',index=None,txHash='tx',timestamp=1,blockNumber=1,logIndex=1)
        with self.assertRaises(RuntimeError): api.replay([leg,leg],{'a':'1'},1)
        with self.assertRaises(RuntimeError): api.replay([leg],None,1)

    def test_ambiguous_trade_taints_account_without_fabricated_borrow(self):
        api = self.api()
        leg=dict(id='x',sourceId='trade-1',type='trade',account='a',wallet='w',deltaPar='-1',ambiguous=True,index=None,txHash='tx',timestamp=1,blockNumber=1,logIndex=1)
        meta=api.replay([leg],{'a':'-1'},1)[0]['semantics']
        self.assertEqual(meta['status'],'unavailable')
        self.assertNotIn('borrow',meta['actions'])

    def test_unrelated_zap_in_same_transaction_is_not_selected_market_action(self):
        leg=dict(id='x',sourceId='deposit-1',type='deposit',account='a',wallet='w',tokenId='selected',deltaPar='1',index=None,txHash='tx',timestamp=1,blockNumber=1,logIndex=1,zaps=[{'id':'zap','effectiveUser':{'id':'w'},'tokenPath':[{'id':'other'}]}])
        meta=self.api().replay([leg],{'a':'1'},1)[0]['semantics']
        self.assertNotIn('zap',meta['actions'])

    def test_zap_linked_token_list_does_not_claim_direction(self):
        leg=dict(id='x',sourceId='deposit-1',type='deposit',account='a',wallet='w',tokenId='selected',deltaPar='1',index=None,txHash='tx',timestamp=1,blockNumber=1,logIndex=1,zaps=[{'id':'zap','effectiveUser':{'id':'w'},'tokenPath':[{'id':'selected'},{'id':'other'}],'amountInToken':'2','amountOutToken':'3'}])
        meta=self.api().replay([leg],{'a':'1'},1)[0]['semantics']
        self.assertEqual(meta['routes'][0].get('orientationStatus'),'unavailable')

    def test_liquidation_preserves_both_signed_legs_without_voluntary_repayment(self):
        api=self.api()
        event=dict(id='x',serialId='1',logIndex='1',transaction={'id':'tx','timestamp':'1','blockNumber':'1'},borrowedToken={'id':'selected'},heldToken={'id':'other'},solidMarginAccount={'id':'s'},solidEffectiveUser={'id':'sw'},liquidMarginAccount={'id':'l'},liquidEffectiveUser={'id':'lw'},solidBorrowedTokenAmountDeltaPar='-3',liquidBorrowedTokenAmountDeltaPar='3',borrowedInterestIndex={'supplyIndex':'1','borrowIndex':'1'})
        rows=api.replay(api.event_legs('liquidation',event,'selected'),{'s':'2','l':'-2'},1)
        self.assertEqual(len(rows),2)
        liquid=next(row for row in rows if row['primaryAddress']=='lw')['semantics']
        self.assertEqual(liquid['debtChange'],'-3')
        self.assertNotIn('repay',liquid['actions'])
        self.assertEqual(next(row for row in rows if row['primaryAddress']=='lw')['secondaryAddress'],'sw')

if __name__ == '__main__': unittest.main()
