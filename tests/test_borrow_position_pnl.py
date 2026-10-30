import unittest
from fractions import Fraction
from build_borrow_position_pnl import performance, usd36, from_usd36, par_to_wei, validate_snapshot


class PerformanceTests(unittest.TestCase):
    def result(self, opening='100', closing='110', flows=(), **kw):
        return performance(usd36(opening), usd36(closing), list(flows), coverage=True, replay=True, **kw)

    def flow(self, value, pre=None, post=None):
        return dict(usd36=usd36(value), preNavUsd36=None if pre is None else usd36(pre), postNavUsd36=None if post is None else usd36(post), exactNav=pre is not None)

    def test_reduced_million_position(self):
        r=self.result('1000000','5000',[self.flow('-1045000','1050000','5000')])
        self.assertEqual(r['pnl']['valueUsdExact'],'50000')
        self.assertEqual(r['return']['pctExact'],'5')

    def test_deposit_does_not_become_profit(self):
        r=self.result('100','160',[self.flow('50','110','160')])
        self.assertEqual(r['pnl']['valueUsdExact'],'10'); self.assertEqual(r['return']['pctExact'],'10')

    def test_external_debt_repayment_is_capital(self):
        r=self.result('100','140',[self.flow('30')])
        self.assertEqual(r['pnl']['valueUsdExact'],'10'); self.assertEqual(r['return']['status'],'unavailable')

    def test_pure_price_interest_and_internal_trade(self):
        for close in ['105','95']:
            r=self.result(closing=close)
            self.assertEqual(r['return']['pctExact'],str(int(close)-100))
            self.assertEqual(r['pnl']['valueUsdExact'],str(int(close)-100))

    def test_complete_close_retains_profit_and_return(self):
        r=self.result(closing='0',flows=[self.flow('-110','110','0')])
        self.assertEqual(r['pnl']['valueUsdExact'],'10'); self.assertEqual(r['return']['pctExact'],'10')

    def test_nonpositive_start_return_is_unavailable(self):
        for start in ['0','-10']:
            r=self.result(opening=start)
            self.assertEqual(r['pnl']['status'],'verified'); self.assertEqual(r['return']['status'],'unavailable')

    def test_negative_interim_nav_is_unavailable(self):
        r=self.result(flows=[self.flow('20','-10','10')])
        self.assertEqual(r['return']['status'],'unavailable')

    def test_zero_nav_then_reopened_is_unavailable(self):
        r=self.result(flows=[self.flow('-100','100','0'),self.flow('100','0','100')])
        self.assertEqual(r['return']['status'],'unavailable')

    def test_missing_quote_is_not_zero(self):
        r=performance(None,usd36('5'),[],coverage=True,replay=True)
        self.assertEqual(r['pnl']['status'],'unavailable'); self.assertIsNone(r['pnl']['valueUsdExact'])

    def test_truncated_coverage_and_replay_independently_block(self):
        for coverage,replay in [(False,True),(True,False)]:
            r=performance(usd36('1'),usd36('2'),[],coverage=coverage,replay=replay)
            self.assertEqual(r['pnl']['status'],'unavailable')

    def test_unknown_flow_cannot_be_zero(self):
        r=self.result(flows=[dict(usd36=None,reason='ambiguous_route')])
        self.assertIsNone(r['pnl']['valueUsdExact'])

    def test_same_mark_nav_difference_must_match_flow(self):
        r=self.result(flows=[self.flow('1','100','102')])
        self.assertEqual(r['pnl']['status'],'verified'); self.assertEqual(r['return']['status'],'unavailable')

    def test_exact_large_values_and_one_usd36_unit(self):
        x='123456789012345678901234567890.000000000000000000000000000000000001'
        self.assertEqual(from_usd36(usd36(x)),x)
        r=performance(usd36(x),usd36(x)+1,[],coverage=True,replay=True)
        self.assertEqual(r['pnl']['valueUsdExact'],'0.000000000000000000000000000000000001')

    def test_fraction_return_retains_exact_ratio(self):
        r=self.result('3','4')
        self.assertEqual(Fraction(int(r['return']['ratioNumerator']),int(r['return']['ratioDenominator'])),Fraction(1,3))

    def test_no_float_or_excess_precision_input(self):
        for x in [1.1,'1e2','0.'+'0'*36+'1']:
            with self.assertRaises(ValueError): usd36(x)

    def test_solidity_half_up_both_signs(self):
        self.assertEqual(par_to_wei(1,1500000000000000000,1),2)
        self.assertEqual(par_to_wei(-1,1,1500000000000000000),-2)

    def test_snapshot_rejects_unknown_status_numeric_and_duplicate_identity(self):
        with self.assertRaises(ValueError): validate_snapshot({'schemaVersion':1,'positions':[]})



class PublicationTests(unittest.TestCase):
    def snapshot(self):
        import time,hashlib,json
        from datetime import datetime,timezone
        now=int(time.time())
        result=performance(usd36('100'),usd36('110'),[],coverage=True,replay=True)
        owner='0x'+'1'*40
        def balance(market,value):
            return dict(marketId=market,address='0x'+'2'*40,symbol='ASSET' if market=='0' else 'DEBT',decimals=18,usdExact=value)
        row=dict(chain='ethereum',chainId=1,address=owner,effectiveAddress=owner,accountId=owner+'-1',accountNumber='1',closed=False,
                 openingEquityUsdExact='100',closingEquityUsdExact='110',openingCollateralUsdExact='140',openingDebtUsdExact='40',closingCollateralUsdExact='150',closingDebtUsdExact='40',
                 historicalSizeUsdExact='190',currentSizeUsdExact='190',historicalSizeMethod='max_observed_boundary_operation_gross_exposure_or_verified_flow',
                 openingBalances=[balance('0','140'),balance('1','-40')],closingBalances=[balance('0','150'),balance('1','-40')],tokens=['ASSET','DEBT'],
                 totalInflowUsdExact='0',totalOutflowUsdExact='0',eventCount=0,externalFlowCount=0,returnProof=[],
                 provenance={'endpointMethod':'graph_par_rpc_current_index_oracle','flowMethod':'operation_oracle_transaction_boundary_or_position_transfer','endingParReconciled':True,'eventRangeComplete':True,'boundaryGetterAudited':False,'boundaryBalancesHash':'a'*64},**result)
        chains={}
        source_hashes=['a'*64]
        for c,cid in [('ethereum',1),('arbitrum',42161),('berachain',80094)]:
            count=int(c=='ethereum')
            chains[c]=dict(chainId=cid,margin='0x6bd780e7fdf01d77e4d475c821f1e7ae05409072' if c=='arbitrum' else '0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d',graphDeployment='QmFixture',
                opening={'number':100,'timestamp':now-2592000,'hash':'0x'+'a'*64},closing={'number':200,'timestamp':now,'hash':'0x'+'b'*64},openingNextBlock={'number':101,'timestamp':now-2591999,'hash':'0x'+'c'*64},
                coverage={'complete':True,'openingBorrowerPages':1,'closingBorrowerPages':1,'additionalOpeningPages':0,'additionalClosingPages':0,
                          'eventPages':{family:1 for family in ['deposits','withdrawals','transfers','trades','liquidations','vaporizations']},'graphEventCounts':{family:0 for family in ['deposits','withdrawals','transfers','trades','liquidations','vaporizations']},
                          'rawBalanceEvents':0,'rangeShards':1,'rangesHash':'a'*64,'graphRpcEventIdentitiesMatch':True,'discoveredAccounts':count,'borrowPositions':count,'nonBorrowAccounts':0},
                valuation={'method':'exact_graph_par_times_rpc_current_index_times_rpc_oracle','rounding':'Solidity Interest.parToWei half-up both signs','accountValuesSemantics':'cached_index' if c=='arbitrum' else 'current_index','sampleGetterAudits':[]},
                failures={},unavailableMarkets={'opening':{},'closing':{}},sourceHashes=source_hashes,sourceCacheManifestHash=hashlib.sha256(json.dumps(source_hashes,separators=(',',':')).encode()).hexdigest(),runtimeSeconds=0)
        return dict(schemaVersion=1,methodology='dolomite-oracle-marked-equity-v1',maxAgeSeconds=21600,windowSeconds=2592000,generatedAt=datetime.now(timezone.utc).isoformat(),sourceAsOf=now,
                    chains=chains,summary={'positions':1,'verifiedPnl':1,'verifiedReturn':1,'unavailablePnl':0,'closedPositions':0},positions=[row])

    def active_snapshot(self):
        d=self.snapshot();r=d['positions'][0]
        r.update(closingEquityUsdExact='5',closingCollateralUsdExact='5',closingDebtUsdExact='0',currentSizeUsdExact='5',closingBalances=[dict(r['closingBalances'][0],usdExact='5')],netInflowUsdExact='-105',totalOutflowUsdExact='105',eventCount=1,externalFlowCount=1)
        tx='0x'+'3'*64
        r['returnProof']=[dict(blockNumber=150,transactionIndex=0,logIndex=2,eventRef=tx+':2',operationRef=tx+':0x0',marketId='0',deltaWeiRawSigned='-105',oraclePriceRaw=str(10**36),signedUsdExact='-105',preNavUsdExact='110',postNavUsdExact='5')]
        d['chains']['ethereum']['coverage']['rawBalanceEvents']=1
        d['chains']['ethereum']['coverage']['graphEventCounts']['withdrawals']=1
        return d

    def test_verified_no_flow_return_must_match_endpoints(self):
        d=self.snapshot();d['positions'][0]['return'].update(pctExact='999900',ratioNumerator='9999',ratioDenominator='1')
        with self.assertRaises(ValueError):validate_snapshot(d)

    def test_flow_proof_method_must_describe_transaction_boundary_classification(self):
        d=self.snapshot();validate_snapshot(d)
        for method in ['operation_oracle_direct_owner_or_position_transfer','router_allowlist','']:
            d=self.snapshot();d['positions'][0]['provenance']['flowMethod']=method
            with self.assertRaises(ValueError):validate_snapshot(d)

    def test_each_chain_boundary_proof_is_required_and_fresh(self):
        for mutate in [lambda d:d['chains']['arbitrum']['closing'].update(timestamp=d['sourceAsOf']+86400),
                       lambda d:d['chains']['arbitrum'].pop('opening'),
                       lambda d:d['chains']['arbitrum']['opening'].update(hash='bad'),
                       lambda d:d['chains']['arbitrum'].update(margin='0x'+'1'*40),
                       lambda d:d['chains']['arbitrum']['opening'].update(timestamp=d['sourceAsOf']-100),
                       lambda d:d['chains']['arbitrum']['coverage'].update(graphRpcEventIdentitiesMatch=False)]:
            d=self.snapshot();mutate(d)
            with self.assertRaises(ValueError):validate_snapshot(d)

    def test_all_summary_counts_and_required_identities_are_checked(self):
        for key in ['positions','verifiedPnl','verifiedReturn','unavailablePnl','closedPositions']:
            d=self.snapshot();d['summary'][key]=99999
            with self.assertRaises(ValueError):validate_snapshot(d)
        for key in ['effectiveAddress','accountId','openingBalances','returnProof']:
            d=self.snapshot();d['positions'][0].pop(key)
            with self.assertRaises(ValueError):validate_snapshot(d)
        for change in [{'accountId':'wrong'},{'chainId':42161},{'accountNumber':'01'},{'eventCount':True}]:
            d=self.snapshot();d['positions'][0].update(change)
            with self.assertRaises(ValueError):validate_snapshot(d)

    def test_verified_active_return_requires_exact_linkable_proof(self):
        d=self.active_snapshot();validate_snapshot(d)
        for mutate in [lambda r:r.update(returnProof=[]),lambda r:r['returnProof'][0].update(preNavUsdExact='109'),
                       lambda r:r['returnProof'][0].update(blockNumber=99),lambda r:r['returnProof'][0].update(deltaWeiRawSigned='-104'),
                       lambda r:r['return'].update(pctExact='20',ratioNumerator='1',ratioDenominator='5')]:
            d=self.active_snapshot();mutate(d['positions'][0])
            with self.assertRaises(ValueError):validate_snapshot(d)

    def test_nonpositive_initial_equity_cannot_have_verified_return(self):
        d=self.snapshot();r=d['positions'][0]
        r.update(openingEquityUsdExact='0',openingCollateralUsdExact='40');r['openingBalances'][0]['usdExact']='40';r['pnl']['valueUsdExact']='110'
        with self.assertRaises(ValueError):validate_snapshot(d)

    def test_valid_and_reject_altered_financial_total(self):
        d=self.snapshot();validate_snapshot(d)
        d['positions'][0]['pnl']['valueUsdExact']='11'
        with self.assertRaises(ValueError):validate_snapshot(d)

    def test_stale_source_is_not_new_generation(self):
        d=self.snapshot();d['sourceAsOf']-=21601
        for chain in d['chains'].values():chain['closing']['timestamp']-=21601
        with self.assertRaises(ValueError):validate_snapshot(d)

    def test_incomplete_proof_blocks_verified_label(self):
        d=self.snapshot();d['positions'][0]['provenance']['endingParReconciled']=False
        with self.assertRaises(ValueError):validate_snapshot(d)

    def test_duplicate_identity_and_invalid_metric_state(self):
        d=self.snapshot();d['positions'].append(dict(d['positions'][0]))
        with self.assertRaises(ValueError):validate_snapshot(d)
        d=self.snapshot();d['positions'][0]['pnl']['status']='approximate'
        with self.assertRaises(ValueError):validate_snapshot(d)

if __name__=='__main__': unittest.main()
