import unittest
from fetch_borrow_position_pnl import exact_par, decode_event, replay, merge_logs, endpoint, ordered_pages, validate_ranges
from scan_earn_netflow import LOG_DEPOSIT, CURRENT_LOG_TRANSFER

OWNER='0x'+'1'*40

def word(x): return format(x,'064x')
def log(topic=LOG_DEPOSIT,data=None):
    return {'topics':[topic,'0x'+OWNER[2:].zfill(64)],'data':'0x'+''.join(word(x) for x in (data or [7,1,1,10,1,10,0])), 'blockNumber':'0xa','blockHash':'0xabc','transactionHash':'0xdef','transactionIndex':'0x0','logIndex':'0x1'}

class SourceTests(unittest.TestCase):
    def test_exact_graph_par_large_and_fractional(self):
        self.assertEqual(exact_par('123456789123456789.000000000000000001',18),123456789123456789000000000000000001)
        with self.assertRaises(ValueError): exact_par('1.0000001',6)

    def test_deposit_retains_route_and_integer_new_par(self):
        e=decode_event(log()); self.assertEqual(e['legs'][0]['new_par'],10); self.assertEqual(e['routeAddress'],'0x'+'0'*40)

    def test_unknown_and_legacy_layout_fail_closed(self):
        with self.assertRaises(ValueError): decode_event(log('0xunknown'))
        with self.assertRaises(ValueError): decode_event(log(data=[1,2,1]))

    def test_duplicate_events_deduplicated_and_conflicts_rejected(self):
        a=log(); self.assertEqual(len(merge_logs([a,a])),1)
        b=dict(a,data='0xdead')
        with self.assertRaises(ValueError): merge_logs([a,b])

    def test_replay_detects_mismatch_and_closed_borrower(self):
        events=[{'legs':[{'owner':OWNER,'account':'7','market':'1','new_par':0,'delta':10}]}]
        state, borrowed = replay({'1':-10},events,OWNER,'7')
        self.assertEqual(state,{}); self.assertTrue(borrowed)
        self.assertNotEqual(state,{'1':1})

    def test_missing_positive_quote_is_not_zero(self):
        with self.assertRaises(ValueError): endpoint({'1':10},{'1':{'supply':10**18,'borrow':10**18,'price':0}}, {})

    def test_endpoint_exact_signed_raw_valuation(self):
        e=endpoint({'1':10,'2':-3},{'1':{'supply':10**18,'borrow':10**18,'price':10**36},'2':{'supply':10**18,'borrow':10**18,'price':10**36}}, {})
        self.assertEqual(e['equityUsd36'],7*10**36)

    def test_pagination_requires_strict_cursor_and_terminal_page(self):
        calls=[]
        def query(cursor):
            calls.append(cursor); return [{'id':'a'},{'id':'b'}] if cursor=='' else []
        rows,pages=ordered_pages(query,2)
        self.assertEqual(len(rows),2); self.assertEqual(calls,['','b']); self.assertEqual(pages,2)
        with self.assertRaises(ValueError): ordered_pages(lambda _: [{'id':'a'},{'id':'a'}],2)

    def test_range_coverage_rejects_gap_and_overlap(self):
        self.assertTrue(validate_ranges([(1,2),(3,5)],1,5))
        for ranges in [[(1,2),(4,5)],[(1,3),(3,5)],[(1,4)]]:
            self.assertFalse(validate_ranges(ranges,1,5))


class FlowTests(unittest.TestCase):
    def test_unidirectional_router_repayment_is_external_capital(self):
        from fetch_borrow_position_pnl import row_flows
        event=decode_event(log(data=[7,1,1,10,0,90,0]))
        context={(event['txHash'],event['logIndex']):{'id':'op','prices':{'1':3}}}
        flow=row_flows([event],OWNER,'7',context)[0]
        self.assertEqual(flow['usd36'],30)
        self.assertEqual(flow['classification'],'transaction_boundary_deposit')

    def test_third_party_withdrawal_is_external_even_with_high_uint_account(self):
        from fetch_borrow_position_pnl import row_flows
        from scan_earn_netflow import LOG_WITHDRAW
        number=2**255+123
        event=decode_event(log(LOG_WITHDRAW,[number,1,0,10,0,90,0]))
        context={(event['txHash'],event['logIndex']):{'id':'op','prices':{'1':3}}}
        self.assertEqual(row_flows([event],OWNER,str(number),context)[0]['usd36'],-30)

    def test_opposite_router_legs_remain_unknown_without_route_proof(self):
        from fetch_borrow_position_pnl import row_flows
        from scan_earn_netflow import LOG_WITHDRAW
        out=decode_event(log(LOG_WITHDRAW,[7,1,0,100,1,100,0]))
        returned=decode_event(dict(log(data=[7,2,1,95,1,95,0]),logIndex='0x2'))
        contexts={(e['txHash'],e['logIndex']):{'id':'op','prices':{'1':3,'2':3}} for e in [out,returned]}
        flows=row_flows([out,returned],OWNER,'7',contexts)
        self.assertEqual([f['reason'] for f in flows],['ambiguous_router_capital_flow']*2)
        self.assertTrue(all(f['usd36'] is None for f in flows))

    def test_opposite_flows_in_different_raw_accounts_are_not_wallet_netted(self):
        from fetch_borrow_position_pnl import row_flows
        from scan_earn_netflow import LOG_WITHDRAW
        out=decode_event(log(LOG_WITHDRAW,[7,1,0,100,1,100,0]))
        returned=decode_event(dict(log(data=[8,2,1,95,1,95,0]),logIndex='0x2'))
        contexts={(e['txHash'],e['logIndex']):{'id':'op','prices':{'1':3,'2':3}} for e in [out,returned]}
        self.assertEqual(row_flows([out,returned],OWNER,'7',contexts)[0]['usd36'],-300)
        self.assertEqual(row_flows([out,returned],OWNER,'8',contexts)[0]['usd36'],285)

    def test_liquidation_trade_path_preserves_each_raw_subaccount_transfer(self):
        from fetch_borrow_position_pnl import row_flows,replay
        def leg(account,market,delta,new_par):
            return dict(owner=OWNER,account=account,market=market,delta=delta,new_par=new_par)
        common=dict(txHash='0xdef',blockNumber=10,txIndex=0)
        events=[dict(common,kind='liquidation',logIndex=1,legs=[leg('7','1',100,100),leg('7','2',-90,-90)]),
                dict(common,kind='transfer',logIndex=2,legs=[leg('7','1',-100,0),leg('8','1',100,100)]),
                dict(common,kind='trade',logIndex=3,legs=[leg('8','1',-100,0),leg('8','2',95,95)]),
                dict(common,kind='transfer',logIndex=4,legs=[leg('8','2',-95,0),leg('7','2',95,5)])]
        contexts={('0xdef',i):{'id':'op','prices':{'1':1,'2':1}} for i in range(1,5)}
        self.assertEqual([x['usd36'] for x in row_flows(events,OWNER,'7',contexts)],[-100,95])
        self.assertEqual([x['usd36'] for x in row_flows(events,OWNER,'8',contexts)],[100,-95])
        self.assertEqual(replay({},events,OWNER,'7'),({'2':5},True))
        self.assertEqual(replay({},events,OWNER,'8'),({},False))

    def test_direct_owner_flow_uses_operation_quote_and_same_owner_transfer_is_external(self):
        from fetch_borrow_position_pnl import row_flows
        event=decode_event(log(data=[7,1,1,10,1,10,int(OWNER,16)]))
        context={(event['txHash'],event['logIndex']):{'id':'op','prices':{'1':3}}}
        flows=row_flows([event],OWNER,'7',context)
        self.assertEqual(flows[0]['usd36'],30)
        event['kind']='transfer'; event['legs'].append(dict(owner=OWNER,account='8',market='1',delta=-10,new_par=0))
        self.assertEqual(row_flows([event],OWNER,'8',context)[0]['usd36'],-30)

    def test_router_and_missing_quote_unavailable(self):
        from fetch_borrow_position_pnl import row_flows
        event=decode_event(log())
        self.assertIsNone(row_flows([event],OWNER,'7',{})[0]['usd36'])

    def test_same_transaction_operation_marks_do_not_leak(self):
        from fetch_borrow_position_pnl import operation_context, ORACLE, OPERATION
        op=log(OPERATION,[0]);op['logIndex']='0x0'
        quote=log(ORACLE,[3]);quote['topics'][1]='0x'+word(1)
        quote['logIndex']='0x1';first=log();first['logIndex']='0x2'
        nextop=dict(op,logIndex='0x3');second=dict(first,logIndex='0x4')
        ctx=operation_context([op,quote,first,nextop,second])
        self.assertEqual(ctx[('0xdef',2)]['prices'],{'1':3})
        self.assertEqual(ctx[('0xdef',4)]['prices'],{})

class FlowNavTests(unittest.TestCase):
    def test_flow_nav_uses_all_markets_and_exact_event_order(self):
        from fetch_borrow_position_pnl import attach_flow_nav
        event=decode_event(log(data=[7,1,0,50,1,50,int(OWNER,16)]))
        ctx={(event['txHash'],event['logIndex']):{'prices':{'1':10**36,'2':10**36},'indices':{'1':{'supply':10**18,'borrow':10**18},'2':{'supply':10**18,'borrow':10**18}}}}
        flows=[{'eventRef':'0xdef:1','usd36':-50*10**36}]
        size=attach_flow_nav({'1':100,'2':-20},[event],OWNER,'7',ctx,flows)
        self.assertEqual(flows[0]['preNavUsd36'],80*10**36)
        self.assertEqual(flows[0]['postNavUsd36'],30*10**36)
        self.assertTrue(flows[0]['exactNav']);self.assertEqual(size,120*10**36)

    def test_missing_unaffected_market_mark_blocks_return_only(self):
        from fetch_borrow_position_pnl import attach_flow_nav
        event=decode_event(log(data=[7,1,0,50,1,50,int(OWNER,16)]))
        flows=[{'eventRef':'0xdef:1','usd36':-50*10**36,'exactNav':False}]
        attach_flow_nav({'1':100,'2':-20},[event],OWNER,'7',{},flows)
        self.assertFalse(flows[0]['exactNav']);self.assertEqual(flows[0]['usd36'],-50*10**36)

class RangeRetryTests(unittest.TestCase):
    def test_forbidden_is_not_split(self):
        from fetch_borrow_position_pnl import Source
        from urllib.error import HTTPError
        source=Source.__new__(Source);source.margin=OWNER;source.block=lambda n:{'hash':'0xa'}
        calls=[]
        def fail(*args):
            calls.append(args);raise HTTPError('https://example.invalid',403,'Forbidden',{},None)
        source.rpc=fail
        with self.assertRaises(HTTPError): source.log_range((1,9999))
        self.assertEqual(len(calls),1)

    def test_size_limit_splits_finitely_and_preserves_coverage(self):
        from fetch_borrow_position_pnl import Source
        source=Source.__new__(Source);source.margin=OWNER;source.block=lambda n:{'hash':'0xa'}
        calls=[]
        def limited(method,params,anchor):
            a=int(params[0]['fromBlock'],16);b=int(params[0]['toBlock'],16);calls.append((a,b))
            if b-a>10:raise ValueError('query returned too many results')
            return []
        source.rpc=limited
        ranges,logs=source.log_range((1,32))
        self.assertTrue(validate_ranges(ranges,1,32));self.assertEqual(len(calls),7)

class ResilienceTests(unittest.TestCase):
    def test_market_second_round_recovers_transient_failure(self):
        from fetch_borrow_position_pnl import Source
        from urllib.error import HTTPError
        source=Source.__new__(Source);attempts=[]
        def call(data,header):
            attempts.append(data)
            if len(attempts)==1:raise HTTPError('https://example.invalid',503,'Unavailable',{},None)
            return '0x'+''.join(word(v) for v in ([10**18,10**18,1] if data.startswith('56ea') else [10**36]))
        source.call=call
        self.assertEqual(source.marks({'hash':'0xa'},['1'])['1']['price'],10**36)
        self.assertLess(len(attempts),6)

    def test_public_balance_compaction_keeps_exact_usd_and_identity(self):
        from fetch_borrow_position_pnl import public_balances
        b={'marketId':'1','address':OWNER,'symbol':'TEST','decimals':18,'usdExact':'1.000000000000000001','parRawSigned':'1','weiRawSigned':'1','oraclePriceRaw':'1'}
        result=public_balances({'balances':[b]})
        self.assertEqual(result[0]['usdExact'],b['usdExact']);self.assertNotIn('parRawSigned',result[0]);self.assertEqual(result[0]['address'],OWNER)

class CacheTests(unittest.TestCase):
    def test_prune_retains_current_proofs_and_unrelated_files(self):
        import tempfile,json
        from pathlib import Path
        from fetch_borrow_position_pnl import prune_cache
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'ethereum';p.mkdir()
            for name in ['a'*64+'.json','b'*64+'.json','notes.json','latest-chain.json']:(p/name).write_text('{}')
            prune_cache(root,{'ethereum':{'sourceHashes':['a'*64]}})
            self.assertTrue((p/('a'*64+'.json')).exists());self.assertFalse((p/('b'*64+'.json')).exists())
            self.assertTrue((p/'notes.json').exists());self.assertTrue((p/'latest-chain.json').exists())

class WorkflowPublicationTests(unittest.TestCase):
    def execute_publish(self,change_source=False):
        import json,os,subprocess,tempfile
        from pathlib import Path
        from test_borrow_position_pnl import PublicationTests
        root=Path(__file__).resolve().parents[1]
        workflow=(root/'.github/workflows/update-borrow-position-pnl.yml').read_text()
        body=workflow.split('      - name: Publish only validated snapshot on latest master\n',1)[1].split('        run: |\n',1)[1].split('\n      - ',1)[0]
        script='\n'.join(line[10:] for line in body.splitlines())
        with tempfile.TemporaryDirectory() as temporary:
            temp=Path(temporary);remote=temp/'remote.git';work=temp/'work';competitor=temp/'competitor'
            env=dict(os.environ,GIT_CONFIG_COUNT='1',GIT_CONFIG_KEY_0='core.hooksPath',GIT_CONFIG_VALUE_0='/dev/null',TMPDIR=str(temp),PYTHONPYCACHEPREFIX=str(temp/'pycache'))
            def git(*args,cwd=temp):return subprocess.run(['git',*args],cwd=cwd,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True).stdout.strip()
            git('init','--bare','--initial-branch=master',str(remote));git('clone',str(remote),str(work))
            git('config','user.name','Fixture',cwd=work);git('config','user.email','fixture@example.invalid',cwd=work)
            (work/'build_borrow_position_pnl.py').write_text((root/'build_borrow_position_pnl.py').read_text())
            for name in ['fetch_borrow_position_pnl.py','scan_earn_netflow.py','rpc_usage.py']:(work/name).write_text('# initial\n')
            (work/'other.txt').write_text('original')
            previous=PublicationTests().snapshot();(work/'borrow_position_pnl.json').write_text(json.dumps(previous))
            git('add','.',cwd=work);git('commit','-m','Initial fixture',cwd=work);git('push','origin','master',cwd=work)
            git('clone',str(remote),str(competitor));git('config','user.name','Fixture',cwd=competitor);git('config','user.email','fixture@example.invalid',cwd=competitor)
            changed='fetch_borrow_position_pnl.py' if change_source else 'other.txt'
            (competitor/changed).write_text('concurrent update\n');git('add',changed,cwd=competitor);git('commit','-m','Concurrent unrelated update',cwd=competitor);git('push','origin','master',cwd=competitor)
            candidate=PublicationTests().snapshot();candidate['generationLabel']='new candidate'
            (work/'borrow_position_pnl.json').write_text(json.dumps(candidate))
            result=subprocess.run(['bash','-c',script],cwd=work,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            published=json.loads(git('show','master:borrow_position_pnl.json',cwd=remote))
            if change_source:
                self.assertNotEqual(result.returncode,0);self.assertNotIn('generationLabel',published)
                self.assertIn('Producer source changed',result.stdout)
            else:
                self.assertEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertEqual(published['generationLabel'],'new candidate')
                self.assertEqual(git('show','master:other.txt',cwd=remote),'concurrent update')
                self.assertEqual(git('diff-tree','--no-commit-id','--name-only','-r','master',cwd=remote),'borrow_position_pnl.json')

    def test_actual_publisher_preserves_concurrent_unrelated_update(self):self.execute_publish()
    def test_actual_publisher_rejects_changed_producer_source(self):self.execute_publish(True)

class HistoricalSizeTests(unittest.TestCase):
    def test_unpriced_history_is_unknown_not_dust(self):
        from fetch_borrow_position_pnl import historical_size
        self.assertIsNone(historical_size(None,None,0,[]))
        self.assertIsNone(historical_size(None,None,0,[{'usd36':None}]))
        self.assertEqual(historical_size({'supplyUsd36':0,'borrowUsd36':0},None,0,[]),0)
        self.assertEqual(historical_size(None,None,123,[]),123)
        self.assertEqual(historical_size(None,None,0,[{'usd36':-456}]),456)


# Real pinned Arbitrum account: current balances differ from cached values.
REAL_BOUNDARY_FIXTURE = {'accountId': '0x000007656f345a789bb422f0307d826660258333-46115366569491961604023175212882732521454355943772925614689843444723828656856',
 'expectedPnl': '-2.56261942353137409656963432',
 'boundaries': [{'block': {'number': 510318037,
                           'hash': '0x706f3b94e37148f0fb7be4b3d2c7a81b64c66400c0b242dd68067faf70484b8a',
                           'timestamp': 1790763774},
                 'par': {'0': 4854526505140895, '1': -4356896120341872354},
                 'marks': {'0': {'borrow': 1206738015647080566,
                                 'supply': 1107392127257535786,
                                 'price': 2691510000000000000000},
                           '1': {'borrow': 1555458639999197318,
                                 'supply': 1364246183085683460,
                                 'price': 999996020000000000}},
                 'balancesRaw': '0x000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000000000000000000e0000000000000000000000000000000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000001e0000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000200000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1000000000000000000000000da10009cbd5d07dd0cecc66161fc93d7c9000da10000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000113f2a87e4429f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003c76cdf008413ee20000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000131951f9e92d2200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005e0ca3de2fbd2c81',
                 'valuesRaw': '0x000000000000000000000000000000000ae2a5ee24cfaeb7d5de86c2e30600000000000000000000000000000000000005191b44be8e79ea8b179264f0de5800',
                 'expectedEquity': '7.6922481394048588743566619'},
                {'block': {'number': 519648917,
                           'hash': '0xf752b83f8551af93f4899ca787a595eeac059eec7db251b81c714083098fb211',
                           'timestamp': 1793355774},
                 'par': {'0': 4854526505140895, '1': -4356896120341872354},
                 'marks': {'0': {'borrow': 1210360667045312887,
                                 'supply': 1109642606306954204,
                                 'price': 2220052997470000000000},
                           '1': {'borrow': 1567587718114270746,
                                 'supply': 1370313401735829553,
                                 'price': 999928620000000000}},
                 'balancesRaw': '0x000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000000000000000000e0000000000000000000000000000000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000001e0000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000200000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1000000000000000000000000da10009cbd5d07dd0cecc66161fc93d7c9000da10000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000113f2a87e4429f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003c76cdf008413ee20000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000132341a7433e0c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005ec8623c9b225291',
                 'valuesRaw': '0x0000000000000000000000000000000008ff35af09a905a41650d6cd2b61ec00000000000000000000000000000000000523430358b54866f0afa2c0e4566800',
                 'expectedEquity': '5.12962871587348477778702758'}]}


class RealBoundaryTests(unittest.TestCase):
    def test_current_interest_matches_real_getters_not_cached_value_getter(self):
        from fetch_borrow_position_pnl import decode_balances,words
        from build_borrow_position_pnl import from_usd36,performance
        points=[]
        for record in REAL_BOUNDARY_FIXTURE['boundaries']:
            point=endpoint(record['par'],record['marks'],{})
            self.assertEqual(from_usd36(point['equityUsd36']),record['expectedEquity'])
            actual=decode_balances(record['balancesRaw'])
            self.assertEqual({b['marketId']:int(b['weiRawSigned']) for b in point['balances']},{m:v['wei'] for m,v in actual.items()})
            cached=words(record['valuesRaw'])
            self.assertNotEqual(cached[0]-cached[1],point['equityUsd36'])
            points.append(point)
        result=performance(points[0]['equityUsd36'],points[1]['equityUsd36'],[],coverage=True,replay=True)
        self.assertEqual(result['pnl']['valueUsdExact'],REAL_BOUNDARY_FIXTURE['expectedPnl'])


class SparseFilesystemTests(unittest.TestCase):
    def test_actual_workflow_sparse_checkout_runs_its_complete_test_command(self):
        import os,shutil,subprocess,tempfile
        from pathlib import Path
        if os.environ.get('BORROW_PNL_IN_SPARSE_TEST'):
            self.skipTest('Outer regression already created this sparse filesystem')
        root=Path(__file__).resolve().parents[1]
        workflow=(root/'.github/workflows/update-borrow-position-pnl.yml').read_text()
        patterns=workflow.split('          sparse-checkout: |\n',1)[1].split('\n      - ',1)[0]
        patterns='\n'.join(line.strip() for line in patterns.splitlines())+'\n'
        with tempfile.TemporaryDirectory() as temporary:
            base=Path(temporary);source=base/'source';source.mkdir();sparse=base/'sparse'
            paths=['build_borrow_position_pnl.py','fetch_borrow_position_pnl.py','scan_earn_netflow.py','rpc_usage.py',
                   '.github/workflows/update-borrow-position-pnl.yml','tests/test_borrow_position_pnl.py','tests/test_borrow_position_pnl_sources.py']
            for relative in paths:
                dest=source/relative;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(root/relative,dest)
            (source/'unrelated.txt').write_text('must not be materialized')
            env=dict(os.environ,BORROW_PNL_IN_SPARSE_TEST='1',PYTHONPYCACHEPREFIX=str(base/'pycache'),GIT_CONFIG_COUNT='1',GIT_CONFIG_KEY_0='core.hooksPath',GIT_CONFIG_VALUE_0='/dev/null')
            def git(*args,cwd=source,input=None):
                return subprocess.run(['git',*args],cwd=cwd,input=input,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True)
            git('init','--initial-branch=master');git('add','.')
            git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','Current workflow fixture')
            git('clone','--no-checkout',str(source),str(sparse))
            git('sparse-checkout','init','--no-cone',cwd=sparse)
            git('sparse-checkout','set','--no-cone','--stdin',cwd=sparse,input=patterns)
            git('checkout','master',cwd=sparse)
            self.assertTrue((sparse/'.github/workflows/update-borrow-position-pnl.yml').is_file())
            self.assertFalse((sparse/'unrelated.txt').exists())
            result=subprocess.run(['python3','-m','unittest','discover','-s','tests','-p','test_borrow_position_pnl*.py'],cwd=sparse,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)


class RpcRateLimitTests(unittest.TestCase):
    class Clock:
        def __init__(self):
            self.now=0.0;self.sleeps=[]
        def monotonic(self):return self.now
        def sleep(self,seconds):self.sleeps.append(seconds);self.now+=seconds

    def test_429_recovery_honors_retry_after_and_preserves_exact_cache(self):
        import io,json,tempfile,contextlib
        from pathlib import Path
        from unittest.mock import patch
        from urllib.error import HTTPError
        from fetch_borrow_position_pnl import Source,digest
        clock=self.Clock();attempts=[];payload={'jsonrpc':'2.0','id':1,'method':'eth_getLogs','params':[]}
        expected={'jsonrpc':'2.0','id':1,'result':[{'exact':'9007199254740993123456789'}]}
        secret_url='https://provider.invalid/private-key?token=secret'
        def reply(request,timeout):
            attempts.append(clock.now)
            if len(attempts)<=4:raise HTTPError(secret_url,429,'Too Many Requests',{'Retry-After':'7'},None)
            return io.BytesIO(json.dumps(expected).encode())
        with tempfile.TemporaryDirectory() as root,patch('fetch_borrow_position_pnl.time.monotonic',clock.monotonic),patch('fetch_borrow_position_pnl.time.sleep',clock.sleep),patch('fetch_borrow_position_pnl.urlopen',reply),contextlib.redirect_stdout(io.StringIO()) as output:
            source=Source('berachain',root);source.rpc_url=secret_url
            actual=source.request(secret_url,payload,'0xpin')
            self.assertEqual(actual,expected);self.assertEqual(len(attempts),5)
            self.assertTrue(all(b-a>=7 for a,b in zip(attempts,attempts[1:])))
            key=digest({'source':secret_url,'request':payload,'anchor':'0xpin'})
            path=Path(root)/'berachain'/(key+'.json');saved=path.read_bytes()
            self.assertEqual(json.loads(saved)['responseHash'],digest(expected))
            self.assertEqual(source.request(secret_url,payload,'0xpin'),expected)
            self.assertEqual(len(attempts),5);self.assertEqual(path.read_bytes(),saved)
            self.assertIn('chain=berachain method=eth_getLogs status=429',output.getvalue())
            self.assertNotIn('private-key',output.getvalue());self.assertNotIn('token=secret',output.getvalue())

    def test_permanent_403_has_one_attempt_and_sanitized_failure(self):
        import io,tempfile,contextlib
        from unittest.mock import patch
        from urllib.error import HTTPError
        from fetch_borrow_position_pnl import Source
        clock=self.Clock();calls=[]
        def forbidden(*args,**kwargs):
            calls.append(1);raise HTTPError('https://secret.invalid/key',403,'Forbidden https://secret.invalid/key',{},None)
        with tempfile.TemporaryDirectory() as root,patch('fetch_borrow_position_pnl.time.monotonic',clock.monotonic),patch('fetch_borrow_position_pnl.time.sleep',clock.sleep),patch('fetch_borrow_position_pnl.urlopen',forbidden),contextlib.redirect_stdout(io.StringIO()) as output:
            source=Source('berachain',root)
            with self.assertRaises(HTTPError) as error:source.rpc('eth_getLogs',[])
            self.assertEqual(len(calls),1);self.assertEqual(clock.sleeps,[])
            self.assertNotIn('secret.invalid',str(error.exception)+output.getvalue())

    def test_429_exhaustion_is_bounded_and_never_splits_ranges(self):
        import io,tempfile,contextlib
        from unittest.mock import patch
        from urllib.error import HTTPError
        from fetch_borrow_position_pnl import Source
        clock=self.Clock();calls=[]
        def limited(*args,**kwargs):
            calls.append(clock.now);raise HTTPError('https://private.invalid/key',429,'Too Many Requests',{},None)
        with tempfile.TemporaryDirectory() as root,patch('fetch_borrow_position_pnl.time.monotonic',clock.monotonic),patch('fetch_borrow_position_pnl.time.sleep',clock.sleep),patch('fetch_borrow_position_pnl.urlopen',limited),contextlib.redirect_stdout(io.StringIO()):
            source=Source('berachain',root);source.block=lambda n:{'hash':'0xpin'}
            with self.assertRaises(HTTPError):source.log_range((1,9999))
            self.assertEqual(len(calls),8);self.assertLessEqual(clock.now,300)
            self.assertGreaterEqual(clock.now,120)
            self.assertEqual(list(source.cache.glob('*.json')),[])

    def test_four_worker_starts_obey_shared_spacing_and_cooldown(self):
        from concurrent.futures import ThreadPoolExecutor
        from unittest.mock import patch
        from fetch_borrow_position_pnl import RequestPacer
        clock=self.Clock()
        with patch('fetch_borrow_position_pnl.time.monotonic',clock.monotonic),patch('fetch_borrow_position_pnl.time.sleep',clock.sleep):
            pacer=RequestPacer(1.0);pacer.cooldown(7)
            with ThreadPoolExecutor(max_workers=4) as pool:starts=sorted(pool.map(lambda _:pacer.wait(),range(4)))
            self.assertEqual(len(starts),4);self.assertGreaterEqual(starts[0],7)
            self.assertTrue(all(b-a>=1 for a,b in zip(starts,starts[1:])))
            # Another worker cannot shorten an already announced provider pause.
            before=clock.now;pacer.cooldown(30);pacer.cooldown(1)
            self.assertGreaterEqual(pacer.wait(),before+30)

    def test_source_paces_bera_headers_and_logs_without_changing_rpc_responses(self):
        import io,json,tempfile
        from unittest.mock import patch
        from fetch_borrow_position_pnl import Source
        clock=self.Clock();starts=[]
        def reply(*args,**kwargs):
            starts.append(clock.now);return io.BytesIO(json.dumps({'jsonrpc':'2.0','id':1,'result':'0x123'}).encode())
        with tempfile.TemporaryDirectory() as root,patch('fetch_borrow_position_pnl.time.monotonic',clock.monotonic),patch('fetch_borrow_position_pnl.time.sleep',clock.sleep),patch('fetch_borrow_position_pnl.urlopen',reply):
            source=Source('berachain',root)
            for method in ['eth_getBlockByNumber','eth_getLogs','eth_call']:
                self.assertEqual(source.rpc(method,[]),'0x123')
            self.assertEqual(starts,[0,1,2])

    def test_json_rpc_429_shares_the_http_cooldown_budget(self):
        import io,json,tempfile,contextlib
        from unittest.mock import patch
        from fetch_borrow_position_pnl import Source
        clock=self.Clock();starts=[]
        def reply(*args,**kwargs):
            starts.append(clock.now)
            data={'error':{'code':429,'message':'private provider detail'}} if len(starts)<=2 else {'result':'0x456'}
            return io.BytesIO(json.dumps(data).encode())
        with tempfile.TemporaryDirectory() as root,patch('fetch_borrow_position_pnl.time.monotonic',clock.monotonic),patch('fetch_borrow_position_pnl.time.sleep',clock.sleep),patch('fetch_borrow_position_pnl.urlopen',reply),contextlib.redirect_stdout(io.StringIO()) as output:
            source=Source('berachain',root)
            self.assertEqual(source.rpc('eth_call',[]),'0x456');self.assertEqual(starts,[0,5,15])
            self.assertNotIn('private provider detail',output.getvalue())

    def test_429_never_becomes_a_range_split_even_with_timeout_text(self):
        from fetch_borrow_position_pnl import Source
        from urllib.error import HTTPError
        source=Source.__new__(Source);source.margin=OWNER;source.block=lambda n:{'hash':'0xa'}
        calls=[]
        def fail(*args):
            calls.append(args);raise HTTPError('https://example.invalid',429,'query timeout',{},None)
        source.rpc=fail
        with self.assertRaises(HTTPError):source.log_range((1,9999))
        self.assertEqual(len(calls),1)

    def test_retry_after_seconds_date_and_fallback_are_bounded(self):
        from fetch_borrow_position_pnl import rate_retry_delay
        from unittest.mock import patch
        from email.utils import formatdate
        self.assertEqual(rate_retry_delay({'Retry-After':'7'},0),7)
        self.assertEqual(rate_retry_delay({'Retry-After':'900'},0),60)
        for bad in ['invalid','NaN','-9','inf']:
            self.assertEqual(rate_retry_delay({'Retry-After':bad},0),5)
        self.assertEqual(rate_retry_delay({},7),60)
        with patch('fetch_borrow_position_pnl.time.time',return_value=1000):
            self.assertEqual(rate_retry_delay({'Retry-After':formatdate(1012,usegmt=True)},0),12)

if __name__=='__main__': unittest.main()
