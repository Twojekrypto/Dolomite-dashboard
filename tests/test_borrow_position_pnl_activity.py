"""Activity must preserve raw positions, pinned marks and immutable parent links."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import borrow_pnl_activity as activity
from fetch_borrow_position_pnl import endpoint

OWNER='0x'+'1'*40
TX='0x'+'a'*64
HASH='0x'+'b'*64
INDEX=10**18
PRICE=10**36
TOKENS={'1':{'id':'0x'+'2'*40,'symbol':'UNIT','decimals':'0','marketId':'1'}}
MARKS={'1':{'supply':INDEX,'borrow':INDEX,'price':PRICE}}

def row_fixture(opening=None,changes=None):
    opening={'1':-10} if opening is None else opening
    changes=[(15,5),( -5,0),(-3,-3)] if changes is None else changes
    events=[];contexts={};closing=dict(opening)
    for n,(delta,par) in enumerate(changes,1):
        events.append(dict(kind='deposit' if delta>=0 else 'withdraw',txHash=TX,blockHash=HASH,blockNumber=12,txIndex=0,logIndex=n*2,timestamp=102,legs=[dict(owner=OWNER,account='7',market='1',delta=delta,new_par=par)]))
        contexts[(TX,n*2)]={'id':TX+':'+hex(n*2-1),'indices':{'1':{'supply':INDEX,'borrow':INDEX}},'prices':{'1':PRICE}}
        if par:closing['1']=par
        else:closing.pop('1',None)
    a=endpoint(opening,MARKS,TOKENS);z=endpoint(closing,MARKS,TOKENS)
    return opening,closing,events,contexts,a,z

def export_fixture():
    op,cp,events,contexts,a,z=row_fixture()
    return activity.export_position(op,cp,events,OWNER,'7',contexts,a,z,MARKS,MARKS)

def snapshot_fixture(item):
    manifests={}
    for n,(chain,chain_id) in enumerate((('ethereum',1),('arbitrum',42161),('berachain',80094)),1):
        manifests[chain]=dict(chainId=chain_id,opening={'number':10,'hash':'0x'+str(n)*64,'timestamp':100},closing={'number':20,'hash':'0x'+str(n+3)*64,'timestamp':200},sourceCacheManifestHash=str(n)*64)
    row=dict(chain='ethereum',accountId=OWNER+'-7',eventCount=len(item['events']),closed=False,openingBalances=[{k:v for k,v in b.items() if k in ('marketId','address','symbol','decimals','usdExact')} for b in item['openingBalances']],closingBalances=[{k:v for k,v in b.items() if k in ('marketId','address','symbol','decimals','usdExact')} for b in item['closingBalances']])
    snapshot=dict(windowSeconds=2592000,maxAgeSeconds=21600,sourceAsOf=200,chains=manifests,positions=[row])
    chains={c:{'tokens':TOKENS,'positions':{}} for c in manifests};chains['ethereum']['positions'][row['accountId']]=item
    return snapshot,chains

class ActivityTests(unittest.TestCase):
    def test_optional_bad_timestamp_does_not_break_existing_balance_decode(self):
        from fetch_borrow_position_pnl import decode_event
        from test_borrow_position_pnl_sources import log
        for value in (None,123,'not-hex'):
            source=log();source['blockTimestamp']=value
            event=decode_event(source)
            self.assertIsNone(event['timestamp'])
            self.assertEqual(event['legs'][0]['new_par'],10)
        source=log();source['blockTimestamp']='0x6ae92e80'
        self.assertEqual(decode_event(source)['timestamp'],1793666688)

    def test_export_replays_cross_zero_closure_and_reopening_without_merging_accounts(self):
        op,cp,events,ctx,a,z=row_fixture()
        events[0]['legs'].append(dict(owner=OWNER,account='8',market='1',delta=999,new_par=999))
        item=activity.export_position(op,cp,events,OWNER,'7',ctx,a,z,MARKS,MARKS)
        self.assertEqual(item['events'][0]['legs'][0]['beforeWeiRawSigned'],'-10')
        self.assertEqual(item['events'][0]['legs'][0]['afterWeiRawSigned'],'5')
        self.assertEqual(item['events'][0]['legs'][0]['deltaUsdExact'],'15')
        self.assertEqual([e['closedAfter'] for e in item['events']],[False,True,False])
        self.assertEqual([len(e['legs']) for e in item['events']],[1,1,1])
        self.assertEqual(item['closingBalances'][0]['weiRawSigned'],'-3')

    def test_emitted_delta_survives_par_rounding_residue(self):
        op,cp,events,ctx,a,z=row_fixture({'1':1},[(1,2)])
        marks={'1':dict(supply=15*10**17,borrow=INDEX,price=PRICE)}
        ctx[(TX,2)]['indices']['1']['supply']=marks['1']['supply']
        a=endpoint(op,marks,TOKENS);z=endpoint(cp,marks,TOKENS)
        item=activity.export_position(op,cp,events,OWNER,'7',ctx,a,z,marks,marks)
        leg=item['events'][0]['legs'][0]
        self.assertEqual((leg['beforeWeiRawSigned'],leg['afterWeiRawSigned'],leg['deltaWeiRawSigned']),('2','3','1'))
        # 3Wei becomes2Par and back3Wei; use another valid residue at1.2index.
        marks['1']['supply']=12*10**17;ctx[(TX,2)]['indices']['1']['supply']=12*10**17
        op={'1':2};cp={'1':3};events[0]['legs'][0].update(delta=1,new_par=3)
        item=activity.export_position(op,cp,events,OWNER,'7',ctx,endpoint(op,marks,TOKENS),endpoint(cp,marks,TOKENS),marks,marks)
        leg=item['events'][0]['legs'][0]
        self.assertEqual((leg['beforeWeiRawSigned'],leg['afterWeiRawSigned'],leg['deltaWeiRawSigned']),('2','4','1'))

    def test_vaporized_target_keeps_authoritative_new_par_despite_rounding(self):
        op={'1':-55290379817575989};cp={'1':-677500958560}
        marks={'1':dict(supply=1110916191087238337,borrow=1181351018138352545,price=999679000000000000)}
        event=dict(kind='vaporization',txHash=TX,blockHash=HASH,blockNumber=12,txIndex=0,logIndex=2,timestamp=102,legs=[dict(owner=OWNER,account='7',market='1',delta=65316546124302431,new_par=-677500958560)])
        ctx={(TX,2):{'id':TX+':0x1','indices':{'1':{'supply':1110916191087238337,'borrow':1181351018138352545}},'prices':{'1':999679000000000000}}}
        item=activity.export_position(op,cp,[event],OWNER,'7',ctx,endpoint(op,marks,TOKENS),endpoint(cp,marks,TOKENS),marks,marks)
        snapshot,chains=snapshot_fixture(item)
        with tempfile.TemporaryDirectory() as tmp:
            activity.write_bundle(snapshot,chains,tmp)
            self.assertTrue(activity.validate_bundle(snapshot,tmp))
        leg=item['events'][0]['legs'][0]
        self.assertEqual((leg['beforeWeiRawSigned'],leg['afterWeiRawSigned']),('-65317346490749614','-800366447185'))

    def test_no_events_and_zero_balances_are_valid_history(self):
        op,cp,events,ctx,a,z=row_fixture({},[])
        self.assertEqual(activity.export_position(op,cp,events,OWNER,'7',ctx,a,z,MARKS,MARKS),{'openingBalances':[],'closingBalances':[],'events':[]})

    def test_missing_exact_source_marks_and_timestamp_fail_closed(self):
        for mutation in ('timestamp','index','price','ending'):
            op,cp,events,ctx,a,z=row_fixture()
            if mutation=='timestamp':events[0]['timestamp']=None
            if mutation=='index':ctx[(TX,2)]['indices'].clear()
            if mutation=='price':ctx[(TX,2)]['prices']['1']=0
            if mutation=='ending':cp={}
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):
                activity.export_position(op,cp,events,OWNER,'7',ctx,a,z,MARKS,MARKS)

    def test_unpriced_history_is_unavailable_without_changing_financial_rows(self):
        snapshot,chains=snapshot_fixture(export_fixture())
        item={'status':'unavailable','reasonCodes':['missing_boundary_valuation'],'openingBalances':[],'closingBalances':[],'events':[]}
        chains['ethereum']['positions'][snapshot['positions'][0]['accountId']]=item
        original=copy.deepcopy(snapshot)
        with tempfile.TemporaryDirectory() as tmp:
            activity.write_bundle(snapshot,chains,tmp)
            self.assertTrue(activity.validate_bundle(snapshot,tmp))
        snapshot.pop('activity');snapshot['positions'][0].pop('activityBucket')
        self.assertEqual(snapshot,original)

    def test_bundle_has_48_immutable_files_and_compact_snapshot_refs(self):
        snapshot,chains=snapshot_fixture(export_fixture());original=copy.deepcopy(snapshot)
        with tempfile.TemporaryDirectory() as tmp:
            activity.write_bundle(snapshot,chains,tmp)
            self.assertEqual(len(snapshot['activity']['files']),48)
            self.assertEqual(len(list(Path(tmp).rglob('*.json'))),48)
            self.assertEqual(len(snapshot['positions'][0]['activityBucket']),1)
            self.assertTrue(activity.validate_bundle(snapshot,tmp))
            first={str(p.relative_to(tmp)):p.read_bytes() for p in Path(tmp).rglob('*.json')}
            activity.write_bundle(snapshot,chains,tmp)
            self.assertEqual(first,{str(p.relative_to(tmp)):p.read_bytes() for p in Path(tmp).rglob('*.json')})
        snapshot.pop('activity');snapshot['positions'][0].pop('activityBucket')
        self.assertEqual(snapshot,original)

    def test_sidecar_tampering_rejected_even_if_attacker_updates_file_hash(self):
        for change in ('hash','boundary','timestamp','source','par','count','closing','operation','delta','price'):
            snapshot,chains=snapshot_fixture(export_fixture())
            with tempfile.TemporaryDirectory() as tmp:
                activity.write_bundle(snapshot,chains,tmp);row=snapshot['positions'][0];ref=snapshot['activity']['files']['ethereum-'+row['activityBucket']];p=Path(tmp)/ref['path'];data=json.loads(p.read_text());item=data['positions'][row['accountId']]
                if change=='boundary':data['closing']['hash']='0x'+'f'*64
                if change=='source':data['sourceCacheManifestHash']='f'*64
                if change=='timestamp':item['events'][0]['timestamp']=99
                if change=='par':item['events'][0]['legs'][0]['beforeParRawSigned']='-11'
                if change=='count':item['events'].pop()
                if change=='closing':item['closingBalances'][0]['parRawSigned']='-4'
                if change=='operation':item['events'][0]['operationRef']=TX+':0xff'
                if change=='delta':item['events'][0]['legs'][0]['deltaWeiRawSigned']='16'
                if change=='price':item['events'][0]['legs'][0]['oraclePriceRaw']='0'
                p.write_text(json.dumps(data,separators=(',',':')))
                if change!='hash':ref['sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
                with self.subTest(change=change),self.assertRaises(ValueError):activity.validate_bundle(snapshot,tmp)

    def test_metadata_rejects_path_traversal_and_wrong_bucket(self):
        for change in ('path','bucket','generation'):
            snapshot,chains=snapshot_fixture(export_fixture())
            with tempfile.TemporaryDirectory() as tmp:
                activity.write_bundle(snapshot,chains,tmp)
                if change=='path':next(iter(snapshot['activity']['files'].values()))['path']='../../outside.json'
                if change=='bucket':snapshot['positions'][0]['activityBucket']='xx'
                if change=='generation':snapshot['activity']['generation']='f'*24
                with self.assertRaises(ValueError):activity.validate_metadata(snapshot)

    def test_retention_keeps_valid_parents_and_latest_four_generations(self):
        with tempfile.TemporaryDirectory() as tmp:
            parents=[]
            for n in range(7):
                snapshot,chains=snapshot_fixture(export_fixture())
                for m in snapshot['chains'].values():m['closing']['hash']='0x'+format(n+1,'064x');m['closing']['timestamp']=100000+n*7200
                snapshot['sourceAsOf']=100000+n*7200
                activity.write_bundle(snapshot,chains,tmp);parents.append(copy.deepcopy(snapshot))
            # Rapid manual collections must retain all still-valid parents.
            activity.prune_generations(tmp,now=120000)
            self.assertEqual(len(list((Path(tmp)/activity.DEFAULT_DIRECTORY).iterdir())),7)
            activity.prune_generations(tmp,now=143201)
            dirs=list((Path(tmp)/activity.DEFAULT_DIRECTORY).iterdir())
            self.assertEqual(len(dirs),4)
            for snapshot in parents[-4:]:self.assertTrue(activity.validate_bundle(snapshot,tmp))


if __name__=='__main__':unittest.main()
