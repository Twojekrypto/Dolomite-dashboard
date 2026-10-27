import unittest
from unittest.mock import patch
from eth_hash.auto import keccak
from copy import deepcopy
import fetch_veborrow_rebate_history as producer
import json
from pathlib import Path
from fetch_veborrow_rebate_history import build_chain, summarize_snapshot, VERIFIED_RECOVERIES, fetch_snapshots

def source_snapshot(amounts=(100, 200, 300)):
    users = {}
    leaves = []
    for i, amount in enumerate(amounts, 1):
        address = '0x' + f'{i:040x}'
        leaf = keccak(bytes.fromhex(address[2:]).rjust(32, b'\0') + amount.to_bytes(32, 'big'))
        users[address] = {'0': {'amount': str(amount), 'leaf': '0x' + leaf.hex()}}
        leaves.append(leaf)
    leaves.sort()
    root = keccak(b''.join(sorted([keccak(b''.join(leaves[:2])), leaves[2]]))) if len(leaves) == 3 else leaves[0]
    return {'metadata': {'epoch': 2, 'totalUsers': len(users), 'marketToTotalRebate': {'0': str(sum(amounts))}, 'marketToMerkleRoot': {'0': '0x'+root.hex()}}, 'users': users}

def snapshot(epoch, amount, stamp='2026-06-01T00:00:00Z', sha='a'*40):
    return {'epoch': epoch, 'totals': {'0': str(amount)}, 'roots': {'0': '0x'+'1'*64},
            'commit': sha, 'committedAt': stamp, 'sha256': 'f'*64, 'totalUsers': 1}

class RebateHistoryTests(unittest.TestCase):
    def interest(self, found='40', expected='40'):
        return {'metadata': {'epoch': 1, 'claimStartTimestamp': 100, 'claimEndTimestamp': 604900,
                'marketTotalBorrowInterest': {'0': '200000000'}, 'marketExpectedTotalRevenue': {'0': expected},
                'marketFoundTotalRevenue': {'0': found}, 'marketPrices': {'0': '1000000000000000000000000000000'}}}

    def test_official_maximum_uses_interest_not_protocol_revenue_and_five_percent_tolerance(self):
        self.assertTrue(hasattr(producer, 'summarize_official_interest'), 'official per-market interest validation is missing')
        result = producer.summarize_official_interest(self.interest('38', '40'), 1, 'url', 'f'*64)
        self.assertEqual(result['eligibleBorrowInterestUSD'], 200)
        self.assertEqual(result['maximumRebateUSD'], 20)
        self.assertEqual(result['maximumRebateUSD']*52*5, 5200)
        self.assertEqual(result['marketRevenueFactors'], {'0': 1})
        scaled = producer.summarize_official_interest(self.interest('37.9', '40'), 1, 'url', 'f'*64)
        self.assertEqual(scaled['maximumRebateUSD'], 18.95)

    def test_official_interest_rejects_missing_prices_markets_and_invalid_windows(self):
        self.assertTrue(hasattr(producer, 'summarize_official_interest'))
        for field, value in [('marketPrices', {}), ('marketFoundTotalRevenue', {}), ('claimEndTimestamp', 100), ('epoch', 2)]:
            data = deepcopy(self.interest()); data['metadata'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                producer.summarize_official_interest(data, 1, 'url', 'f'*64)
        data=self.interest('-1','40')
        with self.assertRaises(ValueError): producer.summarize_official_interest(data, 1, 'url', 'f'*64)

    def test_captured_arbitrum22_and_berachain5_official_per_market_maxima(self):
        fixtures=json.loads((Path(__file__).parent/'fixtures/official_rebate_interest.json').read_text())
        for name, fixture in fixtures.items():
            with self.subTest(period=name):
                result=producer.summarize_official_interest(fixture,fixture['metadata']['epoch'],'url','f'*64)
                self.assertAlmostEqual(result['maximumRebateUSD'],fixture['expectedMaximumRebateUSD'],places=9)
                self.assertAlmostEqual(result['maximumRebateUSD'],sum(result['marketMaximumRebateUSD'].values()),places=9)

    def test_enrichment_keeps_signed_allocations_reference_prices_and_chart_values(self):
        chain=build_chain([snapshot(1,100)],1,1,100,{'0':'1'})
        chain['chainId']=80094
        before=deepcopy(chain)
        result=producer.enrich_official_interest({'chains':{'Berachain':chain}},lambda *_:(self.interest(),'f'*64,'url'))
        enriched=result['chains']['Berachain']
        self.assertEqual(enriched['referencePrices'],before['referencePrices'])
        self.assertEqual(enriched['totalsRaw'],before['totalsRaw'])
        self.assertEqual({key:value for key,value in enriched['epochs'][0].items() if key!='officialInterest'},before['epochs'][0])
        data=self.interest();data['metadata']['marketPrices']={}
        with self.assertRaises(ValueError):producer.enrich_official_interest({'chains':{'Berachain':chain}},lambda *_:(data,'f'*64,'url'))

    def test_same_epoch_revision_replaces_and_negative_correction_is_retained(self):
        chain=build_chain([snapshot(1,100),snapshot(2,200),snapshot(2,50,'2026-06-02T00:00:00Z','b'*40),snapshot(3,80)],1,3,0,{'0':'1000000000000000000000000000000000000'})
        self.assertEqual([r['deltaRaw']['0'] for r in chain['epochs']],['100','-50','30'])
        self.assertEqual(chain['epochs'][1]['revisionCount'],2)
        self.assertEqual(chain['epochs'][1]['status'],'correction')
        self.assertEqual(chain['totalsRaw'],{'0':'80'})
        self.assertTrue(chain['reconciled'])

    def test_missing_epoch_is_unknown_and_next_delta_is_catchup(self):
        chain=build_chain([snapshot(1,100),snapshot(3,180)],1,3,0,{'0':'1'})
        self.assertEqual(chain['missingEpochs'],[2])
        self.assertIsNone(chain['epochs'][1]['deltaUSD'])
        self.assertEqual(chain['epochs'][2]['status'],'catchup')
        self.assertEqual(chain['epochs'][2]['coversEpochs'],[2,3])

    def test_missing_price_is_rejected_instead_of_valued_at_zero(self):
        with self.assertRaisesRegex(ValueError,'price'):build_chain([snapshot(1,100)],1,1,0,{})
        with self.assertRaisesRegex(ValueError,'price'):build_chain([snapshot(1,100)],1,1,0,{'0':'0'})

    def test_raw_integer_validation_and_user_rounding_bound(self):
        data=source_snapshot((100,))
        data['metadata']['marketToTotalRebate']['0']='99'
        self.assertEqual(summarize_snapshot(data,'a'*40,'2026-06-01T00:00:00Z','f'*64)['totals'],{'0':'99'})
        data['metadata']['marketToTotalRebate']['0']='1e18'
        with self.assertRaises(ValueError):summarize_snapshot(data,'a'*40,'date','hash')
        data['metadata']['marketToTotalRebate']['0']='50'
        with self.assertRaisesRegex(ValueError,'rounding'):summarize_snapshot(data,'a'*40,'date','hash')

    def test_verified_recovery_requires_exact_audited_chain_epoch_and_digest(self):
        data=source_snapshot(); data['metadata']['marketToTotalRebate']['0']='9000'
        proof={'epoch':2,'sha256':'digest','block':123,'transaction':'0x'+'a'*64}
        with patch.dict(VERIFIED_RECOVERIES, {'commit':proof}, clear=True):
            result=summarize_snapshot(data,'commit','date','digest',chain='80094')
            self.assertEqual(result['totals'],{'0':'600'})
            self.assertEqual(result['reportedTotals'],{'0':'9000'})
            self.assertEqual(result['recovery'],proof)
            self.assertTrue(result['merkleVerified'])
            for chain,digest,epoch in [('42161','digest',2),('80094','wrong',2),('80094','digest',3)]:
                data['metadata']['epoch']=epoch
                with self.assertRaisesRegex(ValueError,'rounding'):
                    summarize_snapshot(data,'commit','date',digest,chain=chain)

    def test_tampered_leaf_root_or_wallet_cannot_be_recovered(self):
        from copy import deepcopy
        base=source_snapshot(); base['metadata']['marketToTotalRebate']['0']='9000'
        proof={'epoch':2,'sha256':'digest','block':123,'transaction':'tx'}
        for kind in ['amount','leaf','root','address','duplicate']:
            data=deepcopy(base); wallet=next(iter(data['users']))
            if kind=='amount':data['users'][wallet]['0']['amount']='101'
            if kind=='leaf':data['users'][wallet]['0']['leaf']='0x'+'f'*64
            if kind=='root':data['metadata']['marketToMerkleRoot']['0']='0x'+'f'*64
            if kind=='address':data['users']['invalid']=data['users'].pop(wallet)
            if kind=='duplicate':
                wallet='0x'+'a'*40
                data['users'][wallet]=data['users'][next(iter(data['users']))]
                data['users'][wallet.upper()]=data['users'][wallet]
            with self.subTest(kind=kind),patch.dict(VERIFIED_RECOVERIES,{'commit':proof},clear=True):
                with self.assertRaises(ValueError):summarize_snapshot(data,'commit','date','digest',chain='80094')

    def test_old_cached_validation_is_not_reused(self):
        data=source_snapshot(); sha='a'*40
        commits=[{'sha':sha,'commit':{'committer':{'date':'date'}}}]
        with patch('fetch_veborrow_rebate_history.read_json',side_effect=[(commits,'hash'),(data,'digest')]) as read:
            result=fetch_snapshots('80094',[{'commit':sha,'invalid':'old validation'}])
        self.assertEqual(read.call_count,2)
        self.assertTrue(result[0]['merkleVerified'])
        self.assertEqual(result[0]['validationVersion'],2)

    def test_large_wei_and_late_market_are_exact(self):
        large=9007199254740993123456789
        first=snapshot(1,large);second=snapshot(2,large+1)
        second['totals']['1']='2';second['roots']['1']='0x'+'2'*64
        result=build_chain([first,second],1,2,0,{'0':'1','1':'1'})
        self.assertEqual(result['epochs'][1]['deltaRaw'],{'0':'1','1':'2'})
        self.assertEqual(result['totalsRaw']['0'],str(large+1))

    def test_future_unpublished_snapshot_is_excluded(self):
        result=build_chain([snapshot(1,100),snapshot(2,200)],1,1,0,{'0':'1'})
        self.assertEqual(result['totalsRaw'],{'0':'100'})

    def test_invalid_official_revision_creates_gap_and_latest_invalid_fails(self):
        invalid={'epoch':2,'invalid':'aggregate mismatch','committedAt':'2026-06-02T00:00:00Z','commit':'b'*40}
        result=build_chain([snapshot(1,100),snapshot(2,120),invalid,snapshot(3,150)],1,3,0,{'0':'1'})
        self.assertEqual(result['missingEpochs'],[2])
        self.assertEqual(result['epochs'][2]['status'],'catchup')
        with self.assertRaises(ValueError):build_chain([snapshot(1,100),invalid],1,2,0,{'0':'1'})

if __name__=='__main__':unittest.main()

