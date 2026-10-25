import unittest
from fetch_veborrow_rebate_history import build_chain, summarize_snapshot

def snapshot(epoch, amount, stamp='2026-06-01T00:00:00Z', sha='a'*40):
    return {'epoch': epoch, 'totals': {'0': str(amount)}, 'roots': {'0': '0x'+'1'*64},
            'commit': sha, 'committedAt': stamp, 'sha256': 'f'*64, 'totalUsers': 1}

class RebateHistoryTests(unittest.TestCase):
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
        data={'metadata':{'epoch':1,'totalUsers':1,'marketToTotalRebate':{'0':'99'},'marketToMerkleRoot':{'0':'0x'+'1'*64}},'users':{'wallet':{'0':{'amount':'100'}}}}
        self.assertEqual(summarize_snapshot(data,'a'*40,'2026-06-01T00:00:00Z','f'*64)['totals'],{'0':'99'})
        data['metadata']['marketToTotalRebate']['0']='1e18'
        with self.assertRaises(ValueError):summarize_snapshot(data,'a'*40,'date','hash')
        data['metadata']['marketToTotalRebate']['0']='50'
        with self.assertRaisesRegex(ValueError,'rounding'):summarize_snapshot(data,'a'*40,'date','hash')

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
