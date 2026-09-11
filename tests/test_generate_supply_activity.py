import os
import sys
import unittest
import tempfile
import io
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_supply_activity as gsa


class GenerateSupplyActivityTests(unittest.TestCase):
    def test_failed_run_returns_nonzero_without_promoting_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            gsa.write_activity_file(out,'ethereum',{'id':'selected'},[],gsa.utc_now())
            # Establish a real successful manifest through the public entrypoint.
            args=['generate_supply_activity.py','--out-dir',directory,'--chains','ethereum','--symbols','']
            success={'chain':'ethereum','tokens':[],'tokensWritten':0,'tokensSkipped':0,'skippedTokens':[]}
            with patch.object(sys,'argv',args),patch.object(gsa,'generate_chain',return_value=success):
                self.assertEqual(gsa.main(),0)
            previous=(out/'manifest.json').read_bytes()
            failed=dict(success,tokensSkipped=1,skippedTokens=[{'tokenId':'selected','detail':'page cap exceeded'}])
            error=io.StringIO()
            with patch.object(sys,'argv',args),patch.object(gsa,'generate_chain',return_value=failed),redirect_stderr(error):
                self.assertNotEqual(gsa.main(),0)
            self.assertEqual((out/'manifest.json').read_bytes(),previous)
            self.assertIn('ethereum',error.getvalue())
            self.assertIn('selected',error.getvalue())
            self.assertIn('page cap exceeded',error.getvalue())

    def test_compact_preserves_semantics(self):
        metadata={'version':2,'actions':['repay'],'status':'verified'}
        self.assertEqual(gsa.compact_activity_row({'semantics':metadata})[8:], [metadata])

    def test_pinned_generator_replays_repayment_and_validates_baseline(self):
        def response(endpoint,payload):
            query=payload['query']
            if '_meta' in query: return {'_meta':{'block':{'number':42},'hasIndexingErrors':False}}
            if 'token(id:' in query: return {'token':{'totalPar':{'supplyPar':'0','borrowPar':'4'}}}
            self.assertIn('block: { number: 42 }',query)
            if 'marginAccountTokenValues(' in query:
                return {'marginAccountTokenValues':[{'id':'balance','marginAccount':{'id':'a'},'valuePar':'-4'}]}
            for entity in ['deposits','withdrawals','transfers','trades','liquidations','vaporizations']:
                if entity+'(' in query:
                    rows=[]
                    if entity=='deposits':
                        rows=[{'id':'e','serialId':'1','logIndex':'1','transaction':{'id':'tx','timestamp':'5','blockNumber':'40','zaps':[]},'marginAccount':{'id':'a'},'effectiveUser':{'id':'w'},'amountDeltaPar':'6','amountDeltaWei':'7.2','interestIndex':{'supplyIndex':'1.1','borrowIndex':'1.2'}}]
                    return {entity:rows}
            self.fail(query)
        with patch.object(gsa,'post_json',side_effect=response):
            rows=gsa.fetch_activity_rows('endpoint','selected',3,since_ts=1)
        self.assertEqual(rows[0][8]['debtChange'],'-7.2')
        self.assertIn('repay',rows[0][8]['actions'])

    def test_pagination_rejects_missing_entity(self):
        with patch.object(gsa,'post_json',return_value={}):
            with self.assertRaises(RuntimeError):
                gsa.paginate_entity('endpoint','deposits','','id')

    def test_fetch_failure_retains_published_market(self):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            token={'id':'selected','symbol':'LINK'}
            path,_,_=gsa.write_activity_file(out,'ethereum',token,[],gsa.utc_now())
            previous=path.read_bytes()
            with patch.object(gsa,'fetch_tokens',return_value=[token]),patch.object(gsa,'fetch_activity_rows',side_effect=RuntimeError('incomplete page')):
                result=gsa.generate_chain('ethereum','endpoint',out,1,set(),set(),False,3,gsa.utc_now(),False)
            self.assertEqual(path.read_bytes(),previous)
            self.assertEqual(result['tokensSkipped'],1)
            self.assertEqual(result['tokensWritten'],0)

    def test_pagination_pins_every_page_and_rejects_duplicate(self):
        queries=[]
        def request(endpoint,payload):
            queries.append(payload['query'])
            return {'deposits':[{'id':'a'}]}
        with patch.object(gsa,'post_json',side_effect=request):
            with self.assertRaisesRegex(RuntimeError,'Duplicate'):
                gsa.paginate_entity('endpoint','deposits','','id',page_size=1,max_pages=3,block_number=42)
        self.assertTrue(all('block: { number: 42 }' in query for query in queries))

    def test_default_chain_list_skips_archived_chains(self):
        for chain in ("polygon_zkevm", "botanix"):
            self.assertIn(chain, gsa.GRAPH_ENDPOINTS)
            self.assertIn(chain, gsa.RETIRED_GRAPH_CHAINS)
            self.assertNotIn(chain, gsa.DEFAULT_GRAPH_CHAINS)


if __name__ == "__main__":
    unittest.main()
