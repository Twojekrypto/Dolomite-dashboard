import copy,json,unittest,hashlib,io,tempfile
from unittest.mock import patch
from pathlib import Path
from datetime import datetime,timezone
from borrow_pnl_oracle_recovery import recover_price,plutus_formula,validate_reconstruction,RecoveryRejected
ROOT=Path(__file__).parent.parent
class FixtureRpcRevert(ValueError):
 def __init__(self,response):
  e=response['error'];self.code=e.get('code');self.message=e.get('message');self.data=e.get('data');self.response_hash=hashlib.sha256(json.dumps(response,sort_keys=True,separators=(',',':')).encode()).hexdigest();super().__init__(str(e)[:250])
class Replay:
 def __init__(self):
  f=json.loads((ROOT/'tests/fixtures/borrow_pnl_oracle_recovery.json').read_text());self.data={json.dumps([r['request']['method'],r['request']['params']],sort_keys=True):r['response'] for r in f['calls']};self.changes={}
  for b in [510338545,519668970]:
   for a,code in f['runtimeCodeByAddress'].items():self.data[json.dumps(['eth_getCode',[a,hex(b)]],sort_keys=True)]={'id':1,'jsonrpc':'2.0','result':code}
 def response(self,method,params):
  k=json.dumps([method,params],sort_keys=True);r=self.changes.get(k,self.data.get(k))
  if r is None:raise AssertionError('Fixture absent '+k)
  return r
 def __call__(self,method,params,anchor=None):
  r=self.response(method,params)
  if 'error' in r:raise FixtureRpcRevert(r)
  return r['result']
 def replace(self,method,params,result):self.changes[json.dumps([method,params],sort_keys=True)]={'id':1,'jsonrpc':'2.0','result':result}
def h(block):return {'number':block,'hash':{510338545:'0x78186bba0a484b7e0f591edf77c98f32f6717961e01896fad37ba3fc61bad2c2',519668970:'0x726e34de6f9c5b6125f2edb60f77a33fe771f29ac80018523232a7f7f7573a40'}[block],'timestamp':1790769328 if block==510338545 else 1793361328}
TOKENS={9:'0x5c80ac681b6b0e7ef6e0751211012601e6cfb043',41:'0x0c4d46076af67f8ba1cc3c01f7e873bd91ea41ab'}
MARGIN='0x6bd780e7fdf01d77e4d475c821f1e7ae05409072'
def recover(rpc,market=9,b=510338545,**kw):return recover_price(rpc,chain_id=kw.get('chain_id',42161),margin=MARGIN,market_id=market,token=kw.get('token',TOKENS[market]),decimals=18,header=h(b))
class RecoveryTests(unittest.TestCase):
 def test_exact_integer_formula_preserves_solidity_floor_order(self):
  self.assertEqual(plutus_formula(19,13,7,201),35)
  huge=10**65+19;self.assertEqual(plutus_formula(huge,13,7,201),(huge*13//7)-((huge*13//7)*201//10000))
 def test_captured_archive_responses_recover_four_exact_formula_quotes(self):
  expected={(9,510338545):122995496072081441,(9,519668970):119093038767710809,(41,510338545):90572655642288893,(41,519668970):87698925035272572}
  for (m,b),want in expected.items():
   with self.subTest(market=m,block=b):
    result=recover(Replay(),m,b);self.assertEqual(result['price'],want);p=result['proof'];self.assertEqual(p['blockHash'],h(b)['hash']);self.assertFalse(p['provesExecutableExit']);self.assertTrue(p['directFailure']);self.assertTrue(p['calls'])
 def test_wrong_chain_or_token_cannot_trigger_reconstruction(self):
  for kwargs in [{'chain_id':1},{'token':TOKENS[41]}]:
   with self.subTest(kwargs=kwargs),self.assertRaises(RecoveryRejected):recover(Replay(),**kwargs)
 def test_provider_failure_does_not_turn_into_a_quote(self):
  rpc=Replay();k=json.dumps(['eth_call',[{'to':MARGIN,'data':'0x8928378e'+format(9,'064x')},hex(510338545)]],sort_keys=True);rpc.changes[k]={'error':{'code':429,'message':'rate limited'}}
  with self.assertRaises(RecoveryRejected):recover(rpc)
 def test_unexpected_success_does_not_take_fallback(self):
  rpc=Replay();rpc.replace('eth_call',[{'to':MARGIN,'data':'0x8928378e'+format(9,'064x')},hex(510338545)],'0x'+format(1,'064x'))
  with self.assertRaises(RecoveryRejected):recover(rpc)
 def test_changed_contract_code_fails_closed(self):
  rpc=Replay();rpc.replace('eth_getCode',['0x11e4e70b4ac087624f55d1dccab306440250a3a0',hex(510338545)],'0x6000')
  with self.assertRaises(RecoveryRejected):recover(rpc)
 def test_malformed_or_zero_legacy_quote_fails_closed(self):
  for raw in ['0x01','0x'+format(0,'064x'),'0x'+format(1,'064x')]:
   rpc=Replay();rpc.replace('eth_call',[{'to':'0x11e4e70b4ac087624f55d1dccab306440250a3a0','data':'0x41976e09'+TOKENS[9][2:].zfill(64)},hex(510338545)],raw)
   with self.subTest(raw=raw),self.assertRaises(RecoveryRejected):recover(rpc)
 def test_changed_active_oracle_configuration_fails_closed(self):
  rpc=Replay();rpc.replace('eth_call',[{'to':MARGIN,'data':'0x13368364'+format(9,'064x')},hex(510338545)],'0x'+'0'*64)
  with self.assertRaises(RecoveryRejected):recover(rpc)
 def test_direct_failure_record_remains_separate_from_positive_alternative(self):
  result=recover(Replay(),41);self.assertIn('Price cannot be zero',result['proof']['directFailure']);self.assertEqual(result['proof']['method'],'official_contract_formula_reconstruction_v1');self.assertEqual(result['proof']['inputs']['ptToAssetRate'],'1000000000000000000')
 def test_unstructured_revert_text_cannot_supply_provenance(self):
  def unstructured(method,params,anchor=None):raise ValueError("{'code': 3, 'message': 'execution reverted: ChainlinkAutomationPriceOracle: Price is expired'}")
  with self.assertRaises(RecoveryRejected):recover(unstructured)
 def test_invalid_exact_inputs_fail_closed(self):
  for args in [(1,1,0,0),(1,1,1,10001),(1,-1,1,0),(1,1.0,1,0)]:
   with self.subTest(args=args),self.assertRaises(RecoveryRejected):plutus_formula(*args)
 def test_source_rpc_request_integration_uses_anchored_immutable_cache(self):
  import fetch_borrow_position_pnl as producer
  replay=Replay()
  def transport(request,timeout=None):
   q=json.loads(request.data);return io.BytesIO(json.dumps(replay.response(q['method'],q['params'])).encode())
  with tempfile.TemporaryDirectory() as cache,patch.object(producer,'urlopen',side_effect=transport),patch.object(producer.time,'sleep'):
   source=producer.Source('arbitrum',cache);result=recover(source.rpc)
   self.assertEqual(result['price'],122995496072081441);self.assertTrue(source.refs)
   calls_before=source.requests;result2=recover(source.rpc);self.assertEqual(result2,result);self.assertEqual(source.requests,calls_before)
 def test_offline_validator_recomputes_both_reconstructed_quote_types(self):
  for market in [9,41]:
   result=recover(Replay(),market);self.assertEqual(validate_reconstruction(result['proof']),result['price'])
 def test_offline_validator_rejects_changed_formula_or_provenance(self):
  base=recover(Replay())['proof']
  changes=[('price','1'),('method','unproved'),('token',TOKENS[41]),('provesExecutableExit',True),('chainId',1),('directFailure','provider unavailable')]
  for key,value in changes:
   p=copy.deepcopy(base);p[key]=value
   with self.subTest(key=key),self.assertRaises(RecoveryRejected):validate_reconstruction(p)
  for key,value in [('shares','0'),('glpQuote','1'),('exitFeeBp','10001')]:
   p=copy.deepcopy(base);p['inputs'][key]=value
   with self.subTest(input=key),self.assertRaises(RecoveryRejected):validate_reconstruction(p)
 def test_validator_rejects_rounding_invisible_input_tampering(self):
  p=copy.deepcopy(recover(Replay())['proof']);p['inputs']['assets']=str(int(p['inputs']['assets'])+1)
  with self.assertRaises(RecoveryRejected):validate_reconstruction(p)
 def test_validator_rejects_wrong_runtime_hash_or_call_block(self):
  base=recover(Replay())['proof'];p=copy.deepcopy(base);p['runtimeCodeHashes'][next(iter(p['runtimeCodeHashes']))]='0'*64
  with self.assertRaises(RecoveryRejected):validate_reconstruction(p)
  p=copy.deepcopy(base);p['calls'][0]['params'][1]=hex(519668970)
  with self.assertRaises(RecoveryRejected):validate_reconstruction(p)
 def test_validator_binds_direct_error_to_canonical_rpc_envelope_hash(self):
  p=copy.deepcopy(recover(Replay())['proof']);p['directResponseHash']='0'*64
  with self.assertRaises(RecoveryRejected):validate_reconstruction(p)
 def test_validator_binds_each_runtime_code_call_result_hash(self):
  p=copy.deepcopy(recover(Replay())['proof'])
  for c in p['calls']:
   if c['method']=='eth_getCode':c['resultHash']='0'*64
  with self.assertRaises(RecoveryRejected):validate_reconstruction(p)
if __name__=='__main__':unittest.main()

class SnapshotQuoteProofTests(unittest.TestCase):
 def setUp(self):
  # These immutable archive proofs belong to this block's clock, not the CI date.
  clock=patch('build_borrow_position_pnl.time.time',return_value=h(519668970)['timestamp'])
  clock.start();self.addCleanup(clock.stop)
 def fixture(self):
  from test_borrow_position_pnl import PublicationTests
  from build_borrow_position_pnl import from_usd36,performance
  d=PublicationTests().snapshot();r=d['positions'][0];r.update(chain='arbitrum',chainId=42161)
  proofs={str(b)+':9':recover(Replay(),9,b)['proof'] for b in [510338545,519668970]}
  for chain,m in d['chains'].items():
   m['opening']=h(510338545);m['closing']=h(519668970);m['openingNextBlock']=dict(h(510338545),number=510338546,timestamp=h(510338545)['timestamp']+1)
   count=int(chain=='arbitrum');m['coverage'].update(discoveredAccounts=count,borrowPositions=count)
   m['valuation']['reconstructedPrices']=proofs if count else {}
  d['chains']['arbitrum']['unavailableMarkets']={boundary:{'9':{'error':'contract_revert','failureType':'RpcRevert','reconstructionRef':str(block)+':9'}} for boundary,block in [('opening',510338545),('closing',519668970)]}
  d['sourceAsOf']=h(519668970)['timestamp'];d['generatedAt']=datetime.fromtimestamp(d['sourceAsOf'],timezone.utc).isoformat();values=[]
  for boundary,b in [('opening',510338545),('closing',519668970)]:
   ref=str(b)+':9';quote=proofs[ref]['price'];value=10**18*int(quote);values.append(value)
   r[boundary+'Balances']=[{'marketId':'9','address':TOKENS[9],'symbol':'plvGLP','decimals':18,'usdExact':from_usd36(value),'valuationRef':ref,'weiRawSigned':str(10**18),'oraclePriceRaw':quote}]
   r[boundary+'EquityUsdExact']=r[boundary+'CollateralUsdExact']=from_usd36(value);r[boundary+'DebtUsdExact']='0'
  r.update(performance(*values,[],coverage=True,replay=True));r.update(currentSizeUsdExact=from_usd36(values[-1]),historicalSizeUsdExact=from_usd36(max(values)))
  r['provenance']['valuationMethod']='official_oracle_formula_reconstruction'
  return d
 def test_snapshot_binds_exact_reconstructed_balances_to_endpoint_and_token(self):
  from build_borrow_position_pnl import validate_snapshot
  validate_snapshot(self.fixture())
  changes=[lambda d:d['positions'][0]['openingBalances'][0].update(valuationRef='519668970:9'),lambda d:d['positions'][0]['openingBalances'][0].update(weiRawSigned='2'),lambda d:d['positions'][0]['openingBalances'][0].update(oraclePriceRaw='1'),lambda d:d['positions'][0]['openingBalances'][0].update(address=TOKENS[41]),lambda d:d['positions'][0]['provenance'].update(valuationMethod='protocol_market_oracle'),lambda d:d['chains']['arbitrum']['valuation']['reconstructedPrices']['510338545:9'].update(blockHash='0x'+'a'*64)]
  for mutate in changes:
   data=self.fixture();mutate(data)
   with self.subTest(mutation=mutate),self.assertRaises(ValueError):validate_snapshot(data)
 def test_reconstructed_source_cannot_appear_under_another_chain(self):
  from build_borrow_position_pnl import validate_snapshot
  d=self.fixture();d['chains']['ethereum']['valuation']['reconstructedPrices']=d['chains']['arbitrum']['valuation']['reconstructedPrices']
  with self.assertRaises(ValueError):validate_snapshot(d)

class FixtureClockRegressionTests(unittest.TestCase):
 def test_archived_quote_validation_test_survives_later_wall_clocks(self):
  closing=h(519668970)['timestamp']
  for now in [closing+21601,closing+86400,closing+10*365*86400]:
   with self.subTest(now=now),patch('build_borrow_position_pnl.time.time',return_value=now):
    result=unittest.TestResult()
    SnapshotQuoteProofTests('test_snapshot_binds_exact_reconstructed_balances_to_endpoint_and_token').run(result)
    self.assertTrue(result.wasSuccessful(),result.errors+result.failures)

 def test_real_snapshot_freshness_still_rejects_stale_and_future_boundaries(self):
  from build_borrow_position_pnl import validate_snapshot
  data=SnapshotQuoteProofTests().fixture();closing=h(519668970)['timestamp']
  for now in [closing,closing+21600]:
   with self.subTest(now=now),patch('build_borrow_position_pnl.time.time',return_value=now):validate_snapshot(data)
  for now in [closing+21601,closing-1]:
   with self.subTest(now=now),patch('build_borrow_position_pnl.time.time',return_value=now),self.assertRaisesRegex(ValueError,'Stale/future chain closing boundary'):validate_snapshot(data)
