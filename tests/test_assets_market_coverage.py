"""Listed isolation markets must not disappear because of contract labels."""
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent


class AssetsMarketCoverageTests(unittest.TestCase):
    def run_node(self, assertions):
        source = (ROOT / 'scripts/build_assets_live.mjs').read_text()
        helpers = source[source.index('const STABLE_SYMBOLS ='):source.index('async function fetchJson(')]
        fetch_rows = source[source.index('async function fetchRateRowsForChain('):source.index('async function loadPreviousAssetsSnapshot(')]
        code = '''
const assert = require('node:assert/strict');
HELPERS
const LIVE_CHAINS = {arbitrum:{id:42161,name:'Arbitrum'}};
const identities = [
 ['6','dfsGLP','Dolomite: Fee + Staked GLP'],
 ['8','mGLP','magicGLP'],
 ['9','dplvGLP','Dolomite Isolation: Plutus Vault GLP'],
 ['28','dARB','Dolomite Isolation: Arbitrum'],
 ['29','GMX','GMX'],
 ['30','dGMX','Dolomite Isolation: GMX'],
 ['40','sGLP','StakedGlp'],
 ['43','djUSDC','Dolomite Isolation: Jones USDC'],
 ['76','dsavETH','Dolomite Isolation: Staked avETH'],
 ['38','diBGT','Dolomite Isolation: Infrared BGT'],
 ['39','pol-rUSD','Dolomite Isolation: pol-rUSD'],
 ['100','dPT-USDe-31DEC2099','Dolomite Isolation: PT USDe 31DEC2099'],
 ['101','dNewAsset','Dolomite Isolation: Future Listed Market'],
 ['11','dPT-GLP-28MAR2024','Dolomite Isolation: PT GLP 28MAR2024']
];
const input = identities.map(([marketId,tokenSymbol,tokenName]) => ({
 token:{marketId,tokenSymbol,tokenName,tokenAddress:'0x'+marketId.padStart(40,'0')},
 supplyInterestRate:'0.025',borrowInterestRate:'0',totalSupplyInterestRate:'0.025',totalBorrowInterestRate:'0',
 outsideSupplyInterestRateParts:[],outsideBorrowInterestRateParts:[]
}));
async function fetchJson(){return {interestRates:input};}
FETCH_ROWS
(async()=>{ ASSERTIONS })().catch(error=>{console.error(error);process.exitCode=1;});
'''.replace('HELPERS', helpers).replace('FETCH_ROWS', fetch_rows).replace('ASSERTIONS', assertions)
        result = subprocess.run(['node', '-e', code], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_sgmx_and_other_listed_isolation_markets_retain_distinct_rows(self):
        self.run_node('''
const rows = await fetchRateRowsForChain('arbitrum');
assert.deepEqual(rows.map(r=>r.marketId),['6','8','9','28','29','30','40','43','76','38','39','100','101']);
const plain=rows.find(r=>r.marketId==='29'), staked=rows.find(r=>r.marketId==='30');
assert.equal(plain.sym,'GMX'); assert.equal(staked.sym,'dGMX');
assert.notEqual(plain.key,staked.key);
assert.equal(staked.lending,2.5);
assert.equal(staked.price,0,'catalog inclusion must not invent prices');
''')

    def test_expired_pendle_is_hidden_but_future_dated_market_is_retained(self):
        self.run_node('''
assert.equal(shouldSkipMarket(input.find(r=>r.token.marketId==='11')),true);
assert.equal(shouldSkipMarket(input.find(r=>r.token.marketId==='100')),false);
''')

    def test_saveth_and_its_isolation_wrapper_remain_eth_assets(self):
        self.run_node('''
assert.equal(classifyAsset('savETH','Staked avETH'),'eth');
assert.equal(classifyAsset('dsavETH','Dolomite Isolation: Staked avETH'),'eth');
assert.equal(classifyAsset('fakeSavETH','Imposter'),'other');
''')

    def test_positive_one_wei_cap_cannot_become_unlimited_in_simulator(self):
        source = (ROOT / 'scripts/build_assets_live.mjs').read_text()
        helpers = source[source.index('const STABLE_SYMBOLS ='):source.index('async function fetchJson(')]
        depth = source[source.index('async function graphQuery('):source.index('async function fetchRateRowsForChain(')]
        preview = (ROOT / 'assets-preview.html').read_text()
        simulator = preview[preview.index('const KINK ='):preview.index('// Hero answer:')]
        code = '''
const assert=require('node:assert/strict');
const SCALE=10n**18n;
HELPERS
const LIVE_CHAINS={arbitrum:{subgraph:'https://source.invalid'}};
const activeChainKeys=()=>['arbitrum'];
const address='0x790ff506ac24b03a21f3d0019227447ae2b55ca5';
let supplyCap='0.000000000000000001';
async function fetchJson(){return {data:{
 totalPars:[{token:{id:address},supplyPar:'5000',borrowPar:'0'}],
 interestIndexes:[{id:address,supplyIndex:'1',borrowIndex:'1'}],
 oraclePrices:[{token:{id:address},price:'8'}],
 marketRiskInfos:[{id:address,supplyMaxWei:supplyCap,borrowMaxWei:'0.000000000000000001',isBorrowingDisabled:true}]
}};}
DEPTH
const state={mode:'APR',rw:{lending:true,odolo:true,yield:true}};
SIMULATOR
(async()=>{
 const row=(await fetchMarketDepth())['arbitrum:'+address];
 assert.equal(row.supplyCap,1e-18,'one-wei cap must remain positive');
 assert.equal(row.borrowCap,1e-18,'one-wei borrow cap must remain positive');
 const position={...row,tvl:40000,borrowedUsd:0,util:0,lending:0,borrow:0};
 assert.equal(simulate(position,{action:'deposit',amount:10000}).effectiveAmount,0,'saturated cap prevents deposit');
 assert.equal(simulate(position,{action:'withdraw',amount:10000}).effectiveAmount,10000,'withdrawals still work');
 assert.equal(simulate({...position,price:0,tvl:0},{action:'deposit',amount:10000}).effectiveAmount,0,'a saturated token cap still applies when USD price is unavailable');
 supplyCap='0';
 const uncapped=(await fetchMarketDepth())['arbitrum:'+address];
 assert.equal(uncapped.supplyCap,0);
 assert.equal(simulate({...position,supplyCap:uncapped.supplyCap},{action:'deposit',amount:10000}).effectiveAmount,10000,'true zero retains unlimited-cap semantics');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''.replace('HELPERS',helpers).replace('DEPTH',depth).replace('SIMULATOR',simulator)
        result=subprocess.run(['node','-e',code],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)


if __name__ == '__main__':
    unittest.main()
