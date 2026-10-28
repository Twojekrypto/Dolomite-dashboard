"""Regress the USDG market omission without fetching or repricing live data."""
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent


class AssetsStableClassificationTests(unittest.TestCase):
    def test_exact_usdg_and_canonical_arbitrum_market_are_stable(self):
        source = (ROOT / 'scripts/build_assets_live.mjs').read_text()
        helpers = source[source.index('const STABLE_SYMBOLS ='):source.index('async function fetchJson(')]
        fetch_rows = source[source.index('async function fetchRateRowsForChain('):source.index('async function loadPreviousAssetsSnapshot(')]
        code = '''
const assert = require('node:assert/strict');
HELPERS
assert.equal(classifyAsset('USDG', 'Global Dollar'), 'stable');
assert.equal(classifyAsset(' usdg ', 'Global Dollar'), 'stable');
assert.equal(classifyAsset('USDC', 'USD Coin'), 'stable');
assert.equal(classifyAsset('WETH', 'Wrapped Ether'), 'eth');
for (const symbol of ['AUSDG', 'USDG.e', 'dUSDG', 'PT-USDG', 'dPT-USDG', 'FAKE']) {
  assert.equal(classifyAsset(symbol, 'USDG Global Dollar'), 'other', symbol);
}
const LIVE_CHAINS = {arbitrum:{id:42161,name:'Arbitrum'}};
async function fetchJson(url) {
  assert.equal(url, 'https://api.dolomite.io/tokens/42161/interest-rates?exclude-odolo=false');
  return {interestRates:[{
    token:{tokenSymbol:'USDG',tokenName:'Global Dollar',tokenAddress:'0x004b506865409877c9fa29bfb1eba929984b9bbc',marketId:77},
    supplyInterestRate:'0.0825',borrowInterestRate:'0.105',totalSupplyInterestRate:'0.0825',totalBorrowInterestRate:'0.105'
  }]};
}
FETCH_ROWS
fetchRateRowsForChain('arbitrum').then(rows => {
  assert.equal(rows.length, 1);
  assert.equal(rows[0].cat, 'stable');
  assert.equal(rows[0].key, 'arbitrum:0x004b506865409877c9fa29bfb1eba929984b9bbc');
  assert.equal(rows[0].marketId, 77);
  assert.equal(rows[0].sym, 'USDG');
  assert.equal(rows[0].lending, 8.25);
  assert.equal(rows[0].borrow, 10.5);
  assert.equal(rows[0].price, 0, 'category classification must not invent a dollar price');
}).catch(error => { console.error(error); process.exitCode = 1; });
'''.replace('HELPERS', helpers).replace('FETCH_ROWS', fetch_rows)
        result = subprocess.run(['node', '-e', code], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
