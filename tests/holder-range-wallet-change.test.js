import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const html = fs.readFileSync('dolo-preview.html', 'utf8');
function source(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `${name} must exist`);
  return html.slice(start, html.indexOf('\n}', start) + 2);
}
function windowFixture() {
  return Function(`${source('holderSnapshotWindow')}\nreturn holderSnapshotWindow;`)();
}
const points = [0, 86400, 172800, 192894].map((ts, i) => ({key:String(i), ts, buckets:[]}));

test('between-snapshot start retains the baseline instead of losing most of 24H', () => {
  const result = windowFixture()(points, {from:106494, to:192894});
  assert.deepEqual(result.points.map(p => p.ts), [86400, 172800, 192894]);
  assert.deepEqual(result.selection, {from:86400, to:192894});
});

test('custom date bounds are the actual snapshots used at both endpoints', () => {
  const result = windowFixture()(points, {from:25000, to:130000});
  assert.deepEqual(result.points.map(p => p.ts), [0, 86400]);
  assert.deepEqual(result.selection, {from:0, to:86400});
});

test('All preserves every original point and narrow windows keep two real snapshots', () => {
  assert.deepEqual(windowFixture()(points, {from:0,to:192894}).points, points);
  assert.deepEqual(windowFixture()(points, {from:180000,to:185000}).points.map(p=>p.ts), [86400,172800]);
  assert.deepEqual(windowFixture()(points, {from:0,to:100}).points.map(p=>p.ts), [0,86400]);
  assert.deepEqual(windowFixture()([], {from:0,to:1}).points, []);
});

test('partial-day snapshot duration is not labelled as a nominal 24H', () => {
  const label = Function(`${source('holderSnapshotWindowLabel')}\nreturn holderSnapshotWindowLabel;`)();
  assert.equal(label(86400, 192894, 0, 192894), '1D 5H 35M');
  assert.equal(label(0, 192894, 0, 192894), 'All');
  assert.equal(label(86400, 172800, 0, 192894), '1D');
});

function walletFixture(currentRows, baselineRows, bucket) {
  const code = `
    const currentRows = ${JSON.stringify(currentRows)};
    const baselineRows = ${JSON.stringify(baselineRows)};
    const safeHolderNum = v => Number.isFinite(Number(v)) ? Number(v) : 0;
    const lowerHolderAddr = v => String(v || '').toLowerCase();
    const holderDistributionType = (address, row) => row?.type || 'eoa';
    const holderRowsAtPoint = point => point.key === 'end' ? currentRows : baselineRows;
    const fmtNum = String, fmtSignedHolder = String, fmtSignedHolderPct = String;
    const HOLDER_UNAVAILABLE_PERCENT_TOOLTIP = 'Unavailable percentage';
    ${source('holderWalletRangeChange')}
    ${source('zeroHolderRowFrom')}
    ${source('holderWalletSortValue')}
    ${source('holderWalletUnionRows')}
    return holderWalletUnionRows({point:{key:'end'}, baselinePoint:{key:'start'}, includeVeDolo:false,
      matchesRow:row => row.panelTotal >= ${bucket[0]} && row.panelTotal < ${bucket[1]},
      rangeNoteForRow:(current, baseline) => current === baseline ? '' : current ? 'Entered' : 'Left'});
  `;
  return Function(code)();
}
const wallet = (liquid, inDolomite=0, locked=0) => ({addr:'0x1111111111111111111111111111111111111111',
  type:'eoa',liquid,inDolomite,locked,panelTotal:liquid+inDolomite+locked});

test('an upward crossing shows only the acquired DOLO in both bucket Details', () => {
  for (const bucket of [[1e6,Infinity],[500000,1e6]]) {
    const [row] = walletFixture([wallet(1134808.7393)], [wallet(924046.3993)], bucket);
    assert.ok(Math.abs(row.rangeChange.delta - 210762.34) < 1e-6);
    assert.ok(Math.abs(row.rangeChange.sourceDeltas.wallet - 210762.34) < 1e-6);
    assert.equal(row.rangeCurrentTotal, bucket[0]===1e6 ? 1134808.7393 : 0);
    assert.equal(row.rangeBaselineTotal, bucket[0]===1e6 ? 0 : 924046.3993);
  }
});

test('a downward crossing remains a loss, not a gain when entering the smaller bucket', () => {
  const [row] = walletFixture([wallet(305393.3218)], [wallet(2490986.5018)], [100000,500000]);
  assert.ok(Math.abs(row.rangeChange.delta - (-2185593.18)) < 1e-6);
  assert.equal(row.rangeChange.tone, 'down');
});

test('crossing retains real wallet/protocol/lock deltas without counting existing positions again', () => {
  const [row] = walletFixture([wallet(0,2000000,96000)], [wallet(50000,0,96000)], [1e6,Infinity]);
  assert.equal(row.rangeChange.delta, 1950000);
  assert.deepEqual(row.rangeChange.sourceDeltas, {wallet:-50000,dolomite:2000000,vedolo:0});
});

test('a missing historical row below the published threshold is not a proven zero', () => {
  const [entry] = walletFixture([wallet(2000000)], [], [1e6,Infinity]);
  assert.equal(entry.rangeChange.delta, null);
  assert.equal(entry.rangeChange.main, '—');
  assert.equal(entry.rangeChange.sourceDeltas, null);
  const [exit] = walletFixture([], [wallet(2000000)], [1e6,Infinity]);
  assert.equal(exit.balanceKnown, false);
  assert.equal(exit.rangeChange.delta, null);
});

test('a genuinely provided zero balance still permits an exact wallet change', () => {
  const [row] = walletFixture([wallet(2000000)], [wallet(0)], [1e6,Infinity]);
  assert.equal(row.rangeChange.delta, 2000000);
});

test('Details renders missing values as unavailable and preserves numeric sort values for known changes', () => {
  const render = Function(`
    const escHtml = v => String(v ?? '').replaceAll('"','&quot;');
    const safeHolderNum = v => Number(v) || 0;
    const fmtNum = String;
    const TYPE_LABELS = {};
    const holderWalletChainSortValue = () => 'eth';
    const holderWalletChainHtml = () => '';
    const holderWalletAddressCell = row => row.addr;
    const holderRangeNoteTooltip = String;
    ${source('holderWalletChangeSourceHtml')}
    ${source('holderWalletChangeCell')}
    ${source('walletDrilldownPanelHtml')}
    return walletDrilldownPanelHtml;
  `)();
  const [missing] = walletFixture([], [wallet(2000000)], [1e6,Infinity]);
  const unknownHtml = render({title:'Wallets',rows:[missing]});
  assert.match(unknownHtml, /data-wallet-sort-dolo=""/);
  assert.match(unknownHtml, /data-wallet-sort-change=""/);
  assert.match(unknownHtml, /class="bal-val total"[^>]*>—<\/span>/);
  assert.match(unknownHtml, /class="main"[^>]*>—<\/span>/);
  assert.match(unknownHtml, /Missing does not mean zero/);
  const [known] = walletFixture([wallet(1100000)], [wallet(900000)], [1e6,Infinity]);
  const knownHtml = render({title:'Wallets',rows:[known]});
  assert.match(knownHtml, /data-wallet-sort-dolo="1100000"/);
  assert.match(knownHtml, /data-wallet-sort-change="200000"/);
});
