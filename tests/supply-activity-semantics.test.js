const { test } = require('node:test');
const assert = require('node:assert/strict');
const activity = require('../supply-activity-semantics.js');

test('cache roundtrip preserves precise quantities and repayment evidence', () => {
    const row = { id:'a', type:'deposit', timestamp:1, txHash:'0xaa', amount:'200.000000000000000001', usd:'200', primaryAddress:'0x1', secondaryAddress:'', semantics:{version:2,status:'verified',actions:['repay'],debtChange:'-200',supplyChange:'0',sourceIds:['a']} };
    assert.deepEqual(activity.unpack(activity.pack(row)), row);
});
test('verified grouped transaction suppresses legacy technical legs in either input order', () => {
    const legacy = {id:'transfer-1',txHash:'0xaa',type:'transfer',timestamp:1};
    const group = {id:'group-1',txHash:'0xaa',type:'trade',timestamp:1,semantics:{version:2,status:'verified',actions:['zap','repay'],sourceIds:['transfer-1','trade-2']}};
    assert.deepEqual(activity.merge([legacy],[group]), [group]);
    assert.deepEqual(activity.merge([group],[legacy]), [group]);
});
test('an enriched row wins over a legacy cache with the same id', () => {
    const old={id:'a',type:'deposit',timestamp:1};
    const rich={...old,semantics:{version:2,status:'verified',actions:['repay']}};
    assert.deepEqual(activity.merge([old],[rich]),[rich]);
});
test('Zap repayment matches either filter, but is not a supply deposit', () => {
    const row={type:'deposit',semantics:{version:2,status:'verified',actions:['zap','repay']}};
    assert.equal(activity.matches(row,new Set(['repay'])),true);
    assert.equal(activity.matches(row,new Set(['zap'])),true);
    assert.equal(activity.matches(row,new Set(['deposit'])),false);
});
test('unavailable replay never invents Borrow or Repay labels', () => {
    const row={type:'deposit',semantics:{version:2,status:'unavailable',actions:['repay','zap']}};
    assert.equal(activity.matches(row,new Set(['repay'])),false);
    assert.equal(activity.matches(row,new Set(['deposit'])),true);
    assert.equal(activity.matches(row,new Set(['zap'])),true);
});
test('missing historical fields stay null, never zero or interpolated', () => {
    const points=[{timestamp:100,supply:100,debt:null,apr:null},{timestamp:200,supply:120,debt:12,apr:3}];
    assert.deepEqual(activity.periodContext(points,100,200),{start:100,end:200,supplyChange:20,debtChange:null,utilizationStart:null,utilizationEnd:10,aprStart:null,aprEnd:3});
    assert.equal(activity.periodContext(points,50,200),null);
});
test('selecting one action from All isolates it, then supports multiselect and reset', () => {
    const one=activity.toggle(new Set(activity.types),'repay');
    assert.deepEqual([...one],['repay']);
    assert.deepEqual([...activity.toggle(one,'zap')],['repay','zap']);
    assert.deepEqual([...activity.toggle(one,'all')],activity.types);
});
test('newer replay metadata replaces an older enriched cached group', () => {
    const old={id:'g',timestamp:1,semantics:{version:2,status:'verified',blockNumber:10,debtChange:'2'}};
    const fresh={...old,semantics:{...old.semantics,blockNumber:20,debtChange:'3'}};
    assert.deepEqual(activity.merge([old],[fresh]),[fresh]);
});
test('Zap evidence does not assign unordered token relations to input/output amounts', () => {
    const fs = require('node:fs');
    const vm = require('node:vm');
    const html = fs.readFileSync(require('node:path').join(__dirname, '../liquidation-preview.html'), 'utf8');
    const start = html.indexOf('function renderSupplyActivityEvidence(row)');
    const end = html.indexOf('function supply_goPage', start);
    const render = vm.runInNewContext('(' + html.slice(start, end).trim() + ')', {
        currentSupplyOverview: {token:{symbol:'USD1'}}, supplyEscapeHtml: String,
        supplyFormatSignedCompact: String, supplyFormatTokenPrecise: String,
        supplyShortAddr: String, SUPPLY_ACTIVITY_META: {}
    });
    const rendered = render({semantics:{status:'verified',supplyChange:'0',debtChange:'-4801',routes:[{tokenPath:[{symbol:'USD1'},{symbol:'USDC'}],amountInToken:'4800',amountOutToken:'4801'}]}});
    assert.match(rendered, /Zap assets: USD1 · USDC/);
    assert.match(rendered, /Route direction unavailable/);
    assert.doesNotMatch(rendered, /4800 USD1|→/);
});
