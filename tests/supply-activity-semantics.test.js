const { test } = require('node:test');
const assert = require('node:assert/strict');
const activity = require('../supply-activity-semantics.js');

function activityRenderHarness() {
    const fs=require('node:fs'), vm=require('node:vm');
    const html=fs.readFileSync(require('node:path').join(__dirname,'../liquidation-preview.html'),'utf8');
    const extract=(name,next)=>html.slice(html.indexOf('function '+name+'('),html.indexOf('function '+next+'(',html.indexOf('function '+name+'('))).trim();
    const elements=new Map();
    const context=vm.createContext({
        document:{getElementById(id){if(!elements.has(id))elements.set(id,{style:{},value:'ethereum',innerHTML:''});return elements.get(id);},querySelectorAll(){return [];}},
        SupplyActivitySemantics:activity,supplyActivityFilters:new Set(activity.types),supplyActivityPage:1,SUPPLY_ACTIVITY_PAGE_SIZE:10,
        rows:[{id:'zap',type:'trade',amount:'12.34',usd:'12.34',timestamp:1,semantics:{version:2,status:'verified',actions:['zap','repay']}},{id:'transfer',type:'transfer',amount:'0.5',usd:'0.5',timestamp:2}],
        SUPPLY_ACTIVITY_META:{trade:{label:'Swap',cls:'transfer'},zap:{label:'Zap',cls:'transfer'},repay:{label:'Repay Borrow',cls:'transfer'},transfer:{label:'Transfer',cls:'transfer'}},
        currentSupplyOverview:null,SYMBOL_ICONS:{},
        getSupplyTxExplorer:()=>'',getSupplyAddressExplorer:()=>'',
        updateSupplyActivitySortUi(){},syncSupplyActivityColumnFilterUi(){},updateSupplyActivitySubtitle(){},updateSupplyActivityHistoryAction(){},renderSupplyActivitySummary(){},
        supplyEscapeHtml:String,supplyFormatTokenCompact:String,supplyFormatTokenPrecise:String,supplyFormatActivityDate:String,supplyFormatRelativeTime:String,supplyFormatActivityTime:String,
        renderSupplyActivityEvidence:()=>'<div>Evidence</div>',formatBorrowTableRange:()=>'',PAGER_ICON_FIRST:'',PAGER_ICON_PREV:'',PAGER_ICON_NEXT:'',PAGER_ICON_LAST:''
    });
    vm.runInContext('function getFilteredSupplyActivityRows(){return rows.filter(row=>SupplyActivitySemantics.matches(row,supplyActivityFilters));}',context);
    vm.runInContext(extract('formatUSDCompact','supplyTrimAxisNumber'),context);
    vm.runInContext(extract('renderSupplyActivityTable','supplyGoActivityPage'),context);
    vm.runInContext(extract('toggleSupplyActivityType','toggleSupplyActivityDetails'),context);
    return {context,elements};
}

test('Zap then Transfers renders both selections even with sub-$1000 decimal-string USD',()=>{
    const {context,elements}=activityRenderHarness();
    context.toggleSupplyActivityType('zap');
    assert.match(elements.get('supply-activity-body').innerHTML,/\$12/);
    context.toggleSupplyActivityType('transfer');
    assert.deepEqual([...context.supplyActivityFilters],['zap','transfer']);
    assert.match(elements.get('supply-activity-body').innerHTML,/\$0\.50/);
    assert.match(elements.get('supply-activity-body').innerHTML,/Repay Borrow/);
});

test('Activity details occupies the last column and expands across all six columns',()=>{
    const {context,elements}=activityRenderHarness();
    context.rows[0].usd=12.34;context.rows[1].usd=0.5;
    context.renderSupplyActivityTable();
    const html=elements.get('supply-activity-body').innerHTML;
    const row=html.match(/<tr>([\s\S]*?)<\/tr>/)[1];
    const cells=[...row.matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/g)].map(m=>m[1]);
    assert.equal(cells.length,6);
    assert.match(cells[5],/supply-activity-details-button/);
    assert.doesNotMatch(cells[2],/<button/);
    assert.match(html,/colspan="6"/);
});

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
test('official daily APR snapshots enrich matching market days without inventing gaps', () => {
    const fs = require('node:fs');
    const vm = require('node:vm');
    const html = fs.readFileSync(require('node:path').join(__dirname, '../liquidation-preview.html'), 'utf8');
    const start = html.indexOf('function extractSupplyAprPoints(');
    const end = html.indexOf('async function fetchSupplyOfficialMetricsHistory(', start);
    const context = vm.createContext({});
    vm.runInContext(html.slice(start, end), context);
    const token = '0xAbC';
    const aprPoints = context.extractSupplyAprPoints({points:[
        {timestamp:86400, rates:{'0xabc':'1.541026102253218'}},
        {timestamp:172800, rates:{'0xabc':'2.1983518120106907'}},
        {timestamp:259200, rates:{'0xdef':'9.9'}},
    ]}, token);
    const enriched = context.mergeSupplyAprHistory([
        {timestamp:86400, supply:10, debt:5},
        {timestamp:172800, supply:12, debt:6},
        {timestamp:259200, supply:13, debt:7},
    ], aprPoints);

    assert.deepEqual(JSON.parse(JSON.stringify(enriched)), [
        {timestamp:86400, supply:10, debt:5, apr:1.541026102253218},
        {timestamp:172800, supply:12, debt:6, apr:2.1983518120106907},
        {timestamp:259200, supply:13, debt:7, apr:null},
    ]);
});
test('recent market cache rejects legacy history without the APR schema', () => {
    const fs = require('node:fs');
    const vm = require('node:vm');
    const html = fs.readFileSync(require('node:path').join(__dirname, '../liquidation-preview.html'), 'utf8');
    const start = html.indexOf('function getSupplyRecentHistoryCacheKey(');
    const end = html.indexOf('function extractSupplyAprPoints(', start);
    const context = vm.createContext({});
    vm.runInContext(html.slice(start, end), context);

    const activeKey = context.getSupplyRecentHistoryCacheKey('ethereum', '0xabc');
    assert.notEqual(activeKey, 'ethereum:0xabc:history:v6:recent');
    assert.equal(context.hasSupplyRecentHistoryAprSchema({marketPoints:[{apr:null}]}), false);
    assert.equal(context.hasSupplyRecentHistoryAprSchema({aprSchema:1,marketPoints:[{apr:null}]}), true);
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
test('newer unavailable replay replaces stale verified classification', () => {
    const old={id:'a',type:'deposit',timestamp:1,semantics:{version:2,status:'verified',actions:['repay'],blockNumber:10}};
    const fresh={...old,semantics:{version:2,status:'unavailable',actions:['deposit'],blockNumber:20}};
    assert.deepEqual(activity.merge([old],[fresh]),[fresh]);
    assert.deepEqual(activity.merge([fresh],[old]),[fresh]);
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
