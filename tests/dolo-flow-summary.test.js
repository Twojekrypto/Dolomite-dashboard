const fs = require('node:fs');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert/strict');
const html = fs.readFileSync(require('node:path').join(__dirname, '../dolo-preview.html'), 'utf8');
function model() {
  const state = {flowsChain:'all', flowsTypes:new Set(['eoa','cex']), qFlows:'', flowsPeriod:'7d'};
  const rows = [
    {addr:'0xA', label:'Alice', type:'eoa', hasCombinedFlow:true, chgAll:60, chgEth:100, chgBera:-40},
    {addr:'0xB', label:'Exchange', type:'cex', hasCombinedFlow:true, chgAll:-20, chgEth:-30, chgBera:10},
    {addr:'0xC', label:'Carol', type:'eoa', hasCombinedFlow:true, chgAll:0, chgEth:25, chgBera:-25},
  ];
  const ctx = vm.createContext({state, FLOWS:rows, TYPE_LABELS:{eoa:'User',cex:'Exchange'},typeFilterKey:x=>x});
  vm.runInContext(html.slice(html.indexOf('function effectiveChg('),html.indexOf('function effectiveGrossInflow(')), ctx);
  const start=html.indexOf('function flowSummaryTotals(');
  assert.notEqual(start,-1,'Flow summary calculation must exist');
  vm.runInContext(html.slice(start,html.indexOf('function renderFlowSummary(',start)),ctx);
  return {state,ctx,total:()=>JSON.parse(JSON.stringify(vm.runInContext('flowSummaryTotals()',ctx)))};
}
test('summary uses signed combined flows, excludes zero-net wallets and ignores pagination',()=>{
  const m=model();
  assert.deepEqual(m.total(),{inflow:60,outflow:20,net:40,wallets:2});
  m.state.flowsPage={acc:100,out:100};
  assert.deepEqual(m.total(),{inflow:60,outflow:20,net:40,wallets:2});
});
test('summary respects network, type and label/address searches, including empty results',()=>{
  const m=model(); m.state.flowsChain='ethereum';
  assert.deepEqual(m.total(),{inflow:125,outflow:30,net:95,wallets:3});
  m.state.flowsTypes=new Set(['eoa']);
  assert.deepEqual(m.total(),{inflow:125,outflow:0,net:125,wallets:2});
  m.state.qFlows='alice';
  assert.deepEqual(m.total(),{inflow:100,outflow:0,net:100,wallets:1});
  m.state.qFlows='0xc';
  assert.deepEqual(m.total(),{inflow:25,outflow:0,net:25,wallets:1});
  m.state.qFlows='missing';
  assert.deepEqual(m.total(),{inflow:0,outflow:0,net:0,wallets:0});
});
test('unfiltered default loads complete index, not only the top ranking',()=>{
  const ctx=vm.createContext({
    liveFlowsData:{periods:{'7d':{all:{accumulators:[{address:'0xa',net_flow:100}],sellers:[],search_accumulators:[{address:'0xa',net_flow:100},{address:'0xb',net_flow:2}],search_sellers:[]}}}},
    state:{flowsPeriod:'7d',flowsChain:'all',flowsTypes:new Set(['eoa']),qFlows:''},
    ADDRESS_TYPES:['eoa'],FLOWS:[],HOLDERS:[],DOLOMITE_FLOW_BALANCES:{},lower:x=>String(x).toLowerCase(),addressInfo:()=>({}),labelFor:()=>'',mapType:()=> 'eoa',isSafeWallet:()=>false,safeNum:x=>Number(x)||0,
  });
  const start=html.indexOf('  function flowRowsForPeriod(){');
  vm.runInContext(html.slice(start,html.indexOf('  renderFlows = function(){',start))+'\nflowRowsForPeriod();',ctx);
  assert.equal(ctx.FLOWS.length,2);
  assert.equal(ctx.FLOWS[1].chgAll,2);
});
test('summary distinguishes incomplete/loading data from a complete empty period',()=>{
  const m=model();
  const el={innerHTML:''};
  m.ctx.document={getElementById:()=>el};
  m.ctx.fmtNum=n=>String(n);
  const start=html.indexOf('function renderFlowSummary(');
  vm.runInContext(html.slice(start,html.indexOf('function renderFlows(){',start)),m.ctx);
  vm.runInContext('renderFlowSummary(null)',m.ctx);
  assert.equal((el.innerHTML.match(/class="value">—</g)||[]).length,4);
  vm.runInContext('renderFlowSummary({all:{accumulators:[],sellers:[]}})',m.ctx);
  assert.equal((el.innerHTML.match(/class="value">—</g)||[]).length,4);
  m.ctx.FLOWS.length=0;
  vm.runInContext('renderFlowSummary({all:{search_accumulators:[],search_sellers:[]}})',m.ctx);
  assert.equal((el.innerHTML.match(/class="value">0</g)||[]).length,4);
});
test('Net Flow tone follows the filtered sign and stays neutral for zero or unavailable data',()=>{
  const m=model(), el={innerHTML:''};
  m.ctx.document={getElementById:()=>el};
  m.ctx.fmtNum=String;
  const start=html.indexOf('function renderFlowSummary(');
  vm.runInContext(html.slice(start,html.indexOf('function renderFlows(){',start)),m.ctx);
  const render=()=>vm.runInContext('renderFlowSummary({all:{search_accumulators:[],search_sellers:[]}})',m.ctx);
  render();
  assert.match(el.innerHTML,/selected-market-metric flow-positive"><div class="label"[^>]*>Net Flow/);
  m.state.qFlows='exchange'; render();
  assert.match(el.innerHTML,/selected-market-metric flow-negative"><div class="label"[^>]*>Net Flow/);
  m.state.qFlows='missing'; render();
  assert.doesNotMatch(el.innerHTML,/flow-positive|flow-negative/);
  vm.runInContext('renderFlowSummary(null)',m.ctx);
  assert.doesNotMatch(el.innerHTML,/flow-positive|flow-negative/);
});
