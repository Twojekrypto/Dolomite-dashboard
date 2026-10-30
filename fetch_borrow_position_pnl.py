#!/usr/bin/env python3
"""Pinned historical Graph + archive RPC borrow P&L collector.

No current-APR dependency. Complete discovery precedes any ranking. Immutable
source cache is external to the served snapshot. Calls, retries and concurrency
are bounded; an incomplete chain aborts publication rather than shrinking it.
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import threading
from email.utils import parsedate_to_datetime
from http.client import responses as HTTP_REASONS
import os
from pathlib import Path
import re
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from build_borrow_position_pnl import atomic_write, from_usd36, par_to_wei, performance
import scan_earn_netflow as decoder

BASE='https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/'
CONFIG={
 'ethereum': dict(chainId=1,graph='dolomite-ethereum',rpc='https://mainnet.gateway.tenderly.co',chunk=9999,confirmations=64,span=260000),
 'arbitrum': dict(chainId=42161,graph='dolomite-arbitrum',rpc='https://arbitrum.gateway.tenderly.co',chunk=49999,confirmations=256,span=12000000),
 'berachain': dict(chainId=80094,graph='dolomite-berachain-mainnet',rpc='https://rpc.berachain.com/',chunk=9999,confirmations=64,span=1600000,rpc_interval=1.0),
}
ORACLE='0x223e16b9e4703ea2acd14a8caa0fcbbf4a42d3e8b7a33234e841bd1808ee9ec0'
OPERATION='0x91b01baeee3a24b590d112613814d86801005c7ef9353e7fc1eaeaf33ccf83b0'
INDEX_TOPICS={'0xf4626fd1187f91e6761ffb8a6ac3e8d9235a4a92da54e43feb0c57c4a4a322ab','0x247e2f5b851dd23ef755d9ad527e801ee202c4097acd70c21e82dc5602cdd879'}
TOPICS=decoder.ALL_EVENTS+[ORACLE,OPERATION]+sorted(INDEX_TOPICS)
CURRENT={decoder.LOG_DEPOSIT:('deposit',7),decoder.LOG_WITHDRAW:('withdraw',7),decoder.CURRENT_LOG_TRANSFER:('transfer',11),decoder.CURRENT_LOG_BUY:('trade',12),decoder.CURRENT_LOG_SELL:('trade',12),decoder.CURRENT_LOG_TRADE:('trade',21),decoder.CURRENT_LOG_LIQUIDATE:('liquidation',20),decoder.CURRENT_LOG_VAPORIZE:('vaporization',16)}
ACCOUNT_FIELDS='id user{id} effectiveUser{id} accountNumber tokenValues(first:1000){valuePar token{id marketId decimals symbol}}'
EVENT_FAMILIES=['deposits','withdrawals','transfers','trades','liquidations','vaporizations']


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def exact_par(value, decimals):
    if not isinstance(value,str) or not re.fullmatch(r'-?[0-9]+(?:\.[0-9]+)?',value): raise ValueError('Invalid Graph principal')
    whole,_,fraction=value.lstrip('-').partition('.')
    if len(fraction)>decimals and int(fraction[decimals:] or '0'): raise ValueError('Fractional raw Par')
    return (-1 if value.startswith('-') else 1)*(int(whole)*10**decimals+int((fraction+'0'*decimals)[:decimals] or '0'))


def words(data):
    if not isinstance(data,str) or not data.startswith('0x') or (len(data)-2)%64: raise ValueError('Invalid ABI framing')
    return [int(data[i:i+64],16) for i in range(2,len(data),64)]


def decode_event(log):
    topic=log['topics'][0].lower()
    if topic not in CURRENT: raise ValueError('Unsupported balance event '+topic)
    kind,minimum=CURRENT[topic]
    values=words(log['data'])
    # Buy/sell/trade carry a trailing address in deployed ABI. Some older
    # decoders only consumed balance words; retain and validate the full frame.
    if len(values)!=minimum: raise ValueError('Truncated balance event')
    entries=decoder.decode_log_entries(log)
    if not entries or any('account' not in e or 'new_par' not in e for e in entries): raise ValueError('Missing exact account/Par')
    for e in entries:
        if not 0<=int(e['account'])<2**256: raise ValueError('Invalid uint256 account')
    return dict(kind=kind,legs=entries,routeAddress=('0x'+format(values[6],'040x')) if kind in ('deposit','withdraw') else None,
                blockNumber=int(log['blockNumber'],16),blockHash=log['blockHash'],txHash=log['transactionHash'],txIndex=int(log['transactionIndex'],16),logIndex=int(log['logIndex'],16),topic0=topic)


def merge_logs(logs):
    unique={}
    for log in logs:
        if log.get('removed'): raise ValueError('Removed canonical log')
        key=(log['transactionHash'],log['logIndex'])
        if key in unique and unique[key]!=log: raise ValueError('Conflicting duplicate event')
        unique[key]=log
    return sorted(unique.values(),key=lambda x:(int(x['blockNumber'],16),int(x['transactionIndex'],16),int(x['logIndex'],16)))


def replay(opening,events,owner,account):
    state=dict(opening); borrowed=any(x<0 for x in state.values())
    for event in events:
        for leg in event['legs']:
            if leg['owner']==owner and leg['account']==account:
                value=leg['new_par']; borrowed=borrowed or value<0
                if value: state[leg['market']]=value
                else: state.pop(leg['market'],None)
    return state,borrowed


def endpoint(par,marks,tokens):
    balances=[]; supply=borrow=0
    for market,principal in sorted(par.items(),key=lambda x:int(x[0])):
        if not principal: continue
        mark=marks.get(market)
        if not mark or mark.get('price',0)<=0: raise ValueError('missing_boundary_quote')
        wei=par_to_wei(principal,mark['supply'],mark['borrow']); value=wei*mark['price']
        if value>=0:supply+=value
        else:borrow-=value
        token=tokens.get(market,{})
        balances.append(dict(marketId=market,address=token.get('id'),symbol=token.get('symbol'),decimals=int(token.get('decimals',18)),parRawSigned=str(principal),weiRawSigned=str(wei),oraclePriceRaw=str(mark['price']),usdExact=from_usd36(value)))
    return dict(balances=balances,supplyUsd36=supply,borrowUsd36=borrow,equityUsd36=supply-borrow)


def ordered_pages(fetch,page_size=1000):
    result=[];cursor='';pages=0
    while True:
        page=fetch(cursor); pages+=1
        if not isinstance(page,list):raise ValueError('Invalid Graph page')
        for row in page:
            if row['id']<=cursor:raise ValueError('Non-increasing Graph cursor')
            cursor=row['id'];result.append(row)
        if len(page)<page_size:return result,pages


def validate_ranges(ranges,start,end):
    cursor=start
    for a,b in sorted(ranges):
        if a!=cursor or b<a:return False
        cursor=b+1
    return cursor==end+1


class RequestPacer:
    """One chain/provider gate shared by its header, log and mark workers."""
    def __init__(self, interval):
        self.interval = interval
        self.next_start = 0.0
        self.paused_until = 0.0
        self.lock = threading.Lock()

    def wait(self):
        while True:
            with self.lock:
                now = time.monotonic()
                delay = max(self.next_start, self.paused_until) - now
                if delay <= 0:
                    self.next_start = now + self.interval
                    return now
            # Release the lock while waiting: a worker receiving 429 can extend
            # the deadline. Every waiter rechecks that shared deadline on wake.
            time.sleep(min(delay, 60.0))

    def cooldown(self, seconds):
        with self.lock:
            self.paused_until = max(self.paused_until, time.monotonic() + seconds)


def rate_retry_delay(headers, attempt):
    """Bounded Retry-After (seconds or HTTP date), matching RPC cooldown caps."""
    fallback = min(5.0 * 2 ** min(attempt, 4), 60.0)
    value = (headers or {}).get('Retry-After')
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            stamp = parsedate_to_datetime(value)
            seconds = stamp.timestamp() - time.time() if stamp.tzinfo else float('nan')
        except (TypeError, ValueError, OverflowError):
            seconds = float('nan')
    if not math.isfinite(seconds) or seconds < 0:
        return fallback
    return min(max(seconds, 1.0), 60.0)


class Source:
    def __init__(self,chain,cache):
        self.chain=chain;self.config=CONFIG[chain]; self.margin=decoder.CHAINS[chain]['margin']
        self.rpc_url=os.environ.get('BORROW_PNL_'+chain.upper()+'_RPC',self.config['rpc'])
        self.graph_url=BASE+self.config['graph']+'/latest/gn'
        self.cache=Path(cache)/chain;self.cache.mkdir(parents=True,exist_ok=True)
        self.refs=set();self.requests=0;self.hits=0
        # Public BERA throttles bursts from hosted runners. This is a per-chain
        # aggregate one-request/second start rate, not a per-thread sleep.
        self.rpc_pacer = RequestPacer(self.config.get('rpc_interval', 0.0))
        self.graph_pacer = RequestPacer(0.0)

    def request(self,url,payload,anchor=None):
        key=digest({'source':url,'request':payload,'anchor':anchor})
        path=self.cache/(key+'.json')
        if anchor and path.exists():
            saved=json.loads(path.read_text())
            if saved.get('responseHash')!=digest(saved['response']): raise ValueError('Corrupt immutable source cache')
            self.refs.add(key);self.hits+=1;return saved['response']
        pacer = self.rpc_pacer if url == self.rpc_url else self.graph_pacer
        method = payload.get('method', 'graphql')
        for attempt in range(8):
            try:
                pacer.wait()
                req=Request(url,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json','User-Agent':'DolomiteHistoricalPnL/1'})
                with urlopen(req,timeout=40) as response: data=json.load(response)
                if isinstance(data,dict) and isinstance(data.get('error'),dict) and data['error'].get('code') == 429:
                    raise HTTPError('',429,'Too Many Requests',getattr(response,'headers',{}),None)
                if isinstance(data,dict) and (data.get('error') or data.get('errors')): raise ValueError(str(data.get('error') or data.get('errors'))[:250])
                break
            except Exception as error:
                if isinstance(error,HTTPError):
                    rate_limited = error.code == 429
                    delay = rate_retry_delay(error.headers, attempt) if rate_limited else None
                    if rate_limited:
                        pacer.cooldown(delay)
                    # Never print URL, response text, headers, or the original
                    # exception: configured providers may contain credentials.
                    print(f'[borrow-pnl] chain={self.chain} method={method} status={error.code} attempt={attempt+1}/8 cooldownSeconds={delay if delay is not None else 0}',flush=True)
                    terminal = error.code in (400,401,403,404) or attempt == 7 or (not rate_limited and attempt >= 2)
                    if terminal:
                        raise HTTPError('',error.code,HTTP_REASONS.get(error.code,'HTTP failure'),{},None) from None
                    if rate_limited:
                        continue
                if attempt >= 2:raise
                time.sleep(0.5*2**attempt)
        self.requests+=1
        if anchor:
            saved={'request':payload,'anchor':anchor,'retrievedAt':datetime.now(timezone.utc).isoformat(),'responseHash':digest(data),'response':data}
            # Endpoints may be authenticated: intentionally excluded from cache.
            tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(saved,separators=(',',':')));os.replace(tmp,path);self.refs.add(key)
        return data

    def rpc(self,method,params,anchor=None):
        return self.request(self.rpc_url,{'jsonrpc':'2.0','id':1,'method':method,'params':params},anchor)['result']

    def graph(self,query,anchor=None):
        data=self.request(self.graph_url,{'query':query},anchor)
        if 'data' not in data: raise ValueError('Graph missing data')
        return data['data']

    def block(self,n):
        data=self.rpc('eth_getBlockByNumber',[hex(n) if isinstance(n,int) else n,False])
        if not data or not data.get('hash'): raise ValueError('Missing block header')
        return {'number':int(data['number'],16),'hash':data['hash'],'timestamp':int(data['timestamp'],16)}

    def before(self,target,hi):
        lo=max(0,hi-self.config['span'])
        while self.block(lo)['timestamp']>target:lo=max(0,lo-self.config['span'])
        while hi-lo>1:
            mid=(lo+hi)//2
            if self.block(mid)['timestamp']<=target:lo=mid
            else:hi=mid
        return self.block(lo)

    def accounts(self,header,ids=None):
        block=header['number']; pages=0; all_rows=[]
        batches=[None] if ids is None else [ids[i:i+250] for i in range(0,len(ids),250)]
        for batch in batches:
            where='hasBorrowValue:true' if batch is None else 'id_in:'+json.dumps(batch)
            def fetch(cursor):
                q='{marginAccounts(first:1000,orderBy:id,orderDirection:asc,block:{number:%d},where:{%s,id_gt:%s}){%s}}'%(block,where,json.dumps(cursor),ACCOUNT_FIELDS)
                return self.graph(q,header['hash'])['marginAccounts']
            rows,n=ordered_pages(fetch);all_rows+=rows;pages+=n
        return {r['id']:r for r in all_rows},pages

    def event_ids(self,opening,closing):
        result=set();counts={};pages={};liquidation_txs=set()
        for family in EVENT_FAMILIES:
            def fetch(cursor):
                extra=' liquidationType' if family=='trades' else ''
                q='{%s(first:1000,orderBy:id,orderDirection:asc,block:{number:%d},where:{id_gt:%s,transaction_:{blockNumber_gt:"%d",blockNumber_lte:"%d"}}){id logIndex transaction{id blockNumber}%s}}'%(family,closing['number'],json.dumps(cursor),opening['number'],closing['number'],extra)
                return self.graph(q,closing['hash'])[family]
            rows,n=ordered_pages(fetch);counts[family]=len(rows);pages[family]=n
            liquidation_txs.update(r['transaction']['id'] for r in rows if r.get('liquidationType'))
            result.update((r['transaction']['id'].lower(),int(r['logIndex'])) for r in rows)
        return result,counts,pages,liquidation_txs

    def log_range(self,pair):
        a,b=pair;header=self.block(b)
        try:
            values=self.rpc('eth_getLogs',[{'address':self.margin,'fromBlock':hex(a),'toBlock':hex(b),'topics':[TOPICS]}],header['hash'])
            if len(values)>=10000:raise ValueError('Potential provider result cap')
            for log in values:
                if not a<=int(log['blockNumber'],16)<=b or log['address'].lower()!=self.margin.lower():raise ValueError('Out-of-range log')
            return [(a,b)],values
        except Exception as error:
            detail=str(error).lower()
            splittable=(isinstance(error,HTTPError) and error.code==413) or any(word in detail for word in ('too many results','block range','response size','result cap','range limit','query timeout','timed out','timeout'))
            if isinstance(error,HTTPError) and error.code in (401,403,404,429):splittable=False
            if not splittable or b-a<10:raise
            mid=(a+b)//2
            left,x=self.log_range((a,mid));right,y=self.log_range((mid+1,b))
            return left+right,x+y

    def logs(self,opening,closing):
        chunk=self.config['chunk'];start=opening['number']+1;end=closing['number'];ranges=[];all_logs=[]
        shards=[]
        for a in range((start//chunk)*chunk,end+1,chunk):shards.append((max(a,start),min(a+chunk-1,end)))
        with ThreadPoolExecutor(max_workers=4) as pool:
            for n,(spans,logs) in enumerate(pool.map(self.log_range,shards),1):
                ranges+=spans;all_logs+=logs
                if n%20==0:print(self.chain,'log shards',n,'/',len(shards),'logs',len(all_logs),flush=True)
        if not validate_ranges(ranges,start,end): raise ValueError('Incomplete raw log range')
        return merge_logs(all_logs),ranges

    def call(self,data,header):
        return self.rpc('eth_call',[{'to':self.margin,'data':'0x'+data},hex(header['number'])],header['hash'])

    def marks(self,header,markets):
        def get(market):
            arg=format(int(market),'064x')
            try:
                index=words(self.call('56ea84b2'+arg,header));price=words(self.call('8928378e'+arg,header))
                if len(index)!=3 or len(price)!=1 or price[0]<=0:raise ValueError('Missing quote/index')
                return market,dict(borrow=index[0],supply=index[1],price=price[0])
            except Exception as error:
                return market,dict(error='contract_revert' if 'revert' in str(error).lower() else 'provider_failure',failureType=type(error).__name__)
        with ThreadPoolExecutor(max_workers=2) as pool:results=dict(pool.map(get,sorted(markets,key=int)))
        # Retry unresolved transport failures serially; healthy market results
        # survive. Contract reverts are historical evidence, not transient zeros.
        for market,value in list(results.items()):
            if value.get('error')=='provider_failure':
                time.sleep(0.2);_,results[market]=get(market)
        return results


def historical_size(opening,closing,operation_peak,flows):
    known=[point['supplyUsd36']+point['borrowUsd36'] for point in (opening,closing) if point is not None]
    if operation_peak>0:known.append(operation_peak)
    known.extend(abs(flow['usd36']) for flow in flows if flow['usd36'] is not None)
    return max(known) if known else None


def public_balances(point):
    fields=('marketId','address','symbol','decimals','usdExact')
    return [{k:b[k] for k in fields} for b in point['balances']] if point else []


def par_map(account):
    if len(account.get('tokenValues',[]))>=1000:raise ValueError('Nested token page may be truncated')
    return {str(v['token']['marketId']):exact_par(v['valuePar'],int(v['token']['decimals'])) for v in account.get('tokenValues',[]) if exact_par(v['valuePar'],int(v['token']['decimals']))}


def operation_context(logs):
    """Associate emitted oracle marks with exact operate invocation, not block end."""
    contexts={};current=None;tx=None
    for log in logs:
        if log['transactionHash']!=tx:current=None;tx=log['transactionHash']
        topic=log['topics'][0].lower()
        if topic==OPERATION:current={'id':tx+':'+log['logIndex'],'prices':{},'indices':{}}
        elif topic==ORACLE and current is not None:
            vals=words(log['data'])
            if len(vals)!=1:raise ValueError('Invalid operation quote')
            current['prices'][str(int(log['topics'][1],16))]=vals[0]
        elif topic in INDEX_TOPICS and current is not None:
            vals=words(log['data'])
            if len(vals)!=3:raise ValueError('Invalid operation index')
            current['indices'][str(int(log['topics'][1],16))]={'borrow':vals[0],'supply':vals[1]}
        elif topic in decoder.ALL_EVENTS:contexts[(tx,int(log['logIndex'],16))]=current
    return contexts


def row_flows(events,owner,number,contexts):
    flows=[]
    for event in events:
        if event['kind'] in ('trade','liquidation','vaporization'):continue
        ctx=contexts.get((event['txHash'],event['logIndex']))
        for leg in event['legs']:
            if leg['owner']!=owner or leg['account']!=number or not leg['delta']:continue
            reason=None
            if event['kind'] in ('deposit','withdraw') and event['routeAddress']!=owner:
                reason='ambiguous_router_capital_flow'
            price=ctx['prices'].get(leg['market']) if ctx else None
            if price is None or price<=0:reason=reason or 'missing_operation_oracle_quote'
            flows.append(dict(usd36=None if reason else leg['delta']*price,reason=reason,exactNav=False,preNavUsd36=None,postNavUsd36=None,
                              blockNumber=event['blockNumber'],transactionIndex=event['txIndex'],logIndex=event['logIndex'],eventRef=event['txHash']+':'+str(event['logIndex']),operationRef=ctx['id'] if ctx else None,marketId=leg['market'],deltaWeiRawSigned=str(leg['delta']),oraclePriceRaw=str(price) if price else None,
                              classification='position_transfer' if event['kind']=='transfer' else 'direct_owner_'+event['kind']))
    return flows


def attach_flow_nav(opening, events, owner, number, contexts, flows):
    """Exact action-local marks; do not use block-1/block approximations.

    A Wei/Par rounding residue makes this version's TWR unavailable. The dollar
    ledger retains the actual event Wei and hence preserves that economic cost.
    """
    state=dict(opening);peak=0
    by_event=defaultdict(list)
    for flow in flows:by_event[flow['eventRef']].append(flow)
    for event in events:
        ctx=contexts.get((event['txHash'],event['logIndex']))
        marks={m:dict(index,price=ctx['prices'].get(m,0)) for m,index in ctx['indices'].items()} if ctx else {}
        try:pre=endpoint(state,marks,{})
        except ValueError:pre=None
        for leg in event['legs']:
            if leg['owner']==owner and leg['account']==number:
                if leg['new_par']:state[leg['market']]=leg['new_par']
                else:state.pop(leg['market'],None)
        try:post=endpoint(state,marks,{})
        except ValueError:post=None
        for value in (pre,post):
            if value:peak=max(peak,value['supplyUsd36']+value['borrowUsd36'])
        matched=by_event[event['txHash']+':'+str(event['logIndex'])]
        if len(matched)==1 and pre and post:
            flow=matched[0]
            flow.update(preNavUsd36=pre['equityUsd36'],postNavUsd36=post['equityUsd36'],
                        exactNav=flow['usd36'] is not None and post['equityUsd36']-pre['equityUsd36']==flow['usd36'])
    return peak


def public_return_proof(flows):
    """Compact ordered proof only for verified active-flow percentages."""
    fields=('blockNumber','transactionIndex','logIndex','eventRef','operationRef','marketId','deltaWeiRawSigned','oraclePriceRaw')
    return [dict({field:flow[field] for field in fields},signedUsdExact=from_usd36(flow['usd36']),
                 preNavUsdExact=from_usd36(flow['preNavUsd36']),postNavUsdExact=from_usd36(flow['postNavUsd36'])) for flow in flows]


def decode_balances(data):
    vals=words(data)
    if len(vals)<4:raise ValueError('Truncated balance getter')
    arrays=[]
    for i,width in [(0,1),(1,1),(2,2),(3,2)]:
        if vals[i]%32:raise ValueError('Invalid ABI offset')
        offset=vals[i]//32
        n=vals[offset]
        if offset+1+n*width>len(vals):raise ValueError('Truncated ABI array')
        arrays.append([vals[offset+1+j*width:offset+1+(j+1)*width] for j in range(n)])
    if len({len(a) for a in arrays})!=1:raise ValueError('Getter array length mismatch')
    return {str(m[0]):{'par':p[1] if p[0] else -p[1],'wei':w[1] if w[0] else -w[1]} for m,_,p,w in zip(*arrays) if p[1] or w[1]}


def getter_audit(source,header,account,expected):
    args=account['user']['id'][2:].zfill(64)+format(int(account['accountNumber']),'064x')
    raw=source.call('6a8194e7'+args,header);got=decode_balances(raw)
    wanted={b['marketId']:{'par':int(b['parRawSigned']),'wei':int(b['weiRawSigned'])} for b in expected['balances']}
    if got!=wanted:raise ValueError('Boundary account getter mismatch '+account['id'])
    values=words(source.call('124f914c'+args,header))
    return {'accountId':account['id'],'blockNumber':header['number'],'balancesMatch':True,'balancesResponseHash':digest(raw),'valuesResponseUsd36':[str(v) for v in values],
            'valuesMatchCurrentIndex':values==[expected['supplyUsd36'],expected['borrowUsd36']],'valuesSemantics':'cached_index' if source.chain=='arbitrum' else 'current_index'}


def collect_chain(chain,cache,target):
    source=Source(chain,cache); started=time.time()
    meta=source.graph('{_meta{block{number} deployment hasIndexingErrors}}')['_meta']
    if meta['hasIndexingErrors']:raise ValueError('Graph indexing errors')
    head=source.block(int(meta['block']['number'])-source.config['confirmations'])
    closing=source.before(target,head['number']+1) if head['timestamp']>target else head
    opening=source.before(closing['timestamp']-2592000,closing['number'])
    next_open=source.block(opening['number']+1)
    print(chain,'pinned',opening['number'],closing['number'],flush=True)
    opened,nopen=source.accounts(opening);closed,nclose=source.accounts(closing)
    print(chain,'boundary borrowers',len(opened),len(closed),flush=True)
    logs,ranges=source.logs(opening,closing)
    events=[decode_event(log) for log in logs if log['topics'][0].lower() in decoder.ALL_EVENTS]
    graph_ids,family_counts,event_pages,liquidation_txs=source.event_ids(opening,closing)
    raw_ids={(e['txHash'].lower(),e['logIndex']) for e in events}
    if graph_ids!=raw_ids:
        extra_graph=graph_ids-raw_ids;extra_rpc=raw_ids-graph_ids
        raise ValueError('Graph/RPC event identity mismatch '+str({'graphOnly':len(extra_graph),'rpcOnly':len(extra_rpc),'exampleGraph':list(extra_graph)[:2],'exampleRpc':list(extra_rpc)[:2]}))
    contexts=operation_context(logs);by_account=defaultdict(list)
    for e in events:
        for owner,num in {(l['owner'],l['account']) for l in e['legs']}:
            by_account[owner+'-'+num].append(e)
    ids=set(opened)|set(closed)|set(by_account)
    more_open,p1=source.accounts(opening,sorted(ids-set(opened)));opened.update(more_open)
    more_close,p2=source.accounts(closing,sorted(ids-set(closed)));closed.update(more_close)
    token_rows,_=ordered_pages(lambda cursor:source.graph('{tokens(first:1000,orderBy:id,orderDirection:asc,block:{number:%d},where:{id_gt:%s}){id marketId decimals symbol}}'%(closing['number'],json.dumps(cursor)),closing['hash'])['tokens'])
    tokens={str(t['marketId']):t for t in token_rows};market_ids=set()
    for a in list(opened.values())+list(closed.values()):
        for v in a.get('tokenValues',[]):
            market_ids.add(str(v['token']['marketId']));tokens[str(v['token']['marketId'])]=v['token']
    for e in events:market_ids.update(l['market'] for l in e['legs'])
    opening_marks=source.marks(opening,market_ids);closing_marks=source.marks(closing,market_ids)
    rows=[];nonborrow=0;audits=[];failures=Counter()
    audit_candidates=[]
    for aid in sorted(ids):
        account=closed.get(aid) or opened.get(aid)
        if account is None:
            owner,number=aid.split('-',1)
            account={'id':aid,'user':{'id':owner},'effectiveUser':{'id':owner},'accountNumber':number}
        owner=account['user']['id'].lower();number=str(account['accountNumber'])
        op=par_map(opened.get(aid,{}));cp=par_map(closed.get(aid,{}));evs=by_account.get(aid,[])
        ending,was_borrowed=replay(op,evs,owner,number)
        if not (was_borrowed or any(v<0 for v in cp.values())):
            nonborrow+=1;continue
        reasons=[];boundary=[]
        for principal,marks in [(op,opening_marks),(cp,closing_marks)]:
            try:boundary.append(endpoint(principal,marks,tokens))
            except ValueError as error:boundary.append(None);reasons.append(str(error))
        a,z=boundary
        flows=row_flows(evs,owner,number,contexts)
        operation_peak=attach_flow_nav(op,evs,owner,number,contexts,flows)
        # If liquidation routing also uses capital transfer actions, the route
        # is not inferred from a flat transfer. Keep such economic attribution unknown.
        liq_txs=liquidation_txs|{e['txHash'] for e in evs if e['kind'] in ('liquidation','vaporization')}
        for f in flows:
            if f['eventRef'].split(':')[0] in liq_txs:f.update(usd36=None,reason='ambiguous_liquidation_route')
        perf=performance(a['equityUsd36'] if a else None,z['equityUsd36'] if z else None,flows,coverage=True,replay=ending==cp,reasons=reasons)
        if a and z and perf['pnl']['status']=='verified':audit_candidates.append((account,a,z,perf['pnl']['valueUsdExact']))
        for r in perf['pnl']['reasonCodes']:failures[r]+=1
        size=historical_size(a,z,operation_peak,flows)
        row=dict(chain=chain,chainId=source.config['chainId'],address=owner,effectiveAddress=(account.get('effectiveUser') or account['user'])['id'],accountId=aid,accountNumber=number,
                 closed=not cp,openingCollateralUsdExact=from_usd36(a['supplyUsd36']) if a else None,openingDebtUsdExact=from_usd36(a['borrowUsd36']) if a else None,closingCollateralUsdExact=from_usd36(z['supplyUsd36']) if z else None,closingDebtUsdExact=from_usd36(z['borrowUsd36']) if z else None,openingEquityUsdExact=from_usd36(a['equityUsd36']) if a else None,closingEquityUsdExact=from_usd36(z['equityUsd36']) if z else None,
                 historicalSizeUsdExact=from_usd36(size) if size is not None else None,currentSizeUsdExact=from_usd36(z['supplyUsd36']+z['borrowUsd36']) if z else None,
                 historicalSizeMethod='max_observed_boundary_operation_gross_exposure_or_verified_flow',openingBalances=public_balances(a),closingBalances=public_balances(z),
                 tokens=sorted({tokens[m]['symbol'] for m in set(op)|set(cp)|{l['market'] for e in evs for l in e['legs'] if l['owner']==owner and l['account']==number} if m in tokens}),
                 totalInflowUsdExact=from_usd36(sum(f['usd36'] for f in flows if f['usd36']>0)) if all(f['usd36'] is not None for f in flows) else None,totalOutflowUsdExact=from_usd36(-sum(f['usd36'] for f in flows if f['usd36']<0)) if all(f['usd36'] is not None for f in flows) else None,eventCount=len(evs),externalFlowCount=len(flows),returnProof=public_return_proof(flows) if perf['return']['status']=='verified' else [],provenance={'endpointMethod':'graph_par_rpc_current_index_oracle','eventRangeComplete':True,'endingParReconciled':ending==cp,'flowMethod':'operation_oracle_direct_owner_or_position_transfer','boundaryGetterAudited':False,'boundaryBalancesHash':digest({'opening':a,'closing':z})},**perf)
        rows.append(row)
    # Deterministic stratification: first/last/positive/negative/reduced/closed.
    candidates={}
    for c in audit_candidates[:2]+audit_candidates[-2:]:candidates[c[0]['id']]=c
    for predicate in [lambda c:int(c[3].replace('.','').replace('-','') or 0)>0 and not c[3].startswith('-'),lambda c:c[3].startswith('-'),lambda c:0<c[2]['supplyUsd36']<c[1]['supplyUsd36']//2,lambda c:not c[2]['balances']]:
        for c in audit_candidates:
            if predicate(c):candidates[c[0]['id']]=c;break
    for aid,(account,a,z,_) in candidates.items():
        audits.append(getter_audit(source,opening,account,a));audits.append(getter_audit(source,closing,account,z))
    audited=set(candidates)
    for row in rows:row['provenance']['boundaryGetterAudited']=row['accountId'] in audited
    # Hash checks at both boundaries detect reorganization during collection.
    for header in [opening,closing]:
        if source.block(header['number'])['hash']!=header['hash']:raise ValueError('Pinned boundary reorganization')
    final_meta=source.graph('{_meta(block:{number:%d}){deployment hasIndexingErrors}}'%closing['number'])['_meta']
    if final_meta['hasIndexingErrors'] or final_meta['deployment']!=meta['deployment']:raise ValueError('Graph deployment changed')
    manifest=dict(chainId=source.config['chainId'],margin=source.margin.lower(),graphDeployment=meta['deployment'],opening=opening,closing=closing,openingNextBlock=next_open,
                  coverage={'complete':True,'openingBorrowerPages':nopen,'closingBorrowerPages':nclose,'additionalOpeningPages':p1,'additionalClosingPages':p2,'eventPages':event_pages,'graphEventCounts':family_counts,'rawBalanceEvents':len(events),'rangeShards':len(ranges),'rangesHash':digest(ranges),'graphRpcEventIdentitiesMatch':True,'discoveredAccounts':len(ids),'borrowPositions':len(rows),'nonBorrowAccounts':nonborrow},
                  valuation={'method':'exact_graph_par_times_rpc_current_index_times_rpc_oracle','rounding':'Solidity Interest.parToWei half-up both signs','accountValuesSemantics':'cached_index' if chain=='arbitrum' else 'current_index','sampleGetterAudits':audits},
                  failures=dict(failures),unavailableMarkets={'opening':{m:v for m,v in opening_marks.items() if v.get('error')},'closing':{m:v for m,v in closing_marks.items() if v.get('error')}},sourceHashes=sorted(source.refs),sourceCacheManifestHash=digest(sorted(source.refs)),runtimeSeconds=round(time.time()-started,3))
    chain_output={'manifest':manifest,'positions':rows}
    (source.cache/'latest-chain.json').write_text(json.dumps(chain_output,separators=(',',':')))
    print(chain,'complete',len(rows),'positions',sum(r['pnl']['status']=='verified' for r in rows),'verified P&L',sum(r['return']['status']=='verified' for r in rows),'verified returns',flush=True)
    return chain_output


def prune_cache(root,manifests):
    """Only after complete successful publication: keep its immutable proofs."""
    for chain,manifest in manifests.items():
        keep=set(manifest['sourceHashes'])
        for path in (Path(root)/chain).glob('*.json'):
            if re.fullmatch(r'[0-9a-f]{64}',path.stem) and path.stem not in keep:
                path.unlink()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache',default='.cache/borrow-position-pnl')
    parser.add_argument('--output',default='borrow_position_pnl.json')
    parser.add_argument('--target-timestamp',type=int)
    parser.add_argument('--chain',choices=list(CONFIG),help='Collect one chain evidence only; never publish an incomplete universe')
    parser.add_argument('--assemble-cache',action='store_true',help='Validate and assemble completed per-chain evidence')
    args=parser.parse_args();target=args.target_timestamp or int(time.time())-900
    chains=[args.chain] if args.chain else list(CONFIG)
    if args.assemble_cache:
        outputs=[json.loads((Path(args.cache)/c/'latest-chain.json').read_text()) for c in CONFIG]
    else:
        with ThreadPoolExecutor(max_workers=3) as pool:outputs=list(pool.map(lambda c:collect_chain(c,args.cache,target),chains))
    if args.chain:return
    manifests={c:o['manifest'] for c,o in zip(CONFIG,outputs)};rows=[row for o in outputs for row in o['positions']]
    now=int(time.time())
    if any(now-m['closing']['timestamp']>21600 for m in manifests.values()):raise ValueError('Snapshot source exceeds six-hour freshness')
    if max(m['closing']['timestamp'] for m in manifests.values())-min(m['closing']['timestamp'] for m in manifests.values())>1800:raise ValueError('Chain cutoff skew exceeds 30 minutes')
    verified=sum(r['pnl']['status']=='verified' for r in rows)
    if not verified:raise ValueError('No verified real positions; preserve previous snapshot')
    snapshot=dict(schemaVersion=1,methodology='dolomite-oracle-marked-equity-v1',generatedAt=datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),windowSeconds=2592000,maxAgeSeconds=21600,
                  sourceAsOf=min(m['closing']['timestamp'] for m in manifests.values()),chains=manifests,summary={'positions':len(rows),'verifiedPnl':verified,'verifiedReturn':sum(r['return']['status']=='verified' for r in rows),'unavailablePnl':len(rows)-verified,'closedPositions':sum(r['closed'] for r in rows)},positions=rows)
    atomic_write(args.output,snapshot);prune_cache(args.cache,manifests);print('Published local snapshot',args.output,snapshot['summary'],flush=True)

if __name__=='__main__':main()
