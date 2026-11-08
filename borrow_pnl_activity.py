#!/usr/bin/env python3
"""Exact, lazy position activity. This module never collects or computes P&L."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil

from build_borrow_position_pnl import from_usd36, par_to_wei, usd36

DEFAULT_DIRECTORY='data/borrow-position-pnl-history'
CHAINS={'ethereum':1,'arbitrum':42161,'berachain':80094}
BUCKETS='0123456789abcdef'
INTEGER=r'(?:0|-?[1-9][0-9]*)'
NATURAL=r'(?:0|[1-9][0-9]*)'
HASH=r'0x[0-9a-f]{64}'
UNAVAILABLE_REASONS={'missing_boundary_valuation','missing_operation_mark','missing_event_timestamp'}


class ActivityUnavailable(ValueError):
    """A source gap suppresses history without changing the P&L universe."""



def _bytes(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def _digest(value):
    return hashlib.sha256(_bytes(value)).hexdigest()


def _match(value,pattern):
    if not isinstance(value,str) or not re.fullmatch(pattern,value):raise ValueError('Invalid activity string')
    return value


def _int(value,positive=False):
    result=int(_match(value,r'[1-9][0-9]*' if positive else INTEGER))
    return result


def _natural(value):
    if type(value) is not int or value<0:raise ValueError('Invalid activity integer')
    return value


def bucket(account_id):
    return hashlib.sha256(account_id.encode()).hexdigest()[0]


def generation(snapshot):
    boundaries={c:{k:snapshot['chains'][c][k]['hash'] for k in ('opening','closing')} for c in CHAINS}
    return _digest(boundaries)[:24]


def _boundary(point,marks):
    if point is None:raise ActivityUnavailable('missing_boundary_valuation')
    return [dict(b,supplyIndexRaw=str(marks[b['marketId']]['supply']),borrowIndexRaw=str(marks[b['marketId']]['borrow'])) for b in point['balances']]


def export_position(opening,closing,events,owner,number,contexts,opening_point,closing_point,opening_marks,closing_marks):
    """Keep emitted Wei deltas separate from Par-derived rounded balances."""
    state=dict(opening);out=[]
    for event in events:
        ctx=contexts.get((event['txHash'],event['logIndex']))
        if type(event.get('timestamp')) is not int:raise ActivityUnavailable('missing_event_timestamp')
        if not ctx:raise ActivityUnavailable('missing_operation_mark')
        legs=[]
        for leg in event['legs']:
            if (leg['owner'],leg['account'])!=(owner,number):continue
            market=leg['market'];index=ctx['indices'].get(market);price=ctx['prices'].get(market)
            if not index or not price or price<=0:raise ActivityUnavailable('missing_operation_mark')
            before=state.get(market,0);after=leg['new_par']
            before_wei=par_to_wei(before,index['supply'],index['borrow']);after_wei=par_to_wei(after,index['supply'],index['borrow'])
            legs.append(dict(marketId=market,deltaWeiRawSigned=str(leg['delta']),beforeParRawSigned=str(before),afterParRawSigned=str(after),beforeWeiRawSigned=str(before_wei),afterWeiRawSigned=str(after_wei),supplyIndexRaw=str(index['supply']),borrowIndexRaw=str(index['borrow']),oraclePriceRaw=str(price),deltaUsdExact=from_usd36(leg['delta']*price),beforeUsdExact=from_usd36(before_wei*price),afterUsdExact=from_usd36(after_wei*price)))
            if after:state[market]=after
            else:state.pop(market,None)
        if not legs:raise ValueError('Activity event has no own account leg')
        out.append(dict(kind=event['kind'],transactionHash=event['txHash'],blockHash=event['blockHash'],blockNumber=event['blockNumber'],timestamp=event['timestamp'],transactionIndex=event['txIndex'],logIndex=event['logIndex'],operationRef=ctx['id'],legs=legs,closedAfter=not state))
    if state!=closing:raise ValueError('Activity ending principal mismatch')
    return dict(openingBalances=_boundary(opening_point,opening_marks),closingBalances=_boundary(closing_point,closing_marks),events=out)


def validate_metadata(snapshot):
    """Optional additive metadata; historical snapshots remain valid without it."""
    try:
        activity=snapshot.get('activity')
        if activity is None:
            if any('activityBucket' in r for r in snapshot['positions']):raise ValueError('Activity bucket without manifest')
            return snapshot
        if type(activity['schemaVersion']) is not int or activity['schemaVersion']!=1 or activity['generation']!=generation(snapshot):raise ValueError('Invalid activity generation')
        expected={c+'-'+b for c in CHAINS for b in BUCKETS}
        if set(activity['files'])!=expected:raise ValueError('Incomplete activity file manifest')
        for key,ref in activity['files'].items():
            if ref['path']!=DEFAULT_DIRECTORY+'/'+activity['generation']+'/'+key+'.json':raise ValueError('Invalid activity path')
            _match(ref['sha256'],r'[0-9a-f]{64}')
        for row in snapshot['positions']:
            if row['chain'] not in CHAINS or row['activityBucket']!=bucket(row['accountId']):raise ValueError('Wrong activity bucket')
        return snapshot
    except (KeyError,TypeError,AttributeError) as error:
        raise ValueError('Malformed activity metadata') from error


def _validate_balances(balances,tokens):
    state={}
    for b in balances:
        market=_match(b['marketId'],NATURAL);token=tokens[market]
        if market in state:raise ValueError('Duplicate activity balance')
        par=_int(b['parRawSigned']);wei=_int(b['weiRawSigned']);price=_int(b['oraclePriceRaw'],True)
        supply=_int(b['supplyIndexRaw'],True);borrow=_int(b['borrowIndexRaw'],True)
        if not par or wei!=par_to_wei(par,supply,borrow) or usd36(b['usdExact'])!=wei*price:raise ValueError('Invalid exact activity boundary')
        if any(b[k]!=token[k] for k in ('address','symbol','decimals')):raise ValueError('Activity token identity mismatch')
        state[market]=par
    return state


def _validate_position(item,row,manifest,tokens):
    if item.get('status')=='unavailable':
        reasons=item['reasonCodes']
        if not isinstance(reasons,list) or not reasons or any(r not in UNAVAILABLE_REASONS for r in reasons) or any(item[k]!=[] for k in ('openingBalances','closingBalances','events')):raise ValueError('Invalid unavailable history')
        return
    state=_validate_balances(item['openingBalances'],tokens);closing=_validate_balances(item['closingBalances'],tokens)
    for field in ('openingBalances','closingBalances'):
        public={b['marketId']:b for b in row[field]};full={b['marketId']:b for b in item[field]}
        if set(public)!=set(full) or any(any(full[m].get(k)!=v for k,v in b.items()) for m,b in public.items()):raise ValueError('Activity/public boundary mismatch')
    events=item['events']
    if len(events)!=row['eventCount']:raise ValueError('Activity event count mismatch')
    last=None;operation_marks={};operation_order={};block_headers={};transactions={}
    for event in events:
        order=tuple(_natural(event[k]) for k in ('blockNumber','transactionIndex','logIndex'))
        if last is not None and order<=last:raise ValueError('Activity events out of order')
        last=order;timestamp=_natural(event['timestamp']);tx=_match(event['transactionHash'],HASH);block_hash=_match(event['blockHash'],HASH)
        if not manifest['opening']['number']<order[0]<=manifest['closing']['number'] or not manifest['opening']['timestamp']<=timestamp<=manifest['closing']['timestamp']:raise ValueError('Activity outside pinned window')
        if order[0]==manifest['closing']['number'] and (block_hash,timestamp)!=(manifest['closing']['hash'],manifest['closing']['timestamp']):raise ValueError('Activity closing block mismatch')
        if block_headers.setdefault(order[0],(block_hash,timestamp))!=(block_hash,timestamp) or transactions.setdefault(tx,(order[0],order[1],block_hash,timestamp))!=(order[0],order[1],block_hash,timestamp):raise ValueError('Conflicting activity block/transaction')
        operation=_match(event['operationRef'],HASH+r':0x[0-9a-f]+');op_tx,op_log=operation.split(':');op_log=int(op_log,16)
        if op_tx!=tx or op_log>=order[2] or op_log<operation_order.get(tx,-1):raise ValueError('Invalid activity operation')
        operation_order[tx]=op_log
        if event['kind'] not in ('deposit','withdraw','transfer','trade','liquidation','vaporization') or not event['legs']:raise ValueError('Invalid activity kind/legs')
        for leg in event['legs']:
            market=_match(leg['marketId'],NATURAL)
            if market not in tokens:raise ValueError('Unknown activity token')
            before=_int(leg['beforeParRawSigned']);after=_int(leg['afterParRawSigned']);delta=_int(leg['deltaWeiRawSigned'])
            supply=_int(leg['supplyIndexRaw'],True);borrow=_int(leg['borrowIndexRaw'],True);price=_int(leg['oraclePriceRaw'],True)
            if operation_marks.setdefault((operation,market),(supply,borrow,price))!=(supply,borrow,price):raise ValueError('Conflicting activity operation mark')
            before_wei=par_to_wei(before,supply,borrow);after_wei=par_to_wei(after,supply,borrow)
            if before!=state.get(market,0) or _int(leg['beforeWeiRawSigned'])!=before_wei or _int(leg['afterWeiRawSigned'])!=after_wei:raise ValueError('Activity principal replay mismatch')
            # Emitted newPar is authoritative. Vaporization targets and
            # Par-denominated actions need not round-trip an emitted Wei delta.
            if any(usd36(leg[k])!=v*price for k,v in (('beforeUsdExact',before_wei),('afterUsdExact',after_wei),('deltaUsdExact',delta))):raise ValueError('Activity marked value mismatch')
            if after:state[market]=after
            else:state.pop(market,None)
        if type(event['closedAfter']) is not bool or event['closedAfter']!=(not state):raise ValueError('Activity closure mismatch')
    if state!=closing or row['closed']!=(not closing):raise ValueError('Activity ending principal mismatch')


def validate_bundle(snapshot,root):
    """Validate bytes and all own-account replay before publishing any file."""
    validate_metadata(snapshot)
    if 'activity' not in snapshot:raise ValueError('Missing required activity bundle')
    root=Path(root);rows={(r['chain'],r['accountId']):r for r in snapshot['positions']};seen=set()
    try:
        for key,ref in snapshot['activity']['files'].items():
            raw=(root/ref['path']).read_bytes()
            if hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('Activity file hash mismatch')
            data=json.loads(raw);chain,part=key.rsplit('-',1);manifest=snapshot['chains'][chain]
            if data['schemaVersion']!=1 or data['chain']!=chain or data['chainId']!=CHAINS[chain] or data['windowSeconds']!=snapshot['windowSeconds'] or data['opening']!=manifest['opening'] or data['closing']!=manifest['closing'] or data['sourceCacheManifestHash']!=manifest['sourceCacheManifestHash']:raise ValueError('Activity source binding mismatch')
            tokens=data['tokens']
            for market,token in tokens.items():
                _match(market,NATURAL);_match(token['address'],r'0x[0-9a-f]{40}')
                if not isinstance(token['symbol'],str) or not token['symbol'] or type(token['decimals']) is not int or not 0<=token['decimals']<=255:raise ValueError('Invalid activity token')
            for aid,item in data['positions'].items():
                row=rows[(chain,aid)]
                if bucket(aid)!=part or (chain,aid) in seen:raise ValueError('Activity position bucket/duplicate mismatch')
                seen.add((chain,aid));_validate_position(item,row,manifest,tokens)
        if seen!=set(rows):raise ValueError('Incomplete activity position coverage')
    except (KeyError,TypeError,AttributeError,OSError,json.JSONDecodeError) as error:
        raise ValueError('Malformed or missing activity bundle') from error
    return snapshot


def write_bundle(snapshot,chains,root):
    """Immutable files first; the caller atomically replaces the parent snapshot."""
    root=Path(root);gen=generation(snapshot);files={}
    for chain,chain_id in CHAINS.items():
        manifest=snapshot['chains'][chain];source=chains[chain]
        tokens={m:dict(address=t['id'],symbol=t['symbol'],decimals=int(t['decimals'])) for m,t in source['tokens'].items()}
        for part in BUCKETS:
            key=chain+'-'+part;relative=DEFAULT_DIRECTORY+'/'+gen+'/'+key+'.json'
            data=dict(schemaVersion=1,chain=chain,chainId=chain_id,windowSeconds=snapshot['windowSeconds'],opening=manifest['opening'],closing=manifest['closing'],sourceCacheManifestHash=manifest['sourceCacheManifestHash'],tokens=tokens,positions={a:p for a,p in source['positions'].items() if bucket(a)==part})
            raw=_bytes(data);path=root/relative;path.parent.mkdir(parents=True,exist_ok=True)
            if path.exists() and path.read_bytes()!=raw:raise ValueError('Refusing to overwrite immutable activity generation')
            if not path.exists():
                temp=path.with_suffix('.tmp');temp.write_bytes(raw);os.replace(temp,path)
            files[key]=dict(path=relative,sha256=hashlib.sha256(raw).hexdigest())
    snapshot['activity']=dict(schemaVersion=1,generation=gen,files=files)
    for row in snapshot['positions']:row['activityBucket']=bucket(row['accountId'])
    validate_bundle(snapshot,root)
    return snapshot


def prune_generations(root,now):
    """Retain every unexpired parent plus at least the latest four generations."""
    directory=Path(root)/DEFAULT_DIRECTORY
    if not directory.exists():return
    generations=[]
    for path in directory.iterdir():
        if not path.is_dir() or not re.fullmatch(r'[0-9a-f]{24}',path.name):continue
        try:
            headers=[json.loads((path/(chain+'-0.json')).read_text()) for chain in CHAINS]
            cutoff=min(h['closing']['timestamp'] for h in headers)
            if any(type(h['closing']['timestamp']) is not int for h in headers):raise ValueError('Invalid generation cutoff')
        except (OSError,KeyError,TypeError,ValueError):
            # Unknown directories are never silently deleted as "old" evidence.
            raise ValueError('Cannot determine retained activity generation age') from None
        generations.append((cutoff,path))
    generations.sort(key=lambda x:(x[0],x[1].name),reverse=True)
    for cutoff,path in generations[4:]:
        if now-cutoff>21600:shutil.rmtree(path)


if __name__=='__main__':
    import argparse
    import time
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prune',required=True,choices=[DEFAULT_DIRECTORY])
    args=parser.parse_args()
    prune_generations(Path.cwd(),now=int(time.time()))
