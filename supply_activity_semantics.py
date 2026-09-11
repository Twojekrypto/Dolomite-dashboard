"""Pinned subgraph Par replay. Amounts and effects are decimal token units."""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation, localcontext, ROUND_HALF_UP


def number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f'Invalid semantic decimal: {value!r}') from exc
    if not result.is_finite():
        raise RuntimeError('Non-finite semantic decimal')
    return result


def text(value):
    result = format(value, 'f')
    return result.rstrip('0').rstrip('.') if '.' in result else result


def effects(before, after, index, decimals=None):
    if not index or index.get('supplyIndex') is None or index.get('borrowIndex') is None:
        return None, None
    with localcontext() as ctx:
        ctx.prec = 100
        before, after = number(before), number(after)
        supply, borrow = number(index['supplyIndex']), number(index['borrowIndex'])
        if supply <= 0 or borrow <= 0:
            raise RuntimeError('Invalid historical interest index')
        def rounded(value):
            return value if decimals is None else value.quantize(Decimal(1).scaleb(-int(decimals)),rounding=ROUND_HALF_UP)
        return (text(rounded(max(after, 0)*supply)-rounded(max(before, 0)*supply)),
                text(rounded(max(-after, 0)*borrow)-rounded(max(-before, 0)*borrow)))


def event_legs(kind, event, token_id):
    """Map source signed Par fields, never infer signs from absolute Wei."""
    tx = event['transaction']
    legs = []
    side = 'borrowed' if (event.get('borrowedToken') or {}).get('id') == token_id else 'held'
    source_id = f"{kind}-{event['serialId']}" + (f'-{side}' if kind in {'liquidation','vaporization'} else '')

    def add(role, delta, index, amount='amountDeltaWei', usd='amountUSDDeltaWei', ambiguous=False):
        account = event.get(role+'MarginAccount' if role else 'marginAccount')
        user = event.get(role+'EffectiveUser' if role else 'effectiveUser')
        if not account or not user or delta not in event or event[delta] is None:
            raise RuntimeError(f'Missing account/delta in {source_id}: {role}')
        counterpart = {'from':'to','to':'from','liquid':'solid','vapor':'solid','solid':'liquid' if kind=='liquidation' else 'vapor','taker':'maker','maker':'taker'}.get(role)
        secondary = (event.get(counterpart+'EffectiveUser') or {}).get('id','') if counterpart else ''
        legs.append({'id':event['id']+'-'+role+'-'+token_id,'sourceId':source_id,'type':kind,
                     'account':account['id'],'wallet':user['id'],'tokenId':token_id,'deltaPar':text(number(event[delta])),
                     'index':event.get(index),'amount':event.get(amount,'0'),'usd':event.get(usd,'0'),'secondaryAddress':secondary,
                     'txHash':tx['id'],'timestamp':int(tx['timestamp']),'blockNumber':int(tx['blockNumber']),
                     'logIndex':int(event['logIndex']),'ambiguous':ambiguous,'zaps':tx.get('zaps',[])})
    if kind in {'deposit','withdraw'}:
        add('', 'amountDeltaPar','interestIndex')
    elif kind == 'transfer':
        add('from','fromAmountDeltaPar','interestIndex')
        add('to','toAmountDeltaPar','interestIndex')
    elif kind == 'trade':
        is_input = event['takerToken']['id'] == token_id
        prefix = 'taker' if is_input else 'maker'
        # Upstream normalizes Par signs but does not swap token ids for reverse
        # LogTrade directions. External LogSell/LogBuy (no maker) is directional;
        # two-account trades require additional evidence, unavailable here.
        ambiguous = bool(event.get('makerMarginAccount'))
        add('taker','takerInputTokenDeltaPar' if is_input else 'takerOutputTokenDeltaPar',prefix+'InterestIndex',prefix+'TokenDeltaWei',prefix+'AmountUSD',ambiguous)
        if event.get('makerMarginAccount'):
            add('maker','makerInputTokenDeltaPar' if is_input else 'makerOutputTokenDeltaPar',prefix+'InterestIndex',prefix+'TokenDeltaWei',prefix+'AmountUSD',True)
    elif kind in {'liquidation','vaporization'}:
        title=side.title()
        add('solid','solid'+title+'TokenAmountDeltaPar',side+'InterestIndex',side+'TokenAmountDeltaWei',side+'TokenAmountUSD')
        if kind == 'liquidation':
            add('liquid','liquid'+title+'TokenAmountDeltaPar',side+'InterestIndex',side+'TokenAmountDeltaWei',side+'TokenAmountUSD')
        elif side == 'borrowed':
            add('vapor','vaporBorrowedTokenAmountDeltaPar','borrowedInterestIndex','borrowedTokenAmountDeltaWei','amountUSDVaporized')
    else:
        raise RuntimeError('Unsupported balance-changing event '+kind)
    return legs


def replay(legs, balances, block_number, require_zero=False):
    if balances is None:
        raise RuntimeError('Missing pinned balance baseline')
    if len({leg['id'] for leg in legs}) != len(legs):
        raise RuntimeError('Duplicate balance-changing leg')
    with localcontext() as ctx:
        ctx.prec = 100
        current = {account: number(value) for account, value in balances.items()}
        tainted = {leg['account'] for leg in legs if leg.get('ambiguous')}
        groups = defaultdict(list)
        for leg in sorted(legs, key=lambda x:(x['blockNumber'], x['logIndex']), reverse=True):
            if leg['blockNumber'] > block_number:
                raise RuntimeError('Event newer than pinned baseline')
            account = leg['account']
            after = current.get(account, Decimal(0)) # exhaustive baseline omits closed accounts
            before = after-number(leg['deltaPar'])
            current[account] = before
            evidence = dict(leg, beforePar=text(before), afterPar=text(after))
            evidence['supplyChange'], evidence['debtChange'] = effects(before, after, leg.get('index'),leg.get('decimals'))
            if account in tainted:
                evidence['supplyChange'] = evidence['debtChange'] = None
            groups[(leg['txHash'],leg['wallet'])].append(evidence)
        if require_zero and any(value != 0 for account,value in current.items() if account not in tainted):
            raise RuntimeError('Full replay has nonzero initial account Par; incomplete or inconsistent source')
        rows = []
        for (tx_hash, wallet), evidence in groups.items():
            evidence.sort(key=lambda x:x['logIndex'])
            first = evidence[0]
            unavailable = any(leg['account'] in tainted for leg in evidence)
            actions = {leg['type'] for leg in evidence}
            supply_par = sum((max(number(leg['afterPar']),0)-max(number(leg['beforePar']),0) for leg in evidence),Decimal(0))
            debt_par = sum((max(-number(leg['afterPar']),0)-max(-number(leg['beforePar']),0) for leg in evidence),Decimal(0))
            if not unavailable:
                if supply_par <= 0: actions.discard('deposit')
                if supply_par >= 0: actions.discard('withdraw')
                if debt_par > 0: actions.add('borrow')
                if debt_par < 0 and not actions.intersection({'liquidation','vaporization'}): actions.add('repay')
            zaps = {zap['id']:zap for leg in evidence for zap in leg.get('zaps',[])
                    if (zap.get('effectiveUser') or {}).get('id') == wallet
                    and leg.get('tokenId') in {token['id'] for token in zap.get('tokenPath',[])}}
            zaps = {key:dict(zap,orientationStatus='unavailable') for key,zap in zaps.items()}
            for leg in evidence:
                leg.pop('zaps',None) # routes are stored once per transaction/wallet
            if zaps: actions.add('zap')
            if actions.intersection({'trade','zap','borrow','repay','liquidation','vaporization'}):
                actions.discard('transfer')
            if not actions:
                # A net-zero deposit/withdraw round trip is still an observed
                # operation; retain technical actions instead of an empty filter.
                actions = {leg['type'] for leg in evidence}
            exact = not unavailable and all(leg['supplyChange'] is not None for leg in evidence)
            meta = {'version':2,'actions':sorted(actions),'status':'unavailable' if unavailable else 'verified',
                    'supplyChange':text(sum((number(x['supplyChange']) for x in evidence),Decimal(0))) if exact else None,
                    'debtChange':text(sum((number(x['debtChange']) for x in evidence),Decimal(0))) if exact else None,
                    'quantityStatus':'verified' if exact else 'unavailable',
                    'note':'Ambiguous upstream trade token direction; account replay unavailable.' if unavailable else ('Historical interest index unavailable; signs verified from Par.' if not exact else 'Selected-market effects from pinned subgraph Par replay and event indices.'),
                    'legs':evidence,'routes':list(zaps.values()),'blockNumber':block_number,
                    'sourceIds':sorted({leg['sourceId'] for leg in evidence})}
            amount = text(abs(number(meta['supplyChange'])-number(meta['debtChange']))) if exact else first.get('amount','0')
            meta['amountBasis'] = 'net-wallet-balance-change' if exact else 'legacy-source-leg'
            meta['usdStatus'] = 'verified' if exact and len(meta['sourceIds']) == 1 and number(amount) == abs(number(first.get('amount','0'))) else 'unavailable'
            for leg in evidence:
                if leg['account'] in tainted:
                    leg['beforePar'] = leg['afterPar'] = None
            # Keep the legacy primary event type. A stable wallet suffix prevents
            # collisions for two-sided transfers/liquidations in the same tx.
            rows.append({'id':first['sourceId']+'-wallet-'+wallet,'type':first['type'],
                         'timestamp':first['timestamp'],'txHash':tx_hash,
                         'amount':amount,'usd':first.get('usd','0') if meta['usdStatus']=='verified' else '0',
                         'primaryAddress':wallet,'secondaryAddress':first.get('secondaryAddress',''), 'semantics':meta})
        return sorted(rows,key=lambda row:row['timestamp'],reverse=True)


def fetch_semantic_rows(endpoint, token_id, max_pages, since_ts, post, paginate):
    meta = post(endpoint, {'query':'{ _meta { block { number } hasIndexingErrors } }'}).get('_meta')
    if not meta or meta.get('hasIndexingErrors') is not False:
        raise RuntimeError('Missing or unhealthy subgraph head')
    block = int(meta['block']['number'])
    token = post(endpoint, {'query':f'{{ token(id: "{token_id}", block: {{ number: {block} }}) {{ decimals totalPar {{ supplyPar borrowPar }} }} }}'}).get('token')
    if not token or not token.get('totalPar'):
        raise RuntimeError('Missing pinned token totalPar')
    index = ' { id supplyIndex borrowIndex updateTimestamp }'
    common = '''id serialId logIndex transaction { id timestamp blockNumber
        zaps(first:1000) { id effectiveUser { id } marginAccount { id }
            tokenPath { id symbol } amountInToken amountOutToken } }'''
    single = ' effectiveUser { id } marginAccount { id } amountDeltaPar amountDeltaWei amountUSDDeltaWei interestIndex'+index
    transfer = ' fromEffectiveUser { id } toEffectiveUser { id } fromMarginAccount { id } toMarginAccount { id } fromAmountDeltaPar toAmountDeltaPar amountDeltaWei amountUSDDeltaWei interestIndex'+index
    trade = ''' takerEffectiveUser { id } makerEffectiveUser { id } takerMarginAccount { id } makerMarginAccount { id }
        takerToken { id } makerToken { id } takerInputTokenDeltaPar takerOutputTokenDeltaPar makerInputTokenDeltaPar makerOutputTokenDeltaPar
        takerTokenDeltaWei makerTokenDeltaWei takerAmountUSD makerAmountUSD takerInterestIndex'''+index+' makerInterestIndex'+index
    liquidation = ''' solidEffectiveUser { id } liquidEffectiveUser { id } solidMarginAccount { id } liquidMarginAccount { id }
        borrowedToken { id } heldToken { id } solidBorrowedTokenAmountDeltaPar solidHeldTokenAmountDeltaPar liquidBorrowedTokenAmountDeltaPar liquidHeldTokenAmountDeltaPar
        borrowedTokenAmountDeltaWei heldTokenAmountDeltaWei borrowedTokenAmountUSD heldTokenAmountUSD borrowedInterestIndex'''+index+' heldInterestIndex'+index
    vaporization = ''' solidEffectiveUser { id } vaporEffectiveUser { id } solidMarginAccount { id } vaporMarginAccount { id }
        borrowedToken { id } heldToken { id } solidBorrowedTokenAmountDeltaPar solidHeldTokenAmountDeltaPar vaporBorrowedTokenAmountDeltaPar
        borrowedTokenAmountDeltaWei heldTokenAmountDeltaWei amountUSDVaporized borrowedInterestIndex'''+index+' heldInterestIndex'+index
    specs = [('deposit','deposits','token',single),('withdraw','withdrawals','token',single),('transfer','transfers','token',transfer)]
    specs += [('trade','trades',side,trade) for side in ['takerToken','makerToken']]
    specs += [('liquidation','liquidations',side,liquidation) for side in ['borrowedToken','heldToken']]
    specs += [('vaporization','vaporizations',side,vaporization) for side in ['borrowedToken','heldToken']]
    time_filter = f', transaction_: {{ timestamp_gte: "{int(since_ts)}" }}' if since_ts else ''
    with ThreadPoolExecutor(max_workers=5) as pool:
        baseline_future = pool.submit(paginate,endpoint,'marginAccountTokenValues',f'token: "{token_id}"','id marginAccount { id } valuePar',order_by='id',order_direction='asc',max_pages=max_pages,block_number=block)
        futures = [(kind,pool.submit(paginate,endpoint,entity,f'{field}: "{token_id}"'+time_filter,common+fields,max_pages=max_pages,block_number=block)) for kind,entity,field,fields in specs]
        baseline = baseline_future.result()
        events = [(kind,event) for kind,future in futures for event in future.result()]
    with localcontext() as ctx:
        ctx.prec = 100
        balances = {}
        for row in baseline:
            account = row['marginAccount']['id']
            if account in balances:
                raise RuntimeError('Duplicate account baseline')
            balances[account] = number(row['valuePar'])
        supply = sum((max(value,0) for value in balances.values()),Decimal(0))
        borrow = sum((max(-value,0) for value in balances.values()),Decimal(0))
        if supply != number(token['totalPar']['supplyPar']) or borrow != number(token['totalPar']['borrowPar']):
            raise RuntimeError(f'Pinned baseline totalPar mismatch: supply {supply}, borrow {borrow}')
    legs=[]
    seen=set()
    for kind,event in events:
        identity=(kind,event['id'])
        if identity in seen:
            continue # same entity returned by both selected-token side queries
        seen.add(identity)
        if len(event['transaction'].get('zaps',[])) >= 1000:
            raise RuntimeError('Zap evidence reaches nested page cap')
        event_items = event_legs(kind,event,token_id)
        for leg in event_items:
            leg['decimals'] = token.get('decimals')
        legs.extend(event_items)
    return replay(legs,balances,block,require_zero=since_ts is None)
