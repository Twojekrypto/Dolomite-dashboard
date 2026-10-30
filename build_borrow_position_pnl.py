#!/usr/bin/env python3
"""Exact oracle-marked borrow performance. Integer USD36, rational linked returns.

This module has no network or Decimal-context dependency. The collector supplies
source proofs; missing evidence is never coerced to a financial zero.
"""
import argparse
import json
import hashlib
import math
import os
import re
import time
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path

SCALE = 10 ** 36
INDEX_SCALE = 10 ** 18
DECIMAL = re.compile(r'^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$')


def usd36(value):
    if not isinstance(value, str) or not DECIMAL.fullmatch(value):
        raise ValueError('Expected exact decimal string')
    sign = -1 if value.startswith('-') else 1
    whole, _, fraction = value.lstrip('-').partition('.')
    if len(fraction) > 36:
        raise ValueError('USD precision exceeds 36 decimals')
    return sign * (int(whole) * SCALE + int((fraction + '0' * 36)[:36]))


def from_usd36(value):
    if not isinstance(value, int):
        raise ValueError('Expected integer USD36')
    sign = '-' if value < 0 else ''
    whole, fraction = divmod(abs(value), SCALE)
    suffix = str(fraction).zfill(36).rstrip('0')
    return sign + str(whole) + ('.' + suffix if suffix else '')


def par_to_wei(par, supply_index, borrow_index):
    """Interest.parToWei/getPartialRoundHalfUp, including negative debt."""
    index = supply_index if par >= 0 else borrow_index
    if index <= 0:
        raise ValueError('Invalid index')
    return (1 if par >= 0 else -1) * ((abs(par) * index + INDEX_SCALE // 2) // INDEX_SCALE)


def ratio_decimal(value, digits=36):
    """Truncate display ratio; numerator/denominator retain the exact rational."""
    sign = '-' if value < 0 else ''
    n, d = abs(value.numerator), value.denominator
    whole, rem = divmod(n, d)
    tail = str(rem * 10 ** digits // d).zfill(digits).rstrip('0')
    return sign + str(whole) + ('.' + tail if tail else '')


def performance(opening, closing, flows, *, coverage, replay, reasons=()):
    errors = list(reasons)
    if not coverage: errors.append('incomplete_event_coverage')
    if not replay: errors.append('ending_par_mismatch')
    if opening is None or closing is None: errors.append('missing_boundary_quote')
    if any(f.get('usd36') is None for f in flows):
        errors.extend(f.get('reason', 'unverified_external_flow') for f in flows if f.get('usd36') is None)
    errors = sorted(set(errors))
    pnl = {'status': 'unavailable', 'valueUsdExact': None, 'reasonCodes': errors}
    ret = {'status': 'unavailable', 'pctExact': None, 'ratioNumerator': None, 'ratioDenominator': None, 'reasonCodes': list(errors)}
    net = None if any(f.get('usd36') is None for f in flows) else sum(f['usd36'] for f in flows)
    result = {'pnl': pnl, 'return': ret, 'netInflowUsdExact': None if net is None else from_usd36(net)}
    if errors: return result
    pnl.update(status='verified', valueUsdExact=from_usd36(closing - opening - net))
    if opening <= 0:
        ret['reasonCodes'] = ['nonpositive_opening_equity']; return result
    factor, base = Fraction(1), opening
    for i, flow in enumerate(flows):
        pre, post = flow.get('preNavUsd36'), flow.get('postNavUsd36')
        if not flow.get('exactNav') or pre is None or post is None:
            ret['reasonCodes'] = ['missing_exact_flow_nav']; return result
        if post - pre != flow['usd36']:
            ret['reasonCodes'] = ['flow_nav_mismatch']; return result
        if base <= 0 or pre <= 0 or post < 0:
            ret['reasonCodes'] = ['nonpositive_interim_equity']; return result
        factor *= Fraction(pre, base)
        base = post
        if base == 0:
            if i != len(flows)-1 or closing != 0:
                ret['reasonCodes'] = ['zero_equity_then_activity']; return result
            break
    if base > 0:
        if closing < 0:
            ret['reasonCodes'] = ['negative_closing_equity']; return result
        factor *= Fraction(closing, base)
    value = factor - 1
    ret.update(status='verified', pctExact=ratio_decimal(value * 100), ratioNumerator=value.numerator, ratioDenominator=value.denominator, reasonCodes=[])
    # Integers serialized as strings to preserve arbitrary precision in JS.
    ret['ratioNumerator'], ret['ratioDenominator'] = str(value.numerator), str(value.denominator)
    return result


def _require_fields(value, fields):
    if not isinstance(value, dict) or not set(fields).issubset(value):
        raise ValueError('Missing required object fields: ' + ','.join(fields))


def _natural(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError('Expected nonnegative integer count/block/timestamp')
    return value


def _match(value, pattern):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError('Invalid identity/hash/integer string')
    return value


def _boolean(value):
    if type(value) is not bool: raise ValueError('Expected boolean proof flag')
    return value


def _validate_snapshot(data):
    chain_ids = {'ethereum': 1, 'arbitrum': 42161, 'berachain': 80094}
    families = {'deposits', 'withdrawals', 'transfers', 'trades', 'liquidations', 'vaporizations'}
    address_pattern, hash_pattern = r'0x[0-9a-f]{40}', r'0x[0-9a-f]{64}'
    uint_pattern, int_pattern = r'(?:0|[1-9][0-9]*)', r'(?:0|-?[1-9][0-9]*)'
    _require_fields(data, ('schemaVersion','methodology','generatedAt','sourceAsOf','maxAgeSeconds','windowSeconds','chains','summary','positions'))
    if type(data['schemaVersion']) is not int or data['schemaVersion'] != 1 or data['methodology'] != 'dolomite-oracle-marked-equity-v1':
        raise ValueError('Unsupported snapshot')
    if _natural(data['maxAgeSeconds']) != 21600 or _natural(data['windowSeconds']) != 2592000:
        raise ValueError('Invalid freshness/window contract')
    now = time.time()
    generated = datetime.fromisoformat(data['generatedAt'].replace('Z','+00:00'))
    if generated.utcoffset() is None or generated.utcoffset().total_seconds() != 0 or generated.timestamp() > now + 5:
        raise ValueError('Invalid UTC generation time')
    if not isinstance(data['chains'], dict) or set(data['chains']) != set(chain_ids):
        raise ValueError('Full three-chain manifest required')
    for chain, manifest in data['chains'].items():
        _require_fields(manifest, ('chainId','margin','graphDeployment','opening','closing','openingNextBlock','coverage','valuation','failures','unavailableMarkets','sourceHashes','sourceCacheManifestHash','runtimeSeconds'))
        if _natural(manifest['chainId']) != chain_ids[chain]: raise ValueError('Chain ID mismatch')
        _match(manifest['margin'], address_pattern)
        expected_margin = '0x6bd780e7fdf01d77e4d475c821f1e7ae05409072' if chain == 'arbitrum' else '0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d'
        if manifest['margin'] != expected_margin: raise ValueError('Wrong chain margin contract')
        if not isinstance(manifest['graphDeployment'],str) or not manifest['graphDeployment']: raise ValueError('Missing Graph deployment')
        for field in ('opening','closing','openingNextBlock'):
            header = manifest[field]; _require_fields(header, ('number','hash','timestamp'))
            _natural(header['number']); _natural(header['timestamp']); _match(header['hash'], hash_pattern)
        opening, closing, following = (manifest[field] for field in ('opening','closing','openingNextBlock'))
        if not 0 <= now - closing['timestamp'] <= data['maxAgeSeconds'] or generated.timestamp() < closing['timestamp']:
            raise ValueError('Stale/future chain closing boundary')
        target = closing['timestamp'] - data['windowSeconds']
        if not (opening['number'] < closing['number'] and following['number'] == opening['number'] + 1 and following['number'] <= closing['number'] and opening['timestamp'] <= target < following['timestamp'] <= closing['timestamp']):
            raise ValueError('Invalid thirty-day boundary proof')
        coverage = manifest['coverage']
        fields = ('complete','openingBorrowerPages','closingBorrowerPages','additionalOpeningPages','additionalClosingPages','eventPages','graphEventCounts','rawBalanceEvents','rangeShards','rangesHash','graphRpcEventIdentitiesMatch','discoveredAccounts','borrowPositions','nonBorrowAccounts')
        _require_fields(coverage, fields)
        if not _boolean(coverage['complete']) or not _boolean(coverage['graphRpcEventIdentitiesMatch']): raise ValueError('Incomplete source coverage')
        for field in ('openingBorrowerPages','closingBorrowerPages','rangeShards'): _natural(coverage[field],1)
        for field in ('additionalOpeningPages','additionalClosingPages','rawBalanceEvents','discoveredAccounts','borrowPositions','nonBorrowAccounts'): _natural(coverage[field])
        _match(coverage['rangesHash'], r'[0-9a-f]{64}')
        for field, minimum in (('eventPages',1),('graphEventCounts',0)):
            if not isinstance(coverage[field],dict) or set(coverage[field]) != families: raise ValueError('Missing event-family coverage')
            for value in coverage[field].values(): _natural(value,minimum)
        if sum(coverage['graphEventCounts'].values()) != coverage['rawBalanceEvents'] or coverage['discoveredAccounts'] != coverage['borrowPositions'] + coverage['nonBorrowAccounts']:
            raise ValueError('Source coverage counts disagree')
        source_hashes = manifest['sourceHashes']
        if not isinstance(source_hashes,list) or not source_hashes or source_hashes != sorted(set(source_hashes)): raise ValueError('Invalid source hash manifest')
        for value in source_hashes: _match(value,r'[0-9a-f]{64}')
        expected_hash = hashlib.sha256(json.dumps(source_hashes,separators=(',',':')).encode()).hexdigest()
        if manifest['sourceCacheManifestHash'] != expected_hash: raise ValueError('Source manifest hash mismatch')
        if type(manifest['runtimeSeconds']) not in (int,float) or not math.isfinite(manifest['runtimeSeconds']) or manifest['runtimeSeconds'] < 0: raise ValueError('Invalid runtime')
        if not isinstance(manifest['failures'],dict): raise ValueError('Invalid failure counts')
        for reason,count in manifest['failures'].items():
            _match(reason,r'[a-z0-9_]+'); _natural(count)
        _require_fields(manifest['unavailableMarkets'], ('opening','closing'))
        for failures in manifest['unavailableMarkets'].values():
            if not isinstance(failures,dict): raise ValueError('Invalid unavailable markets')
            for market,failure in failures.items():
                _match(market,uint_pattern); _require_fields(failure, ('error','failureType'))
                if failure['error'] not in ('contract_revert','provider_failure') or not isinstance(failure['failureType'],str): raise ValueError('Invalid market failure')
        valuation = manifest['valuation']
        _require_fields(valuation, ('method','rounding','accountValuesSemantics','sampleGetterAudits'))
        semantics = 'cached_index' if chain == 'arbitrum' else 'current_index'
        if valuation['method'] != 'exact_graph_par_times_rpc_current_index_times_rpc_oracle' or valuation['rounding'] != 'Solidity Interest.parToWei half-up both signs' or valuation['accountValuesSemantics'] != semantics or not isinstance(valuation['sampleGetterAudits'],list):
            raise ValueError('Invalid valuation proof')
        for audit in valuation['sampleGetterAudits']:
            _require_fields(audit, ('accountId','blockNumber','balancesMatch','balancesResponseHash','valuesResponseUsd36','valuesMatchCurrentIndex','valuesSemantics'))
            _match(audit['accountId'],address_pattern+'-'+uint_pattern)
            if _natural(audit['blockNumber']) not in (opening['number'],closing['number']) or not _boolean(audit['balancesMatch']) or audit['valuesSemantics'] != semantics: raise ValueError('Invalid getter audit')
            _boolean(audit['valuesMatchCurrentIndex']); _match(audit['balancesResponseHash'],r'[0-9a-f]{64}')
            if not isinstance(audit['valuesResponseUsd36'],list) or len(audit['valuesResponseUsd36']) != 2: raise ValueError('Invalid getter value pair')
            for value in audit['valuesResponseUsd36']: _match(value,uint_pattern)
    closings = [m['closing']['timestamp'] for m in data['chains'].values()]
    if _natural(data['sourceAsOf']) != min(closings) or max(closings)-min(closings) > 1800:
        raise ValueError('Inconsistent source boundary/skew')
    if not isinstance(data['positions'],list): raise ValueError('Invalid position list')
    seen, counts = set(), {chain:0 for chain in chain_ids}
    financial = ('openingEquityUsdExact','closingEquityUsdExact','openingCollateralUsdExact','openingDebtUsdExact','closingCollateralUsdExact','closingDebtUsdExact','netInflowUsdExact','totalInflowUsdExact','totalOutflowUsdExact','historicalSizeUsdExact','currentSizeUsdExact')
    fields = ('chain','chainId','address','effectiveAddress','accountId','accountNumber','closed','historicalSizeMethod','openingBalances','closingBalances','tokens','eventCount','externalFlowCount','pnl','return','returnProof','provenance') + financial
    for row in data['positions']:
        _require_fields(row, fields)
        if row['chain'] not in chain_ids or _natural(row['chainId']) != chain_ids[row['chain']]: raise ValueError('Invalid row chain')
        _match(row['address'],address_pattern); _match(row['effectiveAddress'],address_pattern); _match(row['accountNumber'],uint_pattern)
        if int(row['accountNumber']) >= 2**256 or row['accountId'] != row['address']+'-'+row['accountNumber']: raise ValueError('Invalid raw account identity')
        key = (row['chain'],row['address'],row['accountNumber'])
        if key in seen: raise ValueError('Duplicate position')
        seen.add(key); counts[row['chain']] += 1
        _boolean(row['closed']); _natural(row['eventCount']); _natural(row['externalFlowCount'])
        if row['eventCount'] > data['chains'][row['chain']]['coverage']['rawBalanceEvents']: raise ValueError('Row exceeds complete chain event count')
        if row['externalFlowCount'] > 2*row['eventCount']: raise ValueError('Invalid flow/event count')
        if not isinstance(row['tokens'],list) or any(not isinstance(t,str) or not t for t in row['tokens']) or len(set(row['tokens'])) != len(row['tokens']): raise ValueError('Invalid token labels')
        if row['historicalSizeMethod'] != 'max_observed_boundary_operation_gross_exposure_or_verified_flow': raise ValueError('Invalid historical size method')
        amounts = {field:None if row[field] is None else usd36(row[field]) for field in financial}
        for field in financial:
            if field not in ('openingEquityUsdExact','closingEquityUsdExact','netInflowUsdExact') and amounts[field] is not None and amounts[field] < 0: raise ValueError('Negative unsigned amount')
        known_sizes = []
        for boundary in ('opening','closing'):
            balances = row[boundary+'Balances']
            if not isinstance(balances,list): raise ValueError('Invalid balance list')
            supply = debt = 0; markets = set()
            for balance in balances:
                _require_fields(balance, ('marketId','address','symbol','decimals','usdExact'))
                _match(balance['marketId'],uint_pattern); _match(balance['address'],address_pattern); _natural(balance['decimals'])
                if balance['decimals'] > 255 or not isinstance(balance['symbol'],str) or balance['marketId'] in markets: raise ValueError('Invalid market metadata')
                markets.add(balance['marketId']); value = usd36(balance['usdExact'])
                supply += max(value,0); debt += max(-value,0)
            equity, collateral, borrowed = (amounts[boundary+name+'UsdExact'] for name in ('Equity','Collateral','Debt'))
            if equity is None:
                if collateral is not None or borrowed is not None or balances: raise ValueError('Unavailable endpoint must stay null')
            else:
                if collateral != supply or borrowed != debt or equity != supply-debt: raise ValueError('Boundary financial totals disagree')
                known_sizes.append(supply+debt)
                if boundary == 'closing' and (row['closed'] != (not balances) or amounts['currentSizeUsdExact'] != supply+debt): raise ValueError('Closing size/state mismatch')
        if amounts['closingEquityUsdExact'] is None and amounts['currentSizeUsdExact'] is not None: raise ValueError('Unpriced current size')
        if known_sizes and (amounts['historicalSizeUsdExact'] is None or amounts['historicalSizeUsdExact'] < max(known_sizes)): raise ValueError('Historical size below known exposure')
        net, inflow, outflow = (amounts[field] for field in ('netInflowUsdExact','totalInflowUsdExact','totalOutflowUsdExact'))
        if net is None:
            if inflow is not None or outflow is not None: raise ValueError('Incomplete capital totals')
        elif inflow is None or outflow is None or net != inflow-outflow: raise ValueError('Capital totals disagree')
        if row['externalFlowCount'] == 0 and (net,inflow,outflow) != (0,0,0): raise ValueError('No-flow position has capital flow')
        proof = row['provenance']
        _require_fields(proof, ('endpointMethod','eventRangeComplete','endingParReconciled','flowMethod','boundaryGetterAudited','boundaryBalancesHash'))
        for field in ('eventRangeComplete','endingParReconciled','boundaryGetterAudited'): _boolean(proof[field])
        _match(proof['boundaryBalancesHash'],r'[0-9a-f]{64}')
        if proof['endpointMethod'] != 'graph_par_rpc_current_index_oracle' or proof['flowMethod'] != 'operation_oracle_transaction_boundary_or_position_transfer' or not proof['eventRangeComplete']: raise ValueError('Invalid row proof method/coverage')
        for field,value_key in (('pnl','valueUsdExact'),('return','pctExact')):
            metric = row[field]; _require_fields(metric, ('status',value_key,'reasonCodes'))
            if not isinstance(metric['reasonCodes'],list) or any(not isinstance(r,str) or not r for r in metric['reasonCodes']): raise ValueError('Invalid metric reason list')
            if metric['status'] == 'verified':
                usd36(metric[value_key])
                if metric['reasonCodes']: raise ValueError('Verified metric has failure reasons')
            elif metric['status'] != 'unavailable' or metric[value_key] is not None or not metric['reasonCodes']: raise ValueError('Invalid unavailable metric')
        opening, closing = amounts['openingEquityUsdExact'], amounts['closingEquityUsdExact']
        if row['pnl']['status'] == 'verified':
            if not proof['endingParReconciled'] or opening is None or closing is None or net is None: raise ValueError('Missing verified P&L evidence')
            if usd36(row['pnl']['valueUsdExact']) != closing-opening-net: raise ValueError('Financial total mismatch')
        ret = row['return']; _require_fields(ret, ('ratioNumerator','ratioDenominator'))
        if not isinstance(row['returnProof'],list): raise ValueError('Missing return proof list')
        if ret['status'] == 'unavailable':
            if ret['ratioNumerator'] is not None or ret['ratioDenominator'] is not None or row['returnProof']: raise ValueError('Unavailable return must not claim proof or ratio')
            continue
        if row['pnl']['status'] != 'verified' or opening <= 0: raise ValueError('Return lacks positive initial capital/P&L proof')
        _match(ret['ratioNumerator'],int_pattern); _match(ret['ratioDenominator'],r'[1-9][0-9]*')
        flows = []; last_order = None
        if len(row['returnProof']) != row['externalFlowCount']: raise ValueError('Return flow proof count mismatch')
        bounds = data['chains'][row['chain']]
        for flow in row['returnProof']:
            _require_fields(flow, ('blockNumber','transactionIndex','logIndex','eventRef','operationRef','marketId','deltaWeiRawSigned','oraclePriceRaw','signedUsdExact','preNavUsdExact','postNavUsdExact'))
            order = tuple(_natural(flow[field]) for field in ('blockNumber','transactionIndex','logIndex'))
            if not bounds['opening']['number'] < order[0] <= bounds['closing']['number'] or (last_order is not None and order <= last_order): raise ValueError('Unordered/out-of-window flow proof')
            last_order = order
            _match(flow['eventRef'],hash_pattern+':'+uint_pattern); _match(flow['operationRef'],hash_pattern+r':0x[0-9a-f]+')
            if flow['eventRef'].split(':')[0] != flow['operationRef'].split(':')[0] or int(flow['eventRef'].split(':')[1]) != order[2] or int(flow['operationRef'].split(':')[1],16) >= order[2]: raise ValueError('Invalid operation/event identity')
            _match(flow['marketId'],uint_pattern); _match(flow['deltaWeiRawSigned'],int_pattern); _match(flow['oraclePriceRaw'],r'[1-9][0-9]*')
            capital = usd36(flow['signedUsdExact'])
            if capital != int(flow['deltaWeiRawSigned'])*int(flow['oraclePriceRaw']): raise ValueError('Cash flow does not match raw units/oracle')
            flows.append(dict(usd36=capital,preNavUsd36=usd36(flow['preNavUsdExact']),postNavUsd36=usd36(flow['postNavUsdExact']),exactNav=True))
        if sum(f['usd36'] for f in flows) != net or sum(max(f['usd36'],0) for f in flows) != inflow or -sum(min(f['usd36'],0) for f in flows) != outflow: raise ValueError('Return proof capital totals disagree')
        expected = performance(opening,closing,flows,coverage=True,replay=True)['return']
        if ret != expected: raise ValueError('Verified return disagrees with endpoint/linked-flow proof')
    for chain, count in counts.items():
        if data['chains'][chain]['coverage']['borrowPositions'] != count: raise ValueError('Chain borrower count mismatch')
        actual_failures = {}
        for row in data['positions']:
            if row['chain'] == chain:
                for reason in set(row['pnl']['reasonCodes']): actual_failures[reason] = actual_failures.get(reason,0)+1
        if data['chains'][chain]['failures'] != actual_failures: raise ValueError('Chain failure counts disagree')
    expected_summary = dict(positions=len(seen),verifiedPnl=sum(r['pnl']['status']=='verified' for r in data['positions']),verifiedReturn=sum(r['return']['status']=='verified' for r in data['positions']),unavailablePnl=sum(r['pnl']['status']=='unavailable' for r in data['positions']),closedPositions=sum(r['closed'] for r in data['positions']))
    _require_fields(data['summary'], tuple(expected_summary))
    if any(_natural(data['summary'][key]) != value for key,value in expected_summary.items()): raise ValueError('Summary counts disagree')
    return data


def validate_snapshot(data):
    """Strict public contract, including independently recomputable TWR proof."""
    try:
        return _validate_snapshot(data)
    except (KeyError,TypeError,AttributeError,ZeroDivisionError,OverflowError) as error:
        raise ValueError('Malformed snapshot contract') from error


def atomic_write(path, data):
    validate_snapshot(data)
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(data, separators=(',',':'), allow_nan=False) + '\n')
    os.replace(tmp,path)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validate',required=True)
    args=parser.parse_args()
    data=validate_snapshot(json.loads(Path(args.validate).read_text()))
    print('Validated',len(data['positions']),'borrow positions')
