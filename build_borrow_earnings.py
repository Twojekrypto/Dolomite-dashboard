#!/usr/bin/env python3
"""Build reported base carry for existing Dolomite borrow positions.

This deliberately does not value option emissions or assume eligibility for
conditional rewards. Source risk totals are adjusted: only token principals
are used here. All financial arithmetic stays in Decimal until JSON strings.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import time
from urllib.request import Request, urlopen

CHAINS = {'ethereum': 1, 'arbitrum': 42161, 'berachain': 80094}
RISK_MAX_AGE_SECONDS = 180 * 60
RATE_MAX_AGE_SECONDS = 90 * 60
FUTURE_TOLERANCE_SECONDS = 300
ADDRESS = re.compile(r'^0x[a-f0-9]{40}$')


def timestamp(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, timezone.utc)
    if not isinstance(value, str):
        raise ValueError('Missing source timestamp')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Source timestamp has no timezone')
    return result.astimezone(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def check_age(value, now, limit):
    result = timestamp(value)
    age = (now - result).total_seconds()
    if age < -FUTURE_TOLERANCE_SECONDS or age > limit:
        raise ValueError('Source timestamp is stale or in the future')
    return result


def number(value):
    if value is None or isinstance(value, bool) or len(str(value)) > 200:
        raise ValueError('Missing or invalid decimal')
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError('Invalid decimal') from exc
    if not result.is_finite() or abs(result.adjusted()) > 100:
        raise ValueError('Nonfinite or out-of-range decimal')
    return result


def decimal_text(value):
    if value == 0:
        return '0'
    return format(value, 'f').rstrip('0').rstrip('.') if value.as_tuple().exponent < 0 else format(value, 'f')


def sha256(payload):
    encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def components(rate, side):
    """Return base + signed native carry; monetary incentives are not assumed."""
    key = 'supply' if side == 'collateral' else 'borrow'
    base = number(rate.get(key + 'InterestRate'))
    if base < 0:
        raise ValueError('Negative protocol interest rate')
    parts = rate.get('outside' + key.title() + 'InterestRateParts')
    if not isinstance(parts, list):
        raise ValueError('Missing component breakdown')
    native = Decimal(0)
    conditional = False
    for part in parts:
        if not isinstance(part, dict):
            raise ValueError('Invalid rate component')
        # API labels oDOLO with rewardToken.symbol=DOLO. Identity by label wins.
        label = str(part.get('label') or '')
        category = part.get('category')
        if re.search(r'\boDOLO\b', label, re.I) or category == 'points':
            continue
        if category == 'rewards':
            conditional = True
            continue
        if category != 'nativeYield':
            raise ValueError('Unsupported rate component')
        native += number(part.get('interestRate'))
    return base + native, conditional


def rate_index(payload):
    rows = payload.get('interestRates') if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ValueError('Missing official rate markets')
    result = {}
    seen_addresses = set()
    for row in rows:
        token = row.get('token') if isinstance(row, dict) else None
        if not isinstance(token, dict):
            raise ValueError('Missing rate token')
        address = str(token.get('tokenAddress') or '').lower()
        market = str(token.get('marketId', ''))
        if not ADDRESS.fullmatch(address) or not market.isdigit():
            raise ValueError('Invalid rate market identity')
        if address in seen_addresses:
            raise ValueError('Duplicate rate market identity')
        seen_addresses.add(address)
        result[(address, market)] = row
    return result


def token_side(items, rates, side):
    if not isinstance(items, list) or not items:
        raise ValueError('Missing position assets')
    principal = Decimal(0)
    annual = Decimal(0)
    conditional = False
    tokens = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError('Invalid position asset')
        usd = number(item.get('usdExact', item.get('usd')))
        if usd < 0:
            raise ValueError('Negative position principal')
        if usd == 0:
            continue
        address = str(item.get('tokenAddress') or '').lower()
        market = str(item.get('marketId', ''))
        identity = (address, market)
        if not ADDRESS.fullmatch(address) or not market.isdigit() or identity not in rates:
            raise ValueError('Missing exact market rate')
        if identity in seen:
            raise ValueError('Duplicate position asset')
        seen.add(identity)
        rate, has_conditional = components(rates[identity], side)
        principal += usd
        annual += usd * rate
        conditional = conditional or has_conditional
        tokens.append({'symbol': str(item.get('symbol') or '?'), 'tokenAddress': address,
                       'marketId': market, 'usd': decimal_text(usd)})
    if principal <= 0:
        raise ValueError('No positive position principal')
    return principal, annual, conditional, tokens


def build_output(risk, rate_payloads, now):
    """Model each complete account from one risk generation and fresh rates."""
    if not isinstance(risk, dict) or not isinstance(risk.get('positions'), list):
        raise ValueError('Invalid risk snapshot')
    now = timestamp(now.isoformat()) if isinstance(now, datetime) else timestamp(now)
    risk_time = check_age(risk.get('generatedAtISO', risk.get('generatedAt')), now, RISK_MAX_AGE_SECONDS)
    if risk.get('generatedAt') is not None:
        if abs((timestamp(risk['generatedAt']) - risk_time).total_seconds()) > 1:
            raise ValueError('Risk generation timestamps disagree')
    chain_stats = risk.get('chainStats') or {}
    stale = set(risk.get('staleChains') or [])
    statuses, indexes, rate_sources = {}, {}, {}
    for chain in CHAINS:
        try:
            if chain not in chain_stats or chain in stale or chain_stats[chain].get('stale'):
                raise ValueError('Position source unavailable or stale')
            item = rate_payloads.get(chain)
            if not isinstance(item, dict):
                raise ValueError('Official rates unavailable')
            fetched = check_age(item.get('fetchedAt'), now, RATE_MAX_AGE_SECONDS)
            indexes[chain] = rate_index(item.get('payload'))
            rate_sources[chain] = {'fetchedAt': iso(fetched), 'url': str(item.get('url') or ''),
                                   'sha256': sha256(item['payload']), 'payload': deepcopy(item['payload'])}
            statuses[chain] = {'status': 'ok'}
        except (ValueError, TypeError, AttributeError) as exc:
            statuses[chain] = {'status': 'unavailable', 'reason': str(exc)}
    if not indexes:
        raise ValueError('No fresh active chain can be modeled')
    result, excluded, seen = [], Counter(), set()
    source_count = 0
    with localcontext() as context:
        context.prec = 100
        for position in risk['positions']:
            chain = position.get('chain') if isinstance(position, dict) else None
            if chain not in CHAINS:
                continue
            source_count += 1
            address = str(position.get('address') or '').lower()
            account_number = str(position.get('accountNumber', ''))
            account_id = str(position.get('accountId') or '')
            identity = chain + ':' + account_id
            if not account_id or not ADDRESS.fullmatch(address) or not account_number.isdigit():
                excluded['Invalid account identity'] += 1
                continue
            if identity in seen:
                raise ValueError('Duplicate position identity')
            seen.add(identity)
            if chain not in indexes:
                excluded['Chain unavailable'] += 1
                continue
            if position.get('earningsComplete') is not True:
                excluded['Incomplete source principal'] += 1
                continue
            try:
                collateral, income, collateral_conditional, collateral_tokens = token_side(position.get('collateralTokens'), indexes[chain], 'collateral')
                debt, cost, debt_conditional, debt_tokens = token_side(position.get('debtTokens'), indexes[chain], 'debt')
                net = income - cost
                apr = net / collateral * 100
                hf = position.get('healthFactor')
                if hf is not None and (isinstance(hf, bool) or not isinstance(hf, (int, float)) or not math.isfinite(hf) or hf < 0):
                    raise ValueError('Invalid health factor')
                result.append({'id': identity, 'chain': chain, 'address': address,
                               'accountId': account_id, 'accountNumber': account_number,
                               'healthFactor': hf, 'eMode': bool(position.get('eMode')),
                               'collateralUSD': decimal_text(collateral), 'debtUSD': decimal_text(debt),
                               'collateralTokens': collateral_tokens, 'debtTokens': debt_tokens,
                               'annualIncomeUSD': decimal_text(income), 'annualCostUSD': decimal_text(cost),
                               'annualNetUSD': decimal_text(net), 'netAprPct': decimal_text(apr),
                               'hasConditionalRewards': collateral_conditional or debt_conditional})
            except (ValueError, TypeError, AttributeError, InvalidOperation) as exc:
                excluded[str(exc)] += 1
        result.sort(key=lambda row: (-Decimal(row['annualNetUSD']), row['id']))
    return {'schemaVersion': 1, 'generatedAt': iso(now),
            'sources': {'positionsGeneratedAt': iso(risk_time), 'positionsPayloadSha256': sha256(risk),
                        'rates': rate_sources},
            'methodology': {'metric': 'reported-base-carry', 'aprDenominator': 'raw-collateral-usd',
                            'excluded': ['oDOLO options', 'points', 'conditional cash rewards',
                                         'veDOLO rebates', 'gas', 'unmodeled price changes'],
                            'riskMaxAgeSeconds': RISK_MAX_AGE_SECONDS,
                            'rateMaxAgeSeconds': RATE_MAX_AGE_SECONDS},
            'chainStatus': statuses,
            'coverage': {'sourcePositions': source_count, 'evaluatedPositions': source_count - excluded.get('Chain unavailable', 0),
                         'rankedPositions': len(result), 'excludedPositions': sum(excluded.values()),
                         'excludedByReason': dict(sorted(excluded.items()))},
            'positions': result}


def fetch_rates(chain):
    url = f'https://api.dolomite.io/tokens/{CHAINS[chain]}/interest-rates?exclude-odolo=false'
    for attempt in range(3):
        try:
            request = Request(url, headers={'User-Agent': 'Dolomite-dashboard/borrow-earnings', 'Accept': 'application/json'})
            with urlopen(request, timeout=25) as response:
                payload = json.load(response)
            return chain, {'fetchedAt': iso(datetime.now(timezone.utc)), 'payload': payload, 'url': url}
        except (OSError, ValueError) as exc:
            if attempt == 2:
                print(f'Official rate fetch unavailable for {chain}: {type(exc).__name__}', file=sys.stderr)
                return chain, None
            time.sleep(attempt + 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--risk', type=Path, default=Path('liquidation_risk.json'))
    parser.add_argument('--output', type=Path, default=Path('borrow_earnings.json'))
    args = parser.parse_args()
    risk = json.loads(args.risk.read_text())
    with ThreadPoolExecutor(max_workers=3) as pool:
        rates = dict(pool.map(fetch_rates, CHAINS))
    output = build_output(risk, rates, datetime.now(timezone.utc))
    # Replace only a completely serialized/validated generation. A failure keeps
    # the previous file and stops the workflow before its publication step.
    serialized = json.dumps(output, separators=(',', ':'), allow_nan=False) + '\n'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + '.tmp')
    temporary.write_text(serialized)
    temporary.replace(args.output)
    print(json.dumps({'generatedAt': output['generatedAt'], 'coverage': output['coverage'],
                      'chains': output['chainStatus'], 'bytes': len(serialized.encode())}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
