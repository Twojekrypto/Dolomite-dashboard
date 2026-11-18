"""Publish complete, source-backed additional operating income atomically."""
import argparse
import calendar
import copy
import json
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from pathlib import Path

CHAINS = ('ethereum', 'arbitrum', 'berachain')
ETH_RETAINED_SHARE = Decimal('0.2')
ROOT = Path(__file__).resolve().parent


def periods(now):
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('UTC-aware time is required')
    end = now.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    previous_year = end.year - 1
    annual_start = end.replace(year=previous_year, day=min(end.day, calendar.monthrange(previous_year, end.month)[1]))
    return {'treasury': (int((end - timedelta(days=30)).timestamp()), int(end.timestamp())),
            'liquidations': (int(annual_start.timestamp()), int(end.timestamp()))}


def money(value, *, nonnegative=False):
    if not isinstance(value, str) or len(value) > 150 or not re.fullmatch(r'-?[0-9]+(?:\.[0-9]+)?', value):
        raise ValueError('Missing or invalid exact USD amount')
    result = Decimal(value)
    if not result.is_finite() or (nonnegative and result < 0):
        raise ValueError('Invalid USD amount')
    return result


def decimal_string(value):
    result = format(value, 'f')
    return result.rstrip('0').rstrip('.') if '.' in result else result


def _validate(report, window, amount_key, *, nonnegative=False):
    if not isinstance(report, dict) or report.get('complete') is not True:
        raise ValueError('Incomplete operating income source')
    if report.get('period') != {'startTimestamp': window[0], 'endTimestamp': window[1]}:
        raise ValueError('Operating income source period mismatch')
    if not isinstance(report.get('chains'), dict) or set(report['chains']) != set(CHAINS):
        raise ValueError('Operating income chain coverage mismatch')
    for chain in CHAINS:
        cell = report['chains'][chain]
        if not isinstance(cell, dict) or cell.get('complete') is not True:
            raise ValueError('Incomplete chain source: ' + chain)
        money(cell.get(amount_key), nonnegative=nonnegative)


def build_snapshot(treasury, liquidations, now):
    windows = periods(now)
    _validate(treasury, windows['treasury'], 'netInterestUSD')
    _validate(liquidations, windows['liquidations'], 'grossProtocolFeeUSD', nonnegative=True)
    treasury, liquidations = copy.deepcopy(treasury), copy.deepcopy(liquidations)
    with localcontext() as context:
        context.prec = 100
        treasury['netInterestUSD'] = decimal_string(sum((money(treasury['chains'][c]['netInterestUSD']) for c in CHAINS), Decimal(0)))
        gross, retained = Decimal(0), Decimal(0)
        for chain in CHAINS:
            cell = liquidations['chains'][chain]
            share = ETH_RETAINED_SHARE if chain == 'ethereum' else Decimal(1)
            amount = money(cell['grossProtocolFeeUSD'], nonnegative=True)
            cell.update(retainedShare=decimal_string(share), retainedProtocolFeeUSD=decimal_string(amount * share))
            gross += amount
            retained += amount * share
        liquidations.update(grossUSD=decimal_string(gross), retainedUSD=decimal_string(retained),
                            monthlyRetainedUSD=decimal_string(retained / 12))
    as_day = lambda stamp: datetime.fromtimestamp(stamp, timezone.utc).date().isoformat()
    liquidations['period'].update({'from': as_day(windows['liquidations'][0]),
                                  'to': as_day(windows['liquidations'][1]), 'months': 12})
    liquidations['excludedChains'] = ['retired_and_other_chains_including_mantle']
    return {'schemaVersion': 1, 'generatedAt': now.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'period': {'from': as_day(windows['treasury'][0]), 'to': as_day(windows['treasury'][1]), 'days': 30},
            'treasury': treasury, 'liquidations': liquidations, 'coverage': {'complete': True},
            'sources': {'treasury': 'official_safe_accounts_and_dolomite_rpc_principal_replay',
                        'liquidations': 'official_pinned_subgraphs_and_unique_fee_agent_event_joins',
                        'ethereumLiquidationRetainedShare': decimal_string(ETH_RETAINED_SHARE)},
            'limitations': ['Treasury interest uses end-period protocol oracle prices; principal flows, rewards and external yield excluded.',
                            'Liquidation fees use historical indexed oracle valuations, including the official cached-price fallback.',
                            'Liquidations are a trailing 12-calendar-month monthly average for Ethereum, Arbitrum and Berachain.',
                            'Published veDOLO rebates and borrow revenue are supplied separately by dolomite_revenue.json.']}


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, suffix='.tmp', delete=False) as handle:
            name = handle.name
            json.dump(value, handle, ensure_ascii=False, allow_nan=False, indent=2)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def refresh(config, output, *, now=None, treasury_collector=None, liquidation_collector=None):
    if treasury_collector is None:
        from operating_income_treasury import collect_treasury_income
        treasury_collector = collect_treasury_income
    if liquidation_collector is None:
        from operating_income_liquidations import collect_liquidation_income
        liquidation_collector = collect_liquidation_income
    now = now or datetime.now(timezone.utc)
    windows = periods(now)
    treasury = treasury_collector(config['chains'], *windows['treasury'], options=config.get('treasury', {}))
    liquidations = liquidation_collector(config['chains'], *windows['liquidations'], options=config.get('liquidations', {}))
    result = build_snapshot(treasury, liquidations, now)
    atomic_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=ROOT / 'config/operating_income.json')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/operating-income/latest.json')
    args = parser.parse_args()
    result = refresh(json.loads(args.config.read_text()), args.output)
    print(json.dumps({'output': str(args.output), 'period': result['period'],
                      'treasuryNetInterestUSD': result['treasury']['netInterestUSD'],
                      'liquidationMonthlyRetainedUSD': result['liquidations']['monthlyRetainedUSD']}))


if __name__ == '__main__':
    main()
