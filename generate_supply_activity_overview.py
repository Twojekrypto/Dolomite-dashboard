#!/usr/bin/env python3
"""Build a 30-day event feed from the existing static Supply Activity evidence.

Schema 2 Supply Activity rows are wallet/transaction groups, so the feed uses
their retained source legs, not net wallet amounts or the outer action label.
These are technical deposit/withdraw events, not inferred changes in TVL.
Amounts and historical USD quotes retain their source decimal strings. The
upstream leg writer uses zero as its missing-USD default; a zero quote on a
nonzero amount is therefore unavailable here, with the original quote retained.
No RPC requests, prices, or financial calculations are performed.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import sys


CHAINS = ('ethereum', 'arbitrum', 'berachain')
WINDOW_SECONDS = 30 * 86400
MAX_SOURCE_AGE_SECONDS = 86400
ADDRESS = re.compile(r'0x[0-9a-fA-F]{40}')
TX_HASH = re.compile(r'0x[0-9a-fA-F]{64}')


def parse_time(value):
    if not isinstance(value, str):
        raise ValueError('Missing source generation timestamp')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Source generation timestamp has no timezone')
    return parsed.astimezone(timezone.utc)


def decimal_string(value, field):
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f'{field} must be an exact decimal string')
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f'Invalid {field}') from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f'Invalid {field}')
    return parsed


def event_row(event, token_index):
    kind = event.get('type')
    event_id = event.get('sourceId')
    if not isinstance(event_id, str) or not event_id.startswith(kind + '-'):
        raise ValueError('Invalid source event identity')
    timestamp = event.get('timestamp')
    if not isinstance(timestamp, int) or isinstance(timestamp, bool) or timestamp <= 0:
        raise ValueError(f'{event_id}: invalid timestamp')
    if not TX_HASH.fullmatch(str(event.get('txHash') or '')):
        raise ValueError(f'{event_id}: invalid transaction hash')
    if not ADDRESS.fullmatch(str(event.get('wallet') or '')):
        raise ValueError(f'{event_id}: missing source wallet')
    secondary = event.get('secondaryAddress') or ''
    if secondary and not ADDRESS.fullmatch(secondary):
        raise ValueError(f'{event_id}: invalid secondary wallet')
    if event.get('ambiguous') is not False:
        raise ValueError(f'{event_id}: ambiguous or missing source leg evidence')
    amount = decimal_string(event.get('amount'), 'amount')
    usd = event.get('usd')
    unavailable = usd is None or usd == '' or event.get('usdStatus') == 'unavailable'
    if usd is not None and usd != '':
        quote = decimal_string(usd, 'USD quote')
        unavailable = unavailable or (quote == 0 and amount != 0)
    row = [event_id, kind, timestamp, event['txHash'], event['amount'],
           None if unavailable else usd, event['wallet'], secondary, token_index]
    if unavailable:
        metadata = {'usdStatus': 'unavailable'}
        if usd is not None and usd != '':
            metadata['sourceUsd'] = usd
        row.append(metadata)
    return row


def source_events(payload):
    """Retain source-event grain even when one group has many action types."""
    rows = payload.get('rows')
    if not isinstance(rows, list):
        raise ValueError('Missing activity rows')
    for row in rows:
        if not isinstance(row, list) or len(row) < 8:
            raise ValueError('Malformed activity row')
        if payload['schemaVersion'] == 1:
            if row[1] in ('deposit', 'withdraw'):
                yield {'sourceId': row[0], 'type': row[1], 'timestamp': row[2],
                       'txHash': row[3], 'amount': row[4], 'usd': row[5],
                       'wallet': row[6], 'secondaryAddress': row[7], 'ambiguous': False}
            continue
        metadata = row[8] if len(row) > 8 else None
        if not isinstance(metadata, dict) or metadata.get('version') != 2:
            raise ValueError('Missing schema 2 event evidence')
        legs = metadata.get('legs')
        if not isinstance(legs, list) or not legs or not all(isinstance(x, dict) for x in legs):
            raise ValueError('Missing schema 2 source legs')
        ids = metadata.get('sourceIds')
        if not isinstance(ids, list) or set(ids) != {x.get('sourceId') for x in legs}:
            raise ValueError('Incomplete schema 2 source event evidence')
        for event in legs:
            kind = event.get('type')
            identity = event.get('sourceId')
            if (kind not in ('deposit', 'withdraw', 'transfer', 'trade', 'liquidation', 'vaporization')
                    or not isinstance(identity, str) or not identity.startswith(kind + '-')):
                raise ValueError('Invalid schema 2 source event type or identity')
            if event.get('type') not in ('deposit', 'withdraw'):
                continue
            if (event.get('tokenId') != payload['tokenId'] or event.get('txHash') != row[3]
                    or event.get('wallet') != row[6] or event.get('timestamp') != row[2]):
                raise ValueError('Source leg differs from its token/transaction/wallet group')
            yield event


def read_chain(input_dir, entry, generated, period_start, period_end):
    chain = entry['chain']
    assets = entry.get('tokens')
    if not isinstance(assets, list):
        raise ValueError('No manifested assets')
    expected = entry.get('tokensAvailable')
    if (not isinstance(expected, int) or isinstance(expected, bool)
            or expected != len(assets) or entry.get('tokensWritten') != len(assets)
            or entry.get('tokensSkipped') != 0 or entry.get('skippedTokens')):
        raise ValueError('Incomplete asset coverage or skipped source assets')
    seen_assets = set()
    tokens = []
    events = {}
    for token in sorted(assets, key=lambda x: str(x.get('tokenId') or '')):
        token_id = token.get('tokenId')
        if not isinstance(token_id, str) or not ADDRESS.fullmatch(token_id) or token_id in seen_assets:
            raise ValueError('Invalid or duplicate manifested token')
        seen_assets.add(token_id)
        expected_path = f'data/supply-activity/recent/{chain}/{token_id}.json'
        if token.get('recentPath') != expected_path:
            raise ValueError(f'{token_id}: missing or unexpected recent cache path')
        if parse_time(token.get('generatedAt')) != generated:
            raise ValueError(f'{token_id}: mixed token generation')
        payload = json.loads((input_dir / 'recent' / chain / f'{token_id}.json').read_text())
        if (payload.get('schemaVersion') not in (1, 2) or payload.get('__supplyActivityCompact') != 1
                or payload.get('scope') != '30d' or payload.get('source') != 'static-subgraph-activity'
                or payload.get('chain') != chain or payload.get('tokenId') != token_id):
            raise ValueError(f'{token_id}: invalid recent activity source')
        if parse_time(payload.get('generatedAt')) != generated:
            raise ValueError(f'{token_id}: mixed payload generation')
        if not isinstance(payload.get('rows'), list) or token.get('recentEvents') != len(payload['rows']):
            raise ValueError(f'{token_id}: source row count differs from manifest')
        token_index = len(tokens)
        tokens.append({'chain': chain, 'tokenId': token_id,
                       'symbol': payload.get('symbol') or token.get('symbol') or token_id,
                       'name': payload.get('name'), 'marketId': payload.get('marketId')})
        for event in source_events(payload):
            row = event_row(event, token_index)
            if not period_start <= row[2] <= period_end:
                continue
            key = (row[1], row[0])
            if key in events and events[key] != row:
                raise ValueError(f'{row[0]}: conflicting source event evidence')
            events[key] = row
    return tokens, list(events.values())


def build_overview(input_dir, *, now=None):
    input_dir = Path(input_dir)
    manifest = json.loads((input_dir / 'manifest.json').read_text())
    generated_at = manifest.get('generatedAt')
    generated = parse_time(generated_at)
    now = now or datetime.now(timezone.utc)
    age = (now - generated).total_seconds()
    source_issue = ('Source manifest is stale' if age > MAX_SOURCE_AGE_SECONDS
                    else 'Source manifest is future dated' if age < -300 else None)
    end = int(generated.timestamp())
    start = end - WINDOW_SECONDS
    manifest_chains = manifest.get('chains')
    if not isinstance(manifest_chains, list):
        raise ValueError('Missing source chains')
    result = {'schemaVersion': 1, 'scope': '30d', 'source': 'static-subgraph-activity',
              'sourceManifest': 'data/supply-activity/manifest.json',
              'eventGrain': 'chain-source-event', 'amountBasis': 'source-event-amount',
              'usdBasis': 'source-event-historical-quote',
              'generatedAt': generated_at, 'sourceGeneratedAt': generated_at,
              'periodStart': start, 'periodEnd': end,
              'maxSourceAgeSeconds': MAX_SOURCE_AGE_SECONDS,
              'chains': [], 'tokens': [], 'rows': []}
    for chain in CHAINS:
        matches = [c for c in manifest_chains if c.get('chain') == chain]
        entry = matches[0] if len(matches) == 1 else None
        expected = entry.get('tokensAvailable') if entry else None
        if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
            expected = None
        coverage = {'chain': chain, 'status': 'unavailable', 'expectedAssets': expected,
                    'includedAssets': 0, 'sourceGeneratedAt': None, 'rowCount': None,
                    'skippedTokens': entry.get('skippedTokens', []) if entry else [], 'issues': []}
        result['chains'].append(coverage)
        try:
            if source_issue:
                raise ValueError(source_issue)
            if entry is None:
                raise ValueError('Missing or duplicate source chain')
            tokens, rows = read_chain(input_dir, entry, generated, start, end)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            coverage['issues'].append(str(exc))
            continue
        offset = len(result['tokens'])
        for row in rows:
            row[8] += offset
        result['tokens'].extend(tokens)
        result['rows'].extend(rows)
        coverage.update(status='complete', includedAssets=len(tokens),
                        sourceGeneratedAt=generated_at, rowCount=len(rows))
    result['rows'].sort(key=lambda r: (-r[2], CHAINS.index(result['tokens'][r[8]]['chain']), r[0], r[1]))
    unavailable = [c['chain'] for c in result['chains'] if c['status'] != 'complete']
    result['status'] = 'complete' if not unavailable else 'unavailable' if len(unavailable) == len(CHAINS) else 'partial'
    result['coverage'] = {'status': result['status'],
                          'expectedAssets': sum(c['expectedAssets'] for c in result['chains'])
                          if all(c['expectedAssets'] is not None for c in result['chains']) else None,
                          'includedAssets': len(result['tokens']), 'unavailableChains': unavailable}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, default=Path('data/supply-activity'))
    parser.add_argument('--output', type=Path, default=Path('data/supply-activity/overview.json'))
    parser.add_argument('--now', help='Explicit UTC clock for reproducible verification')
    args = parser.parse_args()
    try:
        result = build_overview(args.input_dir, now=parse_time(args.now) if args.now else None)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print(f'Activity overview unavailable: {exc}', file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + '.tmp')
    temporary.write_text(json.dumps(result, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')
    temporary.replace(args.output)
    print(f"Activity overview: {result['status']}; {len(result['rows'])} events; {len(result['tokens'])} assets")
    return 0 if result['status'] == 'complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
