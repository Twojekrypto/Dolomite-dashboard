"""Official veDOLO rebate allocation history, including signed revisions.

Aggregate entitlements are cumulative and do not subtract claims. Changes between
the latest official snapshot for each epoch are net allocations, not cash paid.
USD values use one finalized price set; raw token amounts reconcile exactly.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from urllib.error import HTTPError
from urllib.request import Request, urlopen

REPO = 'dolomite-exchange/liquidity-mining-data'
RAW = 'https://raw.githubusercontent.com/' + REPO
METADATA_URL = 'https://api.dolomite.io/liquidity-mining/ve-dolo-rebate/metadata'
OUTPUT = Path(__file__).resolve().parent / 'veborrow_rebate_history.json'
CHAINS = {'80094': 'Berachain', '42161': 'Arbitrum'}
WEEK = 7 * 86400

class SnapshotMismatch(ValueError):
    """An official aggregate cannot be reconciled with its user allocations."""

def uint(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d+', value):
        raise ValueError('Invalid raw integer')
    return int(value)

def read_json(url, *, optional=False):
    headers = {'User-Agent': 'Dolomite-dashboard-rebate-history', 'Accept': 'application/json'}
    if url.startswith('https://api.github.com/') and os.environ.get('GH_TOKEN'):
        headers['Authorization'] = 'Bearer ' + os.environ['GH_TOKEN']
    for attempt in range(3):
        try:
            with urlopen(Request(url, headers=headers), timeout=60) as response:
                raw = response.read()
            return json.loads(raw), hashlib.sha256(raw).hexdigest()
        except HTTPError as error:
            if optional and error.code == 404:
                return None, None  # The official history includes file deletions.
            if error.code not in (429, 500, 502, 503, 504) or attempt == 2:
                raise
        except OSError:
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)

def summarize_snapshot(data, commit, committed_at, digest):
    meta = data['metadata']
    totals = {str(k): str(uint(v)) for k, v in meta['marketToTotalRebate'].items()}
    roots = meta['marketToMerkleRoot']
    if set(roots) != set(totals) or any(not re.fullmatch(r'0x[0-9a-fA-F]{64}', v) for v in roots.values()):
        raise ValueError('Invalid market roots')
    sums, counts = {}, {}
    for markets in data['users'].values():
        for market, record in markets.items():
            sums[market] = sums.get(market, 0) + uint(record['amount'])
            counts[market] = counts.get(market, 0) + 1
    if set(sums) - set(totals):
        raise ValueError('User market missing from aggregate')
    # The official calculator rounds user and market totals independently.
    for market, amount in totals.items():
        if abs(sums.get(market, 0) - int(amount)) > counts.get(market, 0) + 1:
            raise SnapshotMismatch('Aggregate exceeds independent rounding bound')
    return {'epoch': int(meta['epoch']), 'totals': totals, 'roots': roots,
            'totalUsers': int(meta['totalUsers']), 'commit': commit,
            'committedAt': committed_at, 'sha256': digest}

def value_usd(amounts, prices):
    with localcontext() as context:
        context.prec = 100
        total = Decimal(0)
        for market, raw in amounts.items():
            if int(raw) and (market not in prices or uint(prices[market]) <= 0):
                raise ValueError('Missing reference price for market ' + market)
            total += Decimal(raw) * Decimal(prices.get(market, '0')) / Decimal(10 ** 36)
        return float(total)

def build_chain(snapshots, start_epoch, published_epoch, start_ts, prices):
    versions = {}
    for snapshot in sorted(snapshots, key=lambda row: (row['committedAt'], row['commit'])):
        epoch = snapshot.get('epoch', 0)
        if start_epoch <= epoch <= published_epoch and (snapshot.get('totals') or snapshot.get('invalid')):
            versions.setdefault(epoch, []).append(snapshot)
    if published_epoch not in versions or versions[published_epoch][-1].get('invalid'):
        raise ValueError('Latest published epoch snapshot missing')
    previous, previous_epoch, rows, missing = {}, start_epoch - 1, [], []
    accumulated = {}
    for epoch in range(start_epoch, published_epoch + 1):
        row = {'epoch': epoch, 'ts': start_ts + (epoch - 1) * WEEK,
               'endTimestamp': start_ts + epoch * WEEK}
        if epoch not in versions or versions[epoch][-1].get('invalid'):
            missing.append(epoch)
            reason = 'Official user allocations and aggregate disagree' if epoch in versions else 'Official epoch snapshot unavailable'
            rows.append({**row, 'status': 'missing', 'reason': reason, 'deltaUSD': None, 'cumulativeUSD': None})
            continue
        snapshot = versions[epoch][-1]
        totals = snapshot['totals']
        markets = set(previous) | set(totals)
        delta = {market: str(int(totals.get(market, 0)) - int(previous.get(market, 0))) for market in sorted(markets, key=int)}
        for market, raw in delta.items():
            accumulated[market] = accumulated.get(market, 0) + int(raw)
        status = 'catchup' if previous_epoch != epoch - 1 else 'correction' if any(int(raw) < 0 for raw in delta.values()) else 'allocated'
        rows.append({**row, 'status': status, 'deltaRaw': delta,
                     'deltaUSD': value_usd(delta, prices), 'cumulativeUSD': value_usd(totals, prices),
                     'revisionCount': len(versions[epoch]), 'coversEpochs': list(range(previous_epoch + 1, epoch + 1)),
                     'commit': snapshot['commit'], 'committedAt': snapshot['committedAt']})
        previous, previous_epoch = totals, epoch
    if any(accumulated.get(market, 0) != int(previous.get(market, 0)) for market in set(accumulated) | set(previous)):
        raise ValueError('Raw allocation reconciliation failed')
    return {'startEpoch': start_epoch, 'publishedEpoch': published_epoch,
            'epochs': rows, 'missingEpochs': missing, 'reconciled': True,
            'totalsRaw': previous, 'totalUSD': value_usd(previous, prices),
            'marketToMerkleRoot': versions[published_epoch][-1]['roots'],
            'sourceCommit': versions[published_epoch][-1]['commit'], 'referencePrices': prices,
            'snapshots': snapshots}

def fetch_snapshots(chain, cache):
    path = f'finalized/{chain}/rebate/rebate-aggregated-output.json'
    commits, page = [], 1
    while True:
        batch, _ = read_json(f'https://api.github.com/repos/{REPO}/commits?path={path}&per_page=100&page={page}')
        commits.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    by_commit = {row['commit']: row for row in cache}
    snapshots = []
    for commit in commits:
        sha = commit['sha']
        if sha in by_commit:
            snapshots.append(by_commit[sha])
            continue
        data, digest = read_json(f'{RAW}/{sha}/{path}', optional=True)
        stamp = commit['commit']['committer']['date']
        if data is None:
            snapshots.append({'commit': sha, 'committedAt': stamp, 'deleted': True})
        else:
            try:
                snapshots.append(summarize_snapshot(data, sha, stamp, digest))
            except SnapshotMismatch as error:
                snapshots.append({'commit': sha, 'committedAt': stamp, 'sha256': digest,
                                  'epoch': int(data['metadata']['epoch']), 'invalid': str(error)})
    return snapshots

def generate(previous=None):
    metadata_response, meta_hash = read_json(METADATA_URL)
    metadata = metadata_response['metadata']
    chains = {}
    for chain, name in CHAINS.items():
        config = metadata['allChainRebateInfo'][chain]
        # Both official publication indices must agree; a mid-publication run is retried later.
        published = metadata['onchainRollingClaimsEpochIndexMap'][chain]
        if published != metadata['onchainFeeRebateEpochIndexMap'][chain]:
            raise ValueError('Publication in progress for ' + name)
        if not config['claimsEnabled'] or published is None:
            raise ValueError('Rebate publication unavailable for ' + name)
        cache = (previous or {}).get('chains', {}).get(name, {}).get('snapshots', [])
        snapshots = fetch_snapshots(chain, cache)
        price_url = f'{RAW}/master/finalized/{chain}/borrow-interest/epoch-{published}-output.json'
        price_data, price_hash = read_json(price_url)
        price_meta = price_data['metadata']
        prices = price_meta['marketPrices']
        for price in prices.values():
            uint(price)
        result = build_chain(snapshots, int(config['startEpoch']), int(published), int(metadata['veDoloStartTimestamp']), prices)
        latest = next(row for row in snapshots if row.get('totals') or row.get('invalid'))
        if latest.get('invalid') or latest['epoch'] != published or latest['totals'] != result['totalsRaw']:
            raise ValueError('Latest official aggregate and published epoch differ for ' + name)
        result.update({'chainId': int(chain), 'referencePriceTimestamp': price_meta['claimEndTimestamp'],
                       'referencePriceSource': price_url, 'referencePriceSha256': price_hash,
                       'interestWindows': {str(published): {'start': price_meta['claimStartTimestamp'], 'end': price_meta['claimEndTimestamp']}}})
        chains[name] = result
    # Ensure publication did not change while collecting the archive.
    final_meta, _ = read_json(METADATA_URL)
    if final_meta['metadata'] != metadata:
        raise ValueError('Rebate metadata changed during refresh; retry')
    return {'schemaVersion': 1, 'generatedAt': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'definition': 'Net historical allocation changes between latest official epoch snapshots, including revisions; not paid or unclaimed balances.',
            'valuation': 'All token changes use the latest finalized epoch reference prices for each chain.',
            'currentEpoch': int(metadata['currentEpochIndex']), 'currentEpochStartTimestamp': int(metadata['currentEpochStartTimestamp']),
            'metadataSource': METADATA_URL, 'metadataSha256': meta_hash, 'chains': chains}

def main():
    previous = json.loads(OUTPUT.read_text()) if OUTPUT.exists() else None
    data = generate(previous)
    temporary = OUTPUT.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.replace(OUTPUT)
    print(json.dumps({'generatedAt': data['generatedAt'], 'chains': {name: {'throughEpoch': row['publishedEpoch'], 'missingEpochs': row['missingEpochs'], 'allocatedUSD': row['totalUSD']} for name, row in data['chains'].items()}}))

if __name__ == '__main__':
    main()
