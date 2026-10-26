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
from eth_hash.auto import keccak
from urllib.error import HTTPError
from urllib.request import Request, urlopen

REPO = 'dolomite-exchange/liquidity-mining-data'
RAW = 'https://raw.githubusercontent.com/' + REPO
METADATA_URL = 'https://api.dolomite.io/liquidity-mining/ve-dolo-rebate/metadata'
OUTPUT = Path(__file__).resolve().parent / 'veborrow_rebate_history.json'
CHAINS = {'80094': 'Berachain', '42161': 'Arbitrum'}
WEEK = 7 * 86400
VALIDATION_VERSION = 2
# The pre-July27 calculator added cumulative user allocations to the previous
# market totals again. Only these immutable BERA files have been audited against
# all 70 MarketIdToMerkleRootSet roots published by RollingClaims on chain.
# SHA256 pins the complete file; leaf and tree validation is still mandatory.
VERIFIED_RECOVERIES = {
    "02054e55015c2de3a2a585884bbb124cf1666bfd": {
        "epoch": 2,
        "sha256": "1a065cbe4a5c1923e8a3429a6871fe7589338f32e42f894653a9b9603d9812db",
        "block": 22370291,
        "transaction": "0x7917bf3dc28eebf19c93f1dbb4dc6e2c16182dae291238c66940215f0a35e66b"
    },
    "b46e764cf1c56e01db21f18e044f6f3e8236dee3": {
        "epoch": 3,
        "sha256": "a2c26177135e9b875141863656c3bd4fd3dc64a8282a6c4dc3a551560cbefa6f",
        "block": 22370490,
        "transaction": "0x05d0b86c65f4ff461caa00cf5f59d93e2bdbe408e85c9aff3fa0aa7fe39cad0d"
    },
    "f16516ad1fc17084c774bc37c2ee60fce8266588": {
        "epoch": 4,
        "sha256": "3ff518ce16e0572b76c0d65c9628ba9620379fb31671091211f9247cbdf14317",
        "block": 22370636,
        "transaction": "0xd973c92b720b9dcbbcdffdae53aa6b1bd02b99de1416b0835a31a15c4867b121"
    },
    "a4d517fbf98a3051794fab86547cb28e77b4c372": {
        "epoch": 5,
        "sha256": "2cce4cce60c91c474d5979b7e2632244e8566adec1fcf5a942dd34df9776efbb",
        "block": 23263814,
        "transaction": "0xe3890949a6af10d46a5be11248f61acc4b7af3e3aecfa803b98273957a190667"
    },
    "3ff90638b58a259d15c96c2b1fdc516346067f50": {
        "epoch": 6,
        "sha256": "74647e839eb72b153a5908ce599a45ea9dffbf7eabed08aaec238f5f199e1fce",
        "block": 23263931,
        "transaction": "0x821139559bf956524b819c57abdc7fca8734076d0066dc245725330145ec5111"
    },
    "906f42252e7d5803d00dce467bd500eb28877090": {
        "epoch": 7,
        "sha256": "efe97a2177b9c13f275c60c85a082cd072a434ae863eecb773bfe67931c7fb51",
        "block": 23270368,
        "transaction": "0x06739ad211a20519d9d50c4895603ecdca9ad34e30101862f6d8783b613a073a"
    },
    "8cd53fdcc62d7c95a27f01f67bafe449f1cee949": {
        "epoch": 8,
        "sha256": "4712b3c8c6692b2e305b6160575ef682f1fa88faf1cdb4b4c6bb59639c12023c",
        "block": 23572770,
        "transaction": "0x40308acbd20cc1bdb8284aaf619be4e609c1881dbdaa42577475729c58f34251"
    }
}

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

def merkle_root(leaves):
    layer = sorted(leaves)
    if not layer:
        raise ValueError('Market has no allocation leaves')
    while len(layer) > 1:
        layer = [keccak(b''.join(sorted(layer[i:i+2]))) if i+1 < len(layer) else layer[i]
                 for i in range(0, len(layer), 2)]
    return '0x' + layer[0].hex()

def summarize_snapshot(data, commit, committed_at, digest, *, chain=None):
    meta = data['metadata']
    totals = {str(k): str(uint(v)) for k, v in meta['marketToTotalRebate'].items()}
    roots = meta['marketToMerkleRoot']
    if set(roots) != set(totals) or any(not re.fullmatch(r'0x[0-9a-fA-F]{64}', v) for v in roots.values()):
        raise ValueError('Invalid market roots')
    sums, counts, leaves, addresses = {}, {}, {}, set()
    for address, markets in data['users'].items():
        if not re.fullmatch(r'0x[0-9a-fA-F]{40}', address) or address.lower() in addresses:
            raise ValueError('Invalid or duplicate wallet address')
        addresses.add(address.lower())
        for market, record in markets.items():
            amount = uint(record['amount'])
            if amount >= 2**256:
                raise ValueError('Raw allocation exceeds uint256')
            leaf = keccak(bytes.fromhex(address[2:]).rjust(32, b'\0') + amount.to_bytes(32, 'big'))
            if record.get('leaf', '').lower() != '0x' + leaf.hex():
                raise ValueError('User allocation leaf mismatch')
            leaves.setdefault(market, []).append(leaf)
            sums[market] = sums.get(market, 0) + amount
            counts[market] = counts.get(market, 0) + 1
    if set(sums) != set(totals):
        raise ValueError('User and aggregate market sets differ')
    if any(merkle_root(leaves[market]) != root.lower() for market, root in roots.items()):
        raise ValueError('User allocation Merkle root mismatch')
    result = {'epoch': int(meta['epoch']), 'totals': totals, 'roots': roots,
              'totalUsers': int(meta['totalUsers']), 'commit': commit,
              'committedAt': committed_at, 'sha256': digest,
              'validationVersion': VALIDATION_VERSION, 'merkleVerified': True}
    # Ordinary snapshots retain official independently rounded market totals.
    mismatch = any(abs(sums[market] - int(amount)) > counts[market] + 1 for market, amount in totals.items())
    if mismatch:
        proof = VERIFIED_RECOVERIES.get(commit)
        if chain != '80094' or not proof or proof['epoch'] != result['epoch'] or proof['sha256'] != digest:
            raise SnapshotMismatch('Aggregate exceeds independent rounding bound')
        result.update({'totals': {market: str(sums[market]) for market in totals},
                       'reportedTotals': totals, 'totalsSource': 'merkle_verified_user_allocations',
                       'recovery': proof})
    return result

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
                     'commit': snapshot['commit'], 'committedAt': snapshot['committedAt'],
                     **({'recovery': snapshot['recovery']} if snapshot.get('recovery') else {})})
        previous, previous_epoch = totals, epoch
    if any(accumulated.get(market, 0) != int(previous.get(market, 0)) for market in set(accumulated) | set(previous)):
        raise ValueError('Raw allocation reconciliation failed')
    return {'startEpoch': start_epoch, 'publishedEpoch': published_epoch,
            'epochs': rows, 'missingEpochs': missing, 'recoveredEpochs': [row['epoch'] for row in rows if row.get('recovery')], 'reconciled': True,
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
    by_commit = {row['commit']: row for row in cache if row.get('validationVersion') == VALIDATION_VERSION}
    snapshots = []
    for commit in commits:
        sha = commit['sha']
        if sha in by_commit:
            snapshots.append(by_commit[sha])
            continue
        data, digest = read_json(f'{RAW}/{sha}/{path}', optional=True)
        stamp = commit['commit']['committer']['date']
        if data is None:
            snapshots.append({'commit': sha, 'committedAt': stamp, 'deleted': True, 'validationVersion': VALIDATION_VERSION})
        else:
            try:
                snapshots.append(summarize_snapshot(data, sha, stamp, digest, chain=chain))
            except SnapshotMismatch as error:
                snapshots.append({'commit': sha, 'committedAt': stamp, 'sha256': digest,
                                  'epoch': int(data['metadata']['epoch']), 'invalid': str(error), 'validationVersion': VALIDATION_VERSION})
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
