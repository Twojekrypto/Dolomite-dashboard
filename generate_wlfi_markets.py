#!/usr/bin/env python3
"""Publish a complete, same-block Ethereum WLFI account-family snapshot.

Discovery and Par come from one healthy pinned subgraph block. Current indices,
oracle prices, account Wei and protocol totals are read directly at that block.
Any incomplete response or reconciliation failure preserves the previous file.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import json
from pathlib import Path
import re
import sys
import time
from urllib.request import Request, urlopen

from eth_abi import decode, encode
from eth_utils import keccak

from rpc_client import RpcClient, sanitize_error
from wlfi_markets import MINIMUM, MAXIMUM, account_number, account_origin

ADDRESS = re.compile(r'0x[0-9a-f]{40}')
HASH = re.compile(r'0x[0-9a-fA-F]{64}')
SCALE = 10**18


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, field, *, signed=False):
    require(not isinstance(value, bool) and isinstance(value, (str, int)), f'Invalid {field}')
    require(re.fullmatch(r'-?[0-9]+' if signed else r'[0-9]+', str(value)) is not None, f'Invalid {field}')
    return int(value)


def raw_par(value, decimals):
    require(isinstance(value, str), 'Par must be a decimal string')
    try:
        with localcontext() as ctx:
            ctx.prec = 160
            result = Decimal(value) * 10**decimals
            require(result.is_finite() and result == result.to_integral_value(), 'Invalid fractional Par')
            return int(result)
    except InvalidOperation as exc:
        raise ValueError('Invalid Par') from exc


def text_decimal(value):
    text = format(value, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def dollars(raw):
    # Integer decimal placement is exact regardless of the process Decimal context.
    sign = '-' if raw < 0 else ''
    digits = str(abs(raw)).rjust(37, '0')
    return text_decimal(Decimal(sign + digits[:-36] + '.' + digits[-36:]))


def share(value, total):
    require(total > 0 or value == 0, 'Nonzero value with zero denominator')
    with localcontext() as ctx:
        ctx.prec = 80
        return text_decimal(Decimal(value) * 100 / total) if total else '0'


def par_to_wei(par, index):
    return (par * index + SCALE // 2) // SCALE


def build_snapshot(head, tokens, indexed_accounts, chain_markets, chain_accounts, *, now=None, max_source_age_seconds=1800):
    now = now or datetime.now(timezone.utc)
    block = integer(head.get('number'), 'block number')
    stamp = integer(head.get('timestamp'), 'block timestamp')
    require(block > 0 and HASH.fullmatch(str(head.get('hash', ''))), 'Missing source block identity')
    age = now.timestamp() - stamp
    require(-300 <= age <= max_source_age_seconds, 'Stale or future source block')
    require(tokens and len(tokens) == len(chain_markets), 'Incomplete market coverage')
    markets = {}
    market_ids = set()
    for token in tokens:
        token_id = token['id']
        require(ADDRESS.fullmatch(token_id) and token_id not in markets, 'Invalid or duplicate token')
        mid = integer(token['marketId'], 'market id')
        decimals = integer(token['decimals'], 'token decimals')
        require(mid not in market_ids and decimals <= 36, 'Invalid or duplicate market')
        market_ids.add(mid)
        matches = [m for m in chain_markets if m.get('token') == token_id]
        require(len(matches) == 1, 'Missing or duplicate chain market')
        chain = matches[0]
        require(integer(chain['marketId'], 'chain market') == mid, 'Market-token mismatch')
        supply_index = integer(chain['supplyIndex'], 'supply index')
        borrow_index = integer(chain['borrowIndex'], 'borrow index')
        price = integer(chain['price'], 'oracle price')
        require(supply_index > 0 and borrow_index > 0 and price > 0, 'Invalid current index or oracle price')
        supply_par = integer(chain['supplyPar'], 'total supply Par')
        borrow_par = integer(chain['borrowPar'], 'total borrow Par')
        require(supply_par == raw_par(token['totalPar']['supplyPar'], decimals), 'Protocol supply Par mismatch')
        require(borrow_par == raw_par(token['totalPar']['borrowPar'], decimals), 'Protocol borrow Par mismatch')
        supply_wei = integer(chain['supplyWei'], 'protocol supply Wei')
        borrow_wei = integer(chain['borrowWei'], 'protocol borrow Wei')
        require(supply_wei == par_to_wei(supply_par, supply_index), 'Protocol supply Wei mismatch')
        require(borrow_wei == par_to_wei(borrow_par, borrow_index), 'Protocol borrow Wei mismatch')
        markets[token_id] = dict(token=token, chain=chain, decimals=decimals,
                                 price=price, supplyIndex=supply_index, borrowIndex=borrow_index,
                                 protocolSupply=supply_wei * price, protocolBorrow=borrow_wei * price)
    require(market_ids == set(range(len(tokens))), 'Incomplete protocol market IDs')
    indexed = {a['id']: a for a in indexed_accounts}
    require(len(indexed) == len(indexed_accounts) == len(chain_accounts), 'Incomplete or duplicate account coverage')
    seen = set()
    aggregate = defaultdict(lambda: {'supply': 0, 'borrow': 0, 'suppliers': set(), 'borrowers': set()})
    wallets = set()
    for account in chain_accounts:
        identity = account['id']
        require(identity in indexed and identity not in seen, 'Unknown or duplicate chain account')
        seen.add(identity)
        source = indexed[identity]
        number = account_number(source.get('accountNumber'))
        require(number is not None and account_origin('ethereum', number) == 'wlfi', 'Account outside strict range')
        owner = source['user']['id']
        wallet = source['effectiveUser']['id']
        require(ADDRESS.fullmatch(owner) and ADDRESS.fullmatch(wallet), 'Invalid account wallet evidence')
        require(identity == owner + '-' + number and account['owner'] == owner
                and account['accountNumber'] == number, 'Raw account identity mismatch')
        expected = {}
        for value in source['tokenValues']:
            token_id = value['token']['id']
            require(token_id in markets and token_id not in expected, 'Unknown or duplicate indexed account token')
            expected[token_id] = raw_par(value['valuePar'], markets[token_id]['decimals'])
        actual = {}
        for balance in account['balances']:
            token_id = balance['token']
            require(token_id in markets and token_id not in actual, 'Unknown or duplicate chain account token')
            market = markets[token_id]
            require(str(balance['marketId']) == str(market['token']['marketId']), 'Account market mismatch')
            par = integer(balance['par'], 'account Par', signed=True)
            wei = integer(balance['wei'], 'account Wei', signed=True)
            actual[token_id] = par
            index = market['supplyIndex'] if par >= 0 else market['borrowIndex']
            predicted = par_to_wei(abs(par), index) * (1 if par >= 0 else -1)
            require(wei == predicted, 'Account Wei/current index mismatch')
            if wei:
                side = 'supply' if wei > 0 else 'borrow'
                data = aggregate[token_id]
                data[side] += abs(wei) * market['price']
                data['suppliers' if wei > 0 else 'borrowers'].add(wallet)
                wallets.add(wallet)
        require({t: v for t, v in expected.items() if v} == {t: v for t, v in actual.items() if v}, 'Account Par mismatch')
    require(seen == set(indexed), 'Missing accounts')
    output_markets = []
    supply = borrowed = protocol_supply = protocol_borrow = 0
    for token_id, market in markets.items():
        data = aggregate[token_id]
        require(data['supply'] <= market['protocolSupply'] and data['borrow'] <= market['protocolBorrow'], 'Cohort exceeds same-block protocol total')
        supply += data['supply']; borrowed += data['borrow']
        protocol_supply += market['protocolSupply']; protocol_borrow += market['protocolBorrow']
        output_markets.append({'chain': 'ethereum', 'tokenId': token_id, 'marketId': str(market['token']['marketId']),
                               'symbol': market['token']['symbol'], 'supplyUsd': dollars(data['supply']),
                               'borrowedUsd': dollars(data['borrow']), 'netTvlUsd': dollars(data['supply'] - data['borrow']),
                               'protocolSupplyUsd': dollars(market['protocolSupply']),
                               'supplySharePct': share(data['supply'], market['protocolSupply']),
                               'supplierWallets': len(data['suppliers']), 'borrowerWallets': len(data['borrowers'])})
    output_markets.sort(key=lambda m: int(m['marketId']))
    return {'schemaVersion': 1, 'generatedAt': now.isoformat().replace('+00:00', 'Z'),
            'source': {'chain': 'ethereum', 'blockNumber': block, 'blockHash': head['hash'].lower(), 'timestamp': stamp},
            'classification': {'minimum': str(MINIMUM), 'maximum': str(MAXIMUM)},
            'totals': {'supplyUsd': dollars(supply), 'borrowedUsd': dollars(borrowed), 'netTvlUsd': dollars(supply - borrowed),
                       'activeWallets': len(wallets), 'supplyShareEthereumPct': share(supply, protocol_supply),
                       'borrowShareEthereumPct': share(borrowed, protocol_borrow)}, 'markets': output_markets}


def write_snapshot(path, snapshot):
    require(snapshot.get('schemaVersion') == 1 and isinstance(snapshot.get('markets'), list)
            and snapshot.get('totals') and snapshot.get('source') and snapshot.get('classification'), 'Incomplete snapshot')
    path = Path(path)
    encoded = json.dumps(snapshot, separators=(',', ':'), allow_nan=False) + '\n'
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(encoded, encoding='utf-8')
    temporary.replace(path)


def calldata(signature, types, values):
    return keccak(text=signature)[:4] + encode(types, values)


class SnapshotReader:
    def __init__(self, config, rpc=None):
        self.config = config
        self.rpc = rpc or RpcClient('ethereum', timeout=config['requestTimeoutSeconds'], retries_per_endpoint=1)

    def graph(self, query):
        for attempt in range(self.config['requestAttempts']):
            try:
                request = Request(self.config['subgraph'], data=json.dumps({'query': query}).encode(),
                                  headers={'Content-Type': 'application/json', 'User-Agent': 'DoloViewWlfiMarkets/1.0'})
                with urlopen(request, timeout=self.config['requestTimeoutSeconds']) as response:
                    result = json.load(response)
                require(not result.get('errors') and isinstance(result.get('data'), dict), 'Incomplete GraphQL response')
                return result['data']
            except (OSError, ValueError) as exc:
                if attempt + 1 == self.config['requestAttempts']:
                    raise RuntimeError('WLFI subgraph request failed') from exc
                time.sleep(attempt + 1)

    def multicall(self, calls, block):
        values = []
        for start in range(0, len(calls), self.config['batchSize']):
            batch = calls[start:start + self.config['batchSize']]
            data = calldata('aggregate3((address,bool,bytes)[])', ['(address,bool,bytes)[]'],
                            [[(self.config['margin'], True, call) for call in batch]])
            raw = self.rpc.call('eth_call', [{'to': self.config['multicall'], 'data': '0x' + data.hex(), 'gas': hex(30000000)}, hex(block)])
            require(isinstance(raw, str) and raw.startswith('0x'), 'Missing multicall result')
            results = decode(['(bool,bytes)[]'], bytes.fromhex(raw[2:]))[0]
            require(len(results) == len(batch) and all(ok and value for ok, value in results), 'Failed or incomplete contract subcall')
            values.extend(value for _, value in results)
        return values

    def collect(self):
        meta = self.graph('{_meta{block{number hash timestamp} hasIndexingErrors}}')['_meta']
        require(meta.get('hasIndexingErrors') is False, 'Unhealthy subgraph')
        head = meta['block']; block = integer(head['number'], 'source block')
        require(-300 <= time.time() - integer(head['timestamp'], 'source time') <= self.config['maxSourceAgeSeconds'], 'Stale subgraph head')
        rpc_head = self.rpc.call('eth_getBlockByNumber', [hex(block), False])
        require(rpc_head and int(rpc_head['number'], 16) == block and int(rpc_head['timestamp'], 16) == head['timestamp']
                and rpc_head['hash'].lower() == str(head['hash']).lower(), 'Subgraph/RPC block identity mismatch')
        state = self.graph('{tokens(first:1000,block:{number:' + str(block) + '}){id symbol decimals marketId totalPar{supplyPar borrowPar}}}')['tokens']
        require(0 < len(state) < 1000, 'Incomplete token coverage')
        accounts = []; last = ''; size = self.config['pageSize']
        for _ in range(self.config['maxAccountPages']):
            query = ('{marginAccounts(first:' + str(size) + ',orderBy:id,orderDirection:asc,block:{number:' + str(block)
                     + '},where:{accountNumber_gte:"' + str(MINIMUM) + '",accountNumber_lte:"' + str(MAXIMUM)
                     + '",id_gt:' + json.dumps(last) + '}){id accountNumber user{id} effectiveUser{id} tokenValues(first:1000){valuePar token{id}}}}')
            page = self.graph(query)['marginAccounts']
            require(isinstance(page, list) and all(len(a['tokenValues']) < 1000 for a in page), 'Incomplete account tokens')
            require(all(a['id'] > last for a in page) and [a['id'] for a in page] == sorted({a['id'] for a in page}), 'Invalid account pagination')
            accounts.extend(page)
            if len(page) < size:
                break
            last = page[-1]['id']
        else:
            raise ValueError('Account pagination limit reached')
        count_raw = self.multicall([calldata('getNumMarkets()', [], [])], block)[0]
        require(decode(['uint256'], count_raw)[0] == len(state), 'Protocol market count mismatch')
        calls = []
        for token in state:
            mid = int(token['marketId'])
            for signature in ('getMarketTokenAddress(uint256)', 'getMarketCurrentIndex(uint256)', 'getMarketPrice(uint256)', 'getMarketTotalPar(uint256)', 'getMarketTotalWei(uint256)'):
                calls.append(calldata(signature, ['uint256'], [mid]))
        raw = self.multicall(calls, block); markets = []
        for i, token in enumerate(state):
            address = decode(['address'], raw[i * 5])[0]
            borrow, supply, _ = decode(['uint96', 'uint96', 'uint32'], raw[i * 5 + 1])
            price = decode(['uint256'], raw[i * 5 + 2])[0]
            borrow_par, supply_par = decode(['uint128', 'uint128'], raw[i * 5 + 3])
            borrow_wei, supply_wei = decode(['uint128', 'uint128'], raw[i * 5 + 4])
            markets.append({'token': address.lower(), 'marketId': token['marketId'], 'borrowIndex': str(borrow), 'supplyIndex': str(supply),
                            'price': str(price), 'borrowPar': str(borrow_par), 'supplyPar': str(supply_par), 'borrowWei': str(borrow_wei), 'supplyWei': str(supply_wei)})
        calls = [calldata('getAccountBalances((address,uint256))', ['(address,uint256)'], [(a['user']['id'], int(a['accountNumber']))]) for a in accounts]
        raw = self.multicall(calls, block); balances = []
        for account, value in zip(accounts, raw):
            ids, addresses, pars, weis = decode(['uint256[]', 'address[]', '(bool,uint128)[]', '(bool,uint256)[]'], value)
            require(len(ids) == len(addresses) == len(pars) == len(weis), 'Incomplete account balance response')
            values = [{'marketId': str(mid), 'token': address.lower(), 'par': str(par if sign else -par), 'wei': str(wei if wei_sign else -wei)}
                      for mid, address, (sign, par), (wei_sign, wei) in zip(ids, addresses, pars, weis)]
            balances.append({'id': account['id'], 'owner': account['user']['id'], 'accountNumber': account['accountNumber'], 'balances': values})
        final_head = self.rpc.call('eth_getBlockByNumber', [hex(block), False])
        require(final_head and final_head['hash'] == rpc_head['hash'], 'Pinned block changed during collection')
        return head, state, accounts, markets, balances


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('config/wlfi_markets.json'))
    parser.add_argument('--output', type=Path, default=Path('data/wlfi-markets/latest.json'))
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text())
        require(config['version'] == 1 and 1 <= config['batchSize'] <= 100
                and 1 <= config['pageSize'] <= 1000 and 1 <= config['maxAccountPages'] <= 100
                and 1 <= config['requestAttempts'] <= 5 and config['maxSourceAgeSeconds'] > 0, 'Invalid WLFI config')
        evidence = SnapshotReader(config).collect()
        snapshot = build_snapshot(*evidence, max_source_age_seconds=config['maxSourceAgeSeconds'])
        if args.evidence_dir:
            args.evidence_dir.mkdir(parents=True, exist_ok=True)
            for name, value in zip(('head', 'tokens', 'indexed-accounts', 'chain-markets', 'chain-accounts'), evidence):
                (args.evidence_dir / (name + '.json')).write_text(json.dumps(value, separators=(',', ':')) + '\n')
            (args.evidence_dir / 'verification.json').write_text(json.dumps({'block': snapshot['source'], 'accounts': len(evidence[2]), 'markets': len(evidence[1]),
                                                                         'allParReconciled': True, 'allWeiReconciled': True, 'allProtocolTotalsReconciled': True}, indent=2) + '\n')
        write_snapshot(args.output, snapshot)
        print(f"WLFI Markets: {len(evidence[2])} accounts, {len(snapshot['markets'])} markets, block {snapshot['source']['blockNumber']}; complete")
        return 0
    except Exception as exc:
        print(f'WLFI Markets refresh failed; previous snapshot preserved: {type(exc).__name__}: {sanitize_error(exc)}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
