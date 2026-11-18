"""Complete indexed protocol liquidation-rake receipts for the active three chains.

Amounts are gross protocol fees, with no Ethereum partner distribution applied.
USD is the official subgraph's historical oracle valuation, whose implementation
can retain a cached quote on a reverted oracle call. Completeness here describes
pinned indexed event discovery and unique raw-account matching, not independent
RPC receipt/price replay for every event. Source failures propagate to the atomic
operating-income publisher; neither missing amounts nor failed pages become zero.
"""
import hashlib
import json
import re
import time
from decimal import Decimal, localcontext

import requests

from rpc_client import RpcClient, RpcError


FEE_AGENT = '0x4d5f0344d245f1d13607e5b61dd317de3b3178b8'
LIQUIDATOR = '0x8564f36e53ac4f16a2c0b8449cf98d51c3438efd'
# Governed classification evidence: V2 ImplementationSet + DolomiteRakeSet and
# historical registry FeeAgentSet. V1 did not implement the protocol rake.
CHAINS = {
    'ethereum': {'chainId': 1, 'margin': '0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d',
                 'activationBlock': 24650900, 'activationTimestamp': 1773433283,
                 'rakeActivationTransaction': '0x37f83291fbfe5e42cc97ad14be5be2cb2c060cf1760101f5a18498caefacc74f'},
    'arbitrum': {'chainId': 42161, 'margin': '0x6bd780e7fdf01d77e4d475c821f1e7ae05409072',
                 'activationBlock': 441386386, 'activationTimestamp': 1773412299,
                 'rakeActivationTransaction': '0x1085d64ff4833e4352ecc7e01c5733a31183a8aed3ad1d0ba44bb8230db66047'},
    'berachain': {'chainId': 80094, 'margin': '0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d',
                  'activationBlock': 18198820, 'activationTimestamp': 1773412356,
                  'rakeActivationTransaction': '0xf4cdb4e90e6a83ebd4594c0344ea36faf1b1cd1b524e9a47bd5a3e1274dc39e8'},
}
DEFAULTS = {'pageSize': 1000, 'maxPages': 1000, 'requestTimeoutSeconds': 30,
            'requestAttempts': 3, 'maxSourceAgeSeconds': 21600}
VALUATION = {'method': 'official_subgraph_event_block_oracle',
             'cachedOracleFallbackPossible': True, 'independentReceiptAudit': False,
             'priceBasis': 'recorded_historical_usd_no_current_price_revaluation',
             'source': 'https://github.com/dolomite-exchange/dolomite-subgraph/blob/master/src/mappings/helpers/pricing.ts'}

META_QUERY = '{_meta{block{number hash} deployment hasIndexingErrors}}'
PIN_META_QUERY = 'query($block:Int!){_meta(block:{number:$block}){block{number hash} deployment hasIndexingErrors}}'
COMMON_FIELDS = 'id logIndex serialId transaction{id timestamp blockNumber}'
TRANSFER_FIELDS = COMMON_FIELDS + '''
 token{id marketId decimals} fromMarginAccount{accountNumber user{id}}
 toMarginAccount{accountNumber user{id}} toEffectiveUser{id}
 amountDeltaWei amountUSDDeltaWei'''
LIQUIDATION_FIELDS = COMMON_FIELDS + '''
 heldToken{id marketId decimals} solidMarginAccount{accountNumber user{id}}
 heldTokenAmountDeltaWei heldTokenAmountUSD'''


def _uint(value, label):
    if isinstance(value, bool) or not re.fullmatch(r'[0-9]+', str(value)):
        raise ValueError('Invalid ' + label)
    return int(value)


def _hex(value, size, label):
    if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{%d}' % (size * 2), value):
        raise ValueError('Invalid ' + label)
    return value.lower()


def _amount(value, label):
    if not isinstance(value, str) or len(value) > 120 or not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', value):
        raise ValueError('Missing or invalid ' + label)
    return Decimal(value)


def _decimal_string(value):
    return format(value, 'f').rstrip('0').rstrip('.') if value % 1 else format(value, '.0f')


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _event(row, start, end):
    if not isinstance(row, dict) or not isinstance(row.get('transaction'), dict):
        raise ValueError('Missing event transaction')
    tx = row['transaction']
    tx_hash = _hex(tx.get('id'), 32, 'transaction hash')
    index = _uint(row.get('logIndex'), 'log index')
    serial = _uint(row.get('serialId'), 'serial id')
    block = _uint(tx.get('blockNumber'), 'event block')
    stamp = _uint(tx.get('timestamp'), 'event timestamp')
    if row.get('id') != tx_hash + '-' + str(index) or not start <= stamp < end:
        raise ValueError('Event identity or source window mismatch')
    return tx_hash, index, serial, block, stamp


def _account(value):
    if not isinstance(value, dict):
        raise ValueError('Missing raw margin account')
    number = _uint(value.get('accountNumber'), 'raw account number')
    if number >= 2**256:
        raise ValueError('Invalid uint256 account number')
    return _hex((value.get('user') or {}).get('id'), 20, 'raw account owner'), number


def _token(value):
    if not isinstance(value, dict):
        raise ValueError('Missing event token')
    address = _hex(value.get('id'), 20, 'token address')
    decimals = _uint(value.get('decimals'), 'token decimals')
    market = _uint(value.get('marketId'), 'market id')
    if decimals > 255:
        raise ValueError('Invalid token decimals')
    return address, market, decimals


def _unique_events(rows, start, end):
    unique, duplicates = {}, 0
    for row in rows:
        event = _event(row, start, end)
        key = event[:2]
        if key in unique:
            if unique[key][1] != row:
                raise ValueError('Conflicting duplicate indexed event')
            duplicates += 1
        else:
            unique[key] = event, row
    return list(unique.values()), duplicates


def summarize_fee_transfers(chain, liquidations, transfers, start_timestamp, end_timestamp):
    """Pure exact reducer; join globally, including transactions split over pages."""
    if chain not in CHAINS or start_timestamp >= end_timestamp:
        raise ValueError('Invalid liquidation scope or period')
    liqs, duplicate_liqs = _unique_events(liquidations, start_timestamp, end_timestamp)
    fees, duplicate_fees = _unique_events(transfers, start_timestamp, end_timestamp)
    groups, liquidation_rows = {}, {}
    for event, row in liqs:
        key = event[0], _token(row.get('heldToken')), _account(row.get('solidMarginAccount'))
        groups.setdefault(key, []).append(event)
        liquidation_rows[event[:2]] = row
    for group in groups.values():
        group.sort(key=lambda event: event[2], reverse=True)

    matched, evidence, zero_count = set(), [], 0
    with localcontext() as context:
        context.prec = 160
        total = Decimal(0)
        for event, row in sorted(fees):
            if _account(row.get('toMarginAccount')) != (FEE_AGENT, 0):
                raise ValueError('Transfer is not to the protocol fee agent account zero')
            if _hex((row.get('toEffectiveUser') or {}).get('id'), 20, 'effective recipient') != FEE_AGENT:
                raise ValueError('Wrong effective fee recipient')
            token = _token(row.get('token'))
            amount = _amount(row.get('amountDeltaWei'), 'fee token amount')
            usd = _amount(row.get('amountUSDDeltaWei'), 'historical fee USD')
            raw = amount * Decimal(10)**token[2]
            if raw != raw.to_integral_value() or raw >= 2**256 or (not amount and usd):
                raise ValueError('Invalid raw fee amount')
            if event[3] < CHAINS[chain]['activationBlock'] or event[4] < CHAINS[chain]['activationTimestamp']:
                raise ValueError('Fee transfer predates protocol-rake activation')
            key = event[0], token, _account(row.get('fromMarginAccount'))
            candidates = [liq for liq in groups.get(key, []) if liq[2] < event[2] and liq[1] < event[1]]
            if not candidates:
                raise ValueError('Protocol fee transfer has no matching preceding liquidation')
            liq = candidates[0]
            if liq[3:] != event[3:] or liq[:2] in matched:
                raise ValueError('Fee transaction context conflicts or liquidation receipt reused')
            if amount and not usd:
                # The official mapping truncates historical USD to 18 places.
                # Prove a positive-token zero quote is sub-quantum dust using
                # the same liquidation's held-token value; never accept a
                # material amount with an unavailable/zero source price.
                source_row = liquidation_rows[liq[:2]]
                held_amount = _amount(source_row.get('heldTokenAmountDeltaWei'), 'liquidation held amount')
                held_usd = _amount(source_row.get('heldTokenAmountUSD'), 'liquidation held USD')
                quantum = Decimal('0.000000000000000001')
                if not held_amount or not held_usd or amount * (held_usd + quantum) > held_amount * quantum:
                    raise ValueError('Zero USD fee lacks a verified historical dust valuation')
            matched.add(liq[:2])
            total += usd
            zero_count += usd == 0
            evidence.append({'transferId': row['id'], 'liquidationId': liq[0] + '-' + str(liq[1]),
                             'blockNumber': event[3], 'timestamp': event[4],
                             'token': token[0], 'marketId': token[1], 'feeWeiRaw': str(int(raw)),
                             'grossProtocolFeeUSD': _decimal_string(usd)})
        exact = _decimal_string(total)
    return {'grossProtocolFeeUSD': exact, 'feeTransferCount': len(fees),
            'positiveUSDTransferCount': len(fees) - zero_count, 'zeroUSDTransferCount': zero_count,
            'liquidationEventCount': len(liqs), 'liquidationsWithoutFeeTransferCount': len(liqs) - len(matched),
            'duplicateEventsRemoved': duplicate_liqs + duplicate_fees,
            'unmatchedTransferCount': 0, 'eventEvidenceSHA256': _digest(evidence),
            'valuation': dict(VALUATION)}


def paginate(fetch_page, page_size, max_pages):
    """Require strict ID cursors and a terminal short page; never publish a cap."""
    rows, cursor = [], ''
    for page in range(1, max_pages + 1):
        batch = fetch_page(cursor)
        if not isinstance(batch, list) or len(batch) > page_size:
            raise ValueError('Invalid indexed event page')
        previous = cursor
        for row in batch:
            identity = row.get('id') if isinstance(row, dict) else None
            if not isinstance(identity, str) or identity <= previous:
                raise ValueError('Repeated, missing, or unordered event cursor')
            previous = identity
        rows.extend(batch)
        if len(batch) < page_size:
            return rows, page
        cursor = previous
    raise ValueError('Indexed event pagination limit reached before terminal page')


def _graphql(url, query, variables, settings):
    for attempt in range(settings['requestAttempts']):
        try:
            response = requests.post(url, json={'query': query, 'variables': variables},
                                     timeout=settings['requestTimeoutSeconds'],
                                     headers={'User-Agent': 'DoloView/operating-income-liquidations', 'Accept': 'application/json'})
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or payload.get('errors') or not isinstance(payload.get('data'), dict):
                raise ValueError('Invalid or incomplete liquidation subgraph response')
            return payload['data']
        except (requests.RequestException, ValueError):
            if attempt + 1 == settings['requestAttempts']:
                raise
            time.sleep(min(2**attempt, 8))


def _meta(value, expected_pin=None):
    if not isinstance(value, dict) or value.get('hasIndexingErrors') is not False:
        raise ValueError('Missing or unhealthy liquidation subgraph metadata')
    block = value.get('block') or {}
    number = _uint(block.get('number'), 'indexed block')
    # Graph Node may omit the hash for a historical _meta(block: ...) query.
    # Only that recheck can reuse our independently RPC-verified original pin.
    block_hash = (expected_pin['blockHash'] if expected_pin and block.get('hash') is None
                  else _hex(block.get('hash'), 32, 'indexed block hash'))
    deployment = value.get('deployment')
    if not isinstance(deployment, str) or not deployment:
        raise ValueError('Missing subgraph deployment identity')
    return {'blockNumber': number, 'blockHash': block_hash, 'deployment': deployment}


def _header_timestamp(header, pin):
    if not isinstance(header, dict) or header.get('number') != hex(pin['blockNumber']):
        return None
    block_hash = header.get('hash')
    if not isinstance(block_hash, str) or not re.fullmatch(r'0x[0-9a-fA-F]{64}', block_hash):
        return None
    # A real conflicting hash is a canonicality failure, never a retry hint.
    if block_hash.lower() != pin['blockHash']:
        raise ValueError('Indexed/RPC pinned block hash mismatch')
    timestamp = header.get('timestamp')
    if not isinstance(timestamp, str) or not re.fullmatch(r'0x[0-9a-fA-F]+', timestamp):
        return None
    return int(timestamp, 16)


def _header(rpc, pin):
    params = [hex(pin['blockNumber']), False]
    try:
        header = rpc.call('eth_getBlockByNumber', params)
    except RpcError:
        header = None
    timestamp = _header_timestamp(header, pin)
    if timestamp is not None:
        return timestamp
    # RpcClient already rotates transport/rate-limit failures. A lagging node
    # can instead return a successful null/incomplete result; try each remaining
    # configured provider once, retaining RpcClient's timeout/rate-limit guards.
    failed_endpoint = getattr(rpc, 'last_endpoint', None)
    disabled = getattr(rpc, '_disabled_endpoints', set())
    for endpoint in dict.fromkeys(getattr(rpc, 'endpoints', [])):
        if endpoint == failed_endpoint or endpoint in disabled:
            continue
        fallback = RpcClient(endpoints=[endpoint], timeout=getattr(rpc, 'timeout', 30),
                             retries_per_endpoint=1, quiet=True)
        try:
            header = fallback.call('eth_getBlockByNumber', params)
        except RpcError:
            continue
        timestamp = _header_timestamp(header, pin)
        if timestamp is not None:
            return timestamp
    raise ValueError('Missing pinned RPC block header on every configured endpoint')


def collect_liquidation_income(chain_configs, start_timestamp, end_timestamp, *, options=None):
    """Collect a complete gross fee report, or raise and preserve prior publication."""
    start = _uint(start_timestamp, 'period start')
    end = _uint(end_timestamp, 'period end')
    if start >= end or set(chain_configs) != set(CHAINS):
        raise ValueError('Require a valid period and exactly the active three chains')
    settings = dict(DEFAULTS, **(options or {}))
    for name in DEFAULTS:
        settings[name] = _uint(settings[name], name)
        if settings[name] < 1:
            raise ValueError('Invalid positive collector option ' + name)
    if settings['pageSize'] > 1000:
        raise ValueError('Subgraph pageSize exceeds 1000')
    for chain, config in chain_configs.items():
        if config.get('chainId') != CHAINS[chain]['chainId'] or str(config.get('margin')).lower() != CHAINS[chain]['margin']:
            raise ValueError('Wrong governed liquidation chain/contract scope')
        if not isinstance(config.get('subgraph'), str) or not config['subgraph'].startswith('https://'):
            raise ValueError('Missing HTTPS liquidation subgraph endpoint')

    reports = {}
    for chain in CHAINS:
        config = chain_configs[chain]
        url = config['subgraph']
        rpc = RpcClient(chain, timeout=settings['requestTimeoutSeconds'], retries_per_endpoint=1, quiet=True)
        if int(rpc.call('eth_chainId', []), 16) != config['chainId']:
            raise ValueError('Wrong RPC chain')
        pin = _meta(_graphql(url, META_QUERY, {}, settings).get('_meta'))
        timestamp = _header(rpc, pin)
        now = int(time.time())
        if timestamp < end or timestamp > now + 120 or now - timestamp > settings['maxSourceAgeSeconds']:
            raise ValueError('Stale, future, or insufficiently indexed liquidation source')
        datasets, pages = {}, {}
        for family, fields in [('liquidations', LIQUIDATION_FIELDS), ('transfers', TRANSFER_FIELDS)]:
            declarations = '$first:Int!,$cursor:ID!,$block:Int!,$start:BigInt!,$end:BigInt!'
            filters = 'id_gt:$cursor,transaction_:{timestamp_gte:$start,timestamp_lt:$end}'
            if family == 'transfers':
                declarations += ',$agent:String!'
                filters += ',toEffectiveUser:$agent'
            query = ('query(' + declarations + '){' + family + '(first:$first,orderBy:id,orderDirection:asc,'
                     'block:{number:$block},where:{' + filters + '}){' + fields + '}}')
            def fetch(cursor):
                variables = {'first': settings['pageSize'], 'cursor': cursor, 'block': pin['blockNumber'],
                             'start': str(start), 'end': str(end)}
                if family == 'transfers':
                    variables['agent'] = FEE_AGENT
                return _graphql(url, query, variables, settings).get(family)
            datasets[family], pages[family] = paginate(fetch, settings['pageSize'], settings['maxPages'])
        # Recheck both canonical block identity and the graph deployment after all pages.
        if _header(rpc, pin) != timestamp or _meta(_graphql(url, PIN_META_QUERY, {'block': pin['blockNumber']}, settings).get('_meta'), pin) != pin:
            raise ValueError('Liquidation source changed during collection')
        if any(_uint(row['transaction']['blockNumber'], 'event block') > pin['blockNumber']
               for rows in datasets.values() for row in rows):
            raise ValueError('Event exceeds pinned block')
        report = summarize_fee_transfers(chain, datasets['liquidations'], datasets['transfers'], start, end)
        report.update(complete=True, chainId=config['chainId'], margin=CHAINS[chain]['margin'],
                      feeAgent=FEE_AGENT, feeAgentAccountNumber='0',
                      source={'subgraph': url, **pin, 'timestamp': timestamp,
                              'eventSourceSHA256': _digest(datasets)},
                      coverage={'complete': True, 'allPagesReached': True, 'pages': pages,
                                'hasIndexingErrors': False, 'method': 'pinned_indexed_events_and_unique_raw_account_join',
                                'independentReceiptAudit': False},
                      activation={'feeAgentAndRakeBlock': CHAINS[chain]['activationBlock'],
                                  'feeAgentAndRakeTimestamp': CHAINS[chain]['activationTimestamp'],
                                  'rakeActivationTransaction': CHAINS[chain]['rakeActivationTransaction'],
                                  'operator': LIQUIDATOR, 'priorMechanism': 'V1_no_protocol_rake'})
        reports[chain] = report
    return {'complete': True, 'period': {'startTimestamp': start, 'endTimestamp': end},
            'scope': 'ethereum_arbitrum_berachain_protocol_fee_agent_liquidation_rake_only',
            'chains': reports, 'valuation': dict(VALUATION)}
