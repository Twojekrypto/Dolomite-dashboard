#!/usr/bin/env python3
"""Fetch exact, pinned archive-RPC evidence for strict EARN replay."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, Optional

from earn_strict_replay import strict_event_key
from rpc_client import RpcClient
from scan_earn_netflow import CHAINS as EARN_CHAIN_CONFIG


INDEX_UPDATE_TOPICS = {
    "default": "0x247e2f5b851dd23ef755d9ad527e801ee202c4097acd70c21e82dc5602cdd879",
    "arbitrum": "0xf4626fd1187f91e6761ffb8a6ac3e8d9235a4a92da54e43feb0c57c4a4a322ab",
}
GET_MARKET_CURRENT_INDEX = "0x56ea84b2"
GET_ACCOUNT_BALANCES = "0x6a8194e7"


def _integer(value, default=None):
    try:
        if isinstance(value, str) and value.startswith("0x"):
            return int(value, 16)
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _pad_uint(value) -> str:
    return f"{int(value):064x}"


def _pad_address(address: str) -> str:
    normalized = str(address).lower().replace("0x", "")
    if len(normalized) != 40:
        raise ValueError(f"Invalid address: {address}")
    return normalized.rjust(64, "0")


def _hex_words(payload: str):
    raw = str(payload or "").removeprefix("0x")
    if not raw or len(raw) % 64 != 0:
        return None
    return [raw[offset:offset + 64] for offset in range(0, len(raw), 64)]


def _decode_current_index(payload: str):
    words = _hex_words(payload)
    if not words or len(words) < 2:
        return None
    borrow_index = int(words[0], 16)
    supply_index = int(words[1], 16)
    if borrow_index <= 0 or supply_index <= 0:
        return None
    return {
        "borrowIndex": str(borrow_index),
        "supplyIndex": str(supply_index),
    }


def _read_dynamic_words(words, byte_offset: int, *, tuple_width=1):
    if byte_offset < 0 or byte_offset % 32:
        return None
    start = byte_offset // 32
    if start >= len(words):
        return None
    length = int(words[start], 16)
    end = start + 1 + length * tuple_width
    if end > len(words):
        return None
    return [
        words[start + 1 + index * tuple_width:start + 1 + (index + 1) * tuple_width]
        for index in range(length)
    ]


def _decode_signed(sign_word: str, value_word: str) -> int:
    value = int(value_word, 16)
    return value if int(sign_word, 16) != 0 else -value


def decode_account_balances(payload: str):
    """Decode getAccountBalances and preserve signed borrow positions."""
    words = _hex_words(payload)
    if not words or len(words) < 4:
        return None
    offsets = [int(words[index], 16) for index in range(4)]
    markets = _read_dynamic_words(words, offsets[0])
    tokens = _read_dynamic_words(words, offsets[1])
    pars = _read_dynamic_words(words, offsets[2], tuple_width=2)
    weis = _read_dynamic_words(words, offsets[3], tuple_width=2)
    if markets is None or tokens is None or pars is None or weis is None:
        return None
    if not (len(markets) == len(tokens) == len(pars) == len(weis)):
        return None
    decoded = []
    for index in range(len(markets)):
        decoded.append({
            "marketId": str(int(markets[index][0], 16)),
            "par": str(_decode_signed(pars[index][0], pars[index][1])),
            "wei": str(_decode_signed(weis[index][0], weis[index][1])),
        })
    return decoded


def _history_events(history_payload: dict, comparison_block: int):
    events = []
    accounts = []
    for raw_account, account_data in (history_payload.get("accounts") or {}).items():
        if not isinstance(account_data, dict) or account_data.get("accountKnown") is not True:
            continue
        account = str(raw_account)
        accounts.append(account)
        for raw_market, market_data in (account_data.get("markets") or {}).items():
            market_id = str(raw_market)
            for event in ((market_data or {}).get("events") or []):
                if not isinstance(event, dict) or event.get("accountKnown") is not True:
                    continue
                block_number = _integer(event.get("blockNumber"), None)
                new_par = _integer(event.get("newPar"), None)
                if block_number is None or new_par is None or block_number > comparison_block:
                    continue
                events.append({
                    **event,
                    "account": account,
                    "marketId": market_id,
                    "blockNumber": block_number,
                    "transactionIndex": _integer(event.get("transactionIndex"), 0),
                    "logIndex": _integer(event.get("logIndex"), 0),
                    "newPar": new_par,
                })
    events.sort(key=lambda row: (
        row["blockNumber"],
        row["transactionIndex"],
        row["logIndex"],
        row["account"],
        row["marketId"],
    ))
    return events, sorted(set(accounts))


def _decode_index_logs(logs: Iterable[dict]):
    decoded = defaultdict(lambda: defaultdict(list))
    for log in logs or []:
        if not isinstance(log, dict):
            continue
        topics = log.get("topics") or []
        words = _hex_words(log.get("data"))
        if len(topics) < 2 or not words or len(words) < 2:
            continue
        market_id = str(_integer(topics[1], 0))
        block_number = _integer(log.get("blockNumber"), 0)
        borrow_index = int(words[0], 16)
        supply_index = int(words[1], 16)
        if borrow_index <= 0 or supply_index <= 0:
            continue
        decoded[block_number][market_id].append({
            "transactionIndex": _integer(log.get("transactionIndex"), 0),
            "logIndex": _integer(log.get("logIndex"), 0),
            "borrowIndex": borrow_index,
            "supplyIndex": supply_index,
        })
    for block_rows in decoded.values():
        for market_rows in block_rows.values():
            market_rows.sort(key=lambda row: (row["transactionIndex"], row["logIndex"]))
    return decoded


def _last_index_before(event: dict, index_rows):
    selected = None
    for row in index_rows or []:
        if row["transactionIndex"] < event["transactionIndex"]:
            selected = row
            continue
        if (
            row["transactionIndex"] == event["transactionIndex"]
            and row["logIndex"] < event["logIndex"]
        ):
            selected = row
            continue
        break
    return selected


def fetch_strict_evidence(
    chain: str,
    address: str,
    history_payload: dict,
    *,
    comparison_block: int,
    client: Optional[RpcClient] = None,
) -> dict:
    """Fetch event indexes and current state at one immutable comparison block."""
    chain = str(chain).lower()
    address = str(address).lower()
    chain_config = EARN_CHAIN_CONFIG.get(chain) or {}
    margin = str(chain_config.get("margin") or "")
    if not margin:
        raise ValueError(f"Unsupported EARN chain: {chain}")
    comparison_block = int(comparison_block)
    if comparison_block <= 0:
        raise ValueError("comparison_block must be positive")
    if client is None:
        client = RpcClient(chain=chain)

    events, accounts = _history_events(history_payload, comparison_block)
    markets = sorted({event["marketId"] for event in events}, key=int)
    block_markets: Dict[int, set[str]] = defaultdict(set)
    for event in events:
        block_markets[event["blockNumber"]].add(event["marketId"])

    all_index_logs = []
    index_topic = INDEX_UPDATE_TOPICS.get(chain, INDEX_UPDATE_TOPICS["default"])
    for block_number in sorted(block_markets):
        block_tag = hex(block_number)
        market_topics = ["0x" + _pad_uint(market) for market in sorted(block_markets[block_number], key=int)]
        rows = client.call("eth_getLogs", [{
            "address": margin,
            "fromBlock": block_tag,
            "toBlock": block_tag,
            "topics": [index_topic, market_topics],
        }])
        if isinstance(rows, list):
            all_index_logs.extend(rows)
    index_logs = _decode_index_logs(all_index_logs)

    event_indexes = {}
    event_index_pairs = {}
    errors = {}
    prior_par: Dict[str, int] = {}
    for event in events:
        account = event["account"]
        market_id = event["marketId"]
        state_key = f"{account}|{market_id}"
        event_key = strict_event_key(account, market_id, event)
        pair = _last_index_before(
            event,
            index_logs[event["blockNumber"]][market_id],
        )
        previous_par = prior_par.get(state_key, 0)
        next_par = event["newPar"]
        prior_par[state_key] = next_par
        if pair is None:
            errors[event_key] = "missing_event_index"
            continue
        selected = (
            pair["supplyIndex"]
            if previous_par > 0
            else pair["borrowIndex"]
            if previous_par < 0
            else pair["borrowIndex"]
            if next_par < 0
            else pair["supplyIndex"]
        )
        event_indexes[event_key] = str(selected)
        event_index_pairs[event_key] = {
            "borrowIndex": str(pair["borrowIndex"]),
            "supplyIndex": str(pair["supplyIndex"]),
        }

    call_specs = []
    for market_id in markets:
        call_specs.append((
            "index",
            market_id,
            margin,
            GET_MARKET_CURRENT_INDEX + _pad_uint(market_id),
        ))
    for account in accounts:
        call_specs.append((
            "balances",
            account,
            margin,
            GET_ACCOUNT_BALANCES + _pad_address(address) + _pad_uint(account),
        ))
    block_tag = hex(comparison_block)
    call_results = client.eth_call_batch(
        [(spec[2], spec[3]) for spec in call_specs],
        block=block_tag,
    ) if call_specs else []

    current_indexes = {}
    current_positions = {}
    for index, spec in enumerate(call_specs):
        result = call_results[index] if index < len(call_results) else None
        kind, identifier = spec[0], spec[1]
        if kind == "index":
            decoded_index = _decode_current_index(result)
            if decoded_index is None:
                errors[f"currentIndex:{identifier}"] = "missing_current_index"
            else:
                current_indexes[str(identifier)] = decoded_index
            continue

        decoded_balances = decode_account_balances(result)
        if decoded_balances is None:
            errors[f"currentPositions:{identifier}"] = "missing_current_positions"
            continue
        for market_id in markets:
            current_positions[f"{identifier}|{market_id}"] = {
                "par": "0",
                "wei": "0",
            }
        for position in decoded_balances:
            market_id = str(position["marketId"])
            if market_id not in markets:
                continue
            current_positions[f"{identifier}|{market_id}"] = {
                "par": str(position["par"]),
                "wei": str(position["wei"]),
            }

    return {
        "version": 1,
        "chain": chain,
        "address": address,
        "comparisonBlock": comparison_block,
        "protocolStartBlock": int(chain_config.get("start_block") or 0),
        "eventIndexes": event_indexes,
        "eventIndexPairs": event_index_pairs,
        "currentIndexes": current_indexes,
        "currentPositions": current_positions,
        "errors": errors,
    }

