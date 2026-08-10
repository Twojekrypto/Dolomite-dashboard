#!/usr/bin/env python3
"""
Fetch Dolomite fees/revenue data from DeFiLlama's fees adapter.

The dashboard is static, so this stores the small revenue dataset used by the
Revenue tab instead of calling the API from every visitor's browser.
"""

import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_HALF_UP, getcontext

import requests
from web3 import Web3

from audit_dolomite_revenue_onchain import (
    CHAIN_CONFIGS as REVENUE_AUDIT_CHAIN_CONFIGS,
    DOLOMITE_MARGIN_ABI as REVENUE_AUDIT_MARGIN_ABI,
    OMITTED_IMMATERIAL_PRICE_SOURCE as REVENUE_AUDIT_OMITTED_PRICE_SOURCE,
    fetch_historical_prices as fetch_revenue_audit_historical_prices,
    find_block_at_or_before as find_revenue_audit_block_at_or_before,
    make_web3 as make_revenue_audit_web3,
    market_interest_rows as revenue_audit_market_interest_rows,
    resolve_token_price as resolve_revenue_audit_token_price,
)
import rpc_client


getcontext().prec = 60


DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_revenue.json")
ONCHAIN_AUDIT_FILE = os.path.join(DATA_DIR, "data", "dolomite-revenue-onchain-audit.json")
ONCHAIN_REVENUE_OVERRIDES_FILE = os.path.join(DATA_DIR, "data", "dolomite-revenue-onchain-overrides.json")
ONCHAIN_AUDIT_EXPECTED_TARGET_AFTER_UTC_HOUR = 10
BASE_URL = "https://api.llama.fi/summary/fees/dolomite"
BORROW_FEE_REBATE_METADATA_URL = "https://api.dolomite.io/liquidity-mining/ve-dolo-rebate/metadata"
BORROW_FEE_REBATE_DOCS_URL = "https://docs.dolomite.io/dolo/borrow-fee-rebates"
BERACHAIN_CHAIN_ID = "80094"
BERACHAIN_CHAIN_NAME = "Berachain"
ONCHAIN_REVENUE_OVERRIDE_CHAINS = {BERACHAIN_CHAIN_NAME, "Ethereum", "Mantle"}
ONCHAIN_CURRENT_INDEX_SOURCE = "onchain-current-index-audit"
BERACHAIN_COIN_CHAIN = "berachain"
BERACHAIN_MARGIN_ADDRESS = "0x003Ca23Fd5F0ca87D01F6eC6CD14A8AE60c2b97D"
FEE_REBATE_CLAIMER_ADDRESS = "0x6BE1fed8a38B3555A337f58BB9E10FC0465964C0"
FEE_REBATE_ROLLING_CLAIMS_ADDRESS = "0xea9421044430FA791c3Ab16E0B90f142aa6C11ef"
FEE_REBATE_ROLLING_CLAIMS_DEPLOY_TX = "0xc7096f2f4e799ff80a25116552932c3baab028cdaec310948e8270bf760ebc43"
FEE_REBATE_ROLLING_CLAIMS_DEPLOY_BLOCK = 21_228_103
FEE_REBATE_FIRST_MARKET_ROOT_BLOCK = 22_269_000
FEE_REBATE_DEPLOYMENTS_URL = "https://raw.githubusercontent.com/dolomite-exchange/dolomite-margin-modules/master/packages/deployment/src/deploy/deployments.json"
FEE_REBATE_EVENT_SOURCE = "FeeRebateRollingClaims.MarketIdToMerkleRootSet"
FEE_REBATE_START_TIMESTAMP = 1779321600
KNOWN_FEE_REBATE_SNAPSHOT_RESETS = {
    (BERACHAIN_CHAIN_ID, "0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c"): {
        "epoch": 9,
        "blockNumber": 24_055_329,
        "calculationMode": "known_epoch_snapshot_reset",
        "sourceLabel": "Published epoch snapshot reset",
    },
}
SECONDS_PER_WEEK = 7 * 24 * 60 * 60
SECONDS_PER_DAY = 24 * 60 * 60
BORROW_FEE_REBATE_MAX_REBATE_METHOD = "eligible_market_daily_current_index"
BORROW_FEE_REBATE_PROTOCOL_RESERVE_FACTOR = Decimal("0.20")
BORROW_FEE_REBATE_USD_SCALE = 1_000_000
BORROW_FEE_REBATE_CALCULATION_MODES = {
    "",
    "cumulative_delta",
    "known_epoch_snapshot_reset",
}
BORROW_FEE_REBATE_MAX_AUDIT_PUBLIC_DAY_DELAY_SECONDS = 8.0
LOG_CHUNK_SIZE = 10_000
STABLE_PRICE_SYMBOL_KEYS = {
    "HONEY",
    "NECT",
    "USDC",
    "USDT",
    "USDE",
    "BYUSD",
    "USDS",
    "USDL",
    "USD0",
}
REQUEST_TIMEOUTS = (
    (10, 45),
    (10, 75),
    (10, 120),
)

DOLOMITE_MARGIN_ABI = [
    {"inputs": [{"type": "uint256", "name": "marketId"}], "name": "getMarketTokenAddress", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
]

ERC20_ABI = [
    {"inputs": [], "name": "symbol", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
]

FEE_REBATE_ROLLING_CLAIMS_ABI = [
    {
        "inputs": [
            {"internalType": "uint256[]", "name": "_marketIds", "type": "uint256[]"},
            {"internalType": "bytes32[]", "name": "_merkleRoots", "type": "bytes32[]"},
            {"internalType": "uint256[]", "name": "_totalAmounts", "type": "uint256[]"},
            {"internalType": "uint256", "name": "_expectedEpoch", "type": "uint256"},
            {"internalType": "bool", "name": "_incrementEpoch", "type": "bool"},
        ],
        "name": "handlerSetMerkleRoots",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def day_from_timestamp(timestamp):
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).strftime("%Y-%m-%d")


def safe_number(value):
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def safe_decimal_number(value):
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def is_finite_real_json_number(value):
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value)


def validated_borrow_fee_rebate_epoch(epoch_rebate):
    if not isinstance(epoch_rebate, dict):
        raise ValueError("borrow fee rebate epoch row must be an object")
    rebate_usd = epoch_rebate.get("rebateUSD")
    if not is_finite_real_json_number(rebate_usd):
        raise ValueError("borrow fee rebateUSD must be a finite JSON number")
    calculation_mode = epoch_rebate.get("calculationMode")
    if calculation_mode is None:
        calculation_mode = ""
    if (
        not isinstance(calculation_mode, str)
        or calculation_mode not in BORROW_FEE_REBATE_CALCULATION_MODES
    ):
        raise ValueError("unsupported borrow fee rebate calculationMode")
    return rebate_usd, calculation_mode


def rebate_usd_micro_units(value):
    scaled = Decimal(str(value)) * BORROW_FEE_REBATE_USD_SCALE
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def allocate_weighted_rebate_micro_units(target_units, weights, capacities):
    if sum(capacities) < target_units:
        raise ValueError("borrow fee rebate exceeds gross revenue capacity")

    allocations = [0] * len(capacities)
    remaining_units = target_units
    while remaining_units > 0:
        active = [
            index for index, capacity in enumerate(capacities)
            if allocations[index] < capacity
        ]
        if not active:
            raise ValueError("borrow fee rebate exceeds gross revenue capacity")

        active_weights = {index: weights[index] for index in active}
        total_weight = sum(active_weights.values(), Decimal(0))
        if total_weight <= 0:
            active_weights = {index: Decimal(1) for index in active}
            total_weight = Decimal(len(active))
        quotas = {
            index: Decimal(remaining_units) * active_weights[index] / total_weight
            for index in active
        }
        capped = [
            index for index in active
            if quotas[index] >= capacities[index] - allocations[index]
        ]
        if capped:
            for index in capped:
                awarded = capacities[index] - allocations[index]
                allocations[index] += awarded
                remaining_units -= awarded
            continue

        floors = {
            index: int(quotas[index].to_integral_value(rounding=ROUND_FLOOR))
            for index in active
        }
        for index, awarded in floors.items():
            allocations[index] += awarded
            remaining_units -= awarded
        if remaining_units <= 0:
            break

        remainder_order = sorted(
            active,
            key=lambda index: (-(quotas[index] - floors[index]), index),
        )
        for index in remainder_order:
            if remaining_units <= 0:
                break
            if allocations[index] < capacities[index]:
                allocations[index] += 1
                remaining_units -= 1

    return allocations


def chain_value(breakdown, chain):
    chain_payload = breakdown.get(chain, {}) if isinstance(breakdown, dict) else {}
    if isinstance(chain_payload, dict):
        return sum(safe_number(value) for value in chain_payload.values())
    return safe_number(chain_payload)


def fetch_metric(data_type):
    url = f"{BASE_URL}?dataType={data_type}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "dolomite-dashboard-revenue/1.0",
    }
    last_error = None
    for attempt, timeout in enumerate(REQUEST_TIMEOUTS, start=1):
        try:
            print(f"   Fetching {data_type}, attempt {attempt}/{len(REQUEST_TIMEOUTS)}")
            response = requests.get(url, timeout=timeout, headers=headers)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError(f"{data_type} response is not a JSON object")
            if len(data.get("totalDataChart") or []) < 30:
                raise ValueError(f"{data_type} chart has too few rows")
            if len(data.get("totalDataChartBreakdown") or []) < 30:
                raise ValueError(f"{data_type} chain breakdown has too few rows")
            return data
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            print(f"   Attempt {attempt} failed: {exc}")
            if attempt < len(REQUEST_TIMEOUTS):
                time.sleep(5 * attempt)
    raise RuntimeError(f"Unable to fetch {data_type}: {last_error}")


def fetch_borrow_fee_rebate_metadata():
    headers = {
        "Accept": "application/json",
        "User-Agent": "dolomite-dashboard-revenue/1.0",
    }
    try:
        response = requests.get(BORROW_FEE_REBATE_METADATA_URL, timeout=(10, 30), headers=headers)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"   Borrow fee rebate metadata unavailable: {exc}")
        return None
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    return metadata if isinstance(metadata, dict) else None


def normalized_symbol(symbol):
    return "".join(ch for ch in str(symbol or "").upper() if ch.isalnum())


def decimal_from_value(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def fetch_historical_prices(timestamp, coin_ids):
    prices = {}
    coin_ids = [coin_id for coin_id in coin_ids if coin_id]
    for start in range(0, len(coin_ids), 80):
        chunk = coin_ids[start:start + 80]
        if not chunk:
            continue
        url = f"https://coins.llama.fi/prices/historical/{int(timestamp)}/{','.join(chunk)}"
        response = requests.get(url, timeout=45, headers={"User-Agent": "dolomite-dashboard-revenue/1.0"})
        response.raise_for_status()
        payload = response.json()
        for coin_id, data in (payload.get("coins") or {}).items():
            if isinstance(data, dict) and isinstance(data.get("price"), (int, float)):
                prices[coin_id] = Decimal(str(data["price"]))
        time.sleep(0.15)
    return prices


def resolve_rebate_token_price(symbol, price):
    if price is not None:
        return price, "coins-llama"
    symbol_key = normalized_symbol(symbol)
    if symbol_key in {normalized_symbol(item) for item in STABLE_PRICE_SYMBOL_KEYS} or symbol_key.startswith("USD") or symbol_key.endswith("USD"):
        return Decimal("1"), "stable-symbol-fallback"
    return None, None


def hex_to_bytes(data):
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    text = data.hex() if hasattr(data, "hex") else str(data or "")
    if text.startswith("0x"):
        text = text[2:]
    return bytes.fromhex(text)


def normalized_transaction_hash(tx_hash):
    text = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)
    if not text.startswith("0x"):
        text = "0x" + text
    return text.lower()


def token_metadata(w3, token):
    contract = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
    try:
        symbol = contract.functions.symbol().call()
    except Exception:
        symbol = token[:10]
    try:
        decimals = int(contract.functions.decimals().call())
    except Exception:
        decimals = 18
    return str(symbol), decimals


def get_market_token_metadata(w3, market_id, cache):
    if market_id in cache:
        return cache[market_id]
    margin = w3.eth.contract(address=Web3.to_checksum_address(BERACHAIN_MARGIN_ADDRESS), abi=DOLOMITE_MARGIN_ABI)
    token = margin.functions.getMarketTokenAddress(int(market_id)).call()
    symbol, decimals = token_metadata(w3, token)
    cache[market_id] = {
        "token": Web3.to_checksum_address(token),
        "symbol": symbol,
        "decimals": decimals,
    }
    return cache[market_id]


def rebate_epoch_for_timestamp(timestamp):
    if timestamp <= FEE_REBATE_START_TIMESTAMP:
        return 1
    return max(1, int((int(timestamp) - FEE_REBATE_START_TIMESTAMP) // SECONDS_PER_WEEK))


def fee_rebate_transaction_context_from_input(w3, transaction_input):
    if not transaction_input:
        return None
    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(FEE_REBATE_ROLLING_CLAIMS_ADDRESS),
            abi=FEE_REBATE_ROLLING_CLAIMS_ABI,
        )
        _function, args = contract.decode_function_input(transaction_input)
        epoch = int(args.get("_expectedEpoch") or 0)
        if epoch <= 0:
            return None
        return {
            "expectedEpoch": epoch,
            "incrementEpoch": bool(args.get("_incrementEpoch")),
        }
    except Exception:
        return None


def fee_rebate_epoch_from_transaction_input(w3, transaction_input):
    context = fee_rebate_transaction_context_from_input(w3, transaction_input)
    return context.get("expectedEpoch") if context else None


def classify_known_rebate_snapshot_reset(chain_id, tx_hash, context, events, previous_totals):
    spec = KNOWN_FEE_REBATE_SNAPSHOT_RESETS.get((chain_id, str(tx_hash).lower()))
    if not spec or not isinstance(context, dict):
        return None
    if context.get("expectedEpoch") != spec["epoch"] or context.get("incrementEpoch") is not True:
        return None
    if not isinstance(events, list) or len(events) < 2 or not isinstance(previous_totals, dict):
        return None

    try:
        market_ids = [event["marketId"] for event in events]
        previous_market_ids = list(previous_totals)
        if any(
            type(market_id) is not int or not 0 <= market_id < 2 ** 256
            for market_id in market_ids + previous_market_ids
        ):
            return None
        if len(market_ids) != len(set(market_ids)) or set(market_ids) != set(previous_totals):
            return None
        for event in events:
            total_raw = event["totalRaw"]
            previous_raw = previous_totals[event["marketId"]]
            if event["blockNumber"] != spec["blockNumber"]:
                return None
            if type(total_raw) is not int or type(previous_raw) is not int:
                return None
            if previous_raw <= 0 or not 0 < total_raw < previous_raw:
                return None
    except (KeyError, TypeError):
        return None

    return {
        "calculationMode": spec["calculationMode"],
        "sourceLabel": spec["sourceLabel"],
        "rebateRawByMarket": {event["marketId"]: event["totalRaw"] for event in events},
        "resetMarketCount": len(events),
        "aggregateAdjustmentRaw": sum(
            event["totalRaw"] - int(previous_totals[event["marketId"]])
            for event in events
        ),
    }


def fee_rebate_epoch_for_log(w3, log, timestamp, transaction_epoch_cache):
    tx_hash = normalized_transaction_hash(log["transactionHash"])
    if tx_hash not in transaction_epoch_cache:
        epoch = None
        try:
            tx = w3.eth.get_transaction(log["transactionHash"])
            epoch = fee_rebate_epoch_from_transaction_input(w3, tx.get("input"))
        except Exception:
            epoch = None
        transaction_epoch_cache[tx_hash] = epoch
    return transaction_epoch_cache.get(tx_hash) or rebate_epoch_for_timestamp(timestamp)


def rebate_epoch_window(epoch):
    start = FEE_REBATE_START_TIMESTAMP + (int(epoch) - 1) * SECONDS_PER_WEEK
    end = FEE_REBATE_START_TIMESTAMP + int(epoch) * SECONDS_PER_WEEK
    return start, end


def get_logs_chunked(w3, address, from_block, to_block, topic):
    logs = []
    start = int(from_block)
    latest = int(to_block)
    while start <= latest:
        end = min(start + LOG_CHUNK_SIZE - 1, latest)
        logs.extend(w3.eth.get_logs({
            "address": Web3.to_checksum_address(address),
            "fromBlock": start,
            "toBlock": end,
            "topics": [topic],
        }))
        start = end + 1
    return logs


def fetch_borrow_fee_rebate_data():
    """Read realized borrow-fee rebate liability from Berachain onchain events.

    The metadata endpoint tells us where rebates are enabled. The rolling-claims
    Merkle root events tell us how much is actually claimable by users after the
    veDOLO vote-weight scaling has been applied.
    """
    try:
        endpoints = rpc_client.get_endpoints("berachain")
    except Exception as exc:
        error = f"{type(exc).__name__}: {rpc_client.sanitize_error(exc)}"
        return {"status": "missing", "error": error, "chains": {BERACHAIN_CHAIN_NAME: {"status": "missing", "error": error}}}

    topic = Web3.keccak(text="MarketIdToMerkleRootSet(uint256,bytes32,uint256)").hex()
    last_error = None
    for endpoint in endpoints:
        try:
            w3 = Web3(Web3.HTTPProvider(endpoint, request_kwargs={"timeout": 30}))
            try:
                receipt = w3.eth.get_transaction_receipt(FEE_REBATE_ROLLING_CLAIMS_DEPLOY_TX)
                from_block = max(int(receipt["blockNumber"]), FEE_REBATE_FIRST_MARKET_ROOT_BLOCK)
            except Exception:
                from_block = FEE_REBATE_FIRST_MARKET_ROOT_BLOCK
            to_block = int(w3.eth.block_number)
            logs = get_logs_chunked(w3, FEE_REBATE_ROLLING_CLAIMS_ADDRESS, from_block, to_block, topic)
            logs = sorted(logs, key=lambda item: (int(item["blockNumber"]), int(item["transactionIndex"]), int(item["logIndex"])))
            block_timestamps = {}
            market_cache = {}
            market_totals = {}
            epoch_rebates = {}
            unsupported_corrections = []
            unsupported_correction_hashes = set()
            missing_price_count = 0
            price_fallback_count = 0
            priced_event_count = 0

            transaction_groups = []
            for log in logs:
                tx_hash = normalized_transaction_hash(log["transactionHash"])
                if not transaction_groups or transaction_groups[-1][0] != tx_hash:
                    transaction_groups.append((tx_hash, []))
                transaction_groups[-1][1].append(log)

            for tx_hash, transaction_logs in transaction_groups:
                decoded_events = []
                for log in transaction_logs:
                    market_id, _merkle_root, total_raw = w3.codec.decode(["uint256", "bytes32", "uint256"], hex_to_bytes(log["data"]))
                    decoded_events.append({
                        "marketId": int(market_id),
                        "totalRaw": int(total_raw),
                        "blockNumber": int(log["blockNumber"]),
                    })

                previous_totals = dict(market_totals)
                transaction_context = None
                try:
                    transaction = w3.eth.get_transaction(transaction_logs[0]["transactionHash"])
                    transaction_context = fee_rebate_transaction_context_from_input(w3, transaction.get("input"))
                except Exception:
                    transaction_context = None
                reset_result = classify_known_rebate_snapshot_reset(
                    BERACHAIN_CHAIN_ID,
                    tx_hash,
                    transaction_context,
                    decoded_events,
                    previous_totals,
                )
                market_ids = [event["marketId"] for event in decoded_events]
                duplicate_market_ids = len(market_ids) != len(set(market_ids))
                unsupported_correction = duplicate_market_ids or (
                    reset_result is None
                    and any(
                        event["totalRaw"] <= int(previous_totals.get(event["marketId"], 0))
                        for event in decoded_events
                    )
                )
                if unsupported_correction and tx_hash not in unsupported_correction_hashes:
                    unsupported_corrections.append({
                        "transactionHash": tx_hash,
                        "eventBlock": max(event["blockNumber"] for event in decoded_events),
                        "marketCount": len(decoded_events),
                        "reason": "unsupported_aggregate_correction",
                    })
                    unsupported_correction_hashes.add(tx_hash)

                if unsupported_correction:
                    for event in decoded_events:
                        market_totals[event["marketId"]] = event["totalRaw"]
                    continue

                for event in decoded_events:
                    market_id = event["marketId"]
                    total_raw = event["totalRaw"]
                    previous_raw = int(previous_totals.get(market_id, 0))
                    if reset_result is not None:
                        effective_raw = reset_result["rebateRawByMarket"][market_id]
                        calculation_mode = reset_result["calculationMode"]
                    else:
                        effective_raw = total_raw - previous_raw
                        calculation_mode = "cumulative_delta"
                        if effective_raw <= 0:
                            continue

                    block_number = event["blockNumber"]
                    if block_number not in block_timestamps:
                        block_timestamps[block_number] = int(w3.eth.get_block(block_number)["timestamp"])
                    timestamp = block_timestamps[block_number]
                    epoch = (
                        transaction_context.get("expectedEpoch")
                        if transaction_context
                        else rebate_epoch_for_timestamp(timestamp)
                    )
                    period_start, period_end = rebate_epoch_window(epoch)
                    metadata = get_market_token_metadata(w3, market_id, market_cache)
                    coin_id = f"{BERACHAIN_COIN_CHAIN}:{metadata['token'].lower()}"
                    prices = fetch_historical_prices(timestamp, [coin_id])
                    price, price_source = resolve_rebate_token_price(metadata["symbol"], prices.get(coin_id))
                    amount = Decimal(effective_raw) / (Decimal(10) ** int(metadata["decimals"]))
                    rebate_usd = Decimal(0)
                    if price is None:
                        missing_price_count += 1
                    else:
                        priced_event_count += 1
                        rebate_usd = amount * price
                        if price_source != "coins-llama":
                            price_fallback_count += 1

                    entry = epoch_rebates.setdefault(epoch, {
                        "epoch": epoch,
                        "periodStartTimestamp": period_start,
                        "periodEndTimestamp": period_end,
                        "eventTimestamp": timestamp,
                        "eventBlock": block_number,
                        "rebateUSD": Decimal(0),
                        "markets": [],
                    })
                    entry["eventTimestamp"] = max(int(entry["eventTimestamp"]), timestamp)
                    entry["eventBlock"] = max(int(entry["eventBlock"]), block_number)
                    entry["rebateUSD"] += rebate_usd
                    if reset_result is not None:
                        entry.update({
                            "calculationMode": reset_result["calculationMode"],
                            "sourceLabel": reset_result["sourceLabel"],
                            "transactionHash": tx_hash,
                            "resetMarketCount": reset_result["resetMarketCount"],
                            "aggregateAdjustmentRaw": reset_result["aggregateAdjustmentRaw"],
                        })
                    market_row = {
                        "marketId": market_id,
                        "token": metadata["token"],
                        "symbol": metadata["symbol"],
                        "amount": str(amount),
                        "rebateUSD": float(rebate_usd),
                        "priceUSD": float(price) if price is not None else None,
                        "priceSource": price_source,
                    }
                    if reset_result is not None:
                        market_row.update({
                            "calculationMode": calculation_mode,
                            "transactionHash": tx_hash,
                            "previousTotalRaw": previous_raw,
                            "publishedTotalRaw": total_raw,
                        })
                    entry["markets"].append(market_row)

                for event in decoded_events:
                    market_totals[event["marketId"]] = event["totalRaw"]

            epoch_rows = []
            total_rebate_usd = Decimal(0)
            for epoch, entry in sorted(epoch_rebates.items()):
                total_rebate_usd += entry["rebateUSD"]
                epoch_row = {
                    "epoch": int(epoch),
                    "periodStartTimestamp": int(entry["periodStartTimestamp"]),
                    "periodEndTimestamp": int(entry["periodEndTimestamp"]),
                    "eventTimestamp": int(entry["eventTimestamp"]),
                    "eventBlock": int(entry["eventBlock"]),
                    "rebateUSD": round(float(entry["rebateUSD"]), 6),
                    "marketCount": len(entry["markets"]),
                    "markets": entry["markets"],
                }
                for key in (
                    "calculationMode",
                    "sourceLabel",
                    "transactionHash",
                    "resetMarketCount",
                    "aggregateAdjustmentRaw",
                ):
                    if key in entry:
                        epoch_row[key] = entry[key]
                epoch_rows.append(epoch_row)

            chain_status = "partial" if missing_price_count else "ok"
            return {
                "status": chain_status,
                "source": FEE_REBATE_EVENT_SOURCE,
                "generatedAt": utc_now_iso(),
                "chains": {
                    BERACHAIN_CHAIN_NAME: {
                        "status": chain_status,
                        "source": FEE_REBATE_EVENT_SOURCE,
                        "chainId": int(BERACHAIN_CHAIN_ID),
                        "feeRebateClaimer": FEE_REBATE_CLAIMER_ADDRESS,
                        "feeRebateRollingClaims": FEE_REBATE_ROLLING_CLAIMS_ADDRESS,
                        "deploymentSource": FEE_REBATE_DEPLOYMENTS_URL,
                        "fromBlock": from_block,
                        "toBlock": to_block,
                        "eventCount": len(logs),
                        "pricedEventCount": priced_event_count,
                        "missingPriceCount": missing_price_count,
                        "priceFallbackCount": price_fallback_count,
                        "totalRebateUSD": round(float(total_rebate_usd), 6),
                        "latestRebateDate": day_from_timestamp(max((row["periodEndTimestamp"] for row in epoch_rows), default=0)) if epoch_rows else None,
                        "epochRebates": epoch_rows,
                        "unsupportedCorrections": unsupported_corrections,
                        "rpcHost": rpc_client.safe_host(endpoint),
                    }
                },
            }
        except Exception as exc:
            last_error = exc
            print(f"   Borrow fee rebate onchain read failed on {rpc_client.safe_host(endpoint)}: {type(exc).__name__}: {rpc_client.sanitize_error(exc)}")

    error = f"{type(last_error).__name__}: {rpc_client.sanitize_error(last_error)}" if last_error else "no RPC endpoint"
    return {"status": "missing", "error": error, "chains": {BERACHAIN_CHAIN_NAME: {"status": "missing", "error": error}}}


def chart_map(rows):
    out = {}
    for row in rows or []:
        if not isinstance(row, list) or len(row) < 2:
            continue
        out[int(row[0])] = safe_number(row[1])
    return out


def breakdown_map(rows):
    out = {}
    for row in rows or []:
        if not isinstance(row, list) or len(row) < 2:
            continue
        out[int(row[0])] = row[1] if isinstance(row[1], dict) else {}
    return out


def merge_series(revenue_data, fees_data):
    revenue_chart = chart_map(revenue_data.get("totalDataChart"))
    fees_chart = chart_map(fees_data.get("totalDataChart"))
    revenue_breakdowns = breakdown_map(revenue_data.get("totalDataChartBreakdown"))
    fees_breakdowns = breakdown_map(fees_data.get("totalDataChartBreakdown"))
    timestamps = sorted(set(revenue_chart) | set(fees_chart))
    chains = sorted({
        chain
        for ts in timestamps
        for source in (revenue_breakdowns.get(ts, {}), fees_breakdowns.get(ts, {}))
        for chain in source.keys()
    })

    rows = []
    for ts in timestamps:
        revenue = revenue_chart.get(ts, 0.0)
        fees = fees_chart.get(ts, 0.0)
        day = day_from_timestamp(ts)
        chain_rows = {}
        for chain in chains:
            chain_revenue = chain_value(revenue_breakdowns.get(ts, {}), chain)
            chain_fees = chain_value(fees_breakdowns.get(ts, {}), chain)
            if chain_revenue > 0 or chain_fees > 0:
                chain_rows[chain] = {
                    "feesUSD": round(chain_fees, 6),
                    "revenueUSD": round(chain_revenue, 6),
                    "supplySideRevenueUSD": round(max(chain_fees - chain_revenue, 0.0), 6),
                }

        rows.append({
            "timestamp": ts,
            "date": day,
            "feesUSD": round(fees, 6),
            "revenueUSD": round(revenue, 6),
            "supplySideRevenueUSD": round(max(fees - revenue, 0.0), 6),
            "protocolCut": round(revenue / fees, 8) if fees > 0 else 0,
            "chains": chain_rows,
        })
    return rows


def initialize_rebate_fields(series):
    for row in series:
        gross_revenue = safe_number(row.get("revenueUSD"))
        row["grossRevenueUSD"] = round(gross_revenue, 6)
        row["borrowFeeRebateUSD"] = 0.0
        row["grossProtocolCut"] = round(gross_revenue / safe_number(row.get("feesUSD")), 8) if safe_number(row.get("feesUSD")) > 0 else 0
        for payload in (row.get("chains") or {}).values():
            chain_gross = safe_number(payload.get("revenueUSD"))
            chain_fees = safe_number(payload.get("feesUSD"))
            payload["grossRevenueUSD"] = round(chain_gross, 6)
            payload["borrowFeeRebateUSD"] = 0.0
            payload["grossProtocolCut"] = round(chain_gross / chain_fees, 8) if chain_fees > 0 else 0


def recompute_revenue_row_from_chains(row):
    chains = row.get("chains") or {}
    fees = sum(safe_number(payload.get("feesUSD")) for payload in chains.values())
    revenue = sum(safe_number(payload.get("revenueUSD")) for payload in chains.values())
    supply_side = sum(safe_number(payload.get("supplySideRevenueUSD")) for payload in chains.values())
    row["feesUSD"] = round(fees, 6)
    row["revenueUSD"] = round(revenue, 6)
    row["supplySideRevenueUSD"] = round(supply_side, 6)
    row["protocolCut"] = round(revenue / fees, 8) if fees > 0 else 0
    return row


def usable_onchain_revenue_row(row):
    if not isinstance(row, dict):
        return False
    if str(row.get("status") or "").lower() not in {"pass", "warn"}:
        return False
    if row.get("error") or row.get("missingReasons"):
        return False
    if int(row.get("priceOmissionCount") or 0) > 0:
        return False
    return safe_number(row.get("feesUSD")) > 0 and safe_number(row.get("revenueUSD")) >= 0


def apply_onchain_revenue_overrides(series, onchain_audit, overrides_payload=None):
    """Prefer current-index onchain economics for chains where the audit is stronger than adapter parity."""
    overrides = normalized_onchain_revenue_overrides(overrides_payload, onchain_audit=onchain_audit)
    if not overrides:
        return series

    overrides_by_chain_date = {
        (row["chain"], row["date"]): row
        for row in overrides
        if row.get("chain") in ONCHAIN_REVENUE_OVERRIDE_CHAINS
    }
    for row in series:
        changed = False
        chain_rows = row.setdefault("chains", {})
        for chain in ONCHAIN_REVENUE_OVERRIDE_CHAINS:
            override = overrides_by_chain_date.get((chain, row.get("date")))
            if not override:
                continue
            payload = chain_rows.setdefault(chain, {})
            defillama_fees = safe_number(payload.get("feesUSD"))
            defillama_revenue = safe_number(payload.get("revenueUSD"))
            fees = safe_number(override.get("feesUSD"))
            revenue = safe_number(override.get("revenueUSD"))
            payload.update({
                "feesUSD": round(fees, 6),
                "revenueUSD": round(revenue, 6),
                "supplySideRevenueUSD": round(max(fees - revenue, 0.0), 6),
                "protocolCut": round(revenue / fees, 8) if fees > 0 else 0,
                "source": ONCHAIN_CURRENT_INDEX_SOURCE,
                "defillamaFeesUSD": round(safe_number(override.get("defillamaFeesUSD")) or defillama_fees, 6),
                "defillamaGrossRevenueUSD": round(safe_number(override.get("defillamaGrossRevenueUSD")) or defillama_revenue, 6),
                "onchainAuditTargetDate": override.get("date"),
                "onchainAuditGeneratedAt": override.get("auditGeneratedAt"),
            })
            changed = True
        if changed:
            recompute_revenue_row_from_chains(row)
    return series


def apply_epoch_rebate_to_chain(series, chain, epoch_rebate):
    rebate_usd, calculation_mode = validated_borrow_fee_rebate_epoch(epoch_rebate)
    target_units = rebate_usd_micro_units(rebate_usd)
    if target_units <= 0:
        return 0.0

    period_start = int_or_none(epoch_rebate.get("periodStartTimestamp"))
    period_end = int_or_none(epoch_rebate.get("periodEndTimestamp"))
    if period_start is None or period_end is None or period_end <= period_start:
        raise ValueError("positive borrow fee rebate epoch has an invalid period")

    rows = [
        row for row in series
        if period_start <= int(row.get("timestamp") or 0) < period_end
        and isinstance((row.get("chains") or {}).get(chain), dict)
    ]
    weights = [
        Decimal(str(max(safe_number(row["chains"][chain].get("feesUSD")), 0)))
        for row in rows
    ]
    if sum(weights, Decimal(0)) <= 0:
        weights = [
            Decimal(str(max(safe_number(
                row["chains"][chain].get("grossRevenueUSD", row["chains"][chain].get("revenueUSD"))
            ), 0)))
            for row in rows
        ]

    existing_units = []
    capacities = []
    for row in rows:
        payload = row["chains"][chain]
        gross_revenue = payload.get("grossRevenueUSD", payload.get("revenueUSD"))
        current_rebate = payload.get("borrowFeeRebateUSD", 0)
        if not is_finite_real_json_number(gross_revenue) or not is_finite_real_json_number(current_rebate):
            raise ValueError("daily borrow fee rebate capacity must use finite JSON numbers")
        gross_units = max(0, int(
            (Decimal(str(gross_revenue)) * BORROW_FEE_REBATE_USD_SCALE)
            .to_integral_value(rounding=ROUND_FLOOR)
        ))
        current_units = max(0, rebate_usd_micro_units(current_rebate))
        existing_units.append(current_units)
        capacities.append(max(gross_units - current_units, 0))

    allocations = allocate_weighted_rebate_micro_units(target_units, weights, capacities)
    for index, row in enumerate(rows):
        payload = row["chains"][chain]
        total_units = existing_units[index] + allocations[index]
        payload["borrowFeeRebateUSD"] = total_units / BORROW_FEE_REBATE_USD_SCALE
        if calculation_mode:
            payload["borrowFeeRebateCalculationMode"] = calculation_mode
    return target_units / BORROW_FEE_REBATE_USD_SCALE


def apply_borrow_fee_rebates(series, rebate_metadata):
    """Apply normalized rebate metadata; an empty object means no rebates."""
    if not isinstance(rebate_metadata, dict):
        raise ValueError("borrow fee rebate root must be an object")

    chains = rebate_metadata.get("chains", {})
    if not isinstance(chains, dict):
        raise ValueError("borrow fee rebate chains must be an object")
    epoch_rows = []
    for chain, chain_payload in chains.items():
        if not isinstance(chain_payload, dict):
            raise ValueError("borrow fee rebate chain payload must be an object")
        chain_epoch_rows = chain_payload.get("epochRebates")
        if not isinstance(chain_epoch_rows, list):
            raise ValueError("borrow fee rebate epochRebates must be a list")
        for epoch_rebate in chain_epoch_rows:
            validated_borrow_fee_rebate_epoch(epoch_rebate)
            epoch_rows.append((chain, epoch_rebate))

    initialize_rebate_fields(series)
    for chain, epoch_rebate in epoch_rows:
        apply_epoch_rebate_to_chain(series, chain, epoch_rebate)

    for row in series:
        total_rebate_units = 0
        calculation_modes = set()
        for payload in (row.get("chains") or {}).values():
            chain_fees = safe_number(payload.get("feesUSD"))
            chain_gross = safe_number(payload.get("grossRevenueUSD", payload.get("revenueUSD")))
            chain_gross_units = rebate_usd_micro_units(chain_gross)
            chain_rebate_units = rebate_usd_micro_units(payload.get("borrowFeeRebateUSD"))
            if chain_rebate_units < 0 or chain_rebate_units > chain_gross_units:
                raise ValueError("borrow fee rebate exceeds chain gross revenue capacity")
            chain_rebate = chain_rebate_units / BORROW_FEE_REBATE_USD_SCALE
            chain_net = (
                chain_gross_units - chain_rebate_units
            ) / BORROW_FEE_REBATE_USD_SCALE
            payload["borrowFeeRebateUSD"] = chain_rebate
            payload["revenueUSD"] = chain_net
            payload["supplySideRevenueUSD"] = round(max(chain_fees - chain_gross, 0.0), 6)
            payload["protocolCut"] = round(chain_net / chain_fees, 8) if chain_fees > 0 else 0
            payload["grossProtocolCut"] = round(chain_gross / chain_fees, 8) if chain_fees > 0 else 0
            total_rebate_units += chain_rebate_units
            calculation_mode = str(payload.get("borrowFeeRebateCalculationMode") or "").strip()
            if calculation_mode:
                calculation_modes.add(calculation_mode)

        fees = safe_number(row.get("feesUSD"))
        gross_revenue = safe_number(row.get("grossRevenueUSD", row.get("revenueUSD")))
        gross_revenue_units = rebate_usd_micro_units(gross_revenue)
        if total_rebate_units > gross_revenue_units:
            raise ValueError("borrow fee rebate exceeds top-level gross revenue capacity")
        total_rebate = total_rebate_units / BORROW_FEE_REBATE_USD_SCALE
        net_revenue = (
            gross_revenue_units - total_rebate_units
        ) / BORROW_FEE_REBATE_USD_SCALE
        row["borrowFeeRebateUSD"] = total_rebate
        row["revenueUSD"] = net_revenue
        row["supplySideRevenueUSD"] = round(max(fees - gross_revenue, 0.0), 6)
        row["protocolCut"] = round(net_revenue / fees, 8) if fees > 0 else 0
        row["grossProtocolCut"] = round(gross_revenue / fees, 8) if fees > 0 else 0
        if len(calculation_modes) == 1:
            row["borrowFeeRebateCalculationMode"] = next(iter(calculation_modes))
        else:
            row.pop("borrowFeeRebateCalculationMode", None)
    return series


def window_chain_totals(series, days):
    rows = series[-days:] if days > 0 else series
    totals = {}
    for row in rows:
        for chain, payload in row.get("chains", {}).items():
            entry = totals.setdefault(chain, {
                "feesUSD": 0.0,
                "grossRevenueUSD": 0.0,
                "borrowFeeRebateUSD": 0.0,
                "revenueUSD": 0.0,
                "supplySideRevenueUSD": 0.0,
            })
            entry["feesUSD"] += safe_number(payload.get("feesUSD"))
            entry["grossRevenueUSD"] += safe_number(payload.get("grossRevenueUSD", payload.get("revenueUSD")))
            entry["borrowFeeRebateUSD"] += safe_number(payload.get("borrowFeeRebateUSD"))
            entry["revenueUSD"] += safe_number(payload.get("revenueUSD"))
            entry["supplySideRevenueUSD"] += safe_number(payload.get("supplySideRevenueUSD"))
    return {
        chain: {key: round(value, 6) for key, value in values.items()}
        for chain, values in sorted(totals.items(), key=lambda item: item[1]["revenueUSD"], reverse=True)
    }


def window_sum(series, days, key):
    rows = series[-days:] if days > 0 else series
    return sum(safe_number(row.get(key)) for row in rows)


def latest_series_value(series, key, fallback):
    latest = series[-1] if series else {}
    value = latest.get(key)
    if isinstance(value, bool):
        return safe_number(fallback)
    if isinstance(value, (int, float)):
        return safe_number(value)
    return safe_number(fallback)


def load_onchain_audit(path=ONCHAIN_AUDIT_FILE):
    try:
        with open(path) as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def load_existing_revenue_output(path=OUTPUT_FILE):
    try:
        with open(path) as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def borrow_fee_rebate_epoch_rows(payload, chain=BERACHAIN_CHAIN_NAME):
    chains = payload.get("chains") if isinstance(payload, dict) else None
    chain_payload = chains.get(chain) if isinstance(chains, dict) else None
    rows = chain_payload.get("epochRebates") if isinstance(chain_payload, dict) else None
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def borrow_fee_rebate_epoch_total(payload, chain=BERACHAIN_CHAIN_NAME):
    return round(sum(safe_number(row.get("rebateUSD")) for row in borrow_fee_rebate_epoch_rows(payload, chain)), 6)


def previous_borrow_fee_rebate_data(previous_output):
    rebates = previous_output.get("borrowFeeRebates") if isinstance(previous_output, dict) else None
    if not isinstance(rebates, dict) or borrow_fee_rebate_epoch_total(rebates) <= 0:
        return None

    chain_payload = (rebates.get("chains") or {}).get(BERACHAIN_CHAIN_NAME)
    if not isinstance(chain_payload, dict):
        return None

    epoch_rebates = json.loads(json.dumps(borrow_fee_rebate_epoch_rows(rebates)))
    previous_generated_at = previous_output.get("generatedAt") or rebates.get("dataGeneratedAt")
    fallback_chain = {
        "status": "fallback_previous_closed_epochs",
        "source": chain_payload.get("source") or FEE_REBATE_EVENT_SOURCE,
        "chainId": int(BERACHAIN_CHAIN_ID),
        "feeRebateClaimer": chain_payload.get("feeRebateClaimer") or FEE_REBATE_CLAIMER_ADDRESS,
        "feeRebateRollingClaims": chain_payload.get("feeRebateRollingClaims") or FEE_REBATE_ROLLING_CLAIMS_ADDRESS,
        "eventCount": int(chain_payload.get("eventCount") or 0),
        "pricedEventCount": int(chain_payload.get("pricedEventCount") or 0),
        "missingPriceCount": int(chain_payload.get("missingPriceCount") or 0),
        "priceFallbackCount": int(chain_payload.get("priceFallbackCount") or 0),
        "latestRebateDate": chain_payload.get("latestRebateDate"),
        "epochRebates": epoch_rebates,
        "unsupportedCorrections": json.loads(json.dumps(chain_payload.get("unsupportedCorrections") or [])),
        "fallbackFromGeneratedAt": previous_generated_at,
    }
    return {
        "status": "fallback_previous_closed_epochs",
        "source": fallback_chain["source"],
        "generatedAt": rebates.get("dataGeneratedAt") or previous_generated_at,
        "fallbackFromGeneratedAt": previous_generated_at,
        "fallbackReason": "current_rebate_fetch_has_no_closed_epochs",
        "chains": {BERACHAIN_CHAIN_NAME: fallback_chain},
    }


PRESERVED_BORROW_FEE_REBATE_EPOCH_FIELDS = (
    "calculationMode",
    "sourceLabel",
    "transactionHash",
    "resetMarketCount",
    "aggregateAdjustmentRaw",
    "maxRebateUSD",
    "maxRebateMethod",
    "maxRebateSource",
    "maxRebateMarketCount",
    "maxRebateEligibleMarketIds",
    "maxRebateDayCount",
    "maxRebatePriceFallbackCount",
    "maxRebatePriceOmissionCount",
    "maxRebateGeneratedAt",
)


def rebate_epoch_identity(row):
    return (
        int_or_none(row.get("epoch")),
        int_or_none(row.get("periodStartTimestamp")),
        int_or_none(row.get("periodEndTimestamp")),
    )


def preserve_previous_borrow_fee_rebate_epoch_audits(current_rebate_data, previous_output):
    if borrow_fee_rebate_epoch_total(current_rebate_data) <= 0:
        return current_rebate_data
    previous_rebates = previous_output.get("borrowFeeRebates") if isinstance(previous_output, dict) else None
    if not isinstance(previous_rebates, dict):
        return current_rebate_data

    current = json.loads(json.dumps(current_rebate_data))
    current_chains = current.get("chains") if isinstance(current.get("chains"), dict) else {}
    previous_chains = previous_rebates.get("chains") if isinstance(previous_rebates.get("chains"), dict) else {}
    for chain, chain_payload in current_chains.items():
        previous_payload = previous_chains.get(chain)
        if not isinstance(chain_payload, dict) or not isinstance(previous_payload, dict):
            continue
        previous_by_epoch = {
            rebate_epoch_identity(row): row
            for row in borrow_fee_rebate_epoch_rows({"chains": {chain: previous_payload}}, chain)
        }
        for row in chain_payload.get("epochRebates") or []:
            if not isinstance(row, dict):
                continue
            previous_row = previous_by_epoch.get(rebate_epoch_identity(row))
            if not previous_row:
                continue
            for field in PRESERVED_BORROW_FEE_REBATE_EPOCH_FIELDS:
                if field not in row and field in previous_row:
                    row[field] = previous_row[field]
    return current


def preserve_previous_borrow_fee_rebate_data(current_rebate_data, previous_output):
    if borrow_fee_rebate_epoch_total(current_rebate_data) > 0:
        return preserve_previous_borrow_fee_rebate_epoch_audits(current_rebate_data, previous_output)

    fallback = previous_borrow_fee_rebate_data(previous_output)
    if not fallback:
        return current_rebate_data

    current_payload = current_rebate_data if isinstance(current_rebate_data, dict) else {}
    current_chain = (current_payload.get("chains") or {}).get(BERACHAIN_CHAIN_NAME, {})
    fallback["fallbackCurrentStatus"] = current_payload.get("status")
    fallback["fallbackCurrentError"] = current_payload.get("error")
    fallback["chains"][BERACHAIN_CHAIN_NAME]["fallbackCurrentStatus"] = current_chain.get("status")
    fallback["chains"][BERACHAIN_CHAIN_NAME]["fallbackCurrentError"] = current_chain.get("error")
    return fallback


def borrow_fee_rebate_chain_info(metadata, chain_id=BERACHAIN_CHAIN_ID):
    if not isinstance(metadata, dict):
        return {}
    all_chain_info = metadata.get("allChainRebateInfo")
    if not isinstance(all_chain_info, dict):
        return {}
    chain_info = all_chain_info.get(str(chain_id))
    if chain_info is None and str(chain_id).isdigit():
        chain_info = all_chain_info.get(int(chain_id))
    return chain_info if isinstance(chain_info, dict) else {}


def borrow_fee_rebate_percentage_from_metadata(metadata, chain_id=BERACHAIN_CHAIN_ID):
    chain_info = borrow_fee_rebate_chain_info(metadata, chain_id=chain_id)
    rebate_percentage = decimal_from_value(chain_info.get("rebatePercentage"))
    return rebate_percentage if rebate_percentage > 0 else Decimal("0.10")


def active_rebate_market_ids_for_epoch(metadata, epoch, chain_id=BERACHAIN_CHAIN_ID):
    chain_info = borrow_fee_rebate_chain_info(metadata, chain_id=chain_id)
    market_info = chain_info.get("marketToRebateInfo") if isinstance(chain_info.get("marketToRebateInfo"), dict) else {}
    epoch = int_or_none(epoch)
    if epoch is None:
        return []

    market_ids = []
    for market_id, market_payload in market_info.items():
        if not isinstance(market_payload, dict):
            continue
        parsed_market_id = int_or_none(market_id)
        if parsed_market_id is None:
            continue
        start_epoch = int_or_none(market_payload.get("startEpoch")) or 0
        end_value = market_payload.get("endEpoch")
        end_epoch = None if end_value is None else int_or_none(end_value)
        if epoch >= start_epoch and (end_epoch is None or epoch <= end_epoch):
            market_ids.append(parsed_market_id)
    return sorted(set(market_ids))


def public_berachain_rebate_audit_delay(endpoint):
    host = rpc_client.safe_host(endpoint)
    if host in {"rpc.berachain.com", "berachain-rpc.publicnode.com", "berachain.drpc.org"}:
        try:
            return max(0.0, float(os.environ.get(
                "BORROW_FEE_REBATE_MAX_AUDIT_PUBLIC_DAY_DELAY_SECONDS",
                BORROW_FEE_REBATE_MAX_AUDIT_PUBLIC_DAY_DELAY_SECONDS,
            )))
        except (TypeError, ValueError):
            return BORROW_FEE_REBATE_MAX_AUDIT_PUBLIC_DAY_DELAY_SECONDS
    return 0.0


def borrow_fee_rebate_epoch_has_current_max_audit(row):
    return (
        isinstance(row, dict)
        and safe_number(row.get("rebateUSD")) > 0
        and safe_number(row.get("maxRebateUSD")) > 0
        and row.get("maxRebateMethod") == BORROW_FEE_REBATE_MAX_REBATE_METHOD
        and row.get("maxRebateSource") == ONCHAIN_CURRENT_INDEX_SOURCE
    )


def abi_output_type(component):
    typ = component.get("type")
    if str(typ).startswith("tuple"):
        suffix = str(typ)[len("tuple"):]
        return "(" + ",".join(abi_output_type(item) for item in component.get("components", [])) + ")" + suffix
    return typ


def function_output_types(function):
    return [abi_output_type(item) for item in function.abi.get("outputs", [])]


def decode_contract_call_result(w3, function, result):
    raw = hex_to_bytes(result or "0x")
    decoded = w3.codec.decode(function_output_types(function), raw)
    return decoded[0] if len(decoded) == 1 else decoded


def batch_contract_calls(w3, contract, calls):
    requests_payload = []
    functions = []
    for function, block_identifier in calls:
        requests_payload.append((
            "eth_call",
            [
                {"to": contract.address, "data": function._encode_transaction_data()},
                hex(int(block_identifier)) if isinstance(block_identifier, int) else block_identifier,
            ],
        ))
        functions.append(function)
    responses = w3.provider.make_batch_request(requests_payload)
    if len(responses) != len(functions):
        raise RuntimeError(f"JSON-RPC batch returned {len(responses)} response(s) for {len(functions)} request(s)")
    results = []
    for function, response in zip(functions, responses):
        if not isinstance(response, dict):
            raise RuntimeError("invalid JSON-RPC batch response")
        if response.get("error"):
            raise RuntimeError(str(response.get("error")))
        results.append(decode_contract_call_result(w3, function, response.get("result")))
    return results


def batched_revenue_audit_market_interest_rows(w3, config, from_block, to_block, market_ids, token_cache=None):
    if config.get("mode") != "standard":
        return revenue_audit_market_interest_rows(
            w3,
            config,
            from_block,
            to_block,
            endpoint_started_at=None,
            market_ids=market_ids,
        )

    margin = Web3.to_checksum_address(config["margin"])
    contract = w3.eth.contract(address=margin, abi=REVENUE_AUDIT_MARGIN_ABI)
    selected_market_ids = sorted({int(market_id) for market_id in market_ids})
    calls = [(contract.functions.getEarningsRate(), from_block)]
    for market_id in selected_market_ids:
        calls.extend((
            (contract.functions.getMarket(market_id), from_block),
            (contract.functions.getMarketCurrentIndex(market_id), from_block),
            (contract.functions.getMarketCurrentIndex(market_id), to_block),
        ))
    results = batch_contract_calls(w3, contract, calls)
    default_earnings_rate = int(results[0])
    rows = []
    offset = 1
    token_cache = token_cache if isinstance(token_cache, dict) else {}
    one = Decimal(10) ** 18
    for market_id in selected_market_ids:
        market = results[offset]
        start_index = results[offset + 1]
        end_index = results[offset + 2]
        offset += 3

        token = market[0]
        total_par = market[2]
        borrow_par = int(total_par[0])
        start_borrow_index = int(start_index[0])
        end_borrow_index = int(end_index[0])
        override = int(market[10][0]) if len(market) > 10 else 0
        earnings_rate = override if override else default_earnings_rate
        interest_raw = (end_borrow_index - start_borrow_index) * borrow_par // 10**18
        if interest_raw <= 0:
            continue

        checksum_token = Web3.to_checksum_address(token)
        if checksum_token not in token_cache:
            symbol, decimals = token_metadata(w3, checksum_token)
            token_cache[checksum_token] = (symbol, decimals)
        symbol, decimals = token_cache[checksum_token]
        amount = Decimal(interest_raw) / (Decimal(10) ** decimals)
        protocol_fraction = (one - Decimal(earnings_rate)) / one
        rows.append({
            "marketId": market_id,
            "token": checksum_token,
            "symbol": symbol,
            "decimals": decimals,
            "borrowInterestRaw": str(interest_raw),
            "borrowInterestAmount": amount,
            "protocolRevenueAmount": amount * protocol_fraction,
            "protocolCut": protocol_fraction,
        })
    return rows


def compute_daily_borrow_fee_max_rebate(w3, config, day_start, day_end, market_ids, rebate_percentage, block_cache, token_cache=None):
    from_block = block_cache.get(day_start)
    if from_block is None:
        from_block = find_revenue_audit_block_at_or_before(w3, day_start)
        block_cache[day_start] = from_block
    to_block = block_cache.get(day_end)
    if to_block is None:
        to_block = find_revenue_audit_block_at_or_before(w3, day_end)
        block_cache[day_end] = to_block

    rows = batched_revenue_audit_market_interest_rows(
        w3,
        config,
        from_block,
        to_block,
        market_ids,
        token_cache=token_cache,
    )
    coin_ids = [f"{config['coinChain']}:{row['token'].lower()}" for row in rows]
    prices = fetch_revenue_audit_historical_prices(day_end, sorted(set(coin_ids)))
    fee_usd = Decimal(0)
    revenue_usd = Decimal(0)
    missing_price_count = 0
    price_fallback_count = 0
    price_omission_count = 0

    for row, coin_id in zip(rows, coin_ids):
        price, price_source = resolve_revenue_audit_token_price(row, prices.get(coin_id))
        if price is None:
            missing_price_count += 1
            continue
        fee_usd += row["borrowInterestAmount"] * price
        revenue_usd += row["protocolRevenueAmount"] * price
        if price_source == REVENUE_AUDIT_OMITTED_PRICE_SOURCE:
            price_omission_count += 1
        elif price_source != "coins-llama":
            price_fallback_count += 1

    if missing_price_count:
        raise RuntimeError(f"missing historical prices for {missing_price_count} eligible rebate token(s)")

    expected_protocol_revenue = fee_usd * BORROW_FEE_REBATE_PROTOCOL_RESERVE_FACTOR
    if expected_protocol_revenue > 0:
        revenue_factor = max(Decimal(0), min(Decimal(1), revenue_usd / expected_protocol_revenue))
    else:
        revenue_factor = Decimal(1)
    return {
        "maxRebateUSD": fee_usd * rebate_percentage * revenue_factor,
        "feeUSD": fee_usd,
        "revenueUSD": revenue_usd,
        "priceFallbackCount": price_fallback_count,
        "priceOmissionCount": price_omission_count,
    }


def audit_borrow_fee_rebate_max_rebates(rebate_metadata, rebate_data):
    if borrow_fee_rebate_epoch_total(rebate_data) <= 0:
        return rebate_data
    if not isinstance(rebate_metadata, dict):
        return rebate_data

    current = json.loads(json.dumps(rebate_data))
    chain_payload = ((current.get("chains") or {}).get(BERACHAIN_CHAIN_NAME))
    if not isinstance(chain_payload, dict):
        return current
    epoch_rows = chain_payload.get("epochRebates")
    if not isinstance(epoch_rows, list):
        return current

    rows_needing_audit = [
        row for row in epoch_rows
        if isinstance(row, dict)
        and safe_number(row.get("rebateUSD")) > 0
        and not borrow_fee_rebate_epoch_has_current_max_audit(row)
    ]
    if not rows_needing_audit:
        return current

    config = REVENUE_AUDIT_CHAIN_CONFIGS["berachain"]
    rebate_percentage = borrow_fee_rebate_percentage_from_metadata(rebate_metadata)
    try:
        endpoints = rpc_client.get_endpoints("berachain")
    except Exception as exc:
        print(f"   Borrow fee rebate max audit skipped: {type(exc).__name__}: {rpc_client.sanitize_error(exc)}")
        return current

    last_error = None
    for endpoint in endpoints:
        try:
            print(f"   Auditing borrow fee rebate max values on {rpc_client.safe_host(endpoint)}")
            w3 = make_revenue_audit_web3(endpoint, config)
            block_cache = {}
            token_cache = {}
            public_day_delay = public_berachain_rebate_audit_delay(endpoint)
            generated_at = utc_now_iso()
            for row in rows_needing_audit:
                epoch = int_or_none(row.get("epoch"))
                period_start = int_or_none(row.get("periodStartTimestamp"))
                period_end = int_or_none(row.get("periodEndTimestamp"))
                market_ids = active_rebate_market_ids_for_epoch(rebate_metadata, epoch)
                if epoch is None or period_start is None or period_end is None or period_end <= period_start or not market_ids:
                    continue

                day = period_start
                day_count = 0
                max_rebate_usd = Decimal(0)
                price_fallback_count = 0
                price_omission_count = 0
                while day < period_end:
                    day_end = min(day + SECONDS_PER_DAY, period_end)
                    daily = None
                    for attempt in range(2):
                        try:
                            daily = compute_daily_borrow_fee_max_rebate(
                                w3,
                                config,
                                day,
                                day_end,
                                market_ids,
                                rebate_percentage,
                                block_cache,
                                token_cache=token_cache,
                            )
                            break
                        except RuntimeError as exc:
                            if attempt == 0 and "250/minute request limit" in str(exc):
                                print("      Public RPC rate limit reached; waiting 70s before retry", flush=True)
                                time.sleep(70)
                                continue
                            raise
                    max_rebate_usd += daily["maxRebateUSD"]
                    price_fallback_count += daily["priceFallbackCount"]
                    price_omission_count += daily["priceOmissionCount"]
                    day_count += 1
                    day = day_end
                    if public_day_delay > 0 and day < period_end:
                        time.sleep(public_day_delay)

                row["maxRebateUSD"] = round(float(max_rebate_usd), 6)
                row["maxRebateMethod"] = BORROW_FEE_REBATE_MAX_REBATE_METHOD
                row["maxRebateSource"] = ONCHAIN_CURRENT_INDEX_SOURCE
                row["maxRebateMarketCount"] = len(market_ids)
                row["maxRebateEligibleMarketIds"] = [str(market_id) for market_id in market_ids]
                row["maxRebateDayCount"] = day_count
                row["maxRebatePriceFallbackCount"] = price_fallback_count
                row["maxRebatePriceOmissionCount"] = price_omission_count
                row["maxRebateGeneratedAt"] = generated_at
                print(
                    f"      Epoch {epoch}: audited max rebate ${row['maxRebateUSD']:,.2f} "
                    f"across {day_count} day(s) and {len(market_ids)} market(s)",
                    flush=True,
                )
            return current
        except Exception as exc:
            last_error = exc
            print(f"   Borrow fee rebate max audit failed on {rpc_client.safe_host(endpoint)}: {type(exc).__name__}: {rpc_client.sanitize_error(exc)}")

    if last_error:
        print(f"   Borrow fee rebate max audit unavailable: {type(last_error).__name__}: {rpc_client.sanitize_error(last_error)}")
    return current


def load_onchain_revenue_overrides(path=ONCHAIN_REVENUE_OVERRIDES_FILE):
    try:
        with open(path) as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return {"schemaVersion": 1, "overrides": []}
    return payload if isinstance(payload, dict) else {"schemaVersion": 1, "overrides": []}


def onchain_audit_to_revenue_override(onchain_audit):
    if not isinstance(onchain_audit, dict):
        return []
    target_date = onchain_audit.get("targetDate")
    if not target_date:
        return []
    current = []
    chains = onchain_audit.get("chains") or {}
    for chain in sorted(ONCHAIN_REVENUE_OVERRIDE_CHAINS):
        chain_audit = chains.get(chain)
        if not usable_onchain_revenue_row(chain_audit):
            continue
        current.append({
            "chain": chain,
            "date": target_date,
            "feesUSD": round(safe_number(chain_audit.get("feesUSD")), 6),
            "revenueUSD": round(safe_number(chain_audit.get("revenueUSD")), 6),
            "defillamaFeesUSD": round(safe_number(chain_audit.get("defillamaFeesUSD")), 6),
            "defillamaGrossRevenueUSD": round(safe_number(chain_audit.get("defillamaRevenueUSD")), 6),
            "source": ONCHAIN_CURRENT_INDEX_SOURCE,
            "auditGeneratedAt": onchain_audit.get("generatedAt"),
            "priceFallbackCount": int(chain_audit.get("priceFallbackCount") or 0),
            "rawTokenCount": int(chain_audit.get("rawTokenCount") or 0),
        })
    return current


def normalized_onchain_revenue_overrides(overrides_payload, onchain_audit=None):
    by_key = {}
    rows = []
    if isinstance(overrides_payload, dict):
        rows = overrides_payload.get("overrides") or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        chain = row.get("chain")
        if chain not in ONCHAIN_REVENUE_OVERRIDE_CHAINS or row.get("source") != ONCHAIN_CURRENT_INDEX_SOURCE:
            continue
        date = row.get("date")
        if not date or safe_number(row.get("feesUSD")) <= 0:
            continue
        by_key[(chain, date)] = {
            "chain": chain,
            "date": date,
            "feesUSD": round(safe_number(row.get("feesUSD")), 6),
            "revenueUSD": round(safe_number(row.get("revenueUSD")), 6),
            "defillamaFeesUSD": round(safe_number(row.get("defillamaFeesUSD")), 6),
            "defillamaGrossRevenueUSD": round(safe_number(row.get("defillamaGrossRevenueUSD")), 6),
            "source": ONCHAIN_CURRENT_INDEX_SOURCE,
            "auditGeneratedAt": row.get("auditGeneratedAt"),
            "priceFallbackCount": int(row.get("priceFallbackCount") or 0),
            "rawTokenCount": int(row.get("rawTokenCount") or 0),
        }

    for current in onchain_audit_to_revenue_override(onchain_audit):
        by_key[(current["chain"], current["date"])] = current
    return [by_key[key] for key in sorted(by_key, key=lambda item: (item[1], item[0]))]


def merge_onchain_revenue_override_history(overrides_payload, onchain_audit):
    overrides = normalized_onchain_revenue_overrides(overrides_payload, onchain_audit=onchain_audit)
    return {
        "schemaVersion": 1,
        "generatedAt": utc_now_iso(),
        "source": "current-index onchain revenue audit overrides",
        "overrides": overrides,
    }


def write_onchain_revenue_overrides(payload, path=ONCHAIN_REVENUE_OVERRIDES_FILE):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))


def expected_onchain_audit_target_date(now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    utc_now = now.astimezone(timezone.utc)
    lag_days = 2 if utc_now.hour >= ONCHAIN_AUDIT_EXPECTED_TARGET_AFTER_UTC_HOUR else 3
    return (utc_now.date() - timedelta(days=lag_days)).isoformat()


def date_is_before(left, right):
    try:
        left_date = datetime.fromisoformat(str(left)).date()
        right_date = datetime.fromisoformat(str(right)).date()
    except (TypeError, ValueError):
        return False
    return left_date < right_date


def summarize_onchain_audit_chains(onchain_audit):
    chains = onchain_audit.get("chains") if isinstance(onchain_audit, dict) else None
    if not isinstance(chains, dict):
        return {}

    fields = (
        "status",
        "feesUSD",
        "revenueUSD",
        "defillamaFeesUSD",
        "defillamaRevenueUSD",
        "feesDiffPct",
        "revenueDiffPct",
        "feesDiffUnbounded",
        "revenueDiffUnbounded",
        "protocolCut",
        "defillamaProtocolCut",
        "protocolCutDiff",
        "warnReasons",
        "infoReasons",
        "missingReasons",
        "defillamaChainMissing",
        "priceFallbackCount",
        "priceOmissionCount",
        "rawTokenCount",
        "error",
    )
    summarized = {}
    for chain, payload in sorted(chains.items()):
        if not isinstance(payload, dict):
            continue
        summarized[str(chain)] = {field: payload[field] for field in fields if field in payload}
    return summarized


def int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalized_borrow_fee_rebate_metadata(metadata, rebate_data=None):
    if not isinstance(metadata, dict):
        return {
            "status": "not_available",
            "netting": "not_netted",
            "source": BORROW_FEE_REBATE_METADATA_URL,
            "chains": {},
        }

    all_chain_info = metadata.get("allChainRebateInfo") or {}
    berachain_info = all_chain_info.get(BERACHAIN_CHAIN_ID)
    rebate_data = rebate_data if isinstance(rebate_data, dict) else {}
    rebate_data_chains = rebate_data.get("chains") if isinstance(rebate_data.get("chains"), dict) else {}
    chains = {}
    if isinstance(berachain_info, dict):
        market_info = berachain_info.get("marketToRebateInfo") or {}
        rebate_percentage = round(safe_decimal_number(berachain_info.get("rebatePercentage")), 8)
        rebate_chain_data = rebate_data_chains.get(BERACHAIN_CHAIN_NAME) if isinstance(rebate_data_chains.get(BERACHAIN_CHAIN_NAME), dict) else {}
        epoch_rebates = rebate_chain_data.get("epochRebates", [])
        if not isinstance(epoch_rebates, list):
            raise ValueError("borrow fee rebate epochRebates must be a list")
        total_rebate_usd = round(sum(safe_number(item.get("rebateUSD")) for item in epoch_rebates if isinstance(item, dict)), 6)
        missing_price_count = int(rebate_chain_data.get("missingPriceCount") or 0)
        if total_rebate_usd > 0 and missing_price_count:
            netting_status = "partial"
        elif total_rebate_usd > 0:
            netting_status = "netted_closed_epochs"
        elif rebate_chain_data.get("status") == "missing":
            netting_status = "rebate_data_unavailable"
        else:
            netting_status = "no_closed_epoch_rebates"
        chains[BERACHAIN_CHAIN_NAME] = {
            "chainId": int(BERACHAIN_CHAIN_ID),
            "status": "active" if rebate_percentage > 0 else "inactive",
            "startEpoch": int_or_none(berachain_info.get("startEpoch")),
            "claimsEnabled": bool(berachain_info.get("claimsEnabled")),
            "rebatePercentage": rebate_percentage,
            "marketCount": len(market_info) if isinstance(market_info, dict) else 0,
            "onchainFeeRebateEpoch": int_or_none((metadata.get("onchainFeeRebateEpochIndexMap") or {}).get(BERACHAIN_CHAIN_ID)),
            "onchainRollingClaimsEpoch": int_or_none((metadata.get("onchainRollingClaimsEpochIndexMap") or {}).get(BERACHAIN_CHAIN_ID)),
            "nettingStatus": netting_status,
            "source": rebate_chain_data.get("source") or FEE_REBATE_EVENT_SOURCE,
            "feeRebateClaimer": rebate_chain_data.get("feeRebateClaimer") or FEE_REBATE_CLAIMER_ADDRESS,
            "feeRebateRollingClaims": rebate_chain_data.get("feeRebateRollingClaims") or FEE_REBATE_ROLLING_CLAIMS_ADDRESS,
            "totalRebateUSD": total_rebate_usd,
            "latestRebateDate": rebate_chain_data.get("latestRebateDate"),
            "eventCount": int(rebate_chain_data.get("eventCount") or 0),
            "pricedEventCount": int(rebate_chain_data.get("pricedEventCount") or 0),
            "missingPriceCount": missing_price_count,
            "priceFallbackCount": int(rebate_chain_data.get("priceFallbackCount") or 0),
            "epochRebates": epoch_rebates,
            "unsupportedCorrections": rebate_chain_data.get("unsupportedCorrections")
            if isinstance(rebate_chain_data.get("unsupportedCorrections"), list)
            else [],
        }

    status = "active" if any(chain.get("status") == "active" for chain in chains.values()) else "inactive"
    netting = "not_netted"
    if any(chain.get("nettingStatus") == "netted_closed_epochs" for chain in chains.values()):
        netting = "netted_closed_epochs"
    elif any(chain.get("nettingStatus") == "partial" for chain in chains.values()):
        netting = "partial"
    elif any(chain.get("nettingStatus") == "rebate_data_unavailable" for chain in chains.values()):
        netting = "rebate_data_unavailable"
    return {
        "status": status,
        "netting": netting,
        "source": BORROW_FEE_REBATE_METADATA_URL,
        "docs": BORROW_FEE_REBATE_DOCS_URL,
        "eventSource": FEE_REBATE_EVENT_SOURCE,
        "dataStatus": rebate_data.get("status") or "not_run",
        "dataGeneratedAt": rebate_data.get("generatedAt"),
        "veDoloStartTimestamp": int_or_none(metadata.get("veDoloStartTimestamp")),
        "veDoloHoldingFactor": round(safe_decimal_number(metadata.get("veDoloHoldingFactor")), 8),
        "currentEpochIndex": int_or_none(metadata.get("currentEpochIndex")),
        "currentEpochStartTimestamp": int_or_none(metadata.get("currentEpochStartTimestamp")),
        "chains": chains,
    }


def borrow_fee_rebate_status(rebate_metadata):
    if rebate_metadata.get("status") == "active":
        netting = rebate_metadata.get("netting")
        if netting == "netted_closed_epochs":
            return "active_netted_closed_epochs"
        if netting == "partial":
            return "active_partial_rebate_netting"
        if netting == "rebate_data_unavailable":
            return "active_rebate_data_unavailable"
        return "active_no_closed_epoch_rebates"
    if rebate_metadata.get("status") == "inactive":
        return "inactive"
    return "not_available"


def onchain_audit_assurance(onchain_audit, now=None):
    expected_target = expected_onchain_audit_target_date(now)
    if not onchain_audit:
        return {
            "onchainAuditStatus": "not_run",
            "onchainAuditRawStatus": "not_run",
            "onchainAuditTargetDate": None,
            "onchainAuditExpectedTargetDate": expected_target,
            "onchainAuditExpectedTargetAfterUtcHour": ONCHAIN_AUDIT_EXPECTED_TARGET_AFTER_UTC_HOUR,
            "onchainAuditStale": False,
            "onchainAuditMaxRevenueDiffPct": None,
            "onchainAuditMaxFeesDiffPct": None,
            "onchainAuditRevenueDiffUnbounded": False,
            "onchainAuditFeesDiffUnbounded": False,
            "onchainAuditChains": {},
        }
    summary = onchain_audit.get("summary") or {}
    raw_status = str(onchain_audit.get("status") or "missing")
    target_date = onchain_audit.get("targetDate")
    stale = date_is_before(target_date, expected_target)
    return {
        "onchainAuditStatus": "stale" if stale else raw_status,
        "onchainAuditRawStatus": raw_status,
        "onchainAuditTargetDate": target_date,
        "onchainAuditExpectedTargetDate": expected_target,
        "onchainAuditExpectedTargetAfterUtcHour": ONCHAIN_AUDIT_EXPECTED_TARGET_AFTER_UTC_HOUR,
        "onchainAuditStale": stale,
        "onchainAuditMaxRevenueDiffPct": summary.get("maxRevenueDiffPct"),
        "onchainAuditMaxFeesDiffPct": summary.get("maxFeesDiffPct"),
        "onchainAuditRevenueDiffUnbounded": bool(summary.get("revenueDiffUnbounded")),
        "onchainAuditFeesDiffUnbounded": bool(summary.get("feesDiffUnbounded")),
        "onchainAuditGeneratedAt": onchain_audit.get("generatedAt"),
        "onchainAuditChains": summarize_onchain_audit_chains(onchain_audit),
    }


def metric_totals(revenue_data, fees_data, series):
    # DeFiLlama aggregate windows can briefly lag or revise while the chart rows
    # are updating. Keep every displayed total tied to the same saved series that
    # powers the chart and chain breakdowns.
    fees_24h = latest_series_value(series, "feesUSD", fees_data.get("total24h"))
    gross_revenue_24h = latest_series_value(series, "grossRevenueUSD", revenue_data.get("total24h"))
    rebate_24h = latest_series_value(series, "borrowFeeRebateUSD", 0)
    revenue_24h = latest_series_value(series, "revenueUSD", revenue_data.get("total24h"))
    previous = series[-2] if len(series) >= 2 else {}
    previous_revenue = previous.get("revenueUSD", revenue_data.get("total48hto24h"))
    previous_gross_revenue = previous.get("grossRevenueUSD", revenue_data.get("total48hto24h"))
    previous_rebate = previous.get("borrowFeeRebateUSD", 0)
    previous_fees = previous.get("feesUSD", fees_data.get("total48hto24h"))
    gross_revenue_all_time = safe_number(revenue_data.get("totalAllTime"))
    rebate_all_time = window_sum(series, 0, "borrowFeeRebateUSD")
    return {
        "dailyRevenueUSD": round(revenue_24h, 6),
        "dailyGrossRevenueUSD": round(gross_revenue_24h, 6),
        "dailyBorrowFeeRebateUSD": round(rebate_24h, 6),
        "dailyFeesUSD": round(fees_24h, 6),
        "dailySupplySideRevenueUSD": round(max(fees_24h - gross_revenue_24h, 0.0), 6),
        "dailyProtocolCut": round(revenue_24h / fees_24h, 8) if fees_24h > 0 else 0,
        "dailyGrossProtocolCut": round(gross_revenue_24h / fees_24h, 8) if fees_24h > 0 else 0,
        "previousDailyRevenueUSD": round(safe_number(previous_revenue), 6),
        "previousDailyGrossRevenueUSD": round(safe_number(previous_gross_revenue), 6),
        "previousDailyBorrowFeeRebateUSD": round(safe_number(previous_rebate), 6),
        "previousDailyFeesUSD": round(safe_number(previous_fees), 6),
        "revenue7dUSD": round(window_sum(series, 7, "revenueUSD"), 6),
        "grossRevenue7dUSD": round(window_sum(series, 7, "grossRevenueUSD"), 6),
        "borrowFeeRebate7dUSD": round(window_sum(series, 7, "borrowFeeRebateUSD"), 6),
        "fees7dUSD": round(window_sum(series, 7, "feesUSD"), 6),
        "revenue30dUSD": round(window_sum(series, 30, "revenueUSD"), 6),
        "grossRevenue30dUSD": round(window_sum(series, 30, "grossRevenueUSD"), 6),
        "borrowFeeRebate30dUSD": round(window_sum(series, 30, "borrowFeeRebateUSD"), 6),
        "fees30dUSD": round(window_sum(series, 30, "feesUSD"), 6),
        "revenueAllTimeUSD": round(max(gross_revenue_all_time - rebate_all_time, 0.0), 6),
        "grossRevenueAllTimeUSD": round(gross_revenue_all_time, 6),
        "borrowFeeRebateAllTimeUSD": round(rebate_all_time, 6),
        "feesAllTimeUSD": round(safe_number(fees_data.get("totalAllTime")), 6),
    }


def build_output(revenue_data, fees_data, onchain_audit=None, borrow_fee_rebate_metadata=None,
                 borrow_fee_rebate_data=None, onchain_revenue_overrides=None, now=None):
    series = merge_series(revenue_data, fees_data)
    if len(series) < 30:
        raise ValueError("Merged revenue series has too few rows")

    if onchain_audit is None:
        onchain_audit = load_onchain_audit()
    if onchain_revenue_overrides is None:
        onchain_revenue_overrides = load_onchain_revenue_overrides()
    series = apply_onchain_revenue_overrides(series, onchain_audit, onchain_revenue_overrides)
    rebate_metadata = normalized_borrow_fee_rebate_metadata(borrow_fee_rebate_metadata, borrow_fee_rebate_data)
    series = apply_borrow_fee_rebates(series, rebate_metadata)
    latest = series[-1]
    generated_at = utc_now_iso()
    return {
        "schemaVersion": 2,
        "protocol": "Dolomite",
        "source": "DeFiLlama fees adapter with current-index onchain overrides",
        "sourceUrls": {
            "dailyRevenue": f"{BASE_URL}?dataType=dailyRevenue",
            "dailyFees": f"{BASE_URL}?dataType=dailyFees",
            "adapter": "https://github.com/DefiLlama/dimension-adapters/tree/master/fees/dolomite",
            "onchainAudit": "data/dolomite-revenue-onchain-audit.json",
            "borrowFeeRebates": BORROW_FEE_REBATE_METADATA_URL,
            "borrowFeeRebateDocs": BORROW_FEE_REBATE_DOCS_URL,
            "borrowFeeRebateDeployments": FEE_REBATE_DEPLOYMENTS_URL,
            "doloModuleDocs": "https://docs.dolomite.io/smart-contract-addresses/module-dolo",
        },
        "generatedAt": generated_at,
        "lastUpdated": generated_at,
        "methodology": {
            "fees": "Interest paid by borrowers.",
            "revenue": "Net protocol-retained borrower interest after closed-epoch borrow-fee rebates.",
            "grossRevenue": "Protocol-retained borrower interest before borrower rebate programs.",
            "borrowFeeRebates": "Claimable veDOLO borrow-fee rebates for closed weekly epochs, read from Berachain rolling-claims Merkle root totals and allocated across the earning period by daily borrow-interest share.",
            "supplySideRevenue": "The portion of borrower interest paid to lenders.",
            "formula": "grossRevenue = interestEarned * (1 - earningsRate); revenue = grossRevenue - borrowFeeRebates; supplySideRevenue = dailyFees - grossRevenue",
            "scope": "Dolomite borrow-interest economics from the DeFiLlama adapter, with audited Ethereum and Berachain rows replaced by independent current-index onchain audit rows when available. Gas fees, token emissions, treasury transfers, trading spreads, liquidator earnings and protocol liquidation-rake attribution are excluded.",
            "sourceLimitations": [
                "DeFiLlama adapter estimates daily interest from borrow index movement and borrowed principal snapshots.",
                "Ethereum, Berachain, and Mantle use the independent current-index onchain audit for audited daily rows, because it better reflects accrued borrow interest than cached adapter indexes.",
                "This is protocol-retained borrow interest, not a direct treasury cashflow audit.",
                "Current unfinalized rebate epochs remain gross until the weekly claim data is published onchain.",
                "Current-day values can be revised by DeFiLlama until the adapter window fully settles.",
            ],
        },
        "assurance": {
            "classification": "hybrid adapter/current-index protocol borrow-interest revenue",
            "confidence": "high for retained borrow-interest direction/split when the independent onchain audit is pass; warn/stale audit states should be treated as data-quality caveats",
            "rollingTotalsSource": "Saved daily series rows, matching chart and chain breakdowns for borrow interest",
            "berachainRevenueSource": "current-index onchain audit for audited daily rows; DeFiLlama adapter fallback outside audited coverage",
            "onchainOverrideRevenueSource": "current-index onchain audit for audited Ethereum/Berachain/Mantle daily rows; DeFiLlama adapter fallback outside audited coverage",
            "netRevenueAfterBorrowFeeRebates": "closed-epoch Berachain veDOLO borrow-fee rebates are netted from displayed revenue; active/unpublished epochs remain gross",
            "borrowFeeRebateStatus": borrow_fee_rebate_status(rebate_metadata),
            **onchain_audit_assurance(onchain_audit, now=now),
        },
        "borrowFeeRebates": rebate_metadata,
        "totals": metric_totals(revenue_data, fees_data, series),
        "latest": latest,
        "chainTotals7d": window_chain_totals(series, 7),
        "chainTotals30d": window_chain_totals(series, 30),
        "series": series,
    }


def main():
    print("Fetching Dolomite revenue data...")
    try:
        previous_output = load_existing_revenue_output()
        revenue_data = fetch_metric("dailyRevenue")
        fees_data = fetch_metric("dailyFees")
        rebate_metadata = fetch_borrow_fee_rebate_metadata()
        rebate_data = preserve_previous_borrow_fee_rebate_data(
            fetch_borrow_fee_rebate_data(),
            previous_output,
        )
        rebate_data = audit_borrow_fee_rebate_max_rebates(rebate_metadata, rebate_data)
        if rebate_data.get("status") == "fallback_previous_closed_epochs":
            print("   Borrow fee rebate fetch unavailable; preserving previous closed-epoch rebate data")
        onchain_audit = load_onchain_audit()
        onchain_revenue_overrides = merge_onchain_revenue_override_history(
            load_onchain_revenue_overrides(),
            onchain_audit,
        )
        write_onchain_revenue_overrides(onchain_revenue_overrides)
        output = build_output(
            revenue_data,
            fees_data,
            onchain_audit=onchain_audit,
            borrow_fee_rebate_metadata=rebate_metadata,
            borrow_fee_rebate_data=rebate_data,
            onchain_revenue_overrides=onchain_revenue_overrides,
        )
        with open(OUTPUT_FILE, "w") as f:
            json.dump(output, f, separators=(",", ":"))

        print(f"Saved {os.path.basename(OUTPUT_FILE)} ({os.path.getsize(OUTPUT_FILE) / 1024:.0f} KB)")
        print(f"Daily revenue: ${output['totals']['dailyRevenueUSD']:,.0f}")
        print(f"Daily gross revenue: ${output['totals']['dailyGrossRevenueUSD']:,.0f}")
        print(f"Daily borrow fee rebates: ${output['totals']['dailyBorrowFeeRebateUSD']:,.0f}")
        print(f"Daily fees: ${output['totals']['dailyFeesUSD']:,.0f}")
    except Exception as exc:
        print(f"Revenue fetch failed: {exc}")
        if os.path.exists(OUTPUT_FILE):
            print(f"Keeping existing {OUTPUT_FILE}")
            return
        raise


if __name__ == "__main__":
    main()
