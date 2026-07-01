#!/usr/bin/env python3
"""
Fetch Dolomite fees/revenue data from DeFiLlama's fees adapter.

The dashboard is static, so this stores the small revenue dataset used by the
Revenue tab instead of calling the API from every visitor's browser.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, getcontext

import requests
from web3 import Web3

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
ONCHAIN_REVENUE_OVERRIDE_CHAINS = {BERACHAIN_CHAIN_NAME, "Ethereum"}
ONCHAIN_CURRENT_INDEX_SOURCE = "onchain-current-index-audit"
BERACHAIN_COIN_CHAIN = "berachain"
BERACHAIN_MARGIN_ADDRESS = "0x003Ca23Fd5F0ca87D01F6eC6CD14A8AE60c2b97D"
FEE_REBATE_CLAIMER_ADDRESS = "0x6BE1fed8a38B3555A337f58BB9E10FC0465964C0"
FEE_REBATE_ROLLING_CLAIMS_ADDRESS = "0xea9421044430FA791c3Ab16E0B90f142aa6C11ef"
FEE_REBATE_ROLLING_CLAIMS_DEPLOY_TX = "0xc7096f2f4e799ff80a25116552932c3baab028cdaec310948e8270bf760ebc43"
FEE_REBATE_ROLLING_CLAIMS_DEPLOY_BLOCK = 21_228_103
FEE_REBATE_DEPLOYMENTS_URL = "https://raw.githubusercontent.com/dolomite-exchange/dolomite-margin-modules/master/packages/deployment/src/deploy/deployments.json"
FEE_REBATE_EVENT_SOURCE = "FeeRebateRollingClaims.MarketIdToMerkleRootSet"
FEE_REBATE_START_TIMESTAMP = 1779321600
SECONDS_PER_WEEK = 7 * 24 * 60 * 60
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
                from_block = int(receipt["blockNumber"])
            except Exception:
                from_block = FEE_REBATE_ROLLING_CLAIMS_DEPLOY_BLOCK
            to_block = int(w3.eth.block_number)
            logs = get_logs_chunked(w3, FEE_REBATE_ROLLING_CLAIMS_ADDRESS, from_block, to_block, topic)
            logs = sorted(logs, key=lambda item: (int(item["blockNumber"]), int(item["transactionIndex"]), int(item["logIndex"])))
            block_timestamps = {}
            market_cache = {}
            market_totals = {}
            epoch_rebates = {}
            missing_price_count = 0
            price_fallback_count = 0
            priced_event_count = 0

            for log in logs:
                market_id, _merkle_root, total_raw = w3.codec.decode(["uint256", "bytes32", "uint256"], hex_to_bytes(log["data"]))
                market_id = int(market_id)
                total_raw = int(total_raw)
                previous_raw = int(market_totals.get(market_id, 0))
                market_totals[market_id] = total_raw
                delta_raw = total_raw - previous_raw
                if delta_raw <= 0:
                    continue

                block_number = int(log["blockNumber"])
                if block_number not in block_timestamps:
                    block_timestamps[block_number] = int(w3.eth.get_block(block_number)["timestamp"])
                timestamp = block_timestamps[block_number]
                epoch = rebate_epoch_for_timestamp(timestamp)
                period_start, period_end = rebate_epoch_window(epoch)
                metadata = get_market_token_metadata(w3, market_id, market_cache)
                coin_id = f"{BERACHAIN_COIN_CHAIN}:{metadata['token'].lower()}"
                prices = fetch_historical_prices(timestamp, [coin_id])
                price, price_source = resolve_rebate_token_price(metadata["symbol"], prices.get(coin_id))
                amount = Decimal(delta_raw) / (Decimal(10) ** int(metadata["decimals"]))
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
                entry["markets"].append({
                    "marketId": market_id,
                    "token": metadata["token"],
                    "symbol": metadata["symbol"],
                    "amount": str(amount),
                    "rebateUSD": float(rebate_usd),
                    "priceUSD": float(price) if price is not None else None,
                    "priceSource": price_source,
                })

            epoch_rows = []
            total_rebate_usd = Decimal(0)
            for epoch, entry in sorted(epoch_rebates.items()):
                total_rebate_usd += entry["rebateUSD"]
                epoch_rows.append({
                    "epoch": int(epoch),
                    "periodStartTimestamp": int(entry["periodStartTimestamp"]),
                    "periodEndTimestamp": int(entry["periodEndTimestamp"]),
                    "eventTimestamp": int(entry["eventTimestamp"]),
                    "eventBlock": int(entry["eventBlock"]),
                    "rebateUSD": round(float(entry["rebateUSD"]), 6),
                    "marketCount": len(entry["markets"]),
                    "markets": entry["markets"],
                })

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
    rebate_usd = safe_decimal_number(epoch_rebate.get("rebateUSD"))
    if rebate_usd <= 0:
        return 0.0

    period_start = int_or_none(epoch_rebate.get("periodStartTimestamp"))
    period_end = int_or_none(epoch_rebate.get("periodEndTimestamp"))
    if period_start is None or period_end is None or period_end <= period_start:
        return 0.0

    rows = [
        row for row in series
        if period_start <= int(row.get("timestamp") or 0) < period_end
        and isinstance((row.get("chains") or {}).get(chain), dict)
    ]
    if not rows:
        return 0.0

    weights = []
    for row in rows:
        payload = row["chains"][chain]
        weights.append(safe_number(payload.get("feesUSD")))
    if sum(weights) <= 0:
        weights = []
        for row in rows:
            payload = row["chains"][chain]
            weights.append(safe_number(payload.get("grossRevenueUSD", payload.get("revenueUSD"))))
    total_weight = sum(weights)
    applied = 0.0
    for index, row in enumerate(rows):
        payload = row["chains"][chain]
        share = (weights[index] / total_weight) if total_weight > 0 else (1 / len(rows))
        chain_rebate = rebate_usd * share
        payload["borrowFeeRebateUSD"] = safe_number(payload.get("borrowFeeRebateUSD")) + chain_rebate
        applied += chain_rebate
    return applied


def apply_borrow_fee_rebates(series, rebate_metadata):
    initialize_rebate_fields(series)
    if not isinstance(rebate_metadata, dict):
        return series

    for chain, chain_payload in (rebate_metadata.get("chains") or {}).items():
        for epoch_rebate in chain_payload.get("epochRebates") or []:
            apply_epoch_rebate_to_chain(series, chain, epoch_rebate if isinstance(epoch_rebate, dict) else {})

    for row in series:
        total_rebate = 0.0
        for payload in (row.get("chains") or {}).values():
            chain_fees = safe_number(payload.get("feesUSD"))
            chain_gross = safe_number(payload.get("grossRevenueUSD", payload.get("revenueUSD")))
            chain_rebate = min(safe_number(payload.get("borrowFeeRebateUSD")), chain_gross)
            chain_net = max(chain_gross - chain_rebate, 0.0)
            payload["borrowFeeRebateUSD"] = round(chain_rebate, 6)
            payload["revenueUSD"] = round(chain_net, 6)
            payload["supplySideRevenueUSD"] = round(max(chain_fees - chain_gross, 0.0), 6)
            payload["protocolCut"] = round(chain_net / chain_fees, 8) if chain_fees > 0 else 0
            payload["grossProtocolCut"] = round(chain_gross / chain_fees, 8) if chain_fees > 0 else 0
            total_rebate += chain_rebate

        fees = safe_number(row.get("feesUSD"))
        gross_revenue = safe_number(row.get("grossRevenueUSD", row.get("revenueUSD")))
        total_rebate = min(total_rebate, gross_revenue)
        net_revenue = max(gross_revenue - total_rebate, 0.0)
        row["borrowFeeRebateUSD"] = round(total_rebate, 6)
        row["revenueUSD"] = round(net_revenue, 6)
        row["supplySideRevenueUSD"] = round(max(fees - gross_revenue, 0.0), 6)
        row["protocolCut"] = round(net_revenue / fees, 8) if fees > 0 else 0
        row["grossProtocolCut"] = round(gross_revenue / fees, 8) if fees > 0 else 0
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
        epoch_rebates = rebate_chain_data.get("epochRebates") if isinstance(rebate_chain_data.get("epochRebates"), list) else []
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
                "Ethereum and Berachain use the independent current-index onchain audit for audited daily rows, because it better reflects accrued borrow interest than cached adapter indexes.",
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
            "onchainOverrideRevenueSource": "current-index onchain audit for audited Ethereum/Berachain daily rows; DeFiLlama adapter fallback outside audited coverage",
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
        revenue_data = fetch_metric("dailyRevenue")
        fees_data = fetch_metric("dailyFees")
        rebate_metadata = fetch_borrow_fee_rebate_metadata()
        rebate_data = fetch_borrow_fee_rebate_data()
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
