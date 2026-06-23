#!/usr/bin/env python3
"""
Independent onchain assurance audit for Dolomite Revenue.

The dashboard still uses DeFiLlama for the displayed revenue series. This script
replays a closed daily window from DolomiteMargin state and writes an assurance
report that can mark the DeFiLlama-derived revenue as pass/warn/partial/missing.
Historical eth_call requires archive-capable RPC endpoints for reliable results.
"""

import argparse
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext

import requests
from web3 import Web3

import rpc_client


getcontext().prec = 60

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REVENUE_FILE = os.path.join(ROOT, "dolomite_revenue.json")
DEFAULT_OUTPUT_FILE = os.path.join(ROOT, "data", "dolomite-revenue-onchain-audit.json")
SECONDS_PER_DAY = 86_400
ONE = Decimal(10) ** 18

DOLOMITE_MARGIN_ABI = [
    {"inputs": [], "name": "getNumMarkets", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getEarningsRate", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"type": "uint256", "name": "marketId"}], "name": "getMarketTokenAddress", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {
        "inputs": [{"type": "uint256", "name": "marketId"}],
        "name": "getMarketTotalPar",
        "outputs": [{"components": [{"name": "borrow", "type": "uint128"}, {"name": "supply", "type": "uint128"}], "type": "tuple"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"type": "uint256", "name": "marketId"}],
        "name": "getMarketCurrentIndex",
        "outputs": [{"components": [{"name": "borrow", "type": "uint112"}, {"name": "supply", "type": "uint112"}, {"name": "lastUpdate", "type": "uint32"}], "type": "tuple"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"type": "uint256", "name": "marketId"}],
        "name": "getMarketWithInfo",
        "outputs": [
            {
                "components": [
                    {"name": "token", "type": "address"},
                    {"name": "isClosing", "type": "bool"},
                    {
                        "components": [{"name": "borrow", "type": "uint128"}, {"name": "supply", "type": "uint128"}],
                        "name": "totalPar",
                        "type": "tuple",
                    },
                    {
                        "components": [{"name": "borrow", "type": "uint112"}, {"name": "supply", "type": "uint112"}, {"name": "lastUpdate", "type": "uint32"}],
                        "name": "index",
                        "type": "tuple",
                    },
                    {"name": "priceOracle", "type": "address"},
                    {"name": "interestSetter", "type": "address"},
                    {"components": [{"name": "value", "type": "uint256"}], "name": "marginPremium", "type": "tuple"},
                    {"components": [{"name": "value", "type": "uint256"}], "name": "liquidationSpreadPremium", "type": "tuple"},
                    {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "maxSupplyWei", "type": "tuple"},
                    {"components": [{"name": "sign", "type": "bool"}, {"name": "value", "type": "uint256"}], "name": "maxBorrowWei", "type": "tuple"},
                    {"components": [{"name": "value", "type": "uint256"}], "name": "earningsRateOverride", "type": "tuple"},
                ],
                "type": "tuple",
            },
            {"components": [{"name": "borrow", "type": "uint112"}, {"name": "supply", "type": "uint112"}, {"name": "lastUpdate", "type": "uint32"}], "type": "tuple"},
            {"components": [{"name": "value", "type": "uint256"}], "type": "tuple"},
            {"components": [{"name": "value", "type": "uint256"}], "type": "tuple"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC20_ABI = [
    {"inputs": [], "name": "symbol", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
]

CHAIN_CONFIGS = {
    "arbitrum": {
        "name": "Arbitrum",
        "coinChain": "arbitrum",
        "mode": "arbitrum",
        "margin": "0x6Bd780E7fDf01D77e4d475c821f1e7AE05409072",
    },
    "berachain": {
        "name": "Berachain",
        "coinChain": "berachain",
        "mode": "standard",
        "margin": "0x003Ca23Fd5F0ca87D01F6eC6CD14A8AE60c2b97D",
    },
    "ethereum": {
        "name": "Ethereum",
        "coinChain": "ethereum",
        "mode": "standard",
        "margin": "0x003Ca23Fd5F0ca87D01F6eC6CD14A8AE60c2b97D",
    },
    "mantle": {
        "name": "Mantle",
        "coinChain": "mantle",
        "mode": "standard",
        "margin": "0xE6Ef4f0B2455bAB92ce7cC78E35324ab58917De8",
    },
    "polygon_zkevm": {
        "name": "Polygon zkEVM",
        "coinChain": "polygon_zkevm",
        "mode": "standard",
        "margin": "0x836b557Cf9eF29fcF49C776841191782df34e4e5",
    },
}


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def default_target_date(now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now.astimezone(timezone.utc).date() - timedelta(days=2)).isoformat()


def timestamp_for_date(date_text):
    dt = datetime.fromisoformat(str(date_text)).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def audit_window_for_date(date_text):
    start_timestamp = timestamp_for_date(date_text)
    return start_timestamp, start_timestamp + SECONDS_PER_DAY


def pct_diff(actual, expected):
    actual = float(actual or 0)
    expected = float(expected or 0)
    if expected == 0:
        return 0.0 if actual == 0 else math.inf
    return abs(actual - expected) / abs(expected)


def serializable_diff(diff):
    return round(diff, 8) if math.isfinite(diff) else None


def classify_chain_result(chain, defillama, onchain, tolerance_pct=0.02, protocol_cut_tolerance=0.002):
    defillama_chain_missing = not isinstance(defillama, dict)
    if defillama_chain_missing:
        defillama = {
            "feesUSD": 0.0,
            "revenueUSD": 0.0,
            "supplySideRevenueUSD": 0.0,
        }
    if not onchain or onchain.get("status") == "missing":
        result = {"chain": chain, "status": "missing"}
        if onchain and onchain.get("error"):
            result["error"] = onchain.get("error")
        if onchain and onchain.get("rawTokens"):
            result["rawTokenCount"] = len(onchain.get("rawTokens") or [])
            result["rawTokens"] = onchain.get("rawTokens") or []
        if defillama_chain_missing:
            result["defillamaChainMissing"] = True
        return result

    fees_diff = pct_diff(onchain.get("feesUSD"), defillama.get("feesUSD"))
    revenue_diff = pct_diff(onchain.get("revenueUSD"), defillama.get("revenueUSD"))
    defillama_fees = float(defillama.get("feesUSD") or 0)
    defillama_cut = float(defillama.get("revenueUSD") or 0) / defillama_fees if defillama_fees > 0 else 0.0
    protocol_cut_diff = abs(float(onchain.get("protocolCut") or 0) - defillama_cut)
    status = "warn" if (
        not math.isfinite(fees_diff)
        or not math.isfinite(revenue_diff)
        or fees_diff > tolerance_pct
        or revenue_diff > tolerance_pct
        or protocol_cut_diff > protocol_cut_tolerance
    ) else "pass"

    result = {
        "chain": chain,
        "status": status,
        "feesUSD": round(float(onchain.get("feesUSD") or 0), 6),
        "revenueUSD": round(float(onchain.get("revenueUSD") or 0), 6),
        "defillamaFeesUSD": round(float(defillama.get("feesUSD") or 0), 6),
        "defillamaRevenueUSD": round(float(defillama.get("revenueUSD") or 0), 6),
        "feesDiffPct": serializable_diff(fees_diff),
        "revenueDiffPct": serializable_diff(revenue_diff),
        "feesDiffUnbounded": not math.isfinite(fees_diff),
        "revenueDiffUnbounded": not math.isfinite(revenue_diff),
        "protocolCut": round(float(onchain.get("protocolCut") or 0), 8),
        "defillamaProtocolCut": round(defillama_cut, 8),
        "protocolCutDiff": round(protocol_cut_diff, 8),
        "rawTokenCount": len(onchain.get("rawTokens") or []),
        "rawTokens": onchain.get("rawTokens") or [],
    }
    if defillama_chain_missing:
        result["defillamaChainMissing"] = True
    return result


def build_audit_report(target_date, target_timestamp, window_start_timestamp, chain_results,
                       window_end_timestamp=None,
                       tolerance_pct=0.02, protocol_cut_tolerance=0.002):
    window_end_timestamp = target_timestamp if window_end_timestamp is None else window_end_timestamp
    statuses = [result.get("status") for result in chain_results.values()]
    audited_count = sum(1 for status in statuses if status in {"pass", "warn"})
    warn_count = statuses.count("warn")
    missing_count = statuses.count("missing")
    pass_count = statuses.count("pass")
    revenue_unbounded = any(result.get("revenueDiffUnbounded") for result in chain_results.values())
    fees_unbounded = any(result.get("feesDiffUnbounded") for result in chain_results.values())
    revenue_diffs = [
        float(result.get("revenueDiffPct") or 0)
        for result in chain_results.values()
        if result.get("status") in {"pass", "warn"} and not result.get("revenueDiffUnbounded")
    ]
    fees_diffs = [
        float(result.get("feesDiffPct") or 0)
        for result in chain_results.values()
        if result.get("status") in {"pass", "warn"} and not result.get("feesDiffUnbounded")
    ]
    max_revenue_diff = None if revenue_unbounded else round(max(revenue_diffs or [0.0]), 8)
    max_fees_diff = None if fees_unbounded else round(max(fees_diffs or [0.0]), 8)
    if warn_count:
        status = "warn"
    elif audited_count and missing_count:
        status = "partial"
    elif audited_count:
        status = "pass"
    else:
        status = "missing"

    return {
        "schemaVersion": 1,
        "generatedAt": utc_now_iso(),
        "targetDate": target_date,
        "targetTimestamp": int(target_timestamp),
        "windowStartTimestamp": int(window_start_timestamp),
        "windowEndTimestamp": int(window_end_timestamp),
        "tolerancePct": float(tolerance_pct),
        "protocolCutTolerance": float(protocol_cut_tolerance),
        "status": status,
        "summary": {
            "auditedChainCount": audited_count,
            "passChainCount": pass_count,
            "warnChainCount": warn_count,
            "missingChainCount": missing_count,
            "maxRevenueDiffPct": max_revenue_diff,
            "maxFeesDiffPct": max_fees_diff,
            "revenueDiffUnbounded": revenue_unbounded,
            "feesDiffUnbounded": fees_unbounded,
        },
        "methodology": {
            "rawOnchainFormula": "borrowInterest = (borrowIndexEnd - borrowIndexStart) * borrowParStart / 1e18; protocolRevenue = borrowInterest * (1 - earningsRate)",
            "window": "Each DeFiLlama row date is audited as that named UTC day, from 00:00 UTC on the date to 00:00 UTC the next day.",
            "usdPricing": "Raw token amounts are independently replayed from DolomiteMargin; USD totals use historical token prices and should be compared with tolerance.",
            "archiveRpcRequirement": "Historical eth_call requires archive-capable RPC. Chains without archive access are marked missing, not pass.",
        },
        "chains": chain_results,
    }


def load_json(path):
    with open(path) as f:
        return json.load(f)


def write_json(path, payload):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))


def revenue_chain_totals(revenue_data, target_date):
    for row in revenue_data.get("series", []):
        if row.get("date") == target_date:
            return row.get("chains", {})
    raise ValueError(f"No dolomite_revenue.json row for target date {target_date}")


def block_timestamp(w3, block_number):
    return int(w3.eth.get_block(int(block_number))["timestamp"])


def find_block_at_or_before(w3, timestamp):
    low = 1
    high = int(w3.eth.block_number)
    while low < high:
        mid = (low + high + 1) // 2
        if block_timestamp(w3, mid) <= timestamp:
            low = mid
        else:
            high = mid - 1
    return low


def token_metadata(w3, token, block_identifier):
    contract = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
    try:
        symbol = contract.functions.symbol().call(block_identifier=block_identifier)
    except Exception:
        symbol = token[:10]
    try:
        decimals = int(contract.functions.decimals().call(block_identifier=block_identifier))
    except Exception:
        decimals = 18
    return str(symbol), decimals


def fetch_historical_prices(timestamp, coin_ids):
    prices = {}
    coin_ids = [coin_id for coin_id in coin_ids if coin_id]
    for start in range(0, len(coin_ids), 80):
        chunk = coin_ids[start:start + 80]
        if not chunk:
            continue
        url = f"https://coins.llama.fi/prices/historical/{int(timestamp)}/{','.join(chunk)}"
        response = requests.get(url, timeout=45, headers={"User-Agent": "dolomite-dashboard-revenue-onchain-audit/1.0"})
        response.raise_for_status()
        payload = response.json()
        for coin_id, data in (payload.get("coins") or {}).items():
            if isinstance(data, dict) and isinstance(data.get("price"), (int, float)):
                prices[coin_id] = Decimal(str(data["price"]))
        time.sleep(0.15)
    return prices


def market_interest_rows(w3, config, from_block, to_block):
    margin = Web3.to_checksum_address(config["margin"])
    contract = w3.eth.contract(address=margin, abi=DOLOMITE_MARGIN_ABI)
    market_count = int(contract.functions.getNumMarkets().call(block_identifier=from_block))
    default_earnings_rate = int(contract.functions.getEarningsRate().call(block_identifier=from_block))
    rows = []

    for market_id in range(market_count):
        if config["mode"] == "arbitrum":
            token = contract.functions.getMarketTokenAddress(market_id).call(block_identifier=to_block)
            total_par = contract.functions.getMarketTotalPar(market_id).call(block_identifier=from_block)
            start_index = contract.functions.getMarketCurrentIndex(market_id).call(block_identifier=from_block)
            end_index = contract.functions.getMarketCurrentIndex(market_id).call(block_identifier=to_block)
            borrow_par = int(total_par[0])
            start_borrow_index = int(start_index[0])
            end_borrow_index = int(end_index[0])
            earnings_rate = default_earnings_rate
        else:
            start = contract.functions.getMarketWithInfo(market_id).call(block_identifier=from_block)
            end = contract.functions.getMarketWithInfo(market_id).call(block_identifier=to_block)
            start_market = start[0]
            end_market = end[0]
            start_current_index = start[1]
            end_current_index = end[1]
            token = end_market[0]
            borrow_par = int(start_market[2][0])
            start_borrow_index = int(start_current_index[0])
            end_borrow_index = int(end_current_index[0])
            override = int(start_market[10][0])
            earnings_rate = override if override else default_earnings_rate

        interest_raw = (end_borrow_index - start_borrow_index) * borrow_par // 10**18
        if interest_raw <= 0:
            continue
        symbol, decimals = token_metadata(w3, token, to_block)
        amount = Decimal(interest_raw) / (Decimal(10) ** decimals)
        protocol_fraction = (ONE - Decimal(earnings_rate)) / ONE
        rows.append({
            "marketId": market_id,
            "token": Web3.to_checksum_address(token),
            "symbol": symbol,
            "decimals": decimals,
            "borrowInterestRaw": str(interest_raw),
            "borrowInterestAmount": amount,
            "protocolRevenueAmount": amount * protocol_fraction,
            "protocolCut": protocol_fraction,
        })
    return rows


def audit_chain_onchain(chain_key, config, target_timestamp, window_start_timestamp):
    try:
        endpoints = rpc_client.get_endpoints(chain_key)
    except Exception as exc:
        print(f"  {config['name']}: no RPC endpoints: {type(exc).__name__}: {rpc_client.sanitize_error(exc)}")
        return {
            "status": "missing",
            "error": f"{type(exc).__name__}: {rpc_client.sanitize_error(exc)}",
        }

    last_error = None
    for endpoint in endpoints:
        try:
            w3 = Web3(Web3.HTTPProvider(endpoint, request_kwargs={"timeout": 25}))
            from_block = find_block_at_or_before(w3, window_start_timestamp)
            to_block = find_block_at_or_before(w3, target_timestamp)
            rows = market_interest_rows(w3, config, from_block, to_block)
            coin_ids = [f"{config['coinChain']}:{row['token'].lower()}" for row in rows]
            prices = fetch_historical_prices(target_timestamp, coin_ids)
            fee_usd = Decimal(0)
            revenue_usd = Decimal(0)
            raw_tokens = []
            missing_price_count = 0
            for row, coin_id in zip(rows, coin_ids):
                price = prices.get(coin_id)
                fee_amount = row["borrowInterestAmount"]
                revenue_amount = row["protocolRevenueAmount"]
                if price is not None:
                    fee_usd += fee_amount * price
                    revenue_usd += revenue_amount * price
                else:
                    missing_price_count += 1
                raw_tokens.append({
                    "marketId": row["marketId"],
                    "token": row["token"],
                    "symbol": row["symbol"],
                    "borrowInterestAmount": str(row["borrowInterestAmount"]),
                    "protocolRevenueAmount": str(row["protocolRevenueAmount"]),
                    "priceUSD": float(price) if price is not None else None,
                })
            if missing_price_count:
                return {
                    "status": "missing",
                    "error": f"missing historical prices for {missing_price_count} token(s)",
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "rawTokens": raw_tokens,
                    "rpcHost": rpc_client.safe_host(endpoint),
                }
            protocol_cut = revenue_usd / fee_usd if fee_usd else Decimal(0)
            return {
                "status": "audited",
                "fromBlock": from_block,
                "toBlock": to_block,
                "feesUSD": float(fee_usd),
                "revenueUSD": float(revenue_usd),
                "protocolCut": float(protocol_cut),
                "rawTokens": raw_tokens,
                "rpcHost": rpc_client.safe_host(endpoint),
            }
        except Exception as exc:
            last_error = exc
            print(f"  {config['name']}: onchain audit failed on {rpc_client.safe_host(endpoint)}: {type(exc).__name__}: {rpc_client.sanitize_error(exc)}")
    return {
        "status": "missing",
        "error": f"{type(last_error).__name__}: {rpc_client.sanitize_error(last_error)}" if last_error else "no RPC endpoint",
    }


def run_audit(target_date, chains, revenue_file, tolerance_pct, protocol_cut_tolerance):
    revenue_data = load_json(revenue_file)
    target_timestamp = timestamp_for_date(target_date)
    window_start_timestamp, window_end_timestamp = audit_window_for_date(target_date)
    defillama_by_chain = revenue_chain_totals(revenue_data, target_date)
    chain_results = {}

    for chain_key in chains:
        config = CHAIN_CONFIGS[chain_key]
        print(f"Auditing {config['name']} for {target_date}...")
        onchain = audit_chain_onchain(chain_key, config, window_end_timestamp, window_start_timestamp)
        chain_results[config["name"]] = classify_chain_result(
            config["name"],
            defillama_by_chain.get(config["name"]),
            onchain,
            tolerance_pct=tolerance_pct,
            protocol_cut_tolerance=protocol_cut_tolerance,
        )
    return build_audit_report(
        target_date,
        target_timestamp,
        window_start_timestamp,
        chain_results,
        window_end_timestamp=window_end_timestamp,
        tolerance_pct=tolerance_pct,
        protocol_cut_tolerance=protocol_cut_tolerance,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Audit Dolomite revenue against onchain DolomiteMargin state")
    parser.add_argument("--date", default=default_target_date(), help="DeFiLlama row date to audit, default UTC T-2")
    parser.add_argument("--chains", default="arbitrum,ethereum,berachain,mantle,polygon_zkevm", help="Comma-separated chain keys")
    parser.add_argument("--revenue-file", default=DEFAULT_REVENUE_FILE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--tolerance-pct", type=float, default=0.02)
    parser.add_argument("--protocol-cut-tolerance", type=float, default=0.002)
    parser.add_argument("--fail-on-warn", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    chains = [chain.strip() for chain in args.chains.split(",") if chain.strip()]
    unknown = [chain for chain in chains if chain not in CHAIN_CONFIGS]
    if unknown:
        raise SystemExit(f"Unknown chain key(s): {', '.join(unknown)}")
    report = run_audit(args.date, chains, args.revenue_file, args.tolerance_pct, args.protocol_cut_tolerance)
    write_json(args.output, report)
    print(f"Saved {args.output}")
    print(f"Onchain revenue audit status: {report['status']}")
    if args.fail_on_warn and report["status"] == "warn":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
