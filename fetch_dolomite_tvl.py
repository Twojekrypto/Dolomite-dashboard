#!/usr/bin/env python3
"""
Fetch current TVL and Total Supply metrics from Dolomite's official liquidity fields.
"""

import json
import os
import time
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_tvl.json")
STALE_CHAIN_SECONDS = 6 * 60 * 60
TOKEN_API_RETRY_DELAYS = (2, 4, 8)
SUBGRAPH_RETRY_DELAYS = (2, 4, 8)
RETRYABLE_TOKEN_API_STATUS_CODES = {408, 429, 500, 502, 503, 504}

ASSETS_CHAINS = {
    "Berachain": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
    "Arbitrum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
    "Ethereum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-ethereum/latest/gn",
    "Botanix": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-botanix/latest/gn",
    "Mantle": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-mantle/latest/gn",
    "Polygon zkEVM": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-polygon-zkevm/latest/gn",
    "X Layer": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-x-layer/latest/gn"
}
RETIRED_ASSETS_CHAINS = {"Botanix", "Polygon zkEVM"}
ACTIVE_ASSETS_CHAINS = {
    chain: endpoint
    for chain, endpoint in ASSETS_CHAINS.items()
    if chain not in RETIRED_ASSETS_CHAINS
}

DOLOMITE_TOKEN_API_CHAINS = {
    "Berachain": 80094,
    "Arbitrum": 42161,
    "Ethereum": 1,
    "Botanix": 3637,
    "Mantle": 5000,
    "Polygon zkEVM": 1101,
    "X Layer": 196,
}

QUERY = """
{
    tokens(first: 1000) {
        id
        marketId
        symbol
        name
        supplyLiquidityUSD
        borrowLiquidityUSD
    }
    _meta {
        block {
            number
            hash
            timestamp
        }
        deployment
    }
}
"""


def incomplete_chain_meta_fields(data):
    meta = (data or {}).get("_meta", {}) or {}
    block = meta.get("block", {}) or {}
    missing = []
    for field in ("number", "timestamp"):
        value = block.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            missing.append(f"block.{field}")
    block_hash = block.get("hash")
    if not isinstance(block_hash, str) or not block_hash.strip():
        missing.append("block.hash")
    deployment = meta.get("deployment")
    if not isinstance(deployment, str) or not deployment.strip():
        missing.append("deployment")
    return missing


def fetch_subgraph_payload(chain_name, url):
    attempts = len(SUBGRAPH_RETRY_DELAYS) + 1
    for attempt in range(attempts):
        try:
            resp = requests.post(url, json={"query": QUERY}, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise RuntimeError("GraphQL response is not a JSON object")
            if payload.get("errors"):
                raise RuntimeError(str(payload["errors"]))
            data = payload.get("data")
            if not isinstance(data, dict) or not data:
                raise RuntimeError("empty GraphQL data")
            missing = incomplete_chain_meta_fields(data)
            if missing:
                raise RuntimeError(
                    f"{chain_name} subgraph metadata incomplete: {', '.join(missing)}"
                )
            return data
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            if attempt == len(SUBGRAPH_RETRY_DELAYS):
                raise
            delay = SUBGRAPH_RETRY_DELAYS[attempt]
            print(
                f"⚠️ {chain_name} subgraph response invalid ({exc}); "
                f"retrying in {delay}s ({attempt + 1}/{attempts})"
            )
            time.sleep(delay)

    raise RuntimeError(f"{chain_name} subgraph retry loop exited unexpectedly")


def as_decimal(value, default="0"):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def to_float(value):
    return float(value)


def clean_symbol_map_from_api_tokens(tokens):
    clean_symbols = {}
    for token in tokens or []:
        clean = str(token.get("cleanSymbol") or token.get("symbol") or "").strip()
        if not clean:
            continue
        token_id = str(token.get("id") or "").strip().lower()
        market_id = str(token.get("marketId") or "").strip()
        if token_id:
            clean_symbols[token_id] = clean
        if market_id:
            clean_symbols[market_id] = clean
    return clean_symbols


def clean_price_map_from_api_prices(prices):
    clean_prices = {}
    for token_id, price in (prices or {}).items():
        token_id = str(token_id or "").strip().lower()
        if token_id:
            clean_prices[token_id] = price
    return clean_prices


def fetch_token_api_json(url):
    attempts = len(TOKEN_API_RETRY_DELAYS) + 1
    for attempt in range(attempts):
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise ValueError("Dolomite token API response is not a JSON object")
            return payload
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            retryable = status_code is None or status_code in RETRYABLE_TOKEN_API_STATUS_CODES
            if not retryable or attempt == len(TOKEN_API_RETRY_DELAYS):
                raise
            delay = TOKEN_API_RETRY_DELAYS[attempt]
            print(
                f"⚠️ Dolomite token API returned {status_code or 'a network error'}; "
                f"retrying in {delay}s ({attempt + 1}/{attempts})"
            )
            time.sleep(delay)

    raise RuntimeError("Dolomite token API retry loop exited unexpectedly")


def fetch_clean_symbol_map(chain_name):
    chain_id = DOLOMITE_TOKEN_API_CHAINS[chain_name]
    payload = fetch_token_api_json(f"https://api.dolomite.io/tokens/{chain_id}")
    return clean_symbol_map_from_api_tokens(payload.get("tokens", []))


def fetch_token_liquidity_payload(chain_name):
    chain_id = DOLOMITE_TOKEN_API_CHAINS[chain_name]
    payload = fetch_token_api_json(f"https://api.dolomite.io/tokens/{chain_id}")
    return payload.get("tokens", [])


def fetch_price_map(chain_name):
    chain_id = DOLOMITE_TOKEN_API_CHAINS[chain_name]
    payload = fetch_token_api_json(f"https://api.dolomite.io/tokens/{chain_id}/prices")
    return clean_price_map_from_api_prices(payload.get("prices", {}))


def resolve_token_symbol(token, clean_symbols):
    token_id = str(token.get("id") or "").strip().lower()
    market_id = str(token.get("marketId") or "").strip()
    return (
        clean_symbols.get(token_id)
        or clean_symbols.get(market_id)
        or str(token.get("symbol") or "UNKNOWN").strip()
        or "UNKNOWN"
    )


def resolve_api_token_symbol(token):
    return (
        str(token.get("cleanSymbol") or token.get("symbol") or "").strip()
        or "UNKNOWN"
    )


def build_snapshot_from_official_liquidity(chain_payloads, token_payloads, price_maps, now=None):
    global_tvl = Decimal("0")
    global_borrows = Decimal("0")
    global_supply = Decimal("0")
    chain_tvls = {}
    chain_borrows = {}
    tokens_in_usd = {}
    chain_tokens_in_usd = {}
    chain_meta = {}
    missing_price_tokens = {}
    now_dt = now or datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())

    for chain_name, api_tokens in token_payloads.items():
        chain_supplied = Decimal("0")
        chain_borrowed = Decimal("0")
        per_chain_tokens = {}
        price_map = price_maps.get(chain_name, {})

        for token in api_tokens or []:
            token_id = str(token.get("id") or "").strip().lower()
            has_price = token_id in price_map
            price = as_decimal(price_map.get(token_id))
            supply_units = as_decimal(token.get("supplyLiquidity"))
            borrow_units = as_decimal(token.get("borrowLiquidity"))
            supply_usd = supply_units * price
            borrow_usd = borrow_units * price

            if not has_price and (supply_units > 0 or borrow_units > 0):
                missing_price_tokens.setdefault(chain_name, []).append(resolve_api_token_symbol(token))

            if supply_usd <= 0 and borrow_usd <= 0:
                continue

            symbol = resolve_api_token_symbol(token)
            chain_supplied += supply_usd
            chain_borrowed += borrow_usd

            if supply_usd > Decimal("1"):
                tokens_in_usd[symbol] = tokens_in_usd.get(symbol, Decimal("0")) + supply_usd
                per_chain_tokens[symbol] = per_chain_tokens.get(symbol, Decimal("0")) + supply_usd

        chain_net_tvl = chain_supplied - chain_borrowed
        chain_tvls[chain_name] = chain_net_tvl
        chain_borrows[chain_name] = chain_borrowed
        global_supply += chain_supplied
        global_tvl += chain_net_tvl
        global_borrows += chain_borrowed

        if per_chain_tokens:
            chain_tokens_in_usd[chain_name] = per_chain_tokens

        meta = (chain_payloads.get(chain_name, {}) or {}).get("_meta", {}) or {}
        block = meta.get("block", {}) or {}
        chain_meta[chain_name] = {
            "blockNumber": block.get("number"),
            "blockHash": block.get("hash"),
            "blockTimestamp": block.get("timestamp"),
            "deployment": meta.get("deployment"),
        }

    stale_chains = []
    for chain_name, meta in chain_meta.items():
        block_ts = meta.get("blockTimestamp")
        if not isinstance(block_ts, int) or now_ts - block_ts > STALE_CHAIN_SECONDS:
            stale_chains.append(chain_name)

    output_currentTvls = {}
    for c, tvl in chain_tvls.items():
        output_currentTvls[c] = to_float(tvl)
        output_currentTvls[f"{c}-borrowed"] = to_float(chain_borrows.get(c, Decimal("0")))
    output_currentTvls["borrowed"] = to_float(global_borrows)

    token_output = {
        symbol: to_float(value)
        for symbol, value in tokens_in_usd.items()
    }
    chain_token_output = {
        chain: {
            symbol: to_float(value)
            for symbol, value in tokens.items()
        }
        for chain, tokens in chain_tokens_in_usd.items()
    }

    output = {
        "currentChainTvls": output_currentTvls,
        "tokensInUsd": [{"tokens": token_output}],
        "chainTokensInUsd": chain_token_output,
        "chainMeta": chain_meta,
        "staleChains": stale_chains,
        "retiredChains": sorted(RETIRED_ASSETS_CHAINS),
        "freshnessMaxAgeSeconds": STALE_CHAIN_SECONDS,
        "source": "dolomite_api_token_liquidity_prices",
        "supplyLiquidity": to_float(global_supply),
        "totalTvl": to_float(global_tvl),
        "totalBorrowed": to_float(global_borrows),
        "last_updated": now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    if missing_price_tokens:
        output["missingPriceTokens"] = missing_price_tokens
    return output


def build_snapshot_from_token_liquidity(chain_payloads, clean_symbol_maps, now=None):
    global_tvl = Decimal("0")
    global_borrows = Decimal("0")
    global_supply = Decimal("0")
    chain_tvls = {}
    chain_borrows = {}
    tokens_in_usd = {}
    chain_tokens_in_usd = {}
    chain_meta = {}
    now_dt = now or datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())

    for chain_name, data in chain_payloads.items():
        chain_supplied = Decimal("0")
        chain_borrowed = Decimal("0")
        per_chain_tokens = {}
        clean_symbols = clean_symbol_maps.get(chain_name, {})

        for token in data.get("tokens", []):
            supply_usd = as_decimal(token.get("supplyLiquidityUSD"))
            borrow_usd = as_decimal(token.get("borrowLiquidityUSD"))
            if supply_usd <= 0 and borrow_usd <= 0:
                continue

            symbol = resolve_token_symbol(token, clean_symbols)
            chain_supplied += supply_usd
            chain_borrowed += borrow_usd

            if supply_usd > Decimal("1"):
                tokens_in_usd[symbol] = tokens_in_usd.get(symbol, Decimal("0")) + supply_usd
                per_chain_tokens[symbol] = per_chain_tokens.get(symbol, Decimal("0")) + supply_usd

        chain_net_tvl = chain_supplied - chain_borrowed
        chain_tvls[chain_name] = chain_net_tvl
        chain_borrows[chain_name] = chain_borrowed
        global_supply += chain_supplied
        global_tvl += chain_net_tvl
        global_borrows += chain_borrowed

        if per_chain_tokens:
            chain_tokens_in_usd[chain_name] = per_chain_tokens

        meta = data.get("_meta", {}) or {}
        block = meta.get("block", {}) or {}
        chain_meta[chain_name] = {
            "blockNumber": block.get("number"),
            "blockHash": block.get("hash"),
            "blockTimestamp": block.get("timestamp"),
            "deployment": meta.get("deployment"),
        }

    stale_chains = []
    for chain_name, meta in chain_meta.items():
        block_ts = meta.get("blockTimestamp")
        if not isinstance(block_ts, int) or now_ts - block_ts > STALE_CHAIN_SECONDS:
            stale_chains.append(chain_name)

    output_currentTvls = {}
    for c, tvl in chain_tvls.items():
        output_currentTvls[c] = to_float(tvl)
        output_currentTvls[f"{c}-borrowed"] = to_float(chain_borrows.get(c, Decimal("0")))
    output_currentTvls["borrowed"] = to_float(global_borrows)

    token_output = {
        symbol: to_float(value)
        for symbol, value in tokens_in_usd.items()
    }
    chain_token_output = {
        chain: {
            symbol: to_float(value)
            for symbol, value in tokens.items()
        }
        for chain, tokens in chain_tokens_in_usd.items()
    }

    return {
        "currentChainTvls": output_currentTvls,
        "tokensInUsd": [{"tokens": token_output}],
        "chainTokensInUsd": chain_token_output,
        "chainMeta": chain_meta,
        "staleChains": stale_chains,
        "freshnessMaxAgeSeconds": STALE_CHAIN_SECONDS,
        "source": "dolomite_subgraph_token_liquidity_usd",
        "supplyLiquidity": to_float(global_supply),
        "totalTvl": to_float(global_tvl),
        "totalBorrowed": to_float(global_borrows),
        "last_updated": now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    }


def blocking_tvl_failures(failed_chains, chain_payloads):
    failed = [chain for chain in failed_chains if chain not in RETIRED_ASSETS_CHAINS]
    missing = sorted(set(ACTIVE_ASSETS_CHAINS) - set(chain_payloads))
    return failed, missing


def main():
    print("📡 Fetching official Dolomite TVL from token liquidity and price APIs...")

    chain_payloads = {}
    token_payloads = {}
    price_maps = {}
    failed_chains = []
    
    for chain_name, url in ACTIVE_ASSETS_CHAINS.items():
        try:
            data = fetch_subgraph_payload(chain_name, url)
            token_payloads[chain_name] = fetch_token_liquidity_payload(chain_name)
            price_maps[chain_name] = fetch_price_map(chain_name)
            chain_payloads[chain_name] = data
            print(
                f"✅ {chain_name}: {len(token_payloads[chain_name])} token API markets | "
                f"{len(price_maps[chain_name])} prices | subgraph block {data.get('_meta', {}).get('block', {}).get('number')}"
            )

        except Exception as e:
            print(f"⚠️ Failed to fetch {chain_name}: {e}")
            failed_chains.append(chain_name)

    blocking_failed, missing = blocking_tvl_failures(failed_chains, chain_payloads)
    if blocking_failed or missing:
        raise RuntimeError(
            "Refusing to write partial Dolomite TVL snapshot. "
            f"failed={blocking_failed}, missing={missing}"
        )

    output = build_snapshot_from_official_liquidity(
        chain_payloads,
        token_payloads,
        price_maps,
        now=datetime.now(timezone.utc),
    )
    if RETIRED_ASSETS_CHAINS:
        output["retiredChains"] = sorted(RETIRED_ASSETS_CHAINS)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f)

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"\n✅ Saved dolomite_tvl.json ({file_size / 1024:.2f} KB)")
    print(f"🎯 Total Supply: ${output['supplyLiquidity']:,.0f}")
    print(f"🎯 Net TVL: ${output['totalTvl']:,.0f}")
    print(f"🎯 Total Borrowed: ${output['totalBorrowed']:,.0f}")

if __name__ == "__main__":
    main()
