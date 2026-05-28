#!/usr/bin/env python3
"""
Fetch current Dolomite supply, borrow and net TVL metrics.

The public Dolomite app uses api.dolomite.io token liquidity plus prices for
its Stats page. Keep this dashboard aligned with that source, and use the
subgraphs only for block/freshness metadata.
"""

import json
import os
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_tvl.json")
STALE_CHAIN_SECONDS = 6 * 60 * 60
DOLOMITE_API_SERVER_URL = "https://api.dolomite.io"
WLFI_MIN_USD = Decimal("10000000")

API_CHAIN_IDS = {
    "Berachain": 80094,
    "Arbitrum": 42161,
    "Ethereum": 1,
    "Botanix": 3637,
    "Mantle": 5000,
    "Polygon zkEVM": 1101,
    "X Layer": 196,
}

ASSETS_CHAINS = {
    "Berachain": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
    "Arbitrum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
    "Ethereum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-ethereum/latest/gn",
    "Botanix": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-botanix/latest/gn",
    "Mantle": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-mantle/latest/gn",
    "Polygon zkEVM": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-polygon-zkevm/latest/gn",
    "X Layer": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-x-layer/latest/gn"
}

META_QUERY = """
{
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

def as_decimal(value, default="0"):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def to_float(value):
    return float(value)


def fetch_json(url):
    response = requests.get(
        url,
        timeout=20,
        headers={
            "Accept": "application/json",
            "User-Agent": "dolomite-dashboard-tvl/1.1",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected JSON shape from {url}")
    return payload


def fetch_chain_meta(chain_name, url):
    response = requests.post(url, json={"query": META_QUERY}, timeout=20)
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    data = payload.get("data") or {}
    meta = data.get("_meta", {}) or {}
    block = meta.get("block", {}) or {}
    return {
        "blockNumber": block.get("number"),
        "blockHash": block.get("hash"),
        "blockTimestamp": block.get("timestamp"),
        "deployment": meta.get("deployment"),
    }


def token_rows_from_payload(payload):
    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        return tokens
    raise RuntimeError("Dolomite token payload is missing tokens[]")


def price_map_from_payload(payload):
    prices = payload.get("prices")
    if not isinstance(prices, dict):
        raise RuntimeError("Dolomite prices payload is missing prices{}")
    return {
        str(address).lower(): as_decimal(price)
        for address, price in prices.items()
    }


def fetch_chain_liquidity(chain_name):
    chain_id = API_CHAIN_IDS[chain_name]
    tokens_payload = fetch_json(f"{DOLOMITE_API_SERVER_URL}/tokens/{chain_id}")
    prices_payload = fetch_json(f"{DOLOMITE_API_SERVER_URL}/tokens/{chain_id}/prices")
    prices = price_map_from_payload(prices_payload)

    chain_supplied = Decimal("0")
    chain_borrowed = Decimal("0")
    per_chain_tokens = {}

    for token in token_rows_from_payload(tokens_payload):
        address = str(token.get("id", "")).lower()
        price = prices.get(address, Decimal("0"))
        supply_qty = as_decimal(token.get("supplyLiquidity"))
        borrow_qty = as_decimal(token.get("borrowLiquidity"))
        supply_usd = supply_qty * price
        borrow_usd = borrow_qty * price

        if supply_usd <= Decimal("1") and borrow_usd <= Decimal("1"):
            continue

        chain_supplied += supply_usd
        chain_borrowed += borrow_usd

        if supply_usd > Decimal("1"):
            symbol = token.get("cleanSymbol") or token.get("symbol") or "UNKNOWN"
            per_chain_tokens[symbol] = per_chain_tokens.get(symbol, Decimal("0")) + supply_usd

    return chain_supplied, chain_borrowed, per_chain_tokens


def validate_expected_token_guards(tokens_in_usd, chain_tokens_in_usd):
    wlfi_global = tokens_in_usd.get("WLFI", Decimal("0"))
    wlfi_ethereum = chain_tokens_in_usd.get("Ethereum", {}).get("WLFI", Decimal("0"))
    if wlfi_global < WLFI_MIN_USD or wlfi_ethereum < WLFI_MIN_USD:
        raise RuntimeError(
            "Refusing to write Dolomite TVL snapshot without material Ethereum WLFI. "
            f"global=${to_float(wlfi_global):,.0f}, ethereum=${to_float(wlfi_ethereum):,.0f}"
        )


def main():
    print("📡 Fetching Official Dolomite Supply from api.dolomite.io...")

    global_tvl = Decimal("0")
    global_borrows = Decimal("0")
    chain_tvls = {}
    chain_borrows = {}
    tokens_in_usd = {}
    chain_tokens_in_usd = {}
    chain_meta = {}
    failed_chains = []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    
    for chain_name, url in ASSETS_CHAINS.items():
        try:
            chain_supplied, chain_borrowed, per_chain_tokens = fetch_chain_liquidity(chain_name)

            for symbol, supply_usd in per_chain_tokens.items():
                tokens_in_usd[symbol] = tokens_in_usd.get(symbol, Decimal("0")) + supply_usd

            # Net TVL = Supply - Borrows
            chain_net_tvl = chain_supplied - chain_borrowed
            
            chain_tvls[chain_name] = chain_net_tvl
            chain_borrows[chain_name] = chain_borrowed

            global_tvl += chain_net_tvl
            global_borrows += chain_borrowed
            if per_chain_tokens:
                chain_tokens_in_usd[chain_name] = per_chain_tokens

            chain_meta[chain_name] = fetch_chain_meta(chain_name, url)

            print(f"✅ {chain_name}: TVL ${to_float(chain_net_tvl):,.0f} | Borrowed ${to_float(chain_borrowed):,.0f} | Supply ${to_float(chain_supplied):,.0f}")

        except Exception as e:
            print(f"⚠️ Failed to fetch {chain_name}: {e}")
            failed_chains.append(chain_name)

    if failed_chains or len(chain_tvls) != len(ASSETS_CHAINS):
        missing = sorted(set(ASSETS_CHAINS) - set(chain_tvls))
        raise RuntimeError(
            "Refusing to write partial Dolomite TVL snapshot. "
            f"failed={failed_chains}, missing={missing}"
        )
    validate_expected_token_guards(tokens_in_usd, chain_tokens_in_usd)

    stale_chains = []
    for chain_name, meta in chain_meta.items():
        block_ts = meta.get("blockTimestamp")
        if not isinstance(block_ts, int) or now_ts - block_ts > STALE_CHAIN_SECONDS:
            stale_chains.append(chain_name)

    # Format exactly like defillama_data.json expect for current numbers
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
        "freshnessMaxAgeSeconds": STALE_CHAIN_SECONDS,
        "supplyLiquidity": to_float(global_tvl + global_borrows),
        "totalTvl": to_float(global_tvl),
        "totalBorrowed": to_float(global_borrows),
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f)

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"\n✅ Saved dolomite_tvl.json ({file_size / 1024:.2f} KB)")
    print(f"🎯 Total Supply: ${to_float(global_tvl + global_borrows):,.0f}")
    print(f"🎯 Net TVL: ${to_float(global_tvl):,.0f}")
    print(f"🎯 Total Borrowed: ${to_float(global_borrows):,.0f}")

if __name__ == "__main__":
    main()
