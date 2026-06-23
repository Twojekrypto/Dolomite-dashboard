#!/usr/bin/env python3
"""
Fetch current TVL and Total Supply metrics from Dolomite's official liquidity fields.
"""

import json
import os
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_tvl.json")
STALE_CHAIN_SECONDS = 6 * 60 * 60

ASSETS_CHAINS = {
    "Berachain": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
    "Arbitrum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
    "Ethereum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-ethereum/latest/gn",
    "Botanix": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-botanix/latest/gn",
    "Mantle": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-mantle/latest/gn",
    "Polygon zkEVM": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-polygon-zkevm/latest/gn",
    "X Layer": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-x-layer/latest/gn"
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


def fetch_clean_symbol_map(chain_name):
    chain_id = DOLOMITE_TOKEN_API_CHAINS[chain_name]
    url = f"https://api.dolomite.io/tokens/{chain_id}"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    return clean_symbol_map_from_api_tokens(payload.get("tokens", []))


def resolve_token_symbol(token, clean_symbols):
    token_id = str(token.get("id") or "").strip().lower()
    market_id = str(token.get("marketId") or "").strip()
    return (
        clean_symbols.get(token_id)
        or clean_symbols.get(market_id)
        or str(token.get("symbol") or "UNKNOWN").strip()
        or "UNKNOWN"
    )


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


def main():
    print("📡 Fetching official Dolomite TVL from token liquidity fields...")

    chain_payloads = {}
    clean_symbol_maps = {}
    failed_chains = []
    
    for chain_name, url in ASSETS_CHAINS.items():
        try:
            resp = requests.post(url, json={"query": QUERY}, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("errors"):
                raise RuntimeError(payload["errors"])
            data = payload.get("data")
            if not data:
                raise RuntimeError("empty GraphQL data")

            clean_symbol_maps[chain_name] = fetch_clean_symbol_map(chain_name)
            chain_payloads[chain_name] = data
            print(f"✅ {chain_name}: {len(data.get('tokens', []))} markets | {len(clean_symbol_maps[chain_name])} clean symbol keys")

        except Exception as e:
            print(f"⚠️ Failed to fetch {chain_name}: {e}")
            failed_chains.append(chain_name)

    if failed_chains or len(chain_payloads) != len(ASSETS_CHAINS):
        missing = sorted(set(ASSETS_CHAINS) - set(chain_payloads))
        raise RuntimeError(
            "Refusing to write partial Dolomite TVL snapshot. "
            f"failed={failed_chains}, missing={missing}"
        )

    output = build_snapshot_from_token_liquidity(
        chain_payloads,
        clean_symbol_maps,
        now=datetime.now(timezone.utc),
    )

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f)

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"\n✅ Saved dolomite_tvl.json ({file_size / 1024:.2f} KB)")
    print(f"🎯 Total Supply: ${output['supplyLiquidity']:,.0f}")
    print(f"🎯 Net TVL: ${output['totalTvl']:,.0f}")
    print(f"🎯 Total Borrowed: ${output['totalBorrowed']:,.0f}")

if __name__ == "__main__":
    main()
