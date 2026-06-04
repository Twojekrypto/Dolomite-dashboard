#!/usr/bin/env python3
"""
Fetch perfectly accurate TVL and Total Supply metrics directly from Dolomite Subgraphs.
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

QUERY = """
{
    totalPars(first: 1000) {
        id
        supplyPar
        borrowPar
        token { id symbol }
    }
    interestIndexes(first: 1000) {
        id
        supplyIndex
        borrowIndex
    }
    oraclePrices(first: 1000) {
        id
        price
        token { id }
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


def main():
    print("📡 Fetching Official Dolomite TVL from Subgraphs...")

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
            resp = requests.post(url, json={"query": QUERY}, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("errors"):
                raise RuntimeError(payload["errors"])
            data = payload.get("data")
            if not data:
                raise RuntimeError("empty GraphQL data")

            # Build lookups
            indexes = {x["id"].lower(): x for x in data.get("interestIndexes", [])}
            prices = {}
            for op in data.get("oraclePrices", []):
                t_id = op.get("token", {}).get("id", "").lower()
                prices[t_id] = as_decimal(op.get("price"))

            chain_supplied = Decimal("0")
            chain_borrowed = Decimal("0")
            per_chain_tokens = {}

            for tp in data.get("totalPars", []):
                t_id = tp.get("token", {}).get("id", "").lower()
                symbol = tp.get("token", {}).get("symbol", "UNKNOWN")
                price = prices.get(t_id, Decimal("0"))
                
                idx = indexes.get(t_id, {})
                sIndex = as_decimal(idx.get("supplyIndex", 1))
                bIndex = as_decimal(idx.get("borrowIndex", 1))

                sPar = as_decimal(tp.get("supplyPar", 0))
                bPar = as_decimal(tp.get("borrowPar", 0))

                supply_usd = sPar * sIndex * price
                borrow_usd = bPar * bIndex * price

                # Exclude dust/empty markets to keep calculation tight
                if supply_usd > Decimal("1"):
                    chain_supplied += supply_usd
                    chain_borrowed += borrow_usd
                    
                    tokens_in_usd[symbol] = tokens_in_usd.get(symbol, 0) + supply_usd
                    per_chain_tokens[symbol] = per_chain_tokens.get(symbol, 0) + supply_usd

            # Net TVL = Supply - Borrows
            chain_net_tvl = chain_supplied - chain_borrowed
            
            chain_tvls[chain_name] = chain_net_tvl
            chain_borrows[chain_name] = chain_borrowed

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
