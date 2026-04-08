#!/usr/bin/env python3
"""
Fetch perfectly accurate TVL and Total Supply metrics directly from Dolomite Subgraphs.
"""

import json
import os
import time
import requests
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_tvl.json")

ASSETS_CHAINS = {
    "Berachain": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
    "Arbitrum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
    "Ethereum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-ethereum/latest/gn",
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
}
"""

def main():
    print("📡 Fetching Official Dolomite TVL from Subgraphs...")

    global_tvl = 0.0
    global_borrows = 0.0
    chain_tvls = {}
    chain_borrows = {}
    tokens_in_usd = {}
    chain_tokens_in_usd = {}
    
    for chain_name, url in ASSETS_CHAINS.items():
        try:
            resp = requests.post(url, json={"query": QUERY}, timeout=20)
            resp.raise_for_status()
            data = resp.json().get("data")
            if not data:
                continue

            # Build lookups
            indexes = {x["id"].lower(): x for x in data.get("interestIndexes", [])}
            prices = {}
            for op in data.get("oraclePrices", []):
                t_id = op.get("token", {}).get("id", "").lower()
                prices[t_id] = float(op["price"])

            chain_supplied = 0.0
            chain_borrowed = 0.0
            per_chain_tokens = {}

            for tp in data.get("totalPars", []):
                t_id = tp.get("token", {}).get("id", "").lower()
                symbol = tp.get("token", {}).get("symbol", "UNKNOWN")
                price = prices.get(t_id, 0.0)
                
                idx = indexes.get(t_id, {})
                sIndex = float(idx.get("supplyIndex", 1.0))
                bIndex = float(idx.get("borrowIndex", 1.0))

                sPar = float(tp.get("supplyPar", 0.0))
                bPar = float(tp.get("borrowPar", 0.0))

                supply_usd = sPar * sIndex * price
                borrow_usd = bPar * bIndex * price

                # Exclude dust/empty markets to keep calculation tight
                if supply_usd > 1.0:
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

            print(f"✅ {chain_name}: TVL ${chain_net_tvl:,.0f} | Borrowed ${chain_borrowed:,.0f} | Supply ${chain_supplied:,.0f}")

        except Exception as e:
            print(f"⚠️ Failed to fetch {chain_name}: {e}")

    # Format exactly like defillama_data.json expect for current numbers
    output_currentTvls = {}
    for c, tvl in chain_tvls.items():
        output_currentTvls[c] = tvl
        output_currentTvls[f"{c}-borrowed"] = chain_borrows.get(c, 0)
    output_currentTvls["borrowed"] = global_borrows

    output = {
        "currentChainTvls": output_currentTvls,
        "tokensInUsd": [{"tokens": tokens_in_usd}],
        "chainTokensInUsd": chain_tokens_in_usd,
        "supplyLiquidity": global_tvl + global_borrows,
        "totalTvl": global_tvl,
        "totalBorrowed": global_borrows,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f)

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"\n✅ Saved dolomite_tvl.json ({file_size / 1024:.2f} KB)")
    print(f"🎯 Total Supply: ${(global_tvl + global_borrows):,.0f}")
    print(f"🎯 Net TVL: ${global_tvl:,.0f}")
    print(f"🎯 Total Borrowed: ${global_borrows:,.0f}")

if __name__ == "__main__":
    main()
