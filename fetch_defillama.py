#!/usr/bin/env python3
"""
Fetch DeFi Llama protocol data for Dolomite and save as LIGHTWEIGHT static JSON.
Only keeps the data actually used by the dashboard (~200KB vs 10MB full response).
This runs in GitHub Actions so the dashboard doesn't need live API calls.
"""

import json
import os
import requests
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "defillama_data.json")


def main():
    print("📡 Fetching DeFi Llama data for Dolomite...")

    try:
        resp = requests.get(
            "https://api.llama.fi/protocol/dolomite",
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        # --- Build SLIM output (only what the dashboard needs) ---

        # 1. TVL history (small — ~0.1MB, used for chart)
        tvl_history = data.get("tvl", [])

        # 2. Current chain TVLs (tiny — used for chain bars + donut fallback)
        current_chain_tvls = data.get("currentChainTvls", {})

        # 3. Token composition — only keep the LAST entry (used by donut chart)
        tokens_in_usd = data.get("tokensInUsd", [])
        last_token_entry = tokens_in_usd[-1] if tokens_in_usd else None

        # 3b. Per-chain token composition (for chain filter on donut chart)
        chain_tvls_raw = data.get("chainTvls", {})
        NON_CHAINS = {'borrowed', 'staking', 'pool2', 'vesting',
                      'offers', 'treasury', 'cex', 'governance'}
        chain_tokens_in_usd = {}
        for chain_name, chain_data in chain_tvls_raw.items():
            if '-' in chain_name or chain_name.lower() in NON_CHAINS:
                continue
            if not isinstance(chain_data, dict):
                continue
            ct = chain_data.get("tokensInUsd", [])
            if ct:
                last_ct = ct[-1]
                if isinstance(last_ct, dict) and "tokens" in last_ct:
                    chain_tokens_in_usd[chain_name] = last_ct["tokens"]

        # 4. Metadata (used by Protocol Info section)
        output = {
            "currentChainTvls": current_chain_tvls,
            "tvl": tvl_history,
            "tokensInUsd": [last_token_entry] if last_token_entry else [],
            "chainTokensInUsd": chain_tokens_in_usd,
            "name": data.get("name", "Dolomite"),
            "category": data.get("category", ""),
            "chains": data.get("chains", []),
            "url": data.get("url", ""),
            "twitter": data.get("twitter", ""),
            "github": data.get("github", []),
            "openSource": data.get("openSource", False),
            "audits": data.get("audits", ""),
            "audit_links": data.get("audit_links", []),
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        with open(OUTPUT_FILE, "w") as f:
            json.dump(output, f)

        file_size = os.path.getsize(OUTPUT_FILE)
        total_tvl = sum(
            v for k, v in current_chain_tvls.items()
            if isinstance(v, (int, float)) and '-' not in k
            and k.lower() not in ('borrowed', 'staking', 'pool2', 'vesting',
                                  'offers', 'treasury', 'cex', 'governance')
        )
        print(f"   ✅ Saved defillama_data.json ({file_size / 1024:.0f} KB)")
        print(f"   TVL: ${total_tvl:,.0f}")
        print(f"   Chains: {len(output['chains'])}")
        print(f"   TVL history points: {len(tvl_history)}")

    except Exception as e:
        print(f"   ⚠️ DeFi Llama fetch failed: {e}")
        # Don't crash — keep existing file if it exists
        if os.path.exists(OUTPUT_FILE):
            print(f"   Keeping existing {OUTPUT_FILE}")
        else:
            print(f"   No existing file — saving empty placeholder")
            with open(OUTPUT_FILE, "w") as f:
                json.dump({"error": str(e), "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}, f)


if __name__ == "__main__":
    main()
