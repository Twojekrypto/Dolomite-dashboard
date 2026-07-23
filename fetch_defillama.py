#!/usr/bin/env python3
"""
Fetch DeFi Llama protocol data for Dolomite and save as LIGHTWEIGHT static JSON.
Only keeps the data actually used by the dashboard (~200KB vs 10MB full response).
This runs in GitHub Actions so the dashboard doesn't need live API calls.
"""

import json
import math
import os
import requests
import time
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "defillama_data.json")
DEFILLAMA_URL = "https://api.llama.fi/protocol/dolomite"
REQUEST_TIMEOUTS = (
    (10, 45),
    (10, 75),
    (10, 120),
)


def _history_value_map(points):
    values = {}
    for point in points or []:
        if not isinstance(point, dict):
            continue
        raw_date = point.get("date")
        raw_value = point.get("totalLiquidityUSD")
        if isinstance(raw_date, bool) or not isinstance(raw_date, (int, float)):
            continue
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            continue
        if not math.isfinite(raw_date) or not math.isfinite(raw_value):
            continue
        timestamp = int(raw_date)
        if timestamp <= 0 or timestamp != raw_date or raw_value <= 0:
            continue
        values[timestamp] = raw_value
    return values


def build_history_series(base_tvl_history, borrowed_history):
    """Return separate Net TVL and Total Supply histories."""
    net_values = _history_value_map(base_tvl_history)
    borrowed_values = _history_value_map(borrowed_history)
    net_history = [
        {"date": timestamp, "totalLiquidityUSD": net_values[timestamp]}
        for timestamp in sorted(net_values)
    ]
    supply_history = [
        {
            "date": timestamp,
            "totalLiquidityUSD": (
                net_values.get(timestamp, 0) + borrowed_values.get(timestamp, 0)
            ),
        }
        for timestamp in sorted(set(net_values) | set(borrowed_values))
    ]
    return net_history, supply_history


def fetch_defillama_protocol():
    headers = {
        "Accept": "application/json",
        "User-Agent": "dolomite-dashboard-tvl/1.0",
    }
    last_error = None
    for attempt, timeout in enumerate(REQUEST_TIMEOUTS, start=1):
        try:
            print(f"   Attempt {attempt}/{len(REQUEST_TIMEOUTS)} (read timeout {timeout[1]}s)")
            resp = requests.get(DEFILLAMA_URL, timeout=timeout, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise ValueError("DeFi Llama response is not a JSON object")
            if len(data.get("tvl") or []) < 1000:
                raise ValueError("DeFi Llama response is missing full TVL history")
            if not isinstance(data.get("currentChainTvls"), dict):
                raise ValueError("DeFi Llama response is missing currentChainTvls")
            return data
        except Exception as exc:
            last_error = exc
            print(f"   ⚠️ Attempt {attempt} failed: {exc}")
            if attempt < len(REQUEST_TIMEOUTS):
                time.sleep(5 * attempt)
    raise RuntimeError(f"DeFi Llama fetch failed after {len(REQUEST_TIMEOUTS)} attempts: {last_error}")


def main():
    print("📡 Fetching DeFi Llama data for Dolomite...")

    try:
        data = fetch_defillama_protocol()

        # --- Build SLIM output (only what the dashboard needs) ---

        # 1. Net TVL and Total Supply histories (used by separate charts)
        base_tvl_history = data.get("tvl", [])
        borrowed_history = data.get("chainTvls", {}).get("borrowed", {}).get("tvl", [])
        tvl_history, total_supply_history = build_history_series(
            base_tvl_history,
            borrowed_history,
        )

        # 2. Current chain TVLs (tiny — used for chain bars + donut fallback)
        current_chain_tvls = data.get("currentChainTvls", {})

        # 3. Token composition — only keep the LAST entry (used by donut chart)
        global_tokens = {}
        tokens_in_usd = data.get("tokensInUsd", [])
        if tokens_in_usd and len(tokens_in_usd) > 0 and "tokens" in tokens_in_usd[-1]:
            for tk, val in tokens_in_usd[-1]["tokens"].items():
                if val > 0: global_tokens[tk] = global_tokens.get(tk, 0) + val
                
        chain_tvls_raw = data.get("chainTvls", {})
        borrowed_data = chain_tvls_raw.get("borrowed", {}).get("tokensInUsd", [])
        if borrowed_data and len(borrowed_data) > 0 and "tokens" in borrowed_data[-1]:
            for tk, val in borrowed_data[-1]["tokens"].items():
                if val > 0: global_tokens[tk] = global_tokens.get(tk, 0) + val
        
        last_token_entry = {"tokens": global_tokens}

        # 3b. Per-chain token composition (for chain filter on donut chart)
        NON_CHAINS = {'borrowed', 'staking', 'pool2', 'vesting',
                      'offers', 'treasury', 'cex', 'governance'}
        chain_tokens_in_usd = {}
        for chain_name, chain_data in chain_tvls_raw.items():
            if '-' in chain_name or chain_name.lower() in NON_CHAINS:
                continue
            if not isinstance(chain_data, dict):
                continue
            
            chain_local_tokens = {}
            # Base TVL tokens
            ct = chain_data.get("tokensInUsd", [])
            if ct and len(ct) > 0 and isinstance(ct[-1], dict) and "tokens" in ct[-1]:
                for tk, val in ct[-1]["tokens"].items():
                    if val > 0: chain_local_tokens[tk] = chain_local_tokens.get(tk, 0) + val
            
            # Borrowed TVL tokens for this chain
            borrowed_key = chain_name + "-borrowed"
            b_data = chain_tvls_raw.get(borrowed_key, {}).get("tokensInUsd", [])
            if b_data and len(b_data) > 0 and isinstance(b_data[-1], dict) and "tokens" in b_data[-1]:
                for tk, val in b_data[-1]["tokens"].items():
                    if val > 0: chain_local_tokens[tk] = chain_local_tokens.get(tk, 0) + val
                    
            if chain_local_tokens:
                chain_tokens_in_usd[chain_name] = chain_local_tokens

        # 4. Metadata (used by Protocol Info section)
        output = {
            "currentChainTvls": current_chain_tvls,
            "tvl": tvl_history,
            "totalSupply": total_supply_history,
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
        print(f"   Total Supply history points: {len(total_supply_history)}")

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
