---
description: Run and debug Dolomite data pipeline scripts locally
---

# Data Pipeline — Run/Debug Scripts

## When to Use
When data needs refreshing, debugging pipeline errors, or testing new data fetching logic.

## Prerequisites
- Python 3 with `web3`, `requests`, `gql` installed
- `.env` or environment variables set (if RPC keys needed)

## Common Data Refresh Commands

// turbo
1. Update core data (holders, stats, expiry, price):
   `cd ~/Desktop/Draft/"Dolomite website" && python3 update_data.py`

// turbo
2. Update DOLO flows:
   `cd ~/Desktop/Draft/"Dolomite website" && python3 generate_dolo_flows.py`

// turbo
3. Update early exits (uses cache — safe to re-run):
   `cd ~/Desktop/Draft/"Dolomite website" && python3 fetch_early_exits.py`

// turbo
4. Update liquidation risk data (includes E-Mode RPC calls):
   `cd ~/Desktop/Draft/"Dolomite website" && python3 fetch_liquidation_risk.py`

## Debugging Tips

- **Subgraph rate limits:** Add `time.sleep(1)` between paginated queries if getting 429s
- **RPC timeouts:** Dolomite RPC can be slow — set timeout ≥30s in web3.py
- **Cache files:** `*_cache.json` and `*_state.json` are incremental sync files; delete them to force full re-fetch
- **E-Mode bug:** Use `user.id` NOT `effectiveUser.id` for `getAccountRiskOverride()` — see `lessons.md`

## Verify Output

// turbo
5. Check JSON output is valid: `cd ~/Desktop/Draft/"Dolomite website" && python3 -c "import json; json.load(open('vedolo_holders.json')); print('✅ Valid JSON')"`
