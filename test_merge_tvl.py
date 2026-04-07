import json, requests

resp = requests.get("https://api.llama.fi/protocol/dolomite")
data = resp.json()

tvl_history = data.get("tvl", [])
borrowed_history = data.get("chainTvls", {}).get("borrowed", {}).get("tvl", [])

combined = {}
# Base TVL
for p in tvl_history:
    combined[p["date"]] = p["totalLiquidityUSD"]

# Add Borrowed TVL
for p in borrowed_history:
    d = p["date"]
    combined[d] = combined.get(d, 0) + p["totalLiquidityUSD"]

# Sort by date
merged_history = [{"date": k, "totalLiquidityUSD": v} for k, v in sorted(combined.items())]

print("Original TVL points:", len(tvl_history))
print("Original Borrowed points:", len(borrowed_history))
print("Merged points:", len(merged_history))
print("Recent TVL:", tvl_history[-1]["totalLiquidityUSD"])
print("Recent Borrowed:", borrowed_history[-1]["totalLiquidityUSD"])
print("Recent Merged:", merged_history[-1]["totalLiquidityUSD"])
