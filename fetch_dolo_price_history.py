#!/usr/bin/env python3
"""
Daily DOLO price history (DeFiLlama chart API) -> dolo_price_history.json.

Consumed by the oDOLO realized-discount curve: market price on the exercise
day vs price actually paid per veDOLO. Incremental and conservative:
historical days already in the file are kept (historical prices are
immutable); only missing days are added, and the most recent two days are
refreshed because the newest point can still move.
"""

import json
import os
import time
import urllib.request
from datetime import datetime, timezone

COIN = "ethereum:0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654"
START_TS = 1745452800  # 2025-04-24 — DOLO launch window
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(SCRIPT_DIR, "dolo_price_history.json")


def fetch_chart(start_ts):
    span = min(1000, int((time.time() - start_ts) // 86400) + 3)
    url = f"https://coins.llama.fi/chart/{COIN}?start={start_ts}&span={span}&period=1d"
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.load(resp)
    points = (data.get("coins", {}).get(COIN, {}) or {}).get("prices", [])
    prices = {}
    for point in points:
        day = datetime.utcfromtimestamp(int(point["timestamp"])).strftime("%Y-%m-%d")
        prices[day] = float(point["price"])
    return prices


def main():
    existing = {}
    if os.path.exists(OUT):
        try:
            existing = json.load(open(OUT)).get("prices", {}) or {}
        except Exception as exc:
            print(f"⚠️ could not read existing history ({exc}); refetching all", flush=True)

    fresh = fetch_chart(START_TS)
    if not fresh:
        if existing:
            print("⚠️ DeFiLlama returned no points — keeping existing file")
            return
        raise SystemExit("No price points returned and no existing file")

    merged = {**fresh, **existing}  # existing wins — immutable history
    for day in sorted(fresh)[-2:]:  # ...except the freshest two days
        merged[day] = fresh[day]

    payload = {
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "defillama-chart-1d",
        "coin": COIN,
        "days": len(merged),
        "prices": dict(sorted(merged.items())),
    }
    with open(OUT, "w") as f:
        json.dump(payload, f, indent=1)
    print(f"💾 {OUT}: {len(merged)} days, last={max(merged)}")


if __name__ == "__main__":
    main()
