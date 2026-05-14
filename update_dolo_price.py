#!/usr/bin/env python3
"""Refresh the lightweight DOLO market price cache for GitHub Pages."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import requests


DATA_DIR = os.path.dirname(os.path.abspath(__file__))
PRICE_FILE = os.path.join(DATA_DIR, "dolo_price.json")
DOLO_TOKEN = "ethereum:0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654"


def load_previous_price() -> dict:
    if not os.path.exists(PRICE_FILE):
        return {}
    try:
        with open(PRICE_FILE, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def fetch_json(url: str) -> dict:
    response = requests.get(url, timeout=20, headers={"User-Agent": "dolomite-dashboard-price/1.0"})
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response shape from {url}")
    return data


def fetch_defillama_price() -> float:
    data = fetch_json(f"https://coins.llama.fi/prices/current/{DOLO_TOKEN}")
    coin = data.get("coins", {}).get(DOLO_TOKEN, {})
    price = coin.get("price") if isinstance(coin, dict) else None
    if not price:
        raise RuntimeError("DeFiLlama response did not include DOLO price")
    return float(price)


def build_price_payload() -> dict:
    previous = load_previous_price()
    price = fetch_defillama_price()
    token = {}
    source = "defillama"
    try:
        simple = fetch_json(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=dolomite&vs_currencies=usd"
            "&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true"
        )
        token = simple.get("dolomite") if isinstance(simple.get("dolomite"), dict) else {}
        if token.get("usd"):
            price = float(token["usd"])
            source = "coingecko"
    except requests.RequestException as exc:
        print(f"Warning: CoinGecko simple price failed, using DeFiLlama price: {exc}")

    details = {}
    try:
        details = fetch_json(
            "https://api.coingecko.com/api/v3/coins/dolomite"
            "?localization=false&tickers=false&community_data=false&developer_data=false"
        )
    except requests.RequestException as exc:
        print(f"Warning: CoinGecko detail endpoint failed, preserving previous supply fields: {exc}")

    market_data = details.get("market_data", {}) if isinstance(details, dict) else {}
    fdv_data = market_data.get("fully_diluted_valuation", {}) if isinstance(market_data, dict) else {}
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    circulating_supply = market_data.get("circulating_supply") or previous.get("circulating_supply", 0)
    total_supply = market_data.get("total_supply") or previous.get("total_supply", 0)
    fdv = fdv_data.get("usd") or previous.get("fdv") or (price * total_supply if total_supply else 0)
    market_cap = token.get("usd_market_cap") or (price * circulating_supply if circulating_supply else previous.get("market_cap", 0))

    payload = {
        "price": price,
        "market_cap": market_cap,
        "volume_24h": token.get("usd_24h_vol", previous.get("volume_24h", 0)),
        "change_24h": token.get("usd_24h_change", previous.get("change_24h", 0)),
        "circulating_supply": circulating_supply,
        "total_supply": total_supply,
        "fdv": fdv,
        "last_updated": now,
        "source": source,
    }
    if payload["price"] <= 0:
        raise RuntimeError("Refusing to write non-positive DOLO price")
    return payload


def main() -> int:
    payload = build_price_payload()
    with open(PRICE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    print(f"Updated dolo_price.json: ${payload['price']:.6f} at {payload['last_updated']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
