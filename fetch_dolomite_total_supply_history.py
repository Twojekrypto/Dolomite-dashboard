#!/usr/bin/env python3
"""Build full Total Supply history from official Dolomite USD market metrics."""

import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests

from fetch_dolomite_tvl import (
    ACTIVE_ASSETS_CHAINS,
    DOLOMITE_TOKEN_API_CHAINS,
    clean_price_map_from_api_prices,
    fetch_token_api_json,
)


DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DEFILLAMA_FILE = os.path.join(DATA_DIR, "defillama_data.json")
OFFICIAL_SNAPSHOT_FILE = os.path.join(DATA_DIR, "dolomite_tvl.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_total_supply_history.json")
METRICS_RETRY_DELAYS = (1, 2, 4)
METRICS_TIMEOUT_SECONDS = 30
MAX_WORKERS = 16
MIN_ACTIVE_SUPPLY_USD = Decimal("1")
MIN_OFFICIAL_HISTORY_POINTS = 60
MAX_METRIC_STALENESS_SECONDS = 2 * 86400
MAX_STALE_MARKET_SHARE = Decimal("0.001")


def _decimal(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
    return result if result.is_finite() else Decimal("0")


def _json_number(value):
    result = float(_decimal(value))
    return int(result) if result.is_integer() else result


def _history_rows(points):
    values = {}
    for row in points or []:
        if not isinstance(row, dict):
            continue
        timestamp = row.get("date")
        value = row.get("totalLiquidityUSD")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if not math.isfinite(timestamp) or not math.isfinite(value):
            continue
        timestamp = int(timestamp)
        if timestamp <= 0 or value < 0:
            continue
        values[timestamp] = _decimal(value)
    return values


def split_recent_market_histories(market_histories):
    latest_timestamp = max(
        (
            max((market.get("points") or {}).keys())
            for market in market_histories or []
            if market.get("points")
        ),
        default=0,
    )
    if latest_timestamp <= 0:
        raise ValueError("Official Dolomite market histories are empty")

    recent = []
    stale_active = []
    freshness_cutoff = latest_timestamp - MAX_METRIC_STALENESS_SECONDS
    for market in market_histories or []:
        points = market.get("points") or {}
        current_supply = _decimal(market.get("currentSupplyUsd"))
        latest_market_timestamp = max(points, default=0)
        if (
            current_supply > MIN_ACTIVE_SUPPLY_USD
            and latest_market_timestamp < freshness_cutoff
        ):
            stale_active.append(market)
        else:
            recent.append(market)
    return recent, stale_active


def aggregate_market_histories(market_histories):
    """Sum official USD histories at timestamps covered by every fresh active market."""
    recent_histories, _ = split_recent_market_histories(market_histories)
    prepared = []
    required_timestamp_sets = []
    for market in recent_histories:
        points = {
            int(timestamp): _decimal(value)
            for timestamp, value in (market.get("points") or {}).items()
            if int(timestamp) > 0 and _decimal(value) >= 0
        }
        current_supply = _decimal(market.get("currentSupplyUsd"))
        prepared.append((market, points))
        if current_supply > MIN_ACTIVE_SUPPLY_USD:
            if not points:
                raise ValueError(
                    f"Active market has no official metrics: {market.get('marketKey')}"
                )
            required_timestamp_sets.append(set(points))

    if not required_timestamp_sets:
        raise ValueError("No active Dolomite markets found for Total Supply history")

    complete_timestamps = set.intersection(*required_timestamp_sets)
    if not complete_timestamps:
        raise ValueError("Official market histories have no complete timestamps")

    result = []
    for timestamp in sorted(complete_timestamps):
        total = sum(
            (points.get(timestamp, Decimal("0")) for _, points in prepared),
            Decimal("0"),
        )
        result.append(
            {
                "date": timestamp,
                "totalLiquidityUSD": _json_number(total),
            }
        )
    return result


def merge_total_supply_histories(
    defillama_history,
    official_history,
    current_timestamp,
    current_supply,
):
    """Use DeFiLlama before the official window and exact Dolomite data after it."""
    llama_values = _history_rows(defillama_history)
    official_values = _history_rows(official_history)
    if not official_values:
        raise ValueError("Official Total Supply history is empty")

    official_start = min(official_values)
    merged = {
        timestamp: value
        for timestamp, value in llama_values.items()
        if timestamp < official_start
    }
    merged.update(official_values)

    timestamp = int(current_timestamp)
    supply = _decimal(current_supply)
    if timestamp <= 0 or supply <= 0:
        raise ValueError("Current Total Supply snapshot is invalid")
    merged[timestamp] = supply

    return [
        {
            "date": timestamp,
            "totalLiquidityUSD": _json_number(merged[timestamp]),
        }
        for timestamp in sorted(merged)
    ]


def _parse_metric_rows(rows):
    points = {}
    for row in rows or []:
        try:
            timestamp = int(row.get("timestamp") or 0)
        except (AttributeError, TypeError, ValueError):
            continue
        value = _decimal(
            row.get("supplyLiquidityUSD") if isinstance(row, dict) else None
        )
        if timestamp > 0 and value >= 0:
            points[timestamp] = value
    return points


def _fetch_metric_series(market):
    url = (
        "https://api.dolomite.io/tokens/"
        f"{market['chainId']}/metrics/{market['tokenId']}/series"
    )
    attempts = len(METRICS_RETRY_DELAYS) + 1
    for attempt in range(attempts):
        try:
            response = requests.get(url, timeout=METRICS_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise ValueError("Dolomite metrics response has no data array")
            return {
                **market,
                "points": _parse_metric_rows(rows),
            }
        except (requests.RequestException, ValueError) as exc:
            if attempt == len(METRICS_RETRY_DELAYS):
                raise RuntimeError(
                    f"Official metrics failed for {market['marketKey']}: {exc}"
                ) from exc
            time.sleep(METRICS_RETRY_DELAYS[attempt])

    raise RuntimeError(f"Metrics retry loop exited for {market['marketKey']}")


def _load_market_definitions():
    markets = []
    for chain_name in ACTIVE_ASSETS_CHAINS:
        chain_id = DOLOMITE_TOKEN_API_CHAINS[chain_name]
        token_payload = fetch_token_api_json(
            f"https://api.dolomite.io/tokens/{chain_id}"
        )
        price_payload = fetch_token_api_json(
            f"https://api.dolomite.io/tokens/{chain_id}/prices"
        )
        prices = clean_price_map_from_api_prices(price_payload.get("prices", {}))
        for token in token_payload.get("tokens", []):
            token_id = str(token.get("id") or "").strip().lower()
            if not token_id:
                continue
            current_supply = _decimal(token.get("supplyLiquidity")) * _decimal(
                prices.get(token_id)
            )
            markets.append(
                {
                    "chain": chain_name,
                    "chainId": chain_id,
                    "tokenId": token_id,
                    "symbol": (
                        token.get("cleanSymbol")
                        or token.get("symbol")
                        or "UNKNOWN"
                    ),
                    "marketKey": f"{chain_name}:{token_id}",
                    "currentSupplyUsd": current_supply,
                }
            )
    return markets


def _fetch_all_market_histories(markets):
    histories = []
    failures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_metric_series, market): market
            for market in markets
        }
        for future in as_completed(futures):
            market = futures[future]
            try:
                histories.append(future.result())
            except RuntimeError as exc:
                failures.append(f"{market['marketKey']}: {exc}")
    if failures:
        sample = "; ".join(failures[:5])
        raise RuntimeError(
            f"{len(failures)} official market histories failed: {sample}"
        )
    return histories


def _read_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _snapshot_timestamp(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def main():
    llama_data = _read_json(DEFILLAMA_FILE)
    official_snapshot = _read_json(OFFICIAL_SNAPSHOT_FILE)
    markets = _load_market_definitions()
    histories = _fetch_all_market_histories(markets)
    recent_histories, stale_histories = split_recent_market_histories(histories)
    stale_market_supply = sum(
        (
            _decimal(market.get("currentSupplyUsd"))
            for market in stale_histories
        ),
        Decimal("0"),
    )
    current_supply = _decimal(official_snapshot.get("supplyLiquidity"))
    if (
        current_supply <= 0
        or stale_market_supply > current_supply * MAX_STALE_MARKET_SHARE
    ):
        raise ValueError(
            "Stale official metrics cover too much current supply: "
            f"${float(stale_market_supply):,.2f}"
        )

    official_history = aggregate_market_histories(histories)
    if len(official_history) < MIN_OFFICIAL_HISTORY_POINTS:
        raise ValueError(
            "Official Total Supply history is too short: "
            f"{len(official_history)} points"
        )

    current_timestamp = _snapshot_timestamp(official_snapshot.get("last_updated"))
    total_supply_history = merge_total_supply_histories(
        llama_data.get("totalSupply", []),
        official_history,
        current_timestamp,
        current_supply,
    )
    active_market_count = sum(
        1
        for market in markets
        if _decimal(market.get("currentSupplyUsd")) > MIN_ACTIVE_SUPPLY_USD
    )
    official_market_count = sum(
        1
        for market in recent_histories
        if _decimal(market.get("currentSupplyUsd")) > MIN_ACTIVE_SUPPLY_USD
    )
    output = {
        "schemaVersion": 1,
        "source": "defillama_then_dolomite_official_market_metrics",
        "totalSupply": total_supply_history,
        "currentSupply": _json_number(current_supply),
        "officialWindowStart": official_history[0]["date"],
        "officialMarketCount": official_market_count,
        "activeMarketCount": active_market_count,
        "allMarketCount": len(markets),
        "staleOfficialMarketCount": len(stale_histories),
        "staleOfficialMarketSupply": _json_number(stale_market_supply),
        "staleOfficialMarkets": [
            {
                "marketKey": market.get("marketKey"),
                "symbol": market.get("symbol"),
                "currentSupplyUsd": _json_number(market.get("currentSupplyUsd")),
            }
            for market in sorted(
                stale_histories,
                key=lambda row: str(row.get("marketKey")),
            )
        ],
        "last_updated": official_snapshot.get("last_updated"),
    }

    temporary_file = f"{OUTPUT_FILE}.tmp"
    with open(temporary_file, "w", encoding="utf-8") as handle:
        json.dump(output, handle, separators=(",", ":"))
    os.replace(temporary_file, OUTPUT_FILE)

    print(
        "✅ Saved full Total Supply history: "
        f"{len(total_supply_history)} points, "
        f"{len(official_history)} official daily points, "
        f"${float(current_supply):,.2f} current supply"
    )


if __name__ == "__main__":
    main()
