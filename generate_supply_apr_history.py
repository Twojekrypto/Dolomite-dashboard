#!/usr/bin/env python3
"""Cache exact historical Dolomite market snapshots for Supply Activity.

The official token metrics series is limited to 90 rows and contains no lending
rates. This generator resolves the last protocol block at each UTC day boundary
and queries supply, debt and lending APR from the official Dolomite subgraph at
that exact block. The browser reads the static cache instead of issuing
historical GraphQL requests.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib import request
from urllib.error import HTTPError, URLError

from generate_supply_history import DEFAULT_GRAPH_CHAINS, GRAPH_ENDPOINTS


getcontext().prec = 50
SECONDS_PER_DAY = 86400

TRANSACTION_BLOCK_QUERY = """
query SupplyAprBlock($timestamp: BigInt!) {
  transactions(
    first: 1
    orderBy: timestamp
    orderDirection: desc
    where: { timestamp_lte: $timestamp }
  ) {
    timestamp
    blockNumber
  }
}
"""

MARKET_SNAPSHOT_QUERY = """
query SupplyMarketSnapshot($block: Int!) {
  interestRates(first: 1000, block: { number: $block }) {
    token { id }
    supplyInterestRate
  }
  tokens(first: 1000, block: { number: $block }, where: { marketId_not: null }) {
    id
    supplyLiquidity
    borrowLiquidity
  }
}
"""


def post_json(url: str, payload: Dict[str, Any], retries: int = 4) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        req = request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "DolomiteDashboardSupplyApr/1.0",
            },
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=60) as response:
                parsed = json.loads(response.read().decode("utf-8"))
            if parsed.get("errors"):
                raise RuntimeError(parsed["errors"][0].get("message", "GraphQL error"))
            return parsed.get("data") or {}
        except (HTTPError, URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Graph request failed after {retries} attempts: {last_error}") from last_error


def decimal_or_none(value: Any) -> Optional[Decimal]:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def decimal_to_plain(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def fetch_rate_snapshot(
    endpoint: str,
    timestamp: int,
    request_fn: Callable[[str, Dict[str, Any]], Dict[str, Any]] = post_json,
) -> Dict[str, Any]:
    block_data = request_fn(
        endpoint,
        {"query": TRANSACTION_BLOCK_QUERY, "variables": {"timestamp": str(int(timestamp))}},
    )
    transactions = list(block_data.get("transactions") or [])
    if not transactions:
        raise RuntimeError(f"No protocol transaction at or before timestamp {timestamp}")
    source_timestamp = int(transactions[0].get("timestamp") or 0)
    block_number = int(transactions[0].get("blockNumber") or 0)
    if source_timestamp <= 0 or source_timestamp > timestamp or block_number <= 0:
        raise RuntimeError(f"Invalid historical block evidence for timestamp {timestamp}")

    snapshot_data = request_fn(
        endpoint,
        {"query": MARKET_SNAPSHOT_QUERY, "variables": {"block": block_number}},
    )
    rates: Dict[str, str] = {}
    for row in snapshot_data.get("interestRates") or []:
        token_id = str((row.get("token") or {}).get("id") or "").lower()
        fraction = decimal_or_none(row.get("supplyInterestRate"))
        if not token_id or fraction is None or fraction < 0:
            continue
        rates[token_id] = decimal_to_plain(fraction * Decimal(100))
    markets: Dict[str, Dict[str, str]] = {}
    for row in snapshot_data.get("tokens") or []:
        token_id = str(row.get("id") or "").lower()
        supply = decimal_or_none(row.get("supplyLiquidity"))
        debt = decimal_or_none(row.get("borrowLiquidity"))
        if not token_id or supply is None or debt is None or supply < 0 or debt < 0:
            continue
        markets[token_id] = {
            "supply": decimal_to_plain(supply),
            "debt": decimal_to_plain(debt),
        }
    if not rates:
        raise RuntimeError(f"No Dolomite supply rates at block {block_number}")
    if not markets:
        raise RuntimeError(f"No Dolomite market liquidity at block {block_number}")
    return {
        "timestamp": int(timestamp),
        "sourceTimestamp": source_timestamp,
        "blockNumber": block_number,
        "rates": rates,
        "markets": markets,
    }


def load_chain_history(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"points": []}
    if not isinstance(payload, dict) or not isinstance(payload.get("points"), list):
        return {"points": []}
    return payload


def build_snapshot_targets(
    *,
    now_timestamp: int,
    days: int,
    refresh_days: int,
    existing: List[Dict[str, Any]],
) -> List[int]:
    current_day = (int(now_timestamp) // SECONDS_PER_DAY) * SECONDS_PER_DAY
    first_day = current_day - (max(1, days) - 1) * SECONDS_PER_DAY
    refresh_from = current_day - (max(1, refresh_days) - 1) * SECONDS_PER_DAY
    existing_days = {
        int(point.get("timestamp") or 0)
        for point in existing
        if first_day <= int(point.get("timestamp") or 0) <= current_day
        and isinstance(point.get("rates"), dict)
        and bool(point.get("rates"))
        and isinstance(point.get("markets"), dict)
        and bool(point.get("markets"))
    }
    return [
        timestamp
        for timestamp in range(first_day, current_day + 1, SECONDS_PER_DAY)
        if timestamp not in existing_days or timestamp >= refresh_from
    ]


def merge_snapshot_rows(
    existing: List[Dict[str, Any]],
    refreshed: List[Dict[str, Any]],
    minimum_timestamp: int,
) -> List[Dict[str, Any]]:
    by_timestamp: Dict[int, Dict[str, Any]] = {}
    for point in [*existing, *refreshed]:
        timestamp = int(point.get("timestamp") or 0)
        rates = point.get("rates")
        markets = point.get("markets")
        if (
            timestamp < minimum_timestamp
            or not isinstance(rates, dict)
            or not rates
            or not isinstance(markets, dict)
            or not markets
        ):
            continue
        safe_markets: Dict[str, Dict[str, str]] = {}
        for token_id, market in markets.items():
            supply = decimal_or_none((market or {}).get("supply") if isinstance(market, dict) else None)
            debt = decimal_or_none((market or {}).get("debt") if isinstance(market, dict) else None)
            if not str(token_id).strip() or supply is None or debt is None or supply < 0 or debt < 0:
                continue
            safe_markets[str(token_id).lower()] = {
                "supply": decimal_to_plain(supply),
                "debt": decimal_to_plain(debt),
            }
        if not safe_markets:
            continue
        by_timestamp[timestamp] = {
            "timestamp": timestamp,
            "sourceTimestamp": int(point.get("sourceTimestamp") or 0),
            "blockNumber": int(point.get("blockNumber") or 0),
            "rates": {
                str(token_id).lower(): str(rate)
                for token_id, rate in rates.items()
                if str(token_id).strip() and decimal_or_none(rate) is not None
            },
            "markets": safe_markets,
        }
    return [by_timestamp[timestamp] for timestamp in sorted(by_timestamp)]


def write_chain_history(
    out_dir: Path,
    chain: str,
    refreshed: List[Dict[str, Any]],
    *,
    generated_at: str,
    minimum_timestamp: int,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{chain}.json"
    existing = load_chain_history(path)
    points = merge_snapshot_rows(existing.get("points") or [], refreshed, minimum_timestamp)
    payload = {
        "schemaVersion": 2,
        "source": "official-dolomite-subgraph",
        "method": "historical-block-market-liquidity-and-supplyInterestRate",
        "chain": chain,
        "generatedAt": generated_at,
        "points": points,
    }
    path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
    return payload


def generate_chain(
    chain: str,
    endpoint: str,
    out_dir: Path,
    *,
    now_timestamp: int,
    days: int,
    refresh_days: int,
    workers: int,
    generated_at: str,
) -> Dict[str, Any]:
    path = out_dir / f"{chain}.json"
    existing = load_chain_history(path)
    existing_points = existing.get("points") or []
    targets = build_snapshot_targets(
        now_timestamp=now_timestamp,
        days=days,
        refresh_days=refresh_days,
        existing=existing_points,
    )
    existing_days = {
        int(point.get("timestamp") or 0)
        for point in existing_points
        if isinstance(point.get("rates"), dict)
        and bool(point.get("rates"))
        and isinstance(point.get("markets"), dict)
        and bool(point.get("markets"))
    }
    refreshed: List[Dict[str, Any]] = []
    unresolved: List[int] = []
    print(f"{chain}: refreshing {len(targets)} market day snapshots", flush=True)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(fetch_rate_snapshot, endpoint, timestamp): timestamp for timestamp in targets}
        for future in as_completed(futures):
            timestamp = futures[future]
            try:
                refreshed.append(future.result())
            except RuntimeError as exc:
                if timestamp not in existing_days:
                    unresolved.append(timestamp)
                print(f"{chain}: market snapshot {timestamp} unavailable ({exc})", file=sys.stderr, flush=True)

    current_day = (now_timestamp // SECONDS_PER_DAY) * SECONDS_PER_DAY
    first_day = current_day - (max(1, days) - 1) * SECONDS_PER_DAY
    payload = write_chain_history(
        out_dir,
        chain,
        refreshed,
        generated_at=generated_at,
        minimum_timestamp=first_day,
    )
    return {
        "chain": chain,
        "points": len(payload["points"]),
        "refreshed": len(refreshed),
        "unresolved": unresolved,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate exact Dolomite Supply market history")
    parser.add_argument("--out-dir", default="data/supply-apr")
    parser.add_argument("--chains", default=os.environ.get("SUPPLY_APR_CHAINS", ",".join(DEFAULT_GRAPH_CHAINS)))
    parser.add_argument("--days", type=int, default=int(os.environ.get("SUPPLY_APR_DAYS", "200")))
    parser.add_argument("--refresh-days", type=int, default=int(os.environ.get("SUPPLY_APR_REFRESH_DAYS", "7")))
    parser.add_argument("--workers", type=int, default=int(os.environ.get("SUPPLY_APR_WORKERS", "4")))
    args = parser.parse_args()

    if args.days < 2 or args.refresh_days < 1 or args.workers < 1:
        parser.error("days must be >= 2; refresh-days and workers must be >= 1")
    chains = [chain.strip() for chain in args.chains.split(",") if chain.strip()]
    unknown = [chain for chain in chains if chain not in GRAPH_ENDPOINTS]
    if unknown:
        parser.error(f"unknown chains: {', '.join(unknown)}")

    now = datetime.now(timezone.utc)
    generated_at = now.isoformat().replace("+00:00", "Z")
    results = []
    for chain in chains:
        results.append(
            generate_chain(
                chain,
                GRAPH_ENDPOINTS[chain],
                Path(args.out_dir),
                now_timestamp=int(now.timestamp()),
                days=args.days,
                refresh_days=args.refresh_days,
                workers=args.workers,
                generated_at=generated_at,
            )
        )

    unresolved = [(result["chain"], timestamp) for result in results for timestamp in result["unresolved"]]
    for result in results:
        print(
            f"{result['chain']}: {result['points']} cached days, "
            f"{result['refreshed']} refreshed, {len(result['unresolved'])} unresolved",
            flush=True,
        )
    if unresolved:
        print(f"Missing {len(unresolved)} required market snapshots; refusing partial publication", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
