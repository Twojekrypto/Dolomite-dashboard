#!/usr/bin/env python3
"""Generate static Supply Pool Health data for the dashboard.

For every Dolomite supply market this script fetches current per-wallet
balances from the subgraph (marginAccountTokenValues), computes concentration
metrics (Top 10 %, largest wallet %, Gini) and a weighted Supply Quality Score,
and enriches them with 7d/30d trends from `data/supply-history`.

Wallet-count history is not available from the subgraph, so each run appends a
daily snapshot per market to `data/supply-health/history.json`; wallet-growth
trends appear once enough snapshots have accumulated.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import request
from urllib.error import HTTPError, URLError


DOLOMITE_SUPPLY_BASE = (
    "https://subgraph.api.dolomite.io/api/public/"
    "1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs"
)

GRAPH_ENDPOINTS = {
    "berachain": f"{DOLOMITE_SUPPLY_BASE}/dolomite-berachain-mainnet/latest/gn",
    "arbitrum": f"{DOLOMITE_SUPPLY_BASE}/dolomite-arbitrum/latest/gn",
    "mantle": f"{DOLOMITE_SUPPLY_BASE}/dolomite-mantle/latest/gn",
    "ethereum": f"{DOLOMITE_SUPPLY_BASE}/dolomite-ethereum/latest/gn",
    "botanix": f"{DOLOMITE_SUPPLY_BASE}/dolomite-botanix/latest/gn",
    "xlayer": f"{DOLOMITE_SUPPLY_BASE}/dolomite-x-layer/latest/gn",
    "polygon_zkevm": f"{DOLOMITE_SUPPLY_BASE}/dolomite-polygon-zkevm/latest/gn",
}
RETIRED_GRAPH_CHAINS = {"polygon_zkevm"}
DEFAULT_GRAPH_CHAINS = [chain for chain in GRAPH_ENDPOINTS if chain not in RETIRED_GRAPH_CHAINS]

HISTORY_MAX_SNAPSHOTS = 400
TOP_WALLETS_IN_PAYLOAD = 10

BUNDLE_QUERY = """
{
  tokens(first: 1000, where:{marketId_not: null}, orderBy: supplyLiquidityUSD, orderDirection: desc) {
    id
    symbol
    name
    decimals
    marketId
    supplyLiquidity
    supplyLiquidityUSD
  }
  interestIndexes(first: 1000) { token { id } supplyIndex }
  oraclePrices(first: 1000) { token { id } price }
}
"""

# Score weights from the community proposal (Discord, 2026-06-29).
SCORE_WEIGHTS = {
    "wallet": 0.25,
    "concentration": 0.30,
    "stability": 0.20,
    "growth": 0.15,
    "resilience": 0.10,
}

LARGEST_WALLET_TABLE = [(5.0, 100.0), (10.0, 90.0), (20.0, 70.0), (30.0, 50.0), (40.0, 30.0), (50.0, 0.0)]
TOP10_TABLE = [(20.0, 100.0), (30.0, 90.0), (40.0, 80.0), (50.0, 65.0), (60.0, 45.0), (70.0, 25.0), (80.0, 0.0)]
STABILITY_TABLE = [(1.0, 100.0), (2.0, 90.0), (4.0, 75.0), (6.0, 60.0), (8.0, 40.0), (10.0, 20.0), (20.0, 0.0)]
GROWTH_TABLE = [(-20.0, 10.0), (-10.0, 35.0), (0.0, 60.0), (5.0, 75.0), (10.0, 90.0), (20.0, 100.0)]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def decimal_or_zero(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def float_or_zero(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else 0.0


def round_or_none(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def post_json(url: str, payload: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        req = request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "DolomiteDashboardSupplyHealth/1.0"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=60) as res:
                raw = res.read().decode("utf-8")
            parsed = json.loads(raw)
            if parsed.get("errors"):
                raise RuntimeError(parsed["errors"][0].get("message", "GraphQL error"))
            return parsed.get("data") or {}
        except (HTTPError, URLError, TimeoutError, OSError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Graph request failed after {retries} attempts: {last_error}") from last_error


def fetch_bundle(endpoint: str) -> Tuple[List[Dict[str, Any]], Dict[str, float], Dict[str, float]]:
    data = post_json(endpoint, {"query": BUNDLE_QUERY})
    indexes: Dict[str, float] = {}
    for row in data.get("interestIndexes") or []:
        token_id = str((row.get("token") or {}).get("id") or "").lower()
        if token_id:
            indexes[token_id] = float_or_zero(row.get("supplyIndex"))
    prices: Dict[str, float] = {}
    for row in data.get("oraclePrices") or []:
        token_id = str((row.get("token") or {}).get("id") or "").lower()
        if token_id:
            prices[token_id] = float_or_zero(row.get("price"))
    return list(data.get("tokens") or []), indexes, prices


def fetch_wallet_balances(endpoint: str, token_id: str, max_pages: int = 60) -> Dict[str, float]:
    """Sum current supply par per effective user for one market."""
    balances: Dict[str, float] = {}
    page_size = 1000
    skip = 0
    for _ in range(max_pages):
        query = f"""
        {{
          marginAccountTokenValues(first: {page_size}, skip: {skip}, where: {{ token: "{token_id}", valuePar_gt: "0" }}, orderBy: valuePar, orderDirection: desc) {{
            valuePar
            marginAccount {{
              effectiveUser {{ id }}
              user {{ id }}
            }}
          }}
        }}
        """
        rows = post_json(endpoint, {"query": query}).get("marginAccountTokenValues") or []
        for row in rows:
            account = row.get("marginAccount") or {}
            address = str(
                (account.get("effectiveUser") or {}).get("id")
                or (account.get("user") or {}).get("id")
                or ""
            ).lower()
            if not address:
                continue
            par = float_or_zero(row.get("valuePar"))
            if par > 0:
                balances[address] = balances.get(address, 0.0) + par
        if len(rows) < page_size:
            return balances
        skip += page_size
    raise RuntimeError(f"marginAccountTokenValues exceeded max_pages={max_pages} for token {token_id}")


def interpolate_score(table: List[Tuple[float, float]], value: float) -> float:
    """Piecewise-linear score lookup; clamps outside the table range."""
    points = sorted(table)
    if value <= points[0][0]:
        return points[0][1]
    if value >= points[-1][0]:
        return points[-1][1]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if x0 <= value <= x1:
            if x1 == x0:
                return y1
            return y0 + (y1 - y0) * (value - x0) / (x1 - x0)
    return points[-1][1]


def clamp_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def gini_coefficient(sorted_ascending: List[float]) -> Optional[float]:
    n = len(sorted_ascending)
    total = sum(sorted_ascending)
    if n < 2 or total <= 0:
        return None
    weighted = sum(rank * value for rank, value in enumerate(sorted_ascending, start=1))
    return (2.0 * weighted) / (n * total) - (n + 1.0) / n


def load_history_points(history_dir: Path, chain: str, token_id: str) -> List[Tuple[int, float]]:
    path = history_dir / chain / f"{token_id.lower()}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    points = []
    for point in payload.get("points") or []:
        ts = int(point.get("timestamp") or 0)
        value = float_or_zero(point.get("tokenValue") if point.get("tokenValue") is not None else point.get("value"))
        if ts > 0:
            points.append((ts, value))
    points.sort()
    return points


def value_at_days_ago(points: List[Tuple[int, float]], now_ts: int, days: int, tolerance_days: float) -> Optional[float]:
    """Value closest to `days` ago; None when history does not reach that far."""
    target = now_ts - days * 86400
    eligible = [(abs(ts - target), value) for ts, value in points if ts <= now_ts]
    if not eligible:
        return None
    distance, value = min(eligible)
    if distance > tolerance_days * 86400:
        return None
    return value


def growth_pct(current: float, past: Optional[float]) -> Optional[float]:
    if past is None or past <= 0:
        return None
    return (current - past) / past * 100.0


def avg_daily_change_pct(points: List[Tuple[int, float]], now_ts: int, days: int = 30) -> Optional[float]:
    cutoff = now_ts - days * 86400
    window = [(ts, value) for ts, value in points if ts >= cutoff]
    if len(window) < 5:
        return None
    changes = []
    for (_, prev), (_, curr) in zip(window, window[1:]):
        if prev > 0:
            changes.append(abs(curr - prev) / prev * 100.0)
    if not changes:
        return None
    return sum(changes) / len(changes)


def score_market(
    wallet_percentile: Optional[float],
    largest_pct: Optional[float],
    top10_pct: Optional[float],
    stability_pct: Optional[float],
    supply_growth_30d: Optional[float],
    wallet_growth_30d: Optional[float],
) -> Dict[str, Any]:
    components: Dict[str, Optional[float]] = {
        "wallet": clamp_score(wallet_percentile) if wallet_percentile is not None else None,
        "concentration": None,
        "stability": None,
        "growth": None,
        "resilience": None,
    }
    if largest_pct is not None and top10_pct is not None:
        components["concentration"] = clamp_score(
            (interpolate_score(LARGEST_WALLET_TABLE, largest_pct) + interpolate_score(TOP10_TABLE, top10_pct)) / 2.0
        )
    if stability_pct is not None:
        components["stability"] = clamp_score(interpolate_score(STABILITY_TABLE, stability_pct))
    growth_parts = [
        interpolate_score(GROWTH_TABLE, value)
        for value in (supply_growth_30d, wallet_growth_30d)
        if value is not None
    ]
    if growth_parts:
        components["growth"] = clamp_score(sum(growth_parts) / len(growth_parts))
    if largest_pct is not None:
        components["resilience"] = clamp_score(100.0 - largest_pct)

    available_weight = sum(SCORE_WEIGHTS[key] for key, value in components.items() if value is not None)
    total: Optional[float] = None
    if available_weight > 0:
        total = sum(
            SCORE_WEIGHTS[key] * value for key, value in components.items() if value is not None
        ) / available_weight
    grade = None
    if total is not None:
        grade = "A" if total >= 85 else "B" if total >= 70 else "C" if total >= 55 else "D" if total >= 40 else "F"
    return {
        "wallet": round_or_none(components["wallet"], 1),
        "concentration": round_or_none(components["concentration"], 1),
        "stability": round_or_none(components["stability"], 1),
        "growth": round_or_none(components["growth"], 1),
        "resilience": round_or_none(components["resilience"], 1),
        "total": round_or_none(total, 1),
        "grade": grade,
        "componentsUsed": [key for key, value in components.items() if value is not None],
    }


def build_market_entry(
    chain: str,
    token: Dict[str, Any],
    balances_par: Dict[str, float],
    supply_index: float,
    price_usd: float,
    dust_usd: float,
) -> Optional[Dict[str, Any]]:
    token_id = str(token.get("id") or "").lower()
    if not token_id:
        return None
    index = supply_index if supply_index > 0 else 1.0
    wallets = []
    for address, par in balances_par.items():
        amount = par * index
        usd = amount * price_usd
        wallets.append((address, amount, usd))
    if price_usd > 0 and dust_usd > 0:
        wallets = [entry for entry in wallets if entry[2] >= dust_usd]
    if not wallets:
        return None
    wallets.sort(key=lambda entry: entry[1], reverse=True)

    amounts = [entry[1] for entry in wallets]
    total_amount = sum(amounts)
    total_usd = total_amount * price_usd
    reported_usd = float_or_zero(token.get("supplyLiquidityUSD"))
    if price_usd <= 0 and reported_usd > 0 and float_or_zero(token.get("supplyLiquidity")) > 0:
        # No oracle price row: fall back to the token's reported USD ratio.
        implied_price = reported_usd / float_or_zero(token.get("supplyLiquidity"))
        total_usd = total_amount * implied_price
        price_usd = implied_price
        wallets = [(address, amount, amount * implied_price) for address, amount, _ in wallets]

    count = len(wallets)
    largest_pct = (amounts[0] / total_amount * 100.0) if total_amount > 0 else None
    top10_pct = (sum(amounts[:10]) / total_amount * 100.0) if total_amount > 0 else None
    gini = gini_coefficient(sorted(amounts))
    top_wallets = [
        {
            "address": address,
            "amount": round(amount, 6),
            "usd": round(usd, 2),
            "sharePct": round_or_none(amount / total_amount * 100.0 if total_amount > 0 else None, 2),
        }
        for address, amount, usd in wallets[:TOP_WALLETS_IN_PAYLOAD]
    ]
    return {
        "chain": chain,
        "tokenId": token_id,
        "symbol": token.get("symbol"),
        "name": token.get("name"),
        "marketId": token.get("marketId"),
        "priceUsd": round(price_usd, 8),
        "supplyToken": round(total_amount, 6),
        "supplyUsd": round(total_usd, 2),
        "wallets": count,
        "avgWalletUsd": round(total_usd / count, 2) if count else None,
        "medianWalletUsd": round(sorted(entry[2] for entry in wallets)[count // 2], 2) if count else None,
        "largestPct": round_or_none(largest_pct, 2),
        "top10Pct": round_or_none(top10_pct, 2),
        "gini": round_or_none(gini, 4),
        "topWallets": top_wallets,
    }


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def update_wallet_history(
    history: Dict[str, Any],
    markets: List[Dict[str, Any]],
    now_ts: int,
) -> Dict[str, Any]:
    snapshots = history.get("markets") if isinstance(history.get("markets"), dict) else {}
    day_ts = (now_ts // 86400) * 86400
    for market in markets:
        key = f"{market['chain']}:{market['tokenId']}"
        rows = [row for row in (snapshots.get(key) or []) if isinstance(row, list) and len(row) >= 3]
        if rows and int(rows[-1][0]) == day_ts:
            rows[-1] = [day_ts, market["wallets"], market["supplyUsd"]]
        else:
            rows.append([day_ts, market["wallets"], market["supplyUsd"]])
        snapshots[key] = rows[-HISTORY_MAX_SNAPSHOTS:]
    return {"schemaVersion": 1, "markets": snapshots}


def wallet_growth_from_history(
    snapshots: Dict[str, Any],
    market: Dict[str, Any],
    now_ts: int,
    days: int,
) -> Optional[float]:
    key = f"{market['chain']}:{market['tokenId']}"
    rows = snapshots.get(key) or []
    points = [(int(row[0]), float(row[1])) for row in rows if isinstance(row, list) and len(row) >= 2]
    past = value_at_days_ago(points, now_ts, days, tolerance_days=max(2.0, days * 0.2))
    return growth_pct(float(market["wallets"]), past)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Dolomite Supply Pool Health data")
    parser.add_argument("--out-dir", default="data/supply-health")
    parser.add_argument("--history-dir", default="data/supply-history")
    parser.add_argument("--chains", default=os.environ.get("SUPPLY_HEALTH_CHAINS", ",".join(DEFAULT_GRAPH_CHAINS)))
    parser.add_argument("--min-supply-usd", type=float, default=float(os.environ.get("SUPPLY_HEALTH_MIN_SUPPLY_USD", "100")))
    parser.add_argument("--dust-usd", type=float, default=float(os.environ.get("SUPPLY_HEALTH_DUST_USD", "1")))
    parser.add_argument("--max-workers", type=int, default=int(os.environ.get("SUPPLY_HEALTH_MAX_WORKERS", "4")))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    history_dir = Path(args.history_dir)
    generated_at = utc_now()
    now_ts = int(generated_at.timestamp())
    chains = [chain.strip() for chain in args.chains.split(",") if chain.strip()]

    markets: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for chain in chains:
        endpoint = GRAPH_ENDPOINTS.get(chain)
        if not endpoint:
            print(f"Unknown chain: {chain}", file=sys.stderr)
            return 2
        try:
            tokens, indexes, prices = fetch_bundle(endpoint)
        except RuntimeError as exc:
            errors.append({"chain": chain, "stage": "bundle", "detail": str(exc)[:500]})
            print(f"{chain}: bundle fetch failed ({exc})", file=sys.stderr, flush=True)
            continue
        selected = [
            token for token in tokens
            if float_or_zero(token.get("supplyLiquidityUSD")) >= args.min_supply_usd
        ]
        print(f"{chain}: computing health for {len(selected)} / {len(tokens)} markets", flush=True)

        def process(token: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            token_id = str(token.get("id") or "").lower()
            balances = fetch_wallet_balances(endpoint, token_id)
            return build_market_entry(
                chain,
                token,
                balances,
                indexes.get(token_id, 0.0),
                prices.get(token_id, 0.0),
                args.dust_usd,
            )

        with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
            futures = {executor.submit(process, token): token for token in selected}
            for future, token in futures.items():
                symbol = str(token.get("symbol") or "")[:12]
                try:
                    entry = future.result()
                except RuntimeError as exc:
                    errors.append({"chain": chain, "symbol": symbol, "stage": "balances", "detail": str(exc)[:500]})
                    print(f"{chain} {symbol}: skipped ({exc})", file=sys.stderr, flush=True)
                    continue
                if entry:
                    markets.append(entry)
                    print(f"{chain} {symbol}: {entry['wallets']} wallets, top10 {entry['top10Pct']}%", flush=True)

    if not markets:
        print("No markets produced — refusing to overwrite existing data", file=sys.stderr)
        return 1

    history_payload = load_json(out_dir / "history.json") or {}
    history_payload = update_wallet_history(history_payload, markets, now_ts)
    snapshots = history_payload["markets"]

    # Wallet-count percentile across all pools (the proposal's preferred
    # normalization) — computed after all chains so it adapts as Dolomite grows.
    counts = sorted(market["wallets"] for market in markets)
    n = len(counts)
    for market in markets:
        below = sum(1 for count in counts if count < market["wallets"])
        percentile = (below / (n - 1) * 100.0) if n > 1 else 100.0

        points = load_history_points(history_dir, market["chain"], market["tokenId"])
        current_supply = points[-1][1] if points else market["supplyToken"]
        supply_7d = growth_pct(current_supply, value_at_days_ago(points, now_ts, 7, tolerance_days=2.5))
        supply_30d = growth_pct(current_supply, value_at_days_ago(points, now_ts, 30, tolerance_days=6.0))
        stability = avg_daily_change_pct(points, now_ts)
        wallets_7d = wallet_growth_from_history(snapshots, market, now_ts, 7)
        wallets_30d = wallet_growth_from_history(snapshots, market, now_ts, 30)

        market["growth"] = {
            "supply7dPct": round_or_none(supply_7d),
            "supply30dPct": round_or_none(supply_30d),
            "wallets7dPct": round_or_none(wallets_7d),
            "wallets30dPct": round_or_none(wallets_30d),
            "avgDailyChange30dPct": round_or_none(stability),
        }
        market["score"] = score_market(
            percentile,
            market["largestPct"],
            market["top10Pct"],
            stability,
            supply_30d,
            wallets_30d,
        )

    markets.sort(key=lambda market: (-float(market["supplyUsd"] or 0), str(market["symbol"] or "")))
    latest = {
        "schemaVersion": 1,
        "source": "static-subgraph-health",
        "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
        "dustUsdThreshold": args.dust_usd,
        "minSupplyUsd": args.min_supply_usd,
        "chains": chains,
        "marketCount": len(markets),
        "errors": errors,
        "markets": markets,
    }
    (out_dir / "latest.json").write_text(json.dumps(latest, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")

    history_payload["updatedAt"] = generated_at.isoformat().replace("+00:00", "Z")
    (out_dir / "history.json").write_text(json.dumps(history_payload, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote {len(markets)} markets to {out_dir / 'latest.json'} ({len(errors)} errors)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
