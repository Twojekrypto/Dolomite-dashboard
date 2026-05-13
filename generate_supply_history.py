#!/usr/bin/env python3
"""Generate static Supply Liquidity history files for the dashboard.

The live Supply chart should stay fast in the browser. This script precomputes
daily token-unit supply history from Dolomite subgraph events so the frontend
can load wider history from GitHub Pages instead of replaying a market in the
user's tab.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import request
from urllib.error import HTTPError, URLError


getcontext().prec = 50

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

DEFAULT_SYMBOLS = {
    "USD1",
    "USDC",
    "USDC.E",
    "USDC.e",
    "USDT",
    "USD₮0",
    "WETH",
    "WBTC",
    "LINK",
    "DOLO",
    "WLFI",
    "WBERA",
    "stBTC",
    "pBTC",
    "oUSDT",
    "mETH",
    "cmETH",
    "weETH",
    "UNI",
}

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
    totalPar { supplyPar }
  }
  interestIndexes(first: 1000) {
    token { id }
    supplyIndex
  }
}
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def decimal_or_zero(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def decimal_to_string(value: Decimal) -> str:
    if not value.is_finite():
        return "0"
    if value == 0:
        return "0"
    quantized = value.quantize(Decimal("0.00000001")) if abs(value) < Decimal("1000000000000") else value.quantize(Decimal("0.0001"))
    text = format(quantized.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def post_json(url: str, payload: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        req = request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "DolomiteDashboardSupplyHistory/1.0"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=60) as res:
                raw = res.read().decode("utf-8")
            parsed = json.loads(raw)
            if parsed.get("errors"):
                raise RuntimeError(parsed["errors"][0].get("message", "GraphQL error"))
            return parsed.get("data") or {}
        except (HTTPError, URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Graph request failed after {retries} attempts: {last_error}") from last_error


def fetch_bundle(endpoint: str) -> Tuple[List[Dict[str, Any]], Dict[str, Decimal]]:
    data = post_json(endpoint, {"query": BUNDLE_QUERY})
    indexes: Dict[str, Decimal] = {}
    for row in data.get("interestIndexes") or []:
        token_id = str((row.get("token") or {}).get("id") or "").lower()
        if token_id:
            indexes[token_id] = decimal_or_zero(row.get("supplyIndex"))
    return list(data.get("tokens") or []), indexes


def paginate_entity(
    endpoint: str,
    entity_name: str,
    where_clause: str,
    fields: str,
    *,
    order_by: str = "serialId",
    order_direction: str = "desc",
    page_size: int = 1000,
    max_pages: int = 250,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    skip = 0
    pages = 0
    while True:
        query = f"""
        {{
          {entity_name}(first: {page_size}, skip: {skip}, orderBy: {order_by}, orderDirection: {order_direction}, where: {{ {where_clause} }}) {{
            {fields}
          }}
        }}
        """
        data = post_json(endpoint, {"query": query})
        chunk = list(data.get(entity_name) or [])
        rows.extend(chunk)
        pages += 1
        if len(chunk) < page_size:
            break
        if pages >= max_pages:
            raise RuntimeError(f"{entity_name} exceeded max_pages={max_pages} for {where_clause}")
        skip += page_size
    return rows


def compress_daily(points: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    safe_points = sorted(
        (
            {
                "timestamp": int(point["timestamp"]),
                "value": decimal_or_zero(point["value"]),
            }
            for point in points
            if int(point.get("timestamp") or 0) > 0
        ),
        key=lambda point: point["timestamp"],
    )
    deduped: List[Dict[str, Any]] = []
    for point in safe_points:
        if deduped and deduped[-1]["timestamp"] == point["timestamp"]:
            deduped[-1] = point
        else:
            deduped.append(point)

    daily: List[Dict[str, Any]] = []
    last_day: Optional[int] = None
    last_point: Optional[Dict[str, Any]] = None
    for point in deduped:
        day = point["timestamp"] // 86400
        if last_day is None:
            last_day = day
            last_point = point
            continue
        if day != last_day and last_point is not None:
            daily.append(last_point)
            last_day = day
        last_point = point
    if last_point is not None:
        daily.append(last_point)

    return [
        {
            "timestamp": point["timestamp"],
            "value": decimal_to_string(max(Decimal(0), point["value"])),
            "tokenValue": decimal_to_string(max(Decimal(0), point["value"])),
        }
        for point in (daily if len(daily) >= 2 else deduped)
    ]


def build_history_points(
    token: Dict[str, Any],
    supply_index: Decimal,
    deposits: List[Dict[str, Any]],
    withdrawals: List[Dict[str, Any]],
    snapshots: List[Dict[str, Any]],
    current_timestamp: int,
) -> List[Dict[str, Any]]:
    current_supply = decimal_or_zero(token.get("supplyLiquidity"))
    current_supply_par = decimal_or_zero((token.get("totalPar") or {}).get("supplyPar"))
    current_supply_index = supply_index
    if current_supply_index <= 0 or current_supply_par < 0:
        return []

    groups: Dict[int, Dict[str, Decimal]] = {}

    def ensure_group(timestamp: Any) -> Optional[Dict[str, Decimal]]:
        ts = int(timestamp or 0)
        if ts <= 0:
            return None
        if ts not in groups:
            groups[ts] = {
                "deltaPar": Decimal(0),
                "snapshotIndex": Decimal(0),
                "eventIndex": Decimal(0),
            }
        return groups[ts]

    def register_event(event: Dict[str, Any]) -> None:
        group = ensure_group((event.get("transaction") or {}).get("timestamp"))
        if group is None:
            return
        group["deltaPar"] += decimal_or_zero(event.get("amountDeltaPar"))
        event_index = decimal_or_zero(event.get("interestIndex"))
        if event_index > group["eventIndex"]:
            group["eventIndex"] = event_index

    for event in deposits:
        register_event(event)
    for event in withdrawals:
        register_event(event)
    for snapshot in snapshots:
        group = ensure_group(snapshot.get("updateTimestamp"))
        if group is None:
            continue
        snapshot_index = decimal_or_zero(snapshot.get("supplyIndex"))
        if snapshot_index > group["snapshotIndex"]:
            group["snapshotIndex"] = snapshot_index

    if not groups:
        return []

    rolling_par = current_supply_par
    rolling_index = current_supply_index
    points_desc: List[Dict[str, Any]] = [
        {
            "timestamp": current_timestamp,
            "value": max(Decimal(0), current_supply),
        }
    ]

    for timestamp in sorted(groups.keys(), reverse=True):
        group = groups[timestamp]
        point_index = group["snapshotIndex"] if group["snapshotIndex"] > 0 else group["eventIndex"]
        if point_index <= 0:
            point_index = rolling_index
        if point_index <= 0:
            continue
        points_desc.append(
            {
                "timestamp": timestamp,
                "value": max(Decimal(0), rolling_par * point_index),
            }
        )
        if group["deltaPar"] != 0:
            rolling_par = max(Decimal(0), rolling_par - group["deltaPar"])
        rolling_index = point_index

    return compress_daily(reversed(points_desc))


def token_matches(token: Dict[str, Any], explicit_tokens: set[str], explicit_symbols: set[str]) -> bool:
    token_id = str(token.get("id") or "").lower()
    symbol = str(token.get("symbol") or "")
    return token_id in explicit_tokens or symbol in explicit_symbols


def select_tokens(
    tokens: List[Dict[str, Any]],
    token_limit: int,
    explicit_tokens: set[str],
    explicit_symbols: set[str],
    explicit_only: bool,
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def push(token: Dict[str, Any]) -> None:
        token_id = str(token.get("id") or "").lower()
        if not token_id or token_id in seen:
            return
        seen.add(token_id)
        selected.append(token)

    for token in tokens:
        if token_matches(token, explicit_tokens, explicit_symbols):
            push(token)
    if explicit_only:
        return selected
    for token in tokens[: token_limit if token_limit > 0 else len(tokens)]:
        push(token)
    return selected


def token_summary(token: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tokenId": str(token.get("id") or "").lower(),
        "symbol": token.get("symbol"),
        "marketId": token.get("marketId"),
        "supplyLiquidityUSD": decimal_to_string(decimal_or_zero(token.get("supplyLiquidityUSD"))),
    }


def write_history_file(out_dir: Path, chain: str, token: Dict[str, Any], points: List[Dict[str, Any]], generated_at: datetime) -> Path:
    token_id = str(token.get("id") or "").lower()
    chain_dir = out_dir / chain
    chain_dir.mkdir(parents=True, exist_ok=True)
    path = chain_dir / f"{token_id}.json"
    payload = {
        "schemaVersion": 1,
        "source": "static-subgraph-replay",
        "metric": "token",
        "scope": "all",
        "chain": chain,
        "tokenId": token_id,
        "symbol": token.get("symbol"),
        "name": token.get("name"),
        "marketId": token.get("marketId"),
        "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
        "points": points,
    }
    path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def generate_chain(
    chain: str,
    endpoint: str,
    out_dir: Path,
    token_limit: int,
    explicit_tokens: set[str],
    explicit_symbols: set[str],
    explicit_only: bool,
    max_pages_per_entity: int,
    generated_at: datetime,
) -> Dict[str, Any]:
    tokens, indexes = fetch_bundle(endpoint)
    selected = select_tokens(tokens, token_limit, explicit_tokens, explicit_symbols, explicit_only)
    print(f"{chain}: selected {len(selected)} / {len(tokens)} tokens", flush=True)
    written: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    common_fields = """
      transaction { timestamp blockNumber }
      amountDeltaPar
      interestIndex
    """

    def record_skip(token: Dict[str, Any], reason: str, detail: str = "") -> None:
        entry = token_summary(token)
        entry["reason"] = reason
        if detail:
            entry["detail"] = detail[:500]
        skipped.append(entry)

    for index, token in enumerate(selected, start=1):
        token_id = str(token.get("id") or "").lower()
        symbol = str(token.get("symbol") or token_id[:8])
        supply_index = indexes.get(token_id, Decimal(0))
        if supply_index <= 0:
            print(f"{chain} {symbol}: skipped missing supply index", flush=True)
            record_skip(token, "missing_supply_index")
            continue
        where = f'token: "{token_id}"'
        print(f"{chain} {symbol}: fetching {index}/{len(selected)}", flush=True)
        try:
            deposits = paginate_entity(endpoint, "deposits", where, common_fields, max_pages=max_pages_per_entity)
            withdrawals = paginate_entity(endpoint, "withdrawals", where, common_fields, max_pages=max_pages_per_entity)
            snapshots = paginate_entity(
                endpoint,
                "interestIndexSnapshots",
                where,
                """
                  updateTimestamp
                  supplyIndex
                """,
                order_by="updateTimestamp",
                order_direction="desc",
                max_pages=max_pages_per_entity,
            )
            points = build_history_points(token, supply_index, deposits, withdrawals, snapshots, int(generated_at.timestamp()))
        except RuntimeError as exc:
            print(f"{chain} {symbol}: skipped ({exc})", flush=True)
            record_skip(token, "fetch_error", str(exc))
            continue
        if len(points) < 2:
            print(f"{chain} {symbol}: skipped insufficient points", flush=True)
            record_skip(token, "insufficient_points")
            continue
        path = write_history_file(out_dir, chain, token, points, generated_at)
        entry = token_summary(token)
        entry["points"] = len(points)
        entry["path"] = str(path.as_posix())
        written.append(entry)
        print(f"{chain} {symbol}: wrote {len(points)} daily points", flush=True)
    return {
        "chain": chain,
        "tokensAvailable": len(tokens),
        "tokensWritten": len(written),
        "tokensSkipped": len(skipped),
        "tokens": written,
        "skippedTokens": skipped,
    }


def parse_csv_set(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def load_existing_manifest(out_dir: Path) -> Optional[Dict[str, Any]]:
    manifest_path = out_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def merge_chain_entry(existing: Dict[str, Any], refreshed: Dict[str, Any]) -> Dict[str, Any]:
    tokens_by_id: Dict[str, Dict[str, Any]] = {}
    skipped_by_id: Dict[str, Dict[str, Any]] = {}

    for token in existing.get("tokens") or []:
        token_id = str(token.get("tokenId") or "").lower()
        if token_id:
            tokens_by_id[token_id] = token
    for token in existing.get("skippedTokens") or []:
        token_id = str(token.get("tokenId") or "").lower()
        if token_id:
            skipped_by_id[token_id] = token

    for token in refreshed.get("tokens") or []:
        token_id = str(token.get("tokenId") or "").lower()
        if token_id:
            tokens_by_id[token_id] = token
            skipped_by_id.pop(token_id, None)
    for token in refreshed.get("skippedTokens") or []:
        token_id = str(token.get("tokenId") or "").lower()
        if token_id:
            skipped_by_id[token_id] = token
            tokens_by_id.pop(token_id, None)

    merged = {**existing, **refreshed}
    sort_key = lambda item: (str(item.get("symbol") or ""), str(item.get("tokenId") or ""))
    merged["tokens"] = sorted(tokens_by_id.values(), key=sort_key)
    merged["skippedTokens"] = sorted(skipped_by_id.values(), key=sort_key)
    merged["tokensWritten"] = len(merged["tokens"])
    merged["tokensSkipped"] = len(merged["skippedTokens"])
    return merged


def merge_manifest_results(
    existing_manifest: Optional[Dict[str, Any]],
    refreshed_results: List[Dict[str, Any]],
    replace_tokens_for_refreshed_chains: bool,
) -> List[Dict[str, Any]]:
    by_chain: Dict[str, Dict[str, Any]] = {}
    if existing_manifest:
        for chain_entry in existing_manifest.get("chains") or []:
            chain = str(chain_entry.get("chain") or "")
            if chain:
                by_chain[chain] = chain_entry

    for refreshed in refreshed_results:
        chain = str(refreshed.get("chain") or "")
        if not chain:
            continue
        existing = by_chain.get(chain)
        if existing and not replace_tokens_for_refreshed_chains:
            by_chain[chain] = merge_chain_entry(existing, refreshed)
        else:
            by_chain[chain] = refreshed

    ordered = [by_chain[chain] for chain in GRAPH_ENDPOINTS if chain in by_chain]
    ordered.extend(entry for chain, entry in sorted(by_chain.items()) if chain not in GRAPH_ENDPOINTS)
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate static Dolomite Supply chart history")
    parser.add_argument("--out-dir", default="data/supply-history")
    parser.add_argument("--chains", default=os.environ.get("SUPPLY_HISTORY_CHAINS", ",".join(GRAPH_ENDPOINTS.keys())))
    parser.add_argument("--token-limit", type=int, default=int(os.environ.get("SUPPLY_HISTORY_TOKEN_LIMIT", "0")))
    parser.add_argument("--tokens", default=os.environ.get("SUPPLY_HISTORY_TOKENS", ""))
    parser.add_argument("--symbols", default=os.environ.get("SUPPLY_HISTORY_SYMBOLS", ",".join(sorted(DEFAULT_SYMBOLS))))
    parser.add_argument("--explicit-only", action="store_true", default=os.environ.get("SUPPLY_HISTORY_EXPLICIT_ONLY", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--max-pages-per-entity", type=int, default=int(os.environ.get("SUPPLY_HISTORY_MAX_PAGES_PER_ENTITY", "600")))
    parser.add_argument("--active-usd-threshold", default=os.environ.get("SUPPLY_HISTORY_ACTIVE_USD_THRESHOLD", "0"))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    chains = [chain.strip() for chain in args.chains.split(",") if chain.strip()]
    explicit_tokens = {token.lower() for token in parse_csv_set(args.tokens)}
    explicit_symbols = parse_csv_set(args.symbols)
    active_usd_threshold = decimal_or_zero(args.active_usd_threshold)
    generated_at = utc_now()
    results = []

    for chain in chains:
        endpoint = GRAPH_ENDPOINTS.get(chain)
        if not endpoint:
            print(f"Unknown chain: {chain}", file=sys.stderr)
            return 2
        results.append(
            generate_chain(
                chain,
                endpoint,
                out_dir,
                args.token_limit,
                explicit_tokens,
                explicit_symbols,
                args.explicit_only,
                args.max_pages_per_entity,
                generated_at,
            )
        )

    existing_manifest = load_existing_manifest(out_dir)
    replace_tokens_for_refreshed_chains = args.token_limit == 0 and not args.explicit_only
    manifest_chains = merge_manifest_results(existing_manifest, results, replace_tokens_for_refreshed_chains)

    manifest = {
        "schemaVersion": 1,
        "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
        "metric": "token",
        "scope": "all",
        "tokenLimit": args.token_limit,
        "explicitOnly": bool(args.explicit_only),
        "maxPagesPerEntity": args.max_pages_per_entity,
        "activeUsdThreshold": decimal_to_string(active_usd_threshold),
        "refreshedChains": chains,
        "chains": manifest_chains,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote manifest for {len(manifest_chains)} chains to {out_dir / 'manifest.json'}")

    active_skips = []
    for chain_result in results:
        chain = chain_result.get("chain")
        for token in chain_result.get("skippedTokens") or []:
            if decimal_or_zero(token.get("supplyLiquidityUSD")) > active_usd_threshold:
                symbol = token.get("symbol") or token.get("tokenId")
                active_skips.append(f"{chain} {symbol} (${token.get('supplyLiquidityUSD')}) {token.get('reason')}")
    if active_skips:
        print("Active Supply history tokens were skipped:", file=sys.stderr)
        print("\n".join(active_skips), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
