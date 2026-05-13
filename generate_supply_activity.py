#!/usr/bin/env python3
"""Generate compact static Supply Activity history files for GitHub Pages."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
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

RECENT_ACTIVITY_DAYS = 30

DEFAULT_ACTIVITY_SYMBOLS = {
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
}

BUNDLE_QUERY = """
{
  tokens(first: 1000, where:{marketId_not: null}, orderBy: supplyLiquidityUSD, orderDirection: desc) {
    id
    symbol
    name
    marketId
    supplyLiquidityUSD
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


def decimal_to_string(value: Any) -> str:
    number = decimal_or_zero(value)
    if not number.is_finite() or number == 0:
        return "0"
    text = format(number.normalize(), "f")
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
            headers={"Content-Type": "application/json", "User-Agent": "DolomiteDashboardSupplyActivity/1.0"},
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


def fetch_tokens(endpoint: str) -> List[Dict[str, Any]]:
    data = post_json(endpoint, {"query": BUNDLE_QUERY})
    return list(data.get("tokens") or [])


def paginate_entity(
    endpoint: str,
    entity_name: str,
    where_clause: str,
    fields: str,
    *,
    order_by: str = "serialId",
    order_direction: str = "desc",
    page_size: int = 1000,
    max_pages: int = 600,
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


def parse_csv_set(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def load_token_config(path_text: str) -> Dict[str, set[str]]:
    if not path_text:
        return {}
    path = Path(path_text)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Could not read token config {path}: {exc}") from exc
    chains = raw.get("chains") if isinstance(raw, dict) and isinstance(raw.get("chains"), dict) else raw
    if not isinstance(chains, dict):
        raise RuntimeError(f"Token config {path} must be a chain-to-token-list object")
    config: Dict[str, set[str]] = {}
    for chain, values in chains.items():
        if isinstance(values, str):
            tokens = parse_csv_set(values)
        elif isinstance(values, list):
            tokens = {str(item).strip() for item in values if str(item).strip()}
        else:
            tokens = set()
        config[str(chain)] = {token.lower() for token in tokens}
    return config


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
        if token_id and token_id not in seen:
            seen.add(token_id)
            selected.append(token)

    for token in tokens:
        token_id = str(token.get("id") or "").lower()
        symbol = str(token.get("symbol") or "")
        if token_id in explicit_tokens or symbol in explicit_symbols:
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
        "supplyLiquidityUSD": decimal_to_string(token.get("supplyLiquidityUSD")),
    }


def compact_activity_row(row: Dict[str, Any]) -> List[Any]:
    return [
        row.get("id") or "",
        row.get("type") or "",
        int(row.get("timestamp") or 0),
        row.get("txHash") or "",
        decimal_to_string(row.get("amount")),
        decimal_to_string(row.get("usd")),
        row.get("primaryAddress") or "",
        row.get("secondaryAddress") or "",
    ]


def build_row(activity_type: str, payload: Dict[str, Any], token_id: str) -> Dict[str, Any]:
    transaction = payload.get("transaction") or {}
    timestamp = int(transaction.get("timestamp") or 0)
    tx_hash = transaction.get("id") or ""
    if activity_type == "deposit":
        return {
            "id": f"deposit-{payload.get('serialId')}",
            "type": "deposit",
            "timestamp": timestamp,
            "txHash": tx_hash,
            "amount": decimal_to_string(payload.get("amountDeltaWei")),
            "usd": decimal_to_string(payload.get("amountUSDDeltaWei")),
            "primaryAddress": (payload.get("effectiveUser") or {}).get("id") or "",
            "secondaryAddress": "",
        }
    if activity_type == "withdraw":
        return {
            "id": f"withdraw-{payload.get('serialId')}",
            "type": "withdraw",
            "timestamp": timestamp,
            "txHash": tx_hash,
            "amount": decimal_to_string(payload.get("amountDeltaWei")),
            "usd": decimal_to_string(payload.get("amountUSDDeltaWei")),
            "primaryAddress": (payload.get("effectiveUser") or {}).get("id") or "",
            "secondaryAddress": "",
        }
    if activity_type == "transfer":
        return {
            "id": f"transfer-{payload.get('serialId')}",
            "type": "transfer",
            "timestamp": timestamp,
            "txHash": tx_hash,
            "amount": decimal_to_string(payload.get("amountDeltaWei")),
            "usd": decimal_to_string(payload.get("amountUSDDeltaWei")),
            "primaryAddress": (payload.get("fromEffectiveUser") or {}).get("id") or "",
            "secondaryAddress": (payload.get("toEffectiveUser") or {}).get("id") or "",
        }

    is_borrowed_side = str((payload.get("borrowedToken") or {}).get("id") or "").lower() == token_id
    return {
        "id": f"liquidation-{payload.get('serialId')}-{'borrowed' if is_borrowed_side else 'held'}",
        "type": "liquidation",
        "timestamp": timestamp,
        "txHash": tx_hash,
        "amount": decimal_to_string(payload.get("borrowedTokenAmountDeltaWei" if is_borrowed_side else "heldTokenAmountDeltaWei")),
        "usd": decimal_to_string(payload.get("borrowedTokenAmountUSD" if is_borrowed_side else "heldTokenAmountUSD")),
        "primaryAddress": (payload.get("liquidEffectiveUser") or {}).get("id") or "",
        "secondaryAddress": (payload.get("solidEffectiveUser") or {}).get("id") or "",
    }


def fetch_activity_rows(endpoint: str, token_id: str, max_pages_per_entity: int) -> List[List[Any]]:
    common_fields = """
      serialId
      transaction { id timestamp blockNumber }
    """
    token_filter = f'token: "{token_id}"'
    liquidation_borrowed_filter = f'borrowedToken: "{token_id}"'
    liquidation_held_filter = f'heldToken: "{token_id}"'
    deposits = paginate_entity(endpoint, "deposits", token_filter, f"""
      {common_fields}
      effectiveUser {{ id }}
      amountDeltaWei
      amountUSDDeltaWei
    """, max_pages=max_pages_per_entity)
    withdrawals = paginate_entity(endpoint, "withdrawals", token_filter, f"""
      {common_fields}
      effectiveUser {{ id }}
      amountDeltaWei
      amountUSDDeltaWei
    """, max_pages=max_pages_per_entity)
    transfers = paginate_entity(endpoint, "transfers", token_filter, f"""
      {common_fields}
      fromEffectiveUser {{ id }}
      toEffectiveUser {{ id }}
      amountDeltaWei
      amountUSDDeltaWei
      isSelfTransfer
      isTransferForMarginPosition
    """, max_pages=max_pages_per_entity)
    liquidations_borrowed = paginate_entity(endpoint, "liquidations", liquidation_borrowed_filter, f"""
      {common_fields}
      liquidEffectiveUser {{ id }}
      solidEffectiveUser {{ id }}
      borrowedToken {{ id }}
      heldToken {{ id }}
      borrowedTokenAmountDeltaWei
      borrowedTokenAmountUSD
      heldTokenAmountDeltaWei
      heldTokenAmountUSD
    """, max_pages=max_pages_per_entity)
    liquidations_held = paginate_entity(endpoint, "liquidations", liquidation_held_filter, f"""
      {common_fields}
      liquidEffectiveUser {{ id }}
      solidEffectiveUser {{ id }}
      borrowedToken {{ id }}
      heldToken {{ id }}
      borrowedTokenAmountDeltaWei
      borrowedTokenAmountUSD
      heldTokenAmountDeltaWei
      heldTokenAmountUSD
    """, max_pages=max_pages_per_entity)

    rows = [
        *(build_row("deposit", item, token_id) for item in deposits),
        *(build_row("withdraw", item, token_id) for item in withdrawals),
        *(build_row("transfer", item, token_id) for item in transfers),
        *(build_row("liquidation", item, token_id) for item in liquidations_borrowed),
        *(build_row("liquidation", item, token_id) for item in liquidations_held),
    ]
    deduped = {row["id"]: row for row in rows}
    return [compact_activity_row(row) for row in sorted(deduped.values(), key=lambda item: int(item.get("timestamp") or 0), reverse=True)]


def write_activity_file(out_dir: Path, chain: str, token: Dict[str, Any], rows: List[List[Any]], generated_at: datetime) -> Tuple[Path, Path, int]:
    token_id = str(token.get("id") or "").lower()
    chain_dir = out_dir / chain
    recent_chain_dir = out_dir / "recent" / chain
    chain_dir.mkdir(parents=True, exist_ok=True)
    recent_chain_dir.mkdir(parents=True, exist_ok=True)
    path = chain_dir / f"{token_id}.json"
    recent_path = recent_chain_dir / f"{token_id}.json"
    recent_cutoff = int(generated_at.timestamp()) - (RECENT_ACTIVITY_DAYS * 86400)
    recent_rows = [row for row in rows if int(row[2] or 0) >= recent_cutoff]
    payload = {
        "schemaVersion": 1,
        "source": "static-subgraph-activity",
        "__supplyActivityCompact": 1,
        "scope": "all",
        "chain": chain,
        "tokenId": token_id,
        "symbol": token.get("symbol"),
        "name": token.get("name"),
        "marketId": token.get("marketId"),
        "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
        "rows": rows,
    }
    recent_payload = {
        **payload,
        "scope": f"{RECENT_ACTIVITY_DAYS}d",
        "rows": recent_rows,
    }
    path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
    recent_path.write_text(json.dumps(recent_payload, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
    return path, recent_path, len(recent_rows)


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
    tokens = fetch_tokens(endpoint)
    selected = select_tokens(tokens, token_limit, explicit_tokens, explicit_symbols, explicit_only)
    written: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    print(f"{chain}: selected {len(selected)} / {len(tokens)} activity tokens", flush=True)
    for index, token in enumerate(selected, start=1):
        token_id = str(token.get("id") or "").lower()
        symbol = str(token.get("symbol") or token_id[:8])
        print(f"{chain} {symbol}: fetching activity {index}/{len(selected)}", flush=True)
        try:
            rows = fetch_activity_rows(endpoint, token_id, max_pages_per_entity)
        except RuntimeError as exc:
            entry = token_summary(token)
            entry["reason"] = "fetch_error"
            entry["detail"] = str(exc)[:500]
            skipped.append(entry)
            print(f"{chain} {symbol}: skipped ({exc})", flush=True)
            continue
        path, recent_path, recent_count = write_activity_file(out_dir, chain, token, rows, generated_at)
        entry = token_summary(token)
        entry["events"] = len(rows)
        entry["recentEvents"] = recent_count
        entry["path"] = str(path.as_posix())
        entry["recentPath"] = str(recent_path.as_posix())
        entry["generatedAt"] = generated_at.isoformat().replace("+00:00", "Z")
        written.append(entry)
        print(f"{chain} {symbol}: wrote {len(rows)} activity rows", flush=True)
    return {
        "chain": chain,
        "tokensAvailable": len(tokens),
        "tokensWritten": len(written),
        "tokensSkipped": len(skipped),
        "tokens": written,
        "skippedTokens": skipped,
    }


def load_existing_manifest(out_dir: Path) -> Optional[Dict[str, Any]]:
    manifest_path = out_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def merge_manifest(existing_manifest: Optional[Dict[str, Any]], refreshed_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_chain: Dict[str, Dict[str, Any]] = {}
    if existing_manifest:
        for chain_entry in existing_manifest.get("chains") or []:
            chain = str(chain_entry.get("chain") or "")
            if chain:
                by_chain[chain] = chain_entry
    for refreshed in refreshed_results:
        chain = str(refreshed.get("chain") or "")
        if chain:
            by_chain[chain] = refreshed
    ordered = [by_chain[chain] for chain in GRAPH_ENDPOINTS if chain in by_chain]
    ordered.extend(entry for chain, entry in sorted(by_chain.items()) if chain not in GRAPH_ENDPOINTS)
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate static Dolomite Supply activity history")
    parser.add_argument("--out-dir", default="data/supply-activity")
    parser.add_argument("--chains", default=os.environ.get("SUPPLY_ACTIVITY_CHAINS", ",".join(GRAPH_ENDPOINTS.keys())))
    parser.add_argument("--token-limit", type=int, default=int(os.environ.get("SUPPLY_ACTIVITY_TOKEN_LIMIT", "8")))
    parser.add_argument("--tokens", default=os.environ.get("SUPPLY_ACTIVITY_TOKENS", ""))
    parser.add_argument("--symbols", default=os.environ.get("SUPPLY_ACTIVITY_SYMBOLS", ",".join(sorted(DEFAULT_ACTIVITY_SYMBOLS))))
    parser.add_argument("--token-config", default=os.environ.get("SUPPLY_ACTIVITY_TOKEN_CONFIG", ""))
    parser.add_argument("--explicit-only", action="store_true", default=os.environ.get("SUPPLY_ACTIVITY_EXPLICIT_ONLY", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--max-pages-per-entity", type=int, default=int(os.environ.get("SUPPLY_ACTIVITY_MAX_PAGES_PER_ENTITY", "600")))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    chains = [chain.strip() for chain in args.chains.split(",") if chain.strip()]
    explicit_tokens = {token.lower() for token in parse_csv_set(args.tokens)}
    explicit_symbols = parse_csv_set(args.symbols)
    token_config = load_token_config(args.token_config)
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
                explicit_tokens | token_config.get(chain, set()),
                explicit_symbols,
                args.explicit_only,
                args.max_pages_per_entity,
                generated_at,
            )
        )

    manifest_chains = merge_manifest(load_existing_manifest(out_dir), results)
    manifest = {
        "schemaVersion": 1,
        "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
        "scope": "all",
        "compact": True,
        "tokenLimit": args.token_limit,
        "explicitOnly": bool(args.explicit_only),
        "maxPagesPerEntity": args.max_pages_per_entity,
        "refreshedChains": chains,
        "chains": manifest_chains,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote manifest for {len(manifest_chains)} chains to {out_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
