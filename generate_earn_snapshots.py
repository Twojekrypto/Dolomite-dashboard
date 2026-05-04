#!/usr/bin/env python3
"""
Generate fresh EARN history snapshots from Dolomite subgraphs.

Outputs:
  - data/earn-snapshots/{YYYY-MM-DD}.json
  - data/earn-snapshots/manifest.json
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
SNAPSHOT_DIR = ROOT / "data" / "earn-snapshots"
INDEX_SCALE = 10**18

CHAINS = {
    "arbitrum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
    "berachain": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
    "ethereum": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-ethereum/latest/gn",
    "mantle": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-mantle/latest/gn",
    "botanix": "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-botanix/latest/gn",
}

INTEREST_INDEXES_QUERY = """
query InterestIndexes {
  interestIndexes(first: 1000) {
    id
    supplyIndex
  }
}
"""

MARGIN_ACCOUNTS_QUERY = """
query MarginAccounts($first: Int!, $lastId: ID!) {
  marginAccounts(
    first: $first
    orderBy: id
    orderDirection: asc
    where: { id_gt: $lastId }
  ) {
    id
    effectiveUser { id }
    tokenValues {
      token { id marketId symbol decimals }
      valuePar
    }
  }
}
"""


def _read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=True)
    tmp.replace(path)


def _utc_now():
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime):
    return dt.isoformat().replace("+00:00", "Z")


def _decimal_to_int(value, decimals):
    places = max(0, int(decimals or 0))
    raw = str(value or "0").strip()
    if not raw:
        return 0

    negative = False
    if raw[0] in "+-":
        negative = raw[0] == "-"
        raw = raw[1:]
    if not raw:
        return 0

    whole, _, frac = raw.partition(".")
    whole_digits = "".join(ch for ch in whole if ch.isdigit()) or "0"
    frac_digits = "".join(ch for ch in frac if ch.isdigit())
    frac_digits = (frac_digits + ("0" * places))[:places] if places else ""
    scale = 10**places
    result = (int(whole_digits) * scale) + (int(frac_digits or "0") if places else 0)
    return -result if negative else result


def _graphql_query(endpoint, query, variables=None, timeout=60, retries=3):
    payload = json.dumps({
        "query": query,
        "variables": variables or {},
    }).encode("utf-8")

    last_error = None
    for attempt in range(retries):
        try:
            req = Request(
                endpoint,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "DolomiteEarnSnapshot/1.0",
                },
                method="POST",
            )
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read()
            data = json.loads(body)
            if data.get("errors"):
                raise RuntimeError(data["errors"][0].get("message") or "GraphQL error")
            return data.get("data") or {}
        except (HTTPError, URLError, TimeoutError, OSError, RuntimeError, ValueError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
    raise RuntimeError(f"GraphQL request failed for {endpoint}: {last_error}")


def _fetch_interest_indexes(endpoint):
    data = _graphql_query(endpoint, INTEREST_INDEXES_QUERY)
    out = {}
    for item in data.get("interestIndexes") or []:
        token_addr = str(item.get("id") or "").lower()
        if not token_addr:
            continue
        out[token_addr] = _decimal_to_int(item.get("supplyIndex") or "1", 18)
    return out


def _aggregate_chain_snapshot(chain, endpoint, page_size):
    indexes = _fetch_interest_indexes(endpoint)
    snapshots = {}
    total_accounts = 0
    page = 0
    last_id = ""

    while True:
        data = _graphql_query(endpoint, MARGIN_ACCOUNTS_QUERY, {
            "first": int(page_size),
            "lastId": last_id,
        })
        accounts = data.get("marginAccounts") or []
        if not accounts:
            break

        page += 1
        total_accounts += len(accounts)
        print(f"[{chain}] page {page}: +{len(accounts)} accounts (total {total_accounts})")

        for account in accounts:
            last_id = str(account.get("id") or last_id)
            effective_user = account.get("effectiveUser") or {}
            address = str(effective_user.get("id") or "").strip().lower()
            if not (address.startswith("0x") and len(address) == 42):
                continue

            user_entry = snapshots.setdefault(address, {"markets": {}})
            markets = user_entry["markets"]

            for token_value in account.get("tokenValues") or []:
                token = token_value.get("token") or {}
                token_addr = str(token.get("id") or "").strip().lower()
                market_id = str(token.get("marketId") or "").strip()
                symbol = str(token.get("symbol") or "UNK")
                decimals = int(token.get("decimals") or 18)
                if not token_addr or not market_id:
                    continue

                par = _decimal_to_int(token_value.get("valuePar") or "0", decimals)
                if par <= 0:
                    continue

                supply_index = indexes.get(token_addr, INDEX_SCALE)
                wei = (par * supply_index) // INDEX_SCALE

                market_entry = markets.get(market_id)
                if market_entry is None:
                    markets[market_id] = {
                        "token": token_addr,
                        "symbol": symbol,
                        "decimals": decimals,
                        "par": str(par),
                        "wei": str(wei),
                    }
                    continue

                market_entry["par"] = str(int(market_entry["par"]) + par)
                market_entry["wei"] = str(int(market_entry["wei"]) + wei)

        if len(accounts) < page_size:
            break

    populated = {addr: data for addr, data in snapshots.items() if data.get("markets")}
    print(f"[{chain}] aggregated {len(populated)} addresses with supply positions")
    return populated


def _merge_snapshot_payload(existing_payload, date_str, timestamp, chain_snapshots):
    merged = {
        "date": date_str,
        "timestamp": timestamp,
        "snapshots": {},
    }

    existing_snapshots = {}
    if isinstance(existing_payload, dict):
        existing_snapshots = existing_payload.get("snapshots") or {}

    merged["snapshots"] = {
        str(chain): value
        for chain, value in existing_snapshots.items()
        if chain not in chain_snapshots
    }
    merged["snapshots"].update(chain_snapshots)
    return merged


def _update_manifest(manifest_path, date_str, written_chains):
    manifest = {"dates": [], "chains": {}}
    if manifest_path.exists():
        try:
            manifest = _read_json(manifest_path)
        except Exception:
            manifest = {"dates": [], "chains": {}}

    dates = sorted(set((manifest.get("dates") or []) + [date_str]))
    chains = manifest.get("chains") or {}
    existing = {
        str(chain).lower()
        for chain in chains.get(date_str, [])
    }
    updated = sorted(existing | {str(chain).lower() for chain in written_chains})
    chains[date_str] = updated

    manifest["dates"] = dates
    manifest["chains"] = chains
    _write_json(manifest_path, manifest)


def main():
    parser = argparse.ArgumentParser(description="Generate EARN snapshots from Dolomite subgraphs")
    parser.add_argument("--chain", action="append", choices=sorted(CHAINS.keys()),
                        help="Specific chain to scan (repeatable). Defaults to all supported chains.")
    parser.add_argument("--date", default="",
                        help="Snapshot date in YYYY-MM-DD. Defaults to current UTC date.")
    parser.add_argument("--page-size", type=int, default=250,
                        help="Margin account page size (default: 250)")
    args = parser.parse_args()

    date_str = args.date or _utc_now().strftime("%Y-%m-%d")
    timestamp = _isoformat(_utc_now())
    chains = args.chain or list(CHAINS.keys())

    chain_snapshots = {}
    for chain in chains:
        endpoint = CHAINS[chain]
        print(f"[{chain}] scanning {endpoint}")
        chain_snapshots[chain] = _aggregate_chain_snapshot(chain, endpoint, max(1, args.page_size))

    snapshot_path = SNAPSHOT_DIR / f"{date_str}.json"
    existing_payload = _read_json(snapshot_path) if snapshot_path.exists() else None
    payload = _merge_snapshot_payload(existing_payload, date_str, timestamp, chain_snapshots)
    _write_json(snapshot_path, payload)
    _update_manifest(SNAPSHOT_DIR / "manifest.json", date_str, chain_snapshots.keys())

    print(f"Wrote snapshot file: {snapshot_path}")
    print(f"Updated manifest: {SNAPSHOT_DIR / 'manifest.json'}")


if __name__ == "__main__":
    main()
