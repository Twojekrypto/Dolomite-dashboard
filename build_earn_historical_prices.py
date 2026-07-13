#!/usr/bin/env python3
"""Incrementally cache daily historical token prices used by EARN P&L."""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SNAPSHOT_DIR = ROOT / "data" / "earn-snapshots"
OUTPUT_DIR = ROOT / "data" / "earn-historical-prices"
STABLE_SYMBOLS = {
    "USDC", "USDC.E", "USDT", "USDT0", "DAI", "FRAX", "LUSD", "USD0",
    "USD1", "USDE", "HONEY", "MIM", "USDS",
}
NON_PEGGED_STABLE_WRAPPERS = {"SUSDE", "SUSDS"}
LLAMA_CHAIN_NAMES = {
    "polygon_zkevm": "polygon_zkevm",
}


def _read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
    temporary.replace(path)


def collect_requirements_for_chains(snapshot_dir: Path, chains) -> dict:
    requested_chains = {str(chain).lower() for chain in chains}
    manifest = _read_json(Path(snapshot_dir) / "manifest.json")
    requirements = {chain: {} for chain in requested_chains}
    for date in sorted(manifest.get("dates") or []):
        available_chains = requested_chains.intersection(
            str(value).lower() for value in (manifest.get("chains") or {}).get(date, [])
        )
        if not available_chains:
            continue
        path = Path(snapshot_dir) / f"{date}.json"
        if not path.is_file():
            continue
        snapshots = _read_json(path).get("snapshots") or {}
        for chain in available_chains:
            chain_requirements = requirements[chain]
            for wallet in (snapshots.get(chain) or {}).values():
                for market in (wallet.get("markets") or {}).values():
                    token = str(market.get("token") or "").lower()
                    if not token.startswith("0x"):
                        continue
                    row = chain_requirements.setdefault(token, {
                        "symbol": str(market.get("symbol") or ""),
                        "decimals": int(market.get("decimals") or 18),
                        "dates": set(),
                    })
                    row["dates"].add(date)
    return requirements


def collect_chain_requirements(snapshot_dir: Path, chain: str) -> dict:
    chain = chain.lower()
    return collect_requirements_for_chains(snapshot_dir, {chain})[chain]


def fetch_defillama_prices(chain: str, date: str, tokens: list[str]) -> dict[str, str]:
    if not tokens:
        return {}
    timestamp = int(datetime.fromisoformat(date + "T23:59:00+00:00").timestamp())
    namespace = LLAMA_CHAIN_NAMES.get(chain, chain)
    coin_ids = [f"{namespace}:{token}" for token in tokens]
    encoded = urllib.parse.quote(",".join(coin_ids), safe=",:")
    url = f"https://coins.llama.fi/prices/historical/{timestamp}/{encoded}?searchWidth=12h"
    request = urllib.request.Request(url, headers={"User-Agent": "dolomite-earn-price-builder/1.0"})
    last_error = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
            coins = payload.get("coins") or {}
            result = {}
            for token, coin_id in zip(tokens, coin_ids):
                price = (coins.get(coin_id) or {}).get("price")
                if price is not None and float(price) > 0:
                    result[token] = str(price)
            return result
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(1 + attempt)
    print(f"::warning::historical price request failed for {chain} {date}: {last_error}")
    return {}


def update_chain_prices(chain: str, requirements: dict, output_path: Path, *, fetcher=fetch_defillama_prices) -> dict:
    output_path = Path(output_path)
    payload = {"version": 1, "chain": chain, "generatedAt": "", "tokens": {}, "prices": {}, "sources": {}}
    if output_path.is_file():
        try:
            existing = _read_json(output_path)
            if isinstance(existing, dict):
                payload.update(existing)
        except (OSError, json.JSONDecodeError):
            pass
    payload["version"] = 1
    payload["chain"] = chain
    payload["generatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    payload.setdefault("tokens", {})
    payload.setdefault("prices", {})
    payload.setdefault("sources", {})

    by_date = {}
    for token, meta in requirements.items():
        payload["tokens"][token] = {"symbol": meta["symbol"], "decimals": meta["decimals"]}
        token_prices = payload["prices"].setdefault(token, {})
        token_sources = payload["sources"].setdefault(token, {})
        if str(meta["symbol"]).upper() in NON_PEGGED_STABLE_WRAPPERS and not token_sources:
            # Migrate legacy caches that previously treated yield-bearing wrappers as $1 pegs.
            token_prices.clear()
        inferred_source = "stable-peg" if str(meta["symbol"]).upper() in STABLE_SYMBOLS else "defillama"
        for existing_date in token_prices:
            token_sources.setdefault(existing_date, inferred_source)
        for date in sorted(meta["dates"]):
            if date in token_prices:
                continue
            if str(meta["symbol"]).upper() in STABLE_SYMBOLS:
                token_prices[date] = "1"
                token_sources[date] = "stable-peg"
            else:
                by_date.setdefault(date, []).append(token)

    for date, tokens in sorted(by_date.items()):
        for offset in range(0, len(tokens), 60):
            batch = tokens[offset:offset + 60]
            for token, price in fetcher(chain, date, batch).items():
                payload["prices"].setdefault(token, {})[date] = str(price)
                payload["sources"].setdefault(token, {})[date] = "defillama"

    required_count = sum(len(meta["dates"]) for meta in requirements.values())
    available_count = sum(
        1
        for token, meta in requirements.items()
        for date in meta["dates"]
        if date in payload["prices"].get(token, {})
    )
    payload["coverage"] = {
        "requiredPriceCount": required_count,
        "availablePriceCount": available_count,
        "missingPriceCount": required_count - available_count,
    }
    _write_json(output_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", action="append", required=True)
    parser.add_argument("--snapshot-dir", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    chains = sorted({value.lower() for value in args.chain})
    requirements_by_chain = collect_requirements_for_chains(args.snapshot_dir, chains)
    for chain in chains:
        requirements = requirements_by_chain[chain]
        payload = update_chain_prices(chain, requirements, args.output_dir / f"{chain}.json")
        coverage = payload["coverage"]
        print(f"[{chain}] historical prices {coverage['availablePriceCount']}/{coverage['requiredPriceCount']} ({coverage['missingPriceCount']} missing)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
