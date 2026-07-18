#!/usr/bin/env python3
"""Publish compact two-character address-prefix shards from EARN ledgers."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_DIR = ROOT / "data" / "earn-verified-ledger"
OUTPUT_DIR = ROOT / "data" / "earn-verified-ledger-shards"
PREFIX_LENGTH = 2

MARKET_KEYS = (
    "token", "symbol", "decimals", "cumulativeYield", "status", "method",
    "strictStatus", "strictMethod", "strictReason", "firstDate", "firstPar",
    "firstWei", "lastDate", "lastPar", "lastWei", "days", "isLatestSnapshot",
    "hasStaticParWindow", "canonicalHistoryCoverageStatus",
    "canonicalHistoryLastScannedBlock", "historicalYieldUsd",
    "historicalYieldPricedIntervals", "historicalYieldEligibleIntervals",
    "historicalYieldSkippedFlowIntervals", "historicalYieldMissingPriceIntervals",
    "historicalYieldValuationStatus", "historicalYieldValuationMethod",
)

HISTORICAL_PNL_DEFAULTS = {
    "historicalYieldUsd": "0",
    "historicalYieldPricedIntervals": 0,
    "historicalYieldEligibleIntervals": 0,
    "historicalYieldSkippedFlowIntervals": 0,
    "historicalYieldMissingPriceIntervals": 0,
    "historicalYieldValuationStatus": "unavailable",
    "historicalYieldValuationMethod": "daily-snapshot-constant-par",
}


def _read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
    temporary.replace(path)


def _compact_ledger(payload: dict) -> dict:
    markets = {}
    for market_id, market in sorted((payload.get("markets") or {}).items(), key=lambda item: str(item[0])):
        if not isinstance(market, dict):
            continue
        compact_market = dict(HISTORICAL_PNL_DEFAULTS)
        compact_market.update({key: market[key] for key in MARKET_KEYS if key in market})
        markets[str(market_id)] = compact_market
    compact = {
        "snapshotDate": str(payload.get("snapshotDate") or ""),
        "markets": markets,
    }
    resolved_interest_ledger = payload.get("resolvedInterestLedger")
    if isinstance(resolved_interest_ledger, dict):
        compact["resolvedInterestLedger"] = resolved_interest_ledger
    return compact


def _merge_published_ledger(existing, candidate: dict, source_payload: dict) -> dict:
    if not existing:
        return candidate
    existing_date = str(existing.get("snapshotDate") or "")
    candidate_date = str(candidate.get("snapshotDate") or "")
    if existing_date and candidate_date and candidate_date < existing_date:
        return existing
    if candidate_date != existing_date:
        return candidate

    source_markets = source_payload.get("markets") or {}
    for market_id, market in (candidate.get("markets") or {}).items():
        raw_market = source_markets.get(str(market_id)) or source_markets.get(market_id) or {}
        if "historicalYieldValuationStatus" in raw_market:
            continue
        existing_market = (existing.get("markets") or {}).get(str(market_id)) or {}
        if existing_market:
            candidate["markets"][str(market_id)] = existing_market
    return candidate


def _encode_compact_ledger(ledger: dict) -> list:
    markets = {
        market_id: [market.get(key) for key in MARKET_KEYS]
        for market_id, market in (ledger.get("markets") or {}).items()
    }
    encoded = [str(ledger.get("snapshotDate") or ""), markets]
    if isinstance(ledger.get("resolvedInterestLedger"), dict):
        encoded.append(ledger["resolvedInterestLedger"])
    return encoded


def decode_compact_ledger(shard: dict, entry) -> dict:
    if isinstance(entry, dict):
        return entry
    if not isinstance(entry, list) or len(entry) < 2:
        return {}
    fields = ((shard.get("schema") or {}).get("market") or list(MARKET_KEYS))
    markets = {}
    for market_id, values in (entry[1] or {}).items():
        if not isinstance(values, list):
            continue
        markets[str(market_id)] = {
            key: values[index]
            for index, key in enumerate(fields)
            if index < len(values) and values[index] is not None
        }
    decoded = {"snapshotDate": str(entry[0] or ""), "markets": markets}
    if len(entry) >= 3 and isinstance(entry[2], dict):
        decoded["resolvedInterestLedger"] = entry[2]
    return decoded


def build_chain_shards(source_dir: Path, output_dir: Path, chain: str, *, prefix_length: int = PREFIX_LENGTH, updated_addresses=None) -> dict:
    chain = chain.strip().lower()
    source_chain_dir = Path(source_dir) / chain
    output_chain_dir = Path(output_dir) / chain
    output_chain_dir.mkdir(parents=True, exist_ok=True)

    grouped: dict[str, dict] = {}
    snapshot_date = ""
    generated_at = ""
    for existing_shard_path in sorted(output_chain_dir.glob("*.json")):
        try:
            existing_shard = _read_json(existing_shard_path)
        except (OSError, json.JSONDecodeError):
            continue
        for address, ledger in (existing_shard.get("ledgers") or {}).items():
            address = str(address).lower()
            ledger = decode_compact_ledger(existing_shard, ledger)
            if len(address) == 42 and ledger:
                grouped.setdefault(address[2:2 + prefix_length], {})[address] = ledger
                snapshot_date = max(snapshot_date, str(ledger.get("snapshotDate") or ""))

    for address in (updated_addresses or set()):
        normalized = str(address).strip().lower()
        if len(normalized) == 42:
            grouped.get(normalized[2:2 + prefix_length], {}).pop(normalized, None)

    for path in sorted(source_chain_dir.glob("0x*.json")):
        address = path.stem.lower()
        if len(address) != 42:
            continue
        payload = _read_json(path)
        if not isinstance(payload, dict) or not isinstance(payload.get("markets"), dict) or not payload["markets"]:
            continue
        prefix = address[2:2 + prefix_length]
        existing = grouped.setdefault(prefix, {}).get(address)
        candidate = _compact_ledger(payload)
        grouped[prefix][address] = _merge_published_ledger(existing, candidate, payload)
        snapshot_date = max(snapshot_date, str(payload.get("snapshotDate") or ""))
        generated_at = max(generated_at, str(payload.get("generatedAt") or ""))

    for stale in output_chain_dir.glob("*.json"):
        stale.unlink()

    shards = {}
    address_count = 0
    for prefix, ledgers in sorted(grouped.items()):
        address_count += len(ledgers)
        relative_path = f"{chain}/{prefix}.json"
        _write_json(output_dir / relative_path, {
            "version": 2,
            "chain": chain,
            "prefix": prefix,
            "snapshotDate": snapshot_date,
            "schema": {"ledger": ["snapshotDate", "markets"], "market": list(MARKET_KEYS)},
            "ledgers": {address: _encode_compact_ledger(ledger) for address, ledger in ledgers.items()},
        })
        shards[prefix] = {"path": relative_path, "addressCount": len(ledgers)}

    return {
        "snapshotDate": snapshot_date,
        "generatedAt": generated_at,
        "prefixLength": prefix_length,
        "addressCount": address_count,
        "shardCount": len(shards),
        "shards": shards,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", action="append", required=True)
    parser.add_argument("--source-dir", type=Path, default=SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--address-file", action="append", default=[], help="Selected addresses refreshed in this run; absent outputs are removed from existing shards")
    args = parser.parse_args()

    manifest_path = args.output_dir / "manifest.json"
    manifest = {"version": 1, "prefixLength": PREFIX_LENGTH, "generatedAt": "", "chains": {}}
    if manifest_path.is_file():
        try:
            existing = _read_json(manifest_path)
            if isinstance(existing, dict):
                manifest.update(existing)
        except (OSError, json.JSONDecodeError):
            pass
    manifest["version"] = 1
    manifest["prefixLength"] = PREFIX_LENGTH
    manifest["generatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    manifest.setdefault("chains", {})
    updated_addresses = set()
    for address_file in args.address_file:
        for raw_line in Path(address_file).read_text(encoding="utf-8").splitlines():
            address = raw_line.split("#", 1)[0].strip().lower()
            if address:
                updated_addresses.add(address)
    for chain in sorted({value.lower() for value in args.chain}):
        manifest["chains"][chain] = build_chain_shards(
            args.source_dir,
            args.output_dir,
            chain,
            updated_addresses=updated_addresses,
        )
        print(f"[{chain}] published {manifest['chains'][chain]['addressCount']} ledgers in {manifest['chains'][chain]['shardCount']} shards")
    _write_json(manifest_path, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
