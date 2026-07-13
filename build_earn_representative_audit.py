#!/usr/bin/env python3
"""Audit deterministic representative EARN wallets for every latest market."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from build_earn_verified_ledger_shards import decode_compact_ledger


ROOT = Path(__file__).resolve().parent
SNAPSHOT_DIR = ROOT / "data" / "earn-snapshots"
SHARD_DIR = ROOT / "data" / "earn-verified-ledger-shards"
OUTPUT_FILE = ROOT / "data" / "earn-quality" / "representative-audit.json"
ACTIVE_CHAINS = {"ethereum", "arbitrum", "berachain", "mantle", "xlayer"}


def _read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _latest_chain_dates(manifest: dict) -> dict[str, str]:
    result = {}
    for date in sorted(manifest.get("dates") or []):
        for chain in (manifest.get("chains") or {}).get(date, []):
            result[str(chain).lower()] = date
    return result


def _representatives(chain_data: dict, count: int) -> dict[str, list[tuple[str, dict]]]:
    candidates: dict[str, list[tuple[int, str, dict]]] = {}
    for address, wallet in chain_data.items():
        for market_id, market in (wallet.get("markets") or {}).items():
            try:
                wei = int(str(market.get("wei") or "0"))
            except (TypeError, ValueError):
                continue
            if wei <= 0:
                continue
            candidates.setdefault(str(market_id), []).append((wei, address.lower(), market))
    return {
        market_id: [(address, market) for _, address, market in sorted(rows, key=lambda row: (-row[0], row[1]))[:count]]
        for market_id, rows in candidates.items()
    }


def _validate_market(snapshot_market: dict, ledger_market: dict, snapshot_date: str) -> list[str]:
    errors = []
    for key in ("token", "symbol"):
        if str(ledger_market.get(key) or "").lower() != str(snapshot_market.get(key) or "").lower():
            errors.append(f"{key}_mismatch")
    if int(ledger_market.get("decimals", -1)) != int(snapshot_market.get("decimals", -2)):
        errors.append("decimals_mismatch")
    if str(ledger_market.get("lastDate") or "") != snapshot_date:
        errors.append("last_snapshot_date_mismatch")
    if str(ledger_market.get("lastPar") or "") != str(snapshot_market.get("par") or ""):
        errors.append("last_par_mismatch")
    if str(ledger_market.get("lastWei") or "") != str(snapshot_market.get("wei") or ""):
        errors.append("last_wei_mismatch")
    if ledger_market.get("isLatestSnapshot") is not True:
        errors.append("not_latest_snapshot")
    try:
        int(str(ledger_market.get("cumulativeYield")))
    except (TypeError, ValueError):
        errors.append("invalid_cumulative_yield")
    status = str(ledger_market.get("strictStatus") or "").lower()
    coverage = str(ledger_market.get("canonicalHistoryCoverageStatus") or "").lower()
    if status == "verified" and coverage != "fresh":
        errors.append("verified_without_fresh_canonical_history")
    valuation_status = str(ledger_market.get("historicalYieldValuationStatus") or "")
    required_counts = (
        "historicalYieldEligibleIntervals", "historicalYieldPricedIntervals",
        "historicalYieldSkippedFlowIntervals", "historicalYieldMissingPriceIntervals",
    )
    if valuation_status not in {"complete", "partial", "unavailable"}:
        errors.append("missing_historical_pnl_status")
    try:
        eligible = int(ledger_market.get("historicalYieldEligibleIntervals"))
        priced = int(ledger_market.get("historicalYieldPricedIntervals"))
        missing = int(ledger_market.get("historicalYieldMissingPriceIntervals"))
        if priced + missing != eligible:
            errors.append("historical_pnl_interval_mismatch")
        if valuation_status == "complete" and priced != eligible:
            errors.append("historical_pnl_false_complete")
        for key in required_counts:
            if int(ledger_market.get(key)) < 0:
                errors.append(f"negative_{key}")
    except (TypeError, ValueError):
        errors.append("invalid_historical_pnl_counts")
    return errors


def build_representative_audit(snapshot_dir: Path, shard_dir: Path, *, representatives_per_market: int = 2, active_chains=ACTIVE_CHAINS) -> dict:
    snapshot_manifest = _read_json(Path(snapshot_dir) / "manifest.json")
    shard_manifest_path = Path(shard_dir) / "manifest.json"
    shard_manifest = _read_json(shard_manifest_path) if shard_manifest_path.is_file() else {"chains": {}}
    prefix_length = int(shard_manifest.get("prefixLength") or 2)
    shard_cache = {}
    rows = []

    for chain, date in sorted(_latest_chain_dates(snapshot_manifest).items()):
        if active_chains is not None and chain not in active_chains:
            continue
        snapshot_payload = _read_json(Path(snapshot_dir) / f"{date}.json")
        chain_data = ((snapshot_payload.get("snapshots") or {}).get(chain) or {})
        for market_id, candidates in sorted(_representatives(chain_data, representatives_per_market).items(), key=lambda item: item[0]):
            checks = []
            for address, snapshot_market in candidates:
                prefix = address[2:2 + prefix_length]
                cache_key = (chain, prefix)
                if cache_key not in shard_cache:
                    path = Path(shard_dir) / chain / f"{prefix}.json"
                    shard_cache[cache_key] = _read_json(path) if path.is_file() else {"ledgers": {}}
                shard = shard_cache[cache_key]
                encoded_ledger = (shard.get("ledgers") or {}).get(address)
                ledger = decode_compact_ledger(shard, encoded_ledger) if encoded_ledger is not None else None
                errors = ["missing_published_ledger"] if not ledger else []
                if ledger and str(ledger.get("snapshotDate") or "") != date:
                    errors.append("stale_published_ledger")
                ledger_market = ((ledger or {}).get("markets") or {}).get(market_id)
                if ledger and not ledger_market:
                    errors.append("missing_published_market")
                if ledger_market:
                    errors.extend(_validate_market(snapshot_market, ledger_market, date))
                checks.append({"address": address, "status": "fail" if errors else "pass", "errors": errors})
            failed = [check for check in checks if check["status"] == "fail"]
            sample_market = candidates[0][1]
            rows.append({
                "chain": chain,
                "snapshotDate": date,
                "marketId": market_id,
                "token": str(sample_market.get("token") or "").lower(),
                "symbol": str(sample_market.get("symbol") or ""),
                "status": "fail" if failed else "pass",
                "representatives": checks,
            })

    failed_count = sum(row["status"] == "fail" for row in rows)
    return {
        "version": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "fail" if failed_count else "pass",
        "summary": {
            "chainCount": len({row["chain"] for row in rows}),
            "marketCount": len(rows),
            "failedMarketCount": failed_count,
        },
        "markets": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--shard-dir", type=Path, default=SHARD_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE)
    parser.add_argument("--check", action="store_true", help="Exit non-zero when representative coverage fails")
    args = parser.parse_args()
    report = build_representative_audit(args.snapshot_dir, args.shard_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
    print(json.dumps(report["summary"], sort_keys=True))
    if args.check and report["status"] != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
