#!/usr/bin/env python3
"""Build a public EARN data-quality summary from committed snapshot/ledger data."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "data" / "earn-quality" / "status.json"
QUALITY_STATUSES = (
    "verified",
    "inferred",
    "mismatch",
    "coverage_incomplete",
    "missing_ledger",
    "missing_market",
    "unavailable",
    "unknown",
)


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _latest_snapshot_date_for_chain(data_dir: Path, chain: str) -> Optional[str]:
    manifest = _read_json(data_dir / "earn-snapshots" / "manifest.json", {})
    dates = manifest.get("dates") or []
    chains_by_date = manifest.get("chains") or {}
    for date_str in reversed(dates):
        if chain in (chains_by_date.get(date_str) or []):
            return str(date_str)
    return None


def _latest_snapshot_chain_payload(data_dir: Path, chain: str, requested_date: Optional[str]) -> tuple[Optional[str], dict]:
    snapshot_date = requested_date or _latest_snapshot_date_for_chain(data_dir, chain)
    if not snapshot_date:
        return None, {}
    payload = _read_json(data_dir / "earn-snapshots" / f"{snapshot_date}.json", {})
    snapshots = (payload.get("snapshots") or {}) if isinstance(payload, dict) else {}
    chain_payload = snapshots.get(chain) or {}
    return snapshot_date, chain_payload if isinstance(chain_payload, dict) else {}


def _normalize_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    if status == "live_balance_adjusted":
        return "verified"
    if status in QUALITY_STATUSES:
        return status
    if status in {"pre_snapshot_carry", "historical", "snapshot", "snapshot_only"}:
        return "inferred"
    if not status:
        return "unknown"
    return status if status in QUALITY_STATUSES else "unknown"


def _ledger_market_status(data_dir: Path, chain: str, address: str, market_id: str) -> str:
    ledger_path = data_dir / "earn-verified-ledger" / chain / f"{address.lower()}.json"
    payload = _read_json(ledger_path, None)
    if not isinstance(payload, dict):
        return "missing_ledger"
    markets = payload.get("markets") or {}
    if not isinstance(markets, dict):
        return "missing_market"
    market = markets.get(str(market_id))
    if not isinstance(market, dict):
        return "missing_market"
    return _normalize_status(market.get("strictStatus") or market.get("status"))


def _ratio(part: int, total: int) -> Optional[float]:
    if total <= 0:
        return None
    return round(part / total, 6)


def _quality_tier(counts: Counter, total: int) -> str:
    if total <= 0:
        return "empty"
    verified_ratio = _ratio(counts["verified"], total) or 0.0
    blocking = counts["mismatch"] + counts["coverage_incomplete"] + counts["missing_ledger"] + counts["missing_market"]
    if verified_ratio >= 0.95 and blocking == 0:
        return "strict"
    if verified_ratio >= 0.75 and counts["missing_ledger"] == 0:
        return "strong"
    if counts["missing_ledger"] > 0 or counts["coverage_incomplete"] > 0:
        return "partial"
    return "review"


def build_quality_status(*, data_dir: Path, snapshot_date: Optional[str] = None) -> Dict[str, Any]:
    snapshot_manifest = _read_json(data_dir / "earn-snapshots" / "manifest.json", {})
    chains = sorted({chain for values in (snapshot_manifest.get("chains") or {}).values() for chain in (values or [])})

    chain_payloads: Dict[str, Any] = {}
    totals = Counter()
    for chain in chains:
        resolved_date, wallets = _latest_snapshot_chain_payload(data_dir, chain, snapshot_date)
        market_counts: Counter = Counter()
        address_counts: Counter = Counter()
        active_address_count = 0
        active_market_count = 0

        for raw_address, wallet_payload in wallets.items():
            address = str(raw_address).lower()
            markets = (wallet_payload.get("markets") or {}) if isinstance(wallet_payload, dict) else {}
            if not markets:
                continue
            active_address_count += 1
            address_has_verified = False
            address_has_non_strict = False
            for market_id in markets:
                active_market_count += 1
                status = _ledger_market_status(data_dir, chain, address, str(market_id))
                market_counts[status] += 1
                address_has_verified = address_has_verified or status == "verified"
                address_has_non_strict = address_has_non_strict or status != "verified"
            if address_has_verified:
                address_counts["hasVerifiedMarket"] += 1
            if address_has_non_strict:
                address_counts["hasNonStrictMarket"] += 1

        non_strict = active_market_count - market_counts["verified"]
        blocking = (
            market_counts["mismatch"]
            + market_counts["coverage_incomplete"]
            + market_counts["missing_ledger"]
            + market_counts["missing_market"]
        )
        chain_payloads[chain] = {
            "snapshotDate": resolved_date,
            "qualityTier": _quality_tier(market_counts, active_market_count),
            "activeAddressCount": active_address_count,
            "activeMarketCount": active_market_count,
            "strictVerifiedMarketCount": market_counts["verified"],
            "strictVerifiedMarketRatio": _ratio(market_counts["verified"], active_market_count),
            "nonStrictMarketCount": non_strict,
            "blockingMarketCount": blocking,
            "marketStatusCounts": {status: market_counts[status] for status in QUALITY_STATUSES if market_counts[status]},
            "addressStatusCounts": dict(sorted(address_counts.items())),
        }
        totals["activeAddressCount"] += active_address_count
        totals["activeMarketCount"] += active_market_count
        totals["strictVerifiedMarketCount"] += market_counts["verified"]
        totals["nonStrictMarketCount"] += non_strict
        totals["blockingMarketCount"] += blocking

    return {
        "version": 1,
        "generatedAt": _utc_now_iso(),
        "snapshotDate": snapshot_date,
        "summary": {
            "activeAddressCount": totals["activeAddressCount"],
            "activeMarketCount": totals["activeMarketCount"],
            "strictVerifiedMarketCount": totals["strictVerifiedMarketCount"],
            "strictVerifiedMarketRatio": _ratio(totals["strictVerifiedMarketCount"], totals["activeMarketCount"]),
            "nonStrictMarketCount": totals["nonStrictMarketCount"],
            "blockingMarketCount": totals["blockingMarketCount"],
        },
        "chains": chain_payloads,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build public EARN data-quality status")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--snapshot-date", default=None)
    args = parser.parse_args()

    status = build_quality_status(data_dir=args.data_dir, snapshot_date=args.snapshot_date)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(status.get("summary", {}), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
