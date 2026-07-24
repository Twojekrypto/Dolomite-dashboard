#!/usr/bin/env python3
"""
Build a bounded hot-wallet selection for canonical EARN history workflows.

Heavy chains should not stamp every known wallet on every scheduled run. This
selector keeps strict replay coverage focused on the wallets most likely to need
borrow-route / hidden-collateral verification, while still allowing manually
prioritized addresses to be pinned into the set.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from build_earn_subaccount_history import _load_known_addresses, _read_json


ROOT = Path(__file__).resolve().parent
SNAPSHOT_DIR = ROOT / "data" / "earn-snapshots"
NETFLOW_DIR = ROOT / "data" / "earn-netflow"
HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
LEDGER_DIR = ROOT / "data" / "earn-verified-ledger"


def _read_addresses(path: Path) -> List[str]:
    if not path.exists():
        return []
    addresses: List[str] = []
    seen = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        address = raw.strip().lower()
        if not address or address.startswith("#"):
            continue
        if not address.startswith("0x") or len(address) != 42:
            raise ValueError(f"Invalid address in {path}: {raw}")
        if address not in seen:
            seen.add(address)
            addresses.append(address)
    return addresses


def _latest_snapshot_payload(chain: str) -> dict:
    manifest = _read_json(SNAPSHOT_DIR / "manifest.json", {})
    chain_dates = [
        str(date)
        for date in (manifest.get("dates") or [])
        if chain in ((manifest.get("chains") or {}).get(date) or [])
    ]
    if not chain_dates:
        return {}
    latest = sorted(chain_dates)[-1]
    payload = _read_json(SNAPSHOT_DIR / f"{latest}.json", {})
    return ((payload.get("snapshots") or {}).get(chain) or {}) if isinstance(payload, dict) else {}


def _intish(value: object) -> int:
    try:
        return int(str(value or "0"))
    except Exception:
        return 0


def _digit_weight(value: int) -> int:
    return len(str(abs(int(value)))) if value else 0


def _add_score(scores: Dict[str, int], address: str, value: int) -> None:
    if not address.startswith("0x") or len(address) != 42:
        return
    scores[address] = scores.get(address, 0) + int(value)


def _score_snapshot_wallets(chain: str, scores: Dict[str, int]) -> None:
    snapshots = _latest_snapshot_payload(chain)
    for raw_address, row in snapshots.items():
        address = str(raw_address).lower()
        markets = (row.get("markets") or {}) if isinstance(row, dict) else {}
        if not markets:
            continue
        market_count = len(markets)
        abs_par_weight = sum(_digit_weight(_intish(market.get("par"))) for market in markets.values())
        multi_market_bonus = 1_500_000 if market_count > 1 else 0
        _add_score(scores, address, 2_000_000 + multi_market_bonus + (market_count * 100_000) + abs_par_weight)


def _active_snapshot_wallets(chain: str) -> set[str]:
    snapshots = _latest_snapshot_payload(chain)
    return {
        str(raw_address).lower()
        for raw_address, row in snapshots.items()
        if isinstance(row, dict)
        and isinstance(row.get("markets"), dict)
        and row.get("markets")
    }


def _score_netflow_wallets(chain: str, scores: Dict[str, int]) -> None:
    payload = _read_json(NETFLOW_DIR / f"{chain}.json", {})
    netflows = (payload.get("netflows") or {}) if isinstance(payload, dict) else {}
    for raw_address, markets in netflows.items():
        address = str(raw_address).lower()
        if not isinstance(markets, dict) or not markets:
            continue
        nonzero_markets = 0
        activity_weight = 0
        ending_par_markets = 0
        for stats in markets.values():
            if not isinstance(stats, dict):
                continue
            values = [_intish(stats.get(key)) for key in ("t", "d", "w", "s", "x", "l", "v")]
            ending_par = _intish(stats.get("endingPar"))
            if any(values) or ending_par:
                nonzero_markets += 1
            if ending_par:
                ending_par_markets += 1
            activity_weight += sum(_digit_weight(value) for value in values) + _digit_weight(ending_par)
        if nonzero_markets:
            _add_score(
                scores,
                address,
                500_000 + (nonzero_markets * 80_000) + (ending_par_markets * 120_000) + activity_weight,
            )


def _unique_preserve_order(addresses: Iterable[str]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for raw in addresses:
        address = str(raw).strip().lower()
        if not address or address.startswith("#"):
            continue
        if not address.startswith("0x") or len(address) != 42:
            continue
        if address not in seen:
            seen.add(address)
            ordered.append(address)
    return ordered


def _existing_history_addresses(history_dir: Path, chain: str) -> set:
    chain_dir = history_dir / chain
    if not chain_dir.exists():
        return set()
    return {
        path.stem.lower()
        for path in chain_dir.glob("0x*.json")
        if path.stem.startswith("0x") and len(path.stem) == 42
    }


def _coverage_target_block(history_dir: Path, chain: str) -> int:
    manifest = _read_json(history_dir / "manifest.json", {})
    chain_payload = ((manifest.get("chains") or {}).get(chain) or {}) if isinstance(manifest, dict) else {}
    try:
        return int(chain_payload.get("lastBlock") or 0)
    except Exception:
        return 0


def _history_last_scanned_block(history_dir: Path, chain: str, address: str) -> int:
    payload = _read_json(history_dir / chain / f"{address.lower()}.json", None)
    if not isinstance(payload, dict):
        return 0
    try:
        return int(payload.get("lastScannedBlock") or 0)
    except Exception:
        return 0


def _active_strict_quality(
    chain: str,
    active_addresses: set[str],
    ledger_dir: Path,
) -> Dict[str, str]:
    """Classify whether every current market has exact resolved replay proof."""
    snapshots = _latest_snapshot_payload(chain)
    quality: Dict[str, str] = {}
    for address in sorted(active_addresses):
        snapshot_row = snapshots.get(address) or snapshots.get(address.lower()) or {}
        active_markets = {
            str(market_id)
            for market_id in ((snapshot_row.get("markets") or {}) if isinstance(snapshot_row, dict) else {})
        }
        ledger = _read_json(ledger_dir / chain / f"{address}.json", {})
        if not isinstance(ledger, dict):
            ledger = {}
        public_markets = ledger.get("markets") or {}
        resolved = ledger.get("resolvedInterestLedger") or {}
        resolved_markets = resolved.get("markets") or {} if isinstance(resolved, dict) else {}
        resolved_is_verified = (
            isinstance(resolved, dict)
            and str(resolved.get("strictStatus") or "") == "verified"
        )

        market_quality: List[str] = []
        for market_id in active_markets:
            resolved_market = (
                resolved_markets.get(market_id)
                if isinstance(resolved_markets, dict)
                else None
            )
            if (
                resolved_is_verified
                and isinstance(resolved_market, dict)
                and str(resolved_market.get("strictStatus") or "") == "verified"
            ):
                market_quality.append("verified")
                continue

            public_market = (
                public_markets.get(market_id)
                if isinstance(public_markets, dict)
                else None
            )
            if not isinstance(public_market, dict):
                market_quality.append("missing")
                continue
            strict_status = str(public_market.get("strictStatus") or "").lower()
            if strict_status == "mismatch":
                market_quality.append("mismatch")
            elif strict_status in {"coverage_incomplete", "pending", "unavailable"}:
                market_quality.append("coverage_incomplete")
            elif strict_status in {"inferred", "pre_snapshot_carry", "carry"}:
                market_quality.append("inferred")
            else:
                market_quality.append("missing")

        if active_markets and market_quality and all(status == "verified" for status in market_quality):
            quality[address] = "verified"
        elif "mismatch" in market_quality:
            quality[address] = "mismatch"
        elif "coverage_incomplete" in market_quality:
            quality[address] = "coverage_incomplete"
        elif "inferred" in market_quality:
            quality[address] = "inferred"
        else:
            quality[address] = "missing"
    return quality


def build_selection(
    chain: str,
    *,
    limit: int,
    priority_files: Sequence[Path],
    include_priority_even_if_unknown: bool,
    history_dir: Path = HISTORY_DIR,
    ledger_dir: Path = LEDGER_DIR,
    existing_history_only: bool = False,
    prefer_stale_history: bool = False,
    coverage_backfill: bool = False,
    strict_remediation: bool = False,
) -> Tuple[List[str], dict]:
    if coverage_backfill:
        existing_history_only = False
        prefer_stale_history = True
    if strict_remediation:
        existing_history_only = False
        prefer_stale_history = True
    known = set(_load_known_addresses(chain))
    scores: Dict[str, int] = {}
    _score_snapshot_wallets(chain, scores)
    _score_netflow_wallets(chain, scores)
    active_snapshot = _active_snapshot_wallets(chain) & known

    priority = _unique_preserve_order(
        address
        for path in priority_files
        for address in _read_addresses(path)
    )
    if not include_priority_even_if_unknown:
        priority = [address for address in priority if address in known]

    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    ranked_addresses = [address for address, _score in ranked]
    existing_history = _existing_history_addresses(history_dir, chain) if (existing_history_only or prefer_stale_history) else set()
    coverage_target = _coverage_target_block(history_dir, chain) if prefer_stale_history else 0
    stale_history: List[str] = []
    missing_history: List[str] = []
    if prefer_stale_history and coverage_target > 0:
        stale_rows = []
        for address in sorted(known):
            has_history = address in existing_history
            if existing_history_only and not has_history:
                continue
            if not has_history:
                missing_history.append(address)
                continue
            last_scanned = _history_last_scanned_block(history_dir, chain, address)
            if last_scanned < coverage_target:
                stale_rows.append((last_scanned, address))
        stale_history = [
            address
            for _last_scanned, address in sorted(
                stale_rows,
                key=lambda row: (row[0], -scores.get(row[1], 0), row[1]),
            )
        ]

    missing_history = sorted(
        missing_history,
        key=lambda address: (-scores.get(address, 0), address),
    )
    active_missing_history = [address for address in missing_history if address in active_snapshot]
    cold_missing_history = [address for address in missing_history if address not in active_snapshot]
    active_stale_history = [address for address in stale_history if address in active_snapshot]
    cold_stale_history = [address for address in stale_history if address not in active_snapshot]
    strict_quality = (
        _active_strict_quality(chain, active_snapshot, ledger_dir)
        if strict_remediation
        else {}
    )
    active_mismatch = sorted(
        (address for address, status in strict_quality.items() if status == "mismatch"),
        key=lambda address: (-scores.get(address, 0), address),
    )
    active_coverage_incomplete = sorted(
        (
            address
            for address, status in strict_quality.items()
            if status in {"coverage_incomplete", "missing"}
        ),
        key=lambda address: (-scores.get(address, 0), address),
    )
    active_inferred = sorted(
        (address for address, status in strict_quality.items() if status == "inferred"),
        key=lambda address: (-scores.get(address, 0), address),
    )
    if strict_remediation:
        selection_order = [
            *priority,
            *active_missing_history,
            *active_stale_history,
            *active_mismatch,
            *active_coverage_incomplete,
            *active_inferred,
        ]
    elif coverage_backfill:
        selection_order = [
            *priority,
            *active_missing_history,
            *active_stale_history,
            *cold_missing_history,
            *cold_stale_history,
            *ranked_addresses,
        ]
    else:
        selection_order = [
            *priority,
            *missing_history,
            *stale_history,
            *ranked_addresses,
            *(sorted(existing_history) if existing_history_only else []),
        ]
    selected = _unique_preserve_order(selection_order)
    if existing_history_only:
        selected = [address for address in selected if address in existing_history]
    if limit > 0:
        selected = selected[:limit]

    metadata = {
        "chain": chain,
        "limit": limit,
        "existingHistoryOnly": bool(existing_history_only),
        "coverageBackfill": bool(coverage_backfill),
        "strictRemediation": bool(strict_remediation),
        "existingHistoryAddressCount": len(existing_history),
        "knownAddressCount": len(known),
        "scoredAddressCount": len(scores),
        "priorityAddressCount": len(priority),
        "preferStaleHistory": bool(prefer_stale_history),
        "selectionPolicy": (
            "active-strict-blockers"
            if strict_remediation
            else ("active-first-then-cold-watermark"
            if coverage_backfill
            else ("missing-then-oldest-watermark" if prefer_stale_history else "score-ranked")
            )
        ),
        "coverageTargetBlock": coverage_target or None,
        "activeSnapshotAddressCount": len(active_snapshot),
        "activeMissingHistoryAddressCount": len(active_missing_history),
        "activeStaleHistoryAddressCount": len(active_stale_history),
        "activeStrictBlockingAddressCount": sum(
            1 for status in strict_quality.values() if status != "verified"
        ),
        "activeStrictVerifiedAddressCount": sum(
            1 for status in strict_quality.values() if status == "verified"
        ),
        "activeMismatchAddressCount": len(active_mismatch),
        "activeCoverageIncompleteAddressCount": sum(
            1 for status in strict_quality.values() if status == "coverage_incomplete"
        ),
        "activeMissingLedgerAddressCount": sum(
            1 for status in strict_quality.values() if status == "missing"
        ),
        "activeInferredAddressCount": len(active_inferred),
        "coldMissingHistoryAddressCount": len(cold_missing_history),
        "coldStaleHistoryAddressCount": len(cold_stale_history),
        "staleHistoryAddressCount": len(stale_history),
        "missingHistoryAddressCount": len(missing_history),
        "selectedAddressCount": len(selected),
    }
    return selected, metadata


def main() -> int:
    parser = argparse.ArgumentParser(description="Select hot wallets for canonical EARN history refreshes")
    parser.add_argument("--chain", required=True)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--priority-address-file", action="append", default=[])
    parser.add_argument("--include-priority-even-if-unknown", action="store_true")
    parser.add_argument("--history-dir", type=Path, default=HISTORY_DIR)
    parser.add_argument("--ledger-dir", type=Path, default=LEDGER_DIR)
    parser.add_argument("--existing-history-only", action="store_true")
    parser.add_argument("--prefer-stale-history", action="store_true")
    parser.add_argument("--coverage-backfill", action="store_true")
    parser.add_argument("--strict-remediation", action="store_true")
    parser.add_argument("--output", required=True)
    parser.add_argument("--metadata-output", default=None)
    args = parser.parse_args()

    selected, metadata = build_selection(
        args.chain,
        limit=max(0, int(args.limit)),
        priority_files=[Path(path) for path in args.priority_address_file],
        include_priority_even_if_unknown=bool(args.include_priority_even_if_unknown),
        history_dir=args.history_dir,
        ledger_dir=args.ledger_dir,
        existing_history_only=bool(args.existing_history_only),
        prefer_stale_history=bool(args.prefer_stale_history),
        coverage_backfill=bool(args.coverage_backfill),
        strict_remediation=bool(args.strict_remediation),
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("".join(f"{address}\n" for address in selected), encoding="utf-8")

    if args.metadata_output:
        metadata_path = Path(args.metadata_output)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
