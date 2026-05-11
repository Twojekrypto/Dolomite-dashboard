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


def build_selection(
    chain: str,
    *,
    limit: int,
    priority_files: Sequence[Path],
    include_priority_even_if_unknown: bool,
    history_dir: Path = HISTORY_DIR,
    existing_history_only: bool = False,
    prefer_stale_history: bool = False,
) -> Tuple[List[str], dict]:
    known = set(_load_known_addresses(chain))
    scores: Dict[str, int] = {}
    _score_snapshot_wallets(chain, scores)
    _score_netflow_wallets(chain, scores)

    priority = _unique_preserve_order(
        address
        for path in priority_files
        for address in _read_addresses(path)
    )
    if not include_priority_even_if_unknown:
        priority = [address for address in priority if address in known]

    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
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
        stale_history = [address for _last_scanned, address in sorted(stale_rows)]

    selected = _unique_preserve_order([
        *priority,
        *stale_history,
        *(address for address, _score in ranked),
        *missing_history,
    ])
    if existing_history_only:
        selected = [address for address in selected if address in existing_history]
    if limit > 0:
        selected = selected[:limit]

    metadata = {
        "chain": chain,
        "limit": limit,
        "existingHistoryOnly": bool(existing_history_only),
        "existingHistoryAddressCount": len(existing_history),
        "knownAddressCount": len(known),
        "scoredAddressCount": len(scores),
        "priorityAddressCount": len(priority),
        "preferStaleHistory": bool(prefer_stale_history),
        "coverageTargetBlock": coverage_target or None,
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
    parser.add_argument("--existing-history-only", action="store_true")
    parser.add_argument("--prefer-stale-history", action="store_true")
    parser.add_argument("--output", required=True)
    parser.add_argument("--metadata-output", default=None)
    args = parser.parse_args()

    selected, metadata = build_selection(
        args.chain,
        limit=max(0, int(args.limit)),
        priority_files=[Path(path) for path in args.priority_address_file],
        include_priority_even_if_unknown=bool(args.include_priority_even_if_unknown),
        history_dir=args.history_dir,
        existing_history_only=bool(args.existing_history_only),
        prefer_stale_history=bool(args.prefer_stale_history),
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
