#!/usr/bin/env python3
"""
Select wallets whose materialized EARN subaccount history contains borrow accounts.

This is the global publication set for strict borrow-route verification: supply-only
wallets do not need canonical subaccount history, while wallets with borrow accounts
do need fresh public history to avoid coverage-incomplete runtime warnings.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Tuple


ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"


def _read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _has_borrow_route_history(payload: dict) -> bool:
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    try:
        if int((summary or {}).get("borrowAccountCount") or 0) > 0:
            return True
    except Exception:
        pass

    accounts = payload.get("accounts") if isinstance(payload, dict) else {}
    if not isinstance(accounts, dict):
        return False
    for account in accounts.values():
        if isinstance(account, dict) and account.get("hasBorrow"):
            return True
    return False


def _history_files(history_dir: Path, chain: str) -> Iterable[Path]:
    chain_dir = history_dir / chain
    if not chain_dir.exists():
        return []
    return sorted(
        path
        for path in chain_dir.glob("*.json")
        if path.stem.startswith("0x") and len(path.stem) == 42
    )


def select_addresses(history_dir: Path, chain: str) -> Tuple[List[str], dict]:
    selected: List[str] = []
    scanned = 0
    stale_or_unreadable = 0
    last_scanned_block = 0
    total_borrow_accounts = 0

    for path in _history_files(history_dir, chain):
        scanned += 1
        try:
            payload = _read_json(path)
        except Exception:
            stale_or_unreadable += 1
            continue
        try:
            last_scanned_block = max(last_scanned_block, int(payload.get("lastScannedBlock") or 0))
        except Exception:
            pass
        summary = payload.get("summary") or {}
        try:
            total_borrow_accounts += int(summary.get("borrowAccountCount") or 0)
        except Exception:
            pass
        if _has_borrow_route_history(payload):
            selected.append(path.stem.lower())

    metadata = {
        "chain": chain,
        "historyAddressCount": scanned,
        "borrowRouteAddressCount": len(selected),
        "borrowAccountCount": total_borrow_accounts,
        "lastScannedBlock": last_scanned_block or None,
        "unreadableHistoryCount": stale_or_unreadable,
    }
    return selected, metadata


def main() -> int:
    parser = argparse.ArgumentParser(description="Select EARN wallets that need borrow-route canonical history")
    parser.add_argument("--chain", required=True)
    parser.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR))
    parser.add_argument("--output", required=True)
    parser.add_argument("--metadata-output", default=None)
    args = parser.parse_args()

    addresses, metadata = select_addresses(Path(args.history_dir), args.chain)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(f"{address}\n" for address in addresses), encoding="utf-8")

    if args.metadata_output:
        metadata_output = Path(args.metadata_output)
        metadata_output.parent.mkdir(parents=True, exist_ok=True)
        metadata_output.write_text(json.dumps(metadata, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(metadata, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
