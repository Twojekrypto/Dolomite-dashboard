#!/usr/bin/env python3
"""
Report canonical EARN subaccount-history coverage per wallet and per market.

This is a strict coverage report:
  - whether a wallet has a fresh canonical history at the target block
  - which markets are already covered by fresh wallet histories
  - where coverage is still missing or partial
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from build_earn_subaccount_history import _load_known_addresses, _read_json
from materialize_earn_subaccount_history import _chain_manifest


ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
DEFAULT_EVENTS_DIR = ROOT / "data" / "earn-subaccount-history-events"


def _select_addresses(
    chain: str,
    *,
    explicit_addresses: Sequence[str],
    limit: Optional[int],
    start_index: int,
    end_index: Optional[int],
) -> List[str]:
    if explicit_addresses:
        addresses = sorted({
            str(address).strip().lower()
            for address in explicit_addresses
            if str(address).strip()
        })
    else:
        addresses = _load_known_addresses(chain)

    start = max(0, int(start_index))
    stop = len(addresses) if end_index is None else max(start, int(end_index))
    sliced = addresses[start:stop]
    if limit is not None:
        sliced = sliced[: max(0, int(limit))]
    return sliced


def _history_path(history_dir: Path, chain: str, address: str) -> Path:
    return history_dir / chain / f"{address.lower()}.json"


def _active_scan_progress_target(events_dir: Path, chain: str) -> Optional[int]:
    progress_dir = events_dir / ".progress"
    targets: List[int] = []
    for path in sorted(progress_dir.glob(f"{chain}--*.json")):
        payload = _read_json(path, None)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("status") or "") not in {"running", "completed"}:
            continue
        try:
            value = int(payload.get("toBlock") or 0)
        except Exception:
            continue
        if value > 0:
            targets.append(value)
    return max(targets) if targets else None


def _resolve_target_block(events_dir: Path, chain: str, target_block: Optional[int]) -> int:
    if target_block is not None:
        return int(target_block)
    progress_target = _active_scan_progress_target(events_dir, chain)
    if progress_target is not None:
        return progress_target
    manifest = _chain_manifest(events_dir, chain)
    for key in ("globalToBlock", "toBlock"):
        try:
            value = int(manifest.get(key))
            if value > 0:
                return value
        except Exception:
            continue
    progress_dir = events_dir / ".progress"
    for path in (
        progress_dir / f"{chain}-scan-launch.json",
        progress_dir / f"{chain}-one-pass-plan.json",
    ):
        payload = _read_json(path, {})
        if not isinstance(payload, dict):
            continue
        try:
            value = int(payload.get("targetBlock"))
            if value > 0:
                return value
        except Exception:
            continue
    raise ValueError(f"Could not resolve target block for {chain}")


def _wallet_status(history_payload: Optional[dict], target_block: int) -> dict:
    if not isinstance(history_payload, dict):
        return {
            "status": "missing_history",
            "lastScannedBlock": None,
        }
    try:
        last_scanned_block = int(history_payload.get("lastScannedBlock") or 0)
    except Exception:
        last_scanned_block = 0
    if last_scanned_block < target_block:
        return {
            "status": "history_behind_target",
            "lastScannedBlock": last_scanned_block,
        }
    return {
        "status": "fresh",
        "lastScannedBlock": last_scanned_block,
    }


def _accumulate_market_coverage(market_rows: Dict[str, dict], address: str, history_payload: dict) -> None:
    accounts = history_payload.get("accounts") or {}
    for account_key, account_state in accounts.items():
        markets = account_state.get("markets") or {}
        for market_id, market_state in markets.items():
            row = market_rows.setdefault(market_id, {
                "marketId": market_id,
                "walletCount": 0,
                "accountCount": 0,
                "eventCount": 0,
                "borrowWalletCount": 0,
                "unknownAccountWalletCount": 0,
                "wallets": set(),
                "borrowWallets": set(),
                "unknownWallets": set(),
            })
            if address not in row["wallets"]:
                row["wallets"].add(address)
                row["walletCount"] += 1
            row["accountCount"] += 1
            row["eventCount"] += int(market_state.get("eventCount") or 0)
            if bool(account_state.get("hasBorrow")) and address not in row["borrowWallets"]:
                row["borrowWallets"].add(address)
                row["borrowWalletCount"] += 1
            if str(account_key) == "legacy-unknown" and address not in row["unknownWallets"]:
                row["unknownWallets"].add(address)
                row["unknownAccountWalletCount"] += 1


def build_coverage_report(
    chain: str,
    *,
    history_dir: Path,
    events_dir: Path,
    addresses: Sequence[str],
    target_block: Optional[int],
    include_wallets: bool,
) -> dict:
    resolved_target_block = _resolve_target_block(events_dir, chain, target_block)
    wallet_rows = []
    market_rows: Dict[str, dict] = {}
    fresh_count = 0
    partial_count = 0
    missing_count = 0

    for address in addresses:
        history_payload = _read_json(_history_path(history_dir, chain, address), None)
        status_row = _wallet_status(history_payload, resolved_target_block)
        status = status_row["status"]
        if status == "fresh":
            fresh_count += 1
            _accumulate_market_coverage(market_rows, address, history_payload)
        elif status == "history_behind_target":
            partial_count += 1
        else:
            missing_count += 1
        if include_wallets:
            wallet_rows.append({
                "address": address,
                **status_row,
            })

    normalized_markets = []
    for market_id, row in sorted(market_rows.items(), key=lambda item: int(item[0])):
        normalized_markets.append({
            "marketId": market_id,
            "walletCount": row["walletCount"],
            "accountCount": row["accountCount"],
            "eventCount": row["eventCount"],
            "borrowWalletCount": row["borrowWalletCount"],
            "unknownAccountWalletCount": row["unknownAccountWalletCount"],
        })

    payload = {
        "chain": chain,
        "targetBlock": resolved_target_block,
        "selectedAddressCount": len(addresses),
        "freshWalletCount": fresh_count,
        "partialWalletCount": partial_count,
        "missingWalletCount": missing_count,
        "freshCoverageRatio": (fresh_count / len(addresses)) if addresses else 0.0,
        "marketCoverage": normalized_markets,
    }
    if include_wallets:
        payload["walletCoverage"] = wallet_rows
    return payload


def _print_human_report(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Target block: {payload['targetBlock']:,}")
    print(f"Selected wallets: {payload['selectedAddressCount']}")
    print(f"Fresh wallets: {payload['freshWalletCount']}")
    print(f"Partial wallets: {payload['partialWalletCount']}")
    print(f"Missing wallets: {payload['missingWalletCount']}")
    print(f"Fresh coverage ratio: {payload['freshCoverageRatio']:.2%}")
    print(f"Covered markets: {len(payload['marketCoverage'])}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Report canonical EARN history coverage per wallet and market")
    parser.add_argument("--chain", default="arbitrum")
    parser.add_argument("--address", action="append", default=[], help="Explicit address to report")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--end-index", type=int, default=None)
    parser.add_argument("--target-block", type=int, default=None)
    parser.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR))
    parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--include-wallets", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    addresses = _select_addresses(
        args.chain,
        explicit_addresses=args.address,
        limit=args.limit,
        start_index=args.start_index,
        end_index=args.end_index,
    )
    if not addresses:
        raise SystemExit("No addresses selected")

    payload = build_coverage_report(
        args.chain,
        history_dir=Path(args.history_dir),
        events_dir=Path(args.events_dir),
        addresses=addresses,
        target_block=args.target_block,
        include_wallets=bool(args.include_wallets),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        _print_human_report(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
