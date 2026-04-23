#!/usr/bin/env python3
"""
Compare canonical per-address subaccount history against existing earn-netflow output.

This is a strict data-consistency tool, not a verification shortcut:
  - sum canonical event deltas per owner/market up to a comparison block
  - compare them against netflow `t`
  - surface exact mismatches, missing coverage, and address/market gaps
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from build_earn_subaccount_history import _load_known_addresses, _read_json
from scan_earn_netflow import CHAINS


ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
DEFAULT_NETFLOW_DIR = ROOT / "data" / "earn-netflow"


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


def _load_netflow_payload(netflow_dir: Path, chain: str) -> dict:
    payload = _read_json(netflow_dir / f"{chain}.json", {})
    return payload if isinstance(payload, dict) else {}


def _derive_history_market_totals(
    history_payload: dict,
    *,
    comparison_block: int,
) -> Tuple[Dict[str, int], int]:
    totals: Dict[str, int] = {}
    included_events = 0
    accounts = history_payload.get("accounts") or {}
    for account_state in accounts.values():
        markets = account_state.get("markets") or {}
        for market_id, market_state in markets.items():
            for event in market_state.get("events") or []:
                try:
                    block_number = int(event.get("blockNumber"))
                except Exception:
                    continue
                if block_number > comparison_block:
                    continue
                totals[market_id] = totals.get(market_id, 0) + int(event.get("deltaWei") or 0)
                included_events += 1
    return totals, included_events


def _market_diff_rows(history_totals: Dict[str, int], netflow_markets: Dict[str, dict]) -> List[dict]:
    market_ids = sorted(set(history_totals.keys()) | set((netflow_markets or {}).keys()), key=lambda value: int(value))
    rows = []
    for market_id in market_ids:
        netflow_entry = (netflow_markets or {}).get(market_id) or {}
        history_total = int(history_totals.get(market_id, 0))
        netflow_total = int(netflow_entry.get("t") or 0)
        diff = history_total - netflow_total
        if diff == 0:
            continue
        rows.append({
            "marketId": market_id,
            "historyTotalDeltaWei": str(history_total),
            "netflowTotalWei": str(netflow_total),
            "diffWei": str(diff),
            "netflowComponents": {
                key: str(netflow_entry.get(key) or "0")
                for key in ("d", "w", "s", "x", "l", "v")
            },
        })
    return rows


def build_consistency_report(
    chain: str,
    *,
    history_dir: Path,
    netflow_dir: Path,
    addresses: Sequence[str],
    comparison_block: Optional[int],
) -> dict:
    netflow_payload = _load_netflow_payload(netflow_dir, chain)
    try:
        netflow_last_block = int(netflow_payload.get("lastBlock") or 0)
    except Exception:
        netflow_last_block = 0
    resolved_comparison_block = int(comparison_block) if comparison_block is not None else netflow_last_block

    results = []
    matched = 0
    mismatched = 0
    missing_history = 0
    missing_netflow = 0

    for address in addresses:
        history_path = _history_path(history_dir, chain, address)
        history_payload = _read_json(history_path, None)
        netflow_markets = (netflow_payload.get("netflows") or {}).get(address)

        if not isinstance(history_payload, dict):
            results.append({
                "address": address,
                "status": "missing_history",
            })
            missing_history += 1
            continue

        if not isinstance(netflow_markets, dict):
            results.append({
                "address": address,
                "status": "missing_netflow",
            })
            missing_netflow += 1
            continue

        try:
            history_last_block = int(history_payload.get("lastScannedBlock") or 0)
        except Exception:
            history_last_block = 0

        if history_last_block < resolved_comparison_block:
            results.append({
                "address": address,
                "status": "history_behind_comparison_block",
                "historyLastScannedBlock": history_last_block,
                "comparisonBlock": resolved_comparison_block,
            })
            continue

        history_totals, included_events = _derive_history_market_totals(
            history_payload,
            comparison_block=resolved_comparison_block,
        )
        diff_rows = _market_diff_rows(history_totals, netflow_markets)

        if diff_rows:
            mismatched += 1
            results.append({
                "address": address,
                "status": "mismatch",
                "historyLastScannedBlock": history_last_block,
                "comparisonBlock": resolved_comparison_block,
                "includedEventCount": included_events,
                "marketDiffs": diff_rows,
            })
        else:
            matched += 1
            results.append({
                "address": address,
                "status": "match",
                "historyLastScannedBlock": history_last_block,
                "comparisonBlock": resolved_comparison_block,
                "includedEventCount": included_events,
            })

    return {
        "chain": chain,
        "comparisonBlock": resolved_comparison_block,
        "netflowLastBlock": netflow_last_block,
        "selectedAddressCount": len(addresses),
        "matchedAddressCount": matched,
        "mismatchedAddressCount": mismatched,
        "missingHistoryCount": missing_history,
        "missingNetflowCount": missing_netflow,
        "results": results,
    }


def _print_human_report(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Comparison block: {payload['comparisonBlock']:,}")
    print(f"Netflow last block: {payload['netflowLastBlock']:,}")
    print(f"Selected addresses: {payload['selectedAddressCount']}")
    print(f"Matched: {payload['matchedAddressCount']}")
    print(f"Mismatched: {payload['mismatchedAddressCount']}")
    print(f"Missing history: {payload['missingHistoryCount']}")
    print(f"Missing netflow: {payload['missingNetflowCount']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare canonical history against earn-netflow totals")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--address", action="append", default=[], help="Explicit address to compare")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--end-index", type=int, default=None)
    parser.add_argument("--comparison-block", type=int, default=None)
    parser.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR))
    parser.add_argument("--netflow-dir", default=str(DEFAULT_NETFLOW_DIR))
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

    payload = build_consistency_report(
        args.chain,
        history_dir=Path(args.history_dir),
        netflow_dir=Path(args.netflow_dir),
        addresses=addresses,
        comparison_block=args.comparison_block,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        _print_human_report(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
