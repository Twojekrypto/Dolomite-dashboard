#!/usr/bin/env python3
"""
Report strict provenance for canonical EARN subaccount-history data.

This tool separates two different questions:
  - coverage: do we have fresh canonical history at the selected target block?
  - consistency: does that canonical history match earn-netflow at the exact comparison block?

It does not upgrade any wallet to verified. It only makes provenance explicit.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from build_earn_subaccount_history import _load_known_addresses, _read_json
from check_earn_subaccount_history_consistency import (
    DEFAULT_NETFLOW_DIR,
    _derive_history_market_totals,
    _history_path,
    _load_netflow_payload,
)
from explain_earn_subaccount_history_mismatches import _history_market_account_breakdown
from report_earn_subaccount_history_coverage import DEFAULT_EVENTS_DIR, DEFAULT_HISTORY_DIR, _resolve_target_block
from scan_earn_netflow import CHAINS


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


def _market_sort_key(value: str) -> tuple:
    try:
        return (0, int(value))
    except Exception:
        return (1, str(value))


def _wallet_coverage_status(history_payload: Optional[dict], *, target_block: int) -> tuple[str, Optional[int]]:
    if not isinstance(history_payload, dict):
        return "missing_history", None
    try:
        last_scanned_block = int(history_payload.get("lastScannedBlock") or 0)
    except Exception:
        last_scanned_block = 0
    if last_scanned_block < target_block:
        return "history_behind_target", last_scanned_block
    return "fresh", last_scanned_block


def _market_row(
    market_id: str,
    *,
    breakdown: dict,
    netflow_entry: Optional[dict],
) -> dict:
    accounts = breakdown.get("accounts") or {}
    history_total = int(breakdown.get("historyTotalDeltaWei") or 0)
    has_netflow = isinstance(netflow_entry, dict) and bool(netflow_entry)
    netflow_total = int((netflow_entry or {}).get("t") or 0)
    diff = history_total - netflow_total

    if accounts and has_netflow:
        consistency_status = "match" if diff == 0 else "mismatch"
    elif accounts:
        consistency_status = "history_only"
    else:
        consistency_status = "netflow_only"

    return {
        "marketId": market_id,
        "consistencyStatus": consistency_status,
        "historyTotalDeltaWei": str(history_total),
        "netflowTotalWei": str(netflow_total),
        "diffWei": str(diff),
        "accountCount": len(accounts),
        "eventCount": sum(int(account.get("eventCount") or 0) for account in accounts.values()),
        "hasBorrowAccount": bool(breakdown.get("hasBorrowAccount")),
        "hasLegacyUnknownAccount": bool(breakdown.get("hasLegacyUnknownAccount")),
        "accountBreakdown": list(accounts.values()),
        "netflowComponents": {
            key: str((netflow_entry or {}).get(key) or "0")
            for key in ("d", "w", "s", "x", "l", "v")
        },
    }


def build_provenance_report(
    chain: str,
    *,
    history_dir: Path,
    events_dir: Path,
    netflow_dir: Path,
    addresses: Sequence[str],
    target_block: Optional[int],
    comparison_block: Optional[int],
    include_markets: bool,
) -> dict:
    resolved_target_block = _resolve_target_block(events_dir, chain, target_block)
    netflow_payload = _load_netflow_payload(netflow_dir, chain)
    try:
        netflow_last_block = int(netflow_payload.get("lastBlock") or 0)
    except Exception:
        netflow_last_block = 0
    resolved_comparison_block = int(comparison_block) if comparison_block is not None else (netflow_last_block or None)

    coverage_counts = {
        "fresh": 0,
        "history_behind_target": 0,
        "missing_history": 0,
    }
    consistency_counts = {
        "match": 0,
        "mismatch": 0,
        "missing_netflow": 0,
        "history_behind_comparison_block": 0,
        "comparison_unavailable": 0,
        "missing_history": 0,
    }
    wallet_rows = []

    for address in addresses:
        history_payload = _read_json(_history_path(history_dir, chain, address), None)
        coverage_status, history_last_scanned_block = _wallet_coverage_status(
            history_payload,
            target_block=resolved_target_block,
        )
        coverage_counts[coverage_status] += 1

        row = {
            "address": address,
            "coverageStatus": coverage_status,
            "historyLastScannedBlock": history_last_scanned_block,
            "targetBlock": resolved_target_block,
            "comparisonBlock": resolved_comparison_block,
            "consistencyStatus": None,
            "summary": None,
            "scanRange": None,
            "sourceMetadata": None,
        }

        if not isinstance(history_payload, dict):
            row["consistencyStatus"] = "missing_history"
            consistency_counts["missing_history"] += 1
            wallet_rows.append(row)
            continue

        row["summary"] = history_payload.get("summary") or {}
        row["scanRange"] = history_payload.get("scanRange") or {}
        row["sourceMetadata"] = history_payload.get("sourceMetadata") or {}

        netflow_markets = (netflow_payload.get("netflows") or {}).get(address)
        if resolved_comparison_block is None:
            row["consistencyStatus"] = "comparison_unavailable"
            consistency_counts["comparison_unavailable"] += 1
            wallet_rows.append(row)
            continue

        if not isinstance(netflow_markets, dict):
            row["consistencyStatus"] = "missing_netflow"
            consistency_counts["missing_netflow"] += 1
            wallet_rows.append(row)
            continue

        if int(history_last_scanned_block or 0) < resolved_comparison_block:
            row["consistencyStatus"] = "history_behind_comparison_block"
            consistency_counts["history_behind_comparison_block"] += 1
            wallet_rows.append(row)
            continue

        history_totals, included_event_count = _derive_history_market_totals(
            history_payload,
            comparison_block=resolved_comparison_block,
        )
        history_breakdown = _history_market_account_breakdown(history_payload, resolved_comparison_block)
        market_ids = sorted(
            set(history_breakdown.keys()) | set((netflow_markets or {}).keys()),
            key=_market_sort_key,
        )

        market_rows = []
        mismatch_market_count = 0
        matched_market_count = 0
        history_only_market_count = 0
        netflow_only_market_count = 0

        for market_id in market_ids:
            breakdown = history_breakdown.get(market_id) or {
                "marketId": market_id,
                "accounts": {},
                "historyTotalDeltaWei": str(history_totals.get(market_id, 0)),
                "hasBorrowAccount": False,
                "hasLegacyUnknownAccount": False,
            }
            market_row = _market_row(
                market_id,
                breakdown=breakdown,
                netflow_entry=(netflow_markets or {}).get(market_id),
            )
            if market_row["consistencyStatus"] == "mismatch":
                mismatch_market_count += 1
            elif market_row["consistencyStatus"] == "match":
                matched_market_count += 1
            elif market_row["consistencyStatus"] == "history_only":
                history_only_market_count += 1
            elif market_row["consistencyStatus"] == "netflow_only":
                netflow_only_market_count += 1
            if include_markets:
                market_rows.append(market_row)

        wallet_consistency_status = (
            "mismatch"
            if mismatch_market_count > 0 or history_only_market_count > 0 or netflow_only_market_count > 0
            else "match"
        )
        consistency_counts[wallet_consistency_status] += 1

        row.update({
            "consistencyStatus": wallet_consistency_status,
            "includedEventCount": included_event_count,
            "matchedMarketCount": matched_market_count,
            "mismatchMarketCount": mismatch_market_count,
            "historyOnlyMarketCount": history_only_market_count,
            "netflowOnlyMarketCount": netflow_only_market_count,
        })
        if include_markets:
            row["markets"] = market_rows
        wallet_rows.append(row)

    return {
        "chain": chain,
        "targetBlock": resolved_target_block,
        "comparisonBlock": resolved_comparison_block,
        "netflowLastBlock": netflow_last_block,
        "selectedAddressCount": len(addresses),
        "coverageCounts": coverage_counts,
        "consistencyCounts": consistency_counts,
        "wallets": wallet_rows,
    }


def _print_human_report(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Target block: {payload['targetBlock']:,}")
    print(f"Comparison block: {payload['comparisonBlock']:,}" if payload.get("comparisonBlock") else "Comparison block: None")
    print(f"Netflow last block: {payload['netflowLastBlock']:,}")
    print(f"Selected wallets: {payload['selectedAddressCount']}")
    print(
        "Coverage: "
        f"fresh={payload['coverageCounts']['fresh']} "
        f"behind={payload['coverageCounts']['history_behind_target']} "
        f"missing={payload['coverageCounts']['missing_history']}"
    )
    print(
        "Consistency: "
        f"match={payload['consistencyCounts']['match']} "
        f"mismatch={payload['consistencyCounts']['mismatch']} "
        f"missing_netflow={payload['consistencyCounts']['missing_netflow']} "
        f"behind_comparison={payload['consistencyCounts']['history_behind_comparison_block']}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Report canonical EARN provenance per wallet and market")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--address", action="append", default=[], help="Explicit address to include")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--end-index", type=int, default=None)
    parser.add_argument("--target-block", type=int, default=None)
    parser.add_argument("--comparison-block", type=int, default=None)
    parser.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR))
    parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--netflow-dir", default=str(DEFAULT_NETFLOW_DIR))
    parser.add_argument("--include-markets", action="store_true")
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

    payload = build_provenance_report(
        args.chain,
        history_dir=Path(args.history_dir),
        events_dir=Path(args.events_dir),
        netflow_dir=Path(args.netflow_dir),
        addresses=addresses,
        target_block=args.target_block,
        comparison_block=args.comparison_block,
        include_markets=bool(args.include_markets),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        _print_human_report(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
