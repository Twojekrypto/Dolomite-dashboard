#!/usr/bin/env python3
"""
Explain canonical-history vs earn-netflow mismatches at market/account level.

This tool is diagnostic only. It does not upgrade any result to verified.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from build_earn_subaccount_history import _load_known_addresses, _read_json
from check_earn_subaccount_history_consistency import (
    DEFAULT_HISTORY_DIR,
    DEFAULT_NETFLOW_DIR,
    _derive_history_market_totals,
    _history_path,
    _load_netflow_payload,
    _market_diff_rows,
    build_consistency_report,
)
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


def _history_market_account_breakdown(history_payload: dict, comparison_block: int) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    accounts = history_payload.get("accounts") or {}
    for account_key, account_state in accounts.items():
        markets = account_state.get("markets") or {}
        for market_id, market_state in markets.items():
            row = result.setdefault(market_id, {
                "marketId": market_id,
                "accounts": {},
                "historyTotalDeltaWei": "0",
                "hasBorrowAccount": False,
                "hasLegacyUnknownAccount": False,
            })
            account_total = 0
            event_count = 0
            for event in market_state.get("events") or []:
                try:
                    block_number = int(event.get("blockNumber"))
                except Exception:
                    continue
                if block_number > comparison_block:
                    continue
                account_total += int(event.get("deltaWei") or 0)
                event_count += 1
            if event_count == 0:
                continue
            row["accounts"][str(account_key)] = {
                "account": str(account_key),
                "accountKnown": str(account_key) != "legacy-unknown",
                "hasBorrow": bool(account_state.get("hasBorrow")),
                "eventCount": event_count,
                "totalDeltaWei": str(account_total),
            }
            row["historyTotalDeltaWei"] = str(int(row["historyTotalDeltaWei"]) + account_total)
            if bool(account_state.get("hasBorrow")):
                row["hasBorrowAccount"] = True
            if str(account_key) == "legacy-unknown":
                row["hasLegacyUnknownAccount"] = True
    return result


def _likely_causes(market_id: str, history_breakdown: dict, netflow_entry: dict) -> List[str]:
    causes = []
    has_netflow_market = bool(netflow_entry)
    accounts = history_breakdown.get("accounts") or {}
    account_count = len(accounts)
    has_legacy_unknown = bool(history_breakdown.get("hasLegacyUnknownAccount"))
    has_borrow = bool(history_breakdown.get("hasBorrowAccount"))

    if not has_netflow_market:
        causes.append("netflow_missing_market")
    if not accounts and has_netflow_market:
        causes.append("history_missing_market")
    if has_legacy_unknown:
        causes.append("legacy_unknown_contribution")
    if account_count > 1 and has_borrow:
        causes.append("mixed_subaccount_borrow_mismatch")
    elif account_count > 1:
        causes.append("multi_account_market_mismatch")
    elif has_borrow:
        causes.append("borrow_account_mismatch")
    if not causes:
        causes.append("single_account_total_mismatch")
    return causes


def build_mismatch_explanations(
    chain: str,
    *,
    history_dir: Path,
    netflow_dir: Path,
    addresses: Sequence[str],
    comparison_block: Optional[int],
) -> dict:
    consistency = build_consistency_report(
        chain,
        history_dir=history_dir,
        netflow_dir=netflow_dir,
        addresses=addresses,
        comparison_block=comparison_block,
    )
    netflow_payload = _load_netflow_payload(netflow_dir, chain)
    explanations = []

    for row in consistency.get("results") or []:
        if row.get("status") != "mismatch":
            continue
        address = str(row["address"]).lower()
        history_payload = _read_json(_history_path(history_dir, chain, address), None)
        if not isinstance(history_payload, dict):
            continue
        comparison_block_value = int(row["comparisonBlock"])
        history_totals, _ = _derive_history_market_totals(history_payload, comparison_block=comparison_block_value)
        history_breakdown = _history_market_account_breakdown(history_payload, comparison_block_value)
        netflow_markets = (netflow_payload.get("netflows") or {}).get(address) or {}
        market_diffs = _market_diff_rows(history_totals, netflow_markets)
        explained_markets = []
        for market_diff in market_diffs:
            market_id = str(market_diff["marketId"])
            breakdown = history_breakdown.get(market_id) or {
                "marketId": market_id,
                "accounts": {},
                "historyTotalDeltaWei": str(history_totals.get(market_id, 0)),
                "hasBorrowAccount": False,
                "hasLegacyUnknownAccount": False,
            }
            netflow_entry = netflow_markets.get(market_id) or {}
            explained_markets.append({
                **market_diff,
                "accountBreakdown": list((breakdown.get("accounts") or {}).values()),
                "hasBorrowAccount": bool(breakdown.get("hasBorrowAccount")),
                "hasLegacyUnknownAccount": bool(breakdown.get("hasLegacyUnknownAccount")),
                "likelyCauses": _likely_causes(market_id, breakdown, netflow_entry),
            })
        explanations.append({
            "address": address,
            "comparisonBlock": comparison_block_value,
            "historyLastScannedBlock": row.get("historyLastScannedBlock"),
            "marketExplanations": explained_markets,
        })

    return {
        "chain": chain,
        "comparisonBlock": consistency.get("comparisonBlock"),
        "selectedAddressCount": len(addresses),
        "mismatchAddressCount": len(explanations),
        "explanations": explanations,
    }


def _print_human_report(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Comparison block: {payload['comparisonBlock']:,}")
    print(f"Selected addresses: {payload['selectedAddressCount']}")
    print(f"Mismatch addresses: {payload['mismatchAddressCount']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Explain canonical history mismatches against earn-netflow")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--address", action="append", default=[], help="Explicit address to explain")
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

    payload = build_mismatch_explanations(
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
