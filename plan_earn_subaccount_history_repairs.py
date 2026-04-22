#!/usr/bin/env python3
"""
Plan targeted repair materialization for canonical EARN subaccount histories.

This tool identifies only the wallets that are still missing or stale at the
current target block and writes deterministic address shards for repair runs.
"""

from __future__ import annotations

import argparse
import json
import math
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from build_earn_subaccount_history import _load_known_addresses, _read_json
from materialize_earn_subaccount_history import DEFAULT_EVENTS_DIR, DEFAULT_HISTORY_OUTPUT_DIR
from report_earn_subaccount_history_coverage import _resolve_target_block
from scan_earn_netflow import CHAINS


ROOT = Path(__file__).resolve().parent
DEFAULT_PLAN_DIR = ROOT / "data" / "earn-subaccount-history" / ".repair-plans"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def _history_last_block(history_dir: Path, chain: str, address: str) -> int:
    payload = _read_json(_history_path(history_dir, chain, address), None)
    if not isinstance(payload, dict):
        return 0
    try:
        return int(payload.get("lastScannedBlock") or 0)
    except Exception:
        return 0


def _normalized_worker_count(requested: int, total_addresses: int) -> int:
    if total_addresses <= 0:
        return 0
    return max(1, min(int(requested), total_addresses))


def _write_address_file(path: Path, addresses: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{address}\n" for address in addresses), encoding="utf-8")


def build_repair_plan(
    chain: str,
    *,
    history_dir: Path,
    events_dir: Path,
    output_dir: Path,
    addresses: Sequence[str],
    target_block: Optional[int],
    workers: int,
) -> dict:
    resolved_target_block = _resolve_target_block(events_dir, chain, target_block)

    missing_addresses: List[str] = []
    stale_addresses: List[str] = []
    fresh_addresses: List[str] = []

    for address in addresses:
        last_block = _history_last_block(history_dir, chain, address)
        if last_block <= 0:
            missing_addresses.append(address)
        elif last_block < resolved_target_block:
            stale_addresses.append(address)
        else:
            fresh_addresses.append(address)

    repair_addresses = sorted(set(missing_addresses + stale_addresses))
    worker_count = _normalized_worker_count(workers, len(repair_addresses))
    plan_id = f"{chain}-t{resolved_target_block}-repair-{len(repair_addresses)}a-{worker_count or 0}w"
    plan_root = output_dir / plan_id
    plan_root.mkdir(parents=True, exist_ok=True)

    tasks = []
    if worker_count > 0:
        shard_size = math.ceil(len(repair_addresses) / worker_count)
        for worker_idx in range(worker_count):
            start_index = worker_idx * shard_size
            end_index = min(len(repair_addresses), start_index + shard_size)
            if start_index >= end_index:
                continue
            worker_addresses = repair_addresses[start_index:end_index]
            progress_key = f"repair-t{resolved_target_block}-m{worker_idx + 1}of{worker_count}"
            address_file = plan_root / f"{progress_key}.txt"
            _write_address_file(address_file, worker_addresses)
            command = (
                "python3 materialize_earn_subaccount_history.py "
                f"--chain {shlex.quote(chain)} "
                f"--address-file {shlex.quote(str(address_file))} "
                f"--progress-key {shlex.quote(progress_key)} "
                f"--events-dir {shlex.quote(str(events_dir))} "
                f"--output-dir {shlex.quote(str(history_dir))}"
            )
            tasks.append({
                "progressKey": progress_key,
                "addressFile": str(address_file),
                "addressCount": len(worker_addresses),
                "startIndex": start_index,
                "endIndex": end_index,
                "command": command,
            })

    payload = {
        "generatedAt": _utc_now_iso(),
        "chain": chain,
        "targetBlock": resolved_target_block,
        "selectedAddressCount": len(addresses),
        "freshAddressCount": len(fresh_addresses),
        "missingAddressCount": len(missing_addresses),
        "staleAddressCount": len(stale_addresses),
        "repairAddressCount": len(repair_addresses),
        "workerCount": worker_count,
        "planId": plan_id,
        "planRoot": str(plan_root),
        "tasks": tasks,
    }
    plan_path = plan_root / "repair-plan.json"
    plan_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    payload["planPath"] = str(plan_path)
    return payload


def _print_human_report(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Target block: {payload['targetBlock']:,}")
    print(f"Selected addresses: {payload['selectedAddressCount']}")
    print(f"Fresh addresses: {payload['freshAddressCount']}")
    print(f"Missing addresses: {payload['missingAddressCount']}")
    print(f"Stale addresses: {payload['staleAddressCount']}")
    print(f"Repair addresses: {payload['repairAddressCount']}")
    print(f"Workers: {payload['workerCount']}")
    print(f"Plan path: {payload['planPath']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan targeted repair materialization for canonical EARN history")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--address", action="append", default=[], help="Explicit address to include")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--end-index", type=int, default=None)
    parser.add_argument("--target-block", type=int, default=None)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--history-dir", default=str(DEFAULT_HISTORY_OUTPUT_DIR))
    parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_PLAN_DIR))
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

    payload = build_repair_plan(
        args.chain,
        history_dir=Path(args.history_dir),
        events_dir=Path(args.events_dir),
        output_dir=Path(args.output_dir),
        addresses=addresses,
        target_block=args.target_block,
        workers=args.workers,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        _print_human_report(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
