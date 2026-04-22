#!/usr/bin/env python3
"""
Plan a strict incremental update cycle for canonical EARN subaccount histories.

Incremental cycle:
  1. scan only new blocks for already-tracked addresses into a dedicated delta directory
  2. fully backfill newly-discovered addresses directly from chain start to the new target
  3. apply the delta onto all existing tracked histories, appending touched events and
     metadata-stamping untouched histories to the same target block
"""

from __future__ import annotations

import argparse
import json
import math
import shlex
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from build_earn_subaccount_history import CHAINS, DEFAULT_OUTPUT_DIR, _load_known_addresses, _read_json
from materialize_earn_subaccount_history import DEFAULT_EVENTS_DIR
from plan_earn_data_correctness import _resolve_target_block, _worker_counts


ROOT = Path(__file__).resolve().parent
DEFAULT_PLAN_DIR = ROOT / "data" / "earn-subaccount-history" / ".incremental-plans"


def _history_chain_dir(history_dir: Path, chain: str) -> Path:
    return history_dir / chain


def _history_addresses(history_dir: Path, chain: str) -> List[str]:
    chain_dir = _history_chain_dir(history_dir, chain)
    if not chain_dir.exists():
        return []
    return sorted(
        path.stem.lower()
        for path in chain_dir.glob("0x*.json")
        if path.is_file() and len(path.stem) == 42
    )


def _history_last_block(path: Path) -> int:
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        return 0
    try:
        return int(payload.get("lastScannedBlock") or 0)
    except Exception:
        return 0


def _base_target_block(history_dir: Path, chain: str) -> int:
    manifest = _read_json(history_dir / "manifest.json", {})
    chains = manifest.get("chains") or {}
    chain_payload = chains.get(chain) or {}
    try:
        value = int(chain_payload.get("lastBlock") or 0)
        if value > 0:
            return value
    except Exception:
        pass

    existing = []
    for address in _history_addresses(history_dir, chain):
        last_block = _history_last_block(_history_chain_dir(history_dir, chain) / f"{address}.json")
        if last_block > 0:
            existing.append(last_block)
    return min(existing) if existing else 0


def _write_lines(path: Path, values: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{value}\n" for value in values), encoding="utf-8")


def _split_ranges(total_count: int, worker_count: int) -> List[tuple[int, int]]:
    shard_size = math.ceil(total_count / max(1, worker_count))
    ranges = []
    for idx in range(worker_count):
        start_index = idx * shard_size
        end_index = min(total_count, start_index + shard_size)
        if start_index >= end_index:
            continue
        ranges.append((start_index, end_index))
    return ranges


def build_incremental_plan(
    chain: str,
    *,
    events_dir: Path,
    history_dir: Path,
    plan_dir: Path,
    to_block: Optional[int],
    max_scan_workers: Optional[int],
    max_apply_workers: Optional[int],
    max_new_backfill_workers: Optional[int],
) -> dict:
    current_known = sorted(set(_load_known_addresses(chain)))
    existing_addresses = _history_addresses(history_dir, chain)
    existing_set = set(existing_addresses)
    base_target = _base_target_block(history_dir, chain)
    target_block = _resolve_target_block(chain, to_block)

    if base_target <= 0:
        raise ValueError("No canonical baseline found; run the full data-correctness pipeline first")
    if target_block < base_target:
        raise ValueError(f"Target block {target_block} is behind baseline {base_target}")

    fresh_existing: List[str] = []
    stale_existing: List[str] = []
    for address in existing_addresses:
        last_block = _history_last_block(_history_chain_dir(history_dir, chain) / f"{address}.json")
        if last_block >= base_target:
            fresh_existing.append(address)
        else:
            stale_existing.append(address)

    new_addresses = sorted(set(current_known) - existing_set)
    orphaned_histories = sorted(existing_set - set(current_known))
    delta_required = target_block > base_target
    delta_from_block = base_target + 1

    cycle_id = f"{chain}-f{delta_from_block}-t{target_block}"
    cycle_root = plan_dir / cycle_id
    delta_events_dir = cycle_root / "events"
    address_dir = cycle_root / "addresses"

    tracked_address_file = address_dir / "tracked-addresses.txt"
    _write_lines(tracked_address_file, fresh_existing)

    scan_workers = _worker_counts(max_scan_workers)[-1]
    apply_workers = _worker_counts(max_apply_workers)[-1]
    new_backfill_workers = _worker_counts(max_new_backfill_workers)[-1]

    scan_tasks = []
    if delta_required and fresh_existing:
        total_blocks = target_block - delta_from_block + 1
        shard_size = math.ceil(total_blocks / max(1, scan_workers))
        current_start = delta_from_block
        for idx in range(scan_workers):
            shard_end = min(target_block, current_start + shard_size - 1)
            if current_start > shard_end:
                break
            progress_key = f"d{idx + 1}of{scan_workers}"
            scan_tasks.append({
                "progressKey": progress_key,
                "fromBlock": int(current_start),
                "toBlock": int(shard_end),
                "eventsDir": str(delta_events_dir),
                "addressFile": str(tracked_address_file),
                "command": " ".join([
                "python3 scan_earn_subaccount_history_events.py",
                    f"--chain {shlex.quote(chain)}",
                    f"--address-file {shlex.quote(str(tracked_address_file))}",
                    f"--from-block {current_start}",
                    f"--to-block {shard_end}",
                    f"--progress-key {shlex.quote(progress_key)}",
                    f"--output-dir {shlex.quote(str(delta_events_dir))}",
                ]),
            })
            current_start = shard_end + 1

    new_address_tasks = []
    if new_addresses:
        for idx, (start_index, end_index) in enumerate(_split_ranges(len(new_addresses), new_backfill_workers), start=1):
            progress_key = f"n{idx}of{new_backfill_workers}"
            address_file = address_dir / f"new-addresses-{progress_key}.txt"
            shard_addresses = new_addresses[start_index:end_index]
            _write_lines(address_file, shard_addresses)
            new_address_tasks.append({
                "progressKey": progress_key,
                "addressFile": str(address_file),
                "addressCount": len(shard_addresses),
                "command": " ".join([
                "python3 build_earn_subaccount_history.py",
                    f"--chain {shlex.quote(chain)}",
                    f"--address-file {shlex.quote(str(address_file))}",
                    f"--to-block {target_block}",
                    f"--output-dir {shlex.quote(str(history_dir))}",
                ]),
            })

    apply_tasks = []
    if delta_required and fresh_existing:
        for idx, (start_index, end_index) in enumerate(_split_ranges(len(fresh_existing), apply_workers), start=1):
            progress_key = f"a{idx}of{apply_workers}"
            apply_tasks.append({
                "progressKey": progress_key,
                "startIndex": int(start_index),
                "endIndex": int(end_index),
                "addressFile": str(tracked_address_file),
                "eventsDir": str(delta_events_dir),
                "historyDir": str(history_dir),
                "outputDir": str(history_dir),
                "command": " ".join([
                "python3 apply_earn_subaccount_history_delta.py",
                    f"--chain {shlex.quote(chain)}",
                    f"--address-file {shlex.quote(str(tracked_address_file))}",
                    f"--start-index {start_index}",
                    f"--end-index {end_index}",
                    f"--progress-key {shlex.quote(progress_key)}",
                    f"--events-dir {shlex.quote(str(delta_events_dir))}",
                    f"--history-dir {shlex.quote(str(history_dir))}",
                    f"--output-dir {shlex.quote(str(history_dir))}",
                ]),
            })

    plan_payload = {
        "chain": chain,
        "baseTargetBlock": int(base_target),
        "targetBlock": int(target_block),
        "deltaRequired": bool(delta_required),
        "deltaFromBlock": int(delta_from_block),
        "trackedAddressCount": len(existing_addresses),
        "freshTrackedAddressCount": len(fresh_existing),
        "staleTrackedAddressCount": len(stale_existing),
        "newAddressCount": len(new_addresses),
        "orphanedHistoryCount": len(orphaned_histories),
        "cycleId": cycle_id,
        "cycleRoot": str(cycle_root),
        "deltaEventsDir": str(delta_events_dir),
        "trackedAddressFile": str(tracked_address_file),
        "scanTasks": scan_tasks,
        "newAddressTasks": new_address_tasks,
        "applyTasks": apply_tasks,
        "staleTrackedAddresses": stale_existing[:50],
        "newAddressesPreview": new_addresses[:50],
        "orphanedHistoriesPreview": orphaned_histories[:50],
    }
    plan_path = cycle_root / "incremental-plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps(plan_payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return plan_payload


def _print_human_plan(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Baseline target: {payload['baseTargetBlock']:,}")
    print(f"Next target: {payload['targetBlock']:,}")
    print(f"Delta required: {payload['deltaRequired']}")
    print(f"Tracked histories: {payload['trackedAddressCount']}")
    print(f"Fresh tracked histories: {payload['freshTrackedAddressCount']}")
    print(f"Stale tracked histories: {payload['staleTrackedAddressCount']}")
    print(f"New known addresses: {payload['newAddressCount']}")
    print(f"Orphaned histories: {payload['orphanedHistoryCount']}")
    print(f"Cycle root: {payload['cycleRoot']}")
    if payload["staleTrackedAddressCount"]:
        print("Stale tracked addresses detected; fix baseline freshness before relying on incremental apply.")
    print("Delta scan commands:")
    for task in payload["scanTasks"]:
        print(f"  {task['command']}")
    print("New-address backfill commands:")
    for task in payload["newAddressTasks"]:
        print(f"  {task['command']}")
    print("Delta apply commands:")
    for task in payload["applyTasks"]:
        print(f"  {task['command']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan a strict incremental EARN subaccount-history update")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--history-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--plan-dir", default=str(DEFAULT_PLAN_DIR))
    parser.add_argument("--max-scan-workers", type=int, default=None)
    parser.add_argument("--max-apply-workers", type=int, default=None)
    parser.add_argument("--max-new-backfill-workers", type=int, default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = build_incremental_plan(
        args.chain,
        events_dir=Path(args.events_dir),
        history_dir=Path(args.history_dir),
        plan_dir=Path(args.plan_dir),
        to_block=args.to_block,
        max_scan_workers=args.max_scan_workers,
        max_apply_workers=args.max_apply_workers,
        max_new_backfill_workers=args.max_new_backfill_workers,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        _print_human_plan(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
