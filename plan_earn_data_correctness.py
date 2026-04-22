#!/usr/bin/env python3
"""
Plan and validate canonical EARN subaccount-history coverage.

This script is intentionally operational:
  - `status` shows how far the canonical source-of-truth backfill has progressed
  - `benchmark` measures sample throughput and stores it for later planning
  - `plan` turns current coverage + benchmark data into shard-safe commands
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shlex
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from scan_earn_netflow import CHAINS, get_block_number

from backfill_earn_subaccount_history import _progress_path
from build_earn_subaccount_history import DEFAULT_OUTPUT_DIR, _load_known_addresses, _read_json, build_history_for_addresses_in_block_range
from materialize_earn_subaccount_history import DEFAULT_EVENTS_DIR


BENCHMARK_DIRNAME = "benchmarks"


def _history_path(output_dir: Path, chain: str, address: str) -> Path:
    return output_dir / chain / f"{address.lower()}.json"


def _latest_history_block(path: Path) -> int:
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        return 0
    try:
        return int(payload.get("lastScannedBlock") or 0)
    except Exception:
        return 0


def _resolve_target_block(chain: str, to_block: Optional[int]) -> int:
    if to_block is not None:
        return int(to_block)
    config = CHAINS[chain]
    return int(get_block_number(config["rpcs"], [0]))


def _benchmark_path(output_dir: Path, chain: str) -> Path:
    return output_dir / ".progress" / BENCHMARK_DIRNAME / f"{chain}.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _read_progress_files(output_dir: Path, chain: str) -> List[dict]:
    progress_dir = output_dir / ".progress"
    if not progress_dir.exists():
        return []
    payloads: List[dict] = []
    for path in sorted(progress_dir.glob(f"{chain}*.json")):
        payload = _read_json(path, None)
        if isinstance(payload, dict):
            payload["_path"] = str(path)
            payloads.append(payload)
    return payloads


def _build_status_payload(
    chain: str,
    *,
    output_dir: Path,
    to_block: Optional[int],
) -> dict:
    selected_addresses = _load_known_addresses(chain)
    target_block = _resolve_target_block(chain, to_block)
    fresh_count = 0
    partial_count = 0
    missing_count = 0
    existing_blocks: List[int] = []

    for address in selected_addresses:
        path = _history_path(output_dir, chain, address)
        if not path.exists():
            missing_count += 1
            continue
        last_block = _latest_history_block(path)
        if last_block >= target_block:
            fresh_count += 1
        else:
            partial_count += 1
        if last_block > 0:
            existing_blocks.append(last_block)

    progress_payloads = _read_progress_files(output_dir, chain)
    benchmark_payload = _read_json(_benchmark_path(output_dir, chain), None)
    return {
        "chain": chain,
        "targetBlock": target_block,
        "selectedAddressCount": len(selected_addresses),
        "freshAddressCount": fresh_count,
        "partialAddressCount": partial_count,
        "missingAddressCount": missing_count,
        "freshCoverageRatio": (fresh_count / len(selected_addresses)) if selected_addresses else 0.0,
        "minExistingLastBlock": min(existing_blocks) if existing_blocks else None,
        "maxExistingLastBlock": max(existing_blocks) if existing_blocks else None,
        "progressFiles": progress_payloads,
        "benchmark": benchmark_payload if isinstance(benchmark_payload, dict) else None,
    }


def _print_human_status(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Target block: {payload['targetBlock']:,}")
    print(f"Selected addresses: {payload['selectedAddressCount']}")
    print(f"Fresh coverage: {payload['freshAddressCount']}")
    print(f"Partial coverage: {payload['partialAddressCount']}")
    print(f"Missing coverage: {payload['missingAddressCount']}")
    print(f"Fresh ratio: {payload['freshCoverageRatio']:.2%}")
    if payload.get("minExistingLastBlock") is not None:
        print(
            "Existing lastScannedBlock range: "
            f"{payload['minExistingLastBlock']:,} - {payload['maxExistingLastBlock']:,}"
        )
    if payload.get("progressFiles"):
        print("Progress files:")
        for entry in payload["progressFiles"]:
            print(
                f"  - {entry.get('_path')}: "
                f"{entry.get('completedBatchCount', 0)}/{entry.get('totalBatchCount', 0)} batches, "
                f"status={entry.get('status', 'unknown')}, progressKey={entry.get('progressKey')}"
            )
    if payload.get("benchmark"):
        bench = payload["benchmark"]
        print(
            "Saved benchmark: "
            f"{bench.get('secondsPerAddress', 0):.2f}s/address "
            f"({bench.get('addressesPerHour', 0):.1f} addr/hour)"
        )


def _run_benchmark(
    chain: str,
    *,
    output_dir: Path,
    sample_size: int,
    from_block: Optional[int],
    to_block: Optional[int],
    start_index: int,
) -> dict:
    selected_addresses = _load_known_addresses(chain)
    addresses = selected_addresses[max(0, int(start_index)): max(0, int(start_index)) + max(1, int(sample_size))]
    if not addresses:
        raise SystemExit("No addresses selected for benchmark")

    resolved_to_block = _resolve_target_block(chain, to_block)
    resolved_from_block = CHAINS[chain]["start_block"] if from_block is None else int(from_block)

    started = time.perf_counter()
    histories = build_history_for_addresses_in_block_range(
        chain,
        addresses,
        from_block=resolved_from_block,
        to_block=resolved_to_block,
    )
    elapsed = time.perf_counter() - started
    address_count = len(addresses)
    seconds_per_address = elapsed / address_count if address_count else 0.0
    addresses_per_hour = (3600.0 / seconds_per_address) if seconds_per_address > 0 else 0.0
    event_count = sum(int((history.get("summary") or {}).get("eventCount") or 0) for history in histories.values())

    payload = {
        "chain": chain,
        "measuredAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sampleSize": address_count,
        "fromBlock": resolved_from_block,
        "toBlock": resolved_to_block,
        "elapsedSeconds": elapsed,
        "secondsPerAddress": seconds_per_address,
        "addressesPerHour": addresses_per_hour,
        "eventCount": event_count,
    }
    _write_json(_benchmark_path(output_dir, chain), payload)
    return payload


def _worker_counts(max_workers: Optional[int]) -> List[int]:
    cpu = max(1, os.cpu_count() or 1)
    cap = min(cpu, max_workers) if max_workers else cpu
    candidates = [1, 2, 3, 4, 6, 8, 10, 12]
    unique = sorted({count for count in candidates if count <= cap})
    if cap not in unique:
        unique.append(cap)
    return sorted(set(unique))


def _estimate_hours(address_count: int, seconds_per_address: float, workers: int) -> float:
    if address_count <= 0 or seconds_per_address <= 0 or workers <= 0:
        return 0.0
    return (address_count * seconds_per_address) / (workers * 3600.0)


def _build_shard_commands(
    chain: str,
    *,
    worker_count: int,
    total_addresses: int,
    target_block: int,
    batch_size: int,
    output_dir: Path,
) -> List[str]:
    commands = []
    shard_size = math.ceil(total_addresses / worker_count)
    for shard_idx in range(worker_count):
        start_index = shard_idx * shard_size
        end_index = min(total_addresses, start_index + shard_size)
        if start_index >= end_index:
            continue
        progress_key = f"w{shard_idx + 1}of{worker_count}"
        cmd = (
            f"python3 backfill_earn_subaccount_history.py "
            f"--chain {shlex.quote(chain)} "
            f"--batch-size {batch_size} "
            f"--to-block {target_block} "
            f"--start-index {start_index} "
            f"--end-index {end_index} "
            f"--progress-key {shlex.quote(progress_key)} "
            f"--output-dir {shlex.quote(str(output_dir))}"
        )
        commands.append(cmd)
    return commands


def _run_plan(
    chain: str,
    *,
    output_dir: Path,
    to_block: Optional[int],
    batch_size: int,
    desired_hours: Optional[float],
    max_workers: Optional[int],
    seconds_per_address: Optional[float],
) -> dict:
    status_payload = _build_status_payload(chain, output_dir=output_dir, to_block=to_block)
    benchmark_payload = status_payload.get("benchmark") or {}
    measured_seconds_per_address = (
        float(seconds_per_address)
        if seconds_per_address is not None
        else float(benchmark_payload.get("secondsPerAddress") or 0.0)
    )
    pending_addresses = int(status_payload["selectedAddressCount"]) - int(status_payload["freshAddressCount"])
    worker_options = []
    for workers in _worker_counts(max_workers):
        worker_options.append({
            "workers": workers,
            "estimatedHours": _estimate_hours(pending_addresses, measured_seconds_per_address, workers),
        })

    recommended_workers = None
    if worker_options:
        if measured_seconds_per_address > 0:
            if desired_hours is not None:
                for option in worker_options:
                    if option["estimatedHours"] <= desired_hours:
                        recommended_workers = option["workers"]
                        break
            if recommended_workers is None:
                recommended_workers = worker_options[-1]["workers"]
        else:
            recommended_workers = worker_options[-1]["workers"]

    shard_commands = _build_shard_commands(
        chain,
        worker_count=recommended_workers or 1,
        total_addresses=int(status_payload["selectedAddressCount"]),
        target_block=int(status_payload["targetBlock"]),
        batch_size=batch_size,
        output_dir=output_dir,
    )

    return {
        "status": status_payload,
        "secondsPerAddress": measured_seconds_per_address,
        "workerOptions": worker_options,
        "recommendedWorkers": recommended_workers,
        "recommendedCommands": shard_commands,
        "desiredHours": desired_hours,
    }


def _print_human_plan(payload: dict) -> None:
    status_payload = payload["status"]
    pending_addresses = status_payload["selectedAddressCount"] - status_payload["freshAddressCount"]
    print(f"Chain: {status_payload['chain']}")
    print(f"Target block: {status_payload['targetBlock']:,}")
    print(f"Selected addresses: {status_payload['selectedAddressCount']}")
    print(f"Pending addresses: {pending_addresses}")
    if payload["secondsPerAddress"] > 0:
        print(f"Measured throughput: {payload['secondsPerAddress']:.2f}s/address")
        print("Estimated runtime:")
        for option in payload["workerOptions"]:
            print(f"  - {option['workers']} worker(s): {option['estimatedHours']:.2f}h")
    else:
        print("Measured throughput: unavailable (run benchmark first or pass --seconds-per-address)")
    print(f"Recommended workers: {payload['recommendedWorkers']}")
    print("Recommended shard commands:")
    for cmd in payload["recommendedCommands"]:
        print(f"  {cmd}")


def _split_block_ranges(start_block: int, end_block: int, worker_count: int) -> List[tuple[int, int]]:
    total_blocks = max(0, end_block - start_block + 1)
    if total_blocks <= 0 or worker_count <= 0:
        return []
    shard_size = math.ceil(total_blocks / worker_count)
    ranges = []
    current = start_block
    for _ in range(worker_count):
        shard_end = min(end_block, current + shard_size - 1)
        if current > shard_end:
            break
        ranges.append((current, shard_end))
        current = shard_end + 1
    return ranges


def _build_one_pass_scan_tasks(
    chain: str,
    *,
    start_block: int,
    target_block: int,
    worker_count: int,
    events_dir: Path,
) -> List[dict]:
    tasks = []
    for idx, (block_start, block_end) in enumerate(_split_block_ranges(start_block, target_block, worker_count), start=1):
        progress_key = f"s{idx}of{worker_count}"
        tasks.append({
            "fromBlock": int(block_start),
            "toBlock": int(block_end),
            "progressKey": progress_key,
            "outputDir": str(events_dir),
            "command": " ".join([
                "python3 scan_earn_subaccount_history_events.py",
                f"--chain {shlex.quote(chain)}",
                "--all-known-addresses",
                f"--from-block {block_start}",
                f"--to-block {block_end}",
                f"--progress-key {shlex.quote(progress_key)}",
                f"--output-dir {shlex.quote(str(events_dir))}",
            ]),
        })
    return tasks


def _build_one_pass_materialize_tasks(
    chain: str,
    *,
    total_addresses: int,
    worker_count: int,
    events_dir: Path,
    history_output_dir: Path,
) -> List[dict]:
    tasks = []
    shard_size = math.ceil(total_addresses / max(1, worker_count))
    for idx in range(worker_count):
        start_index = idx * shard_size
        end_index = min(total_addresses, start_index + shard_size)
        if start_index >= end_index:
            continue
        progress_key = f"m{idx + 1}of{worker_count}"
        tasks.append({
            "startIndex": int(start_index),
            "endIndex": int(end_index),
            "progressKey": progress_key,
            "eventsDir": str(events_dir),
            "outputDir": str(history_output_dir),
            "command": " ".join([
                "python3 materialize_earn_subaccount_history.py",
                f"--chain {shlex.quote(chain)}",
                "--all-known-addresses",
                f"--start-index {start_index}",
                f"--end-index {end_index}",
                f"--progress-key {shlex.quote(progress_key)}",
                f"--events-dir {shlex.quote(str(events_dir))}",
                f"--output-dir {shlex.quote(str(history_output_dir))}",
            ]),
        })
    return tasks


def _run_one_pass_plan(
    chain: str,
    *,
    events_dir: Path,
    history_output_dir: Path,
    to_block: Optional[int],
    max_scan_workers: Optional[int],
    max_materialize_workers: Optional[int],
) -> dict:
    target_block = _resolve_target_block(chain, to_block)
    start_block = int(CHAINS[chain]["start_block"])
    selected_addresses = _load_known_addresses(chain)
    scan_workers = _worker_counts(max_scan_workers)[-1]
    materialize_workers = _worker_counts(max_materialize_workers)[-1]
    scan_tasks = _build_one_pass_scan_tasks(
        chain,
        start_block=start_block,
        target_block=target_block,
        worker_count=scan_workers,
        events_dir=events_dir,
    )
    materialize_tasks = _build_one_pass_materialize_tasks(
        chain,
        total_addresses=len(selected_addresses),
        worker_count=materialize_workers,
        events_dir=events_dir,
        history_output_dir=history_output_dir,
    )
    return {
        "chain": chain,
        "targetBlock": target_block,
        "startBlock": start_block,
        "selectedAddressCount": len(selected_addresses),
        "scanWorkers": scan_workers,
        "materializeWorkers": materialize_workers,
        "scanTasks": scan_tasks,
        "scanCommands": [task["command"] for task in scan_tasks],
        "materializeTasks": materialize_tasks,
        "materializeCommands": [task["command"] for task in materialize_tasks],
    }


def _print_human_one_pass_plan(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Start block: {payload['startBlock']:,}")
    print(f"Target block: {payload['targetBlock']:,}")
    print(f"Selected addresses: {payload['selectedAddressCount']}")
    print(f"Scan workers: {payload['scanWorkers']}")
    print(f"Materialize workers: {payload['materializeWorkers']}")
    print("One-pass scan commands:")
    for cmd in payload["scanCommands"]:
        print(f"  {cmd}")
    print("Materialize commands:")
    for cmd in payload["materializeCommands"]:
        print(f"  {cmd}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan and validate canonical EARN data correctness work")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="Show canonical coverage status")
    status_parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    status_parser.add_argument("--to-block", type=int, default=None)
    status_parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    status_parser.add_argument("--json", action="store_true")

    benchmark_parser = subparsers.add_parser("benchmark", help="Benchmark sample throughput")
    benchmark_parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    benchmark_parser.add_argument("--sample-size", type=int, default=5)
    benchmark_parser.add_argument("--start-index", type=int, default=0)
    benchmark_parser.add_argument("--from-block", type=int, default=None)
    benchmark_parser.add_argument("--to-block", type=int, default=None)
    benchmark_parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    benchmark_parser.add_argument("--json", action="store_true")

    plan_parser = subparsers.add_parser("plan", help="Build shard-safe backfill plan")
    plan_parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    plan_parser.add_argument("--to-block", type=int, default=None)
    plan_parser.add_argument("--batch-size", type=int, default=250)
    plan_parser.add_argument("--desired-hours", type=float, default=None)
    plan_parser.add_argument("--max-workers", type=int, default=None)
    plan_parser.add_argument("--seconds-per-address", type=float, default=None)
    plan_parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    plan_parser.add_argument("--json", action="store_true")

    one_pass_parser = subparsers.add_parser("one-pass-plan", help="Build fast one-pass scan + materialize plan")
    one_pass_parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    one_pass_parser.add_argument("--to-block", type=int, default=None)
    one_pass_parser.add_argument("--max-scan-workers", type=int, default=None)
    one_pass_parser.add_argument("--max-materialize-workers", type=int, default=None)
    one_pass_parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    one_pass_parser.add_argument("--history-output-dir", default=str(DEFAULT_OUTPUT_DIR))
    one_pass_parser.add_argument("--json", action="store_true")

    args = parser.parse_args()
    output_dir = Path(getattr(args, "output_dir", DEFAULT_OUTPUT_DIR))

    if args.command == "status":
        payload = _build_status_payload(args.chain, output_dir=output_dir, to_block=args.to_block)
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            _print_human_status(payload)
        return 0

    if args.command == "benchmark":
        payload = _run_benchmark(
            args.chain,
            output_dir=output_dir,
            sample_size=max(1, int(args.sample_size)),
            from_block=args.from_block,
            to_block=args.to_block,
            start_index=max(0, int(args.start_index)),
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    if args.command == "plan":
        payload = _run_plan(
            args.chain,
            output_dir=output_dir,
            to_block=args.to_block,
            batch_size=max(1, int(args.batch_size)),
            desired_hours=args.desired_hours,
            max_workers=args.max_workers,
            seconds_per_address=args.seconds_per_address,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            _print_human_plan(payload)
        return 0

    if args.command == "one-pass-plan":
        payload = _run_one_pass_plan(
            args.chain,
            events_dir=Path(args.events_dir),
            history_output_dir=Path(args.history_output_dir),
            to_block=args.to_block,
            max_scan_workers=args.max_scan_workers,
            max_materialize_workers=args.max_materialize_workers,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            _print_human_one_pass_plan(payload)
        return 0

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
