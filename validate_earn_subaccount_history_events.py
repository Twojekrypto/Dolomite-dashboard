#!/usr/bin/env python3
"""
Validate one-pass EARN subaccount-history event shards and their progress metadata.

The goal is strict operational confidence:
  - detect gaps or overlaps between scanned block ranges
  - verify that all shards share the same address selection hash
  - compare shard coverage against the saved one-pass plan
  - summarize worker progress without inventing success
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from build_earn_subaccount_history import _read_json
from materialize_earn_subaccount_history import _iter_shard_paths


ROOT = Path(__file__).resolve().parent
DEFAULT_EVENTS_DIR = ROOT / "data" / "earn-subaccount-history-events"


def _load_json(path: Path) -> dict:
    payload = _read_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _progress_dir(events_dir: Path) -> Path:
    return events_dir / ".progress"


def _load_plan(events_dir: Path, chain: str) -> dict:
    return _load_json(_progress_dir(events_dir) / f"{chain}-one-pass-plan.json")


def _load_launch(events_dir: Path, chain: str) -> dict:
    return _load_json(_progress_dir(events_dir) / f"{chain}-scan-launch.json")


def _load_progress_payloads(events_dir: Path, chain: str) -> List[dict]:
    payloads = []
    for path in sorted(_progress_dir(events_dir).glob(f"{chain}--*.json")):
        payload = _load_json(path)
        if payload:
            payload["_path"] = str(path)
            payloads.append(payload)
    return payloads


def _load_chain_manifest(events_dir: Path, chain: str) -> dict:
    manifest = _load_json(events_dir / "manifest.json")
    chains = manifest.get("chains") or {}
    payload = chains.get(chain) or {}
    return payload if isinstance(payload, dict) else {}


def _expected_ranges_from_plan(plan: dict) -> List[Tuple[int, int, Optional[str]]]:
    task_ranges = []
    for task in plan.get("scanTasks") or []:
        if not isinstance(task, dict):
            continue
        try:
            task_ranges.append((
                int(task.get("fromBlock")),
                int(task.get("toBlock")),
                task.get("progressKey"),
            ))
        except Exception:
            continue
    if task_ranges:
        return sorted(task_ranges, key=lambda item: (item[0], item[1], str(item[2] or "")))

    ranges = []
    for command in plan.get("scanCommands") or []:
        if not isinstance(command, str):
            continue
        try:
            from_block = int(command.split("--from-block ", 1)[1].split()[0])
            to_block = int(command.split("--to-block ", 1)[1].split()[0])
            progress_key = command.split("--progress-key ", 1)[1].split()[0]
            ranges.append((from_block, to_block, progress_key))
        except Exception:
            continue
    return sorted(ranges, key=lambda item: (item[0], item[1], str(item[2] or "")))


def _actual_ranges_from_manifest(chain_manifest: dict) -> List[Tuple[int, int, Optional[str], Optional[str]]]:
    ranges = []
    for entry in chain_manifest.get("scanRanges") or []:
        if not isinstance(entry, dict):
            continue
        try:
            ranges.append((
                int(entry.get("fromBlock")),
                int(entry.get("toBlock")),
                entry.get("progressKey"),
                entry.get("selectionHash"),
            ))
        except Exception:
            continue
    return sorted(ranges, key=lambda item: (item[0], item[1], str(item[2] or "")))


def _ranges_from_progress(progress_payloads: List[dict]) -> List[Tuple[int, int, Optional[str], Optional[str]]]:
    ranges = []
    for payload in progress_payloads:
        if str(payload.get("status") or "") not in {"running", "completed"}:
            continue
        try:
            ranges.append((
                int(payload.get("fromBlock")),
                int(payload.get("toBlock")),
                payload.get("progressKey"),
                payload.get("selectionHash"),
            ))
        except Exception:
            continue
    return sorted(ranges, key=lambda item: (item[0], item[1], str(item[2] or "")))


def _check_contiguity(ranges: Iterable[Tuple[int, int]]) -> dict:
    ordered = sorted(ranges, key=lambda item: (item[0], item[1]))
    gaps = []
    overlaps = []
    previous_end = None
    for start, end in ordered:
        if previous_end is None:
            previous_end = end
            continue
        if start > previous_end + 1:
            gaps.append({"fromBlock": previous_end + 1, "toBlock": start - 1})
        elif start <= previous_end:
            overlaps.append({"fromBlock": start, "toBlock": min(previous_end, end)})
        previous_end = max(previous_end, end)
    return {
        "gapCount": len(gaps),
        "overlapCount": len(overlaps),
        "gaps": gaps,
        "overlaps": overlaps,
    }


def _check_selection_hashes(actual_ranges: Iterable[Tuple[int, int, Optional[str], Optional[str]]]) -> dict:
    hashes = sorted({str(selection_hash) for _, _, _, selection_hash in actual_ranges if selection_hash})
    return {
        "selectionHashCount": len(hashes),
        "selectionHashes": hashes,
        "consistent": len(hashes) <= 1,
    }


def _compare_expected_vs_actual(expected, actual) -> dict:
    expected_keys = {(start, end, progress_key) for start, end, progress_key in expected}
    actual_keys = {(start, end, progress_key) for start, end, progress_key, _ in actual}
    missing = sorted(expected_keys - actual_keys)
    unexpected = sorted(actual_keys - expected_keys)
    return {
        "expectedRangeCount": len(expected_keys),
        "actualRangeCount": len(actual_keys),
        "missingRanges": [
            {"fromBlock": start, "toBlock": end, "progressKey": progress_key}
            for start, end, progress_key in missing
        ],
        "unexpectedRanges": [
            {"fromBlock": start, "toBlock": end, "progressKey": progress_key}
            for start, end, progress_key in unexpected
        ],
    }


def _progress_summary(progress_payloads: List[dict]) -> dict:
    workers = []
    completed = 0
    for payload in progress_payloads:
        status = str(payload.get("status") or "unknown")
        if status == "completed":
            completed += 1
        workers.append({
            "progressKey": payload.get("progressKey"),
            "status": status,
            "lastBlockExclusive": payload.get("lastBlockExclusive"),
            "totalLogCount": payload.get("totalLogCount"),
            "totalSelectedEventCount": payload.get("totalSelectedEventCount"),
            "path": payload.get("_path"),
        })
    return {
        "workerCount": len(workers),
        "completedWorkerCount": completed,
        "workers": workers,
    }


def build_validation_report(events_dir: Path, chain: str) -> dict:
    plan = _load_plan(events_dir, chain)
    launch = _load_launch(events_dir, chain)
    progress_payloads = _load_progress_payloads(events_dir, chain)
    chain_manifest = _load_chain_manifest(events_dir, chain)
    expected_ranges = _expected_ranges_from_plan(plan)
    actual_ranges = _actual_ranges_from_manifest(chain_manifest)
    progress_ranges = _ranges_from_progress(progress_payloads)
    shard_ranges = [(start, end) for start, end, _ in _iter_shard_paths(events_dir, chain)]

    range_compare = _compare_expected_vs_actual(expected_ranges, actual_ranges)
    contiguity = _check_contiguity([(start, end) for start, end, _, _ in actual_ranges])
    selection_hashes = _check_selection_hashes(actual_ranges or progress_ranges)
    progress = _progress_summary(progress_payloads)
    shard_contiguity = _check_contiguity(shard_ranges)
    progress_compare = _compare_expected_vs_actual(
        expected_ranges,
        progress_ranges,
    )
    live_progress_target_block = max(
        (end for _, end, _, _ in progress_ranges),
        default=None,
    )

    try:
        planned_target_block = int(plan.get("targetBlock"))
    except Exception:
        planned_target_block = None
    try:
        manifest_target_block = int(chain_manifest.get("globalToBlock"))
    except Exception:
        manifest_target_block = None

    scan_ranges_complete = (
        not range_compare["missingRanges"]
        and not range_compare["unexpectedRanges"]
        and contiguity["gapCount"] == 0
        and contiguity["overlapCount"] == 0
        and selection_hashes["consistent"]
    )
    ok = bool(scan_ranges_complete)

    return {
        "chain": chain,
        "ok": ok,
        "scanRangesComplete": scan_ranges_complete,
        "plannedTargetBlock": planned_target_block,
        "manifestTargetBlock": manifest_target_block,
        "plan": {
            "scanWorkers": plan.get("scanWorkers"),
            "materializeWorkers": plan.get("materializeWorkers"),
            "selectedAddressCount": plan.get("selectedAddressCount"),
        },
        "liveProgressTargetBlock": live_progress_target_block,
        "launch": {
            "launchedAt": launch.get("launchedAt"),
            "runCount": len(launch.get("runs") or []),
        },
        "rangeCompare": range_compare,
        "progressRangeCompare": progress_compare,
        "manifestContiguity": contiguity,
        "shardContiguity": shard_contiguity,
        "shardFilesSparseByDesign": True,
        "selectionHashes": selection_hashes,
        "progress": progress,
    }


def _print_human_report(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"OK: {payload['ok']}")
    print(f"Scan ranges complete: {payload.get('scanRangesComplete')}")
    print(f"Planned target block: {payload.get('plannedTargetBlock')}")
    print(f"Manifest target block: {payload.get('manifestTargetBlock')}")
    print(f"Live progress target block: {payload.get('liveProgressTargetBlock')}")
    print(
        "Workers: "
        f"{payload['progress']['completedWorkerCount']}/{payload['progress']['workerCount']} completed"
    )
    print(
        "Ranges: "
        f"missing={len(payload['rangeCompare']['missingRanges'])} "
        f"unexpected={len(payload['rangeCompare']['unexpectedRanges'])} "
        f"gaps={payload['manifestContiguity']['gapCount']} "
        f"overlaps={payload['manifestContiguity']['overlapCount']}"
    )
    print(
        "Live progress ranges: "
        f"missing={len(payload['progressRangeCompare']['missingRanges'])} "
        f"unexpected={len(payload['progressRangeCompare']['unexpectedRanges'])}"
    )
    print(
        "Shard files: "
        f"gaps={payload['shardContiguity']['gapCount']} "
        f"overlaps={payload['shardContiguity']['overlapCount']}"
    )
    if payload.get("shardFilesSparseByDesign"):
        print("Shard file gaps are informational only: empty block chunks do not create shard files.")
    print(
        "Selection hash consistency: "
        f"{payload['selectionHashes']['consistent']} "
        f"({payload['selectionHashes']['selectionHashCount']} unique)"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate EARN subaccount-history event shards")
    parser.add_argument("--chain", default="arbitrum")
    parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = build_validation_report(Path(args.events_dir), args.chain)
    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        _print_human_report(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
