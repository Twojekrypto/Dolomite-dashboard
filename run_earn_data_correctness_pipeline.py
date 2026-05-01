#!/usr/bin/env python3
"""
Run and monitor the canonical EARN data-correctness pipeline.

Pipeline:
  1. one-pass block-range scan -> event shards
  2. local per-address materialization from those shards

This runner exists so the process is resumable and operationally safe without
manual shell snippets.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from build_earn_subaccount_history import _load_known_addresses
from materialize_earn_subaccount_history import DEFAULT_EVENTS_DIR, DEFAULT_HISTORY_OUTPUT_DIR
from plan_earn_data_correctness import _run_one_pass_plan
from plan_earn_subaccount_history_repairs import DEFAULT_PLAN_DIR as DEFAULT_REPAIR_PLAN_DIR, build_repair_plan
from validate_earn_subaccount_history_events import build_validation_report


ROOT = Path(__file__).resolve().parent
SCAN_LOG_SUBDIR = ".logs"
PROGRESS_SUBDIR = ".progress"


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _read_json(path: Path, default):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _read_address_file(path: Optional[Path]) -> List[str]:
    if path is None:
        return []
    resolved = path if path.is_absolute() else ROOT / path
    if not resolved.exists():
        raise FileNotFoundError(f"Address selection file not found: {path}")
    addresses: List[str] = []
    seen = set()
    for raw in resolved.read_text(encoding="utf-8").splitlines():
        address = raw.strip().lower()
        if not address or address.startswith("#"):
            continue
        if not address.startswith("0x") or len(address) != 42:
            raise ValueError(f"Invalid address in {path}: {raw}")
        if address not in seen:
            seen.add(address)
            addresses.append(address)
    return addresses


def _is_pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _progress_dir(base_dir: Path) -> Path:
    return base_dir / PROGRESS_SUBDIR


def _log_dir(base_dir: Path) -> Path:
    return base_dir / SCAN_LOG_SUBDIR


def _plan_path(events_dir: Path, chain: str) -> Path:
    return _progress_dir(events_dir) / f"{chain}-one-pass-plan.json"


def _launch_path(base_dir: Path, chain: str, stage: str) -> Path:
    return _progress_dir(base_dir) / f"{chain}-{stage}-launch.json"


def _scan_progress_path(events_dir: Path, chain: str, progress_key: str) -> Path:
    return _progress_dir(events_dir) / f"{chain}--{progress_key}.json"


def _materialize_progress_path(history_dir: Path, chain: str, progress_key: str) -> Path:
    return _progress_dir(history_dir) / f"{chain}--{progress_key}.json"


def _load_launch(path: Path) -> dict:
    payload = _read_json(path, None)
    if isinstance(payload, dict):
        return payload
    return {"runs": []}


def _save_launch(path: Path, payload: dict) -> None:
    _write_json(path, payload)


def _scan_progress_payloads(events_dir: Path, chain: str) -> List[dict]:
    payloads: List[dict] = []
    for path in sorted(_progress_dir(events_dir).glob(f"{chain}--*.json")):
        payload = _read_json(path, None)
        if isinstance(payload, dict):
            payload["_path"] = str(path)
            payloads.append(payload)
    return payloads


def _materialize_progress_payloads(history_dir: Path, chain: str) -> List[dict]:
    payloads: List[dict] = []
    for path in sorted(_progress_dir(history_dir).glob(f"{chain}--*.json")):
        payload = _read_json(path, None)
        if isinstance(payload, dict):
            payload["_path"] = str(path)
            payloads.append(payload)
    return payloads


def _max_positive_int(values: Iterable[object]) -> Optional[int]:
    normalized: List[int] = []
    for value in values:
        try:
            number = int(value)
        except Exception:
            continue
        if number > 0:
            normalized.append(number)
    return max(normalized) if normalized else None


def _active_scan_target_block(events_dir: Path, chain: str) -> Optional[int]:
    payloads = _scan_progress_payloads(events_dir, chain)
    active = [
        payload
        for payload in payloads
        if str(payload.get("status") or "") in {"running", "completed"}
    ]
    return _max_positive_int(payload.get("toBlock") for payload in active)


def _active_materialize_target_block(history_dir: Path, chain: str) -> Optional[int]:
    payloads = _materialize_progress_payloads(history_dir, chain)
    active = [
        payload
        for payload in payloads
        if str(payload.get("status") or "") in {"running", "completed"}
    ]
    return _max_positive_int(payload.get("targetBlock") for payload in active)


def _discover_locked_target_block(events_dir: Path, history_dir: Path, chain: str) -> Optional[int]:
    scan_progress_target = _active_scan_target_block(events_dir, chain)
    if scan_progress_target is not None:
        return scan_progress_target

    materialize_progress_target = _active_materialize_target_block(history_dir, chain)
    if materialize_progress_target is not None:
        return materialize_progress_target

    candidates: List[int] = []

    existing_plan = _read_json(_plan_path(events_dir, chain), None)
    if isinstance(existing_plan, dict):
        try:
            value = int(existing_plan.get("targetBlock"))
            if value > 0:
                candidates.append(value)
        except Exception:
            pass

    for base_dir, stage in ((events_dir, "scan"), (history_dir, "materialize")):
        launch = _load_launch(_launch_path(base_dir, chain, stage))
        try:
            value = int(launch.get("targetBlock"))
            if value > 0:
                candidates.append(value)
        except Exception:
            pass

    unique = sorted(set(candidates))
    return max(unique) if unique else None


def _task_lists_match_target(plan: dict, target_block: Optional[int]) -> bool:
    if target_block is None:
        return True
    scan_tasks = plan.get("scanTasks") or []
    if not scan_tasks:
        return False
    try:
        last_task_to_block = max(int(task.get("toBlock") or 0) for task in scan_tasks)
    except Exception:
        return False
    return int(plan.get("targetBlock") or 0) == int(target_block) and last_task_to_block == int(target_block)


def _repair_launch_target(launch_path: Path, *, chain: str, target_block: int) -> Optional[dict]:
    if not launch_path.exists():
        return None
    launch = _load_launch(launch_path)
    if not launch:
        return None
    previous_target = launch.get("targetBlock")
    if int(previous_target or 0) == int(target_block):
        return None
    launch["chain"] = chain
    launch["targetBlock"] = int(target_block)
    launch["updatedAt"] = _utc_now_iso()
    _save_launch(launch_path, launch)
    return {
        "path": str(launch_path),
        "previousTargetBlock": previous_target,
        "targetBlock": int(target_block),
    }


def _repair_target_metadata(chain: str, *, events_dir: Path, history_dir: Path, target_block: int) -> dict:
    repairs: List[dict] = []
    for base_dir, stage in ((events_dir, "scan"), (history_dir, "materialize")):
        result = _repair_launch_target(
            _launch_path(base_dir, chain, stage),
            chain=chain,
            target_block=target_block,
        )
        if result is not None:
            repairs.append(result)
    return {
        "targetBlock": int(target_block),
        "updatedPaths": repairs,
    }


def _ensure_plan(
    chain: str,
    *,
    events_dir: Path,
    history_dir: Path,
    max_scan_workers: Optional[int],
    max_materialize_workers: Optional[int],
    selection_address_file: Optional[Path],
    refresh: bool,
) -> dict:
    path = _plan_path(events_dir, chain)
    locked_target_block = _discover_locked_target_block(events_dir, history_dir, chain)
    if path.exists() and not refresh:
        payload = _read_json(path, {})
        expected_selection = str(selection_address_file) if selection_address_file else None
        plan_selection = payload.get("selectionAddressFile")
        if (
            payload
            and payload.get("scanTasks")
            and payload.get("materializeTasks")
            and str(plan_selection or "") == str(expected_selection or "")
            and _task_lists_match_target(payload, locked_target_block)
        ):
            if locked_target_block is not None:
                _repair_target_metadata(chain, events_dir=events_dir, history_dir=history_dir, target_block=locked_target_block)
            return payload
    payload = _run_one_pass_plan(
        chain,
        events_dir=events_dir,
        history_output_dir=history_dir,
        to_block=locked_target_block,
        max_scan_workers=max_scan_workers,
        max_materialize_workers=max_materialize_workers,
        selection_address_file=selection_address_file,
    )
    _write_json(path, payload)
    if locked_target_block is not None:
        _repair_target_metadata(chain, events_dir=events_dir, history_dir=history_dir, target_block=locked_target_block)
    return payload


def _runs_by_key(launch_payload: dict) -> Dict[str, dict]:
    mapping = {}
    for run in launch_payload.get("runs") or []:
        key = str(run.get("progressKey") or "")
        if key:
            mapping[key] = run
    return mapping


def _start_task(argv: List[str], *, cwd: Path, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as handle:
        proc = subprocess.Popen(
            argv,
            cwd=str(cwd),
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return int(proc.pid)


def _scan_task_argv(task: dict) -> List[str]:
    argv = [
        "python3",
        "scan_earn_subaccount_history_events.py",
        "--chain",
        str(task["chain"]),
    ]
    address_file = task.get("addressFile")
    if address_file:
        argv.extend(["--address-file", str(address_file)])
    else:
        argv.append("--all-known-addresses")
    argv.extend([
        "--from-block",
        str(task["fromBlock"]),
        "--to-block",
        str(task["toBlock"]),
        "--progress-key",
        str(task["progressKey"]),
        "--output-dir",
        str(task["outputDir"]),
    ])
    return argv


def _materialize_task_argv(task: dict) -> List[str]:
    argv = [
        "python3",
        "materialize_earn_subaccount_history.py",
        "--chain",
        str(task["chain"]),
        "--progress-key",
        str(task["progressKey"]),
        "--events-dir",
        str(task["eventsDir"]),
        "--output-dir",
        str(task["outputDir"]),
    ]
    address_file = task.get("addressFile")
    if address_file:
        argv.extend([
            "--address-file",
            str(address_file),
        ])
    else:
        argv.extend([
            "--all-known-addresses",
        ])
    if task.get("startIndex") is not None:
        argv.extend(["--start-index", str(task["startIndex"])])
    if task.get("endIndex") is not None:
        argv.extend(["--end-index", str(task["endIndex"])])
    return argv


def _with_chain(task_list: Iterable[dict], chain: str) -> List[dict]:
    return [{**task, "chain": chain} for task in task_list]


def _ensure_scan_tasks_running(chain: str, *, plan: dict, events_dir: Path) -> dict:
    tasks = _with_chain(plan.get("scanTasks") or [], chain)
    launch_path = _launch_path(events_dir, chain, "scan")
    launch = _load_launch(launch_path)
    runs = _runs_by_key(launch)
    started = []
    skipped = []
    for task in tasks:
        key = str(task["progressKey"])
        progress = _read_json(_scan_progress_path(events_dir, chain, key), None)
        if isinstance(progress, dict) and str(progress.get("status") or "") == "completed":
            skipped.append({"progressKey": key, "reason": "completed"})
            continue
        existing = runs.get(key)
        if existing and _is_pid_alive(existing.get("pid")):
            skipped.append({"progressKey": key, "reason": "already_running", "pid": existing.get("pid")})
            continue
        log_path = _log_dir(events_dir) / f"{key}.log"
        pid = _start_task(_scan_task_argv(task), cwd=ROOT, log_path=log_path)
        run_payload = {
            "progressKey": key,
            "pid": pid,
            "logPath": str(log_path),
            "argv": _scan_task_argv(task),
            "startedAt": _utc_now_iso(),
        }
        runs[key] = run_payload
        started.append({"progressKey": key, "pid": pid})
    launch_payload = {
        "launchedAt": launch.get("launchedAt") or _utc_now_iso(),
        "updatedAt": _utc_now_iso(),
        "chain": chain,
        "targetBlock": plan.get("targetBlock"),
        "scanWorkers": plan.get("scanWorkers"),
        "materializeWorkers": plan.get("materializeWorkers"),
        "runs": list(runs.values()),
    }
    _save_launch(launch_path, launch_payload)
    return {
        "started": started,
        "skipped": skipped,
        "launchPath": str(launch_path),
    }


def _scan_stage_complete(chain: str, *, events_dir: Path) -> bool:
    report = build_validation_report(events_dir, chain)
    progress = report.get("progress") or {}
    return bool(
        report.get("ok")
        and report.get("plannedTargetBlock") is not None
        and report.get("manifestTargetBlock") == report.get("plannedTargetBlock")
        and progress.get("workerCount") == progress.get("completedWorkerCount")
    )


def _ensure_materialize_tasks_running(chain: str, *, plan: dict, history_dir: Path) -> dict:
    tasks = _with_chain(plan.get("materializeTasks") or [], chain)
    for task in tasks:
        task["targetBlock"] = plan.get("targetBlock")
    return _ensure_task_set_running(
        chain,
        tasks=tasks,
        history_dir=history_dir,
        stage="materialize",
    )


def _ensure_task_set_running(chain: str, *, tasks: List[dict], history_dir: Path, stage: str) -> dict:
    launch_path = _launch_path(history_dir, chain, stage)
    launch = _load_launch(launch_path)
    runs = _runs_by_key(launch)
    started = []
    skipped = []
    for task in tasks:
        key = str(task["progressKey"])
        progress = _read_json(_materialize_progress_path(history_dir, chain, key), None)
        if isinstance(progress, dict) and str(progress.get("status") or "") == "completed":
            skipped.append({"progressKey": key, "reason": "completed"})
            continue
        existing = runs.get(key)
        if existing and _is_pid_alive(existing.get("pid")):
            skipped.append({"progressKey": key, "reason": "already_running", "pid": existing.get("pid")})
            continue
        log_path = _log_dir(history_dir) / f"{key}.log"
        pid = _start_task(_materialize_task_argv(task), cwd=ROOT, log_path=log_path)
        run_payload = {
            "progressKey": key,
            "pid": pid,
            "logPath": str(log_path),
            "argv": _materialize_task_argv(task),
            "startedAt": _utc_now_iso(),
        }
        runs[key] = run_payload
        started.append({"progressKey": key, "pid": pid})
    launch_payload = {
        "launchedAt": launch.get("launchedAt") or _utc_now_iso(),
        "updatedAt": _utc_now_iso(),
        "chain": chain,
        "targetBlock": _max_positive_int([task.get("targetBlock") for task in tasks]) or launch.get("targetBlock"),
        "runs": list(runs.values()),
    }
    _save_launch(launch_path, launch_payload)
    return {
        "started": started,
        "skipped": skipped,
        "launchPath": str(launch_path),
    }


def _stage_status(base_dir: Path, chain: str, stage: str) -> dict:
    launch = _load_launch(_launch_path(base_dir, chain, stage))
    runs = []
    for run in launch.get("runs") or []:
        runs.append({
            "progressKey": run.get("progressKey"),
            "pid": run.get("pid"),
            "alive": _is_pid_alive(run.get("pid")),
            "logPath": run.get("logPath"),
            "startedAt": run.get("startedAt"),
        })
    return {
        "runCount": len(runs),
        "aliveCount": sum(1 for run in runs if run["alive"]),
        "runs": runs,
    }


def _progress_summary_for_prefix(history_dir: Path, chain: str, *, prefix: Optional[str]) -> dict:
    workers = []
    completed = 0
    for payload in _materialize_progress_payloads(history_dir, chain):
        progress_key = str(payload.get("progressKey") or "")
        if prefix is None:
            if progress_key.startswith("repair-"):
                continue
        elif not progress_key.startswith(prefix):
            continue
        status = str(payload.get("status") or "unknown")
        if status == "completed":
            completed += 1
        workers.append({
            "progressKey": progress_key,
            "status": status,
            "materializedAddressCount": payload.get("materializedAddressCount"),
            "pendingAddressCount": payload.get("pendingAddressCount"),
            "path": payload.get("_path"),
        })
    return {
        "workerCount": len(workers),
        "completedWorkerCount": completed,
        "workers": workers,
    }


def _materialize_progress_summary(history_dir: Path, chain: str) -> dict:
    return _progress_summary_for_prefix(history_dir, chain, prefix=None)


def _repair_progress_summary(history_dir: Path, chain: str) -> dict:
    return _progress_summary_for_prefix(history_dir, chain, prefix="repair-")


def _materialize_stage_complete(chain: str, *, history_dir: Path, plan: dict) -> bool:
    expected_workers = len(plan.get("materializeTasks") or [])
    progress = _materialize_progress_summary(history_dir, chain)
    if expected_workers == 0:
        return True
    return bool(
        progress.get("workerCount") == expected_workers
        and progress.get("completedWorkerCount") == expected_workers
    )


def _repair_stage_complete(chain: str, *, history_dir: Path, repair_plan: dict) -> bool:
    expected_workers = len(repair_plan.get("tasks") or [])
    progress = _repair_progress_summary(history_dir, chain)
    if expected_workers == 0:
        return True
    return bool(
        progress.get("workerCount") == expected_workers
        and progress.get("completedWorkerCount") == expected_workers
    )


def _selected_addresses_for_repair(
    chain: str,
    *,
    selection_address_file: Optional[Path],
    limit: Optional[int],
    start_index: int,
    end_index: Optional[int],
) -> List[str]:
    addresses = _read_address_file(selection_address_file) if selection_address_file else _load_known_addresses(chain)
    start = max(0, int(start_index))
    stop = len(addresses) if end_index is None else max(start, int(end_index))
    selected = addresses[start:stop]
    if limit is not None:
        selected = selected[: max(0, int(limit))]
    return selected


def _build_runner_repair_plan(
    chain: str,
    *,
    history_dir: Path,
    events_dir: Path,
    repair_plan_dir: Path,
    max_materialize_workers: int,
    target_block: Optional[int],
    selection_address_file: Optional[Path] = None,
    limit: Optional[int] = None,
    start_index: int = 0,
    end_index: Optional[int] = None,
) -> dict:
    selected_addresses = _selected_addresses_for_repair(
        chain,
        selection_address_file=selection_address_file,
        limit=limit,
        start_index=start_index,
        end_index=end_index,
    )
    return build_repair_plan(
        chain,
        history_dir=history_dir,
        events_dir=events_dir,
        output_dir=repair_plan_dir,
        addresses=selected_addresses,
        target_block=target_block,
        workers=max_materialize_workers,
    )


def build_pipeline_status(chain: str, *, events_dir: Path, history_dir: Path) -> dict:
    validation = build_validation_report(events_dir, chain)
    locked_target_block = _discover_locked_target_block(events_dir, history_dir, chain)
    return {
        "chain": chain,
        "lockedTargetBlock": locked_target_block,
        "scanValidation": validation,
        "scanLaunch": _stage_status(events_dir, chain, "scan"),
        "materializeLaunch": _stage_status(history_dir, chain, "materialize"),
        "materializeProgress": _materialize_progress_summary(history_dir, chain),
        "repairLaunch": _stage_status(history_dir, chain, "repair-materialize"),
        "repairProgress": _repair_progress_summary(history_dir, chain),
    }


def _print_status(payload: dict) -> None:
    scan = payload["scanValidation"]
    print(f"Chain: {payload['chain']}")
    if payload.get("lockedTargetBlock") is not None:
        print(f"Locked target block: {payload['lockedTargetBlock']}")
    print(f"Scan OK: {scan['ok']}")
    print(
        "Scan workers: "
        f"{scan['progress']['completedWorkerCount']}/{scan['progress']['workerCount']} completed, "
        f"{payload['scanLaunch']['aliveCount']}/{payload['scanLaunch']['runCount']} alive"
    )
    print(
        "Materialize workers: "
        f"{payload['materializeProgress']['completedWorkerCount']}/{payload['materializeProgress']['workerCount']} completed, "
        f"{payload['materializeLaunch']['aliveCount']}/{payload['materializeLaunch']['runCount']} alive"
    )
    print(
        "Repair workers: "
        f"{payload['repairProgress']['completedWorkerCount']}/{payload['repairProgress']['workerCount']} completed, "
        f"{payload['repairLaunch']['aliveCount']}/{payload['repairLaunch']['runCount']} alive"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run and monitor canonical EARN data-correctness pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--chain", default="arbitrum")
    common.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    common.add_argument("--history-dir", default=str(DEFAULT_HISTORY_OUTPUT_DIR))
    common.add_argument("--max-scan-workers", type=int, default=8)
    common.add_argument("--max-materialize-workers", type=int, default=8)
    common.add_argument("--repair-plan-dir", default=str(DEFAULT_REPAIR_PLAN_DIR))
    common.add_argument("--repair-limit", type=int, default=None)
    common.add_argument("--repair-start-index", type=int, default=0)
    common.add_argument("--repair-end-index", type=int, default=None)
    common.add_argument("--selection-address-file", default=None)

    start_scan = subparsers.add_parser("start-scan", parents=[common], help="Create/load plan and start missing scan workers")
    start_scan.add_argument("--refresh-plan", action="store_true")

    start_materialize = subparsers.add_parser("start-materialize", parents=[common], help="Start missing materialize workers if scan is complete")
    start_materialize.add_argument("--refresh-plan", action="store_true")

    cont = subparsers.add_parser("continue", parents=[common], help="Ensure scan runs; when complete, ensure materialization runs")
    cont.add_argument("--refresh-plan", action="store_true")

    status = subparsers.add_parser("status", parents=[common], help="Show current pipeline status")
    status.add_argument("--json", action="store_true")

    repair = subparsers.add_parser("repair", parents=[common], help="Repair target-block metadata from active progress")
    repair.add_argument("--refresh-plan", action="store_true")

    plan_repair = subparsers.add_parser("plan-repair", parents=[common], help="Build a targeted repair-materialization plan")
    plan_repair.add_argument("--target-block", type=int, default=None)
    plan_repair.add_argument("--limit", type=int, default=None)
    plan_repair.add_argument("--start-index", type=int, default=0)
    plan_repair.add_argument("--end-index", type=int, default=None)
    plan_repair.add_argument("--json", action="store_true")

    start_repair = subparsers.add_parser("start-repair", parents=[common], help="Build and start targeted repair-materialization workers")
    start_repair.add_argument("--target-block", type=int, default=None)
    start_repair.add_argument("--limit", type=int, default=None)
    start_repair.add_argument("--start-index", type=int, default=0)
    start_repair.add_argument("--end-index", type=int, default=None)

    args = parser.parse_args()
    events_dir = Path(args.events_dir)
    history_dir = Path(args.history_dir)
    repair_plan_dir = Path(args.repair_plan_dir)

    if args.command == "status":
        payload = build_pipeline_status(args.chain, events_dir=events_dir, history_dir=history_dir)
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            _print_status(payload)
        return 0

    if args.command == "plan-repair":
        payload = _build_runner_repair_plan(
            args.chain,
            history_dir=history_dir,
            events_dir=events_dir,
            repair_plan_dir=repair_plan_dir,
            max_materialize_workers=args.max_materialize_workers,
            target_block=args.target_block,
            selection_address_file=Path(args.selection_address_file) if args.selection_address_file else None,
            limit=args.limit,
            start_index=args.start_index,
            end_index=args.end_index,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    plan = _ensure_plan(
        args.chain,
        events_dir=events_dir,
        history_dir=history_dir,
        max_scan_workers=args.max_scan_workers,
        max_materialize_workers=args.max_materialize_workers,
        selection_address_file=Path(args.selection_address_file) if args.selection_address_file else None,
        refresh=bool(getattr(args, "refresh_plan", False)),
    )

    if args.command == "repair":
        target_block = _discover_locked_target_block(events_dir, history_dir, args.chain)
        if target_block is None:
            target_block = int(plan.get("targetBlock") or 0) or None
        if target_block is None:
            raise SystemExit("Could not resolve a locked target block to repair")
        payload = {
            "planPath": str(_plan_path(events_dir, args.chain)),
            "planTargetBlock": plan.get("targetBlock"),
            "lockedTargetBlock": target_block,
            "repairs": _repair_target_metadata(
                args.chain,
                events_dir=events_dir,
                history_dir=history_dir,
                target_block=int(target_block),
            ),
        }
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    if args.command == "start-scan":
        payload = _ensure_scan_tasks_running(args.chain, plan=plan, events_dir=events_dir)
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    if args.command == "start-materialize":
        if not _scan_stage_complete(args.chain, events_dir=events_dir):
            raise SystemExit("Scan stage is not complete yet")
        payload = _ensure_materialize_tasks_running(args.chain, plan=plan, history_dir=history_dir)
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    if args.command == "continue":
        scan_payload = _ensure_scan_tasks_running(args.chain, plan=plan, events_dir=events_dir)
        materialize_payload = None
        repair_plan = None
        repair_payload = None
        materialize_complete = False
        repair_complete = False
        if _scan_stage_complete(args.chain, events_dir=events_dir):
            materialize_payload = _ensure_materialize_tasks_running(args.chain, plan=plan, history_dir=history_dir)
            materialize_complete = _materialize_stage_complete(args.chain, history_dir=history_dir, plan=plan)
            if materialize_complete:
                repair_plan = _build_runner_repair_plan(
                    args.chain,
                    history_dir=history_dir,
                    events_dir=events_dir,
                    repair_plan_dir=repair_plan_dir,
                    max_materialize_workers=args.max_materialize_workers,
                    target_block=plan.get("targetBlock"),
                    selection_address_file=Path(args.selection_address_file) if args.selection_address_file else None,
                    limit=args.repair_limit,
                    start_index=args.repair_start_index,
                    end_index=args.repair_end_index,
                )
                repair_tasks = _with_chain(repair_plan.get("tasks") or [], args.chain)
                for task in repair_tasks:
                    task["eventsDir"] = str(events_dir)
                    task["outputDir"] = str(history_dir)
                    task["targetBlock"] = repair_plan.get("targetBlock")
                if repair_tasks:
                    repair_payload = _ensure_task_set_running(
                        args.chain,
                        tasks=repair_tasks,
                        history_dir=history_dir,
                        stage="repair-materialize",
                    )
                repair_complete = _repair_stage_complete(
                    args.chain,
                    history_dir=history_dir,
                    repair_plan=repair_plan,
                )
        print(json.dumps({
            "lockedTargetBlock": _discover_locked_target_block(events_dir, history_dir, args.chain),
            "scan": scan_payload,
            "scanComplete": _scan_stage_complete(args.chain, events_dir=events_dir),
            "materialize": materialize_payload,
            "materializeComplete": materialize_complete,
            "repairPlan": repair_plan,
            "repair": repair_payload,
            "repairComplete": repair_complete,
        }, ensure_ascii=True, indent=2))
        return 0

    if args.command == "start-repair":
        repair_plan = _build_runner_repair_plan(
            args.chain,
            history_dir=history_dir,
            events_dir=events_dir,
            repair_plan_dir=repair_plan_dir,
            max_materialize_workers=args.max_materialize_workers,
            target_block=args.target_block,
            selection_address_file=Path(args.selection_address_file) if args.selection_address_file else None,
            limit=args.limit,
            start_index=args.start_index,
            end_index=args.end_index,
        )
        repair_tasks = _with_chain(repair_plan.get("tasks") or [], args.chain)
        for task in repair_tasks:
            task["eventsDir"] = str(events_dir)
            task["outputDir"] = str(history_dir)
            task["targetBlock"] = repair_plan.get("targetBlock")
        payload = {
            "repairPlan": repair_plan,
            "repairLaunch": _ensure_task_set_running(
                args.chain,
                tasks=repair_tasks,
                history_dir=history_dir,
                stage="repair-materialize",
            ),
        }
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
