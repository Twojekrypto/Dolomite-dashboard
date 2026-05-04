#!/usr/bin/env python3
"""
Run and monitor strict incremental EARN subaccount-history update cycles.

Cycle:
  1. delta scan for already-tracked addresses
  2. full backfill only for newly-discovered addresses
  3. apply the delta onto all tracked histories

This runner keeps the process resumable and operationally safe without
reconstructing shell snippets by hand every time a new incremental cycle is
needed.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from build_earn_subaccount_history import _load_known_addresses, _read_json
from plan_earn_subaccount_history_incremental import (
    DEFAULT_PLAN_DIR,
    build_incremental_plan,
)
from report_earn_subaccount_history_coverage import build_coverage_report


ROOT = Path(__file__).resolve().parent
DEFAULT_EVENTS_DIR = ROOT / "data" / "earn-subaccount-history-events"
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
STATE_FILENAME_TEMPLATE = "{chain}-runner-state.json"
PROGRESS_SUBDIR = ".progress"
LOG_SUBDIR = ".logs"


def _runner_session_id() -> str:
    explicit = os.environ.get("EARN_RUNNER_SESSION_ID")
    if explicit:
        return explicit
    github_run_id = os.environ.get("GITHUB_RUN_ID")
    if github_run_id:
        attempt = os.environ.get("GITHUB_RUN_ATTEMPT") or "1"
        return f"github-{github_run_id}-{attempt}"
    return "local"


RUNNER_SESSION_ID = _runner_session_id()


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _is_pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _launch_run_belongs_to_session(run: dict) -> bool:
    session_id = str(run.get("runnerSessionId") or "")
    if session_id:
        return session_id == RUNNER_SESSION_ID
    return not os.environ.get("GITHUB_ACTIONS")


def _is_launch_run_alive(run: object) -> bool:
    if not isinstance(run, dict) or not _launch_run_belongs_to_session(run):
        return False
    return _is_pid_alive(run.get("pid"))


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


def _state_path(plan_dir: Path, chain: str) -> Path:
    return plan_dir / STATE_FILENAME_TEMPLATE.format(chain=chain)


def _load_runner_state(plan_dir: Path, chain: str) -> dict:
    payload = _read_json(_state_path(plan_dir, chain), None)
    if isinstance(payload, dict):
        return payload
    return {}


def _save_runner_state(plan_dir: Path, chain: str, payload: dict) -> None:
    _write_json(_state_path(plan_dir, chain), payload)


def _plan_path_from_state(plan_dir: Path, chain: str) -> Optional[Path]:
    state = _load_runner_state(plan_dir, chain)
    raw = state.get("planPath")
    if not raw:
        return None
    path = Path(str(raw))
    return path if path.exists() else None


def _latest_plan_path(plan_dir: Path, chain: str) -> Optional[Path]:
    candidates = sorted(
        plan_dir.glob(f"{chain}-f*-t*/incremental-plan.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_plan(path: Path) -> dict:
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid plan payload: {path}")
    return payload


def _persist_plan_state(plan_dir: Path, chain: str, plan: dict) -> None:
    _save_runner_state(plan_dir, chain, {
        "chain": chain,
        "cycleId": plan.get("cycleId"),
        "cycleRoot": plan.get("cycleRoot"),
        "planPath": str(Path(plan["cycleRoot"]) / "incremental-plan.json"),
        "targetBlock": plan.get("targetBlock"),
        "updatedAt": _utc_now_iso(),
    })


def _resolve_current_plan(
    chain: str,
    *,
    events_dir: Path,
    history_dir: Path,
    plan_dir: Path,
    to_block: Optional[int],
    max_scan_workers: Optional[int],
    max_apply_workers: Optional[int],
    max_new_backfill_workers: Optional[int],
    selection_address_file: Optional[Path],
    refresh: bool,
) -> dict:
    if not refresh:
        existing = _plan_path_from_state(plan_dir, chain) or _latest_plan_path(plan_dir, chain)
        if existing is not None:
            payload = _load_plan(existing)
            _persist_plan_state(plan_dir, chain, payload)
            return payload

    payload = build_incremental_plan(
        chain,
        events_dir=events_dir,
        history_dir=history_dir,
        plan_dir=plan_dir,
        to_block=to_block,
        max_scan_workers=max_scan_workers,
        max_apply_workers=max_apply_workers,
        max_new_backfill_workers=max_new_backfill_workers,
        selection_address_file=selection_address_file,
    )
    _persist_plan_state(plan_dir, chain, payload)
    return payload


def _build_fresh_plan(
    chain: str,
    *,
    events_dir: Path,
    history_dir: Path,
    plan_dir: Path,
    to_block: Optional[int],
    max_scan_workers: Optional[int],
    max_apply_workers: Optional[int],
    max_new_backfill_workers: Optional[int],
    selection_address_file: Optional[Path],
) -> dict:
    payload = build_incremental_plan(
        chain,
        events_dir=events_dir,
        history_dir=history_dir,
        plan_dir=plan_dir,
        to_block=to_block,
        max_scan_workers=max_scan_workers,
        max_apply_workers=max_apply_workers,
        max_new_backfill_workers=max_new_backfill_workers,
        selection_address_file=selection_address_file,
    )
    _persist_plan_state(plan_dir, chain, payload)
    return payload


def _cycle_root(plan: dict) -> Path:
    return Path(str(plan["cycleRoot"]))


def _cycle_progress_dir(plan: dict) -> Path:
    return _cycle_root(plan) / PROGRESS_SUBDIR


def _cycle_log_dir(plan: dict) -> Path:
    return _cycle_root(plan) / LOG_SUBDIR


def _launch_path(plan: dict, stage: str) -> Path:
    return _cycle_progress_dir(plan) / f"{stage}-launch.json"


def _load_launch(path: Path) -> dict:
    payload = _read_json(path, None)
    if isinstance(payload, dict):
        return payload
    return {"runs": []}


def _save_launch(path: Path, payload: dict) -> None:
    _write_json(path, payload)


def _runs_by_key(launch_payload: dict) -> Dict[str, dict]:
    mapping = {}
    for run in launch_payload.get("runs") or []:
        key = str(run.get("progressKey") or "")
        if key:
            mapping[key] = run
    return mapping


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


def _read_address_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    return [
        str(line).strip().lower()
        for line in path.read_text(encoding="utf-8").splitlines()
        if str(line).strip()
    ]


def _scan_progress_path(plan: dict, chain: str, progress_key: str) -> Path:
    return Path(plan["deltaEventsDir"]) / PROGRESS_SUBDIR / f"{chain}--{progress_key}.json"


def _apply_progress_path(history_dir: Path, chain: str, progress_key: str) -> Path:
    return history_dir / PROGRESS_SUBDIR / f"{chain}--{progress_key}.json"


def _scan_task_argv(plan: dict, chain: str, task: dict) -> List[str]:
    return [
        "python3",
        "scan_earn_subaccount_history_events.py",
        "--chain",
        chain,
        "--address-file",
        str(task["addressFile"]),
        "--from-block",
        str(task["fromBlock"]),
        "--to-block",
        str(task["toBlock"]),
        "--progress-key",
        str(task["progressKey"]),
        "--output-dir",
        str(plan["deltaEventsDir"]),
    ]


def _apply_task_argv(plan: dict, chain: str, task: dict, history_dir: Path) -> List[str]:
    return [
        "python3",
        "apply_earn_subaccount_history_delta.py",
        "--chain",
        chain,
        "--address-file",
        str(task["addressFile"]),
        "--start-index",
        str(task["startIndex"]),
        "--end-index",
        str(task["endIndex"]),
        "--progress-key",
        str(task["progressKey"]),
        "--events-dir",
        str(plan["deltaEventsDir"]),
        "--history-dir",
        str(history_dir),
        "--output-dir",
        str(history_dir),
    ]


def _task_run_payload(progress_key: str, pid: int, argv: List[str], log_path: Path) -> dict:
    return {
        "progressKey": progress_key,
        "pid": pid,
        "argv": argv,
        "logPath": str(log_path),
        "startedAt": _utc_now_iso(),
        "runnerSessionId": RUNNER_SESSION_ID,
    }


def _stage_launch_status(plan: dict, stage: str) -> dict:
    launch = _load_launch(_launch_path(plan, stage))
    runs = []
    for run in launch.get("runs") or []:
        runs.append({
            "progressKey": run.get("progressKey"),
            "pid": run.get("pid"),
            "alive": _is_launch_run_alive(run),
            "logPath": run.get("logPath"),
            "startedAt": run.get("startedAt"),
        })
    return {
        "runCount": len(runs),
        "aliveCount": sum(1 for run in runs if run["alive"]),
        "runs": runs,
    }


def _scan_stage_status(plan: dict, chain: str) -> dict:
    tasks = plan.get("scanTasks") or []
    launch = _load_launch(_launch_path(plan, "scan"))
    runs = _runs_by_key(launch)
    worker_rows = []
    completed = 0
    for task in tasks:
        key = str(task["progressKey"])
        progress = _read_json(_scan_progress_path(plan, chain, key), None)
        status = str((progress or {}).get("status") or "pending")
        if status == "completed":
            completed += 1
        run = runs.get(key)
        worker_rows.append({
            "progressKey": key,
            "status": status,
            "alive": _is_launch_run_alive(run),
            "fromBlock": task.get("fromBlock"),
            "toBlock": task.get("toBlock"),
            "eventCount": (progress or {}).get("eventCount"),
            "selectedEventCount": (progress or {}).get("selectedEventCount"),
        })
    manifest = _read_json(Path(plan["deltaEventsDir"]) / "manifest.json", {})
    chain_manifest = ((manifest.get("chains") or {}).get(chain) or {}) if isinstance(manifest, dict) else {}
    try:
        manifest_target = int(chain_manifest.get("globalToBlock") or chain_manifest.get("toBlock") or 0)
    except Exception:
        manifest_target = 0
    stage_complete = bool(
        len(tasks) == completed
        and (not tasks or manifest_target == int(plan.get("targetBlock") or 0))
    )
    return {
        "workerCount": len(tasks),
        "completedWorkerCount": completed,
        "manifestTargetBlock": manifest_target or None,
        "complete": stage_complete,
        "workers": worker_rows,
    }


def _new_address_task_status(task: dict, *, history_dir: Path, chain: str, target_block: int) -> dict:
    addresses = _read_address_file(Path(task["addressFile"]))
    fresh = 0
    for address in addresses:
        if _history_last_block(history_dir, chain, address) >= target_block:
            fresh += 1
    return {
        "progressKey": str(task["progressKey"]),
        "addressCount": len(addresses),
        "freshAddressCount": fresh,
        "complete": fresh == len(addresses),
    }


def _new_address_stage_status(plan: dict, chain: str, *, history_dir: Path) -> dict:
    tasks = plan.get("newAddressTasks") or []
    launch = _load_launch(_launch_path(plan, "new-address"))
    runs = _runs_by_key(launch)
    target_block = int(plan.get("targetBlock") or 0)
    worker_rows = []
    completed = 0
    for task in tasks:
        row = _new_address_task_status(task, history_dir=history_dir, chain=chain, target_block=target_block)
        run = runs.get(row["progressKey"])
        row["alive"] = _is_launch_run_alive(run)
        if row["complete"]:
            completed += 1
        worker_rows.append(row)
    return {
        "workerCount": len(tasks),
        "completedWorkerCount": completed,
        "complete": completed == len(tasks),
        "workers": worker_rows,
    }


def _apply_stage_status(plan: dict, chain: str, *, history_dir: Path) -> dict:
    tasks = plan.get("applyTasks") or []
    launch = _load_launch(_launch_path(plan, "apply"))
    runs = _runs_by_key(launch)
    target_block = int(plan.get("targetBlock") or 0)
    worker_rows = []
    completed = 0
    for task in tasks:
        key = str(task["progressKey"])
        progress = _read_json(_apply_progress_path(history_dir, chain, key), None)
        raw_status = str((progress or {}).get("status") or "pending")
        progress_target = None
        try:
            progress_target = int((progress or {}).get("targetBlock") or 0)
        except Exception:
            progress_target = 0
        missing_count = int((progress or {}).get("missingExistingCount") or 0)
        stale_count = int((progress or {}).get("staleExistingCount") or 0)
        done = raw_status == "completed" and progress_target == target_block and missing_count == 0 and stale_count == 0
        if done:
            completed += 1
        run = runs.get(key)
        alive = _is_launch_run_alive(run)
        status = raw_status
        if raw_status == "completed" and progress_target and progress_target != target_block:
            status = "running" if alive else "completed_for_previous_target"
        worker_rows.append({
            "progressKey": key,
            "status": status,
            "alive": alive,
            "selectedAddressCount": (progress or {}).get("selectedAddressCount"),
            "updatedAddressCount": (progress or {}).get("updatedAddressCount"),
            "targetBlock": progress_target or None,
        })
    return {
        "workerCount": len(tasks),
        "completedWorkerCount": completed,
        "complete": completed == len(tasks),
        "workers": worker_rows,
    }


def _coverage_status(plan: dict, chain: str, *, history_dir: Path) -> dict:
    selection_file = plan.get("selectionAddressFile")
    addresses = _read_address_file(Path(selection_file)) if selection_file else _load_known_addresses(chain)
    if not addresses:
        return {
            "selectedAddressCount": 0,
            "freshWalletCount": 0,
            "partialWalletCount": 0,
            "missingWalletCount": 0,
            "freshCoverageRatio": 0.0,
            "complete": True,
        }
    payload = build_coverage_report(
        chain,
        history_dir=history_dir,
        events_dir=Path(plan["deltaEventsDir"]),
        addresses=addresses,
        target_block=int(plan["targetBlock"]),
        include_wallets=False,
    )
    payload["complete"] = (
        int(payload.get("freshWalletCount") or 0) == int(payload.get("selectedAddressCount") or 0)
        and int(payload.get("partialWalletCount") or 0) == 0
        and int(payload.get("missingWalletCount") or 0) == 0
    )
    return payload


def build_incremental_status(plan: dict, chain: str, *, history_dir: Path) -> dict:
    scan = _scan_stage_status(plan, chain)
    new_address = _new_address_stage_status(plan, chain, history_dir=history_dir)
    apply_stage = _apply_stage_status(plan, chain, history_dir=history_dir)
    coverage = None
    if scan["complete"] and new_address["complete"] and apply_stage["complete"]:
        coverage = _coverage_status(plan, chain, history_dir=history_dir)
    return {
        "chain": chain,
        "cycleId": plan.get("cycleId"),
        "cycleRoot": plan.get("cycleRoot"),
        "targetBlock": plan.get("targetBlock"),
        "baseTargetBlock": plan.get("baseTargetBlock"),
        "deltaRequired": plan.get("deltaRequired"),
        "scan": scan,
        "newAddressBackfill": new_address,
        "apply": apply_stage,
        "coverage": coverage,
        "complete": bool(
            scan["complete"]
            and new_address["complete"]
            and apply_stage["complete"]
            and ((coverage or {}).get("complete", True))
        ),
    }


def _should_build_fresh_plan(*, status: dict, refresh_plan: bool) -> bool:
    if refresh_plan:
        return True
    if status.get("complete"):
        return False
    return False


def _ensure_scan_running(plan: dict, chain: str) -> dict:
    tasks = plan.get("scanTasks") or []
    launch_path = _launch_path(plan, "scan")
    launch = _load_launch(launch_path)
    runs = _runs_by_key(launch)
    started = []
    skipped = []
    for task in tasks:
        key = str(task["progressKey"])
        progress = _read_json(_scan_progress_path(plan, chain, key), None)
        if isinstance(progress, dict) and str(progress.get("status") or "") == "completed":
            skipped.append({"progressKey": key, "reason": "completed"})
            continue
        existing = runs.get(key)
        if existing and _is_launch_run_alive(existing):
            skipped.append({"progressKey": key, "reason": "already_running", "pid": existing.get("pid")})
            continue
        log_path = _cycle_log_dir(plan) / f"scan-{key}.log"
        argv = _scan_task_argv(plan, chain, task)
        pid = _start_task(argv, cwd=ROOT, log_path=log_path)
        runs[key] = _task_run_payload(key, pid, argv, log_path)
        started.append({"progressKey": key, "pid": pid})
    _save_launch(launch_path, {
        "chain": chain,
        "cycleId": plan.get("cycleId"),
        "targetBlock": plan.get("targetBlock"),
        "updatedAt": _utc_now_iso(),
        "runnerSessionId": RUNNER_SESSION_ID,
        "runs": list(runs.values()),
    })
    return {"started": started, "skipped": skipped, "launchPath": str(launch_path)}


def _ensure_new_address_running(plan: dict, chain: str, *, history_dir: Path) -> dict:
    tasks = plan.get("newAddressTasks") or []
    launch_path = _launch_path(plan, "new-address")
    launch = _load_launch(launch_path)
    runs = _runs_by_key(launch)
    started = []
    skipped = []
    target_block = int(plan.get("targetBlock") or 0)
    for task in tasks:
        key = str(task["progressKey"])
        status = _new_address_task_status(task, history_dir=history_dir, chain=chain, target_block=target_block)
        if status["complete"]:
            skipped.append({"progressKey": key, "reason": "completed"})
            continue
        existing = runs.get(key)
        if existing and _is_launch_run_alive(existing):
            skipped.append({"progressKey": key, "reason": "already_running", "pid": existing.get("pid")})
            continue
        log_path = _cycle_log_dir(plan) / f"new-address-{key}.log"
        argv = [
            "python3",
            "build_earn_subaccount_history.py",
            "--chain",
            chain,
            "--address-file",
            str(task["addressFile"]),
            "--to-block",
            str(plan["targetBlock"]),
            "--output-dir",
            str(history_dir),
        ]
        pid = _start_task(argv, cwd=ROOT, log_path=log_path)
        runs[key] = _task_run_payload(key, pid, argv, log_path)
        started.append({"progressKey": key, "pid": pid})
    _save_launch(launch_path, {
        "chain": chain,
        "cycleId": plan.get("cycleId"),
        "targetBlock": plan.get("targetBlock"),
        "updatedAt": _utc_now_iso(),
        "runnerSessionId": RUNNER_SESSION_ID,
        "runs": list(runs.values()),
    })
    return {"started": started, "skipped": skipped, "launchPath": str(launch_path)}


def _ensure_apply_running(plan: dict, chain: str, *, history_dir: Path) -> dict:
    tasks = plan.get("applyTasks") or []
    launch_path = _launch_path(plan, "apply")
    launch = _load_launch(launch_path)
    runs = _runs_by_key(launch)
    started = []
    skipped = []
    target_block = int(plan.get("targetBlock") or 0)
    for task in tasks:
        key = str(task["progressKey"])
        progress = _read_json(_apply_progress_path(history_dir, chain, key), None)
        progress_target = None
        try:
            progress_target = int((progress or {}).get("targetBlock") or 0)
        except Exception:
            progress_target = 0
        missing_count = int((progress or {}).get("missingExistingCount") or 0)
        stale_count = int((progress or {}).get("staleExistingCount") or 0)
        if (
            isinstance(progress, dict)
            and str(progress.get("status") or "") == "completed"
            and progress_target == target_block
            and missing_count == 0
            and stale_count == 0
        ):
            skipped.append({"progressKey": key, "reason": "completed"})
            continue
        existing = runs.get(key)
        if existing and _is_launch_run_alive(existing):
            skipped.append({"progressKey": key, "reason": "already_running", "pid": existing.get("pid")})
            continue
        log_path = _cycle_log_dir(plan) / f"apply-{key}.log"
        argv = _apply_task_argv(plan, chain, task, history_dir)
        pid = _start_task(argv, cwd=ROOT, log_path=log_path)
        runs[key] = _task_run_payload(key, pid, argv, log_path)
        started.append({"progressKey": key, "pid": pid})
    _save_launch(launch_path, {
        "chain": chain,
        "cycleId": plan.get("cycleId"),
        "targetBlock": plan.get("targetBlock"),
        "updatedAt": _utc_now_iso(),
        "runnerSessionId": RUNNER_SESSION_ID,
        "runs": list(runs.values()),
    })
    return {"started": started, "skipped": skipped, "launchPath": str(launch_path)}


def _print_status(payload: dict) -> None:
    print(f"Chain: {payload['chain']}")
    print(f"Cycle: {payload['cycleId']}")
    print(f"Baseline target: {int(payload['baseTargetBlock']):,}")
    print(f"Target: {int(payload['targetBlock']):,}")
    print(f"Delta required: {payload['deltaRequired']}")
    print(
        "Scan workers: "
        f"{payload['scan']['completedWorkerCount']}/{payload['scan']['workerCount']} completed"
    )
    print(
        "New-address workers: "
        f"{payload['newAddressBackfill']['completedWorkerCount']}/{payload['newAddressBackfill']['workerCount']} completed"
    )
    print(
        "Apply workers: "
        f"{payload['apply']['completedWorkerCount']}/{payload['apply']['workerCount']} completed"
    )
    coverage = payload.get("coverage")
    if coverage:
        print(
            "Coverage: "
            f"{coverage['freshWalletCount']}/{coverage['selectedAddressCount']} fresh "
            f"({coverage['freshCoverageRatio']:.2%})"
        )
    print(f"Complete: {payload['complete']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run and monitor strict incremental EARN history cycles")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--chain", default="arbitrum")
    common.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    common.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR))
    common.add_argument("--plan-dir", default=str(DEFAULT_PLAN_DIR))
    common.add_argument("--to-block", type=int, default=None)
    common.add_argument("--max-scan-workers", type=int, default=12)
    common.add_argument("--max-apply-workers", type=int, default=12)
    common.add_argument("--max-new-backfill-workers", type=int, default=12)
    common.add_argument("--selection-address-file", default=None)

    plan_cmd = subparsers.add_parser("plan", parents=[common], help="Build and persist a fresh incremental cycle plan")
    plan_cmd.add_argument("--json", action="store_true")

    status_cmd = subparsers.add_parser("status", parents=[common], help="Show status for the current incremental cycle")
    status_cmd.add_argument("--json", action="store_true")

    continue_cmd = subparsers.add_parser("continue", parents=[common], help="Run or resume the current incremental cycle")
    continue_cmd.add_argument("--refresh-plan", action="store_true")
    continue_cmd.add_argument("--json", action="store_true")

    args = parser.parse_args()
    chain = args.chain
    events_dir = Path(args.events_dir)
    history_dir = Path(args.history_dir)
    plan_dir = Path(args.plan_dir)

    if args.command == "plan":
        plan = _build_fresh_plan(
            chain,
            events_dir=events_dir,
            history_dir=history_dir,
            plan_dir=plan_dir,
            to_block=args.to_block,
            max_scan_workers=args.max_scan_workers,
            max_apply_workers=args.max_apply_workers,
            max_new_backfill_workers=args.max_new_backfill_workers,
            selection_address_file=Path(args.selection_address_file) if args.selection_address_file else None,
        )
        if args.json:
            print(json.dumps(plan, ensure_ascii=True, indent=2))
        else:
            print(json.dumps({
                "cycleId": plan.get("cycleId"),
                "targetBlock": plan.get("targetBlock"),
                "scanTasks": len(plan.get("scanTasks") or []),
                "newAddressTasks": len(plan.get("newAddressTasks") or []),
                "applyTasks": len(plan.get("applyTasks") or []),
                "planPath": str(Path(plan["cycleRoot"]) / "incremental-plan.json"),
            }, ensure_ascii=True, indent=2))
        return 0

    plan = _resolve_current_plan(
        chain,
        events_dir=events_dir,
        history_dir=history_dir,
        plan_dir=plan_dir,
        to_block=args.to_block,
        max_scan_workers=args.max_scan_workers,
        max_apply_workers=args.max_apply_workers,
        max_new_backfill_workers=args.max_new_backfill_workers,
        selection_address_file=Path(args.selection_address_file) if args.selection_address_file else None,
        refresh=False,
    )
    status = build_incremental_status(plan, chain, history_dir=history_dir)

    if args.command == "status":
        if args.json:
            print(json.dumps(status, ensure_ascii=True, indent=2))
        else:
            _print_status(status)
        return 0

    if _should_build_fresh_plan(status=status, refresh_plan=bool(args.refresh_plan)):
        plan = _build_fresh_plan(
            chain,
            events_dir=events_dir,
            history_dir=history_dir,
            plan_dir=plan_dir,
            to_block=args.to_block,
            max_scan_workers=args.max_scan_workers,
            max_apply_workers=args.max_apply_workers,
            max_new_backfill_workers=args.max_new_backfill_workers,
            selection_address_file=Path(args.selection_address_file) if args.selection_address_file else None,
        )
        status = build_incremental_status(plan, chain, history_dir=history_dir)

    scan_launch = _ensure_scan_running(plan, chain)
    status = build_incremental_status(plan, chain, history_dir=history_dir)

    new_address_launch = None
    apply_launch = None
    if status["scan"]["complete"]:
        new_address_launch = _ensure_new_address_running(plan, chain, history_dir=history_dir)
        status = build_incremental_status(plan, chain, history_dir=history_dir)
        if status["newAddressBackfill"]["complete"]:
            apply_launch = _ensure_apply_running(plan, chain, history_dir=history_dir)
            status = build_incremental_status(plan, chain, history_dir=history_dir)

    payload = {
        "plan": {
            "cycleId": plan.get("cycleId"),
            "cycleRoot": plan.get("cycleRoot"),
            "targetBlock": plan.get("targetBlock"),
            "baseTargetBlock": plan.get("baseTargetBlock"),
            "deltaRequired": plan.get("deltaRequired"),
        },
        "scanLaunch": scan_launch,
        "newAddressLaunch": new_address_launch,
        "applyLaunch": apply_launch,
        "status": status,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        _print_status(status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
