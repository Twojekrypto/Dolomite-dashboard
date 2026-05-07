#!/usr/bin/env python3
"""
GitHub Actions wrapper for canonical EARN subaccount-history refreshes.

It bootstraps a selected wallet set when a chain has no baseline yet, then uses
the strict incremental runner for later scheduled refreshes.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import List, Optional

from build_earn_subaccount_history import _read_json
from scan_earn_netflow import CHAINS


ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
DEFAULT_EVENTS_DIR = ROOT / "data" / "earn-subaccount-history-events"


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


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


class RefreshIncomplete(Exception):
    def __init__(self, *, chain: str, phase: str, max_steps: int, payload: Optional[dict]):
        super().__init__(f"{phase} did not complete for {chain} after {max_steps} polling step(s)")
        self.chain = chain
        self.phase = phase
        self.max_steps = max_steps
        self.payload = payload if isinstance(payload, dict) else {}


def _read_address_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    addresses: List[str] = []
    seen = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        address = raw.strip().lower()
        if not address or address.startswith("#"):
            continue
        if not address.startswith("0x") or len(address) != 42:
            raise ValueError(f"Invalid address in {path}: {raw}")
        if address not in seen:
            seen.add(address)
            addresses.append(address)
    return addresses


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


def _is_launch_run_alive(run: dict) -> bool:
    if not isinstance(run, dict) or not _launch_run_belongs_to_session(run):
        return False
    return _is_pid_alive(run.get("pid"))


def _run_json(argv: List[str]) -> dict:
    print("+ " + " ".join(argv), flush=True)
    proc = subprocess.run(
        argv,
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n", flush=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {proc.returncode}: {' '.join(argv)}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Command did not return JSON: {' '.join(argv)}") from exc


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _stage_counts(payload: object) -> dict:
    if not isinstance(payload, dict):
        return {}
    return {
        "complete": bool(payload.get("complete")),
        "completedWorkerCount": _safe_int(payload.get("completedWorkerCount")),
        "workerCount": _safe_int(payload.get("workerCount")),
    }


def _summarize_progress(payload: Optional[dict]) -> dict:
    if not isinstance(payload, dict):
        return {}
    status = payload.get("status")
    if isinstance(status, dict):
        return {
            "cycleId": status.get("cycleId"),
            "targetBlock": status.get("targetBlock"),
            "complete": bool(status.get("complete")),
            "scan": _stage_counts(status.get("scan")),
            "newAddressBackfill": _stage_counts(status.get("newAddressBackfill")),
            "apply": _stage_counts(status.get("apply")),
            "coverage": status.get("coverage"),
        }
    return {
        "lockedTargetBlock": payload.get("lockedTargetBlock"),
        "scanComplete": bool(payload.get("scanComplete")),
        "materializeComplete": bool(payload.get("materializeComplete")),
        "repairComplete": bool(payload.get("repairComplete")),
    }


def _status_payload(
    *,
    chain: str,
    phase: str,
    complete: bool,
    selected_addresses: List[str],
    validation: Optional[dict] = None,
    incomplete: Optional[RefreshIncomplete] = None,
) -> dict:
    payload = {
        "chain": chain,
        "phase": phase,
        "complete": bool(complete),
        "selectedAddressCount": len(selected_addresses),
    }
    if validation is not None:
        payload["validation"] = validation
    if incomplete is not None:
        payload.update({
            "maxSteps": incomplete.max_steps,
            "message": str(incomplete),
            "progress": _summarize_progress(incomplete.payload),
        })
    return payload


def _validation_incomplete(
    *, args: argparse.Namespace, phase: str, payload: Optional[dict], exc: AssertionError
) -> RefreshIncomplete:
    validation_payload = dict(payload or {})
    validation_payload["validationError"] = str(exc)
    return RefreshIncomplete(
        chain=args.chain,
        phase=phase,
        max_steps=args.max_steps,
        payload=validation_payload,
    )


def _write_status_output(path: Optional[Path], payload: dict) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _terminate_launch_processes(path: Path) -> List[dict]:
    launch = _read_json(path, None)
    if not isinstance(launch, dict):
        return []
    terminated = []
    changed = False
    for run in launch.get("runs") or []:
        if not isinstance(run, dict):
            continue
        pid = _safe_int(run.get("pid"))
        if pid <= 0:
            continue
        if not _is_launch_run_alive(run):
            continue
        argv = run.get("argv") or []
        script = str(argv[1] if isinstance(argv, list) and len(argv) > 1 else "")
        if script and not script.endswith(".py"):
            continue
        try:
            os.killpg(pid, signal.SIGTERM)
        except OSError:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                continue
        run["terminatedAt"] = _utc_now_iso()
        terminated.append({"pid": pid, "progressKey": run.get("progressKey"), "launchPath": str(path)})
        changed = True
    if changed:
        _write_json(path, launch)
    return terminated


def _stop_checkpoint_workers(args: argparse.Namespace) -> List[dict]:
    paths = [
        args.events_dir / ".progress" / f"{args.chain}-scan-launch.json",
        args.history_dir / ".progress" / f"{args.chain}-materialize-launch.json",
        args.history_dir / ".progress" / f"{args.chain}-repair-materialize-launch.json",
    ]
    state = _read_json(args.history_dir / ".incremental-plans" / f"{args.chain}-runner-state.json", {})
    plan_path = state.get("planPath") if isinstance(state, dict) else None
    if plan_path:
        cycle_root = Path(str(plan_path)).parent
        paths.extend([
            cycle_root / ".progress" / "scan-launch.json",
            cycle_root / ".progress" / "new-address-launch.json",
            cycle_root / ".progress" / "apply-launch.json",
        ])
    terminated: List[dict] = []
    for path in paths:
        terminated.extend(_terminate_launch_processes(path))
    if terminated:
        time.sleep(2)
        print(f"Stopped {len(terminated)} background worker(s) before checkpoint cache save", flush=True)
    return terminated


def _manifest_last_block(history_dir: Path, chain: str) -> int:
    manifest = _read_json(history_dir / "manifest.json", {})
    chain_payload = ((manifest.get("chains") or {}).get(chain) or {}) if isinstance(manifest, dict) else {}
    try:
        return int(chain_payload.get("lastBlock") or 0)
    except Exception:
        return 0


def _has_complete_baseline(history_dir: Path, chain: str, selected_addresses: List[str]) -> bool:
    target_block = _manifest_last_block(history_dir, chain)
    if target_block <= 0:
        return False
    chain_dir = history_dir / chain
    if not chain_dir.exists():
        return False
    if not selected_addresses:
        return False
    for address in selected_addresses:
        payload = _read_json(chain_dir / f"{address}.json", None)
        if not isinstance(payload, dict):
            return False
        try:
            last_scanned = int(payload.get("lastScannedBlock") or 0)
        except Exception:
            return False
        if last_scanned < target_block:
            return False
    return True


def _validate_selected_histories(history_dir: Path, chain: str, selected_addresses: List[str]) -> dict:
    target_block = _manifest_last_block(history_dir, chain)
    if target_block <= 0:
        raise AssertionError(f"No manifest target block for {chain}")
    missing = []
    stale = []
    for address in selected_addresses:
        path = history_dir / chain / f"{address}.json"
        payload = _read_json(path, None)
        if not isinstance(payload, dict):
            missing.append(address)
            continue
        try:
            last_scanned = int(payload.get("lastScannedBlock") or 0)
        except Exception:
            last_scanned = 0
        if last_scanned < target_block:
            stale.append({"address": address, "lastScannedBlock": last_scanned})
    if missing or stale:
        raise AssertionError(
            f"{chain} canonical history incomplete: "
            f"missing={len(missing)} stale={len(stale)} target={target_block}"
        )
    payload = {
        "chain": chain,
        "targetBlock": target_block,
        "selectedAddressCount": len(selected_addresses),
        "freshWalletCount": len(selected_addresses),
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2), flush=True)
    return payload


def _bootstrap_baseline(args: argparse.Namespace, selected_addresses: List[str]) -> dict:
    complete = False
    payload: Optional[dict] = None
    for step in range(max(1, int(args.max_steps))):
        argv = [
            "python3",
            "run_earn_data_correctness_pipeline.py",
            "continue",
            "--chain",
            args.chain,
            "--selection-address-file",
            str(args.selection_address_file),
            "--events-dir",
            str(args.events_dir),
            "--history-dir",
            str(args.history_dir),
            "--max-scan-workers",
            str(args.max_scan_workers),
            "--max-materialize-workers",
            str(args.max_materialize_workers),
            "--repair-limit",
            str(len(selected_addresses)),
        ]
        if step == 0:
            argv.append("--refresh-plan")
        payload = _run_json(argv)
        complete = bool(
            payload.get("scanComplete")
            and payload.get("materializeComplete")
            and payload.get("repairComplete")
        )
        if complete:
            break
        time.sleep(max(1, int(args.sleep_seconds)))
    if not complete:
        raise RefreshIncomplete(chain=args.chain, phase="bootstrap", max_steps=args.max_steps, payload=payload)
    try:
        validation = _validate_selected_histories(args.history_dir, args.chain, selected_addresses)
    except AssertionError as exc:
        raise _validation_incomplete(args=args, phase="bootstrap-validation", payload=payload, exc=exc) from exc
    return _status_payload(
        chain=args.chain,
        phase="bootstrap",
        complete=True,
        selected_addresses=selected_addresses,
        validation=validation,
    )


def _incremental_refresh(args: argparse.Namespace, selected_addresses: List[str]) -> dict:
    complete = False
    payload: Optional[dict] = None
    for step in range(max(1, int(args.max_steps))):
        argv = [
            "python3",
            "run_earn_subaccount_history_incremental.py",
            "continue",
            "--chain",
            args.chain,
            "--selection-address-file",
            str(args.selection_address_file),
            "--history-dir",
            str(args.history_dir),
            "--max-scan-workers",
            str(args.max_incremental_scan_workers),
            "--max-apply-workers",
            str(args.max_incremental_apply_workers),
            "--max-new-backfill-workers",
            str(args.max_new_backfill_workers),
            "--json",
        ]
        if step == 0:
            argv.append("--refresh-plan")
        payload = _run_json(argv)
        status = payload.get("status") or {}
        coverage = status.get("coverage") or {}
        ratio = float(coverage.get("freshCoverageRatio") or 0)
        complete = bool(status.get("complete") and ratio >= float(args.min_fresh_ratio))
        if complete:
            break
        time.sleep(max(1, int(args.sleep_seconds)))
    if not complete:
        raise RefreshIncomplete(chain=args.chain, phase="incremental", max_steps=args.max_steps, payload=payload)
    try:
        validation = _validate_selected_histories(args.history_dir, args.chain, selected_addresses)
    except AssertionError as exc:
        raise _validation_incomplete(args=args, phase="incremental-validation", payload=payload, exc=exc) from exc
    return _status_payload(
        chain=args.chain,
        phase="incremental",
        complete=True,
        selected_addresses=selected_addresses,
        validation=validation,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh selected canonical EARN subaccount histories")
    parser.add_argument("--chain", required=True, choices=sorted(CHAINS.keys()))
    parser.add_argument("--selection-address-file", type=Path, required=True)
    parser.add_argument("--events-dir", type=Path, default=DEFAULT_EVENTS_DIR)
    parser.add_argument("--history-dir", type=Path, default=DEFAULT_HISTORY_DIR)
    parser.add_argument("--max-scan-workers", type=int, default=8)
    parser.add_argument("--max-materialize-workers", type=int, default=8)
    parser.add_argument("--max-incremental-scan-workers", type=int, default=8)
    parser.add_argument("--max-incremental-apply-workers", type=int, default=8)
    parser.add_argument("--max-new-backfill-workers", type=int, default=4)
    parser.add_argument("--max-steps", type=int, default=720)
    parser.add_argument("--sleep-seconds", type=int, default=20)
    parser.add_argument("--min-fresh-ratio", type=float, default=1.0)
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument("--skip-unsupported-start-block", action="store_true")
    parser.add_argument("--allow-checkpoint-incomplete", action="store_true")
    parser.add_argument("--status-output", type=Path, default=None)
    args = parser.parse_args()

    start_block = int(CHAINS[args.chain].get("start_block") or 0)
    if start_block <= 0:
        message = f"{args.chain} canonical history skipped: start_block is not configured ({start_block})"
        if args.skip_unsupported_start_block:
            print(message)
            _write_status_output(args.status_output, {
                "chain": args.chain,
                "phase": "skipped",
                "complete": True,
                "skipped": True,
                "message": message,
            })
            return 0
        raise SystemExit(message)

    selected_addresses = _read_address_file(args.selection_address_file)
    if not selected_addresses:
        message = f"{args.chain} canonical history skipped: selection is empty"
        if args.allow_empty:
            print(message)
            _write_status_output(args.status_output, {
                "chain": args.chain,
                "phase": "skipped",
                "complete": True,
                "skipped": True,
                "message": message,
            })
            return 0
        raise SystemExit(message)

    try:
        if _has_complete_baseline(args.history_dir, args.chain, selected_addresses):
            print(f"[{args.chain}] baseline found; running incremental refresh", flush=True)
            payload = _incremental_refresh(args, selected_addresses)
        else:
            print(f"[{args.chain}] no baseline found; bootstrapping selected canonical history", flush=True)
            payload = _bootstrap_baseline(args, selected_addresses)
    except RefreshIncomplete as exc:
        payload = _status_payload(
            chain=args.chain,
            phase=exc.phase,
            complete=False,
            selected_addresses=selected_addresses,
            incomplete=exc,
        )
        if args.allow_checkpoint_incomplete:
            try:
                payload["terminatedWorkers"] = _stop_checkpoint_workers(args)
            except Exception as cleanup_exc:
                payload["checkpointCleanupError"] = f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                print(
                    f"Warning: checkpoint worker cleanup failed, preserving checkpoint status anyway: {cleanup_exc}",
                    flush=True,
                )
            _write_status_output(args.status_output, payload)
            print(json.dumps(payload, ensure_ascii=True, indent=2), flush=True)
            return 0
        _write_status_output(args.status_output, payload)
        raise TimeoutError(str(exc)) from exc

    _write_status_output(args.status_output, payload)
    print(json.dumps(payload, ensure_ascii=True, indent=2), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
