#!/usr/bin/env python3
"""
GitHub Actions wrapper for canonical EARN subaccount-history refreshes.

It bootstraps a selected wallet set when a chain has no baseline yet, then uses
the strict incremental runner for later scheduled refreshes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import List, Optional

from build_earn_subaccount_history import _read_json
from scan_earn_netflow import CHAINS


ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
DEFAULT_EVENTS_DIR = ROOT / "data" / "earn-subaccount-history-events"


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
        raise TimeoutError(f"Bootstrap did not complete for {args.chain} after {args.max_steps} polling step(s)")
    return _validate_selected_histories(args.history_dir, args.chain, selected_addresses)


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
        raise TimeoutError(f"Incremental refresh did not complete for {args.chain} after {args.max_steps} polling step(s)")
    return _validate_selected_histories(args.history_dir, args.chain, selected_addresses)


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
    args = parser.parse_args()

    start_block = int(CHAINS[args.chain].get("start_block") or 0)
    if start_block <= 0:
        message = f"{args.chain} canonical history skipped: start_block is not configured ({start_block})"
        if args.skip_unsupported_start_block:
            print(message)
            return 0
        raise SystemExit(message)

    selected_addresses = _read_address_file(args.selection_address_file)
    if not selected_addresses:
        message = f"{args.chain} canonical history skipped: selection is empty"
        if args.allow_empty:
            print(message)
            return 0
        raise SystemExit(message)

    if _has_complete_baseline(args.history_dir, args.chain, selected_addresses):
        print(f"[{args.chain}] baseline found; running incremental refresh", flush=True)
        _incremental_refresh(args, selected_addresses)
    else:
        print(f"[{args.chain}] no baseline found; bootstrapping selected canonical history", flush=True)
        _bootstrap_baseline(args, selected_addresses)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
