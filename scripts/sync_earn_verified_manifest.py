#!/usr/bin/env python3
"""Merge verified EARN ledger manifest entries after workflow rebases.

Concurrent chain workflows can each rebuild one chain's ledger while carrying a
manifest snapshot from checkout time. After a rebase, rebuild only the touched
chain entries from the ledger files and keep unrelated chain entries from the
fresh remote manifest.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER_DIR = ROOT / "data" / "earn-verified-ledger"
DEFAULT_MANIFEST_PATH = DEFAULT_LEDGER_DIR / "manifest.json"


def _read_json(path: Path, default: Any, *, strict: bool = False) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except json.JSONDecodeError as exc:
        if strict:
            raise ValueError(f"Invalid JSON in {path}") from exc
        return default


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
    tmp.replace(path)


def _read_manifest_from_ref(base_ref: str, manifest_path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        relative = manifest_path.resolve().relative_to(ROOT)
    except ValueError:
        return default
    proc = subprocess.run(
        ["git", "show", f"{base_ref}:{relative.as_posix()}"],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return default
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return default
    return payload if isinstance(payload, dict) else default


def _chain_dirs(ledger_dir: Path) -> list[str]:
    if not ledger_dir.exists():
        return []
    return sorted(
        path.name
        for path in ledger_dir.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    )


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _latest_chain_meta(
    ledger_dir: Path,
    chain: str,
    *,
    current_meta: Dict[str, Any],
    base_meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    chain_dir = ledger_dir / chain
    if not chain_dir.exists():
        return dict(current_meta or base_meta) if (current_meta or base_meta) else None

    rows: list[tuple[str, int]] = []
    for path in chain_dir.glob("*.json"):
        payload = _read_json(path, None, strict=True)
        if not isinstance(payload, dict):
            continue
        snapshot_date = str(payload.get("snapshotDate") or "").strip()
        if not snapshot_date:
            continue
        comparison_block = _safe_int((payload.get("canonicalHistory") or {}).get("comparisonBlock"))
        rows.append((snapshot_date, comparison_block or 0))

    if not rows:
        return dict(current_meta or base_meta) if (current_meta or base_meta) else None

    latest_date = max(snapshot_date for snapshot_date, _ in rows)
    latest_rows = [(snapshot_date, block) for snapshot_date, block in rows if snapshot_date == latest_date]
    current_snapshot = str((current_meta or {}).get("snapshotDate") or "")
    base_snapshot = str((base_meta or {}).get("snapshotDate") or "")
    latest_block = 0
    if current_snapshot == latest_date:
        latest_block = _safe_int((current_meta or {}).get("lastNetflowBlock")) or 0
    if latest_block <= 0 and base_snapshot == latest_date:
        latest_block = _safe_int((base_meta or {}).get("lastNetflowBlock")) or 0
    if latest_block <= 0:
        latest_block = max((block for _, block in latest_rows), default=0)
    if latest_block <= 0:
        latest_block = (
            _safe_int((current_meta or {}).get("lastNetflowBlock"))
            or _safe_int((base_meta or {}).get("lastNetflowBlock"))
            or 0
        )

    return {
        "snapshotDate": latest_date,
        "lastNetflowBlock": latest_block,
        "addressCount": len(latest_rows),
    }


def sync_manifest(
    *,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    ledger_dir: Path = DEFAULT_LEDGER_DIR,
    chains: Iterable[str],
    base_manifest: Optional[Dict[str, Any]] = None,
    base_ref: str = "",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    current_manifest = _read_json(manifest_path, {"version": 2, "generatedAt": "", "chains": {}})
    if not isinstance(current_manifest, dict):
        current_manifest = {"version": 2, "generatedAt": "", "chains": {}}

    default_base = current_manifest if isinstance(current_manifest, dict) else {"chains": {}}
    if base_manifest is None:
        base_manifest = (
            _read_manifest_from_ref(base_ref, manifest_path, default_base)
            if base_ref
            else default_base
        )
    if not isinstance(base_manifest, dict):
        base_manifest = {"version": 2, "generatedAt": "", "chains": {}}

    selected_chains = sorted({str(chain).strip().lower() for chain in chains if str(chain).strip()})
    current_chains = current_manifest.get("chains") or {}
    base_chains = base_manifest.get("chains") or {}
    out_chains = dict(base_chains)

    for chain in selected_chains:
        meta = _latest_chain_meta(
            ledger_dir,
            chain,
            current_meta=(current_chains.get(chain) or {}),
            base_meta=(base_chains.get(chain) or {}),
        )
        if meta:
            out_chains[chain] = meta

    generated_at = (now or datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z")
    payload = {
        "version": 2,
        "generatedAt": generated_at,
        "chains": out_chains,
    }
    _write_json(manifest_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge EARN verified ledger manifest entries")
    parser.add_argument("--chain", action="append", default=[], help="Chain entry to rebuild from ledger files")
    parser.add_argument("--all-chains", action="store_true", help="Rebuild entries for every chain directory")
    parser.add_argument("--base-ref", default="", help="Git ref to use as the base manifest, e.g. origin/master")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST_PATH))
    parser.add_argument("--ledger-dir", default=str(DEFAULT_LEDGER_DIR))
    args = parser.parse_args()

    ledger_dir = Path(args.ledger_dir)
    chains = _chain_dirs(ledger_dir) if args.all_chains else args.chain
    if not chains:
        print("No EARN verified manifest chains requested; nothing to sync.")
        return 0

    payload = sync_manifest(
        manifest_path=Path(args.manifest),
        ledger_dir=ledger_dir,
        chains=chains,
        base_ref=args.base_ref,
    )
    synced = ", ".join(sorted({str(chain).lower() for chain in chains}))
    print(f"Synced EARN verified ledger manifest for: {synced}")
    print(json.dumps({"chains": payload.get("chains", {})}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
