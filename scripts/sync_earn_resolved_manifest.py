#!/usr/bin/env python3
"""Rebuild resolved-interest manifest from files present after a rebase."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path("data/earn-resolved-interest-ledger")


def _read_json(path: Path, default=None):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default


def sync_manifest(root: Path, *, chains=None, generated_at=None):
    root = Path(root)
    manifest_path = root / "manifest.json"
    existing = _read_json(manifest_path, {}) or {}
    existing_chains = existing.get("chains") or {}
    if chains is None:
        selected = sorted(path.name for path in root.iterdir() if path.is_dir()) if root.is_dir() else []
    else:
        selected = sorted({str(chain).lower() for chain in chains})

    chain_meta = dict(existing_chains)
    for chain in selected:
        files = sorted((root / chain).glob("0x*.json")) if (root / chain).is_dir() else []
        snapshot_dates = []
        for path in files:
            payload = _read_json(path, {}) or {}
            snapshot_date = str(payload.get("snapshotDate") or "")
            if snapshot_date:
                snapshot_dates.append(snapshot_date)
        chain_meta[chain] = {
            "snapshotDate": max(snapshot_dates) if snapshot_dates else "",
            "addressCount": len(files),
        }

    payload = {
        "version": 1,
        "generatedAt": generated_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "chains": chain_meta,
    }
    root.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
    temporary.replace(manifest_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Synchronize resolved EARN interest manifest")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--chain", action="append", default=[])
    parser.add_argument("--all-chains", action="store_true")
    args = parser.parse_args()
    if not args.chain and not args.all_chains:
        raise SystemExit("Provide --chain or --all-chains")
    payload = sync_manifest(args.root, chains=None if args.all_chains else args.chain)
    print(json.dumps(payload, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
