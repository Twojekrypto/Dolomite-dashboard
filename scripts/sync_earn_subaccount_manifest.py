#!/usr/bin/env python3
"""Make canonical EARN manifest counts match publishable history files."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
DEFAULT_MANIFEST_PATH = DEFAULT_HISTORY_DIR / "manifest.json"
ADDRESS_FILE_RE = re.compile(r"^0x[0-9a-fA-F]{40}\.json$")


def _read_manifest(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Missing EARN subaccount manifest: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("chains"), dict):
        raise ValueError(f"Invalid EARN subaccount manifest structure: {path}")
    return payload


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
    tmp.replace(path)


def _published_address_count(history_dir: Path, chain: str) -> int:
    chain_dir = history_dir / chain
    if not chain_dir.exists():
        return 0
    return sum(
        1
        for path in chain_dir.iterdir()
        if path.is_file() and ADDRESS_FILE_RE.fullmatch(path.name)
    )


def sync_manifest(
    *,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    history_dir: Path = DEFAULT_HISTORY_DIR,
    chains: Iterable[str],
) -> Dict[str, Any]:
    payload = _read_manifest(manifest_path)
    chain_payloads = payload["chains"]
    selected = sorted({str(chain).strip().lower() for chain in chains if str(chain).strip()})
    for chain in selected:
        meta = chain_payloads.get(chain)
        if not isinstance(meta, dict):
            raise ValueError(f"Missing EARN subaccount manifest entry for {chain}")
        address_count = _published_address_count(history_dir, chain)
        meta["addressCount"] = address_count
        try:
            previous_selection_count = int(meta.get("selectionAddressCount") or 0)
        except (TypeError, ValueError):
            previous_selection_count = 0
        meta["selectionAddressCount"] = max(previous_selection_count, address_count)
    _write_json(manifest_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync EARN canonical manifest file counts")
    parser.add_argument("--chain", action="append", required=True)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST_PATH))
    parser.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR))
    args = parser.parse_args()
    payload = sync_manifest(
        manifest_path=Path(args.manifest),
        history_dir=Path(args.history_dir),
        chains=args.chain,
    )
    for chain in args.chain:
        count = ((payload.get("chains") or {}).get(chain) or {}).get("addressCount")
        print(f"Synced {chain} canonical manifest: {count} published wallet files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
