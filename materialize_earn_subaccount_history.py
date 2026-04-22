#!/usr/bin/env python3
"""
Materialize canonical per-address EARN subaccount history from local event shards.

This is the local second phase after `scan_earn_subaccount_history_events.py`:
  - read block-range event shards that were scanned once from the chain
  - rebuild strict per-address histories without any additional RPC scanning
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from build_earn_subaccount_history import (
    CHAINS,
    DEFAULT_OUTPUT_DIR as DEFAULT_HISTORY_OUTPUT_DIR,
    _append_normalized_event,
    _empty_history,
    _finalize_history,
    _get_latest_snapshot_date,
    _get_netflow_last_block,
    _load_known_addresses,
    _read_json,
    _write_histories,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_EVENTS_DIR = ROOT / "data" / "earn-subaccount-history-events"
RANGE_RE = re.compile(r"^(?P<from>\d{12})-(?P<to>\d{12})\.json$")
PROGRESS_SUBDIR = ".progress"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _select_addresses(
    chain: str,
    *,
    explicit_addresses: Sequence[str],
    address_files: Sequence[str],
    use_all_known_addresses: bool,
    limit: Optional[int],
    start_index: int,
    end_index: Optional[int],
) -> List[str]:
    file_addresses = set()
    for file_path in address_files:
        path = Path(str(file_path))
        if not path.exists():
            raise FileNotFoundError(f"Address file not found: {path}")
        for line in path.read_text(encoding="utf-8").splitlines():
            address = str(line).strip().lower()
            if address:
                file_addresses.add(address)

    explicit = {
        str(address).strip().lower()
        for address in explicit_addresses
        if str(address).strip()
    }

    if explicit or file_addresses:
        addresses = sorted(explicit | file_addresses)
    else:
        addresses = _load_known_addresses(chain) if use_all_known_addresses or not explicit_addresses else []

    start = max(0, int(start_index))
    stop = len(addresses) if end_index is None else max(start, int(end_index))
    sliced = addresses[start:stop]
    if limit is not None:
        sliced = sliced[: max(0, int(limit))]
    return sliced


def _load_events_manifest(events_dir: Path) -> dict:
    payload = _read_json(events_dir / "manifest.json", {})
    return payload if isinstance(payload, dict) else {}


def _chain_manifest(events_dir: Path, chain: str) -> dict:
    manifest = _load_events_manifest(events_dir)
    chains = manifest.get("chains") or {}
    payload = chains.get(chain) or {}
    return payload if isinstance(payload, dict) else {}


def _iter_shard_paths(events_dir: Path, chain: str) -> Iterable[Tuple[int, int, Path]]:
    chain_dir = events_dir / chain
    if not chain_dir.exists():
        return []
    items: List[Tuple[int, int, Path]] = []
    for path in chain_dir.glob("*.json"):
        match = RANGE_RE.match(path.name)
        if not match:
            continue
        items.append((int(match.group("from")), int(match.group("to")), path))
    items.sort(key=lambda item: (item[0], item[1], str(item[2])))
    return items


def _history_last_block(path: Path) -> int:
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        return 0
    try:
        return int(payload.get("lastScannedBlock") or 0)
    except Exception:
        return 0


def _progress_path(output_dir: Path, chain: str, progress_key: Optional[str] = None) -> Path:
    suffix = ""
    if progress_key:
        normalized = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in str(progress_key))
        normalized = normalized.strip("-_")
        if normalized:
            suffix = f"--{normalized}"
    return output_dir / PROGRESS_SUBDIR / f"{chain}{suffix}.json"


def _write_progress(output_dir: Path, chain: str, payload: dict, progress_key: Optional[str]) -> None:
    path = _progress_path(output_dir, chain, progress_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _load_selected_target_block(events_dir: Path, chain: str) -> int:
    manifest = _chain_manifest(events_dir, chain)
    try:
        return int(manifest.get("toBlock") or 0)
    except Exception:
        return 0


def _infer_scan_bounds(events_dir: Path, chain: str) -> Tuple[int, int]:
    shard_ranges = list(_iter_shard_paths(events_dir, chain))
    if not shard_ranges:
        raise ValueError(f"No event shards found for {chain} in {events_dir}")
    start_block = min(item[0] for item in shard_ranges)
    target_block = max(item[1] for item in shard_ranges)
    return start_block, target_block


def materialize_histories(
    chain: str,
    *,
    addresses: Sequence[str],
    events_dir: Path,
    output_dir: Path,
    no_skip_existing: bool,
    progress_key: Optional[str],
    start_index: Optional[int] = None,
    end_index: Optional[int] = None,
) -> dict:
    start_block, target_block = _infer_scan_bounds(events_dir, chain)

    selected_addresses = [str(address).lower() for address in addresses]
    if not selected_addresses:
        raise ValueError("No addresses selected")

    chain_output_dir = output_dir / chain
    pending_addresses: List[str] = []
    if no_skip_existing:
        pending_addresses = list(selected_addresses)
    else:
        for address in selected_addresses:
            history_path = chain_output_dir / f"{address}.json"
            if _history_last_block(history_path) >= target_block:
                continue
            pending_addresses.append(address)

    if not pending_addresses:
        payload = {
            "chain": chain,
            "targetBlock": target_block,
            "selectedAddressCount": len(selected_addresses),
            "pendingAddressCount": 0,
            "materializedAddressCount": 0,
            "shardCount": 0,
            "eventCount": 0,
            "progressKey": progress_key,
            "status": "completed",
            "updatedAt": _utc_now_iso(),
            "startIndex": start_index,
            "endIndex": end_index,
        }
        _write_progress(output_dir, chain, payload, progress_key)
        return payload

    config = CHAINS[chain]
    source_snapshot_date = _get_latest_snapshot_date(chain)
    source_netflow_last_block = _get_netflow_last_block(chain)
    histories = {
        address: _empty_history(
            chain,
            config["margin"],
            address,
            target_block,
            start_block=start_block,
            source_snapshot_date=source_snapshot_date,
            source_netflow_last_block=source_netflow_last_block,
        )
        for address in pending_addresses
    }

    event_count = 0
    shard_count = 0
    pending_set = set(pending_addresses)

    for shard_from, shard_to, shard_path in _iter_shard_paths(events_dir, chain):
        if shard_to > target_block:
            continue
        payload = _read_json(shard_path, None)
        if not isinstance(payload, dict):
            continue
        owners = payload.get("owners") or {}
        if not isinstance(owners, dict):
            continue

        touched_addresses = sorted(pending_set.intersection(owners.keys()))
        if not touched_addresses:
            continue
        for address in touched_addresses:
            owner_events = owners.get(address) or []
            if not owner_events:
                continue
            for event in owner_events:
                if not isinstance(event, dict):
                    continue
                _append_normalized_event(histories[address], event)
                event_count += 1
        shard_count += 1
        print(
            f"[{chain}] materialized shard {shard_from:,}-{shard_to:,} "
            f"into {len(touched_addresses)} address(es)"
        )
        progress_payload = {
            "chain": chain,
            "targetBlock": target_block,
            "selectedAddressCount": len(selected_addresses),
            "pendingAddressCount": len(pending_addresses),
            "materializedAddressCount": len(histories),
            "shardCount": shard_count,
            "eventCount": event_count,
            "progressKey": progress_key,
            "status": "running",
            "updatedAt": _utc_now_iso(),
            "startIndex": start_index,
            "endIndex": end_index,
            "lastShardFromBlock": shard_from,
            "lastShardToBlock": shard_to,
        }
        _write_progress(output_dir, chain, progress_payload, progress_key)

    finalized = {address: _finalize_history(history) for address, history in histories.items()}
    _write_histories(
        output_dir,
        chain,
        finalized,
        target_block,
        start_block=start_block,
        selection_address_count=len(selected_addresses),
    )
    payload = {
        "chain": chain,
        "targetBlock": target_block,
        "selectedAddressCount": len(selected_addresses),
        "pendingAddressCount": len(pending_addresses),
        "materializedAddressCount": len(finalized),
        "shardCount": shard_count,
        "eventCount": event_count,
        "progressKey": progress_key,
        "status": "completed",
        "updatedAt": _utc_now_iso(),
        "startIndex": start_index,
        "endIndex": end_index,
    }
    _write_progress(output_dir, chain, payload, progress_key)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize canonical EARN histories from local event shards")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--address", action="append", default=[], help="Explicit owner address (repeatable)")
    parser.add_argument("--address-file", action="append", default=[], help="Path to newline-delimited address file (repeatable)")
    parser.add_argument("--all-known-addresses", action="store_true", help="Use addresses from latest snapshot/netflow data")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--end-index", type=int, default=None)
    parser.add_argument("--progress-key", default=None)
    parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_HISTORY_OUTPUT_DIR))
    parser.add_argument("--no-skip-existing", action="store_true")
    args = parser.parse_args()

    selected_addresses = _select_addresses(
        args.chain,
        explicit_addresses=args.address,
        address_files=args.address_file,
        use_all_known_addresses=args.all_known_addresses or not args.address,
        limit=args.limit,
        start_index=args.start_index,
        end_index=args.end_index,
    )
    if not selected_addresses:
        raise SystemExit("No addresses selected")

    payload = materialize_histories(
        args.chain,
        addresses=selected_addresses,
        events_dir=Path(args.events_dir),
        output_dir=Path(args.output_dir),
        no_skip_existing=bool(args.no_skip_existing),
        progress_key=args.progress_key,
        start_index=max(0, int(args.start_index)),
        end_index=None if args.end_index is None else max(0, int(args.end_index)),
    )
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
