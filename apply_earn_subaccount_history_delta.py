#!/usr/bin/env python3
"""
Apply a scanned delta-cycle onto canonical EARN subaccount histories.

This is the strict incremental fast path:
  1. scan only blocks after the current canonical target block
  2. append exact normalized events to histories that changed
  3. metadata-stamp untouched tracked histories to the new target block

No heuristics are introduced here: unchanged histories are advanced only because
the delta scan for the tracked address universe proved there were no matching
events in the scanned block range.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from build_earn_subaccount_history import (
    CHAINS,
    DEFAULT_OUTPUT_DIR,
    _append_normalized_event,
    _finalize_history,
    _get_latest_snapshot_date,
    _get_netflow_last_block,
    _read_json,
    _write_histories,
)
from materialize_earn_subaccount_history import _chain_manifest, _iter_shard_paths


ROOT = Path(__file__).resolve().parent
DEFAULT_EVENTS_DIR = ROOT / "data" / "earn-subaccount-history-events"
PROGRESS_SUBDIR = ".progress"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _history_path(history_dir: Path, chain: str, address: str) -> Path:
    return history_dir / chain / f"{address.lower()}.json"


def _history_addresses(history_dir: Path, chain: str) -> List[str]:
    chain_dir = history_dir / chain
    if not chain_dir.exists():
        return []
    return sorted(
        path.stem.lower()
        for path in chain_dir.glob("0x*.json")
        if path.is_file() and len(path.stem) == 42
    )


def _delta_bounds(events_dir: Path, chain: str) -> Tuple[int, int]:
    manifest = _chain_manifest(events_dir, chain)
    try:
        start_block = int(manifest.get("globalFromBlock") or 0)
        target_block = int(manifest.get("globalToBlock") or 0)
    except Exception:
        start_block = 0
        target_block = 0
    if start_block > 0 and target_block >= start_block:
        return start_block, target_block

    shard_ranges = list(_iter_shard_paths(events_dir, chain))
    if not shard_ranges:
        raise ValueError(f"No delta shards found for {chain} in {events_dir}")
    return min(item[0] for item in shard_ranges), max(item[1] for item in shard_ranges)


def _collect_touched_addresses(events_dir: Path, chain: str) -> List[str]:
    touched = set()
    for _, _, shard_path in _iter_shard_paths(events_dir, chain):
        payload = _read_json(shard_path, None)
        if not isinstance(payload, dict):
            continue
        explicit = payload.get("touchedAddresses")
        if isinstance(explicit, list):
            touched.update(str(address).lower() for address in explicit if str(address).strip())
            continue
        owners = payload.get("owners") or {}
        if isinstance(owners, dict):
            touched.update(str(address).lower() for address in owners.keys() if str(address).strip())
    return sorted(touched)


def _select_addresses(
    chain: str,
    *,
    history_dir: Path,
    explicit_addresses: Sequence[str],
    address_files: Sequence[str],
    use_all_history_addresses: bool,
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
    elif use_all_history_addresses:
        addresses = _history_addresses(history_dir, chain)
    else:
        addresses = []

    start = max(0, int(start_index))
    stop = len(addresses) if end_index is None else max(start, int(end_index))
    sliced = addresses[start:stop]
    if limit is not None:
        sliced = sliced[: max(0, int(limit))]
    return sliced


def _progress_path(output_dir: Path, chain: str, progress_key: Optional[str]) -> Path:
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


def _history_manifest_selection_count(history_dir: Path, chain: str) -> Optional[int]:
    manifest = _read_json(history_dir / "manifest.json", {})
    chains = manifest.get("chains") or {}
    chain_payload = chains.get(chain) or {}
    try:
        value = int(chain_payload.get("selectionAddressCount"))
        return value if value > 0 else None
    except Exception:
        return None


def apply_delta_histories(
    chain: str,
    *,
    events_dir: Path,
    history_dir: Path,
    output_dir: Path,
    addresses: Sequence[str],
    progress_key: Optional[str],
    start_index: Optional[int],
    end_index: Optional[int],
) -> dict:
    delta_from_block, delta_target_block = _delta_bounds(events_dir, chain)
    selected_addresses = [str(address).lower() for address in addresses]
    if not selected_addresses:
        raise ValueError("No addresses selected")

    touched_addresses = set(_collect_touched_addresses(events_dir, chain))
    source_snapshot_date = _get_latest_snapshot_date(chain)
    source_netflow_last_block = _get_netflow_last_block(chain)

    updated_histories: Dict[str, dict] = {}
    missing_existing: List[str] = []
    stale_existing: List[str] = []
    already_fresh: List[str] = []
    appended_addresses = set()
    appended_count = 0
    event_count = 0
    shard_count = 0
    previous_last_blocks: Dict[str, int] = {}

    for address in selected_addresses:
        history_path = _history_path(history_dir, chain, address)
        history = _read_json(history_path, None)
        if not isinstance(history, dict):
            missing_existing.append(address)
            continue
        try:
            last_scanned_block = int(history.get("lastScannedBlock") or 0)
        except Exception:
            last_scanned_block = 0
        if last_scanned_block >= delta_target_block:
            already_fresh.append(address)
            continue
        if last_scanned_block < delta_from_block - 1:
            stale_existing.append(address)
            continue

        previous_last_blocks[address] = last_scanned_block
        history["lastScannedBlock"] = int(delta_target_block)
        history["generatedAt"] = _utc_now_iso()
        scan_range = history.setdefault("scanRange", {})
        if not scan_range.get("fromBlock"):
            scan_range["fromBlock"] = max(0, int(delta_from_block) - 1)
        scan_range["toBlock"] = int(delta_target_block)
        source_meta = history.setdefault("sourceMetadata", {})
        source_meta["latestSnapshotDate"] = source_snapshot_date
        source_meta["lastNetflowBlock"] = source_netflow_last_block
        updated_histories[address] = history

    if not updated_histories:
        payload = {
            "chain": chain,
            "deltaFromBlock": delta_from_block,
            "targetBlock": delta_target_block,
            "selectedAddressCount": len(selected_addresses),
            "updatedAddressCount": 0,
            "touchedSelectedAddressCount": 0,
            "stampedAddressCount": 0,
            "appendedAddressCount": 0,
            "missingExistingCount": len(missing_existing),
            "staleExistingCount": len(stale_existing),
            "alreadyFreshCount": len(already_fresh),
            "eventCount": 0,
            "shardCount": 0,
            "progressKey": progress_key,
            "status": "completed",
            "updatedAt": _utc_now_iso(),
            "startIndex": start_index,
            "endIndex": end_index,
        }
        _write_progress(output_dir, chain, payload, progress_key)
        return payload

    updatable_touched = {address for address in updated_histories.keys() if address in touched_addresses}

    for shard_from, shard_to, shard_path in _iter_shard_paths(events_dir, chain):
        if shard_to < delta_from_block or shard_from > delta_target_block:
            continue
        payload = _read_json(shard_path, None)
        if not isinstance(payload, dict):
            continue
        owners = payload.get("owners") or {}
        if not isinstance(owners, dict):
            continue

        touched_in_shard = sorted(updatable_touched.intersection(owners.keys()))
        if not touched_in_shard:
            continue

        for address in touched_in_shard:
            owner_events = owners.get(address) or []
            if not owner_events:
                continue
            min_event_block = int(previous_last_blocks.get(address, delta_from_block - 1)) + 1
            relevant_events = []
            for event in owner_events:
                if not isinstance(event, dict):
                    continue
                try:
                    block_number = int(event.get("blockNumber") or 0)
                except Exception:
                    block_number = 0
                if min_event_block <= block_number <= delta_target_block:
                    relevant_events.append(event)
            if not relevant_events:
                continue
            appended_addresses.add(address)
            for event in relevant_events:
                _append_normalized_event(updated_histories[address], event)
                event_count += 1
            appended_count = len(appended_addresses)
        shard_count += 1
        progress_payload = {
            "chain": chain,
            "deltaFromBlock": delta_from_block,
            "targetBlock": delta_target_block,
            "selectedAddressCount": len(selected_addresses),
            "updatedAddressCount": len(updated_histories),
            "touchedSelectedAddressCount": len(appended_addresses),
            "stampedAddressCount": len(updated_histories) - len(appended_addresses),
            "appendedAddressCount": appended_count,
            "missingExistingCount": len(missing_existing),
            "staleExistingCount": len(stale_existing),
            "alreadyFreshCount": len(already_fresh),
            "eventCount": event_count,
            "shardCount": shard_count,
            "progressKey": progress_key,
            "status": "running",
            "updatedAt": _utc_now_iso(),
            "startIndex": start_index,
            "endIndex": end_index,
            "lastShardFromBlock": shard_from,
            "lastShardToBlock": shard_to,
        }
        _write_progress(output_dir, chain, progress_payload, progress_key)

    finalized = {address: _finalize_history(history) for address, history in updated_histories.items()}
    _write_histories(
        output_dir,
        chain,
        finalized,
        delta_target_block,
        selection_address_count=_history_manifest_selection_count(history_dir, chain),
    )
    payload = {
        "chain": chain,
        "deltaFromBlock": delta_from_block,
        "targetBlock": delta_target_block,
        "selectedAddressCount": len(selected_addresses),
        "updatedAddressCount": len(finalized),
        "touchedSelectedAddressCount": len(appended_addresses),
        "stampedAddressCount": len(finalized) - len(appended_addresses),
        "appendedAddressCount": appended_count,
        "missingExistingCount": len(missing_existing),
        "staleExistingCount": len(stale_existing),
        "alreadyFreshCount": len(already_fresh),
        "eventCount": event_count,
        "shardCount": shard_count,
        "progressKey": progress_key,
        "status": "completed",
        "updatedAt": _utc_now_iso(),
        "startIndex": start_index,
        "endIndex": end_index,
    }
    _write_progress(output_dir, chain, payload, progress_key)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply a canonical EARN delta-cycle onto existing histories")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--address", action="append", default=[], help="Explicit owner address (repeatable)")
    parser.add_argument("--address-file", action="append", default=[], help="Path to newline-delimited address file (repeatable)")
    parser.add_argument("--all-existing-addresses", action="store_true", help="Use all existing canonical history files on this chain")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--end-index", type=int, default=None)
    parser.add_argument("--progress-key", default=None)
    parser.add_argument("--events-dir", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--history-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    history_dir = Path(args.history_dir)
    selected_addresses = _select_addresses(
        args.chain,
        history_dir=history_dir,
        explicit_addresses=args.address,
        address_files=args.address_file,
        use_all_history_addresses=args.all_existing_addresses or not args.address,
        limit=args.limit,
        start_index=args.start_index,
        end_index=args.end_index,
    )
    if not selected_addresses:
        raise SystemExit("No addresses selected")

    payload = apply_delta_histories(
        args.chain,
        events_dir=Path(args.events_dir),
        history_dir=history_dir,
        output_dir=Path(args.output_dir),
        addresses=selected_addresses,
        progress_key=args.progress_key,
        start_index=args.start_index,
        end_index=args.end_index,
    )
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
