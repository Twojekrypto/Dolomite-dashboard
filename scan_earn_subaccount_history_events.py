#!/usr/bin/env python3
"""
Scan margin events once and write decoded EARN subaccount-history event shards.

This is the fast-path precursor to canonical per-address history files:
  1. scan the chain once into block-range event shards
  2. materialize per-address histories locally from those shards

That keeps correctness strict while avoiding repeated full-chain rescans for
every address shard.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from build_earn_subaccount_history import (
    _load_known_addresses,
    _normalized_event_from_decoded,
    _read_json,
    _write_json,
)
from scan_earn_netflow import (
    ALL_EVENTS,
    BLOCK_CHUNK,
    CHAINS,
    _dedupe_logs,
    decode_log_entries,
    get_block_number,
    get_logs,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "data" / "earn-subaccount-history-events"
EVENT_SHARD_VERSION = 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _selection_hash(addresses: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for address in sorted(str(address).lower() for address in addresses):
        digest.update(address.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _sort_logs(logs: Sequence[dict]) -> List[dict]:
    return sorted(
        logs,
        key=lambda log: (
            int(log.get("blockNumber", "0x0"), 16),
            int(log.get("transactionIndex", "0x0"), 16),
            int(log.get("logIndex", "0x0"), 16),
        ),
    )


def _progress_path(output_dir: Path, chain: str, progress_key: Optional[str] = None) -> Path:
    suffix = f"--{progress_key}" if progress_key else ""
    return output_dir / ".progress" / f"{chain}{suffix}.json"


def _shard_path(output_dir: Path, chain: str, from_block: int, to_block: int) -> Path:
    chain_dir = output_dir / chain
    chain_dir.mkdir(parents=True, exist_ok=True)
    return chain_dir / f"{from_block:012d}-{to_block:012d}.json"


def _count_shards(chain_dir: Path) -> int:
    if not chain_dir.exists():
        return 0
    return sum(1 for path in chain_dir.glob("*.json") if path.is_file())


def _write_manifest(
    output_dir: Path,
    chain: str,
    *,
    from_block: int,
    to_block: int,
    latest_chain_block: int,
    selected_address_count: int,
    selection_hash: str,
    total_log_count: int,
    total_decoded_entry_count: int,
    total_selected_event_count: int,
    progress_key: Optional[str],
) -> None:
    manifest_path = output_dir / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = output_dir / ".manifest.lock"
    chain_dir = output_dir / chain
    range_entry = {
        "fromBlock": int(from_block),
        "toBlock": int(to_block),
        "latestChainBlock": int(latest_chain_block),
        "selectedAddressCount": int(selected_address_count),
        "selectionHash": str(selection_hash),
        "totalLogCount": int(total_log_count),
        "totalDecodedEntryCount": int(total_decoded_entry_count),
        "totalSelectedEventCount": int(total_selected_event_count),
        "progressKey": progress_key,
        "updatedAt": _utc_now_iso(),
    }
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        manifest = _read_json(manifest_path, {"chains": {}})
        chains = manifest.setdefault("chains", {})
        chain_entry = chains.setdefault(chain, {"scanRanges": []})
        scan_ranges = chain_entry.setdefault("scanRanges", [])
        replaced = False
        for idx, existing in enumerate(list(scan_ranges)):
            if (
                int(existing.get("fromBlock") or -1) == int(from_block)
                and int(existing.get("toBlock") or -1) == int(to_block)
                and str(existing.get("progressKey") or "") == str(progress_key or "")
            ):
                scan_ranges[idx] = range_entry
                replaced = True
                break
        if not replaced:
            scan_ranges.append(range_entry)
        scan_ranges.sort(key=lambda item: (int(item.get("fromBlock") or 0), int(item.get("toBlock") or 0)))
        chain_entry.update({
            "version": EVENT_SHARD_VERSION,
            "updatedAt": _utc_now_iso(),
            "selectedAddressCount": int(selected_address_count),
            "selectionHash": str(selection_hash),
            "shardCount": _count_shards(chain_dir),
            "rangeCount": len(scan_ranges),
            "globalFromBlock": min(int(item.get("fromBlock") or 0) for item in scan_ranges),
            "globalToBlock": max(int(item.get("toBlock") or 0) for item in scan_ranges),
            "latestChainBlock": max(int(item.get("latestChainBlock") or 0) for item in scan_ranges),
            "totalLogCount": sum(int(item.get("totalLogCount") or 0) for item in scan_ranges),
            "totalDecodedEntryCount": sum(int(item.get("totalDecodedEntryCount") or 0) for item in scan_ranges),
            "totalSelectedEventCount": sum(int(item.get("totalSelectedEventCount") or 0) for item in scan_ranges),
        })
        _write_json(manifest_path, manifest)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _build_progress_payload(
    *,
    chain: str,
    contract: str,
    from_block: int,
    to_block: int,
    latest_chain_block: int,
    last_block_exclusive: int,
    selected_address_count: int,
    selection_hash: str,
    total_log_count: int,
    total_decoded_entry_count: int,
    total_selected_event_count: int,
    shard_count: int,
    progress_key: Optional[str],
    status: str,
) -> dict:
    return {
        "version": EVENT_SHARD_VERSION,
        "chain": chain,
        "marginContract": contract.lower(),
        "fromBlock": int(from_block),
        "toBlock": int(to_block),
        "latestChainBlock": int(latest_chain_block),
        "lastBlockExclusive": int(last_block_exclusive),
        "selectedAddressCount": int(selected_address_count),
        "selectionHash": str(selection_hash),
        "totalLogCount": int(total_log_count),
        "totalDecodedEntryCount": int(total_decoded_entry_count),
        "totalSelectedEventCount": int(total_selected_event_count),
        "shardCount": int(shard_count),
        "progressKey": progress_key,
        "status": status,
        "updatedAt": _utc_now_iso(),
    }


def _load_progress(output_dir: Path, chain: str, progress_key: Optional[str]) -> Optional[dict]:
    path = _progress_path(output_dir, chain, progress_key)
    payload = _read_json(path, None)
    return payload if isinstance(payload, dict) else None


def _save_progress(output_dir: Path, chain: str, progress_key: Optional[str], payload: dict) -> None:
    _write_json(_progress_path(output_dir, chain, progress_key), payload)


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
        addresses = sorted({
            *explicit,
            *file_addresses,
        })
    else:
        addresses = _load_known_addresses(chain) if use_all_known_addresses or not explicit_addresses else []

    start = max(0, int(start_index))
    stop = len(addresses) if end_index is None else max(start, int(end_index))
    sliced = addresses[start:stop]
    if limit is not None:
        sliced = sliced[: max(0, int(limit))]
    return sliced


def scan_chain_to_event_shards(
    chain: str,
    *,
    selected_addresses: Sequence[str],
    output_dir: Path,
    from_block: Optional[int],
    to_block: Optional[int],
    chunk_size: int,
    no_resume: bool,
    progress_key: Optional[str],
) -> dict:
    if not selected_addresses:
        raise ValueError("No addresses selected")

    config = CHAINS[chain]
    contract = config["margin"]
    rpcs = config["rpcs"]
    rpc_idx = [0]
    latest_chain_block = int(get_block_number(rpcs, rpc_idx))
    resolved_from_block = int(config["start_block"] if from_block is None else from_block)
    resolved_to_block = min(int(to_block) if to_block is not None else latest_chain_block, latest_chain_block)
    if resolved_to_block < resolved_from_block:
        raise ValueError(f"Invalid block range: from={resolved_from_block} to={resolved_to_block}")

    address_set = set(str(address).lower() for address in selected_addresses)
    selection_hash = _selection_hash(selected_addresses)

    current = resolved_from_block
    total_log_count = 0
    total_decoded_entry_count = 0
    total_selected_event_count = 0
    shard_count = 0
    adaptive_chunk_size = max(1_000, int(chunk_size))

    progress = None if no_resume else _load_progress(output_dir, chain, progress_key)
    if progress:
        if (
            str(progress.get("selectionHash") or "") == selection_hash
            and int(progress.get("fromBlock") or -1) == resolved_from_block
            and int(progress.get("toBlock") or -1) == resolved_to_block
        ):
            current = max(current, int(progress.get("lastBlockExclusive") or current))
            total_log_count = int(progress.get("totalLogCount") or 0)
            total_decoded_entry_count = int(progress.get("totalDecodedEntryCount") or 0)
            total_selected_event_count = int(progress.get("totalSelectedEventCount") or 0)
            shard_count = int(progress.get("shardCount") or 0)
            print(
                f"[{chain}] resuming event-shard scan at block {current:,} "
                f"({shard_count} shard(s), {total_selected_event_count} selected event(s))"
            )

    while current <= resolved_to_block:
        chunk_end = min(current + adaptive_chunk_size - 1, resolved_to_block)
        try:
            logs = _sort_logs(_dedupe_logs(get_logs(rpcs, rpc_idx, contract, [ALL_EVENTS], current, chunk_end) or []))
            owners: Dict[str, List[dict]] = {}
            decoded_entry_count = 0
            selected_event_count = 0

            for log in logs:
                entries = decode_log_entries(log) or []
                decoded_entry_count += len(entries)
                for entry in entries:
                    owner = str(entry.get("owner") or "").lower()
                    if owner not in address_set:
                        continue
                    normalized = _normalized_event_from_decoded(entry, log)
                    if normalized is None:
                        continue
                    owners.setdefault(owner, []).append(normalized)
                    selected_event_count += 1

            if owners:
                touched_addresses = sorted(owners.keys())
                shard_payload = {
                    "version": EVENT_SHARD_VERSION,
                    "chain": chain,
                    "marginContract": contract.lower(),
                    "fromBlock": int(current),
                    "toBlock": int(chunk_end),
                    "targetBlock": int(resolved_to_block),
                    "selectedAddressCount": len(selected_addresses),
                    "selectionHash": selection_hash,
                    "generatedAt": _utc_now_iso(),
                    "logCount": len(logs),
                    "decodedEntryCount": decoded_entry_count,
                    "selectedEventCount": selected_event_count,
                    "ownerCount": len(owners),
                    "touchedAddressCount": len(touched_addresses),
                    "touchedAddresses": touched_addresses,
                    "owners": owners,
                }
                _write_json(_shard_path(output_dir, chain, current, chunk_end), shard_payload)
                shard_count += 1

            total_log_count += len(logs)
            total_decoded_entry_count += decoded_entry_count
            total_selected_event_count += selected_event_count

            pct = ((chunk_end - resolved_from_block) / max(1, resolved_to_block - resolved_from_block)) * 100
            print(
                f"[{chain}] [{pct:5.1f}%] block {chunk_end:,} "
                f"logs={len(logs)} decoded={decoded_entry_count} selected={selected_event_count} shards={shard_count}"
            )

            progress_payload = _build_progress_payload(
                chain=chain,
                contract=contract,
                from_block=resolved_from_block,
                to_block=resolved_to_block,
                latest_chain_block=latest_chain_block,
                last_block_exclusive=chunk_end + 1,
                selected_address_count=len(selected_addresses),
                selection_hash=selection_hash,
                total_log_count=total_log_count,
                total_decoded_entry_count=total_decoded_entry_count,
                total_selected_event_count=total_selected_event_count,
                shard_count=shard_count,
                progress_key=progress_key,
                status="running",
            )
            _save_progress(output_dir, chain, progress_key, progress_payload)

            if len(logs) > 5_000 and adaptive_chunk_size > 5_000:
                adaptive_chunk_size = max(5_000, adaptive_chunk_size // 2)
                print(f"[{chain}] reducing chunk size to {adaptive_chunk_size:,}")
            elif len(logs) < 100 and adaptive_chunk_size < BLOCK_CHUNK:
                adaptive_chunk_size = min(BLOCK_CHUNK, adaptive_chunk_size * 2)

            current = chunk_end + 1
        except Exception as exc:
            error_msg = str(exc)
            if "Too Many" in error_msg or "rate" in error_msg.lower():
                print(f"[{chain}] rate limited, waiting 5s")
                time.sleep(5)
                continue
            if "range" in error_msg.lower() or "10000" in error_msg or "exceed" in error_msg.lower():
                adaptive_chunk_size = max(1_000, adaptive_chunk_size // 2)
                print(f"[{chain}] block range too large, reducing to {adaptive_chunk_size:,}")
                continue
            print(f"[{chain}] error at block {current:,}: {exc}")
            time.sleep(2)
            rpc_idx[0] += 1

    final_payload = _build_progress_payload(
        chain=chain,
        contract=contract,
        from_block=resolved_from_block,
        to_block=resolved_to_block,
        latest_chain_block=latest_chain_block,
        last_block_exclusive=resolved_to_block + 1,
        selected_address_count=len(selected_addresses),
        selection_hash=selection_hash,
        total_log_count=total_log_count,
        total_decoded_entry_count=total_decoded_entry_count,
        total_selected_event_count=total_selected_event_count,
        shard_count=shard_count,
        progress_key=progress_key,
        status="completed",
    )
    _save_progress(output_dir, chain, progress_key, final_payload)
    _write_manifest(
        output_dir,
        chain,
        from_block=resolved_from_block,
        to_block=resolved_to_block,
        latest_chain_block=latest_chain_block,
        selected_address_count=len(selected_addresses),
        selection_hash=selection_hash,
        total_log_count=total_log_count,
        total_decoded_entry_count=total_decoded_entry_count,
        total_selected_event_count=total_selected_event_count,
        progress_key=progress_key,
    )
    return final_payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan global subaccount-history event shards once")
    parser.add_argument("--chain", default="arbitrum", choices=sorted(CHAINS.keys()))
    parser.add_argument("--address", action="append", default=[], help="Explicit owner address (repeatable)")
    parser.add_argument("--address-file", action="append", default=[], help="Path to newline-delimited address file (repeatable)")
    parser.add_argument("--all-known-addresses", action="store_true", help="Use addresses from latest snapshot/netflow data")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--end-index", type=int, default=None)
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=BLOCK_CHUNK)
    parser.add_argument("--progress-key", default=None)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
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

    payload = scan_chain_to_event_shards(
        args.chain,
        selected_addresses=selected_addresses,
        output_dir=Path(args.output_dir),
        from_block=args.from_block,
        to_block=args.to_block,
        chunk_size=max(1_000, int(args.chunk_size)),
        no_resume=bool(args.no_resume),
        progress_key=args.progress_key,
    )
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
