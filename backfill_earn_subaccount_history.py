#!/usr/bin/env python3
"""
Backfill canonical EARN subaccount history in resumable batches.

This script is the operational companion to build_earn_subaccount_history.py.
Use it when you want to populate `data/earn-subaccount-history` for large
address sets (for example all Arbitrum EARN addresses) without manually
tracking which batch was already written.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Optional, Sequence

from scan_earn_netflow import CHAINS, get_block_number

from build_earn_subaccount_history import (
    DEFAULT_OUTPUT_DIR,
    SUBACCOUNT_HISTORY_VERSION,
    _load_known_addresses,
    _read_json,
    _utc_now_iso,
    _write_histories,
    build_history_for_addresses_in_block_range,
)


PROGRESS_SUBDIR = ".progress"


def _history_path(output_dir: Path, chain: str, address: str) -> Path:
    return output_dir / chain / f"{address.lower()}.json"


def _history_covers_target(path: Path, target_to_block: int) -> bool:
    if not path.exists():
        return False
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        return False
    try:
        return int(payload.get("lastScannedBlock") or 0) >= int(target_to_block)
    except Exception:
        return False


def _progress_path(output_dir: Path, chain: str, progress_key: Optional[str] = None) -> Path:
    suffix = ""
    if progress_key:
        normalized = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in str(progress_key))
        normalized = normalized.strip("-_")
        if normalized:
            suffix = f"--{normalized}"
    return output_dir / PROGRESS_SUBDIR / f"{chain}{suffix}.json"


def _write_progress(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _load_progress(path: Path) -> Optional[dict]:
    payload = _read_json(path, None)
    return payload if isinstance(payload, dict) else None


def _resolve_addresses(
    chain: str,
    *,
    limit: Optional[int],
    start_index: int,
    end_index: Optional[int],
) -> List[str]:
    addresses = _load_known_addresses(chain)
    if start_index > 0:
        addresses = addresses[start_index:]
    if end_index is not None:
        capped = max(0, end_index - max(0, start_index))
        addresses = addresses[:capped]
    if limit is not None:
        addresses = addresses[: max(0, int(limit))]
    return addresses


def _resolve_target_to_block(chain: str, to_block: Optional[int]) -> int:
    if to_block is not None:
        return int(to_block)
    config = CHAINS[chain]
    return int(get_block_number(config["rpcs"], [0]))


def _build_batches(addresses: Sequence[str], batch_size: int) -> List[List[str]]:
    size = max(1, int(batch_size))
    return [list(addresses[idx: idx + size]) for idx in range(0, len(addresses), size)]


def run_chain_backfill(
    chain: str,
    *,
    output_dir: Path,
    batch_size: int,
    from_block: Optional[int],
    to_block: Optional[int],
    limit: Optional[int],
    start_index: int,
    end_index: Optional[int],
    resume: bool,
    skip_existing: bool,
    stop_after_batches: Optional[int],
    progress_key: Optional[str],
) -> None:
    config = CHAINS[chain]
    resolved_from_block = config["start_block"] if from_block is None else int(from_block)
    resolved_to_block = _resolve_target_to_block(chain, to_block)
    if resolved_to_block < resolved_from_block:
        raise ValueError(f"Invalid block range for {chain}: from={resolved_from_block} to={resolved_to_block}")

    selected_addresses = _resolve_addresses(
        chain,
        limit=limit,
        start_index=start_index,
        end_index=end_index,
    )
    if not selected_addresses:
        print(f"[{chain}] no addresses selected, skipping")
        return

    if skip_existing:
        pending_addresses = [
            address
            for address in selected_addresses
            if not _history_covers_target(_history_path(output_dir, chain, address), resolved_to_block)
        ]
    else:
        pending_addresses = list(selected_addresses)

    batch_list = _build_batches(pending_addresses, batch_size)
    total_batches = len(batch_list)
    progress_file = _progress_path(output_dir, chain, progress_key=progress_key)
    progress = _load_progress(progress_file) if resume else None
    next_batch_index = 0

    if progress:
        same_range = (
            progress.get("fromBlock") == resolved_from_block and
            progress.get("toBlock") == resolved_to_block and
            progress.get("batchSize") == batch_size
        )
        if same_range:
            try:
                next_batch_index = min(int(progress.get("nextBatchIndex") or 0), total_batches)
            except Exception:
                next_batch_index = 0

    print(
        f"[{chain}] canonical subaccount backfill: selected={len(selected_addresses)} "
        f"pending={len(pending_addresses)} batches={total_batches} "
        f"range={resolved_from_block:,}-{resolved_to_block:,}"
    )

    if total_batches == 0:
        payload = {
            "version": SUBACCOUNT_HISTORY_VERSION,
            "chain": chain,
            "progressKey": progress_key,
            "updatedAt": _utc_now_iso(),
            "status": "complete",
            "fromBlock": resolved_from_block,
            "toBlock": resolved_to_block,
            "batchSize": batch_size,
            "selectedAddressCount": len(selected_addresses),
            "pendingAddressCount": 0,
            "completedBatchCount": 0,
            "totalBatchCount": 0,
            "nextBatchIndex": 0,
        }
        _write_progress(progress_file, payload)
        return

    batches_run = 0
    for batch_index in range(next_batch_index, total_batches):
        if stop_after_batches is not None and batches_run >= stop_after_batches:
            break

        batch_addresses = batch_list[batch_index]
        print(
            f"[{chain}] batch {batch_index + 1}/{total_batches}: "
            f"{len(batch_addresses)} address(es)"
        )
        histories = build_history_for_addresses_in_block_range(
            chain,
            batch_addresses,
            from_block=resolved_from_block,
            to_block=resolved_to_block,
        )
        latest_block = next(iter(histories.values()))["lastScannedBlock"] if histories else resolved_to_block
        _write_histories(
            output_dir,
            chain,
            histories,
            latest_block,
            start_block=resolved_from_block,
            selection_address_count=len(selected_addresses),
        )

        batches_run += 1
        completed_batch_count = batch_index + 1
        remaining_batches = max(0, total_batches - completed_batch_count)
        remaining_addresses = sum(len(batch) for batch in batch_list[completed_batch_count:])
        progress_payload = {
            "version": SUBACCOUNT_HISTORY_VERSION,
            "chain": chain,
            "progressKey": progress_key,
            "updatedAt": _utc_now_iso(),
            "status": "complete" if remaining_batches == 0 else "running",
            "fromBlock": resolved_from_block,
            "toBlock": resolved_to_block,
            "batchSize": batch_size,
            "selectedAddressCount": len(selected_addresses),
            "pendingAddressCount": len(pending_addresses),
            "completedBatchCount": completed_batch_count,
            "totalBatchCount": total_batches,
            "nextBatchIndex": completed_batch_count,
            "remainingBatchCount": remaining_batches,
            "remainingAddressCount": remaining_addresses,
            "lastCompletedBatchAddressCount": len(batch_addresses),
        }
        _write_progress(progress_file, progress_payload)

    final_progress = _load_progress(progress_file) or {}
    print(
        f"[{chain}] progress: {final_progress.get('completedBatchCount', 0)}/"
        f"{final_progress.get('totalBatchCount', total_batches)} batches complete"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill canonical EARN subaccount history in resumable batches")
    parser.add_argument("--chain", action="append", choices=sorted(CHAINS.keys()), help="Specific chain to backfill (repeatable)")
    parser.add_argument("--batch-size", type=int, default=250, help="Addresses per batch")
    parser.add_argument("--from-block", type=int, default=None, help="Optional starting block override")
    parser.add_argument("--to-block", type=int, default=None, help="Optional ending block override")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on selected addresses")
    parser.add_argument("--start-index", type=int, default=0, help="Skip the first N known addresses before batching")
    parser.add_argument("--end-index", type=int, default=None, help="Stop selection before this absolute index")
    parser.add_argument("--stop-after-batches", type=int, default=None, help="Run only the next N batches (useful for smoke tests)")
    parser.add_argument("--progress-key", default=None, help="Optional suffix for shard-specific progress files")
    parser.add_argument("--no-resume", action="store_true", help="Ignore existing progress metadata")
    parser.add_argument("--no-skip-existing", action="store_true", help="Rebuild even if a per-address file already covers the target block")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for per-address subaccount history files",
    )
    args = parser.parse_args()

    selected_chains = args.chain or ["arbitrum"]
    output_dir = Path(args.output_dir)

    for chain in selected_chains:
        run_chain_backfill(
            chain,
            output_dir=output_dir,
            batch_size=max(1, int(args.batch_size)),
            from_block=args.from_block,
            to_block=args.to_block,
            limit=args.limit,
            start_index=max(0, int(args.start_index)),
            end_index=args.end_index,
            resume=not args.no_resume,
            skip_existing=not args.no_skip_existing,
            stop_after_batches=args.stop_after_batches,
            progress_key=args.progress_key,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
