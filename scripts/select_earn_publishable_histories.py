#!/usr/bin/env python3
"""Select canonical wallet histories that are complete to a locked target."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from scan_earn_netflow import CHAINS


ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _as_int(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def select_publishable_histories(
    *,
    chain: str,
    addresses: Iterable[str],
    history_dir: Path,
    start_block: int,
    target_block: int,
) -> Tuple[List[str], Dict[str, str]]:
    normalized_chain = str(chain).strip().lower()
    required_start = int(start_block)
    required_target = int(target_block)
    if required_target < required_start:
        raise ValueError("target_block must be greater than or equal to start_block")

    selected: List[str] = []
    rejected: Dict[str, str] = {}
    seen = set()
    for raw_address in addresses:
        raw = str(raw_address).strip()
        address = raw.lower()
        if not ADDRESS_RE.fullmatch(address):
            rejected[raw] = "invalid_address"
            continue
        if address in seen:
            continue
        seen.add(address)

        path = history_dir / normalized_chain / f"{address}.json"
        payload = _read_json(path)
        if not isinstance(payload, dict):
            rejected[address] = "missing_history"
            continue
        if str(payload.get("chain") or "").strip().lower() != normalized_chain:
            rejected[address] = "wrong_chain"
            continue
        if str(payload.get("address") or "").strip().lower() != address:
            rejected[address] = "wrong_address"
            continue

        scan_range = payload.get("scanRange")
        if not isinstance(scan_range, dict):
            rejected[address] = "missing_scan_range"
            continue
        from_block = _as_int(scan_range.get("fromBlock"))
        to_block = _as_int(scan_range.get("toBlock"))
        last_scanned_block = _as_int(payload.get("lastScannedBlock"))
        if from_block is None or from_block > required_start:
            rejected[address] = "truncated_start"
            continue
        if (
            to_block is None
            or last_scanned_block is None
            or to_block < required_target
            or last_scanned_block < required_target
        ):
            rejected[address] = "stale_target"
            continue
        selected.append(address)

    return selected, rejected


def _read_addresses(paths: Sequence[str]) -> List[str]:
    addresses: List[str] = []
    for raw_path in paths:
        path = Path(raw_path)
        for line in path.read_text(encoding="utf-8").splitlines():
            value = line.strip()
            if value and not value.startswith("#"):
                addresses.append(value)
    return addresses


def _write_lines(path: Path, values: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(values)
    path.write_text(f"{body}\n" if body else "", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", required=True, choices=sorted(CHAINS))
    parser.add_argument("--selection-address-file", action="append", required=True)
    parser.add_argument("--history-dir", required=True)
    parser.add_argument("--target-block", required=True, type=int)
    parser.add_argument("--output", required=True)
    parser.add_argument("--metadata-output")
    args = parser.parse_args()

    chain_config = CHAINS[args.chain]
    addresses = _read_addresses(args.selection_address_file)
    selected, rejected = select_publishable_histories(
        chain=args.chain,
        addresses=addresses,
        history_dir=Path(args.history_dir),
        start_block=int(chain_config["start_block"]),
        target_block=args.target_block,
    )
    _write_lines(Path(args.output), selected)

    metadata = {
        "chain": args.chain,
        "startBlock": int(chain_config["start_block"]),
        "targetBlock": int(args.target_block),
        "selectedAddressCount": len(addresses),
        "publishableAddressCount": len(selected),
        "rejectedAddressCount": len(rejected),
        "rejectedReasons": {
            reason: sum(1 for value in rejected.values() if value == reason)
            for reason in sorted(set(rejected.values()))
        },
    }
    if args.metadata_output:
        metadata_path = Path(args.metadata_output)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
