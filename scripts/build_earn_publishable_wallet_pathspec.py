#!/usr/bin/env python3
"""Build a NUL-delimited pathspec for an exact EARN wallet cohort."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


CHAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
ALL_PUBLISHABLE_TREES = (
    "data/earn-subaccount-history",
    "data/earn-verified-ledger",
    "data/earn-resolved-interest-ledger",
)
VERIFIED_PUBLISHABLE_TREES = (
    "data/earn-verified-ledger",
    "data/earn-resolved-interest-ledger",
)


def fail(message: str) -> "NoReturn":
    raise SystemExit(message)


def main() -> int:
    if len(sys.argv) != 5:
        fail(
            "usage: build_earn_publishable_wallet_pathspec.py "
            "CHAIN ADDRESS_FILE OUTPUT SCOPE"
        )

    chain, address_file_arg, output_arg, scope = sys.argv[1:]
    if not CHAIN_RE.fullmatch(chain):
        fail(f"invalid chain: {chain!r}")
    if scope == "--all":
        publishable_trees = ALL_PUBLISHABLE_TREES
    elif scope == "--verified-only":
        publishable_trees = VERIFIED_PUBLISHABLE_TREES
    else:
        fail(f"invalid publication scope: {scope!r}")

    address_file = Path(address_file_arg)
    try:
        raw_addresses = address_file.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read address file {address_file}: {exc}")

    addresses: list[str] = []
    seen_addresses: set[str] = set()
    for line_number, address in enumerate(raw_addresses, start=1):
        if not address:
            continue
        if not ADDRESS_RE.fullmatch(address):
            fail(f"invalid address on line {line_number}: {address!r}")
        normalized = address.lower()
        if normalized not in seen_addresses:
            seen_addresses.add(normalized)
            addresses.append(normalized)

    tracked_result = subprocess.run(
        [
            "git",
            "ls-files",
            "-z",
            "--",
            *(f"{tree}/{chain}" for tree in publishable_trees),
        ],
        check=True,
        stdout=subprocess.PIPE,
    )
    tracked_paths = {
        path.decode("utf-8")
        for path in tracked_result.stdout.split(b"\0")
        if path
    }

    selected_paths: list[str] = []
    for address in addresses:
        for tree in publishable_trees:
            candidate = f"{tree}/{chain}/{address}.json"
            if Path(candidate).is_file() or candidate in tracked_paths:
                selected_paths.append(candidate)

    output_path = Path(output_arg)
    output_path.write_bytes(
        b"".join(path.encode("utf-8") + b"\0" for path in selected_paths)
    )
    print(len(selected_paths))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
