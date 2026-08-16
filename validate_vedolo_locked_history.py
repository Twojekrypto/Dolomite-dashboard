#!/usr/bin/env python3
"""Fail-closed semantic validation for the Locked DOLO event history."""

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path


class LockedHistoryValidationError(RuntimeError):
    """Raised when veDOLO flow history cannot be reconciled safely."""


def _snapshot_seconds(payload):
    raw = payload.get("timestamp")
    if not isinstance(raw, str) or not raw:
        raise LockedHistoryValidationError("holder snapshot timestamp is missing")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LockedHistoryValidationError("holder snapshot timestamp is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _event_identity(row):
    return (
        str(row.get("txHash") or "").lower(),
        int(row.get("tokenId") or 0),
        int(row.get("block") or 0),
    )


def _validated_rows(flows, key):
    rows = flows.get(key)
    total = flows.get(f"total_{key}")
    if not isinstance(rows, list) or isinstance(total, bool) or not isinstance(total, int):
        raise LockedHistoryValidationError(f"invalid {key} collection")
    if total != len(rows):
        raise LockedHistoryValidationError(
            f"{key} total mismatch: declared {total}, found {len(rows)}"
        )
    identities = [_event_identity(row) for row in rows]
    if len(identities) != len(set(identities)):
        singular = "lock" if key == "locks" else "unlock"
        raise LockedHistoryValidationError(f"duplicate {singular} event identity")
    return rows


def reconstructed_active_locked_dolo(flows, snapshot_sec):
    locks = _validated_rows(flows, "locks")
    unlocks = _validated_rows(flows, "unlocks")
    earliest_unlock = {}
    for row in unlocks:
        token_id = int(row.get("tokenId") or 0)
        timestamp = int(row.get("timestamp") or 0)
        if token_id > 0 and timestamp > 0:
            earliest_unlock[token_id] = min(earliest_unlock.get(token_id, timestamp), timestamp)

    active = 0.0
    for row in locks:
        token_id = int(row.get("tokenId") or 0)
        amount = float(row.get("dolo") or 0)
        start = int(row.get("timestamp") or 0)
        expiry = int(row.get("locktime") or 0)
        unlock = earliest_unlock.get(token_id)
        out = unlock if unlock is not None else expiry
        if (
            token_id > 0
            and math.isfinite(amount)
            and amount >= 0
            and 0 < start <= snapshot_sec
            and (out <= 0 or out > snapshot_sec)
        ):
            active += amount
    return active


def active_locked_dolo_from_holders(holders_payload, snapshot_sec):
    holders = holders_payload.get("holders")
    if not isinstance(holders, list):
        raise LockedHistoryValidationError("invalid holder collection")
    total = 0.0
    position_count = 0
    for holder in holders:
        details = holder.get("token_details")
        if not isinstance(details, list):
            continue
        for position in details:
            end = int(position.get("end") or 0)
            amount = float(position.get("dolo") or 0)
            if not math.isfinite(amount) or amount < 0:
                raise LockedHistoryValidationError("invalid holder position amount")
            position_count += 1
            if end > snapshot_sec:
                total += amount
    if position_count == 0:
        raise LockedHistoryValidationError("holder snapshot has no token positions")
    return total


def validate_locked_history(
    flows,
    holders,
    *,
    max_absolute_gap=150_000,
    max_relative_gap=0.0025,
):
    snapshot_sec = _snapshot_seconds(holders)
    reconstructed = reconstructed_active_locked_dolo(flows, snapshot_sec)
    active_holders = active_locked_dolo_from_holders(holders, snapshot_sec)
    gap = abs(reconstructed - active_holders)
    allowed_gap = max(float(max_absolute_gap), active_holders * float(max_relative_gap))
    if gap > allowed_gap:
        raise LockedHistoryValidationError(
            "active balance mismatch: event history "
            f"{reconstructed:,.2f} DOLO vs holder positions {active_holders:,.2f} DOLO "
            f"(gap {gap:,.2f}, allowed {allowed_gap:,.2f})"
        )
    return {
        "snapshot": snapshot_sec,
        "event_active_dolo": reconstructed,
        "holder_active_dolo": active_holders,
        "gap": gap,
        "allowed_gap": allowed_gap,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flows", default="vedolo_flows.json")
    parser.add_argument("--holders", default="vedolo_holders.json")
    args = parser.parse_args()

    flows = json.loads(Path(args.flows).read_text(encoding="utf-8"))
    holders = json.loads(Path(args.holders).read_text(encoding="utf-8"))
    result = validate_locked_history(flows, holders)
    print("✅ veDOLO Locked DOLO history reconciled")
    print(f"   Event-derived active: {result['event_active_dolo']:,.2f} DOLO")
    print(f"   Holder active:        {result['holder_active_dolo']:,.2f} DOLO")
    print(f"   Gap:                  {result['gap']:,.2f} DOLO")


if __name__ == "__main__":
    main()
