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


def _snapshot_block(payload, key):
    value = payload.get(key)
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LockedHistoryValidationError(f"invalid {key}")
    return value


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


def _validated_transfers(flows, target_block):
    rows = flows.get("transfers")
    total = flows.get("total_transfers")
    if not isinstance(rows, list) or isinstance(total, bool) or not isinstance(total, int):
        raise LockedHistoryValidationError("invalid transfers collection")
    if total != len(rows):
        raise LockedHistoryValidationError(
            f"transfers total mismatch: declared {total}, found {len(rows)}"
        )
    schema_version = flows.get("transfers_schema_version")
    if schema_version is None:
        # Legacy checked-in artifacts remain readable during the one-time v2
        # rebuild. Newly generated output always declares and validates v2.
        return rows
    if schema_version != 2 or not rows:
        raise LockedHistoryValidationError("invalid veDOLO transfer schema")

    seen_logs = set()
    for row in rows:
        block = _exact_positive_int(row, "block")
        timestamp = _exact_positive_int(row, "timestamp")
        token_id = _exact_positive_int(row, "tokenId")
        log_index = row.get("logIndex")
        tx_hash = str(row.get("txHash") or "").lower()
        from_address = str(row.get("from") or "").lower()
        to_address = str(row.get("to") or "").lower()
        if not timestamp:
            raise LockedHistoryValidationError("invalid transfer timestamp")
        expected_date = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
        valid_address = lambda value: (
            len(value) == 42
            and value.startswith("0x")
            and all(char in "0123456789abcdef" for char in value[2:])
        )
        if (
            not block
            or (target_block and block > target_block)
            or not token_id
            or isinstance(log_index, bool)
            or not isinstance(log_index, int)
            or log_index < 0
            or len(tx_hash) != 66
            or not tx_hash.startswith("0x")
            or any(char not in "0123456789abcdef" for char in tx_hash[2:])
            or not valid_address(from_address)
            or not valid_address(to_address)
            or str(row.get("date") or "") != expected_date
        ):
            raise LockedHistoryValidationError("invalid transfer replay evidence")
        identity = (tx_hash, log_index)
        if identity in seen_logs:
            raise LockedHistoryValidationError("duplicate transfer event identity")
        seen_logs.add(identity)
    return rows


def _exact_positive_int(row, key):
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return 0
    return value


def _finite_nonnegative(row, key):
    value = row.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LockedHistoryValidationError(f"invalid {key} value")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise LockedHistoryValidationError(f"invalid {key} value")
    return number


def _apply_locked_event(positions, event):
    row = event["row"]
    token_id = _exact_positive_int(row, "tokenId")
    if not token_id:
        raise LockedHistoryValidationError("invalid position token id")
    if event["kind"] == "unlock":
        if token_id not in positions:
            raise LockedHistoryValidationError(f"unlock source position #{token_id} is missing")
        positions.pop(token_id)
        return

    deposit_type = row.get("depositType")
    if isinstance(deposit_type, bool) or not isinstance(deposit_type, int) or deposit_type not in range(6):
        raise LockedHistoryValidationError("invalid deposit type")
    amount = _finite_nonnegative(row, "dolo")
    locktime = _exact_positive_int(row, "locktime")
    current = positions.get(token_id)

    if deposit_type in (0, 2):
        if current is None:
            raise LockedHistoryValidationError(f"increase source position #{token_id} is missing")
        positions[token_id] = {
            "amount": current["amount"] + amount,
            "end": locktime or current["end"],
        }
        return
    if deposit_type == 1:
        if current is not None:
            raise LockedHistoryValidationError(f"create target position #{token_id} already exists")
        positions[token_id] = {"amount": amount, "end": locktime}
        return
    if deposit_type == 3:
        if current is None:
            raise LockedHistoryValidationError(f"extend source position #{token_id} is missing")
        positions[token_id] = {"amount": current["amount"], "end": locktime or current["end"]}
        return
    if deposit_type == 4:
        source_token_id = _exact_positive_int(row, "sourceTokenId")
        target_token_id = _exact_positive_int(row, "targetTokenId")
        if not source_token_id or target_token_id != token_id or source_token_id == target_token_id:
            raise LockedHistoryValidationError(f"merge transition is incomplete for position #{token_id}")
        source = positions.get(source_token_id)
        target = positions.get(target_token_id)
        if source is None:
            raise LockedHistoryValidationError(f"merge source position #{source_token_id} is missing")
        if target is None:
            raise LockedHistoryValidationError(f"merge target position #{target_token_id} is missing")
        positions.pop(source_token_id)
        positions[target_token_id] = {
            "amount": target["amount"] + source["amount"],
            "end": max(target["end"], source["end"], locktime),
        }
        return

    source_token_id = _exact_positive_int(row, "sourceTokenId")
    target_token_id = _exact_positive_int(row, "targetTokenId")
    if not source_token_id or target_token_id != token_id or source_token_id == target_token_id:
        raise LockedHistoryValidationError(f"split transition is incomplete for position #{token_id}")
    source = positions.get(source_token_id)
    if source is None:
        raise LockedHistoryValidationError(f"split source position #{source_token_id} is missing")
    if target_token_id in positions:
        raise LockedHistoryValidationError(f"split target position #{target_token_id} already exists")
    # Event amounts are published to four decimals; a valid positive wei split
    # can therefore appear as 0.0000 DOLO in the generated artifact.
    if source["amount"] + 1e-6 < amount:
        raise LockedHistoryValidationError(f"split amount does not reconcile for position #{source_token_id}")
    positions[source_token_id] = {"amount": max(0.0, source["amount"] - amount), "end": source["end"]}
    positions[target_token_id] = {"amount": amount, "end": locktime or source["end"]}


def reconstructed_active_positions(flows, snapshot_sec, snapshot_block=0):
    locks = _validated_rows(flows, "locks")
    unlocks = _validated_rows(flows, "unlocks")
    if isinstance(snapshot_sec, bool) or not isinstance(snapshot_sec, int) or snapshot_sec <= 0:
        raise LockedHistoryValidationError("invalid snapshot timestamp")
    events = []
    for kind, rows in (("lock", locks), ("unlock", unlocks)):
        for row in rows:
            timestamp = _exact_positive_int(row, "timestamp")
            block = _exact_positive_int(row, "block")
            if not timestamp or not block:
                raise LockedHistoryValidationError(f"invalid {kind} event position")
            if timestamp <= snapshot_sec and (not snapshot_block or block <= snapshot_block):
                events.append({"kind": kind, "row": row, "timestamp": timestamp, "block": block})
    events.sort(key=lambda event: (event["timestamp"], event["block"], 0 if event["kind"] == "lock" else 1))

    positions = {}
    for event in events:
        _apply_locked_event(positions, event)
    return {
        token_id: position["amount"]
        for token_id, position in positions.items()
        if position["end"] > snapshot_sec
    }


def reconstructed_active_locked_dolo(flows, snapshot_sec, snapshot_block=0):
    return sum(reconstructed_active_positions(flows, snapshot_sec, snapshot_block).values())


def validate_flow_history(flows):
    """Validate one flow artifact without comparing it with holder state."""
    locks = _validated_rows(flows, "locks")
    unlocks = _validated_rows(flows, "unlocks")
    target_block = _snapshot_block(flows, "target_block")
    transfers = _validated_transfers(flows, target_block)
    rows = locks + unlocks
    if rows:
        snapshot_sec = max(_exact_positive_int(row, "timestamp") for row in rows)
        if snapshot_sec <= 0:
            raise LockedHistoryValidationError("invalid event timestamp")
        reconstructed_active_positions(flows, snapshot_sec, target_block)
    return {
        "lock_count": len(locks),
        "unlock_count": len(unlocks),
        "transfer_count": len(transfers),
        "target_block": target_block,
    }


def active_positions_from_holders(holders_payload, snapshot_sec):
    holders = holders_payload.get("holders")
    if not isinstance(holders, list):
        raise LockedHistoryValidationError("invalid holder collection")
    active = {}
    position_count = 0
    for holder in holders:
        details = holder.get("token_details")
        if not isinstance(details, list):
            continue
        for position in details:
            token_id = _exact_positive_int(position, "id")
            end = _exact_positive_int(position, "end")
            amount = _finite_nonnegative(position, "dolo")
            if not token_id or not end:
                raise LockedHistoryValidationError("invalid holder position")
            position_count += 1
            if end > snapshot_sec:
                if token_id in active:
                    raise LockedHistoryValidationError(f"duplicate holder position #{token_id}")
                active[token_id] = amount
    if position_count == 0:
        raise LockedHistoryValidationError("holder snapshot has no token positions")
    return active


def active_locked_dolo_from_holders(holders_payload, snapshot_sec):
    return sum(active_positions_from_holders(holders_payload, snapshot_sec).values())


def validate_locked_history(
    flows,
    holders,
    *,
    max_absolute_gap=1,
    max_relative_gap=0.00000001,
):
    snapshot_sec = _snapshot_seconds(holders)
    flow_block = _snapshot_block(flows, "target_block")
    holder_block = _snapshot_block(holders, "snapshot_block")
    if bool(flow_block) != bool(holder_block) or (flow_block and flow_block != holder_block):
        raise LockedHistoryValidationError(
            f"snapshot block mismatch: flows {flow_block or 'missing'}, "
            f"holders {holder_block or 'missing'}"
        )
    reconstructed_positions = reconstructed_active_positions(flows, snapshot_sec, flow_block)
    holder_positions = active_positions_from_holders(holders, snapshot_sec)
    reconstructed = sum(reconstructed_positions.values())
    active_holders = sum(holder_positions.values())
    gap = abs(reconstructed - active_holders)
    allowed_gap = max(float(max_absolute_gap), active_holders * float(max_relative_gap))
    if gap > allowed_gap:
        raise LockedHistoryValidationError(
            "active balance mismatch: event history "
            f"{reconstructed:,.2f} DOLO vs holder positions {active_holders:,.2f} DOLO "
            f"(gap {gap:,.2f}, allowed {allowed_gap:,.2f})"
        )
    event_ids = set(reconstructed_positions)
    holder_ids = set(holder_positions)
    if event_ids != holder_ids:
        missing = sorted(holder_ids - event_ids)
        extra = sorted(event_ids - holder_ids)
        raise LockedHistoryValidationError(
            "active position IDs mismatch: "
            f"{len(missing)} missing from event replay, {len(extra)} absent from holder snapshot"
        )
    for token_id in sorted(event_ids):
        event_amount = reconstructed_positions[token_id]
        holder_amount = holder_positions[token_id]
        # Flow values are stored to four decimals and holder values to two.
        if abs(event_amount - holder_amount) > 0.011:
            raise LockedHistoryValidationError(
                f"active position amount mismatch for #{token_id}: "
                f"event history {event_amount:,.4f} vs holder snapshot {holder_amount:,.4f} DOLO"
            )
    return {
        "snapshot": snapshot_sec,
        "snapshot_block": flow_block,
        "event_active_dolo": reconstructed,
        "holder_active_dolo": active_holders,
        "gap": gap,
        "allowed_gap": allowed_gap,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flows", default="vedolo_flows.json")
    parser.add_argument("--holders", default="vedolo_holders.json")
    parser.add_argument(
        "--flows-only",
        action="store_true",
        help="Validate the flow artifact without reconciling a holder snapshot.",
    )
    args = parser.parse_args()

    flows = json.loads(Path(args.flows).read_text(encoding="utf-8"))
    if args.flows_only:
        result = validate_flow_history(flows)
        print("✅ veDOLO flow history validated")
        print(f"   Locks:                {result['lock_count']:,}")
        print(f"   Unlocks:              {result['unlock_count']:,}")
        print(f"   Transfers:            {result['transfer_count']:,}")
        if result["target_block"]:
            print(f"   Target block:         {result['target_block']:,}")
        return
    holders = json.loads(Path(args.holders).read_text(encoding="utf-8"))
    result = validate_locked_history(flows, holders)
    print("✅ veDOLO Locked DOLO history reconciled")
    print(f"   Event-derived active: {result['event_active_dolo']:,.2f} DOLO")
    print(f"   Holder active:        {result['holder_active_dolo']:,.2f} DOLO")
    print(f"   Gap:                  {result['gap']:,.2f} DOLO")


if __name__ == "__main__":
    main()
