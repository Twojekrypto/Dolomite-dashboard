#!/usr/bin/env python3
"""
Fetch veDOLO early exit penalty data from on-chain Withdraw events.

For each Withdraw event, analyzes its exact receipt-log segment to calculate:
- Burn fee (5% of locked DOLO, transferred to address(0))
- Recoup fee (variable %, transferred to oDOLO vester)
- DOLO returned to user

Outputs: early_exits.json with aggregated stats + per-exit details.

Usage:
    python3 fetch_early_exits.py

The checked-in output is used as an audited incremental checkpoint. Monetary
arithmetic stays in integer wei until JSON output.
"""

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from rpc_client import RpcClient, RpcError, get_endpoints as rpc_endpoints

# ===== CONFIG =====
VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
DOLO_TOKEN = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654"
VEDOLO_DEPLOYMENT_BLOCK = 2_926_448
CACHE_SCHEMA_VERSION = 3
OUTPUT_SCHEMA_VERSION = 2
REORG_OVERLAP_BLOCKS = 1_000
WEI = 10**18
AUDITED_CHECKPOINT_BASELINE = {
    "coverage": {
        "fromBlock": 2_926_448,
        "minimumToBlock": 24_819_325,
    },
    "stats": {
        "total_withdrawals": 9_063,
        "unique_withdrawal_transactions": 9_060,
        "total_early_exits": 7_999,
        "total_burn_fee_raw": "1562201311256111315281866",
        "total_recoup_fee_raw": "13736860507157405327229275",
        "total_penalty_raw": "15299061818413516642511141",
        "total_original_locked_raw": "31244026225122226305692208",
        "total_received_raw": "15944964406708709663181067",
    },
}

# Event topics (keccak256)
WITHDRAW_TOPIC = "0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Endpoint list and single-call rotation/retry come from the shared client.
RPC_URLS = rpc_endpoints("berachain")
_RPC = RpcClient(endpoints=RPC_URLS, timeout=20)
ZERO_ADDR = "0x0000000000000000000000000000000000000000"

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "early_exits.json")
CACHE_FILE = os.path.join(DATA_DIR, "early_exits_cache.json")
MIN_CHECKPOINT_BYTES = 1_000

LOG_INITIAL_STEP = int(os.environ.get("EARLY_EXITS_LOG_INITIAL_STEP") or "50000")
TRANSFER_LOG_MAX_STEP = int(os.environ.get("EARLY_EXITS_TRANSFER_LOG_MAX_STEP") or "50000")
LOG_MIN_STEP = int(os.environ.get("EARLY_EXITS_LOG_MIN_STEP") or "250")
LOG_MIN_STEP_FAILURES = int(os.environ.get("EARLY_EXITS_LOG_MIN_STEP_FAILURES") or "3")


def rpc_call(method, params, retries=3):
    """Make an RPC call with fallback across multiple providers.
    Keeps the historical contract: returns None when all endpoints fail."""
    try:
        return _RPC.call(method, params)
    except RpcError as exc:
        print(f"  ⚠️ RPC {method} failed on all endpoints: {exc}", flush=True)
        return None


def _atomic_dump(payload, path):
    """Atomic JSON write (tmp + rename) — a kill mid-write must not leave a truncated file."""
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def _address_topic(address):
    return "0x" + ("0" * 24) + address.lower().removeprefix("0x")


def _fetch_event_logs(address, topics, start_block, label, latest_block=None, max_step=None):
    """Fetch a complete log stream with adaptive RPC windows."""
    print(f"📡 Fetching {label} events...")
    if latest_block is None:
        block_number_hex = rpc_call("eth_blockNumber", [])
        if block_number_hex is None:
            raise SystemExit("❌ eth_blockNumber failed on all RPC endpoints — aborting run")
        latest_block = int(block_number_hex, 16)
    print(f"  Latest block: {latest_block:,}")
    print(f"  Scanning from block: {start_block:,}")

    all_logs = []
    block = start_block
    max_step = max(LOG_MIN_STEP, max_step or LOG_INITIAL_STEP)
    step = max_step
    min_step_failures = 0
    small_window_successes = 0

    while block <= latest_block:
        to_block = min(block + step - 1, latest_block)

        result = rpc_call("eth_getLogs", [{
            "address": address,
            "topics": topics,
            "fromBlock": hex(block),
            "toBlock": hex(to_block)
        }])

        # None = RPC failure (≠ empty list). Abort WITHOUT advancing the
        # checkpoint — otherwise events in this window would be skipped
        # forever, silently understating early-exit stats.
        if result is None:
            if step > LOG_MIN_STEP:
                next_step = max(LOG_MIN_STEP, step // 2)
                print(
                    f"  ⚠️ Reducing {label} log window from {step:,} to {next_step:,} blocks "
                    f"after RPC failure at {block:,}-{to_block:,}",
                    flush=True,
                )
                step = next_step
                small_window_successes = 0
                time.sleep(0.5)
                continue
            min_step_failures += 1
            if min_step_failures < LOG_MIN_STEP_FAILURES:
                delay = min(8, 2 * min_step_failures)
                print(
                    f"  ⚠️ Minimum Withdraw log window ({step:,} blocks) failed at "
                    f"{block:,}-{to_block:,}; retrying in {delay}s "
                    f"({min_step_failures}/{LOG_MIN_STEP_FAILURES})",
                    flush=True,
                )
                small_window_successes = 0
                time.sleep(delay)
                continue
            raise SystemExit(
                f"❌ eth_getLogs failed for blocks {block:,}-{to_block:,} on all endpoints; "
                "aborting without advancing last_scanned_block"
            )
        min_step_failures = 0
        small_window_successes += 1
        all_logs.extend(result)
        if len(result) > 0:
            print(f"  Block {block:,}-{to_block:,}: {len(result)} events (total: {len(all_logs)})")

        block = to_block + 1
        if len(result) < 25 and step < max_step and small_window_successes >= 4:
            step = min(max_step, step * 2)
            small_window_successes = 0

        # Small delay to avoid rate limits
        if block % 500000 == 0:
            time.sleep(0.1)

    print(f"  ✅ Found {len(all_logs)} {label} events")
    return all_logs, latest_block


def fetch_withdraw_events(start_block=VEDOLO_DEPLOYMENT_BLOCK, latest_block=None):
    """Fetch veDOLO Withdraw logs from a complete or overlapping range."""
    return _fetch_event_logs(
        VEDOLO_CONTRACT,
        [WITHDRAW_TOPIC],
        start_block,
        "Withdraw",
        latest_block,
    )


def fetch_outbound_transfer_events(start_block=VEDOLO_DEPLOYMENT_BLOCK, latest_block=None):
    """Fetch every DOLO transfer emitted from veDOLO over the same range."""
    return _fetch_event_logs(
        DOLO_TOKEN,
        [TRANSFER_TOPIC, _address_topic(VEDOLO_CONTRACT)],
        start_block,
        "outbound DOLO Transfer",
        latest_block,
        TRANSFER_LOG_MAX_STEP,
    )


def decode_withdraw_event(log):
    """Decode a Withdraw event log into structured data."""
    provider = "0x" + log["topics"][1][26:]  # indexed address
    data = log.get("data", "0x")[2:]
    if len(data) < 192:
        raise ValueError("Withdraw event data is truncated")
    token_id = int(data[0:64], 16)
    value_raw = int(data[64:128], 16)
    ts = int(data[128:192], 16)
    tx_hash = str(log["transactionHash"]).lower()
    log_index = int(log["logIndex"], 16)

    return {
        "provider": provider.lower(),
        "token_id": token_id,
        "value_raw": str(value_raw),
        "timestamp": ts,
        "block": int(log["blockNumber"], 16),
        "log_index": log_index,
        "tx_hash": tx_hash,
        "event_id": f"{tx_hash}:{log_index}",
    }


def _wei_to_decimal(value_raw):
    """Return an exact, non-exponential decimal string for an integer wei value."""
    value_raw = int(value_raw)
    if value_raw < 0:
        raise ValueError("DOLO wei value cannot be negative")
    whole, fraction = divmod(value_raw, WEI)
    if not fraction:
        return str(whole)
    return f"{whole}.{fraction:018d}".rstrip("0")


def _percentage(numerator, denominator, places=6):
    if denominator <= 0:
        return "0"
    quant = Decimal(1).scaleb(-places)
    value = (Decimal(numerator) * Decimal(100) / Decimal(denominator)).quantize(
        quant, rounding=ROUND_HALF_UP
    )
    return format(value, "f").rstrip("0").rstrip(".") or "0"


def _decode_outbound_transfer(log):
    """Decode a DOLO Transfer emitted from veDOLO, or return None."""
    if not isinstance(log, dict):
        return None
    topics = log.get("topics")
    if str(log.get("address", "")).lower() != DOLO_TOKEN.lower():
        return None
    if not isinstance(topics, list) or len(topics) < 3 or topics[0].lower() != TRANSFER_TOPIC:
        return None
    from_addr = "0x" + topics[1][-40:]
    if from_addr.lower() != VEDOLO_CONTRACT.lower():
        return None
    return {
        "log_index": int(log.get("logIndex", "0x0"), 16),
        "recipient": ("0x" + topics[2][-40:]).lower(),
        "amount_raw": int(log.get("data", "0x0"), 16),
    }


def calculate_receipt_events(receipt, events):
    """Calculate exact fee components for every Withdraw event in one receipt.

    veDOLO emits its DOLO transfers immediately before each Withdraw event.  For
    a transaction containing multiple withdrawals, the preceding Withdraw log
    therefore forms the lower boundary of the next event.  The event's provider
    is the only valid user recipient; every other non-zero recipient is protocol
    recoup. This intentionally avoids a recipient allowlist that can go stale.
    """
    if not isinstance(receipt, dict) or not isinstance(receipt.get("logs"), list):
        raise ValueError("Missing transaction receipt logs")
    ordered_events = sorted(events, key=lambda event: int(event["log_index"]))
    if not ordered_events:
        raise ValueError("Receipt has no Withdraw events")
    transfers = []
    for log in receipt["logs"]:
        decoded = _decode_outbound_transfer(log)
        if decoded:
            transfers.append(decoded)
    transfers.sort(key=lambda transfer: transfer["log_index"])

    calculations = {}
    previous_withdraw_index = -1
    for event in ordered_events:
        withdraw_index = int(event["log_index"])
        segment = [
            transfer for transfer in transfers
            if previous_withdraw_index < transfer["log_index"] < withdraw_index
        ]
        provider = str(event["provider"]).lower()
        expected_received = int(event["value_raw"])
        received_raw = sum(
            transfer["amount_raw"] for transfer in segment
            if transfer["recipient"] == provider
        )
        burn_raw = sum(
            transfer["amount_raw"] for transfer in segment
            if transfer["recipient"] == ZERO_ADDR.lower()
        )
        recoup_raw = sum(
            transfer["amount_raw"] for transfer in segment
            if transfer["recipient"] not in {provider, ZERO_ADDR.lower()}
        )
        if received_raw != expected_received:
            raise ValueError(
                f"{event['event_id']}: provider received {received_raw} wei, "
                f"Withdraw event reports {expected_received} wei"
            )
        penalty_raw = burn_raw + recoup_raw
        original_raw = received_raw + penalty_raw
        calculations[event["event_id"]] = {
            "burn_fee_raw": str(burn_raw),
            "recoup_fee_raw": str(recoup_raw),
            "total_penalty_raw": str(penalty_raw),
            "original_locked_raw": str(original_raw),
            "user_received_raw": str(received_raw),
            "is_early_exit": penalty_raw > 0,
        }
        previous_withdraw_index = withdraw_index
    return calculations


def merge_event_logs(cached_logs, fresh_logs, rescan_from):
    """Replace the reorg overlap and deduplicate by tx hash + log index."""
    merged = [
        log for log in cached_logs
        if int(log.get("blockNumber", "0x0"), 16) < rescan_from
    ]
    merged.extend(fresh_logs)
    by_event = {}
    for log in merged:
        tx_hash = str(log.get("transactionHash", "")).lower()
        log_index = int(log.get("logIndex", "0x0"), 16)
        by_event[f"{tx_hash}:{log_index}"] = log
    return sorted(
        by_event.values(),
        key=lambda log: (
            int(log.get("blockNumber", "0x0"), 16),
            int(log.get("transactionIndex", "0x0"), 16),
            int(log.get("logIndex", "0x0"), 16),
        ),
    )


def load_cache(path=CACHE_FILE):
    """Load only a cache proving both log streams cover the deployment block."""
    if not os.path.exists(path):
        return [], [], VEDOLO_DEPLOYMENT_BLOCK
    try:
        with open(path) as handle:
            payload = json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  ⚠️ Cache file unusable ({exc}); forcing full history scan")
        return [], [], VEDOLO_DEPLOYMENT_BLOCK
    meta = payload.get("_meta", {}) if isinstance(payload, dict) else {}
    if (
        meta.get("schema_version") != CACHE_SCHEMA_VERSION
        or meta.get("complete_from_block") != VEDOLO_DEPLOYMENT_BLOCK
    ):
        print("  ⚠️ Legacy or incomplete cache rejected; forcing full history scan")
        return [], [], VEDOLO_DEPLOYMENT_BLOCK
    withdraw_logs = payload.get("withdraw_logs")
    transfer_logs = payload.get("transfer_logs")
    last_scanned = int(meta.get("last_scanned_block", VEDOLO_DEPLOYMENT_BLOCK))
    if not isinstance(withdraw_logs, list) or not isinstance(transfer_logs, list):
        print("  ⚠️ Invalid cache shape; forcing full history scan")
        return [], [], VEDOLO_DEPLOYMENT_BLOCK
    rescan_from = max(VEDOLO_DEPLOYMENT_BLOCK, last_scanned - REORG_OVERLAP_BLOCKS)
    return withdraw_logs, transfer_logs, rescan_from


def _cache_payload(withdraw_logs, transfer_logs, latest_block):
    return {
        "_meta": {
            "schema_version": CACHE_SCHEMA_VERSION,
            "complete_from_block": VEDOLO_DEPLOYMENT_BLOCK,
            "last_scanned_block": latest_block,
        },
        "withdraw_logs": withdraw_logs,
        "transfer_logs": transfer_logs,
    }


def build_output(events, calculations, latest_block, updated_at=None):
    """Build the checked-in artifact from exact event-level calculations."""
    if len(calculations) != len(events):
        missing = [event["event_id"] for event in events if event["event_id"] not in calculations]
        raise ValueError(f"Missing calculations for {len(missing)} Withdraw events")
    early_exits = []
    normal_count = 0
    total_burn_raw = 0
    total_recoup_raw = 0
    total_penalty_raw = 0
    total_original_raw = 0
    total_received_raw = 0
    tx_hashes = set()
    for event in events:
        calculation = calculations[event["event_id"]]
        tx_hashes.add(event["tx_hash"])
        if not calculation.get("is_early_exit"):
            normal_count += 1
            continue
        burn_raw = int(calculation["burn_fee_raw"])
        recoup_raw = int(calculation["recoup_fee_raw"])
        penalty_raw = int(calculation["total_penalty_raw"])
        original_raw = int(calculation["original_locked_raw"])
        received_raw = int(calculation["user_received_raw"])
        total_burn_raw += burn_raw
        total_recoup_raw += recoup_raw
        total_penalty_raw += penalty_raw
        total_original_raw += original_raw
        total_received_raw += received_raw
        early_exits.append({
            "event_id": event["event_id"],
            "address": event["provider"],
            "tx_hash": event["tx_hash"],
            "log_index": event["log_index"],
            "block": event["block"],
            "token_id": event["token_id"],
            "timestamp": event["timestamp"],
            "date": datetime.fromtimestamp(event["timestamp"], timezone.utc).strftime("%Y-%m-%d"),
            "original_locked": _wei_to_decimal(original_raw),
            "original_locked_raw": str(original_raw),
            "total_penalty": _wei_to_decimal(penalty_raw),
            "total_penalty_raw": str(penalty_raw),
            "penalty_pct": _percentage(penalty_raw, original_raw),
            "burn_fee": _wei_to_decimal(burn_raw),
            "burn_fee_raw": str(burn_raw),
            "recoup_fee": _wei_to_decimal(recoup_raw),
            "recoup_fee_raw": str(recoup_raw),
            "user_received": _wei_to_decimal(received_raw),
            "user_received_raw": str(received_raw),
        })
    early_exits.sort(key=lambda row: (row["timestamp"], row["log_index"]), reverse=True)
    updated_at = updated_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stats = {
        "total_early_exits": len(early_exits),
        "total_normal_exits": normal_count,
        "total_withdrawals": len(events),
        "unique_withdrawal_transactions": len(tx_hashes),
        "total_burn_fee_dolo": _wei_to_decimal(total_burn_raw),
        "total_burn_fee_raw": str(total_burn_raw),
        "total_recoup_fee_dolo": _wei_to_decimal(total_recoup_raw),
        "total_recoup_fee_raw": str(total_recoup_raw),
        "total_penalty_dolo": _wei_to_decimal(total_penalty_raw),
        "total_penalty_raw": str(total_penalty_raw),
        "total_original_locked": _wei_to_decimal(total_original_raw),
        "total_original_locked_raw": str(total_original_raw),
        "total_received_dolo": _wei_to_decimal(total_received_raw),
        "total_received_raw": str(total_received_raw),
        "avg_penalty_pct": _percentage(total_penalty_raw, total_original_raw),
        "last_updated": updated_at,
    }
    return {
        "schemaVersion": OUTPUT_SCHEMA_VERSION,
        "coverage": {
            "complete": True,
            "fromBlock": VEDOLO_DEPLOYMENT_BLOCK,
            "toBlock": latest_block,
            "eventCount": len(events),
            "uniqueTransactionCount": len(tx_hashes),
        },
        "stats": stats,
        "recent_exits": early_exits,
    }


def _checkpoint_error(message):
    raise ValueError(f"invalid early-exits checkpoint: {message}")


def _checkpoint_int(value, field):
    if isinstance(value, bool):
        _checkpoint_error(f"{field} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        _checkpoint_error(f"{field} must be an integer")
    if str(value).strip() != str(parsed):
        _checkpoint_error(f"{field} must be an integer")
    if parsed < 0:
        _checkpoint_error(f"{field} cannot be negative")
    return parsed


def _strict_checkpoint_int(value, field):
    if type(value) is not int or value < 0:
        _checkpoint_error(f"{field} must be a strict non-negative integer")
    return value


def _checkpoint_raw(value, field):
    if not isinstance(value, str) or not value.isascii() or not value.isdigit():
        _checkpoint_error(f"{field} must be an exact digit string")
    return int(value)


def load_checkpoint(path=None):
    """Load the audited checkpoint, rejecting truncated/partial artifacts first."""
    path = OUTPUT_FILE if path is None else path
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        raise ValueError(f"checkpoint cannot be read: {exc}") from exc
    if size < MIN_CHECKPOINT_BYTES:
        raise ValueError(f"checkpoint is too small ({size} bytes; minimum {MIN_CHECKPOINT_BYTES})")
    try:
        with open(path) as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"checkpoint cannot be parsed: {exc}") from exc


def _validate_checkpoint(previous):
    if not isinstance(previous, dict) or previous.get("schemaVersion") != OUTPUT_SCHEMA_VERSION:
        _checkpoint_error("schemaVersion must be 2")
    coverage = previous.get("coverage")
    stats = previous.get("stats")
    early_rows = previous.get("recent_exits")
    if not isinstance(coverage, dict) or coverage.get("complete") is not True:
        _checkpoint_error("coverage must be complete")
    if (
        type(coverage.get("fromBlock")) is not int
        or coverage.get("fromBlock") != AUDITED_CHECKPOINT_BASELINE["coverage"]["fromBlock"]
    ):
        _checkpoint_error("coverage must start at the deployment block")
    to_block = _strict_checkpoint_int(coverage.get("toBlock"), "coverage.toBlock")
    if not isinstance(stats, dict) or not isinstance(early_rows, list):
        _checkpoint_error("stats and recent_exits are required")

    count_fields = (
        "total_early_exits",
        "total_normal_exits",
        "total_withdrawals",
        "unique_withdrawal_transactions",
    )
    counts = {field: _strict_checkpoint_int(stats.get(field), f"stats.{field}") for field in count_fields}
    if counts["total_early_exits"] + counts["total_normal_exits"] != counts["total_withdrawals"]:
        _checkpoint_error("withdrawal counts do not reconcile")
    event_count = _strict_checkpoint_int(coverage.get("eventCount"), "coverage.eventCount")
    unique_transaction_count = _strict_checkpoint_int(
        coverage.get("uniqueTransactionCount"),
        "coverage.uniqueTransactionCount",
    )
    if event_count != counts["total_withdrawals"]:
        _checkpoint_error("coverage.eventCount does not match total withdrawals")
    if unique_transaction_count != counts["unique_withdrawal_transactions"]:
        _checkpoint_error("coverage.uniqueTransactionCount does not match unique transactions")
    if len(early_rows) != counts["total_early_exits"]:
        _checkpoint_error("early-exit row count does not match total early exits")

    raw_fields = (
        "total_burn_fee_raw",
        "total_recoup_fee_raw",
        "total_penalty_raw",
        "total_original_locked_raw",
        "total_received_raw",
    )
    totals = {field: _checkpoint_raw(stats.get(field), f"stats.{field}") for field in raw_fields}
    if totals["total_burn_fee_raw"] + totals["total_recoup_fee_raw"] != totals["total_penalty_raw"]:
        _checkpoint_error("penalty totals do not reconcile")
    if totals["total_penalty_raw"] + totals["total_received_raw"] != totals["total_original_locked_raw"]:
        _checkpoint_error("locked and received totals do not reconcile")

    if to_block < AUDITED_CHECKPOINT_BASELINE["coverage"]["minimumToBlock"]:
        _checkpoint_error("coverage.toBlock is below the audited baseline")
    for field in (
        "total_withdrawals",
        "unique_withdrawal_transactions",
        "total_early_exits",
    ):
        if counts[field] < AUDITED_CHECKPOINT_BASELINE["stats"][field]:
            _checkpoint_error(f"stats.{field} is below the audited baseline")
    for field in raw_fields:
        if totals[field] < int(AUDITED_CHECKPOINT_BASELINE["stats"][field]):
            _checkpoint_error(f"stats.{field} is below the audited baseline")

    event_ids = set()
    row_totals = {field: 0 for field in raw_fields}
    for row in early_rows:
        if not isinstance(row, dict) or not row.get("event_id"):
            _checkpoint_error("every early-exit row requires an event_id")
        event_id = str(row["event_id"])
        if event_id in event_ids:
            _checkpoint_error("early-exit event IDs must be unique")
        event_ids.add(event_id)
        row_values = {
            "total_burn_fee_raw": _checkpoint_raw(row.get("burn_fee_raw"), f"{event_id}.burn_fee_raw"),
            "total_recoup_fee_raw": _checkpoint_raw(row.get("recoup_fee_raw"), f"{event_id}.recoup_fee_raw"),
            "total_penalty_raw": _checkpoint_raw(row.get("total_penalty_raw"), f"{event_id}.total_penalty_raw"),
            "total_original_locked_raw": _checkpoint_raw(row.get("original_locked_raw"), f"{event_id}.original_locked_raw"),
            "total_received_raw": _checkpoint_raw(row.get("user_received_raw"), f"{event_id}.user_received_raw"),
        }
        if row_values["total_burn_fee_raw"] + row_values["total_recoup_fee_raw"] != row_values["total_penalty_raw"]:
            _checkpoint_error(f"{event_id} penalty components do not reconcile")
        if row_values["total_penalty_raw"] + row_values["total_received_raw"] != row_values["total_original_locked_raw"]:
            _checkpoint_error(f"{event_id} locked and received values do not reconcile")
        for field, value in row_values.items():
            row_totals[field] += value
    if row_totals != totals:
        _checkpoint_error("raw aggregates do not match early-exit rows")
    return {"to_block": to_block, "counts": counts, "totals": totals, "event_ids": event_ids}


def _fresh_calculation(calculation, event_id):
    if not isinstance(calculation, dict):
        _checkpoint_error(f"missing calculation for {event_id}")
    if not isinstance(calculation.get("is_early_exit"), bool):
        _checkpoint_error(f"{event_id}.is_early_exit must be a boolean")
    values = {}
    for key in ("burn_fee_raw", "recoup_fee_raw", "total_penalty_raw", "original_locked_raw", "user_received_raw"):
        values[key] = _checkpoint_raw(calculation.get(key), f"{event_id}.{key}")
    if values["burn_fee_raw"] + values["recoup_fee_raw"] != values["total_penalty_raw"]:
        _checkpoint_error(f"{event_id} penalty components do not reconcile")
    if values["total_penalty_raw"] + values["user_received_raw"] != values["original_locked_raw"]:
        _checkpoint_error(f"{event_id} locked and received values do not reconcile")
    values["is_early_exit"] = calculation["is_early_exit"]
    return values


def _build_early_exit_row(event, values):
    original_raw = values["original_locked_raw"]
    penalty_raw = values["total_penalty_raw"]
    burn_raw = values["burn_fee_raw"]
    recoup_raw = values["recoup_fee_raw"]
    received_raw = values["user_received_raw"]
    return {
        "event_id": event["event_id"],
        "address": event["provider"],
        "tx_hash": event["tx_hash"],
        "log_index": event["log_index"],
        "block": event["block"],
        "token_id": event["token_id"],
        "timestamp": event["timestamp"],
        "date": datetime.fromtimestamp(event["timestamp"], timezone.utc).strftime("%Y-%m-%d"),
        "original_locked": _wei_to_decimal(original_raw),
        "original_locked_raw": str(original_raw),
        "total_penalty": _wei_to_decimal(penalty_raw),
        "total_penalty_raw": str(penalty_raw),
        "penalty_pct": _percentage(penalty_raw, original_raw),
        "burn_fee": _wei_to_decimal(burn_raw),
        "burn_fee_raw": str(burn_raw),
        "recoup_fee": _wei_to_decimal(recoup_raw),
        "recoup_fee_raw": str(recoup_raw),
        "user_received": _wei_to_decimal(received_raw),
        "user_received_raw": str(received_raw),
    }


def merge_incremental_output(previous, fresh_events, fresh_calculations, latest_block, updated_at=None):
    """Merge only Withdraw events after a validated audited checkpoint."""
    checkpoint = _validate_checkpoint(previous)
    latest_block = _checkpoint_int(latest_block, "latest_block")
    if latest_block < checkpoint["to_block"]:
        _checkpoint_error("latest block regresses before checkpoint coverage")
    if not isinstance(fresh_events, list) or not isinstance(fresh_calculations, dict):
        raise ValueError("invalid fresh early-exits data")

    previous_rows = list(previous["recent_exits"])
    fresh_ids = set()
    fresh_rows = []
    fresh_early_count = 0
    fresh_normal_count = 0
    fresh_totals = {
        "total_burn_fee_raw": 0,
        "total_recoup_fee_raw": 0,
        "total_penalty_raw": 0,
        "total_original_locked_raw": 0,
        "total_received_raw": 0,
    }
    fresh_tx_hashes = set()
    for event in fresh_events:
        if not isinstance(event, dict) or not event.get("event_id"):
            raise ValueError("invalid fresh early-exits data: every event requires an event_id")
        event_id = str(event["event_id"])
        if event_id in fresh_ids or event_id in checkpoint["event_ids"]:
            raise ValueError(f"invalid fresh early-exits data: duplicate event_id {event_id}")
        fresh_ids.add(event_id)
        try:
            block = int(event["block"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"invalid fresh early-exits data: {event_id} has no valid block")
        if block <= checkpoint["to_block"]:
            raise ValueError(f"invalid fresh early-exits data: {event_id} is not after checkpoint coverage")
        if block > latest_block:
            raise ValueError(f"invalid fresh early-exits data: {event_id} is after latest_block")
        if not event.get("tx_hash"):
            raise ValueError(f"invalid fresh early-exits data: {event_id} has no transaction hash")
        calculation = _fresh_calculation(fresh_calculations.get(event_id), event_id)
        fresh_tx_hashes.add(str(event["tx_hash"]).lower())
        if calculation.get("is_early_exit", False):
            fresh_early_count += 1
            for key, total_key in (
                ("burn_fee_raw", "total_burn_fee_raw"),
                ("recoup_fee_raw", "total_recoup_fee_raw"),
                ("total_penalty_raw", "total_penalty_raw"),
                ("original_locked_raw", "total_original_locked_raw"),
                ("user_received_raw", "total_received_raw"),
            ):
                fresh_totals[total_key] += calculation[key]
            fresh_rows.append(_build_early_exit_row(event, calculation))
        else:
            fresh_normal_count += 1
    if set(fresh_calculations) != fresh_ids:
        raise ValueError("invalid fresh early-exits data: calculations do not match events")

    counts = checkpoint["counts"]
    totals = checkpoint["totals"]
    merged_rows = previous_rows + fresh_rows
    merged_rows.sort(key=lambda row: (row["timestamp"], row["log_index"]), reverse=True)
    merged_counts = {
        "total_early_exits": counts["total_early_exits"] + fresh_early_count,
        "total_normal_exits": counts["total_normal_exits"] + fresh_normal_count,
        "total_withdrawals": counts["total_withdrawals"] + len(fresh_events),
        "unique_withdrawal_transactions": counts["unique_withdrawal_transactions"] + len(fresh_tx_hashes),
    }
    merged_totals = {field: totals[field] + fresh_totals[field] for field in totals}
    updated_at = updated_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stats = {
        **{field: merged_counts[field] for field in ("total_early_exits", "total_normal_exits", "total_withdrawals", "unique_withdrawal_transactions")},
        "total_burn_fee_dolo": _wei_to_decimal(merged_totals["total_burn_fee_raw"]),
        "total_burn_fee_raw": str(merged_totals["total_burn_fee_raw"]),
        "total_recoup_fee_dolo": _wei_to_decimal(merged_totals["total_recoup_fee_raw"]),
        "total_recoup_fee_raw": str(merged_totals["total_recoup_fee_raw"]),
        "total_penalty_dolo": _wei_to_decimal(merged_totals["total_penalty_raw"]),
        "total_penalty_raw": str(merged_totals["total_penalty_raw"]),
        "total_original_locked": _wei_to_decimal(merged_totals["total_original_locked_raw"]),
        "total_original_locked_raw": str(merged_totals["total_original_locked_raw"]),
        "total_received_dolo": _wei_to_decimal(merged_totals["total_received_raw"]),
        "total_received_raw": str(merged_totals["total_received_raw"]),
        "avg_penalty_pct": _percentage(merged_totals["total_penalty_raw"], merged_totals["total_original_locked_raw"]),
        "last_updated": updated_at,
    }
    return {
        "schemaVersion": OUTPUT_SCHEMA_VERSION,
        "coverage": {
            "complete": True,
            "fromBlock": VEDOLO_DEPLOYMENT_BLOCK,
            "toBlock": latest_block,
            "eventCount": merged_counts["total_withdrawals"],
            "uniqueTransactionCount": merged_counts["unique_withdrawal_transactions"],
        },
        "stats": stats,
        "recent_exits": merged_rows,
    }


def main():
    print("=" * 60)
    print("🔄 veDOLO Early Exit Penalty — Data Fetcher")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    try:
        previous = load_checkpoint()
        checkpoint = _validate_checkpoint(previous)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"❌ Cannot load audited early_exits.json checkpoint — preserving previous artifact: {exc}")

    start_block = checkpoint["to_block"] + 1
    print(f"  📦 Loaded audited checkpoint through block {checkpoint['to_block']:,}")
    fresh_withdraws, latest_block = fetch_withdraw_events(start_block=start_block)
    if not fresh_withdraws:
        output = merge_incremental_output(previous, [], {}, latest_block)
        _atomic_dump(output, OUTPUT_FILE)
        print(f"\n💾 Refreshed early_exits.json coverage through block {latest_block:,}")
        return

    fresh_transfers, _ = fetch_outbound_transfer_events(
        start_block=start_block,
        latest_block=latest_block,
    )
    if not fresh_transfers:
        raise SystemExit("❌ No outbound DOLO transfers found for fresh Withdraw events — preserving previous artifact")
    events = [decode_withdraw_event(log) for log in fresh_withdraws]
    events_by_tx = {}
    for event in events:
        events_by_tx.setdefault(event["tx_hash"], []).append(event)
    transfers_by_tx = {}
    for transfer in fresh_transfers:
        tx_hash = str(transfer.get("transactionHash", "")).lower()
        transfers_by_tx.setdefault(tx_hash, []).append(transfer)
    print(
        f"\n💰 Reconciling {len(events):,} Withdraw events against "
        f"{len(fresh_transfers):,} outbound DOLO transfers"
    )
    calculations = {}
    errors = []
    for tx_hash, tx_events in events_by_tx.items():
        try:
            calculations.update(
                calculate_receipt_events(
                    {"logs": transfers_by_tx.get(tx_hash, [])},
                    tx_events,
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"{tx_hash}: {exc}")
    if errors:
        for error in errors[:10]:
            print(f"  ⚠️ {error}")
        raise SystemExit(
            f"❌ {len(errors)} event reconciliations failed — preserving previous artifact"
        )
    event_ids = {event["event_id"] for event in events}
    missing = event_ids - calculations.keys()
    if missing:
        raise SystemExit(
            f"❌ {len(missing)} Withdraw events have no exact calculation — preserving previous artifact"
        )
    output = merge_incremental_output(previous, events, calculations, latest_block)
    _atomic_dump(output, OUTPUT_FILE)
    print(f"\n💾 Saved early_exits.json ({os.path.getsize(OUTPUT_FILE) / 1024:.0f} KB)")
    stats = output["stats"]
    print(f"   Event withdrawals: {stats['total_withdrawals']:,}")
    print(f"   Unique transactions: {stats['unique_withdrawal_transactions']:,}")
    print(f"   Early exits: {stats['total_early_exits']:,}")
    print(f"   Normal exits: {stats['total_normal_exits']:,}")
    print(f"   Burn fee: {stats['total_burn_fee_dolo']} DOLO")
    print(f"   Recoup fee: {stats['total_recoup_fee_dolo']} DOLO")
    print(f"   Total penalty: {stats['total_penalty_dolo']} DOLO")
    print(f"   Average penalty: {stats['avg_penalty_pct']}%")
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
