#!/usr/bin/env python3
"""
Single source of truth for oDOLO exercise detection on the Pair Vester.

The vester (0x3E9b9A16743551DA49b5e136C716bBa7932d2cEc) has TWO exercise
methods; both lock DOLO into veDOLO and both count as exercises:

- 0xa88f8139 — paid in USDC.e; calldata param[2] is the ABSOLUTE lock_end
  timestamp, so duration = lock_end - tx.timeStamp.
- 0xf3621c90 — paid in DOLO; calldata param[0] is the lock DURATION in
  seconds (verified on-chain 2026-06-12: e.g. 604800 = 7 days).

Consumers: generate_exercisers.py, calculate_avg_lock.py,
update_exercised_usd.py. Keep filters consistent across all three —
historically they disagreed and the page showed three different
"exercise" counts.
"""

EXERCISE_METHOD_USDC = "0xa88f8139"
EXERCISE_METHOD_DOLO = "0xf3621c90"
EXERCISE_METHOD_IDS = {EXERCISE_METHOD_USDC, EXERCISE_METHOD_DOLO}

MAX_LOCK_SECONDS = 3 * 365 * 86400  # sanity: max 3 years


def tx_method_id(tx):
    return str(tx.get("methodId") or str(tx.get("input") or "")[:10]).lower()


def is_exercise_tx(tx):
    """Successful exercise via either method."""
    return (
        tx_method_id(tx) in EXERCISE_METHOD_IDS
        and tx.get("isError") == "0"
        and tx.get("txreceipt_status") == "1"
    )


def extract_lock_duration_seconds(tx):
    """Lock duration in seconds for either exercise method, or None."""
    inp = str(tx.get("input") or "")
    params_hex = inp[10:]
    method_id = tx_method_id(tx)
    try:
        if method_id == EXERCISE_METHOD_DOLO:
            if len(params_hex) < 64:
                return None
            duration_seconds = int(params_hex[0:64], 16)
        elif method_id == EXERCISE_METHOD_USDC:
            if len(params_hex) < 3 * 64:
                return None
            lock_end = int(params_hex[2 * 64:3 * 64], 16)
            duration_seconds = lock_end - int(tx["timeStamp"])
        else:
            return None
    except (KeyError, TypeError, ValueError):
        return None
    if duration_seconds <= 0 or duration_seconds > MAX_LOCK_SECONDS:
        return None
    return duration_seconds


def extract_lock_duration_days(tx):
    seconds = extract_lock_duration_seconds(tx)
    if seconds is None:
        return None
    days = seconds / 86400
    return round(days, 4) if days < 1 else round(days, 1)
