#!/usr/bin/env python3
"""
Generate exercisers_by_address.json with per-address aggregation AND per-tx details
including lock duration, oDOLO amount, and price per veDOLO.

Uses incremental caching — only fetches receipts for NEW transactions since the last run.
"""

import requests
import time
import json
import os
from collections import defaultdict
from datetime import datetime

# Global timeout: abort gracefully before CI step timeout kills us
SCRIPT_START = time.time()
MAX_RUNTIME_SECONDS = 25 * 60  # 25 minutes (CI step timeout = 30 min)

def check_timeout(phase=""):
    elapsed = time.time() - SCRIPT_START
    if elapsed > MAX_RUNTIME_SECONDS:
        print(f"\n⏰ TIMEOUT after {elapsed/60:.0f} min in {phase}! Saving cache and exiting.", flush=True)
        return True
    return False

ROUTESCAN_API = "https://api.routescan.io/v2/network/mainnet/evm/80094/etherscan/api"
VESTER_CONTRACT = "0x3E9b9A16743551DA49b5e136C716bBa7932d2cEc"
USDC_E_CONTRACT = "0x549943e04f40284185054145c6e4e9568c1d3241".lower()
ODOLO_CONTRACT = "0x02e513b5b54ee216bf836ceb471507488fc89543".lower()
DOLO_CONTRACT = "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Shared single source of truth for exercise detection (both methods).
from odolo_exercises import (
    EXERCISE_METHOD_USDC as EXERCISE_METHOD_ID,
    EXERCISE_METHOD_DOLO as EXERCISE_METHOD_ID_2,
    EXERCISE_METHOD_IDS,
    extract_lock_duration_days,
    extract_lock_duration_seconds,
)
USDC_DECIMALS = 6
ODOLO_DECIMALS = 18

PAGE_SIZE = 100
RATE_LIMIT_DELAY = 0.35
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RECEIPT_CACHE_VERSION = 2
TRANSACTION_FETCH_ATTEMPTS = 3

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(DATA_DIR, "exercisers_cache.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "exercisers_by_address.json")
VEDOLO_HOLDERS_FILE = os.path.join(DATA_DIR, "vedolo_holders.json")
VEDOLO_FLOWS_FILE = os.path.join(DATA_DIR, "vedolo_flows.json")


def round_amount(value, decimals=2, tiny_decimals=6):
    """Keep dust-sized valid exercises from rounding down to zero."""
    if value is None:
        return None
    if value == 0:
        return 0
    places = tiny_decimals if abs(value) < 1 else decimals
    rounded = round(value, places)
    return rounded if rounded != 0 else value


def cache_entry_needs_receipt_refresh(entry):
    """Refresh legacy zero-USDC rows once; verified dust zeroes stay cached."""
    if not isinstance(entry, dict):
        return True
    if entry.get("paid_token") == "DOLO":
        return False
    return (
        (entry.get("usdc") is None or entry.get("usdc") == 0)
        and int(entry.get("receipt_version") or 0) < RECEIPT_CACHE_VERSION
    )


def has_valid_existing_output():
    """Return True when the last generated production output is safe to keep."""
    if not os.path.exists(OUTPUT_FILE):
        return False
    try:
        with open(OUTPUT_FILE) as f:
            data = json.load(f)
    except Exception:
        return False
    return (
        isinstance(data, dict)
        and data.get("total_addresses", 0) >= 5
        and data.get("total_exercises", 0) >= 5
        and isinstance(data.get("exercisers"), list)
        and len(data["exercisers"]) >= 5
    )


def preserve_existing_output(reason):
    """Keep the previous good output when an upstream API returns no history."""
    if has_valid_existing_output():
        print(f"  ⚠️ {reason}; keeping existing exercisers_by_address.json", flush=True)
        return True
    print(f"  ❌ {reason}; no valid existing exercisers_by_address.json to keep", flush=True)
    return False


def summarize_exercise_totals(exercisers):
    """Split true oDOLO exercises from the oDOLO/DOLO pairing method."""
    totals = {
        "total_vedolo": 0.0,
        "total_odolo_exercised": 0.0,
        "total_odolo_exercise_usdc_paid": 0.0,
        "total_odolo_exercised_exercises": 0,
        "total_dolo_pair_vedolo": 0.0,
        "total_dolo_pair_exercises": 0,
        "total_dolo_paired": 0.0,
    }
    for exerciser in exercisers:
        for tx in exerciser.get("txs", []):
            vedolo = tx.get("vedolo") or 0
            if not vedolo:
                continue
            totals["total_vedolo"] += vedolo
            if tx.get("paid_token") == "DOLO":
                totals["total_dolo_pair_vedolo"] += vedolo
                totals["total_dolo_pair_exercises"] += 1
                totals["total_dolo_paired"] += tx.get("dolo_paid") or 0
            else:
                totals["total_odolo_exercised"] += vedolo
                totals["total_odolo_exercise_usdc_paid"] += tx.get("usdc") or 0
                totals["total_odolo_exercised_exercises"] += 1
    return {
        key: round(value, 2) if isinstance(value, float) else value
        for key, value in totals.items()
    }


def load_vedolo_holder_lookup(path=VEDOLO_HOLDERS_FILE):
    """Load current veDOLO holder totals for wallet-level reconciliation."""
    if not os.path.exists(path):
        print("  ℹ️ vedolo_holders.json not found; skipping current-lock reconciliation", flush=True)
        return {}, None
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  ⚠️ failed to read vedolo_holders.json ({exc}); skipping current-lock reconciliation", flush=True)
        return {}, None

    lookup = {}
    for holder in data.get("holders", []):
        addr = str(holder.get("address") or "").lower()
        if not addr.startswith("0x"):
            continue
        token_ids = []
        for token_id in holder.get("token_ids") or []:
            try:
                token_ids.append(int(token_id))
            except (TypeError, ValueError):
                continue
        token_amounts = {}
        for detail in holder.get("token_details") or []:
            try:
                token_amounts[int(detail.get("id"))] = float(detail.get("dolo") or 0)
            except (TypeError, ValueError):
                continue
        lookup[addr] = {
            "total_dolo": float(holder.get("total_dolo") or 0),
            "nft_count": int(holder.get("nft_count") or len(token_ids)),
            "total_vote_weight": float(holder.get("total_vote_weight") or 0),
            "token_ids": token_ids,
            "token_amounts": token_amounts,
        }
    return lookup, data.get("timestamp") or data.get("updated") or data.get("updated_at")


def load_vedolo_lock_token_lookup(path=VEDOLO_FLOWS_FILE):
    """Map veDOLO lock tx hash to minted token ids for route-level reconciliation."""
    if not os.path.exists(path):
        print("  ℹ️ vedolo_flows.json not found; skipping route-level current-lock split", flush=True)
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  ⚠️ failed to read vedolo_flows.json ({exc}); skipping route-level current-lock split", flush=True)
        return {}

    lookup = defaultdict(list)
    for lock in data.get("locks", []) or []:
        tx_hash = str(lock.get("txHash") or lock.get("hash") or "").lower()
        if not tx_hash.startswith("0x"):
            continue
        try:
            token_id = int(lock.get("tokenId"))
        except (TypeError, ValueError):
            continue
        lookup[tx_hash].append(token_id)
    return {tx_hash: sorted(set(token_ids)) for tx_hash, token_ids in lookup.items()}


def holder_reconciliation_fields(address_totals, holder, txs=None):
    """Compare historical oDOLO activity with the wallet's current veDOLO position."""
    current_locked = float(holder.get("total_dolo") or holder.get("current_vedolo_locked") or 0)
    current_positions = int(holder.get("nft_count") or holder.get("current_vedolo_positions") or 0)
    current_vote_weight = float(holder.get("total_vote_weight") or holder.get("current_vedolo_vote_weight") or 0)
    usdc_exercised = float(address_totals.get("total_odolo_exercised") or 0)
    all_method_history = float(address_totals.get("total_vedolo") or 0)
    fields = {
        "current_vedolo_locked": round(current_locked, 2),
        "current_vedolo_positions": current_positions,
        "current_vedolo_vote_weight": round(current_vote_weight, 4),
        "current_locked_delta_vs_usdc_exercise": round(current_locked - usdc_exercised, 2),
        "current_locked_delta_vs_all_exercise_history": round(current_locked - all_method_history, 2),
    }
    holder_token_ids = set(holder.get("token_ids") or [])
    token_amounts = holder.get("token_amounts") or {}
    txs = txs or []
    def tx_token_ids(tx):
        for token_id in tx.get("token_ids", []) or []:
            try:
                yield int(token_id)
            except (TypeError, ValueError):
                continue
    usdc_token_ids = {
        token_id
        for tx in txs
        if tx.get("paid_token") != "DOLO"
        for token_id in tx_token_ids(tx)
    }
    pair_token_ids = {
        token_id
        for tx in txs
        if tx.get("paid_token") == "DOLO"
        for token_id in tx_token_ids(tx)
    }
    route_available = bool(holder_token_ids and (usdc_token_ids or pair_token_ids))
    fields["current_vedolo_route_breakdown_available"] = route_available
    if route_available:
        current_usdc = holder_token_ids & usdc_token_ids
        current_pair = holder_token_ids & pair_token_ids
        routed = current_usdc | current_pair
        current_other = holder_token_ids - routed
        fields.update({
            "current_usdc_exercise_positions": len(current_usdc),
            "current_usdc_exercise_locked": round(sum(token_amounts.get(token_id, 0) for token_id in current_usdc), 2),
            "current_dolo_pair_positions": len(current_pair),
            "current_dolo_pair_locked": round(sum(token_amounts.get(token_id, 0) for token_id in current_pair), 2),
            "current_other_vedolo_positions": len(current_other),
            "current_other_vedolo_locked": round(sum(token_amounts.get(token_id, 0) for token_id in current_other), 2),
            "current_exercise_positions_missing_from_holder_snapshot": len(usdc_token_ids - holder_token_ids),
        })
    return fields


def load_cache():
    """Load incremental cache: already-processed tx hashes → receipt data."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception as exc:
            print(f"⚠️ load_cache: failed to read {CACHE_FILE} ({exc}); starting full resync", flush=True)
    return {}


def seed_cache_from_existing_output():
    """Recover receipt cache entries from the last generated JSON snapshot."""
    if not os.path.exists(OUTPUT_FILE):
        return {}
    try:
        with open(OUTPUT_FILE) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"⚠️ seed_cache: failed to read {OUTPUT_FILE} ({exc}); skipping seed", flush=True)
        return {}

    exercisers = data.get("exercisers")
    if not isinstance(exercisers, list):
        return {}

    seed = {}
    for exerciser in exercisers:
        for tx in exerciser.get("txs", []):
            tx_hash = tx.get("hash")
            vedolo = tx.get("vedolo")
            if not tx_hash or vedolo is None:
                continue
            paid_token = tx.get("paid_token") or ("DOLO" if tx.get("dolo_paid") else "USDC.e")
            seed[tx_hash] = {
                "usdc": tx.get("usdc"),
                "odolo": vedolo,
                "lock_days": tx.get("lock_days"),
                "lock_seconds": tx.get("lock_seconds"),
                "dolo_paid": tx.get("dolo_paid"),
                "paid_token": paid_token,
                "receipt_version": int(tx.get("receipt_version") or 0),
            }
    return seed


def save_cache(cache):
    """Save incremental cache."""
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, CACHE_FILE)


def _get_all_transactions_once():
    all_txs = []
    page = 1
    while True:
        print(f"  Fetching page {page}...", flush=True)
        params = {
            "module": "account", "action": "txlist",
            "address": VESTER_CONTRACT,
            "startblock": 0, "endblock": 99999999,
            "page": page, "offset": PAGE_SIZE, "sort": "asc"
        }
        resp = requests.get(ROUTESCAN_API, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        if data["status"] != "1" or not data["result"]:
            break
        txs = data["result"]
        all_txs.extend(txs)
        if (page % 10 == 0):
            print(f"    Total: {len(all_txs)}")
        if len(txs) < PAGE_SIZE:
            break
        page += 1
        time.sleep(RATE_LIMIT_DELAY)
        if check_timeout("fetch-txs"):
            return None
    return all_txs


def _duplicate_transaction_hashes(transactions):
    seen = set()
    duplicates = set()
    for tx in transactions:
        tx_hash = str(tx.get("hash") or "").lower()
        if not tx_hash:
            continue
        if tx_hash in seen:
            duplicates.add(tx_hash)
        seen.add(tx_hash)
    return sorted(duplicates)


def _invalid_exercise_transaction_hashes(transactions):
    invalid = []
    for tx in transactions:
        if not (
            tx.get("methodId") in EXERCISE_METHOD_IDS
            and tx.get("isError") == "0"
            and tx.get("txreceipt_status") == "1"
        ):
            continue
        tx_hash = str(tx.get("hash") or "").lower()
        address = str(tx.get("from") or "").lower()
        try:
            timestamp = int(tx.get("timeStamp"))
        except (TypeError, ValueError):
            timestamp = 0
        if not (
            len(tx_hash) == 66
            and tx_hash.startswith("0x")
            and len(address) == 42
            and address.startswith("0x")
            and timestamp > 0
            and extract_lock_duration_days(tx)
        ):
            invalid.append(tx_hash or "<missing-hash>")
    return invalid


def get_all_transactions():
    for attempt in range(1, TRANSACTION_FETCH_ATTEMPTS + 1):
        transactions = _get_all_transactions_once()
        if transactions is None:
            return []
        duplicates = _duplicate_transaction_hashes(transactions)
        invalid_exercises = _invalid_exercise_transaction_hashes(transactions)
        if not duplicates and not invalid_exercises:
            return transactions
        issue_parts = []
        if duplicates:
            issue_parts.append(f"{len(duplicates)} duplicate transaction hash(es)")
        if invalid_exercises:
            issue_parts.append(f"{len(invalid_exercises)} invalid exercise transaction(s)")
        print(
            f"  ⚠️ Routescan pagination returned {' and '.join(issue_parts)} "
            f"on attempt {attempt}/{TRANSACTION_FETCH_ATTEMPTS}; retrying the full snapshot",
            flush=True,
        )
        if attempt < TRANSACTION_FETCH_ATTEMPTS:
            time.sleep(attempt)
    print("  ❌ Routescan pagination stayed inconsistent; refusing to build partial data", flush=True)
    return []


# Lock-duration extraction lives in odolo_exercises (shared with
# calculate_avg_lock.py; handles both exercise methods).
extract_lock_duration = extract_lock_duration_days


def get_tx_details_from_receipt(tx_hash, retries=MAX_RETRIES):
    """Get USDC.e amount AND oDOLO amount from a tx receipt, with retry."""
    params = {
        "module": "proxy",
        "action": "eth_getTransactionReceipt",
        "txhash": tx_hash
    }

    for attempt in range(retries):
        try:
            resp = requests.get(ROUTESCAN_API, params=params, timeout=REQUEST_TIMEOUT)
            data = resp.json()
            if "result" not in data or data["result"] is None:
                if attempt < retries - 1:
                    delay = (2 ** attempt)
                    time.sleep(delay)
                    continue
                return None, None, None

            usdc_amount = None
            odolo_amount = None
            dolo_amount = None  # for 0xf3621c90 variant

            for log in data["result"].get("logs", []):
                if len(log["topics"]) < 3 or log["topics"][0] != TRANSFER_TOPIC:
                    continue

                token_addr = log["address"].lower()
                from_addr = "0x" + log["topics"][1][26:].lower()
                to_addr = "0x" + log["topics"][2][26:].lower()

                # USDC.e payment: from user TO vester (original exercise)
                if token_addr == USDC_E_CONTRACT and to_addr == VESTER_CONTRACT.lower():
                    usdc_amount = int(log["data"], 16) / (10 ** USDC_DECIMALS)

                # DOLO payment: from user TO vester (newer exercise variant)
                if token_addr == DOLO_CONTRACT and to_addr == VESTER_CONTRACT.lower():
                    raw = log.get("data", "0x")
                    if len(raw) > 2:
                        dolo_amount = int(raw, 16) / (10 ** ODOLO_DECIMALS)

                # oDOLO burn: from vester TO 0x0 (burn address) during exercise
                # The burned oDOLO amount = veDOLO received (1:1)
                if token_addr == ODOLO_CONTRACT:
                    to_addr_check = "0x" + log["topics"][2][26:].lower()
                    if from_addr == VESTER_CONTRACT.lower() and to_addr_check == "0x0000000000000000000000000000000000000000":
                        raw = log.get("data", "0x")
                        if len(raw) > 2:
                            odolo_amount = int(raw, 16) / (10 ** ODOLO_DECIMALS)

                # oDOLO transfer: from user TO vester (newer variant where oDOLO is sent, not burned)
                if token_addr == ODOLO_CONTRACT and to_addr == VESTER_CONTRACT.lower() and from_addr != VESTER_CONTRACT.lower():
                    raw = log.get("data", "0x")
                    if len(raw) > 2 and odolo_amount is None:
                        odolo_amount = int(raw, 16) / (10 ** ODOLO_DECIMALS)

            return usdc_amount, odolo_amount, dolo_amount

        except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            if attempt < retries - 1:
                delay = (2 ** attempt)
                time.sleep(delay)
            else:
                print(f"    Receipt failed after {retries} retries: {tx_hash[:16]}... ({e})")
                return None, None, None

    return None, None, None


def main():
    print("=" * 60)
    print("oDOLO Exercisers — Enhanced Data Generator (Incremental)")
    print("=" * 60)

    # Load receipt cache
    cache = load_cache()
    seeded_cache = seed_cache_from_existing_output()
    seeded_count = 0
    for tx_hash, entry in seeded_cache.items():
        if tx_hash not in cache:
            cache[tx_hash] = entry
            seeded_count += 1
    if seeded_count:
        print(f"  🌱 Seeded {seeded_count} cached receipts from existing exercisers_by_address.json")
        save_cache(cache)
    cached_count = len(cache)
    if cached_count:
        print(f"  📦 Loaded {cached_count} cached tx receipts")
    holder_lookup, holder_snapshot_ts = load_vedolo_holder_lookup()
    lock_token_lookup = load_vedolo_lock_token_lookup()

    # One-time cache invalidation: evict entries with wrong DOLO-as-USDC,
    # old dust-rounded oDOLO, or an unverified zero USDC receipt.
    evicted = 0
    evict_keys = []
    for tx_hash, entry in cache.items():
        if "paid_token" not in entry:
            usdc = entry.get("usdc") or 0
            odolo = entry.get("odolo") or 0
            if usdc > 0 and odolo > 0 and abs(usdc - odolo) < 1:
                evict_keys.append(tx_hash)
        elif entry.get("odolo") == 0:
            evict_keys.append(tx_hash)
        elif cache_entry_needs_receipt_refresh(entry):
            evict_keys.append(tx_hash)
    for k in evict_keys:
        del cache[k]
        evicted += 1
    if evicted:
        print(f"  🔄 Invalidated {evicted} legacy exercise cache entries (re-fetch needed)")
        save_cache(cache)

    print("\n[1/3] Fetching Vester transactions...")
    all_txs = get_all_transactions()
    print(f"  Total: {len(all_txs)}")
    if not all_txs:
        if preserve_existing_output("Routescan returned zero Vester transactions"):
            return
        raise SystemExit(1)

    exercise_txs = [
        tx for tx in all_txs
        if tx.get("methodId") in EXERCISE_METHOD_IDS
        and tx.get("isError") == "0"
        and tx.get("txreceipt_status") == "1"
    ]
    print(f"\n[2/3] Exercise transactions: {len(exercise_txs)}")
    if not exercise_txs:
        if preserve_existing_output("Routescan returned zero exercise transactions"):
            return
        raise SystemExit(1)

    # Lock duration comes from immutable calldata and is cheaper and more
    # authoritative than the receipt cache. Recompute it on every build so
    # old 0.0-day rounding repairs itself without another receipt request.
    lock_updates = 0
    for tx in exercise_txs:
        entry = cache.get(tx.get("hash"))
        if not entry:
            continue
        lock_days = extract_lock_duration(tx)
        lock_seconds = extract_lock_duration_seconds(tx)
        if entry.get("lock_days") != lock_days or entry.get("lock_seconds") != lock_seconds:
            entry["lock_days"] = lock_days
            entry["lock_seconds"] = lock_seconds
            lock_updates += 1
    if lock_updates:
        print(f"  🔄 Recomputed {lock_updates} cached lock duration(s)")
        save_cache(cache)
    # Count by method
    m1 = sum(1 for tx in exercise_txs if tx.get('methodId') == EXERCISE_METHOD_ID)
    m2 = sum(1 for tx in exercise_txs if tx.get('methodId') == EXERCISE_METHOD_ID_2)
    print(f"  Method 0xa88f8139: {m1}, Method 0xf3621c90: {m2}")
    print(f"\n[2/3] Exercise transactions: {len(exercise_txs)}")

    # Split into cached and uncached
    uncached_txs = [tx for tx in exercise_txs if tx["hash"] not in cache]
    cached_txs = [tx for tx in exercise_txs if tx["hash"] in cache]
    print(f"  Cached: {len(cached_txs)}, Need fetch: {len(uncached_txs)}")

    print("\n[3/3] Scanning receipts for uncached transactions...")
    errors = 0
    failed_txs = []

    # Process uncached transactions (the slow part — only new ones)
    for i, tx in enumerate(uncached_txs):
        tx_hash = tx["hash"]
        usdc_amount, odolo_amount, dolo_amount = get_tx_details_from_receipt(tx_hash)
        lock_days = extract_lock_duration(tx)
        lock_seconds = extract_lock_duration_seconds(tx)

        if usdc_amount is not None or dolo_amount is not None or odolo_amount is not None:
            cache[tx_hash] = {
                "usdc": round_amount(usdc_amount) if usdc_amount is not None else None,
                "odolo": round_amount(odolo_amount) if odolo_amount is not None else None,
                "lock_days": lock_days,
                "lock_seconds": lock_seconds,
                "dolo_paid": round_amount(dolo_amount) if dolo_amount is not None else None,
                # Classify by the tx's method id (authoritative), not by which
                # transfer logs happened to be found in the receipt.
                "paid_token": "DOLO" if str(tx.get("methodId") or tx.get("input", "")[:10]) == "0xf3621c90" else "USDC.e",
                "receipt_version": RECEIPT_CACHE_VERSION,
            }
        else:
            errors += 1
            failed_txs.append(tx)

        if (i + 1) % 50 == 0 or i == len(uncached_txs) - 1:
            print(f"  [{i+1}/{len(uncached_txs)}] Fetched, Errors: {errors}", flush=True)

        if check_timeout("fetch-receipts"):
            break

        time.sleep(RATE_LIMIT_DELAY)

    # Second-pass retry for failed receipts (skip if timed out)
    if failed_txs and not check_timeout("pre-retry"):
        print(f"\n[3b/3] Retrying {len(failed_txs)} failed receipts with longer delays...", flush=True)
        recovered = 0
        for i, tx in enumerate(failed_txs):
            if check_timeout("retry-receipts"):
                break
            tx_hash = tx["hash"]
            lock_days = extract_lock_duration(tx)
            lock_seconds = extract_lock_duration_seconds(tx)

            time.sleep(1.5)
            usdc_amount, odolo_amount, dolo_amount = get_tx_details_from_receipt(tx_hash, retries=5)

            if usdc_amount is not None or dolo_amount is not None or odolo_amount is not None:
                recovered += 1
                errors -= 1
                cache[tx_hash] = {
                    "usdc": round_amount(usdc_amount) if usdc_amount is not None else None,
                    "odolo": round_amount(odolo_amount) if odolo_amount is not None else None,
                    "lock_days": lock_days,
                    "lock_seconds": lock_seconds,
                    "dolo_paid": round_amount(dolo_amount) if dolo_amount is not None else None,
                    # Classify by the tx's method id (authoritative), not by which
                    # transfer logs happened to be found in the receipt.
                    "paid_token": "DOLO" if str(tx.get("methodId") or tx.get("input", "")[:10]) == "0xf3621c90" else "USDC.e",
                    "receipt_version": RECEIPT_CACHE_VERSION,
                }

            if (i + 1) % 25 == 0 or i == len(failed_txs) - 1:
                print(f"    Retry [{i+1}/{len(failed_txs)}] Recovered: {recovered}", flush=True)

        print(f"  Recovered {recovered}/{len(failed_txs)} previously failed receipts", flush=True)

    # Save cache for next run
    save_cache(cache)
    print(f"  💾 Saved {len(cache)} cached receipts to {CACHE_FILE}")

    # Build final output from ALL exercise txs (cached + newly fetched)
    print("\n  Building final output...")
    address_data = defaultdict(lambda: {
        "total_usdc": 0, "exercises": 0, "lock_days_sum": 0,
        "lock_count": 0, "first": None, "last": None, "txs": []
    })

    for tx in exercise_txs:
        tx_hash = tx["hash"]
        if tx_hash not in cache:
            continue  # skip any that still failed

        cached = cache[tx_hash]
        addr = tx["from"].lower()
        timestamp = int(tx["timeStamp"])
        date_str = time.strftime("%Y-%m-%d", time.gmtime(timestamp))
        usdc_amount = cached.get("usdc") or 0
        odolo_amount = cached.get("odolo")
        lock_days = cached.get("lock_days")
        lock_seconds = cached.get("lock_seconds")
        dolo_paid = cached.get("dolo_paid")
        paid_token = cached.get("paid_token", "USDC.e")

        # For DOLO-based exercises: usdc=None, dolo_paid has the DOLO amount
        is_dolo_exercise = paid_token == "DOLO"

        d = address_data[addr]
        if not is_dolo_exercise:
            d["total_usdc"] += usdc_amount
        d["total_dolo_paid"] = d.get("total_dolo_paid", 0) + (dolo_paid or 0)
        d["exercises"] += 1
        if lock_days is not None:
            d["lock_days_sum"] += lock_days
            d["lock_count"] += 1
        if d["first"] is None or date_str < d["first"]:
            d["first"] = date_str
        if d["last"] is None or date_str > d["last"]:
            d["last"] = date_str

        vedolo_amount = odolo_amount if odolo_amount else None
        price_per_vedolo = None
        if not is_dolo_exercise and usdc_amount and vedolo_amount and vedolo_amount > 0:
            price_per_vedolo = round(usdc_amount / vedolo_amount, 6)

        tx_entry = {
            "hash": tx_hash,
            "date": date_str,
            "timestamp": timestamp,
            "usdc": round_amount(usdc_amount) if not is_dolo_exercise else None,
            "vedolo": round_amount(vedolo_amount) if vedolo_amount else None,
            "price": price_per_vedolo,
            "lock_days": lock_days,
            "lock_seconds": lock_seconds,
            "paid_token": paid_token,
            "receipt_version": int(cached.get("receipt_version") or 0),
        }
        if is_dolo_exercise:
            tx_entry["dolo_paid"] = round_amount(dolo_paid) if dolo_paid else None
        token_ids = lock_token_lookup.get(tx_hash.lower()) or []
        if token_ids:
            tx_entry["token_ids"] = token_ids
            if len(token_ids) == 1:
                tx_entry["token_id"] = token_ids[0]

        d["txs"].append(tx_entry)

    # Build sorted list — filter out empty txs (vedolo=None = no real exercise data)
    exercisers = []
    for addr, d in address_data.items():
        # Remove txs with no veDOLO data (failed/empty exercises)
        valid_txs = [tx for tx in d["txs"] if tx.get("vedolo") is not None and tx["vedolo"] > 0]
        if not valid_txs:
            continue  # skip addresses with no valid exercises

        avg_lock = round(d["lock_days_sum"] / d["lock_count"], 1) if d["lock_count"] > 0 else None
        address_totals = summarize_exercise_totals([{"txs": valid_txs}])
        holder_fields = holder_reconciliation_fields(address_totals, holder_lookup.get(addr, {}), valid_txs)
        exercisers.append({
            "address": addr,
            "total_usdc": round(d["total_usdc"], 2),
            "total_vedolo": address_totals["total_vedolo"],
            "total_odolo_exercised": address_totals["total_odolo_exercised"],
            "total_odolo_exercise_usdc_paid": address_totals["total_odolo_exercise_usdc_paid"],
            "total_odolo_exercised_exercises": address_totals["total_odolo_exercised_exercises"],
            "total_dolo_pair_vedolo": address_totals["total_dolo_pair_vedolo"],
            "total_dolo_pair_exercises": address_totals["total_dolo_pair_exercises"],
            "total_dolo_paired": address_totals["total_dolo_paired"],
            **holder_fields,
            "exercises": len(valid_txs),
            "avg_lock_days": avg_lock,
            "first": d["first"],
            "last": d["last"],
            "txs": valid_txs
        })

    exercisers.sort(key=lambda x: x["total_usdc"], reverse=True)

    totals = summarize_exercise_totals(exercisers)

    result = {
        "updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_addresses": len(exercisers),
        "total_usdc": round(sum(e["total_usdc"] for e in exercisers), 2),
        "total_vedolo": totals["total_vedolo"],
        "total_odolo_exercised": totals["total_odolo_exercised"],
        "total_odolo_exercise_usdc_paid": totals["total_odolo_exercise_usdc_paid"],
        "total_odolo_exercised_exercises": totals["total_odolo_exercised_exercises"],
        "total_dolo_pair_vedolo": totals["total_dolo_pair_vedolo"],
        "total_dolo_pair_exercises": totals["total_dolo_pair_exercises"],
        "total_dolo_paired": totals["total_dolo_paired"],
        "vedolo_holder_reconciliation_updated": holder_snapshot_ts,
        "holderReconciliationMethodology": "Per-address current_vedolo_locked is imported from vedolo_holders.json and kept separate from historical USDC-paid oDOLO exercise and DOLO-pair activity.",
        "exerciseMetricMethodology": "total_odolo_exercised counts USDC-paid exercises only; DOLO pairing method is tracked separately",
        "total_exercises": sum(e["exercises"] for e in exercisers),
        "exercisers": exercisers
    }

    if not exercisers:
        if preserve_existing_output("generated exerciser set was empty"):
            return
        raise SystemExit(1)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    avg_price = result["total_odolo_exercise_usdc_paid"] / result["total_odolo_exercised"] if result["total_odolo_exercised"] > 0 else 0
    print(f"\n{'=' * 60}")
    print(f"DONE!")
    print(f"  Unique addresses:  {len(exercisers)}")
    print(f"  USDC paid:         ${result['total_odolo_exercise_usdc_paid']:,.2f}")
    print(f"  oDOLO exercised:   {result['total_odolo_exercised']:,.2f} (USDC-paid only)")
    print(f"  oDOLO/DOLO paired: {result['total_dolo_pair_vedolo']:,.2f}")
    print(f"  Total veDOLO:      {result['total_vedolo']:,.2f} (all methods)")
    print(f"  Avg exercise cost: ${avg_price:.6f}")
    print(f"  Total exercises:   {result['total_exercises']}")
    print(f"  Errors:            {errors}")
    print(f"  Saved to exercisers_by_address.json")


if __name__ == "__main__":
    main()
