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
EXERCISE_METHOD_ID = "0xa88f8139"
EXERCISE_METHOD_ID_2 = "0xf3621c90"  # newer exercise variant (DOLO-based, no USDC.e)
EXERCISE_METHOD_IDS = {EXERCISE_METHOD_ID, EXERCISE_METHOD_ID_2}
USDC_DECIMALS = 6
ODOLO_DECIMALS = 18

PAGE_SIZE = 100
RATE_LIMIT_DELAY = 0.35
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(DATA_DIR, "exercisers_cache.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "exercisers_by_address.json")


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


def load_cache():
    """Load incremental cache: already-processed tx hashes → receipt data."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cache(cache):
    """Save incremental cache."""
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, CACHE_FILE)


def get_all_transactions():
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
            break
    return all_txs


def extract_lock_duration(tx):
    """Extract lock duration in days from tx input data."""
    inp = tx["input"]
    method_id = tx.get("methodId", inp[:10])
    params_hex = inp[10:]

    if method_id == EXERCISE_METHOD_ID_2:
        # 0xf3621c90: param[0] = lock_duration in SECONDS (e.g. 604800 = 7 days)
        if len(params_hex) < 64:
            return None
        duration_seconds = int(params_hex[0:64], 16)
        if duration_seconds <= 0 or duration_seconds > 3 * 365 * 86400:
            return None
        return round(duration_seconds / 86400, 1)
    else:
        # 0xa88f8139: param[2] = lock_end timestamp
        if len(params_hex) < 3 * 64:
            return None
        lock_end = int(params_hex[2*64:3*64], 16)
        tx_time = int(tx["timeStamp"])
        duration_seconds = lock_end - tx_time
        if duration_seconds <= 0 or duration_seconds > 3 * 365 * 86400:
            return None
        return round(duration_seconds / 86400, 1)


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
                return None, None

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
    cached_count = len(cache)
    if cached_count:
        print(f"  📦 Loaded {cached_count} cached tx receipts")

    # One-time cache invalidation: evict entries with wrong DOLO-as-USDC data
    evicted = 0
    evict_keys = []
    for tx_hash, entry in cache.items():
        if "paid_token" not in entry:
            usdc = entry.get("usdc") or 0
            odolo = entry.get("odolo") or 0
            if usdc > 0 and odolo > 0 and abs(usdc - odolo) < 1:
                evict_keys.append(tx_hash)
    for k in evict_keys:
        del cache[k]
        evicted += 1
    if evicted:
        print(f"  🔄 Invalidated {evicted} old DOLO exercise cache entries (re-fetch needed)")
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

        if usdc_amount is not None or dolo_amount is not None or odolo_amount is not None:
            cache[tx_hash] = {
                "usdc": round(usdc_amount, 2) if usdc_amount else None,
                "odolo": round(odolo_amount, 2) if odolo_amount else None,
                "lock_days": lock_days,
                "dolo_paid": round(dolo_amount, 2) if dolo_amount else None,
                "paid_token": "DOLO" if (dolo_amount and not usdc_amount) else "USDC.e",
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

            time.sleep(1.5)
            usdc_amount, odolo_amount, dolo_amount = get_tx_details_from_receipt(tx_hash, retries=5)

            if usdc_amount is not None or dolo_amount is not None or odolo_amount is not None:
                recovered += 1
                errors -= 1
                cache[tx_hash] = {
                    "usdc": round(usdc_amount, 2) if usdc_amount else None,
                    "odolo": round(odolo_amount, 2) if odolo_amount else None,
                    "lock_days": lock_days,
                    "dolo_paid": round(dolo_amount, 2) if dolo_amount else None,
                    "paid_token": "DOLO" if (dolo_amount and not usdc_amount) else "USDC.e",
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
            "usdc": round(usdc_amount, 2) if not is_dolo_exercise else None,
            "vedolo": round(vedolo_amount, 2) if vedolo_amount else None,
            "price": price_per_vedolo,
            "lock_days": lock_days,
            "paid_token": paid_token,
        }
        if is_dolo_exercise:
            tx_entry["dolo_paid"] = round(dolo_paid, 2) if dolo_paid else None

        d["txs"].append(tx_entry)

    # Build sorted list — filter out empty txs (vedolo=None = no real exercise data)
    exercisers = []
    for addr, d in address_data.items():
        # Remove txs with no veDOLO data (failed/empty exercises)
        valid_txs = [tx for tx in d["txs"] if tx.get("vedolo") is not None and tx["vedolo"] > 0]
        if not valid_txs:
            continue  # skip addresses with no valid exercises

        avg_lock = round(d["lock_days_sum"] / d["lock_count"], 1) if d["lock_count"] > 0 else None
        exercisers.append({
            "address": addr,
            "total_usdc": round(d["total_usdc"], 2),
            "exercises": len(valid_txs),
            "avg_lock_days": avg_lock,
            "first": d["first"],
            "last": d["last"],
            "txs": valid_txs
        })

    exercisers.sort(key=lambda x: x["total_usdc"], reverse=True)

    # Calculate total veDOLO from all tx-level data (same source as USDC)
    total_vedolo = 0
    for e in exercisers:
        for tx in e.get("txs", []):
            if tx.get("vedolo"):
                total_vedolo += tx["vedolo"]

    result = {
        "updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_addresses": len(exercisers),
        "total_usdc": round(sum(e["total_usdc"] for e in exercisers), 2),
        "total_vedolo": round(total_vedolo, 2),
        "total_exercises": sum(e["exercises"] for e in exercisers),
        "exercisers": exercisers
    }

    if not exercisers:
        if preserve_existing_output("generated exerciser set was empty"):
            return
        raise SystemExit(1)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    avg_price = result["total_usdc"] / total_vedolo if total_vedolo > 0 else 0
    print(f"\n{'=' * 60}")
    print(f"DONE!")
    print(f"  Unique addresses:  {len(exercisers)}")
    print(f"  Total USDC.e:      ${result['total_usdc']:,.2f}")
    print(f"  Total veDOLO:      {result['total_vedolo']:,.2f}")
    print(f"  Avg veDOLO price:  ${avg_price:.6f}")
    print(f"  Total exercises:   {result['total_exercises']}")
    print(f"  Errors:            {errors}")
    print(f"  Saved to exercisers_by_address.json")


if __name__ == "__main__":
    main()
