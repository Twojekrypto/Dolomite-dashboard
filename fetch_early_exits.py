#!/usr/bin/env python3
"""
Fetch veDOLO early exit penalty data from on-chain Withdraw events.

For each Withdraw event, analyzes the transaction receipt to calculate:
- Burn fee (5% of locked DOLO, transferred to address(0))
- Recoup fee (variable %, transferred to oDOLO vester)
- DOLO returned to user

Outputs: early_exits.json with aggregated stats + per-exit details.

Usage:
    python3 fetch_early_exits.py

Requires BERASCAN_API_KEY environment variable.
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== CONFIG =====
VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
DOLO_TOKEN = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654"
ODOLO_VESTER = "0x3E9b9A16743551DA49b5e136C716bBa7932d2cEc"

# Event topics (keccak256)
WITHDRAW_TOPIC = "0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
CHAIN_ID = 80094  # Berachain
ALCHEMY_RPC = os.environ.get("ALCHEMY_BERACHAIN_RPC", "")
ALCHEMY_RPC_2 = os.environ.get("ALCHEMY_BERACHAIN_RPC_2", "")
RPC_URLS = [
    *([] if not ALCHEMY_RPC else [ALCHEMY_RPC]),
    *([] if not ALCHEMY_RPC_2 else [ALCHEMY_RPC_2]),
    "https://berachain-rpc.publicnode.com/",
    "https://berachain.drpc.org/",
    "https://rpc.berachain.com/",
]
ZERO_ADDR = "0x0000000000000000000000000000000000000000"

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "early_exits.json")
CACHE_FILE = os.path.join(DATA_DIR, "early_exits_cache.json")

API_KEY = os.environ.get("BERASCAN_API_KEY", "")


def rpc_call(method, params, retries=3):
    """Make an RPC call with fallback across multiple providers."""
    for rpc_idx, rpc_url in enumerate(RPC_URLS):
        for attempt in range(retries):
            try:
                resp = requests.post(rpc_url, json={
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                    "id": 1
                }, timeout=20)
                data = resp.json()
                if "result" in data:
                    return data["result"]
                if "error" in data:
                    if attempt < retries - 1:
                        time.sleep(0.5 * (attempt + 1))
                        continue
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(0.5 * (attempt + 1))
    return None


def fetch_withdraw_events(start_block=0):
    """Fetch Withdraw events from the veDOLO contract using RPC getLogs with pagination.
    Supports incremental fetching from a given start_block."""
    print("📡 Phase 1: Fetching Withdraw events...")

    # Get latest block
    latest_block = int(rpc_call("eth_blockNumber", []), 16)
    print(f"  Latest block: {latest_block:,}")
    print(f"  Scanning from block: {start_block:,}")

    all_logs = []
    block = start_block
    step = 50000  # 50K block pages

    while block <= latest_block:
        to_block = min(block + step - 1, latest_block)

        result = rpc_call("eth_getLogs", [{
            "address": VEDOLO_CONTRACT,
            "topics": [WITHDRAW_TOPIC],
            "fromBlock": hex(block),
            "toBlock": hex(to_block)
        }])

        if result:
            all_logs.extend(result)
            if len(result) > 0:
                print(f"  Block {block:,}-{to_block:,}: {len(result)} events (total: {len(all_logs)})")

        block = to_block + 1

        # Small delay to avoid rate limits
        if block % 500000 == 0:
            time.sleep(0.1)

    print(f"  ✅ Found {len(all_logs)} new Withdraw events")
    return all_logs, latest_block


def decode_withdraw_event(log):
    """Decode a Withdraw event log into structured data."""
    provider = "0x" + log["topics"][1][26:]  # indexed address
    data = log.get("data", "0x")[2:]

    token_id = int(data[0:64], 16)
    value = int(data[64:128], 16) / 1e18  # DOLO returned to user
    ts = int(data[128:192], 16)

    return {
        "provider": provider.lower(),
        "token_id": token_id,
        "value": value,
        "timestamp": ts,
        "block": int(log["blockNumber"], 16),
        "tx_hash": log["transactionHash"],
    }


def _calc_penalty_from_receipt(receipt):
    """Calculate penalty from a transaction receipt."""
    if not receipt:
        return None

    burn_amount = 0.0
    recoup_amount = 0.0
    user_amount = 0.0
    user_addr = None

    dolo_lower = DOLO_TOKEN.lower()
    vedolo_lower = VEDOLO_CONTRACT.lower()
    zero_lower = ZERO_ADDR.lower()
    vester_lower = ODOLO_VESTER.lower()

    for log in receipt.get("logs", []):
        # Only look at DOLO token Transfer events
        if log["address"].lower() != dolo_lower:
            continue
        if not log["topics"] or log["topics"][0] != TRANSFER_TOPIC:
            continue
        if len(log["topics"]) < 3:
            continue

        from_addr = "0x" + log["topics"][1][26:]
        to_addr = "0x" + log["topics"][2][26:]
        amount = int(log.get("data", "0x0"), 16) / 1e18

        from_addr_l = from_addr.lower()
        to_addr_l = to_addr.lower()

        # Transfer FROM veDOLO contract
        if from_addr_l == vedolo_lower:
            if to_addr_l == zero_lower:
                burn_amount += amount  # Burn fee
            elif to_addr_l == vester_lower:
                recoup_amount += amount  # Recoup fee (to oDOLO vester)
            elif to_addr_l.startswith("0xcfc30d38"):
                recoup_amount += amount  # Recoup fee (to secondary address)
            else:
                user_amount += amount  # DOLO returned to user
                user_addr = to_addr_l

    total_penalty = burn_amount + recoup_amount
    original_locked = burn_amount + recoup_amount + user_amount

    return {
        "burn_fee": round(burn_amount, 4),
        "recoup_fee": round(recoup_amount, 4),
        "total_penalty": round(total_penalty, 4),
        "original_locked": round(original_locked, 4),
        "user_received": round(user_amount, 4),
        "penalty_pct": round((total_penalty / original_locked * 100) if original_locked > 0 else 0, 2),
        "is_early_exit": total_penalty > 0,
    }


def fetch_receipt_and_calc_penalty(tx_hash):
    """Fetch transaction receipt and calculate penalty from Transfer events."""
    receipt = rpc_call("eth_getTransactionReceipt", [tx_hash])
    if not receipt:
        return None
    return _calc_penalty_from_receipt(receipt)


def main():
    print("=" * 60)
    print("🔄 veDOLO Early Exit Penalty — Data Fetcher")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load cache (receipts + last scanned block + cached logs)
    cache = {}
    cached_logs = []
    last_scanned_block = 0
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cache_data = json.load(f)
        # Support both old format (flat dict) and new format (with metadata)
        if isinstance(cache_data, dict) and "_meta" in cache_data:
            cache = cache_data.get("receipts", {})
            cached_logs = cache_data.get("logs", [])
            last_scanned_block = cache_data.get("_meta", {}).get("last_scanned_block", 0)
        else:
            cache = cache_data  # old format: just receipts
        print(f"  📦 Loaded {len(cache)} cached tx receipts, {len(cached_logs)} cached logs")
        if last_scanned_block:
            print(f"  📦 Last scanned block: {last_scanned_block:,}")

    # Phase 1: Fetch new Withdraw events (incremental from last scanned block)
    new_logs, latest_block = fetch_withdraw_events(start_block=last_scanned_block)
    # Merge: deduplicate by transactionHash
    seen_hashes = {log["transactionHash"] for log in cached_logs}
    for log in new_logs:
        if log["transactionHash"] not in seen_hashes:
            cached_logs.append(log)
            seen_hashes.add(log["transactionHash"])
    logs = cached_logs
    if not logs:
        print("⚠️ No Withdraw events found!")
        sys.exit(0)

    # Phase 2: Decode events
    print(f"\n📊 Phase 2: Decoding {len(logs)} Withdraw events...")
    events = []
    for log in logs:
        ev = decode_withdraw_event(log)
        events.append(ev)

    # Phase 3: Fetch receipts and calculate penalties
    print(f"\n💰 Phase 3: Calculating penalties for {len(events)} events...")

    # Check which tx_hashes need receipts
    tx_hashes_needed = [ev["tx_hash"] for ev in events if ev["tx_hash"] not in cache]
    print(f"  Cached: {len(events) - len(tx_hashes_needed)}/{len(events)}")
    print(f"  To fetch: {len(tx_hashes_needed)}")

    # Fetch receipts for uncached transactions
    if tx_hashes_needed:
        cached_count = 0
        errors = 0
        BATCH_SIZE = 50  # JSON-RPC batch: 50 receipts per HTTP request

        def fetch_batch_receipts(batch_hashes):
            """Fetch multiple tx receipts in a single JSON-RPC batch call."""
            payload = [
                {"jsonrpc": "2.0", "method": "eth_getTransactionReceipt",
                 "params": [tx_hash], "id": i}
                for i, tx_hash in enumerate(batch_hashes)
            ]
            for rpc_url in RPC_URLS:
                try:
                    resp = requests.post(rpc_url, json=payload,
                                         timeout=60, headers={"Content-Type": "application/json"})
                    results = resp.json()
                    if isinstance(results, list):
                        return results, rpc_url
                except Exception:
                    time.sleep(0.3)
            return None, None

        def fetch_single_receipt(tx_hash):
            """Fetch a single receipt trying ALL RPCs."""
            for rpc_url in RPC_URLS:
                try:
                    resp = requests.post(rpc_url, json={
                        "jsonrpc": "2.0", "method": "eth_getTransactionReceipt",
                        "params": [tx_hash], "id": 1
                    }, timeout=20, headers={"Content-Type": "application/json"})
                    data = resp.json()
                    if data.get("result"):
                        return data["result"]
                except Exception:
                    time.sleep(0.2)
            return None

        # Process in batches
        batches = [tx_hashes_needed[i:i+BATCH_SIZE] for i in range(0, len(tx_hashes_needed), BATCH_SIZE)]
        for batch_idx, batch in enumerate(batches):
            batch_results, used_rpc = fetch_batch_receipts(batch)
            failed_in_batch = []

            if batch_results:
                # Track which hashes got responses
                responded_indices = set()
                for resp_item in batch_results:
                    idx = resp_item.get("id", -1)
                    if 0 <= idx < len(batch):
                        responded_indices.add(idx)
                        tx_hash = batch[idx]
                        receipt = resp_item.get("result")
                        if receipt:
                            penalty = _calc_penalty_from_receipt(receipt)
                            if penalty:
                                cache[tx_hash] = penalty
                                cached_count += 1
                            else:
                                failed_in_batch.append(tx_hash)
                        else:
                            # Null result from this RPC — retry individually later
                            failed_in_batch.append(tx_hash)

                # Also add any hashes that had no response at all
                for idx in range(len(batch)):
                    if idx not in responded_indices:
                        failed_in_batch.append(batch[idx])
            else:
                # Entire batch failed
                failed_in_batch = list(batch)

            # Retry failed hashes individually across ALL RPCs
            if failed_in_batch:
                for tx_hash in failed_in_batch:
                    receipt = fetch_single_receipt(tx_hash)
                    if receipt:
                        penalty = _calc_penalty_from_receipt(receipt)
                        if penalty:
                            cache[tx_hash] = penalty
                            cached_count += 1
                        else:
                            errors += 1
                    else:
                        errors += 1
                    time.sleep(0.05)

            if (batch_idx + 1) % 5 == 0 or (batch_idx + 1) == len(batches):
                print(f"  Progress: batch {batch_idx+1}/{len(batches)} — cached: {cached_count:,} errors: {errors:,} / {len(tx_hashes_needed):,} total")
                cache_progress = {"_meta": {"last_scanned_block": latest_block}, "receipts": cache, "logs": cached_logs}
                with open(CACHE_FILE, "w") as f:
                    json.dump(cache_progress, f)

            time.sleep(0.1)  # Small delay between batches

        print(f"  ✅ Receipt fetch complete: {cached_count:,} new, {errors:,} failed")

        # Final cache save
        cache_output = {"_meta": {"last_scanned_block": latest_block}, "receipts": cache, "logs": cached_logs}
        with open(CACHE_FILE, "w") as f:
            json.dump(cache_output, f)
    else:
        # No new receipts needed, but still save updated metadata (logs + block number)
        cache_output = {"_meta": {"last_scanned_block": latest_block}, "receipts": cache, "logs": cached_logs}
        with open(CACHE_FILE, "w") as f:
            json.dump(cache_output, f)
        print(f"  💾 Cache updated with latest block {latest_block:,}")


    # Phase 4: Merge data and calculate stats
    print(f"\n📈 Phase 4: Computing statistics...")

    early_exits = []
    normal_exits = []
    total_burn = 0
    total_recoup = 0
    total_penalty_dolo = 0
    total_original_locked = 0

    for ev in events:
        penalty = cache.get(ev["tx_hash"])
        if not penalty:
            continue

        entry = {**ev, **penalty}
        entry["date"] = datetime.utcfromtimestamp(ev["timestamp"]).strftime("%Y-%m-%d")

        if penalty.get("is_early_exit"):
            early_exits.append(entry)
            total_burn += penalty["burn_fee"]
            total_recoup += penalty["recoup_fee"]
            total_penalty_dolo += penalty["total_penalty"]
            total_original_locked += penalty["original_locked"]
        else:
            normal_exits.append(entry)

    # Sort by timestamp
    early_exits.sort(key=lambda x: x["timestamp"], reverse=True)

    # Calculate aggregate stats
    stats = {
        "total_early_exits": len(early_exits),
        "total_normal_exits": len(normal_exits),
        "total_withdrawals": len(events),
        "total_burn_fee_dolo": round(total_burn, 2),
        "total_recoup_fee_dolo": round(total_recoup, 2),
        "total_penalty_dolo": round(total_penalty_dolo, 2),
        "total_original_locked": round(total_original_locked, 2),
        "avg_penalty_pct": round(
            (total_penalty_dolo / total_original_locked * 100) if total_original_locked > 0 else 0, 2
        ),
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    output = {
        "stats": stats,
        "early_exits": early_exits,
    }

    # Save full data for analysis
    full_file = os.path.join(DATA_DIR, "early_exits_full.json")
    with open(full_file, "w") as f:
        json.dump(output, f)
    print(f"\n💾 Saved: early_exits_full.json ({os.path.getsize(full_file) / 1024:.0f} KB)")

    # Save slim stats + top 50 recent exits for the dashboard
    top_exits = []
    for ex in early_exits[:50]:
        top_exits.append({
            "address": ex.get("provider", ""),
            "tx_hash": ex.get("tx_hash", ""),
            "original_locked": round(ex.get("original_locked", 0), 2),
            "total_penalty": round(ex.get("total_penalty", 0), 2),
            "penalty_pct": ex.get("penalty_pct", 0),
            "burn_fee": round(ex.get("burn_fee", 0), 2),
            "recoup_fee": round(ex.get("recoup_fee", 0), 2),
            "date": ex.get("date", ""),
        })
    with open(OUTPUT_FILE, "w") as f:
        json.dump({"stats": stats, "recent_exits": top_exits}, f, indent=2)
    print(f"💾 Saved: early_exits.json ({os.path.getsize(OUTPUT_FILE)} bytes) — includes {len(top_exits)} recent exits")


    print(f"\n💾 Saved: early_exits.json")
    print(f"   Early exits: {stats['total_early_exits']}")
    print(f"   Normal exits: {stats['total_normal_exits']}")
    print(f"   Total burn fee: {stats['total_burn_fee_dolo']:,.2f} DOLO")
    print(f"   Total recoup fee: {stats['total_recoup_fee_dolo']:,.2f} DOLO")
    print(f"   Total penalty: {stats['total_penalty_dolo']:,.2f} DOLO")
    print(f"   Avg penalty: {stats['avg_penalty_pct']:.1f}%")
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
