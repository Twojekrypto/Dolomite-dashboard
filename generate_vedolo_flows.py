#!/usr/bin/env python3
"""
veDOLO Events Pipeline — Deposit (Lock) & Withdraw (Unlock) events
Scans on-chain events from veDOLO contract deployment, outputs JSON for frontend.
Uses incremental sync — first run scans all, subsequent runs fetch only new blocks.
"""
import argparse
import json, time, os, sys
import requests
from datetime import datetime, timezone

ALCHEMY_BERA_RPC = os.environ.get("ALCHEMY_BERACHAIN_RPC", "")
ALCHEMY_BERA_RPC_2 = os.environ.get("ALCHEMY_BERACHAIN_RPC_2", "")
ALCHEMY_BERA_RPC_3 = os.environ.get("ALCHEMY_BERACHAIN_RPC_3", "")

# ===== CONFIG =====
VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
WITHDRAW_TOPIC = "0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94"
DEPOSIT_TOPIC = "0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624"

# oDOLO Vester — locks via oDOLO exercise go through this contract
ODOLO_VESTER = "0x3e9b9a16743551da49b5e136c716bba7932d2cec".lower()
# ERC-20/ERC-721 Transfer topic; useful for finding the real veDOLO NFT recipient.
ODOLO_EXERCISE_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_TOPIC = "0x" + ("0" * 64)

RPC_URLS = [
    *([] if not ALCHEMY_BERA_RPC else [ALCHEMY_BERA_RPC]),
    *([] if not ALCHEMY_BERA_RPC_2 else [ALCHEMY_BERA_RPC_2]),
    *([] if not ALCHEMY_BERA_RPC_3 else [ALCHEMY_BERA_RPC_3]),
    "https://berachain-rpc.publicnode.com/",
    "https://berachain.drpc.org/",
    "https://rpc.berachain.com/",
]

DEPLOY_BLOCK = 2_925_000  # veDOLO contract first events
CHUNK_SIZE = 50_000
BLOCK_TIME = 2  # ~2 seconds per block on Berachain

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "vedolo_flows.json")
STATE_FILE = os.path.join(DATA_DIR, "vedolo_flows_state.json")
RUN_STATUS_FILE = os.path.join(DATA_DIR, "vedolo_flows_run_status.json")
EXERCISERS_BY_ADDRESS_FILE = os.path.join(DATA_DIR, "exercisers_by_address.json")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate veDOLO lock/unlock flow data")
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=int(os.environ.get("VEDOLO_FLOWS_MAX_RUNTIME_SECONDS") or 0),
        help="Exit cleanly after this many seconds, saving resumable state first.",
    )
    return parser.parse_args()


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def save_run_status(completed, **extra):
    payload = {
        "completed": bool(completed),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        **extra,
    }
    tmp = RUN_STATUS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, RUN_STATUS_FILE)


def save_pending_sync(state, target_block, unlocks, locks, tx_hashes):
    state["pending_vedolo_sync"] = {
        "target_block": target_block,
        "unlocks": unlocks,
        "locks": locks,
        "tx_hashes": sorted(tx_hashes),
        "updated": datetime.utcnow().isoformat() + "Z",
    }
    save_state(state)


def load_pending_sync(state):
    pending = state.get("pending_vedolo_sync")
    if not isinstance(pending, dict):
        return None

    target_block = int(pending.get("target_block") or 0)
    locks = pending.get("locks")
    unlocks = pending.get("unlocks")
    tx_hashes = pending.get("tx_hashes")
    if target_block <= 0 or not isinstance(locks, list) or not isinstance(unlocks, list):
        return None
    if not isinstance(tx_hashes, list):
        tx_hashes = []

    return {
        "target_block": target_block,
        "locks": locks,
        "unlocks": unlocks,
        "tx_hashes": [str(tx).lower() for tx in tx_hashes],
    }


def load_odolo_exerciser_lookup():
    """Map oDOLO exercise tx hash -> end-user wallet."""
    if not os.path.exists(EXERCISERS_BY_ADDRESS_FILE):
        return {}
    try:
        with open(EXERCISERS_BY_ADDRESS_FILE) as f:
            payload = json.load(f)
    except Exception:
        return {}

    lookup = {}
    for entry in payload.get("exercisers", []) or []:
        address = str(entry.get("address") or "").strip().lower()
        if not address.startswith("0x") or len(address) != 42:
            continue
        for tx in entry.get("txs", []) or []:
            tx_hash = str(tx.get("hash") or "").strip().lower()
            if tx_hash.startswith("0x") and len(tx_hash) == 66 and tx_hash not in lookup:
                lookup[tx_hash] = address
    return lookup


def normalize_address(value):
    address = str(value or "").strip().lower()
    if address.startswith("0x") and len(address) == 42:
        return address
    return ""


def address_from_topic(topic):
    topic = str(topic or "").strip().lower()
    if topic.startswith("0x") and len(topic) == 66:
        return "0x" + topic[-40:]
    return ""


def extract_odolo_receipt_beneficiary(receipt):
    """Extract the end-user wallet from Vester/veDOLO receipt logs."""
    if not receipt or not receipt.get("logs"):
        return None

    for log in receipt["logs"]:
        if normalize_address(log.get("address")) != ODOLO_VESTER:
            continue
        for topic in log.get("topics", [])[1:]:
            address = address_from_topic(topic)
            if address and address not in {ZERO_ADDRESS, ODOLO_VESTER}:
                return address

    vedolo_lower = VEDOLO_CONTRACT.lower()
    for log in receipt["logs"]:
        if normalize_address(log.get("address")) != vedolo_lower:
            continue
        topics = log.get("topics", [])
        if len(topics) < 3 or str(topics[0]).lower() != ODOLO_EXERCISE_TOPIC:
            continue
        if str(topics[1]).lower() != ZERO_TOPIC:
            continue
        address = address_from_topic(topics[2])
        if address and address not in {ZERO_ADDRESS, ODOLO_VESTER}:
            return address

    return None


def remap_odolo_lock_beneficiaries(locks, exerciser_lookup):
    """Use oDOLO exercise metadata to attribute protocol-routed locks to users."""
    resolved = 0
    unresolved = 0

    for lock in locks:
        tx_hash = str(lock.get("txHash") or "").strip().lower()
        original_address = normalize_address(lock.get("address")) or ODOLO_VESTER
        protocol_address = normalize_address(lock.get("protocolAddress"))
        is_vester_provider = original_address == ODOLO_VESTER or protocol_address == ODOLO_VESTER
        lookup_beneficiary = exerciser_lookup.get(tx_hash)
        receipt_beneficiary = normalize_address(lock.get("beneficiaryAddress"))
        beneficiary = lookup_beneficiary or receipt_beneficiary
        if not lock.get("isOdolo") and not is_vester_provider and not beneficiary:
            continue

        lock["isOdolo"] = True
        lock["protocolAddress"] = ODOLO_VESTER
        if beneficiary:
            lock["address"] = beneficiary
            lock["beneficiaryAddress"] = beneficiary
            lock["addressSource"] = "odolo-exerciser" if lookup_beneficiary else "odolo-receipt"
            resolved += 1
        else:
            lock["address"] = ODOLO_VESTER
            lock["beneficiaryAddress"] = None
            lock["addressSource"] = "odolo-vester-fallback"
            unresolved += 1

    return resolved, unresolved


def rpc_call(method, params, timeout=15):
    """Call RPC with fallback across multiple endpoints."""
    for attempt in range(len(RPC_URLS) * 2):
        rpc = RPC_URLS[attempt % len(RPC_URLS)]
        try:
            resp = requests.post(rpc, json={
                "jsonrpc": "2.0", "method": method, "params": params, "id": 1
            }, timeout=timeout, headers={"Content-Type": "application/json"})
            data = resp.json()
            if "error" in data:
                err = data["error"].get("message", "")
                if "range" in err.lower() or "limit" in err.lower():
                    return {"error": data["error"]}
                time.sleep(0.3)
                continue
            return data
        except requests.exceptions.Timeout:
            time.sleep(1)
        except Exception:
            time.sleep(0.3)
    return None


def get_current_block():
    """Get current block number with robust retries."""
    for round_num in range(3):  # 3 full rounds across all RPCs
        for rpc in RPC_URLS:
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
                }, timeout=15, headers={"Content-Type": "application/json"})
                data = resp.json()
                if "error" in data:
                    print(f"  ⚠️ RPC error from {rpc[:50]}...: {data['error']}")
                    continue
                block = int(data.get("result", "0x0"), 16)
                if block > 0:
                    return block
                print(f"  ⚠️ Got block 0 from {rpc[:50]}...")
            except requests.exceptions.Timeout:
                print(f"  ⚠️ Timeout from {rpc[:50]}...")
            except Exception as e:
                print(f"  ⚠️ Error from {rpc[:50]}...: {e}")
            time.sleep(0.5)
        if round_num < 2:
            print(f"  Retry round {round_num + 2}/3...")
            time.sleep(2)
    return 0



def fetch_event_logs(start_block, end_block, topic):
    """Fetch event logs for a specific topic from veDOLO contract."""
    chunk_size = CHUNK_SIZE
    if start_block >= end_block:
        return []

    total_blocks = end_block - start_block
    print(f"  Scanning blocks {start_block:,} → {end_block:,} ({total_blocks:,} blocks)")

    all_logs = []
    current = start_block
    chunks_done = 0

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        success = False
        for attempt in range(len(RPC_URLS) * 2):
            rpc = RPC_URLS[attempt % len(RPC_URLS)]
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_getLogs",
                    "params": [{
                        "address": VEDOLO_CONTRACT,
                        "topics": [topic],
                        "fromBlock": hex(current),
                        "toBlock": hex(chunk_end),
                    }], "id": 1
                }, timeout=30, headers={"Content-Type": "application/json"})

                r = resp.json()
                if "error" in r:
                    err_msg = r["error"].get("message", "")
                    if "range" in err_msg.lower() or "limit" in err_msg.lower():
                        chunk_size = max(chunk_size // 2, 1000)
                        chunk_end = min(current + chunk_size - 1, end_block)
                        continue
                    time.sleep(0.5)
                    continue

                logs = r.get("result", [])
                all_logs.extend(logs)
                success = True
                break
            except requests.exceptions.Timeout:
                chunk_size = max(chunk_size // 2, 1000)
                chunk_end = min(current + chunk_size - 1, end_block)
                time.sleep(1)
            except Exception:
                time.sleep(0.5)

        if not success:
            print(f"    ⚠️ Failed at block {current}, skipping chunk")
            current = chunk_end + 1
            continue

        current = chunk_end + 1
        chunks_done += 1

        if chunks_done % 20 == 0 or current > end_block:
            pct = min(100, (current - start_block) * 100 // max(total_blocks, 1))
            print(f"    {pct}% (block {current:,}/{end_block:,}, {len(all_logs):,} events)", flush=True)

        if chunk_size < CHUNK_SIZE:
            chunk_size = min(chunk_size * 2, CHUNK_SIZE)

        time.sleep(0.05)

    print(f"  ✅ {len(all_logs):,} events found")
    return all_logs


def get_tx_receipt(tx_hash):
    """Get transaction receipt to check for oDOLO exercise events."""
    data = rpc_call("eth_getTransactionReceipt", [tx_hash], timeout=10)
    if data and "result" in data:
        return data["result"]
    return None


def _checkpoint_receipt_checks(state, receipt_checks, pending_sync=None):
    if state is None:
        return
    state["odolo_receipt_checks"] = receipt_checks
    if pending_sync:
        state["pending_vedolo_sync"] = {
            **pending_sync,
            "updated": datetime.utcnow().isoformat() + "Z",
        }
    save_state(state)


def check_odolo_exercise_batch(
    tx_hashes,
    exerciser_lookup=None,
    receipt_checks=None,
    state=None,
    pending_sync=None,
    soft_deadline=None,
):
    """Check which tx hashes involve oDOLO exercise (transfer from Vester)."""
    exerciser_lookup = exerciser_lookup or {}
    receipt_checks = receipt_checks if isinstance(receipt_checks, dict) else {}
    exercise_txs = {}
    normalized_txs = [str(tx_hash or "").strip().lower() for tx_hash in tx_hashes]
    normalized_txs = [tx_hash for tx_hash in normalized_txs if tx_hash.startswith("0x") and len(tx_hash) == 66]
    total = len(normalized_txs)
    rpc_checks = 0

    for i, tx_hash in enumerate(normalized_txs):
        lookup_beneficiary = exerciser_lookup.get(tx_hash)
        if lookup_beneficiary:
            exercise_txs[tx_hash] = lookup_beneficiary
            continue

        if tx_hash in receipt_checks:
            cached = receipt_checks[tx_hash]
            if isinstance(cached, dict) and cached.get("isOdolo"):
                exercise_txs[tx_hash] = normalize_address(cached.get("beneficiary"))
            continue

        if soft_deadline is not None and time.monotonic() >= soft_deadline:
            _checkpoint_receipt_checks(state, receipt_checks, pending_sync)
            print(
                f"    ⏸️ Soft runtime limit reached while checking oDOLO receipts "
                f"({i}/{total}). Progress saved.",
                flush=True,
            )
            return exercise_txs, False

        receipt = get_tx_receipt(tx_hash)
        beneficiary = None
        is_odolo = False
        if receipt and receipt.get("logs"):
            for log in receipt["logs"]:
                # Check if any log is from the oDOLO Vester contract
                log_addr = log.get("address", "").lower()
                if log_addr == ODOLO_VESTER:
                    beneficiary = extract_odolo_receipt_beneficiary(receipt)
                    exercise_txs[tx_hash] = beneficiary
                    is_odolo = True
                    break

        receipt_checks[tx_hash] = {
            "isOdolo": is_odolo,
            "beneficiary": beneficiary,
        }
        rpc_checks += 1

        if (i + 1) % 50 == 0:
            print(f"    Checking oDOLO exercises: {i+1}/{total} ({rpc_checks} RPC receipts)", flush=True)
        if rpc_checks and rpc_checks % 250 == 0:
            _checkpoint_receipt_checks(state, receipt_checks, pending_sync)
        time.sleep(0.03)

    _checkpoint_receipt_checks(state, receipt_checks, pending_sync)
    return exercise_txs, True


def resolve_receipt_fallback_beneficiaries(locks):
    """Resolve Vester fallback rows from on-chain receipts when exerciser cache missed them."""
    resolved = 0
    fallback_locks = [
        lock for lock in locks
        if lock.get("addressSource") == "odolo-vester-fallback" and lock.get("txHash")
    ]

    for lock in fallback_locks:
        receipt = get_tx_receipt(lock["txHash"])
        beneficiary = extract_odolo_receipt_beneficiary(receipt)
        if beneficiary:
            lock["address"] = beneficiary
            lock["beneficiaryAddress"] = beneficiary
            lock["addressSource"] = "odolo-receipt"
            lock["isOdolo"] = True
            lock["protocolAddress"] = ODOLO_VESTER
            resolved += 1
        time.sleep(0.03)

    return resolved


def decode_withdraw(log):
    """Decode Withdraw event: Withdraw(address indexed provider) + data: [tokenId, value, timestamp]"""
    provider = "0x" + log["topics"][1][26:]
    data = log["data"][2:]
    token_id = int(data[0:64], 16)
    value = int(data[64:128], 16) / 1e18
    ts = int(data[128:192], 16)

    return {
        "address": provider.lower(),
        "txHash": log["transactionHash"],
        "tokenId": token_id,
        "dolo": round(value, 4),
        "timestamp": ts,
        "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
        "block": int(log["blockNumber"], 16),
    }


def decode_deposit(log):
    """Decode Deposit event: Deposit(address indexed provider, uint256 indexed locktime)
    + data: [tokenId, value, deposit_type, timestamp]"""
    provider = "0x" + log["topics"][1][26:]
    locktime = int(log["topics"][2], 16)
    data = log["data"][2:]
    token_id = int(data[0:64], 16)
    value = int(data[64:128], 16) / 1e18
    deposit_type = int(data[128:192], 16)
    ts = int(data[192:256], 16)

    lock_days = max(0, round((locktime - ts) / 86400))

    return {
        "address": provider.lower(),
        "txHash": log["transactionHash"],
        "tokenId": token_id,
        "dolo": round(value, 4),
        "lockDays": lock_days,
        "locktime": locktime,
        "depositType": deposit_type,
        "timestamp": ts,
        "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
        "block": int(log["blockNumber"], 16),
    }


def main():
    args = parse_args()
    soft_deadline = None
    if args.max_runtime_seconds > 0:
        soft_deadline = time.monotonic() + args.max_runtime_seconds

    print("=" * 60)
    print("🔄 veDOLO Events Pipeline — Locks & Unlocks")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load incremental state
    state = load_state()
    is_incremental = bool(state.get("last_block"))
    if is_incremental:
        print("📦 Found previous state — running incremental sync")
    else:
        print("🆕 No previous state — running full sync (first run)")

    pending_sync = load_pending_sync(state)
    exerciser_lookup = load_odolo_exerciser_lookup()
    if pending_sync:
        print(
            "📦 Found unfinished veDOLO sync — resuming receipt checks "
            f"for target block {pending_sync['target_block']:,}"
        )
        current_block = pending_sync["target_block"]
        all_unlocks = pending_sync["unlocks"]
        all_locks = pending_sync["locks"]
        pending_tx_hashes = pending_sync["tx_hashes"]
        print(f"  Resumed: {len(all_unlocks)} unlocks, {len(all_locks)} locks")
    else:
        # Get current block
        print("\n📡 Getting current block number...")
        current_block = get_current_block()
        print(f"  Berachain: block {current_block:,}")

        if current_block == 0:
            print("❌ Could not get current block. Aborting.")
            sys.exit(1)

        # Determine scan range
        last_block = state.get("last_block", 0)
        cached_unlocks = state.get("unlocks", [])
        cached_locks = state.get("locks", [])

        if is_incremental and last_block > 0:
            fetch_start = last_block + 1
            if fetch_start >= current_block:
                print(f"  Already up to date (block {last_block:,})")
                new_withdraw_logs = []
                new_deposit_logs = []
            else:
                print(f"\n📡 Fetching Withdraw events (unlocks)...")
                new_withdraw_logs = fetch_event_logs(fetch_start, current_block, WITHDRAW_TOPIC)
                print(f"\n📡 Fetching Deposit events (locks)...")
                new_deposit_logs = fetch_event_logs(fetch_start, current_block, DEPOSIT_TOPIC)
        else:
            print(f"\n📡 Fetching ALL Withdraw events (unlocks) from block {DEPLOY_BLOCK:,}...")
            new_withdraw_logs = fetch_event_logs(DEPLOY_BLOCK, current_block, WITHDRAW_TOPIC)
            print(f"\n📡 Fetching ALL Deposit events (locks) from block {DEPLOY_BLOCK:,}...")
            new_deposit_logs = fetch_event_logs(DEPLOY_BLOCK, current_block, DEPOSIT_TOPIC)
            cached_unlocks = []
            cached_locks = []

        # Decode new events
        print(f"\n🔧 Decoding events...")
        new_unlocks = [decode_withdraw(log) for log in new_withdraw_logs]
        new_locks = [decode_deposit(log) for log in new_deposit_logs]
        print(f"  New: {len(new_unlocks)} unlocks, {len(new_locks)} locks")

        # Merge with cached
        all_unlocks = cached_unlocks + new_unlocks
        all_locks = cached_locks + new_locks
        print(f"  Total: {len(all_unlocks)} unlocks, {len(all_locks)} locks")
        pending_tx_hashes = list(set(l["txHash"].lower() for l in new_locks))
        if pending_tx_hashes:
            save_pending_sync(state, current_block, all_unlocks, all_locks, pending_tx_hashes)

    # Check oDOLO exercise status for new lock events
    if pending_tx_hashes:
        print(f"\n🔍 Checking oDOLO exercise status for {len(pending_tx_hashes)} new locks...")
        receipt_checks = state.get("odolo_receipt_checks", {})
        exercise_txs, exercise_complete = check_odolo_exercise_batch(
            pending_tx_hashes,
            exerciser_lookup=exerciser_lookup,
            receipt_checks=receipt_checks,
            state=state,
            pending_sync=state.get("pending_vedolo_sync"),
            soft_deadline=soft_deadline,
        )
        if not exercise_complete:
            save_pending_sync(state, current_block, all_unlocks, all_locks, pending_tx_hashes)
            save_run_status(
                False,
                reason="soft_runtime_limit",
                target_block=current_block,
                checked_receipts=len(state.get("odolo_receipt_checks", {})),
                total_lock_txs=len(pending_tx_hashes),
            )
            print("⏸️ veDOLO sync paused before timeout. State cached for the next workflow run.")
            return
        print(f"  Found {len(exercise_txs)} locks via oDOLO exercise")

        # Tag new locks
        pending_tx_hash_set = set(pending_tx_hashes)
        for lock in all_locks:
            tx_hash = lock["txHash"].lower()
            if tx_hash not in pending_tx_hash_set:
                continue
            lock["isOdolo"] = tx_hash in exercise_txs
            beneficiary = exercise_txs.get(tx_hash)
            if beneficiary:
                lock["beneficiaryAddress"] = beneficiary
                lock["addressSource"] = "odolo-receipt"
                lock["protocolAddress"] = ODOLO_VESTER

    resolved_odolo, unresolved_odolo = remap_odolo_lock_beneficiaries(all_locks, exerciser_lookup)
    receipt_resolved_odolo = 0
    if unresolved_odolo:
        receipt_resolved_odolo = resolve_receipt_fallback_beneficiaries(all_locks)
        resolved_odolo += receipt_resolved_odolo
        unresolved_odolo = max(0, unresolved_odolo - receipt_resolved_odolo)
    if resolved_odolo or unresolved_odolo:
        print(
            f"  Remapped {resolved_odolo:,} oDOLO-routed locks to end-user wallets"
            f" ({unresolved_odolo:,} still routed through protocol fallback)"
        )
    if receipt_resolved_odolo:
        print(f"  Resolved {receipt_resolved_odolo:,} fallback locks from transaction receipts")

    # Sort by timestamp desc
    all_unlocks.sort(key=lambda x: x["timestamp"], reverse=True)
    all_locks.sort(key=lambda x: x["timestamp"], reverse=True)

    # Data protection: don't overwrite good data with empty
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON) as f:
                old = json.load(f)
            old_unlocks = len(old.get("unlocks", []))
            old_locks = len(old.get("locks", []))
            if len(all_unlocks) == 0 and old_unlocks > 0:
                print(f"\n⚠️ 0 unlocks but old file has {old_unlocks}. Preserving old data.")
                all_unlocks = old["unlocks"]
            if len(all_locks) == 0 and old_locks > 0:
                print(f"\n⚠️ 0 locks but old file has {old_locks}. Preserving old data.")
                all_locks = old["locks"]
        except Exception:
            pass

    # Build output
    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_unlocks": len(all_unlocks),
        "total_locks": len(all_locks),
        "unlocks": all_unlocks,
        "locks": all_locks,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    # Save state for incremental sync
    state["last_block"] = current_block
    state["unlocks"] = all_unlocks
    state["locks"] = all_locks
    state.pop("pending_vedolo_sync", None)
    save_state(state)
    save_run_status(True, target_block=current_block)

    print(f"\n💾 Saved: {OUTPUT_JSON}")
    print(f"   {len(all_unlocks)} unlocks, {len(all_locks)} locks")
    print(f"   State saved for incremental sync")

    # Summary
    if all_unlocks:
        oldest = min(u["date"] for u in all_unlocks)
        newest = max(u["date"] for u in all_unlocks)
        total_dolo = sum(u["dolo"] for u in all_unlocks)
        print(f"\n📊 Unlocks: {oldest} → {newest}, {total_dolo:,.0f} DOLO total")

    if all_locks:
        oldest = min(l["date"] for l in all_locks)
        newest = max(l["date"] for l in all_locks)
        total_dolo = sum(l["dolo"] for l in all_locks)
        odolo_count = sum(1 for l in all_locks if l.get("isOdolo"))
        direct_count = len(all_locks) - odolo_count
        print(f"📊 Locks: {oldest} → {newest}, {total_dolo:,.0f} DOLO total")
        print(f"   {odolo_count} via oDOLO, {direct_count} direct")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
