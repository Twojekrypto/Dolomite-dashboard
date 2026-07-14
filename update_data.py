#!/usr/bin/env python3
"""
veDOLO Dashboard — Auto-updater (Etherscan V2 API)
Phase 1: Fetches all NFT transfers via Etherscan V2 tokennfttx (paginated, 100% accurate).
Phase 2: Fetches locked DOLO amounts from Berachain RPC (batched, cached).
Outputs: vedolo_holders.json, vedolo_holders.csv
"""
import json, time, os, csv, sys
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


def env_int(name, default, minimum=0):
    try:
        return max(minimum, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


# Global timeout: abort gracefully before CI kills us
SCRIPT_START = time.time()
MAX_RUNTIME_SECONDS = env_int("VEDOLO_MAX_RUNTIME_SECONDS", 18 * 60)  # CI timeout = 90 min

def check_timeout(phase=""):
    """Check if script has exceeded max runtime. Exit gracefully if so."""
    elapsed = time.time() - SCRIPT_START
    if elapsed > MAX_RUNTIME_SECONDS:
        print(f"\n⏰ TIMEOUT after {elapsed/60:.0f} min in {phase}! Saving current data and exiting.", flush=True)
        return True
    return False

# ===== CONFIG =====
VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
CHAIN_ID = 80094  # Berachain
RPC_URL = "https://rpc.berachain.com/"

# Endpoint list comes from the shared client (env-injected Alchemy keys first,
# then public fallbacks) — single source of truth for RPC endpoints.
from rpc_client import get_endpoints as _rpc_endpoints
from generate_vedolo_vote_power_history import fetch_canonical_snapshot

import rpc_usage

RPC_URLS = _rpc_endpoints("berachain")
LOCKED_SELECTOR = "0xb45a3c0e"  # locked(uint256)
BALANCE_OF_NFT_SELECTOR = "0xe7e242d4"  # balanceOfNFT(uint256) — current vote weight
BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address) — ERC20
DOLO_TOKEN = "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654"  # Underlying DOLO token
# Multicall3 (same address on every EVM chain) batches many per-tokenId reads
# into ONE eth_call. JSON-RPC batching does NOT cut compute units; Multicall3 does.
MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_AGG3_ABI = [{
    "inputs": [{"components": [
        {"name": "target", "type": "address"},
        {"name": "allowFailure", "type": "bool"},
        {"name": "callData", "type": "bytes"},
    ], "name": "calls", "type": "tuple[]"}],
    "name": "aggregate3",
    "outputs": [{"components": [
        {"name": "success", "type": "bool"},
        {"name": "returnData", "type": "bytes"},
    ], "name": "returnData", "type": "tuple[]"}],
    "stateMutability": "payable", "type": "function",
}]

BATCH_SIZE = 25
MAX_WORKERS = 3
RPC_TIMEOUT_SECONDS = env_int("VEDOLO_RPC_TIMEOUT_SECONDS", 8, minimum=1)
RPC_RETRIES = env_int("VEDOLO_RPC_RETRIES", 2, minimum=1)
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(DATA_DIR, "locked_cache.json")
OUTPUT_JSON = os.path.join(DATA_DIR, "vedolo_holders.json")
OUTPUT_CSV = os.path.join(DATA_DIR, "vedolo_holders.csv")

API_KEY = os.environ.get("BERASCAN_API_KEY", "")


def apply_canonical_vote_weight(stats, snapshot):
    holder_sum = stats.get("total_vote_weight", 0)
    stats["total_vote_weight_holder_sum"] = round(float(holder_sum), 4)
    stats["total_vote_weight"] = round(snapshot.total_supply_wei / 10**18, 4)
    stats["total_vote_weight_source"] = "contract_totalSupply"
    stats["total_vote_weight_block"] = snapshot.block_number
    stats["total_vote_weight_timestamp"] = snapshot.timestamp
    return stats


class IncompleteNftTransferFetch(RuntimeError):
    """Raised when the explorer returns only partial veDOLO NFT history."""


def preserve_existing_vedolo_snapshot(reason):
    """Keep the previous holder snapshot instead of publishing partial data."""
    print(f"\n⚠️  {reason}", flush=True)
    if os.path.exists(OUTPUT_JSON):
        print(f"   Preserving existing {os.path.basename(OUTPUT_JSON)}; no partial holder data will be written.", flush=True)
        sys.exit(0)
    print(f"   No existing {os.path.basename(OUTPUT_JSON)} to preserve.", flush=True)
    sys.exit(1)


def nft_transfer_coverage_stats(txs):
    blocks = []
    for tx in txs:
        try:
            blocks.append(int(tx.get("blockNumber", 0)))
        except (TypeError, ValueError):
            continue
    return {
        "nft_transfer_count": len(txs),
        "nft_transfer_min_block": min(blocks) if blocks else 0,
        "nft_transfer_max_block": max(blocks) if blocks else 0,
    }


def ownership_snapshot_issues(stats, path=OUTPUT_JSON):
    """Detect impossible drops that indicate a partial NFT transfer fetch."""
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            previous = json.load(f)
    except Exception as exc:
        print(f"   ⚠️ Could not read previous veDOLO holder snapshot for ownership guard: {exc}", flush=True)
        return []

    prev_stats = previous.get("stats", {}) if isinstance(previous, dict) else {}
    issues = []

    monotonic_fields = (
        ("total_minted", "minted veDOLO positions"),
        ("total_burned", "burned veDOLO positions"),
        ("nft_transfer_count", "NFT transfer events"),
        ("nft_transfer_max_block", "latest NFT transfer block"),
    )
    for field, label in monotonic_fields:
        prev_val = prev_stats.get(field)
        new_val = stats.get(field)
        if prev_val is None or new_val is None:
            continue
        try:
            prev_num = int(prev_val)
            new_num = int(new_val)
        except (TypeError, ValueError):
            continue
        if prev_num > 0 and new_num < prev_num:
            issues.append(f"{label} decreased from {prev_num:,} to {new_num:,}")

    return issues


# ===== PHASE 1: Fetch all NFT transfers via Etherscan V2 API =====

def fetch_all_nft_transfers():
    """Fetch complete NFT transfer history using startblock/endblock pagination.
    
    Etherscan V2 caps page*offset <= 10,000. To get ALL transactions,
    we paginate by block range: fetch 10k sorted asc, then use the last
    block number as the next startblock.
    """
    print("📡 Phase 1: Fetching NFT transfers via Etherscan V2 API...", flush=True)

    if not API_KEY:
        print("❌ BERASCAN_API_KEY not set! Cannot fetch data.")
        sys.exit(1)

    all_txs = []
    seen_hashes = set()  # Deduplicate txs spanning block boundaries
    start_block = 0

    while True:
        params = {
            "chainid": CHAIN_ID,
            "module": "account",
            "action": "tokennfttx",
            "contractaddress": VEDOLO_CONTRACT,
            "startblock": start_block,
            "endblock": 99999999,
            "page": 1,
            "offset": 10000,
            "sort": "asc",
            "apikey": API_KEY,
        }

        for retry in range(3):
            try:
                resp = requests.get(ETHERSCAN_V2, params=params, timeout=30)
                data = resp.json()

                if data.get("status") == "1" and isinstance(data.get("result"), list):
                    results = data["result"]

                    # Deduplicate (same block may appear in consecutive calls)
                    new_count = 0
                    for tx in results:
                        tx_key = tx.get("hash", "") + tx.get("tokenID", "")
                        if tx_key not in seen_hashes:
                            seen_hashes.add(tx_key)
                            all_txs.append(tx)
                            new_count += 1

                    print(f"  Block {start_block}+: {len(results)} txs, {new_count} new (total: {len(all_txs)})", flush=True)

                    if len(results) < 10000:
                        # Got all remaining transfers
                        print(f"  ✅ Fetched all {len(all_txs)} NFT transfers")
                        return all_txs

                    # Move startblock to the last block in results
                    last_block = int(results[-1].get("blockNumber", start_block))
                    if last_block == start_block:
                        # Edge case: >10k txs in same block. Skip to next block.
                        start_block = last_block + 1
                    else:
                        start_block = last_block

                    time.sleep(0.25)  # Rate limit
                    break

                elif "rate" in str(data.get("result", "")).lower() or "max rate" in str(data.get("message", "")).lower():
                    print(f"  Rate limited, waiting {2*(retry+1)}s...", flush=True)
                    time.sleep(2 * (retry + 1))
                    if check_timeout("Phase1-rate-limit"):
                        raise IncompleteNftTransferFetch(
                            f"Timed out while fetching veDOLO NFT transfers after {len(all_txs):,} rows "
                            f"(current start block {start_block:,})"
                        )
                    continue

                else:
                    if data.get("message") == "No transactions found" or (
                        isinstance(data.get("result"), str) and "No transactions" in data["result"]):
                        print(f"  ✅ Fetched all {len(all_txs)} NFT transfers")
                        return all_txs
                    print(f"  ⚠️ API: {data.get('message')}: {str(data.get('result',''))[:100]}")
                    if all_txs:
                        raise IncompleteNftTransferFetch(
                            f"Explorer returned an API warning after {len(all_txs):,} veDOLO NFT transfer rows "
                            f"(current start block {start_block:,})"
                        )
                    sys.exit(1)

            except Exception as e:
                print(f"  Error: {e}, retry {retry+1}/3")
                time.sleep(2 * (retry + 1))
        else:
            print(f"  ❌ Failed after 3 retries at block {start_block}")
            raise IncompleteNftTransferFetch(
                f"Failed to fetch complete veDOLO NFT transfer history after {len(all_txs):,} rows "
                f"(failed at block {start_block:,})"
            )

    raise IncompleteNftTransferFetch(
        f"Stopped before completing veDOLO NFT transfer history after {len(all_txs):,} rows"
    )




def build_ownership(txs):
    """Build current ownership map from NFT transfers."""
    print("\n📊 Building ownership map...")
    ZERO = "0x0000000000000000000000000000000000000000"

    # Sort by block number and transaction index for correct ordering
    txs.sort(key=lambda t: (int(t.get("blockNumber", 0)), int(t.get("transactionIndex", 0))))

    ownership = {}  # token_id -> current_owner
    all_minted = set()

    for tx in txs:
        token_id = int(tx.get("tokenID", 0))
        if token_id <= 0:
            continue
        from_addr = tx.get("from", "").lower()
        to_addr = tx.get("to", "").lower()

        if from_addr == ZERO.lower():
            all_minted.add(token_id)

        ownership[token_id] = to_addr

    # Count stats
    burned = sum(1 for addr in ownership.values() if addr == ZERO.lower())

    active_owners = {}
    for tid, owner in ownership.items():
        if owner == ZERO.lower():
            continue
        if owner not in active_owners:
            active_owners[owner] = []
        active_owners[owner].append(tid)

    stats = {
        "total_minted": len(all_minted),
        "total_burned": burned,
        "active_nfts": len(all_minted) - burned,
        "unique_holders": len(active_owners),
    }

    holders = []
    for addr, tids in active_owners.items():
        holders.append({
            "address": addr,
            "nft_count": len(tids),
            "token_ids": sorted(tids),
        })

    print(f"  Minted: {stats['total_minted']:,}  Burned: {stats['total_burned']:,}  Active: {stats['active_nfts']:,}")
    print(f"  Unique holders: {stats['unique_holders']:,}")

    return holders, stats


# ===== PHASE 2: Fetch locked DOLO + PHASE 3: Fetch vote weights =====

def _multicall_vedolo_reads(token_ids, selector):
    """Multicall3 fast path for VEDOLO_CONTRACT read `selector`(uint256 tokenId).

    Returns {token_id: returnData_bytes} for ids the multicall resolved
    successfully. Ids absent from the result must be resolved by the per-call
    JSON-RPC fallback, which preserves the failed-vs-zero handling. The caller
    decodes the raw bytes per method. If web3 is unavailable, returns {} so every
    id defers to the fallback. Data-identical to individual eth_calls.
    """
    ids = list(token_ids)
    out = {}
    if not ids:
        return out
    try:
        from web3 import Web3
    except ImportError:
        return out
    rpc_list = [r for r in RPC_URLS if r]
    if not rpc_list:
        return out

    target = Web3.to_checksum_address(VEDOLO_CONTRACT)
    multicall_addr = Web3.to_checksum_address(MULTICALL3_ADDR)
    sel = selector[2:] if selector.startswith("0x") else selector
    rpc_idx = 0

    for start in range(0, len(ids), max(1, BATCH_SIZE)):
        chunk = ids[start:start + max(1, BATCH_SIZE)]
        calls = [(target, True, bytes.fromhex(sel + hex(tid)[2:].zfill(64))) for tid in chunk]
        results = None
        for attempt in range(len(rpc_list)):
            rpc = rpc_list[(rpc_idx + attempt) % len(rpc_list)]
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": RPC_TIMEOUT_SECONDS}))
                multicall = w3.eth.contract(address=multicall_addr, abi=MULTICALL3_AGG3_ABI)
                results = multicall.functions.aggregate3(calls).call()
                rpc_usage.record_request("eth_call")  # one aggregate3 == one eth_call
                rpc_idx = (rpc_idx + attempt) % len(rpc_list)
                break
            except Exception:
                results = None
                continue
        if not results or len(results) != len(chunk):
            continue  # whole chunk unresolved -> per-call fallback handles it
        for tid, item in zip(chunk, results):
            data = bytes(item[1]) if item[1] is not None else b""
            if bool(item[0]) and len(data) >= 32:
                out[tid] = data
            # else: leave unresolved -> fallback (failed call, not a trusted zero)
    return out


def make_batch_call(token_ids):
    """Batch RPC call for locked(uint256) with RPC failover.
    Returns (results_dict, failed_ids) to distinguish errors from real zeros."""
    out = {}
    responded_ids = set()
    # Fast path: Multicall3 resolves most ids in one eth_call per chunk
    # (data-identical; ids it can't resolve fall through to the batch below).
    for tid, data in _multicall_vedolo_reads(token_ids, LOCKED_SELECTOR).items():
        amount_raw = int.from_bytes(data[0:32], "big")
        if amount_raw >= 2**127:
            amount_raw -= 2**128
        out[tid] = {"amount": amount_raw / 1e18, "end": int.from_bytes(data[32:64], "big")}
        responded_ids.add(tid)
    token_ids = [tid for tid in token_ids if tid not in responded_ids]
    if not token_ids:
        return out, []

    s = requests.Session()
    batch = []
    for i, tid in enumerate(token_ids):
        encoded = hex(tid)[2:].zfill(64)
        batch.append({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": VEDOLO_CONTRACT, "data": LOCKED_SELECTOR + encoded}, "latest"],
            "id": i
        })

    for rpc_url in RPC_URLS:
        for retry in range(RPC_RETRIES):
            try:
                resp = s.post(rpc_url, json=batch, timeout=RPC_TIMEOUT_SECONDS,
                              headers={"Content-Type": "application/json"})
                if resp.status_code == 429:
                    time.sleep(1 * (retry + 1))
                    continue
                resp.raise_for_status()
                results = resp.json()
                if not isinstance(results, list):
                    time.sleep(0.5 * (retry + 1))
                    continue
                for r in results:
                    idx = r.get("id", 0)
                    if idx < len(token_ids):
                        tid = token_ids[idx]
                        if "error" in r:
                            # RPC error (e.g. batch limit exceeded) — skip, don't set 0
                            pass
                        elif "result" in r and r["result"] and len(r["result"]) >= 66:
                            raw = r["result"]
                            amount_raw = int(raw[2:66], 16)
                            if amount_raw >= 2**127:
                                amount_raw -= 2**128
                            end_raw = int(raw[66:130], 16)
                            out[tid] = {"amount": amount_raw / 1e18, "end": end_raw}
                            responded_ids.add(tid)
                        else:
                            # Explicit zero result from RPC — genuinely no lock
                            out[tid] = {"amount": 0, "end": 0}
                            responded_ids.add(tid)
                failed = [tid for tid in token_ids if tid not in responded_ids]
                return out, failed
            except Exception as e:
                if retry < RPC_RETRIES - 1:
                    time.sleep(0.3 * (retry + 1))
        # If this RPC failed entirely, try next one
        if out:
            failed = [tid for tid in token_ids if tid not in responded_ids]
            return out, failed

    # Complete failure — all RPCs failed
    return out, list(token_ids)


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, CACHE_FILE)


CACHE_MAX_AGE = 86400  # 24 hours in seconds
VOTE_CACHE_MAX_AGE = 21600  # 6 hours — vote weights decay, need fresher data
FALLBACK_CACHE_MAX_AGE = 21600  # retry recent flow fallbacks sooner than RPC-verified cache
LOCKED_STALE_REFRESH_LIMIT = env_int("VEDOLO_LOCKED_STALE_REFRESH_LIMIT", 100)
LOCKED_FAILED_RETRY_LIMIT = env_int("VEDOLO_LOCKED_FAILED_RETRY_LIMIT", 20)
LOCKED_ZERO_RETRY_LIMIT = env_int("VEDOLO_LOCKED_ZERO_RETRY_LIMIT", 20)
VOTE_STALE_REFRESH_LIMIT = env_int("VEDOLO_VOTE_STALE_REFRESH_LIMIT", 100)
VOTE_FAILED_RETRY_LIMIT = env_int("VEDOLO_VOTE_FAILED_RETRY_LIMIT", 20)
VOTE_ZERO_RETRY_LIMIT = env_int("VEDOLO_VOTE_ZERO_RETRY_LIMIT", 20)
FLOW_FALLBACK_LOOKBACK_SECONDS = env_int("VEDOLO_FLOW_FALLBACK_LOOKBACK_SECONDS", 14 * 86400)


def dedupe_ids(*groups):
    out = []
    seen = set()
    for group in groups:
        for raw in group or []:
            try:
                tid = int(raw)
            except (TypeError, ValueError):
                continue
            if tid in seen:
                continue
            seen.add(tid)
            out.append(tid)
    return out


def oldest_cached_ids(token_ids, cache, timestamp_key, limit):
    if limit <= 0:
        return []
    return sorted(
        token_ids,
        key=lambda tid: cache.get(str(tid), {}).get(timestamp_key, 0)
    )[:limit]


def load_recent_flow_lock_fallbacks(path=None, now_ts=None, lookback_seconds=None):
    """Recent veDOLO lock rows used only when RPC has no cache for a fresh NFT."""
    path = path or os.path.join(DATA_DIR, "vedolo_flows.json")
    now_ts = int(now_ts or time.time())
    lookback_seconds = FLOW_FALLBACK_LOOKBACK_SECONDS if lookback_seconds is None else lookback_seconds
    min_ts = now_ts - lookback_seconds
    if not os.path.exists(path):
        return {}

    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as exc:
        print(f"  ⚠️ Could not read veDOLO flow fallback data: {exc}", flush=True)
        return {}

    fallbacks = {}
    for row in data.get("locks", []):
        try:
            tid = int(row.get("tokenId"))
            ts = int(row.get("timestamp") or 0)
            amount = float(row.get("dolo") or 0)
            end = int(row.get("locktime") or 0)
        except (TypeError, ValueError):
            continue
        if ts < min_ts or amount <= 0 or end <= 0:
            continue
        entry = fallbacks.setdefault(tid, {
            "amount": 0.0,
            "end": 0,
            "fetched_at": now_ts,
            "source": "vedolo_flows_recent_lock",
            "flow_timestamp": ts,
        })
        # Deposit events are incremental, so sum amounts and keep the latest end.
        entry["amount"] += amount
        entry["end"] = max(entry["end"], end)
        entry["flow_timestamp"] = max(entry["flow_timestamp"], ts)
    return fallbacks


def fetch_contract_dolo_balance():
    """Fetch the actual DOLO token balance held by the veDOLO contract (ground truth)."""
    padded_addr = VEDOLO_CONTRACT[2:].lower().zfill(64)
    for rpc_url in RPC_URLS:
        try:
            resp = requests.post(rpc_url, json={
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": DOLO_TOKEN, "data": BALANCE_OF_SELECTOR + padded_addr}, "latest"],
                "id": 1
            }, timeout=RPC_TIMEOUT_SECONDS)
            r = resp.json()
            if "result" in r and r["result"]:
                return int(r["result"], 16) / 1e18
        except Exception:
            continue
    return 0


def fetch_locked_dolo(all_token_ids, vote_weights=None, priority_token_ids=None, fallback_locks=None):
    """Fetch locked DOLO with bounded stale refresh so snapshots can publish.

    Missing/currently changed token IDs are always prioritized. Old stale cache
    is refreshed in a rotating sample; if RPC is degraded, existing cache remains
    usable and recent flow rows can fill brand-new tokens that have no cache yet.
    """
    print(f"\n🔒 Phase 2: Fetching locked DOLO for {len(all_token_ids):,} tokens...")

    cache = load_cache()
    now_ts = int(time.time())
    active_ids = set(all_token_ids)
    priority_ids = [tid for tid in dedupe_ids(priority_token_ids) if tid in active_ids]
    fallback_locks = fallback_locks or {}

    missing = []
    stale = []
    suspicious_zero = []
    for tid in all_token_ids:
        entry = cache.get(str(tid))
        if entry is None:
            missing.append(tid)
        elif entry.get("source") == "vedolo_flows_recent_lock" and now_ts - entry.get("fetched_at", 0) > FALLBACK_CACHE_MAX_AGE:
            stale.append(tid)
        elif now_ts - entry.get("fetched_at", 0) > CACHE_MAX_AGE:
            stale.append(tid)
        elif vote_weights and vote_weights.get(tid, 0) > 0 and entry.get("amount", 0) == 0:
            suspicious_zero.append(tid)

    priority_stale = [tid for tid in priority_ids if tid in stale]
    stale_limited = oldest_cached_ids(
        [tid for tid in stale if tid not in set(priority_stale)],
        cache,
        "fetched_at",
        LOCKED_STALE_REFRESH_LIMIT,
    )
    to_fetch = dedupe_ids(missing, suspicious_zero, priority_stale, stale_limited)
    to_fetch_set = set(to_fetch)
    deferred_stale = len([tid for tid in stale if tid not in to_fetch_set])

    print(f"  Cached: {len(all_token_ids) - len(missing):,}/{len(all_token_ids):,}")
    print(f"  New: {len(missing):,}  |  Stale (>24h): {len(stale):,}")
    if priority_stale:
        print(f"  Priority stale from recent flows: {len(priority_stale):,}")
    if deferred_stale:
        print(f"  Deferred stale refresh: {deferred_stale:,} (budget {LOCKED_STALE_REFRESH_LIMIT:,}/run)")
    if suspicious_zero:
        print(f"  🔍 Suspicious zeros (vote>0, dolo=0): {len(suspicious_zero):,}")
    print(f"  To fetch: {len(to_fetch):,}")

    timed_out = False
    if to_fetch:
        chunks = [to_fetch[i:i+BATCH_SIZE] for i in range(0, len(to_fetch), BATCH_SIZE)]
        errors = 0
        done = 0
        all_failed = []
        chunk_idx = 0

        while chunk_idx < len(chunks):
            window = chunks[chunk_idx:chunk_idx + MAX_WORKERS]

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(make_batch_call, c): ci for ci, c in enumerate(window)}
                for future in as_completed(futures):
                    results, failed = future.result()
                    for tid, data_item in results.items():
                        data_item["fetched_at"] = now_ts
                        cache[str(tid)] = data_item
                        done += 1
                    all_failed.extend(failed)
                    errors += len(failed)

            chunk_idx += len(window)
            if chunk_idx % 20 == 0 or chunk_idx >= len(chunks):
                pct = (done / len(to_fetch)) * 100
                print(f"  Progress: {pct:.0f}% ({done:,}/{len(to_fetch):,}) | Errors: {errors}", flush=True)
                save_cache(cache)
            if check_timeout("Phase2-locked"):
                save_cache(cache)
                timed_out = True
                break
            time.sleep(0.3)

        if all_failed and not timed_out:
            failed_set = set(all_failed)
            retry_ids = dedupe_ids(
                [tid for tid in missing if tid in failed_set],
                [tid for tid in priority_ids if tid in failed_set],
                all_failed,
            )[:LOCKED_FAILED_RETRY_LIMIT]
            skipped_retry = max(0, len(failed_set) - len(retry_ids))
            print(f"  ⚠️  {len(failed_set):,} tokens failed initial fetch; retrying {len(retry_ids):,}.")
            if skipped_retry:
                print(f"     Deferred failed retries: {skipped_retry:,} (budget {LOCKED_FAILED_RETRY_LIMIT:,}/run)")
            retry_chunks = [retry_ids[i:i+10] for i in range(0, len(retry_ids), 10)]
            fixed = 0
            for chunk in retry_chunks:
                if check_timeout("Phase2-failed-retry"):
                    timed_out = True
                    break
                results, still_failed = make_batch_call(chunk)
                for tid, data_item in results.items():
                    data_item["fetched_at"] = now_ts
                    cache[str(tid)] = data_item
                    fixed += 1
                time.sleep(0.2)
            print(f"  ✅ Retry fixed {fixed}/{len(retry_ids)} tokens.")

        fallback_applied = 0
        for tid, data_item in fallback_locks.items():
            if tid not in active_ids:
                continue
            existing = cache.get(str(tid))
            if existing and existing.get("amount", 0) > 0 and existing.get("end", 0) > 0:
                continue
            if tid not in missing and existing is not None:
                continue
            cache[str(tid)] = dict(data_item)
            fallback_applied += 1
        if fallback_applied:
            print(f"  🧩 Applied veDOLO flow fallback for {fallback_applied:,} fresh token(s).", flush=True)

        save_cache(cache)
        print(f"  ✅ Done. Errors: {errors}/{len(to_fetch):,}")
    else:
        print("  ✅ All cached & fresh!")

    our_total = sum(cache.get(str(tid), {}).get("amount", 0) for tid in all_token_ids)
    onchain_balance = fetch_contract_dolo_balance()

    if onchain_balance > 0:
        discrepancy_pct = abs(our_total - onchain_balance) / onchain_balance * 100
        print(f"\n  📊 Ground truth check:")
        print(f"     Our total:      {our_total:>16,.2f} DOLO")
        print(f"     On-chain:       {onchain_balance:>16,.2f} DOLO")
        print(f"     Discrepancy:    {discrepancy_pct:.2f}%")

        MAX_ROUNDS = 3
        TARGET_DISCREPANCY = 5.0

        for round_num in range(MAX_ROUNDS):
            if discrepancy_pct <= TARGET_DISCREPANCY:
                print(f"  ✅ Data accuracy within {TARGET_DISCREPANCY}% — good enough!")
                break

            zero_tokens = [tid for tid in all_token_ids
                           if cache.get(str(tid), {}).get("amount", 0) == 0]
            if not zero_tokens:
                print(f"  ℹ️  No zero-amount tokens left to retry")
                break

            zero_set = set(zero_tokens)
            retry_ids = dedupe_ids(
                [tid for tid in missing if tid in zero_set],
                [tid for tid in priority_ids if tid in zero_set],
                oldest_cached_ids(zero_tokens, cache, "fetched_at", LOCKED_ZERO_RETRY_LIMIT),
            )[:LOCKED_ZERO_RETRY_LIMIT]
            if not retry_ids:
                print("  ℹ️  Zero-token retry budget is 0; deferring convergence retry")
                break

            print(f"  🔄 Round {round_num + 1}/{MAX_ROUNDS}: Retrying {len(retry_ids):,}/{len(zero_tokens):,} zero-amount tokens...")
            retry_chunks = [retry_ids[i:i+5] for i in range(0, len(retry_ids), 5)]
            fixed = 0
            for chunk in retry_chunks:
                if check_timeout("Phase2-convergence"):
                    timed_out = True
                    break
                results, failed = make_batch_call(chunk)
                for tid, data_item in results.items():
                    if data_item.get("amount", 0) > 0:
                        data_item["fetched_at"] = now_ts
                        cache[str(tid)] = data_item
                        fixed += 1
                time.sleep(0.25)

            our_total = sum(cache.get(str(tid), {}).get("amount", 0) for tid in all_token_ids)
            discrepancy_pct = abs(our_total - onchain_balance) / onchain_balance * 100
            print(f"     Fixed {fixed:,} tokens. New total: {our_total:,.2f} DOLO ({discrepancy_pct:.2f}% off)", flush=True)
            save_cache(cache)

            if timed_out or check_timeout("Phase2-convergence"):
                break

            if fixed == 0:
                print(f"  ⏳ No progress this round — waiting 3s before next attempt...")
                time.sleep(3)
        else:
            if discrepancy_pct > TARGET_DISCREPANCY:
                print(f"  ⚠️  Still {discrepancy_pct:.1f}% off after {MAX_ROUNDS} rounds")
    else:
        print(f"  ⚠️  Could not fetch on-chain DOLO balance for ground truth check")

    return cache


def make_vote_batch_call(token_ids):
    """True JSON-RPC batch call for balanceOfNFT(uint256).
    Returns (results_dict, failed_ids) to distinguish real zeros from errors."""
    out = {}
    responded_ids = set()
    # Fast path: Multicall3 resolves most ids in one eth_call per chunk
    # (data-identical; ids it can't resolve fall through to the batch below).
    for tid, data in _multicall_vedolo_reads(token_ids, BALANCE_OF_NFT_SELECTOR).items():
        out[tid] = int.from_bytes(data[:32], "big") / 1e18
        responded_ids.add(tid)
    token_ids = [tid for tid in token_ids if tid not in responded_ids]
    if not token_ids:
        return out, []

    s = requests.Session()
    batch = []
    for i, tid in enumerate(token_ids):
        encoded = hex(tid)[2:].zfill(64)
        batch.append({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": VEDOLO_CONTRACT, "data": BALANCE_OF_NFT_SELECTOR + encoded}, "latest"],
            "id": i
        })

    for rpc_url in RPC_URLS:
        for retry in range(RPC_RETRIES):
            try:
                resp = s.post(rpc_url, json=batch, timeout=RPC_TIMEOUT_SECONDS,
                              headers={"Content-Type": "application/json"})
                if resp.status_code == 429:
                    time.sleep(1 * (retry + 1))
                    continue
                resp.raise_for_status()
                results = resp.json()
                if not isinstance(results, list):
                    time.sleep(0.5 * (retry + 1))
                    continue
                for r in results:
                    idx = r.get("id", 0)
                    if idx < len(token_ids):
                        tid = token_ids[idx]
                        if "result" in r and r["result"] and len(r["result"]) > 2:
                            val = int(r["result"], 16)
                            out[tid] = val / 1e18
                            responded_ids.add(tid)
                        elif "error" in r:
                            # RPC error — don't set 0, mark as failed
                            pass
                        else:
                            # Empty result — could be genuinely 0 or an error
                            out[tid] = 0.0
                            responded_ids.add(tid)
                failed = [tid for tid in token_ids if tid not in responded_ids]
                # Only return if we got at least some valid responses;
                # if ALL items errored (e.g. batch limit exceeded), try next RPC
                if responded_ids:
                    return out, failed
                else:
                    break  # All items errored on this RPC, try next one
            except Exception as e:
                if retry < RPC_RETRIES - 1:
                    time.sleep(0.5 * (retry + 1))
        # If this RPC got partial results, return them
        if responded_ids:
            failed = [tid for tid in token_ids if tid not in responded_ids]
            return out, failed

    # Complete failure — all RPCs failed
    return out, list(token_ids)


def fetch_vote_weights(all_token_ids, locked_cache=None, priority_token_ids=None):
    """Fetch current vote weights for all tokens using true JSON-RPC batch calls.
    Uses locked_cache to store/retrieve cached vote weights (key: 'vote_weight', 'vote_fetched_at').
    Re-fetches missing weights plus a bounded stale sample so snapshots publish."""
    print(f"\n⚖️  Phase 3: Fetching vote weights for {len(all_token_ids):,} tokens...")

    now_ts = int(time.time())
    vote_weights = {}
    active_ids = set(all_token_ids)
    priority_ids = [tid for tid in dedupe_ids(priority_token_ids) if tid in active_ids]
    missing = []
    stale = []

    # Check cache for existing vote weights
    if locked_cache:
        for tid in all_token_ids:
            entry = locked_cache.get(str(tid))
            if entry and "vote_weight" in entry:
                vote_weights[tid] = entry["vote_weight"]
                vote_age = now_ts - entry.get("vote_fetched_at", 0)
                if vote_age > VOTE_CACHE_MAX_AGE:
                    stale.append(tid)
            else:
                missing.append(tid)
    else:
        missing = list(all_token_ids)

    priority_stale = [tid for tid in priority_ids if tid in stale]
    stale_limited = oldest_cached_ids(
        [tid for tid in stale if tid not in set(priority_stale)],
        locked_cache or {},
        "vote_fetched_at",
        VOTE_STALE_REFRESH_LIMIT,
    )
    to_fetch = dedupe_ids(missing, priority_stale, stale_limited)
    deferred_stale = len([tid for tid in stale if tid not in set(to_fetch)])

    print(f"  Cached: {len(vote_weights):,}/{len(all_token_ids):,}")
    print(f"  Missing: {len(missing):,}  |  Stale (>6h): {len(stale):,}")
    if priority_stale:
        print(f"  Priority stale vote weights: {len(priority_stale):,}")
    if deferred_stale:
        print(f"  Deferred stale vote refresh: {deferred_stale:,} (budget {VOTE_STALE_REFRESH_LIMIT:,}/run)")
    print(f"  To fetch: {len(to_fetch):,}")

    if not to_fetch:
        print("  ✅ Vote weights available from cache!")
        for tid in all_token_ids:
            if tid not in vote_weights:
                vote_weights[tid] = 0.0
        return vote_weights

    all_failed = []
    fetched_vote_ids = set()
    chunks = [to_fetch[i:i+BATCH_SIZE] for i in range(0, len(to_fetch), BATCH_SIZE)]
    done = 0
    chunk_idx = 0
    timed_out = False

    while chunk_idx < len(chunks):
        window = chunks[chunk_idx:chunk_idx + MAX_WORKERS]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(make_vote_batch_call, c): ci for ci, c in enumerate(window)}
            for future in as_completed(futures):
                results, failed = future.result()
                for tid, weight in results.items():
                    vote_weights[tid] = weight
                    fetched_vote_ids.add(tid)
                    done += 1
                all_failed.extend(failed)

        chunk_idx += len(window)
        pct = (done / len(to_fetch)) * 100 if to_fetch else 100
        print(f"  Progress: {pct:.0f}% ({done:,}/{len(to_fetch):,})")
        if check_timeout("Phase3-votes"):
            timed_out = True
            break
        time.sleep(0.1)

    # --- Retry 1: retry all tokens that failed the initial pass ---
    if all_failed and not timed_out:
        failed_set = set(all_failed)
        retry_ids = dedupe_ids(
            [tid for tid in missing if tid in failed_set],
            [tid for tid in priority_ids if tid in failed_set],
            all_failed,
        )[:VOTE_FAILED_RETRY_LIMIT]
        skipped_retry = max(0, len(failed_set) - len(retry_ids))
        print(f"  ⚠️  {len(failed_set):,} tokens failed initial vote fetch; retrying {len(retry_ids):,}.")
        if skipped_retry:
            print(f"     Deferred failed vote retries: {skipped_retry:,} (budget {VOTE_FAILED_RETRY_LIMIT:,}/run)")
        retry_chunks = [retry_ids[i:i+25] for i in range(0, len(retry_ids), 25)]
        for chunk in retry_chunks:
            if check_timeout("Phase3-failed-retry"):
                timed_out = True
                break
            results, still_failed = make_vote_batch_call(chunk)
            for tid, weight in results.items():
                vote_weights[tid] = weight
                fetched_vote_ids.add(tid)
            time.sleep(0.2)

    # --- Retry 2: validate against locked DOLO data ---
    # Any token with active lock (dolo > 0, end > now) but vote_weight = 0 is suspicious
    now_ts = int(time.time())
    suspicious = []
    if locked_cache:
        for tid in all_token_ids:
            ld = locked_cache.get(str(tid), {"amount": 0, "end": 0})
            amt = ld.get("amount", 0)
            end = ld.get("end", 0)
            vw = vote_weights.get(tid, 0)
            if amt > 0 and end > now_ts and vw == 0:
                suspicious.append(tid)

    if suspicious:
        print(f"  🔍 {len(suspicious)} tokens have active locks but 0 vote weight — retrying...")
        # Convergence-based retry: keep going until no more progress
        MAX_VOTE_ROUNDS = 3
        for round_num in range(MAX_VOTE_ROUNDS):
            zero_vote_active = [tid for tid in all_token_ids
                                if locked_cache.get(str(tid), {"amount": 0, "end": 0}).get("amount", 0) > 0
                                and locked_cache.get(str(tid), {"amount": 0, "end": 0}).get("end", 0) > now_ts
                                and vote_weights.get(tid, 0) == 0]
            if not zero_vote_active:
                break
            zero_set = set(zero_vote_active)
            retry_ids = dedupe_ids(
                [tid for tid in missing if tid in zero_set],
                [tid for tid in priority_ids if tid in zero_set],
                oldest_cached_ids(zero_vote_active, locked_cache or {}, "vote_fetched_at", VOTE_ZERO_RETRY_LIMIT),
            )[:VOTE_ZERO_RETRY_LIMIT]
            if not retry_ids:
                print("  ℹ️  Zero-vote retry budget is 0; deferring convergence retry")
                break
            print(f"  🔄 Vote round {round_num + 1}/{MAX_VOTE_ROUNDS}: Retrying {len(retry_ids):,}/{len(zero_vote_active):,} zero-vote tokens...")
            retry_chunks = [retry_ids[i:i+5] for i in range(0, len(retry_ids), 5)]
            fixed = 0
            for chunk in retry_chunks:
                if check_timeout("Phase3-convergence"):
                    timed_out = True
                    break
                for rpc_url in RPC_URLS:
                    results, failed = _single_rpc_vote_batch(chunk, rpc_url)
                    for tid, weight in results.items():
                        if weight > 0:
                            vote_weights[tid] = weight
                            fetched_vote_ids.add(tid)
                            fixed += 1
                        elif tid not in vote_weights:
                            vote_weights[tid] = weight
                            fetched_vote_ids.add(tid)
                    if not failed:
                        break
                time.sleep(0.25)
            print(f"     Fixed {fixed}/{len(retry_ids)} tokens.")
            if fixed == 0:
                print(f"  ⏳ No progress — waiting 3s before next attempt...", flush=True)
                time.sleep(3)
            if timed_out or check_timeout("Phase3-convergence"):
                break

    # Fill any still-missing tokens with 0
    for tid in all_token_ids:
        if tid not in vote_weights:
            vote_weights[tid] = 0.0

    print(f"  ✅ Done. {len(vote_weights):,} vote weights fetched.")

    # Save vote weights back to locked_cache for next run
    if locked_cache:
        for tid, vw in vote_weights.items():
            key = str(tid)
            if key not in locked_cache:
                locked_cache[key] = {}
            locked_cache[key]["vote_weight"] = vw
            if tid in fetched_vote_ids or "vote_fetched_at" not in locked_cache[key]:
                locked_cache[key]["vote_fetched_at"] = now_ts
        save_cache(locked_cache)
        print(f"  💾 Vote weights saved to cache")

    return vote_weights


def _single_rpc_vote_batch(token_ids, rpc_url):
    """Small batch call to a specific RPC URL, with careful error handling."""
    s = requests.Session()
    batch = []
    for i, tid in enumerate(token_ids):
        encoded = hex(tid)[2:].zfill(64)
        batch.append({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": VEDOLO_CONTRACT, "data": BALANCE_OF_NFT_SELECTOR + encoded}, "latest"],
            "id": i
        })

    out = {}
    responded = set()
    for retry in range(RPC_RETRIES):
        try:
            resp = s.post(rpc_url, json=batch, timeout=RPC_TIMEOUT_SECONDS,
                          headers={"Content-Type": "application/json"})
            if resp.status_code == 429:
                time.sleep(2 * (retry + 1))
                continue
            resp.raise_for_status()
            results = resp.json()
            if not isinstance(results, list):
                time.sleep(1 * (retry + 1))
                continue
            for r in results:
                idx = r.get("id", 0)
                if idx < len(token_ids):
                    tid = token_ids[idx]
                    if "result" in r and r["result"] and len(r["result"]) > 2:
                        out[tid] = int(r["result"], 16) / 1e18
                        responded.add(tid)
                    elif "error" not in r:
                        out[tid] = 0.0
                        responded.add(tid)
            failed = [tid for tid in token_ids if tid not in responded]
            return out, failed
        except Exception:
            if retry < RPC_RETRIES - 1:
                time.sleep(1 * (retry + 1))
    return out, [tid for tid in token_ids if tid not in responded]


# ===== MAIN =====

def main():
    print("=" * 60)
    print("🔄 veDOLO Dashboard — Data Update (Etherscan V2)")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Phase 1: Fetch all NFT transfers
    try:
        txs = fetch_all_nft_transfers()
    except IncompleteNftTransferFetch as exc:
        preserve_existing_vedolo_snapshot(str(exc))

    if not txs:
        print("⚠️  No transfers found! Keeping existing data.")
        sys.exit(0)

    holders, stats = build_ownership(txs)
    stats.update(nft_transfer_coverage_stats(txs))

    ownership_issues = ownership_snapshot_issues(stats)
    if ownership_issues:
        preserve_existing_vedolo_snapshot(
            "veDOLO ownership snapshot looks incomplete: " + "; ".join(ownership_issues)
        )

    if not holders:
        print("⚠️  No holders found!")
        sys.exit(0)

    # Collect all active token IDs
    all_token_ids = sorted({tid for h in holders for tid in h["token_ids"]})
    recent_lock_fallbacks = load_recent_flow_lock_fallbacks()
    recent_lock_ids = [tid for tid in recent_lock_fallbacks if tid in set(all_token_ids)]
    if recent_lock_ids:
        print(f"\n🧭 Recent veDOLO flow hints: {len(recent_lock_ids):,} active token(s)")

    # Phase 2: Fetch locked DOLO FIRST (cached — more reliable)
    cache = fetch_locked_dolo(
        all_token_ids,
        priority_token_ids=recent_lock_ids,
        fallback_locks=recent_lock_fallbacks,
    )

    # Phase 3: Fetch vote weights (uses locked cache to cross-validate zeros)
    vote_weights = fetch_vote_weights(
        all_token_ids,
        locked_cache=cache,
        priority_token_ids=recent_lock_ids,
    )

    # Merge locked DOLO + vote weights into holders
    print("\n📊 Merging data...")
    total_locked_dolo = 0
    total_vote_weight = 0
    for holder in holders:
        holder_dolo = 0
        holder_vote = 0
        token_details = []
        earliest_end = float('inf')
        latest_end = 0

        for tid in holder["token_ids"]:
            ld = cache.get(str(tid), {"amount": 0, "end": 0})
            amt = ld.get("amount", 0)
            end = ld.get("end", 0)
            vw = vote_weights.get(tid, 0)
            holder_dolo += amt
            holder_vote += vw
            if end > 0:
                earliest_end = min(earliest_end, end)
                latest_end = max(latest_end, end)
            token_details.append({"id": tid, "dolo": round(amt, 2), "end": end, "vote_weight": round(vw, 4)})

        holder["total_dolo"] = round(holder_dolo, 2)
        holder["total_vote_weight"] = round(holder_vote, 4)
        holder["earliest_lock_end"] = earliest_end if earliest_end != float('inf') else 0
        holder["latest_lock_end"] = latest_end
        holder["token_details"] = token_details
        total_locked_dolo += holder_dolo
        total_vote_weight += holder_vote

    # Sort & rank
    holders.sort(key=lambda h: h["total_dolo"], reverse=True)
    for i, h in enumerate(holders, 1):
        h["rank"] = i

    # Checksum addresses
    try:
        from web3 import Web3
        for h in holders:
            try:
                h["address"] = Web3.to_checksum_address(h["address"])
            except Exception:
                pass
    except ImportError:
        pass

    stats["total_locked_dolo"] = round(total_locked_dolo, 2)
    stats["total_vote_weight"] = round(total_vote_weight, 4)
    apply_canonical_vote_weight(stats, fetch_canonical_snapshot())
    total_vote_weight = stats["total_vote_weight"]

    # ===== GROUND TRUTH: Use on-chain balanceOf as definitive DOLO Locked =====
    # Individual locked() calls may fail (RPC rate limits), but balanceOf is a
    # single call that always returns the exact contract balance.
    onchain_locked = fetch_contract_dolo_balance()
    if onchain_locked > 0:
        print(f"\n  📊 On-chain DOLO balance (ground truth): {onchain_locked:,.2f}")
        print(f"     Per-holder sum (approximate):          {total_locked_dolo:,.2f}")
        diff_pct = abs(total_locked_dolo - onchain_locked) / onchain_locked * 100
        print(f"     Difference: {diff_pct:.1f}%")
        stats["total_locked_dolo"] = round(onchain_locked, 2)
        total_locked_dolo = onchain_locked
    else:
        print(f"\n  ⚠️  Could not fetch on-chain balance — using per-holder sum as fallback")

    # ===== DATA PROTECTION: Don't overwrite good stats with corrupted data =====
    # Guard against both total zero AND suspicious drops (>50% decline = likely RPC failure)
    try:
        if os.path.exists(OUTPUT_JSON):
            with open(OUTPUT_JSON) as f:
                prev = json.load(f)
            prev_locked = prev.get("stats", {}).get("total_locked_dolo", 0)
            if prev_locked > 0:
                drop_pct = (1 - total_locked_dolo / prev_locked) * 100 if prev_locked > 0 else 0
                if total_locked_dolo == 0 or drop_pct > 50:
                    print(f"\n⚠️  WARNING: total_locked_dolo dropped {drop_pct:.1f}%")
                    print(f"   Previous: {prev_locked:,.2f}  New: {total_locked_dolo:,.2f}")
                    print(f"   This likely means RPC calls failed. Preserving previous locked DOLO stats.")
                    stats["total_locked_dolo"] = prev_locked
                    stats["_stale_data"] = True
                    total_locked_dolo = prev_locked
    except Exception as e:
        print(f"   ⚠️ Could not read previous data: {e}")

    output = {
        "contract": VEDOLO_CONTRACT,
        "network": "berachain",
        "timestamp": datetime.utcnow().isoformat(),
        "stats": stats,
        "holders": holders,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    # Save a tiny stats-only JSON for fast frontend loading (avoids parsing 5MB+ file)
    stats_file = os.path.join(DATA_DIR, "vedolo_stats.json")
    with open(stats_file, "w") as f:
        json.dump({"stats": stats, "timestamp": datetime.utcnow().isoformat()}, f, indent=2)

    # Save pre-computed expiry buckets for instant Lock Expiry Timeline chart
    now_ts = int(time.time())
    expiry_buckets = {}
    expiry_total = 0
    for h in holders:
        for td in h.get("token_details", []):
            end = td.get("end", 0)
            dolo = td.get("dolo", 0)
            if end and dolo > 0 and end > now_ts:
                dt = datetime.utcfromtimestamp(end)
                q = f"Q{(dt.month - 1) // 3 + 1} {dt.year}"
                expiry_buckets[q] = expiry_buckets.get(q, 0) + dolo
                expiry_total += dolo
    # Sort by year+quarter
    sorted_expiry = sorted(expiry_buckets.items(),
                           key=lambda x: (int(x[0].split()[1]), int(x[0][1])))
    expiry_file = os.path.join(DATA_DIR, "vedolo_expiry.json")
    with open(expiry_file, "w") as f:
        json.dump({
            "buckets": [{"label": k, "dolo": round(v, 2)} for k, v in sorted_expiry],
            "total_dolo": round(expiry_total, 2),
            "timestamp": datetime.utcnow().isoformat()
        }, f, indent=2)
    print(f"   Saved vedolo_expiry.json ({len(sorted_expiry)} quarters, {expiry_total:,.0f} DOLO)")

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Rank", "Address", "NFT_Count", "Total_DOLO", "Vote_Weight",
                         "Earliest_Lock_End", "Latest_Lock_End", "Token_IDs"])
        for h in holders:
            writer.writerow([
                h["rank"], h["address"], h["nft_count"], h["total_dolo"],
                h.get("total_vote_weight", 0),
                datetime.utcfromtimestamp(h["earliest_lock_end"]).strftime('%Y-%m-%d') if h["earliest_lock_end"] > 0 else "",
                datetime.utcfromtimestamp(h["latest_lock_end"]).strftime('%Y-%m-%d') if h["latest_lock_end"] > 0 else "",
                ";".join(str(t) for t in h["token_ids"])
            ])

    print(f"\n💾 Saved: vedolo_holders.json + .csv")
    print(f"   Locked DOLO: {total_locked_dolo:,.2f}")
    print(f"   Vote Weight: {total_vote_weight:,.2f}")
    print(f"   Holders: {len(holders):,}")

    print(f"\n🏆 TOP 5:")
    for h in holders[:5]:
        print(f"   #{h['rank']:<4} {h['address'][:12]}… {h['nft_count']:>4} NFT  {h['total_dolo']:>14,.2f} DOLO  {h.get('total_vote_weight',0):>12,.2f} veDOLO")

    print("\n✅ Update complete!")

    # Auto-generate dolo_price.json for GitHub Pages (no CORS proxy needed)
    update_dolo_price()

    # Save metrics snapshot for 24h change indicators
    save_metrics_snapshot()


def update_dolo_price():
    """Refresh dolo_price.json by delegating to the standalone updater.

    update_dolo_price.py (run hourly by update-dolo-price.yml) is the single
    source of truth for the payload schema and guards: it preserves previous
    supply/FDV fields when the CoinGecko detail endpoint fails (e.g. 429) and
    refuses to write a non-positive price. Delegating keeps both workflows
    writing an identical file instead of this function zeroing supply fields.
    """
    print("\n💰 Updating dolo_price.json...")
    try:
        import update_dolo_price as dolo_price_module
        dolo_price_module.main()
    except Exception as e:
        # Keep the previous dolo_price.json on failure; the hourly
        # update-dolo-price.yml workflow will refresh it soon anyway.
        print(f"   ⚠️ dolo_price.json update failed: {e}")


def save_metrics_snapshot():
    """Append current metrics to metrics_snapshot.json for 24h change tracking."""
    print("\n📸 Saving metrics snapshot...")
    snapshot_file = os.path.join(DATA_DIR, "metrics_snapshot.json")
    MAX_SNAPSHOTS = 48  # 48 hours of hourly snapshots

    try:
        # Read current metrics from existing data files
        snapshot = {"timestamp": datetime.utcnow().isoformat() + "Z"}

        # veDOLO stats
        vedolo_file = os.path.join(DATA_DIR, "vedolo_holders.json")
        if os.path.exists(vedolo_file):
            with open(vedolo_file) as f:
                vedolo = json.load(f)
            stats = vedolo.get("stats", {})
            snapshot["vedolo_holders"] = stats.get("unique_holders", 0)
            snapshot["dolo_locked"] = stats.get("total_locked_dolo", 0)
            snapshot["vote_weight"] = stats.get("total_vote_weight", 0)

        # oDOLO exercised
        exercised_file = os.path.join(DATA_DIR, "exercised_usd.json")
        if os.path.exists(exercised_file):
            with open(exercised_file) as f:
                ex = json.load(f)
            snapshot["exercised_usd"] = ex.get("total_usdc", 0)
            snapshot["exercised_txs"] = ex.get("total_txs", 0)

        # TVL from DeFi Llama
        defillama_file = os.path.join(DATA_DIR, "defillama_data.json")
        if os.path.exists(defillama_file):
            with open(defillama_file) as f:
                dl = json.load(f)
            tvl_arr = dl.get("tvl", [])
            if tvl_arr:
                snapshot["tvl"] = tvl_arr[-1].get("totalLiquidityUSD", 0)

        # 24h Volume from dolo_price
        price_file = os.path.join(DATA_DIR, "dolo_price.json")
        if os.path.exists(price_file):
            with open(price_file) as f:
                dp = json.load(f)
            snapshot["volume_24h"] = dp.get("volume_24h", 0)

        # Load existing snapshots
        snapshots = []
        if os.path.exists(snapshot_file):
            with open(snapshot_file) as f:
                data = json.load(f)
            snapshots = data.get("snapshots", [])

        # Append new snapshot and trim old ones
        snapshots.append(snapshot)
        cutoff = (datetime.utcnow() - timedelta(hours=MAX_SNAPSHOTS)).isoformat() + "Z"
        snapshots = [s for s in snapshots if s.get("timestamp", "") >= cutoff]

        with open(snapshot_file, "w") as f:
            json.dump({"snapshots": snapshots}, f, indent=2)

        print(f"   Saved snapshot ({len(snapshots)} total, trimmed to {MAX_SNAPSHOTS}h)")
    except Exception as e:
        print(f"   ⚠️ metrics_snapshot.json update failed: {e}")


if __name__ == "__main__":
    main()
