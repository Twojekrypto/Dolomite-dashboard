#!/usr/bin/env python3
"""
DOLO Token Flows — Top Accumulators & Sellers (1d / 7d / 30d)
Fetches ERC-20 Transfer events via eth_getLogs for ETH and Berachain,
calculates net inflow/outflow per address, outputs top 5 each.
"""
import json, time, os, sys, signal, re, shutil, subprocess
import requests
from datetime import datetime, timedelta, timezone

from rpc_client import (
    RpcError,
    decode_uint256,
    get_endpoints as _rpc_endpoints,
    rpc_batch_requests,
    rpc_single_request,
)

import rpc_usage

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "").strip()
BERASCAN_API_KEY = os.environ.get("BERASCAN_API_KEY", "").strip()

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
# Multicall3 (same address on every EVM chain) batches many balanceOf reads into
# ONE eth_call. JSON-RPC batching does NOT cut compute units; Multicall3 does.
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
TOP_N = 100
FLOW_SKIP_ADDRS = {
    ZERO,
    DOLO_CONTRACT,
    "0x0000000000000000000000000000000000000001",
    "0x3e9b9a16743551da49b5e136c716bba7932d2cec",  # oDOLO Vester
}
BRIDGE_ADDRS = {
    ZERO,
    DOLO_CONTRACT,
    "0x0000000000000000000000000000000000000001",
}
HOLDER_BUCKET_GROUPS = {
    "whales": [
        {"key": "1mplus", "min": 1_000_000, "max": float("inf")},
        {"key": "500k1m", "min": 500_000, "max": 1_000_000},
        {"key": "100k500k", "min": 100_000, "max": 500_000},
    ],
    "smaller": [
        {"key": "50k100k", "min": 50_000, "max": 100_000},
        {"key": "10k50k", "min": 10_000, "max": 50_000},
        {"key": "1k10k", "min": 1_000, "max": 10_000},
    ],
}
HOLDER_WALLET_HISTORY_VIEWS = {"whales"}
HOLDER_MARKET_EXCLUDED_TYPES = {"cex", "ca", "team", "investor"}
HOLDER_POTENTIAL_TYPES = {"watch", "mm", "bot"}

# Known contract addresses to exclude (DEX routers, LP pools, bots, etc.)
EXCLUDED_ADDRS = {
    ZERO,
    DOLO_CONTRACT,
    "0x0000000000000000000000000000000000000001",
    # --- Berachain contracts (verified via eth_getCode, 2026-03-06) ---
    # oDOLO Vester
    "0x3e9b9a16743551da49b5e136c716bba7932d2cec",
    # Kodiak DEX
    "0x43dac637c4383f91b4368041e7a8687da3806cae",
    # Protocol contracts
    "0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0",
    "0x596384bdffc9f563b53791aeec50a42ff51c3e42",
    "0x27f66ba3fda600239f48526bb26a1f8d5700ccf7",
    "0xa575f37e869e6887564f87c07e2885e08d542c4a",
    # DEX swap router
    "0xbedfac7488dccaafdd66d1d7d56349780fe0477e",
    # Contract (proxy)
    "0xf909c4ae16622898b885b89d7f839e0244851c66",
    # LP/router contracts
    "0x7ab286e9da6b5a1c80664b382092a8a4b91c276c",
    "0x16f13296c85c308b37bae567284e62b4c21a1ee9",
    "0xf5042e6ffac5a625d4e7848e0b01373d8eb9e222",
    "0x36f4e1803f6ff34562db567f347dea00dec87246",
    # DEX/aggregator contracts
    "0x12622dae56ec7a25f6cfeb96db88651c5bf7861d",
    "0x089b95152253b6af73e7f7267d749058d56ce231",
    "0x8430e3574eeb85b39b053b4022cfa27f951f48c7",
    "0x8c7ba8f245aef3216698087461e05b85483f791f",
    "0x062a2b0eea575f659a1aaf18c1df5d93e0528245",
    # Bot/aggregator contracts
    "0x893785e5c2a4ccfe0790e580c8e4ef363fabde1e",
    "0x4be03f781c497a489e3cb0287833452ca9b9e80b",
    "0x221dd2bb8b25f5e46b00c174b0111d383eb5c0bc",
    "0x71355972c9e332f73ff6921f9b3a02f349ff9752",
    # Protocol/other contracts
    "0x4fe93ebc4ce6ae4f81601cc7ce7139023919e003",
    "0x08b14bb09ac4819c16f68d7c92f7dcc20750eaff",
    "0x74d09665900a5f29bac25befd30c73a5962d44e7",
    # Bots / Market Makers (verified 2026-03-19)
    "0x5a6f918fcda24e9b5143f3a1b77e63df6de30f74",  # EOA bot, 51k nonce
    "0x6a2383cff0d46d2b7d29759f17c26fba726f3ea3",  # EOA bot, 35k nonce
    "0x278d858f05b94576c1e6f73285886876ff6ef8d2",  # Contract bot, 53k DOLO txs
    "0xf10f81795b359f8a72682cc2a39444bf818ef4ca",  # EOA Bot/MM, 13.9k nonce, 1.9k DOLO transfer rows (2026-06-24)
    "0x9e7728077f753dfdf53c2236097e27c743890992",  # DEX/router contract, 327M throughput
    # --- MM / CEX relay cluster (verified 2026-03-24) ---
    "0x0002810d2b1d621f3ae6c8a7af9e2f09efa1f8bb",  # MM relay: receives DOLO from CEX → sends to bridge
    "0x81879c14fe0efd4c8f6a99a34ce414190be8dbab",  # Bridge relay: CCIP bridges DOLO ETH→Bera
    "0x67790d0eaea043330be5415c0b512d8e0a2ab5c2",  # CEX hot wallet (nonce 3400+), USDT/USDC distributor
    # --- ETH DEX router ---
    "0xbdb3ba9ffe392549e1f8658dd2630c141fdf47b6",  # DEX aggregator/router (nonce 1.3M+)
}

CHAINS = {
    "eth": {
        "name": "Ethereum",
        "rpcs": _rpc_endpoints("ethereum"),
        "block_time": 12,   # ~12 seconds per block
        "chunk_size": 50_000,
        "deploy_block": 21_500_000,  # DOLO deployed ~Jan 2025
    },
    "bera": {
        "name": "Berachain",
        "rpcs": _rpc_endpoints("berachain"),
        "block_time": 2,    # ~2 seconds per block
        "chunk_size": 100_000,  # Berachain: keep ranges small enough to avoid dropped log chunks
        "deploy_block": 2_900_000,   # DOLO deployed on Berachain ~block 2,925,727 (Mar 2025)
    },
}

# Re-read one normal getLogs chunk on every incremental run and replace that
# cached range authoritatively. This repairs silent checkpoint holes (including
# a previously skipped single-block range) without duplicating transfer rows.
RECENT_RESCAN_BLOCKS = {"eth": 50_000, "bera": 100_000}

PERIODS = {
    "1d": 86400,
    "7d": 86400 * 7,
    "30d": 86400 * 30,
    "90d": 86400 * 90,
    "180d": 86400 * 180,
    # Wide enough to pin the cutoff to each chain's deploy block. This keeps
    # the holder-bucket chart's All range at DOLO inception instead of a rolling
    # yearly window.
    "all": 86400 * 365 * 10,
}
FRESH_HOLDER_PERIODS = ("1d", "7d", "30d", "90d")
FRESH_HOLDER_MIN_RECEIVED = 0.000001
FRESH_HOLDER_MIN_EXPOSURE = 10_000.0
FRESH_WALLET_ACTIVITY_CACHE_KEY = "fresh_wallet_activity_cache"
FRESH_WALLET_NO_ACTIVITY_CACHE_SECONDS = 6 * 3600
ETHERSCAN_V2_API = "https://api.etherscan.io/v2/api"
BERACHAIN_EXPLORER_API = "https://api.routescan.io/v2/network/mainnet/evm/80094/etherscan/api"
FRESH_ETHERSCAN_ACTIVITY_CHAINS = [
    {"key": "eth", "name": "Ethereum", "chainid": 1},
    {"key": "arb", "name": "Arbitrum", "chainid": 42161},
    {"key": "base", "name": "Base", "chainid": 8453},
    {"key": "op", "name": "Optimism", "chainid": 10},
    {"key": "polygon", "name": "Polygon", "chainid": 137},
    {"key": "bsc", "name": "BNB Chain", "chainid": 56},
    {"key": "avax", "name": "Avalanche", "chainid": 43114},
    {"key": "mantle", "name": "Mantle", "chainid": 5000},
    {"key": "linea", "name": "Linea", "chainid": 59144},
    {"key": "scroll", "name": "Scroll", "chainid": 534352},
    {"key": "zksync", "name": "zkSync Era", "chainid": 324},
]
FRESH_WALLET_ACTIVITY_SOURCES = [
    {**source, "provider": "etherscan"} for source in FRESH_ETHERSCAN_ACTIVITY_CHAINS
] + [
    {"key": "bera", "name": "Berachain", "provider": "routescan"},
]
FRESH_ETHERSCAN_REQUEST_DELAY_SECONDS = 0.22
FRESH_WALLET_AUDIT_VERBOSE = os.getenv("FRESH_WALLET_AUDIT_VERBOSE", "").lower() in {"1", "true", "yes"}
FRESH_DEBANK_AGE_FALLBACK = os.getenv("FRESH_DEBANK_AGE_FALLBACK", "1").lower() not in {"0", "false", "no"}
FRESH_DEBANK_AGE_CACHE_KEY = "fresh_wallet_debank_age_cache"
FRESH_DEBANK_AGE_CACHE_SECONDS = 7 * 86400
FRESH_DEBANK_FAILURE_CACHE_SECONDS = 6 * 3600
FRESH_DEBANK_HEADLESS_TIMEOUT_SECONDS = int(os.getenv("FRESH_DEBANK_HEADLESS_TIMEOUT_SECONDS", "75"))
FRESH_DEBANK_VIRTUAL_TIME_BUDGET_MS = int(os.getenv("FRESH_DEBANK_VIRTUAL_TIME_BUDGET_MS", "12000"))
FRESH_DEBANK_CHROME_BIN = os.getenv("FRESH_DEBANK_CHROME_BIN", "").strip()
# Single source of truth for Safe singleton addresses (all versions).
from safe_wallets import SAFE_SINGLETON_ADDRS

# veDOLO contract — pooled locks live here; never treat it as a user wallet.
VEDOLO_CONTRACT_ADDR = "0xcb86b75ee6133d179a12d550b09fb3cdb1e141d4"

# Direct sends of at least this many DOLO to a labeled CEX wallet flag the
# sender as a potential CEX deposit address (cheap new-CEX funnel detection).
CEX_DEPOSIT_FLAG_MIN_DOLO = 10_000
USER_CONTRACT_WALLET_ADDRS = {
    "0xbabcc964619cf5c8a57f2b989a35cd887e8ce739",  # User Safe/multisig DOLO holder
}

HOLDER_HISTORY_START_TIMESTAMP = int(datetime(2025, 4, 24, tzinfo=timezone.utc).timestamp())  # DOLO TGE

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "dolo_flows.json")
# Heavy per-day address-level snapshots live in a separate file so the main
# dolo_flows.json stays small for first render; the UI lazy-loads this one.
WALLET_HISTORY_JSON = os.path.join(DATA_DIR, "dolo_holder_wallet_history.json")
STATE_FILE = os.path.join(DATA_DIR, "dolo_flows_state.json")
RPC_BATCH_SIZE = int(os.environ.get("DOLO_FLOWS_RPC_BATCH_SIZE", "50"))
RPC_RETRIES_PER_ENDPOINT = int(os.environ.get("DOLO_FLOWS_RPC_RETRIES_PER_ENDPOINT", "2"))
RPC_LOG_RETRIES_PER_ENDPOINT = int(
    os.environ.get("DOLO_FLOWS_LOG_RETRIES_PER_ENDPOINT", str(RPC_RETRIES_PER_ENDPOINT))
)

MAX_PERIOD_SECONDS = max(PERIODS.values())  # longest period for pruning
# Cache ALL transfers from genesis — state file lives only in Actions cache (10 GB limit),
# never committed to git. After first full scan, every run just fetches new blocks.
MAX_CACHE_SECONDS = MAX_PERIOD_SECONDS

# Global state reference for signal handler
_global_state = {}


def _sigterm_handler(signum, frame):
    """Save state to disk on SIGTERM (Actions timeout kill signal)."""
    print(f"\n⚠️  Received signal {signum} — saving state before exit...")
    save_state(_global_state)
    print(f"💾 State saved ({STATE_FILE}). Progress preserved for next run.")
    sys.exit(0)

signal.signal(signal.SIGTERM, _sigterm_handler)
signal.signal(signal.SIGINT, _sigterm_handler)


def load_state():
    """Load incremental sync state (cached transfers + last blocks)."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception as exc:
            print(f"⚠️ load_state: failed to read {STATE_FILE} ({exc}); starting full resync", flush=True)
    return {}


def save_state(state):
    """Save incremental sync state."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def get_current_block(rpcs):
    """Get current block number from RPC. Tries all RPCs in sequence."""
    if isinstance(rpcs, str):
        rpcs = [rpcs]
    for rpc in rpcs:
        for _ in range(3):
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
                }, timeout=10, headers={"Content-Type": "application/json"})
                blk = int(resp.json().get("result", "0x0"), 16)
                if blk > 0:
                    return blk
            except Exception:
                time.sleep(1)
    return 0


def block_range_has_work(start_block, end_block):
    return int(start_block) <= int(end_block)


def validated_scan_end(chain_key, rpc_block, reorg_buffer, previous_last_block=0):
    if int(rpc_block) <= 0:
        raise RuntimeError(f"{CHAINS[chain_key]['name']}: could not resolve current block")
    end_block = max(
        int(CHAINS[chain_key].get("deploy_block", 0)),
        int(rpc_block) - int(reorg_buffer),
    )
    if int(previous_last_block or 0) > end_block:
        raise RuntimeError(
            f"{CHAINS[chain_key]['name']}: refusing checkpoint rewind from "
            f"{int(previous_last_block):,} to {end_block:,}"
        )
    return end_block


def incremental_refresh_start(last_block, oldest_needed, overlap_blocks):
    return max(
        int(oldest_needed),
        int(last_block) + 1 - max(0, int(overlap_blocks)),
    )


def replace_transfer_range(transfers, replacement, start_block, end_block):
    authoritative = [
        transfer for transfer in transfers
        if not int(start_block) <= int(transfer[3]) <= int(end_block)
    ]
    authoritative.extend(replacement)
    authoritative.sort(key=lambda transfer: int(transfer[3]))
    return authoritative


def _rate_limit_retry_seconds(response, attempt):
    """Honor an RPC Retry-After hint, with a bounded exponential fallback."""
    headers = getattr(response, "headers", {}) or {}
    retry_after = headers.get("Retry-After") if hasattr(headers, "get") else None
    try:
        seconds = float(retry_after)
    except (TypeError, ValueError):
        seconds = min(15.0, 1.0 * (2 ** min(attempt, 4)))
    return max(1.0, min(seconds, 60.0))


def _is_rate_limit_error(message):
    normalized = str(message or "").lower()
    return "too many" in normalized or "rate limit" in normalized or "rate-limit" in normalized


def fetch_transfer_logs(chain_key, start_block, end_block, state=None, cached_transfers_so_far=None):
    """Fetch ERC-20 Transfer event logs via eth_getLogs.
    Saves state progressively during long scans so timeout kills preserve progress."""
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    chunk_size = cfg["chunk_size"]

    if not block_range_has_work(start_block, end_block):
        return [], 0, 0  # transfers, failed_chunks, total_chunks

    total_blocks = end_block - start_block
    total_expected_chunks = max(1, (total_blocks + chunk_size - 1) // chunk_size)
    print(f"  {cfg['name']}: scanning blocks {start_block:,} → {end_block:,} ({total_blocks:,} blocks, ~{total_expected_chunks} chunks)")

    if not rpcs:
        print(f"  ⚠️ {cfg['name']}: NO RPCs configured! Skipping.")
        return [], total_expected_chunks, total_expected_chunks

    all_transfers = []
    current = start_block
    chunks_done = 0
    chunks_failed = 0
    skipped_ranges = []  # [start, end] of block ranges lost to persistent RPC failure

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        success = False
        for attempt in range(len(rpcs) * max(2, RPC_LOG_RETRIES_PER_ENDPOINT)):
            rpc = rpcs[attempt % len(rpcs)]
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_getLogs",
                    "params": [{
                        "address": DOLO_CONTRACT,
                        "topics": [TRANSFER_TOPIC],
                        "fromBlock": hex(current),
                        "toBlock": hex(chunk_end),
                    }], "id": 1
                }, timeout=60, headers={"Content-Type": "application/json"})

                if resp.status_code == 429:
                    delay = _rate_limit_retry_seconds(resp, attempt)
                    print(
                        f"    ⚠️ {cfg['name']}: RPC rate-limited block "
                        f"{current:,}-{chunk_end:,}; waiting {delay:.1f}s before retry"
                    )
                    time.sleep(delay)
                    continue

                resp.raise_for_status()
                r = resp.json()
                if not isinstance(r, dict):
                    raise ValueError("RPC response was not a JSON object")
                if "error" in r:
                    err_msg = r["error"].get("message", "")
                    if _is_rate_limit_error(err_msg):
                        delay = _rate_limit_retry_seconds(resp, attempt)
                        print(
                            f"    ⚠️ {cfg['name']}: RPC rate-limited block "
                            f"{current:,}-{chunk_end:,}; waiting {delay:.1f}s before retry"
                        )
                        time.sleep(delay)
                        continue
                    if "range" in err_msg.lower() or "limit" in err_msg.lower():
                        chunk_size = max(chunk_size // 2, 1000)
                        chunk_end = min(current + chunk_size - 1, end_block)
                        continue
                    time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))
                    continue

                logs = r.get("result", [])
                for log in logs:
                    if len(log.get("topics", [])) < 3:
                        continue
                    from_addr = "0x" + log["topics"][1][26:].lower()
                    to_addr = "0x" + log["topics"][2][26:].lower()
                    value_wei = int(log["data"], 16)
                    block_num = int(log["blockNumber"], 16)
                    all_transfers.append((from_addr, to_addr, value_wei, block_num))

                success = True
                break
            except requests.exceptions.Timeout:
                chunk_size = max(chunk_size // 2, 1000)
                chunk_end = min(current + chunk_size - 1, end_block)
                time.sleep(1)
            except requests.exceptions.RequestException as exc:
                print(
                    f"    ⚠️ {cfg['name']}: RPC request failed for block "
                    f"{current:,}-{chunk_end:,} ({type(exc).__name__}); retrying"
                )
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))
            except ValueError as exc:
                print(
                    f"    ⚠️ {cfg['name']}: RPC response was invalid for block "
                    f"{current:,}-{chunk_end:,} ({type(exc).__name__}); retrying"
                )
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))

        if not success:
            if chunk_size > 1000:
                chunk_size = max(chunk_size // 2, 1000)
                print(f"    ⚠️ Retrying block {current:,} with smaller chunk ({chunk_size:,} blocks)")
                continue
            chunks_failed += 1
            skipped_ranges.append([current, chunk_end])
            print(f"    ⚠️ Failed at block {current}, skipping chunk {current}-{chunk_end} ({chunks_failed} failures so far)")
            current = chunk_end + 1
            continue

        current = chunk_end + 1
        chunks_done += 1

        if chunks_done % 10 == 0 or current > end_block:
            pct = min(100, (current - start_block) * 100 // max(total_blocks, 1))
            print(f"    {cfg['name']}: {pct}% (block {current:,}/{end_block:,}, {len(all_transfers):,} txs)", flush=True)

        # Progressive state save every 20 chunks — ensures timeout kills preserve progress
        if state is not None and chunks_done % 20 == 0:
            _save_scan_progress(state, chain_key, current - 1, all_transfers, cached_transfers_so_far)

        if chunk_size < cfg["chunk_size"]:
            chunk_size = min(chunk_size * 2, cfg["chunk_size"])

        time.sleep(0.05)

    total_chunks_attempted = chunks_done + chunks_failed
    if chunks_failed > 0:
        fail_pct = chunks_failed * 100 // max(total_chunks_attempted, 1)
        print(f"  ⚠️ {cfg['name']}: {chunks_failed}/{total_chunks_attempted} chunks FAILED ({fail_pct}%)")
        print(f"     skipped block ranges: {skipped_ranges[:10]}{' …' if len(skipped_ranges) > 10 else ''}")
        if fail_pct > 50:
            print(f"  🚨 {cfg['name']}: >50% chunk failure rate! Data may be incomplete.")
    if state is not None and skipped_ranges:
        # Persist gaps so they are visible across runs and can be re-scanned;
        # mid-range gaps are NOT covered by the start-block coverage check.
        gaps = state.setdefault(f"skipped_ranges_{chain_key}", [])
        gaps.extend(skipped_ranges)

    print(f"  ✅ {cfg['name']}: {len(all_transfers):,} transfers found")
    return all_transfers, chunks_failed, total_chunks_attempted


def _save_scan_progress(state, chain_key, last_block_scanned, new_transfers, cached_transfers_so_far):
    """Save intermediate scan progress to state file so SIGTERM/timeout preserves work."""
    cached_key = f"{chain_key}_transfers"
    last_block_key = f"{chain_key}_last_block"
    # Merge cached + new transfers found so far
    all_so_far = (cached_transfers_so_far or []) + [list(t) for t in new_transfers]
    state[cached_key] = all_so_far
    state[last_block_key] = last_block_scanned
    save_state(state)


def _normalize_skipped_ranges(ranges, oldest_needed, end_block):
    clipped = []
    for raw_range in ranges or []:
        if not isinstance(raw_range, (list, tuple)) or len(raw_range) != 2:
            continue
        try:
            start = max(int(raw_range[0]), int(oldest_needed))
            end = min(int(raw_range[1]), int(end_block))
        except (TypeError, ValueError):
            continue
        if start <= end:
            clipped.append([start, end])

    merged = []
    for start, end in sorted(clipped):
        if merged and start <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def repair_skipped_ranges(chain_key, transfers, state, oldest_needed, end_block):
    """Replace previously skipped block ranges with authoritative RPC results.

    A repair range is committed only when its full scan succeeds. Partial rows
    are discarded so a later retry cannot double-count the successful portion.
    """
    gap_key = f"skipped_ranges_{chain_key}"
    gaps = _normalize_skipped_ranges(state.get(gap_key, []), oldest_needed, end_block)
    if not gaps:
        state[gap_key] = []
        return list(transfers), [], 0

    repaired_transfers = list(transfers)
    unresolved = []
    repaired_count = 0
    print(f"  {CHAINS[chain_key]['name']}: repairing {len(gaps)} skipped range(s)")

    for start, end in gaps:
        replacement, failed_chunks, _ = fetch_transfer_logs(chain_key, start, end)
        if failed_chunks:
            unresolved.append([start, end])
            continue

        repaired_transfers = replace_transfer_range(
            repaired_transfers,
            replacement,
            start,
            end,
        )
        repaired_count += len(replacement)

    state[gap_key] = unresolved
    return repaired_transfers, unresolved, repaired_count


def require_complete_flow_history(gaps_by_chain):
    unresolved = []
    for chain_key, ranges in gaps_by_chain.items():
        for start, end in ranges or []:
            chain_name = CHAINS.get(chain_key, {}).get("name", chain_key)
            unresolved.append(f"{chain_name} {start}-{end}")
    if unresolved:
        preview = ", ".join(unresolved[:5])
        suffix = f" (+{len(unresolved) - 5} more)" if len(unresolved) > 5 else ""
        raise RuntimeError(
            "DOLO flow history is incomplete; refusing to publish with unresolved RPC gaps: "
            f"{preview}{suffix}"
        )


def detect_contracts_batch(addresses, chain_key):
    """Detect which addresses are contracts using eth_getCode."""
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    contracts = set()

    payloads = []
    meta_by_id = {}
    for idx, addr in enumerate(addresses):
        request_id = f"{chain_key}:code:{idx}"
        payloads.append({
            "jsonrpc": "2.0",
            "method": "eth_getCode",
            "params": [addr, "latest"],
            "id": request_id,
        })
        meta_by_id[request_id] = addr

    try:
        responses, missing_ids = rpc_batch_requests(
            rpcs,
            payloads,
            timeout=5,
            retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
            batch_size=RPC_BATCH_SIZE,
            quiet=True,
            describe=f"{cfg['name']} flow eth_getCode",
        )
    except RpcError:
        responses, missing_ids = {}, [payload["id"] for payload in payloads]

    for payload in payloads:
        request_id = payload["id"]
        response = responses.get(request_id)
        if request_id in missing_ids or not isinstance(response, dict) or response.get("error") or "result" not in response:
            try:
                response = rpc_single_request(
                    rpcs,
                    payload,
                    timeout=5,
                    retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                    quiet=True,
                    describe=f"{cfg['name']} flow eth_getCode fallback",
                )
            except RpcError:
                continue
        code = str(response.get("result", "0x") if isinstance(response, dict) else "0x")
        if code and len(code) > 4:
            contracts.add(meta_by_id[request_id])

    return contracts


def _multicall_dolo_balances(rpcs, addresses):
    """Fast path: DOLO balanceOf via Multicall3 aggregate3 — ONE eth_call per
    chunk instead of one per address.

    Returns (resolved, unresolved): `resolved` maps address -> raw uint256
    balance; `unresolved` lists addresses Multicall3 could not cleanly resolve
    (a reverted/short sub-call or an unreachable endpoint), which the caller
    sends through the per-address fallback so the failed-vs-zero handling
    (lessons.md) still applies. Output is data-identical to individual
    balanceOf calls — only the request count drops.
    """
    addresses = list(addresses)
    if not addresses:
        return {}, []
    try:
        from web3 import Web3
    except ImportError:
        return {}, addresses  # no web3 -> per-address fallback handles everything
    rpc_list = [r for r in (rpcs or []) if r]
    if not rpc_list:
        return {}, addresses

    token = Web3.to_checksum_address(DOLO_CONTRACT)
    multicall_addr = Web3.to_checksum_address(MULTICALL3_ADDR)
    selector = "70a08231"  # balanceOf(address)
    chunk_size = max(1, RPC_BATCH_SIZE)
    resolved = {}
    unresolved = []
    rpc_idx = 0

    for start in range(0, len(addresses), chunk_size):
        chunk = addresses[start:start + chunk_size]
        calls = [
            (token, True, bytes.fromhex(selector + a.replace("0x", "").lower().zfill(64)))
            for a in chunk
        ]
        results = None
        for attempt in range(len(rpc_list)):
            rpc = rpc_list[(rpc_idx + attempt) % len(rpc_list)]
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
                multicall = w3.eth.contract(address=multicall_addr, abi=MULTICALL3_AGG3_ABI)
                results = multicall.functions.aggregate3(calls).call()
                rpc_usage.record_request("eth_call")  # one aggregate3 == one eth_call
                rpc_idx = (rpc_idx + attempt) % len(rpc_list)
                break
            except Exception:
                results = None
                continue
        if not results or len(results) != len(chunk):
            unresolved.extend(chunk)  # whole chunk unreadable -> per-address fallback
            continue
        for addr, item in zip(chunk, results):
            success = bool(item[0])
            data = bytes(item[1]) if item[1] is not None else b""
            if success and len(data) >= 32:
                resolved[addr] = int.from_bytes(data[:32], "big")
            else:
                unresolved.append(addr)  # reverted/short -> treat as failed, retry below
    return resolved, unresolved


def fetch_dolo_balances(addresses):
    """Fetch current DOLO balances across tracked chains using JSON-RPC batches."""
    unique = sorted({addr.lower() for addr in addresses if addr})
    balances = {}
    failed_addrs = set()
    failures = 0
    if not unique:
        return balances, failures, failed_addrs

    bal_selector = "0x70a08231"  # balanceOf(address)
    totals = {addr: 0.0 for addr in unique}

    for chain_key, cfg in CHAINS.items():
        # Fast path: Multicall3 batches many balanceOf reads into one eth_call.
        resolved, pending_addrs = _multicall_dolo_balances(cfg["rpcs"], unique)
        for resolved_addr, raw_balance in resolved.items():
            totals[resolved_addr] += raw_balance / 1e18
        if not pending_addrs:
            continue

        # Fallback (unchanged): per-address eth_call with batch + individual
        # retry, only for addresses Multicall3 could not resolve.
        payloads = []
        meta_by_id = {}
        for idx, addr in enumerate(pending_addrs):
            padded = addr.replace("0x", "").lower().zfill(64)
            request_id = f"{chain_key}:{idx}"
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": DOLO_CONTRACT, "data": bal_selector + padded}, "latest"],
                "id": request_id,
            }
            payloads.append(payload)
            meta_by_id[request_id] = addr

        try:
            responses, missing_ids = rpc_batch_requests(
                cfg["rpcs"],
                payloads,
                timeout=10,
                retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                batch_size=RPC_BATCH_SIZE,
                quiet=True,
                describe=f"{cfg['name']} DOLO balanceOf",
            )
        except RpcError:
            responses, missing_ids = {}, [payload["id"] for payload in payloads]

        for payload in payloads:
            request_id = payload["id"]
            addr = meta_by_id[request_id]
            response = responses.get(request_id)
            if request_id in missing_ids or not isinstance(response, dict) or response.get("error") or "result" not in response:
                try:
                    response = rpc_single_request(
                        cfg["rpcs"],
                        payload,
                        timeout=10,
                        retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                        quiet=True,
                        describe=f"{cfg['name']} DOLO balanceOf fallback",
                    )
                except RpcError:
                    failures += 1
                    failed_addrs.add(addr)
                    continue
            if not isinstance(response, dict) or response.get("error"):
                failures += 1
                failed_addrs.add(addr)
                continue
            totals[addr] += decode_uint256(response.get("result", "0x0")) / 1e18

    for addr, value in totals.items():
        balances[addr] = round(value, 2)
    return balances, failures, failed_addrs


def calculate_flows(transfers, excluded):
    """Calculate net flow per address from transfer list.
    Positive = accumulator, Negative = seller.
    Transfers involving mint/burn addresses (ZERO, DOLO contract) are skipped
    entirely — mints are not accumulation and burns are not selling.
    Detected DEX/LP contracts are kept in the calculation (both legs counted)
    but filtered from the final results by get_top()."""
    flows = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        if from_addr in FLOW_SKIP_ADDRS or to_addr in FLOW_SKIP_ADDRS:
            continue
        value = value_wei / (10 ** 18)
        flows[from_addr] = flows.get(from_addr, 0) - value
        flows[to_addr] = flows.get(to_addr, 0) + value
    return flows


def calculate_flow_components(transfers):
    """Return gross directional amounts and their reconciled net per address."""
    components = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        if from_addr in FLOW_SKIP_ADDRS or to_addr in FLOW_SKIP_ADDRS:
            continue
        value = value_wei / (10 ** 18)
        sender = components.setdefault(
            from_addr,
            {"gross_inflow": 0.0, "gross_outflow": 0.0, "net_flow": 0.0},
        )
        receiver = components.setdefault(
            to_addr,
            {"gross_inflow": 0.0, "gross_outflow": 0.0, "net_flow": 0.0},
        )
        sender["gross_outflow"] += value
        sender["net_flow"] -= value
        receiver["gross_inflow"] += value
        receiver["net_flow"] += value
    return components


def calculate_bridge_flows(transfers):
    """Calculate flows from mint/burn transfers only (from/to 0x0).
    These are invisible to calculate_flows() but critical for cross-chain
    bridge detection. Bridges use burn (to 0x0) on the source chain and
    mint (from 0x0) on the destination chain.
    
    Returns: {addr: net_bridge_flow} where positive = received mints,
    negative = sent burns."""
    bridge_flows = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        value = value_wei / (10 ** 18)
        if from_addr in BRIDGE_ADDRS and to_addr not in BRIDGE_ADDRS:
            # Mint: receiver got tokens via bridge
            bridge_flows[to_addr] = bridge_flows.get(to_addr, 0) + value
        elif to_addr in BRIDGE_ADDRS and from_addr not in BRIDGE_ADDRS:
            # Burn: sender sent tokens to bridge
            bridge_flows[from_addr] = bridge_flows.get(from_addr, 0) - value
    return bridge_flows


def neutralize_cross_chain_flows(flows_by_chain):
    """Neutralize cross-chain bridge transfers.
    When the same address has outflow on one chain and inflow on another,
    it's a bridge transfer (same person moving tokens between networks).
    Cancel the overlapping amount so it doesn't count as accumulation or selling.
    
    Args:
        flows_by_chain: dict of {chain_key: {addr: net_flow, ...}}
    Returns:
        Adjusted flows_by_chain (mutated in place and returned).
        Also returns count of neutralized addresses for logging.
    """
    chain_keys = list(flows_by_chain.keys())
    if len(chain_keys) < 2:
        return flows_by_chain, 0, 0.0
    
    # Collect all addresses that appear on multiple chains
    all_addrs = set()
    for flows in flows_by_chain.values():
        all_addrs.update(flows.keys())
    
    neutralized_count = 0
    neutralized_volume = 0
    
    for addr in all_addrs:
        # Get flows across all chains for this address
        chain_flows = {}
        for ck in chain_keys:
            flow = flows_by_chain[ck].get(addr, 0)
            if abs(flow) > 0.01:  # skip dust
                chain_flows[ck] = flow
        
        if len(chain_flows) < 2:
            continue
        
        # Check for opposing flows (outflow on one chain, inflow on another)
        # This indicates a bridge transfer
        positive_chains = {ck: v for ck, v in chain_flows.items() if v > 0}
        negative_chains = {ck: v for ck, v in chain_flows.items() if v < 0}
        
        if not positive_chains or not negative_chains:
            continue  # same direction on all chains — not a bridge
        
        # Cancel the overlapping amount
        total_inflow = sum(positive_chains.values())
        total_outflow = abs(sum(negative_chains.values()))
        cancel_amount = min(total_inflow, total_outflow)
        
        if cancel_amount < 1:  # skip dust cancellations
            continue
        
        # Distribute cancellation proportionally across chains
        # Reduce inflows
        remaining_cancel = cancel_amount
        for ck in sorted(positive_chains, key=lambda k: positive_chains[k], reverse=True):
            reduce = min(positive_chains[ck], remaining_cancel)
            flows_by_chain[ck][addr] -= reduce
            remaining_cancel -= reduce
            if remaining_cancel <= 0:
                break
        
        # Reduce outflows (add to negative values to bring closer to 0)
        remaining_cancel = cancel_amount
        for ck in sorted(negative_chains, key=lambda k: negative_chains[k]):
            reduce = min(abs(negative_chains[ck]), remaining_cancel)
            flows_by_chain[ck][addr] += reduce
            remaining_cancel -= reduce
            if remaining_cancel <= 0:
                break
        
        neutralized_count += 1
        neutralized_volume += cancel_amount
    
    return flows_by_chain, neutralized_count, neutralized_volume


def build_holder_history_schedule(base_ts):
    """Build daily holder-bucket chart cutoffs from DOLO TGE to yesterday."""
    points_by_day = {}
    start_ts = HOLDER_HISTORY_START_TIMESTAMP
    end_ts = int(base_ts)

    def add_point(ts):
        ts = max(start_ts, min(int(ts), end_ts))
        if ts >= end_ts - 3600:
            return
        dt = datetime.utcfromtimestamp(ts)
        key = dt.strftime("hist_%Y%m%d")
        existing = points_by_day.get(key)
        if existing is None or abs(ts - end_ts) < abs(existing["ts"] - end_ts):
            points_by_day[key] = {
                "key": key,
                "timestamp": dt.isoformat() + "Z",
                "ts": ts,
            }

    add_point(start_ts)
    current = datetime.fromtimestamp(start_ts, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if int(current.timestamp()) < start_ts:
        current = current + timedelta(days=1)
    while int(current.timestamp()) < end_ts - 3600:
        add_point(int(current.timestamp()))
        current = current + timedelta(days=1)

    return sorted(points_by_day.values(), key=lambda point: point["ts"])


def calculate_neutralized_flows_for_cutoffs(all_transfers, cutoff_by_chain):
    raw_flows = {}
    bridge_flows_by_chain = {}
    for chain_key in CHAINS:
        cutoff = cutoff_by_chain[chain_key]
        period_transfers = [t for t in all_transfers[chain_key] if t[3] >= cutoff]
        raw_flows[chain_key] = calculate_flows(period_transfers, EXCLUDED_ADDRS)
        bridge_flows_by_chain[chain_key] = calculate_bridge_flows(period_transfers)
    return neutralize_raw_and_bridge_flows(raw_flows, bridge_flows_by_chain)


def neutralize_raw_and_bridge_flows(raw_flows, bridge_flows_by_chain):
    augmented_flows = {}
    for chain_key in CHAINS:
        augmented_flows[chain_key] = dict(raw_flows[chain_key])
        for addr, bflow in bridge_flows_by_chain[chain_key].items():
            augmented_flows[chain_key][addr] = augmented_flows[chain_key].get(addr, 0) + bflow

    neutralized_aug, _, _ = neutralize_cross_chain_flows(augmented_flows)
    neutralized = {}
    for chain_key in CHAINS:
        neutralized[chain_key] = dict(raw_flows[chain_key])
        for addr in raw_flows[chain_key]:
            original_aug = raw_flows[chain_key].get(addr, 0) + bridge_flows_by_chain[chain_key].get(addr, 0)
            neutralized_aug_val = neutralized_aug[chain_key].get(addr, 0)
            delta = neutralized_aug_val - original_aug
            if abs(delta) > 0.01:
                neutralized[chain_key][addr] = raw_flows[chain_key][addr] + delta
    return neutralized


def neutralize_holder_balance_flows(raw_flows, bridge_flows_by_chain):
    """Neutralize flows for historical holder balances.

    Flow leaderboards intentionally keep mint/burn bridge transfers out of the
    final rows. Holder-balance reconstruction is different: a bridge mint or
    burn changes the visible balance on a tracked chain, so the augmented flow
    must be used after cross-chain cancellation. Otherwise bridge/router
    addresses can look like they held large balances at TGE.
    """
    augmented_flows = {}
    for chain_key in CHAINS:
        augmented_flows[chain_key] = dict(raw_flows[chain_key])
        for addr, bflow in bridge_flows_by_chain[chain_key].items():
            augmented_flows[chain_key][addr] = augmented_flows[chain_key].get(addr, 0) + bflow
    neutralized_aug, _, _ = neutralize_cross_chain_flows(augmented_flows)
    return neutralized_aug


def add_transfer_to_running_flows(raw_flows, bridge_flows, transfer):
    from_addr, to_addr, value_wei, _ = transfer
    value = value_wei / (10 ** 18)
    if from_addr not in FLOW_SKIP_ADDRS and to_addr not in FLOW_SKIP_ADDRS:
        raw_flows[from_addr] = raw_flows.get(from_addr, 0) - value
        raw_flows[to_addr] = raw_flows.get(to_addr, 0) + value
    if from_addr in BRIDGE_ADDRS and to_addr not in BRIDGE_ADDRS:
        bridge_flows[to_addr] = bridge_flows.get(to_addr, 0) + value
    elif to_addr in BRIDGE_ADDRS and from_addr not in BRIDGE_ADDRS:
        bridge_flows[from_addr] = bridge_flows.get(from_addr, 0) - value


def ensure_transfers_sorted_by_block(transfers):
    if len(transfers) < 2:
        return transfers
    for idx in range(1, len(transfers)):
        if transfers[idx - 1][3] > transfers[idx][3]:
            return sorted(transfers, key=lambda t: t[3])
    return transfers


def holder_history_cutoff_block(chain_key, point_ts, base_ts, current_blocks):
    cfg = CHAINS[chain_key]
    seconds_back = max(0, int(base_ts) - int(point_ts))
    blocks_back = int(seconds_back // cfg["block_time"])
    return max(current_blocks[chain_key] - blocks_back, cfg.get("deploy_block", 0))


def historical_liquid_by_chain(current_liquid_by_chain, chain_changes):
    liquid_by_chain = {}
    for chain_key in CHAINS:
        balances = dict(current_liquid_by_chain.get(chain_key, {}))
        for addr, net in chain_changes.get(chain_key, {}).items():
            if addr in EXCLUDED_ADDRS:
                continue
            historical = balances.get(addr.lower(), 0) - net
            if historical > 0.0001:
                balances[addr.lower()] = historical
            elif addr.lower() in balances:
                balances.pop(addr.lower(), None)
        liquid_by_chain[chain_key] = balances
    return liquid_by_chain


def calculate_holder_bucket_history(all_transfers, points, current_blocks, base_ts, vesting_labels=None):
    holder_rows = load_current_holder_rows()
    address_labels = load_address_labels(vesting_labels)
    current_liquid = {
        addr: float(row.get("balance") or 0)
        for addr, row in holder_rows.items()
    }
    current_liquid_by_chain = {
        "eth": {
            addr: float(row.get("balance_eth") or 0)
            for addr, row in holder_rows.items()
        },
        "bera": {
            addr: float(row.get("balance_bera") or 0)
            for addr, row in holder_rows.items()
        },
    }
    current_locks = load_current_vedolo_locks()
    vedolo_events = load_vedolo_flow_events()
    sorted_transfers = {
        chain_key: ensure_transfers_sorted_by_block(transfers)
        for chain_key, transfers in all_transfers.items()
    }
    cursors = {
        chain_key: len(transfers) - 1
        for chain_key, transfers in sorted_transfers.items()
    }
    running_raw = {chain_key: {} for chain_key in CHAINS}
    running_bridge = {chain_key: {} for chain_key in CHAINS}
    history = []
    wallet_history = {}

    for point in sorted(points, key=lambda row: row["ts"], reverse=True):
        for chain_key in CHAINS:
            cutoff = holder_history_cutoff_block(chain_key, point["ts"], base_ts, current_blocks)
            chain_transfers = sorted_transfers[chain_key]
            cursor = cursors[chain_key]
            while cursor >= 0 and chain_transfers[cursor][3] >= cutoff:
                add_transfer_to_running_flows(
                    running_raw[chain_key],
                    running_bridge[chain_key],
                    chain_transfers[cursor],
                )
                cursor -= 1
            cursors[chain_key] = cursor

        raw_snapshot = {chain_key: dict(running_raw[chain_key]) for chain_key in CHAINS}
        bridge_snapshot = {chain_key: dict(running_bridge[chain_key]) for chain_key in CHAINS}
        chain_changes = neutralize_holder_balance_flows(raw_snapshot, bridge_snapshot)
        changes = merge_balance_changes(chain_changes)
        liquid_balances = dict(current_liquid)
        for addr, net in changes.items():
            historical = liquid_balances.get(addr.lower(), 0) - net
            if historical > 0.0001:
                liquid_balances[addr.lower()] = historical
            elif addr.lower() in liquid_balances:
                liquid_balances.pop(addr.lower(), None)

        liquid_by_chain = historical_liquid_by_chain(current_liquid_by_chain, chain_changes)
        locked_balances = locked_map_at_holder_point(point["ts"], current_locks, vedolo_events)
        row = {
            "key": point["key"],
            "timestamp": point["timestamp"],
            "liquid": {},
            "with_vedolo": {},
        }
        wallet_row = {
            "timestamp": point["timestamp"],
            "liquid": {},
            "with_vedolo": {},
        }
        for audience in ("market", "potential"):
            row["liquid"][audience] = {}
            row["with_vedolo"][audience] = {}
            if HOLDER_WALLET_HISTORY_VIEWS:
                wallet_row["liquid"][audience] = {}
                wallet_row["with_vedolo"][audience] = {}
            for view, bucket_defs in HOLDER_BUCKET_GROUPS.items():
                row["liquid"][audience][view] = build_bucket_model(
                    liquid_balances,
                    {},
                    holder_rows,
                    address_labels,
                    bucket_defs,
                    audience=audience,
                )
                row["with_vedolo"][audience][view] = build_bucket_model(
                    liquid_balances,
                    locked_balances,
                    holder_rows,
                    address_labels,
                    bucket_defs,
                    audience=audience,
                )
                if view in HOLDER_WALLET_HISTORY_VIEWS:
                    wallet_row["liquid"][audience][view] = build_bucket_wallet_history_rows(
                        liquid_by_chain,
                        {},
                        holder_rows,
                        address_labels,
                        bucket_defs,
                        audience=audience,
                    )
                    wallet_row["with_vedolo"][audience][view] = build_bucket_wallet_history_rows(
                        liquid_by_chain,
                        locked_balances,
                        holder_rows,
                        address_labels,
                        bucket_defs,
                        audience=audience,
                    )
        history.append(row)
        wallet_history[point["key"]] = wallet_row

    return sorted(history, key=lambda row: row["timestamp"]), wallet_history


def calculate_cex_supply_history(all_transfers, points, current_blocks, base_ts, vesting_labels=None):
    holder_rows = load_current_holder_rows()
    address_labels = load_address_labels(vesting_labels)
    current_liquid = {
        addr: float(row.get("balance") or 0)
        for addr, row in holder_rows.items()
    }
    sorted_transfers = {
        chain_key: ensure_transfers_sorted_by_block(transfers)
        for chain_key, transfers in all_transfers.items()
    }
    cursors = {
        chain_key: len(transfers) - 1
        for chain_key, transfers in sorted_transfers.items()
    }
    running_raw = {chain_key: {} for chain_key in CHAINS}
    running_bridge = {chain_key: {} for chain_key in CHAINS}
    history = []

    for point in sorted(points, key=lambda row: row["ts"], reverse=True):
        for chain_key in CHAINS:
            cutoff = holder_history_cutoff_block(chain_key, point["ts"], base_ts, current_blocks)
            chain_transfers = sorted_transfers[chain_key]
            cursor = cursors[chain_key]
            while cursor >= 0 and chain_transfers[cursor][3] >= cutoff:
                add_transfer_to_running_flows(
                    running_raw[chain_key],
                    running_bridge[chain_key],
                    chain_transfers[cursor],
                )
                cursor -= 1
            cursors[chain_key] = cursor

        raw_snapshot = {chain_key: dict(running_raw[chain_key]) for chain_key in CHAINS}
        bridge_snapshot = {chain_key: dict(running_bridge[chain_key]) for chain_key in CHAINS}
        changes = merge_balance_changes(neutralize_holder_balance_flows(raw_snapshot, bridge_snapshot))
        liquid_balances = dict(current_liquid)
        for addr, net in changes.items():
            historical = liquid_balances.get(addr.lower(), 0) - net
            if historical > 0.0001:
                liquid_balances[addr.lower()] = historical
            elif addr.lower() in liquid_balances:
                liquid_balances.pop(addr.lower(), None)

        cex = build_cex_supply_point(liquid_balances, holder_rows, address_labels)
        history.append({
            "key": point["key"],
            "timestamp": point["timestamp"],
            "liquid": cex["liquid"],
            "wallets": cex["wallets"],
        })

    return sorted(history, key=lambda row: row["timestamp"])


def build_cex_supply_point(liquid_balances, holder_rows, address_labels):
    total = 0
    wallets = 0
    for addr, value in liquid_balances.items():
        liquid = max(0, float(value or 0))
        if liquid <= 0:
            continue
        if holder_distribution_type(addr, holder_rows, address_labels) != "cex":
            continue
        total += liquid
        wallets += 1
    return {
        "wallets": wallets,
        "liquid": round(total, 2),
    }


def merge_balance_changes(flows_by_chain):
    merged = {}
    for chain_key in CHAINS:
        flows = flows_by_chain[chain_key]
        for addr, net in flows.items():
            if addr in EXCLUDED_ADDRS:
                continue
            if abs(net) < 1:
                continue
            merged[addr] = merged.get(addr, 0) + net
    return {addr: round(v, 2) for addr, v in merged.items()}


def load_current_holder_rows():
    holders_file = os.path.join(DATA_DIR, "dolo_holders.json")
    if not os.path.exists(holders_file):
        return {}
    try:
        with open(holders_file) as f:
            holders_data = json.load(f)
        return {
            h.get("address", "").lower(): h
            for h in holders_data.get("holders", [])
            if h.get("address")
        }
    except Exception as e:
        print(f"  ⚠️ Could not load holder rows for bucket history: {e}")
        return {}


def extract_vesting_investors(all_transfers):
    claim_contracts = {
        ("bera", "0x7efd088ae500598a19a242d6d48b9f7e0d061176"): "strategic_investor_claims",
        ("bera", "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"): "investor_claims",
    }
    source_order = ["strategic_investor_claims", "investor_claims"]
    records = {}

    for chain_key in ["eth", "bera"]:
        for transfer in all_transfers.get(chain_key, []):
            from_addr = str(transfer[0] or "").lower()
            to_addr = str(transfer[1] or "").lower()
            source = claim_contracts.get((chain_key, from_addr))
            if not source or not re.fullmatch(r"0x[a-f0-9]{40}", to_addr):
                continue
            try:
                amount_wei = int(transfer[2])
                block = int(transfer[3])
            except (TypeError, ValueError, IndexError):
                continue
            row = records.setdefault(to_addr, {
                "sources": set(),
                "transfer_count": 0,
                "received_wei": 0,
                "first_block": block,
                "last_block": block,
                "chains": set(),
            })
            row["sources"].add(source)
            row["transfer_count"] += 1
            row["received_wei"] += amount_wei
            row["first_block"] = min(row["first_block"], block)
            row["last_block"] = max(row["last_block"], block)
            row["chains"].add(chain_key)

    def format_dolo_wei(value):
        whole, remainder = divmod(max(0, int(value)), 10**18)
        if not remainder:
            return str(whole)
        return f"{whole}.{remainder:018d}".rstrip("0")

    wallet_rows = []
    for address in sorted(records):
        row = records[address]
        sources = [source for source in source_order if source in row["sources"]]
        is_early = "strategic_investor_claims" in row["sources"]
        has_long_term = "investor_claims" in row["sources"]
        wallet_rows.append({
            "address": address,
            "label": "Early Investor" if is_early else "Investor",
            "type": "investor",
            "claimSources": sources,
            "primarySource": sources[0],
            "alsoReceivedLongTermTranche": bool(is_early and has_long_term),
            "transferCount": row["transfer_count"],
            "firstTransferBlock": row["first_block"],
            "lastTransferBlock": row["last_block"],
            "sourceChains": sorted(row["chains"]),
            "receivedDolo": format_dolo_wei(row["received_wei"]),
        })

    early_set = {
        row["address"] for row in wallet_rows
        if "strategic_investor_claims" in row["claimSources"]
    }
    inv_set = {
        row["address"] for row in wallet_rows
        if "investor_claims" in row["claimSources"]
    }

    return {
        "schemaVersion": 2,
        "contracts": {
            "strategicInvestorClaims": "0x7efd088ae500598a19a242d6d48b9f7e0d061176",
            "investorClaims": "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07",
        },
        "methodology": {
            "classification": "direct-dolo-transfer-from-official-claim-contract",
            "overlapPriority": "early-investor",
            "team": "not-derived-from-investor-claims",
        },
        "early_investors": sorted(early_set),
        "investors": sorted(inv_set),
        "team": [],
        "wallets": wallet_rows,
    }


def merge_vesting_labels(labels, vesting_data):
    if not isinstance(vesting_data, dict):
        return labels
    structured_wallets = vesting_data.get("wallets") or []
    for row in structured_wallets:
        if not isinstance(row, dict):
            continue
        addr_key = str(row.get("address") or "").lower()
        label = row.get("label")
        if not re.fullmatch(r"0x[a-f0-9]{40}", addr_key) or label not in {"Early Investor", "Investor"}:
            continue
        labels.setdefault(addr_key, {
            "label": label,
            "type": "investor",
            "source": "official-claim-contract-transfer",
            "confidence": "confirmed",
        })

    investor_rows = set(vesting_data.get("investors", []) or [])
    legacy_team_rows = set(vesting_data.get("team", []) or [])
    if investor_rows and legacy_team_rows == investor_rows:
        legacy_team_rows = set()
    for key, label, label_type in [
        ("early_investors", "Early Investor", "investor"),
        ("investors", "Investor", "investor"),
        ("team", "Core Team", "protocol"),
    ]:
        rows = legacy_team_rows if key == "team" else vesting_data.get(key, []) or []
        for addr in rows:
            addr_key = str(addr or "").lower()
            if not re.fullmatch(r"0x[a-f0-9]{40}", addr_key):
                continue
            labels.setdefault(addr_key, {"label": label, "type": label_type})
    return labels


def load_address_labels(vesting_labels=None):
    labels_file = os.path.join(DATA_DIR, "dolo-address-labels.js")
    if not os.path.exists(labels_file):
        return {}
    try:
        text = open(labels_file).read()
    except Exception as e:
        print(f"  ⚠️ Could not load address labels for bucket history: {e}")
        return {}
    labels = {}
    for match in re.finditer(r'"(0x[a-fA-F0-9]{40})"\s*:\s*\{([^}]+)\}', text):
        body = match.group(2)
        label_match = re.search(r'label\s*:\s*"([^"]+)"', body)
        type_match = re.search(r'type\s*:\s*"([^"]+)"', body)
        labels[match.group(1).lower()] = {
            "label": label_match.group(1) if label_match else "",
            "type": type_match.group(1) if type_match else "",
        }
    vesting_file = os.path.join(DATA_DIR, "vesting_investors.json")
    if os.path.exists(vesting_file):
        try:
            with open(vesting_file) as f:
                merge_vesting_labels(labels, json.load(f))
        except Exception as e:
            print(f"  ⚠️ Could not load vesting labels for bucket history: {e}")
    merge_vesting_labels(labels, vesting_labels)
    return labels


def select_dynamic_flow_exclusions(detected_contracts, address_labels):
    """Keep known custody/user contracts visible; exclude infrastructure CAs."""
    visible_label_types = {"cex", "multisig", "safe", "contract_wallet"}
    exclusions = set()
    for raw_addr in detected_contracts:
        addr = str(raw_addr or "").lower()
        label_type = str((address_labels.get(addr) or {}).get("type") or "").lower()
        if addr in USER_CONTRACT_WALLET_ADDRS or label_type in visible_label_types:
            continue
        exclusions.add(addr)
    return exclusions


def holder_distribution_type(addr, holder_rows, labels):
    key = addr.lower()
    info = labels.get(key, {})
    label = info.get("label", "")
    label_type = info.get("type", "")
    holder = holder_rows.get(key) or {}
    contract_wallet_type = str(holder.get("contract_wallet_type") or "").lower()
    if (
        key in USER_CONTRACT_WALLET_ADDRS
        or label_type in {"multisig", "safe", "contract_wallet"}
    ):
        return "multisig"
    if label_type == "cex":
        return "cex"
    if label.startswith("Core Team"):
        return "team"
    if label_type == "investor":
        return "investor"
    if label_type in {"protocol", "lp", "contract", "dead"}:
        return "ca"
    if contract_wallet_type in {"safe", "multisig"}:
        return "multisig"
    if holder.get("is_contract"):
        return "ca"
    if label_type in {"bot", "liquidator"}:
        return "bot"
    return label_type or "eoa"


def transfer_estimated_timestamp(chain_key, block_number, current_blocks, base_ts):
    """Approximate block time well enough to compare first DOLO receipts across chains."""
    cfg = CHAINS[chain_key]
    blocks_back = max(0, int(current_blocks.get(chain_key, block_number)) - int(block_number))
    return int(base_ts) - blocks_back * int(cfg["block_time"])


def source_label_for_fresh_wallet(source_addr, labels):
    key = (source_addr or "").lower()
    if key in BRIDGE_ADDRS:
        return "Bridge / Mint"
    if key == "0x3e9b9a16743551da49b5e136c716bba7932d2cec":
        return "oDOLO Exercise"
    info = labels.get(key, {})
    label_type = info.get("type", "")
    label = info.get("label", "")
    if label_type == "cex":
        return "CEX Withdrawal"
    if label_type in {"protocol", "lp", "contract"} or key in EXCLUDED_ADDRS:
        return "DEX / Contract"
    if label:
        return f"From {label}"
    return "Received"


def validate_fresh_wallet_activity_config():
    if any(source["provider"] == "etherscan" for source in FRESH_WALLET_ACTIVITY_SOURCES) and not ETHERSCAN_API_KEY:
        raise RuntimeError(
            "Fresh DOLO wallet cohorts require ETHERSCAN_API_KEY because first wallet activity "
            "is verified with Etherscan V2 txlist across tracked EVM chains."
        )


def _explorer_first_activity_params(activity_source, address):
    chain_key = activity_source["key"]
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "page": 1,
        "offset": 1,
        "sort": "asc",
    }
    if activity_source["provider"] == "etherscan":
        if not ETHERSCAN_API_KEY:
            return None, None, "missing_ETHERSCAN_API_KEY"
        params["chainid"] = activity_source["chainid"]
        params["apikey"] = ETHERSCAN_API_KEY
        return ETHERSCAN_V2_API, params, ""
    if chain_key == "bera" and BERASCAN_API_KEY:
        params["apikey"] = BERASCAN_API_KEY
    return BERACHAIN_EXPLORER_API, params, ""


def fetch_first_chain_activity(activity_source, address, state, session, base_ts):
    """Return the first normal tx for an address on one chain.

    EOA creation is not an on-chain field. For this dashboard, a true fresh
    wallet means the first normal account transaction observed by the explorer.
    Token spam is intentionally ignored so random old token receipts do not age
    a wallet that never actually used the chain.
    """
    chain_key = activity_source["key"]
    addr = (address or "").lower()
    cache = state.setdefault(FRESH_WALLET_ACTIVITY_CACHE_KEY, {})
    entry = cache.setdefault(addr, {})
    cached = entry.get(chain_key) or {}
    checked_at = int(cached.get("checked_at") or 0)
    if cached.get("status") == "ok" and int(cached.get("first_timestamp") or 0) > 0:
        return dict(cached)
    if cached.get("status") == "no_activity" and base_ts - checked_at < FRESH_WALLET_NO_ACTIVITY_CACHE_SECONDS:
        return dict(cached)

    url, params, setup_error = _explorer_first_activity_params(activity_source, address)
    if setup_error:
        return {"status": setup_error, "chain": chain_key, "chain_name": activity_source.get("name", chain_key), "first_timestamp": 0}

    last_error = ""
    for attempt in range(3):
        try:
            response = session.get(url, params=params, timeout=20)
            data = response.json()
        except requests.RequestException as exc:
            last_error = f"request_error:{exc.__class__.__name__}"
            time.sleep(0.5 + attempt * 0.5)
            continue
        except ValueError:
            last_error = "invalid_json"
            time.sleep(0.5 + attempt * 0.5)
            continue

        result = data.get("result")
        message = str(data.get("message") or "")
        if data.get("status") == "1" and isinstance(result, list) and result:
            tx = result[0]
            item = {
                "status": "ok",
                "chain": chain_key,
                "chain_name": activity_source.get("name", chain_key),
                "first_timestamp": int(tx.get("timeStamp") or 0),
                "first_block": int(tx.get("blockNumber") or 0),
                "first_tx": tx.get("hash") or "",
                "source": "normal_tx",
                "checked_at": int(base_ts),
            }
            entry[chain_key] = item
            return dict(item)
        if message.lower().startswith("no transactions") or result == []:
            item = {
                "status": "no_activity",
                "chain": chain_key,
                "chain_name": activity_source.get("name", chain_key),
                "first_timestamp": 0,
                "first_block": 0,
                "first_tx": "",
                "source": "normal_tx",
                "checked_at": int(base_ts),
            }
            entry[chain_key] = item
            return dict(item)

        last_error = f"explorer_{data.get('status', 'unknown')}:{message or str(result)[:80]}"
        if "rate" in last_error.lower() or "limit" in last_error.lower():
            time.sleep(1.0 + attempt)
            continue
        break

    return {
        "status": last_error or "explorer_error",
        "chain": chain_key,
        "chain_name": activity_source.get("name", chain_key),
        "first_timestamp": 0,
    }


def _debank_age_range_from_text(text):
    """Return lower/upper age bounds in days for a rendered DeBank age label."""
    match = re.search(
        r'([0-9]+(?:\.[0-9]+)?)\s*'
        r'(seconds?|secs?|minutes?|mins?|hours?|hrs?|days?|months?|mos?|years?|yrs?)',
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None, None, ""
    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit.startswith("sec"):
        days = value / 86400
        max_days = (value + 1) / 86400
    elif unit.startswith("min"):
        days = value / 1440
        max_days = (value + 1) / 1440
    elif unit.startswith("hour") or unit.startswith("hr"):
        days = value / 24
        max_days = (value + 1) / 24
    elif unit.startswith("day"):
        days = value
        max_days = value + 1
    elif unit.startswith("month") or unit.startswith("mo"):
        days = value * 30
        max_days = (value + 1) * 30
    elif unit.startswith("year") or unit.startswith("yr"):
        days = value * 365
        max_days = (value + 1) * 365
    else:
        return None, None, ""
    return days, max_days, f"{match.group(1)} {match.group(2)}"


def parse_debank_age_range_days(html):
    """Extract the rendered DeBank wallet age tag and conservative bounds."""
    if not html:
        return None, None, ""
    class_pattern = (
        r'<[^>]+class=["\'][^"\']*(?:db-user-tag[^"\']*is-age|is-age[^"\']*db-user-tag)[^"\']*["\'][^>]*>'
        r'(.*?)</[^>]+>'
    )
    for match in re.finditer(class_pattern, html, flags=re.IGNORECASE | re.DOTALL):
        text = re.sub(r'<[^>]+>', ' ', match.group(1))
        age_days, age_max_days, raw_age = _debank_age_range_from_text(text)
        if age_days is not None:
            return age_days, age_max_days, raw_age
    return None, None, ""


def parse_debank_age_days(html):
    """Extract the rendered DeBank wallet age tag from profile HTML."""
    age_days, _age_max_days, raw_age = parse_debank_age_range_days(html)
    return age_days, raw_age


def _normalize_cached_debank_age(item):
    if not item or item.get("status") != "ok":
        return item
    if item.get("debank_age_max_days") is not None:
        return item
    age_days, age_max_days, _raw_age = _debank_age_range_from_text(item.get("debank_raw_age", ""))
    if age_max_days is None and item.get("debank_age_days") is not None:
        try:
            age_days = float(item.get("debank_age_days"))
            age_max_days = age_days + 1
        except (TypeError, ValueError):
            return item
    if age_days is not None and item.get("debank_age_days") is None:
        item["debank_age_days"] = round(age_days, 4)
    if age_max_days is not None:
        item["debank_age_max_days"] = round(age_max_days, 4)
    return item


def _current_debank_age_range(first_activity, base_ts):
    _normalize_cached_debank_age(first_activity)
    age_days = first_activity.get("debank_age_days")
    age_max_days = first_activity.get("debank_age_max_days")
    first_timestamp = int(first_activity.get("first_timestamp") or 0)
    if age_days is None or age_max_days is None:
        return None, None
    if first_timestamp <= 0:
        return float(age_days), float(age_max_days)
    uncertainty_days = max(0.0, float(age_max_days) - float(age_days))
    current_age_days = max(0.0, (int(base_ts) - first_timestamp) / 86400)
    return current_age_days, current_age_days + uncertainty_days


def _fresh_activity_within_period(first_activity, period, base_ts):
    if first_activity.get("source") == "debank_age":
        _age_days, age_max_days = _current_debank_age_range(first_activity, base_ts)
        if age_max_days is not None:
            return age_max_days <= (PERIODS[period] / 86400)
    wallet_created_ts = int(first_activity.get("first_timestamp") or 0)
    period_start_ts = int(base_ts) - PERIODS[period]
    return wallet_created_ts >= period_start_ts


def find_debank_chrome_binary():
    candidates = [
        FRESH_DEBANK_CHROME_BIN,
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if os.path.isabs(candidate) and os.path.exists(candidate):
            return candidate
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return ""


def fetch_debank_first_activity(address, state, base_ts):
    """Fallback wallet-age verifier using rendered DeBank profile age.

    This is intentionally narrow: it only runs for 10K+ fresh-wallet candidates
    after explorer verification returns an inconclusive result. Successful
    lookups are cached so the scheduled GitHub job does not repeatedly render
    the same DeBank profile.
    """
    if not FRESH_DEBANK_AGE_FALLBACK:
        return {"verified": False, "status": "debank_disabled", "first_timestamp": 0}
    addr = (address or "").lower()
    if not addr:
        return {"verified": False, "status": "debank_missing_address", "first_timestamp": 0}

    cache = state.setdefault(FRESH_DEBANK_AGE_CACHE_KEY, {})
    cached = cache.get(addr) or {}
    checked_at = int(cached.get("checked_at") or 0)
    cache_ttl = FRESH_DEBANK_AGE_CACHE_SECONDS if cached.get("status") == "ok" else FRESH_DEBANK_FAILURE_CACHE_SECONDS
    if cached and base_ts - checked_at < cache_ttl:
        item = dict(cached)
        _normalize_cached_debank_age(item)
        item["verified"] = item.get("status") == "ok" and int(item.get("first_timestamp") or 0) > 0
        return item

    chrome = find_debank_chrome_binary()
    if not chrome:
        item = {
            "status": "debank_no_chrome",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": 0,
            "source": "debank_age",
            "checked_at": int(base_ts),
        }
        cache[addr] = item
        return {"verified": False, **item}

    url = f"https://debank.com/profile/{addr}/history"
    try:
        cmd = [
            chrome,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-default-apps",
            f"--virtual-time-budget={FRESH_DEBANK_VIRTUAL_TIME_BUDGET_MS}",
            "--dump-dom",
            url,
        ]
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=FRESH_DEBANK_HEADLESS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        item = {
            "status": "debank_timeout",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": 0,
            "source": "debank_age",
            "checked_at": int(base_ts),
        }
        cache[addr] = item
        return {"verified": False, **item}
    except Exception as exc:
        item = {
            "status": f"debank_error:{exc.__class__.__name__}",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": 0,
            "source": "debank_age",
            "checked_at": int(base_ts),
        }
        cache[addr] = item
        return {"verified": False, **item}

    if proc.returncode != 0 and not proc.stdout:
        item = {
            "status": f"debank_chrome_exit_{proc.returncode}",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": 0,
            "source": "debank_age",
            "checked_at": int(base_ts),
        }
        cache[addr] = item
        return {"verified": False, **item}

    age_days, age_max_days, raw_age = parse_debank_age_range_days(proc.stdout)
    if age_days is None:
        item = {
            "status": "debank_age_missing",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": 0,
            "source": "debank_age",
            "checked_at": int(base_ts),
        }
        cache[addr] = item
        return {"verified": False, **item}

    first_ts = max(0, int(base_ts - age_days * 86400))
    item = {
        "status": "ok",
        "chain": "debank",
        "chain_name": "DeBank",
        "first_timestamp": first_ts,
        "first_block": 0,
        "first_tx": "",
        "source": "debank_age",
        "debank_age_days": round(age_days, 4),
        "debank_age_max_days": round(age_max_days, 4),
        "debank_raw_age": raw_age,
        "checked_at": int(base_ts),
    }
    cache[addr] = item
    return {"verified": True, **item}


def wallet_first_activity(address, state, session, base_ts):
    results = []
    errors = []
    oldest_fresh_cutoff = int(base_ts) - max(PERIODS[period] for period in FRESH_HOLDER_PERIODS)
    for activity_source in FRESH_WALLET_ACTIVITY_SOURCES:
        item = fetch_first_chain_activity(activity_source, address, state, session, base_ts)
        status = item.get("status")
        if status == "ok" and int(item.get("first_timestamp") or 0) > 0:
            if int(item.get("first_timestamp") or 0) < oldest_fresh_cutoff:
                return {"verified": True, **item}
            results.append(item)
        elif status != "no_activity":
            errors.append({
                "chain": activity_source["key"],
                "chain_name": activity_source.get("name", activity_source["key"]),
                "status": status,
            })
        time.sleep(FRESH_ETHERSCAN_REQUEST_DELAY_SECONDS if activity_source["provider"] == "etherscan" else 0.08)

    if errors:
        fallback = fetch_debank_first_activity(address, state, base_ts)
        if fallback.get("verified"):
            return {**fallback, "explorer_errors": errors}
        return {"verified": False, "status": "explorer_unverified", "errors": errors}
    if not results:
        fallback = fetch_debank_first_activity(address, state, base_ts)
        if fallback.get("verified"):
            return fallback
        return {"verified": False, "status": "no_normal_activity", "errors": []}
    first = min(results, key=lambda item: int(item.get("first_timestamp") or 0))
    if int(first.get("first_timestamp") or 0) >= oldest_fresh_cutoff:
        fallback = fetch_debank_first_activity(address, state, base_ts)
        if fallback.get("verified") and int(fallback.get("first_timestamp") or 0) <= int(first.get("first_timestamp") or 0):
            return {**fallback, "explorer_first_activity": first}
        if fallback.get("verified"):
            return {"verified": True, **first, "debank_crosscheck": fallback}
        return {
            "verified": False,
            "status": "debank_crosscheck_unverified",
            "errors": [{"chain": "debank", "chain_name": "DeBank", "status": fallback.get("status", "unknown")}],
            "explorer_first_activity": first,
        }
    return {"verified": True, **first}


def calculate_current_balances_by_chain(all_transfers):
    balances = {chain_key: {} for chain_key in CHAINS}
    for chain_key, transfers in all_transfers.items():
        chain_balances = balances.setdefault(chain_key, {})
        for from_addr, to_addr, value_wei, _ in transfers:
            value = value_wei / (10 ** 18)
            if from_addr != ZERO:
                chain_balances[from_addr] = chain_balances.get(from_addr, 0) - value
            if to_addr != ZERO:
                chain_balances[to_addr] = chain_balances.get(to_addr, 0) + value
    return balances


def fresh_holder_history_coverage(state, all_transfers, cutoff_blocks):
    meta = {}
    ok = True
    for chain_key, cfg in CHAINS.items():
        required_start = cutoff_blocks.get(chain_key, {}).get("all", cfg.get("deploy_block", 0))
        state_start = state.get(f"{chain_key}_history_start_block")
        transfer_min = min((t[3] for t in all_transfers.get(chain_key, [])), default=None)
        coverage_start = int(state_start or transfer_min or 0)
        chain_ok = bool(coverage_start and coverage_start <= required_start)
        ok = ok and chain_ok
        meta[chain_key] = {
            "requiredStartBlock": required_start,
            "coverageStartBlock": coverage_start,
            "transferMinBlock": transfer_min,
            "ok": chain_ok,
        }
    return ok, meta


def build_fresh_holders(all_transfers, cutoff_blocks, current_blocks, neutralized_flows_cache, base_ts, state):
    """Build true fresh DOLO wallets.

    A wallet is fresh for a period only when its first normal on-chain account
    transaction across Ethereum/Berachain happened inside that period. DOLO
    transfer history is still used for received amount, liquid balance, and
    current DOLO exposure including active veDOLO locks.
    """
    holder_rows = load_current_holder_rows()
    address_labels = load_address_labels()
    current_locks = load_current_vedolo_locks()
    current_balances = calculate_current_balances_by_chain(all_transfers)
    session = requests.Session()
    first_in_period = {period: {} for period in FRESH_HOLDER_PERIODS}
    preexisting_by_period = {period: set() for period in FRESH_HOLDER_PERIODS}
    received_by_period = {period: {} for period in FRESH_HOLDER_PERIODS}
    tx_count_by_period = {period: {} for period in FRESH_HOLDER_PERIODS}
    audit = {
        period: {
            "candidateWallets": 0,
            "verifiedFreshWallets": 0,
            "oldWalletsExcluded": 0,
            "unverifiedExcluded": 0,
        }
        for period in FRESH_HOLDER_PERIODS
    }

    for chain_key, transfers in all_transfers.items():
        for from_addr, to_addr, value_wei, block_number in transfers:
            to_key = (to_addr or "").lower()
            from_key = (from_addr or "").lower()
            value = value_wei / (10 ** 18)
            if value <= 0:
                continue
            estimated_ts = transfer_estimated_timestamp(chain_key, block_number, current_blocks, base_ts)
            for period in FRESH_HOLDER_PERIODS:
                cutoff = cutoff_blocks.get(chain_key, {}).get(period, 0)
                if block_number < cutoff:
                    if to_key and to_key not in FLOW_SKIP_ADDRS:
                        preexisting_by_period[period].add(to_key)
                    if from_key and from_key not in FLOW_SKIP_ADDRS:
                        preexisting_by_period[period].add(from_key)
                    continue
                if not to_key or to_key in FLOW_SKIP_ADDRS:
                    continue
                received_by_period[period][to_key] = received_by_period[period].get(to_key, 0) + value
                tx_count_by_period[period][to_key] = tx_count_by_period[period].get(to_key, 0) + 1
                existing = first_in_period[period].get(to_key)
                if existing is None or estimated_ts < existing["estimated_ts"]:
                    first_in_period[period][to_key] = {
                        "chain": chain_key,
                        "block": block_number,
                        "estimated_ts": estimated_ts,
                        "source_address": from_key,
                        "amount": value,
                    }

    merged_net_by_period = {
        period: merge_balance_changes(neutralized_flows_cache.get(period, {chain_key: {} for chain_key in CHAINS}))
        for period in FRESH_HOLDER_PERIODS
    }
    result = {}

    def emit_fresh_audit(period, reason, addr, received, current_exposure, liquid_balance, locked_balance, holder_type, first_activity=None):
        if not FRESH_WALLET_AUDIT_VERBOSE or period != "90d":
            return
        first_activity = first_activity or {}
        first_ts = int(first_activity.get("first_timestamp") or 0)
        first_iso = datetime.utcfromtimestamp(first_ts).isoformat() + "Z" if first_ts > 0 else ""
        errors = ",".join(
            f"{item.get('chain')}:{item.get('status')}"
            for item in first_activity.get("errors", [])[:8]
        )
        print(
            "FRESH_AUDIT_90D\t"
            f"reason={reason}\t"
            f"address={addr}\t"
            f"received={received:.6f}\t"
            f"exposure={current_exposure:.6f}\t"
            f"liquid={liquid_balance:.6f}\t"
            f"locked={locked_balance:.6f}\t"
            f"type={holder_type}\t"
            f"status={first_activity.get('status', '')}\t"
            f"source={first_activity.get('source', '')}\t"
            f"debank_age_days={first_activity.get('debank_age_days', '')}\t"
            f"first_chain={first_activity.get('chain', '')}\t"
            f"first_timestamp={first_iso}\t"
            f"first_block={first_activity.get('first_block', '')}\t"
            f"errors={errors}",
            flush=True,
        )

    for period in FRESH_HOLDER_PERIODS:
        rows = []
        period_received = received_by_period.get(period, {})
        period_txs = tx_count_by_period.get(period, {})
        period_net = merged_net_by_period.get(period, {})
        preexisting = preexisting_by_period.get(period, set())
        for addr, received in period_received.items():
            audit[period]["candidateWallets"] += 1
            if addr in preexisting:
                continue
            first = first_in_period.get(period, {}).get(addr)
            if not first:
                continue
            first_chain = first["chain"]
            if addr in EXCLUDED_ADDRS:
                continue
            holder_type = holder_distribution_type(addr, holder_rows, address_labels)
            if holder_type in {"cex", "ca", "watch"}:
                continue
            if received <= FRESH_HOLDER_MIN_RECEIVED:
                continue
            balance_eth = max(0, current_balances.get("eth", {}).get(addr, 0))
            balance_bera = max(0, current_balances.get("bera", {}).get(addr, 0))
            liquid_balance = balance_eth + balance_bera
            locked_balance = max(0, current_locks.get(addr, 0))
            current_exposure = liquid_balance + locked_balance
            if current_exposure <= FRESH_HOLDER_MIN_EXPOSURE:
                continue
            first_activity = wallet_first_activity(addr, state, session, base_ts)
            if not first_activity.get("verified"):
                emit_fresh_audit(period, "unverified", addr, received, current_exposure, liquid_balance, locked_balance, holder_type, first_activity)
                audit[period]["unverifiedExcluded"] += 1
                continue
            wallet_created_ts = int(first_activity.get("first_timestamp") or 0)
            if not _fresh_activity_within_period(first_activity, period, base_ts):
                emit_fresh_audit(period, "old", addr, received, current_exposure, liquid_balance, locked_balance, holder_type, first_activity)
                audit[period]["oldWalletsExcluded"] += 1
                continue
            emit_fresh_audit(period, "fresh", addr, received, current_exposure, liquid_balance, locked_balance, holder_type, first_activity)
            # DeBank's conservative wallet-age range gates freshness, but when
            # the explorer supplied the first normal transaction we retain that
            # exact timestamp for the dashboard's Created column.
            explorer_first = first_activity.get("explorer_first_activity")
            created_activity = explorer_first if isinstance(explorer_first, dict) else first_activity
            wallet_created_ts = int(created_activity.get("first_timestamp") or wallet_created_ts)
            created_source = created_activity.get("source", "normal_tx")
            age_verification_source = first_activity.get("source", created_source)
            verification_source = (
                f"{created_source}+{age_verification_source}"
                if created_source != age_verification_source
                else created_source
            )
            wallet_age_days = first_activity.get("debank_age_days")
            wallet_age_max_days = first_activity.get("debank_age_max_days")
            if first_activity.get("source") == "debank_age":
                wallet_age_days, wallet_age_max_days = _current_debank_age_range(first_activity, base_ts)
            chains = []
            if balance_eth > 0.0001:
                chains.append("eth")
            if balance_bera > 0.0001:
                chains.append("bera")
            if locked_balance > 0.0001 and "bera" not in chains:
                chains.append("bera")
            if not chains:
                chains.append(first_chain)
            info = address_labels.get(addr, {})
            row = {
                "address": addr,
                "label": info.get("label", ""),
                "type": holder_type,
                "source": source_label_for_fresh_wallet(first.get("source_address"), address_labels),
                "source_address": first.get("source_address", ""),
                "first_chain": created_activity.get("chain", first_chain),
                "first_block": int(created_activity.get("first_block") or 0),
                "first_timestamp_estimate": datetime.utcfromtimestamp(max(0, wallet_created_ts)).isoformat() + "Z",
                "wallet_created_chain": created_activity.get("chain", ""),
                "wallet_created_block": int(created_activity.get("first_block") or 0),
                "wallet_created_tx": created_activity.get("first_tx", ""),
                "wallet_created_timestamp": datetime.utcfromtimestamp(max(0, wallet_created_ts)).isoformat() + "Z",
                "wallet_created_source": created_source,
                "verification_source": verification_source,
                "wallet_age_verification_source": age_verification_source,
                "wallet_age_days": round(wallet_age_days, 4) if wallet_age_days is not None else None,
                "wallet_age_max_days": round(wallet_age_max_days, 4) if wallet_age_max_days is not None else None,
                "first_dolo_chain": first_chain,
                "first_dolo_block": first["block"],
                "first_dolo_timestamp_estimate": datetime.utcfromtimestamp(max(0, first["estimated_ts"])).isoformat() + "Z",
                "received": round(received, 6),
                "net_flow": round(period_net.get(addr, 0), 6),
                "balance": round(current_exposure, 6),
                "exposure": round(current_exposure, 6),
                "liquid_balance": round(liquid_balance, 6),
                "locked_balance": round(locked_balance, 6),
                "balance_eth": round(balance_eth, 6),
                "balance_bera": round(balance_bera, 6),
                "chains": chains,
                "tx_count": period_txs.get(addr, 0),
                "retention": round((current_exposure / received) if received > 0 else 0, 4),
            }
            rows.append(row)
            audit[period]["verifiedFreshWallets"] += 1
        rows.sort(key=lambda item: item["received"], reverse=True)
        result[period] = rows
    return result, audit


def load_current_vedolo_locks():
    holders_file = os.path.join(DATA_DIR, "vedolo_holders.json")
    if not os.path.exists(holders_file):
        return {}
    try:
        with open(holders_file) as f:
            holders_data = json.load(f)
        return {
            h.get("address", "").lower(): float(h.get("total_dolo") or 0)
            for h in holders_data.get("holders", [])
            if h.get("address") and float(h.get("total_dolo") or 0) > 0
        }
    except Exception as e:
        print(f"  ⚠️ Could not load veDOLO holders for bucket history: {e}")
        return {}


def load_vedolo_flow_events():
    flows_file = os.path.join(DATA_DIR, "vedolo_flows.json")
    if not os.path.exists(flows_file):
        return {"locks": [], "unlocks": []}
    try:
        with open(flows_file) as f:
            data = json.load(f)
        return {
            "locks": data.get("locks") or [],
            "unlocks": data.get("unlocks") or [],
        }
    except Exception as e:
        print(f"  ⚠️ Could not load veDOLO flows for bucket history: {e}")
        return {"locks": [], "unlocks": []}


def locked_map_at_holder_point(point_ts, current_locks, vedolo_events):
    locked = dict(current_locks)
    for lock in vedolo_events.get("locks", []):
        ts = int(lock.get("timestamp") or 0)
        if ts < point_ts:
            continue
        addr = (lock.get("beneficiaryAddress") or lock.get("address") or "").lower()
        if not addr:
            continue
        locked[addr] = locked.get(addr, 0) - float(lock.get("dolo") or 0)
    for unlock in vedolo_events.get("unlocks", []):
        ts = int(unlock.get("timestamp") or 0)
        if ts < point_ts:
            continue
        addr = (unlock.get("address") or "").lower()
        if not addr:
            continue
        locked[addr] = locked.get(addr, 0) + float(unlock.get("dolo") or 0)
    # Hard guard: never attribute pooled locks to the veDOLO contract itself
    # (possible if a lock event lacks beneficiaryAddress and falls back to the
    # provider address) — that would double-count locked supply per wallet.
    locked.pop(VEDOLO_CONTRACT_ADDR, None)
    return {addr: value for addr, value in locked.items() if value > 0.0001}


def detect_cex_deposit_candidates(all_transfers, vesting_labels=None,
                                  min_dolo=CEX_DEPOSIT_FLAG_MIN_DOLO, top_n=25):
    """Flag potential NEW CEX deposit addresses.

    Classic funnel: many users -> fresh deposit address -> sweep to a labeled
    CEX hot wallet. The sweep sender is the deposit address, so any unlabeled
    wallet that has sent >= min_dolo directly to a type:"cex" label is a
    candidate. Results are advisory (written to dolo_flows.json cex_watch and
    the CEX label audit); promotion to a real label stays manual.
    """
    labels = load_address_labels(vesting_labels)
    cex_addrs = {a for a, info in labels.items() if str(info.get("type", "")).lower() == "cex"}
    sent, tx_counts = {}, {}
    for chain_key, transfers in all_transfers.items():
        for from_addr, to_addr, value_wei, _block in transfers:
            if to_addr in cex_addrs and from_addr not in cex_addrs \
                    and from_addr not in labels and from_addr not in EXCLUDED_ADDRS:
                sent[from_addr] = sent.get(from_addr, 0.0) + value_wei / 1e18
                tx_counts[from_addr] = tx_counts.get(from_addr, 0) + 1
    ranked = sorted(((a, v) for a, v in sent.items() if v >= min_dolo), key=lambda item: -item[1])
    return [
        {"address": addr, "sentToCexDolo": round(value, 2), "txCount": tx_counts.get(addr, 0)}
        for addr, value in ranked[:top_n]
    ]


def empty_bucket_model(bucket_defs, audience):
    buckets = [
        {
            "wallets": 0,
            "total": 0,
            "liquid": 0,
            "locked": 0,
            "allocationWallets": 0,
            "allocationTotal": 0,
            "allocationLiquid": 0,
            "allocationLocked": 0,
            "teamWallets": 0,
            "teamTotal": 0,
            "investorWallets": 0,
            "investorTotal": 0,
        }
        for _ in bucket_defs
    ]
    return {
        "audience": audience,
        "buckets": buckets,
        "trackedWallets": 0,
        "trackedTotal": 0,
        "trackedLiquid": 0,
        "trackedLocked": 0,
        "excludedCexWallets": 0,
        "excludedCexTotal": 0,
        "excludedPotentialWallets": 0,
        "excludedPotentialTotal": 0,
        "excludedInsiderWallets": 0,
        "excludedInsiderTotal": 0,
        "allocationWallets": 0,
        "allocationTotal": 0,
        "allocationLiquid": 0,
        "allocationLocked": 0,
        "teamWallets": 0,
        "teamTotal": 0,
        "investorWallets": 0,
        "investorTotal": 0,
    }


def holder_belongs_to_audience(holder_type, audience):
    if audience == "potential":
        return holder_type in HOLDER_POTENTIAL_TYPES
    if audience == "market":
        return holder_type not in HOLDER_MARKET_EXCLUDED_TYPES and holder_type not in HOLDER_POTENTIAL_TYPES
    raise ValueError(f"Unsupported holder audience: {audience}")


def build_bucket_model(
    liquid_balances,
    locked_balances,
    holder_rows,
    address_labels,
    bucket_defs,
    include_allocations=False,
    audience="market",
):
    model = empty_bucket_model(bucket_defs, audience)
    addresses = set(liquid_balances.keys()) | set(locked_balances.keys())
    for addr in addresses:
        liquid = max(0, float(liquid_balances.get(addr) or 0))
        locked = max(0, float(locked_balances.get(addr) or 0))
        total = liquid + locked
        if total <= 0:
            continue
        holder_type = holder_distribution_type(addr, holder_rows, address_labels)
        if holder_type in {"cex", "ca"}:
            model["excludedCexWallets"] += 1
            model["excludedCexTotal"] += total
            continue
        is_allocation = holder_type in {"team", "investor"}
        if is_allocation:
            model["allocationWallets"] += 1
            model["allocationTotal"] += total
            model["allocationLiquid"] += liquid
            model["allocationLocked"] += locked
            if holder_type == "team":
                model["teamWallets"] += 1
                model["teamTotal"] += total
            elif holder_type == "investor":
                model["investorWallets"] += 1
                model["investorTotal"] += total
        if is_allocation and (audience == "potential" or not include_allocations):
            model["excludedInsiderWallets"] += 1
            model["excludedInsiderTotal"] += total
            continue
        if holder_type in HOLDER_POTENTIAL_TYPES and audience == "market":
            model["excludedPotentialWallets"] += 1
            model["excludedPotentialTotal"] += total
            continue
        if not holder_belongs_to_audience(holder_type, audience):
            continue
        bucket_index = next((idx for idx, bucket in enumerate(bucket_defs) if total >= bucket["min"] and total < bucket["max"]), None)
        if bucket_index is None:
            continue
        bucket = model["buckets"][bucket_index]
        bucket["wallets"] += 1
        bucket["total"] += total
        bucket["liquid"] += liquid
        bucket["locked"] += locked
        if is_allocation:
            bucket["allocationWallets"] += 1
            bucket["allocationTotal"] += total
            bucket["allocationLiquid"] += liquid
            bucket["allocationLocked"] += locked
            if holder_type == "team":
                bucket["teamWallets"] += 1
                bucket["teamTotal"] += total
            elif holder_type == "investor":
                bucket["investorWallets"] += 1
                bucket["investorTotal"] += total
        model["trackedWallets"] += 1
        model["trackedTotal"] += total
        model["trackedLiquid"] += liquid
        model["trackedLocked"] += locked
    for bucket in model["buckets"]:
        for key in [
            "total", "liquid", "locked", "allocationTotal", "allocationLiquid",
            "allocationLocked", "teamTotal", "investorTotal",
        ]:
            bucket[key] = round(bucket[key], 2)
    for key in [
        "trackedTotal", "trackedLiquid", "trackedLocked", "excludedCexTotal",
        "excludedPotentialTotal",
        "excludedInsiderTotal", "allocationTotal", "allocationLiquid",
        "allocationLocked", "teamTotal", "investorTotal",
    ]:
        model[key] = round(model[key], 2)
    return model


def build_bucket_wallet_history_rows(
    liquid_by_chain,
    locked_balances,
    holder_rows,
    address_labels,
    bucket_defs,
    audience="market",
):
    rows = []
    addresses = set(locked_balances.keys())
    for chain_balances in liquid_by_chain.values():
        addresses.update(chain_balances.keys())
    for addr in addresses:
        bal_eth = max(0, float(liquid_by_chain.get("eth", {}).get(addr) or 0))
        bal_bera = max(0, float(liquid_by_chain.get("bera", {}).get(addr) or 0))
        liquid = bal_eth + bal_bera
        locked = max(0, float(locked_balances.get(addr) or 0))
        total = liquid + locked
        if total <= 0.0001:
            continue
        holder_type = holder_distribution_type(addr, holder_rows, address_labels)
        if not holder_belongs_to_audience(holder_type, audience):
            continue
        if not any(total >= bucket["min"] and total < bucket["max"] for bucket in bucket_defs):
            continue
        holder = holder_rows.get(addr, {})
        info = address_labels.get(addr, {})
        contract_wallet_type = str(holder.get("contract_wallet_type") or "").lower()
        rows.append({
            "address": addr,
            "label": info.get("label", ""),
            "type": holder_type,
            "balance": round(total, 6),
            "liquid": round(liquid, 6),
            "locked": round(locked, 6),
            "balance_eth": round(bal_eth, 6),
            "balance_bera": round(bal_bera, 6),
            "contract_wallet_type": contract_wallet_type,
            "safe": bool(contract_wallet_type in {"safe", "multisig"} or info.get("safe")),
        })
    return sorted(rows, key=lambda row: row["balance"], reverse=True)


def count_txs(transfers, excluded):
    """Count number of transactions per address."""
    counts = {}
    for from_addr, to_addr, _, _ in transfers:
        counts[from_addr] = counts.get(from_addr, 0) + 1
        counts[to_addr] = counts.get(to_addr, 0) + 1
    return counts


def get_top(flows, tx_counts, n, mode="accumulator", excluded=None):
    """Get top N accumulators or sellers, excluding known contracts."""
    if excluded is None:
        excluded = set()
    # 0.005 floor: float dust (e.g. neutralization residue of 0.004 DOLO)
    # passes a bare `> 0` check and then rounds to a misleading "0.00" row.
    if mode == "accumulator":
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1], reverse=True)
        filtered = [(addr, val) for addr, val in sorted_addrs if val >= 0.005 and addr not in excluded]
    else:
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1])
        filtered = [(addr, abs(val)) for addr, val in sorted_addrs if val <= -0.005 and addr not in excluded]

    result = []
    for addr, net in filtered[:n]:
        result.append({
            "address": addr,
            "net_flow": round(net, 2),
            "tx_count": tx_counts.get(addr, 0),
        })
    return result


def get_dolo_price():
    """Fetch current DOLO price from DeFiLlama / CoinGecko."""
    try:
        # Try DeFiLlama
        resp = requests.get(
            "https://coins.llama.fi/prices/current/ethereum:0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654",
            timeout=10
        )
        data = resp.json()
        coins = data.get("coins", {})
        for key, val in coins.items():
            if "price" in val:
                return val["price"]
    except Exception as exc:
        print(f"⚠️ get_dolo_price: DeFiLlama fetch failed ({exc}); falling back to price file", flush=True)

    # Fallback: try reading from existing price file
    price_file = os.path.join(DATA_DIR, "dolo_price.json")
    if os.path.exists(price_file):
        try:
            with open(price_file) as f:
                d = json.load(f)
                return d.get("price", 0)
        except Exception:
            pass
    return 0


def rebuild_holder_history_from_cached_transfers():
    """Rebuild holder snapshots from the committed flow output and local transfer cache."""
    if not os.path.exists(OUTPUT_JSON):
        raise RuntimeError(f"Missing flow output: {OUTPUT_JSON}")
    state = load_state()
    if not state:
        raise RuntimeError(f"Missing transfer cache: {STATE_FILE}")

    with open(OUTPUT_JSON) as f:
        output = json.load(f)
    raw_timestamp = str(output.get("timestamp") or "")
    try:
        parsed_timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError(f"Invalid flow output timestamp: {raw_timestamp!r}") from exc
    if parsed_timestamp.tzinfo is None:
        parsed_timestamp = parsed_timestamp.replace(tzinfo=timezone.utc)
    base_ts = int(parsed_timestamp.timestamp())

    all_transfers = {}
    current_blocks = {}
    for chain_key in CHAINS:
        transfer_key = f"{chain_key}_transfers"
        block_key = f"{chain_key}_last_block"
        transfers = state.get(transfer_key)
        block = int(state.get(block_key) or 0)
        if not isinstance(transfers, list) or not block:
            raise RuntimeError(f"Incomplete cached transfer history for {CHAINS[chain_key]['name']}")
        all_transfers[chain_key] = transfers
        current_blocks[chain_key] = block

    print("📈 Rebuilding holder audience history from cached transfers...")
    print(
        "   " + " | ".join(
            f"{CHAINS[key]['name']}: {len(rows):,} transfers"
            for key, rows in all_transfers.items()
        )
    )
    points = build_holder_history_schedule(base_ts)
    vesting_investors = extract_vesting_investors(all_transfers)
    holder_bucket_history, holder_wallet_history = calculate_holder_bucket_history(
        all_transfers,
        points,
        current_blocks,
        base_ts,
        vesting_investors,
    )

    output["holder_history_points"] = [
        {"key": point["key"], "timestamp": point["timestamp"]}
        for point in points
    ]
    output["holder_bucket_history"] = holder_bucket_history
    output["holder_history_schema"] = "audience-v2"
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    with open(WALLET_HISTORY_JSON, "w") as f:
        json.dump(
            {
                "timestamp": raw_timestamp,
                "holder_wallet_history": holder_wallet_history,
            },
            f,
            separators=(",", ":"),
        )

    latest = holder_bucket_history[-1] if holder_bucket_history else {}
    market = latest.get("liquid", {}).get("market", {}).get("whales", {})
    potential = latest.get("liquid", {}).get("potential", {}).get("whales", {})
    print(
        "  ✅ Rebuilt "
        f"{len(holder_bucket_history):,} snapshots | market {market.get('trackedWallets', 0):,} wallets | "
        f"potential {potential.get('trackedWallets', 0):,} wallets"
    )


def main():
    if "--rebuild-holder-history-only" in sys.argv[1:]:
        rebuild_holder_history_from_cached_transfers()
        return
    print("=" * 60)
    print("🔄 DOLO Token Flows — Top Accumulators & Sellers")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load incremental state
    global _global_state
    state = load_state()
    _global_state = state  # Allow signal handler to save on kill
    is_incremental = bool(state)
    if is_incremental:
        print("📦 Found previous state — running incremental sync")
    else:
        print("🆕 No previous state — running full sync (first run)")

    dolo_price = get_dolo_price()
    print(f"\n💰 DOLO Price: ${dolo_price:.4f}" if dolo_price else "\n⚠️ Could not fetch DOLO price")

    # Get current blocks for each chain. Back off a small confirmation buffer
    # from the live tip: logs at the head can reorg out, and cached transfers
    # are never invalidated (no tx-hash dedup), so scanning the unstable tip
    # risks counting a transfer that later disappears.
    REORG_BUFFER_BLOCKS = {"eth": 5, "bera": 15}
    print("\n📡 Getting current block numbers...")
    current_blocks = {}
    for chain_key, cfg in CHAINS.items():
        blk = get_current_block(cfg["rpcs"])
        previous_last_block = int(state.get(f"{chain_key}_last_block") or 0)
        buffered = validated_scan_end(
            chain_key,
            blk,
            REORG_BUFFER_BLOCKS.get(chain_key, 10),
            previous_last_block,
        )
        current_blocks[chain_key] = buffered
        print(f"  {cfg['name']}: block {blk:,} (scanning to {buffered:,}, reorg buffer)")

    # Calculate cutoff blocks for each period
    cutoff_blocks = {}
    for chain_key, cfg in CHAINS.items():
        cutoff_blocks[chain_key] = {}
        deploy_block = cfg.get("deploy_block", 0)
        for period, seconds in PERIODS.items():
            blocks_back = seconds // cfg["block_time"]
            cutoff = max(current_blocks[chain_key] - blocks_back, deploy_block)
            cutoff_blocks[chain_key][period] = cutoff

    # Determine the oldest block we need per chain (longest period cutoff)
    max_period = max(PERIODS.keys(), key=lambda k: PERIODS[k])

    # Fetch transfers — incremental: only new blocks since last run
    print("\n📡 Fetching Transfer events...")
    all_transfers = {}
    unresolved_history_gaps = {}
    for chain_key in CHAINS:
        oldest_needed = cutoff_blocks[chain_key][max_period]
        end = current_blocks[chain_key]

        # Load cached transfers for this chain
        cached_key = f"{chain_key}_transfers"
        last_block_key = f"{chain_key}_last_block"
        history_start_key = f"{chain_key}_history_start_block"
        cached_transfers = state.get(cached_key, [])
        last_block = state.get(last_block_key, 0)
        history_coverage_start = None

        if is_incremental and last_block > 0 and cached_transfers:
            # Replace a recent overlap authoritatively on every run. The old
            # `last_block + 1` path could silently miss a single block when
            # fetch_start equaled the buffered chain tip.
            fetch_start = incremental_refresh_start(
                last_block,
                oldest_needed,
                RECENT_RESCAN_BLOCKS.get(chain_key, 0),
            )
            restored = [tuple(t) for t in cached_transfers]
            cached_min_block = min((t[3] for t in restored), default=last_block + 1)
            coverage_min_block = int(state.get(history_start_key) or cached_min_block)
            backfill_transfers = []
            backfill_failed = 0
            if coverage_min_block > oldest_needed:
                backfill_end = coverage_min_block - 1
                print(f"  {CHAINS[chain_key]['name']}: backfilling All history blocks {oldest_needed:,} → {backfill_end:,}")
                backfill_transfers, backfill_failed, _ = fetch_transfer_logs(
                    chain_key, oldest_needed, backfill_end
                )
                if backfill_failed:
                    print(f"  ⚠️ {CHAINS[chain_key]['name']}: {backfill_failed} historical backfill chunks failed")
            history_coverage_start = oldest_needed if backfill_failed == 0 else coverage_min_block
            if not block_range_has_work(fetch_start, end):
                print(f"  {CHAINS[chain_key]['name']}: already up to date (block {last_block:,})")
                new_transfers = []
                chunks_failed = 0
            else:
                print(
                    f"  {CHAINS[chain_key]['name']}: authoritative recent refresh "
                    f"{fetch_start:,} → {end:,}"
                )
                # Keep only rows outside the replacement range in progressive
                # checkpoints so an interrupted scan cannot save duplicates.
                cached_as_lists = [
                    list(t) for t in restored
                    if not fetch_start <= int(t[3]) <= end
                ]
                new_transfers, chunks_failed, _ = fetch_transfer_logs(
                    chain_key, fetch_start, end, state=state, cached_transfers_so_far=cached_as_lists
                )

            # Merge: historical backfill + authoritative recent replacement.
            merged = backfill_transfers + replace_transfer_range(
                restored,
                new_transfers,
                fetch_start,
                end,
            )

            # Prune: drop transfers from blocks older than the oldest needed
            merged = [t for t in merged if t[3] >= oldest_needed]

            all_transfers[chain_key] = merged
            print(f"  {CHAINS[chain_key]['name']}: {len(backfill_transfers):,} backfilled + {len(new_transfers):,} new + {len(restored):,} cached → {len(merged):,} total (after pruning)")
        else:
            # Full scan from the oldest needed block (or resume from cached last_block)
            scan_start = oldest_needed
            cached_as_lists = None
            if last_block > 0 and last_block > oldest_needed:
                # Resume from where we left off (partial previous scan)
                scan_start = last_block + 1
                cached_as_lists = cached_transfers  # already lists from JSON
                print(f"  {CHAINS[chain_key]['name']}: resuming partial scan from block {scan_start:,} (had {len(cached_transfers):,} cached txs)")

            fresh_transfers, chunks_failed, total_chunks = fetch_transfer_logs(
                chain_key, scan_start, end, state=state, cached_transfers_so_far=cached_as_lists
            )
            if chunks_failed == 0:
                history_coverage_start = oldest_needed

            # Merge with any cached partial data
            if cached_as_lists:
                restored = [tuple(t) for t in cached_as_lists]
                merged = restored + fresh_transfers
                merged = [t for t in merged if t[3] >= oldest_needed]
                all_transfers[chain_key] = merged
            elif len(fresh_transfers) == 0 and cached_transfers:
                # DEFENSIVE FALLBACK: if fresh scan returns 0 but cache has data, use cache
                restored = [tuple(t) for t in cached_transfers]
                restored = [t for t in restored if t[3] >= oldest_needed]
                if restored:
                    print(f"  🛡️ {CHAINS[chain_key]['name']}: fresh scan returned 0 transfers but cache has {len(restored):,} — using cached data as fallback")
                    all_transfers[chain_key] = restored
                else:
                    all_transfers[chain_key] = fresh_transfers
            else:
                all_transfers[chain_key] = fresh_transfers

        all_transfers[chain_key], unresolved_gaps, repaired_count = repair_skipped_ranges(
            chain_key,
            all_transfers[chain_key],
            state,
            oldest_needed,
            end,
        )
        unresolved_history_gaps[chain_key] = unresolved_gaps
        if repaired_count:
            print(
                f"  🩹 {CHAINS[chain_key]['name']}: restored "
                f"{repaired_count:,} transfer(s) from skipped ranges"
            )

        # Diagnostic: warn if chain has 0 transfers
        if len(all_transfers[chain_key]) == 0:
            print(f"  🚨 WARNING: {CHAINS[chain_key]['name']} has 0 transfers! Flow data will be empty for this chain.")

        # Update state for this chain — save immediately so next timeout preserves this chain's data
        state[last_block_key] = end
        if history_coverage_start is not None:
            state[history_start_key] = history_coverage_start
        # Store transfers as lists (JSON can't serialize tuples)
        # Only cache transfers within MAX_CACHE_SECONDS window (180d)
        # to keep state file small — "all" period recalculates from scratch
        cache_blocks_back = MAX_CACHE_SECONDS // CHAINS[chain_key]["block_time"]
        cache_cutoff = max(end - cache_blocks_back, CHAINS[chain_key].get("deploy_block", 0))
        state[cached_key] = [
            list(t) for t in all_transfers[chain_key]
            if t[3] >= cache_cutoff
        ]
        # Save state after each chain completes
        save_state(state)
        print(f"  💾 State saved for {CHAINS[chain_key]['name']} (up to block {end:,})")

    require_complete_flow_history(unresolved_history_gaps)

    # Detect contracts among top addresses (to exclude DEX routers, etc.)
    print("\n🔍 Detecting contract addresses to exclude...")
    address_labels = load_address_labels()
    # Collect all unique addresses from transfers
    for chain_key in CHAINS:
        addr_set = set()
        for from_addr, to_addr, _, _ in all_transfers[chain_key]:
            addr_set.add(from_addr)
            addr_set.add(to_addr)

        # Get flows to find the most active addresses
        flows = calculate_flows(all_transfers[chain_key], EXCLUDED_ADDRS)
        # Check top 30 by absolute flow for contracts
        top_by_flow = sorted(flows.items(), key=lambda x: abs(x[1]), reverse=True)[:30]
        addrs_to_check = [addr for addr, _ in top_by_flow]

        contracts = detect_contracts_batch(addrs_to_check, chain_key)
        dynamic_exclusions = select_dynamic_flow_exclusions(contracts, address_labels)
        EXCLUDED_ADDRS.update(dynamic_exclusions)
        visible_contracts = len(contracts) - len(dynamic_exclusions)
        print(
            f"  {CHAINS[chain_key]['name']}: excluded {len(dynamic_exclusions)} infrastructure contract(s); "
            f"kept {visible_contracts} labeled custody/user contract(s) visible"
        )

    # Calculate flows for each period and chain
    # Cross-chain neutralization: detect bridge transfers (same address, opposite
    # flows on ETH vs Bera) and cancel them so they don't count as in/outflow.
    print("\n📊 Calculating flows...")
    output_periods = {}
    neutralized_flows_cache = {}  # {period: {chain: flows_dict}} — reused for balance_changes
    for period, seconds in PERIODS.items():
        output_periods[period] = {}

        # Step 1: Compute raw flows per chain for this period
        raw_flows = {}
        flow_components_by_chain = {}
        period_transfers_by_chain = {}
        tx_counts_by_chain = {}
        for chain_key in CHAINS:
            cutoff = cutoff_blocks[chain_key][period]
            period_transfers = [t for t in all_transfers[chain_key] if t[3] >= cutoff]
            period_transfers_by_chain[chain_key] = period_transfers
            raw_flows[chain_key] = calculate_flows(period_transfers, EXCLUDED_ADDRS)
            flow_components_by_chain[chain_key] = calculate_flow_components(period_transfers)
            tx_counts_by_chain[chain_key] = count_txs(period_transfers, EXCLUDED_ADDRS)

        # Step 2: Inject bridge mint/burn flows for cross-chain detection
        # Bridge mints (from 0x0) and burns (to 0x0) are skipped by calculate_flows()
        # but needed for neutralization to detect opposing cross-chain patterns.
        # We add them as supplementary flows that only affect neutralization.
        bridge_flows_by_chain = {}
        for chain_key in CHAINS:
            bridge_flows_by_chain[chain_key] = calculate_bridge_flows(
                period_transfers_by_chain[chain_key]
            )
        
        # Merge bridge flows into raw_flows for neutralization
        augmented_flows = {}
        for chain_key in CHAINS:
            augmented_flows[chain_key] = dict(raw_flows[chain_key])  # copy
            for addr, bflow in bridge_flows_by_chain[chain_key].items():
                augmented_flows[chain_key][addr] = augmented_flows[chain_key].get(addr, 0) + bflow
        
        # Step 3: Neutralize cross-chain bridge transfers using augmented flows
        neutralized_aug, n_count, n_volume = neutralize_cross_chain_flows(augmented_flows)
        
        # Apply the neutralization delta back to the ORIGINAL raw_flows
        # (so mints/burns don't pollute the final output, only cancellations do)
        neutralized = {}
        for chain_key in CHAINS:
            neutralized[chain_key] = dict(raw_flows[chain_key])  # start from original
            for addr in raw_flows[chain_key]:
                original_aug = raw_flows[chain_key].get(addr, 0) + bridge_flows_by_chain[chain_key].get(addr, 0)
                neutralized_aug_val = neutralized_aug[chain_key].get(addr, 0)
                delta = neutralized_aug_val - original_aug
                if abs(delta) > 0.01:
                    neutralized[chain_key][addr] = raw_flows[chain_key][addr] + delta
        
        neutralized_flows_cache[period] = neutralized
        if n_count > 0:
            print(f"  🔀 {period}: neutralized {n_count} cross-chain bridge transfers ({n_volume:,.0f} DOLO)")

        # Step 3: Build output using neutralized flows
        for chain_key, cfg in CHAINS.items():
            flows = neutralized[chain_key]
            tx_counts = tx_counts_by_chain[chain_key]

            accumulators = get_top(flows, tx_counts, TOP_N, "accumulator", EXCLUDED_ADDRS)
            sellers = get_top(flows, tx_counts, TOP_N, "seller", EXCLUDED_ADDRS)

            for entry in accumulators + sellers:
                components = flow_components_by_chain[chain_key].get(entry["address"], {})
                entry["gross_inflow"] = round(components.get("gross_inflow", 0), 2)
                entry["gross_outflow"] = round(components.get("gross_outflow", 0), 2)

            # Add USD values
            if dolo_price:
                for entry in accumulators + sellers:
                    entry["usd_value"] = round(entry["net_flow"] * dolo_price, 2)

            output_periods[period][chain_key] = {
                "accumulators": accumulators,
                "sellers": sellers,
                "total_transfers": len(period_transfers_by_chain[chain_key]),
            }

            print(f"  {period} {cfg['name']}: {len(period_transfers_by_chain[chain_key]):,} transfers, "
                  f"top accumulator: {accumulators[0]['net_flow']:,.0f} DOLO" if accumulators else
                  f"  {period} {cfg['name']}: no data")

    # Fetch DOLO balances for all addresses across both chains
    all_addrs = set()
    for period_data in output_periods.values():
        for chain_data in period_data.values():
            for entry in chain_data["accumulators"] + chain_data["sellers"]:
                all_addrs.add(entry["address"])

    print(f"\n💰 Fetching DOLO balances for {len(all_addrs)} addresses...")
    balances, bal_failures, bal_failed_addrs = fetch_dolo_balances(all_addrs)
    if bal_failures:
        print(f"  ⚠️ {bal_failures} balance lookups failed across all retries")

    # Fallback: cross-reference with dolo_holders.json ONLY for addresses where RPC FAILED
    # (not for addresses that legitimately have zero balance — those sold/transferred all tokens)
    holders_file = os.path.join(DATA_DIR, "dolo_holders.json")
    if bal_failed_addrs and os.path.exists(holders_file):
        try:
            with open(holders_file) as f:
                holders_data = json.load(f)
            holders_lookup = {h["address"].lower(): h for h in holders_data.get("holders", [])}
            fixed = 0
            for addr in bal_failed_addrs:
                if balances.get(addr, 0) == 0:
                    h = holders_lookup.get(addr.lower())
                    if h and h.get("balance", 0) > 0:
                        balances[addr] = round(h["balance"], 2)
                        fixed += 1
            if fixed:
                print(f"  🛡️ Patched {fixed} RPC-failed addresses from dolo_holders.json fallback")
        except Exception as e:
            print(f"  ⚠️ Could not load holders fallback: {e}")

    # Add balances to all entries
    for period_data in output_periods.values():
        for chain_data in period_data.values():
            for entry in chain_data["accumulators"] + chain_data["sellers"]:
                entry["balance"] = balances.get(entry["address"], 0)
    # Build balance_changes: address -> net_flow for ALL addresses per period
    # Uses already-neutralized flows from the cache (bridge transfers cancelled out)
    balance_changes = {}
    for period in PERIODS:
        balance_changes[period] = merge_balance_changes(neutralized_flows_cache[period])

    holder_history_base_ts = int(time.time())
    fresh_coverage_ok, fresh_coverage_meta = fresh_holder_history_coverage(state, all_transfers, cutoff_blocks)
    if not fresh_coverage_ok:
        raise RuntimeError(f"Fresh DOLO wallet cohorts require full transfer history coverage: {fresh_coverage_meta}")
    validate_fresh_wallet_activity_config()
    print("\n🆕 Building fresh DOLO wallet cohorts...")
    fresh_holders, fresh_wallet_audit = build_fresh_holders(
        all_transfers,
        cutoff_blocks,
        current_blocks,
        neutralized_flows_cache,
        holder_history_base_ts,
        state,
    )
    for period in FRESH_HOLDER_PERIODS:
        audit_row = fresh_wallet_audit.get(period, {})
        print(
            f"  {period}: {len(fresh_holders.get(period, [])):,} true fresh wallet(s) "
            f"({audit_row.get('oldWalletsExcluded', 0):,} old, "
            f"{audit_row.get('unverifiedExcluded', 0):,} unverified excluded)"
        )

    vesting_investors = extract_vesting_investors(all_transfers)

    # Potential NEW CEX deposit addresses: classic funnel is users -> deposit
    # address -> sweep into a labeled hot wallet, so any unlabeled wallet
    # sending >=10K DOLO straight to a known CEX is a candidate for labeling.
    cex_deposit_candidates = detect_cex_deposit_candidates(all_transfers, vesting_investors)
    if cex_deposit_candidates:
        print(f"\n👀 Potential CEX deposit addresses (top {len(cex_deposit_candidates)}):")
        for c in cex_deposit_candidates[:5]:
            print(f"   {c['address']} sent {c['sentToCexDolo']:,.0f} DOLO to labeled CEXes ({c['txCount']} tx)")

    # Extra holder-bucket chart history. These are still derived from the same
    # transfer logs as the flow tables, but add enough cutoffs for a usable hover.
    holder_history_points = build_holder_history_schedule(holder_history_base_ts)
    print(f"\n📈 Building holder bucket chart history ({len(holder_history_points)} points)...")
    holder_bucket_history, holder_wallet_history = calculate_holder_bucket_history(
        all_transfers,
        holder_history_points,
        current_blocks,
        holder_history_base_ts,
        vesting_investors,
    )
    cex_supply_history = calculate_cex_supply_history(
        all_transfers,
        holder_history_points,
        current_blocks,
        holder_history_base_ts,
        vesting_investors,
    )
    print(f"  ... {len(holder_history_points)}/{len(holder_history_points)} holder history points")

    # Checksum addresses in balance_changes
    try:
        from web3 import Web3
        for period_data in output_periods.values():
            for chain_data in period_data.values():
                for entry in chain_data["accumulators"] + chain_data["sellers"]:
                    try:
                        entry["address"] = Web3.to_checksum_address(entry["address"])
                    except Exception:
                        pass
        for period in balance_changes:
            checksummed = {}
            for addr, val in balance_changes[period].items():
                try:
                    checksummed[Web3.to_checksum_address(addr)] = val
                except Exception:
                    checksummed[addr] = val
            balance_changes[period] = checksummed
        for rows in fresh_holders.values():
            for entry in rows:
                for key in ("address", "source_address"):
                    if not entry.get(key):
                        continue
                    try:
                        entry[key] = Web3.to_checksum_address(entry[key])
                    except Exception:
                        pass
    except ImportError:
        pass

    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "holder_history_start_timestamp": datetime.utcfromtimestamp(HOLDER_HISTORY_START_TIMESTAMP).isoformat() + "Z",
        "holder_history_points": [
            {"key": point["key"], "timestamp": point["timestamp"]}
            for point in holder_history_points
        ],
        "holder_bucket_history": holder_bucket_history,
        # holder_wallet_history is written to WALLET_HISTORY_JSON (separate
        # lazy-loaded file); keep a marker so the UI knows where to find it.
        "holder_wallet_history_file": "dolo_holder_wallet_history.json",
        "cex_supply_history": cex_supply_history,
        # Advisory watchlist: unlabeled wallets funneling DOLO into labeled
        # CEX hot wallets — candidates for new CEX deposit-address labels.
        "cex_watch": {
            "depositCandidates": cex_deposit_candidates,
            "minDolo": CEX_DEPOSIT_FLAG_MIN_DOLO,
        },
        # Mid-range block gaps from persistent RPC failures (if any) — daily
        # points OLDER than a gap may be reconstructed with missing flow.
        "history_gaps": {
            chain_key: state.get(f"skipped_ranges_{chain_key}", [])
            for chain_key in CHAINS
        },
        "flow_history_integrity": {
            "status": "complete",
            "unresolvedGapCount": 0,
        },
        "dolo_price": dolo_price,
        "periods": output_periods,
        "balance_changes": balance_changes,
        "fresh_holders": fresh_holders,
        "fresh_holders_meta": {
            "source": "full_transfer_history_plus_multichain_explorer_first_tx_plus_debank_age_fallback",
            "definition": "address first normal on-chain transaction across tracked EVM activity sources within the selected period, plus required conservative DeBank wallet age fallback/cross-check for fresh candidates, with current liquid DOLO plus veDOLO locked exposure above 10K DOLO",
            "walletActivitySource": "Etherscan v2 txlist + Routescan Berachain txlist + DeBank rendered wallet age fallback",
            "walletAgeBoundaryPolicy": "DeBank rendered age labels are treated as ranges; a fallback wallet is included only when the upper bound fits inside the selected period.",
            "activityChains": [source["name"] for source in FRESH_WALLET_ACTIVITY_SOURCES],
            "activityChainKeys": [source["key"] for source in FRESH_WALLET_ACTIVITY_SOURCES],
            "fallbackSources": ["DeBank rendered wallet age"],
            "periods": list(FRESH_HOLDER_PERIODS),
            "minReceivedDolo": FRESH_HOLDER_MIN_RECEIVED,
            "minExposureDolo": FRESH_HOLDER_MIN_EXPOSURE,
            "minCurrentBalanceDolo": FRESH_HOLDER_MIN_EXPOSURE,
            "coverage": fresh_coverage_meta,
            "audit": fresh_wallet_audit,
        },
    }

    with open(os.path.join(DATA_DIR, "vesting_investors.json"), "w") as f:
        json.dump(vesting_investors, f, indent=2)
    print(
        f"  🧑‍💼 Saved {len(vesting_investors.get('early_investors', []))} early investors "
        f"and {len(vesting_investors.get('investors', []))} investors to vesting_investors.json"
    )

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    wallet_history_payload = {
        "timestamp": output["timestamp"],
        "holder_wallet_history": holder_wallet_history,
    }
    with open(WALLET_HISTORY_JSON, "w") as f:
        json.dump(wallet_history_payload, f, separators=(",", ":"))
    print(f"💾 Saved wallet-level holder history: {WALLET_HISTORY_JSON}")

    # Save incremental state for next run
    save_state(state)

    print(f"\n💾 Saved: {OUTPUT_JSON}")
    print(f"   State saved to {STATE_FILE} for incremental sync")

    # Summary
    for period in PERIODS:
        print(f"\n📊 {period.upper()}:")
        for chain_key, cfg in CHAINS.items():
            data = output_periods[period][chain_key]
            if data["accumulators"]:
                top = data["accumulators"][0]
                print(f"  🟢 {cfg['name']} top accumulator: {top['address'][:14]}… +{top['net_flow']:,.0f} DOLO")
            if data["sellers"]:
                top = data["sellers"][0]
                print(f"  🔴 {cfg['name']} top seller: {top['address'][:14]}… -{top['net_flow']:,.0f} DOLO")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
