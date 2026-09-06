#!/usr/bin/env python3
"""
DOLO Token Flows — Top Accumulators & Sellers (1d / 7d / 30d)
Fetches ERC-20 Transfer events via eth_getLogs for ETH and Berachain,
calculates net inflow/outflow per address, outputs top 5 each.
"""
import hashlib, json, time, os, sys, signal, re, shutil, subprocess, math
import requests
from cex_label_evidence import cex_label_evidence_status
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html import unescape
from urllib.parse import urlparse

from rpc_client import (
    RpcError,
    decode_uint256,
    get_endpoints as _rpc_endpoints,
    rpc_batch_requests,
    rpc_single_request,
)

import rpc_usage
from flow_tx_metadata import (
    attach_latest_lp_metadata,
    attach_latest_flow_metadata,
    collect_verified_lp_activities,
    fetch_transaction_receipts,
    fetch_token_block_evidence,
)

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "").strip()
BERASCAN_API_KEY = os.environ.get("BERASCAN_API_KEY", "").strip()

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ETHERSCAN_LOG_ENDPOINT = "etherscan://logs"
BLOCKSCOUT_LOG_ENDPOINT = "blockscout://logs"
BLOCKSCOUT_LOG_API = "https://eth.blockscout.com/api"
ZERO = "0x0000000000000000000000000000000000000000"
# Official Core router is deployed at the same deterministic address across
# Dolomite networks. DOLO itself is currently a Dolomite market on Ethereum
# and Berachain only; the Arbitrum token contract exists, but is not listed as
# a Dolomite market and must not activate protocol-flow attribution there.
DOLO_DEPOSIT_WITHDRAWAL_ROUTER = "0xf8b2c637a68cf6a17b1df9f8992eebeff63d2dff"
DOLOMITE_MARGIN_ADDRS = {
    "eth": "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
    "bera": "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
    "arb": "0x6bd780e7fdf01d77e4d475c821f1e7ae05409072",
}
DOLO_MARKET_IDS = {"eth": 16, "bera": 35}
# Canonical DOLO CCIP adapter on Berachain. A bridge-out transaction transfers
# DOLO to this adapter and burns the same amount before the destination mint.
BERACHAIN_DOLO_CCIP_ADAPTER = "0x9e7728077f753dfdf53c2236097e27c743890992"
BRIDGE_ADAPTER_ADDRS = {BERACHAIN_DOLO_CCIP_ADAPTER}
# Verified trading infrastructure. EnsoAggregatorTrader can finish a period
# with zero net DOLO while routing large custody legs, so net-flow-only
# contract discovery cannot reliably remove it from wallet leaderboards.
ENSO_AGGREGATOR_TRADER = "0x40e816361e9eceb4ded402def58cc77e9f097914"
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
ADDRESS_TYPE_ALIASES = {"trader": "bot"}

# Known contract addresses to exclude (DEX routers, LP pools, bots, etc.)
EXCLUDED_ADDRS = {
    ZERO,
    DOLO_CONTRACT,
    "0x0000000000000000000000000000000000000001",
    ENSO_AGGREGATOR_TRADER,
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
    BERACHAIN_DOLO_CCIP_ADAPTER,
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
        "rpcs": _rpc_endpoints("ethereum") + [BLOCKSCOUT_LOG_ENDPOINT] + (
            [ETHERSCAN_LOG_ENDPOINT] if ETHERSCAN_API_KEY else []
        ),
        "block_time": 12,   # ~12 seconds per block
        # Sparse history is scanned in wider ranges to reduce RPC load. Dense
        # distribution ranges automatically shrink to 1K blocks, while the
        # Blockscout verifier recursively splits any capped response.
        "chunk_size": 10_000,
        "deploy_block": 21_500_000,  # DOLO deployed ~Jan 2025
    },
    "bera": {
        "name": "Berachain",
        "rpcs": _rpc_endpoints("berachain"),
        "block_time": 2,    # ~2 seconds per block
        # The independent official + dRPC archive pair is reliable through
        # 6,250 blocks. Larger ranges are rejected by one or more providers;
        # regrowing above that cap caused every verified chunk to pay for a
        # guaranteed failed 12,500-block attempt before retrying.
        "chunk_size": 6_250,
        "deploy_block": 2_900_000,   # DOLO deployed on Berachain ~block 2,925,727 (Mar 2025)
    },
}

DOLOMITE_SUBGRAPH_BASE = (
    "https://subgraph.api.dolomite.io/api/public/"
    "1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs"
)
DOLOMITE_DOLO_POSITION_SUBGRAPHS = {
    "eth": {
        "name": "Ethereum",
        "url": f"{DOLOMITE_SUBGRAPH_BASE}/dolomite-ethereum/latest/gn",
        # Verified against the official archive subgraph on 2026-08-31:
        # the DOLO interest index is absent at 23,523,265 and first exists at
        # 23,523,266. Therefore positive DOLO Par cannot exist before it.
        "marketStartBlock": 23_523_266,
    },
    "bera": {
        "name": "Berachain",
        "url": f"{DOLOMITE_SUBGRAPH_BASE}/dolomite-berachain-mainnet/latest/gn",
        # Same boundary proof as Ethereum: absent at 4,791,097, first present
        # at 4,791,098 on the official Dolomite archive subgraph.
        "marketStartBlock": 4_791_098,
    },
}
DOLOMITE_POSITION_MAX_AGE_SECONDS = 6 * 3600
HOLDER_DOLOMITE_HISTORY_CACHE_VERSION = 1
HOLDER_DOLOMITE_HISTORY_REQUEST_DELAY_SECONDS = max(
    0.0,
    float(os.environ.get("DOLO_HOLDER_EXPOSURE_REQUEST_DELAY_SECONDS", "0.1")),
)

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
# Re-read a bounded, quorum-verified tail on every incremental run. Historical
# omissions are repaired by the explicit full verified backfill; expanding the
# destructive overlap cannot make a single unverified RPC response trustworthy.
RECENT_RESCAN_BLOCKS = {
    "eth": 50_000,
    "bera": 100_000,
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
FRESH_USER_WALLET_TYPES = {"eoa", "multisig"}
FRESH_AUTOMATION_MIN_INBOUND_TRANSFERS = 100
FRESH_AUTOMATION_MIN_OUTBOUND_TRANSFERS = 100
FRESH_AUTOMATION_MIN_SIDE_DOLO = 1_000_000
FRESH_AUTOMATION_MAX_NET_SHARE_BPS = 300
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
HOLDER_HISTORY_CUTOFF_CACHE_VERSION = 1

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "dolo_flows.json")
# Heavy per-day address-level snapshots live in a separate file so the main
# dolo_flows.json stays small for first render; the UI lazy-loads this one.
WALLET_HISTORY_JSON = os.path.join(DATA_DIR, "dolo_holder_wallet_history.json")
STATE_FILE = os.path.join(DATA_DIR, "dolo_flows_state.json")
LIQUIDITY_REGISTRY_JSON = os.path.join(DATA_DIR, "data", "dolo-liquidity-pools.json")
LIQUIDITY_OUTPUT_JSON = os.path.join(DATA_DIR, "data", "dolo-liquidity.json")
RPC_BATCH_SIZE = int(os.environ.get("DOLO_FLOWS_RPC_BATCH_SIZE", "50"))
RPC_RETRIES_PER_ENDPOINT = int(os.environ.get("DOLO_FLOWS_RPC_RETRIES_PER_ENDPOINT", "2"))
RPC_LOG_RETRIES_PER_ENDPOINT = int(
    os.environ.get("DOLO_FLOWS_LOG_RETRIES_PER_ENDPOINT", str(RPC_RETRIES_PER_ENDPOINT))
)
RPC_LOG_CHUNK_DELAY_SECONDS = max(
    0.05,
    float(os.environ.get("DOLO_FLOWS_LOG_CHUNK_DELAY_SECONDS", "0.25")),
)
RPC_LOG_CHUNK_REGROW_SUCCESS_THRESHOLD = max(
    1,
    int(os.environ.get("DOLO_FLOWS_LOG_CHUNK_REGROW_SUCCESS_THRESHOLD", "8")),
)
ETHERSCAN_LOG_PAGE_DELAY_SECONDS = max(
    0.05,
    float(os.environ.get("DOLO_FLOWS_ETHERSCAN_PAGE_DELAY_SECONDS", "0.4")),
)
BLOCKSCOUT_LOG_REQUEST_DELAY_SECONDS = max(
    0.05,
    float(os.environ.get("DOLO_FLOWS_BLOCKSCOUT_REQUEST_DELAY_SECONDS", "0.3")),
)
RPC_LOG_QUORUM = 2
FLOW_LOG_INTEGRITY_VERSION = 2

MAX_PERIOD_SECONDS = max(PERIODS.values())  # longest period for pruning
# Cache ALL transfers from genesis — state file lives only in Actions cache (10 GB limit),
# never committed to git. After first full scan, every run just fetches new blocks.
MAX_CACHE_SECONDS = MAX_PERIOD_SECONDS


def eip7702_delegation_address(code):
    """Return the delegate for an exact EIP-7702 designator, else None."""
    match = re.fullmatch(r"0xef0100([0-9a-fA-F]{40})", str(code or ""))
    return "0x" + match.group(1).lower() if match else None

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


def load_liquidity_registry():
    """Load tracked LP contracts used only for optional flow attribution."""
    try:
        with open(LIQUIDITY_REGISTRY_JSON) as f:
            registry = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  ⚠️ LP flow attribution unavailable: {exc}")
        return {}
    if not isinstance(registry, dict) or not isinstance(registry.get("chains"), dict) or not isinstance(registry.get("pools"), list):
        print("  ⚠️ LP flow attribution unavailable: invalid liquidity registry")
        return {}
    return registry


def lp_liquidity_provider_wallets(payload):
    """Return verified beneficial LP owners from the shared liquidity artifact."""
    wallets = set()
    if not isinstance(payload, dict):
        return wallets
    for section in ("activePositions", "history"):
        for row in payload.get(section) or []:
            if not isinstance(row, dict):
                continue
            address = str(row.get("beneficialOwner") or "").strip().lower()
            if re.fullmatch(r"0x[a-f0-9]{40}", address):
                wallets.add(address)
    return wallets


def load_liquidity_provider_wallets():
    """Load LP owners without treating protocol custodians as user wallets."""
    try:
        with open(LIQUIDITY_OUTPUT_JSON) as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  ⚠️ LP provider candidate index unavailable: {exc}")
        return set()
    return lp_liquidity_provider_wallets(payload)


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


def find_first_block_at_or_after_timestamp(
    start_block,
    end_block,
    target_timestamp,
    timestamp_loader,
):
    """Return the earliest block whose exact timestamp reaches the target."""
    low = int(start_block)
    high = int(end_block)
    target = int(target_timestamp)
    if low < 0 or high < low:
        raise ValueError("invalid block search range")
    start_timestamp = int(timestamp_loader(low))
    end_timestamp = int(timestamp_loader(high))
    if start_timestamp > end_timestamp:
        raise ValueError("block timestamps are not monotonic")
    if target <= start_timestamp:
        return low
    if target > end_timestamp:
        raise ValueError("target timestamp is newer than the confirmed head")

    while low < high:
        midpoint = (low + high) // 2
        timestamp = int(timestamp_loader(midpoint))
        if timestamp < start_timestamp or timestamp > end_timestamp:
            raise ValueError("block timestamp falls outside the exact search range")
        if timestamp < target:
            low = midpoint + 1
        else:
            high = midpoint
    return low


def find_first_blocks_at_or_after_timestamps(
    start_block,
    end_block,
    target_timestamps,
    timestamp_batch_loader,
    nominal_block_time,
):
    """Resolve many timestamp cutoffs exactly with batched block lookups.

    A linear block estimate gives every target a narrow initial bracket.  The
    bracket is verified against real block timestamps and expands if needed,
    so the final result is still the first block at-or-after each target even
    on chains with missed slots or irregular production.
    """
    start = int(start_block)
    end = int(end_block)
    targets = [int(value) for value in target_timestamps]
    if start < 0 or end < start:
        raise ValueError("invalid block search range")
    if not targets:
        return []
    nominal = max(1, int(nominal_block_time))
    timestamp_cache = {}

    def load_many(blocks):
        missing = sorted({int(block) for block in blocks if int(block) not in timestamp_cache})
        if missing:
            loaded = timestamp_batch_loader(missing)
            if not isinstance(loaded, dict):
                raise ValueError("timestamp batch loader must return a block mapping")
            for block in missing:
                if block not in loaded:
                    raise ValueError(f"missing timestamp for block {block}")
                timestamp_cache[block] = int(loaded[block])
        return {int(block): timestamp_cache[int(block)] for block in blocks}

    edge_timestamps = load_many([start, end])
    start_ts = edge_timestamps[start]
    end_ts = edge_timestamps[end]
    if start_ts > end_ts:
        raise ValueError("block timestamps are not monotonic")
    if any(target > end_ts for target in targets):
        raise ValueError("target timestamp is newer than the confirmed head")

    intervals = []
    span_blocks = max(1, end - start)
    span_seconds = max(1, end_ts - start_ts)
    candidate_blocks = []
    for target in targets:
        if target <= start_ts:
            intervals.append([start, start, target])
            candidate_blocks.append(start)
            continue
        ratio = (target - start_ts) / span_seconds
        candidate = max(start, min(end, start + int(ratio * span_blocks)))
        intervals.append([candidate, candidate, target])
        candidate_blocks.append(candidate)

    candidate_timestamps = load_many(candidate_blocks)
    for interval, candidate in zip(intervals, candidate_blocks):
        target = interval[2]
        if target <= start_ts:
            continue
        candidate_ts = candidate_timestamps[candidate]
        distance = abs(candidate_ts - target)
        step = max(2, (distance + nominal - 1) // nominal + 2)
        if candidate_ts < target:
            interval[0] = candidate
            interval[1] = min(end, candidate + step)
        else:
            interval[0] = max(start, candidate - step)
            interval[1] = candidate

    # Verify and, if a chain violated the nominal-time bracket assumption,
    # expand exponentially until every target is actually enclosed.
    while True:
        bounds = load_many([value for low, high, _ in intervals for value in (low, high)])
        changed = False
        for interval in intervals:
            low, high, target = interval
            if low == high:
                continue
            if bounds[low] >= target and low > start:
                width = max(2, high - low + 1)
                interval[0] = max(start, low - width * 2)
                changed = True
            elif bounds[high] < target and high < end:
                width = max(2, high - low + 1)
                interval[1] = min(end, high + width * 2)
                changed = True
            elif bounds[low] >= target and low == start:
                interval[1] = low
            elif bounds[high] < target and high == end:
                raise ValueError("target timestamp is newer than the confirmed head")
        if not changed:
            break

    while any(low < high for low, high, _ in intervals):
        mids = [(low + high) // 2 for low, high, _ in intervals if low < high]
        mid_timestamps = load_many(mids)
        for interval in intervals:
            low, high, target = interval
            if low >= high:
                continue
            midpoint = (low + high) // 2
            if mid_timestamps[midpoint] < target:
                interval[0] = midpoint + 1
            else:
                interval[1] = midpoint

    return [low for low, _high, _target in intervals]


def load_block_timestamp(chain_key, block_number, cache=None):
    """Resolve one exact block timestamp through the configured RPC failover."""
    cache = cache if cache is not None else {}
    key = (chain_key, int(block_number))
    if key in cache:
        return cache[key]
    cfg = CHAINS[chain_key]
    eligible_rpcs = [
        endpoint
        for endpoint in cfg["rpcs"]
        if str(endpoint).startswith(("https://", "http://"))
    ]
    if chain_key == "eth":
        # OnFinality is useful for log quorum but currently rate-limits these
        # repeated historical block lookups. dRPC serves the same exact block
        # timestamps without the failed request in front of every binary step.
        eligible_rpcs.sort(
            key=lambda endpoint: (
                0 if rpc_provider_family(endpoint) == "drpc.org" else 1,
                endpoint,
            )
        )
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(int(block_number)), False],
        "id": f"period-boundary:{chain_key}:{int(block_number)}",
    }
    response = rpc_single_request(
        eligible_rpcs,
        payload,
        timeout=12,
        retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
        quiet=True,
        describe=f"{cfg['name']} period boundary block",
    )
    result = response.get("result") if isinstance(response, dict) else None
    raw_timestamp = result.get("timestamp") if isinstance(result, dict) else None
    try:
        timestamp = int(str(raw_timestamp), 16)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"{cfg['name']}: block {int(block_number):,} has no exact timestamp"
        ) from exc
    if timestamp <= 0:
        raise RuntimeError(
            f"{cfg['name']}: block {int(block_number):,} has an invalid timestamp"
        )
    cache[key] = timestamp
    return timestamp


def load_block_timestamps_batch(chain_key, block_numbers, cache=None):
    """Resolve exact timestamps for many blocks through RPC batch failover."""
    cache = cache if cache is not None else {}
    blocks = sorted({int(block) for block in block_numbers})
    resolved = {}
    missing = []
    for block in blocks:
        key = (chain_key, block)
        if key in cache:
            resolved[block] = int(cache[key])
        else:
            missing.append(block)
    if not missing:
        return resolved

    cfg = CHAINS[chain_key]
    eligible_rpcs = [
        endpoint
        for endpoint in cfg["rpcs"]
        if str(endpoint).startswith(("https://", "http://"))
    ]
    if chain_key == "eth":
        eligible_rpcs.sort(
            key=lambda endpoint: (
                0 if rpc_provider_family(endpoint) == "drpc.org" else 1,
                endpoint,
            )
        )
    payloads = [
        {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(block), False],
            "id": f"holder-boundary:{chain_key}:{block}",
        }
        for block in missing
    ]
    try:
        responses, missing_ids = rpc_batch_requests(
            eligible_rpcs,
            payloads,
            timeout=12,
            retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
            batch_size=RPC_BATCH_SIZE,
            quiet=True,
            describe=f"{cfg['name']} holder boundary blocks",
        )
    except RpcError:
        responses, missing_ids = {}, [payload["id"] for payload in payloads]

    for block, payload in zip(missing, payloads):
        response = responses.get(payload["id"])
        result = response.get("result") if isinstance(response, dict) else None
        raw_timestamp = result.get("timestamp") if isinstance(result, dict) else None
        if payload["id"] in missing_ids:
            timestamp = load_block_timestamp(chain_key, block, cache)
        else:
            try:
                timestamp = int(str(raw_timestamp), 16)
            except (TypeError, ValueError):
                timestamp = 0
            if timestamp <= 0:
                timestamp = load_block_timestamp(chain_key, block, cache)
        cache[(chain_key, block)] = timestamp
        resolved[block] = timestamp
    return resolved


def calculate_exact_period_cutoffs(current_blocks):
    """Build timestamp-exact cutoffs and their auditable chain metadata."""
    cutoff_blocks = {}
    boundaries = {}
    timestamp_cache = {}
    for chain_key, cfg in CHAINS.items():
        start_block = int(cfg.get("deploy_block", 0))
        end_block = int(current_blocks[chain_key])
        timestamp_loader = lambda block, key=chain_key: load_block_timestamp(
            key, block, timestamp_cache
        )
        start_timestamp = timestamp_loader(start_block)
        end_timestamp = timestamp_loader(end_block)
        cutoff_blocks[chain_key] = {}
        boundaries[chain_key] = {}
        for period, seconds in PERIODS.items():
            target_timestamp = end_timestamp - int(seconds)
            cutoff = find_first_block_at_or_after_timestamp(
                start_block,
                end_block,
                target_timestamp,
                timestamp_loader,
            )
            cutoff_timestamp = timestamp_loader(cutoff)
            cutoff_blocks[chain_key][period] = cutoff
            boundaries[chain_key][period] = {
                "targetTimestamp": target_timestamp,
                "startBlock": cutoff,
                "startTimestamp": cutoff_timestamp,
                "endBlock": end_block,
                "endTimestamp": end_timestamp,
            }
    return cutoff_blocks, boundaries


def flow_snapshot_timestamp(period_boundaries):
    """Return the oldest verified chain head represented by the snapshot."""
    end_timestamps = []
    for chain_key in CHAINS:
        chain_boundaries = period_boundaries.get(chain_key, {})
        all_boundary = chain_boundaries.get("all", {})
        end_timestamp = all_boundary.get("endTimestamp")
        if not isinstance(end_timestamp, int) or isinstance(end_timestamp, bool):
            raise RuntimeError(
                f"{CHAINS[chain_key]['name']}: missing exact all-time end timestamp"
            )
        end_timestamps.append(end_timestamp)
    if not end_timestamps:
        raise RuntimeError("No tracked chain head timestamps are available")
    return (
        datetime.fromtimestamp(min(end_timestamps), timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


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


TRANSFER_METADATA_VERSION = 1


def transfer_has_transaction_metadata(transfer):
    if not isinstance(transfer, (list, tuple)) or len(transfer) < 6:
        return False
    tx_hash = str(transfer[4] or "").strip().lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", tx_hash):
        return False
    try:
        return int(transfer[5]) >= 0
    except (TypeError, ValueError):
        return False


def ensure_transfer_metadata_coverage(
    chain_key,
    transfers,
    state,
    start_block,
    end_block,
    *,
    verified_refresh_start=None,
    fetcher=None,
):
    """Guarantee tx hash/log index metadata for a bounded transfer range.

    Old state files contain four-field rows. LP attribution needs the original
    transaction hash, so refresh the recent range once and remember the
    verified coverage. Subsequent incremental scans extend that coverage
    without re-fetching already enriched blocks.
    """
    start_block = int(start_block)
    end_block = int(end_block)
    coverage_by_chain = state.setdefault("transfer_metadata_coverage", {})
    coverage = coverage_by_chain.get(chain_key) or {}
    coverage_start = int(coverage.get("startBlock") or 0)
    coverage_end = int(coverage.get("endBlock") or -1)
    coverage_valid = (
        int(coverage.get("version") or 0) == TRANSFER_METADATA_VERSION
        and coverage_start <= start_block
        and coverage_end >= end_block
    )
    if coverage_valid:
        return list(transfers)

    rows_in_range = [
        transfer for transfer in transfers
        if start_block <= int(transfer[3]) <= end_block
    ]
    refreshed_from = (
        int(verified_refresh_start)
        if verified_refresh_start is not None
        else None
    )
    range_is_already_enriched = (
        (not rows_in_range or all(transfer_has_transaction_metadata(t) for t in rows_in_range))
        and refreshed_from is not None
        and refreshed_from <= start_block
    )
    coverage_can_extend = (
        int(coverage.get("version") or 0) == TRANSFER_METADATA_VERSION
        and coverage_start <= start_block
        and refreshed_from is not None
        and refreshed_from <= coverage_end + 1
        and all(transfer_has_transaction_metadata(t) for t in rows_in_range)
    )
    if range_is_already_enriched or coverage_can_extend:
        coverage_by_chain[chain_key] = {
            "version": TRANSFER_METADATA_VERSION,
            "startBlock": start_block,
            "endBlock": end_block,
        }
        return list(transfers)

    fetch_logs = fetcher or fetch_transfer_logs
    replacement, failed_chunks, _ = fetch_logs(chain_key, start_block, end_block)
    if int(failed_chunks or 0) != 0:
        raise TransferLogQuorumError(
            f"{CHAINS[chain_key]['name']}: transaction metadata migration "
            f"did not reach RPC quorum"
        )
    if any(not transfer_has_transaction_metadata(t) for t in replacement):
        raise RuntimeError(
            f"{CHAINS[chain_key]['name']}: transaction metadata migration "
            "returned legacy transfer rows"
        )
    migrated = merge_verified_transfer_scan(
        transfers,
        replacement,
        start_block,
        end_block,
        failed_chunks=failed_chunks,
    )
    coverage_by_chain[chain_key] = {
        "version": TRANSFER_METADATA_VERSION,
        "startBlock": start_block,
        "endBlock": end_block,
    }
    return migrated


def merge_verified_transfer_scan(
    transfers,
    replacement,
    start_block,
    end_block,
    *,
    failed_chunks,
):
    if int(failed_chunks or 0) != 0:
        raise TransferLogQuorumError(
            f"refusing to replace blocks {int(start_block)}-{int(end_block)} "
            f"after {int(failed_chunks)} unverified chunk(s)"
        )
    return replace_transfer_range(
        transfers,
        replacement,
        start_block,
        end_block,
    )


class TransferLogQuorumError(RuntimeError):
    """Raised when independent RPC providers cannot agree on a log range."""


class TransferLogRangeError(RuntimeError):
    """Raised when an RPC requires a smaller eth_getLogs block range."""


def mark_verified_chain_coverage(
    state,
    chain_key,
    start_block,
    end_block,
    *,
    full_baseline,
):
    integrity = state.setdefault("flow_log_integrity", {})
    chains = integrity.setdefault("chains", {})
    existing = chains.get(chain_key)
    deploy_block = int(CHAINS[chain_key]["deploy_block"])
    start_block = int(start_block)
    end_block = int(end_block)

    if full_baseline:
        if start_block > deploy_block:
            raise TransferLogQuorumError(
                f"{CHAINS[chain_key]['name']}: verified baseline starts after deploy block"
            )
        coverage_start = start_block
    else:
        if (
            int(integrity.get("version", 0)) != FLOW_LOG_INTEGRITY_VERSION
            or not isinstance(existing, dict)
            or existing.get("verification") != "independent-rpc-exact-quorum"
            or int(existing.get("coverageStartBlock", deploy_block + 1)) > deploy_block
        ):
            raise TransferLogQuorumError(
                f"{CHAINS[chain_key]['name']}: full verified baseline is required"
            )
        coverage_start = int(existing["coverageStartBlock"])
        if end_block < int(existing.get("verifiedThroughBlock", 0)):
            raise TransferLogQuorumError(
                f"{CHAINS[chain_key]['name']}: verified coverage cannot rewind"
            )

    integrity.update({
        "version": FLOW_LOG_INTEGRITY_VERSION,
        "verification": "independent-rpc-exact-quorum",
    })
    chains[chain_key] = {
        "deployBlock": deploy_block,
        "coverageStartBlock": coverage_start,
        "verifiedThroughBlock": end_block,
        "lastPublishedBlock": end_block,
        "lastVerifiedRange": [start_block, end_block],
        "verification": "independent-rpc-exact-quorum",
        "verifiedAt": datetime.utcnow().isoformat() + "Z",
        "lastVerificationProof": dict(
            (state.get("verified_scan_proofs", {}) or {}).get(chain_key, {})
        ),
    }
    integrity["status"] = (
        "complete"
        if all(
            isinstance(chains.get(key), dict)
            and int(chains[key].get("coverageStartBlock", CHAINS[key]["deploy_block"] + 1))
            <= int(CHAINS[key]["deploy_block"])
            for key in CHAINS
        )
        else "building"
    )
    integrity["unresolvedGapCount"] = 0


def has_complete_verified_baseline(state):
    integrity = (state or {}).get("flow_log_integrity")
    if not isinstance(integrity, dict):
        return False
    if (
        integrity.get("version") != FLOW_LOG_INTEGRITY_VERSION
        or integrity.get("status") != "complete"
        or integrity.get("verification") != "independent-rpc-exact-quorum"
        or integrity.get("unresolvedGapCount") != 0
    ):
        return False
    chains = integrity.get("chains")
    if not isinstance(chains, dict):
        return False
    for chain_key, cfg in CHAINS.items():
        row = chains.get(chain_key)
        deploy_block = int(cfg["deploy_block"])
        if (
            not isinstance(row, dict)
            or row.get("verification") != "independent-rpc-exact-quorum"
            or int(row.get("coverageStartBlock", deploy_block + 1)) > deploy_block
            or int(row.get("verifiedThroughBlock", 0)) < deploy_block
        ):
            return False
    return True


def completed_verified_backfill_chains(state):
    """Return fully verified chain baselines safe to resume incrementally.

    A full multi-chain backfill can time out after one chain has already been
    completed. Its active cache is reusable only when the persisted coverage,
    cursor, full-history marker, and independent quorum proof all agree.
    """
    integrity = (state or {}).get("flow_log_integrity")
    if (
        not isinstance(integrity, dict)
        or integrity.get("version") != FLOW_LOG_INTEGRITY_VERSION
        or integrity.get("verification") != "independent-rpc-exact-quorum"
        or integrity.get("unresolvedGapCount") != 0
    ):
        return set()

    chains = integrity.get("chains")
    if not isinstance(chains, dict):
        return set()

    resumable = set()
    for chain_key, cfg in CHAINS.items():
        row = chains.get(chain_key)
        deploy_block = int(cfg["deploy_block"])
        transfers = (state or {}).get(f"{chain_key}_transfers")
        try:
            last_block = int((state or {}).get(f"{chain_key}_last_block") or 0)
            history_start = int(
                (state or {}).get(f"{chain_key}_history_start_block")
                or deploy_block + 1
            )
            proof_families = int(
                ((row or {}).get("lastVerificationProof") or {}).get(
                    "minimumMatchingProviderFamilies", 0
                )
            )
        except (TypeError, ValueError):
            continue
        if (
            isinstance(row, dict)
            and isinstance(transfers, list)
            and row.get("verification") == "independent-rpc-exact-quorum"
            and int(row.get("coverageStartBlock", deploy_block + 1)) <= deploy_block
            and int(row.get("verifiedThroughBlock", 0)) == last_block
            and int(row.get("lastPublishedBlock", 0)) == last_block
            and history_start <= deploy_block
            and last_block >= deploy_block
            and proof_families >= RPC_LOG_QUORUM
        ):
            resumable.add(chain_key)
    return resumable


def rpc_provider_family(url):
    """Return a vendor identity so two keys from one provider count once."""
    if str(url or "").lower() == ETHERSCAN_LOG_ENDPOINT:
        return "etherscan.io"
    if str(url or "").lower() == BLOCKSCOUT_LOG_ENDPOINT:
        return "blockscout.com"
    hostname = (urlparse(str(url or "")).hostname or "").lower().rstrip(".")
    vendor_suffixes = (
        "alchemy.com",
        "drpc.org",
        "publicnode.com",
        "quicknode.pro",
        "quiknode.pro",
        "ankr.com",
        "infura.io",
        "berachain.com",
    )
    for suffix in vendor_suffixes:
        if hostname == suffix or hostname.endswith("." + suffix):
            return suffix
    return hostname or "unknown"


def _canonical_transfer_log(log):
    if not isinstance(log, dict):
        raise ValueError("transfer log must be an object")
    topics = log.get("topics")
    if not isinstance(topics, list) or len(topics) < 3:
        raise ValueError("transfer log is missing indexed topics")
    return (
        int(log.get("blockNumber", "0x0"), 16),
        str(log.get("transactionHash") or "").lower(),
        int(log.get("logIndex", "0x0"), 16),
        str(log.get("address") or "").lower(),
        tuple(str(topic).lower() for topic in topics),
        int(log.get("data", "0x0"), 16),
    )


def transfer_log_digest(logs):
    """Digest an eth_getLogs result independently of provider ordering/hex padding."""
    canonical = sorted(_canonical_transfer_log(log) for log in (logs or []))
    payload = json.dumps(canonical, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def select_transfer_log_quorum(endpoint_results, required=RPC_LOG_QUORUM):
    """Choose an exact result agreed by independent provider families."""
    by_family = {}
    inconsistent_families = set()
    for endpoint, logs in endpoint_results:
        family = rpc_provider_family(endpoint)
        digest = transfer_log_digest(logs)
        prior = by_family.get(family)
        if prior is not None and prior[0] != digest:
            inconsistent_families.add(family)
            continue
        by_family.setdefault(family, (digest, list(logs or [])))

    for family in inconsistent_families:
        by_family.pop(family, None)

    by_digest = {}
    for family, (digest, logs) in by_family.items():
        entry = by_digest.setdefault(digest, {"families": [], "logs": logs})
        entry["families"].append(family)

    ranked = sorted(
        by_digest.items(),
        key=lambda item: (-len(item[1]["families"]), item[0]),
    )
    if not ranked or len(ranked[0][1]["families"]) < int(required):
        counts = {digest: len(entry["families"]) for digest, entry in ranked}
        raise TransferLogQuorumError(
            f"independent RPC quorum unavailable; family vote counts={counts}"
        )
    if len(ranked) > 1 and len(ranked[0][1]["families"]) == len(ranked[1][1]["families"]):
        raise TransferLogQuorumError("independent RPC quorum is ambiguous")

    selected_digest, selected = ranked[0]
    agreeing = sorted(selected["families"])
    disagreeing = sorted(
        family
        for family, (digest, _logs) in by_family.items()
        if digest != selected_digest
    )
    proof = {
        "digest": selected_digest,
        "logCount": len(selected["logs"]),
        "matchingProviderFamilies": len(agreeing),
        "providerFamilies": agreeing,
        "disagreeingProviderFamilies": disagreeing,
    }
    return selected["logs"], proof


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


def _is_capacity_exhausted_error(error_obj):
    """True when the endpoint's quota is exhausted (e.g. Alchemy 429
    "Monthly capacity limit exceeded"). Waiting will not help — rotate to the
    next endpoint immediately, and never treat it as a block-range error."""
    if not isinstance(error_obj, dict):
        return False
    code = error_obj.get("code")
    msg = str(error_obj.get("message", "")).lower()
    return (
        code in (429, -32005, -32029, -32097)
        or "capacity" in msg
        or "quota" in msg
        or "monthly" in msg
        or "compute units" in msg
    )


def _request_etherscan_transfer_logs(cfg, start_block, end_block, api_key=None):
    """Fetch one exact log range through Etherscan's paginated Logs API."""
    api_key = str(api_key if api_key is not None else ETHERSCAN_API_KEY).strip()
    if not api_key:
        return None

    offset = 1_000
    page = 1
    all_logs = []
    previous_page_digest = None
    while page <= 100_000:
        page_logs = None
        for attempt in range(max(2, RPC_LOG_RETRIES_PER_ENDPOINT)):
            try:
                response = requests.get(
                    ETHERSCAN_V2_API,
                    params={
                        "chainid": "1",
                        "module": "logs",
                        "action": "getLogs",
                        "fromBlock": int(start_block),
                        "toBlock": int(end_block),
                        "address": DOLO_CONTRACT,
                        "topic0": TRANSFER_TOPIC,
                        "page": page,
                        "offset": offset,
                        "apikey": api_key,
                    },
                    timeout=60,
                )
                if response.status_code == 429:
                    time.sleep(_rate_limit_retry_seconds(response, attempt))
                    continue
                response.raise_for_status()
                payload = response.json()
                result = payload.get("result") if isinstance(payload, dict) else None
                message = str(payload.get("message", "")) if isinstance(payload, dict) else ""
                if isinstance(result, list):
                    page_logs = result
                    break
                detail = f"{message} {result}".lower()
                if "no records found" in detail:
                    page_logs = []
                    break
                if _is_rate_limit_error(detail) or "rate limit" in detail:
                    time.sleep(min(8.0, 1.0 * (2 ** min(attempt, 3))))
                    continue
            except requests.exceptions.RequestException:
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))
            except ValueError:
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))

        if page_logs is None:
            return None
        if not page_logs:
            return all_logs

        # Validate every page before it can participate in an independent vote.
        page_digest = transfer_log_digest(page_logs)
        if page_digest == previous_page_digest:
            return None
        previous_page_digest = page_digest
        all_logs.extend(page_logs)
        if len(page_logs) < offset:
            return all_logs
        page += 1
        time.sleep(ETHERSCAN_LOG_PAGE_DELAY_SECONDS)

    return None


def _normalize_blockscout_transfer_log(log):
    """Remove Blockscout's non-EVM trailing topic placeholders."""
    normalized = dict(log or {})
    topics = list(normalized.get("topics") or [])
    while topics and (
        topics[-1] is None
        or str(topics[-1]).strip().lower() in {"", "none", "null"}
    ):
        topics.pop()
    normalized["topics"] = topics
    return normalized


def _request_blockscout_transfer_logs(cfg, start_block, end_block):
    """Fetch a complete range from Blockscout without trusting its 1K cap.

    The per-instance Logs API does not paginate. A full 1,000-row response is
    therefore ambiguous and is recursively split by block until every accepted
    leaf is strictly below the documented cap. A capped single block is
    rejected because completeness cannot be proven.
    """
    result_cap = 1_000
    pending_ranges = [(int(start_block), int(end_block))]
    all_logs = []
    requests_used = 0

    while pending_ranges:
        range_start, range_end = pending_ranges.pop()
        range_logs = None
        for attempt in range(max(2, RPC_LOG_RETRIES_PER_ENDPOINT)):
            try:
                response = requests.get(
                    BLOCKSCOUT_LOG_API,
                    params={
                        "module": "logs",
                        "action": "getLogs",
                        "fromBlock": range_start,
                        "toBlock": range_end,
                        "address": DOLO_CONTRACT,
                        "topic0": TRANSFER_TOPIC,
                    },
                    timeout=60,
                )
                if response.status_code == 429:
                    time.sleep(_rate_limit_retry_seconds(response, attempt))
                    continue
                response.raise_for_status()
                payload = response.json()
                result = payload.get("result") if isinstance(payload, dict) else None
                message = str(payload.get("message", "")) if isinstance(payload, dict) else ""
                if isinstance(result, list):
                    range_logs = [
                        _normalize_blockscout_transfer_log(log)
                        for log in result
                    ]
                    break
                detail = f"{message} {result}".lower()
                if "no records found" in detail:
                    range_logs = []
                    break
                if _is_rate_limit_error(detail):
                    time.sleep(min(8.0, 1.0 * (2 ** min(attempt, 3))))
                    continue
            except requests.exceptions.RequestException:
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))
            except ValueError:
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))

        if range_logs is None:
            return None

        # Validate the canonical shape before this source receives a vote.
        transfer_log_digest(range_logs)
        if len(range_logs) < result_cap:
            all_logs.extend(range_logs)
        else:
            if range_start >= range_end:
                return None
            midpoint = (range_start + range_end) // 2
            # Stack order keeps the final rows grouped from older to newer
            # blocks, though quorum hashing itself is order-independent.
            pending_ranges.append((midpoint + 1, range_end))
            pending_ranges.append((range_start, midpoint))

        requests_used += 1
        if requests_used > 100_000:
            return None
        time.sleep(BLOCKSCOUT_LOG_REQUEST_DELAY_SECONDS)

    return all_logs


def _rpc_families(rpcs, chain_key=None):
    grouped = {}
    for endpoint in rpcs or []:
        grouped.setdefault(rpc_provider_family(endpoint), []).append(endpoint)
    families = list(grouped.items())
    if chain_key == "eth":
        # Public archive providers are independent and have materially more
        # headroom for historical eth_getLogs than the shared Alchemy quota.
        # Keep Alchemy as a fallback family, but do not burn through every key
        # before trying the archive endpoints on every chunk.
        priority = {
            "eth.api.onfinality.io": 0,
            "blockscout.com": 1,
            "etherscan.io": 2,
            "mainnet.gateway.tenderly.co": 3,
            "rpc.mevblocker.io": 4,
            "drpc.org": 5,
            "alchemy.com": 6,
        }
        families.sort(key=lambda item: priority.get(item[0], 100))
    elif chain_key == "bera":
        # Start with the two independent public archive endpoints that return
        # exact matching DOLO logs for historical and recent ranges. This
        # avoids spending retry/backoff time on exhausted keyed endpoints
        # before quorum can be reached. Keyed vendors remain fallbacks.
        priority = {
            "berachain.com": 0,
            "drpc.org": 1,
            "quicknode.pro": 2,
            "quiknode.pro": 2,
            "alchemy.com": 3,
            "publicnode.com": 4,
        }
        families.sort(key=lambda item: priority.get(item[0], 100))
        for family, endpoints in families:
            if family == "drpc.org":
                endpoints.sort(
                    key=lambda endpoint: (
                        0 if endpoint.rstrip("/") == "https://berachain.drpc.org" else 1
                    )
                )
    return families


def _request_transfer_logs(endpoint, cfg, start_block, end_block):
    if endpoint == BLOCKSCOUT_LOG_ENDPOINT:
        return _request_blockscout_transfer_logs(cfg, start_block, end_block)
    if endpoint == ETHERSCAN_LOG_ENDPOINT:
        return _request_etherscan_transfer_logs(cfg, start_block, end_block)
    for attempt in range(max(2, RPC_LOG_RETRIES_PER_ENDPOINT)):
        try:
            resp = requests.post(endpoint, json={
                "jsonrpc": "2.0", "method": "eth_getLogs",
                "params": [{
                    "address": DOLO_CONTRACT,
                    "topics": [TRANSFER_TOPIC],
                    "fromBlock": hex(start_block),
                    "toBlock": hex(end_block),
                }], "id": 1
            }, timeout=60, headers={"Content-Type": "application/json"})

            if resp.status_code == 429:
                delay = _rate_limit_retry_seconds(resp, attempt)
                print(
                    f"    ⚠️ {cfg['name']}: RPC rate-limited block "
                    f"{start_block:,}-{end_block:,}; waiting {delay:.1f}s before retry"
                )
                time.sleep(delay)
                continue

            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise ValueError("RPC response was not a JSON object")
            error = payload.get("error")
            if error:
                message = str(error.get("message", ""))
                if _is_capacity_exhausted_error(error):
                    return None
                if _is_rate_limit_error(message):
                    delay = _rate_limit_retry_seconds(resp, attempt)
                    print(
                        f"    ⚠️ {cfg['name']}: RPC rate-limited block "
                        f"{start_block:,}-{end_block:,}; waiting {delay:.1f}s before retry"
                    )
                    time.sleep(delay)
                    continue
                if "range" in message.lower() or "limit" in message.lower():
                    raise TransferLogRangeError(message)
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))
                continue

            logs = payload.get("result")
            if not isinstance(logs, list):
                raise ValueError("RPC eth_getLogs result was not a list")
            # Validate the canonical shape before this provider receives a vote.
            transfer_log_digest(logs)
            return logs
        except TransferLogRangeError:
            raise
        except requests.exceptions.Timeout:
            if attempt + 1 < max(2, RPC_LOG_RETRIES_PER_ENDPOINT):
                time.sleep(1)
        except requests.exceptions.RequestException as exc:
            print(
                f"    ⚠️ {cfg['name']}: RPC request failed for block "
                f"{start_block:,}-{end_block:,} ({type(exc).__name__}); retrying"
            )
            time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))
        except ValueError as exc:
            print(
                f"    ⚠️ {cfg['name']}: RPC response was invalid for block "
                f"{start_block:,}-{end_block:,} ({type(exc).__name__}); retrying"
            )
            time.sleep(min(8.0, 0.5 * (2 ** min(attempt, 4))))
    return None


def load_verified_scan_staging(state, chain_key, start_block, end_block):
    staging = (state or {}).get("verified_scan_staging", {}).get(chain_key)
    if not isinstance(staging, dict):
        return [], int(start_block)
    staged_end_block = int(staging.get("endBlock", -1))
    if (
        staging.get("verification") != "independent-rpc-exact-quorum"
        or int(staging.get("startBlock", -1)) != int(start_block)
        or staged_end_block < int(start_block)
        or staged_end_block > int(end_block)
    ):
        return [], int(start_block)
    try:
        next_block = int(staging.get("nextBlock"))
        transfers = [tuple(transfer) for transfer in staging.get("transfers", [])]
    except (TypeError, ValueError):
        return [], int(start_block)
    if next_block < int(start_block) or next_block > int(end_block) + 1:
        return [], int(start_block)
    return transfers, next_block


def fetch_transfer_logs(chain_key, start_block, end_block, state=None, cached_transfers_so_far=None):
    """Fetch Transfer logs only after independent RPC providers agree exactly.

    Progressive checkpoints are written to a staging namespace. The active
    transfer cache and last-block cursor are promoted only by the caller after
    the entire requested range succeeds.
    """
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    chunk_size = cfg["chunk_size"]

    if not block_range_has_work(start_block, end_block):
        return [], 0, 0  # transfers, failed_chunks, total_chunks

    total_blocks = end_block - start_block
    total_expected_chunks = max(1, (total_blocks + chunk_size - 1) // chunk_size)
    print(f"  {cfg['name']}: scanning blocks {start_block:,} → {end_block:,} ({total_blocks:,} blocks, ~{total_expected_chunks} chunks)")

    families = _rpc_families(rpcs, chain_key=chain_key)
    if len(families) < RPC_LOG_QUORUM:
        print(
            f"  ⚠️ {cfg['name']}: requires {RPC_LOG_QUORUM} independent RPC "
            f"families, found {len(families)}."
        )
        return [], total_expected_chunks, total_expected_chunks

    all_transfers, current = load_verified_scan_staging(
        state, chain_key, start_block, end_block
    )
    staged = (state or {}).get("verified_scan_staging", {}).get(chain_key, {})
    if current > start_block:
        print(
            f"  {cfg['name']}: resuming verified staging at block "
            f"{current:,} ({len(all_transfers):,} transfers retained)"
        )
    chunks_done = int(staged.get("verifiedChunkCount", 0) or 0) if current > start_block else 0
    chunks_failed = 0
    reduced_chunk_successes = 0
    skipped_ranges = []  # [start, end] of block ranges lost to persistent RPC failure
    agreeing_provider_families = set(staged.get("providerFamilies", [])) if current > start_block else set()
    disagreeing_provider_families = set(staged.get("disagreeingProviderFamilies", [])) if current > start_block else set()
    minimum_matching_families = staged.get("minimumMatchingProviderFamilies") if current > start_block else None

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        selected_logs = None
        selected_proof = None
        endpoint_results = []
        shrink_range = False
        for _family, endpoints in families:
            family_result = None
            family_endpoint = None
            for endpoint in endpoints:
                try:
                    family_result = _request_transfer_logs(
                        endpoint, cfg, current, chunk_end
                    )
                except TransferLogRangeError:
                    shrink_range = True
                    break
                if family_result is not None:
                    family_endpoint = endpoint
                    break
            if shrink_range:
                break
            if family_result is None:
                continue

            endpoint_results.append((family_endpoint, family_result))
            if len(endpoint_results) >= RPC_LOG_QUORUM:
                try:
                    selected_logs, selected_proof = select_transfer_log_quorum(
                        endpoint_results
                    )
                    break
                except TransferLogQuorumError:
                    pass

        if shrink_range:
            if chunk_size > 1000:
                chunk_size = max(chunk_size // 2, 1000)
                reduced_chunk_successes = 0
                print(
                    f"    ⚠️ Retrying block {current:,} with smaller chunk "
                    f"({chunk_size:,} blocks)"
                )
                continue
            selected_logs = None

        success = selected_logs is not None
        if success:
            agreeing_provider_families.update(selected_proof["providerFamilies"])
            disagreeing_provider_families.update(
                selected_proof["disagreeingProviderFamilies"]
            )
            matched = int(selected_proof["matchingProviderFamilies"])
            minimum_matching_families = (
                matched
                if minimum_matching_families is None
                else min(minimum_matching_families, matched)
            )
            for log in selected_logs:
                from_addr = "0x" + log["topics"][1][26:].lower()
                to_addr = "0x" + log["topics"][2][26:].lower()
                value_wei = int(log["data"], 16)
                block_num = int(log["blockNumber"], 16)
                tx_hash = str(log.get("transactionHash") or "").lower()
                log_index = int(log.get("logIndex") or "0x0", 16)
                all_transfers.append(
                    (from_addr, to_addr, value_wei, block_num, tx_hash, log_index)
                )

        if not success:
            if chunk_size > 1000:
                chunk_size = max(chunk_size // 2, 1000)
                reduced_chunk_successes = 0
                print(
                    f"    ⚠️ {cfg['name']}: no independent RPC quorum for "
                    f"{current:,}-{chunk_end:,}; retrying with {chunk_size:,} blocks"
                )
                continue
            if state is not None:
                _save_verified_scan_staging(
                    state,
                    chain_key,
                    start_block,
                    end_block,
                    current,
                    all_transfers,
                    verified_chunk_count=chunks_done,
                    provider_families=agreeing_provider_families,
                    disagreeing_provider_families=disagreeing_provider_families,
                    minimum_matching_provider_families=minimum_matching_families,
                )
            chunks_failed += 1
            skipped_ranges.append([current, chunk_end])
            print(
                f"    ⚠️ Failed quorum at block {current}; stopping before "
                f"the active cache can be changed"
            )
            break

        current = chunk_end + 1
        chunks_done += 1

        if chunks_done % 10 == 0 or current > end_block:
            pct = min(100, (current - start_block) * 100 // max(total_blocks, 1))
            print(f"    {cfg['name']}: {pct}% (block {current:,}/{end_block:,}, {len(all_transfers):,} txs)", flush=True)

        # Checkpoint only staged, quorum-verified rows. Never move the active
        # cache cursor until the whole requested range succeeds.
        if state is not None and chunks_done % 20 == 0:
            _save_verified_scan_staging(
                state,
                chain_key,
                start_block,
                end_block,
                current,
                all_transfers,
                verified_chunk_count=chunks_done,
                provider_families=agreeing_provider_families,
                disagreeing_provider_families=disagreeing_provider_families,
                minimum_matching_provider_families=minimum_matching_families,
            )

        if chunk_size < cfg["chunk_size"]:
            reduced_chunk_successes += 1
            if reduced_chunk_successes >= RPC_LOG_CHUNK_REGROW_SUCCESS_THRESHOLD:
                chunk_size = min(chunk_size * 2, cfg["chunk_size"])
                reduced_chunk_successes = 0
        else:
            reduced_chunk_successes = 0

        time.sleep(RPC_LOG_CHUNK_DELAY_SECONDS)

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

    if state is not None and chunks_failed == 0:
        state.setdefault("verified_scan_proofs", {})[chain_key] = {
            "startBlock": int(start_block),
            "endBlock": int(end_block),
            "verifiedChunkCount": int(chunks_done),
            "logCount": len(all_transfers),
            "minimumMatchingProviderFamilies": int(
                minimum_matching_families or RPC_LOG_QUORUM
            ),
            "providerFamilies": sorted(agreeing_provider_families),
            "disagreeingProviderFamilies": sorted(disagreeing_provider_families),
        }

    print(f"  ✅ {cfg['name']}: {len(all_transfers):,} transfers found")
    return all_transfers, chunks_failed, total_chunks_attempted


def _save_verified_scan_staging(
    state,
    chain_key,
    start_block,
    end_block,
    next_block,
    transfers,
    *,
    verified_chunk_count,
    provider_families,
    disagreeing_provider_families,
    minimum_matching_provider_families,
):
    staging = state.setdefault("verified_scan_staging", {})
    staging[chain_key] = {
        "startBlock": int(start_block),
        "endBlock": int(end_block),
        "nextBlock": int(next_block),
        "transfers": [list(transfer) for transfer in transfers],
        "verification": "independent-rpc-exact-quorum",
        "verifiedChunkCount": int(verified_chunk_count),
        "providerFamilies": sorted(provider_families),
        "disagreeingProviderFamilies": sorted(disagreeing_provider_families),
        "minimumMatchingProviderFamilies": int(
            minimum_matching_provider_families or RPC_LOG_QUORUM
        ),
    }
    save_state(state)


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
        if eip7702_delegation_address(code):
            continue
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


def fetch_dolomite_dolo_balances(
    addresses,
    request_fn=None,
    subgraphs=None,
    attempts=3,
    now_ts=None,
    block_numbers=None,
):
    """Return positive DOLO balances held inside Dolomite.

    Positive Par is aggregated by effective user across every subaccount, then
    converted with the supply index at the queried block. Without
    ``block_numbers`` the snapshot must be current and fresh. Results are
    published only when every requested DOLO-market subgraph responds with a
    complete snapshot; a partial cross-chain total would be misleading.
    """
    request_fn = request_fn or requests.post
    subgraphs = subgraphs or DOLOMITE_DOLO_POSITION_SUBGRAPHS
    attempts = max(1, int(attempts or 1))
    now_ts = int(now_ts if now_ts is not None else time.time())
    include_all_positive_users = addresses is None
    targets = {
        str(address or "").lower()
        for address in addresses or []
        if re.fullmatch(r"0x[a-fA-F0-9]{40}", str(address or ""))
    }
    scope = "all-positive-effective-users" if include_all_positive_users else "selected-wallets"
    if not include_all_positive_users and not targets:
        return {}, {"status": "complete", "failedChains": [], "chains": {}, "scope": scope}

    block_numbers = block_numbers or {}
    balances = {
        address: {chain_key: Decimal(0) for chain_key in subgraphs}
        for address in targets
    }
    chain_metadata = {}
    failed_chains = []
    page_size = 1000

    for chain_key, config in subgraphs.items():
        requested_block = int(block_numbers.get(chain_key) or 0)
        historical_query = requested_block > 0
        meta_block_arg = f"(block: {{ number: {requested_block} }})" if historical_query else ""
        entity_block_arg = f", block: {{ number: {requested_block} }}" if historical_query else ""
        chain_rows = []
        supply_index = None
        block_number = 0
        block_timestamp = 0
        error = None
        skip = 0

        while True:
            query = f'''{{
              _meta{meta_block_arg} {{ block {{ number timestamp }} }}
              interestIndexes(first: 1000{entity_block_arg}) {{
                id
                supplyIndex
                token {{ id symbol marketId }}
              }}
              marginAccountTokenValues(
                first: {page_size},
                skip: {skip},
                {"block: { number: " + str(requested_block) + " }," if historical_query else ""}
                where: {{ token: "{DOLO_CONTRACT}", valuePar_gt: "0" }},
                orderBy: id,
                orderDirection: asc
              ) {{
                valuePar
                marginAccount {{
                  effectiveUser {{ id }}
                  user {{ id }}
                }}
              }}
            }}'''
            payload = None
            for attempt in range(attempts):
                try:
                    response = request_fn(
                        config["url"],
                        json={"query": query},
                        timeout=30,
                        headers={"Content-Type": "application/json"},
                    )
                    status_code = int(getattr(response, "status_code", 200) or 0)
                    if status_code >= 400:
                        raise RuntimeError(f"HTTP {status_code}")
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise RuntimeError("invalid JSON response")
                    if payload.get("errors"):
                        raise RuntimeError(str(payload["errors"][0]))
                    break
                except (OSError, ValueError, RuntimeError, requests.RequestException) as exc:
                    error = exc
                    payload = None
                    if attempt + 1 < attempts:
                        time.sleep(min(2 ** attempt, 4))

            if payload is None:
                break
            data = payload.get("data")
            if not isinstance(data, dict):
                error = RuntimeError("missing GraphQL data")
                break

            meta_block = ((data.get("_meta") or {}).get("block") or {})
            block_number = int(meta_block.get("number") or block_number or 0)
            block_timestamp = int(meta_block.get("timestamp") or block_timestamp or 0)
            if historical_query and block_number != requested_block:
                error = RuntimeError(
                    f"historical block mismatch ({block_number} != {requested_block})"
                )
                break
            if not historical_query and (
                not block_timestamp
                or now_ts - block_timestamp > DOLOMITE_POSITION_MAX_AGE_SECONDS
            ):
                error = RuntimeError("stale subgraph snapshot")
                break

            rows = data.get("marginAccountTokenValues")
            if not isinstance(rows, list):
                error = RuntimeError("missing DOLO position rows")
                break

            if supply_index is None:
                for index_row in data.get("interestIndexes") or []:
                    token = index_row.get("token") or {}
                    if str(token.get("id") or index_row.get("id") or "").lower() != DOLO_CONTRACT:
                        continue
                    try:
                        supply_index = Decimal(str(index_row.get("supplyIndex") or ""))
                    except InvalidOperation:
                        supply_index = None
                    break
                if (supply_index is None or supply_index <= 0) and rows:
                    error = RuntimeError("missing DOLO supply index")
                    break
                if supply_index is None or supply_index <= 0:
                    # Before the DOLO market existed, a pinned historical block
                    # correctly has neither an index nor positive positions.
                    supply_index = Decimal(1)
            chain_rows.extend(rows)
            if len(rows) < page_size:
                break
            skip += page_size

        if error is not None:
            failed_chains.append(chain_key)
            print(
                f"  ⚠️ {config.get('name', chain_key)} Dolomite DOLO positions unavailable: {error}",
                flush=True,
            )
            continue

        seen_wallets = set()
        for row in chain_rows:
            account = row.get("marginAccount") or {}
            owner = account.get("effectiveUser") or account.get("user") or {}
            address = str(owner.get("id") or "").lower()
            if not re.fullmatch(r"0x[a-f0-9]{40}", address):
                continue
            if not include_all_positive_users and address not in targets:
                continue
            try:
                value_par = Decimal(str(row.get("valuePar") or "0"))
            except InvalidOperation:
                continue
            if value_par <= 0:
                continue
            if address not in balances:
                balances[address] = {
                    active_chain_key: Decimal(0)
                    for active_chain_key in subgraphs
                }
            balances[address][chain_key] += value_par * supply_index
            seen_wallets.add(address)

        chain_metadata[chain_key] = {
            "requestedBlock": requested_block or block_number,
            "blockNumber": block_number,
            "blockTimestamp": block_timestamp,
            "matchedWallets": len(seen_wallets),
            "custodyAddress": DOLOMITE_MARGIN_ADDRS[chain_key],
        }

    if failed_chains:
        return {}, {
            "status": "unavailable",
            "failedChains": sorted(failed_chains),
            "chains": chain_metadata,
            "scope": scope,
        }

    output = {}
    for address, chain_values in balances.items():
        row = {
            chain_key: round(float(chain_values.get(chain_key, Decimal(0))), 6)
            for chain_key in subgraphs
        }
        row["total"] = round(sum(row.values()), 6)
        output[address] = row
    return output, {
        "status": "complete",
        "failedChains": [],
        "chains": chain_metadata,
        "scope": scope,
    }


def fetch_dolomite_dolo_trades(
    chain_key,
    block_number,
    block_timestamp=0,
    request_fn=None,
    subgraphs=None,
    attempts=3,
):
    """Fetch every Dolomite trade whose input or output asset is DOLO.

    The query is pinned to the same chain block used by the ERC-20 flow scan.
    Returning a partial side would bias buys or sells, so any incomplete page
    fails the whole chain closed and leaves the last published artifact live.
    """
    request_fn = request_fn or requests.post
    subgraphs = subgraphs or DOLOMITE_DOLO_POSITION_SUBGRAPHS
    config = subgraphs.get(chain_key)
    target_block = max(0, int(block_number or 0))
    target_timestamp = max(0, int(block_timestamp or 0))
    attempts = max(1, int(attempts or 1))
    if not config or target_block <= 0:
        return [], {
            "status": "unavailable",
            "blockNumber": target_block,
            "eventCount": 0,
            "error": "missing subgraph config or target block",
        }

    page_size = 1000
    rows_by_id = {}
    for token_field in ("takerToken", "makerToken"):
        cursor = ""
        while True:
            cursor_filter = f", id_gt: {json.dumps(cursor)}" if cursor else ""
            query = f'''{{
              _meta(block: {{ number: {target_block} }}) {{
                block {{ number timestamp }}
              }}
              trades(
                first: {page_size},
                orderBy: id,
                orderDirection: asc,
                block: {{ number: {target_block} }},
                where: {{ {token_field}: "{DOLO_CONTRACT}"{cursor_filter} }}
              ) {{
                id
                takerEffectiveUser {{ id }}
                makerEffectiveUser {{ id }}
                takerToken {{ id }}
                makerToken {{ id }}
                takerTokenDeltaWei
                makerTokenDeltaWei
                transaction {{ id timestamp blockNumber }}
              }}
            }}'''
            payload = None
            error = None
            for attempt in range(attempts):
                try:
                    response = request_fn(
                        config["url"],
                        json={"query": query},
                        timeout=30,
                        headers={"Content-Type": "application/json"},
                    )
                    status_code = int(getattr(response, "status_code", 200) or 0)
                    if status_code >= 400:
                        raise RuntimeError(f"HTTP {status_code}")
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise RuntimeError("invalid JSON response")
                    if payload.get("errors"):
                        raise RuntimeError(str(payload["errors"][0]))
                    break
                except (OSError, ValueError, RuntimeError, requests.RequestException) as exc:
                    error = exc
                    payload = None
                    if attempt + 1 < attempts:
                        time.sleep(min(2 ** attempt, 4))
            if payload is None:
                return [], {
                    "status": "unavailable",
                    "blockNumber": target_block,
                    "eventCount": 0,
                    "error": str(error or "subgraph request failed"),
                }

            data = payload.get("data")
            if not isinstance(data, dict) or not isinstance(data.get("trades"), list):
                return [], {
                    "status": "unavailable",
                    "blockNumber": target_block,
                    "eventCount": 0,
                    "error": "missing GraphQL trade data",
                }
            meta_block = ((data.get("_meta") or {}).get("block") or {})
            try:
                response_block = int(meta_block.get("number") or 0)
                response_timestamp = int(meta_block.get("timestamp") or 0)
            except (TypeError, ValueError):
                response_block = 0
                response_timestamp = 0
            if response_block != target_block:
                return [], {
                    "status": "unavailable",
                    "blockNumber": target_block,
                    "eventCount": 0,
                    "error": f"pinned block mismatch ({response_block} != {target_block})",
                }
            if response_timestamp > 0:
                target_timestamp = response_timestamp

            page = data["trades"]
            for row in page:
                if not isinstance(row, dict):
                    return [], {
                        "status": "unavailable",
                        "blockNumber": target_block,
                        "eventCount": 0,
                        "error": "malformed Dolomite trade event",
                    }
                transaction = row.get("transaction") or {}
                row_id = str(row.get("id") or "")
                taker = row.get("takerEffectiveUser") or {}
                maker = row.get("makerEffectiveUser")
                taker_token_row = row.get("takerToken") or {}
                maker_token_row = row.get("makerToken") or {}
                taker_token = (
                    str(taker_token_row.get("id") or "").lower()
                    if isinstance(taker_token_row, dict)
                    else ""
                )
                maker_token = (
                    str(maker_token_row.get("id") or "").lower()
                    if isinstance(maker_token_row, dict)
                    else ""
                )
                try:
                    transaction_block = int(transaction.get("blockNumber") or 0)
                    transaction_timestamp = int(transaction.get("timestamp") or 0)
                    taker_amount = Decimal(
                        str(row.get("takerTokenDeltaWei") or "0")
                    )
                    maker_amount = Decimal(
                        str(row.get("makerTokenDeltaWei") or "0")
                    )
                except (AttributeError, InvalidOperation, TypeError, ValueError):
                    transaction_block = 0
                    transaction_timestamp = 0
                    taker_amount = Decimal(0)
                    maker_amount = Decimal(0)
                valid_maker = maker is None or (
                    isinstance(maker, dict)
                    and re.fullmatch(
                        r"0x[a-f0-9]{40}", str(maker.get("id") or "").lower()
                    )
                )
                if (
                    not row_id
                    or not isinstance(transaction, dict)
                    or not re.fullmatch(
                        r"0x[a-f0-9]{64}",
                        str(transaction.get("id") or "").lower(),
                    )
                    or transaction_timestamp <= 0
                    or transaction_block <= 0
                    or transaction_block > target_block
                    or not isinstance(taker, dict)
                    or not re.fullmatch(
                        r"0x[a-f0-9]{40}", str(taker.get("id") or "").lower()
                    )
                    or not valid_maker
                    or not re.fullmatch(r"0x[a-f0-9]{40}", taker_token)
                    or not re.fullmatch(r"0x[a-f0-9]{40}", maker_token)
                    or (taker_token if token_field == "takerToken" else maker_token)
                    != DOLO_CONTRACT
                    or taker_amount <= 0
                    or maker_amount <= 0
                ):
                    return [], {
                        "status": "unavailable",
                        "blockNumber": target_block,
                        "eventCount": 0,
                        "error": "malformed Dolomite trade event",
                    }
                rows_by_id[row_id] = row
            if len(page) < page_size:
                break
            next_cursor = str((page[-1] or {}).get("id") or "")
            if not next_cursor or next_cursor == cursor:
                return [], {
                    "status": "unavailable",
                    "blockNumber": target_block,
                    "eventCount": 0,
                    "error": "trade pagination cursor did not advance",
                }
            cursor = next_cursor

    rows = sorted(rows_by_id.values(), key=lambda row: str(row.get("id") or ""))
    return rows, {
        "status": "complete",
        "blockNumber": target_block,
        "blockTimestamp": target_timestamp,
        "eventCount": len(rows),
        "source": "official-dolomite-subgraph-pinned-block",
    }


def _holder_protocol_snapshot_from_cached_chains(chain_rows, chain_keys):
    """Merge one point's exact per-chain protocol balances by effective owner."""
    addresses = set()
    for chain_key in chain_keys:
        addresses.update((chain_rows.get(chain_key) or {}).get("balances", {}).keys())
    snapshot = {}
    for address in addresses:
        row = {
            chain_key: round(
                float((chain_rows.get(chain_key) or {}).get("balances", {}).get(address) or 0),
                6,
            )
            for chain_key in chain_keys
        }
        row["total"] = round(sum(row.values()), 6)
        if row["total"] > 0:
            snapshot[address] = row
    return snapshot


def _holder_cached_protocol_chain_valid(row):
    if not isinstance(row, dict) or int(row.get("blockNumber") or 0) <= 0:
        return False
    balances = row.get("balances")
    if not isinstance(balances, dict):
        return False
    for address, value in balances.items():
        if (
            not re.fullmatch(r"0x[a-f0-9]{40}", str(address or ""))
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            return False
    return True


def load_holder_dolomite_history_snapshots(
    state,
    points,
    current_blocks,
    base_ts,
    request_fn=None,
    subgraphs=None,
    checkpoint_fn=save_state,
    request_delay_seconds=HOLDER_DOLOMITE_HISTORY_REQUEST_DELAY_SECONDS,
    cutoff_blocks_by_point=None,
):
    """Load or fetch complete daily DOLO-in-Dolomite snapshots.

    Each chain is cached independently so an interrupted initial backfill can
    resume without repeating successful archive-subgraph requests. A public
    combined snapshot is returned only when every requested point has every
    active chain; partial cross-chain exposure would move wallets into the
    wrong holder bucket.
    """
    subgraphs = subgraphs or DOLOMITE_DOLO_POSITION_SUBGRAPHS
    chain_keys = tuple(subgraphs.keys())
    cache = state.get("holder_dolomite_history")
    if not isinstance(cache, dict) or cache.get("version") != HOLDER_DOLOMITE_HISTORY_CACHE_VERSION:
        cache = {
            "version": HOLDER_DOLOMITE_HISTORY_CACHE_VERSION,
            "points": {},
        }
        state["holder_dolomite_history"] = cache
    cache_points = cache.setdefault("points", {})
    fetched = 0
    cached = 0
    pre_market_zero = 0
    snapshots = {}

    for point in sorted(points, key=lambda item: item["ts"]):
        point_key = point["key"]
        point_cache = cache_points.setdefault(point_key, {
            "timestamp": point["timestamp"],
            "chains": {},
        })
        point_cache["timestamp"] = point["timestamp"]
        chain_cache = point_cache.setdefault("chains", {})
        for chain_key in chain_keys:
            exact_cutoff = int(
                (cutoff_blocks_by_point or {}).get(point_key, {}).get(chain_key)
                or 0
            )
            if exact_cutoff > 0:
                # ERC-20 replay removes every transfer in the first block at or
                # after the point timestamp. Query protocol state at the prior
                # block so wallet and Dolomite legs describe the same instant.
                requested_block = max(
                    int(CHAINS.get(chain_key, {}).get("deploy_block", 0)),
                    exact_cutoff - 1,
                )
            else:
                requested_block = int(holder_history_cutoff_block(
                    chain_key, point["ts"], base_ts, current_blocks
                ))
            cached_chain = chain_cache.get(chain_key)
            if (
                _holder_cached_protocol_chain_valid(cached_chain)
                and int(cached_chain.get("requestedBlock") or 0) == requested_block
                and (
                    exact_cutoff <= 0
                    or int(cached_chain.get("transferCutoffBlock") or 0) == exact_cutoff
                )
            ):
                cached += 1
                continue

            market_start_block = int(subgraphs[chain_key].get("marketStartBlock") or 0)
            if market_start_block and requested_block < market_start_block:
                # This is not an availability fallback. The official archive
                # subgraph proves the DOLO market/index did not yet exist, so
                # the exact positive protocol balance at this block is zero.
                chain_cache[chain_key] = {
                    "requestedBlock": requested_block,
                    "transferCutoffBlock": exact_cutoff or None,
                    "blockNumber": requested_block,
                    "blockTimestamp": int(point["ts"]),
                    "balances": {},
                    "marketStartBlock": market_start_block,
                    "evidence": "verified-zero-before-dolo-market",
                }
                pre_market_zero += 1
                continue

            balances, metadata = fetch_dolomite_dolo_balances(
                None,
                request_fn=request_fn,
                subgraphs={chain_key: subgraphs[chain_key]},
                attempts=3,
                now_ts=base_ts,
                block_numbers={chain_key: requested_block},
            )
            chain_meta = (metadata.get("chains") or {}).get(chain_key) or {}
            if metadata.get("status") != "complete" or not chain_meta:
                raise RuntimeError(
                    f"{subgraphs[chain_key].get('name', chain_key)} historical "
                    f"Dolomite DOLO snapshot unavailable for {point_key}"
                )
            chain_cache[chain_key] = {
                "requestedBlock": requested_block,
                "transferCutoffBlock": exact_cutoff or None,
                "blockNumber": int(chain_meta.get("blockNumber") or 0),
                "blockTimestamp": int(chain_meta.get("blockTimestamp") or 0),
                "balances": {
                    address: round(float(values.get(chain_key) or 0), 6)
                    for address, values in balances.items()
                    if float(values.get(chain_key) or 0) > 0
                },
            }
            fetched += 1
            if request_delay_seconds:
                time.sleep(request_delay_seconds)
            if checkpoint_fn and fetched % 10 == 0:
                checkpoint_fn(state)

        missing = [chain_key for chain_key in chain_keys if chain_key not in chain_cache]
        if missing:
            raise RuntimeError(
                f"Incomplete historical Dolomite DOLO coverage for {point_key}: {missing}"
            )
        snapshots[point_key] = _holder_protocol_snapshot_from_cached_chains(
            chain_cache, chain_keys
        )

    keep_keys = {point["key"] for point in points}
    cache["points"] = {
        key: value for key, value in cache_points.items() if key in keep_keys
    }
    cache["updatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if checkpoint_fn and fetched:
        checkpoint_fn(state)
    return snapshots, {
        "status": "complete",
        "schemaVersion": HOLDER_DOLOMITE_HISTORY_CACHE_VERSION,
        "pointCount": len(points),
        "chainCount": len(chain_keys),
        "chains": list(chain_keys),
        "fetchedChainSnapshots": fetched,
        "cachedChainSnapshots": cached,
        "preMarketZeroChainSnapshots": pre_market_zero,
        "startTimestamp": points[0]["timestamp"] if points else None,
        "endTimestamp": points[-1]["timestamp"] if points else None,
    }


def calculate_flows(transfers, excluded):
    """Calculate net flow per address from transfer list.
    Positive = accumulator, Negative = seller.
    Transfers involving mint/burn addresses (ZERO, DOLO contract) are skipped
    entirely — mints are not accumulation and burns are not selling.
    Detected DEX/LP contracts are kept in the calculation (both legs counted)
    but filtered from the final results by get_top()."""
    flows = {}
    for from_addr, to_addr, value_wei, _, *_ in transfers:
        if from_addr in FLOW_SKIP_ADDRS or to_addr in FLOW_SKIP_ADDRS:
            continue
        value = value_wei / (10 ** 18)
        flows[from_addr] = flows.get(from_addr, 0) - value
        flows[to_addr] = flows.get(to_addr, 0) + value
    return flows


def calculate_flow_components(transfers):
    """Return gross directional amounts and their reconciled net per address."""
    components = {}
    for from_addr, to_addr, value_wei, _, *_ in transfers:
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


def protocol_custody_addresses(chain_key):
    """Return verified Dolomite custody endpoints for an active DOLO market."""
    if chain_key not in DOLO_MARKET_IDS:
        return set()
    margin = DOLOMITE_MARGIN_ADDRS.get(chain_key)
    return {
        address
        for address in (DOLO_DEPOSIT_WITHDRAWAL_ROUTER, margin)
        if address
    }


def contract_detection_candidates(transfers, chain_key, raw_flows, top_n=30):
    """Return high-signal addresses for contract detection.

    Net flow catches ordinary routers but misses balanced intermediaries. Add
    the largest direct Dolomite-custody counterparties by gross transferred
    value so a contract such as EnsoAggregatorTrader is still inspected while
    keeping RPC usage bounded.
    """
    limit = max(0, int(top_n or 0))
    top_by_net = sorted(
        (raw_flows or {}).items(),
        key=lambda item: abs(item[1]),
        reverse=True,
    )[:limit]
    candidates = {str(address or "").lower() for address, _ in top_by_net if address}

    custody = protocol_custody_addresses(chain_key)
    custody_volume = {}
    if custody:
        for transfer in transfers or []:
            if not isinstance(transfer, (list, tuple)) or len(transfer) < 3:
                continue
            from_addr = str(transfer[0] or "").lower()
            to_addr = str(transfer[1] or "").lower()
            from_custody = from_addr in custody
            to_custody = to_addr in custody
            if from_custody == to_custody:
                continue
            counterparty = to_addr if from_custody else from_addr
            if not re.fullmatch(r"0x[a-f0-9]{40}", counterparty):
                continue
            try:
                value_wei = int(transfer[2])
            except (TypeError, ValueError):
                continue
            if value_wei <= 0:
                continue
            custody_volume[counterparty] = custody_volume.get(counterparty, 0) + value_wei

    top_by_custody_volume = sorted(
        custody_volume.items(),
        key=lambda item: item[1],
        reverse=True,
    )[:limit]
    candidates.update(address for address, _ in top_by_custody_volume)
    return sorted(candidates)


def neutralize_protocol_custody_transfers(flows, components, transfers, chain_key):
    """Remove wallet↔Dolomite custody legs from market-behavior flow totals.

    ERC-20 transfers into Dolomite custody move tokens out of the wallet, but
    do not transfer beneficial ownership or prove selling. The inverse applies
    to withdrawals. Raw transfers remain untouched for holder-balance history;
    this adjusted view is used only by the DOLO Flow leaderboards.
    """
    custody = protocol_custody_addresses(chain_key)
    adjusted_flows = dict(flows or {})
    adjusted_components = {
        address: dict(values)
        for address, values in (components or {}).items()
    }
    if not custody:
        return adjusted_flows, adjusted_components

    def ensure_component(address):
        return adjusted_components.setdefault(
            address,
            {
                "gross_inflow": 0.0,
                "gross_outflow": 0.0,
                "net_flow": 0.0,
            },
        )

    for transfer in transfers or []:
        if not isinstance(transfer, (list, tuple)) or len(transfer) < 4:
            continue
        from_addr = str(transfer[0] or "").lower()
        to_addr = str(transfer[1] or "").lower()
        if from_addr in FLOW_SKIP_ADDRS or to_addr in FLOW_SKIP_ADDRS:
            continue
        try:
            value = int(transfer[2]) / (10 ** 18)
        except (TypeError, ValueError):
            continue
        if value <= 0:
            continue

        from_custody = from_addr in custody
        to_custody = to_addr in custody
        if from_custody == to_custody:
            continue

        if to_custody:
            # Undo the raw wallet outflow. The wallet still beneficially owns
            # the supplied DOLO inside Dolomite.
            adjusted_flows[from_addr] = adjusted_flows.get(from_addr, 0.0) + value
            entry = ensure_component(from_addr)
            entry["gross_outflow"] = max(0.0, entry.get("gross_outflow", 0.0) - value)
            entry["net_flow"] = entry.get("net_flow", 0.0) + value
            entry["protocol_deposit"] = entry.get("protocol_deposit", 0.0) + value
        else:
            # Undo the raw wallet inflow from custody. It is a protocol
            # withdrawal/borrow leg, not evidence of a market acquisition.
            adjusted_flows[to_addr] = adjusted_flows.get(to_addr, 0.0) - value
            entry = ensure_component(to_addr)
            entry["gross_inflow"] = max(0.0, entry.get("gross_inflow", 0.0) - value)
            entry["net_flow"] = entry.get("net_flow", 0.0) - value
            entry["protocol_withdrawal"] = entry.get("protocol_withdrawal", 0.0) + value

    # Custody endpoints are accounting infrastructure, not market actors. A
    # complete deposit normally ends with router→Margin and a withdrawal starts
    # with Margin→router; leaving the Margin leg in the adjusted map would emit
    # the protocol itself as an accumulator/seller and break conservation.
    for address in custody:
        adjusted_flows[address] = 0.0
        if address in adjusted_components:
            adjusted_components[address]["gross_inflow"] = 0.0
            adjusted_components[address]["gross_outflow"] = 0.0
            adjusted_components[address]["net_flow"] = 0.0

    for address, value in list(adjusted_flows.items()):
        if abs(value) < 0.000001:
            adjusted_flows[address] = 0.0
    return adjusted_flows, adjusted_components


def calculate_dolomite_trade_adjustments(trades, cutoff_block=0):
    """Return beneficial-owner DOLO deltas created inside Dolomite trades.

    ERC-20 transfers only show the wallet-to-custody deposit. A later Zap/Trade
    changes the user's DOLO balance inside Dolomite without another wallet
    Transfer, so the market-flow view must add that internal balance delta.
    Subgraph ``*DeltaWei`` values are already decimal token amounts.
    """
    cutoff_block = max(0, int(cutoff_block or 0))
    working = {}

    def add_delta(address, amount, transaction):
        address = str(address or "").lower()
        if not re.fullmatch(r"0x[a-f0-9]{40}", address) or amount == 0:
            return
        row = working.setdefault(
            address,
            {
                "net_flow": Decimal(0),
                "gross_inflow": Decimal(0),
                "gross_outflow": Decimal(0),
                "tx_hashes": set(),
                "latest_inflow": None,
                "latest_outflow": None,
            },
        )
        row["net_flow"] += amount
        direction = "inflow" if amount > 0 else "outflow"
        row[f"gross_{direction}"] += abs(amount)
        tx_hash = str(transaction.get("id") or "").lower()
        timestamp = int(transaction.get("timestamp") or 0)
        block_number = int(transaction.get("blockNumber") or 0)
        if re.fullmatch(r"0x[a-f0-9]{64}", tx_hash):
            row["tx_hashes"].add(tx_hash)
        evidence = {
            "tx_hash": tx_hash,
            "timestamp": timestamp,
            "block_number": block_number,
        }
        latest_key = f"latest_{direction}"
        prior = row[latest_key]
        if prior is None or (
            block_number,
            timestamp,
            tx_hash,
        ) > (
            int(prior.get("block_number") or 0),
            int(prior.get("timestamp") or 0),
            str(prior.get("tx_hash") or ""),
        ):
            row[latest_key] = evidence

    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        transaction = trade.get("transaction") or {}
        try:
            block_number = int(transaction.get("blockNumber") or 0)
        except (TypeError, ValueError):
            continue
        if block_number < cutoff_block:
            continue
        taker = ((trade.get("takerEffectiveUser") or {}).get("id") or "").lower()
        maker = ((trade.get("makerEffectiveUser") or {}).get("id") or "").lower()
        taker_token = str((trade.get("takerToken") or {}).get("id") or "").lower()
        maker_token = str((trade.get("makerToken") or {}).get("id") or "").lower()
        row_deltas = {}

        def stage(address, amount):
            if re.fullmatch(r"0x[a-f0-9]{40}", address or ""):
                row_deltas[address] = row_deltas.get(address, Decimal(0)) + amount

        try:
            if taker_token == DOLO_CONTRACT:
                amount = Decimal(str(trade.get("takerTokenDeltaWei") or "0"))
                if amount > 0:
                    stage(taker, -amount)
                    stage(maker, amount)
            if maker_token == DOLO_CONTRACT:
                amount = Decimal(str(trade.get("makerTokenDeltaWei") or "0"))
                if amount > 0:
                    stage(taker, amount)
                    stage(maker, -amount)
        except (InvalidOperation, TypeError, ValueError):
            continue
        for address, amount in row_deltas.items():
            add_delta(address, amount, transaction)

    return {
        address: {
            "net_flow": float(row["net_flow"]),
            "gross_inflow": float(row["gross_inflow"]),
            "gross_outflow": float(row["gross_outflow"]),
            "tx_count": len(row["tx_hashes"]),
            "latest_inflow": row["latest_inflow"],
            "latest_outflow": row["latest_outflow"],
        }
        for address, row in working.items()
        if row["net_flow"] or row["gross_inflow"] or row["gross_outflow"]
    }


def apply_dolomite_trade_adjustments(flows, components, adjustments):
    """Apply internal Dolomite trade deltas to the beneficial-owner flow view."""
    adjusted_flows = dict(flows or {})
    adjusted_components = {
        address: dict(values)
        for address, values in (components or {}).items()
    }
    for raw_address, summary in (adjustments or {}).items():
        address = str(raw_address or "").lower()
        if not re.fullmatch(r"0x[a-f0-9]{40}", address):
            continue
        net_flow = float((summary or {}).get("net_flow") or 0)
        trade_inflow = max(0.0, float((summary or {}).get("gross_inflow") or 0))
        trade_outflow = max(0.0, float((summary or {}).get("gross_outflow") or 0))
        adjusted_flows[address] = adjusted_flows.get(address, 0.0) + net_flow
        row = adjusted_components.setdefault(
            address,
            {"gross_inflow": 0.0, "gross_outflow": 0.0, "net_flow": 0.0},
        )
        row["gross_inflow"] = float(row.get("gross_inflow") or 0) + trade_inflow
        row["gross_outflow"] = float(row.get("gross_outflow") or 0) + trade_outflow
        row["net_flow"] = float(row.get("net_flow") or 0) + net_flow
        row["dolomite_trade_inflow"] = (
            float(row.get("dolomite_trade_inflow") or 0) + trade_inflow
        )
        row["dolomite_trade_outflow"] = (
            float(row.get("dolomite_trade_outflow") or 0) + trade_outflow
        )
    for address, value in list(adjusted_flows.items()):
        if abs(value) < 0.000001:
            adjusted_flows[address] = 0.0
    return adjusted_flows, adjusted_components


def attach_dolomite_trade_metadata(rows, adjustments, direction, chain):
    """Prefer a newer exact Dolomite trade over ERC-20 transfer evidence."""
    if direction not in {"inbound", "outbound"}:
        raise ValueError("Unsupported Dolomite trade metadata direction")
    latest_key = "latest_inflow" if direction == "inbound" else "latest_outflow"
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        address = str(row.get("address") or "").lower()
        evidence = ((adjustments or {}).get(address) or {}).get(latest_key)
        if not isinstance(evidence, dict):
            continue
        tx_hash = str(evidence.get("tx_hash") or "").lower()
        try:
            timestamp = int(evidence.get("timestamp") or 0)
        except (TypeError, ValueError):
            continue
        if (
            not re.fullmatch(r"0x[a-f0-9]{64}", tx_hash)
            or timestamp <= int(row.get("latest_tx_timestamp") or 0)
        ):
            continue
        row["latest_tx_hash"] = tx_hash
        row["latest_tx_timestamp"] = timestamp
        row["latest_tx_chain"] = chain
    return rows


def calculate_bridge_flows(transfers):
    """Calculate flows from canonical bridge mint/burn transfers.
    These are invisible to calculate_flows() but critical for cross-chain
    bridge detection. Bridges use burn (to 0x0) on the source chain and
    mint (from 0x0) on the destination chain.
    
    Returns: {addr: net_bridge_flow} where positive = received mints,
    negative = sent burns."""
    bridge_flows = {}
    for from_addr, to_addr, value_wei, _, *_ in transfers:
        value = value_wei / (10 ** 18)
        if from_addr in BRIDGE_ADDRS and to_addr not in BRIDGE_ADDRS:
            # Mint: receiver got tokens via bridge
            bridge_flows[to_addr] = bridge_flows.get(to_addr, 0) + value
        elif to_addr in BRIDGE_ADDRS and from_addr not in BRIDGE_ADDRS:
            # Burn: sender sent tokens to bridge
            bridge_flows[from_addr] = bridge_flows.get(from_addr, 0) - value
    return bridge_flows


def calculate_bridge_adapter_outflows(transfers):
    """Return user outflows into a canonical bridge adapter."""
    outflows = {}
    for from_addr, to_addr, value_wei, _, *_ in transfers:
        if to_addr not in BRIDGE_ADAPTER_ADDRS or from_addr in BRIDGE_ADAPTER_ADDRS:
            continue
        value = value_wei / (10 ** 18)
        outflows[from_addr] = outflows.get(from_addr, 0) + value
    return outflows


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


def load_holder_history_cutoff_blocks(state, points, current_blocks):
    """Return timestamp-exact daily cutoff blocks, cached permanently by day."""
    cache = state.get("holder_history_cutoff_blocks")
    if not isinstance(cache, dict) or cache.get("version") != HOLDER_HISTORY_CUTOFF_CACHE_VERSION:
        cache = {
            "version": HOLDER_HISTORY_CUTOFF_CACHE_VERSION,
            "chains": {},
        }
        state["holder_history_cutoff_blocks"] = cache
    cutoff_by_point = {point["key"]: {} for point in points}
    fetched = 0
    cached = 0
    timestamp_cache = {}
    keep_keys = {point["key"] for point in points}

    for chain_key, cfg in CHAINS.items():
        chain_cache = cache.setdefault("chains", {}).setdefault(chain_key, {})
        unresolved = []
        for point in points:
            saved = chain_cache.get(point["key"])
            block = int(saved.get("block") or 0) if isinstance(saved, dict) else 0
            if (
                block >= int(cfg.get("deploy_block", 0))
                and block <= int(current_blocks[chain_key])
                and int(saved.get("targetTimestamp") or 0) == int(point["ts"])
            ):
                cutoff_by_point[point["key"]][chain_key] = block
                cached += 1
            else:
                unresolved.append(point)

        if unresolved:
            loader = lambda blocks, key=chain_key: load_block_timestamps_batch(
                key, blocks, timestamp_cache
            )
            resolved_blocks = find_first_blocks_at_or_after_timestamps(
                int(cfg.get("deploy_block", 0)),
                int(current_blocks[chain_key]),
                [point["ts"] for point in unresolved],
                loader,
                nominal_block_time=cfg["block_time"],
            )
            resolved_timestamps = loader(resolved_blocks)
            previous_blocks = [
                block - 1
                for block in resolved_blocks
                if block > int(cfg.get("deploy_block", 0))
            ]
            previous_timestamps = loader(previous_blocks)
            for point, block in zip(unresolved, resolved_blocks):
                previous_timestamp = (
                    previous_timestamps[block - 1]
                    if block > int(cfg.get("deploy_block", 0))
                    else None
                )
                if resolved_timestamps[block] < int(point["ts"]) or (
                    previous_timestamp is not None
                    and previous_timestamp >= int(point["ts"])
                ):
                    raise RuntimeError(
                        f"{cfg['name']}: non-exact holder cutoff for {point['key']}"
                    )
                chain_cache[point["key"]] = {
                    "targetTimestamp": int(point["ts"]),
                    "block": int(block),
                    "blockTimestamp": int(resolved_timestamps[block]),
                    "method": "first-block-at-or-after-timestamp",
                }
                cutoff_by_point[point["key"]][chain_key] = int(block)
                fetched += 1

        cache["chains"][chain_key] = {
            key: value for key, value in chain_cache.items() if key in keep_keys
        }

    cache["updatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return cutoff_by_point, {
        "status": "complete",
        "method": "first-block-at-or-after-timestamp",
        "pointCount": len(points),
        "chainCount": len(CHAINS),
        "fetchedChainCutoffs": fetched,
        "cachedChainCutoffs": cached,
    }


def calculate_neutralized_flows_for_cutoffs(all_transfers, cutoff_by_chain):
    raw_flows = {}
    bridge_flows_by_chain = {}
    adapter_outflows_by_chain = {}
    for chain_key in CHAINS:
        cutoff = cutoff_by_chain[chain_key]
        period_transfers = [t for t in all_transfers[chain_key] if t[3] >= cutoff]
        raw_flows[chain_key] = calculate_flows(period_transfers, EXCLUDED_ADDRS)
        bridge_flows_by_chain[chain_key] = calculate_bridge_flows(period_transfers)
        adapter_outflows_by_chain[chain_key] = calculate_bridge_adapter_outflows(
            period_transfers
        )
    return neutralize_raw_and_bridge_flows(
        raw_flows,
        bridge_flows_by_chain,
        adapter_outflows_by_chain,
    )


def _legacy_neutralize_raw_and_bridge_flows(raw_flows, bridge_flows_by_chain):
    augmented_flows = {}
    for chain_key in CHAINS:
        augmented_flows[chain_key] = dict(raw_flows[chain_key])
        for addr, bflow in bridge_flows_by_chain[chain_key].items():
            augmented_flows[chain_key][addr] = augmented_flows[chain_key].get(addr, 0) + bflow

    neutralized_aug, count, volume = neutralize_cross_chain_flows(augmented_flows)
    neutralized = {}
    for chain_key in CHAINS:
        neutralized[chain_key] = dict(raw_flows[chain_key])
        for addr in raw_flows[chain_key]:
            original_aug = raw_flows[chain_key].get(addr, 0) + bridge_flows_by_chain[chain_key].get(addr, 0)
            neutralized_aug_val = neutralized_aug[chain_key].get(addr, 0)
            delta = neutralized_aug_val - original_aug
            if abs(delta) > 0.01:
                neutralized[chain_key][addr] = raw_flows[chain_key][addr] + delta
    return neutralized, count, volume


def neutralize_raw_and_bridge_flows(
    raw_flows,
    bridge_flows_by_chain,
    adapter_outflows_by_chain=None,
):
    neutralized, _, _, _ = neutralize_raw_and_bridge_flows_with_stats(
        raw_flows,
        bridge_flows_by_chain,
        adapter_outflows_by_chain,
    )
    return neutralized


def neutralize_raw_and_bridge_flows_with_audit(
    raw_flows,
    bridge_flows_by_chain,
    adapter_outflows_by_chain=None,
):
    """Return neutralized flows with exact-vs-heuristic bridge telemetry."""
    adapter_outflows_by_chain = adapter_outflows_by_chain or {
        chain_key: {} for chain_key in CHAINS
    }
    neutralized = {
        chain_key: dict(raw_flows[chain_key])
        for chain_key in CHAINS
    }
    cancellations = {chain_key: {} for chain_key in CHAINS}
    matched_addresses = set()
    matched_volume = 0.0

    bridge_mints = {
        chain_key: {
            addr: amount
            for addr, amount in bridge_flows_by_chain[chain_key].items()
            if amount > 0.01
        }
        for chain_key in CHAINS
    }
    source_addresses = set()
    for chain_outflows in adapter_outflows_by_chain.values():
        source_addresses.update(chain_outflows)

    for addr in source_addresses:
        for source_chain in CHAINS:
            remaining = float(
                adapter_outflows_by_chain.get(source_chain, {}).get(addr, 0) or 0
            )
            if remaining <= 0.01:
                continue
            for destination_chain in CHAINS:
                if destination_chain == source_chain:
                    continue
                available_mint = bridge_mints[destination_chain].get(addr, 0)
                if available_mint <= 0.01:
                    continue
                cancel_amount = min(remaining, available_mint)
                neutralized[source_chain][addr] = (
                    neutralized[source_chain].get(addr, 0) + cancel_amount
                )
                cancellations[source_chain][addr] = (
                    cancellations[source_chain].get(addr, 0) + cancel_amount
                )
                bridge_mints[destination_chain][addr] -= cancel_amount
                remaining -= cancel_amount
                matched_addresses.add(addr)
                matched_volume += cancel_amount
                if remaining <= 0.01:
                    break

    # This compatibility path has no transaction/message identity. Observe it
    # for diagnostics only: applying it can cancel unrelated opposing flows.
    # Only exact adapter-outflow -> same-wallet destination-mint evidence is
    # allowed to change published flow arithmetic.
    legacy_raw = {
        chain_key: {
            addr: amount
            for addr, amount in raw_flows[chain_key].items()
            if addr not in matched_addresses
        }
        for chain_key in CHAINS
    }
    legacy_bridge = {
        chain_key: {
            addr: amount
            for addr, amount in bridge_flows_by_chain[chain_key].items()
            if addr not in matched_addresses
        }
        for chain_key in CHAINS
    }
    _legacy_neutralized, legacy_count, legacy_volume = (
        _legacy_neutralize_raw_and_bridge_flows(legacy_raw, legacy_bridge)
    )

    audit = {
        "canonicalAdapter": {
            "addressCount": len(matched_addresses),
            "dolo": round(matched_volume, 6),
        },
        "legacyHeuristic": {
            "addressCount": 0,
            "dolo": 0.0,
        },
        "legacyHeuristicObserved": {
            "addressCount": legacy_count,
            "dolo": round(legacy_volume, 6),
        },
        "total": {
            "addressCount": len(matched_addresses),
            "dolo": round(matched_volume, 6),
        },
    }
    return neutralized, audit, cancellations


def neutralize_raw_and_bridge_flows_with_stats(
    raw_flows,
    bridge_flows_by_chain,
    adapter_outflows_by_chain=None,
):
    """Remove same-wallet CCIP legs while preserving cross-wallet transfers."""
    neutralized, audit, cancellations = neutralize_raw_and_bridge_flows_with_audit(
        raw_flows,
        bridge_flows_by_chain,
        adapter_outflows_by_chain,
    )
    return (
        neutralized,
        audit["total"]["addressCount"],
        audit["total"]["dolo"],
        cancellations,
    )


def apply_bridge_outflow_cancellations(components_by_chain, cancellations_by_chain):
    adjusted = {
        chain_key: {
            addr: dict(component)
            for addr, component in chain_components.items()
        }
        for chain_key, chain_components in components_by_chain.items()
    }
    for chain_key, cancellations in cancellations_by_chain.items():
        for addr, amount in cancellations.items():
            component = adjusted.get(chain_key, {}).get(addr)
            if not component:
                continue
            component["gross_outflow"] = max(
                0.0,
                float(component.get("gross_outflow") or 0) - amount,
            )
            component["net_flow"] = float(component.get("net_flow") or 0) + amount
    return adjusted


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
    from_addr, to_addr, value_wei, _, *_ = transfer
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


def calculate_holder_bucket_history(
    all_transfers,
    points,
    current_blocks,
    base_ts,
    vesting_labels=None,
    dolomite_history=None,
    current_dolomite_balances=None,
    cutoff_blocks_by_point=None,
):
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
    current_vedolo_positions = load_current_vedolo_positions()
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
            cutoff = (
                int(current_blocks[chain_key]) + 1
                if point["key"] == "now"
                else int(
                    (cutoff_blocks_by_point or {}).get(point["key"], {}).get(chain_key)
                    or holder_history_cutoff_block(
                        chain_key, point["ts"], base_ts, current_blocks
                    )
                )
            )
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
        locked_balances = locked_map_at_holder_point(
            point["ts"],
            current_locks,
            vedolo_events,
            current_positions=current_vedolo_positions,
        )
        if point["key"] == "now":
            protocol_snapshot = current_dolomite_balances or {}
        else:
            protocol_snapshot = (dolomite_history or {}).get(point["key"], {})
        if dolomite_history is not None and point["key"] != "now" and point["key"] not in dolomite_history:
            raise RuntimeError(
                f"Missing historical Dolomite DOLO snapshot for {point['key']}"
            )
        protocol_balances = {
            address: max(0, float(values.get("total") or 0))
            for address, values in protocol_snapshot.items()
            if isinstance(values, dict) and float(values.get("total") or 0) > 0
        }
        protocol_by_chain = {
            chain_key: {
                address: max(0, float(values.get(chain_key) or 0))
                for address, values in protocol_snapshot.items()
                if isinstance(values, dict) and float(values.get(chain_key) or 0) > 0
            }
            for chain_key in CHAINS
        }
        row = {
            "key": point["key"],
            "timestamp": point["timestamp"],
            "liquid": {},
            "with_vedolo": {},
            "total_exposure": {},
            "total_exposure_with_vedolo": {},
        }
        wallet_row = {
            "timestamp": point["timestamp"],
            "liquid": {},
            "with_vedolo": {},
            "total_exposure": {},
            "total_exposure_with_vedolo": {},
        }
        for audience in ("market", "holders", "potential"):
            for source_key in (
                "liquid",
                "with_vedolo",
                "total_exposure",
                "total_exposure_with_vedolo",
            ):
                row[source_key][audience] = {}
            # The UI's wallet Details panel uses the canonical "holders"
            # audience only. Persisting the almost-identical market and
            # potential lists for every day and every exposure mode bloats the
            # lazy history file beyond GitHub's 100 MB blob limit.
            if HOLDER_WALLET_HISTORY_VIEWS and audience == "holders":
                for source_key in (
                    "liquid",
                    "with_vedolo",
                    "total_exposure",
                    "total_exposure_with_vedolo",
                ):
                    wallet_row[source_key][audience] = {}
            for view, bucket_defs in HOLDER_BUCKET_GROUPS.items():
                row["liquid"][audience][view] = build_bucket_model(
                    liquid_balances,
                    {},
                    holder_rows,
                    address_labels,
                    bucket_defs,
                    include_allocations=audience == "holders",
                    audience=audience,
                )
                row["with_vedolo"][audience][view] = build_bucket_model(
                    liquid_balances,
                    locked_balances,
                    holder_rows,
                    address_labels,
                    bucket_defs,
                    include_allocations=audience == "holders",
                    audience=audience,
                )
                row["total_exposure"][audience][view] = build_bucket_model(
                    liquid_balances,
                    {},
                    holder_rows,
                    address_labels,
                    bucket_defs,
                    include_allocations=audience == "holders",
                    audience=audience,
                    protocol_balances=protocol_balances,
                )
                row["total_exposure_with_vedolo"][audience][view] = build_bucket_model(
                    liquid_balances,
                    locked_balances,
                    holder_rows,
                    address_labels,
                    bucket_defs,
                    include_allocations=audience == "holders",
                    audience=audience,
                    protocol_balances=protocol_balances,
                )
                if view in HOLDER_WALLET_HISTORY_VIEWS and audience == "holders":
                    wallet_row["liquid"][audience][view] = build_bucket_wallet_history_rows(
                        liquid_by_chain,
                        {},
                        holder_rows,
                        address_labels,
                        bucket_defs,
                        audience=audience,
                        liquid_balances=liquid_balances,
                    )
                    wallet_row["with_vedolo"][audience][view] = build_bucket_wallet_history_rows(
                        liquid_by_chain,
                        locked_balances,
                        holder_rows,
                        address_labels,
                        bucket_defs,
                        audience=audience,
                        liquid_balances=liquid_balances,
                    )
                    wallet_row["total_exposure"][audience][view] = build_bucket_wallet_history_rows(
                        liquid_by_chain,
                        {},
                        holder_rows,
                        address_labels,
                        bucket_defs,
                        audience=audience,
                        protocol_by_chain=protocol_by_chain,
                        liquid_balances=liquid_balances,
                    )
                    wallet_row["total_exposure_with_vedolo"][audience][view] = build_bucket_wallet_history_rows(
                        liquid_by_chain,
                        locked_balances,
                        holder_rows,
                        address_labels,
                        bucket_defs,
                        audience=audience,
                        protocol_by_chain=protocol_by_chain,
                        liquid_balances=liquid_balances,
                    )
        history.append(row)
        wallet_history[point["key"]] = wallet_row

    return sorted(history, key=lambda row: row["timestamp"]), wallet_history


def calculate_cex_supply_history(
    all_transfers,
    points,
    current_blocks,
    base_ts,
    vesting_labels=None,
    cutoff_blocks_by_point=None,
):
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
            cutoff = int(
                (cutoff_blocks_by_point or {}).get(point["key"], {}).get(chain_key)
                or holder_history_cutoff_block(
                    chain_key, point["ts"], base_ts, current_blocks
                )
            )
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
            "exchanges": cex["exchanges"],
            # Same balances and classification as the aggregate, not today's
            # holder file substituted into an older selected date.
            "walletBalances": sorted([
                {
                    "address": addr.lower(),
                    "label": (address_labels.get(addr.lower()) or {}).get("label", ""),
                    "exchange": canonical_cex_name((address_labels.get(addr.lower()) or {}).get("label")),
                    "balance": round(float(value), 6),
                    "evidenceStatus": cex_label_evidence_status(address_labels.get(addr.lower()) or {}),
                }
                for addr, value in liquid_balances.items()
                if float(value or 0) > 0
                and holder_distribution_type(addr, holder_rows, address_labels) == "cex"
            ], key=lambda row: (-row["balance"], row["address"])),
        })

    return sorted(history, key=lambda row: row["timestamp"])


def canonical_cex_name(label):
    """Collapse verified CEX wallet-role labels into their exchange family."""
    name = str(label or "").strip()
    verified_variants = {
        "mexc wallet": "MEXC",
        "bingx-linked": "BingX",
        "gate.io routing wallet": "Gate.io",
        "kucoin wallet": "KuCoin",
    }
    if name.casefold() in verified_variants:
        return verified_variants[name.casefold()]
    name = re.sub(r"\s+(?:deposit|hot wallet|cold wallet)(?:\s+\d+)?$", "", name, flags=re.I)
    return re.sub(r"\s+\d+$", "", name).strip()


def build_cex_supply_point(liquid_balances, holder_rows, address_labels):
    total = 0
    wallets = 0
    exchanges = {}
    for addr, value in liquid_balances.items():
        liquid = max(0, float(value or 0))
        if liquid <= 0:
            continue
        if holder_distribution_type(addr, holder_rows, address_labels) != "cex":
            continue
        total += liquid
        wallets += 1
        name = canonical_cex_name((address_labels.get(addr.lower()) or {}).get("label"))
        aggregate = exchanges.setdefault(name, {"name": name, "liquid": 0, "wallets": 0})
        aggregate["liquid"] += liquid
        aggregate["wallets"] += 1
    total_liquid = round(total, 2)
    rounded_exchanges = [
        {
            "name": aggregate["name"],
            "liquid": round(aggregate["liquid"], 2),
            "wallets": aggregate["wallets"],
        }
        for aggregate in exchanges.values()
    ]
    residual_cents = int(round(total_liquid * 100)) - sum(
        int(round(aggregate["liquid"] * 100)) for aggregate in rounded_exchanges
    )
    if residual_cents and rounded_exchanges:
        reconciliation_order = sorted(
            exchanges.values(),
            key=lambda aggregate: (-aggregate["liquid"], aggregate["name"]),
        )
        rounded_by_name = {aggregate["name"]: aggregate for aggregate in rounded_exchanges}
        if residual_cents > 0:
            target = rounded_by_name[reconciliation_order[0]["name"]]
            target["liquid"] = round(target["liquid"] + residual_cents / 100, 2)
        else:
            cents_to_remove = -residual_cents
            for aggregate in reconciliation_order:
                target = rounded_by_name[aggregate["name"]]
                target_cents = int(round(target["liquid"] * 100))
                removed_cents = min(target_cents, cents_to_remove)
                target["liquid"] = round((target_cents - removed_cents) / 100, 2)
                cents_to_remove -= removed_cents
                if cents_to_remove == 0:
                    break
    return {
        "wallets": wallets,
        "liquid": total_liquid,
        "exchanges": sorted(
            rounded_exchanges,
            key=lambda aggregate: (-aggregate["liquid"], aggregate["name"]),
        ),
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


def merge_chain_flow_maps(flows_by_chain):
    """Sum complete per-chain flow maps before any leaderboard truncation."""
    merged = {}
    for chain_flows in (flows_by_chain or {}).values():
        for raw_addr, raw_value in (chain_flows or {}).items():
            addr = str(raw_addr or "").lower()
            if not addr:
                continue
            merged[addr] = merged.get(addr, 0.0) + float(raw_value or 0)
    return merged


def merge_chain_flow_components(components_by_chain):
    """Sum gross and protocol components for the exact combined flow rows."""
    fields = (
        "gross_inflow",
        "gross_outflow",
        "net_flow",
        "protocol_deposit",
        "protocol_withdrawal",
        "dolomite_trade_inflow",
        "dolomite_trade_outflow",
    )
    merged = {}
    for chain_components in (components_by_chain or {}).values():
        for raw_addr, values in (chain_components or {}).items():
            addr = str(raw_addr or "").lower()
            if not addr:
                continue
            target = merged.setdefault(addr, {field: 0.0 for field in fields})
            for field in fields:
                target[field] += float((values or {}).get(field, 0) or 0)
    return merged


def merge_chain_counts(counts_by_chain):
    merged = {}
    for chain_counts in (counts_by_chain or {}).values():
        for raw_addr, raw_count in (chain_counts or {}).items():
            addr = str(raw_addr or "").lower()
            if addr:
                merged[addr] = merged.get(addr, 0) + int(raw_count or 0)
    return merged


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


def verified_user_contract_addresses(holder_rows):
    """Return holder contracts proven to represent a user-controlled wallet."""
    visible_contract_wallet_types = {
        "safe",
        "multisig",
        "delegated_eoa",
        "smart_account",
    }
    return {
        str(addr or "").lower()
        for addr, row in (holder_rows or {}).items()
        if str((row or {}).get("contract_wallet_type") or "").lower()
        in visible_contract_wallet_types
    }


STRATEGIC_INVESTOR_CLAIMS = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
INVESTOR_CLAIMS = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"
INVESTOR_INTERNAL_RECIPIENTS = {
    "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4",  # Dolomite Gnosis Safe
    STRATEGIC_INVESTOR_CLAIMS,
    INVESTOR_CLAIMS,
}


def extract_vesting_investors(all_transfers):
    claim_contracts = {
        ("bera", STRATEGIC_INVESTOR_CLAIMS): "strategic_investor_claims",
        ("bera", INVESTOR_CLAIMS): "investor_claims",
    }
    source_order = ["strategic_investor_claims", "investor_claims"]
    records = {}

    for chain_key in ["eth", "bera"]:
        for transfer in all_transfers.get(chain_key, []):
            from_addr = str(transfer[0] or "").lower()
            to_addr = str(transfer[1] or "").lower()
            source = claim_contracts.get((chain_key, from_addr))
            if (
                not source
                or not re.fullmatch(r"0x[a-f0-9]{40}", to_addr)
                or to_addr in INVESTOR_INTERNAL_RECIPIENTS
            ):
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
        is_strategic = "strategic_investor_claims" in row["sources"]
        has_long_term = "investor_claims" in row["sources"]
        wallet_rows.append({
            "address": address,
            "label": "Strategic Investor" if is_strategic else "Long-term Investor",
            "type": "investor",
            "claimSources": sources,
            "primarySource": sources[0],
            "alsoReceivedLongTermTranche": bool(is_strategic and has_long_term),
            "roundAttribution": {
                "key": "2024-strategic-900k",
                "label": "2024 strategic round · $900K",
                "status": "high-confidence-onchain-attribution",
            } if is_strategic else None,
            "vestingSchedule": "3-year vesting · 1-year cliff" if has_long_term else None,
            "transferCount": row["transfer_count"],
            "firstTransferBlock": row["first_block"],
            "lastTransferBlock": row["last_block"],
            "sourceChains": sorted(row["chains"]),
            "receivedDolo": format_dolo_wei(row["received_wei"]),
        })

    strategic_set = {
        row["address"] for row in wallet_rows
        if "strategic_investor_claims" in row["claimSources"]
    }
    inv_set = {
        row["address"] for row in wallet_rows
        if "investor_claims" in row["claimSources"]
    }

    return {
        "schemaVersion": 3,
        "contracts": {
            "strategicInvestorClaims": STRATEGIC_INVESTOR_CLAIMS,
            "investorClaims": INVESTOR_CLAIMS,
        },
        "methodology": {
            "classification": "direct-dolo-transfer-from-official-claim-contract",
            "overlapPriority": "strategic-investor",
            "team": "not-derived-from-investor-claims",
            "internalRecipients": "known-protocol-and-claim-controller-addresses-excluded",
        },
        "strategic_investors": sorted(strategic_set),
        "early_investors": sorted(strategic_set),
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
        label = {
            "Early Investor": "Strategic Investor",
            "Investor": "Long-term Investor",
        }.get(row.get("label"), row.get("label"))
        if not re.fullmatch(r"0x[a-f0-9]{40}", addr_key) or label not in {"Strategic Investor", "Long-term Investor"}:
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
    strategic_rows = vesting_data.get("strategic_investors")
    if strategic_rows is None:
        strategic_rows = vesting_data.get("early_investors", []) or []
    for key, label, label_type in [
        ("strategic_investors", "Strategic Investor", "investor"),
        ("investors", "Long-term Investor", "investor"),
        ("team", "Core Team", "protocol"),
    ]:
        rows = legacy_team_rows if key == "team" else (
            strategic_rows if key == "strategic_investors" else vesting_data.get(key, []) or []
        )
        for addr in rows:
            addr_key = str(addr or "").lower()
            if not re.fullmatch(r"0x[a-f0-9]{40}", addr_key):
                continue
            labels.setdefault(addr_key, {"label": label, "type": label_type})
    return labels


def load_address_labels(vesting_labels=None):
    labels = {}
    for filename in ("dolo-address-labels.js", "dolo-address-overrides.js"):
        labels_file = os.path.join(DATA_DIR, filename)
        if not os.path.exists(labels_file):
            continue
        try:
            with open(labels_file, encoding="utf-8") as handle:
                text = handle.read()
        except OSError as e:
            print(f"  ⚠️ Could not load {filename} for bucket history: {e}")
            continue
        for match in re.finditer(r'"(0x[a-fA-F0-9]{40})"\s*:\s*\{([^}]+)\}', text):
            body = match.group(2)
            label_match = re.search(r'label\s*:\s*"([^"]+)"', body)
            type_match = re.search(r'type\s*:\s*"([^"]+)"', body)
            raw_type = type_match.group(1).lower() if type_match else ""
            labels[match.group(1).lower()] = {
                "label": label_match.group(1) if label_match else "",
                "type": ADDRESS_TYPE_ALIASES.get(raw_type, raw_type),
            }
            for field in ("source", "confidence"):
                field_match = re.search(r'\b' + field + r'\s*:\s*"([^"]+)"', body)
                if field_match:
                    labels[match.group(1).lower()][field] = field_match.group(1)
    vesting_file = os.path.join(DATA_DIR, "vesting_investors.json")
    if os.path.exists(vesting_file):
        try:
            with open(vesting_file) as f:
                merge_vesting_labels(labels, json.load(f))
        except Exception as e:
            print(f"  ⚠️ Could not load vesting labels for bucket history: {e}")
    merge_vesting_labels(labels, vesting_labels)
    return labels


def select_dynamic_flow_exclusions(
    detected_contracts,
    address_labels,
    verified_user_contracts=None,
):
    """Keep known custody/user contracts visible; exclude infrastructure CAs."""
    visible_label_types = {
        "cex",
        "multisig",
        "safe",
        "contract_wallet",
        "protocol",
        "bot",
        "mm",
        "trader",
    }
    verified_user_contracts = {
        str(address or "").lower()
        for address in (verified_user_contracts or set())
    }
    exclusions = set()
    for raw_addr in detected_contracts:
        addr = str(raw_addr or "").lower()
        label_type = str((address_labels.get(addr) or {}).get("type") or "").lower()
        if (
            addr in USER_CONTRACT_WALLET_ADDRS
            or addr in verified_user_contracts
            or label_type in visible_label_types
        ):
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
    if label_type in {"multisig", "safe", "contract_wallet"}:
        return "multisig"
    if label_type == "cex":
        return "cex"
    if label.startswith("Core Team"):
        return "team"
    if label_type == "investor":
        return "investor"
    # Safe/delegation describes account technology, not who operates it.
    # Keep this identity precedence aligned with the chart's JS classifier.
    if label_type in {"watch", "mm"}:
        return label_type
    if label_type in {"bot", "liquidator", "trader"}:
        return "bot"
    if label_type in {"protocol", "lp", "contract", "dead"}:
        return "ca"
    if key in USER_CONTRACT_WALLET_ADDRS or contract_wallet_type in {"safe", "multisig"}:
        return "multisig"
    if contract_wallet_type in {"delegated_eoa", "smart_account"}:
        return "eoa"
    if holder.get("is_contract"):
        return "ca"
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


def parse_debank_explicit_entity(page_html):
    """Return only a direct, explicit DeBank CEX owner badge.

    Funding/counterparty text such as ``Funded By Coinbase`` is deliberately
    ignored: it describes a relationship, not ownership of the inspected
    wallet, and is not strong enough evidence to remove a user from Fresh.
    """
    if not page_html:
        return {}
    for tag_match in re.finditer(r"<[^>]+>", page_html, flags=re.DOTALL):
        tag = tag_match.group(0)
        class_match = re.search(
            r"\bclass\s*=\s*([\"'])(.*?)\1",
            tag,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not class_match:
            continue
        classes = set(re.split(r"\s+", class_match.group(2).strip().lower()))
        if not {"db-user-tag", "is-cex"}.issubset(classes):
            continue
        title_match = re.search(
            r"\btitle\s*=\s*([\"'])(.*?)\1",
            tag,
            flags=re.IGNORECASE | re.DOTALL,
        )
        label = unescape(title_match.group(2)).strip() if title_match else ""
        if label:
            return {
                "label": label,
                "type": "cex",
                "source": "debank-explicit-cex-badge",
                "confidence": "confirmed",
            }
    return {}


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
    explicit_entity = parse_debank_explicit_entity(proc.stdout)
    if age_days is None:
        item = {
            "status": "debank_age_missing",
            "chain": "debank",
            "chain_name": "DeBank",
            "first_timestamp": 0,
            "source": "debank_age",
            "checked_at": int(base_ts),
        }
        if explicit_entity:
            item["entity"] = explicit_entity
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
    if explicit_entity:
        item["entity"] = explicit_entity
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
        for from_addr, to_addr, value_wei, _, *_ in transfers:
            value = value_wei / (10 ** 18)
            if from_addr != ZERO:
                chain_balances[from_addr] = chain_balances.get(from_addr, 0) - value
            if to_addr != ZERO:
                chain_balances[to_addr] = chain_balances.get(to_addr, 0) + value
    return balances


def detect_high_confidence_fresh_automation(all_transfers):
    """Identify balanced high-frequency DOLO actors that are not fresh users.

    This is intentionally strict and only gates the Fresh Wallet cohort. It
    does not create a permanent public wallet label: ambiguous addresses stay
    visible until they are manually confirmed in the canonical resolver.
    """
    stats = {}
    for transfers in (all_transfers or {}).values():
        for from_addr, to_addr, value_wei, *_ in transfers:
            from_key = str(from_addr or "").lower()
            to_key = str(to_addr or "").lower()
            if not from_key or not to_key or from_key == to_key:
                continue
            try:
                amount_wei = int(value_wei)
            except (TypeError, ValueError):
                continue
            if amount_wei <= 0:
                continue
            if to_key not in FLOW_SKIP_ADDRS:
                inbound = stats.setdefault(to_key, {"in_count": 0, "out_count": 0, "in_wei": 0, "out_wei": 0})
                inbound["in_count"] += 1
                inbound["in_wei"] += amount_wei
            if from_key not in FLOW_SKIP_ADDRS:
                outbound = stats.setdefault(from_key, {"in_count": 0, "out_count": 0, "in_wei": 0, "out_wei": 0})
                outbound["out_count"] += 1
                outbound["out_wei"] += amount_wei

    min_side_wei = FRESH_AUTOMATION_MIN_SIDE_DOLO * 10**18
    automated = set()
    for address, row in stats.items():
        inbound_wei = row["in_wei"]
        outbound_wei = row["out_wei"]
        turnover_wei = inbound_wei + outbound_wei
        if (
            row["in_count"] >= FRESH_AUTOMATION_MIN_INBOUND_TRANSFERS
            and row["out_count"] >= FRESH_AUTOMATION_MIN_OUTBOUND_TRANSFERS
            and min(inbound_wei, outbound_wei) >= min_side_wei
            and turnover_wei > 0
            and abs(inbound_wei - outbound_wei) * 10_000
            <= turnover_wei * FRESH_AUTOMATION_MAX_NET_SHARE_BPS
        ):
            automated.add(address)
    return automated


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
    address_labels = load_address_labels(extract_vesting_investors(all_transfers))
    current_locks = load_current_vedolo_locks()
    current_balances = calculate_current_balances_by_chain(all_transfers)
    automated_addresses = detect_high_confidence_fresh_automation(all_transfers)
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
            "nonUserWalletsExcluded": 0,
            "automatedWalletsExcluded": 0,
            "explicitEntityWalletsExcluded": 0,
        }
        for period in FRESH_HOLDER_PERIODS
    }

    for chain_key, transfers in all_transfers.items():
        for from_addr, to_addr, value_wei, block_number, *_ in transfers:
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
            if holder_type == "eoa" and addr in automated_addresses:
                audit[period]["automatedWalletsExcluded"] += 1
                continue
            if holder_type not in FRESH_USER_WALLET_TYPES:
                audit[period]["nonUserWalletsExcluded"] += 1
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
            explicit_entity = first_activity.get("entity") or (
                first_activity.get("debank_crosscheck") or {}
            ).get("entity") or {}
            if (
                explicit_entity.get("type") == "cex"
                and explicit_entity.get("confidence") == "confirmed"
            ):
                audit[period]["explicitEntityWalletsExcluded"] += 1
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


def load_current_vedolo_positions():
    """Return current veDOLO ownership and locked DOLO at token-id grain."""
    holders_file = os.path.join(DATA_DIR, "vedolo_holders.json")
    if not os.path.exists(holders_file):
        return {}
    try:
        with open(holders_file) as f:
            holders_data = json.load(f)
        snapshot_block = int(holders_data.get("snapshot_block") or 0)
        positions = {}
        for holder in holders_data.get("holders", []):
            owner = str(holder.get("address") or "").lower()
            if not owner:
                continue
            holder_tokens = []
            for token in holder.get("token_details") or []:
                try:
                    token_id = int(token.get("id"))
                    locked = float(token.get("dolo") or 0)
                except (TypeError, ValueError):
                    continue
                if token_id >= 0 and locked > 0:
                    positions[token_id] = {
                        "owner": owner,
                        "dolo": locked,
                        "snapshot_block": snapshot_block,
                    }
                    holder_tokens.append(token_id)
            if holder_tokens:
                expected_total = float(holder.get("total_dolo") or 0)
                observed_total = sum(positions[token_id]["dolo"] for token_id in holder_tokens)
                residual = expected_total - observed_total
                # token_details are rounded for publication; preserve the
                # authoritative per-wallet total by assigning only that tiny
                # rounding residue to its largest position.
                if abs(residual) <= max(1.0, expected_total * 1e-8):
                    largest = max(holder_tokens, key=lambda token_id: positions[token_id]["dolo"])
                    positions[largest]["dolo"] += residual
        return positions
    except Exception as e:
        print(f"  ⚠️ Could not load veDOLO positions for bucket history: {e}")
        return {}


def load_vedolo_flow_events():
    flows_file = os.path.join(DATA_DIR, "vedolo_flows.json")
    if not os.path.exists(flows_file):
        return {"locks": [], "unlocks": [], "transfers": []}
    try:
        with open(flows_file) as f:
            data = json.load(f)
        return {
            "locks": data.get("locks") or [],
            "unlocks": data.get("unlocks") or [],
            "transfers": data.get("transfers") or [],
        }
    except Exception as e:
        print(f"  ⚠️ Could not load veDOLO flows for bucket history: {e}")
        return {"locks": [], "unlocks": [], "transfers": []}


def locked_map_at_holder_point(
    point_ts,
    current_locks,
    vedolo_events,
    current_positions=None,
):
    if current_positions is not None:
        position_snapshot_block = max(
            (
                int(position.get("snapshot_block") or 0)
                for position in current_positions.values()
                if isinstance(position, dict)
            ),
            default=0,
        )
        expected_positions = {
            int(token_id): {
                "owner": str(position.get("owner") or "").lower(),
                "dolo": float(position.get("dolo") or 0),
            }
            for token_id, position in current_positions.items()
            if isinstance(position, dict)
            and str(position.get("owner") or "")
            and float(position.get("dolo") or 0) > 0
        }
        if current_locks and not expected_positions:
            raise RuntimeError(
                "Current veDOLO token positions are unavailable; refusing partial holder history"
            )
        first_transfer_from = {}
        unlock_owner = {}
        for transfer in sorted(
            vedolo_events.get("transfers", []),
            key=lambda item: (
                int(item.get("timestamp") or 0),
                int(item.get("block") or 0),
                int(item.get("logIndex") or 0),
            ),
        ):
            try:
                token_id = int(transfer.get("tokenId"))
            except (TypeError, ValueError):
                continue
            first_transfer_from.setdefault(
                token_id, str(transfer.get("from") or "").lower()
            )
        for unlock in vedolo_events.get("unlocks", []):
            try:
                token_id = int(unlock.get("tokenId"))
            except (TypeError, ValueError):
                continue
            owner = str(unlock.get("address") or "").lower()
            if owner:
                unlock_owner[token_id] = owner

        positions = {}
        events = []
        sequence = 0
        for kind in ("locks", "unlocks", "transfers"):
            for item in vedolo_events.get(kind, []):
                try:
                    timestamp = int(item.get("timestamp") or 0)
                    token_id = int(item.get("tokenId"))
                    block = int(item.get("block") or 0)
                except (TypeError, ValueError):
                    continue
                if timestamp >= point_ts or (
                    position_snapshot_block > 0 and block > position_snapshot_block
                ):
                    continue
                events.append((
                    timestamp,
                    block,
                    int(item.get("logIndex") or 0),
                    sequence,
                    kind,
                    token_id,
                    item,
                ))
                sequence += 1
        # Replay the token lifecycle forward. Withdraw events remove the full
        # position (their emitted DOLO can be the post-penalty payout), while
        # merge/split events move the locked principal between token IDs.
        for _timestamp, _block, _log_index, _sequence, kind, token_id, item in sorted(events):
            if kind == "transfers":
                position = positions.get(token_id)
                next_owner = str(item.get("to") or "").lower()
                if position and next_owner and next_owner != ZERO:
                    position["owner"] = next_owner
                continue
            amount = float(item.get("dolo") or 0)
            if kind == "unlocks":
                positions.pop(token_id, None)
                continue
            deposit_type = int(item.get("depositType") or 0)
            owner = (
                str(item.get("beneficiaryAddress") or "").lower()
                or first_transfer_from.get(token_id, "")
                or (expected_positions.get(token_id) or {}).get("owner", "")
                or unlock_owner.get(token_id, "")
                or str(item.get("address") or "").lower()
            )
            if deposit_type == 4:
                source_token_id = int(item.get("sourceTokenId") or 0)
                source = positions.pop(source_token_id, None)
                if not source and amount > 1:
                    raise RuntimeError(
                        f"Cannot replay veDOLO merge from missing token {source_token_id}"
                    )
                moved = float(source.get("dolo") if source else amount)
                target = positions.get(token_id)
                if target:
                    target["dolo"] += moved
                elif owner and moved > 0:
                    positions[token_id] = {"owner": owner, "dolo": moved}
                continue
            if deposit_type == 5:
                source_token_id = int(item.get("sourceTokenId") or 0)
                source = positions.get(source_token_id)
                if not source and amount > 1:
                    raise RuntimeError(
                        f"Cannot replay veDOLO split from missing token {source_token_id}"
                    )
                if source:
                    source["dolo"] = max(0, source["dolo"] - amount)
                if owner and amount > 0:
                    positions[token_id] = {"owner": owner, "dolo": amount}
                continue
            if deposit_type == 3 or amount <= 0:
                continue
            position = positions.get(token_id)
            if position:
                position["dolo"] += amount
            elif owner:
                positions[token_id] = {"owner": owner, "dolo": amount}

        latest_event_ts = max(
            (
                int(item.get("timestamp") or 0)
                for kind in ("locks", "unlocks", "transfers")
                for item in vedolo_events.get(kind, [])
                if position_snapshot_block <= 0
                or int(item.get("block") or 0) <= position_snapshot_block
            ),
            default=0,
        )
        if point_ts > latest_event_ts and expected_positions:
            reconciliation_tokens = {
                token_id
                for token_id in set(expected_positions) | set(positions)
                if max(
                    float((expected_positions.get(token_id) or {}).get("dolo") or 0),
                    float((positions.get(token_id) or {}).get("dolo") or 0),
                ) > 1
            }
            mismatches = []
            for token_id in sorted(reconciliation_tokens):
                expected = expected_positions.get(token_id)
                replayed = positions.get(token_id)
                if (
                    not expected
                    or not replayed
                    or replayed["owner"] != expected["owner"]
                    or abs(replayed["dolo"] - expected["dolo"]) > 1
                ):
                    mismatches.append(token_id)
            if mismatches:
                raise RuntimeError(
                    "veDOLO event ownership history is incomplete; "
                    f"{len(mismatches)} current material position(s) do not reconcile"
                )
            # The event replay proves ownership continuity; the current holder
            # snapshot is authoritative for exact principal. Canonicalizing the
            # latest point removes sub-DOLO event/serialization dust without
            # weakening the material (>1 DOLO) reconciliation guard.
            positions = {
                token_id: dict(position)
                for token_id, position in expected_positions.items()
            }

        locked = {}
        for position in positions.values():
            owner = position["owner"]
            if owner and owner != VEDOLO_CONTRACT_ADDR:
                locked[owner] = locked.get(owner, 0) + position["dolo"]
        return {addr: value for addr, value in locked.items() if value > 0.0001}

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
        for from_addr, to_addr, value_wei, _block, *_ in transfers:
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
            "protocol": 0,
            "locked": 0,
            "allocationWallets": 0,
            "allocationTotal": 0,
            "allocationLiquid": 0,
            "allocationProtocol": 0,
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
        "trackedProtocol": 0,
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
        "allocationProtocol": 0,
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
    if audience == "holders":
        return holder_type not in {"cex", "ca"} and holder_type not in HOLDER_POTENTIAL_TYPES
    raise ValueError(f"Unsupported holder audience: {audience}")


def build_bucket_model(
    liquid_balances,
    locked_balances,
    holder_rows,
    address_labels,
    bucket_defs,
    include_allocations=False,
    audience="market",
    protocol_balances=None,
):
    protocol_balances = protocol_balances or {}
    model = empty_bucket_model(bucket_defs, audience)
    addresses = (
        set(liquid_balances.keys())
        | set(locked_balances.keys())
        | set(protocol_balances.keys())
    )
    for addr in addresses:
        liquid = max(0, float(liquid_balances.get(addr) or 0))
        protocol = max(0, float(protocol_balances.get(addr) or 0))
        locked = max(0, float(locked_balances.get(addr) or 0))
        total = liquid + protocol + locked
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
            model["allocationProtocol"] += protocol
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
        bucket["protocol"] += protocol
        bucket["locked"] += locked
        if is_allocation:
            bucket["allocationWallets"] += 1
            bucket["allocationTotal"] += total
            bucket["allocationLiquid"] += liquid
            bucket["allocationProtocol"] += protocol
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
        model["trackedProtocol"] += protocol
        model["trackedLocked"] += locked
    for bucket in model["buckets"]:
        for key in [
            "total", "liquid", "protocol", "locked", "allocationTotal", "allocationLiquid",
            "allocationProtocol", "allocationLocked", "teamTotal", "investorTotal",
        ]:
            bucket[key] = round(bucket[key], 2)
    for key in [
        "trackedTotal", "trackedLiquid", "trackedProtocol", "trackedLocked", "excludedCexTotal",
        "excludedPotentialTotal",
        "excludedInsiderTotal", "allocationTotal", "allocationLiquid", "allocationProtocol",
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
    protocol_by_chain=None,
    liquid_balances=None,
):
    protocol_by_chain = protocol_by_chain or {}
    rows = []
    addresses = set(locked_balances.keys())
    if liquid_balances is not None:
        addresses.update(liquid_balances.keys())
    for chain_balances in liquid_by_chain.values():
        addresses.update(chain_balances.keys())
    for chain_balances in protocol_by_chain.values():
        addresses.update(chain_balances.keys())
    for addr in addresses:
        bal_eth = max(0, float(liquid_by_chain.get("eth", {}).get(addr) or 0))
        bal_bera = max(0, float(liquid_by_chain.get("bera", {}).get(addr) or 0))
        split_liquid = bal_eth + bal_bera
        liquid = (
            max(0, float(liquid_balances.get(addr) or 0))
            if liquid_balances is not None
            else split_liquid
        )
        # Wallet-level cross-chain neutralisation is authoritative. Keep the
        # displayed chain split proportional while reconciling Details exactly
        # to the total used by the chart model.
        if split_liquid > 0 and abs(split_liquid - liquid) > 0.000001:
            scale = liquid / split_liquid
            bal_eth *= scale
            bal_bera *= scale
        elif split_liquid <= 0 and liquid > 0:
            holder = holder_rows.get(addr, {})
            current_eth = max(0, float(holder.get("balance_eth") or 0))
            current_bera = max(0, float(holder.get("balance_bera") or 0))
            current_split = current_eth + current_bera
            if current_split > 0:
                bal_eth = liquid * current_eth / current_split
                bal_bera = liquid * current_bera / current_split
            else:
                bal_eth = liquid
        protocol_eth = max(0, float(protocol_by_chain.get("eth", {}).get(addr) or 0))
        protocol_bera = max(0, float(protocol_by_chain.get("bera", {}).get(addr) or 0))
        protocol = protocol_eth + protocol_bera
        locked = max(0, float(locked_balances.get(addr) or 0))
        total = liquid + protocol + locked
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
        row = {
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
        }
        # Protocol fields are sparse. Wallet-balance modes have no protocol
        # component, so repeating three zero-valued keys across daily rows is
        # wasted transfer and repository size.
        if protocol > 0:
            row["in_dolomite"] = round(protocol, 6)
            if protocol_eth > 0:
                row["in_dolomite_eth"] = round(protocol_eth, 6)
            if protocol_bera > 0:
                row["in_dolomite_bera"] = round(protocol_bera, 6)
        rows.append(row)
    return sorted(rows, key=lambda row: row["balance"], reverse=True)


def count_txs(transfers, excluded):
    """Count number of transactions per address."""
    counts = {}
    for from_addr, to_addr, _, _, *_ in transfers:
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


def build_searchable_flow_rows(flows, components, tx_counts, excluded=None):
    """Build the complete, post-neutralization flow index used by address search."""
    accumulators = get_top(flows, tx_counts, None, "accumulator", excluded)
    sellers = get_top(flows, tx_counts, None, "seller", excluded)
    for entry in accumulators + sellers:
        row_components = components.get(entry["address"], {})
        entry["gross_inflow"] = round(row_components.get("gross_inflow", 0), 2)
        entry["gross_outflow"] = round(row_components.get("gross_outflow", 0), 2)
        entry["protocol_deposit"] = round(row_components.get("protocol_deposit", 0), 2)
        entry["protocol_withdrawal"] = round(
            row_components.get("protocol_withdrawal", 0), 2
        )
        entry["dolomite_trade_inflow"] = round(
            row_components.get("dolomite_trade_inflow", 0), 2
        )
        entry["dolomite_trade_outflow"] = round(
            row_components.get("dolomite_trade_outflow", 0), 2
        )
    return accumulators, sellers


def _flow_decimal_text(value):
    text = format(Decimal(value), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def merge_verified_lp_activities(activities_by_chain):
    merged = {}
    for activities in (activities_by_chain or {}).values():
        for raw_addr, summary in (activities or {}).items():
            addr = str(raw_addr or "").lower()
            if not addr or not isinstance(summary, dict):
                continue
            target = merged.setdefault(
                addr,
                {
                    "deposit": Decimal(0),
                    "withdrawal": Decimal(0),
                    "pairs": set(),
                    "adapters": set(),
                    "latest": None,
                },
            )
            for key in ("deposit", "withdrawal"):
                try:
                    target[key] += Decimal(str(summary.get(key) or "0"))
                except InvalidOperation:
                    continue
            target["pairs"].update(summary.get("pairs") or [])
            target["adapters"].update(summary.get("adapters") or [])
            latest = summary.get("latest")
            prior = target["latest"]
            if isinstance(latest, dict) and (
                prior is None
                or (
                    int(latest.get("timestamp") or 0),
                    int(latest.get("block_number") or 0),
                    str(latest.get("tx_hash") or ""),
                )
                > (
                    int(prior.get("timestamp") or 0),
                    int(prior.get("block_number") or 0),
                    str(prior.get("tx_hash") or ""),
                )
            ):
                target["latest"] = latest
    return {
        addr: {
            "deposit": _flow_decimal_text(summary["deposit"]),
            "withdrawal": _flow_decimal_text(summary["withdrawal"]),
            "pairs": sorted(summary["pairs"]),
            "adapters": sorted(summary["adapters"]),
            "latest": summary["latest"],
        }
        for addr, summary in merged.items()
        if summary["latest"] is not None
    }


def classify_verified_lp_outflows(market_flows, activities):
    """Annotate verified LP deposits without changing transfer-derived net flow."""
    classified = dict(market_flows or {})
    annotations = {}
    for raw_addr, summary in (activities or {}).items():
        addr = str(raw_addr or "").lower()
        if not addr or not isinstance(summary, dict):
            continue
        try:
            deposit = Decimal(str(summary.get("deposit") or "0"))
            withdrawal = Decimal(str(summary.get("withdrawal") or "0"))
        except InvalidOperation:
            continue
        net_lp_deposit = deposit - withdrawal
        if net_lp_deposit < Decimal("0.005"):
            continue
        market_net = float(classified.get(addr, 0) or 0)
        latest = dict(summary.get("latest") or {})
        if latest.get("direction") != "deposit":
            continue
        latest["period_wallet_net_flow"] = round(market_net, 2)
        annotations[addr] = {
            "flow_basis": "wallet_net",
            "market_net_flow": round(market_net, 2),
            "latest_lp_activity": latest,
            "latest_tx_hash": latest.get("tx_hash"),
            "latest_tx_timestamp": latest.get("timestamp"),
            "latest_tx_chain": latest.get("chain"),
        }
    return classified, annotations


def apply_flow_annotations(rows, annotations):
    for row in rows or []:
        annotation = (annotations or {}).get(str(row.get("address") or "").lower())
        if not annotation:
            continue
        for key, value in annotation.items():
            if value is not None:
                row[key] = value


def apply_period_lp_totals(rows, activities):
    """Attach exact period LP totals and, when needed, their latest proof."""
    for row in rows or []:
        summary = (activities or {}).get(str(row.get("address") or "").lower())
        if not isinstance(summary, dict):
            continue
        activity = row.get("latest_lp_activity")
        if not isinstance(activity, dict):
            latest = summary.get("latest")
            if not isinstance(latest, dict) or latest.get("confidence") != "verified_same_tx":
                continue
            activity = dict(latest)
            row["latest_lp_activity"] = activity
            row["latest_tx_hash"] = latest.get("tx_hash")
            row["latest_tx_timestamp"] = latest.get("timestamp")
            row["latest_tx_chain"] = latest.get("chain")
        try:
            deposit = Decimal(str(summary.get("deposit") or "0"))
            withdrawal = Decimal(str(summary.get("withdrawal") or "0"))
        except InvalidOperation:
            continue
        if deposit < 0 or withdrawal < 0:
            continue
        activity["period_lp_deposit"] = _flow_decimal_text(deposit)
        activity["period_lp_withdrawal"] = _flow_decimal_text(withdrawal)
        for key in (
            "period_net_lp_deposit",
            "period_net_lp_withdrawal",
            "period_lp_rebalance",
        ):
            activity.pop(key, None)
        net = deposit - withdrawal
        if net > 0:
            activity["period_net_lp_deposit"] = _flow_decimal_text(net)
        elif net < 0:
            activity["period_net_lp_withdrawal"] = _flow_decimal_text(-net)
        overlap = min(deposit, withdrawal)
        if overlap > 0 and abs(net) <= max(Decimal("1"), overlap * Decimal("0.001")):
            activity["period_lp_rebalance"] = _flow_decimal_text(overlap)
    return rows


def lp_period_scan_wallets(row_groups):
    """Return only rows whose latest transaction already proved LP activity."""
    wallets = set()
    for rows in row_groups or []:
        for row in rows or []:
            activity = row.get("latest_lp_activity") if isinstance(row, dict) else None
            if not isinstance(activity, dict) or activity.get("confidence") != "verified_same_tx":
                continue
            address = str(row.get("address") or "").lower()
            if address:
                wallets.add(address)
    return wallets


def lp_global_candidate_wallets(output_periods, liquidity_wallets=None):
    """Collect bounded LP candidates across every period and the LP artifact."""
    row_groups = []
    for period_data in (output_periods or {}).values():
        if not isinstance(period_data, dict):
            continue
        for chain_data in period_data.values():
            if not isinstance(chain_data, dict):
                continue
            for key in (
                "accumulators",
                "sellers",
                "search_accumulators",
                "search_sellers",
            ):
                row_groups.append(chain_data.get(key) or [])
    return lp_period_scan_wallets(row_groups) | {
        address
        for address in (
            str(value or "").strip().lower() for value in (liquidity_wallets or set())
        )
        if re.fullmatch(r"0x[a-f0-9]{40}", address)
    }


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
    holder_cutoff_blocks, holder_cutoff_meta = load_holder_history_cutoff_blocks(
        state, points, current_blocks
    )
    save_state(state)
    dolomite_history, dolomite_history_meta = load_holder_dolomite_history_snapshots(
        state,
        points,
        current_blocks,
        base_ts,
        cutoff_blocks_by_point=holder_cutoff_blocks,
    )
    dolomite_history_meta["cutoff"] = holder_cutoff_meta
    current_dolomite_balances = output.get("dolomite_balances") or {}
    current_dolomite_meta = output.get("dolomite_balance_meta") or {}
    if current_dolomite_meta.get("status") != "complete":
        raise RuntimeError("Current cross-chain Dolomite DOLO snapshot is incomplete")
    chart_points = [
        *points,
        {"key": "now", "timestamp": raw_timestamp, "ts": base_ts},
    ]
    vesting_investors = extract_vesting_investors(all_transfers)
    holder_bucket_history, holder_wallet_history = calculate_holder_bucket_history(
        all_transfers,
        chart_points,
        current_blocks,
        base_ts,
        vesting_investors,
        dolomite_history=dolomite_history,
        current_dolomite_balances=current_dolomite_balances,
        cutoff_blocks_by_point=holder_cutoff_blocks,
    )

    output["holder_history_points"] = [
        {"key": point["key"], "timestamp": point["timestamp"]}
        for point in chart_points
    ]
    output["holder_bucket_history"] = holder_bucket_history
    output["cex_supply_history"] = calculate_cex_supply_history(
        all_transfers, points, current_blocks, base_ts, vesting_investors,
        cutoff_blocks_by_point=holder_cutoff_blocks,
    )
    output["holder_history_schema"] = "audience-exposure-v3"
    output["holder_dolomite_history_meta"] = dolomite_history_meta
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
    force_full_verified_backfill = (
        "--full-verified-backfill" in sys.argv[1:]
        or os.environ.get("DOLO_FLOWS_FULL_VERIFIED_BACKFILL", "").lower()
        in {"1", "true", "yes"}
    )
    print("=" * 60)
    print("🔄 DOLO Token Flows — Top Accumulators & Sellers")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load incremental state
    global _global_state
    state = load_state()
    _global_state = state  # Allow signal handler to save on kill
    is_incremental = bool(state)
    resumable_verified_chains = (
        completed_verified_backfill_chains(state)
        if force_full_verified_backfill
        else set()
    )
    if force_full_verified_backfill:
        prior_chain_integrity = dict(
            ((state.get("flow_log_integrity") or {}).get("chains") or {})
        )
        state["flow_log_integrity"] = {
            "version": FLOW_LOG_INTEGRITY_VERSION,
            "status": "building",
            "verification": "independent-rpc-exact-quorum",
            "unresolvedGapCount": 0,
            "chains": {
                chain_key: prior_chain_integrity[chain_key]
                for chain_key in resumable_verified_chains
            },
        }
        for chain_key in CHAINS:
            state[f"skipped_ranges_{chain_key}"] = []
        print("🔐 Full verified backfill requested — active transfer cache will be replaced only after quorum")
        if resumable_verified_chains:
            labels = ", ".join(
                CHAINS[chain_key]["name"]
                for chain_key in sorted(resumable_verified_chains)
            )
            print(
                "♻️ Reusing completed verified chain baseline(s): "
                f"{labels}; only the confirmed recent overlap will be refreshed"
            )
    elif is_incremental and not has_complete_verified_baseline(state):
        raise RuntimeError(
            "Existing DOLO flow cache predates independent RPC verification; "
            "run with --full-verified-backfill before publishing again"
        )
    elif is_incremental:
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
        previous_last_block = (
            0
            if force_full_verified_backfill
            else int(state.get(f"{chain_key}_last_block") or 0)
        )
        buffered = validated_scan_end(
            chain_key,
            blk,
            REORG_BUFFER_BLOCKS.get(chain_key, 10),
            previous_last_block,
        )
        current_blocks[chain_key] = buffered
        print(f"  {cfg['name']}: block {blk:,} (scanning to {buffered:,}, reorg buffer)")

    # Resolve every period from exact block timestamps. Average block-time
    # arithmetic drifts materially on longer Ethereum windows.
    cutoff_blocks, period_boundaries = calculate_exact_period_cutoffs(current_blocks)
    for chain_key, cfg in CHAINS.items():
        print(
            f"  {cfg['name']}: exact 180D boundary "
            f"{cutoff_blocks[chain_key]['180d']:,} "
            f"({datetime.utcfromtimestamp(period_boundaries[chain_key]['180d']['startTimestamp']).isoformat()}Z)"
        )

    # Determine the oldest block we need per chain (longest period cutoff)
    max_period = max(PERIODS.keys(), key=lambda k: PERIODS[k])

    # Fetch transfers — incremental: only new blocks since last run
    print("\n📡 Fetching Transfer events...")
    all_transfers = {}
    unresolved_history_gaps = {}
    for chain_key in CHAINS:
        oldest_needed = cutoff_blocks[chain_key][max_period]
        end = current_blocks[chain_key]
        verified_refresh_start = None

        # Load cached transfers for this chain
        cached_key = f"{chain_key}_transfers"
        last_block_key = f"{chain_key}_last_block"
        history_start_key = f"{chain_key}_history_start_block"
        rebuild_chain_from_deploy = (
            force_full_verified_backfill
            and chain_key not in resumable_verified_chains
        )
        cached_transfers = (
            [] if rebuild_chain_from_deploy else state.get(cached_key, [])
        )
        last_block = (
            0 if rebuild_chain_from_deploy else state.get(last_block_key, 0)
        )
        history_coverage_start = None
        full_baseline = rebuild_chain_from_deploy or not (
            last_block > 0 and cached_transfers
        )

        if (
            not rebuild_chain_from_deploy
            and is_incremental
            and last_block > 0
            and cached_transfers
        ):
            # Replace a recent overlap authoritatively on every run. The old
            # `last_block + 1` path could silently miss a single block when
            # fetch_start equaled the buffered chain tip.
            fetch_start = incremental_refresh_start(
                last_block,
                oldest_needed,
                RECENT_RESCAN_BLOCKS.get(chain_key, 0),
            )
            verified_refresh_start = fetch_start
            restored = [tuple(t) for t in cached_transfers]
            cached_min_block = min((t[3] for t in restored), default=last_block + 1)
            coverage_min_block = int(state.get(history_start_key) or cached_min_block)
            backfill_transfers = []
            backfill_failed = 0
            if coverage_min_block > oldest_needed:
                backfill_end = coverage_min_block - 1
                print(f"  {CHAINS[chain_key]['name']}: backfilling All history blocks {oldest_needed:,} → {backfill_end:,}")
                backfill_transfers, backfill_failed, _ = fetch_transfer_logs(
                    chain_key, oldest_needed, backfill_end, state=state
                )
                if backfill_failed:
                    raise TransferLogQuorumError(
                        f"{CHAINS[chain_key]['name']}: historical backfill did not reach RPC quorum"
                    )
            history_coverage_start = oldest_needed
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
                if chunks_failed:
                    raise TransferLogQuorumError(
                        f"{CHAINS[chain_key]['name']}: recent refresh did not reach RPC quorum"
                    )

            # Merge: historical backfill + authoritative recent replacement.
            merged = backfill_transfers + merge_verified_transfer_scan(
                restored,
                new_transfers,
                fetch_start,
                end,
                failed_chunks=chunks_failed,
            )

            # Prune: drop transfers from blocks older than the oldest needed
            merged = [t for t in merged if t[3] >= oldest_needed]

            all_transfers[chain_key] = merged
            print(f"  {CHAINS[chain_key]['name']}: {len(backfill_transfers):,} backfilled + {len(new_transfers):,} new + {len(restored):,} cached → {len(merged):,} total (after pruning)")
        else:
            # Full scan from the oldest needed block (or resume from cached last_block)
            scan_start = oldest_needed
            verified_refresh_start = scan_start
            cached_as_lists = None
            if last_block > 0 and last_block > oldest_needed:
                # Resume from where we left off (partial previous scan)
                scan_start = last_block + 1
                cached_as_lists = cached_transfers  # already lists from JSON
                print(f"  {CHAINS[chain_key]['name']}: resuming partial scan from block {scan_start:,} (had {len(cached_transfers):,} cached txs)")

            fresh_transfers, chunks_failed, total_chunks = fetch_transfer_logs(
                chain_key, scan_start, end, state=state, cached_transfers_so_far=cached_as_lists
            )
            if chunks_failed:
                raise TransferLogQuorumError(
                    f"{CHAINS[chain_key]['name']}: full scan did not reach RPC quorum"
                )
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
        if unresolved_gaps:
            require_complete_flow_history({chain_key: unresolved_gaps})
        if repaired_count:
            print(
                f"  🩹 {CHAINS[chain_key]['name']}: restored "
                f"{repaired_count:,} transfer(s) from skipped ranges"
            )

        metadata_start = cutoff_blocks[chain_key]["30d"]
        before_metadata_rows = all_transfers[chain_key]
        all_transfers[chain_key] = ensure_transfer_metadata_coverage(
            chain_key,
            before_metadata_rows,
            state,
            metadata_start,
            end,
            verified_refresh_start=verified_refresh_start,
        )
        if all_transfers[chain_key] is not before_metadata_rows:
            print(
                f"  🧾 {CHAINS[chain_key]['name']}: transaction metadata "
                f"verified for 30D blocks {metadata_start:,} → {end:,}"
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
        mark_verified_chain_coverage(
            state,
            chain_key,
            oldest_needed if full_baseline else fetch_start,
            end,
            full_baseline=full_baseline,
        )
        state.get("verified_scan_staging", {}).pop(chain_key, None)
        # Save state after each chain completes
        save_state(state)
        print(f"  💾 State saved for {CHAINS[chain_key]['name']} (up to block {end:,})")

    require_complete_flow_history(unresolved_history_gaps)

    # ERC-20 logs stop at Dolomite custody. Fetch every internal trade at the
    # exact same chain head so a later Zap/Trade of deposited DOLO is reflected
    # in the beneficial-owner flow leaderboard. Partial coverage would bias a
    # direction, so generation fails closed instead of publishing stale math.
    print("\n🔄 Fetching Dolomite-internal DOLO trades...")
    dolomite_trades_by_chain = {}
    dolomite_trade_meta = {
        "status": "complete",
        "source": "official-dolomite-subgraph-pinned-block",
        "chains": {},
    }
    for chain_key, cfg in CHAINS.items():
        rows, metadata = fetch_dolomite_dolo_trades(
            chain_key,
            current_blocks[chain_key],
            block_timestamp=period_boundaries[chain_key]["all"]["endTimestamp"],
        )
        if metadata.get("status") != "complete":
            raise RuntimeError(
                f"{cfg['name']} Dolomite DOLO trade scan is incomplete: "
                f"{metadata.get('error') or 'unknown error'}"
            )
        dolomite_trades_by_chain[chain_key] = rows
        dolomite_trade_meta["chains"][chain_key] = metadata
        print(f"  {cfg['name']}: {len(rows):,} exact trade event(s)")

    # Detect contracts among top addresses (to exclude DEX routers, etc.)
    print("\n🔍 Detecting contract addresses to exclude...")
    address_labels = load_address_labels()
    holder_rows = load_current_holder_rows()
    verified_user_contracts = verified_user_contract_addresses(holder_rows)
    # Collect all unique addresses from transfers
    for chain_key in CHAINS:
        addr_set = set()
        for from_addr, to_addr, _, _, *_ in all_transfers[chain_key]:
            addr_set.add(from_addr)
            addr_set.add(to_addr)

        # Check top net-flow actors plus high-volume direct custody
        # counterparties. The second group catches balanced routers whose net
        # flow is close to zero without scanning every depositor via RPC.
        flows = calculate_flows(all_transfers[chain_key], EXCLUDED_ADDRS)
        addrs_to_check = contract_detection_candidates(
            all_transfers[chain_key],
            chain_key,
            flows,
            top_n=30,
        )

        contracts = detect_contracts_batch(addrs_to_check, chain_key)
        dynamic_exclusions = select_dynamic_flow_exclusions(
            contracts,
            address_labels,
            verified_user_contracts,
        )
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
    flow_metadata_cache = {"eth": {}, "bera": {}}
    lp_receipt_cache = {"eth": {}, "bera": {}}
    liquidity_registry = load_liquidity_registry()
    bridge_neutralization_audit = {}
    period_transfers_cache = {}
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
        period_transfers_cache[period] = period_transfers_by_chain

        # Step 2: Inject bridge mint/burn flows for cross-chain detection
        # Bridge mints (from 0x0) and burns (to 0x0) are skipped by calculate_flows()
        # but needed for neutralization to detect opposing cross-chain patterns.
        # We add them as supplementary flows that only affect neutralization.
        bridge_flows_by_chain = {}
        adapter_outflows_by_chain = {}
        for chain_key in CHAINS:
            bridge_flows_by_chain[chain_key] = calculate_bridge_flows(
                period_transfers_by_chain[chain_key]
            )
            adapter_outflows_by_chain[chain_key] = calculate_bridge_adapter_outflows(
                period_transfers_by_chain[chain_key]
            )

        # Step 3: Remove canonical bridge legs only when the same wallet receives
        # the matching destination mint. Cross-wallet bridges remain real flows.
        neutralized, bridge_audit, bridge_cancellations = (
            neutralize_raw_and_bridge_flows_with_audit(
                raw_flows,
                bridge_flows_by_chain,
                adapter_outflows_by_chain,
            )
        )
        bridge_neutralization_audit[period] = bridge_audit
        n_count = bridge_audit["total"]["addressCount"]
        n_volume = bridge_audit["total"]["dolo"]
        flow_components_by_chain = apply_bridge_outflow_cancellations(
            flow_components_by_chain,
            bridge_cancellations,
        )

        # Keep raw bridge-neutralized flows for balance-history reconstruction,
        # but remove wallet↔Dolomite custody legs from the market-behavior
        # leaderboards. Depositing collateral is not selling; withdrawing from
        # custody is not a market purchase. Then apply DOLO changes caused by
        # internal Dolomite trades, which have no wallet-level ERC-20 leg.
        market_flows_by_chain = {}
        dolomite_trade_adjustments_by_chain = {}
        for chain_key in CHAINS:
            market_flows, market_components = neutralize_protocol_custody_transfers(
                neutralized[chain_key],
                flow_components_by_chain[chain_key],
                period_transfers_by_chain[chain_key],
                chain_key,
            )
            trade_adjustments = calculate_dolomite_trade_adjustments(
                dolomite_trades_by_chain[chain_key],
                cutoff_block=cutoff_blocks[chain_key][period],
            )
            market_flows, market_components = apply_dolomite_trade_adjustments(
                market_flows,
                market_components,
                trade_adjustments,
            )
            for address, summary in trade_adjustments.items():
                tx_counts_by_chain[chain_key][address] = (
                    tx_counts_by_chain[chain_key].get(address, 0)
                    + int(summary.get("tx_count") or 0)
                )
            market_flows_by_chain[chain_key] = market_flows
            flow_components_by_chain[chain_key] = market_components
            dolomite_trade_adjustments_by_chain[chain_key] = trade_adjustments
        
        neutralized_flows_cache[period] = neutralized
        if n_count > 0:
            print(f"  🔀 {period}: neutralized {n_count} cross-chain bridge transfers ({n_volume:,.0f} DOLO)")

        # Step 4: Build output using neutralized flows. Exact transaction metadata
        # is optional presentation provenance and never participates in arithmetic.
        flow_evidence_loaders = {}
        lp_receipt_loaders = {}
        for chain_key, cfg in CHAINS.items():
            tx_counts = tx_counts_by_chain[chain_key]

            def load_flow_evidence(blocks, _chain_key=chain_key, _cfg=cfg):
                cache = flow_metadata_cache[_chain_key]
                missing = set(blocks) - set(cache)
                if missing:
                    cache.update(fetch_token_block_evidence(
                        _cfg["rpcs"], DOLO_CONTRACT, missing, rpc_batch_requests,
                        retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                        batch_size=RPC_BATCH_SIZE,
                        describe=f"{_cfg['name']} DOLO flow transaction metadata",
                    ))
                return {block: cache[block] for block in blocks if block in cache}

            def load_lp_receipts(tx_hashes, _chain_key=chain_key, _cfg=cfg):
                cache = lp_receipt_cache[_chain_key]
                missing = set(tx_hashes) - set(cache)
                if missing:
                    cache.update(fetch_transaction_receipts(
                        _cfg["rpcs"], missing, rpc_batch_requests,
                        retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                        batch_size=RPC_BATCH_SIZE,
                        describe=f"{_cfg['name']} verified LP flow evidence",
                    ))
                return {tx_hash: cache[tx_hash] for tx_hash in tx_hashes if tx_hash in cache}

            flow_evidence_loaders[chain_key] = load_flow_evidence
            lp_receipt_loaders[chain_key] = load_lp_receipts

            chain_name = "ethereum" if chain_key == "eth" else "berachain"
            # Net flow must remain the exact transfer-derived wallet balance
            # change. LP receipts are presentation evidence for rows that are
            # already ranked; they must never promote a flat rebalance into a
            # synthetic outflow.
            flows = dict(market_flows_by_chain[chain_key])
            lp_annotations = {}

            search_accumulators, search_sellers = build_searchable_flow_rows(
                flows,
                flow_components_by_chain[chain_key],
                tx_counts,
                EXCLUDED_ADDRS,
            )
            attach_dolomite_trade_metadata(
                search_accumulators,
                dolomite_trade_adjustments_by_chain[chain_key],
                "inbound",
                chain_name,
            )
            attach_dolomite_trade_metadata(
                search_sellers,
                dolomite_trade_adjustments_by_chain[chain_key],
                "outbound",
                chain_name,
            )
            apply_flow_annotations(search_accumulators, lp_annotations)
            apply_flow_annotations(search_sellers, lp_annotations)
            accumulators = search_accumulators[:TOP_N]
            sellers = search_sellers[:TOP_N]

            chain_name = "ethereum" if chain_key == "eth" else "berachain"
            attach_latest_flow_metadata(
                accumulators, period_transfers_by_chain[chain_key], "inbound", chain_name, load_flow_evidence,
            )
            attach_latest_flow_metadata(
                sellers, period_transfers_by_chain[chain_key], "outbound", chain_name, load_flow_evidence,
            )
            # Transfer metadata may describe the earlier custody deposit. A
            # newer internal Zap/Trade is the true latest directional action.
            attach_dolomite_trade_metadata(
                accumulators,
                dolomite_trade_adjustments_by_chain[chain_key],
                "inbound",
                chain_name,
            )
            attach_dolomite_trade_metadata(
                sellers,
                dolomite_trade_adjustments_by_chain[chain_key],
                "outbound",
                chain_name,
            )

            attach_latest_lp_metadata(
                accumulators, chain_name, liquidity_registry, DOLO_CONTRACT, load_lp_receipts,
            )
            attach_latest_lp_metadata(
                sellers, chain_name, liquidity_registry, DOLO_CONTRACT, load_lp_receipts,
            )
            apply_flow_annotations(accumulators, lp_annotations)
            apply_flow_annotations(sellers, lp_annotations)

            # Add USD values
            if dolo_price:
                for entry in accumulators + sellers:
                    entry["usd_value"] = round(entry["net_flow"] * dolo_price, 2)

            output_periods[period][chain_key] = {
                "accumulators": accumulators,
                "sellers": sellers,
                "search_accumulators": search_accumulators,
                "search_sellers": search_sellers,
                "total_transfers": len(period_transfers_by_chain[chain_key]),
            }

            print(f"  {period} {cfg['name']}: {len(period_transfers_by_chain[chain_key]):,} transfers, "
                  f"top accumulator: {accumulators[0]['net_flow']:,.0f} DOLO" if accumulators else
                  f"  {period} {cfg['name']}: no data")

        # Rank only after merging the complete market maps. Merging two already
        # truncated Top 100 lists drops material opposite legs and can emit a
        # false non-zero combined result for an address whose true net is zero.
        combined_flows = merge_chain_flow_maps(market_flows_by_chain)
        combined_lp_annotations = {}
        combined_components = merge_chain_flow_components(flow_components_by_chain)
        combined_counts = merge_chain_counts(tx_counts_by_chain)
        combined_search_accumulators, combined_search_sellers = build_searchable_flow_rows(
            combined_flows,
            combined_components,
            combined_counts,
            EXCLUDED_ADDRS,
        )
        apply_flow_annotations(combined_search_accumulators, combined_lp_annotations)
        apply_flow_annotations(combined_search_sellers, combined_lp_annotations)
        combined_accumulators = combined_search_accumulators[:TOP_N]
        combined_sellers = combined_search_sellers[:TOP_N]

        # Reuse exact per-chain presentation evidence when the combined row is
        # also present in a chain ranking. Evidence remains optional and never
        # participates in flow arithmetic.
        chain_evidence = {}
        for chain_key in CHAINS:
            chain_data = output_periods[period][chain_key]
            for row in chain_data["search_accumulators"] + chain_data["search_sellers"]:
                timestamp = int(row.get("latest_tx_timestamp") or 0)
                prior = chain_evidence.get(row["address"])
                if prior is None or timestamp > int(prior.get("latest_tx_timestamp") or 0):
                    chain_evidence[row["address"]] = row
        for entry in combined_search_accumulators + combined_search_sellers:
            evidence = chain_evidence.get(entry["address"])
            if evidence:
                for key in (
                    "latest_tx_hash",
                    "latest_tx_timestamp",
                    "latest_tx_chain",
                    "latest_lp_activity",
                ):
                    if key in evidence:
                        value = evidence[key]
                        # The combined row is enriched with all-chain LP totals
                        # later. Keep its activity metadata independent so that
                        # those totals cannot overwrite a single-chain row.
                        if key == "latest_lp_activity" and isinstance(value, dict):
                            value = dict(value)
                        entry[key] = value

        if dolo_price:
            for entry in combined_accumulators + combined_sellers:
                entry["usd_value"] = round(entry["net_flow"] * dolo_price, 2)

        apply_flow_annotations(combined_accumulators, combined_lp_annotations)
        apply_flow_annotations(combined_sellers, combined_lp_annotations)

        output_periods[period]["all"] = {
            "accumulators": combined_accumulators,
            "sellers": combined_sellers,
            "search_accumulators": combined_search_accumulators,
            "search_sellers": combined_search_sellers,
            "total_transfers": sum(
                len(rows) for rows in period_transfers_by_chain.values()
            ),
        }

    # Resolve LP evidence after every period has been ranked. A wallet that is
    # material in 90D/All (or is a verified current/historical LP owner) must
    # remain eligible for exact receipt attribution in a near-flat 30D window.
    # The bounded candidate set avoids scanning receipts for every address that
    # touched a shared v4 PoolManager.
    liquidity_provider_wallets = load_liquidity_provider_wallets()
    global_lp_wallets = lp_global_candidate_wallets(
        output_periods,
        liquidity_provider_wallets,
    )
    print(
        f"  🧩 LP attribution candidates: {len(global_lp_wallets):,} "
        f"({len(liquidity_provider_wallets):,} from liquidity index)"
    )
    for period in PERIODS:
        lp_activities_by_chain = {}
        for chain_key in CHAINS:
            chain_name = "ethereum" if chain_key == "eth" else "berachain"
            lp_activities = collect_verified_lp_activities(
                period_transfers_cache[period][chain_key],
                chain_name,
                liquidity_registry,
                DOLO_CONTRACT,
                flow_evidence_loaders[chain_key],
                lp_receipt_loaders[chain_key],
                wallet_filter=global_lp_wallets,
            )
            lp_activities_by_chain[chain_key] = lp_activities
            chain_rows = output_periods[period][chain_key]
            for key in (
                "accumulators",
                "sellers",
                "search_accumulators",
                "search_sellers",
            ):
                apply_period_lp_totals(chain_rows[key], lp_activities)

        combined_lp_activities = merge_verified_lp_activities(lp_activities_by_chain)
        combined_rows = output_periods[period]["all"]
        for key in (
            "accumulators",
            "sellers",
            "search_accumulators",
            "search_sellers",
        ):
            apply_period_lp_totals(combined_rows[key], combined_lp_activities)

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

    print("\n🏦 Fetching current DOLO positions inside Dolomite...")
    dolomite_balances, dolomite_balance_meta = fetch_dolomite_dolo_balances(None)
    if dolomite_balance_meta.get("status") == "complete":
        positioned_wallets = sum(
            1 for values in dolomite_balances.values()
            if float(values.get("total") or 0) > 0
        )
        print(f"  ✅ Current Dolomite DOLO found for {positioned_wallets} wallet(s)")
    else:
        print("  ⚠️ Current Dolomite DOLO is omitted because cross-chain coverage is incomplete")
        raise RuntimeError("Current cross-chain Dolomite DOLO snapshot is incomplete")

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
    holder_cutoff_blocks, holder_cutoff_meta = load_holder_history_cutoff_blocks(
        state, holder_history_points, current_blocks
    )
    save_state(state)
    holder_dolomite_history, holder_dolomite_history_meta = load_holder_dolomite_history_snapshots(
        state,
        holder_history_points,
        current_blocks,
        holder_history_base_ts,
        cutoff_blocks_by_point=holder_cutoff_blocks,
    )
    holder_dolomite_history_meta["cutoff"] = holder_cutoff_meta
    holder_chart_points = [
        *holder_history_points,
        {
            "key": "now",
            "timestamp": datetime.fromtimestamp(
                holder_history_base_ts, timezone.utc
            ).isoformat().replace("+00:00", "Z"),
            "ts": holder_history_base_ts,
        },
    ]
    holder_bucket_history, holder_wallet_history = calculate_holder_bucket_history(
        all_transfers,
        holder_chart_points,
        current_blocks,
        holder_history_base_ts,
        vesting_investors,
        dolomite_history=holder_dolomite_history,
        current_dolomite_balances=dolomite_balances,
        cutoff_blocks_by_point=holder_cutoff_blocks,
    )
    cex_supply_history = calculate_cex_supply_history(
        all_transfers,
        holder_history_points,
        current_blocks,
        holder_history_base_ts,
        vesting_investors,
        cutoff_blocks_by_point=holder_cutoff_blocks,
    )
    print(f"  ... {len(holder_chart_points)}/{len(holder_chart_points)} holder history points")

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
        "schemaVersion": 6,
        "timestamp": flow_snapshot_timestamp(period_boundaries),
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "tracked_flow_chains": ["ethereum", "berachain"],
        "period_boundaries": period_boundaries,
        "bridge_neutralization_audit": bridge_neutralization_audit,
        "dolomite_trade_meta": dolomite_trade_meta,
        "holder_history_start_timestamp": datetime.utcfromtimestamp(HOLDER_HISTORY_START_TIMESTAMP).isoformat() + "Z",
        "holder_history_points": [
            {"key": point["key"], "timestamp": point["timestamp"]}
            for point in holder_chart_points
        ],
        "holder_bucket_history": holder_bucket_history,
        "holder_history_schema": "audience-exposure-v3",
        "holder_dolomite_history_meta": holder_dolomite_history_meta,
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
        "flow_history_integrity": state.get("flow_log_integrity", {}),
        "dolomite_balance_meta": dolomite_balance_meta,
        "dolomite_balances": dolomite_balances,
        "dolo_price": dolo_price,
        "periods": output_periods,
        "balance_changes": balance_changes,
        "fresh_holders": fresh_holders,
        "fresh_holders_meta": {
            "source": "full_transfer_history_plus_multichain_explorer_first_tx_plus_debank_age_fallback",
            "definition": "user-controlled wallet first normal on-chain transaction across tracked EVM activity sources within the selected period, plus required conservative DeBank wallet age fallback/cross-check for fresh candidates, with current liquid DOLO plus veDOLO locked exposure above 10K DOLO",
            "includedWalletTypes": sorted(FRESH_USER_WALLET_TYPES),
            "confirmedNonUserTypesExcluded": ["bot", "ca", "cex", "investor", "liquidator", "mm", "protocol", "team", "trader", "watch"],
            "automationExclusion": {
                "minimumInboundTransfers": FRESH_AUTOMATION_MIN_INBOUND_TRANSFERS,
                "minimumOutboundTransfers": FRESH_AUTOMATION_MIN_OUTBOUND_TRANSFERS,
                "minimumGrossDoloPerSide": FRESH_AUTOMATION_MIN_SIDE_DOLO,
                "maximumNetShareBps": FRESH_AUTOMATION_MAX_NET_SHARE_BPS,
                "classification": "cohort-only-high-confidence-automation; no permanent wallet label",
            },
            "walletActivitySource": "Etherscan v2 txlist + Routescan Berachain txlist + DeBank rendered wallet age fallback",
            "walletAgeBoundaryPolicy": "DeBank rendered age labels are treated as ranges; a fallback wallet is included only when the upper bound fits inside the selected period.",
            "activityChains": [source["name"] for source in FRESH_WALLET_ACTIVITY_SOURCES],
            "activityChainKeys": [source["key"] for source in FRESH_WALLET_ACTIVITY_SOURCES],
            "fallbackSources": ["DeBank rendered wallet age"],
            "directEntityChecks": [
                "DeBank explicit is-cex owner badge; funding relationships ignored"
            ],
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
        f"  🧑‍💼 Saved {len(vesting_investors.get('strategic_investors', []))} strategic investors "
        f"and {len(vesting_investors.get('investors', []))} long-term investors to vesting_investors.json"
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
