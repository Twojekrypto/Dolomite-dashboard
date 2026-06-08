#!/usr/bin/env python3
"""
DOLO Token Flows — Top Accumulators & Sellers (1d / 7d / 30d)
Fetches ERC-20 Transfer events via eth_getLogs for ETH and Berachain,
calculates net inflow/outflow per address, outputs top 5 each.
"""
import json, time, os, sys, signal, re, shutil, subprocess
import requests
from datetime import datetime, timedelta, timezone

ALCHEMY_BERA_RPC = os.environ.get("ALCHEMY_BERACHAIN_RPC", "")
ALCHEMY_BERA_RPC_2 = os.environ.get("ALCHEMY_BERACHAIN_RPC_2", "")
ALCHEMY_BERA_RPC_3 = os.environ.get("ALCHEMY_BERACHAIN_RPC_3", "")
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "").strip()
BERASCAN_API_KEY = os.environ.get("BERASCAN_API_KEY", "").strip()

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
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
        "rpcs": [
            "https://eth.drpc.org/",
            "https://ethereum-rpc.publicnode.com/",
            "https://rpc.ankr.com/eth",
        ],
        "block_time": 12,   # ~12 seconds per block
        "chunk_size": 50_000,
        "deploy_block": 21_500_000,  # DOLO deployed ~Jan 2025
    },
    "bera": {
        "name": "Berachain",
        "rpcs": [
            *([] if not ALCHEMY_BERA_RPC else [ALCHEMY_BERA_RPC]),
            *([] if not ALCHEMY_BERA_RPC_2 else [ALCHEMY_BERA_RPC_2]),
            *([] if not ALCHEMY_BERA_RPC_3 else [ALCHEMY_BERA_RPC_3]),
            "https://berachain-rpc.publicnode.com/",
            "https://berachain.drpc.org/",
            "https://rpc.berachain.com/",
        ],
        "block_time": 2,    # ~2 seconds per block
        "chunk_size": 100_000,  # Berachain: keep ranges small enough to avoid dropped log chunks
        "deploy_block": 2_900_000,   # DOLO deployed on Berachain ~block 2,925,727 (Mar 2025)
    },
}

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
SAFE_SINGLETON_ADDRS = {
    "0x41675c099f32341bf84bfc5382af534df5c7461a",  # Safe 1.4.1
    "0xd9db270c1b5e3bd161e8c8503c55ceabee709552",  # Gnosis Safe 1.3.0
    "0x3e5c63644e683549055b9be8653de26e0b4cd36e",  # Gnosis Safe 1.1.1
    "0x29fcb43b46531bca003ddc8fcb67ffe91900c762",  # Gnosis Safe L2
}
USER_CONTRACT_WALLET_ADDRS = {
    "0xbabcc964619cf5c8a57f2b989a35cd887e8ce739",  # User Safe/multisig DOLO holder
}

HOLDER_HISTORY_START_TIMESTAMP = int(datetime(2025, 4, 24, tzinfo=timezone.utc).timestamp())  # DOLO TGE

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "dolo_flows.json")
STATE_FILE = os.path.join(DATA_DIR, "dolo_flows_state.json")

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
        except Exception:
            pass
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


def fetch_transfer_logs(chain_key, start_block, end_block, state=None, cached_transfers_so_far=None):
    """Fetch ERC-20 Transfer event logs via eth_getLogs.
    Saves state progressively during long scans so timeout kills preserve progress."""
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    chunk_size = cfg["chunk_size"]

    if start_block >= end_block:
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

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        success = False
        for attempt in range(len(rpcs) * 2):
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
            except Exception:
                time.sleep(0.5)

        if not success:
            if chunk_size > 1000:
                chunk_size = max(chunk_size // 2, 1000)
                print(f"    ⚠️ Retrying block {current:,} with smaller chunk ({chunk_size:,} blocks)")
                continue
            chunks_failed += 1
            print(f"    ⚠️ Failed at block {current}, skipping chunk ({chunks_failed} failures so far)")
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
        if fail_pct > 50:
            print(f"  🚨 {cfg['name']}: >50% chunk failure rate! Data may be incomplete.")

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


def detect_contracts_batch(addresses, chain_key):
    """Detect which addresses are contracts using eth_getCode."""
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    contracts = set()

    for addr in addresses:
        for rpc in rpcs:
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_getCode",
                    "params": [addr, "latest"], "id": 1
                }, timeout=5, headers={"Content-Type": "application/json"})
                code = resp.json().get("result", "0x")
                if code and len(code) > 4:
                    contracts.add(addr)
                break
            except Exception:
                time.sleep(0.3)
        time.sleep(0.03)

    return contracts


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
        return flows_by_chain, 0
    
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


def calculate_holder_bucket_history(all_transfers, points, current_blocks, base_ts, vesting_labels=None):
    holder_rows = load_current_holder_rows()
    address_labels = load_address_labels(vesting_labels)
    current_liquid = {
        addr: float(row.get("balance") or 0)
        for addr, row in holder_rows.items()
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

        locked_balances = locked_map_at_holder_point(point["ts"], current_locks, vedolo_events)
        row = {
            "key": point["key"],
            "timestamp": point["timestamp"],
            "liquid": {},
            "with_vedolo": {},
        }
        for view, bucket_defs in HOLDER_BUCKET_GROUPS.items():
            row["liquid"][view] = build_bucket_model(
                liquid_balances, {}, holder_rows, address_labels, bucket_defs
            )
            row["with_vedolo"][view] = build_bucket_model(
                liquid_balances, locked_balances, holder_rows, address_labels, bucket_defs
            )
        history.append(row)

    return sorted(history, key=lambda row: row["timestamp"])


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
    vesting_ca = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
    investor_ca = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"

    early_set = set()
    inv_set = set()
    team_set = set()

    for chain_key in ["eth", "bera"]:
        for transfer in all_transfers.get(chain_key, []):
            from_addr = str(transfer[0] or "").lower()
            to_addr = str(transfer[1] or "").lower()
            if not to_addr:
                continue
            if from_addr == vesting_ca:
                early_set.add(to_addr)
            elif from_addr == investor_ca:
                inv_set.add(to_addr)
                team_set.add(to_addr)

    return {
        "early_investors": sorted(early_set),
        "investors": sorted(inv_set),
        "team": sorted(team_set),
    }


def merge_vesting_labels(labels, vesting_data):
    if not isinstance(vesting_data, dict):
        return labels
    for key, label, label_type in [
        ("early_investors", "Early Investor", "investor"),
        ("investors", "Investor", "investor"),
        ("team", "Core Team", "protocol"),
    ]:
        for addr in vesting_data.get(key, []) or []:
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


def _fresh_activity_within_period(first_activity, period, base_ts):
    if first_activity.get("source") == "debank_age":
        age_max_days = first_activity.get("debank_age_max_days")
        if age_max_days is None:
            _normalize_cached_debank_age(first_activity)
            age_max_days = first_activity.get("debank_age_max_days")
        if age_max_days is not None:
            return float(age_max_days) <= (PERIODS[period] / 86400)
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
                "first_chain": first_activity.get("chain", first_chain),
                "first_block": int(first_activity.get("first_block") or 0),
                "first_timestamp_estimate": datetime.utcfromtimestamp(max(0, wallet_created_ts)).isoformat() + "Z",
                "wallet_created_chain": first_activity.get("chain", ""),
                "wallet_created_block": int(first_activity.get("first_block") or 0),
                "wallet_created_tx": first_activity.get("first_tx", ""),
                "wallet_created_timestamp": datetime.utcfromtimestamp(max(0, wallet_created_ts)).isoformat() + "Z",
                "wallet_created_source": first_activity.get("source", "normal_tx"),
                "verification_source": first_activity.get("source", "normal_tx"),
                "wallet_age_days": first_activity.get("debank_age_days"),
                "wallet_age_max_days": first_activity.get("debank_age_max_days"),
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
    return {addr: value for addr, value in locked.items() if value > 0.0001}


def empty_bucket_model(bucket_defs):
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
        "buckets": buckets,
        "trackedWallets": 0,
        "trackedTotal": 0,
        "trackedLiquid": 0,
        "trackedLocked": 0,
        "excludedCexWallets": 0,
        "excludedCexTotal": 0,
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


def build_bucket_model(liquid_balances, locked_balances, holder_rows, address_labels, bucket_defs, include_allocations=False):
    model = empty_bucket_model(bucket_defs)
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
        if is_allocation and not include_allocations:
            model["excludedInsiderWallets"] += 1
            model["excludedInsiderTotal"] += total
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
        "excludedInsiderTotal", "allocationTotal", "allocationLiquid",
        "allocationLocked", "teamTotal", "investorTotal",
    ]:
        model[key] = round(model[key], 2)
    return model


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
    if mode == "accumulator":
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1], reverse=True)
        filtered = [(addr, val) for addr, val in sorted_addrs if val > 0 and addr not in excluded]
    else:
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1])
        filtered = [(addr, abs(val)) for addr, val in sorted_addrs if val < 0 and addr not in excluded]

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
    except Exception:
        pass

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


def main():
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

    # Get current blocks for each chain
    print("\n📡 Getting current block numbers...")
    current_blocks = {}
    for chain_key, cfg in CHAINS.items():
        blk = get_current_block(cfg["rpcs"])
        current_blocks[chain_key] = blk
        print(f"  {cfg['name']}: block {blk:,}")

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
            # Only fetch new blocks since last run
            fetch_start = last_block + 1
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
            if fetch_start >= end:
                print(f"  {CHAINS[chain_key]['name']}: already up to date (block {last_block:,})")
                new_transfers = []
                chunks_failed = 0
            else:
                # Convert cached for progressive save during fetch
                cached_as_lists = [list(t) for t in cached_transfers] if isinstance(cached_transfers[0], (list, tuple)) else cached_transfers
                new_transfers, chunks_failed, _ = fetch_transfer_logs(
                    chain_key, fetch_start, end, state=state, cached_transfers_so_far=cached_as_lists
                )

            # Merge: cached + new
            merged = backfill_transfers + restored + new_transfers

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

    # Detect contracts among top addresses (to exclude DEX routers, etc.)
    print("\n🔍 Detecting contract addresses to exclude...")
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
        EXCLUDED_ADDRS.update(contracts)
        print(f"  {CHAINS[chain_key]['name']}: excluded {len(contracts)} contract(s)")

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
        period_transfers_by_chain = {}
        tx_counts_by_chain = {}
        for chain_key in CHAINS:
            cutoff = cutoff_blocks[chain_key][period]
            period_transfers = [t for t in all_transfers[chain_key] if t[3] >= cutoff]
            period_transfers_by_chain[chain_key] = period_transfers
            raw_flows[chain_key] = calculate_flows(period_transfers, EXCLUDED_ADDRS)
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
    balances = {}
    bal_selector = "0x70a08231"  # balanceOf(address)
    bal_failures = 0
    bal_failed_addrs = set()  # Track which addresses had RPC failures (for fallback)
    for i, addr in enumerate(all_addrs):
        padded = addr.replace("0x", "").lower().zfill(64)
        total_bal = 0
        for chain_key, cfg in CHAINS.items():
            chain_bal = 0
            got_balance = False
            for attempt in range(3):  # 3 retry attempts
                for rpc in cfg["rpcs"]:
                    try:
                        resp = requests.post(rpc, json={
                            "jsonrpc": "2.0", "method": "eth_call",
                            "params": [{"to": DOLO_CONTRACT, "data": bal_selector + padded}, "latest"],
                            "id": 1
                        }, timeout=10, headers={"Content-Type": "application/json"})
                        r = resp.json()
                        if "error" in r:
                            continue
                        result = r.get("result", "0x0")
                        chain_bal = int(result, 16) / (10 ** 18) if result and result != "0x" else 0
                        got_balance = True
                        break
                    except Exception:
                        time.sleep(0.3)
                if got_balance:
                    break
                time.sleep(0.5)  # backoff between retry attempts
            if not got_balance:
                bal_failures += 1
                bal_failed_addrs.add(addr)
            total_bal += chain_bal
        balances[addr] = round(total_bal, 2)
        time.sleep(0.05)
        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{len(all_addrs)} balances fetched")
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

    # Extra holder-bucket chart history. These are still derived from the same
    # transfer logs as the flow tables, but add enough cutoffs for a usable hover.
    holder_history_points = build_holder_history_schedule(holder_history_base_ts)
    print(f"\n📈 Building holder bucket chart history ({len(holder_history_points)} points)...")
    holder_bucket_history = calculate_holder_bucket_history(
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
        "cex_supply_history": cex_supply_history,
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
