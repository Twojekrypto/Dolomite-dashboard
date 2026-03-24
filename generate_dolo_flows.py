#!/usr/bin/env python3
"""
DOLO Token Flows — Top Accumulators & Sellers (1d / 7d / 30d)
Fetches ERC-20 Transfer events via eth_getLogs for ETH and Berachain,
calculates net inflow/outflow per address, outputs top 5 each.
"""
import json, time, os, sys, signal
import requests
from datetime import datetime

ALCHEMY_BERA_RPC = os.environ.get("ALCHEMY_BERACHAIN_RPC", "")
ALCHEMY_BERA_RPC_2 = os.environ.get("ALCHEMY_BERACHAIN_RPC_2", "")
ALCHEMY_BERA_RPC_3 = os.environ.get("ALCHEMY_BERACHAIN_RPC_3", "")

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
TOP_N = 100

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
        "chunk_size": 500_000,  # Berachain: large chunks (sparse DOLO txs, 2s blocks)
        "deploy_block": 2_900_000,   # DOLO deployed on Berachain ~block 2,925,727 (Mar 2025)
    },
}

PERIODS = {
    "1d": 86400,
    "7d": 86400 * 7,
    "30d": 86400 * 30,
    "90d": 86400 * 90,
    "180d": 86400 * 180,
    "all": 86400 * 365,        # 1 year — covers full DOLO history
}

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
    # Mint/burn addresses whose transfers should be SKIPPED entirely
    SKIP_ADDRS = {
        ZERO,
        DOLO_CONTRACT,
        "0x0000000000000000000000000000000000000001",
    }
    flows = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        if from_addr in SKIP_ADDRS or to_addr in SKIP_ADDRS:
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
    BRIDGE_ADDRS = {
        ZERO,
        DOLO_CONTRACT,
        "0x0000000000000000000000000000000000000001",
    }
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
        cached_transfers = state.get(cached_key, [])
        last_block = state.get(last_block_key, 0)

        if is_incremental and last_block > 0 and cached_transfers:
            # Only fetch new blocks since last run
            fetch_start = last_block + 1
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

            # Convert cached transfers back from lists to tuples
            restored = [tuple(t) for t in cached_transfers]

            # Merge: cached + new
            merged = restored + new_transfers

            # Prune: drop transfers from blocks older than the oldest needed
            merged = [t for t in merged if t[3] >= oldest_needed]

            all_transfers[chain_key] = merged
            print(f"  {CHAINS[chain_key]['name']}: {len(new_transfers):,} new + {len(restored):,} cached → {len(merged):,} total (after pruning)")
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
        merged = {}
        for chain_key in CHAINS:
            flows = neutralized_flows_cache[period][chain_key]
            for addr, net in flows.items():
                if addr in EXCLUDED_ADDRS:
                    continue
                if abs(net) < 1:  # skip dust
                    continue
                merged[addr] = merged.get(addr, 0) + net
        # Round values to reduce JSON size
        balance_changes[period] = {addr: round(v, 2) for addr, v in merged.items()}

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
    except ImportError:
        pass

    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "dolo_price": dolo_price,
        "periods": output_periods,
        "balance_changes": balance_changes,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

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
