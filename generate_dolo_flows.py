#!/usr/bin/env python3
"""
DOLO Token Flows — Top Accumulators & Sellers (1d / 7d / 30d)
Fetches ERC-20 Transfer events via eth_getLogs for ETH and Berachain,
calculates net inflow/outflow per address, outputs top 5 each.
"""
import json, time, os, sys
import requests
from datetime import datetime

ALCHEMY_BERA_RPC = os.environ.get("ALCHEMY_BERACHAIN_RPC", "")
ALCHEMY_BERA_RPC_2 = os.environ.get("ALCHEMY_BERACHAIN_RPC_2", "")

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
TOP_N = 20

# Known contract addresses to exclude (DEX routers, LP pools, etc.)
EXCLUDED_ADDRS = {
    ZERO,
    DOLO_CONTRACT,
    "0x0000000000000000000000000000000000000001",
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
            "https://berachain-rpc.publicnode.com/",
            "https://berachain.drpc.org/",
            "https://rpc.berachain.com/",
        ],
        "block_time": 2,    # ~2 seconds per block
        "chunk_size": 50_000,
        "deploy_block": 3_500_000,   # DOLO deployed on Berachain mainnet
    },
}

PERIODS = {
    "1d": 86400,
    "7d": 86400 * 7,
    "30d": 86400 * 30,
    "90d": 86400 * 90,
    "180d": 86400 * 180,
    "1y": 86400 * 365,
    "all": 86400 * 365 * 3,   # 3 years — effectively "all time"
}

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "dolo_flows.json")


def get_current_block(rpc_url):
    """Get current block number from RPC."""
    for _ in range(3):
        try:
            resp = requests.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
            }, timeout=10, headers={"Content-Type": "application/json"})
            return int(resp.json().get("result", "0x0"), 16)
        except Exception:
            time.sleep(1)
    return 0


def fetch_transfer_logs(chain_key, start_block, end_block):
    """Fetch ERC-20 Transfer event logs via eth_getLogs."""
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    chunk_size = cfg["chunk_size"]

    if start_block >= end_block:
        return []

    total_chunks = (end_block - start_block + chunk_size - 1) // chunk_size
    print(f"  {cfg['name']}: scanning blocks {start_block:,} → {end_block:,} ({total_chunks} chunks)")

    all_transfers = []
    current = start_block
    chunks_done = 0

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
            print(f"    ⚠️ Failed at block {current}, skipping chunk")
            current = chunk_end + 1
            continue

        current = chunk_end + 1
        chunks_done += 1

        if chunks_done % 20 == 0 or chunks_done == total_chunks:
            pct = chunks_done * 100 // max(total_chunks, 1)
            print(f"    {cfg['name']}: {pct}% ({chunks_done}/{total_chunks}, {len(all_transfers):,} txs)", flush=True)

        if chunk_size < cfg["chunk_size"]:
            chunk_size = min(chunk_size * 2, cfg["chunk_size"])

        time.sleep(0.05)

    print(f"  ✅ {cfg['name']}: {len(all_transfers):,} transfers found")
    return all_transfers


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
    ALL transfers are counted for both parties so net flows stay accurate.
    Excluded addresses are filtered out of the final results, not from the
    calculation itself — otherwise a user buying from a DEX and selling to
    a DEX would appear as a huge accumulator (only buys counted)."""
    flows = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        value = value_wei / (10 ** 18)
        flows[from_addr] = flows.get(from_addr, 0) - value
        flows[to_addr] = flows.get(to_addr, 0) + value
    return flows


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

    dolo_price = get_dolo_price()
    print(f"\n💰 DOLO Price: ${dolo_price:.4f}" if dolo_price else "\n⚠️ Could not fetch DOLO price")

    # Get current blocks for each chain
    print("\n📡 Getting current block numbers...")
    current_blocks = {}
    for chain_key, cfg in CHAINS.items():
        blk = get_current_block(cfg["rpcs"][0])
        current_blocks[chain_key] = blk
        print(f"  {cfg['name']}: block {blk:,}")

    # Calculate cutoff blocks for each period
    # We need to fetch from the earliest cutoff (30d) to latest
    cutoff_blocks = {}
    for chain_key, cfg in CHAINS.items():
        cutoff_blocks[chain_key] = {}
        deploy_block = cfg.get("deploy_block", 0)
        for period, seconds in PERIODS.items():
            blocks_back = seconds // cfg["block_time"]
            cutoff = max(current_blocks[chain_key] - blocks_back, deploy_block)
            cutoff_blocks[chain_key][period] = cutoff

    # Fetch transfers from the longest period (covers all periods)
    max_period = max(PERIODS.keys(), key=lambda k: PERIODS[k])
    print("\n📡 Fetching Transfer events...")
    all_transfers = {}
    for chain_key in CHAINS:
        start = cutoff_blocks[chain_key][max_period]
        end = current_blocks[chain_key]
        all_transfers[chain_key] = fetch_transfer_logs(chain_key, start, end)

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
    print("\n📊 Calculating flows...")
    output_periods = {}
    for period, seconds in PERIODS.items():
        output_periods[period] = {}
        for chain_key, cfg in CHAINS.items():
            cutoff = cutoff_blocks[chain_key][period]
            # Filter transfers to only those after cutoff
            period_transfers = [
                t for t in all_transfers[chain_key] if t[3] >= cutoff
            ]

            flows = calculate_flows(period_transfers, EXCLUDED_ADDRS)
            tx_counts = count_txs(period_transfers, EXCLUDED_ADDRS)

            accumulators = get_top(flows, tx_counts, TOP_N, "accumulator", EXCLUDED_ADDRS)
            sellers = get_top(flows, tx_counts, TOP_N, "seller", EXCLUDED_ADDRS)

            # Add USD values
            if dolo_price:
                for entry in accumulators + sellers:
                    entry["usd_value"] = round(entry["net_flow"] * dolo_price, 2)

            output_periods[period][chain_key] = {
                "accumulators": accumulators,
                "sellers": sellers,
                "total_transfers": len(period_transfers),
            }

            print(f"  {period} {cfg['name']}: {len(period_transfers):,} transfers, "
                  f"top accumulator: {accumulators[0]['net_flow']:,.0f} DOLO" if accumulators else
                  f"  {period} {cfg['name']}: no data")

    # Checksum addresses
    try:
        from web3 import Web3
        for period_data in output_periods.values():
            for chain_data in period_data.values():
                for entry in chain_data["accumulators"] + chain_data["sellers"]:
                    try:
                        entry["address"] = Web3.to_checksum_address(entry["address"])
                    except Exception:
                        pass
    except ImportError:
        pass

    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "dolo_price": dolo_price,
        "periods": output_periods,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n💾 Saved: {OUTPUT_JSON}")

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
