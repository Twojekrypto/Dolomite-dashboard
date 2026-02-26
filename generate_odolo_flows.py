#!/usr/bin/env python3
"""
oDOLO Token Flows — Top Accumulators & Sellers (1d / 7d / 30d)
Berachain only. Fetches ERC-20 Transfer events via eth_getLogs,
calculates net inflow/outflow per address, outputs top 5 each.
"""
import json, time, os, sys
import requests
from datetime import datetime

ALCHEMY_BERA_RPC = os.environ.get("ALCHEMY_BERACHAIN_RPC", "")
ALCHEMY_BERA_RPC_2 = os.environ.get("ALCHEMY_BERACHAIN_RPC_2", "")

# ===== CONFIG =====
ODOLO_CONTRACT = "0x02E513b5B54eE216Bf836ceb471507488fC89543".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
TOP_N = 5

# Known contract addresses to exclude
EXCLUDED_ADDRS = {
    ZERO,
    ODOLO_CONTRACT,
    "0x0000000000000000000000000000000000000001",
    # oDOLO Vester
    "0x3e9b9a16743551da49b5e136c716bba7932d2cec",
}

RPC_URLS = [
    *([] if not ALCHEMY_BERA_RPC else [ALCHEMY_BERA_RPC]),
    *([] if not ALCHEMY_BERA_RPC_2 else [ALCHEMY_BERA_RPC_2]),
    "https://berachain-rpc.publicnode.com/",
    "https://berachain.drpc.org/",
    "https://rpc.berachain.com/",
]

BLOCK_TIME = 2  # ~2 seconds per block on Berachain
CHUNK_SIZE = 50_000

PERIODS = {
    "1d": 86400,
    "7d": 86400 * 7,
    "30d": 86400 * 30,
}

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "odolo_flows.json")


def get_current_block():
    for rpc in RPC_URLS:
        for _ in range(3):
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
                }, timeout=10, headers={"Content-Type": "application/json"})
                return int(resp.json().get("result", "0x0"), 16)
            except Exception:
                time.sleep(1)
    return 0


def fetch_transfer_logs(start_block, end_block):
    chunk_size = CHUNK_SIZE
    if start_block >= end_block:
        return []

    total_chunks = (end_block - start_block + chunk_size - 1) // chunk_size
    print(f"  Berachain: scanning blocks {start_block:,} → {end_block:,} ({total_chunks} chunks)")

    all_transfers = []
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
                        "address": ODOLO_CONTRACT,
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
            print(f"    Berachain: {pct}% ({chunks_done}/{total_chunks}, {len(all_transfers):,} txs)", flush=True)

        if chunk_size < CHUNK_SIZE:
            chunk_size = min(chunk_size * 2, CHUNK_SIZE)

        time.sleep(0.05)

    print(f"  ✅ Berachain: {len(all_transfers):,} transfers found")
    return all_transfers


def detect_contracts_batch(addresses):
    contracts = set()
    for addr in addresses:
        for rpc in RPC_URLS:
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
    flows = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        value = value_wei / (10 ** 18)
        if from_addr not in excluded:
            flows[from_addr] = flows.get(from_addr, 0) - value
        if to_addr not in excluded:
            flows[to_addr] = flows.get(to_addr, 0) + value
    return flows


def count_txs(transfers, excluded):
    counts = {}
    for from_addr, to_addr, _, _ in transfers:
        if from_addr not in excluded:
            counts[from_addr] = counts.get(from_addr, 0) + 1
        if to_addr not in excluded:
            counts[to_addr] = counts.get(to_addr, 0) + 1
    return counts


def get_top(flows, tx_counts, n, mode="accumulator"):
    if mode == "accumulator":
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1], reverse=True)
        filtered = [(addr, val) for addr, val in sorted_addrs if val > 0]
    else:
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1])
        filtered = [(addr, abs(val)) for addr, val in sorted_addrs if val < 0]

    result = []
    for addr, net in filtered[:n]:
        result.append({
            "address": addr,
            "net_flow": round(net, 2),
            "tx_count": tx_counts.get(addr, 0),
        })
    return result


def main():
    print("=" * 60)
    print("🔄 oDOLO Token Flows — Top Accumulators & Sellers")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Get current block
    print("\n📡 Getting current block number...")
    current_block = get_current_block()
    print(f"  Berachain: block {current_block:,}")

    # Calculate cutoff blocks
    cutoff_blocks = {}
    for period, seconds in PERIODS.items():
        blocks_back = seconds // BLOCK_TIME
        cutoff_blocks[period] = max(current_block - blocks_back, 0)

    # Fetch all transfers from 30d ago
    print("\n📡 Fetching Transfer events...")
    all_transfers = fetch_transfer_logs(cutoff_blocks["30d"], current_block)

    # Detect contracts
    print("\n🔍 Detecting contract addresses to exclude...")
    flows_30d = calculate_flows(all_transfers, EXCLUDED_ADDRS)
    top_by_flow = sorted(flows_30d.items(), key=lambda x: abs(x[1]), reverse=True)[:30]
    addrs_to_check = [addr for addr, _ in top_by_flow]
    contracts = detect_contracts_batch(addrs_to_check)
    EXCLUDED_ADDRS.update(contracts)
    print(f"  Excluded {len(contracts)} contract(s)")

    # Calculate flows for each period
    print("\n📊 Calculating flows...")
    output_periods = {}
    for period, seconds in PERIODS.items():
        cutoff = cutoff_blocks[period]
        period_transfers = [t for t in all_transfers if t[3] >= cutoff]

        flows = calculate_flows(period_transfers, EXCLUDED_ADDRS)
        tx_counts = count_txs(period_transfers, EXCLUDED_ADDRS)

        accumulators = get_top(flows, tx_counts, TOP_N, "accumulator")
        sellers = get_top(flows, tx_counts, TOP_N, "seller")

        output_periods[period] = {
            "accumulators": accumulators,
            "sellers": sellers,
            "total_transfers": len(period_transfers),
        }

        if accumulators:
            print(f"  {period}: {len(period_transfers):,} transfers, "
                  f"top accumulator: {accumulators[0]['net_flow']:,.0f} oDOLO")
        else:
            print(f"  {period}: no data")

    # Collect all unique addresses from results
    all_addrs = set()
    for period_data in output_periods.values():
        for entry in period_data["accumulators"] + period_data["sellers"]:
            all_addrs.add(entry["address"])

    # Fetch oDOLO balances for all addresses
    print(f"\n💰 Fetching oDOLO balances for {len(all_addrs)} addresses...")
    balances = {}
    bal_selector = "0x70a08231"  # balanceOf(address)
    for addr in all_addrs:
        padded = addr.replace("0x", "").lower().zfill(64)
        for rpc in RPC_URLS:
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_call",
                    "params": [{"to": ODOLO_CONTRACT, "data": bal_selector + padded}, "latest"],
                    "id": 1
                }, timeout=5, headers={"Content-Type": "application/json"})
                result = resp.json().get("result", "0x0")
                bal = int(result, 16) / (10 ** 18) if result and result != "0x" else 0
                balances[addr] = round(bal, 2)
                break
            except Exception:
                time.sleep(0.3)
        time.sleep(0.05)

    # Add balances to all entries
    for period_data in output_periods.values():
        for entry in period_data["accumulators"] + period_data["sellers"]:
            entry["balance"] = balances.get(entry["address"], 0)

    # Checksum addresses
    try:
        from web3 import Web3
        for period_data in output_periods.values():
            for entry in period_data["accumulators"] + period_data["sellers"]:
                try:
                    entry["address"] = Web3.to_checksum_address(entry["address"])
                except Exception:
                    pass
    except ImportError:
        pass

    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "periods": output_periods,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n💾 Saved: {OUTPUT_JSON}")

    for period in PERIODS:
        data = output_periods[period]
        print(f"\n📊 {period.upper()}:")
        if data["accumulators"]:
            top = data["accumulators"][0]
            print(f"  🟢 Top accumulator: {top['address'][:14]}… +{top['net_flow']:,.0f} oDOLO")
        if data["sellers"]:
            top = data["sellers"][0]
            print(f"  🔴 Top seller: {top['address'][:14]}… -{top['net_flow']:,.0f} oDOLO")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
