#!/usr/bin/env python3
"""
DOLO Token Holders — ERC-20 holder generator (ETH + Berachain)
Uses eth_getLogs for 100% accuracy (catches DEX swaps, internal transfers, etc.)
With incremental sync: saves last processed block per chain.
"""
import json, time, os, sys
import requests
from datetime import datetime

ALCHEMY_BERA_RPC = os.environ.get("ALCHEMY_BERACHAIN_RPC", "")

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"

CHAINS = {
    "eth": {
        "name": "Ethereum",
        "rpcs": [
            "https://eth.drpc.org/",
            "https://ethereum-rpc.publicnode.com/",
            "https://rpc.ankr.com/eth",
        ],
        "start_block": 21_000_000,  # DOLO deployed around this block on ETH
        "chunk_size": 50_000,
    },
    "bera": {
        "name": "Berachain",
        "rpcs": [
            *([] if not ALCHEMY_BERA_RPC else [ALCHEMY_BERA_RPC]),
            "https://berachain-rpc.publicnode.com/",
            "https://berachain.drpc.org/",
            "https://rpc.berachain.com/",
        ],
        "start_block": 2_925_000,
        "chunk_size": 50_000,
    },
}

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "dolo_holders.json")
STATE_FILE = os.path.join(DATA_DIR, "dolo_holders_state.json")
MIN_BALANCE = 1.0  # 1 DOLO


def load_state():
    """Load incremental sync state."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state):
    """Save incremental sync state."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


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


def fetch_transfer_logs(chain_key, start_block, end_block=None):
    """Fetch ERC-20 Transfer event logs via eth_getLogs.
    Returns list of (from_addr, to_addr, value_wei, block_number) tuples."""
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    chunk_size = cfg["chunk_size"]
    rpc_idx = 0

    if end_block is None:
        end_block = get_current_block(rpcs[0])

    if start_block >= end_block:
        print(f"  {cfg['name']}: already up to date (block {start_block})")
        return [], start_block

    total_chunks = (end_block - start_block + chunk_size - 1) // chunk_size
    print(f"  {cfg['name']}: scanning blocks {start_block:,} → {end_block:,} ({total_chunks} chunks)")

    all_transfers = []
    current = start_block
    chunks_done = 0

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        success = False
        for attempt in range(len(rpcs) * 2):
            rpc = rpcs[(rpc_idx + attempt) % len(rpcs)]
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
                        # Range too large — halve chunk
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
                # Reduce chunk size on timeout
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
            print(f"    {cfg['name']}: {pct}% ({chunks_done}/{total_chunks} chunks, {len(all_transfers):,} transfers)", flush=True)

        # Restore chunk size gradually
        if chunk_size < cfg["chunk_size"]:
            chunk_size = min(chunk_size * 2, cfg["chunk_size"])

        time.sleep(0.05)

    rpc_idx = (rpc_idx + 1) % len(rpcs)
    print(f"  ✅ {cfg['name']}: {len(all_transfers):,} transfers found")
    return all_transfers, end_block


def apply_transfers(balances, transfers):
    """Apply transfer events to balance map."""
    zero = ZERO.lower()
    max_block = 0
    for from_addr, to_addr, value_wei, block_num in transfers:
        value = value_wei / (10 ** 18)
        if block_num > max_block:
            max_block = block_num
        if from_addr != zero:
            balances[from_addr] = balances.get(from_addr, 0) - value
        if to_addr != zero:
            balances[to_addr] = balances.get(to_addr, 0) + value
    return balances, max_block


def merge_holders(eth_balances, bera_balances):
    """Merge holders from both chains into a single list."""
    all_addrs = set(eth_balances.keys()) | set(bera_balances.keys())

    holders = []
    for addr in all_addrs:
        bal_eth = eth_balances.get(addr, 0)
        bal_bera = bera_balances.get(addr, 0)

        if bal_eth < MIN_BALANCE:
            bal_eth = 0
        if bal_bera < MIN_BALANCE:
            bal_bera = 0
        total = round(bal_eth + bal_bera, 4)
        if total < MIN_BALANCE:
            continue

        chains = []
        if bal_eth >= MIN_BALANCE:
            chains.append("eth")
        if bal_bera >= MIN_BALANCE:
            chains.append("bera")

        holders.append({
            "address": addr,
            "balance": total,
            "balance_eth": round(bal_eth, 4),
            "balance_bera": round(bal_bera, 4),
            "chains": chains,
        })

    holders.sort(key=lambda h: h["balance"], reverse=True)
    for i, h in enumerate(holders, 1):
        h["rank"] = i

    return holders


def verify_top_balances(holders, eth_balances, bera_balances, max_check=200):
    """Verify top holders' balances against on-chain balanceOf().
    Fixes any residual discrepancies."""
    print(f"\n🔎 Verifying top {max_check} holders with on-chain balanceOf()...")

    BALANCE_OF_SEL = "0x70a08231"
    RPCs = {
        "eth": "https://eth.drpc.org/",
        "bera": ALCHEMY_BERA_RPC or "https://berachain-rpc.publicnode.com/",
    }

    to_check = holders[:max_check]
    corrections = 0

    for chain, rpc_url in RPCs.items():
        for h in to_check:
            addr = h["address"].lower()
            data_hex = BALANCE_OF_SEL + addr.replace("0x", "").zfill(64)
            for retry in range(2):
                try:
                    resp = requests.post(rpc_url, json={
                        "jsonrpc": "2.0", "method": "eth_call",
                        "params": [{"to": DOLO_CONTRACT, "data": data_hex}, "latest"], "id": 1
                    }, timeout=10, headers={"Content-Type": "application/json"})
                    r = resp.json()
                    if "error" not in r:
                        onchain_bal = int(r.get("result", "0x0"), 16) / 1e18
                        bal_key = f"balance_{chain}"
                        old_bal = h.get(bal_key, 0)

                        if old_bal > 0 and abs(onchain_bal - old_bal) / max(old_bal, 1) > 0.01:
                            h[bal_key] = round(onchain_bal, 4)
                            corrections += 1
                            if chain == "eth":
                                eth_balances[addr] = onchain_bal
                            else:
                                bera_balances[addr] = onchain_bal
                        elif old_bal == 0 and onchain_bal >= MIN_BALANCE:
                            h[bal_key] = round(onchain_bal, 4)
                            if chain not in h["chains"]:
                                h["chains"].append(chain)
                            corrections += 1
                            if chain == "eth":
                                eth_balances[addr] = onchain_bal
                            else:
                                bera_balances[addr] = onchain_bal
                    break
                except Exception:
                    time.sleep(0.5)
            time.sleep(0.03)

    for h in holders[:max_check]:
        h["balance"] = round(h.get("balance_eth", 0) + h.get("balance_bera", 0), 4)
        h["chains"] = []
        if h.get("balance_eth", 0) >= MIN_BALANCE:
            h["chains"].append("eth")
        if h.get("balance_bera", 0) >= MIN_BALANCE:
            h["chains"].append("bera")

    holders = [h for h in holders if h["balance"] >= MIN_BALANCE]
    holders.sort(key=lambda h: h["balance"], reverse=True)
    for i, h in enumerate(holders, 1):
        h["rank"] = i

    print(f"  ✅ Corrected {corrections} balances via on-chain verification")
    return holders, eth_balances, bera_balances


def detect_contracts(holders, max_check=200):
    """Detect which top holders are contracts (not EOAs)."""
    print(f"\n🔍 Detecting contracts in top {max_check} holders...")

    RPC_URLS = [
        ("eth", "https://eth.drpc.org/"),
        ("bera", ALCHEMY_BERA_RPC or "https://berachain-rpc.publicnode.com/"),
    ]

    to_check = holders[:max_check]
    contract_addrs = set()

    for chain, rpc_url in RPC_URLS:
        for h in to_check:
            addr = h["address"]
            for retry in range(2):
                try:
                    resp = requests.post(rpc_url, json={
                        "jsonrpc": "2.0", "method": "eth_getCode",
                        "params": [addr, "latest"], "id": 1
                    }, timeout=5, headers={"Content-Type": "application/json"})
                    code = resp.json().get("result", "0x")
                    if code and len(code) > 4:
                        contract_addrs.add(addr.lower())
                    break
                except Exception:
                    time.sleep(0.3)
            time.sleep(0.03)

    for h in holders:
        if h["address"].lower() in contract_addrs:
            h["is_contract"] = True

    print(f"  ✅ Found {len(contract_addrs)} contracts in top {max_check}")
    return holders


# ===== MAIN =====
def main():
    print("=" * 60)
    print("🔄 DOLO Token Holders — Generator (RPC eth_getLogs)")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    state = load_state()
    is_incremental = bool(state)

    if is_incremental:
        print("📦 Found previous state — running incremental sync")
    else:
        print("🆕 No previous state — running full sync (first run)")

    eth_balances = state.get("eth_balances", {})
    bera_balances = state.get("bera_balances", {})
    eth_last_block = state.get("eth_last_block", CHAINS["eth"]["start_block"])
    bera_last_block = state.get("bera_last_block", CHAINS["bera"]["start_block"])

    # Fetch new Transfer events via eth_getLogs
    print("\n📡 Fetching Transfer events via RPC logs...")
    eth_txs, eth_end = fetch_transfer_logs("eth", start_block=eth_last_block)
    bera_txs, bera_end = fetch_transfer_logs("bera", start_block=bera_last_block)

    if not eth_txs and not bera_txs and not is_incremental:
        print("⚠️  No transfers found on any chain!")
        sys.exit(1)

    # Apply new transfers
    print("\n📊 Applying transfers...")
    if eth_txs:
        eth_balances, eth_max = apply_transfers(eth_balances, eth_txs)
        eth_last_block = max(eth_last_block, eth_max, eth_end)
        print(f"  ETH: {len(eth_txs):,} transfers applied, now at block {eth_last_block:,}")

    if bera_txs:
        bera_balances, bera_max = apply_transfers(bera_balances, bera_txs)
        bera_last_block = max(bera_last_block, bera_max, bera_end)
        print(f"  BERA: {len(bera_txs):,} transfers applied, now at block {bera_last_block:,}")

    eth_clean = {a: round(b, 4) for a, b in eth_balances.items() if b >= MIN_BALANCE}
    bera_clean = {a: round(b, 4) for a, b in bera_balances.items() if b >= MIN_BALANCE}
    print(f"  ETH holders: {len(eth_clean):,} | BERA holders: {len(bera_clean):,}")

    # Merge
    print("\n🔀 Merging holders across chains...")
    holders = merge_holders(eth_balances, bera_balances)

    # Verify top holders on-chain
    holders, eth_balances, bera_balances = verify_top_balances(
        holders, eth_balances, bera_balances, max_check=200
    )

    # Detect contracts
    holders = detect_contracts(holders, max_check=200)

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

    # Stats
    eth_only = sum(1 for h in holders if h["chains"] == ["eth"])
    bera_only = sum(1 for h in holders if h["chains"] == ["bera"])
    both_chains = sum(1 for h in holders if len(h["chains"]) == 2)
    total_supply = sum(h["balance"] for h in holders)
    contracts = sum(1 for h in holders if h.get("is_contract"))

    stats = {
        "total_holders": len(holders),
        "eth_only": eth_only,
        "bera_only": bera_only,
        "both_chains": both_chains,
        "total_supply_tracked": round(total_supply, 2),
        "contracts_detected": contracts,
    }

    output = {
        "contract": DOLO_CONTRACT,
        "networks": ["ethereum", "berachain"],
        "timestamp": datetime.utcnow().isoformat(),
        "stats": stats,
        "holders": holders,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    # Save state for incremental sync
    save_state({
        "eth_balances": {a: b for a, b in eth_balances.items() if abs(b) > 0.0001},
        "bera_balances": {a: b for a, b in bera_balances.items() if abs(b) > 0.0001},
        "eth_last_block": eth_last_block,
        "bera_last_block": bera_last_block,
    })

    print(f"\n💾 Saved: {OUTPUT_JSON}")
    print(f"   Total holders: {len(holders):,}")
    print(f"   ETH only: {eth_only:,} | BERA only: {bera_only:,} | Both: {both_chains:,}")
    print(f"   Supply tracked: {total_supply:,.2f} DOLO")
    print(f"   Contracts: {contracts}")
    print(f"\n🏆 TOP 5:")
    for h in holders[:5]:
        tag = " 📜" if h.get("is_contract") else ""
        print(f"   #{h['rank']:<4} {h['address'][:14]}… {h['balance']:>15,.2f} DOLO{tag}")
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
