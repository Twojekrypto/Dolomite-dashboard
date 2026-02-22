#!/usr/bin/env python3
"""
DOLO Token Holders — ERC-20 holder generator (ETH + Berachain)
With incremental sync: saves last processed block per chain,
only fetches new transfers on subsequent runs (~2min vs ~30min).
"""
import json, time, os, sys
import requests
from datetime import datetime

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654"
ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
ZERO = "0x0000000000000000000000000000000000000000"

CHAINS = {
    "eth": {"chain_id": 1, "name": "Ethereum", "env_keys": ["ETHERSCAN_API_KEY", "BERASCAN_API_KEY"]},
    "bera": {"chain_id": 80094, "name": "Berachain", "env_keys": ["ETHERSCAN_API_KEY", "BERASCAN_API_KEY"]},
}

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "dolo_holders.json")
STATE_FILE = os.path.join(DATA_DIR, "dolo_holders_state.json")

# Minimum balance to include (filter dust)
MIN_BALANCE = 1.0  # 1 DOLO


def load_state():
    """Load incremental sync state (last block, cached balances)."""
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


def fetch_erc20_transfers(chain_key, start_block=0):
    """Fetch ERC-20 Transfer events for DOLO on a given chain from start_block."""
    cfg = CHAINS[chain_key]
    api_key = ""
    for key_name in cfg["env_keys"]:
        api_key = os.environ.get(key_name, "")
        if api_key:
            break
    if not api_key:
        print(f"  ⚠️  No API key set ({', '.join(cfg['env_keys'])}) — skipping {cfg['name']}")
        return []

    print(f"\n📡 Fetching DOLO transfers on {cfg['name']} from block {start_block}...")

    all_txs = []
    seen = set()
    current_block = start_block
    consecutive_errors = 0

    while True:
        params = {
            "chainid": cfg["chain_id"],
            "module": "account",
            "action": "tokentx",
            "contractaddress": DOLO_CONTRACT,
            "startblock": current_block,
            "endblock": 99999999,
            "page": 1,
            "offset": 10000,
            "sort": "asc",
            "apikey": api_key,
        }

        for retry in range(5):
            try:
                resp = requests.get(ETHERSCAN_V2, params=params, timeout=60)
                data = resp.json()

                if data.get("status") == "1" and isinstance(data.get("result"), list):
                    results = data["result"]
                    new_count = 0
                    for tx in results:
                        tx_key = tx.get("hash", "") + tx.get("logIndex", "")
                        if tx_key not in seen:
                            seen.add(tx_key)
                            all_txs.append(tx)
                            new_count += 1

                    print(f"  Block {current_block}+: {len(results)} txs, {new_count} new (total: {len(all_txs)})")
                    consecutive_errors = 0

                    if len(results) < 10000:
                        print(f"  ✅ {cfg['name']}: {len(all_txs)} new transfers")
                        return all_txs

                    last_block = int(results[-1].get("blockNumber", current_block))
                    current_block = last_block if last_block != current_block else last_block + 1
                    time.sleep(0.3)
                    break

                elif "rate" in str(data.get("result", "")).lower() or "max rate" in str(data.get("message", "")).lower():
                    wait_time = 3 * (retry + 1)
                    print(f"  Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    if "No transactions" in str(data.get("result", "")):
                        print(f"  ✅ {cfg['name']}: {len(all_txs)} new transfers")
                        return all_txs
                    print(f"  ⚠️ API: {data.get('message')}: {str(data.get('result',''))[:200]}")
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        print(f"  ❌ Too many errors, returning {len(all_txs)} transfers so far")
                        return all_txs
                    time.sleep(2 * (retry + 1))
                    continue

            except requests.exceptions.Timeout:
                print(f"  Timeout (retry {retry+1}/5), waiting {3*(retry+1)}s...")
                time.sleep(3 * (retry + 1))
            except Exception as e:
                print(f"  Error: {e}, retry {retry+1}/5")
                time.sleep(2 * (retry + 1))
        else:
            consecutive_errors += 1
            print(f"  ❌ Failed after 5 retries at block {current_block}")
            if consecutive_errors >= 3:
                print(f"  ❌ Aborting {cfg['name']} — returning {len(all_txs)} transfers")
                break
            current_block += 10000

    return all_txs


def apply_transfers(balances, txs):
    """Apply transfer events to a balances dict. Returns (balances, max_block)."""
    max_block = 0
    # Sort by block + logIndex
    txs.sort(key=lambda t: (int(t.get("blockNumber", 0)), int(t.get("logIndex", 0))))

    for tx in txs:
        from_addr = tx.get("from", "").lower()
        to_addr = tx.get("to", "").lower()
        value_raw = int(tx.get("value", "0"))
        value = value_raw / (10 ** 18)
        block = int(tx.get("blockNumber", 0))
        if block > max_block:
            max_block = block

        if from_addr != ZERO.lower():
            balances[from_addr] = balances.get(from_addr, 0) - value
        if to_addr != ZERO.lower():
            balances[to_addr] = balances.get(to_addr, 0) + value

    return balances, max_block


def merge_holders(eth_balances, bera_balances):
    """Merge holders from both chains into a single list."""
    all_addrs = set(eth_balances.keys()) | set(bera_balances.keys())

    holders = []
    for addr in all_addrs:
        bal_eth = eth_balances.get(addr, 0)
        bal_bera = bera_balances.get(addr, 0)

        # Skip dust/negative
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

    # Sort by total balance descending
    holders.sort(key=lambda h: h["balance"], reverse=True)

    # Assign ranks
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

    return holders


def verify_top_balances(holders, eth_balances, bera_balances, max_check=200):
    """Verify top holders' balances against on-chain balanceOf().
    Fixes discrepancies caused by Etherscan API missing transfers."""
    print(f"\n🔎 Verifying top {max_check} holders with on-chain balanceOf()...")

    BALANCE_OF_SEL = "0x70a08231"  # balanceOf(address)
    RPCs = {
        "eth": "https://eth.drpc.org/",
        "bera": "https://berachain-rpc.publicnode.com/",
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

                        # Correct if discrepancy > 1%
                        if old_bal > 0 and abs(onchain_bal - old_bal) / max(old_bal, 1) > 0.01:
                            h[bal_key] = round(onchain_bal, 4)
                            corrections += 1
                            # Also update cached balances for state
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

    # Recalculate totals and re-sort
    for h in holders[:max_check]:
        h["balance"] = round(h.get("balance_eth", 0) + h.get("balance_bera", 0), 4)
        h["chains"] = []
        if h.get("balance_eth", 0) >= MIN_BALANCE:
            h["chains"].append("eth")
        if h.get("balance_bera", 0) >= MIN_BALANCE:
            h["chains"].append("bera")

    # Remove holders with 0 balance after correction
    holders = [h for h in holders if h["balance"] >= MIN_BALANCE]

    # Re-sort and re-rank
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
        ("bera", "https://berachain-rpc.publicnode.com/"),
    ]

    to_check = holders[:max_check]
    contract_addrs = set()

    for chain, rpc_url in RPC_URLS:
        found = 0
        for i, h in enumerate(to_check):
            for retry in range(2):
                try:
                    resp = requests.post(rpc_url, json={
                        "jsonrpc": "2.0", "method": "eth_getCode",
                        "params": [h["address"], "latest"], "id": 1
                    }, timeout=10, headers={"Content-Type": "application/json"})
                    r = resp.json()
                    code = r.get("result", "0x")
                    if code and code != "0x" and len(code) > 2:
                        contract_addrs.add(h["address"].lower())
                        found += 1
                    break
                except Exception:
                    time.sleep(1)
            time.sleep(0.05)
        print(f"  {chain}: {found} contracts found")

    count = 0
    for h in holders:
        if h["address"].lower() in contract_addrs:
            h["is_contract"] = True
            count += 1
        else:
            h["is_contract"] = False

    print(f"  ✅ Total: {count} contracts in top {max_check}")
    return holders


def main():
    print("=" * 60)
    print("🔄 DOLO Token Holders — Generator (Incremental Sync)")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load previous state
    state = load_state()
    is_incremental = bool(state)

    if is_incremental:
        print("📦 Found previous state — running incremental sync")
    else:
        print("🆕 No previous state — running full sync (first run)")

    # Get cached balances or start fresh
    eth_balances = state.get("eth_balances", {})
    bera_balances = state.get("bera_balances", {})
    eth_last_block = state.get("eth_last_block", 0)
    bera_last_block = state.get("bera_last_block", 0)

    # Fetch new transfers from last processed block
    eth_txs = fetch_erc20_transfers("eth", start_block=eth_last_block)
    bera_txs = fetch_erc20_transfers("bera", start_block=bera_last_block)

    if not eth_txs and not bera_txs and not is_incremental:
        print("⚠️  No transfers found on any chain!")
        sys.exit(1)

    # Apply new transfers to cached balances
    print("\n📊 Applying transfers...")
    if eth_txs:
        eth_balances, eth_max = apply_transfers(eth_balances, eth_txs)
        eth_last_block = max(eth_last_block, eth_max)
        print(f"  ETH: {len(eth_txs)} new transfers applied, now at block {eth_last_block}")

    if bera_txs:
        bera_balances, bera_max = apply_transfers(bera_balances, bera_txs)
        bera_last_block = max(bera_last_block, bera_max)
        print(f"  BERA: {len(bera_txs)} new transfers applied, now at block {bera_last_block}")

    # Filter positive balances
    eth_clean = {a: round(b, 4) for a, b in eth_balances.items() if b >= MIN_BALANCE}
    bera_clean = {a: round(b, 4) for a, b in bera_balances.items() if b >= MIN_BALANCE}
    print(f"  ETH holders: {len(eth_clean)} | BERA holders: {len(bera_clean)}")

    # Merge
    print("\n🔀 Merging holders across chains...")
    holders = merge_holders(eth_balances, bera_balances)

    # Verify top holders with on-chain balanceOf()
    holders, eth_balances, bera_balances = verify_top_balances(
        holders, eth_balances, bera_balances, max_check=200
    )

    # Detect contracts
    holders = detect_contracts(holders, max_check=200)

    # Stats
    eth_only = sum(1 for h in holders if h["chains"] == ["eth"])
    bera_only = sum(1 for h in holders if h["chains"] == ["bera"])
    both_chains = sum(1 for h in holders if len(h["chains"]) == 2)
    total_supply = sum(h["balance"] for h in holders)
    contracts = sum(1 for h in holders if h.get("is_contract"))

    stats = {
        "total_holders": len(holders),
        "eth_holders": sum(1 for h in holders if "eth" in h["chains"]),
        "bera_holders": sum(1 for h in holders if "bera" in h["chains"]),
        "both_chains": both_chains,
        "total_supply": round(total_supply, 2),
        "contracts_detected": contracts,
    }

    output = {
        "contract": DOLO_CONTRACT,
        "timestamp": datetime.utcnow().isoformat(),
        "stats": stats,
        "holders": holders,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    # Save state for next incremental run
    save_state({
        "eth_balances": {a: b for a, b in eth_balances.items() if abs(b) >= 0.0001},
        "bera_balances": {a: b for a, b in bera_balances.items() if abs(b) >= 0.0001},
        "eth_last_block": eth_last_block,
        "bera_last_block": bera_last_block,
        "last_run": datetime.utcnow().isoformat(),
    })

    print(f"\n💾 Saved: dolo_holders.json")
    print(f"   Total holders: {stats['total_holders']:,}")
    print(f"   ETH only: {eth_only:,}  |  BERA only: {bera_only:,}  |  Both: {both_chains:,}")
    print(f"   Contracts: {contracts}")
    print(f"   Total supply tracked: {total_supply:,.2f} DOLO")
    print(f"   Sync: {'incremental' if is_incremental else 'full'}")

    print(f"\n🏆 TOP 10:")
    for h in holders[:10]:
        chains = "+".join(c.upper() for c in h["chains"])
        tag = " 📜" if h.get("is_contract") else ""
        print(f"   #{h['rank']:<4} {h['address'][:12]}… {h['balance']:>14,.2f} DOLO  [{chains}]{tag}")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
