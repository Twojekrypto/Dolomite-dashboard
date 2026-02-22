#!/usr/bin/env python3
"""
Scan DolomiteMargin on-chain events to compute netflow per address per market.
Generates data/earn-netflow/{chainId}.json for each chain.

Events scanned:
  - LogDeposit(address indexed accountOwner, uint256 accountNumber, uint256 market, BalanceUpdate update)
  - LogWithdraw(address indexed accountOwner, uint256 accountNumber, uint256 market, BalanceUpdate update)
  
Netflow = sum of deposits - sum of withdrawals (in wei)
Yield = currentWei - netflow
"""

import json, os, sys, time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# --- Chain configs ---
CHAINS = {
    "arbitrum": {
        "margin": "0x6bd780e7fDf01D77e4d475c821f1e7AE05409072",
        "rpcs": [
            "https://arbitrum-one-rpc.publicnode.com/",
            "https://1rpc.io/arb",
            "https://arb1.arbitrum.io/rpc",
        ],
        "start_block": 29_750_000,
    },
    "ethereum": {
        "margin": "0x003Ca23Fd5F0ca87D01F6eC6CD14A8AE60c2b97D",
        "rpcs": [
            "https://ethereum-rpc.publicnode.com/",
            "https://1rpc.io/eth",
            "https://eth.llamarpc.com/",
        ],
        "start_block": 22_790_000,
    },
    "berachain": {
        "margin": "0x003Ca23Fd5F0ca87D01F6eC6CD14A8AE60c2b97D",
        "rpcs": [
            "https://rpc.berachain.com/",
            "https://berachain-rpc.publicnode.com/",
            "https://1rpc.io/berachain",
        ],
        "start_block": -1,  # No events found
    },
    "mantle": {
        "margin": "0xe6ef4f0b2455bab92ce7cc78e35324ab58917de8",
        "rpcs": [
            "https://rpc.mantle.xyz/",
            "https://mantle-rpc.publicnode.com/",
            "https://1rpc.io/mantle",
        ],
        "start_block": 64_046_000,
    },
    "polygonzkevm": {
        "margin": "0x836b557cf9ef29fcf49c776841191782df34e4e5",
        "rpcs": [
            "https://zkevm-rpc.com/",
            "https://polygon-zkevm-rpc.publicnode.com/",
        ],
        "start_block": -1,  # No events found
    },
    "xlayer": {
        "margin": "0x836b557cf9ef29fcf49c776841191782df34e4e5",
        "rpcs": [
            "https://rpc.xlayer.tech/",
            "https://xlayer-mainnet.public.blastapi.io/",
        ],
        "start_block": -1,  # No events found
    },
}

# Event signatures (keccak256 hashes)
# LogDeposit(address indexed accountOwner, uint256 accountNumber, uint256 market, ((bool,uint256),(bool,uint256)) update)
LOG_DEPOSIT  = "0x2bad8bc95088af2c247b30fa2b2e6a0886f88625e0945cd3051008e0e270198f"
# LogWithdraw(address indexed accountOwner, uint256 accountNumber, uint256 market, ((bool,uint256),(bool,uint256)) update)
LOG_WITHDRAW = "0xbc83c08f0b269b1726990c8348ffdf1ae1696244a14868d766e542a2f18cd7d4"

BLOCK_CHUNK = 49999  # blocks per getLogs request (RPC max is typically 50k)
MAX_RETRIES = 3
OUTPUT_DIR = Path(__file__).parent / "data" / "earn-netflow"
PROGRESS_DIR = Path(__file__).parent / "data" / ".netflow-progress"

rpc_id = 1


def rpc_call(rpcs, method, params, rpc_idx_ref):
    """Make an RPC call with failover across multiple endpoints."""
    global rpc_id
    for attempt in range(MAX_RETRIES * len(rpcs)):
        idx = rpc_idx_ref[0] % len(rpcs)
        rpc_url = rpcs[idx]
        payload = json.dumps({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": rpc_id,
        }).encode()
        rpc_id += 1
        req = Request(rpc_url, data=payload, headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) DolomiteScanner/1.0",
            "Accept": "application/json",
        })
        try:
            with urlopen(req, timeout=30) as response:
                data = json.loads(response.read())
                if "error" in data:
                    print(f"  RPC error from {rpc_url}: {data['error']}")
                    rpc_idx_ref[0] += 1
                    time.sleep(0.5)
                    continue
                return data.get("result")
        except (URLError, HTTPError, TimeoutError, OSError) as e:
            print(f"  RPC failed ({rpc_url}): {e}")
            rpc_idx_ref[0] += 1
            time.sleep(1)
    raise Exception(f"All RPCs failed after {MAX_RETRIES * len(rpcs)} attempts")


def get_block_number(rpcs, rpc_idx):
    """Get the latest block number."""
    result = rpc_call(rpcs, "eth_blockNumber", [], rpc_idx)
    return int(result, 16)


def get_logs(rpcs, rpc_idx, contract, topics, from_block, to_block):
    """Fetch logs for a given block range."""
    params = [{
        "address": contract,
        "topics": topics,
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
    }]
    return rpc_call(rpcs, "eth_getLogs", params, rpc_idx)


def find_first_event_block(rpcs, rpc_idx, contract, latest_block):
    """Find the first block with events using log-based binary search in 1M chunks."""
    chunk = 1_000_000
    for start in range(0, latest_block, chunk):
        end = min(start + chunk - 1, latest_block)
        try:
            logs = get_logs(rpcs, rpc_idx, contract, [[LOG_DEPOSIT, LOG_WITHDRAW]], start, min(start + 49_999, end))
            if logs:
                return int(logs[0]["blockNumber"], 16)
        except:
            pass
        # Quick binary within this 1M chunk
        if end - start > 50_000:
            # Try midpoints in 50k chunks
            for s in range(start, end, 50_000):
                try:
                    sub_logs = get_logs(rpcs, rpc_idx, contract, [[LOG_DEPOSIT, LOG_WITHDRAW]], s, min(s + 49_999, end))
                    if sub_logs:
                        return int(sub_logs[0]["blockNumber"], 16)
                except:
                    continue
        time.sleep(0.1)
    return 0


def decode_deposit_withdraw_log(log):
    """Decode a LogDeposit or LogWithdraw event.
    
    Topics:
      [0] = event signature
      [1] = indexed accountOwner (address, padded to 32 bytes)
    
    Data (non-indexed):
      word 0: accountNumber (uint256)
      word 1: market (uint256)
      word 2-5: BalanceUpdate { Types.Wei newPar, Types.Wei deltaWei }
        word 2: newPar.sign (bool)
        word 3: newPar.value (uint128 as uint256)
        word 4: deltaWei.sign (bool)  
        word 5: deltaWei.value (uint256) — this is what we want
    """
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    
    owner = "0x" + topics[1][-40:]  # last 20 bytes of topic
    
    # Decode data words
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 6:
        return None
    
    market_id = int(words[1], 16)
    delta_sign = int(words[4], 16)  # 1 = positive, 0 = negative
    delta_value = int(words[5], 16)
    
    # For deposits: delta is positive (sign=1)
    # For withdrawals: delta is negative (sign=0)
    delta_wei = delta_value if delta_sign else -delta_value
    
    return {
        "owner": owner.lower(),
        "market": str(market_id),
        "delta": delta_wei,
    }


def load_progress(chain_id):
    """Load scanning progress for a chain."""
    progress_file = PROGRESS_DIR / f"{chain_id}.json"
    if progress_file.exists():
        with open(progress_file) as f:
            return json.load(f)
    return {"last_block": 0, "netflows": {}}


def save_progress(chain_id, progress):
    """Save scanning progress."""
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_DIR / f"{chain_id}.json", "w") as f:
        json.dump(progress, f)


def scan_chain(chain_id, chain_config, only_chains=None):
    """Scan a single chain for deposit/withdraw events."""
    if only_chains and chain_id not in only_chains:
        return
    
    print(f"\n{'='*60}")
    print(f"Scanning {chain_id.upper()}")
    print(f"{'='*60}")
    
    rpcs = chain_config["rpcs"]
    contract = chain_config["margin"]
    rpc_idx = [0]
    
    # Chains with no events — generate empty file
    if chain_config["start_block"] < 0:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_file = OUTPUT_DIR / f"{chain_id}.json"
        output_data = {
            "chain": chain_id,
            "lastBlock": 0,
            "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "addressCount": 0,
            "netflows": {},
        }
        with open(output_file, "w") as f:
            json.dump(output_data, f, separators=(",", ":"))
        print(f"  ✓ No events on chain — wrote empty file")
        return
    
    # Get latest block
    try:
        latest_block = get_block_number(rpcs, rpc_idx)
    except Exception as e:
        print(f"  ✗ Cannot get block number: {e}")
        return
    
    print(f"  Latest block: {latest_block:,}")
    
    # Load progress
    progress = load_progress(chain_id)
    netflows = progress["netflows"]
    start_block = progress["last_block"]
    
    if start_block == 0:
        # Use hardcoded start block from config
        start_block = chain_config["start_block"]
        print(f"  Starting from block: {start_block:,}")
    else:
        print(f"  Resuming from block: {start_block:,}")
    
    # Scan in chunks
    current = start_block
    total_events = 0
    chunk_size = BLOCK_CHUNK
    
    while current <= latest_block:
        to_block = min(current + chunk_size - 1, latest_block)
        
        try:
            # Fetch deposits AND withdrawals in a single query using OR topic filter
            all_logs = get_logs(rpcs, rpc_idx, contract, [[LOG_DEPOSIT, LOG_WITHDRAW]], current, to_block) or []
            
            events_in_chunk = len(all_logs)
            total_events += events_in_chunk
            
            # Process all logs
            for log in all_logs:
                topic0 = log["topics"][0]
                decoded = decode_deposit_withdraw_log(log)
                if not decoded:
                    continue
                addr = decoded["owner"]
                mid = decoded["market"]
                if addr not in netflows:
                    netflows[addr] = {}
                if mid not in netflows[addr]:
                    netflows[addr][mid] = {"t": "0", "d": "0", "w": "0"}
                
                old_t = int(netflows[addr][mid]["t"])
                netflows[addr][mid]["t"] = str(old_t + decoded["delta"])
                
                if topic0 == LOG_DEPOSIT:
                    old_d = int(netflows[addr][mid]["d"])
                    netflows[addr][mid]["d"] = str(old_d + abs(decoded["delta"]))
                else:
                    old_w = int(netflows[addr][mid]["w"])
                    netflows[addr][mid]["w"] = str(old_w + abs(decoded["delta"]))
            
            pct = ((to_block - start_block) / max(1, latest_block - start_block)) * 100
            blocks_done = to_block - start_block
            if events_in_chunk > 0 or blocks_done % 500_000 < chunk_size:
                print(f"  [{pct:5.1f}%] Block {to_block:,} — {events_in_chunk} events (total: {total_events}, addrs: {len(netflows)})")
            
            # Save progress periodically
            if (to_block - start_block) % (chunk_size * 10) < chunk_size:
                progress["last_block"] = to_block + 1
                progress["netflows"] = netflows
                save_progress(chain_id, progress)
            
            # If we get too many logs, reduce chunk size
            if events_in_chunk > 5000:
                chunk_size = max(5000, chunk_size // 2)
                print(f"  ⚠ Reducing chunk size to {chunk_size}")
            elif events_in_chunk < 100 and chunk_size < BLOCK_CHUNK:
                chunk_size = min(BLOCK_CHUNK, chunk_size * 2)
            
            current = to_block + 1
            
        except Exception as e:
            error_msg = str(e)
            if "Too Many" in error_msg or "rate" in error_msg.lower():
                print(f"  ⚠ Rate limited, waiting 5s...")
                time.sleep(5)
                continue
            elif "range" in error_msg.lower() or "10000" in error_msg or "exceed" in error_msg.lower():
                chunk_size = max(500, chunk_size // 2)
                print(f"  ⚠ Block range too large, reducing to {chunk_size}")
                continue
            else:
                print(f"  ✗ Error at block {current}: {e}")
                time.sleep(2)
                rpc_idx[0] += 1
                continue
    
    # Final save
    progress["last_block"] = latest_block + 1
    progress["netflows"] = netflows
    save_progress(chain_id, progress)
    
    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / f"{chain_id}.json"
    
    output_data = {
        "chain": chain_id,
        "lastBlock": latest_block,
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "addressCount": len(netflows),
        "netflows": netflows,
    }
    
    with open(output_file, "w") as f:
        json.dump(output_data, f, separators=(",", ":"))
    
    file_size = output_file.stat().st_size
    print(f"\n  ✓ {chain_id}: {len(netflows)} addresses, {total_events} events")
    print(f"  ✓ Output: {output_file} ({file_size/1024:.1f} KB)")


def main():
    only_chains = None
    if len(sys.argv) > 1:
        only_chains = [c.strip().lower() for c in sys.argv[1].split(",")]
        print(f"Scanning only: {', '.join(only_chains)}")
    
    for chain_id, config in CHAINS.items():
        scan_chain(chain_id, config, only_chains)
    
    print(f"\n{'='*60}")
    print("Done! Files written to data/earn-netflow/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
