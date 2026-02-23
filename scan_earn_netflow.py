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
        "start_block": 7_000_000,  # Berachain mainnet events start around here
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
LOG_DEPOSIT    = "0x2bad8bc95088af2c247b30fa2b2e6a0886f88625e0945cd3051008e0e270198f"
LOG_WITHDRAW   = "0xbc83c08f0b269b1726990c8348ffdf1ae1696244a14868d766e542a2f18cd7d4"
LOG_TRADE      = "0x551e705b3457d01be14140987c43896f782b70542f778b43b9f5f94522302b6f"
LOG_TRANSFER   = "0xe95afb1ad6381b7e0935d86b6442b2f145381aa2f821b5d49096b61d9ee08d4b"
LOG_LIQUIDATE  = "0x5c18eb2c52b455100d6a3f07c1b2223d05d50135af3348404e76b1e42ddaef85"

ALL_EVENTS = [LOG_DEPOSIT, LOG_WITHDRAW, LOG_TRADE, LOG_TRANSFER, LOG_LIQUIDATE]

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
            logs = get_logs(rpcs, rpc_idx, contract, [ALL_EVENTS], start, min(start + 49_999, end))
            if logs:
                return int(logs[0]["blockNumber"], 16)
        except:
            pass
        # Quick binary within this 1M chunk
        if end - start > 50_000:
            # Try midpoints in 50k chunks
            for s in range(start, end, 50_000):
                try:
                    sub_logs = get_logs(rpcs, rpc_idx, contract, [ALL_EVENTS], s, min(s + 49_999, end))
                    if sub_logs:
                        return int(sub_logs[0]["blockNumber"], 16)
                except:
                    continue
        time.sleep(0.1)
    return 0


def decode_balance_update(words, offset):
    """Decode a BalanceUpdate struct: { Types.Wei { bool sign, uint256 value } x2 }.
    Returns deltaWei (signed integer). The struct is 4 words:
      word 0: newPar.sign, word 1: newPar.value
      word 2: deltaWei.sign, word 3: deltaWei.value
    """
    sign = int(words[offset + 2], 16)   # deltaWei.sign: 1=positive, 0=negative
    value = int(words[offset + 3], 16)  # deltaWei.value
    return value if sign else -value


def decode_deposit_withdraw_log(log):
    """Decode a LogDeposit or LogWithdraw event.
    Topics: [0] event sig, [1] indexed accountOwner
    Data: accountNumber, market, BalanceUpdate(4 words)
    """
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    owner = "0x" + topics[1][-40:]
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 6:
        return None
    market_id = int(words[1], 16)
    delta_wei = decode_balance_update(words, 2)
    return {"owner": owner.lower(), "market": str(market_id), "delta": delta_wei}


def decode_trade_log(log):
    """Decode a LogTrade event (swap).
    Topics: [0] event sig, [1] indexed makerOwner, [2] indexed takerOwner
    Data: makerAccountNumber(0), takerAccountNumber(1), inputMarket(2), outputMarket(3),
          makerInputUpdate(4-7), makerOutputUpdate(8-11), takerInputUpdate(12-15), takerOutputUpdate(16-19)
    Actually the layout is:
      word 0: makerAccountNumber
      word 1: takerAccountNumber
      word 2: inputMarket
      word 3: outputMarket
      words 4-7:   makerInputUpdate  (BalanceUpdate for inputMarket on maker)
      words 8-11:  makerOutputUpdate (BalanceUpdate for outputMarket on maker)
      words 12-15: takerInputUpdate  (BalanceUpdate for inputMarket on taker - but we only track total)
      words 16-19: takerOutputUpdate (BalanceUpdate for outputMarket on taker)
    """
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    maker = "0x" + topics[1][-40:]
    taker = "0x" + topics[2][-40:] if len(topics) > 2 else None
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 12:
        return None
    input_market = str(int(words[2], 16))
    output_market = str(int(words[3], 16))
    # Maker: gets delta on input and output markets
    maker_input_delta = decode_balance_update(words, 4)
    maker_output_delta = decode_balance_update(words, 8) if len(words) >= 12 else 0
    
    results = [
        {"owner": maker.lower(), "market": input_market, "delta": maker_input_delta},
        {"owner": maker.lower(), "market": output_market, "delta": maker_output_delta},
    ]
    # Taker side (if enough data)
    if taker and len(words) >= 20:
        taker_input_delta = decode_balance_update(words, 12)
        taker_output_delta = decode_balance_update(words, 16)
        results.append({"owner": taker.lower(), "market": input_market, "delta": taker_input_delta})
        results.append({"owner": taker.lower(), "market": output_market, "delta": taker_output_delta})
    return results


def decode_transfer_log(log):
    """Decode a LogTransfer event.
    Topics: [0] event sig, [1] indexed accountOneOwner, [2] indexed accountTwoOwner
    Data: accountOneNumber(0), accountTwoNumber(1), market(2),
          updateOne(3-6), updateTwo(7-10)
    """
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    owner1 = "0x" + topics[1][-40:]
    owner2 = "0x" + topics[2][-40:] if len(topics) > 2 else None
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 7:
        return None
    market = str(int(words[2], 16))
    delta1 = decode_balance_update(words, 3)
    results = [{"owner": owner1.lower(), "market": market, "delta": delta1}]
    if owner2 and len(words) >= 11:
        delta2 = decode_balance_update(words, 7)
        results.append({"owner": owner2.lower(), "market": market, "delta": delta2})
    return results


def decode_liquidate_log(log):
    """Decode a LogLiquidate event.
    Topics: [0] event sig, [1] indexed solidOwner, [2] indexed liquidOwner
    Data: solidAccountNumber(0), liquidAccountNumber(1), heldMarket(2), owedMarket(3),
          solidHeldUpdate(4-7), solidOwedUpdate(8-11), liquidHeldUpdate(12-15), liquidOwedUpdate(16-19)
    """
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    solid = "0x" + topics[1][-40:]
    liquid = "0x" + topics[2][-40:] if len(topics) > 2 else None
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 12:
        return None
    held_market = str(int(words[2], 16))
    owed_market = str(int(words[3], 16))
    solid_held_delta = decode_balance_update(words, 4)
    solid_owed_delta = decode_balance_update(words, 8)
    results = [
        {"owner": solid.lower(), "market": held_market, "delta": solid_held_delta},
        {"owner": solid.lower(), "market": owed_market, "delta": solid_owed_delta},
    ]
    if liquid and len(words) >= 20:
        liquid_held_delta = decode_balance_update(words, 12)
        liquid_owed_delta = decode_balance_update(words, 16)
        results.append({"owner": liquid.lower(), "market": held_market, "delta": liquid_held_delta})
        results.append({"owner": liquid.lower(), "market": owed_market, "delta": liquid_owed_delta})
    return results


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
            # Fetch ALL events in a single query
            all_logs = get_logs(rpcs, rpc_idx, contract, [ALL_EVENTS], current, to_block) or []
            
            events_in_chunk = len(all_logs)
            total_events += events_in_chunk
            
            # Helper to add flow to netflows
            def add_flow(addr, mid, delta, flow_type):
                if addr not in netflows:
                    netflows[addr] = {}
                if mid not in netflows[addr]:
                    netflows[addr][mid] = {"t": "0", "d": "0", "w": "0", "s": "0", "x": "0", "l": "0"}
                # Ensure all keys exist (for old-format data)
                for k in ("d", "w", "s", "x", "l"):
                    if k not in netflows[addr][mid]:
                        netflows[addr][mid][k] = "0"
                old_t = int(netflows[addr][mid]["t"])
                netflows[addr][mid]["t"] = str(old_t + delta)
                old_ft = int(netflows[addr][mid][flow_type])
                netflows[addr][mid][flow_type] = str(old_ft + abs(delta))
            
            # Process all logs
            for log in all_logs:
                topic0 = log["topics"][0]
                
                if topic0 == LOG_DEPOSIT:
                    decoded = decode_deposit_withdraw_log(log)
                    if decoded:
                        add_flow(decoded["owner"], decoded["market"], decoded["delta"], "d")
                
                elif topic0 == LOG_WITHDRAW:
                    decoded = decode_deposit_withdraw_log(log)
                    if decoded:
                        add_flow(decoded["owner"], decoded["market"], decoded["delta"], "w")
                
                elif topic0 == LOG_TRADE:
                    entries = decode_trade_log(log)
                    if entries:
                        for e in entries:
                            add_flow(e["owner"], e["market"], e["delta"], "s")
                
                elif topic0 == LOG_TRANSFER:
                    entries = decode_transfer_log(log)
                    if entries:
                        for e in entries:
                            add_flow(e["owner"], e["market"], e["delta"], "x")
                
                elif topic0 == LOG_LIQUIDATE:
                    entries = decode_liquidate_log(log)
                    if entries:
                        for e in entries:
                            add_flow(e["owner"], e["market"], e["delta"], "l")
            
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
