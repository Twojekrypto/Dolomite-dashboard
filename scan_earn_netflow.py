#!/usr/bin/env python3
"""
Scan DolomiteMargin on-chain events to compute netflow per address per market.
Generates data/earn-netflow/{chainId}.json for each chain.

The scanner supports both the legacy DolomiteMargin event layout and the newer
event topics/layout used by the replay verifier in the dashboard. This keeps
backend netflow accounting aligned with the frontend replay model.

Netflow = signed sum of all balance-changing deltaWei updates.
Yield = currentWei - netflow.

For current-layout events we also keep enough rolling balance state to derive a
"recent cycle" baseline. That lets downstream verification distinguish all-time
historical turnover from the active supply cycle of a market.
"""

import argparse
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
            *([] if not os.environ.get("ALCHEMY_ETHEREUM_RPC") else [os.environ["ALCHEMY_ETHEREUM_RPC"]]),
            *([] if not os.environ.get("ALCHEMY_ETHEREUM_RPC_2") else [os.environ["ALCHEMY_ETHEREUM_RPC_2"]]),
            *([] if not os.environ.get("ALCHEMY_ETHEREUM_RPC_3") else [os.environ["ALCHEMY_ETHEREUM_RPC_3"]]),
            "https://ethereum-rpc.publicnode.com/",
            "https://1rpc.io/eth",
            "https://eth.llamarpc.com/",
        ],
        "start_block": 22_790_000,
    },
    "berachain": {
        "margin": "0x003Ca23Fd5F0ca87D01F6eC6CD14A8AE60c2b97D",
        "rpcs": [
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC") else [os.environ["ALCHEMY_BERACHAIN_RPC"]]),
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC_2") else [os.environ["ALCHEMY_BERACHAIN_RPC_2"]]),
            "https://rpc.berachain.com/",
            "https://berachain-rpc.publicnode.com/",
            "https://1rpc.io/berachain",
        ],
        "start_block": 7_000_000,
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
LOG_DEPOSIT = "0x2bad8bc95088af2c247b30fa2b2e6a0886f88625e0945cd3051008e0e270198f"
LOG_WITHDRAW = "0xbc83c08f0b269b1726990c8348ffdf1ae1696244a14868d766e542a2f18cd7d4"

# Legacy DolomiteMargin events
LEGACY_LOG_TRADE = "0x551e705b3457d01be14140987c43896f782b70542f778b43b9f5f94522302b6f"
LEGACY_LOG_TRANSFER = "0xe95afb1ad6381b7e0935d86b6442b2f145381aa2f821b5d49096b61d9ee08d4b"
LEGACY_LOG_LIQUIDATE = "0x5c18eb2c52b455100d6a3f07c1b2223d05d50135af3348404e76b1e42ddaef85"

# Current dashboard replay event topics/layout
CURRENT_LOG_TRANSFER = "0x21281f8d59117d0399dc467dbdd321538ceffe3225e80e2bd4de6f1b3355cbc7"
CURRENT_LOG_BUY = "0x2e346762bf4ae4568971c30b51fcebd2138275aafcfe12d872956e9f3e120893"
CURRENT_LOG_SELL = "0xcc3330184b6d88cad87f9e9543b4d4110a6a3eaf20164ca5252d598d0acba3f1"
CURRENT_LOG_TRADE = "0x54d4cc60cf7d570631cc1a58942812cb0fc461713613400f56932040c3497d19"
CURRENT_LOG_LIQUIDATE = "0x1b9e65b359b871d74b1af1fc8b13b11635bfb097c4631b091eb762fda7e67dc7"
CURRENT_LOG_VAPORIZE = "0xefdcfda4e0be180f29bfeebc4bcb6de1e950d70b41e9ee813bff9882ee16ca91"

ALL_EVENTS = [
    LOG_DEPOSIT, LOG_WITHDRAW,
    LEGACY_LOG_TRADE, LEGACY_LOG_TRANSFER, LEGACY_LOG_LIQUIDATE,
    CURRENT_LOG_TRANSFER, CURRENT_LOG_BUY, CURRENT_LOG_SELL,
    CURRENT_LOG_TRADE, CURRENT_LOG_LIQUIDATE, CURRENT_LOG_VAPORIZE,
]

SECOND_OWNER_EVENTS = [
    LEGACY_LOG_TRADE, LEGACY_LOG_TRANSFER, LEGACY_LOG_LIQUIDATE,
    CURRENT_LOG_TRANSFER, CURRENT_LOG_TRADE, CURRENT_LOG_LIQUIDATE, CURRENT_LOG_VAPORIZE,
]

BLOCK_CHUNK = 49999  # blocks per getLogs request (RPC max is typically 50k)
ADDRESS_FILTER_CHUNK = 500000
MAX_RETRIES = 3
OUTPUT_DIR = Path(__file__).parent / "data" / "earn-netflow"
PROGRESS_DIR = Path(__file__).parent / "data" / ".netflow-progress"
PROGRESS_VERSION = 2

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


def _addr_topic(addr):
    return "0x" + str(addr).lower().replace("0x", "").rjust(64, "0")


def _dedupe_logs(logs):
    seen = set()
    out = []
    for log in logs or []:
        key = (
            str(log.get("transactionHash", "")).lower(),
            str(log.get("logIndex", "")).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(log)
    return out


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


def decode_signed_bool_uint(words, offset):
    """Decode { bool sign, uint256 value } into a signed integer."""
    sign = int(words[offset], 16)
    value = int(words[offset + 1], 16)
    return value if sign else -value


def decode_uint(words, offset):
    return int(words[offset], 16)


def decode_balance_update_current(words, offset):
    """Decode current dashboard replay BalanceUpdate layout."""
    return {
        "delta": decode_signed_bool_uint(words, offset),
        "new_par": decode_signed_bool_uint(words, offset + 2),
    }


def decode_balance_update_legacy(words, offset):
    """Decode legacy BalanceUpdate layout used by older scanner logic."""
    sign = int(words[offset + 2], 16)
    value = int(words[offset + 3], 16)
    return value if sign else -value


def decode_legacy_deposit_withdraw_log(log):
    """Decode a legacy LogDeposit or LogWithdraw event."""
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    owner = "0x" + topics[1][-40:]
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 6:
        return None
    market_id = int(words[1], 16)
    delta_wei = decode_balance_update_legacy(words, 2)
    return {"owner": owner.lower(), "market": str(market_id), "delta": delta_wei}


def decode_legacy_trade_log(log):
    """Decode a legacy LogTrade event."""
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
    maker_input_delta = decode_balance_update_legacy(words, 4)
    maker_output_delta = decode_balance_update_legacy(words, 8) if len(words) >= 12 else 0
    
    results = [
        {"owner": maker.lower(), "market": input_market, "delta": maker_input_delta},
        {"owner": maker.lower(), "market": output_market, "delta": maker_output_delta},
    ]
    # Taker side (if enough data)
    if taker and len(words) >= 20:
        taker_input_delta = decode_balance_update_legacy(words, 12)
        taker_output_delta = decode_balance_update_legacy(words, 16)
        results.append({"owner": taker.lower(), "market": input_market, "delta": taker_input_delta})
        results.append({"owner": taker.lower(), "market": output_market, "delta": taker_output_delta})
    return results


def decode_legacy_transfer_log(log):
    """Decode a legacy LogTransfer event."""
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    owner1 = "0x" + topics[1][-40:]
    owner2 = "0x" + topics[2][-40:] if len(topics) > 2 else None
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 7:
        return None
    market = str(int(words[2], 16))
    delta1 = decode_balance_update_legacy(words, 3)
    results = [{"owner": owner1.lower(), "market": market, "delta": delta1}]
    if owner2 and len(words) >= 11:
        delta2 = decode_balance_update_legacy(words, 7)
        results.append({"owner": owner2.lower(), "market": market, "delta": delta2})
    return results


def decode_legacy_liquidate_log(log):
    """Decode a legacy LogLiquidate event."""
    topics = log["topics"]
    data = log["data"].replace("0x", "")
    solid = "0x" + topics[1][-40:]
    liquid = "0x" + topics[2][-40:] if len(topics) > 2 else None
    words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
    if len(words) < 12:
        return None
    held_market = str(int(words[2], 16))
    owed_market = str(int(words[3], 16))
    solid_held_delta = decode_balance_update_legacy(words, 4)
    solid_owed_delta = decode_balance_update_legacy(words, 8)
    results = [
        {"owner": solid.lower(), "market": held_market, "delta": solid_held_delta},
        {"owner": solid.lower(), "market": owed_market, "delta": solid_owed_delta},
    ]
    if liquid and len(words) >= 20:
        liquid_held_delta = decode_balance_update_legacy(words, 12)
        liquid_owed_delta = decode_balance_update_legacy(words, 16)
        results.append({"owner": liquid.lower(), "market": held_market, "delta": liquid_held_delta})
        results.append({"owner": liquid.lower(), "market": owed_market, "delta": liquid_owed_delta})
    return results


def _empty_progress():
    return {
        "last_block": 0,
        "netflows": {},
        "cycle_account_state": {},
        "cycle_market_state": {},
        "cycle_state_enabled": True,
    }


def _serialize_cycle_account_state(cycle_account_state):
    return {
        f"{owner}|{account}|{market}": str(par)
        for (owner, account, market), par in cycle_account_state.items()
    }


def _deserialize_cycle_account_state(raw_state):
    cycle_account_state = {}
    if not isinstance(raw_state, dict):
        return cycle_account_state
    for key, value in raw_state.items():
        try:
            owner, account, market = str(key).split("|", 2)
            cycle_account_state[(owner, account, market)] = int(value)
        except Exception:
            continue
    return cycle_account_state


def _serialize_cycle_market_state(cycle_market_state):
    serialized = {}
    for (owner, market), state in cycle_market_state.items():
        serialized[f"{owner}|{market}"] = {
            "endingPar": str(state.get("endingPar", 0)),
            "peakPar": str(state.get("peakPar", 0)),
            "totalWei": str(state.get("totalWei", 0)),
            "suffixCandidates": [
                {
                    "balance": str(candidate.get("balance", 0)),
                    "prefixWei": str(candidate.get("prefixWei", 0)),
                }
                for candidate in state.get("suffixCandidates", [])
            ],
        }
    return serialized


def _deserialize_cycle_market_state(raw_state):
    cycle_market_state = {}
    if not isinstance(raw_state, dict):
        return cycle_market_state
    for key, value in raw_state.items():
        try:
            owner, market = str(key).split("|", 1)
            suffix_candidates = []
            for candidate in value.get("suffixCandidates", []) or []:
                suffix_candidates.append({
                    "balance": int(candidate.get("balance", 0)),
                    "prefixWei": int(candidate.get("prefixWei", 0)),
                })
            cycle_market_state[(owner, market)] = {
                "endingPar": int(value.get("endingPar", 0)),
                "peakPar": int(value.get("peakPar", 0)),
                "totalWei": int(value.get("totalWei", 0)),
                "suffixCandidates": suffix_candidates,
            }
        except Exception:
            continue
    return cycle_market_state


def _build_progress_payload(last_block, netflows, cycle_account_state, cycle_market_state):
    return {
        "version": PROGRESS_VERSION,
        "last_block": last_block,
        "netflows": netflows,
        "cycleAccountState": _serialize_cycle_account_state(cycle_account_state),
        "cycleMarketState": _serialize_cycle_market_state(cycle_market_state),
    }


def _build_legacy_progress_payload(last_block, netflows):
    return {
        "last_block": last_block,
        "netflows": netflows,
    }


def load_progress(chain_id):
    """Load scanning progress for a chain."""
    progress_file = PROGRESS_DIR / f"{chain_id}.json"
    if progress_file.exists():
        with open(progress_file) as f:
            data = json.load(f)
        if data.get("version") != PROGRESS_VERSION:
            legacy = _empty_progress()
            legacy["last_block"] = int(data.get("last_block", 0))
            legacy["netflows"] = data.get("netflows", {}) or {}
            legacy["cycle_state_enabled"] = False
            print(f"  ℹ Legacy progress detected for {chain_id}; keeping incremental netflow and waiting for reset_chain to enable cycle-aware rebuild")
            return legacy
        return {
            "last_block": int(data.get("last_block", 0)),
            "netflows": data.get("netflows", {}) or {},
            "cycle_account_state": _deserialize_cycle_account_state(data.get("cycleAccountState")),
            "cycle_market_state": _deserialize_cycle_market_state(data.get("cycleMarketState")),
            "cycle_state_enabled": True,
        }
    return _empty_progress()


def decode_current_deposit_withdraw_log(log):
    """Decode current replay-style deposit/withdraw event."""
    try:
        topics = log["topics"]
        if len(topics) < 2:
            return None
        owner = "0x" + topics[1][-40:]
        data = log["data"].replace("0x", "")
        words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
        if len(words) < 6:
            return None
        account = str(decode_uint(words, 0))
        market_id = str(int(words[1], 16))
        update = decode_balance_update_current(words, 2)
        return [{
            "owner": owner.lower(),
            "account": account,
            "market": market_id,
            "delta": update["delta"],
            "new_par": update["new_par"],
        }]
    except Exception:
        return None


def decode_current_transfer_log(log):
    """Decode current replay-style transfer event."""
    try:
        topics = log["topics"]
        if len(topics) < 3:
            return None
        owner1 = "0x" + topics[1][-40:]
        owner2 = "0x" + topics[2][-40:]
        data = log["data"].replace("0x", "")
        words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
        if len(words) < 11:
            return None
        account1 = str(decode_uint(words, 0))
        account2 = str(decode_uint(words, 1))
        market_id = str(int(words[2], 16))
        update1 = decode_balance_update_current(words, 3)
        update2 = decode_balance_update_current(words, 7)
        return [
            {"owner": owner1.lower(), "account": account1, "market": market_id, "delta": update1["delta"], "new_par": update1["new_par"]},
            {"owner": owner2.lower(), "account": account2, "market": market_id, "delta": update2["delta"], "new_par": update2["new_par"]},
        ]
    except Exception:
        return None


def decode_current_buy_sell_log(log):
    """Decode current replay-style buy/sell event."""
    try:
        topics = log["topics"]
        if len(topics) < 2:
            return None
        owner = "0x" + topics[1][-40:]
        data = log["data"].replace("0x", "")
        words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
        if len(words) < 11:
            return None
        account = str(decode_uint(words, 0))
        update_a = decode_balance_update_current(words, 3)
        update_b = decode_balance_update_current(words, 7)
        return [
            {"owner": owner.lower(), "account": account, "market": str(int(words[1], 16)), "delta": update_a["delta"], "new_par": update_a["new_par"]},
            {"owner": owner.lower(), "account": account, "market": str(int(words[2], 16)), "delta": update_b["delta"], "new_par": update_b["new_par"]},
        ]
    except Exception:
        return None


def decode_current_trade_log(log):
    """Decode current replay-style trade event."""
    try:
        topics = log["topics"]
        if len(topics) < 3:
            return None
        owner1 = "0x" + topics[1][-40:]
        owner2 = "0x" + topics[2][-40:]
        data = log["data"].replace("0x", "")
        words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
        if len(words) < 20:
            return None
        account1 = str(decode_uint(words, 0))
        account2 = str(decode_uint(words, 1))
        input_market = str(int(words[2], 16))
        output_market = str(int(words[3], 16))
        update1_in = decode_balance_update_current(words, 4)
        update1_out = decode_balance_update_current(words, 8)
        update2_in = decode_balance_update_current(words, 12)
        update2_out = decode_balance_update_current(words, 16)
        return [
            {"owner": owner1.lower(), "account": account1, "market": input_market, "delta": update1_in["delta"], "new_par": update1_in["new_par"]},
            {"owner": owner1.lower(), "account": account1, "market": output_market, "delta": update1_out["delta"], "new_par": update1_out["new_par"]},
            {"owner": owner2.lower(), "account": account2, "market": input_market, "delta": update2_in["delta"], "new_par": update2_in["new_par"]},
            {"owner": owner2.lower(), "account": account2, "market": output_market, "delta": update2_out["delta"], "new_par": update2_out["new_par"]},
        ]
    except Exception:
        return None


def decode_current_liquidate_log(log):
    """Decode current replay-style liquidate event."""
    try:
        topics = log["topics"]
        if len(topics) < 3:
            return None
        owner1 = "0x" + topics[1][-40:]
        owner2 = "0x" + topics[2][-40:]
        data = log["data"].replace("0x", "")
        words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
        if len(words) < 20:
            return None
        account1 = str(decode_uint(words, 0))
        account2 = str(decode_uint(words, 1))
        held_market = str(int(words[2], 16))
        owed_market = str(int(words[3], 16))
        solid_held = decode_balance_update_current(words, 4)
        solid_owed = decode_balance_update_current(words, 8)
        liquid_held = decode_balance_update_current(words, 12)
        liquid_owed = decode_balance_update_current(words, 16)
        return [
            {"owner": owner1.lower(), "account": account1, "market": held_market, "delta": solid_held["delta"], "new_par": solid_held["new_par"]},
            {"owner": owner1.lower(), "account": account1, "market": owed_market, "delta": solid_owed["delta"], "new_par": solid_owed["new_par"]},
            {"owner": owner2.lower(), "account": account2, "market": held_market, "delta": liquid_held["delta"], "new_par": liquid_held["new_par"]},
            {"owner": owner2.lower(), "account": account2, "market": owed_market, "delta": liquid_owed["delta"], "new_par": liquid_owed["new_par"]},
        ]
    except Exception:
        return None


def decode_current_vaporize_log(log):
    """Decode current replay-style vaporize event."""
    try:
        topics = log["topics"]
        if len(topics) < 3:
            return None
        owner1 = "0x" + topics[1][-40:]
        owner2 = "0x" + topics[2][-40:]
        data = log["data"].replace("0x", "")
        words = [data[i*64:(i+1)*64] for i in range(len(data)//64)]
        if len(words) < 16:
            return None
        account1 = str(decode_uint(words, 0))
        account2 = str(decode_uint(words, 1))
        held_market = str(int(words[2], 16))
        borrowed_market = str(int(words[3], 16))
        solid_held = decode_balance_update_current(words, 4)
        solid_borrowed = decode_balance_update_current(words, 8)
        vapor_borrowed = decode_balance_update_current(words, 12)
        return [
            {"owner": owner1.lower(), "account": account1, "market": held_market, "delta": solid_held["delta"], "new_par": solid_held["new_par"]},
            {"owner": owner1.lower(), "account": account1, "market": borrowed_market, "delta": solid_borrowed["delta"], "new_par": solid_borrowed["new_par"]},
            {"owner": owner2.lower(), "account": account2, "market": borrowed_market, "delta": vapor_borrowed["delta"], "new_par": vapor_borrowed["new_par"]},
        ]
    except Exception:
        return None


def decode_log_entries(log):
    """Decode any known log type into [{owner, market, delta}, ...]."""
    topic0 = (log.get("topics") or [""])[0].lower()
    if topic0 == LOG_DEPOSIT:
        current = decode_current_deposit_withdraw_log(log)
        if current:
            return current
        legacy = decode_legacy_deposit_withdraw_log(log)
        return [legacy] if legacy else None
    if topic0 == LOG_WITHDRAW:
        current = decode_current_deposit_withdraw_log(log)
        if current:
            return current
        legacy = decode_legacy_deposit_withdraw_log(log)
        if legacy:
            legacy["delta"] = -abs(int(legacy["delta"]))
            return [legacy]
        return None
    if topic0 == LEGACY_LOG_TRADE:
        return decode_legacy_trade_log(log)
    if topic0 == LEGACY_LOG_TRANSFER:
        return decode_legacy_transfer_log(log)
    if topic0 == LEGACY_LOG_LIQUIDATE:
        return decode_legacy_liquidate_log(log)
    if topic0 == CURRENT_LOG_TRANSFER:
        return decode_current_transfer_log(log)
    if topic0 == CURRENT_LOG_BUY or topic0 == CURRENT_LOG_SELL:
        return decode_current_buy_sell_log(log)
    if topic0 == CURRENT_LOG_TRADE:
        return decode_current_trade_log(log)
    if topic0 == CURRENT_LOG_LIQUIDATE:
        return decode_current_liquidate_log(log)
    if topic0 == CURRENT_LOG_VAPORIZE:
        return decode_current_vaporize_log(log)
    return None


def save_progress(chain_id, progress):
    """Save scanning progress."""
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_DIR / f"{chain_id}.json", "w") as f:
        json.dump(progress, f)


def scan_chain(chain_id, chain_config, only_chains=None, target_addresses=None):
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
    
    target_addresses = {a.lower() for a in (target_addresses or []) if a}
    target_addr_topics = [_addr_topic(addr) for addr in sorted(target_addresses)]

    output_file = OUTPUT_DIR / f"{chain_id}.json"
    existing_output = {}
    if output_file.exists():
        try:
            with open(output_file) as f:
                existing_output = json.load(f)
        except Exception:
            existing_output = {}

    if target_addresses:
        existing_netflows = existing_output.get("netflows", {}) or {}
        netflows = {addr: data for addr, data in existing_netflows.items() if addr.lower() not in target_addresses}
        filtered_netflows = {}
        cycle_account_state = {}
        cycle_market_state = {}
        cycle_state_enabled = True
        start_block = chain_config["start_block"]
        print(f"  Targeted rescan for {len(target_addresses)} address(es) from block: {start_block:,}")
    else:
        progress = load_progress(chain_id)
        netflows = progress["netflows"]
        filtered_netflows = netflows
        cycle_account_state = progress["cycle_account_state"]
        cycle_market_state = progress["cycle_market_state"]
        cycle_state_enabled = progress["cycle_state_enabled"]
        start_block = progress["last_block"]

        if start_block == 0:
            start_block = chain_config["start_block"]
            print(f"  Starting from block: {start_block:,}")
        else:
            print(f"  Resuming from block: {start_block:,}")
    
    # Scan in chunks
    current = start_block
    total_events = 0
    chunk_size = ADDRESS_FILTER_CHUNK if target_addresses else BLOCK_CHUNK
    
    while current <= latest_block:
        to_block = min(current + chunk_size - 1, latest_block)
        
        try:
            # Fetch all logs. For targeted rescans, filter by indexed owner topics so we can
            # rebuild a single address from genesis without a full-chain rescan.
            if target_addresses:
                topic_one_logs = get_logs(rpcs, rpc_idx, contract, [ALL_EVENTS, target_addr_topics], current, to_block) or []
                topic_two_logs = get_logs(rpcs, rpc_idx, contract, [SECOND_OWNER_EVENTS, None, target_addr_topics], current, to_block) or []
                all_logs = _dedupe_logs([*topic_one_logs, *topic_two_logs])
            else:
                all_logs = get_logs(rpcs, rpc_idx, contract, [ALL_EVENTS], current, to_block) or []
            
            events_in_chunk = len(all_logs)
            total_events += events_in_chunk
            
            # Helper to add flow to netflows
            def add_flow(storage, addr, mid, delta, flow_type):
                if addr not in storage:
                    storage[addr] = {}
                if mid not in storage[addr]:
                    storage[addr][mid] = {"t": "0", "d": "0", "w": "0", "s": "0", "x": "0", "l": "0", "v": "0"}
                # Ensure all keys exist (for old-format data)
                for k in ("d", "w", "s", "x", "l", "v"):
                    if k not in storage[addr][mid]:
                        storage[addr][mid][k] = "0"
                old_t = int(storage[addr][mid]["t"])
                storage[addr][mid]["t"] = str(old_t + delta)
                old_ft = int(storage[addr][mid][flow_type])
                storage[addr][mid][flow_type] = str(old_ft + abs(delta))

            def update_cycle_state(owner, account, market, delta, new_par):
                if not cycle_state_enabled:
                    return
                if account is None or new_par is None:
                    return

                account_key = (owner, str(account), market)
                prev_account_par = cycle_account_state.get(account_key, 0)
                next_account_par = int(new_par)
                par_delta = next_account_par - prev_account_par
                cycle_account_state[account_key] = next_account_par

                market_key = (owner, market)
                state = cycle_market_state.setdefault(market_key, {
                    "endingPar": 0,
                    "peakPar": 0,
                    "totalWei": 0,
                    "suffixCandidates": [],
                })
                state["endingPar"] += par_delta
                if state["endingPar"] > state["peakPar"]:
                    state["peakPar"] = state["endingPar"]
                state["totalWei"] += int(delta)

                candidate = {
                    "balance": state["endingPar"],
                    "prefixWei": state["totalWei"],
                }
                while state["suffixCandidates"] and state["suffixCandidates"][-1]["balance"] >= candidate["balance"]:
                    state["suffixCandidates"].pop()
                state["suffixCandidates"].append(candidate)
            
            # Process all logs
            for log in all_logs:
                topic0 = log["topics"][0].lower()
                flow_type = (
                    "d" if topic0 == LOG_DEPOSIT else
                    "w" if topic0 == LOG_WITHDRAW else
                    "x" if topic0 in (LEGACY_LOG_TRANSFER, CURRENT_LOG_TRANSFER) else
                    "l" if topic0 in (LEGACY_LOG_LIQUIDATE, CURRENT_LOG_LIQUIDATE) else
                    "v" if topic0 == CURRENT_LOG_VAPORIZE else
                    "s"
                )
                entries = decode_log_entries(log)
                if not entries:
                    continue
                for entry in entries:
                    owner = entry["owner"].lower()
                    if target_addresses and owner not in target_addresses:
                        continue
                    storage = filtered_netflows if target_addresses else netflows
                    add_flow(storage, owner, entry["market"], int(entry["delta"]), flow_type)
                    update_cycle_state(
                        owner,
                        entry.get("account"),
                        entry["market"],
                        entry["delta"],
                        entry.get("new_par"),
                    )
            
            pct = ((to_block - start_block) / max(1, latest_block - start_block)) * 100
            blocks_done = to_block - start_block
            if events_in_chunk > 0 or blocks_done % 500_000 < chunk_size:
                addr_count = len(filtered_netflows if target_addresses else netflows)
                print(f"  [{pct:5.1f}%] Block {to_block:,} — {events_in_chunk} events (total: {total_events}, addrs: {addr_count})")
            
            # Save progress periodically
            if not target_addresses and (to_block - start_block) % (chunk_size * 10) < chunk_size:
                payload = (
                    _build_progress_payload(
                        to_block + 1,
                        netflows,
                        cycle_account_state,
                        cycle_market_state,
                    )
                    if cycle_state_enabled
                    else _build_legacy_progress_payload(to_block + 1, netflows)
                )
                save_progress(chain_id, payload)
            
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
    if not target_addresses:
        payload = (
            _build_progress_payload(
                latest_block + 1,
                netflows,
                cycle_account_state,
                cycle_market_state,
            )
            if cycle_state_enabled
            else _build_legacy_progress_payload(latest_block + 1, netflows)
        )
        save_progress(chain_id, payload)

    for (owner, mid), state in cycle_market_state.items():
        storage = filtered_netflows if target_addresses and owner in filtered_netflows else netflows
        if owner not in storage or mid not in storage[owner]:
            continue
        storage_entry = storage[owner][mid]
        storage_entry["endingPar"] = str(state["endingPar"])
        if state["endingPar"] <= 0 or state["peakPar"] <= 0:
            continue

        reset_threshold = state["endingPar"] // 5
        reset_candidate = None
        for candidate in reversed(state["suffixCandidates"]):
            if candidate["balance"] <= 0:
                continue
            if candidate["balance"] <= reset_threshold:
                reset_candidate = candidate
                break
        if reset_candidate is None:
            continue
        recent_netflow = state["totalWei"] - reset_candidate["prefixWei"]
        storage_entry["recentNetFlow"] = str(recent_netflow)
        storage_entry["resetPar"] = str(reset_candidate["balance"])

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if target_addresses:
        netflows.update(filtered_netflows)
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
    parser = argparse.ArgumentParser(description="Scan DolomiteMargin balance-changing events into netflow files")
    parser.add_argument("chains", nargs="?", default="", help="Comma-separated chain ids, e.g. arbitrum,ethereum")
    parser.add_argument("--address", action="append", default=[], help="Targeted owner address to rebuild from genesis")
    args = parser.parse_args()

    only_chains = None
    if args.chains:
        only_chains = [c.strip().lower() for c in args.chains.split(",") if c.strip()]
        print(f"Scanning only: {', '.join(only_chains)}")

    target_addresses = [a.strip().lower() for a in args.address if str(a).strip()]
    if target_addresses:
        print(f"Target addresses: {', '.join(target_addresses)}")
    
    for chain_id, config in CHAINS.items():
        scan_chain(chain_id, config, only_chains, target_addresses=target_addresses)
    
    print(f"\n{'='*60}")
    print("Done! Files written to data/earn-netflow/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
