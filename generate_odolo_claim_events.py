#!/usr/bin/env python3
"""
Generate dated oDOLO reward claim events for the History page.

Dolomite's subgraph exposes liquidityMiningClaims as aggregate rows without a
transaction hash or timestamp. The History page needs transaction-level claim
evidence, so this script indexes RewardClaimed logs from Berachain.
"""
import json
import os
import time
from datetime import datetime, timezone

import requests


SCHEMA_VERSION = 1
ODOLO_CONTRACT = "0x02e513b5b54ee216bf836ceb471507488fc89543"
REWARDS_DISTRIBUTOR = "0x79e6e932bf6686a4d357d7821e6e08835ba8a026"
EVENT_EMITTER = "0x6d40138c99f6d9116f738f44a0e6751a42232486"
REWARD_CLAIMED_TOPIC = "0x7a84a08b02c91f3c62d572853f966fc799bbd121e8ad7833a4494ab8dcfcb404"
DEPLOY_BLOCK = 3_500_000
BLOCK_TIME_SECONDS = 2
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("ODOLO_CLAIM_LOOKBACK_DAYS", "370"))
DEFAULT_CHUNK_SIZE = int(os.environ.get("ODOLO_CLAIM_CHUNK_SIZE", "50000"))

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(ROOT_DIR, "data", "odolo-claim-events.json")
STATE_FILE = os.path.join(ROOT_DIR, "odolo_claim_events_state.json")

RPC_URLS = [
    *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC") else [os.environ["ALCHEMY_BERACHAIN_RPC"]]),
    *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC_2") else [os.environ["ALCHEMY_BERACHAIN_RPC_2"]]),
    *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC_3") else [os.environ["ALCHEMY_BERACHAIN_RPC_3"]]),
    "https://rpc.berachain.com/",
    "https://berachain-rpc.publicnode.com/",
    "https://berachain.drpc.org/",
]


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rpc_request(method, params, timeout=30):
    last_error = None
    for attempt in range(len(RPC_URLS) * 2):
        rpc = RPC_URLS[attempt % len(RPC_URLS)]
        try:
            response = requests.post(
                rpc,
                json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
                timeout=timeout,
                headers={"Content-Type": "application/json"},
            )
            payload = response.json()
            if payload.get("error"):
                raise RuntimeError(payload["error"].get("message") or "RPC error")
            return payload.get("result")
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            time.sleep(0.2 + attempt * 0.05)
    raise RuntimeError(f"{method} failed: {last_error}")


def get_current_block():
    return int(rpc_request("eth_blockNumber", [], timeout=10), 16)


def topic_address(address):
    value = str(address).lower().replace("0x", "")
    return "0x" + value.rjust(64, "0")


def decode_topic_address(topic):
    value = str(topic or "")
    if len(value) < 66:
        return ""
    return "0x" + value[-40:].lower()


def decode_claim_data(data):
    raw = str(data or "0x")[2:]
    if len(raw) < 128:
        return None
    epoch = int(raw[:64], 16)
    amount_wei = int(raw[64:128], 16)
    return epoch, amount_wei


def format_units(value, decimals=18):
    negative = value < 0
    value = abs(int(value))
    scale = 10 ** decimals
    whole = value // scale
    fraction = str(value % scale).rjust(decimals, "0").rstrip("0")
    return f"{'-' if negative else ''}{whole}{'.' + fraction if fraction else ''}"


def load_json(path, fallback):
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Warning: could not read {path}: {exc}")
        return fallback


def save_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")
    os.replace(tmp, path)


def fetch_reward_claimed_logs(start_block, end_block):
    if start_block > end_block:
        return []
    chunk_size = max(1000, DEFAULT_CHUNK_SIZE)
    logs = []
    current = start_block
    total_blocks = max(1, end_block - start_block + 1)
    print(f"Scanning oDOLO reward claims: blocks {start_block:,} -> {end_block:,}")

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)
        success = False
        last_error = None
        for attempt in range(len(RPC_URLS) * 2):
            try:
                result = rpc_request(
                    "eth_getLogs",
                    [{
                        "address": EVENT_EMITTER,
                        "fromBlock": hex(current),
                        "toBlock": hex(chunk_end),
                        "topics": [REWARD_CLAIMED_TOPIC, topic_address(REWARDS_DISTRIBUTOR)],
                    }],
                    timeout=35,
                )
                logs.extend(result or [])
                success = True
                break
            except RuntimeError as exc:
                last_error = exc
                message = str(exc).lower()
                if "range" in message or "limit" in message or "too many" in message:
                    chunk_size = max(chunk_size // 2, 1000)
                    chunk_end = min(current + chunk_size - 1, end_block)
                    continue
                time.sleep(0.5)
        if not success:
            print(f"Warning: skipped claim-log chunk at block {current:,}: {last_error}")
            current = chunk_end + 1
            continue

        current = chunk_end + 1
        if chunk_size < DEFAULT_CHUNK_SIZE:
            chunk_size = min(chunk_size * 2, DEFAULT_CHUNK_SIZE)
        if current > end_block or len(logs) % 50 == 0:
            pct = min(100, (current - start_block) * 100 // total_blocks)
            print(f"  {pct}% scanned, {len(logs):,} claim logs")
        time.sleep(0.04)

    return logs


def fetch_block_timestamps(block_numbers):
    timestamps = {}
    for block in sorted(set(block_numbers)):
        try:
            result = rpc_request("eth_getBlockByNumber", [hex(block), False], timeout=15)
            timestamps[block] = int(result.get("timestamp", "0x0"), 16) if result else 0
        except RuntimeError as exc:
            print(f"Warning: timestamp unavailable for block {block}: {exc}")
            timestamps[block] = 0
        time.sleep(0.02)
    return timestamps


def fetch_block_timestamp(block_number):
    return fetch_block_timestamps([block_number]).get(block_number, 0)


def claim_events_from_logs(logs):
    decoded = []
    block_numbers = []
    for log in logs:
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue
        data = decode_claim_data(log.get("data"))
        if not data:
            continue
        epoch, amount_wei = data
        user = decode_topic_address(topics[2])
        block_number = int(log.get("blockNumber", "0x0"), 16)
        block_numbers.append(block_number)
        decoded.append({
            "txHash": str(log.get("transactionHash", "")).lower(),
            "blockNumber": block_number,
            "timestamp": 0,
            "logIndex": int(log.get("logIndex", "0x0"), 16),
            "user": user,
            "distributor": REWARDS_DISTRIBUTOR,
            "epoch": epoch,
            "amountWei": str(amount_wei),
            "amount": format_units(amount_wei, 18),
            "tokenSymbol": "oDOLO",
            "tokenAddress": ODOLO_CONTRACT,
            "source": "RewardClaimed",
        })

    timestamps = fetch_block_timestamps(block_numbers)
    for event in decoded:
        event["timestamp"] = timestamps.get(event["blockNumber"], 0)
    return decoded


def merge_events(existing_events, new_events):
    merged = {}
    for event in (existing_events or []) + (new_events or []):
        key = f"{str(event.get('txHash', '')).lower()}:{event.get('logIndex', 0)}"
        if not key.strip(":"):
            continue
        merged[key] = event
    return sorted(merged.values(), key=lambda row: (row.get("timestamp") or 0, row.get("logIndex") or 0), reverse=True)


def main():
    print("=" * 60)
    print("oDOLO claim events")
    print(f"Generated at {utc_now_iso()}")
    print("=" * 60)

    current_block = get_current_block()
    state = load_json(STATE_FILE, {})
    existing = load_json(OUTPUT_JSON, {})
    existing_events = existing.get("events", []) if isinstance(existing, dict) else []

    env_start = os.environ.get("ODOLO_CLAIM_START_BLOCK")
    force_full = os.environ.get("ODOLO_CLAIM_FORCE_FULL", "").lower() in {"1", "true", "yes"}
    lookback_blocks = DEFAULT_LOOKBACK_DAYS * 86400 // BLOCK_TIME_SECONDS
    default_start = max(DEPLOY_BLOCK, current_block - lookback_blocks)

    if env_start:
        start_block = max(DEPLOY_BLOCK, int(env_start))
    elif force_full or state.get("schemaVersion") != SCHEMA_VERSION:
        start_block = default_start
    else:
        start_block = max(default_start, int(state.get("lastBlock", default_start)) + 1)

    end_block = current_block
    new_logs = fetch_reward_claimed_logs(start_block, end_block)
    new_events = claim_events_from_logs(new_logs)
    all_events = merge_events(existing_events, new_events)

    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": utc_now_iso(),
        "chainKey": "berachain",
        "source": "Berachain RewardClaimed logs for Dolomite oDOLO rewards",
        "fromBlock": min([event.get("blockNumber", end_block) for event in all_events] + [start_block]),
        "toBlock": end_block,
        "fromTimestamp": fetch_block_timestamp(start_block),
        "toTimestamp": fetch_block_timestamp(end_block),
        "eventEmitter": EVENT_EMITTER,
        "distributor": REWARDS_DISTRIBUTOR,
        "token": {
            "symbol": "oDOLO",
            "address": ODOLO_CONTRACT,
            "decimals": 18,
        },
        "events": all_events,
    }
    save_json(OUTPUT_JSON, payload)
    save_json(STATE_FILE, {
        "schemaVersion": SCHEMA_VERSION,
        "lastBlock": end_block,
        "generatedAt": payload["generatedAt"],
        "eventCount": len(all_events),
    })

    print(f"Saved {len(all_events):,} oDOLO claim events to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
