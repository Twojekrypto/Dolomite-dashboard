#!/usr/bin/env python3
"""
Generate transaction-level Dolomite reward claim events for the History page.

Dolomite subgraphs expose liquidityMiningClaims as aggregate rows without a
transaction hash or timestamp. History needs wallet-level tx evidence, so this
script indexes RewardClaimed logs emitted by each chain's EventEmitterRegistry.
"""
import json
import os
import time
from datetime import datetime, timezone

import requests


SCHEMA_VERSION = 2
REWARD_CLAIMED_TOPIC = "0x7a84a08b02c91f3c62d572853f966fc799bbd121e8ad7833a4494ab8dcfcb404"
GRAPH_BASE = "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs"
ODOLO_CONTRACT = "0x02e513b5b54ee216bf836ceb471507488fc89543"
BERA_ODOLO_DISTRIBUTORS = {
    "0x79e6e932bf6686a4d357d7821e6e08835ba8a026",
}

DEFAULT_LOOKBACK_DAYS = int(os.environ.get("REWARD_CLAIM_LOOKBACK_DAYS", os.environ.get("ODOLO_CLAIM_LOOKBACK_DAYS", "730")))
DEFAULT_CHUNK_SIZE = int(os.environ.get("REWARD_CLAIM_CHUNK_SIZE", os.environ.get("ODOLO_CLAIM_CHUNK_SIZE", "50000")))
MAX_DISTRIBUTOR_PAGES = int(os.environ.get("REWARD_CLAIM_DISTRIBUTOR_PAGES", os.environ.get("ODOLO_CLAIM_DISTRIBUTOR_PAGES", "50")))

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(ROOT_DIR, "data", "reward-claim-events.json")
LEGACY_ODOLO_OUTPUT_JSON = os.path.join(ROOT_DIR, "data", "odolo-claim-events.json")
STATE_FILE = os.path.join(ROOT_DIR, "reward_claim_events_state.json")


CHAIN_CONFIGS = {
    "berachain": {
        "name": "Berachain",
        "subgraph": os.environ.get(
            "DOLOMITE_BERACHAIN_SUBGRAPH",
            "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
        ),
        "eventEmitter": "0x6d40138c99f6d9116f738f44a0e6751a42232486",
        "deployBlock": 3_500_000,
        "blockTimeSeconds": 2,
        "chunkSize": 50_000,
        "fallbackDistributors": BERA_ODOLO_DISTRIBUTORS,
        "token": {"symbol": "oDOLO", "address": ODOLO_CONTRACT, "decimals": 18},
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC") else [os.environ["ALCHEMY_BERACHAIN_RPC"]]),
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC_2") else [os.environ["ALCHEMY_BERACHAIN_RPC_2"]]),
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC_3") else [os.environ["ALCHEMY_BERACHAIN_RPC_3"]]),
            "https://rpc.berachain.com/",
            "https://berachain-rpc.publicnode.com/",
            "https://berachain.drpc.org/",
        ],
    },
    "arbitrum": {
        "name": "Arbitrum",
        "subgraph": os.environ.get(
            "DOLOMITE_ARBITRUM_SUBGRAPH",
            "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
        ),
        "eventEmitter": "0x4bff12773b0dc3cb35f174b5cd351f662018cc2f",
        "deployBlock": 209_000_000,
        "blockTimeSeconds": 0.25,
        "chunkSize": 1_000_000,
        "fallbackDistributors": set(),
        "token": {"symbol": "Reward", "address": "", "decimals": 18},
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_ARBITRUM_RPC") else [os.environ["ALCHEMY_ARBITRUM_RPC"]]),
            *([] if not os.environ.get("ALCHEMY_ARBITRUM_RPC_2") else [os.environ["ALCHEMY_ARBITRUM_RPC_2"]]),
            *([] if not os.environ.get("ALCHEMY_ARBITRUM_RPC_3") else [os.environ["ALCHEMY_ARBITRUM_RPC_3"]]),
            "https://arb1.arbitrum.io/rpc",
            "https://arbitrum.drpc.org/",
            "https://arbitrum-one-rpc.publicnode.com/",
        ],
    },
    "mantle": {
        "name": "Mantle",
        "subgraph": os.environ.get("DOLOMITE_MANTLE_SUBGRAPH", f"{GRAPH_BASE}/dolomite-mantle/latest/gn"),
        "eventEmitter": "0x778cea4ce43ba1a3ed6306ca692b8d9d3dfb827c",
        "deployBlock": 49_000_000,
        "blockTimeSeconds": 2,
        "chunkSize": 500_000,
        "fallbackDistributors": set(),
        "token": {"symbol": "Reward", "address": "", "decimals": 18},
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_MANTLE_RPC") else [os.environ["ALCHEMY_MANTLE_RPC"]]),
            *([] if not os.environ.get("ALCHEMY_MANTLE_RPC_2") else [os.environ["ALCHEMY_MANTLE_RPC_2"]]),
            "https://rpc.mantle.xyz/",
            "https://mantle-rpc.publicnode.com/",
            "https://mantle.drpc.org/",
        ],
    },
    "xlayer": {
        "name": "X Layer",
        "subgraph": os.environ.get("DOLOMITE_XLAYER_SUBGRAPH", f"{GRAPH_BASE}/dolomite-x-layer/latest/gn"),
        "eventEmitter": "0xd86233e2e53a87f0735c5643f3189cfec07269bf",
        "deployBlock": 0,
        "blockTimeSeconds": 2,
        "chunkSize": 250_000,
        "fallbackDistributors": set(),
        "token": {"symbol": "Reward", "address": "", "decimals": 18},
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_XLAYER_RPC") else [os.environ["ALCHEMY_XLAYER_RPC"]]),
            "https://rpc.xlayer.tech/",
            "https://xlayer.drpc.org/",
        ],
    },
}


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def chain_env_key(chain_key):
    return chain_key.upper().replace("-", "_")


def selected_chain_keys():
    raw = os.environ.get("REWARD_CLAIM_CHAINS", "").strip()
    if not raw:
        return list(CHAIN_CONFIGS)
    requested = [item.strip().lower().replace(" ", "") for item in raw.split(",") if item.strip()]
    unknown = [item for item in requested if item not in CHAIN_CONFIGS]
    if unknown:
        raise ValueError(f"Unknown REWARD_CLAIM_CHAINS entries: {', '.join(unknown)}")
    return requested


def rpc_request(rpc_urls, method, params, timeout=30):
    if not rpc_urls:
        raise RuntimeError("no RPC URLs configured")
    last_error = None
    for attempt in range(len(rpc_urls) * 2):
        rpc = rpc_urls[attempt % len(rpc_urls)]
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


def rpc_batch_request(rpc_urls, calls, timeout=30):
    if not calls:
        return []
    if not rpc_urls:
        raise RuntimeError("no RPC URLs configured")
    last_error = None
    for attempt in range(len(rpc_urls) * 2):
        rpc = rpc_urls[attempt % len(rpc_urls)]
        try:
            response = requests.post(
                rpc,
                json=calls,
                timeout=timeout,
                headers={"Content-Type": "application/json"},
            )
            payload = response.json()
            if not isinstance(payload, list):
                raise RuntimeError("RPC batch response was not a list")
            by_id = {item.get("id"): item for item in payload if isinstance(item, dict)}
            results = []
            for call in calls:
                item = by_id.get(call["id"], {})
                if item.get("error"):
                    raise RuntimeError(item["error"].get("message") or "RPC batch item error")
                results.append(item.get("result"))
            return results
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            time.sleep(0.2 + attempt * 0.05)
    raise RuntimeError(f"RPC batch failed: {last_error}")


def graph_query(endpoint, query, variables=None, timeout=30):
    response = requests.post(
        endpoint,
        json={"query": query, "variables": variables or {}},
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    )
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return payload.get("data") or {}


def get_current_block(config):
    return int(rpc_request(config["rpcUrls"], "eth_blockNumber", [], timeout=10), 16)


def normalize_address(address):
    value = str(address or "").strip().lower()
    if not value.startswith("0x"):
        value = "0x" + value
    return value


def is_address(address):
    value = normalize_address(address)
    if len(value) != 42:
        return False
    try:
        int(value[2:], 16)
        return True
    except ValueError:
        return False


def fetch_claim_distributors(config):
    distributors = {
        normalize_address(address)
        for address in config.get("fallbackDistributors", set())
        if is_address(address)
    }
    query = """
    query ClaimDistributors($first: Int!, $skip: Int!) {
      liquidityMiningClaims(first: $first, skip: $skip, orderBy: id, orderDirection: asc) {
        distributor
      }
    }
    """
    first = 1000
    try:
        for page in range(MAX_DISTRIBUTOR_PAGES):
            rows = graph_query(config["subgraph"], query, {"first": first, "skip": page * first}).get("liquidityMiningClaims") or []
            for row in rows:
                address = row.get("distributor")
                if is_address(address):
                    distributors.add(normalize_address(address))
            if len(rows) < first:
                break
    except (requests.RequestException, ValueError, RuntimeError) as exc:
        print(f"Warning: {config['name']} claim distributor discovery failed, using fallback list: {exc}")

    return sorted(distributors)


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


def env_int(*names):
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return int(value)
    return None


def fetch_reward_claimed_logs(chain_key, config, start_block, end_block, distributors):
    if start_block > end_block:
        return []
    distributor_topics = [topic_address(distributor) for distributor in distributors]
    if not distributor_topics:
        return []
    configured_chunk_size = int(config.get("chunkSize") or DEFAULT_CHUNK_SIZE)
    chunk_size = max(1000, configured_chunk_size)
    logs = []
    current = start_block
    total_blocks = max(1, end_block - start_block + 1)
    print(f"Scanning {config['name']} reward claims for {len(distributor_topics):,} distributors: blocks {start_block:,} -> {end_block:,}")

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)
        success = False
        last_error = None
        for _attempt in range(len(config["rpcUrls"]) * 2):
            try:
                result = rpc_request(
                    config["rpcUrls"],
                    "eth_getLogs",
                    [{
                        "address": config["eventEmitter"],
                        "fromBlock": hex(current),
                        "toBlock": hex(chunk_end),
                        "topics": [REWARD_CLAIMED_TOPIC, distributor_topics],
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
            print(f"Warning: skipped {chain_key} claim-log chunk at block {current:,}: {last_error}")
            current = chunk_end + 1
            continue

        current = chunk_end + 1
        if chunk_size < configured_chunk_size:
            chunk_size = min(chunk_size * 2, configured_chunk_size)
        if current > end_block or len(logs) % 50 == 0:
            pct = min(100, (current - start_block) * 100 // total_blocks)
            print(f"  {config['name']}: {pct}% scanned, {len(logs):,} claim logs")
        time.sleep(0.04)

    return logs


def fetch_block_timestamps(config, block_numbers):
    timestamps = {}
    blocks = sorted(set(block_numbers))
    batch_size = int(config.get("timestampBatchSize") or 100)
    for i in range(0, len(blocks), batch_size):
        chunk = blocks[i:i + batch_size]
        calls = [
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(block), False],
                "id": idx + 1,
            }
            for idx, block in enumerate(chunk)
        ]
        try:
            results = rpc_batch_request(config["rpcUrls"], calls, timeout=30)
            for block, result in zip(chunk, results):
                timestamps[block] = int(result.get("timestamp", "0x0"), 16) if result else 0
        except RuntimeError as exc:
            print(f"Warning: {config['name']} batch timestamps unavailable for {len(chunk)} blocks: {exc}")
            for block in chunk:
                try:
                    result = rpc_request(config["rpcUrls"], "eth_getBlockByNumber", [hex(block), False], timeout=15)
                    timestamps[block] = int(result.get("timestamp", "0x0"), 16) if result else 0
                except RuntimeError as single_exc:
                    print(f"Warning: {config['name']} timestamp unavailable for block {block}: {single_exc}")
                    timestamps[block] = 0
                time.sleep(0.02)
        time.sleep(0.02)
    return timestamps


def fetch_block_timestamp(config, block_number):
    if block_number is None:
        return 0
    return fetch_block_timestamps(config, [block_number]).get(block_number, 0)


def claim_events_from_logs(chain_key, config, logs):
    decoded = []
    block_numbers = []
    token = config.get("token", {})
    decimals = int(token.get("decimals") or 18)
    for log in logs:
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue
        data = decode_claim_data(log.get("data"))
        if not data:
            continue
        epoch, amount_wei = data
        distributor = decode_topic_address(topics[1])
        user = decode_topic_address(topics[2])
        if not is_address(distributor) or not is_address(user):
            continue
        block_number = int(log.get("blockNumber", "0x0"), 16)
        block_numbers.append(block_number)
        decoded.append({
            "chainKey": chain_key,
            "chainName": config["name"],
            "txHash": str(log.get("transactionHash", "")).lower(),
            "blockNumber": block_number,
            "timestamp": 0,
            "logIndex": int(log.get("logIndex", "0x0"), 16),
            "user": user,
            "distributor": distributor,
            "epoch": epoch,
            "seasonNumber": "",
            "amountWei": str(amount_wei),
            "amount": format_units(amount_wei, decimals),
            "tokenSymbol": token.get("symbol") or "Reward",
            "tokenAddress": normalize_address(token.get("address")) if token.get("address") else "",
            "tokenDecimals": decimals,
            "source": "RewardClaimed",
        })

    timestamps = fetch_block_timestamps(config, block_numbers)
    for event in decoded:
        event["timestamp"] = timestamps.get(event["blockNumber"], 0)
    return decoded


def event_key(event):
    return f"{event.get('chainKey', '')}:{str(event.get('txHash', '')).lower()}:{event.get('logIndex', 0)}"


def merge_events(existing_events, new_events):
    merged = {}
    for event in (existing_events or []) + (new_events or []):
        key = event_key(event)
        if key.count(":") < 2 or key.endswith(":"):
            continue
        merged[key] = event
    return sorted(merged.values(), key=lambda row: (row.get("timestamp") or 0, row.get("chainKey") or "", row.get("logIndex") or 0), reverse=True)


def seed_events_from_legacy_odolo(existing):
    if not isinstance(existing, dict):
        return []
    events = []
    for event in existing.get("events", []) or []:
        copied = dict(event)
        copied.setdefault("chainKey", "berachain")
        copied.setdefault("chainName", "Berachain")
        copied.setdefault("tokenDecimals", 18)
        events.append(copied)
    return events


def existing_events_for_chain(existing, chain_key):
    events = existing.get("events", []) if isinstance(existing, dict) else []
    return [event for event in events if event.get("chainKey") == chain_key]


def chain_meta(existing, chain_key):
    if not isinstance(existing, dict):
        return {}
    return (existing.get("chains") or {}).get(chain_key) or {}


def scan_bounds_for_chain(chain_key, config, current_block, state, existing):
    deploy_block = int(config.get("deployBlock") or 0)
    env_key = chain_env_key(chain_key)
    env_start = env_int(f"REWARD_CLAIM_START_BLOCK_{env_key}", "REWARD_CLAIM_START_BLOCK")
    if chain_key == "berachain":
        env_start = env_start if env_start is not None else env_int("ODOLO_CLAIM_START_BLOCK")
    env_end = env_int(f"REWARD_CLAIM_END_BLOCK_{env_key}", "REWARD_CLAIM_END_BLOCK")
    force_full = os.environ.get("REWARD_CLAIM_FORCE_FULL", os.environ.get("ODOLO_CLAIM_FORCE_FULL", "")).lower() in {"1", "true", "yes"}
    lookback_blocks = int(DEFAULT_LOOKBACK_DAYS * 86400 / float(config.get("blockTimeSeconds") or 1))
    default_start = max(deploy_block, current_block - lookback_blocks)

    meta = chain_meta(existing, chain_key)
    existing_rows = existing_events_for_chain(existing, chain_key)
    existing_from_block = int(meta.get("fromBlock") or 0)
    existing_to_block = int(meta.get("toBlock") or 0)
    existing_is_full_coverage = existing_from_block and existing_from_block <= deploy_block
    state_chain = (state.get("chains") or {}).get(chain_key, {}) if isinstance(state, dict) else {}
    state_is_usable = state.get("schemaVersion") == SCHEMA_VERSION and int(state_chain.get("lastBlock", 0) or 0) > 0

    if env_start is not None:
        start_block = max(deploy_block, env_start)
    elif force_full:
        start_block = deploy_block
    elif existing_rows and not existing_is_full_coverage:
        start_block = deploy_block
    elif state_is_usable:
        start_block = max(deploy_block, int(state_chain.get("lastBlock", 0)) + 1)
    elif existing_is_full_coverage and existing_to_block > 0:
        start_block = existing_to_block + 1
    else:
        start_block = default_start

    end_block = min(current_block, env_end) if env_end is not None else current_block
    return start_block, end_block


def build_chain_payload(chain_key, config, current_block, start_block, end_block, existing, distributors, events, warning=""):
    meta = chain_meta(existing, chain_key)
    existing_from_block = int(meta.get("fromBlock") or 0)
    existing_to_block = int(meta.get("toBlock") or 0)
    event_blocks = [
        int(event.get("blockNumber") or 0)
        for event in events
        if event.get("chainKey") == chain_key and int(event.get("blockNumber") or 0) > 0
    ]
    from_candidates = [start_block]
    if existing_from_block:
        from_candidates.append(existing_from_block)
    if event_blocks:
        from_candidates.append(min(event_blocks))
    coverage_from_block = min(block for block in from_candidates if block is not None)
    coverage_to_block = max([block for block in [end_block, existing_to_block, max(event_blocks) if event_blocks else 0] if block is not None])
    event_distributors = sorted({
        normalize_address(event.get("distributor"))
        for event in events
        if event.get("chainKey") == chain_key and is_address(event.get("distributor"))
    })
    output_distributors = sorted(set(distributors) | set(event_distributors))
    from_timestamp = (
        int(meta.get("fromTimestamp") or 0)
        if coverage_from_block == existing_from_block and meta.get("fromTimestamp")
        else fetch_block_timestamp(config, coverage_from_block)
    )
    to_timestamp = (
        int(meta.get("toTimestamp") or 0)
        if coverage_to_block == existing_to_block and meta.get("toTimestamp")
        else fetch_block_timestamp(config, coverage_to_block)
    )
    return {
        "chainKey": chain_key,
        "chainName": config["name"],
        "eventEmitter": normalize_address(config["eventEmitter"]),
        "fromBlock": coverage_from_block,
        "toBlock": coverage_to_block if coverage_to_block is not None else current_block,
        "fromTimestamp": from_timestamp,
        "toTimestamp": to_timestamp,
        "distributors": output_distributors,
        "token": config.get("token", {}),
        "eventCount": sum(1 for event in events if event.get("chainKey") == chain_key),
        "warning": warning,
    }


def build_chain_payload_from_events(chain_key, config, events):
    chain_events = [event for event in events if event.get("chainKey") == chain_key]
    block_numbers = [int(event.get("blockNumber") or 0) for event in chain_events if int(event.get("blockNumber") or 0) > 0]
    timestamps = [int(event.get("timestamp") or 0) for event in chain_events if int(event.get("timestamp") or 0) > 0]
    distributors = sorted({
        normalize_address(event.get("distributor"))
        for event in chain_events
        if is_address(event.get("distributor"))
    })
    return {
        "chainKey": chain_key,
        "chainName": config["name"],
        "eventEmitter": normalize_address(config["eventEmitter"]),
        "fromBlock": min(block_numbers) if block_numbers else 0,
        "toBlock": max(block_numbers) if block_numbers else 0,
        "fromTimestamp": min(timestamps) if timestamps else 0,
        "toTimestamp": max(timestamps) if timestamps else 0,
        "distributors": distributors,
        "token": config.get("token", {}),
        "eventCount": len(chain_events),
        "warning": "Seeded from existing reward-claim event file; next workflow scan refreshes full chain coverage.",
    }


def build_legacy_odolo_payload(payload):
    chain = (payload.get("chains") or {}).get("berachain") or {}
    events = [
        {key: value for key, value in event.items() if key not in {"chainKey", "chainName", "tokenDecimals"}}
        for event in payload.get("events", [])
        if event.get("chainKey") == "berachain"
    ]
    distributors = chain.get("distributors") or sorted({
        normalize_address(event.get("distributor"))
        for event in events
        if is_address(event.get("distributor"))
    })
    token = chain.get("token") or {"symbol": "oDOLO", "address": ODOLO_CONTRACT, "decimals": 18}
    return {
        "schemaVersion": 1,
        "generatedAt": payload.get("generatedAt") or utc_now_iso(),
        "chainKey": "berachain",
        "source": "Berachain RewardClaimed logs for Dolomite oDOLO rewards",
        "fromBlock": chain.get("fromBlock") or 0,
        "toBlock": chain.get("toBlock") or 0,
        "fromTimestamp": chain.get("fromTimestamp") or 0,
        "toTimestamp": chain.get("toTimestamp") or 0,
        "eventEmitter": chain.get("eventEmitter") or CHAIN_CONFIGS["berachain"]["eventEmitter"],
        "distributor": distributors[0] if len(distributors) == 1 else "",
        "distributors": distributors,
        "token": token,
        "events": events,
    }


def main():
    print("=" * 60)
    print("Dolomite reward claim events")
    print(f"Generated at {utc_now_iso()}")
    print("=" * 60)

    state = load_json(STATE_FILE, {})
    existing = load_json(OUTPUT_JSON, {})
    if not existing:
        existing = {
            "schemaVersion": SCHEMA_VERSION,
            "chains": {},
            "events": seed_events_from_legacy_odolo(load_json(LEGACY_ODOLO_OUTPUT_JSON, {})),
        }

    all_events = existing.get("events", []) if isinstance(existing, dict) else []
    chains_payload = dict(existing.get("chains") or {}) if isinstance(existing, dict) else {}
    state_chains = dict(state.get("chains") or {}) if isinstance(state, dict) else {}

    for chain_key in selected_chain_keys():
        config = CHAIN_CONFIGS[chain_key]
        try:
            current_block = get_current_block(config)
            start_block, end_block = scan_bounds_for_chain(chain_key, config, current_block, state, existing)
            existing_distributors = {
                normalize_address(address)
                for address in (chain_meta(existing, chain_key).get("distributors") or [])
                if is_address(address)
            }
            distributors = sorted(set(fetch_claim_distributors(config)) | existing_distributors)
            new_logs = fetch_reward_claimed_logs(chain_key, config, start_block, end_block, distributors)
            new_events = claim_events_from_logs(chain_key, config, new_logs)
            all_events = merge_events(all_events, new_events)
            chains_payload[chain_key] = build_chain_payload(
                chain_key,
                config,
                current_block,
                start_block,
                end_block,
                existing,
                distributors,
                all_events,
            )
            state_chains[chain_key] = {
                "lastBlock": end_block,
                "eventCount": chains_payload[chain_key]["eventCount"],
            }
            print(f"Saved {chains_payload[chain_key]['eventCount']:,} {config['name']} reward claim events")
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            print(f"Warning: {config['name']} reward claim scan failed: {exc}")
            meta = chain_meta(existing, chain_key)
            if meta:
                meta = dict(meta)
                meta["warning"] = f"Latest scan failed: {exc}"
                chains_payload[chain_key] = meta

    for chain_key in sorted({event.get("chainKey") for event in all_events if event.get("chainKey")}):
        if chain_key not in chains_payload and chain_key in CHAIN_CONFIGS:
            chains_payload[chain_key] = build_chain_payload_from_events(chain_key, CHAIN_CONFIGS[chain_key], all_events)

    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": utc_now_iso(),
        "protocol": "Dolomite",
        "source": "Dolomite EventEmitterRegistry RewardClaimed logs",
        "methodology": "Reward claim transactions are indexed from EventEmitterRegistry RewardClaimed logs and matched to wallet, chain, tx hash, log index, epoch and amount.",
        "chains": chains_payload,
        "events": merge_events([], all_events),
    }
    save_json(OUTPUT_JSON, payload)
    save_json(LEGACY_ODOLO_OUTPUT_JSON, build_legacy_odolo_payload(payload))
    save_json(STATE_FILE, {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": payload["generatedAt"],
        "eventCount": len(payload["events"]),
        "chains": state_chains,
    })

    print(f"Saved {len(payload['events']):,} reward claim events to {OUTPUT_JSON}")
    print(f"Saved Berachain oDOLO compatibility file to {LEGACY_ODOLO_OUTPUT_JSON}")


if __name__ == "__main__":
    main()
