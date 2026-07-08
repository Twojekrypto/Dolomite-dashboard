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
    "0xd88f473832b0403c7736ef237af5aff8759b99ef",
}
ARB_MIN_DISTRIBUTOR = "0x2e3d10cc42227af0ce908f00c76ffe1de1728b4b"
ARB_OARB_DISTRIBUTOR = "0x66cd7d0cc677f42f6662622c60a5e60ef573db67"

ERC20_SYMBOL_SELECTOR = "0x95d89b41"
ERC20_NAME_SELECTOR = "0x06fdde03"
ERC20_DECIMALS_SELECTOR = "0x313ce567"
TOKEN_RESOLVER_SELECTORS = (
    ("token", "0xfc0c546a"),
    ("oARB", "0xe8616b24"),
    ("rewardToken", "0xf7c618c1"),
    ("REWARD_TOKEN", "0x99248ea7"),
    ("rewardTokenAddress", "0x125f9e33"),
    ("rewardsToken", "0xd1af0c7d"),
)

DEFAULT_LOOKBACK_DAYS = int(os.environ.get("REWARD_CLAIM_LOOKBACK_DAYS", os.environ.get("ODOLO_CLAIM_LOOKBACK_DAYS", "730")))
DEFAULT_CHUNK_SIZE = int(os.environ.get("REWARD_CLAIM_CHUNK_SIZE", os.environ.get("ODOLO_CLAIM_CHUNK_SIZE", "50000")))
MAX_DISTRIBUTOR_PAGES = int(os.environ.get("REWARD_CLAIM_DISTRIBUTOR_PAGES", os.environ.get("ODOLO_CLAIM_DISTRIBUTOR_PAGES", "50")))

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(ROOT_DIR, "data", "reward-claim-events.json")
LEGACY_ODOLO_OUTPUT_JSON = os.path.join(ROOT_DIR, "data", "odolo-claim-events.json")
CHAIN_OUTPUT_DIR = os.path.join(ROOT_DIR, "data", "reward-claim-events")
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
        "knownDistributorTokens": {
            distributor: {"symbol": "oDOLO", "address": ODOLO_CONTRACT, "decimals": 18}
            for distributor in BERA_ODOLO_DISTRIBUTORS
        },
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC") else [os.environ["ALCHEMY_BERACHAIN_RPC"]]),
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC_2") else [os.environ["ALCHEMY_BERACHAIN_RPC_2"]]),
            *([] if not os.environ.get("ALCHEMY_BERACHAIN_RPC_3") else [os.environ["ALCHEMY_BERACHAIN_RPC_3"]]),
            "https://berachain-rpc.publicnode.com/",
            "https://rpc.berachain.com/",
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
        "knownDistributorTokens": {
            ARB_MIN_DISTRIBUTOR: {"symbol": "MIN", "address": "0x946f4a316e8ae3c7fdcdf86e84496c3ee3fbf26d", "decimals": 18},
            ARB_OARB_DISTRIBUTOR: {"symbol": "oARB", "address": "0xcbed801b4162bf2a19b06968663438b5165a6a93", "decimals": 18},
        },
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_ARBITRUM_RPC_KAT") else [os.environ["ALCHEMY_ARBITRUM_RPC_KAT"]]),
            *([] if not os.environ.get("ALCHEMY_ARBITRUM_RPC_DAN") else [os.environ["ALCHEMY_ARBITRUM_RPC_DAN"]]),
            *([] if not os.environ.get("ALCHEMY_ARBITRUM_RPC_ZEN") else [os.environ["ALCHEMY_ARBITRUM_RPC_ZEN"]]),
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
        "knownDistributorTokens": {},
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_MANTLE_RPC") else [os.environ["ALCHEMY_MANTLE_RPC"]]),
            *([] if not os.environ.get("ALCHEMY_MANTLE_RPC_2") else [os.environ["ALCHEMY_MANTLE_RPC_2"]]),
            "https://rpc.mantle.xyz",
            "https://mantle-rpc.publicnode.com/",
            "https://mantle.drpc.org/",
        ],
    },
    "xlayer": {
        "name": "X Layer",
        "subgraph": os.environ.get("DOLOMITE_XLAYER_SUBGRAPH", f"{GRAPH_BASE}/dolomite-x-layer/latest/gn"),
        "eventEmitter": "0xd86233e2e53a87f0735c5643f3189cfec07269bf",
        "deployBlock": 850_676,
        "blockTimeSeconds": 2,
        "chunkSize": 250_000,
        "requiresConfiguredRpcForFullClaimScan": True,
        "fallbackDistributors": set(),
        "token": {"symbol": "Reward", "address": "", "decimals": 18},
        "knownDistributorTokens": {},
        "rpcUrls": [
            *([] if not os.environ.get("ALCHEMY_XLAYER_RPC_ZEN") else [os.environ["ALCHEMY_XLAYER_RPC_ZEN"]]),
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


def has_configured_rpc(chain_key):
    env_key = chain_env_key(chain_key)
    return any(
        os.environ.get(f"ALCHEMY_{env_key}_RPC{suffix}")
        for suffix in ("_ZEN", "", "_2", "_3")
    )


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


def decode_call_address(result):
    if not result or result == "0x" or len(result) < 66:
        return ""
    address = "0x" + result[-40:].lower()
    return address if is_address(address) and int(address[2:], 16) != 0 else ""


def decode_abi_string(result):
    if not result or result == "0x":
        return ""
    try:
        raw = bytes.fromhex(result[2:])
    except ValueError:
        return ""
    try:
        if len(raw) >= 64:
            offset = int.from_bytes(raw[:32], "big")
            length = int.from_bytes(raw[offset:offset + 32], "big")
            value = raw[offset + 32:offset + 32 + length].decode("utf-8", "replace").rstrip("\x00")
            if value:
                return value
    except (OverflowError, UnicodeDecodeError, ValueError):
        return ""
    return raw.rstrip(b"\x00").decode("utf-8", "replace").strip()


def normalize_token_metadata(token):
    symbol = str((token or {}).get("symbol") or "").strip()
    address = normalize_address((token or {}).get("address")) if (token or {}).get("address") else ""
    try:
        decimals = int((token or {}).get("decimals") if (token or {}).get("decimals") is not None else 18)
    except (TypeError, ValueError):
        decimals = 18
    metadata = {"symbol": symbol or "Reward", "address": address if is_address(address) else "", "decimals": decimals}
    name = str((token or {}).get("name") or "").strip()
    if name:
        metadata["name"] = name
    return metadata


def fetch_erc20_metadata(config, token_address):
    token_address = normalize_address(token_address)
    if not is_address(token_address):
        raise RuntimeError("invalid token address")
    symbol = decode_abi_string(rpc_request(config["rpcUrls"], "eth_call", [{"to": token_address, "data": ERC20_SYMBOL_SELECTOR}, "latest"], timeout=10))
    decimals_result = rpc_request(config["rpcUrls"], "eth_call", [{"to": token_address, "data": ERC20_DECIMALS_SELECTOR}, "latest"], timeout=10)
    if not decimals_result or decimals_result == "0x":
        raise RuntimeError(f"token decimals unavailable for {token_address}")
    decimals = int(decimals_result, 16)
    name = ""
    try:
        name = decode_abi_string(rpc_request(config["rpcUrls"], "eth_call", [{"to": token_address, "data": ERC20_NAME_SELECTOR}, "latest"], timeout=10))
    except RuntimeError:
        name = ""
    if not symbol:
        raise RuntimeError(f"token symbol unavailable for {token_address}")
    metadata = {"symbol": symbol, "address": token_address, "decimals": decimals}
    if name:
        metadata["name"] = name
    return metadata


def resolve_distributor_token(config, distributor):
    distributor = normalize_address(distributor)
    known = {
        normalize_address(address): normalize_token_metadata(token)
        for address, token in (config.get("knownDistributorTokens") or {}).items()
        if is_address(address)
    }
    if distributor in known:
        return known[distributor]

    for label, selector in TOKEN_RESOLVER_SELECTORS:
        try:
            result = rpc_request(config["rpcUrls"], "eth_call", [{"to": distributor, "data": selector}, "latest"], timeout=10)
            token_address = decode_call_address(result)
            if not token_address:
                continue
            token = fetch_erc20_metadata(config, token_address)
            token["resolver"] = label
            return normalize_token_metadata(token)
        except RuntimeError:
            continue

    return normalize_token_metadata(config.get("token", {}))


def resolve_distributor_tokens(config, distributors):
    resolved = {}
    for distributor in sorted({normalize_address(address) for address in distributors if is_address(address)}):
        token = resolve_distributor_token(config, distributor)
        resolved[distributor] = token
        label = token.get("symbol") or "Reward"
        address = token.get("address") or "unknown token"
        print(f"  {config['name']}: distributor {distributor} -> {label} ({address})")
    return resolved


def token_for_distributor(config, distributor_tokens, distributor):
    distributor = normalize_address(distributor)
    return normalize_token_metadata(distributor_tokens.get(distributor) or config.get("token", {}))


def fetch_claim_distributors(config):
    distributors = {
        normalize_address(address)
        for address in set(config.get("fallbackDistributors", set())) | set((config.get("knownDistributorTokens") or {}).keys())
        if is_address(address)
    }
    query_template = """
    query ClaimDistributors($first: Int!, $skip: Int!) {
      liquidityMiningClaims(first: $first, skip: $skip, orderBy: id, orderDirection: %s) {
        distributor
      }
    }
    """
    first = 1000
    for direction in ("asc", "desc"):
        query = query_template % direction
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
            print(f"Warning: {config['name']} {direction} claim distributor discovery failed, using partial list: {exc}")

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


def save_json(path, payload, compact=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as file:
        if compact:
            json.dump(payload, file, separators=(",", ":"), sort_keys=True)
        else:
            json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")
    os.replace(tmp, path)


def env_int(*names):
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return int(value)
    return None


def claim_chunk_size(chain_key, config):
    env_key = chain_env_key(chain_key)
    configured = env_int(f"REWARD_CLAIM_CHUNK_SIZE_{env_key}", "REWARD_CLAIM_CHUNK_SIZE")
    return int(configured or config.get("chunkSize") or DEFAULT_CHUNK_SIZE)


def timestamp_batch_size(chain_key, config):
    env_key = chain_env_key(chain_key)
    configured = env_int(f"REWARD_CLAIM_TIMESTAMP_BATCH_SIZE_{env_key}", "REWARD_CLAIM_TIMESTAMP_BATCH_SIZE")
    return int(configured or config.get("timestampBatchSize") or 100)


def distributor_batch_size(chain_key, config):
    env_key = chain_env_key(chain_key)
    configured = env_int(f"REWARD_CLAIM_DISTRIBUTOR_BATCH_SIZE_{env_key}", "REWARD_CLAIM_DISTRIBUTOR_BATCH_SIZE")
    return max(1, int(configured or config.get("distributorBatchSize") or 50))


def fetch_reward_claimed_logs(chain_key, config, start_block, end_block, distributors):
    if start_block > end_block:
        return []
    distributor_topics = [topic_address(distributor) for distributor in distributors]
    if not distributor_topics:
        return []
    configured_chunk_size = claim_chunk_size(chain_key, config)
    chunk_size = max(1000, configured_chunk_size)
    distributor_chunk_size = distributor_batch_size(chain_key, config)
    topic_batches = [
        distributor_topics[index:index + distributor_chunk_size]
        for index in range(0, len(distributor_topics), distributor_chunk_size)
    ]
    logs = []
    total_blocks = max(1, end_block - start_block + 1)
    print(f"Scanning {config['name']} reward claims for {len(distributor_topics):,} distributors: blocks {start_block:,} -> {end_block:,}")

    for batch_index, topic_batch in enumerate(topic_batches, start=1):
        if len(topic_batches) > 1:
            print(f"  {config['name']}: scanning distributor batch {batch_index}/{len(topic_batches)}")
        current = start_block
        chunk_size = max(1000, configured_chunk_size)
        last_progress_percent = -1
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
                            "topics": [REWARD_CLAIMED_TOPIC, topic_batch],
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
            pct = min(100, (current - start_block) * 100 // total_blocks)
            if current > end_block or pct >= last_progress_percent + 5:
                suffix = f" batch {batch_index}/{len(topic_batches)}" if len(topic_batches) > 1 else ""
                print(f"  {config['name']}: {pct}% scanned{suffix}, {len(logs):,} claim logs")
                last_progress_percent = pct
            time.sleep(0.04)

    return logs


def fetch_block_timestamps(chain_key, config, block_numbers):
    timestamps = {}
    blocks = sorted(set(block_numbers))
    batch_size = max(1, timestamp_batch_size(chain_key, config))
    total = len(blocks)
    last_percent = -1
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
        if total >= 1000:
            percent = int(min(100, ((i + len(chunk)) / total) * 100))
            if percent >= last_percent + 10 or percent == 100:
                print(f"  {config['name']}: {percent}% claim block timestamps resolved")
                last_percent = percent
        time.sleep(0.02)
    return timestamps


def fetch_block_timestamp(chain_key, config, block_number):
    if block_number is None:
        return 0
    return fetch_block_timestamps(chain_key, config, [block_number]).get(block_number, 0)


def claim_events_from_logs(chain_key, config, logs, distributor_tokens, known_timestamps=None):
    known_timestamps = known_timestamps or {}
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
        distributor = decode_topic_address(topics[1])
        user = decode_topic_address(topics[2])
        if not is_address(distributor) or not is_address(user):
            continue
        token = token_for_distributor(config, distributor_tokens, distributor)
        decimals = int(token.get("decimals") or 18)
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

    # Block timestamps are immutable: reuse any already resolved in earlier runs
    # and fetch only blocks we have never seen (data-identical, fewer RPC calls).
    unknown_blocks = [b for b in set(block_numbers) if int(known_timestamps.get(b) or 0) <= 0]
    fetched = fetch_block_timestamps(chain_key, config, unknown_blocks) if unknown_blocks else {}
    for event in decoded:
        block = event["blockNumber"]
        event["timestamp"] = int(known_timestamps.get(block) or 0) or int(fetched.get(block) or 0)
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


def reward_claim_shard_path(path):
    if os.path.isabs(path):
        return path
    return os.path.join(ROOT_DIR, path)


def load_existing_reward_claim_payload():
    existing = load_json(OUTPUT_JSON, {})
    if not isinstance(existing, dict) or not existing:
        return existing

    events = list(existing.get("events", []) or [])
    chains = dict(existing.get("chains") or {})
    should_hydrate_shards = bool(existing.get("eventsShardedByChain") or existing.get("chainEventFiles"))
    if should_hydrate_shards:
        chain_files = dict(existing.get("chainEventFiles") or {})
        for chain_key in chains:
            chain_files.setdefault(chain_key, f"data/reward-claim-events/{chain_key}.json")
        for chain_key, path in sorted(chain_files.items()):
            chain_payload = load_json(reward_claim_shard_path(path), {})
            if not isinstance(chain_payload, dict):
                continue
            chains.update(chain_payload.get("chains") or {})
            for event in chain_payload.get("events", []) or []:
                copied = dict(event)
                copied.setdefault("chainKey", chain_key)
                events.append(copied)

    hydrated = dict(existing)
    hydrated["chains"] = chains
    hydrated["events"] = merge_events([], events)
    return hydrated


def apply_distributor_token_metadata(events, chain_key, config, distributor_tokens):
    updated = []
    for event in events or []:
        if event.get("chainKey") != chain_key:
            updated.append(event)
            continue
        distributor = event.get("distributor")
        if not is_address(distributor):
            updated.append(event)
            continue
        token = token_for_distributor(config, distributor_tokens, distributor)
        copied = dict(event)
        copied["tokenSymbol"] = token.get("symbol") or copied.get("tokenSymbol") or "Reward"
        copied["tokenAddress"] = token.get("address") or copied.get("tokenAddress") or ""
        copied["tokenDecimals"] = int(token.get("decimals") or copied.get("tokenDecimals") or 18)
        amount_wei = copied.get("amountWei")
        if amount_wei not in (None, ""):
            try:
                copied["amount"] = format_units(int(amount_wei), copied["tokenDecimals"])
            except (TypeError, ValueError):
                pass
        updated.append(copied)
    return updated


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
    coverage_status = str(meta.get("coverageStatus") or "").lower()
    meta_failed = coverage_status == "failed"
    existing_is_full_coverage = bool(existing_from_block and existing_from_block <= deploy_block and not meta_failed)
    state_chain = (state.get("chains") or {}).get(chain_key, {}) if isinstance(state, dict) else {}
    state_is_usable = state.get("schemaVersion") == SCHEMA_VERSION and int(state_chain.get("lastBlock", 0) or 0) > 0

    if env_start is not None:
        start_block = max(deploy_block, env_start)
    elif force_full:
        start_block = deploy_block
    elif meta_failed:
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


def build_chain_payload(chain_key, config, current_block, start_block, end_block, existing, distributors, events, distributor_tokens, warning=""):
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
    deploy_block = int(config.get("deployBlock") or 0)
    coverage_status = "complete" if coverage_from_block <= deploy_block else "partial"
    event_distributors = sorted({
        normalize_address(event.get("distributor"))
        for event in events
        if event.get("chainKey") == chain_key and is_address(event.get("distributor"))
    })
    output_distributors = sorted(set(distributors) | set(event_distributors))
    output_tokens = {
        distributor: token_for_distributor(config, distributor_tokens, distributor)
        for distributor in output_distributors
    }
    from_timestamp = (
        int(meta.get("fromTimestamp") or 0)
        if coverage_from_block == existing_from_block and meta.get("fromTimestamp")
        else fetch_block_timestamp(chain_key, config, coverage_from_block)
    )
    to_timestamp = (
        int(meta.get("toTimestamp") or 0)
        if coverage_to_block == existing_to_block and meta.get("toTimestamp")
        else fetch_block_timestamp(chain_key, config, coverage_to_block)
    )
    return {
        "chainKey": chain_key,
        "chainName": config["name"],
        "eventEmitter": normalize_address(config["eventEmitter"]),
        "fromBlock": coverage_from_block,
        "toBlock": coverage_to_block if coverage_to_block is not None else current_block,
        "coverageStatus": coverage_status,
        "fromTimestamp": from_timestamp,
        "toTimestamp": to_timestamp,
        "distributors": output_distributors,
        "token": config.get("token", {}),
        "tokensByDistributor": output_tokens,
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
    tokens_by_distributor = {}
    for event in chain_events:
        distributor = normalize_address(event.get("distributor")) if is_address(event.get("distributor")) else ""
        if not distributor or distributor in tokens_by_distributor:
            continue
        tokens_by_distributor[distributor] = normalize_token_metadata({
            "symbol": event.get("tokenSymbol") or (config.get("token") or {}).get("symbol"),
            "address": event.get("tokenAddress") or (config.get("token") or {}).get("address"),
            "decimals": event.get("tokenDecimals") or (config.get("token") or {}).get("decimals"),
        })
    return {
        "chainKey": chain_key,
        "chainName": config["name"],
        "eventEmitter": normalize_address(config["eventEmitter"]),
        "fromBlock": min(block_numbers) if block_numbers else 0,
        "toBlock": max(block_numbers) if block_numbers else 0,
        "coverageStatus": "seeded",
        "fromTimestamp": min(timestamps) if timestamps else 0,
        "toTimestamp": max(timestamps) if timestamps else 0,
        "distributors": distributors,
        "token": config.get("token", {}),
        "tokensByDistributor": tokens_by_distributor,
        "eventCount": len(chain_events),
        "warning": "Seeded from existing reward-claim event file; next workflow scan refreshes full chain coverage.",
    }


def build_chain_payload_from_scan_failure(chain_key, config, start_block=0, end_block=0, distributors=None, warning=""):
    output_distributors = sorted({
        normalize_address(address)
        for address in (distributors or set(config.get("fallbackDistributors", set())) | set((config.get("knownDistributorTokens") or {}).keys()))
        if is_address(address)
    })
    output_tokens = {
        distributor: normalize_token_metadata((config.get("knownDistributorTokens") or {}).get(distributor) or config.get("token", {}))
        for distributor in output_distributors
    }
    return {
        "chainKey": chain_key,
        "chainName": config["name"],
        "eventEmitter": normalize_address(config["eventEmitter"]),
        "fromBlock": int(start_block or config.get("deployBlock") or 0),
        "toBlock": int(end_block or start_block or 0),
        "coverageStatus": "failed",
        "fromTimestamp": 0,
        "toTimestamp": 0,
        "distributors": output_distributors,
        "token": config.get("token", {}),
        "tokensByDistributor": output_tokens,
        "eventCount": 0,
        "warning": warning or "RewardClaimed scan failed before chain coverage metadata could be refreshed.",
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


def reward_claim_manifest(payload):
    return {
        **payload,
        "events": [],
        "eventCount": len(payload.get("events") or []),
        "eventsShardedByChain": True,
        "chainEventFiles": {
            chain_key: f"data/reward-claim-events/{chain_key}.json"
            for chain_key in sorted(payload.get("chains") or {})
        },
    }


def reward_claim_chain_payload(payload, chain_key):
    events = [
        event
        for event in payload.get("events", [])
        if event.get("chainKey") == chain_key
    ]
    chain_meta = (payload.get("chains") or {}).get(chain_key)
    return {
        "schemaVersion": payload.get("schemaVersion"),
        "generatedAt": payload.get("generatedAt"),
        "protocol": payload.get("protocol"),
        "source": payload.get("source"),
        "methodology": payload.get("methodology"),
        "chainKey": chain_key,
        "chains": {chain_key: chain_meta} if chain_meta else {},
        "events": events,
    }


def save_reward_claim_outputs(events, chains_payload, state_chains):
    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": utc_now_iso(),
        "protocol": "Dolomite",
        "source": "Dolomite EventEmitterRegistry RewardClaimed logs",
        "methodology": "Reward claim transactions are indexed from EventEmitterRegistry RewardClaimed logs and matched to wallet, chain, tx hash, log index, epoch and amount.",
        "chains": chains_payload,
        "events": merge_events([], events),
    }
    save_json(OUTPUT_JSON, reward_claim_manifest(payload), compact=True)
    for chain_key in sorted(chains_payload):
        save_json(os.path.join(CHAIN_OUTPUT_DIR, f"{chain_key}.json"), reward_claim_chain_payload(payload, chain_key), compact=True)
    save_json(LEGACY_ODOLO_OUTPUT_JSON, build_legacy_odolo_payload(payload), compact=True)
    save_json(STATE_FILE, {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": payload["generatedAt"],
        "eventCount": len(payload["events"]),
        "chains": state_chains,
    })
    return payload


def main():
    print("=" * 60)
    print("Dolomite reward claim events")
    print(f"Generated at {utc_now_iso()}")
    print("=" * 60)

    state = load_json(STATE_FILE, {})
    existing = load_existing_reward_claim_payload()
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
        current_block = 0
        start_block = 0
        end_block = 0
        distributors = []
        try:
            current_block = get_current_block(config)
            if config.get("requiresConfiguredRpcForFullClaimScan") and not has_configured_rpc(chain_key):
                distributors = sorted(set(fetch_claim_distributors(config)) | {
                    normalize_address(address)
                    for address in (chain_meta(existing, chain_key).get("distributors") or [])
                    if is_address(address)
                })
                chains_payload[chain_key] = build_chain_payload_from_scan_failure(
                    chain_key,
                    config,
                    start_block=int(config.get("deployBlock") or 0),
                    end_block=current_block,
                    distributors=distributors,
                    warning=(
                        f"{config['name']} public RPC limits eth_getLogs too tightly for a full reward-claim backfill; "
                        f"configure ALCHEMY_{chain_env_key(chain_key)}_RPC or ALCHEMY_{chain_env_key(chain_key)}_RPC_ZEN to index claim transactions."
                    ),
                )
                save_reward_claim_outputs(all_events, chains_payload, state_chains)
                print(f"Skipped {config['name']} reward claim scan: configured RPC is required for full backfill")
                continue
            start_block, end_block = scan_bounds_for_chain(chain_key, config, current_block, state, existing)
            existing_distributors = {
                normalize_address(address)
                for address in (chain_meta(existing, chain_key).get("distributors") or [])
                if is_address(address)
            }
            event_distributors = {
                normalize_address(event.get("distributor"))
                for event in existing_events_for_chain(existing, chain_key)
                if is_address(event.get("distributor"))
            }
            distributors = sorted(set(fetch_claim_distributors(config)) | existing_distributors | event_distributors)
            distributor_tokens = resolve_distributor_tokens(config, distributors)
            new_logs = fetch_reward_claimed_logs(chain_key, config, start_block, end_block, distributors)
            known_timestamps = {
                int(event.get("blockNumber") or 0): int(event.get("timestamp") or 0)
                for event in existing_events_for_chain(existing, chain_key)
                if int(event.get("blockNumber") or 0) > 0 and int(event.get("timestamp") or 0) > 0
            }
            new_events = claim_events_from_logs(
                chain_key, config, new_logs, distributor_tokens, known_timestamps
            )
            all_events = merge_events(all_events, new_events)
            all_events = apply_distributor_token_metadata(all_events, chain_key, config, distributor_tokens)
            chains_payload[chain_key] = build_chain_payload(
                chain_key,
                config,
                current_block,
                start_block,
                end_block,
                existing,
                distributors,
                all_events,
                distributor_tokens,
            )
            state_chains[chain_key] = {
                "lastBlock": end_block,
                "eventCount": chains_payload[chain_key]["eventCount"],
            }
            save_reward_claim_outputs(all_events, chains_payload, state_chains)
            print(f"Saved {chains_payload[chain_key]['eventCount']:,} {config['name']} reward claim events")
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            print(f"Warning: {config['name']} reward claim scan failed: {exc}")
            meta = chain_meta(existing, chain_key)
            if meta:
                meta = dict(meta)
                meta["warning"] = f"Latest scan failed: {exc}"
                chains_payload[chain_key] = meta
            else:
                chains_payload[chain_key] = build_chain_payload_from_scan_failure(
                    chain_key,
                    config,
                    start_block=start_block,
                    end_block=end_block or current_block,
                    distributors=distributors,
                    warning=f"Latest scan failed: {exc}",
                )
            save_reward_claim_outputs(all_events, chains_payload, state_chains)

    for chain_key in sorted({event.get("chainKey") for event in all_events if event.get("chainKey")}):
        if chain_key not in chains_payload and chain_key in CHAIN_CONFIGS:
            chains_payload[chain_key] = build_chain_payload_from_events(chain_key, CHAIN_CONFIGS[chain_key], all_events)

    payload = save_reward_claim_outputs(all_events, chains_payload, state_chains)

    print(f"Saved {len(payload['events']):,} reward claim events to {OUTPUT_JSON}")
    print(f"Saved Berachain oDOLO compatibility file to {LEGACY_ODOLO_OUTPUT_JSON}")


if __name__ == "__main__":
    main()
