#!/usr/bin/env python3
"""On-chain DOLO liquidity provider data pipeline.

The first layer in this module deliberately contains only strict registry
parsing and exact AMM arithmetic. Protocol scanners build on these pure
functions so event replay can be tested without network access.
"""

from __future__ import annotations

import copy
import itertools
import json
import re
from collections import defaultdict
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from eth_abi import decode
from web3 import Web3

from rpc_client import (
    get_endpoints,
    rpc_batch_requests,
    rpc_single_request,
    sanitize_error,
)


Q96 = 1 << 96
DOLO_ADDRESS = "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654"
SUPPORTED_ADAPTERS = {
    "uniswap-v3",
    "uniswap-v4",
    "kodiak-v2",
    "kodiak-v3",
    "bulla-v2",
    "beraswap-v2",
}

ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
POOL_ID_RE = re.compile(r"^0x[0-9a-f]{64}$")
TX_HASH_RE = POOL_ID_RE
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

V2_EVENT_SIGNATURES = {
    "transfer": "Transfer(address,address,uint256)",
    "mint": "Mint(address,uint256,uint256)",
    "burn": "Burn(address,uint256,uint256,address)",
    "sync": "Sync(uint112,uint112)",
}
V3_EVENT_SIGNATURES = {
    "pool_created": "PoolCreated(address,address,uint24,int24,address)",
    "increase": "IncreaseLiquidity(uint256,uint128,uint256,uint256)",
    "decrease": "DecreaseLiquidity(uint256,uint128,uint256,uint256)",
    "transfer": "Transfer(address,address,uint256)",
}
V4_EVENT_SIGNATURES = {
    "initialize": "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)",
    "modify_liquidity": "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)",
    "swap": "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)",
    "modify_position": "ModifyPosition(bytes32,address,int24,int24,int256,bytes32)",
    "transfer": "Transfer(address,address,uint256)",
}
KODIAK_ISLAND_CREATED_SIGNATURE = "IslandCreated(address,address,address,address)"


def _exact_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    return value


def _normalized_address(value: Any, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if not ADDRESS_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a 20-byte EVM address")
    return normalized


def _normalize_address_fields(node: Any, path: str = "registry") -> Any:
    """Normalize fields whose names denote exact EVM contract addresses."""
    address_keys = {
        "factory",
        "positionManager",
        "poolManager",
        "stateView",
        "kodiakIslandFactory",
        "kodiakIslandRouter",
        "kodiakFarmFactory",
    }
    if isinstance(node, dict):
        for key, value in list(node.items()):
            child_path = f"{path}.{key}"
            if key in address_keys:
                node[key] = _normalized_address(value, child_path)
            elif key == "addresses":
                if not isinstance(value, dict) or not value:
                    raise ValueError(f"{child_path} must be a non-empty object")
                node[key] = {
                    str(chain_key): _normalized_address(address, f"{child_path}.{chain_key}")
                    for chain_key, address in value.items()
                }
            else:
                _normalize_address_fields(value, child_path)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            _normalize_address_fields(value, f"{path}[{index}]")
    return node


def load_registry(path: str | Path) -> dict[str, Any]:
    """Load and fail closed on an invalid operational pool registry."""
    registry_path = Path(path)
    try:
        parsed = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid liquidity registry {registry_path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("liquidity registry must be an object")

    registry = _normalize_address_fields(copy.deepcopy(parsed))
    if registry.get("schemaVersion") != 1:
        raise ValueError("registry schemaVersion must equal 1")

    token = registry.get("token")
    if not isinstance(token, dict) or token.get("symbol") != "DOLO":
        raise ValueError("registry token must be DOLO")

    display = registry.get("display")
    if not isinstance(display, dict):
        raise ValueError("registry display configuration is required")
    threshold = display.get("hideBelowLiquidityUsd")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or threshold <= 0:
        raise ValueError("hideBelowLiquidityUsd must be a positive number")

    chains = registry.get("chains")
    if not isinstance(chains, dict) or not chains:
        raise ValueError("registry chains must be a non-empty object")
    for chain_key, chain in chains.items():
        if not isinstance(chain, dict):
            raise ValueError(f"chain {chain_key} must be an object")
        start = chain.get("discoveryStartBlock")
        if isinstance(start, bool) or not isinstance(start, int) or start < 0:
            raise ValueError(f"chain {chain_key} discoveryStartBlock must be a nonnegative integer")
        chain_id = chain.get("chainId")
        if isinstance(chain_id, bool) or not isinstance(chain_id, int) or chain_id <= 0:
            raise ValueError(f"chain {chain_key} chainId must be a positive integer")
        adapters = chain.get("adapters")
        if not isinstance(adapters, dict) or not adapters:
            raise ValueError(f"chain {chain_key} adapters must be a non-empty object")
        unknown = set(adapters) - SUPPORTED_ADAPTERS
        if unknown:
            raise ValueError(f"chain {chain_key} has unknown adapter {sorted(unknown)[0]}")

    pools = registry.get("pools")
    if not isinstance(pools, list) or not pools:
        raise ValueError("registry pools must be a non-empty list")
    identities: set[tuple[str, str, str]] = set()
    for index, pool in enumerate(pools):
        if not isinstance(pool, dict):
            raise ValueError(f"pool {index} must be an object")
        chain_key = str(pool.get("chainKey") or "").strip().lower()
        adapter = str(pool.get("adapter") or "").strip().lower()
        identifier = str(pool.get("identifier") or "").strip().lower()
        identifier_type = pool.get("identifierType")
        if chain_key not in chains:
            raise ValueError(f"pool {index} references unknown chain {chain_key!r}")
        if adapter not in SUPPORTED_ADAPTERS:
            raise ValueError(f"pool {index} uses unknown adapter {adapter!r}")
        if adapter not in chains[chain_key]["adapters"]:
            raise ValueError(f"pool {index} adapter {adapter!r} is not configured for {chain_key}")
        expected_identifier_type = "poolId" if adapter == "uniswap-v4" else "contract"
        if identifier_type != expected_identifier_type:
            raise ValueError(
                f"pool {index} {adapter} must use identifierType {expected_identifier_type}; "
                f"v4 poolId values are not contract addresses"
            )
        if identifier_type == "poolId" and not POOL_ID_RE.fullmatch(identifier):
            raise ValueError(f"pool {index} poolId must be a bytes32 hex value")
        if identifier_type == "contract" and not ADDRESS_RE.fullmatch(identifier):
            raise ValueError(f"pool {index} contract identifier must be a 20-byte EVM address")
        priority = pool.get("priority")
        if isinstance(priority, bool) or not isinstance(priority, int) or priority <= 0:
            raise ValueError(f"pool {index} priority must be a positive integer")
        if not isinstance(pool.get("primary"), bool):
            raise ValueError(f"pool {index} primary must be a boolean")
        if not str(pool.get("pair") or "").strip():
            raise ValueError(f"pool {index} pair is required")
        identity = (chain_key, adapter, identifier)
        if identity in identities:
            raise ValueError(f"duplicate pool identity: {identity}")
        identities.add(identity)
        pool["chainKey"] = chain_key
        pool["adapter"] = adapter
        pool["identifier"] = identifier

    return registry


def event_key(chain_key: Any, tx_hash: Any, log_index: Any) -> str:
    """Return the canonical identity for an on-chain event."""
    chain = str(chain_key or "").strip().lower()
    tx = str(tx_hash or "").strip().lower()
    if not chain:
        raise ValueError("chain key is required")
    if not TX_HASH_RE.fullmatch(tx):
        raise ValueError("transaction hash must be a bytes32 hex value")
    if isinstance(log_index, str):
        try:
            index = int(log_index, 16) if log_index.lower().startswith("0x") else int(log_index)
        except ValueError as exc:
            raise ValueError("log index must be a nonnegative integer") from exc
    else:
        index = _exact_int(log_index, "log index")
    if index < 0:
        raise ValueError("log index must be a nonnegative integer")
    return f"{chain}:{tx}:{index}"


def event_topic(signature: str) -> str:
    """Return the canonical Keccak event signature topic."""
    normalized = str(signature or "").strip()
    if not normalized or "(" not in normalized or not normalized.endswith(")"):
        raise ValueError("event signature must be a canonical Solidity signature")
    topic = Web3.keccak(text=normalized).hex().lower()
    return topic if topic.startswith("0x") else "0x" + topic


def tick_to_paired_per_dolo(
    tick: int,
    token0: str,
    token1: str,
    decimals0: int,
    decimals1: int,
    dolo_address: str,
) -> Decimal:
    """Convert a v3/v4 tick into paired-token units per one DOLO."""
    tick_value = _exact_int(tick, "tick")
    dec0 = _exact_int(decimals0, "token0 decimals")
    dec1 = _exact_int(decimals1, "token1 decimals")
    if not 0 <= dec0 <= 255 or not 0 <= dec1 <= 255:
        raise ValueError("token decimals must be between 0 and 255")
    token0_normalized = _normalized_address(token0, "token0")
    token1_normalized = _normalized_address(token1, "token1")
    dolo = _normalized_address(dolo_address, "DOLO address")
    if dolo not in {token0_normalized, token1_normalized}:
        raise ValueError("pool pair does not contain DOLO")

    with localcontext() as context:
        context.prec = 90
        token1_per_token0 = (Decimal("1.0001") ** tick_value) * (
            Decimal(10) ** (dec0 - dec1)
        )
        if token1_per_token0 <= 0:
            raise ValueError("tick produced a non-positive price")
        if dolo == token0_normalized:
            return +token1_per_token0
        return +(Decimal(1) / token1_per_token0)


def amounts_for_liquidity(
    liquidity: int,
    sqrt_price_x96: int,
    sqrt_lower_x96: int,
    sqrt_upper_x96: int,
) -> tuple[int, int]:
    """Return floored raw token principal for an exact concentrated position."""
    liquidity_value = _exact_int(liquidity, "liquidity")
    current = _exact_int(sqrt_price_x96, "sqrt price")
    lower = _exact_int(sqrt_lower_x96, "sqrt lower")
    upper = _exact_int(sqrt_upper_x96, "sqrt upper")
    if liquidity_value < 0:
        raise ValueError("liquidity must be nonnegative")
    if current <= 0 or lower <= 0 or upper <= 0 or lower >= upper:
        raise ValueError("sqrt range must contain positive increasing values")
    if liquidity_value == 0:
        return 0, 0
    if current <= lower:
        amount0 = liquidity_value * (upper - lower) * Q96 // (lower * upper)
        return amount0, 0
    if current >= upper:
        amount1 = liquidity_value * (upper - lower) // Q96
        return 0, amount1
    amount0 = liquidity_value * (upper - current) * Q96 // (current * upper)
    amount1 = liquidity_value * (current - lower) // Q96
    return amount0, amount1


def sqrt_ratio_at_tick(tick: int) -> int:
    """Port of Uniswap TickMath.getSqrtRatioAtTick with identical rounding."""
    tick_value = _exact_int(tick, "tick")
    absolute_tick = abs(tick_value)
    if absolute_tick > 887272:
        raise ValueError("tick must be within Uniswap TickMath bounds")
    ratio = (
        0xFFFcb933BD6fAD37AA2d162D1A594001
        if absolute_tick & 0x1
        else 0x100000000000000000000000000000000
    )
    multipliers = (
        (0x2, 0xFFF97272373D413259A46990580E213A),
        (0x4, 0xFFF2E50F5F656932EF12357CF3C7FDCC),
        (0x8, 0xFFE5CACA7E10E4E61C3624EAA0941CD0),
        (0x10, 0xFFCB9843D60F6159C9DB58835C926644),
        (0x20, 0xFF973B41FA98C081472E6896DFB254C0),
        (0x40, 0xFF2EA16466C96A3843EC78B326B52861),
        (0x80, 0xFE5DEE046A99A2A811C461F1969C3053),
        (0x100, 0xFCBE86C7900A88AEDCFFC83B479AA3A4),
        (0x200, 0xF987A7253AC413176F2B074CF7815E54),
        (0x400, 0xF3392B0822B70005940C7A398E4B70F3),
        (0x800, 0xE7159475A2C29B7443B29C7FA6E889D9),
        (0x1000, 0xD097F3BDFD2022B8845AD8F792AA5825),
        (0x2000, 0xA9F746462D870FDF8A65DC1F90E061E5),
        (0x4000, 0x70D869A156D2A1B890BB3DF62BAF32F7),
        (0x8000, 0x31BE135F97D08FD981231505542FCFA6),
        (0x10000, 0x9AA508B5B7A84E1C677DE54F3E99BC9),
        (0x20000, 0x5D6AF8DEDB81196699C329225EE604),
        (0x40000, 0x2216E584F5FA1EA926041BEDFE98),
        (0x80000, 0x48A170391F7DC42444E8FA2),
    )
    for bit, multiplier in multipliers:
        if absolute_tick & bit:
            ratio = (ratio * multiplier) >> 128
    if tick_value > 0:
        ratio = ((1 << 256) - 1) // ratio
    remainder_mask = (1 << 32) - 1
    return (ratio >> 32) + (1 if ratio & remainder_mask else 0)


def v2_underlying(
    lp_balance: int,
    total_supply: int,
    reserve0: int,
    reserve1: int,
) -> tuple[int, int]:
    """Return a wallet's floored raw underlying reserves for a V2 LP balance."""
    balance = _exact_int(lp_balance, "LP balance")
    supply = _exact_int(total_supply, "LP total supply")
    raw_reserve0 = _exact_int(reserve0, "reserve0")
    raw_reserve1 = _exact_int(reserve1, "reserve1")
    if supply <= 0:
        raise ValueError("LP total supply must be positive")
    if balance < 0 or raw_reserve0 < 0 or raw_reserve1 < 0:
        raise ValueError("LP balance and reserves must be nonnegative")
    return (
        balance * raw_reserve0 // supply,
        balance * raw_reserve1 // supply,
    )


def classify_range(
    current_tick: int | None,
    tick_lower: int | None,
    tick_upper: int | None,
) -> str:
    """Classify exact v3/v4 tick state using the lower-inclusive range rule."""
    if current_tick is None or tick_lower is None or tick_upper is None:
        return "unavailable"
    current = _exact_int(current_tick, "current tick")
    lower = _exact_int(tick_lower, "tick lower")
    upper = _exact_int(tick_upper, "tick upper")
    if lower >= upper:
        raise ValueError("tick lower must be less than tick upper")
    return "in_range" if lower <= current < upper else "out_of_range"


def resume_block(
    previous_source: dict[str, Any] | None,
    configured_start: int,
    *,
    overlap: int = 128,
) -> int:
    """Choose an inclusive replay start with the requested overlap."""
    start = _exact_int(configured_start, "configured start block")
    overlap_blocks = _exact_int(overlap, "overlap")
    if start < 0:
        raise ValueError("configured start block must be nonnegative")
    if overlap_blocks <= 0:
        raise ValueError("overlap must be positive")
    if not previous_source:
        return start
    if not isinstance(previous_source, dict):
        raise ValueError("previous source must be an object")
    last_scanned = previous_source.get("lastScannedBlock")
    if isinstance(last_scanned, bool) or not isinstance(last_scanned, int) or last_scanned < 0:
        raise ValueError("previous source lastScannedBlock must be a nonnegative integer")
    return max(start, last_scanned - overlap_blocks + 1)


def block_ranges(from_block: int, to_block: int, chunk_size: int):
    """Yield inclusive block ranges without gaps or boundary duplication."""
    start = _exact_int(from_block, "from block")
    end = _exact_int(to_block, "to block")
    size = _exact_int(chunk_size, "chunk size")
    if start < 0 or end < 0:
        raise ValueError("block range must be nonnegative")
    if size <= 0:
        raise ValueError("chunk size must be positive")
    while start <= end:
        chunk_end = min(end, start + size - 1)
        yield start, chunk_end
        start = chunk_end + 1


def _rpc_hex_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an RPC hex integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.lower().startswith("0x"):
        try:
            parsed = int(value, 16)
        except ValueError as exc:
            raise ValueError(f"{label} must be an RPC hex integer") from exc
    else:
        raise ValueError(f"{label} must be an RPC hex integer")
    if parsed < 0:
        raise ValueError(f"{label} must be nonnegative")
    return parsed


def normalize_rpc_log(row: Any) -> dict[str, Any]:
    """Validate and normalize one JSON-RPC log for deterministic replay."""
    if not isinstance(row, dict):
        raise ValueError("RPC log must be an object")
    if row.get("removed") is True:
        raise ValueError("removed log cannot be used for canonical replay")
    address = _normalized_address(row.get("address"), "log address")
    tx_hash = str(row.get("transactionHash") or "").strip().lower()
    if not TX_HASH_RE.fullmatch(tx_hash):
        raise ValueError("log transaction hash must be a bytes32 hex value")
    block_hash = str(row.get("blockHash") or "").strip().lower()
    if block_hash and not POOL_ID_RE.fullmatch(block_hash):
        raise ValueError("log block hash must be a bytes32 hex value")
    topics = row.get("topics")
    if not isinstance(topics, list):
        raise ValueError("log topics must be a list")
    normalized_topics = []
    for topic in topics:
        normalized = str(topic or "").strip().lower()
        if not POOL_ID_RE.fullmatch(normalized):
            raise ValueError("log topic must be a bytes32 hex value")
        normalized_topics.append(normalized)
    data = str(row.get("data") or "").strip().lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data):
        raise ValueError("log data must be even-length hex")
    return {
        "address": address,
        "blockNumber": _rpc_hex_int(row.get("blockNumber"), "log block number"),
        "transactionIndex": _rpc_hex_int(
            row.get("transactionIndex"), "log transaction index"
        ),
        "logIndex": _rpc_hex_int(row.get("logIndex"), "log index"),
        "transactionHash": tx_hash,
        "blockHash": block_hash,
        "data": data,
        "topics": normalized_topics,
        "removed": False,
    }


def dedupe_logs(chain_key: str, logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate exact events while failing on conflicting event identities."""
    by_key: dict[str, dict[str, Any]] = {}
    for raw in logs:
        row = normalize_rpc_log(raw) if isinstance(raw.get("blockNumber"), str) else copy.deepcopy(raw)
        key = event_key(chain_key, row.get("transactionHash"), row.get("logIndex"))
        previous = by_key.get(key)
        if previous is not None and previous != row:
            raise ValueError(f"event-key collision has conflicting payloads: {key}")
        by_key[key] = row
    return sorted(
        by_key.values(),
        key=lambda row: (
            row["blockNumber"],
            row["transactionIndex"],
            row["logIndex"],
        ),
    )


def scan_logs(
    chain: str,
    addresses: str | list[str],
    topics: list[Any],
    from_block: int,
    to_block: int,
    chunk_size: int,
    *,
    rpc=None,
    endpoints: list[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Scan and normalize a complete inclusive log range.

    The cursor is returned only after every requested chunk has succeeded.
    """
    chain_key = str(chain or "").strip().lower()
    if not chain_key:
        raise ValueError("chain is required")
    start = _exact_int(from_block, "from block")
    end = _exact_int(to_block, "to block")
    if end < start:
        return [], end
    rpc_call = rpc or rpc_single_request
    rpc_endpoints = list(endpoints) if endpoints is not None else get_endpoints(chain_key)
    if isinstance(addresses, str):
        address_filter: str | list[str] = _normalized_address(addresses, "log address filter")
    elif isinstance(addresses, list) and addresses:
        address_filter = [
            _normalized_address(address, "log address filter") for address in addresses
        ]
    else:
        raise ValueError("log address filter must be an address or non-empty list")
    if not isinstance(topics, list):
        raise ValueError("topics filter must be a list")

    collected: list[dict[str, Any]] = []
    for chunk_start, chunk_end in block_ranges(start, end, chunk_size):
        request_id = f"logs:{chain_key}:{chunk_start}:{chunk_end}"
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "eth_getLogs",
            "params": [
                {
                    "address": address_filter,
                    "topics": topics,
                    "fromBlock": hex(chunk_start),
                    "toBlock": hex(chunk_end),
                }
            ],
        }
        response = rpc_call(
            rpc_endpoints,
            payload,
            describe=f"{chain_key} logs {chunk_start}-{chunk_end}",
        )
        if not isinstance(response, dict):
            raise RuntimeError(f"{chain_key} log response was not an object")
        if response.get("error"):
            raise RuntimeError(
                f"{chain_key} log response error: {sanitize_error(response['error'])}"
            )
        rows = response.get("result")
        if not isinstance(rows, list):
            raise RuntimeError(f"{chain_key} log response result was not a list")
        collected.extend(normalize_rpc_log(row) for row in rows)
    return dedupe_logs(chain_key, collected), end


def load_previous_artifact(path: str | Path) -> dict[str, Any]:
    """Load a checked-in last-known-good artifact, or empty state if absent."""
    artifact_path = Path(path)
    if not artifact_path.exists():
        return {}
    try:
        data = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid previous artifact {artifact_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("previous artifact must be an object")
    return data


def preserve_stale_adapter(
    previous: dict[str, Any],
    adapter_key: str,
    error: Exception,
    generated_at: str,
) -> dict[str, Any]:
    """Extract last-known-good rows for one failed adapter and mark them stale."""
    if not isinstance(previous, dict):
        raise ValueError("previous artifact must be an object")
    source = next(
        (
            copy.deepcopy(row)
            for row in previous.get("sources", [])
            if isinstance(row, dict) and row.get("key") == adapter_key
        ),
        None,
    )
    if source is None:
        raise ValueError(f"previous artifact has no source {adapter_key}")
    source["status"] = "stale"
    source["staleSince"] = generated_at
    source["errors"] = [sanitize_error(error)]

    def rows_for(field: str) -> list[dict[str, Any]]:
        rows = []
        for row in previous.get(field, []):
            if not isinstance(row, dict) or row.get("sourceKey") != adapter_key:
                continue
            preserved = copy.deepcopy(row)
            if preserved.get("quality") == "verified":
                preserved["quality"] = "stale"
            preserved["staleSince"] = generated_at
            rows.append(preserved)
        return rows

    return {
        "source": source,
        "pools": rows_for("pools"),
        "activePositions": rows_for("activePositions"),
        "history": rows_for("history"),
    }


def fetch_block_timestamps(
    chain: str,
    block_numbers: list[int],
    *,
    endpoints: list[str] | None = None,
    rpc_batch=None,
    cache: dict[tuple[str, int], int] | None = None,
) -> dict[int, int]:
    """Fetch exact block timestamps in one batch and fail on any missing block."""
    chain_key = str(chain or "").strip().lower()
    if not chain_key:
        raise ValueError("chain is required")
    timestamp_cache = cache if cache is not None else {}
    requested = sorted({_exact_int(block, "block number") for block in block_numbers})
    if any(block < 0 for block in requested):
        raise ValueError("block number must be nonnegative")
    missing_blocks = [
        block for block in requested if (chain_key, block) not in timestamp_cache
    ]
    if missing_blocks:
        payloads = [
            {
                "jsonrpc": "2.0",
                "id": f"block:{block}",
                "method": "eth_getBlockByNumber",
                "params": [hex(block), False],
            }
            for block in missing_blocks
        ]
        batch_call = rpc_batch or rpc_batch_requests
        rpc_endpoints = list(endpoints) if endpoints is not None else get_endpoints(chain_key)
        responses, missing_ids = batch_call(
            rpc_endpoints,
            payloads,
            describe=f"{chain_key} block timestamps",
        )
        missing_id_set = set(missing_ids or [])
        for block in missing_blocks:
            request_id = f"block:{block}"
            response = responses.get(request_id) if isinstance(responses, dict) else None
            result = response.get("result") if isinstance(response, dict) else None
            if request_id in missing_id_set or not isinstance(result, dict):
                raise RuntimeError(f"missing exact timestamp for {chain_key} block {block}")
            timestamp = _rpc_hex_int(result.get("timestamp"), "block timestamp")
            returned_block = _rpc_hex_int(result.get("number"), "returned block number")
            if returned_block != block:
                raise RuntimeError(
                    f"timestamp response block mismatch for {chain_key}: "
                    f"requested {block}, received {returned_block}"
                )
            timestamp_cache[(chain_key, block)] = timestamp
    return {block: timestamp_cache[(chain_key, block)] for block in requested}


def _address_from_topic(topic: Any, label: str) -> str:
    normalized = str(topic or "").strip().lower()
    if not POOL_ID_RE.fullmatch(normalized):
        raise ValueError(f"{label} topic must be bytes32")
    return _normalized_address("0x" + normalized[-40:], label)


def _decoded_log_base(log: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(log, dict):
        raise ValueError("event log must be an object")
    timestamp = log.get("timestamp")
    if timestamp is not None:
        timestamp = _exact_int(timestamp, "event timestamp")
        if timestamp <= 0:
            raise ValueError("event timestamp must be positive")
    tx_hash = str(log.get("transactionHash") or "").strip().lower()
    if not TX_HASH_RE.fullmatch(tx_hash):
        raise ValueError("event transaction hash must be a bytes32 hex value")
    return {
        "blockNumber": _rpc_hex_int(log.get("blockNumber"), "event block number"),
        "transactionIndex": _rpc_hex_int(
            log.get("transactionIndex"), "event transaction index"
        ),
        "logIndex": _rpc_hex_int(log.get("logIndex"), "event log index"),
        "txHash": tx_hash,
        "timestamp": timestamp,
    }


def decode_v2_log(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode one exact Uniswap-v2-compatible pool event."""
    topics = log.get("topics") if isinstance(log, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("V2 log topics are required")
    signature_topic = str(topics[0]).strip().lower()
    data_hex = str(log.get("data") or "").strip().lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data_hex):
        raise ValueError("V2 log data must be even-length hex")
    data = bytes.fromhex(data_hex[2:])
    base = _decoded_log_base(log)

    if signature_topic == event_topic(V2_EVENT_SIGNATURES["transfer"]):
        if len(topics) != 3:
            raise ValueError("V2 Transfer must have exactly two indexed addresses")
        (value,) = decode(["uint256"], data)
        return {
            **base,
            "kind": "transfer",
            "from": _address_from_topic(topics[1], "transfer sender"),
            "to": _address_from_topic(topics[2], "transfer recipient"),
            "valueRaw": int(value),
        }
    if signature_topic == event_topic(V2_EVENT_SIGNATURES["mint"]):
        if len(topics) != 2:
            raise ValueError("V2 Mint must have exactly one indexed sender")
        amount0, amount1 = decode(["uint256", "uint256"], data)
        return {
            **base,
            "kind": "mint",
            "sender": _address_from_topic(topics[1], "mint sender"),
            "amount0Raw": int(amount0),
            "amount1Raw": int(amount1),
        }
    if signature_topic == event_topic(V2_EVENT_SIGNATURES["burn"]):
        if len(topics) != 3:
            raise ValueError("V2 Burn must have indexed sender and recipient")
        amount0, amount1 = decode(["uint256", "uint256"], data)
        return {
            **base,
            "kind": "burn",
            "sender": _address_from_topic(topics[1], "burn sender"),
            "to": _address_from_topic(topics[2], "burn recipient"),
            "amount0Raw": int(amount0),
            "amount1Raw": int(amount1),
        }
    if signature_topic == event_topic(V2_EVENT_SIGNATURES["sync"]):
        if len(topics) != 1:
            raise ValueError("V2 Sync cannot have indexed values")
        reserve0, reserve1 = decode(["uint112", "uint112"], data)
        return {
            **base,
            "kind": "sync",
            "reserve0Raw": int(reserve0),
            "reserve1Raw": int(reserve1),
        }
    return None


def decode_v3_pool_created(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode a Uniswap-v3-compatible Factory PoolCreated event."""
    topics = log.get("topics") if isinstance(log, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("V3 factory log topics are required")
    if str(topics[0]).lower() != event_topic(V3_EVENT_SIGNATURES["pool_created"]):
        return None
    if len(topics) != 4:
        raise ValueError("V3 PoolCreated must have token0, token1, and fee topics")
    data_hex = str(log.get("data") or "").strip().lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data_hex):
        raise ValueError("V3 PoolCreated data must be even-length hex")
    tick_spacing, pool_address = decode(
        ["int24", "address"], bytes.fromhex(data_hex[2:])
    )
    base = _decoded_log_base(log)
    return {
        **base,
        "kind": "pool_created",
        "token0": _address_from_topic(topics[1], "V3 token0"),
        "token1": _address_from_topic(topics[2], "V3 token1"),
        "fee": int(str(topics[3]), 16),
        "tickSpacing": int(tick_spacing),
        "pool": _normalized_address(pool_address, "V3 pool"),
    }


def discover_v3_dolo_pools(
    logs: list[dict[str, Any]], dolo_address: str
) -> list[dict[str, Any]]:
    """Return Factory pools containing DOLO in deterministic creation order."""
    dolo = _normalized_address(dolo_address, "DOLO address")
    discovered = []
    seen = set()
    for log in logs:
        event = decode_v3_pool_created(log)
        if event is None or dolo not in {event["token0"], event["token1"]}:
            continue
        if event["pool"] in seen:
            continue
        seen.add(event["pool"])
        discovered.append(event)
    return sorted(
        discovered,
        key=lambda row: (row["blockNumber"], row["transactionIndex"], row["logIndex"]),
    )


def decode_v3_npm_log(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode canonical v3 NPM ownership and liquidity events."""
    topics = log.get("topics") if isinstance(log, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("V3 NPM log topics are required")
    signature_topic = str(topics[0]).lower()
    base = _decoded_log_base(log)
    if signature_topic == event_topic(V3_EVENT_SIGNATURES["transfer"]):
        if len(topics) != 4:
            raise ValueError("V3 NFT Transfer must have three indexed values")
        return {
            **base,
            "kind": "transfer",
            "from": _address_from_topic(topics[1], "V3 NFT sender"),
            "to": _address_from_topic(topics[2], "V3 NFT recipient"),
            "tokenId": int(str(topics[3]), 16),
        }
    if signature_topic not in {
        event_topic(V3_EVENT_SIGNATURES["increase"]),
        event_topic(V3_EVENT_SIGNATURES["decrease"]),
    }:
        return None
    if len(topics) != 2:
        raise ValueError("V3 liquidity event must have an indexed tokenId")
    data_hex = str(log.get("data") or "").strip().lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data_hex):
        raise ValueError("V3 liquidity event data must be even-length hex")
    liquidity_raw, amount0, amount1 = decode(
        ["uint128", "uint256", "uint256"], bytes.fromhex(data_hex[2:])
    )
    return {
        **base,
        "kind": (
            "increase"
            if signature_topic == event_topic(V3_EVENT_SIGNATURES["increase"])
            else "decrease"
        ),
        "tokenId": int(str(topics[1]), 16),
        "liquidityRaw": int(liquidity_raw),
        "amount0Raw": int(amount0),
        "amount1Raw": int(amount1),
    }


def map_v3_events_to_pools(
    logs: list[dict[str, Any]],
    position_snapshots: dict[Any, dict[str, Any]],
    known_pool_ids: set[str],
    dolo_address: str,
) -> dict[str, Any]:
    """Map NPM token IDs to pools only with an exact historical snapshot."""
    dolo = _normalized_address(dolo_address, "DOLO address")
    known_pools = {
        _normalized_address(pool_address, "known V3 pool")
        for pool_address in known_pool_ids
    }
    snapshots = {}
    for key, value in position_snapshots.items():
        token_id = int(key)
        if isinstance(key, bool) or token_id < 0 or not isinstance(value, dict):
            raise ValueError("V3 position snapshots must use nonnegative token IDs")
        snapshots[token_id] = value
    events_by_pool: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    unresolved = []
    decoded_events = [decode_v3_npm_log(log) for log in logs]
    decoded_events = [event for event in decoded_events if event is not None]
    decoded_events.sort(
        key=lambda row: (row["blockNumber"], row["transactionIndex"], row["logIndex"])
    )
    for event in decoded_events:
        token_id = event["tokenId"]
        snapshot = snapshots.get(token_id)
        if snapshot is None:
            if event["kind"] != "transfer":
                unresolved.append(
                    {
                        **event,
                        "pool": None,
                        "quality": "partial",
                        "reason": "archive position snapshot unavailable; pool attribution withheld",
                    }
                )
            continue
        pool_address = _normalized_address(snapshot.get("pool"), "V3 snapshot pool")
        token0 = _normalized_address(snapshot.get("token0"), "V3 snapshot token0")
        token1 = _normalized_address(snapshot.get("token1"), "V3 snapshot token1")
        if dolo not in {token0, token1} or pool_address not in known_pools:
            continue
        events_by_pool[pool_address].append(
            {
                **event,
                "pool": pool_address,
                "token0": token0,
                "token1": token1,
                "fee": _exact_int(snapshot.get("fee"), "V3 snapshot fee"),
                "tickLower": _exact_int(snapshot.get("tickLower"), "V3 tick lower"),
                "tickUpper": _exact_int(snapshot.get("tickUpper"), "V3 tick upper"),
                "snapshotBlock": _exact_int(
                    snapshot.get("snapshotBlock"), "V3 snapshot block"
                ),
            }
        )
    return {"eventsByPool": dict(events_by_pool), "unresolved": unresolved}


def build_v3_rows(
    pool: dict[str, Any],
    mapped_events: list[dict[str, Any]],
    latest_positions: dict[Any, dict[str, Any]],
    pool_state: dict[str, Any],
    *,
    contract_addresses: set[str] | None = None,
) -> dict[str, Any]:
    """Build exact v3 active and history rows from mapped NPM evidence."""
    chain_key = str(pool.get("chainKey") or "").strip().lower()
    adapter = str(pool.get("adapter") or "").strip().lower()
    if adapter not in {"uniswap-v3", "kodiak-v3"}:
        raise ValueError("V3 rows require a v3 adapter")
    pool_address = _normalized_address(pool.get("identifier"), "V3 pool")
    source_key = f"{chain_key}:{adapter}"
    normalized_contracts = {
        _normalized_address(address, "contract address")
        for address in (contract_addresses or set())
    }
    current_sqrt = _exact_int(pool_state.get("sqrtPriceX96"), "V3 sqrt price")
    current_tick = _exact_int(pool_state.get("currentTick"), "V3 current tick")
    decimals0 = _exact_int(pool_state.get("decimals0"), "V3 token0 decimals")
    decimals1 = _exact_int(pool_state.get("decimals1"), "V3 token1 decimals")

    owners: dict[int, str] = {}
    position_liquidity: defaultdict[int, int] = defaultdict(int)
    history = []
    ordered_events = sorted(
        mapped_events,
        key=lambda row: (row["blockNumber"], row["transactionIndex"], row["logIndex"]),
    )
    for event in ordered_events:
        token_id = _exact_int(event.get("tokenId"), "V3 tokenId")
        if event["kind"] == "transfer":
            recipient = _normalized_address(event.get("to"), "V3 NFT recipient")
            if recipient == ZERO_ADDRESS:
                owners.pop(token_id, None)
            else:
                owners[token_id] = recipient
            continue
        if event["timestamp"] is None:
            raise ValueError("V3 liquidity history requires an exact block timestamp")
        delta = _exact_int(event.get("liquidityRaw"), "V3 liquidity delta")
        before = position_liquidity[token_id]
        if event["kind"] == "increase":
            after = before + delta
            action = "Added" if before == 0 else "Increased"
        elif event["kind"] == "decrease":
            if delta > before:
                raise ValueError(f"V3 token {token_id} decrease exceeds replayed liquidity")
            after = before - delta
            action = "Closed" if after == 0 else "Removed"
        else:
            raise ValueError(f"unsupported V3 event kind {event['kind']!r}")
        position_liquidity[token_id] = after
        owner = owners.get(token_id)
        attribution = _event_attribution(owner, normalized_contracts)
        amount0 = _exact_int(event.get("amount0Raw"), "V3 amount0")
        amount1 = _exact_int(event.get("amount1Raw"), "V3 amount1")
        token0 = _normalized_address(event.get("token0"), "V3 event token0")
        history.append(
            {
                "id": event_key(chain_key, event["txHash"], event["logIndex"]),
                "sourceKey": source_key,
                "poolId": pool_address,
                "poolIdentifierType": "contract",
                "chainKey": chain_key,
                "adapter": adapter,
                "pair": pool.get("pair"),
                "positionId": str(token_id),
                "action": action,
                "blockNumber": event["blockNumber"],
                "timestamp": event["timestamp"],
                "txHash": event["txHash"],
                "logIndex": event["logIndex"],
                "amount0Raw": str(amount0),
                "amount1Raw": str(amount1),
                "doloRaw": str(amount0 if token0 == DOLO_ADDRESS else amount1),
                "pairedRaw": str(amount1 if token0 == DOLO_ADDRESS else amount0),
                "valueUsd": None,
                **attribution,
            }
        )

    active_positions = []
    for token_key, latest in sorted(latest_positions.items(), key=lambda item: int(item[0])):
        token_id = int(token_key)
        if isinstance(token_key, bool) or token_id < 0 or not isinstance(latest, dict):
            raise ValueError("latest V3 positions must use nonnegative token IDs")
        latest_pool = _normalized_address(latest.get("pool"), "latest V3 pool")
        if latest_pool != pool_address:
            continue
        liquidity_raw = _exact_int(latest.get("liquidity"), "latest V3 liquidity")
        if liquidity_raw <= 0:
            continue
        token0 = _normalized_address(latest.get("token0"), "latest V3 token0")
        token1 = _normalized_address(latest.get("token1"), "latest V3 token1")
        if DOLO_ADDRESS not in {token0, token1}:
            raise ValueError(f"latest V3 token {token_id} is not a DOLO position")
        tick_lower = _exact_int(latest.get("tickLower"), "latest V3 tick lower")
        tick_upper = _exact_int(latest.get("tickUpper"), "latest V3 tick upper")
        amount0, amount1 = amounts_for_liquidity(
            liquidity_raw,
            current_sqrt,
            sqrt_ratio_at_tick(tick_lower),
            sqrt_ratio_at_tick(tick_upper),
        )
        owner = _normalized_address(latest.get("owner"), "latest V3 owner")
        attribution = _event_attribution(owner, normalized_contracts)
        bound_a = tick_to_paired_per_dolo(
            tick_lower, token0, token1, decimals0, decimals1, DOLO_ADDRESS
        )
        bound_b = tick_to_paired_per_dolo(
            tick_upper, token0, token1, decimals0, decimals1, DOLO_ADDRESS
        )
        range_lower, range_upper = sorted((bound_a, bound_b))
        active_positions.append(
            {
                "id": f"{source_key}:{pool_address}:{token_id}",
                "sourceKey": source_key,
                "poolId": pool_address,
                "poolIdentifierType": "contract",
                "chainKey": chain_key,
                "adapter": adapter,
                "pair": pool.get("pair"),
                "positionType": "concentrated_nft",
                "positionId": str(token_id),
                "liquidityRaw": str(liquidity_raw),
                "tickLower": tick_lower,
                "tickUpper": tick_upper,
                "rangeLower": str(range_lower),
                "rangeUpper": str(range_upper),
                "rangeStatus": classify_range(current_tick, tick_lower, tick_upper),
                "feeTier": _exact_int(latest.get("fee"), "latest V3 fee"),
                "amount0Raw": str(amount0),
                "amount1Raw": str(amount1),
                "doloRaw": str(amount0 if token0 == DOLO_ADDRESS else amount1),
                "pairedRaw": str(amount1 if token0 == DOLO_ADDRESS else amount0),
                "valueUsd": None,
                **attribution,
            }
        )
    return {
        "sourceKey": source_key,
        "sourceStatus": "complete",
        "activePositions": active_positions,
        "history": history,
    }


def _pool_id_from_topic(topic: Any, label: str) -> str:
    normalized = str(topic or "").strip().lower()
    if not POOL_ID_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a bytes32 pool ID")
    return normalized


def decode_v4_pool_manager_log(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode the v4 PoolManager events used by position reconstruction."""
    topics = log.get("topics") if isinstance(log, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("V4 PoolManager log topics are required")
    signature_topic = str(topics[0]).lower()
    data_hex = str(log.get("data") or "").strip().lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data_hex):
        raise ValueError("V4 PoolManager log data must be even-length hex")
    data = bytes.fromhex(data_hex[2:])
    base = _decoded_log_base(log)
    if signature_topic == event_topic(V4_EVENT_SIGNATURES["initialize"]):
        if len(topics) != 4:
            raise ValueError("V4 Initialize must index pool ID and both currencies")
        fee, tick_spacing, hooks, sqrt_price_x96, tick = decode(
            ["uint24", "int24", "address", "uint160", "int24"], data
        )
        return {
            **base,
            "kind": "initialize",
            "poolId": _pool_id_from_topic(topics[1], "V4 Initialize pool ID"),
            "currency0": _address_from_topic(topics[2], "V4 currency0"),
            "currency1": _address_from_topic(topics[3], "V4 currency1"),
            "fee": int(fee),
            "tickSpacing": int(tick_spacing),
            "hooks": _normalized_address(hooks, "V4 hooks"),
            "sqrtPriceX96": int(sqrt_price_x96),
            "tick": int(tick),
        }
    if signature_topic == event_topic(V4_EVENT_SIGNATURES["modify_liquidity"]):
        if len(topics) != 3:
            raise ValueError("V4 ModifyLiquidity must index pool ID and sender")
        tick_lower, tick_upper, liquidity_delta, salt = decode(
            ["int24", "int24", "int256", "bytes32"], data
        )
        return {
            **base,
            "kind": "modify_liquidity",
            "poolId": _pool_id_from_topic(topics[1], "V4 ModifyLiquidity pool ID"),
            "sender": _address_from_topic(topics[2], "V4 ModifyLiquidity sender"),
            "tickLower": int(tick_lower),
            "tickUpper": int(tick_upper),
            "liquidityDelta": int(liquidity_delta),
            "salt": "0x" + bytes(salt).hex(),
        }
    if signature_topic == event_topic(V4_EVENT_SIGNATURES["swap"]):
        if len(topics) != 3:
            raise ValueError("V4 Swap must index pool ID and sender")
        amount0, amount1, sqrt_price_x96, active_liquidity, tick, fee = decode(
            ["int128", "int128", "uint160", "uint128", "int24", "uint24"], data
        )
        return {
            **base,
            "kind": "swap",
            "poolId": _pool_id_from_topic(topics[1], "V4 Swap pool ID"),
            "sender": _address_from_topic(topics[2], "V4 Swap sender"),
            "amount0Raw": int(amount0),
            "amount1Raw": int(amount1),
            "sqrtPriceX96": int(sqrt_price_x96),
            "activeLiquidityRaw": int(active_liquidity),
            "tick": int(tick),
            "fee": int(fee),
        }
    return None


def decode_v4_position_manager_log(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode v4 PositionManager ERC-721 and modify-position evidence."""
    topics = log.get("topics") if isinstance(log, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("V4 PositionManager log topics are required")
    signature_topic = str(topics[0]).lower()
    base = _decoded_log_base(log)
    if signature_topic == event_topic(V4_EVENT_SIGNATURES["transfer"]):
        if len(topics) != 4:
            raise ValueError("V4 NFT Transfer must have three indexed values")
        return {
            **base,
            "kind": "transfer",
            "from": _address_from_topic(topics[1], "V4 NFT sender"),
            "to": _address_from_topic(topics[2], "V4 NFT recipient"),
            "tokenId": int(str(topics[3]), 16),
        }
    if signature_topic != event_topic(V4_EVENT_SIGNATURES["modify_position"]):
        return None
    if len(topics) != 3:
        raise ValueError("V4 ModifyPosition must index pool ID and sender")
    data_hex = str(log.get("data") or "").strip().lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data_hex):
        raise ValueError("V4 ModifyPosition data must be even-length hex")
    tick_lower, tick_upper, liquidity_delta, salt = decode(
        ["int24", "int24", "int256", "bytes32"], bytes.fromhex(data_hex[2:])
    )
    return {
        **base,
        "kind": "modify_position",
        "poolId": _pool_id_from_topic(topics[1], "V4 ModifyPosition pool ID"),
        "sender": _address_from_topic(topics[2], "V4 ModifyPosition sender"),
        "tickLower": int(tick_lower),
        "tickUpper": int(tick_upper),
        "liquidityDelta": int(liquidity_delta),
        "salt": "0x" + bytes(salt).hex(),
    }


def discover_v4_dolo_pools(
    logs: list[dict[str, Any]], dolo_address: str
) -> list[dict[str, Any]]:
    """Return initialized v4 pools containing DOLO, preserving bytes32 identity."""
    dolo = _normalized_address(dolo_address, "DOLO address")
    discovered = []
    seen = set()
    for log in logs:
        event = decode_v4_pool_manager_log(log)
        if event is None or event["kind"] != "initialize":
            continue
        if dolo not in {event["currency0"], event["currency1"]}:
            continue
        if event["poolId"] in seen:
            continue
        seen.add(event["poolId"])
        discovered.append({**event, "identifierType": "poolId"})
    return sorted(
        discovered,
        key=lambda row: (row["blockNumber"], row["transactionIndex"], row["logIndex"]),
    )


def _v4_modify_match_key(event: dict[str, Any]) -> tuple[Any, ...]:
    return (
        event["txHash"],
        event["poolId"],
        event["tickLower"],
        event["tickUpper"],
        event["liquidityDelta"],
        event["salt"],
    )


def build_v4_rows(
    pool: dict[str, Any],
    pool_manager_logs: list[dict[str, Any]],
    position_manager_logs: list[dict[str, Any]],
    latest_positions: dict[Any, dict[str, Any]],
    pool_state: dict[str, Any],
    *,
    pool_manager: str,
    position_manager: str,
    archive_sqrt_prices: dict[Any, int] | None = None,
    contract_addresses: set[str] | None = None,
) -> dict[str, Any]:
    """Build v4 rows with exact sender, event-pair, and price evidence."""
    if pool.get("identifierType") != "poolId":
        raise ValueError("V4 pool must use identifierType poolId")
    pool_id = _pool_id_from_topic(pool.get("identifier"), "V4 pool identifier")
    chain_key = str(pool.get("chainKey") or "").strip().lower()
    adapter = str(pool.get("adapter") or "").strip().lower()
    if adapter != "uniswap-v4":
        raise ValueError("V4 rows require the uniswap-v4 adapter")
    pool_manager_address = _normalized_address(pool_manager, "V4 PoolManager")
    position_manager_address = _normalized_address(position_manager, "V4 PositionManager")
    source_key = f"{chain_key}:{adapter}"
    normalized_contracts = {
        _normalized_address(address, "contract address")
        for address in (contract_addresses or set())
    }
    archive_prices = dict(archive_sqrt_prices or {})

    decoded_pool_events = []
    for log in pool_manager_logs:
        if _normalized_address(log.get("address"), "V4 pool log emitter") != pool_manager_address:
            continue
        event = decode_v4_pool_manager_log(log)
        if event is not None and event.get("poolId") == pool_id:
            decoded_pool_events.append(event)
    decoded_position_events = []
    for log in position_manager_logs:
        if _normalized_address(log.get("address"), "V4 position log emitter") != position_manager_address:
            continue
        event = decode_v4_position_manager_log(log)
        if event is not None:
            decoded_position_events.append(event)

    exact_modify_counts: defaultdict[tuple[Any, ...], int] = defaultdict(int)
    for event in decoded_position_events:
        if event["kind"] == "modify_position" and event["poolId"] == pool_id:
            exact_modify_counts[_v4_modify_match_key(event)] += 1

    canonical_modifications = []
    unresolved = []
    token_ids = set()
    for event in decoded_pool_events:
        if event["kind"] != "modify_liquidity":
            continue
        if event["sender"] != position_manager_address:
            unresolved.append(
                {
                    **event,
                    "quality": "partial",
                    "reason": "non-canonical sender; tokenId attribution withheld",
                }
            )
            continue
        token_id = int(event["salt"], 16)
        if token_id <= 0:
            unresolved.append(
                {
                    **event,
                    "quality": "partial",
                    "reason": "canonical sender emitted a zero tokenId salt",
                }
            )
            continue
        token_ids.add(token_id)
        canonical_modifications.append({**event, "tokenId": token_id})

    ownership_events = [
        event
        for event in decoded_position_events
        if event["kind"] == "transfer" and event["tokenId"] in token_ids
    ]
    timeline = sorted(
        decoded_pool_events + ownership_events,
        key=lambda row: (row["blockNumber"], row["transactionIndex"], row["logIndex"]),
    )
    canonical_by_identity = {
        (event["txHash"], event["logIndex"]): event for event in canonical_modifications
    }
    owners: dict[int, str] = {}
    position_liquidity: defaultdict[int, int] = defaultdict(int)
    latest_event_price: int | None = None
    currency0 = pool_state.get("currency0")
    currency1 = pool_state.get("currency1")
    history = []
    for event in timeline:
        if event["kind"] == "initialize":
            latest_event_price = event["sqrtPriceX96"]
            currency0 = event["currency0"]
            currency1 = event["currency1"]
            continue
        if event["kind"] == "swap":
            latest_event_price = event["sqrtPriceX96"]
            continue
        if event["kind"] == "transfer":
            recipient = event["to"]
            if recipient == ZERO_ADDRESS:
                owners.pop(event["tokenId"], None)
            else:
                owners[event["tokenId"]] = recipient
            continue
        canonical = canonical_by_identity.get((event["txHash"], event["logIndex"]))
        if canonical is None:
            continue
        if canonical["timestamp"] is None:
            raise ValueError("V4 liquidity history requires an exact block timestamp")
        token_id = canonical["tokenId"]
        delta = canonical["liquidityDelta"]
        before = position_liquidity[token_id]
        if delta > 0:
            after = before + delta
            action = "Added" if before == 0 else "Increased"
        elif delta < 0:
            if -delta > before:
                raise ValueError(f"V4 token {token_id} decrease exceeds replayed liquidity")
            after = before + delta
            action = "Closed" if after == 0 else "Removed"
        else:
            continue
        position_liquidity[token_id] = after
        owner = owners.get(token_id)
        attribution = _event_attribution(owner, normalized_contracts)
        match_count = exact_modify_counts[_v4_modify_match_key(canonical)]
        sqrt_price = latest_event_price
        price_evidence = "event_state" if sqrt_price is not None else None
        if sqrt_price is None:
            archive_key = (canonical["blockNumber"], canonical["transactionIndex"])
            sqrt_price = archive_prices.get(archive_key)
            if sqrt_price is None:
                sqrt_price = archive_prices.get(canonical["blockNumber"])
            if sqrt_price is not None:
                sqrt_price = _exact_int(sqrt_price, "V4 archive sqrt price")
                price_evidence = "archive_state"
        amount_status = "verified" if match_count == 1 and sqrt_price is not None else "unavailable"
        amount0 = amount1 = None
        if amount_status == "verified":
            amount0, amount1 = amounts_for_liquidity(
                abs(delta),
                sqrt_price,
                sqrt_ratio_at_tick(canonical["tickLower"]),
                sqrt_ratio_at_tick(canonical["tickUpper"]),
            )
        row_quality = attribution["quality"]
        if amount_status == "unavailable" and row_quality == "verified":
            row_quality = "partial"
        normalized_currency0 = (
            _normalized_address(currency0, "V4 currency0") if currency0 is not None else None
        )
        normalized_currency1 = (
            _normalized_address(currency1, "V4 currency1") if currency1 is not None else None
        )
        dolo_raw = paired_raw = None
        if amount0 is not None and DOLO_ADDRESS in {normalized_currency0, normalized_currency1}:
            dolo_raw = amount0 if normalized_currency0 == DOLO_ADDRESS else amount1
            paired_raw = amount1 if normalized_currency0 == DOLO_ADDRESS else amount0
        history.append(
            {
                "id": event_key(chain_key, canonical["txHash"], canonical["logIndex"]),
                "sourceKey": source_key,
                "poolId": pool_id,
                "poolIdentifierType": "poolId",
                "poolExplorerUrl": None,
                "chainKey": chain_key,
                "adapter": adapter,
                "pair": pool.get("pair"),
                "positionId": str(token_id),
                "action": action,
                "blockNumber": canonical["blockNumber"],
                "timestamp": canonical["timestamp"],
                "txHash": canonical["txHash"],
                "logIndex": canonical["logIndex"],
                "liquidityDeltaRaw": str(delta),
                "amountStatus": amount_status,
                "priceEvidence": price_evidence,
                "amount0Raw": str(amount0) if amount0 is not None else None,
                "amount1Raw": str(amount1) if amount1 is not None else None,
                "doloRaw": str(dolo_raw) if dolo_raw is not None else None,
                "pairedRaw": str(paired_raw) if paired_raw is not None else None,
                "valueUsd": None,
                **attribution,
                "quality": row_quality,
            }
        )

    current_sqrt = _exact_int(pool_state.get("sqrtPriceX96"), "current V4 sqrt price")
    current_tick = _exact_int(pool_state.get("currentTick"), "current V4 tick")
    decimals0 = _exact_int(pool_state.get("decimals0"), "V4 currency0 decimals")
    decimals1 = _exact_int(pool_state.get("decimals1"), "V4 currency1 decimals")
    active_positions = []
    for token_key, latest in sorted(latest_positions.items(), key=lambda item: int(item[0])):
        token_id = int(token_key)
        if isinstance(token_key, bool) or token_id <= 0 or not isinstance(latest, dict):
            raise ValueError("latest V4 positions must use positive token IDs")
        latest_pool_id = _pool_id_from_topic(latest.get("poolId"), "latest V4 pool ID")
        if latest_pool_id != pool_id:
            continue
        liquidity_raw = _exact_int(latest.get("liquidity"), "latest V4 liquidity")
        if liquidity_raw <= 0:
            continue
        token0 = _normalized_address(latest.get("currency0"), "latest V4 currency0")
        token1 = _normalized_address(latest.get("currency1"), "latest V4 currency1")
        if DOLO_ADDRESS not in {token0, token1}:
            raise ValueError(f"latest V4 token {token_id} is not a DOLO position")
        tick_lower = _exact_int(latest.get("tickLower"), "latest V4 tick lower")
        tick_upper = _exact_int(latest.get("tickUpper"), "latest V4 tick upper")
        amount0, amount1 = amounts_for_liquidity(
            liquidity_raw,
            current_sqrt,
            sqrt_ratio_at_tick(tick_lower),
            sqrt_ratio_at_tick(tick_upper),
        )
        owner = _normalized_address(latest.get("owner"), "latest V4 owner")
        attribution = _event_attribution(owner, normalized_contracts)
        bound_a = tick_to_paired_per_dolo(
            tick_lower, token0, token1, decimals0, decimals1, DOLO_ADDRESS
        )
        bound_b = tick_to_paired_per_dolo(
            tick_upper, token0, token1, decimals0, decimals1, DOLO_ADDRESS
        )
        range_lower, range_upper = sorted((bound_a, bound_b))
        active_positions.append(
            {
                "id": f"{source_key}:{pool_id}:{token_id}",
                "sourceKey": source_key,
                "poolId": pool_id,
                "poolIdentifierType": "poolId",
                "poolExplorerUrl": None,
                "dexscreenerUrl": pool.get("sourceUrl")
                or f"https://dexscreener.com/{chain_key}/{pool_id}",
                "chainKey": chain_key,
                "adapter": adapter,
                "pair": pool.get("pair"),
                "positionType": "concentrated_nft",
                "positionId": str(token_id),
                "liquidityRaw": str(liquidity_raw),
                "tickLower": tick_lower,
                "tickUpper": tick_upper,
                "rangeLower": str(range_lower),
                "rangeUpper": str(range_upper),
                "rangeStatus": classify_range(current_tick, tick_lower, tick_upper),
                "feeTier": _exact_int(latest.get("fee"), "latest V4 fee"),
                "tickSpacing": _exact_int(
                    latest.get("tickSpacing"), "latest V4 tick spacing"
                ),
                "hooks": _normalized_address(latest.get("hooks"), "latest V4 hooks"),
                "amount0Raw": str(amount0),
                "amount1Raw": str(amount1),
                "doloRaw": str(amount0 if token0 == DOLO_ADDRESS else amount1),
                "pairedRaw": str(amount1 if token0 == DOLO_ADDRESS else amount0),
                "valueUsd": None,
                **attribution,
            }
        )
    source_status = "partial" if unresolved or any(
        row["quality"] == "partial" for row in history
    ) else "complete"
    return {
        "sourceKey": source_key,
        "sourceStatus": source_status,
        "activePositions": active_positions,
        "history": history,
        "unresolved": unresolved,
    }


def decode_kodiak_island_created(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode the official Kodiak Island Factory creation event."""
    topics = log.get("topics") if isinstance(log, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("Kodiak Island factory log topics are required")
    if str(topics[0]).lower() != event_topic(KODIAK_ISLAND_CREATED_SIGNATURE):
        return None
    if len(topics) != 4:
        raise ValueError("IslandCreated must index pool, manager, and island")
    data_hex = str(log.get("data") or "").strip().lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data_hex):
        raise ValueError("IslandCreated data must be even-length hex")
    (implementation,) = decode(["address"], bytes.fromhex(data_hex[2:]))
    return {
        **_decoded_log_base(log),
        "kind": "island_created",
        "underlyingPool": _address_from_topic(topics[1], "Kodiak underlying pool"),
        "manager": _address_from_topic(topics[2], "Kodiak Island manager"),
        "island": _address_from_topic(topics[3], "Kodiak Island"),
        "implementation": _normalized_address(
            implementation, "Kodiak Island implementation"
        ),
    }


def discover_kodiak_islands(
    logs: list[dict[str, Any]],
    pool_tokens: dict[str, tuple[str, str] | list[str]],
    dolo_address: str,
) -> list[dict[str, Any]]:
    """Discover Islands only when their exact underlying V3 pool contains DOLO."""
    dolo = _normalized_address(dolo_address, "DOLO address")
    normalized_pool_tokens = {}
    for pool_address, tokens in pool_tokens.items():
        pool = _normalized_address(pool_address, "Kodiak underlying pool")
        if not isinstance(tokens, (tuple, list)) or len(tokens) != 2:
            raise ValueError("Kodiak pool token metadata must contain token0 and token1")
        normalized_pool_tokens[pool] = (
            _normalized_address(tokens[0], "Kodiak token0"),
            _normalized_address(tokens[1], "Kodiak token1"),
        )
    discovered = []
    seen = set()
    for log in logs:
        event = decode_kodiak_island_created(log)
        if event is None:
            continue
        tokens = normalized_pool_tokens.get(event["underlyingPool"])
        if tokens is None or dolo not in set(tokens) or event["island"] in seen:
            continue
        seen.add(event["island"])
        discovered.append(event)
    return sorted(
        discovered,
        key=lambda row: (row["blockNumber"], row["transactionIndex"], row["logIndex"]),
    )


def _raw_integer(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a nonnegative integer string")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        parsed = int(value)
    else:
        raise ValueError(f"{label} must be a nonnegative integer string")
    if parsed < 0:
        raise ValueError(f"{label} must be nonnegative")
    return parsed


def _allocate_total_by_claims(
    total: int, claims: list[dict[str, Any]]
) -> list[int]:
    """Allocate raw units by share, assigning deterministic dust to the last row."""
    if not claims:
        if total != 0:
            raise ValueError("cannot allocate nonzero underlying without share claims")
        return []
    total_shares = sum(claim["shares"] for claim in claims)
    if total_shares <= 0:
        raise ValueError("allocation total shares must be positive")
    allocated = [total * claim["shares"] // total_shares for claim in claims]
    allocated[-1] += total - sum(allocated)
    return allocated


def allocate_kodiak_island_position(
    underlying_position: dict[str, Any],
    island_state: dict[str, Any],
    farm_states: list[dict[str, Any]],
    *,
    contract_addresses: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Replace one Island-custodied V3 row with exact beneficial-owner rows."""
    if not isinstance(underlying_position, dict):
        raise ValueError("underlying Island position must be an object")
    island = _normalized_address(island_state.get("address"), "Kodiak Island")
    total_shares = _raw_integer(island_state.get("totalShares"), "Island total shares")
    if total_shares <= 0:
        raise ValueError("Island total shares must be positive")
    balances = {
        _normalized_address(address, "Island share holder"): _raw_integer(
            value, "Island share balance"
        )
        for address, value in (island_state.get("balances") or {}).items()
    }
    balances = {address: value for address, value in balances.items() if value > 0}
    if sum(balances.values()) != total_shares:
        raise ValueError("Island holder balances must reconcile exactly to total shares")
    contracts = {
        _normalized_address(address, "contract address")
        for address in (contract_addresses or set())
    }

    farms_by_address = {}
    for state in farm_states:
        if not isinstance(state, dict):
            raise ValueError("Kodiak farm state must be an object")
        farm = _normalized_address(state.get("address"), "Kodiak farm")
        if farm in farms_by_address:
            raise ValueError(f"duplicate Kodiak farm state for {farm}")
        staking_token = _normalized_address(
            state.get("stakingToken"), "Kodiak farm staking token"
        )
        custody_balance = _raw_integer(
            state.get("custodyBalance"), "Kodiak farm custody balance"
        )
        staked_balances = {
            _normalized_address(address, "Kodiak farm staker"): _raw_integer(
                value, "Kodiak farm staked balance"
            )
            for address, value in (state.get("stakedBalances") or {}).items()
        }
        reasons = []
        if staking_token != island:
            reasons.append("staking token mismatch")
        if custody_balance != balances.get(farm, 0):
            reasons.append("farm custody balance mismatch")
        if sum(staked_balances.values()) != custody_balance:
            reasons.append("farm staker balances do not reconcile")
        farms_by_address[farm] = {
            "address": farm,
            "stakedBalances": staked_balances,
            "supported": not reasons,
            "reason": "; ".join(reasons),
        }

    claims = []
    for holder, shares in sorted(balances.items()):
        farm = farms_by_address.get(holder)
        if farm and farm["supported"]:
            for staker, staked_shares in sorted(farm["stakedBalances"].items()):
                if staked_shares <= 0:
                    continue
                if staker in contracts:
                    claims.append(
                        {
                            "beneficialOwner": None,
                            "custodian": staker,
                            "shares": staked_shares,
                            "path": "unresolved_contract",
                            "quality": "unavailable",
                            "status": "custodied_unresolved",
                            "reason": "farm staker is an unsupported contract",
                        }
                    )
                else:
                    claims.append(
                        {
                            "beneficialOwner": staker,
                            "custodian": holder,
                            "shares": staked_shares,
                            "path": "kodiak_island_farm",
                            "quality": "verified",
                            "status": "active",
                            "reason": "farm staking token and per-user balances reconcile",
                        }
                    )
            continue
        if holder in contracts:
            reason = (
                farm["reason"]
                if farm and farm["reason"]
                else "Island shares are held by an unsupported custody contract"
            )
            claims.append(
                {
                    "beneficialOwner": None,
                    "custodian": holder,
                    "shares": shares,
                    "path": "unresolved_contract",
                    "quality": "unavailable",
                    "status": "custodied_unresolved",
                    "reason": reason,
                }
            )
        else:
            claims.append(
                {
                    "beneficialOwner": holder,
                    "custodian": island,
                    "shares": shares,
                    "path": "kodiak_island",
                    "quality": "verified",
                    "status": "active",
                    "reason": "direct Kodiak Island share balance",
                }
            )
    claims.sort(
        key=lambda claim: (
            claim["beneficialOwner"] or "~" + claim["custodian"],
            claim["custodian"],
            claim["path"],
        )
    )
    if sum(claim["shares"] for claim in claims) != total_shares:
        raise ValueError("resolved Island claims must reconcile exactly to total shares")

    raw_fields = ("amount0Raw", "amount1Raw", "doloRaw", "pairedRaw")
    allocations = {
        field: _allocate_total_by_claims(
            _raw_integer(underlying_position.get(field), f"underlying {field}"), claims
        )
        for field in raw_fields
    }
    underlying_quality = underlying_position.get("quality")
    rows = []
    for index, claim in enumerate(claims):
        quality = claim["quality"]
        if quality == "verified" and underlying_quality in {"partial", "stale"}:
            quality = underlying_quality
        identity = claim["beneficialOwner"] or "unresolved-" + claim["custodian"]
        rows.append(
            {
                **underlying_position,
                "id": (
                    f"{underlying_position.get('id')}:{claim['path']}:"
                    f"{claim['custodian']}:{identity}"
                ),
                "positionType": "kodiak_island_share",
                "island": island,
                "shareBalanceRaw": str(claim["shares"]),
                "shareOfIsland": str(Decimal(claim["shares"]) / Decimal(total_shares)),
                "custodian": claim["custodian"],
                "beneficialOwner": claim["beneficialOwner"],
                "attributionPath": claim["path"],
                "attributionReason": claim["reason"],
                "positionStatus": claim["status"],
                "quality": quality,
                **{field: str(allocations[field][index]) for field in raw_fields},
            }
        )
    return rows


def build_kodiak_island_history(
    pool: dict[str, Any],
    island_address: str,
    actions: list[dict[str, Any]],
    *,
    contract_addresses: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Build user liquidity history while excluding custody-only Island actions."""
    chain_key = str(pool.get("chainKey") or "").strip().lower()
    adapter = str(pool.get("adapter") or "").strip().lower()
    pool_address = _normalized_address(pool.get("identifier"), "Kodiak pool")
    island = _normalized_address(island_address, "Kodiak Island")
    token0 = _normalized_address(pool.get("token0"), "Kodiak token0")
    token1 = _normalized_address(pool.get("token1"), "Kodiak token1")
    if DOLO_ADDRESS not in {token0, token1}:
        raise ValueError("Kodiak Island pool does not contain DOLO")
    contracts = {
        _normalized_address(address, "contract address")
        for address in (contract_addresses or set())
    }
    ignored = {"rebalance", "stake", "unstake", "share_transfer"}
    share_balances: defaultdict[str, int] = defaultdict(int)
    history = []
    for action in actions:
        kind = action.get("kind")
        if kind in ignored:
            continue
        if kind not in {"deposit", "withdraw"}:
            raise ValueError(f"unsupported Kodiak Island action {kind!r}")
        owner = _normalized_address(action.get("owner"), "Kodiak Island action owner")
        share_delta = _raw_integer(
            action.get("shareDeltaRaw"), "Kodiak Island share delta"
        )
        if share_delta <= 0:
            raise ValueError("Kodiak Island share delta must be positive")
        before = share_balances[owner]
        if kind == "deposit":
            share_balances[owner] += share_delta
            action_name = "Added" if before == 0 else "Increased"
        else:
            if share_delta > before:
                raise ValueError("Kodiak Island withdrawal exceeds replayed user shares")
            share_balances[owner] -= share_delta
            action_name = "Closed" if share_balances[owner] == 0 else "Removed"
        amount0_value = action.get("amount0Raw")
        amount1_value = action.get("amount1Raw")
        amounts_exact = amount0_value is not None and amount1_value is not None
        amount0 = _raw_integer(amount0_value, "Kodiak Island amount0") if amounts_exact else None
        amount1 = _raw_integer(amount1_value, "Kodiak Island amount1") if amounts_exact else None
        if owner in contracts:
            beneficial_owner = None
            quality = "unavailable"
            status = "custodied_unresolved"
        else:
            beneficial_owner = owner
            quality = "verified" if amounts_exact else "partial"
            status = "active"
        block_number = _exact_int(action.get("blockNumber"), "Island action block")
        timestamp = _exact_int(action.get("timestamp"), "Island action timestamp")
        log_index = _exact_int(action.get("logIndex"), "Island action log index")
        tx_hash = str(action.get("txHash") or "").lower()
        history.append(
            {
                "id": event_key(chain_key, tx_hash, log_index),
                "sourceKey": f"{chain_key}:{adapter}:kodiak-island",
                "poolId": pool_address,
                "poolIdentifierType": "contract",
                "chainKey": chain_key,
                "adapter": adapter,
                "pair": pool.get("pair"),
                "island": island,
                "action": action_name,
                "blockNumber": block_number,
                "timestamp": timestamp,
                "txHash": tx_hash,
                "logIndex": log_index,
                "shareDeltaRaw": str(share_delta),
                "amountStatus": "verified" if amounts_exact else "unavailable",
                "amount0Raw": str(amount0) if amount0 is not None else None,
                "amount1Raw": str(amount1) if amount1 is not None else None,
                "doloRaw": str(amount0 if token0 == DOLO_ADDRESS else amount1)
                if amounts_exact
                else None,
                "pairedRaw": str(amount1 if token0 == DOLO_ADDRESS else amount0)
                if amounts_exact
                else None,
                "valueUsd": None,
                "custodian": island,
                "beneficialOwner": beneficial_owner,
                "attributionPath": "kodiak_island",
                "positionStatus": status,
                "quality": quality,
            }
        )
    return history


def _event_attribution(
    owner: str | None,
    contract_addresses: set[str],
) -> dict[str, Any]:
    if owner is None:
        return {
            "custodian": None,
            "beneficialOwner": None,
            "attributionPath": "unresolved",
            "positionStatus": "custodied_unresolved",
            "quality": "partial",
        }
    if owner in contract_addresses:
        return {
            "custodian": owner,
            "beneficialOwner": None,
            "attributionPath": "unresolved_contract",
            "positionStatus": "custodied_unresolved",
            "quality": "unavailable",
        }
    return {
        "custodian": owner,
        "beneficialOwner": owner,
        "attributionPath": "direct",
        "positionStatus": "active",
        "quality": "verified",
    }


def replay_v2_pool(
    pool: dict[str, Any],
    logs: list[dict[str, Any]],
    latest_state: dict[str, Any],
    *,
    contract_addresses: set[str] | None = None,
) -> dict[str, Any]:
    """Replay one V2-style LP token and return exact current/history rows."""
    if not isinstance(pool, dict) or pool.get("identifierType") != "contract":
        raise ValueError("V2 pool must use a contract identifier")
    chain_key = str(pool.get("chainKey") or "").strip().lower()
    adapter = str(pool.get("adapter") or "").strip().lower()
    if adapter not in {"kodiak-v2", "bulla-v2", "beraswap-v2"}:
        raise ValueError("V2 replay requires a supported V2 adapter")
    pool_address = _normalized_address(pool.get("identifier"), "V2 pool")
    source_key = f"{chain_key}:{adapter}"
    normalized_contracts = {
        _normalized_address(address, "contract address")
        for address in (contract_addresses or set())
    }

    token0 = _normalized_address(latest_state.get("token0"), "V2 token0")
    token1 = _normalized_address(latest_state.get("token1"), "V2 token1")
    dolo = DOLO_ADDRESS
    if dolo not in {token0, token1}:
        raise ValueError("V2 pool does not contain DOLO")
    total_supply = _exact_int(latest_state.get("totalSupply"), "V2 total supply")
    reserve0 = _exact_int(latest_state.get("reserve0"), "V2 reserve0")
    reserve1 = _exact_int(latest_state.get("reserve1"), "V2 reserve1")

    decoded_events = []
    for raw_log in logs:
        log_address = _normalized_address(raw_log.get("address"), "V2 log address")
        if log_address != pool_address:
            continue
        decoded = decode_v2_log(raw_log)
        if decoded is not None:
            decoded_events.append(decoded)
    decoded_events.sort(
        key=lambda row: (
            row["blockNumber"],
            row["transactionIndex"],
            row["logIndex"],
        )
    )

    balances: defaultdict[str, int] = defaultdict(int)
    history: list[dict[str, Any]] = []
    for _tx_hash, group_iter in itertools.groupby(decoded_events, key=lambda row: row["txHash"]):
        group = list(group_iter)
        starting_balances = dict(balances)
        transfers = [event for event in group if event["kind"] == "transfer"]
        for transfer in transfers:
            sender = transfer["from"]
            recipient = transfer["to"]
            value = transfer["valueRaw"]
            if sender != ZERO_ADDRESS:
                balances[sender] -= value
                if balances[sender] < 0:
                    raise ValueError(
                        f"V2 transfer replay produced a negative LP balance for {sender}"
                    )
            if recipient != ZERO_ADDRESS:
                balances[recipient] += value

        mint_recipients = {
            transfer["to"]
            for transfer in transfers
            if transfer["from"] == ZERO_ADDRESS
            and transfer["to"] not in {ZERO_ADDRESS, pool_address}
            and transfer["valueRaw"] > 0
        }
        burn_owners = {
            transfer["from"]
            for transfer in transfers
            if transfer["to"] == pool_address
            and transfer["from"] not in {ZERO_ADDRESS, pool_address}
            and transfer["valueRaw"] > 0
        }

        for event in group:
            if event["kind"] not in {"mint", "burn"}:
                continue
            if event["timestamp"] is None:
                raise ValueError("V2 liquidity history requires an exact block timestamp")
            if event["kind"] == "mint":
                owner = next(iter(mint_recipients)) if len(mint_recipients) == 1 else None
                action = "Added" if owner is None or starting_balances.get(owner, 0) == 0 else "Increased"
            else:
                owner = next(iter(burn_owners)) if len(burn_owners) == 1 else None
                action = "Closed" if owner is not None and balances.get(owner, 0) == 0 else "Removed"
            attribution = _event_attribution(owner, normalized_contracts)
            amount0 = event["amount0Raw"]
            amount1 = event["amount1Raw"]
            history.append(
                {
                    "id": event_key(chain_key, event["txHash"], event["logIndex"]),
                    "sourceKey": source_key,
                    "poolId": pool_address,
                    "poolIdentifierType": "contract",
                    "chainKey": chain_key,
                    "adapter": adapter,
                    "pair": pool.get("pair"),
                    "action": action,
                    "blockNumber": event["blockNumber"],
                    "timestamp": event["timestamp"],
                    "txHash": event["txHash"],
                    "logIndex": event["logIndex"],
                    "amount0Raw": str(amount0),
                    "amount1Raw": str(amount1),
                    "doloRaw": str(amount0 if token0 == dolo else amount1),
                    "pairedRaw": str(amount1 if token0 == dolo else amount0),
                    "valueUsd": None,
                    **attribution,
                }
            )

    onchain_balances = {
        _normalized_address(address, "onchain LP holder"): _exact_int(value, "onchain LP balance")
        for address, value in (latest_state.get("balances") or {}).items()
    }
    mismatches = []
    active_positions = []
    excluded = {ZERO_ADDRESS, pool_address}
    for owner, balance in sorted(balances.items()):
        if balance <= 0 or owner in excluded:
            continue
        amount0, amount1 = v2_underlying(balance, total_supply, reserve0, reserve1)
        attribution = _event_attribution(owner, normalized_contracts)
        onchain_balance = onchain_balances.get(owner)
        if onchain_balance is not None and onchain_balance != balance:
            mismatches.append(
                {
                    "address": owner,
                    "replayedBalanceRaw": str(balance),
                    "onchainBalanceRaw": str(onchain_balance),
                }
            )
            if attribution["quality"] == "verified":
                attribution["quality"] = "partial"
        active_positions.append(
            {
                "id": f"{source_key}:{pool_address}:{owner}",
                "sourceKey": source_key,
                "poolId": pool_address,
                "poolIdentifierType": "contract",
                "chainKey": chain_key,
                "adapter": adapter,
                "pair": pool.get("pair"),
                "positionType": "v2_full_range",
                "lpBalanceRaw": str(balance),
                "onchainLpBalanceRaw": str(onchain_balance) if onchain_balance is not None else None,
                "amount0Raw": str(amount0),
                "amount1Raw": str(amount1),
                "doloRaw": str(amount0 if token0 == dolo else amount1),
                "pairedRaw": str(amount1 if token0 == dolo else amount0),
                "rangeStatus": "full_range",
                "valueUsd": None,
                **attribution,
            }
        )

    return {
        "sourceKey": source_key,
        "sourceStatus": "partial" if mismatches else "complete",
        "activePositions": active_positions,
        "history": history,
        "mismatches": mismatches,
        "ledger": {address: str(value) for address, value in balances.items()},
    }
