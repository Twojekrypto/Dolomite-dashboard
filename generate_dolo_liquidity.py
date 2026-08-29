#!/usr/bin/env python3
"""On-chain DOLO liquidity provider data pipeline.

The first layer in this module deliberately contains only strict registry
parsing and exact AMM arithmetic. Protocol scanners build on these pure
functions so event replay can be tested without network access.
"""

from __future__ import annotations

import argparse
import copy
import inspect
import itertools
import json
import math
import os
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

import requests
from eth_abi import decode, encode
from web3 import Web3

from rpc_client import (
    get_endpoints,
    rpc_batch_requests,
    rpc_single_request,
    sanitize_error,
)
from safe_wallets import SAFE_SINGLETON_ADDRS


Q96 = 1 << 96
DOLO_ADDRESS = "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654"
SUPPORTED_ADAPTERS = {
    "uniswap-v3",
    "uniswap-v4",
    "kodiak-v2",
    "kodiak-v3",
    "bulla-v2",
    "bulla-v3",
    "beraswap-v2",
    "brownfi-v3",
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
BROWNFI_MINT_SIGNATURE = "Mint(address,uint256,uint256,uint256,uint256,uint256,address)"
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
KODIAK_FARM_DEPLOYED_SIGNATURE = "FarmDeployed(address,address)"
KODIAK_STAKE_LOCKED_SIGNATURE = "StakeLocked(address,uint256,uint256,bytes32)"
KODIAK_WITHDRAW_LOCKED_SIGNATURE = "WithdrawLocked(address,uint256,bytes32)"
BULLA_INCREASE_SIGNATURE = "IncreaseLiquidity(uint256,uint128,uint128,uint256,uint256,address)"
KODIAK_V3_SUBGRAPH = (
    "https://api.subgraph.ormilabs.com/api/public/"
    "d7eed6cc-ad4a-4862-8017-89893c4095d3/subgraphs/kodiak-v3/latest/gn"
)
BULLA_BOUNDED_LOOKBACK_BLOCKS = 1_500_000
ROUTESCAN_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
_KODIAK_FARM_INDEX_CACHE: dict[tuple[str, str, int], dict[str, list[dict[str, Any]]]] = {}


def _finite_decimal(value: Any, label: str, *, allow_zero: bool = True) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{label} must be a finite decimal") from exc
    if not parsed.is_finite() or parsed < 0 or (not allow_zero and parsed == 0):
        raise ValueError(f"{label} must be a finite nonnegative decimal")
    return parsed


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


def incremental_pool_context(
    previous: dict[str, Any],
    source_key: str,
    pool_id: str,
    configured_start: int,
) -> dict[str, Any]:
    """Reuse a source cursor and known live NFT IDs without trusting stale values."""
    if not isinstance(previous, dict):
        raise ValueError("previous artifact must be an object")
    normalized_source = str(source_key or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9-]+:[a-z0-9-]+", normalized_source):
        raise ValueError("source key must contain normalized chain and adapter names")
    normalized_pool = str(pool_id or "").strip().lower()
    if not (ADDRESS_RE.fullmatch(normalized_pool) or POOL_ID_RE.fullmatch(normalized_pool)):
        raise ValueError("pool identifier must be an address or bytes32 pool ID")

    matching_sources = [
        row
        for row in previous.get("sources", [])
        if isinstance(row, dict) and row.get("key") == normalized_source
    ]
    if len(matching_sources) > 1:
        raise ValueError(f"previous artifact has duplicate source {normalized_source}")
    pool_was_tracked = any(
        isinstance(row, dict)
        and str(row.get("sourceKey") or "").lower() == normalized_source
        and str(row.get("id") or row.get("identifier") or "").lower()
        == normalized_pool
        for row in previous.get("pools", [])
    ) or any(
        isinstance(row, dict)
        and str(row.get("sourceKey") or "").lower() == normalized_source
        and str(row.get("poolId") or "").lower() == normalized_pool
        for collection in ("activePositions", "history")
        for row in previous.get(collection, [])
    )
    previous_source = (
        matching_sources[0]
        if matching_sources and pool_was_tracked
        else None
    )
    token_ids: set[int] = set()
    for row in previous.get("activePositions", []):
        if not isinstance(row, dict):
            continue
        if row.get("sourceKey") != normalized_source or row.get("poolId") != normalized_pool:
            continue
        position_type = str(row.get("positionType") or "").strip().lower()
        if position_type and position_type != "concentrated_nft":
            continue
        raw_token_id = row.get("positionId")
        if isinstance(raw_token_id, bool) or not isinstance(raw_token_id, (int, str)):
            raise ValueError("previous positionId must be a nonnegative integer")
        token_text = str(raw_token_id).strip()
        if not re.fullmatch(r"\d+", token_text):
            raise ValueError("previous positionId must be a nonnegative integer")
        token_ids.add(int(token_text))

    history = []
    for row in previous.get("history", []):
        if not isinstance(row, dict):
            continue
        if row.get("sourceKey") != normalized_source or row.get("poolId") != normalized_pool:
            continue
        preserved = copy.deepcopy(row)
        preserved.pop("staleSince", None)
        if preserved.get("quality") == "stale":
            preserved["quality"] = "partial"
        history.append(preserved)
    history.sort(
        key=lambda row: (
            row.get("blockNumber", -1),
            row.get("logIndex", -1),
            str(row.get("id") or ""),
        )
    )
    return {
        "incremental": previous_source is not None,
        "scanStart": resume_block(previous_source, configured_start),
        "tokenIds": token_ids,
        "history": history,
    }


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


def assert_refresh_not_degraded(
    previous: dict[str, Any],
    candidate: dict[str, Any],
) -> None:
    """Reject failed refreshes so they cannot replace a last-known-good artifact."""
    if not previous:
        return
    if not isinstance(previous, dict) or not isinstance(candidate, dict):
        raise ValueError("liquidity refresh artifacts must be objects")
    failed_sources = []
    for source in candidate.get("sources", []):
        if not isinstance(source, dict):
            raise ValueError("candidate liquidity sources must be objects")
        errors = source.get("errors")
        if source.get("status") == "stale" or (isinstance(errors, list) and errors):
            source_key = str(source.get("key") or "unknown")
            error_details = [
                sanitize_error(error)
                for error in (errors if isinstance(errors, list) else [])
                if str(error or "").strip()
            ]
            failed_sources.append(
                f"{source_key} ({'; '.join(error_details)})"
                if error_details
                else source_key
            )
    if failed_sources:
        raise RuntimeError(
            "degraded liquidity refresh rejected; failed sources: "
            + ", ".join(sorted(failed_sources))
        )


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
    if signature_topic == event_topic(BROWNFI_MINT_SIGNATURE):
        if len(topics) != 3:
            raise ValueError("BrownFi Mint must have indexed sender and recipient")
        amount0, amount1, _price0, _price1, _amm_price = decode(
            ["uint256", "uint256", "uint256", "uint256", "uint256"],
            data,
        )
        return {
            **base,
            "kind": "mint",
            "sender": _address_from_topic(topics[1], "BrownFi mint sender"),
            "to": _address_from_topic(topics[2], "BrownFi mint recipient"),
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


def first_v3_increase_blocks(events: list[dict[str, Any]]) -> dict[int, int]:
    """Return first newly observed increase block without requiring seeded IDs."""
    first_blocks: dict[int, int] = {}
    for event in events:
        if not isinstance(event, dict) or event.get("kind") != "increase":
            continue
        token_id = _exact_int(event.get("tokenId"), "V3 tokenId")
        block_number = _exact_int(event.get("blockNumber"), "V3 block number")
        if token_id < 0 or block_number < 0:
            raise ValueError("V3 token ID and block number must be nonnegative")
        first_blocks[token_id] = min(first_blocks.get(token_id, block_number), block_number)
    return first_blocks


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
    if adapter not in {"uniswap-v3", "kodiak-v3", "bulla-v3"}:
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


def partition_v4_modifications(
    modifications: list[dict[str, Any]], canonical_sender: str
) -> dict[str, list[dict[str, Any]]]:
    """Keep canonical NFT and non-canonical manager liquidity in separate paths."""
    canonical = _normalized_address(canonical_sender, "V4 canonical sender")
    result = {"canonical": [], "noncanonical": []}
    for event in modifications:
        if not isinstance(event, dict) or event.get("kind") != "modify_liquidity":
            raise ValueError("V4 sender partition requires ModifyLiquidity events")
        sender = _normalized_address(event.get("sender"), "V4 liquidity sender")
        result["canonical" if sender == canonical else "noncanonical"].append(event)
    return result


def build_v4_unresolved_sender_rows(
    pool: dict[str, Any],
    modifications: list[dict[str, Any]],
    pool_state: dict[str, Any],
) -> list[dict[str, Any]]:
    """Expose exact in-pool amounts for unsupported V4 manager custody."""
    pool_id = _pool_id_from_topic(pool.get("identifier"), "V4 pool ID")
    currency0 = _normalized_address(pool_state.get("currency0"), "V4 currency0")
    currency1 = _normalized_address(pool_state.get("currency1"), "V4 currency1")
    if DOLO_ADDRESS not in {currency0, currency1}:
        raise ValueError("V4 unresolved manager pool must contain DOLO")
    current_sqrt = _exact_int(pool_state.get("sqrtPriceX96"), "current V4 sqrt price")
    current_tick = _exact_int(pool_state.get("currentTick"), "current V4 tick")
    decimals0 = _exact_int(pool_state.get("decimals0"), "V4 currency0 decimals")
    decimals1 = _exact_int(pool_state.get("decimals1"), "V4 currency1 decimals")
    net: defaultdict[tuple[str, int, int, str], int] = defaultdict(int)
    for event in modifications:
        if event.get("kind") != "modify_liquidity":
            raise ValueError("unsupported manager rows require ModifyLiquidity events")
        if _pool_id_from_topic(event.get("poolId"), "V4 event pool ID") != pool_id:
            continue
        sender = _normalized_address(event.get("sender"), "V4 liquidity sender")
        lower = _exact_int(event.get("tickLower"), "V4 lower tick")
        upper = _exact_int(event.get("tickUpper"), "V4 upper tick")
        if lower >= upper:
            raise ValueError("V4 manager tick range must be increasing")
        salt = _pool_id_from_topic(event.get("salt"), "V4 position salt")
        net[(sender, lower, upper, salt)] += _exact_int(
            event.get("liquidityDelta"), "V4 liquidity delta"
        )
    grouped: defaultdict[str, list[tuple[int, int, str, int]]] = defaultdict(list)
    for (sender, lower, upper, salt), liquidity_raw in sorted(net.items()):
        if liquidity_raw < 0:
            raise ValueError("V4 manager liquidity replay became negative")
        if liquidity_raw > 0:
            grouped[sender].append((lower, upper, salt, liquidity_raw))
    rows = []
    for sender, positions in sorted(grouped.items()):
        amount0 = amount1 = total_liquidity = 0
        ranges = []
        bounds = []
        any_in_range = False
        for lower, upper, salt, liquidity_raw in positions:
            part0, part1 = amounts_for_liquidity(
                liquidity_raw,
                current_sqrt,
                sqrt_ratio_at_tick(lower),
                sqrt_ratio_at_tick(upper),
            )
            amount0 += part0
            amount1 += part1
            total_liquidity += liquidity_raw
            in_range = lower <= current_tick < upper
            any_in_range = any_in_range or in_range
            bound_a = tick_to_paired_per_dolo(
                lower, currency0, currency1, decimals0, decimals1, DOLO_ADDRESS
            )
            bound_b = tick_to_paired_per_dolo(
                upper, currency0, currency1, decimals0, decimals1, DOLO_ADDRESS
            )
            low, high = sorted((bound_a, bound_b))
            bounds.extend((low, high))
            ranges.append(
                {
                    "salt": salt,
                    "tickLower": lower,
                    "tickUpper": upper,
                    "liquidityRaw": str(liquidity_raw),
                    "rangeLower": str(low),
                    "rangeUpper": str(high),
                    "rangeStatus": "in_range" if in_range else "out_of_range",
                }
            )
        range_lower = min(bounds)
        range_upper = max(bounds)
        rows.append(
            {
                "id": f"{pool['chainKey']}:uniswap-v4:{pool_id}:manager:{sender}",
                "sourceKey": f"{pool['chainKey']}:uniswap-v4",
                "poolId": pool_id,
                "poolIdentifierType": "poolId",
                "poolExplorerUrl": None,
                "dexscreenerUrl": pool.get("sourceUrl")
                or f"https://dexscreener.com/{pool['chainKey']}/{pool_id}",
                "chainKey": pool["chainKey"],
                "adapter": "uniswap-v4",
                "pair": pool.get("pair"),
                "positionType": "uniswap_v4_manager_custody",
                "positionId": sender,
                "liquidityRaw": str(total_liquidity),
                "rangeLower": str(range_lower),
                "rangeUpper": str(range_upper),
                "rangeStatus": "in_range" if any_in_range else "out_of_range",
                "positionRanges": ranges,
                "amount0Raw": str(amount0),
                "amount1Raw": str(amount1),
                "doloRaw": str(amount0 if currency0 == DOLO_ADDRESS else amount1),
                "pairedRaw": str(amount1 if currency0 == DOLO_ADDRESS else amount0),
                "valueUsd": None,
                "custodian": sender,
                "beneficialOwner": None,
                "attributionPath": "unresolved_contract",
                "attributionReason": (
                    "non-canonical V4 manager; in-pool liquidity is exact but "
                    "beneficial share ownership is unresolved"
                ),
                "positionStatus": "custodied_unresolved",
                "quality": "unavailable",
            }
        )
    return rows


def _build_v4_noncanonical_rows(
    registry: dict[str, Any],
    pool: dict[str, Any],
    modifications: list[dict[str, Any]],
    pool_state: dict[str, Any],
    latest_block: int,
) -> dict[str, Any]:
    """Resolve supported V4 share vaults and retain exact unsupported custody."""
    aggregate_rows = build_v4_unresolved_sender_rows(pool, modifications, pool_state)
    if not aggregate_rows:
        return {"activePositions": [], "unresolved": []}
    chain_key = pool["chainKey"]
    chain = registry["chains"][chain_key]
    expected_pool_manager = chain["adapters"]["uniswap-v4"]["poolManager"]
    active = []
    unresolved = []
    for aggregate in aggregate_rows:
        vault = aggregate["custodian"]
        try:
            pool_manager, = _eth_call(chain_key, vault, "poolManager()", ["address"])
            if _normalized_address(pool_manager, "V4 vault pool manager") != expected_pool_manager:
                raise ValueError("vault pool manager mismatch")
            pool_key, = _eth_call(
                chain_key,
                vault,
                "poolKey()",
                ["(address,address,uint24,int24,address)"],
            )
            normalized_pool_key = (
                _normalized_address(pool_key[0], "V4 vault currency0"),
                _normalized_address(pool_key[1], "V4 vault currency1"),
                _exact_int(int(pool_key[2]), "V4 vault fee"),
                _exact_int(int(pool_key[3]), "V4 vault tick spacing"),
                _normalized_address(pool_key[4], "V4 vault hooks"),
            )
            if _v4_pool_id(normalized_pool_key) != pool["identifier"]:
                raise ValueError("vault pool key mismatch")
            total0, total1, _fee0, _fee1 = _eth_call(
                chain_key,
                vault,
                "getTotalAmounts()",
                ["uint256", "uint256", "uint256", "uint256"],
            )
            total_supply, = _eth_call(chain_key, vault, "totalSupply()", ["uint256"])
            total0 = _raw_integer(int(total0), "V4 vault total0")
            total1 = _raw_integer(int(total1), "V4 vault total1")
            total_supply = _raw_integer(int(total_supply), "V4 vault total supply")
            if total_supply <= 0:
                raise ValueError("V4 vault total supply must be positive")
            transfer_logs = _routescan_logs(
                chain["chainId"],
                vault,
                event_topic(V2_EVENT_SIGNATURES["transfer"]),
                chain["discoveryStartBlock"],
                latest_block,
            )
            balances = replay_erc20_share_balances(transfer_logs, total_supply)
            contracts = _contract_owners(chain_key, set(balances) | {vault})
            currency0 = pool_state["currency0"]
            underlying = {
                **aggregate,
                "id": f"{aggregate['id']}:vault-total",
                "positionType": "uniswap_v4_share_vault",
                "amount0Raw": str(total0),
                "amount1Raw": str(total1),
                "doloRaw": str(total0 if currency0 == DOLO_ADDRESS else total1),
                "pairedRaw": str(total1 if currency0 == DOLO_ADDRESS else total0),
                "attributionPath": "uniswap_v4_share_vault",
                "attributionReason": "exact V4 share-vault totals and ERC-20 balances",
                "quality": "verified",
            }
            allocated = allocate_v4_share_vault_position(
                underlying,
                {"address": vault, "totalShares": total_supply, "balances": balances},
                contract_addresses=contracts,
            )
            active.extend(allocated)
            unresolved.extend(
                {
                    "manager": vault,
                    "custodian": row.get("custodian"),
                    "reason": row.get("attributionReason"),
                }
                for row in allocated
                if not row.get("beneficialOwner")
            )
        except Exception as exc:
            reason = sanitize_error(exc)
            aggregate["attributionReason"] = (
                "non-canonical V4 manager retained as custody because exact share "
                f"attribution is unavailable: {reason}"
            )
            active.append(aggregate)
            unresolved.append({"manager": vault, "reason": reason})
    return {"activePositions": active, "unresolved": unresolved}


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


def _ordered_event_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(logs, list):
        raise ValueError("event logs must be a list")
    return sorted(
        logs,
        key=lambda log: (
            _exact_int(log.get("blockNumber"), "event block number"),
            _exact_int(log.get("transactionIndex"), "event transaction index"),
            _exact_int(log.get("logIndex"), "event log index"),
        ),
    )


def replay_erc20_share_balances(
    logs: list[dict[str, Any]], total_supply: int
) -> dict[str, int]:
    """Replay canonical ERC-20 transfers and reconcile current share supply."""
    expected_supply = _raw_integer(total_supply, "share total supply")
    transfer_topic = event_topic("Transfer(address,address,uint256)")
    balances: defaultdict[str, int] = defaultdict(int)
    for log in _ordered_event_logs(logs):
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 3:
            raise ValueError("share Transfer must have sender and recipient topics")
        if str(topics[0]).lower() != transfer_topic:
            raise ValueError("share replay received a non-Transfer event")
        sender = _address_from_topic(topics[1], "share sender")
        recipient = _address_from_topic(topics[2], "share recipient")
        data_hex = str(log.get("data") or "").strip().lower()
        if not re.fullmatch(r"0x[0-9a-f]{64}", data_hex):
            raise ValueError("share Transfer amount must be one uint256 word")
        (amount,) = decode(["uint256"], bytes.fromhex(data_hex[2:]))
        amount = _raw_integer(int(amount), "share Transfer amount")
        if sender != ZERO_ADDRESS:
            if balances[sender] < amount:
                raise ValueError("share transfer exceeds proven balance")
            balances[sender] -= amount
        if recipient != ZERO_ADDRESS:
            balances[recipient] += amount
    result = {address: amount for address, amount in sorted(balances.items()) if amount > 0}
    if sum(result.values()) != expected_supply:
        raise ValueError("share balances do not reconcile to total supply")
    return result


def replay_kodiak_farm_balances(
    logs: list[dict[str, Any]], total_locked: int
) -> dict[str, int]:
    """Replay Kodiak locked-stake events and reconcile current farm custody."""
    expected_locked = _raw_integer(total_locked, "farm total locked")
    stake_topic = event_topic("StakeLocked(address,uint256,uint256,bytes32)")
    withdraw_topic = event_topic("WithdrawLocked(address,uint256,bytes32)")
    balances: defaultdict[str, int] = defaultdict(int)
    for log in _ordered_event_logs(logs):
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 2:
            raise ValueError("Kodiak farm event must have one user topic")
        signature = str(topics[0]).lower()
        user = _address_from_topic(topics[1], "Kodiak farm user")
        data_hex = str(log.get("data") or "").strip().lower()
        if not re.fullmatch(r"0x(?:[0-9a-f]{64})+", data_hex):
            raise ValueError("Kodiak farm event data must contain ABI words")
        data = bytes.fromhex(data_hex[2:])
        if signature == stake_topic:
            amount, _seconds, _stake_id = decode(
                ["uint256", "uint256", "bytes32"], data
            )
            balances[user] += _raw_integer(int(amount), "Kodiak farm stake")
        elif signature == withdraw_topic:
            amount, _stake_id = decode(["uint256", "bytes32"], data)
            amount = _raw_integer(int(amount), "Kodiak farm withdrawal")
            if balances[user] < amount:
                raise ValueError("Kodiak farm withdrawal exceeds proven stake")
            balances[user] -= amount
        else:
            raise ValueError("farm replay received an unsupported event")
    result = {address: amount for address, amount in sorted(balances.items()) if amount > 0}
    if sum(result.values()) != expected_locked:
        raise ValueError("farm balances do not reconcile to total locked")
    return result


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
        unresolved_balance = _raw_integer(
            state.get("unresolvedBalance", 0), "Kodiak staking unresolved balance"
        )
        reasons = []
        if staking_token != island:
            reasons.append("staking token mismatch")
        if custody_balance != balances.get(farm, 0):
            reasons.append("farm custody balance mismatch")
        if sum(staked_balances.values()) + unresolved_balance != custody_balance:
            reasons.append("farm staker balances do not reconcile")
        farms_by_address[farm] = {
            "address": farm,
            "stakedBalances": staked_balances,
            "supported": not reasons,
            "reason": "; ".join(reasons),
            "unresolvedBalance": unresolved_balance,
            "attributionPath": state.get(
                "attributionPath", "kodiak_island_farm"
            ),
            "attributionReason": state.get(
                "attributionReason",
                "farm staking token and per-user balances reconcile",
            ),
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
                            "path": farm["attributionPath"],
                            "quality": "verified",
                            "status": "active",
                            "reason": farm["attributionReason"],
                        }
                    )
            if farm["unresolvedBalance"] > 0:
                claims.append(
                    {
                        "beneficialOwner": None,
                        "custodian": holder,
                        "shares": farm["unresolvedBalance"],
                        "path": "unresolved_contract",
                        "quality": "unavailable",
                        "status": "custodied_unresolved",
                        "reason": (
                            "exact on-chain staking residual has no proven wallet owner"
                        ),
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
    allocation_group = f"{underlying_position.get('id')}:kodiak-island:{island}"
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
                "allocationGroup": allocation_group,
                "allocationTotalAmount0Raw": str(
                    _raw_integer(underlying_position.get("amount0Raw"), "underlying amount0Raw")
                ),
                "allocationTotalAmount1Raw": str(
                    _raw_integer(underlying_position.get("amount1Raw"), "underlying amount1Raw")
                ),
                "allocationTotalDoloRaw": str(
                    _raw_integer(underlying_position.get("doloRaw"), "underlying doloRaw")
                ),
                "allocationTotalPairedRaw": str(
                    _raw_integer(underlying_position.get("pairedRaw"), "underlying pairedRaw")
                ),
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


def allocate_v4_share_vault_position(
    underlying_position: dict[str, Any],
    vault_state: dict[str, Any],
    *,
    contract_addresses: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Allocate exact V4 fungible-vault totals without guessing contract owners."""
    if not isinstance(underlying_position, dict):
        raise ValueError("underlying V4 vault position must be an object")
    vault = _normalized_address(vault_state.get("address"), "V4 share vault")
    total_shares = _raw_integer(vault_state.get("totalShares"), "V4 vault total shares")
    if total_shares <= 0:
        raise ValueError("V4 vault total shares must be positive")
    balances = {
        _normalized_address(address, "V4 vault share holder"): _raw_integer(
            value, "V4 vault share balance"
        )
        for address, value in (vault_state.get("balances") or {}).items()
    }
    balances = {address: value for address, value in balances.items() if value > 0}
    if sum(balances.values()) != total_shares:
        raise ValueError("V4 vault holder balances must reconcile exactly to total shares")
    contracts = {
        _normalized_address(address, "contract address")
        for address in (contract_addresses or set())
    }
    claims = []
    for holder, shares in sorted(balances.items()):
        unresolved = holder in contracts
        claims.append(
            {
                "beneficialOwner": None if unresolved else holder,
                "custodian": holder if unresolved else vault,
                "shares": shares,
                "path": "unresolved_contract" if unresolved else "uniswap_v4_share_vault",
                "quality": "unavailable" if unresolved else "verified",
                "status": "custodied_unresolved" if unresolved else "active",
                "reason": (
                    "vault shares are held by an unsupported custody contract"
                    if unresolved
                    else "direct ERC-20 share balance in a verified V4 liquidity vault"
                ),
            }
        )
    raw_fields = ("amount0Raw", "amount1Raw", "doloRaw", "pairedRaw")
    allocations = {
        field: _allocate_total_by_claims(
            _raw_integer(underlying_position.get(field), f"underlying {field}"),
            claims,
        )
        for field in raw_fields
    }
    allocation_group = f"{underlying_position.get('id')}:v4-share-vault:{vault}"
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
                "positionType": "uniswap_v4_vault_share",
                "allocationGroup": allocation_group,
                "allocationTotalAmount0Raw": str(
                    _raw_integer(underlying_position.get("amount0Raw"), "underlying amount0Raw")
                ),
                "allocationTotalAmount1Raw": str(
                    _raw_integer(underlying_position.get("amount1Raw"), "underlying amount1Raw")
                ),
                "allocationTotalDoloRaw": str(
                    _raw_integer(underlying_position.get("doloRaw"), "underlying doloRaw")
                ),
                "allocationTotalPairedRaw": str(
                    _raw_integer(underlying_position.get("pairedRaw"), "underlying pairedRaw")
                ),
                "vault": vault,
                "shareBalanceRaw": str(claim["shares"]),
                "shareOfVault": str(Decimal(claim["shares"]) / Decimal(total_shares)),
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


def derive_dexscreener_pool_metadata(
    pool: dict[str, Any], pair: dict[str, Any], dolo_price: Decimal
) -> dict[str, Any]:
    """Return market metadata only; Dexscreener never supplies LP ownership."""
    chain_key = str(pool.get("chainKey") or "").lower()
    if str(pair.get("chainId") or "").lower() != chain_key:
        raise ValueError("Dexscreener chain does not match the registered pool")
    identifier = str(pool.get("identifier") or "").lower()
    pair_address = str(pair.get("pairAddress") or "").lower()
    if pair_address != identifier:
        raise ValueError("Dexscreener pair identity does not match the registered pool")
    canonical_dolo_price = _finite_decimal(dolo_price, "DOLO price", allow_zero=False)
    base_address = _normalized_address(
        (pair.get("baseToken") or {}).get("address"), "Dexscreener base token"
    )
    quote_address = _normalized_address(
        (pair.get("quoteToken") or {}).get("address"), "Dexscreener quote token"
    )
    if DOLO_ADDRESS not in {base_address, quote_address}:
        raise ValueError("Dexscreener pair does not contain DOLO")
    if base_address == DOLO_ADDRESS:
        native_ratio = _finite_decimal(
            pair.get("priceNative"), "Dexscreener native price", allow_zero=False
        )
        paired_price = canonical_dolo_price / native_ratio
    else:
        paired_price = _finite_decimal(
            pair.get("priceUsd"), "Dexscreener base-token price", allow_zero=False
        )
    liquidity = _finite_decimal(
        (pair.get("liquidity") or {}).get("usd"), "Dexscreener liquidity"
    )
    volume = _finite_decimal(
        (pair.get("volume") or {}).get("h24"), "Dexscreener 24h volume"
    )
    return {
        "doloPriceUsd": float(canonical_dolo_price),
        "pairedPriceUsd": float(paired_price),
        "liquidityUsd": float(liquidity),
        "volume24hUsd": float(volume),
        "dexscreenerUrl": str(pair.get("url") or pool.get("sourceUrl") or ""),
        "priceStatus": "verified",
    }


def value_position_row(
    row: dict[str, Any],
    *,
    dolo_decimals: int,
    paired_decimals: int,
    dolo_price_usd: Decimal | None,
    paired_price_usd: Decimal | None,
) -> dict[str, Any]:
    """Value an exact raw position; missing market prices stay explicitly null."""
    output = dict(row)
    dolo_raw = _raw_integer(row.get("doloRaw"), "position doloRaw")
    paired_raw = _raw_integer(row.get("pairedRaw"), "position pairedRaw")
    for value, label in ((dolo_decimals, "DOLO decimals"), (paired_decimals, "paired decimals")):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 255:
            raise ValueError(f"{label} must be an integer from 0 to 255")
    if dolo_price_usd is None or paired_price_usd is None:
        output["valueUsd"] = None
        output["valueStatus"] = "unavailable"
        return output
    dolo_price = _finite_decimal(dolo_price_usd, "DOLO price")
    paired_price = _finite_decimal(paired_price_usd, "paired price")
    value = (
        Decimal(dolo_raw) * dolo_price / (Decimal(10) ** dolo_decimals)
        + Decimal(paired_raw) * paired_price / (Decimal(10) ** paired_decimals)
    )
    output["valueUsd"] = float(value.quantize(Decimal("0.000001")))
    output["valueStatus"] = "verified"
    return output


def value_exact_history_rows(
    rows: list[dict[str, Any]],
    *,
    dolo_decimals: int,
    paired_decimals: int,
    dolo_price_usd: Decimal | None,
    paired_price_usd: Decimal | None,
) -> list[dict[str, Any]]:
    """Value only history rows whose token amounts are exactly known."""
    return [
        value_position_row(
            row,
            dolo_decimals=dolo_decimals,
            paired_decimals=paired_decimals,
            dolo_price_usd=dolo_price_usd,
            paired_price_usd=paired_price_usd,
        )
        for row in rows
        if row.get("doloRaw") is not None and row.get("pairedRaw") is not None
    ]


def assemble_artifact(
    registry: dict[str, Any],
    sources: list[dict[str, Any]],
    pools: list[dict[str, Any]],
    active_positions: list[dict[str, Any]],
    history: list[dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    """Build the deterministic, UI-ready liquidity artifact."""
    pool_priority = {
        row["identifier"]: row["priority"] for row in registry.get("pools", [])
    }
    sorted_pools = sorted(
        (dict(row) for row in pools),
        key=lambda row: (
            pool_priority.get(row.get("identifier") or row.get("id"), 10**9),
            str(row.get("pair") or ""),
            str(row.get("identifier") or row.get("id") or ""),
        ),
    )

    def active_key(row: dict[str, Any]) -> tuple[Any, ...]:
        value = row.get("valueUsd")
        available = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        return (0 if available else 1, -float(value) if available else 0, str(row.get("id") or ""))

    sorted_active = sorted((dict(row) for row in active_positions), key=active_key)
    sorted_history = sorted(
        (dict(row) for row in history),
        key=lambda row: (
            -int(row.get("blockNumber") or 0),
            -int(row.get("logIndex") or 0),
            str(row.get("id") or ""),
        ),
    )
    values = [
        Decimal(str(row["valueUsd"]))
        for row in sorted_active
        if row.get("valueUsd") is not None
    ]
    quality_counts = {
        quality: sum(row.get("quality") == quality for row in sorted_active)
        for quality in ("verified", "partial", "stale", "unavailable")
    }
    for pool in sorted_pools:
        pool_id = str(pool.get("identifier") or pool.get("id") or "").lower()
        pool_rows = [
            row
            for row in sorted_active
            if str(row.get("poolId") or "").lower() == pool_id
            and row.get("valueUsd") is not None
        ]
        attributed = sum(
            (Decimal(str(row["valueUsd"])) for row in pool_rows), Decimal(0)
        )
        verified_wallet = sum(
            (
                Decimal(str(row["valueUsd"]))
                for row in pool_rows
                if row.get("beneficialOwner") and row.get("quality") == "verified"
            ),
            Decimal(0),
        )
        unresolved_custody = sum(
            (
                Decimal(str(row["valueUsd"]))
                for row in pool_rows
                if not row.get("beneficialOwner")
            ),
            Decimal(0),
        )
        money_quantum = Decimal("0.000001")
        coverage = {
            "attributedValueUsd": float(attributed.quantize(money_quantum)),
            "verifiedWalletValueUsd": float(verified_wallet.quantize(money_quantum)),
            "unresolvedCustodyValueUsd": float(unresolved_custody.quantize(money_quantum)),
            "coveragePct": None,
            "residualValueUsd": None,
            "status": "unavailable",
            "residualReason": "Pool liquidity is unavailable for coverage comparison.",
        }
        liquidity_value = pool.get("liquidityUsd")
        if (
            isinstance(liquidity_value, (int, float))
            and not isinstance(liquidity_value, bool)
            and math.isfinite(liquidity_value)
            and liquidity_value >= 0
        ):
            pool_liquidity = Decimal(str(liquidity_value))
            tolerance = Decimal("0.000001")
            residual = pool_liquidity - attributed
            over_attributed = residual < -tolerance
            if over_attributed or abs(residual) <= tolerance:
                residual = Decimal(0)
            coverage_pct = (
                Decimal(100)
                if pool_liquidity == 0 and attributed == 0
                else Decimal(0)
                if pool_liquidity == 0
                else attributed * Decimal(100) / pool_liquidity
            )
            complete = Decimal("99.5") <= coverage_pct <= Decimal("100.5")
            coverage.update(
                {
                    "coveragePct": float(coverage_pct.quantize(Decimal("0.0001"))),
                    "residualValueUsd": float(residual.quantize(money_quantum)),
                    "status": "complete" if complete else "partial",
                    "residualReason": (
                        "Attributed active positions reconcile with pool liquidity."
                        if complete
                        else "Attributed position valuation exceeds pool liquidity; pricing or source timing requires review."
                        if over_attributed
                        else "Pool liquidity exceeds currently attributed active positions."
                    ),
                }
            )
        pool["coverage"] = coverage
    return {
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "summary": {
            "activeLiquidityUsd": float(sum(values, Decimal(0)).quantize(Decimal("0.000001"))),
            "lpWallets": len(
                {row.get("beneficialOwner") for row in sorted_active if row.get("beneficialOwner")}
            ),
            "activePositions": len(sorted_active),
            "outOfRange": sum(row.get("rangeStatus") == "out_of_range" for row in sorted_active),
        },
        "sources": sorted(
            (dict(row) for row in sources), key=lambda row: str(row.get("key") or "")
        ),
        "pools": sorted_pools,
        "activePositions": sorted_active,
        "history": sorted_history,
        "quality": {
            "verifiedActivePositions": quality_counts["verified"],
            "partialActivePositions": quality_counts["partial"],
            "staleActivePositions": quality_counts["stale"],
            "unavailableActivePositions": quality_counts["unavailable"],
            "unresolvedCustody": sum(not row.get("beneficialOwner") for row in sorted_active),
        },
    }


def write_artifact_atomic(
    path: str | Path, artifact: dict[str, Any], *, max_bytes: int = 2_000_000
) -> None:
    """Write one bounded artifact without exposing a partially written JSON file."""
    output = Path(path)
    encoded = (json.dumps(artifact, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    if len(encoded) >= max_bytes:
        raise ValueError(
            f"liquidity artifact is {len(encoded):,} bytes; limit is {max_bytes:,}"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()


def _latest_block(chain_key: str) -> int:
    response = rpc_single_request(
        get_endpoints(chain_key),
        {"jsonrpc": "2.0", "id": f"latest:{chain_key}", "method": "eth_blockNumber", "params": []},
        describe=f"{chain_key} latest block",
    )
    if not isinstance(response, dict) or response.get("error") or "result" not in response:
        raise RuntimeError(f"could not read latest {chain_key} block")
    return _rpc_hex_int(response["result"], "latest block")


def _eth_call_args(
    chain_key: str,
    address: str,
    signature: str,
    output_types: list[str],
    *,
    input_types: list[str] | None = None,
    args: list[Any] | None = None,
    block: int | str = "latest",
) -> tuple[Any, ...]:
    contract = _normalized_address(address, "call contract")
    selector = Web3.keccak(text=signature).hex()[:8]
    encoded_args = encode(input_types or [], args or []).hex()
    block_tag = hex(block) if isinstance(block, int) else block
    response = rpc_single_request(
        get_endpoints(chain_key),
        {
            "jsonrpc": "2.0",
            "id": f"call:{chain_key}:{contract}:{signature}",
            "method": "eth_call",
            "params": [{"to": contract, "data": "0x" + selector + encoded_args}, block_tag],
        },
        describe=f"{chain_key} {signature}",
    )
    result = response.get("result") if isinstance(response, dict) else None
    if not isinstance(result, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})+", result):
        raise RuntimeError(f"invalid {chain_key} {signature} response")
    try:
        return tuple(decode(output_types, bytes.fromhex(result[2:])))
    except Exception as exc:
        raise RuntimeError(f"could not decode {chain_key} {signature}") from exc


def _eth_call(chain_key: str, address: str, signature: str, output_types: list[str]) -> tuple[Any, ...]:
    return _eth_call_args(chain_key, address, signature, output_types)


def _batch_eth_call_args(
    chain_key: str,
    calls: list[dict[str, Any]],
) -> tuple[dict[str, tuple[Any, ...]], dict[str, str]]:
    """Run bounded read-only contract calls and decode each result independently."""
    payloads = []
    definitions = {}
    for call in calls:
        call_id = str(call.get("id") or "")
        if not call_id or call_id in definitions:
            raise ValueError("batch eth_call IDs must be unique and non-empty")
        address = _normalized_address(call.get("address"), "batch call contract")
        signature = str(call.get("signature") or "")
        selector = Web3.keccak(text=signature).hex()[:8]
        encoded_args = encode(call.get("inputTypes") or [], call.get("args") or []).hex()
        payloads.append(
            {
                "jsonrpc": "2.0",
                "id": call_id,
                "method": "eth_call",
                "params": [{"to": address, "data": "0x" + selector + encoded_args}, "latest"],
            }
        )
        definitions[call_id] = call
    responses, missing = rpc_batch_requests(
        get_endpoints(chain_key),
        payloads,
        timeout=45,
        batch_size=40,
        min_batch_interval_seconds=0.12,
        quiet=True,
        describe=f"{chain_key} liquidity contract calls",
    )
    values = {}
    errors = {str(call_id): "RPC call unavailable" for call_id in missing}
    for call_id, response in responses.items():
        result = response.get("result") if isinstance(response, dict) else None
        if not isinstance(result, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})+", result):
            errors[str(call_id)] = "invalid eth_call response"
            continue
        try:
            values[str(call_id)] = tuple(
                decode(definitions[str(call_id)]["outputTypes"], bytes.fromhex(result[2:]))
            )
        except Exception as exc:
            errors[str(call_id)] = sanitize_error(exc)
    return values, errors


def _eth_code(chain_key: str, address: str) -> str:
    contract = _normalized_address(address, "code address")
    response = rpc_single_request(
        get_endpoints(chain_key),
        {"jsonrpc": "2.0", "id": f"code:{contract}", "method": "eth_getCode", "params": [contract, "latest"]},
        describe=f"{chain_key} code {contract}",
    )
    code = response.get("result") if isinstance(response, dict) else None
    if not isinstance(code, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", code):
        raise RuntimeError(f"invalid {chain_key} eth_getCode response")
    return code.lower()


def _eip7702_delegation_address(code: str) -> str | None:
    """Return the delegate only for an exact EIP-7702 designator."""
    match = re.fullmatch(r"0xef0100([0-9a-f]{40})", str(code or "").lower())
    return "0x" + match.group(1) if match else None


def _safe_singleton_address(chain_key: str, address: str) -> str:
    """Read and validate a proxy's slot-zero singleton address."""
    contract = _normalized_address(address, "Safe proxy address")
    response = rpc_single_request(
        get_endpoints(chain_key),
        {
            "jsonrpc": "2.0",
            "id": f"safe-singleton:{contract}",
            "method": "eth_getStorageAt",
            "params": [contract, "0x0", "latest"],
        },
        timeout=10,
        retries_per_endpoint=2,
        quiet=True,
        describe=f"{chain_key} Safe singleton {contract}",
    )
    slot_zero = response.get("result") if isinstance(response, dict) else None
    if not isinstance(slot_zero, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", slot_zero):
        raise RuntimeError(f"invalid {chain_key} eth_getStorageAt response")
    return "0x" + slot_zero[-40:].lower()


def _routescan_request(
    client: requests.Session,
    url: str,
    *,
    params: dict[str, Any],
    timeout: int,
    max_attempts: int = 5,
) -> requests.Response:
    """Retry only transient Routescan transport failures with a bounded delay."""
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            response = client.get(
                url,
                params=params,
                timeout=timeout,
                headers={"User-Agent": "dolomite-dashboard-liquidity/1.0"},
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            transient = status in ROUTESCAN_TRANSIENT_STATUS_CODES or status is None
            if not transient or attempt + 1 >= max_attempts:
                raise
            retry_after = getattr(getattr(exc, "response", None), "headers", {}).get(
                "Retry-After"
            )
            try:
                delay = float(retry_after) if retry_after is not None else 0.5 * (2**attempt)
            except (TypeError, ValueError):
                delay = 0.5 * (2**attempt)
            time.sleep(max(0.25, min(delay, 8.0)))
    raise RuntimeError(f"Routescan request failed: {sanitize_error(last_error)}")


def _routescan_token_holder_candidates(
    chain_id: int,
    token: str,
    *,
    session: requests.Session | None = None,
) -> set[str]:
    """Discover current holder candidates without trusting indexed balances."""
    client = session or requests.Session()
    contract = _normalized_address(token, "Routescan token holder contract")
    url = f"https://api.routescan.io/v2/network/mainnet/evm/{chain_id}/etherscan/api"
    candidates: set[str] = set()
    for page in range(1, 101):
        response = _routescan_request(
            client,
            url,
            params={
                "module": "token",
                "action": "tokenholderlist",
                "contractaddress": contract,
                "page": page,
                "offset": 1000,
            },
            timeout=45,
        )
        payload = response.json()
        rows = payload.get("result") if isinstance(payload, dict) else None
        no_records = (
            isinstance(payload, dict)
            and payload.get("status") == "0"
            and isinstance(rows, str)
            and "No records" in rows
        )
        if no_records:
            break
        if not isinstance(rows, list):
            raise RuntimeError(
                f"Routescan token holders failed: {sanitize_error(rows or payload)}"
            )
        for row in rows:
            if not isinstance(row, dict):
                raise RuntimeError("Routescan token holder row must be an object")
            address = _normalized_address(
                row.get("TokenHolderAddress"), "Routescan token holder"
            )
            quantity = str(row.get("TokenHolderQuantity") or "").strip()
            if not re.fullmatch(r"[0-9]+", quantity):
                raise RuntimeError("Routescan token holder quantity must be an integer")
            if int(quantity) > 0:
                candidates.add(address)
        if len(rows) < 1000:
            break
    else:
        raise RuntimeError("Routescan token holder pagination exceeded safety limit")
    return candidates


def _routescan_method_callers(
    chain_id: int,
    address: str,
    signature: str,
    from_block: int,
    to_block: int,
    *,
    session: requests.Session | None = None,
) -> set[str]:
    """Discover successful direct callers of one exact contract method."""
    start = _exact_int(from_block, "method caller start block")
    end = _exact_int(to_block, "method caller end block")
    if start < 0 or end < start:
        raise ValueError("method caller block range is invalid")
    client = session or requests.Session()
    contract = _normalized_address(address, "Routescan method contract")
    method_id = "0x" + Web3.keccak(text=signature).hex()[:8]
    url = f"https://api.routescan.io/v2/network/mainnet/evm/{chain_id}/etherscan/api"

    def fetch_range(range_start: int, range_end: int, depth: int = 0) -> list[Any]:
        if depth > 24:
            raise RuntimeError("Routescan method-call subdivision exceeded safety limit")
        response = _routescan_request(
            client,
            url,
            params={
                "module": "account",
                "action": "txlist",
                "address": contract,
                "startblock": range_start,
                "endblock": range_end,
                "page": 1,
                "offset": 10000,
                "sort": "asc",
            },
            timeout=45,
        )
        payload = response.json()
        rows = payload.get("result") if isinstance(payload, dict) else None
        if (
            isinstance(payload, dict)
            and payload.get("status") == "0"
            and isinstance(rows, str)
            and "No transactions" in rows
        ):
            return []
        if not isinstance(rows, list):
            raise RuntimeError(
                f"Routescan method callers failed: {sanitize_error(rows or payload)}"
            )
        if len(rows) < 10000:
            return rows
        if range_start == range_end:
            raise RuntimeError("Routescan method-call block exceeds page ceiling")
        midpoint = (range_start + range_end) // 2
        return fetch_range(range_start, midpoint, depth + 1) + fetch_range(
            midpoint + 1, range_end, depth + 1
        )

    callers: set[str] = set()
    for row in fetch_range(start, end):
        if not isinstance(row, dict):
            raise RuntimeError("Routescan method-call row must be an object")
        if str(row.get("to") or "").lower() != contract:
            continue
        if str(row.get("methodId") or "").lower() != method_id:
            continue
        if str(row.get("isError") or "") != "0":
            continue
        callers.add(_normalized_address(row.get("from"), "Routescan method caller"))
    return callers


def _routescan_token_transfer_counterparties(
    chain_id: int,
    token: str,
    account: str,
    from_block: int,
    to_block: int,
    *,
    session: requests.Session | None = None,
) -> set[str]:
    """Discover bounded early/recent token counterparties for custody resolution."""
    start = _exact_int(from_block, "token transfer start block")
    end = _exact_int(to_block, "token transfer end block")
    if start < 0 or end < start:
        raise ValueError("token transfer block range is invalid")
    client = session or requests.Session()
    contract = _normalized_address(token, "Routescan transfer token")
    target = _normalized_address(account, "Routescan transfer account")
    url = f"https://api.routescan.io/v2/network/mainnet/evm/{chain_id}/etherscan/api"
    counterparties: set[str] = set()
    for sort in ("asc", "desc"):
        response = _routescan_request(
            client,
            url,
            params={
                "module": "account",
                "action": "tokentx",
                "address": target,
                "contractaddress": contract,
                "startblock": start,
                "endblock": end,
                "page": 1,
                "offset": 1000,
                "sort": sort,
            },
            timeout=60,
        )
        payload = response.json()
        rows = payload.get("result") if isinstance(payload, dict) else None
        if (
            isinstance(payload, dict)
            and payload.get("status") == "0"
            and isinstance(rows, str)
            and "No transactions" in rows
        ):
            continue
        if not isinstance(rows, list):
            raise RuntimeError(
                f"Routescan token transfers failed: {sanitize_error(rows or payload)}"
            )
        for row in rows:
            if not isinstance(row, dict):
                raise RuntimeError("Routescan token transfer row must be an object")
            sender = _normalized_address(row.get("from"), "token transfer sender")
            recipient = _normalized_address(row.get("to"), "token transfer recipient")
            if recipient == target and sender != ZERO_ADDRESS:
                counterparties.add(sender)
            if sender == target and recipient != ZERO_ADDRESS:
                counterparties.add(recipient)
    return counterparties


def _read_indexed_holder_balances(
    chain_key: str,
    token: str,
    candidates: set[str],
) -> dict[str, int]:
    """Read exact current balances for an index-discovered candidate set."""
    contract = _normalized_address(token, "holder balance contract")
    holders = sorted(
        {_normalized_address(address, "holder candidate") for address in candidates}
    )
    calls = [
        {
            "id": f"balance:{holder}",
            "address": contract,
            "signature": "balanceOf(address)",
            "outputTypes": ["uint256"],
            "inputTypes": ["address"],
            "args": [holder],
        }
        for holder in holders
    ]
    values, errors = _batch_eth_call_args(chain_key, calls)
    if errors or len(values) != len(calls):
        raise RuntimeError("one or more exact holder balances are unavailable")
    balances: dict[str, int] = {}
    for holder in holders:
        call_id = f"balance:{holder}"
        raw = values.get(call_id)
        if not isinstance(raw, tuple) or len(raw) != 1:
            raise RuntimeError(f"invalid exact holder balance for {holder}")
        amount = _raw_integer(int(raw[0]), "exact holder balance")
        if amount > 0:
            balances[holder] = amount
    return balances


def _reconcile_indexed_holder_balances(
    chain_key: str,
    token: str,
    candidates: set[str],
    expected_total: int,
) -> dict[str, int]:
    """Read exact on-chain balances and require full raw-unit reconciliation."""
    total = _raw_integer(expected_total, "holder balance total")
    balances = _read_indexed_holder_balances(chain_key, token, candidates)
    if sum(balances.values()) != total:
        raise RuntimeError("exact indexed holder balances do not reconcile to total supply")
    return balances


def _standard_staking_custody_state(
    chain_key: str,
    chain_id: int,
    staking_contract: str,
    staking_token: str,
    custody_balance: int,
    from_block: int,
    to_block: int,
) -> dict[str, Any] | None:
    """Resolve a 1:1 staking custodian only when all user claims reconcile on-chain."""
    contract = _normalized_address(staking_contract, "staking custody contract")
    token = _normalized_address(staking_token, "staking custody token")
    expected = _raw_integer(custody_balance, "staking custody balance")
    try:
        total_supply, = _eth_call(chain_key, contract, "totalSupply()", ["uint256"])
    except Exception:
        return None
    if _raw_integer(int(total_supply), "staking custody total supply") != expected:
        return None
    candidates = _routescan_method_callers(
        chain_id,
        contract,
        "stake(uint256)",
        from_block,
        to_block,
    )
    candidates.update(
        _routescan_token_transfer_counterparties(
            chain_id,
            token,
            contract,
            from_block,
            to_block,
        )
    )
    balances = _read_indexed_holder_balances(chain_key, contract, candidates)
    resolved = sum(balances.values())
    if resolved > expected:
        raise RuntimeError("exact staking balances exceed custody balance")
    return {
        "address": contract,
        "stakingToken": token,
        "custodyBalance": expected,
        "stakedBalances": balances,
        "unresolvedBalance": expected - resolved,
        "attributionPath": "kodiak_island_staking",
        "attributionReason": (
            "staking custody and every user balance reconcile exactly on-chain"
        ),
    }


def _infrared_staking_custody_state(
    chain_key: str,
    chain_id: int,
    infrared_vault: str,
    rewards_vault: str,
    staking_token: str,
    custody_balance: int,
    from_block: int,
    to_block: int,
) -> dict[str, Any] | None:
    """Resolve an Infrared vault after proving its one-share bootstrap invariant."""
    vault = _normalized_address(infrared_vault, "Infrared vault")
    parent = _normalized_address(rewards_vault, "Infrared rewards vault")
    token = _normalized_address(staking_token, "Infrared staking token")
    expected = _raw_integer(custody_balance, "Infrared custody balance")
    try:
        onchain_token, = _eth_call(
            chain_key, vault, "stakingToken()", ["address"]
        )
        onchain_parent, = _eth_call(
            chain_key, vault, "rewardsVault()", ["address"]
        )
        infrared, = _eth_call(chain_key, vault, "infrared()", ["address"])
        total_supply, = _eth_call(
            chain_key, vault, "totalSupply()", ["uint256"]
        )
    except Exception:
        return None
    infrared = _normalized_address(infrared, "Infrared coordinator")
    if _normalized_address(onchain_token, "Infrared on-chain staking token") != token:
        return None
    if _normalized_address(onchain_parent, "Infrared on-chain rewards vault") != parent:
        return None
    total = _raw_integer(int(total_supply), "Infrared total supply")
    if total != expected + 1:
        return None
    candidates = _routescan_method_callers(
        chain_id, vault, "stake(uint256)", from_block, to_block
    )
    candidates.update(
        _routescan_token_transfer_counterparties(
            chain_id, token, vault, from_block, to_block
        )
    )
    candidates.add(infrared)
    balances = _read_indexed_holder_balances(chain_key, vault, candidates)
    if sum(balances.values()) != total or balances.get(infrared) != 1:
        raise RuntimeError("Infrared user balances do not reconcile exactly")
    balances.pop(infrared)
    if sum(balances.values()) != expected:
        raise RuntimeError("Infrared backed balances do not match rewards-vault custody")
    return {
        "address": vault,
        "stakingToken": token,
        "custodyBalance": expected,
        "stakedBalances": balances,
        "unresolvedBalance": 0,
        "attributionPath": "kodiak_island_infrared",
        "attributionReason": (
            "Infrared rewards-vault custody and every user balance reconcile "
            "exactly on-chain"
        ),
    }


def _flatten_nested_staking_state(
    parent_state: dict[str, Any], child_state: dict[str, Any]
) -> dict[str, Any]:
    """Replace one proven contract claim with its exact nested user balances."""
    parent = copy.deepcopy(parent_state)
    child = copy.deepcopy(child_state)
    child_address = _normalized_address(child.get("address"), "nested custody")
    parent_token = _normalized_address(parent.get("stakingToken"), "parent staking token")
    child_token = _normalized_address(child.get("stakingToken"), "nested staking token")
    if parent_token != child_token:
        raise ValueError("nested staking token mismatch")
    parent_balances = {
        _normalized_address(address, "parent staker"): _raw_integer(
            amount, "parent staker balance"
        )
        for address, amount in (parent.get("stakedBalances") or {}).items()
    }
    child_balance = parent_balances.get(child_address)
    child_custody = _raw_integer(child.get("custodyBalance"), "nested custody balance")
    child_residual = _raw_integer(
        child.get("unresolvedBalance", 0), "nested unresolved balance"
    )
    child_balances = {
        _normalized_address(address, "nested staker"): _raw_integer(
            amount, "nested staker balance"
        )
        for address, amount in (child.get("stakedBalances") or {}).items()
    }
    if child_balance != child_custody:
        raise ValueError("nested custody does not match its parent claim")
    if child_residual != 0 or sum(child_balances.values()) != child_custody:
        raise ValueError("nested custody must be fully resolved before flattening")
    del parent_balances[child_address]
    for address, amount in child_balances.items():
        parent_balances[address] = parent_balances.get(address, 0) + amount
    parent["stakedBalances"] = dict(sorted(parent_balances.items()))
    parent["attributionReason"] = (
        str(parent.get("attributionReason") or "staking custody reconciles")
        + "; nested Infrared vault user balances reconcile exactly on-chain"
    )
    return parent


def _routescan_logs(
    chain_id: int,
    address: str,
    topic0: str,
    from_block: int,
    to_block: int,
    *,
    session: requests.Session | None = None,
    discovery_only: bool = False,
    indexed_topics: dict[int, str] | None = None,
) -> list[dict[str, Any]]:
    """Read canonical on-chain logs from Routescan's Etherscan-compatible API."""
    client = session or requests.Session()
    url = f"https://api.routescan.io/v2/network/mainnet/evm/{chain_id}/etherscan/api"
    normalized_address = _normalized_address(address, "Routescan log address")
    filters = {}
    previous_topic = 0
    for topic_index, topic_value in sorted((indexed_topics or {}).items()):
        if isinstance(topic_index, bool) or topic_index not in {1, 2, 3}:
            raise ValueError("Routescan indexed topic must be 1, 2, or 3")
        normalized_topic = str(topic_value or "").strip().lower()
        if not POOL_ID_RE.fullmatch(normalized_topic):
            raise ValueError(f"Routescan topic{topic_index} must be bytes32")
        filters[f"topic{topic_index}"] = normalized_topic
        filters[f"topic{previous_topic}_{topic_index}_opr"] = "and"
        previous_topic = topic_index

    def needs_receipt_recovery(raw: Any) -> bool:
        if not isinstance(raw, dict):
            return True
        raw_data = raw.get("data")
        data_complete = isinstance(raw_data, str) and bool(
            re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", raw_data)
        )
        try:
            _rpc_hex_int(raw.get("logIndex"), "Routescan log index")
        except ValueError:
            return True
        return not data_complete

    def recover_incomplete_log(
        raw: dict[str, Any], receipt: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            raise ValueError("incomplete Routescan log must be an object")
        chain_key = {1: "ethereum", 80094: "berachain"}.get(chain_id)
        if chain_key is None:
            raise ValueError(f"unsupported Routescan chain {chain_id}")
        tx_hash = str(raw.get("transactionHash") or "").lower()
        if receipt is None:
            response = rpc_single_request(
                get_endpoints(chain_key),
                {
                    "jsonrpc": "2.0",
                    "id": f"receipt:{tx_hash}",
                    "method": "eth_getTransactionReceipt",
                    "params": [tx_hash],
                },
                describe=f"{chain_key} incomplete Routescan log receipt",
            )
            receipt = response.get("result") if isinstance(response, dict) else None
        receipt_logs = receipt.get("logs") if isinstance(receipt, dict) else None
        if not isinstance(receipt_logs, list):
            raise RuntimeError("could not recover incomplete Routescan log receipt")
        if not receipt_logs and _rpc_hex_int(receipt.get("status"), "receipt status") == 0:
            # Routescan occasionally indexes a phantom event row for a reverted
            # transaction. The canonical receipt proves that no log was emitted.
            return None
        try:
            expected_index = _rpc_hex_int(raw.get("logIndex"), "Routescan log index")
        except ValueError:
            expected_topics = [str(topic or "").lower() for topic in raw.get("topics", [])]
            expected_data = str(raw.get("data") or "").lower()
            matches = [
                row for row in receipt_logs
                if str(row.get("address") or "").lower() == normalized_address
                and [str(topic or "").lower() for topic in row.get("topics", [])]
                == expected_topics
                and str(row.get("data") or "").lower() == expected_data
            ]
        else:
            matches = [
                row for row in receipt_logs
                if _rpc_hex_int(row.get("logIndex"), "receipt log index") == expected_index
                and str(row.get("address") or "").lower() == normalized_address
            ]
        if not matches:
            # A canonical receipt without the indexed address/logIndex proves
            # the Routescan row is non-canonical and must be excluded.
            return None
        if len(matches) != 1:
            raise RuntimeError("could not identify one exact incomplete Routescan log")
        return {**matches[0], "timeStamp": raw.get("timeStamp")}

    def fetch_range(start: int, end: int, depth: int = 0) -> list[dict[str, Any]]:
        if depth > 24:
            raise RuntimeError("Routescan block-range subdivision exceeded the safety limit")
        local = []
        if start < end:
            ceiling_response = _routescan_request(
                client,
                url,
                params={
                    "module": "logs",
                    "action": "getLogs",
                    "fromBlock": start,
                    "toBlock": end,
                    "address": normalized_address,
                    "topic0": topic0,
                    "page": 10,
                    "offset": 1000,
                    **filters,
                },
                timeout=45,
            )
            ceiling_payload = ceiling_response.json()
            ceiling_rows = (
                ceiling_payload.get("result")
                if isinstance(ceiling_payload, dict)
                else None
            )
            no_ceiling_records = (
                isinstance(ceiling_payload, dict)
                and ceiling_payload.get("status") == "0"
                and isinstance(ceiling_rows, str)
                and "No records" in ceiling_rows
            )
            if not no_ceiling_records and not isinstance(ceiling_rows, list):
                raise RuntimeError(
                    f"Routescan logs failed: {sanitize_error(ceiling_rows or ceiling_payload)}"
                )
            if isinstance(ceiling_rows, list) and len(ceiling_rows) >= 1000:
                midpoint = (start + end) // 2
                return fetch_range(start, midpoint, depth + 1) + fetch_range(
                    midpoint + 1, end, depth + 1
                )
        for page in range(1, 11):
            response = _routescan_request(
                client,
                url,
                params={
                    "module": "logs",
                    "action": "getLogs",
                    "fromBlock": start,
                    "toBlock": end,
                    "address": normalized_address,
                    "topic0": topic0,
                    "page": page,
                    "offset": 1000,
                    **filters,
                },
                timeout=45,
            )
            payload = response.json()
            rows = payload.get("result") if isinstance(payload, dict) else None
            if payload.get("status") == "0" and (
                rows is None or isinstance(rows, str)
            ):
                if page == 1 and rows and "No records" not in rows:
                    raise RuntimeError(f"Routescan logs failed: {sanitize_error(rows)}")
                return local
            if not isinstance(rows, list):
                raise RuntimeError(f"Routescan logs failed: {sanitize_error(rows or payload)}")
            incomplete_rows = [raw for raw in rows if needs_receipt_recovery(raw)]
            receipt_cache = {}
            tx_hashes = sorted(
                {
                    str(raw.get("transactionHash") or "").lower()
                    for raw in incomplete_rows
                    if isinstance(raw, dict)
                    and TX_HASH_RE.fullmatch(
                        str(raw.get("transactionHash") or "").lower()
                    )
                }
            )
            if tx_hashes:
                chain_key = {1: "ethereum", 80094: "berachain"}.get(chain_id)
                if chain_key is None:
                    raise ValueError(f"unsupported Routescan chain {chain_id}")
                receipt_payloads = [
                    {
                        "jsonrpc": "2.0",
                        "id": f"receipt:{tx_hash}",
                        "method": "eth_getTransactionReceipt",
                        "params": [tx_hash],
                    }
                    for tx_hash in tx_hashes
                ]
                responses, _missing = rpc_batch_requests(
                    get_endpoints(chain_key),
                    receipt_payloads,
                    timeout=30,
                    batch_size=50,
                    describe=f"{chain_key} incomplete Routescan log receipts",
                )
                for tx_hash in tx_hashes:
                    response = responses.get(f"receipt:{tx_hash}")
                    receipt = response.get("result") if isinstance(response, dict) else None
                    if not (
                        isinstance(receipt, dict)
                        and isinstance(receipt.get("logs"), list)
                    ):
                        raise RuntimeError(
                            f"canonical RPC receipt unavailable for {tx_hash}"
                        )
                    receipt_cache[tx_hash] = receipt
            for row_index, raw in enumerate(rows):
                if needs_receipt_recovery(raw):
                    tx_hash = str(raw.get("transactionHash") or "").lower()
                    raw = recover_incomplete_log(raw, receipt_cache.get(tx_hash))
                    if raw is None:
                        continue
                if discovery_only and not (
                    isinstance(raw.get("logIndex"), str)
                    and str(raw.get("logIndex")).lower().startswith("0x")
                    and len(str(raw.get("logIndex"))) > 2
                ):
                    raw = {**raw, "logIndex": hex(row_index)}
                transaction_index = raw.get("transactionIndex")
                if not (
                    isinstance(transaction_index, str)
                    and transaction_index.lower().startswith("0x")
                    and len(transaction_index) > 2
                ):
                    # Routescan occasionally omits this redundant field. logIndex is
                    # canonical and globally ordered within the block, so it is an
                    # exact ordering surrogate rather than inferred event data.
                    raw = {**raw, "transactionIndex": raw.get("logIndex")}
                normalized = normalize_rpc_log(raw)
                timestamp = _rpc_hex_int(raw.get("timeStamp"), "Routescan timestamp")
                if timestamp <= 0:
                    raise ValueError("Routescan timestamp must be positive")
                normalized["timestamp"] = timestamp
                local.append(normalized)
            if len(rows) < 1000:
                return local
        if start >= end:
            raise RuntimeError("Routescan returned more than 10,000 logs in one block")
        midpoint = (start + end) // 2
        return fetch_range(start, midpoint, depth + 1) + fetch_range(midpoint + 1, end, depth + 1)

    collected = fetch_range(from_block, to_block)
    return collected if discovery_only else dedupe_logs(str(chain_id), collected)


def _dexscreener_pair(pool: dict[str, Any], *, session: requests.Session | None = None) -> dict[str, Any]:
    client = session or requests.Session()
    chain_key = pool["chainKey"]
    identifier = pool["identifier"]
    response = client.get(
        f"https://api.dexscreener.com/latest/dex/pairs/{chain_key}/{identifier}",
        timeout=30,
        headers={"User-Agent": "dolomite-dashboard-liquidity/1.0"},
    )
    response.raise_for_status()
    rows = response.json().get("pairs")
    exact = [
        row for row in (rows or [])
        if str(row.get("pairAddress") or "").lower() == identifier
    ]
    if len(exact) != 1:
        raise RuntimeError(f"Dexscreener returned {len(exact)} exact rows for {identifier}")
    return exact[0]


def _load_dolo_price(path: str | Path) -> Decimal:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid DOLO price file {path}: {exc}") from exc
    return _finite_decimal(payload.get("price"), "DOLO price", allow_zero=False)


def _build_v2_live_source(
    registry: dict[str, Any], pool: dict[str, Any], latest_block: int
) -> dict[str, Any]:
    chain_key = pool["chainKey"]
    chain = registry["chains"][chain_key]
    address = pool["identifier"]
    logs = []
    mint_signature = (
        BROWNFI_MINT_SIGNATURE
        if pool.get("adapter") == "brownfi-v3"
        else V2_EVENT_SIGNATURES["mint"]
    )
    for signature in (
        V2_EVENT_SIGNATURES["transfer"],
        mint_signature,
        V2_EVENT_SIGNATURES["burn"],
    ):
        logs.extend(
            _routescan_logs(
                chain["chainId"],
                address,
                event_topic(signature),
                chain["discoveryStartBlock"],
                latest_block,
            )
        )
    logs = dedupe_logs(chain_key, logs)
    token0, = _eth_call(chain_key, address, "token0()", ["address"])
    token1, = _eth_call(chain_key, address, "token1()", ["address"])
    total_supply, = _eth_call(chain_key, address, "totalSupply()", ["uint256"])
    reserve0, reserve1, _ = _eth_call(chain_key, address, "getReserves()", ["uint112", "uint112", "uint32"])
    result = replay_v2_pool(
        pool,
        logs,
        {
            "token0": token0,
            "token1": token1,
            "totalSupply": int(total_supply),
            "reserve0": int(reserve0),
            "reserve1": int(reserve1),
            "balances": {},
        },
    )
    result["token0"] = str(token0).lower()
    result["token1"] = str(token1).lower()
    return result


def decode_bulla_npm_log(log: dict[str, Any]) -> dict[str, Any] | None:
    topics = log.get("topics") if isinstance(log, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("Bulla NPM log topics are required")
    if str(topics[0]).lower() != event_topic(BULLA_INCREASE_SIGNATURE):
        return decode_v3_npm_log(log)
    if len(topics) != 2:
        raise ValueError("Bulla IncreaseLiquidity must have one indexed tokenId")
    data_hex = str(log.get("data") or "").lower()
    if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", data_hex):
        raise ValueError("Bulla IncreaseLiquidity data must be even-length hex")
    desired, actual, amount0, amount1, pool = decode(
        ["uint128", "uint128", "uint256", "uint256", "address"],
        bytes.fromhex(data_hex[2:]),
    )
    return {
        **_decoded_log_base(log),
        "kind": "increase",
        "tokenId": int(str(topics[1]), 16),
        "liquidityDesiredRaw": int(desired),
        "liquidityRaw": int(actual),
        "amount0Raw": int(amount0),
        "amount1Raw": int(amount1),
        "pool": _normalized_address(pool, "Bulla event pool"),
    }


def _bulla_position(
    chain_key: str, position_manager: str, token_id: int, *, block: int | str = "latest"
) -> dict[str, Any]:
    values = _eth_call_args(
        chain_key,
        position_manager,
        "positions(uint256)",
        ["uint88", "address", "address", "address", "int24", "int24", "uint128", "uint256", "uint256", "uint128", "uint128"],
        input_types=["uint256"],
        args=[token_id],
        block=block,
    )
    return {
        "token0": str(values[2]).lower(),
        "token1": str(values[3]).lower(),
        "tickLower": int(values[4]),
        "tickUpper": int(values[5]),
        "liquidity": int(values[6]),
    }


def _bulla_owner(
    chain_key: str, position_manager: str, token_id: int
) -> str:
    owner, = _eth_call_args(
        chain_key,
        position_manager,
        "ownerOf(uint256)",
        ["address"],
        input_types=["uint256"],
        args=[token_id],
    )
    return _normalized_address(owner, "Bulla position owner")


def select_bulla_pool_token_ids(
    position_manager_logs: list[dict[str, Any]], pool_address: str
) -> set[int]:
    """Return token IDs proven by Bulla's pool-bearing NPM increase event."""
    target = _normalized_address(pool_address, "Bulla pool")
    token_ids = set()
    for log in position_manager_logs:
        event = decode_bulla_npm_log(log)
        if event is not None and event["kind"] == "increase" and event.get("pool") == target:
            token_ids.add(_exact_int(event["tokenId"], "Bulla tokenId"))
    return token_ids


def _topic_for_address(address: str) -> str:
    return "0x" + _normalized_address(address, "indexed address")[2:].rjust(64, "0")


def _topic_for_uint(value: int) -> str:
    integer = _exact_int(value, "indexed uint")
    if integer < 0:
        raise ValueError("indexed uint must be nonnegative")
    return "0x" + f"{integer:064x}"


def _v3_position(
    chain_key: str, position_manager: str, token_id: int, *, block: int | str = "latest"
) -> dict[str, Any]:
    values = _eth_call_args(
        chain_key,
        position_manager,
        "positions(uint256)",
        [
            "uint96", "address", "address", "address", "uint24", "int24", "int24",
            "uint128", "uint256", "uint256", "uint128", "uint128",
        ],
        input_types=["uint256"],
        args=[token_id],
        block=block,
    )
    return {
        "token0": str(values[2]).lower(),
        "token1": str(values[3]).lower(),
        "fee": int(values[4]),
        "tickLower": int(values[5]),
        "tickUpper": int(values[6]),
        "liquidity": int(values[7]),
    }


def _v3_owner(chain_key: str, position_manager: str, token_id: int) -> str:
    owner, = _eth_call_args(
        chain_key,
        position_manager,
        "ownerOf(uint256)",
        ["address"],
        input_types=["uint256"],
        args=[token_id],
    )
    return _normalized_address(owner, "V3 position owner")


def _v3_pool_state(chain_key: str, pool_address: str, adapter: str) -> dict[str, Any]:
    token0, = _eth_call(chain_key, pool_address, "token0()", ["address"])
    token1, = _eth_call(chain_key, pool_address, "token1()", ["address"])
    if adapter == "kodiak-v3":
        slot0 = _eth_call(
            chain_key,
            pool_address,
            "slot0()",
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint32", "bool"],
        )
    else:
        slot0 = _eth_call(
            chain_key,
            pool_address,
            "slot0()",
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
        )
    decimals0, = _eth_call(chain_key, str(token0), "decimals()", ["uint8"])
    decimals1, = _eth_call(chain_key, str(token1), "decimals()", ["uint8"])
    return {
        "token0": str(token0).lower(),
        "token1": str(token1).lower(),
        "sqrtPriceX96": int(slot0[0]),
        "currentTick": int(slot0[1]),
        "decimals0": int(decimals0),
        "decimals1": int(decimals1),
    }


def _contract_owners(chain_key: str, owners: set[str]) -> set[str]:
    contracts = set()
    for owner in sorted(owners):
        try:
            code = _eth_code(chain_key, owner)
        except Exception:
            # Classification fails closed: an owner with unavailable bytecode is
            # treated as a contract so it cannot be mislabeled as a personal LP.
            contracts.add(owner)
            continue
        if code in {"0x", "0x0", "0x00"} or _eip7702_delegation_address(code):
            continue
        try:
            if _safe_singleton_address(chain_key, owner) in SAFE_SINGLETON_ADDRS:
                continue
        except Exception:
            # Unknown bytecode or an unavailable singleton read is not enough to
            # prove wallet ownership, so custody remains unresolved.
            pass
        contracts.add(owner)
    return contracts


def _kodiak_position_index(pool_address: str) -> dict[int, dict[str, Any]]:
    target = _normalized_address(pool_address, "Kodiak pool")
    query = """
      query Positions($pool: String!, $after: ID!) {
        positions(first: 1000, orderBy: id, orderDirection: asc,
          where: {pool: $pool, id_gt: $after}) {
          id owner liquidity tickLower { tickIdx } tickUpper { tickIdx }
          pool { id feeTier tick sqrtPrice token0 { id decimals } token1 { id decimals } }
        }
      }
    """
    rows = []
    after = ""
    session = requests.Session()
    while True:
        response = session.post(
            KODIAK_V3_SUBGRAPH,
            json={"query": query, "variables": {"pool": target, "after": after}},
            timeout=45,
            headers={"User-Agent": "dolomite-dashboard-liquidity/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        page = payload.get("data", {}).get("positions") if isinstance(payload, dict) else None
        if not isinstance(page, list) or payload.get("errors"):
            raise RuntimeError(f"Kodiak position index failed: {sanitize_error(payload)}")
        rows.extend(page)
        if len(page) < 1000:
            break
        after = str(page[-1].get("id") or "")
        if not after:
            raise RuntimeError("Kodiak position index returned an invalid cursor")
    indexed = {}
    for row in rows:
        token_id = int(str(row.get("id") or ""))
        pool = row.get("pool")
        if token_id < 0 or not isinstance(pool, dict) or str(pool.get("id") or "").lower() != target:
            raise RuntimeError("Kodiak position index returned an invalid position")
        token0 = _normalized_address(pool.get("token0", {}).get("id"), "Kodiak token0")
        token1 = _normalized_address(pool.get("token1", {}).get("id"), "Kodiak token1")
        if DOLO_ADDRESS not in {token0, token1}:
            raise RuntimeError("Kodiak index returned a non-DOLO position")
        indexed[token_id] = {
            "pool": target,
            "token0": token0,
            "token1": token1,
            "fee": int(str(pool.get("feeTier"))),
            "tickLower": int(str(row.get("tickLower", {}).get("tickIdx"))),
            "tickUpper": int(str(row.get("tickUpper", {}).get("tickIdx"))),
            "indexedLiquidity": int(str(row.get("liquidity"))),
        }
    if not indexed:
        raise RuntimeError("Kodiak position index returned no positions")
    return indexed


def _kodiak_farm_index(
    registry: dict[str, Any], chain_key: str, latest_block: int
) -> dict[str, list[dict[str, Any]]]:
    """Index official Kodiak farms by their exact staking-token contract."""
    chain = registry["chains"][chain_key]
    factory = chain["custody"]["kodiakFarmFactory"]
    cache_key = (chain_key, factory, latest_block)
    cached = _KODIAK_FARM_INDEX_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)
    logs = _routescan_logs(
        chain["chainId"],
        factory,
        event_topic(KODIAK_FARM_DEPLOYED_SIGNATURE),
        chain["discoveryStartBlock"],
        latest_block,
    )
    farms = []
    seen = set()
    for log in logs:
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 3:
            raise ValueError("FarmDeployed must index farm and implementation")
        farm = _address_from_topic(topics[1], "Kodiak farm")
        if farm in seen:
            continue
        seen.add(farm)
        farms.append(
            {
                "address": farm,
                "implementation": _address_from_topic(
                    topics[2], "Kodiak farm implementation"
                ),
                "deploymentBlock": _exact_int(
                    log.get("blockNumber"), "Kodiak farm deployment block"
                ),
            }
        )
    values, errors = _batch_eth_call_args(
        chain_key,
        [
            {
                "id": f"staking:{row['address']}",
                "address": row["address"],
                "signature": "stakingToken()",
                "outputTypes": ["address"],
            }
            for row in farms
        ],
    )
    by_token: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in farms:
        call_id = f"staking:{row['address']}"
        if call_id not in values:
            raise RuntimeError(
                f"Kodiak farm staking token unavailable for {row['address']}: "
                f"{errors.get(call_id, 'unknown RPC error')}"
            )
        staking_token = _normalized_address(
            values[call_id][0], "Kodiak farm staking token"
        )
        by_token[staking_token].append({**row, "stakingToken": staking_token})
    result = {
        token: sorted(rows, key=lambda row: (row["deploymentBlock"], row["address"]))
        for token, rows in sorted(by_token.items())
    }
    _KODIAK_FARM_INDEX_CACHE[cache_key] = copy.deepcopy(result)
    return result


def _kodiak_farms_for_island(
    registry: dict[str, Any],
    chain_key: str,
    island: str,
    island_balances: dict[str, int],
    latest_block: int,
) -> list[dict[str, Any]]:
    """Reconcile official farm custody for one Kodiak Island share token."""
    chain = registry["chains"][chain_key]
    factory = chain["custody"]["kodiakFarmFactory"]
    farms = _kodiak_farm_index(registry, chain_key, latest_block).get(island, [])
    states = []
    for farm in farms:
        legitimate, = _eth_call_args(
            chain_key,
            factory,
            "isLegitFarm(address)",
            ["bool"],
            input_types=["address"],
            args=[farm["address"]],
        )
        if legitimate is not True:
            raise RuntimeError(f"Kodiak farm is not factory-approved: {farm['address']}")
        total_locked, = _eth_call(
            chain_key, farm["address"], "totalLiquidityLocked()", ["uint256"]
        )
        event_logs = []
        for signature in (
            KODIAK_STAKE_LOCKED_SIGNATURE,
            KODIAK_WITHDRAW_LOCKED_SIGNATURE,
        ):
            event_logs.extend(
                _routescan_logs(
                    chain["chainId"],
                    farm["address"],
                    event_topic(signature),
                    farm["deploymentBlock"],
                    latest_block,
                )
            )
        staked_balances = replay_kodiak_farm_balances(
            dedupe_logs(chain_key, event_logs), int(total_locked)
        )
        states.append(
            {
                "address": farm["address"],
                "stakingToken": island,
                "custodyBalance": island_balances.get(farm["address"], 0),
                "stakedBalances": staked_balances,
            }
        )
    return states


def _build_kodiak_island_rows(
    registry: dict[str, Any],
    pool: dict[str, Any],
    pool_state: dict[str, Any],
    latest_block: int,
) -> dict[str, Any]:
    """Build exact Island share rows for one registered Kodiak V3 pool."""
    chain_key = pool["chainKey"]
    chain = registry["chains"][chain_key]
    factory = chain["custody"]["kodiakIslandFactory"]
    creation_logs = _routescan_logs(
        chain["chainId"],
        factory,
        event_topic(KODIAK_ISLAND_CREATED_SIGNATURE),
        chain["discoveryStartBlock"],
        latest_block,
        indexed_topics={1: _topic_for_address(pool["identifier"])},
    )
    islands = discover_kodiak_islands(
        creation_logs,
        {pool["identifier"]: (pool_state["token0"], pool_state["token1"])},
        DOLO_ADDRESS,
    )
    active = []
    unresolved = []
    island_addresses = set()
    for discovered in islands:
        island = discovered["island"]
        island_addresses.add(island)
        onchain_pool, = _eth_call(chain_key, island, "pool()", ["address"])
        token0, = _eth_call(chain_key, island, "token0()", ["address"])
        token1, = _eth_call(chain_key, island, "token1()", ["address"])
        island_factory, = _eth_call(chain_key, island, "islandFactory()", ["address"])
        normalized_pool = _normalized_address(onchain_pool, "Kodiak Island pool")
        normalized_token0 = _normalized_address(token0, "Kodiak Island token0")
        normalized_token1 = _normalized_address(token1, "Kodiak Island token1")
        if normalized_pool != pool["identifier"]:
            raise RuntimeError(f"Kodiak Island pool mismatch for {island}")
        if (normalized_token0, normalized_token1) != (
            pool_state["token0"],
            pool_state["token1"],
        ):
            raise RuntimeError(f"Kodiak Island token mismatch for {island}")
        if _normalized_address(island_factory, "Kodiak Island factory") != factory:
            raise RuntimeError(f"Kodiak Island factory mismatch for {island}")
        total_supply, = _eth_call(chain_key, island, "totalSupply()", ["uint256"])
        amount0, amount1 = _eth_call(
            chain_key, island, "getUnderlyingBalances()", ["uint256", "uint256"]
        )
        lower_tick, = _eth_call(chain_key, island, "lowerTick()", ["int24"])
        upper_tick, = _eth_call(chain_key, island, "upperTick()", ["int24"])
        total_supply = _raw_integer(int(total_supply), "Island total supply")
        amount0 = _raw_integer(int(amount0), "Island underlying amount0")
        amount1 = _raw_integer(int(amount1), "Island underlying amount1")
        if total_supply == 0:
            if amount0 or amount1:
                raise RuntimeError(f"Kodiak Island {island} has assets without shares")
            continue
        holder_candidates = _routescan_token_holder_candidates(
            chain["chainId"], island
        )
        balances = _reconcile_indexed_holder_balances(
            chain_key, island, holder_candidates, total_supply
        )
        farms = _kodiak_farms_for_island(
            registry, chain_key, island, balances, latest_block
        )
        official_farms = {state["address"] for state in farms}
        initial_contracts = _contract_owners(
            chain_key, set(balances) | {island}
        )
        for custody in sorted(initial_contracts - official_farms - {island}):
            try:
                state = _standard_staking_custody_state(
                    chain_key,
                    chain["chainId"],
                    custody,
                    island,
                    balances.get(custody, 0),
                    chain["discoveryStartBlock"],
                    latest_block,
                )
            except Exception:
                state = None
            if state is not None:
                farms.append(state)
        expanded_farms = []
        for farm in farms:
            expanded = farm
            nested_contracts = _contract_owners(
                chain_key, set(farm.get("stakedBalances", {}))
            )
            for custody in sorted(nested_contracts):
                try:
                    nested = _infrared_staking_custody_state(
                        chain_key,
                        chain["chainId"],
                        custody,
                        farm["address"],
                        island,
                        int(farm["stakedBalances"].get(custody, 0)),
                        chain["discoveryStartBlock"],
                        latest_block,
                    )
                    if nested is not None:
                        expanded = _flatten_nested_staking_state(expanded, nested)
                except Exception:
                    continue
            expanded_farms.append(expanded)
        farms = expanded_farms
        stakers = {
            address
            for farm in farms
            for address in farm.get("stakedBalances", {})
        }
        contracts = _contract_owners(
            chain_key, set(balances) | stakers | {island}
        )
        lower_tick = _exact_int(int(lower_tick), "Kodiak Island lower tick")
        upper_tick = _exact_int(int(upper_tick), "Kodiak Island upper tick")
        if lower_tick >= upper_tick:
            raise RuntimeError(f"Kodiak Island {island} has an invalid tick range")
        bound_a = tick_to_paired_per_dolo(
            lower_tick,
            normalized_token0,
            normalized_token1,
            pool_state["decimals0"],
            pool_state["decimals1"],
            DOLO_ADDRESS,
        )
        bound_b = tick_to_paired_per_dolo(
            upper_tick,
            normalized_token0,
            normalized_token1,
            pool_state["decimals0"],
            pool_state["decimals1"],
            DOLO_ADDRESS,
        )
        range_lower, range_upper = sorted((bound_a, bound_b))
        underlying = {
            "id": f"{chain_key}:kodiak-v3:{pool['identifier']}:island:{island}",
            "sourceKey": f"{chain_key}:kodiak-v3",
            "poolId": pool["identifier"],
            "poolIdentifierType": "contract",
            "poolExplorerUrl": pool.get("explorerUrl"),
            "dexscreenerUrl": pool.get("sourceUrl"),
            "chainKey": chain_key,
            "adapter": "kodiak-v3",
            "pair": pool.get("pair"),
            "positionType": "kodiak_island_aggregate",
            "positionId": island,
            "tickLower": lower_tick,
            "tickUpper": upper_tick,
            "rangeLower": str(range_lower),
            "rangeUpper": str(range_upper),
            "rangeStatus": classify_range(
                pool_state["currentTick"], lower_tick, upper_tick
            ),
            "amount0Raw": str(amount0),
            "amount1Raw": str(amount1),
            "doloRaw": str(amount0 if normalized_token0 == DOLO_ADDRESS else amount1),
            "pairedRaw": str(amount1 if normalized_token0 == DOLO_ADDRESS else amount0),
            "valueUsd": None,
            "custodian": island,
            "beneficialOwner": None,
            "attributionPath": "kodiak_island",
            "attributionReason": "exact Kodiak Island underlying balances",
            "positionStatus": "custodied_unresolved",
            "quality": "verified",
        }
        rows = allocate_kodiak_island_position(
            underlying,
            {"address": island, "totalShares": total_supply, "balances": balances},
            farms,
            contract_addresses=contracts,
        )
        active.extend(rows)
        unresolved.extend(
            {
                "island": island,
                "custodian": row.get("custodian"),
                "reason": row.get("attributionReason"),
            }
            for row in rows
            if not row.get("beneficialOwner")
        )
    return {
        "activePositions": active,
        "islands": island_addresses,
        "unresolved": unresolved,
    }


def _receipt_logs_for_transactions(
    chain_key: str,
    transaction_timestamps: dict[str, int],
) -> list[dict[str, Any]]:
    chain_id = {"ethereum": 1, "berachain": 80094}.get(chain_key)
    if chain_id is None:
        raise ValueError(f"unsupported receipt chain {chain_key}")
    url = f"https://api.routescan.io/v2/network/mainnet/evm/{chain_id}/etherscan/api"

    def fetch_rpc_receipt(tx_hash: str) -> tuple[str, dict[str, Any]]:
        response = rpc_single_request(
            get_endpoints(chain_key),
            {
                "jsonrpc": "2.0",
                "id": tx_hash,
                "method": "eth_getTransactionReceipt",
                "params": [tx_hash],
            },
            timeout=30,
            retries_per_endpoint=2,
            quiet=True,
            describe=f"{chain_key} liquidity receipt {tx_hash}",
        )
        receipt = response.get("result") if isinstance(response, dict) else None
        if not isinstance(receipt, dict) or not isinstance(receipt.get("logs"), list):
            raise RuntimeError(f"canonical RPC receipt unavailable for {tx_hash}")
        return tx_hash, response

    def fetch_receipt(tx_hash: str) -> tuple[str, dict[str, Any]]:
        last_error = None
        for attempt in range(5):
            try:
                response = requests.get(
                    url,
                    params={"module": "proxy", "action": "eth_getTransactionReceipt", "txhash": tx_hash},
                    timeout=30,
                    headers={"User-Agent": "dolomite-dashboard-liquidity/1.0"},
                )
                response.raise_for_status()
                payload = response.json()
                receipt = payload.get("result") if isinstance(payload, dict) else None
                if not isinstance(receipt, dict) or not isinstance(receipt.get("logs"), list):
                    raise RuntimeError(f"Routescan receipt unavailable for {tx_hash}")
                return tx_hash, {"jsonrpc": "2.0", "id": tx_hash, "result": receipt}
            except Exception as exc:
                last_error = exc
                if attempt < 4:
                    time.sleep(0.5 * (2**attempt))
        raise RuntimeError(f"Routescan receipt failed: {sanitize_error(last_error)}")

    payloads = [
        {
            "jsonrpc": "2.0",
            "id": tx_hash,
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
        }
        for tx_hash in sorted(transaction_timestamps)
    ]
    responses, missing = rpc_batch_requests(
        get_endpoints(chain_key),
        payloads,
        timeout=30,
        batch_size=50,
        describe=f"{chain_key} liquidity receipts",
    )
    if missing:
        failures = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fetch_rpc_receipt, tx_hash): tx_hash for tx_hash in missing}
            for future in as_completed(futures):
                try:
                    tx_hash, response = future.result()
                    responses[tx_hash] = response
                except Exception as exc:
                    failures.append((futures[future], sanitize_error(exc)))
        if failures:
            retry_failures = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(fetch_receipt, tx_hash): (tx_hash, rpc_error)
                    for tx_hash, rpc_error in failures
                }
                for future in as_completed(futures):
                    tx_hash, rpc_error = futures[future]
                    try:
                        recovered_hash, response = future.result()
                        responses[recovered_hash] = response
                    except Exception as exc:
                        retry_failures.append(
                            f"{tx_hash}: RPC {rpc_error}; Routescan {sanitize_error(exc)}"
                        )
            failures = retry_failures
        if failures:
            raise RuntimeError(
                f"missing {len(failures)} exact liquidity receipts after Routescan fallback"
            )
    logs = []
    for tx_hash in sorted(transaction_timestamps):
        response = responses.get(tx_hash)
        receipt = response.get("result") if isinstance(response, dict) else None
        if not isinstance(receipt, dict) or not isinstance(receipt.get("logs"), list):
            _, fallback = fetch_receipt(tx_hash)
            receipt = fallback.get("result")
            if not isinstance(receipt, dict) or not isinstance(receipt.get("logs"), list):
                raise RuntimeError(f"invalid liquidity receipt for {tx_hash}")
        for raw in receipt["logs"]:
            normalized = normalize_rpc_log(raw)
            normalized["timestamp"] = transaction_timestamps[tx_hash]
            logs.append(normalized)
    return dedupe_logs(chain_key, logs)


def _build_kodiak_v3_live_source(
    registry: dict[str, Any], pool: dict[str, Any], latest_block: int
) -> dict[str, Any]:
    """Build current Kodiak NFT positions from index discovery + on-chain reconciliation."""
    chain_key = pool["chainKey"]
    position_manager = registry["chains"][chain_key]["adapters"]["kodiak-v3"]["positionManager"]
    indexed = _kodiak_position_index(pool["identifier"])
    latest_positions = {}
    unresolved = []
    owners = set()
    for token_id, index_row in sorted(indexed.items()):
        try:
            current = _v3_position(chain_key, position_manager, token_id)
            for field in ("token0", "token1", "fee", "tickLower", "tickUpper"):
                if current[field] != index_row[field]:
                    raise RuntimeError(f"Kodiak position {token_id} index mismatch in {field}")
            if current["liquidity"] <= 0:
                continue
            owner = _v3_owner(chain_key, position_manager, token_id)
            owners.add(owner)
            latest_positions[token_id] = {
                **current,
                "pool": pool["identifier"],
                "owner": owner,
            }
        except Exception as exc:
            unresolved.append({"tokenId": token_id, "reason": sanitize_error(exc)})
    state = _v3_pool_state(chain_key, pool["identifier"], "kodiak-v3")
    result = build_v3_rows(
        pool,
        [],
        latest_positions,
        state,
        contract_addresses=_contract_owners(chain_key, owners),
    )
    island_result = _build_kodiak_island_rows(
        registry, pool, state, latest_block
    )
    island_addresses = island_result["islands"]
    result["activePositions"] = [
        row
        for row in result["activePositions"]
        if not (
            row.get("positionType") == "concentrated_nft"
            and row.get("custodian") in island_addresses
        )
    ]
    result["activePositions"].extend(island_result["activePositions"])
    # Current position balances and owners are on-chain verified. Kodiak's public
    # index is used only to discover token IDs; historical ownership/action rows
    # are withheld until an archive-complete replay can prove them.
    result["sourceStatus"] = "partial"
    result["unresolved"] = [
        *unresolved,
        *island_result["unresolved"],
        {"reason": "Kodiak history withheld; current NFT positions are on-chain reconciled"},
    ]
    result["token0"] = state["token0"]
    result["token1"] = state["token1"]
    return result


def _build_uniswap_v3_live_source(
    registry: dict[str, Any],
    pool: dict[str, Any],
    latest_block: int,
    *,
    previous_artifact: dict[str, Any] | None = None,
    full_history: bool = False,
) -> dict[str, Any]:
    """Discover target-pool NPM tokens from pool events and reconcile live state."""
    chain_key = pool["chainKey"]
    chain = registry["chains"][chain_key]
    position_manager = chain["adapters"]["uniswap-v3"]["positionManager"]
    context = incremental_pool_context(
        {} if full_history else (previous_artifact or {}),
        f"{chain_key}:uniswap-v3",
        pool["identifier"],
        chain["discoveryStartBlock"],
    )
    pool_logs = []
    for signature in (
        "Mint(address,address,int24,int24,uint128,uint256,uint256)",
        "Burn(address,int24,int24,uint128,uint256,uint256)",
    ):
        pool_logs.extend(
            _routescan_logs(
                chain["chainId"],
                pool["identifier"],
                event_topic(signature),
                context["scanStart"],
                latest_block,
                discovery_only=True,
            )
        )
    transaction_timestamps = {}
    for log in pool_logs:
        tx_hash = log["transactionHash"]
        timestamp = log["timestamp"]
        previous_timestamp = transaction_timestamps.get(tx_hash)
        if previous_timestamp is not None and previous_timestamp != timestamp:
            raise ValueError("one transaction cannot have conflicting timestamps")
        transaction_timestamps[tx_hash] = timestamp
    receipt_logs = _receipt_logs_for_transactions(chain_key, transaction_timestamps)
    npm_logs = []
    decoded_events = []
    for log in receipt_logs:
        if log["address"] != position_manager:
            continue
        event = decode_v3_npm_log(log)
        if event is not None:
            npm_logs.append(log)
            decoded_events.append(event)
    token_ids = {
        *context["tokenIds"],
        *(event["tokenId"] for event in decoded_events if event["kind"] == "increase"),
    }
    if not token_ids:
        raise RuntimeError("Uniswap pool emitted no attributable position events")
    first_blocks = first_v3_increase_blocks(decoded_events)
    snapshots = {}
    latest_positions = {}
    unresolved = []
    owners = set()
    position_values, position_errors = _batch_eth_call_args(
        chain_key,
        [
            {
                "id": f"position:{token_id}",
                "address": position_manager,
                "signature": "positions(uint256)",
                "inputTypes": ["uint256"],
                "args": [token_id],
                "outputTypes": [
                    "uint96", "address", "address", "address", "uint24", "int24", "int24",
                    "uint128", "uint256", "uint256", "uint128", "uint128",
                ],
            }
            for token_id in sorted(token_ids)
        ],
    )
    current_positions = {}
    pool_cache = {}
    for token_id in sorted(token_ids):
        call_id = f"position:{token_id}"
        try:
            if call_id not in position_values:
                raise RuntimeError(position_errors.get(call_id, "position unavailable"))
            values = position_values[call_id]
            current = {
                "token0": str(values[2]).lower(),
                "token1": str(values[3]).lower(),
                "fee": int(values[4]),
                "tickLower": int(values[5]),
                "tickUpper": int(values[6]),
                "liquidity": int(values[7]),
            }
            if current["token0"] == ZERO_ADDRESS or DOLO_ADDRESS not in {current["token0"], current["token1"]}:
                continue
            pool_key = (current["token0"], current["token1"], current["fee"])
            if pool_key not in pool_cache:
                snapshot_pool, = _eth_call_args(
                    chain_key,
                    chain["adapters"]["uniswap-v3"]["factory"],
                    "getPool(address,address,uint24)",
                    ["address"],
                    input_types=["address", "address", "uint24"],
                    args=list(pool_key),
                )
                pool_cache[pool_key] = str(snapshot_pool).lower()
            snapshot_pool = pool_cache[pool_key]
            if str(snapshot_pool).lower() != pool["identifier"]:
                continue
            snapshots[token_id] = {
                **current,
                "pool": pool["identifier"],
                "snapshotBlock": first_blocks.get(token_id, context["scanStart"]),
            }
            if current["liquidity"] > 0:
                current_positions[token_id] = current
        except Exception as exc:
            unresolved.append({"tokenId": token_id, "reason": sanitize_error(exc)})
    owner_values, owner_errors = _batch_eth_call_args(
        chain_key,
        [
            {
                "id": f"owner:{token_id}",
                "address": position_manager,
                "signature": "ownerOf(uint256)",
                "inputTypes": ["uint256"],
                "args": [token_id],
                "outputTypes": ["address"],
            }
            for token_id in sorted(current_positions)
        ],
    )
    for token_id, current in current_positions.items():
        call_id = f"owner:{token_id}"
        if call_id not in owner_values:
            unresolved.append({"tokenId": token_id, "reason": owner_errors.get(call_id, "owner unavailable")})
            continue
        owner = _normalized_address(owner_values[call_id][0], "Uniswap v3 owner")
        owners.add(owner)
        latest_positions[token_id] = {
            **current,
            "pool": pool["identifier"],
            "owner": owner,
        }
    mapped = map_v3_events_to_pools(npm_logs, snapshots, {pool["identifier"]}, DOLO_ADDRESS)
    unresolved.extend(mapped["unresolved"])
    state = _v3_pool_state(chain_key, pool["identifier"], "uniswap-v3")
    contract_addresses = _contract_owners(chain_key, owners)
    replay_events = (
        []
        if context["incremental"]
        else mapped["eventsByPool"].get(pool["identifier"], [])
    )
    try:
        result = build_v3_rows(
            pool,
            replay_events,
            latest_positions,
            state,
            contract_addresses=contract_addresses,
        )
    except ValueError as exc:
        unresolved.append({"reason": f"Uniswap v3 history withheld: {sanitize_error(exc)}"})
        result = build_v3_rows(
            pool,
            [],
            latest_positions,
            state,
            contract_addresses=contract_addresses,
        )
    if context["incremental"]:
        result["history"] = context["history"]
    # Standalone NFT transfers are not part of pool Mint/Burn receipts. Current
    # ownerOf is exact; historical wallet attribution remains explicitly partial.
    for row in result["history"]:
        if row.get("quality") == "verified":
            row["quality"] = "partial"
    result["sourceStatus"] = "partial"
    result["unresolved"] = [
        *unresolved,
        {"reason": "historical standalone NFT transfers are not attributed"},
    ]
    result["token0"] = state["token0"]
    result["token1"] = state["token1"]
    return result


def _build_bulla_v3_live_source(
    registry: dict[str, Any], pool: dict[str, Any], latest_block: int
) -> dict[str, Any]:
    chain_key = pool["chainKey"]
    chain = registry["chains"][chain_key]
    config = chain["adapters"]["bulla-v3"]
    position_manager = config["positionManager"]
    scan_start = max(
        chain["discoveryStartBlock"],
        latest_block - BULLA_BOUNDED_LOOKBACK_BLOCKS,
    )
    increase_logs = _routescan_logs(
        chain["chainId"],
        position_manager,
        event_topic(BULLA_INCREASE_SIGNATURE),
        scan_start,
        latest_block,
    )
    token_ids = select_bulla_pool_token_ids(increase_logs, pool["identifier"])
    if not token_ids:
        raise RuntimeError("Bulla pool emitted no attributable position events")
    decrease_logs = _routescan_logs(
        chain["chainId"],
        position_manager,
        event_topic(V3_EVENT_SIGNATURES["decrease"]),
        scan_start,
        latest_block,
    )
    npm_events = [
        event
        for event in (decode_bulla_npm_log(log) for log in increase_logs + decrease_logs)
        if event is not None
    ]
    related = [
        event for event in npm_events
        if event.get("tokenId") in token_ids
        and event["kind"] in {"increase", "decrease"}
        and (event["kind"] != "increase" or event.get("pool") == pool["identifier"])
    ]
    related.sort(key=lambda row: (row["blockNumber"], row["transactionIndex"], row["logIndex"]))
    first_blocks = {
        token_id: min(event["blockNumber"] for event in related if event["tokenId"] == token_id)
        for token_id in token_ids
    }
    snapshots = {}
    unresolved = []
    for token_id in sorted(token_ids):
        try:
            snapshot = _bulla_position(chain_key, position_manager, token_id)
            if snapshot["token0"] == ZERO_ADDRESS:
                snapshot = _bulla_position(
                    chain_key, position_manager, token_id, block=first_blocks[token_id]
                )
            if DOLO_ADDRESS not in {snapshot["token0"], snapshot["token1"]}:
                raise ValueError("Bulla position does not contain DOLO")
            snapshots[token_id] = snapshot
        except Exception as exc:
            unresolved.append({"tokenId": token_id, "reason": sanitize_error(exc)})
    mapped = []
    for event in related:
        token_id = event["tokenId"]
        snapshot = snapshots.get(token_id)
        if snapshot is None:
            continue
        mapped.append(
            {
                **event,
                "pool": pool["identifier"],
                "token0": snapshot["token0"],
                "token1": snapshot["token1"],
                "fee": 0,
                "tickLower": snapshot["tickLower"],
                "tickUpper": snapshot["tickUpper"],
                "snapshotBlock": first_blocks[token_id],
            }
        )
    latest_positions = {}
    for token_id, snapshot in snapshots.items():
        if snapshot["liquidity"] <= 0:
            continue
        try:
            owner = _bulla_owner(chain_key, position_manager, token_id)
        except Exception as exc:
            unresolved.append({"tokenId": token_id, "reason": sanitize_error(exc)})
            continue
        latest_positions[token_id] = {
            **snapshot,
            "pool": pool["identifier"],
            "owner": owner,
            "fee": 0,
        }
    contract_addresses = set()
    for latest in latest_positions.values():
        owner = latest["owner"]
        try:
            if _eth_code(chain_key, owner) not in {"0x", "0x0", "0x00"}:
                contract_addresses.add(owner)
        except Exception:
            contract_addresses.add(owner)
    token0, = _eth_call(chain_key, pool["identifier"], "token0()", ["address"])
    token1, = _eth_call(chain_key, pool["identifier"], "token1()", ["address"])
    state = _eth_call(
        chain_key, pool["identifier"], "globalState()",
        ["uint160", "int24", "uint16", "uint8", "uint16", "bool"],
    )
    decimals0, = _eth_call(chain_key, str(token0), "decimals()", ["uint8"])
    decimals1, = _eth_call(chain_key, str(token1), "decimals()", ["uint8"])
    result = build_v3_rows(
        pool,
        mapped,
        latest_positions,
        {
            "sqrtPriceX96": int(state[0]),
            "currentTick": int(state[1]),
            "decimals0": int(decimals0),
            "decimals1": int(decimals1),
        },
        contract_addresses=contract_addresses,
    )
    for row in result["history"]:
        if row.get("quality") == "verified":
            row["quality"] = "partial"
    result["sourceStatus"] = "partial"
    result["unresolved"] = [
        *unresolved,
        {
            "reason": (
                "Bulla discovery is bounded to recent blocks and historical standalone "
                "NFT transfers are not attributed"
            )
        },
    ]
    result["token0"] = str(token0).lower()
    result["token1"] = str(token1).lower()
    return result


def decode_v4_position_info(value: int) -> dict[str, int]:
    """Decode Uniswap v4 PositionInfo's subscriber and signed int24 ticks."""
    raw = _exact_int(value, "V4 position info")
    if raw < 0 or raw >= 1 << 256:
        raise ValueError("V4 position info must fit uint256")

    def signed_int24(bits: int) -> int:
        return bits - (1 << 24) if bits & (1 << 23) else bits

    return {
        "subscriber": raw & 0xFF,
        "tickLower": signed_int24((raw >> 8) & 0xFFFFFF),
        "tickUpper": signed_int24((raw >> 32) & 0xFFFFFF),
    }


def _v4_pool_id(pool_key: tuple[Any, ...]) -> str:
    if len(pool_key) != 5:
        raise ValueError("V4 PoolKey must contain five fields")
    currency0 = _normalized_address(pool_key[0], "V4 currency0")
    currency1 = _normalized_address(pool_key[1], "V4 currency1")
    fee = int(pool_key[2])
    tick_spacing = int(pool_key[3])
    hooks = _normalized_address(pool_key[4], "V4 hooks")
    digest = Web3.keccak(
        encode(
            ["address", "address", "uint24", "int24", "address"],
            [currency0, currency1, fee, tick_spacing, hooks],
        )
    ).hex().lower()
    return digest if digest.startswith("0x") else "0x" + digest


def _build_uniswap_v4_live_source(
    registry: dict[str, Any],
    pool: dict[str, Any],
    latest_block: int,
    *,
    previous_artifact: dict[str, Any] | None = None,
    full_history: bool = False,
) -> dict[str, Any]:
    chain_key = pool["chainKey"]
    chain = registry["chains"][chain_key]
    config = chain["adapters"]["uniswap-v4"]
    pool_manager = config["poolManager"]
    position_manager = config["positionManager"]
    context = incremental_pool_context(
        {} if full_history else (previous_artifact or {}),
        f"{chain_key}:uniswap-v4",
        pool["identifier"],
        chain["discoveryStartBlock"],
    )
    pool_logs = _routescan_logs(
        chain["chainId"],
        pool_manager,
        event_topic(V4_EVENT_SIGNATURES["modify_liquidity"]),
        context["scanStart"],
        latest_block,
        indexed_topics={
            1: pool["identifier"],
        },
    )
    decoded_logs = [decode_v4_pool_manager_log(log) for log in pool_logs]
    all_modifications = [
        event
        for event in decoded_logs
        if event is not None and event.get("kind") == "modify_liquidity"
    ]
    partitioned = partition_v4_modifications(all_modifications, position_manager)
    modifications = partitioned["canonical"]
    noncanonical_modifications = partitioned["noncanonical"]
    if context["incremental"]:
        # Canonical NFT positions are reconciled against PositionManager below,
        # but non-canonical manager positions have no enumerable current-state
        # API. Replaying only the overlap can begin with a removal and create a
        # false negative balance, so always rebuild that small custody subset
        # from the configured discovery boundary.
        replay_logs = _routescan_logs(
            chain["chainId"],
            pool_manager,
            event_topic(V4_EVENT_SIGNATURES["modify_liquidity"]),
            chain["discoveryStartBlock"],
            latest_block,
            indexed_topics={
                1: pool["identifier"],
            },
        )
        replay_modifications = [
            event
            for event in (decode_v4_pool_manager_log(log) for log in replay_logs)
            if event is not None and event.get("kind") == "modify_liquidity"
        ]
        noncanonical_modifications = partition_v4_modifications(
            replay_modifications,
            position_manager,
        )["noncanonical"]
    canonical_pool_logs = [
        log
        for log, event in zip(pool_logs, decoded_logs)
        if event is not None and event.get("sender") == position_manager
    ]
    token_ids = {
        *context["tokenIds"],
        *(int(event["salt"], 16) for event in modifications if int(event["salt"], 16) > 0),
    }
    if not token_ids and not noncanonical_modifications:
        raise RuntimeError("Uniswap v4 pool emitted no attributable position events")
    latest_positions = {}
    unresolved = []
    owners = set()
    info_values, info_errors = {}, {}
    if token_ids:
        info_values, info_errors = _batch_eth_call_args(
            chain_key,
            [
                {
                    "id": f"info:{token_id}",
                    "address": position_manager,
                    "signature": "getPoolAndPositionInfo(uint256)",
                    "inputTypes": ["uint256"],
                    "args": [token_id],
                    "outputTypes": ["(address,address,uint24,int24,address)", "uint256"],
                }
                for token_id in sorted(token_ids)
            ],
        )
    candidates = {}
    for token_id in sorted(token_ids):
        call_id = f"info:{token_id}"
        try:
            if call_id not in info_values:
                raise RuntimeError(info_errors.get(call_id, "position info unavailable"))
            pool_key, packed_info = info_values[call_id]
            if _v4_pool_id(tuple(pool_key)) != pool["identifier"]:
                continue
            candidates[token_id] = (tuple(pool_key), int(packed_info))
        except Exception as exc:
            unresolved.append({"tokenId": token_id, "reason": sanitize_error(exc)})
    liquidity_values, liquidity_errors = {}, {}
    if candidates:
        liquidity_values, liquidity_errors = _batch_eth_call_args(
            chain_key,
            [
                {
                    "id": f"liquidity:{token_id}",
                    "address": position_manager,
                    "signature": "getPositionLiquidity(uint256)",
                    "inputTypes": ["uint256"],
                    "args": [token_id],
                    "outputTypes": ["uint128"],
                }
                for token_id in sorted(candidates)
            ],
        )
    active_candidates = {}
    for token_id, definition in candidates.items():
        call_id = f"liquidity:{token_id}"
        if call_id not in liquidity_values:
            unresolved.append({"tokenId": token_id, "reason": liquidity_errors.get(call_id, "liquidity unavailable")})
            continue
        liquidity_raw = int(liquidity_values[call_id][0])
        if liquidity_raw > 0:
            active_candidates[token_id] = (*definition, liquidity_raw)
    owner_values, owner_errors = {}, {}
    if active_candidates:
        owner_values, owner_errors = _batch_eth_call_args(
            chain_key,
            [
                {
                    "id": f"owner:{token_id}",
                    "address": position_manager,
                    "signature": "ownerOf(uint256)",
                    "inputTypes": ["uint256"],
                    "args": [token_id],
                    "outputTypes": ["address"],
                }
                for token_id in sorted(active_candidates)
            ],
        )
    for token_id, (pool_key, packed_info, liquidity_raw) in active_candidates.items():
        call_id = f"owner:{token_id}"
        if call_id not in owner_values:
            unresolved.append({"tokenId": token_id, "reason": owner_errors.get(call_id, "owner unavailable")})
            continue
        owner = _normalized_address(owner_values[call_id][0], "V4 position owner")
        owners.add(owner)
        ticks = decode_v4_position_info(int(packed_info))
        latest_positions[token_id] = {
            "poolId": pool["identifier"],
            "currency0": str(pool_key[0]).lower(),
            "currency1": str(pool_key[1]).lower(),
            "fee": int(pool_key[2]),
            "tickSpacing": int(pool_key[3]),
            "hooks": str(pool_key[4]).lower(),
            "tickLower": ticks["tickLower"],
            "tickUpper": ticks["tickUpper"],
            "liquidity": liquidity_raw,
            "owner": owner,
        }
    if latest_positions:
        sample = next(iter(latest_positions.values()))
        currency0 = sample["currency0"]
        currency1 = sample["currency1"]
    else:
        pair = _dexscreener_pair(pool)
        addresses = {
            str(pair.get("baseToken", {}).get("address") or "").lower(),
            str(pair.get("quoteToken", {}).get("address") or "").lower(),
        }
        if DOLO_ADDRESS not in addresses or len(addresses) != 2:
            raise RuntimeError("could not resolve Uniswap v4 pool currencies")
        currency0, currency1 = sorted(addresses)
    slot0 = _eth_call_args(
        chain_key,
        config["stateView"],
        "getSlot0(bytes32)",
        ["uint160", "int24", "uint24", "uint24"],
        input_types=["bytes32"],
        args=[bytes.fromhex(pool["identifier"][2:])],
    )
    decimals0 = 18 if currency0 == ZERO_ADDRESS else int(
        _eth_call(chain_key, currency0, "decimals()", ["uint8"])[0]
    )
    decimals1 = 18 if currency1 == ZERO_ADDRESS else int(
        _eth_call(chain_key, currency1, "decimals()", ["uint8"])[0]
    )
    pool_state = {
        "currency0": currency0,
        "currency1": currency1,
        "sqrtPriceX96": int(slot0[0]),
        "currentTick": int(slot0[1]),
        "decimals0": decimals0,
        "decimals1": decimals1,
    }
    contract_addresses = _contract_owners(chain_key, owners)
    try:
        result = build_v4_rows(
            pool,
            [] if context["incremental"] else canonical_pool_logs,
            [],
            latest_positions,
            pool_state,
            pool_manager=pool_manager,
            position_manager=position_manager,
            contract_addresses=contract_addresses,
        )
    except ValueError as exc:
        # A history replay gap must never suppress current positions that were
        # independently reconciled against PositionManager + StateView.
        unresolved.append({"reason": f"v4 history withheld: {sanitize_error(exc)}"})
        result = build_v4_rows(
            pool,
            [],
            [],
            latest_positions,
            pool_state,
            pool_manager=pool_manager,
            position_manager=position_manager,
            contract_addresses=contract_addresses,
        )
    noncanonical = _build_v4_noncanonical_rows(
        registry,
        pool,
        noncanonical_modifications,
        pool_state,
        latest_block,
    )
    result["activePositions"].extend(noncanonical["activePositions"])
    if context["incremental"]:
        result["history"] = context["history"]
    result["sourceStatus"] = "partial"
    result["unresolved"] = [
        *result.get("unresolved", []),
        *unresolved,
        *noncanonical["unresolved"],
        {"reason": "v4 historical token amounts and standalone NFT transfers are withheld"},
    ]
    result["token0"] = currency0
    result["token1"] = currency1
    return result


def build_registered_source(
    registry: dict[str, Any],
    source_key: str,
    registered: list[dict[str, Any]],
    latest_block: int,
    *,
    builders: dict[str, Any] | None = None,
    previous_artifact: dict[str, Any] | None = None,
    full_history: bool = False,
) -> dict[str, Any]:
    """Dispatch every configured pool through its exact live adapter."""
    chain_key, adapter = source_key.split(":", 1)
    if any(
        pool.get("chainKey") != chain_key or pool.get("adapter") != adapter
        for pool in registered
    ):
        raise ValueError("registered source contains a mismatched pool")
    available = builders or {
        "uniswap-v3": _build_uniswap_v3_live_source,
        "uniswap-v4": _build_uniswap_v4_live_source,
        "kodiak-v3": _build_kodiak_v3_live_source,
        "bulla-v3": _build_bulla_v3_live_source,
        "kodiak-v2": _build_v2_live_source,
        "bulla-v2": _build_v2_live_source,
        "beraswap-v2": _build_v2_live_source,
        "brownfi-v3": _build_v2_live_source,
    }
    builder = available.get(adapter)
    if builder is None:
        raise RuntimeError(f"no live adapter registered for {adapter}")
    pool_results = {}
    active = []
    history = []
    statuses = []
    unresolved = []
    for pool in registered:
        signature = inspect.signature(builder)
        accepts_incremental = (
            "previous_artifact" in signature.parameters
            and "full_history" in signature.parameters
        ) or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        if accepts_incremental:
            result = builder(
                registry,
                pool,
                latest_block,
                previous_artifact=previous_artifact,
                full_history=full_history,
            )
        else:
            result = builder(registry, pool, latest_block)
        pool_results[pool["identifier"]] = result
        active.extend(result.get("activePositions", []))
        history.extend(result.get("history", []))
        statuses.append(result.get("sourceStatus", "unavailable"))
        unresolved.extend(result.get("unresolved", []))
    status = "complete" if statuses and all(value == "complete" for value in statuses) else "partial"
    return {
        "sourceKey": source_key,
        "sourceStatus": status,
        "poolResults": pool_results,
        "activePositions": active,
        "history": history,
        "unresolved": unresolved,
    }


def generate_artifact(
    registry_path: str | Path,
    output_path: str | Path,
    *,
    price_path: str | Path = "dolo_price.json",
    full_history: bool = False,
) -> dict[str, Any]:
    """Run the bounded production pipeline and preserve uncertainty explicitly."""
    registry = load_registry(registry_path)
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    previous = load_previous_artifact(output_path)
    dolo_price = _load_dolo_price(price_path)
    latest_by_chain = {chain_key: _latest_block(chain_key) for chain_key in registry["chains"]}
    pool_rows = []
    metadata_by_pool = {}
    session = requests.Session()
    for pool in registry["pools"]:
        row = {
            "id": pool["identifier"],
            "sourceKey": f"{pool['chainKey']}:{pool['adapter']}",
            **pool,
            "liquidityUsd": None,
            "liquidityStatus": "unavailable",
            "quality": "unavailable",
        }
        try:
            pair = _dexscreener_pair(pool, session=session)
            metadata = derive_dexscreener_pool_metadata(pool, pair, dolo_price)
            metadata_by_pool[pool["identifier"]] = {"pair": pair, **metadata}
            row.update(metadata)
            row["liquidityStatus"] = "verified"
            base = pair["baseToken"]
            quote = pair["quoteToken"]
            paired = quote if str(base.get("address") or "").lower() == DOLO_ADDRESS else base
            row["pairedToken"] = str(paired.get("address") or "").lower()
            row["pairedSymbol"] = str(paired.get("symbol") or "")
        except Exception as exc:
            row["metadataError"] = sanitize_error(exc)
        pool_rows.append(row)

    sources = []
    active = []
    history = []
    source_pools: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for pool in registry["pools"]:
        source_pools[f"{pool['chainKey']}:{pool['adapter']}"].append(pool)
    for source_key, registered in sorted(source_pools.items()):
        chain_key, adapter = source_key.split(":", 1)
        latest = latest_by_chain[chain_key]
        source = {
            "key": source_key,
            "chainKey": chain_key,
            "adapter": adapter,
            "status": "unavailable",
            "lastScannedBlock": latest,
            "latestChainBlock": latest,
            "errors": [],
        }
        try:
            built = build_registered_source(
                registry,
                source_key,
                registered,
                latest,
                previous_artifact=previous,
                full_history=full_history,
            )
            for pool in registered:
                result = built["poolResults"][pool["identifier"]]
                matching_pool = next(row for row in pool_rows if row["identifier"] == pool["identifier"])
                matching_pool["quality"] = "verified" if result["sourceStatus"] == "complete" else "partial"
                metadata = metadata_by_pool.get(pool["identifier"])
                paired = matching_pool.get("pairedToken")
                paired_decimals = 18
                if paired and paired != ZERO_ADDRESS:
                    paired_decimals, = _eth_call(chain_key, paired, "decimals()", ["uint8"])
                paired_decimals = _exact_int(paired_decimals, "paired token decimals")
                if not 0 <= paired_decimals <= 255:
                    raise ValueError("paired token decimals must be an integer from 0 to 255")
                matching_pool["pairedDecimals"] = paired_decimals
                for row in result["activePositions"]:
                    active.append(
                        value_position_row(
                            row,
                            dolo_decimals=registry["token"]["decimals"],
                            paired_decimals=paired_decimals,
                            dolo_price_usd=dolo_price if metadata else None,
                            paired_price_usd=Decimal(str(metadata["pairedPriceUsd"])) if metadata else None,
                        )
                    )
                history.extend(
                    value_exact_history_rows(
                        result["history"],
                        dolo_decimals=registry["token"]["decimals"],
                        paired_decimals=paired_decimals,
                        dolo_price_usd=dolo_price if metadata else None,
                        paired_price_usd=(
                            Decimal(str(metadata["pairedPriceUsd"])) if metadata else None
                        ),
                    )
                )
            source["status"] = built["sourceStatus"]
            if built["unresolved"]:
                source["unresolvedCount"] = len(built["unresolved"])
            source["lastScannedBlock"] = latest
        except Exception as exc:
            source["errors"] = [sanitize_error(exc)]
            try:
                stale = preserve_stale_adapter(previous, source_key, exc, generated_at)
            except ValueError:
                pass
            else:
                source = stale["source"]
                active.extend(stale["activePositions"])
                history.extend(stale["history"])
                stale_pools = {row["identifier"]: row for row in stale["pools"]}
                pool_rows = [stale_pools.get(row["identifier"], row) for row in pool_rows]
        sources.append(source)

    artifact = assemble_artifact(registry, sources, pool_rows, active, history, generated_at)
    assert_refresh_not_degraded(previous, artifact)
    write_artifact_atomic(output_path, artifact)
    return artifact


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate verified DOLO liquidity providers")
    parser.add_argument("--registry", default="data/dolo-liquidity-pools.json")
    parser.add_argument("--output", default="data/dolo-liquidity.json")
    parser.add_argument("--price", default="dolo_price.json")
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Replay configured historical ranges instead of using saved cursors",
    )
    args = parser.parse_args()
    artifact = generate_artifact(
        args.registry,
        args.output,
        price_path=args.price,
        full_history=args.full_history,
    )
    print(
        "Generated DOLO liquidity: "
        f"{artifact['summary']['lpWallets']} wallets, "
        f"{artifact['summary']['activePositions']} positions, "
        f"{len(artifact['history'])} history rows"
    )


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
    if adapter not in {"kodiak-v2", "bulla-v2", "beraswap-v2", "brownfi-v3"}:
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


if __name__ == "__main__":
    main()
