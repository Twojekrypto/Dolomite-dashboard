#!/usr/bin/env python3
"""On-chain DOLO liquidity provider data pipeline.

The first layer in this module deliberately contains only strict registry
parsing and exact AMM arithmetic. Protocol scanners build on these pure
functions so event replay can be tested without network access.
"""

from __future__ import annotations

import copy
import json
import re
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from rpc_client import (
    get_endpoints,
    rpc_batch_requests,
    rpc_single_request,
    sanitize_error,
)


Q96 = 1 << 96
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
