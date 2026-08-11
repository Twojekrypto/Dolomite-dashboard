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
