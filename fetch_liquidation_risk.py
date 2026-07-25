#!/usr/bin/env python3
"""
Dolomite Liquidation Risk Dashboard — Data Fetcher
Queries Dolomite subgraphs on all supported chains, computes health factors,
and outputs at-risk positions to liquidation_risk.json.

Usage:
  python3 fetch_liquidation_risk.py                  # Live data (needs API key)
  python3 fetch_liquidation_risk.py --sample         # Generate sample data for UI testing
  python3 fetch_liquidation_risk.py --api-key YOUR_KEY  # Use specific API key
"""

import json
import os
import random
import sys
import time
import urllib.request
import urllib.error
from decimal import Decimal, getcontext
from pathlib import Path

import rpc_usage  # stdlib-only RPC/CU accounting


def sanitize_symbol(value):
    """HTML-escape token symbols/names at the pipeline boundary.

    Subgraph token metadata is deployer-controlled; the frontend interpolates
    these strings into innerHTML, so neutralize markup here once instead of
    auditing every render site (legit symbols are unaffected).
    """
    return (str(value or "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))

getcontext().prec = 50

# ─── Configuration ────────────────────────────────────────────────────────────

# Dolomite's official public subgraph endpoints (no API key required)
# Source: https://github.com/dolomite-exchange/dolomite-subgraph/blob/master/config/subgraph-endpoints.json
DOLOMITE_SUBGRAPH_BASE = "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs"

CHAINS = {
    "berachain": {
        "subgraph_name": "dolomite-berachain-mainnet",
        "label": "Berachain",
        "explorer": "https://berascan.com/address/",
        "rpc": "https://rpc.berachain.com",
    },
    "arbitrum": {
        "subgraph_name": "dolomite-arbitrum",
        "label": "Arbitrum",
        "explorer": "https://arbiscan.io/address/",
        "rpc": "https://arb1.arbitrum.io/rpc",
    },
    "mantle": {
        "subgraph_name": "dolomite-mantle",
        "label": "Mantle",
        "explorer": "https://mantlescan.xyz/address/",
        "rpc": "https://rpc.mantle.xyz",
    },
    "base": {
        "subgraph_name": "dolomite-botanix",
        "label": "Botanix",
        "explorer": "https://explorer.botanixlabs.dev/address/",
        "rpc": "https://rpc.ankr.com/botanix_mainnet",
        "rpc_fallbacks": ["https://rpc.botanixlabs.dev"],
    },
    "ethereum": {
        "subgraph_name": "dolomite-ethereum",
        "label": "Ethereum",
        "explorer": "https://etherscan.io/address/",
        "rpc": "https://rpc.ankr.com/eth",
        "rpc_fallbacks": ["https://eth.drpc.org", "https://eth.llamarpc.com", "https://cloudflare-eth.com"],
    },
    "xlayer": {
        "subgraph_name": "dolomite-x-layer",
        "label": "X Layer",
        "explorer": "https://www.okx.com/web3/explorer/xlayer/address/",
        "rpc": "https://rpc.xlayer.tech",
    },
    "polygon_zkevm": {
        "subgraph_name": "dolomite-polygon-zkevm",
        "label": "Polygon zkEVM",
        "explorer": "https://zkevm.polygonscan.com/address/",
        "rpc": "https://zkevm-rpc.com",
    },
}

# ABI for IAccountRiskOverrideSetter.getAccountRiskOverride
SETTER_ABI = [{
    "inputs": [{"components": [
        {"name": "owner", "type": "address"},
        {"name": "number", "type": "uint256"}
    ], "name": "_account", "type": "tuple"}],
    "name": "getAccountRiskOverride",
    "outputs": [
        {"components": [{"name": "value", "type": "uint256"}], "name": "", "type": "tuple"},
        {"components": [{"name": "value", "type": "uint256"}], "name": "", "type": "tuple"}
    ],
    "stateMutability": "view", "type": "function"
}]

# DolomiteMargin minimal ABI (getters we need)
DOLOMITE_MARGIN_ABI = [
    {
        "inputs": [], "name": "getMarginRatio",
        "outputs": [{"components": [{"name": "value", "type": "uint256"}], "name": "", "type": "tuple"}],
        "stateMutability": "view", "type": "function"
    },
    {
        "inputs": [{"name": "_accountOwner", "type": "address"}],
        "name": "getAccountRiskOverrideSetterByAccountOwner",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view", "type": "function"
    },
]

def get_subgraph_url(chain_config):
    """Build the subgraph URL for a chain."""
    return f"{DOLOMITE_SUBGRAPH_BASE}/{chain_config['subgraph_name']}/latest/gn"

# Health factor thresholds — standard
HF_CRITICAL = 1.05   # 🔴
HF_DANGER = 1.15     # 🟠
HF_WARNING = 1.30    # 🟡
HF_SAFE = 1.50       # 🟢 (anything above warning)

# Health factor thresholds — E-Mode (correlated assets, lower risk)
HF_EMODE_CRITICAL = 1.01   # 🔴
HF_EMODE_DANGER = 1.03     # 🟠
HF_EMODE_WARNING = 1.08    # 🟡

OUTPUT_FILE = "liquidation_risk.json"
# Full liquidation event history lives in a separate lazy-loaded file so that
# consumers that only need positions/stats (portfolio, earn, liq-monitor) do
# not download it. Only liquidation-preview fetches this file on demand.
HISTORY_FILE = "liquidation_history.json"
LIQUIDATION_SHARD_DIR = Path("data/liquidation-risk")
POSITION_COUNT_HISTORY_FILE = LIQUIDATION_SHARD_DIR / "position-count-history.json"
MONITORED_POSITION_MIN_USD = 10
POSITION_COUNT_HISTORY_RETENTION_SECONDS = 72 * 60 * 60
POSITION_COUNT_24H_SECONDS = 24 * 60 * 60
POSITION_COUNT_24H_MAX_SKEW_SECONDS = 6 * 60 * 60
# LiquidatorProxyV6 transfers Dolomite's liquidation fee rake to the registry
# fee agent. This is a protocol address, not a secret. Keep the metric
# conservative: only subgraph-confirmed fee-agent transfers count as protocol
# liquidation fee revenue.
DOLOMITE_PROTOCOL_FEE_AGENT = "0x4d5f0344d245f1d13607e5b61dd317de3b3178b8"


def write_liquidation_risk_shards(payload, output_dir=LIQUIDATION_SHARD_DIR, prefix_length=2):
    """Write deterministic per-chain/address-prefix position shards."""
    output_dir = Path(output_dir)
    metadata = {key: value for key, value in payload.items() if key != "positions"}
    grouped = {}
    for position in payload.get("positions") or []:
        chain = str(position.get("chain") or "").strip().lower()
        address = str(position.get("address") or "").strip().lower()
        if not chain or not address.startswith("0x") or len(address) != 42:
            continue
        prefix = address[2:2 + prefix_length]
        grouped.setdefault(chain, {}).setdefault(prefix, []).append(position)

    chains = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for chain, prefixes in sorted(grouped.items()):
        chain_dir = output_dir / chain
        chain_dir.mkdir(parents=True, exist_ok=True)
        for stale in chain_dir.glob("*.json"):
            stale.unlink()
        shards = {}
        position_count = 0
        for prefix, positions in sorted(prefixes.items()):
            positions.sort(key=lambda row: (str(row.get("address") or ""), str(row.get("accountNumber") or "")))
            relative = f"{chain}/{prefix}.json"
            shard = {
                "version": 1,
                "chain": chain,
                "prefix": prefix,
                "generatedAtISO": payload.get("generatedAtISO"),
                "positions": positions,
            }
            (output_dir / relative).write_text(json.dumps(shard, separators=(",", ":")), encoding="utf-8")
            position_count += len(positions)
            shards[prefix] = {"path": relative, "positionCount": len(positions)}
        chains[chain] = {
            "prefixLength": prefix_length,
            "positionCount": position_count,
            "shardCount": len(shards),
            "shards": shards,
        }

    manifest = {
        "version": 1,
        "prefixLength": prefix_length,
        "generatedAt": payload.get("generatedAt"),
        "generatedAtISO": payload.get("generatedAtISO"),
        "metadata": metadata,
        "chains": chains,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
    return manifest

# ─── GraphQL Queries ──────────────────────────────────────────────────────────

QUERY_DOLOMITE_MARGIN = """
{
  dolomiteMargins(first: 1) {
    id
    liquidationRatio
    liquidationReward
    numberOfMarkets
    defaultAccountRiskOverrideSetter
  }
}
"""

QUERY_MARKET_RISK_INFO = """
{
  marketRiskInfos(first: 100) {
    id
    token {
      id
      symbol
      name
      decimals
      marketId
    }
    marginPremium
    liquidationRewardPremium
    isBorrowingDisabled
  }
}
"""

QUERY_ORACLE_PRICES = """
{
  oraclePrices(first: 100) {
    id
    price
    token {
      id
      symbol
      decimals
      marketId
    }
  }
}
"""

QUERY_INTEREST_INDICES = """
{
  interestIndexes(first: 100) {
    id
    token {
      id
      symbol
      marketId
    }
    borrowIndex
    supplyIndex
  }
}
"""

QUERY_TOTAL_PARS = """
{
  totalPars(first: 1000) {
    id
    supplyPar
    token {
      id
      symbol
      decimals
    }
  }
}
"""

# Paginated query for margin accounts with borrow value
QUERY_MARGIN_ACCOUNTS = """
query($skip: Int!, $first: Int!) {
  marginAccounts(
    first: $first,
    skip: $skip,
    where: { hasBorrowValue: true }
  ) {
    id
    user {
      id
    }
    effectiveUser {
      id
    }
    accountNumber
    lastUpdatedTimestamp
    borrowTokens {
      id
      symbol
      marketId
    }
    supplyTokens {
      id
      symbol
      marketId
    }
    tokenValues {
      id
      token {
        id
        symbol
        decimals
        marketId
      }
      valuePar
    }
  }
}
"""

QUERY_LIQUIDATIONS = """
query($skip: Int!, $first: Int!) {
  liquidations(
    first: $first,
    skip: $skip,
    orderBy: serialId,
    orderDirection: desc
  ) {
    id
    serialId
    transaction {
      id
      timestamp
      blockNumber
    }
    liquidEffectiveUser { id }
    solidEffectiveUser { id }
    liquidMarginAccount {
      accountNumber
      user { id }
      effectiveUser { id }
    }
    solidMarginAccount {
      accountNumber
      user { id }
      effectiveUser { id }
    }
    borrowedToken {
      id
      symbol
      decimals
      marketId
    }
    heldToken {
      id
      symbol
      decimals
      marketId
    }
    borrowedTokenAmountDeltaWei
    borrowedTokenAmountUSD
    heldTokenAmountDeltaWei
    heldTokenAmountUSD
    heldTokenLiquidationRewardUSD
  }
}
"""

QUERY_PROTOCOL_LIQUIDATION_FEE_TRANSFERS = """
query($txs: [String!], $feeAgent: String!) {
  transfers(
    first: 1000,
    where: {
      transaction_in: $txs,
      toEffectiveUser: $feeAgent
    },
    orderBy: serialId,
    orderDirection: asc
  ) {
    id
    serialId
    transaction {
      id
      timestamp
    }
    fromEffectiveUser { id }
    toEffectiveUser { id }
    toMarginAccount {
      accountNumber
      user { id }
    }
    token {
      id
      symbol
      decimals
      marketId
    }
    amountDeltaWei
    amountUSDDeltaWei
  }
}
"""


# ─── Helper Functions ─────────────────────────────────────────────────────────

def graphql_request(url, query, variables=None, retries=3):
    """Send a GraphQL request and return the JSON response."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    
    data = json.dumps(payload).encode("utf-8")
    
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                if "errors" in result:
                    print(f"  ⚠️  GraphQL errors: {result['errors']}")
                return result.get("data", {})
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            print(f"  ⚠️  Request failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return {}


def safe_float(value, default=0.0):
    """Parse numeric strings from the subgraph without failing the whole run."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalized_address(value):
    return str(value or "").strip().lower()


def normalized_token_id(token):
    return normalized_address((token or {}).get("id"))


def fetch_protocol_liquidation_fee_transfers(url, tx_hashes, fee_agent=DOLOMITE_PROTOCOL_FEE_AGENT):
    """Fetch confirmed Dolomite liquidation fee-rake transfers for a batch of transactions."""
    txs = sorted({normalized_address(tx) for tx in tx_hashes if normalized_address(tx)})
    if not txs:
        return []

    rows = []
    chunk_size = 100
    for start in range(0, len(txs), chunk_size):
        chunk = txs[start:start + chunk_size]
        data = graphql_request(
            url,
            QUERY_PROTOCOL_LIQUIDATION_FEE_TRANSFERS,
            variables={"txs": chunk, "feeAgent": normalized_address(fee_agent)},
        )
        rows.extend(data.get("transfers", []) or [])
        time.sleep(0.05)
    return rows


def attach_protocol_liquidation_fees(liquidation_rows, fee_transfers):
    """Attach only same-transaction fee-agent transfer amounts to matching liquidation rows."""
    transfer_groups = {}
    for transfer in fee_transfers or []:
        tx = normalized_address((transfer.get("transaction") or {}).get("id"))
        token_id = normalized_token_id(transfer.get("token"))
        if not tx or not token_id:
            continue
        to_user = normalized_address((transfer.get("toEffectiveUser") or {}).get("id"))
        if to_user and to_user != DOLOMITE_PROTOCOL_FEE_AGENT:
            continue
        account_number = str((transfer.get("toMarginAccount") or {}).get("accountNumber") or "0")
        if account_number != "0":
            continue
        transfer_groups.setdefault((tx, token_id), []).append({
            "id": transfer.get("id") or "",
            "serialId": safe_float(transfer.get("serialId"), -1),
            "amount": safe_float(transfer.get("amountDeltaWei"), 0),
            "amountUSD": safe_float(transfer.get("amountUSDDeltaWei"), 0),
            "token": transfer.get("token") or {},
        })

    for group in transfer_groups.values():
        group.sort(key=lambda item: item["serialId"])

    matched_transfer_ids = set()
    for row in sorted(liquidation_rows, key=lambda item: safe_float(item.get("serialId"), -1)):
        row["protocolFeeAmount"] = 0.0
        row["protocolFeeUSD"] = 0.0
        row["protocolFeeTransferId"] = ""
        row["protocolFeeSource"] = "none"

        tx = normalized_address(row.get("txHash"))
        held_token_id = normalized_token_id(row.get("heldToken"))
        serial_id = safe_float(row.get("serialId"), -1)
        for transfer in transfer_groups.get((tx, held_token_id), []):
            if transfer["id"] in matched_transfer_ids:
                continue
            if transfer["serialId"] <= serial_id:
                continue
            if transfer["amountUSD"] <= 0:
                continue
            matched_transfer_ids.add(transfer["id"])
            row["protocolFeeAmount"] = transfer["amount"]
            row["protocolFeeUSD"] = transfer["amountUSD"]
            row["protocolFeeTransferId"] = transfer["id"]
            row["protocolFeeSource"] = "feeAgentTransfer"
            break

    return liquidation_rows


def compute_health_factor(token_values, oracle_prices, interest_indices, market_risk_infos,
                          liquidation_ratio, margin_ratio_override=None):
    """
    Compute health factor for a margin account.
    
    HF = Σ(collateral_wei × price × weight) / Σ(debt_wei × price × weight)
    
    Where weight adjustments use marginPremium from MarketRiskInfo:
    - For collateral: weight = 1 / (1 + marginPremium) — collateral is worth LESS with higher premium
    - For debt: weight = (1 + marginPremium) — debt is worth MORE with higher premium
    
    When margin_ratio_override > 0, the account is in E-Mode:
    - Uses the override as liq_ratio (instead of global liquidation_ratio)
    - Zeroes all margin premiums (E-Mode assets are correlated, no risk adjustment)
    """
    total_collateral_usd = Decimal("0")
    total_debt_usd = Decimal("0")
    
    # E-Mode: if there's a margin ratio override, zero margin premiums
    has_emode = margin_ratio_override is not None and margin_ratio_override > 0
    
    collateral_tokens = []
    debt_tokens = []
    
    for tv in token_values:
        token_id = tv["token"]["id"]
        symbol = sanitize_symbol(tv["token"]["symbol"])
        decimals = int(tv["token"].get("decimals", "18"))
        market_id = tv["token"].get("marketId", "-1")
        value_par = Decimal(tv["valuePar"])
        
        if value_par == 0:
            continue
        
        # Get oracle price (USD per token unit, already adjusted for decimals by subgraph)
        price = Decimal("0")
        if token_id in oracle_prices:
            price = Decimal(oracle_prices[token_id])
        
        if price == 0:
            continue
        
        # Get interest index for par → wei conversion
        supply_index = Decimal("1")
        borrow_index = Decimal("1")
        if token_id in interest_indices:
            supply_index = Decimal(interest_indices[token_id]["supplyIndex"])
            borrow_index = Decimal(interest_indices[token_id]["borrowIndex"])
        
        # Get margin premium — zeroed in E-Mode
        margin_premium = Decimal("0")
        if not has_emode and token_id in market_risk_infos:
            margin_premium = Decimal(market_risk_infos[token_id].get("marginPremium", "0"))
        
        if value_par > 0:
            # Collateral (supply)
            wei_value = value_par * supply_index
            usd_value = wei_value * price
            # Apply margin premium: collateral is discounted
            weight = Decimal("1") / (Decimal("1") + margin_premium)
            adjusted_usd = usd_value * weight
            total_collateral_usd += adjusted_usd
            collateral_tokens.append({
                "symbol": symbol,
                "usd": float(usd_value),
                "marginPremium": float(margin_premium),
            })
        else:
            # Debt (borrow) — valuePar is negative
            wei_value = abs(value_par) * borrow_index
            usd_value = wei_value * price
            # Apply margin premium: debt is amplified
            weight = Decimal("1") + margin_premium
            adjusted_usd = usd_value * weight
            total_debt_usd += adjusted_usd
            debt_tokens.append({
                "symbol": symbol,
                "usd": float(usd_value),
                "marginPremium": float(margin_premium),
            })
    
    # Health factor = adjusted collateral / (adjusted debt × liquidation ratio)
    health_factor = None
    if total_debt_usd > 0:
        if has_emode:
            # E-Mode: use override (stored as e.g. 0.111 → liq_ratio = 1.111)
            liq_ratio = Decimal("1") + Decimal(str(margin_ratio_override))
        else:
            # Global ratio: stored as e.g. "1.15" in subgraph
            liq_ratio = Decimal(liquidation_ratio) if liquidation_ratio else Decimal("1.15")
        health_factor = float(total_collateral_usd / (total_debt_usd * liq_ratio))
    
    return {
        "healthFactor": health_factor,
        "collateralUSD": float(total_collateral_usd),
        "debtUSD": float(total_debt_usd),
        "collateralTokens": collateral_tokens,
        "debtTokens": debt_tokens,
    }


def fetch_risk_overrides(rpc_url, setter_address, accounts, label="", rpc_fallbacks=None):
    """
    Batch-query per-account E-Mode risk overrides via Multicall3.
    Uses smaller batch sizes to avoid 413 Payload Too Large on free RPCs.
    Falls back to individual calls with retry+backoff if multicall fails.
    Returns dict: {accountId: marginRatioOverride} where override > 0 means E-Mode.
    """
    try:
        from web3 import Web3
        from eth_abi import encode, decode
    except ImportError:
        print(f"  ⚠️  web3/eth_abi not installed — skipping E-Mode overrides for {label}")
        print(f"     Install with: pip3 install web3 eth_abi")
        return {}
    
    if not setter_address or setter_address == "0x0000000000000000000000000000000000000000":
        return {}
    
    # Multicall3 is deployed at same address on all EVM chains
    MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
    MULTICALL3_ABI = [{
        "inputs": [{"components": [
            {"name": "target", "type": "address"},
            {"name": "allowFailure", "type": "bool"},
            {"name": "callData", "type": "bytes"}
        ], "name": "calls", "type": "tuple[]"}],
        "name": "aggregate3",
        "outputs": [{"components": [
            {"name": "success", "type": "bool"},
            {"name": "returnData", "type": "bytes"}
        ], "name": "returnData", "type": "tuple[]"}],
        "stateMutability": "payable", "type": "function"
    }]
    
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 120}))
    setter = w3.eth.contract(
        address=Web3.to_checksum_address(setter_address),
        abi=SETTER_ABI
    )
    multicall = w3.eth.contract(
        address=Web3.to_checksum_address(MULTICALL3),
        abi=MULTICALL3_ABI
    )
    setter_addr_cs = Web3.to_checksum_address(setter_address)
    
    # Build list of RPC URLs to try (primary + fallbacks)
    rpc_urls = [rpc_url] + (rpc_fallbacks or [])
    
    overrides = {}
    # Use smaller batch size to avoid 413 Payload Too Large on free RPCs (Ethereum)
    batch_size = 50
    total = len(accounts)
    emode_count = 0
    failed_count = 0
    current_rpc_idx = 0
    
    print(f"  🔗 Fetching E-Mode overrides via Multicall3 ({total} accounts, {batch_size}/batch, {len(rpc_urls)} RPCs)...")
    
    for i in range(0, total, batch_size):
        batch = accounts[i:i + batch_size]
        
        # Create fresh Web3 connection for this batch (using current RPC)
        current_rpc = rpc_urls[current_rpc_idx % len(rpc_urls)]
        w3 = Web3(Web3.HTTPProvider(current_rpc, request_kwargs={"timeout": 120}))
        setter = w3.eth.contract(address=Web3.to_checksum_address(setter_address), abi=SETTER_ABI)
        multicall = w3.eth.contract(address=Web3.to_checksum_address(MULTICALL3), abi=MULTICALL3_ABI)
        setter_addr_cs = Web3.to_checksum_address(setter_address)
        
        # Build multicall3 call array
        calls = []
        batch_meta = []  # track account_id for each call
        for acct in batch:
            # IMPORTANT: use user.id (actual on-chain owner/vault), NOT effectiveUser.id
            # For vault/proxy accounts, getAccountRiskOverride needs the vault address
            # (user), not the wallet behind it (effectiveUser).
            owner = acct["user"]["id"]
            account_num = int(acct["accountNumber"])
            account_id = acct["id"]
            
            # Encode the calldata for getAccountRiskOverride((address,uint256))
            calldata = setter.functions.getAccountRiskOverride(
                (Web3.to_checksum_address(owner), account_num)
            )._encode_transaction_data()
            
            calls.append((setter_addr_cs, True, bytes.fromhex(calldata[2:])))
            batch_meta.append(account_id)
        
        # Execute multicall3.aggregate3 — single RPC request for entire batch
        batch_ok = False
        try:
            results = multicall.functions.aggregate3(calls).call()
            rpc_usage.record_request("eth_call")  # Multicall3 aggregate3 = 1 eth_call
            batch_ok = True
            
            for j, (success, return_data) in enumerate(results):
                if success and len(return_data) >= 64:
                    # Decode (uint256 marginRatioOverride, uint256 liquidationSpreadOverride)
                    mr_raw = int.from_bytes(return_data[:32], "big")
                    mr = mr_raw / 10**18
                    if mr > 0:
                        overrides[batch_meta[j]] = mr
                        emode_count += 1
        except Exception as e:
            # Fallback: try individual calls with retry + RPC rotation if multicall fails
            print(f"     ⚠️ Multicall3 failed for batch {i//batch_size + 1} on {current_rpc.split('//')[1].split('/')[0]}, falling back to individual calls: {e}")
            batch_failed = 0
            for k, acct in enumerate(batch):
                owner = acct["user"]["id"]
                account_num = int(acct["accountNumber"])
                account_id = acct["id"]
                
                # Retry with exponential backoff + RPC rotation
                success = False
                for attempt in range(len(rpc_urls) * 2):  # try each RPC twice
                    try:
                        retry_rpc = rpc_urls[attempt % len(rpc_urls)]
                        retry_w3 = Web3(Web3.HTTPProvider(retry_rpc, request_kwargs={"timeout": 30}))
                        retry_setter = retry_w3.eth.contract(address=Web3.to_checksum_address(setter_address), abi=SETTER_ABI)
                        margin_override, _ = retry_setter.functions.getAccountRiskOverride(
                            (Web3.to_checksum_address(owner), account_num)
                        ).call()
                        rpc_usage.record_request("eth_call")
                        mr = margin_override[0] / 10**18
                        if mr > 0:
                            overrides[account_id] = mr
                            emode_count += 1
                        success = True
                        break
                    except Exception as call_err:
                        if attempt < len(rpc_urls) * 2 - 1:
                            time.sleep(1.0 + attempt * 0.5)
                        else:
                            batch_failed += 1
                
                # Small delay between individual calls to avoid rate limiting
                if k % 10 == 9:
                    time.sleep(0.5)
            
            if batch_failed > 0:
                failed_count += batch_failed
                print(f"     ⚠️ {batch_failed}/{len(batch)} individual calls failed in this batch")
        
        # Rotate to next RPC after each batch to distribute load
        if not batch_ok:
            current_rpc_idx += 1
            time.sleep(2.0)
        else:
            time.sleep(0.3)
        
        progress = min(i + batch_size, total)
        print(f"     Processed {progress}/{total} accounts ({emode_count} E-Mode)...")
    
    if failed_count > 0:
        print(f"     ⚠️ WARNING: {failed_count} accounts failed E-Mode check — results may be incomplete")
    print(f"     ✅ Found {emode_count} accounts with E-Mode overrides")
    return overrides


def fetch_live_interest_indices(rpc_url, dolomite_margin_address, interest_indices, label="", rpc_fallbacks=None):
    """
    Fetch live interest indices from DolomiteMargin contract via Multicall3.
    Replaces stale subgraph indices with current on-chain values.
    
    Args:
        rpc_url: Primary RPC URL
        dolomite_margin_address: DolomiteMargin contract address on this chain
        interest_indices: Dict from subgraph {token_id: {borrowIndex, supplyIndex}}
            Token entries must have a 'marketId' key.
        label: Chain label for logging
        rpc_fallbacks: Optional list of fallback RPC URLs
    
    Returns:
        Updated interest_indices with live on-chain values, or original if RPC fails.
    """
    try:
        from web3 import Web3
    except ImportError:
        print(f"  ⚠️  web3 not installed — using subgraph interest indices for {label}")
        return interest_indices
    
    if not dolomite_margin_address or not rpc_url:
        return interest_indices
    
    # Build market_id → token_id mapping from subgraph data
    market_to_token = {}
    for token_id, idx_data in interest_indices.items():
        market_id = idx_data.get("marketId")
        if market_id is not None:
            market_to_token[int(market_id)] = token_id
    
    if not market_to_token:
        print(f"  ⚠️  No marketId mapping found — using subgraph interest indices")
        return interest_indices
    
    MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
    MULTICALL3_ABI = [{
        "inputs": [{"components": [
            {"name": "target", "type": "address"},
            {"name": "allowFailure", "type": "bool"},
            {"name": "callData", "type": "bytes"}
        ], "name": "calls", "type": "tuple[]"}],
        "name": "aggregate3",
        "outputs": [{"components": [
            {"name": "success", "type": "bool"},
            {"name": "returnData", "type": "bytes"}
        ], "name": "returnData", "type": "tuple[]"}],
        "stateMutability": "payable", "type": "function"
    }]
    
    # ABI for getMarketCurrentIndex(uint256) -> (Index { borrow: uint96, supply: uint96, lastUpdate: uint32 })
    GET_MARKET_CURRENT_INDEX_ABI = [{
        "inputs": [{"name": "_marketId", "type": "uint256"}],
        "name": "getMarketCurrentIndex",
        "outputs": [{"components": [
            {"name": "borrow", "type": "uint96"},
            {"name": "supply", "type": "uint96"},
            {"name": "lastUpdate", "type": "uint32"}
        ], "name": "", "type": "tuple"}],
        "stateMutability": "view", "type": "function"
    }]
    
    all_rpcs = [rpc_url] + (rpc_fallbacks or [])
    
    for rpc in all_rpcs:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 60}))
            dm_contract = w3.eth.contract(
                address=Web3.to_checksum_address(dolomite_margin_address),
                abi=GET_MARKET_CURRENT_INDEX_ABI
            )
            multicall = w3.eth.contract(
                address=Web3.to_checksum_address(MULTICALL3),
                abi=MULTICALL3_ABI
            )
            dm_addr_cs = Web3.to_checksum_address(dolomite_margin_address)
            
            # Build multicall3 calls for getMarketCurrentIndex
            market_ids = sorted(market_to_token.keys())
            calls = []
            for mid in market_ids:
                calldata = dm_contract.functions.getMarketCurrentIndex(mid)._encode_transaction_data()
                calls.append((dm_addr_cs, True, bytes.fromhex(calldata[2:])))
            
            print(f"  🔗 Fetching live interest indices via Multicall3 ({len(market_ids)} markets)...")
            results = multicall.functions.aggregate3(calls).call()
            rpc_usage.record_request("eth_call")  # Multicall3 aggregate3 = 1 eth_call
            
            updated_count = 0
            for i, (success, return_data) in enumerate(results):
                if success and len(return_data) >= 96:
                    # Decode: borrow (uint96), supply (uint96), lastUpdate (uint32)
                    # Packed as 3 x uint256 in return data (padded)
                    borrow_raw = int.from_bytes(return_data[0:32], "big")
                    supply_raw = int.from_bytes(return_data[32:64], "big")
                    
                    market_id = market_ids[i]
                    token_id = market_to_token[market_id]
                    
                    # Exact decimal strings — no float64 rounding on 18-decimal indices
                    new_borrow = format(Decimal(borrow_raw).scaleb(-18), "f")
                    new_supply = format(Decimal(supply_raw).scaleb(-18), "f")
                    
                    old_borrow = interest_indices[token_id]["borrowIndex"]
                    old_supply = interest_indices[token_id]["supplyIndex"]
                    
                    interest_indices[token_id]["borrowIndex"] = new_borrow
                    interest_indices[token_id]["supplyIndex"] = new_supply
                    updated_count += 1
            
            print(f"     ✅ Updated {updated_count}/{len(market_ids)} interest indices from on-chain")
            return interest_indices
            
        except Exception as e:
            rpc_host = rpc.split("//")[-1].split("/")[0]
            print(f"     ⚠️ Live index fetch failed on {rpc_host}: {e}")
            continue
    
    print(f"     ⚠️ All RPCs failed for live indices — using subgraph data (may be stale)")
    return interest_indices


def fetch_live_oracle_prices(rpc_url, dolomite_margin_address, oracle_prices, market_to_token, label="", rpc_fallbacks=None):
    """
    Fetch live oracle prices from DolomiteMargin contract via Multicall3.
    Replaces stale subgraph prices with current on-chain values.
    
    Args:
        rpc_url: Primary RPC URL
        dolomite_margin_address: DolomiteMargin contract address
        oracle_prices: Dict {token_id: price_string} from subgraph
        market_to_token: Dict {int(marketId): token_id} mapping
        label: Chain label for logging
        rpc_fallbacks: Optional list of fallback RPC URLs
    
    Returns:
        Updated oracle_prices with live on-chain values.
    """
    try:
        from web3 import Web3
    except ImportError:
        return oracle_prices
    
    if not dolomite_margin_address or not rpc_url:
        return oracle_prices
    
    if not market_to_token:
        return oracle_prices
    
    MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
    MULTICALL3_ABI = [{
        "inputs": [{"components": [
            {"name": "target", "type": "address"},
            {"name": "allowFailure", "type": "bool"},
            {"name": "callData", "type": "bytes"}
        ], "name": "calls", "type": "tuple[]"}],
        "name": "aggregate3",
        "outputs": [{"components": [
            {"name": "success", "type": "bool"},
            {"name": "returnData", "type": "bytes"}
        ], "name": "returnData", "type": "tuple[]"}],
        "stateMutability": "payable", "type": "function"
    }]
    
    GET_MARKET_PRICE_ABI = [{
        "inputs": [{"name": "_marketId", "type": "uint256"}],
        "name": "getMarketPrice",
        "outputs": [{"components": [{"name": "value", "type": "uint256"}], "name": "", "type": "tuple"}],
        "stateMutability": "view", "type": "function"
    }]
    
    all_rpcs = [rpc_url] + (rpc_fallbacks or [])
    
    for rpc in all_rpcs:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 60}))
            dm_contract = w3.eth.contract(
                address=Web3.to_checksum_address(dolomite_margin_address),
                abi=GET_MARKET_PRICE_ABI
            )
            multicall = w3.eth.contract(
                address=Web3.to_checksum_address(MULTICALL3),
                abi=MULTICALL3_ABI
            )
            dm_addr_cs = Web3.to_checksum_address(dolomite_margin_address)
            
            market_ids = sorted(market_to_token.keys())
            calls = []
            for mid in market_ids:
                calldata = dm_contract.functions.getMarketPrice(mid)._encode_transaction_data()
                calls.append((dm_addr_cs, True, bytes.fromhex(calldata[2:])))
            
            print(f"  🔗 Fetching live oracle prices via Multicall3 ({len(market_ids)} markets)...")
            results = multicall.functions.aggregate3(calls).call()
            rpc_usage.record_request("eth_call")  # Multicall3 aggregate3 = 1 eth_call
            
            updated_count = 0
            for i, (success, return_data) in enumerate(results):
                if success and len(return_data) >= 32:
                    price_raw = int.from_bytes(return_data[0:32], "big")
                    market_id = market_ids[i]
                    entry = market_to_token[market_id]
                    
                    # Price from contract: raw / 10^(36-decimals) = price per TOKEN in USD (matching subgraph)
                    token_decimals = entry.get("decimals", 18)
                    divisor = 10 ** (36 - token_decimals)
                    new_price = str(price_raw / divisor)
                    token_id = entry["token_id"]
                    oracle_prices[token_id] = new_price
                    updated_count += 1
            
            print(f"     ✅ Updated {updated_count}/{len(market_ids)} oracle prices from on-chain")
            return oracle_prices
            
        except Exception as e:
            rpc_host = rpc.split("//")[-1].split("/")[0]
            print(f"     ⚠️ Live price fetch failed on {rpc_host}: {e}")
            continue
    
    print(f"     ⚠️ All RPCs failed for live prices — using subgraph data (may be stale)")
    return oracle_prices


def classify_risk(hf, emode=False):
    """Classify risk level based on health factor.
    Uses lower thresholds for E-Mode (correlated assets)."""
    if hf is None:
        return "UNKNOWN"
    if emode:
        if hf < HF_EMODE_CRITICAL:
            return "CRITICAL"
        elif hf < HF_EMODE_DANGER:
            return "DANGER"
        elif hf < HF_EMODE_WARNING:
            return "WARNING"
        else:
            return "SAFE"
    else:
        if hf < HF_CRITICAL:
            return "CRITICAL"
        elif hf < HF_DANGER:
            return "DANGER"
        elif hf < HF_WARNING:
            return "WARNING"
        else:
            return "SAFE"


def fetch_chain_data(chain_key, chain_config):
    """Fetch all liquidation risk data for a single chain."""
    
    url = get_subgraph_url(chain_config)
    label = chain_config["label"]
    
    print(f"\n{'='*60}")
    print(f"  Fetching {label} data...")
    print(f"{'='*60}")
    
    # 1. Fetch DolomiteMargin params
    print(f"  📊 Fetching DolomiteMargin params...")
    dm_data = graphql_request(url, QUERY_DOLOMITE_MARGIN)
    margins = dm_data.get("dolomiteMargins", [])
    if not margins:
        print(f"  ❌ No DolomiteMargin data found for {label}")
        return None
    
    dolomite_margin_address = margins[0].get("id", "")
    liquidation_ratio = margins[0].get("liquidationRatio", "1.15")
    liquidation_reward = margins[0].get("liquidationReward", "0.05")
    num_markets = margins[0].get("numberOfMarkets", 0)
    default_setter = margins[0].get("defaultAccountRiskOverrideSetter", None)
    print(f"     Liquidation ratio: {liquidation_ratio}")
    print(f"     Liquidation reward: {liquidation_reward}")
    print(f"     Number of markets: {num_markets}")
    print(f"     DolomiteMargin contract: {dolomite_margin_address}")
    print(f"     Default risk override setter: {default_setter or 'None'}")
    
    # 2. Fetch market risk info (margin premiums per token)
    print(f"  📊 Fetching market risk info...")
    risk_data = graphql_request(url, QUERY_MARKET_RISK_INFO)
    market_risk_infos = {}
    for info in risk_data.get("marketRiskInfos", []):
        token_id = info["token"]["id"]
        market_risk_infos[token_id] = {
            "marginPremium": info["marginPremium"],
            "liquidationRewardPremium": info["liquidationRewardPremium"],
            "symbol": sanitize_symbol(info["token"]["symbol"]),
            "isBorrowingDisabled": info.get("isBorrowingDisabled", False),
        }
    print(f"     Found {len(market_risk_infos)} markets with risk info")
    
    # Show stablecoin vs volatile premiums
    for tid, rinfo in sorted(market_risk_infos.items(), key=lambda x: float(x[1]["marginPremium"])):
        premium = float(rinfo["marginPremium"])
        symbol = rinfo["symbol"]
        label_type = "STABLE" if premium < 0.01 else "VOLATILE"
        print(f"       {symbol:>10s}: marginPremium={premium:.4f} ({label_type})")
    
    # 3. Fetch oracle prices (start with subgraph, then upgrade to live on-chain)
    print(f"  📊 Fetching oracle prices...")
    price_data = graphql_request(url, QUERY_ORACLE_PRICES)
    oracle_prices = {}
    price_market_to_token = {}  # {int(marketId): {token_id, decimals}} for live price fetch
    for op in price_data.get("oraclePrices", []):
        token_id = op["token"]["id"]
        oracle_prices[token_id] = op["price"]
        market_id = op["token"].get("marketId")
        decimals = int(op["token"].get("decimals", "18"))
        if market_id is not None:
            price_market_to_token[int(market_id)] = {
                "token_id": token_id,
                "decimals": decimals,
            }
    print(f"     Found {len(oracle_prices)} oracle prices (subgraph)")
    
    # 3b. Upgrade to live on-chain oracle prices for accurate HF
    rpc_url_early = chain_config.get("rpc")
    rpc_fallbacks_early = chain_config.get("rpc_fallbacks", [])
    if rpc_url_early and dolomite_margin_address and price_market_to_token:
        oracle_prices = fetch_live_oracle_prices(
            rpc_url_early, dolomite_margin_address, oracle_prices,
            price_market_to_token,
            label=label, rpc_fallbacks=rpc_fallbacks_early
        )
    
    # 4. Fetch interest indices (start with subgraph, then upgrade to live on-chain)
    print(f"  📊 Fetching interest indices...")
    index_data = graphql_request(url, QUERY_INTEREST_INDICES)
    interest_indices = {}
    for idx in index_data.get("interestIndexes", []):
        token_id = idx["token"]["id"]
        interest_indices[token_id] = {
            "borrowIndex": idx["borrowIndex"],
            "supplyIndex": idx["supplyIndex"],
            "marketId": idx["token"].get("marketId"),
        }
    print(f"     Found {len(interest_indices)} interest indices (subgraph)")
    
    # 4c. Fetch totalPars for supply liquidity ranking
    print(f"  📊 Fetching total supply pars...")
    pars_data = graphql_request(url, QUERY_TOTAL_PARS)
    market_supply = {}  # {symbol: supplyUSD}
    for tp in pars_data.get("totalPars", []):
        token_id = tp.get("id", "").lower()
        sym = tp.get("token", {}).get("symbol", "")
        supply_par = float(tp.get("supplyPar", "0"))
        if not sym or supply_par <= 0:
            continue
        # Convert par → wei using supply index
        sup_idx = float(interest_indices.get(token_id, {}).get("supplyIndex", "1"))
        supply_wei = supply_par * sup_idx
        # Convert to USD using oracle price
        price = float(oracle_prices.get(token_id, "0"))
        supply_usd = supply_wei * price
        if supply_usd > 0:
            market_supply[sym] = round(market_supply.get(sym, 0) + supply_usd, 2)
    print(f"     Computed supply USD for {len(market_supply)} tokens")
    
    # 4b. Upgrade to live on-chain interest indices for accurate HF
    rpc_url = chain_config.get("rpc")
    rpc_fallbacks = chain_config.get("rpc_fallbacks", [])
    if rpc_url and dolomite_margin_address:
        interest_indices = fetch_live_interest_indices(
            rpc_url, dolomite_margin_address, interest_indices,
            label=label, rpc_fallbacks=rpc_fallbacks
        )
    
    # 5. Fetch all margin accounts with borrows (paginated)
    print(f"  📊 Fetching margin accounts with borrows...")
    all_accounts = []
    skip = 0
    page_size = 500
    
    while True:
        accounts_data = graphql_request(
            url, QUERY_MARGIN_ACCOUNTS,
            variables={"skip": skip, "first": page_size}
        )
        accounts = accounts_data.get("marginAccounts", [])
        if not accounts:
            break
        all_accounts.extend(accounts)
        print(f"     Fetched {len(all_accounts)} accounts so far...")
        if len(accounts) < page_size:
            break
        skip += page_size
        time.sleep(0.5)  # Rate limiting
    
    print(f"     Total accounts with borrows: {len(all_accounts)}")
    
    # 5b. Fetch E-Mode risk overrides via RPC
    risk_overrides = {}
    rpc_url = chain_config.get("rpc")
    if rpc_url and default_setter:
        rpc_fallbacks = chain_config.get("rpc_fallbacks", [])
        risk_overrides = fetch_risk_overrides(rpc_url, default_setter, all_accounts, label, rpc_fallbacks=rpc_fallbacks)
    else:
        print(f"  ℹ️  No E-Mode setter on {label} — using global liquidation ratio for all accounts")
    
    # 6. Compute health factors
    print(f"  🧮 Computing health factors...")
    positions = []
    
    for account in all_accounts:
        user_addr = account["effectiveUser"]["id"]
        account_number = account["accountNumber"]
        token_values = account.get("tokenValues", [])
        
        if not token_values:
            continue
        
        # Check for per-account E-Mode override
        account_id = account["id"]
        mr_override = risk_overrides.get(account_id)
        
        result = compute_health_factor(
            token_values, oracle_prices, interest_indices,
            market_risk_infos, liquidation_ratio,
            margin_ratio_override=mr_override
        )
        
        hf = result["healthFactor"]
        is_emode = mr_override is not None and mr_override > 0
        risk_level = classify_risk(hf, emode=is_emode)
        
        positions.append({
            "chain": chain_key,
            "chainLabel": chain_config["label"],
            "address": user_addr,
            "accountNumber": str(account_number),
            "accountId": account["id"],
            "healthFactor": round(hf, 4) if hf is not None else None,
            "riskLevel": risk_level,
            "collateralUSD": round(result["collateralUSD"], 2),
            "debtUSD": round(result["debtUSD"], 2),
            "collateralTokens": result["collateralTokens"],
            "debtTokens": result["debtTokens"],
            "eMode": mr_override is not None and mr_override > 0,
            "explorer": chain_config["explorer"] + user_addr,
            "lastUpdated": account.get("lastUpdatedTimestamp", ""),
        })
    
    # Sort by health factor ascending (most at-risk first)
    positions.sort(key=lambda x: x["healthFactor"] if x["healthFactor"] is not None else 999)
    
    # Stats
    critical = sum(1 for p in positions if p["riskLevel"] == "CRITICAL")
    danger = sum(1 for p in positions if p["riskLevel"] == "DANGER")
    warning = sum(1 for p in positions if p["riskLevel"] == "WARNING")
    safe = sum(1 for p in positions if p["riskLevel"] == "SAFE")
    
    print(f"\n  📈 {label} Results:")
    print(f"     🔴 CRITICAL (HF < {HF_CRITICAL}): {critical}")
    print(f"     🟠 DANGER   (HF < {HF_DANGER}): {danger}")
    print(f"     🟡 WARNING  (HF < {HF_WARNING}): {warning}")
    print(f"     🟢 SAFE     (HF ≥ {HF_WARNING}): {safe}")
    
    return {
        "positions": positions,
        "stats": {
            "total": len(positions),
            "critical": critical,
            "danger": danger,
            "warning": warning,
            "safe": safe,
            "totalCollateralUSD": round(sum(p["collateralUSD"] for p in positions), 2),
            "totalDebtUSD": round(sum(p["debtUSD"] for p in positions), 2),
        },
        "params": {
            "liquidationRatio": liquidation_ratio,
            "liquidationReward": liquidation_reward,
            "numberOfMarkets": num_markets,
        },
        "marketSupply": market_supply,
    }


def fetch_chain_liquidation_history(chain_key, chain_config):
    """Fetch historical Dolomite liquidation events for one chain."""
    label = chain_config["label"]
    url = get_subgraph_url(chain_config)
    page_size = 1000
    skip = 0
    rows = []

    print(f"\n  🧾 Fetching liquidation history for {label}...")

    while True:
        data = graphql_request(
            url,
            QUERY_LIQUIDATIONS,
            variables={"skip": skip, "first": page_size},
        )
        liquidations = data.get("liquidations", [])
        if not liquidations:
            break

        page_rows = []
        for item in liquidations:
            tx = item.get("transaction") or {}
            liquid_user = item.get("liquidEffectiveUser") or {}
            solid_user = item.get("solidEffectiveUser") or {}
            liquid_account = item.get("liquidMarginAccount") or {}
            solid_account = item.get("solidMarginAccount") or {}
            borrowed_token = item.get("borrowedToken") or {}
            held_token = item.get("heldToken") or {}
            tx_hash = tx.get("id") or ""
            liquidated_address = liquid_user.get("id") or (liquid_account.get("effectiveUser") or {}).get("id") or ""
            liquidator_address = solid_user.get("id") or (solid_account.get("effectiveUser") or {}).get("id") or ""

            page_rows.append({
                "id": item.get("id") or f"{chain_key}-{tx_hash}-{item.get('serialId', skip)}",
                "chain": chain_key,
                "chainLabel": label,
                "serialId": str(item.get("serialId") or ""),
                "txHash": tx_hash,
                "blockNumber": str(tx.get("blockNumber") or ""),
                "timestamp": int(safe_float(tx.get("timestamp"), 0)),
                "liquidatedAddress": liquidated_address,
                "liquidatedAccountNumber": str(liquid_account.get("accountNumber") or ""),
                "liquidatorAddress": liquidator_address,
                "liquidatorAccountNumber": str(solid_account.get("accountNumber") or ""),
                "borrowedToken": {
                    "id": borrowed_token.get("id") or "",
                    "symbol": borrowed_token.get("symbol") or "TOKEN",
                    "marketId": str(borrowed_token.get("marketId") or ""),
                },
                "heldToken": {
                    "id": held_token.get("id") or "",
                    "symbol": held_token.get("symbol") or "TOKEN",
                    "marketId": str(held_token.get("marketId") or ""),
                },
                "debtRepaidAmount": safe_float(item.get("borrowedTokenAmountDeltaWei"), 0),
                "debtRepaidUSD": safe_float(item.get("borrowedTokenAmountUSD"), 0),
                "collateralSeizedAmount": safe_float(item.get("heldTokenAmountDeltaWei"), 0),
                "collateralSeizedUSD": safe_float(item.get("heldTokenAmountUSD"), 0),
                "liquidationRewardUSD": safe_float(item.get("heldTokenLiquidationRewardUSD"), 0),
                "explorer": (chain_config.get("explorer") or "") + liquidated_address,
                "txExplorer": (chain_config.get("tx_explorer") or "").rstrip("/") + "/" + tx_hash if chain_config.get("tx_explorer") else "",
            })

        fee_transfers = fetch_protocol_liquidation_fee_transfers(url, [row.get("txHash") for row in page_rows])
        attach_protocol_liquidation_fees(page_rows, fee_transfers)
        rows.extend(page_rows)

        print(f"     Fetched {len(rows)} liquidations so far...")
        if len(liquidations) < page_size:
            break
        skip += page_size
        time.sleep(0.25)

    rows.sort(key=lambda x: (x.get("timestamp") or 0, safe_float(x.get("serialId"), 0)), reverse=True)
    print(f"     Total liquidations: {len(rows)}")
    return rows


def load_previous_snapshot(path):
    """Load a previously committed output JSON (for stale-chain fallback)."""
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def count_monitored_positions(positions, min_usd=MONITORED_POSITION_MIN_USD):
    """Count the same non-dust position scope displayed in the Borrow hero."""
    return sum(
        1
        for position in positions or []
        if (
            safe_float((position or {}).get("collateralUSD"))
            + safe_float((position or {}).get("debtUSD"))
        ) >= min_usd
    )


def build_position_count_history(previous, generated_at, current_count):
    """Append one observation and derive an honest nearest-to-24h comparison."""
    generated_at = int(generated_at)
    current_count = int(current_count)
    earliest_kept = generated_at - POSITION_COUNT_HISTORY_RETENTION_SECONDS
    observations_by_timestamp = {}

    for observation in (previous or {}).get("snapshots") or []:
        try:
            timestamp = int(observation.get("timestamp"))
            count = int(observation.get("count"))
        except (AttributeError, TypeError, ValueError):
            continue
        if timestamp < earliest_kept or timestamp > generated_at or count < 0:
            continue
        observations_by_timestamp[timestamp] = count

    observations_by_timestamp[generated_at] = max(0, current_count)
    snapshots = [
        {"timestamp": timestamp, "count": count}
        for timestamp, count in sorted(observations_by_timestamp.items())
    ]

    target_timestamp = generated_at - POSITION_COUNT_24H_SECONDS
    baseline_candidates = [
        observation
        for observation in snapshots
        if abs(observation["timestamp"] - target_timestamp) <= POSITION_COUNT_24H_MAX_SKEW_SECONDS
    ]
    baseline = min(
        baseline_candidates,
        key=lambda observation: (
            abs(observation["timestamp"] - target_timestamp),
            observation["timestamp"],
        ),
        default=None,
    )
    change_24h = None
    if baseline is not None:
        change_24h = {
            "currentCount": max(0, current_count),
            "baselineCount": baseline["count"],
            "change": max(0, current_count) - baseline["count"],
            "baselineAt": baseline["timestamp"],
        }

    return {
        "generatedAt": generated_at,
        "snapshots": snapshots,
        "change24h": change_24h,
    }


def write_position_count_history(positions, generated_at, output_path=POSITION_COUNT_HISTORY_FILE):
    output_path = Path(output_path)
    previous = load_previous_snapshot(output_path)
    payload = build_position_count_history(
        previous,
        generated_at,
        count_monitored_positions(positions),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    return payload


def build_liquidation_history_stats(rows):
    by_chain = {}
    for row in rows:
        chain = row.get("chain") or "unknown"
        stats = by_chain.setdefault(chain, {
            "count": 0,
            "debtRepaidUSD": 0.0,
            "collateralSeizedUSD": 0.0,
            "liquidationRewardUSD": 0.0,
            "protocolFeeUSD": 0.0,
        })
        stats["count"] += 1
        stats["debtRepaidUSD"] += safe_float(row.get("debtRepaidUSD"), 0)
        stats["collateralSeizedUSD"] += safe_float(row.get("collateralSeizedUSD"), 0)
        stats["liquidationRewardUSD"] += safe_float(row.get("liquidationRewardUSD"), 0)
        stats["protocolFeeUSD"] += safe_float(row.get("protocolFeeUSD"), 0)

    for stats in by_chain.values():
        for key in ("debtRepaidUSD", "collateralSeizedUSD", "liquidationRewardUSD", "protocolFeeUSD"):
            stats[key] = round(stats[key], 2)

    return {
        "total": len(rows),
        "debtRepaidUSD": round(sum(s["debtRepaidUSD"] for s in by_chain.values()), 2),
        "collateralSeizedUSD": round(sum(s["collateralSeizedUSD"] for s in by_chain.values()), 2),
        "liquidationRewardUSD": round(sum(s["liquidationRewardUSD"] for s in by_chain.values()), 2),
        "protocolFeeUSD": round(sum(s["protocolFeeUSD"] for s in by_chain.values()), 2),
        "byChain": by_chain,
    }


# ─── Sample Data Generator ───────────────────────────────────────────────────

def generate_sample_data():
    """Generate realistic sample data for UI testing without a subgraph connection."""
    print("=" * 60)
    print("  Generating sample data for UI testing...")
    print("=" * 60)
    
    random.seed(42)  # Reproducible
    
    # Token definitions with realistic prices
    TOKENS = {
        # Stablecoins (low margin premium → higher leverage allowed)
        "USDC":   {"price": 1.00,   "marginPremium": 0.0, "type": "stable"},
        "USDT":   {"price": 1.00,   "marginPremium": 0.0, "type": "stable"},
        "DAI":    {"price": 1.00,   "marginPremium": 0.0, "type": "stable"},
        "HONEY":  {"price": 1.00,   "marginPremium": 0.0, "type": "stable"},
        # Volatile tokens (higher margin premium)
        "WETH":   {"price": 2450.0, "marginPremium": 0.05, "type": "volatile"},
        "WBTC":   {"price": 84500.0, "marginPremium": 0.05, "type": "volatile"},
        "BERA":   {"price": 5.80,   "marginPremium": 0.10, "type": "volatile"},
        "ARB":    {"price": 0.42,   "marginPremium": 0.10, "type": "volatile"},
        "DOLO":   {"price": 0.085,  "marginPremium": 0.15, "type": "volatile"},
        "BGT":    {"price": 2.35,   "marginPremium": 0.12, "type": "volatile"},
    }
    
    chain_configs = [
        ("berachain", "Berachain", "https://berascan.com/address/",
         ["HONEY", "USDC", "WETH", "BERA", "BGT", "DOLO", "WBTC"]),
        ("arbitrum", "Arbitrum", "https://arbiscan.io/address/",
         ["USDC", "USDT", "DAI", "WETH", "WBTC", "ARB", "DOLO"]),
        ("base", "Botanix", "https://explorer.botanixlabs.dev/address/",
         ["USDC.e", "WETH", "pBTC", "stBTC", "oUSDT"]),
        ("xlayer", "X Layer", "https://www.okx.com/web3/explorer/xlayer/address/",
         ["USDC", "USDT", "WETH", "WBTC", "DOLO"]),
        ("polygon_zkevm", "Polygon zkEVM", "https://zkevm.polygonscan.com/address/",
         ["USDC", "USDT", "WETH", "WBTC", "DAI", "DOLO"]),
        ("mantle", "Mantle", "https://mantlescan.xyz/address/",
         ["USDC", "USDT", "WETH", "WBTC", "DOLO"]),
    ]
    
    all_positions = []
    chain_stats = {}
    
    for chain_key, chain_label, explorer_base, available_tokens in chain_configs:
        positions = []
        # Generate positions with a realistic HF distribution
        # More safe positions, fewer critical
        n_positions = random.randint(50, 80)
        
        for i in range(n_positions):
            addr = "0x" + "".join(random.choices("0123456789abcdef", k=40))
            acct_num = str(random.choice([0, 0, 0, 0, 1, 2, 3]))
            
            # Generate health factor with realistic distribution
            # 5% critical, 10% danger, 15% warning, 70% safe
            r = random.random()
            if r < 0.05:
                hf = round(random.uniform(0.95, 1.05), 4)
            elif r < 0.15:
                hf = round(random.uniform(1.05, 1.15), 4)
            elif r < 0.30:
                hf = round(random.uniform(1.15, 1.30), 4)
            else:
                hf = round(random.uniform(1.30, 5.0), 4)
            
            risk_level = classify_risk(hf)
            
            # Generate collateral/debt with realistic values
            # Bigger positions for critical accounts (whale-like)
            if risk_level in ("CRITICAL", "DANGER"):
                base_value = random.uniform(10000, 500000)
            else:
                base_value = random.uniform(100, 200000)
            
            # Pick collateral and debt tokens
            n_collateral = random.choice([1, 1, 1, 2, 2, 3])
            n_debt = random.choice([1, 1, 2])
            
            collateral_syms = random.sample(available_tokens, min(n_collateral, len(available_tokens)))
            debt_syms = random.sample([t for t in available_tokens if t not in collateral_syms],
                                      min(n_debt, len(available_tokens) - len(collateral_syms)))
            
            if not debt_syms:
                debt_syms = [random.choice(["USDC", "USDT", "HONEY"])]
            
            collateral_usd = base_value * hf  # Approximate
            debt_usd = base_value
            
            # Distribute across tokens
            collateral_tokens = []
            remaining = collateral_usd
            for j, sym in enumerate(collateral_syms):
                if j == len(collateral_syms) - 1:
                    amt = remaining
                else:
                    amt = remaining * random.uniform(0.2, 0.7)
                    remaining -= amt
                collateral_tokens.append({
                    "symbol": sym,
                    "usd": round(amt, 2),
                    "marginPremium": TOKENS.get(sym, {}).get("marginPremium", 0.05),
                })
            
            debt_tokens = []
            remaining = debt_usd
            for j, sym in enumerate(debt_syms):
                if j == len(debt_syms) - 1:
                    amt = remaining
                else:
                    amt = remaining * random.uniform(0.3, 0.7)
                    remaining -= amt
                debt_tokens.append({
                    "symbol": sym,
                    "usd": round(amt, 2),
                    "marginPremium": TOKENS.get(sym, {}).get("marginPremium", 0.05),
                })
            
            positions.append({
                "chain": chain_key,
                "chainLabel": chain_label,
                "address": addr,
                "accountNumber": acct_num,
                "accountId": f"{addr}-{acct_num}",
                "healthFactor": hf,
                "riskLevel": risk_level,
                "collateralUSD": round(collateral_usd, 2),
                "debtUSD": round(debt_usd, 2),
                "collateralTokens": collateral_tokens,
                "debtTokens": debt_tokens,
                "explorer": explorer_base + addr,
                "lastUpdated": str(int(time.time()) - random.randint(60, 3600)),
            })
        
        positions.sort(key=lambda x: x["healthFactor"])
        all_positions.extend(positions)
        
        critical = sum(1 for p in positions if p["riskLevel"] == "CRITICAL")
        danger = sum(1 for p in positions if p["riskLevel"] == "DANGER")
        warning = sum(1 for p in positions if p["riskLevel"] == "WARNING")
        safe = sum(1 for p in positions if p["riskLevel"] == "SAFE")
        
        chain_stats[chain_key] = {
            "total": len(positions),
            "critical": critical,
            "danger": danger,
            "warning": warning,
            "safe": safe,
            "totalCollateralUSD": round(sum(p["collateralUSD"] for p in positions), 2),
            "totalDebtUSD": round(sum(p["debtUSD"] for p in positions), 2),
        }
        
        print(f"\n  📈 {chain_label}: {len(positions)} positions")
        print(f"     🔴 {critical} 🟠 {danger} 🟡 {warning} 🟢 {safe}")
    
    all_positions.sort(key=lambda x: x["healthFactor"])
    
    total_critical = sum(s["critical"] for s in chain_stats.values())
    total_danger = sum(s["danger"] for s in chain_stats.values())
    total_warning = sum(s["warning"] for s in chain_stats.values())
    
    output = {
        "generatedAt": int(time.time()),
        "generatedAtISO": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "isSampleData": True,
        "thresholds": {
            "critical": HF_CRITICAL,
            "danger": HF_DANGER,
            "warning": HF_WARNING,
            "emodeCritical": HF_EMODE_CRITICAL,
            "emodeDanger": HF_EMODE_DANGER,
            "emodeWarning": HF_EMODE_WARNING,
        },
        "globalStats": {
            "totalPositions": len(all_positions),
            "atRisk": total_critical + total_danger + total_warning,
            "critical": total_critical,
            "danger": total_danger,
            "warning": total_warning,
            "totalCollateralUSD": round(sum(s["totalCollateralUSD"] for s in chain_stats.values()), 2),
            "totalDebtUSD": round(sum(s["totalDebtUSD"] for s in chain_stats.values()), 2),
        },
        "chainStats": chain_stats,
        "chainParams": {
            "berachain": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 12},
            "arbitrum": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 18},
            "base": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 5},
            "xlayer": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 6},
            "polygon_zkevm": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 10},
        },
        "liquidationHistoryStats": build_liquidation_history_stats([]),
        "liquidationHistoryFile": HISTORY_FILE,
        "positions": all_positions,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    write_liquidation_risk_shards(output)
    write_position_count_history(all_positions, output["generatedAt"])

    with open(HISTORY_FILE, "w") as f:
        json.dump({
            "generatedAt": output["generatedAt"],
            "generatedAtISO": output["generatedAtISO"],
            "liquidationHistory": [],
        }, f, separators=(",", ":"))
    
    print(f"\n{'='*60}")
    print(f"  ✅ Sample data saved to {OUTPUT_FILE}")
    print(f"  📊 Total positions: {len(all_positions)}")
    print(f"  ⚠️  At risk: {total_critical + total_danger + total_warning}")
    print(f"  ⚡ NOTE: This is SAMPLE data for UI testing.")
    print(f"     For live data, set GRAPH_API_KEY or use --api-key flag.")
    print(f"     Get a free key at: https://thegraph.com/studio/apikeys/")
    print(f"{'='*60}")


def main():
    # Parse CLI arguments
    args = sys.argv[1:]
    use_sample = "--sample" in args
    
    if use_sample:
        generate_sample_data()
        return
    
    print("=" * 60)
    print("  Dolomite Liquidation Risk Scanner")
    print("=" * 60)
    
    all_positions = []
    chain_stats = {}
    chain_params = {}
    all_market_supply = {}
    all_liquidations = []
    failed_chains = []
    failed_history_chains = []

    for chain_key, chain_config in CHAINS.items():
        try:
            result = fetch_chain_data(chain_key, chain_config)
            if result:
                all_positions.extend(result["positions"])
                chain_stats[chain_key] = result["stats"]
                chain_params[chain_key] = result["params"]
                # Merge market supply data across chains
                for sym, usd in result.get("marketSupply", {}).items():
                    all_market_supply[sym] = round(all_market_supply.get(sym, 0) + usd, 2)
            else:
                # Empty subgraph response also drops the chain from output.
                failed_chains.append(chain_key)
        except Exception as e:
            print(f"\n  ❌ Error fetching {chain_config['label']}: {e}")
            import traceback
            traceback.print_exc()
            failed_chains.append(chain_key)
        try:
            all_liquidations.extend(fetch_chain_liquidation_history(chain_key, chain_config))
        except Exception as e:
            print(f"\n  ❌ Error fetching liquidation history for {chain_config['label']}: {e}")
            import traceback
            traceback.print_exc()
            failed_history_chains.append(chain_key)

    # Stale-chain fallback: a failed subgraph must not erase the whole chain
    # from the published files. Carry the chain's data over from the previous
    # snapshot and mark it stale instead.
    stale_chains = []
    if failed_chains:
        previous = load_previous_snapshot(OUTPUT_FILE)
        prev_stats = previous.get("chainStats") or {}
        prev_params = previous.get("chainParams") or {}
        prev_positions = previous.get("positions") or []
        for chain_key in failed_chains:
            if chain_key not in prev_stats:
                print(f"::warning::chain {chain_key} failed and has no previous snapshot data to fall back to")
                continue
            stats = dict(prev_stats[chain_key])
            stats["stale"] = True
            chain_stats[chain_key] = stats
            if chain_key in prev_params:
                chain_params[chain_key] = prev_params[chain_key]
            carried = [p for p in prev_positions if p.get("chain") == chain_key]
            all_positions.extend(carried)
            stale_chains.append(chain_key)
            print(f"::warning::chain {chain_key} served from previous snapshot ({len(carried)} positions)")
        # Note: marketSupply is aggregated per symbol across chains, so the
        # failed chain's share cannot be carried over; it stays fresh-only.
    if failed_history_chains:
        prev_history = load_previous_snapshot(HISTORY_FILE)
        prev_rows = prev_history.get("liquidationHistory") or []
        for chain_key in failed_history_chains:
            carried_rows = [r for r in prev_rows if r.get("chain") == chain_key]
            if not carried_rows:
                print(f"::warning::chain {chain_key} history failed and has no previous snapshot rows")
                continue
            all_liquidations.extend(carried_rows)
            if chain_key not in stale_chains:
                stale_chains.append(chain_key)
            print(f"::warning::chain {chain_key} liquidation history served from previous snapshot ({len(carried_rows)} rows)")

    # Sort all positions by HF
    all_positions.sort(key=lambda x: x["healthFactor"] if x["healthFactor"] is not None else 999)
    all_liquidations.sort(key=lambda x: (x.get("timestamp") or 0, safe_float(x.get("serialId"), 0)), reverse=True)
    
    # Global stats
    total_critical = sum(s.get("critical", 0) for s in chain_stats.values())
    total_danger = sum(s.get("danger", 0) for s in chain_stats.values())
    total_warning = sum(s.get("warning", 0) for s in chain_stats.values())
    total_at_risk = total_critical + total_danger + total_warning
    
    output = {
        "generatedAt": int(time.time()),
        "generatedAtISO": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "thresholds": {
            "critical": HF_CRITICAL,
            "danger": HF_DANGER,
            "warning": HF_WARNING,
            "emodeCritical": HF_EMODE_CRITICAL,
            "emodeDanger": HF_EMODE_DANGER,
            "emodeWarning": HF_EMODE_WARNING,
        },
        "globalStats": {
            "totalPositions": len(all_positions),
            "atRisk": total_at_risk,
            "critical": total_critical,
            "danger": total_danger,
            "warning": total_warning,
            "totalCollateralUSD": round(sum(s.get("totalCollateralUSD", 0) for s in chain_stats.values()), 2),
            "totalDebtUSD": round(sum(s.get("totalDebtUSD", 0) for s in chain_stats.values()), 2),
        },
        "chainStats": chain_stats,
        "chainParams": chain_params,
        # Chains whose data was carried over from the previous snapshot
        # because their subgraph failed this run (see stale-chain fallback).
        "staleChains": stale_chains,
        "marketSupply": all_market_supply,
        "liquidationHistoryStats": build_liquidation_history_stats(all_liquidations),
        # Events moved to HISTORY_FILE (lazy-loaded); marker tells the UI where.
        "liquidationHistoryFile": HISTORY_FILE,
        "positions": all_positions,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    write_position_count_history(all_positions, output["generatedAt"])

    history_output = {
        "generatedAt": output["generatedAt"],
        "generatedAtISO": output["generatedAtISO"],
        "liquidationHistory": all_liquidations,
    }
    with open(HISTORY_FILE, "w") as f:
        json.dump(history_output, f, separators=(",", ":"))
    print(f"  💾 Liquidation history saved to {HISTORY_FILE}")

    print(f"\n{'='*60}")
    print(f"  ✅ Results saved to {OUTPUT_FILE}")
    print(f"  📊 Total positions: {len(all_positions)}")
    print(f"  ⚠️  At risk: {total_at_risk} (🔴{total_critical} 🟠{total_danger} 🟡{total_warning})")
    print(f"  🧾 Liquidations: {len(all_liquidations)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
