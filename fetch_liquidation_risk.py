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
        "subgraph_name": "dolomite-base",
        "label": "Base",
        "explorer": "https://basescan.org/address/",
        "rpc": "https://mainnet.base.org",
    },
    "ethereum": {
        "subgraph_name": "dolomite-ethereum",
        "label": "Ethereum",
        "explorer": "https://etherscan.io/address/",
        "rpc": "https://eth.llamarpc.com",
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

# Health factor thresholds
HF_CRITICAL = 1.05   # 🔴
HF_DANGER = 1.15     # 🟠
HF_WARNING = 1.30    # 🟡
HF_SAFE = 1.50       # 🟢 (anything above warning)

OUTPUT_FILE = "liquidation_risk.json"

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
        symbol = tv["token"]["symbol"]
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


def fetch_risk_overrides(rpc_url, setter_address, accounts, label=""):
    """
    Batch-query per-account E-Mode risk overrides via Multicall3.
    Bundles up to 200 getAccountRiskOverride calls per single RPC request.
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
    
    overrides = {}
    batch_size = 200  # calls per multicall request
    total = len(accounts)
    emode_count = 0
    
    print(f"  🔗 Fetching E-Mode overrides via Multicall3 ({total} accounts, {batch_size}/batch)...")
    
    for i in range(0, total, batch_size):
        batch = accounts[i:i + batch_size]
        
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
        try:
            results = multicall.functions.aggregate3(calls).call()
            
            for j, (success, return_data) in enumerate(results):
                if success and len(return_data) >= 64:
                    # Decode (uint256 marginRatioOverride, uint256 liquidationSpreadOverride)
                    mr_raw = int.from_bytes(return_data[:32], "big")
                    mr = mr_raw / 10**18
                    if mr > 0:
                        overrides[batch_meta[j]] = mr
                        emode_count += 1
        except Exception as e:
            # Fallback: try individual calls if multicall fails
            print(f"     ⚠️ Multicall3 failed, falling back to individual calls: {e}")
            for acct in batch:
                owner = acct["user"]["id"]
                account_num = int(acct["accountNumber"])
                account_id = acct["id"]
                try:
                    margin_override, _ = setter.functions.getAccountRiskOverride(
                        (Web3.to_checksum_address(owner), account_num)
                    ).call()
                    mr = margin_override[0] / 10**18
                    if mr > 0:
                        overrides[account_id] = mr
                        emode_count += 1
                except Exception:
                    pass
        
        progress = min(i + batch_size, total)
        print(f"     Processed {progress}/{total} accounts ({emode_count} E-Mode)...")
    
    print(f"     ✅ Found {emode_count} accounts with E-Mode overrides")
    return overrides


def classify_risk(hf):
    """Classify risk level based on health factor."""
    if hf is None:
        return "UNKNOWN"
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
            "symbol": info["token"]["symbol"],
            "isBorrowingDisabled": info.get("isBorrowingDisabled", False),
        }
    print(f"     Found {len(market_risk_infos)} markets with risk info")
    
    # Show stablecoin vs volatile premiums
    for tid, rinfo in sorted(market_risk_infos.items(), key=lambda x: float(x[1]["marginPremium"])):
        premium = float(rinfo["marginPremium"])
        symbol = rinfo["symbol"]
        label_type = "STABLE" if premium < 0.01 else "VOLATILE"
        print(f"       {symbol:>10s}: marginPremium={premium:.4f} ({label_type})")
    
    # 3. Fetch oracle prices
    print(f"  📊 Fetching oracle prices...")
    price_data = graphql_request(url, QUERY_ORACLE_PRICES)
    oracle_prices = {}
    for op in price_data.get("oraclePrices", []):
        token_id = op["token"]["id"]
        oracle_prices[token_id] = op["price"]
    print(f"     Found {len(oracle_prices)} oracle prices")
    
    # 4. Fetch interest indices
    print(f"  📊 Fetching interest indices...")
    index_data = graphql_request(url, QUERY_INTEREST_INDICES)
    interest_indices = {}
    for idx in index_data.get("interestIndexes", []):
        token_id = idx["token"]["id"]
        interest_indices[token_id] = {
            "borrowIndex": idx["borrowIndex"],
            "supplyIndex": idx["supplyIndex"],
        }
    print(f"     Found {len(interest_indices)} interest indices")
    
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
        risk_overrides = fetch_risk_overrides(rpc_url, default_setter, all_accounts, label)
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
        risk_level = classify_risk(hf)
        
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
        ("base", "Base", "https://basescan.org/address/",
         ["USDC", "WETH", "WBTC", "DAI", "DOLO"]),
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
            "base": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 8},
            "xlayer": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 6},
            "polygon_zkevm": {"liquidationRatio": "1.15", "liquidationReward": "0.05", "numberOfMarkets": 10},
        },
        "positions": all_positions,
    }
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    
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
    
    for chain_key, chain_config in CHAINS.items():
        try:
            result = fetch_chain_data(chain_key, chain_config)
            if result:
                all_positions.extend(result["positions"])
                chain_stats[chain_key] = result["stats"]
                chain_params[chain_key] = result["params"]
        except Exception as e:
            print(f"\n  ❌ Error fetching {chain_config['label']}: {e}")
            import traceback
            traceback.print_exc()
    
    # Sort all positions by HF
    all_positions.sort(key=lambda x: x["healthFactor"] if x["healthFactor"] is not None else 999)
    
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
        "positions": all_positions,
    }
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"  ✅ Results saved to {OUTPUT_FILE}")
    print(f"  📊 Total positions: {len(all_positions)}")
    print(f"  ⚠️  At risk: {total_at_risk} (🔴{total_critical} 🟠{total_danger} 🟡{total_warning})")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
