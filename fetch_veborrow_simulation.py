#!/usr/bin/env python3
"""
Build the ETH + Arbitrum veBorrow simulation data used by revenue-preview.html.

This is not a claim generator. It snapshots current borrower debt by wallet and
current veDOLO vote weight, so the static UI can model what users would save if
the Berachain-style veBorrow rebate rules were enabled on other networks.
"""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext

from rpc_client import RpcClient, RpcError, decode_uint256


getcontext().prec = 50

DOLOMITE_SUBGRAPH_BASE = "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs"
BORROW_FEE_REBATE_METADATA_URL = "https://api.dolomite.io/liquidity-mining/ve-dolo-rebate/metadata"
OFFICIAL_REBATE_SCRIPT_URL = "https://github.com/dolomite-exchange/liquidity-mining-bot/blob/master/scripts/calculate-borrow-rebate-per-network.ts"
OUTPUT_FILE = "veborrow_simulation.json"
BERACHAIN_VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
GET_VOTES_SELECTOR = "0x9ab24eb0"
PROTOCOL_RESERVE_FACTOR = Decimal("0.20")

DISPLAY_SIMULATION_CHAINS = ["Ethereum", "Arbitrum"]
ELIGIBILITY_CHAINS = ["Ethereum", "Arbitrum", "Berachain"]

CHAIN_CONFIGS = {
    "Ethereum": {
        "chainKey": "ethereum",
        "chainId": 1,
        "subgraphName": "dolomite-ethereum",
    },
    "Arbitrum": {
        "chainKey": "arbitrum",
        "chainId": 42161,
        "subgraphName": "dolomite-arbitrum",
    },
    "Berachain": {
        "chainKey": "berachain",
        "chainId": 80094,
        "subgraphUrl": "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
    },
}

QUERY_MARKET_DATA = """
{
  _meta {
    block {
      number
      timestamp
    }
  }
  oraclePrices(first: 1000) {
    price
    token {
      id
      symbol
      decimals
      marketId
    }
  }
  interestIndexes(first: 1000) {
    borrowIndex
    supplyIndex
    token {
      id
      symbol
      marketId
    }
  }
}
"""

QUERY_MARGIN_ACCOUNTS = """
query($skip: Int!, $first: Int!) {
  marginAccounts(
    first: $first,
    skip: $skip,
    orderBy: id,
    orderDirection: asc,
    where: { hasBorrowValue: true }
  ) {
    id
    user {
      id
    }
    effectiveUser {
      id
    }
    tokenValues {
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


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_address(value):
    return str(value or "").strip().lower()


def safe_decimal(value, default="0"):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def round_number(value, digits=6):
    return round(float(value), digits)


def subgraph_url(chain_config):
    if chain_config.get("subgraphUrl"):
        return chain_config["subgraphUrl"]
    return f"{DOLOMITE_SUBGRAPH_BASE}/{chain_config['subgraphName']}/latest/gn"


def http_json(url, payload=None, timeout=60, retries=3):
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "DolomiteDashboard/veborrow-simulation",
    }
    last_error = None
    for attempt in range(retries):
        try:
            request = urllib.request.Request(url, data=body, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"request failed for {url}: {last_error}")


def graphql_request(url, query, variables=None):
    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    result = http_json(url, payload=payload)
    if result.get("errors"):
        raise RuntimeError(f"GraphQL errors: {result['errors']}")
    return result.get("data") or {}


def load_dolo_price(path="dolo_price.json"):
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return safe_decimal(data.get("price")), data.get("source") or "dolo_price.json"


def load_vedolo_vote_weights(path="vedolo_holders.json", dolo_price_usd=None):
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    vote_weight_by_wallet = {}
    locked_dolo_by_wallet = {}
    for holder in data.get("holders") or []:
        address = normalize_address(holder.get("address"))
        if not address:
            continue
        vote_weight = safe_decimal(holder.get("total_vote_weight"))
        locked_dolo = safe_decimal(holder.get("total_dolo"))
        vote_weight_by_wallet[address] = vote_weight_by_wallet.get(address, Decimal("0")) + vote_weight
        locked_dolo_by_wallet[address] = locked_dolo_by_wallet.get(address, Decimal("0")) + locked_dolo

    total_vote_weight = sum(vote_weight_by_wallet.values(), Decimal("0"))
    total_value_usd = total_vote_weight * (dolo_price_usd or Decimal("0"))
    return {
        "voteWeightByWallet": vote_weight_by_wallet,
        "lockedDoloByWallet": locked_dolo_by_wallet,
        "holderCount": len(vote_weight_by_wallet),
        "totalVoteWeight": total_vote_weight,
        "totalVeDoloValueUSD": total_value_usd,
        "generatedAt": data.get("timestamp"),
    }


def encode_get_votes_call(address):
    normalized = normalize_address(address).replace("0x", "")
    if len(normalized) != 40:
        raise ValueError(f"Invalid address for getVotes: {address!r}")
    return GET_VOTES_SELECTOR + ("0" * 24) + normalized


def fetch_onchain_vedolo_vote_weights(addresses, batch_size=None):
    """Fetch current veDOLO vote weights from the Berachain veDOLO contract.

    Official closed epochs use getPastVotes at epoch end. This simulation uses
    getVotes because it intentionally models today's borrower footprint.
    """
    normalized_addresses = sorted({
        normalize_address(address)
        for address in addresses
        if normalize_address(address)
    })
    if not normalized_addresses:
        return {}, {
            "status": "skipped",
            "requestedWallets": 0,
            "fetchedWallets": 0,
            "failedBatches": 0,
            "source": "veDOLO.getVotes(address)",
            "contract": BERACHAIN_VEDOLO_CONTRACT,
        }

    batch_size = int(batch_size or os.environ.get("VEBORROW_VOTES_RPC_BATCH_SIZE", "50"))
    batch_size = max(1, min(batch_size, 200))
    chunk_retries = max(1, int(os.environ.get("VEBORROW_VOTES_CHUNK_RETRIES", "4")))
    retry_delay = max(0, int(os.environ.get("VEBORROW_VOTES_RETRY_DELAY_SECONDS", "12")))
    rpc = RpcClient(
        chain="berachain",
        timeout=int(os.environ.get("VEBORROW_VOTES_RPC_TIMEOUT_SECONDS", "20")),
        retries_per_endpoint=int(os.environ.get("VEBORROW_VOTES_RPC_RETRIES_PER_ENDPOINT", "2")),
        quiet=True,
    )
    vote_weights = {}
    failed_batches = 0
    errors = []
    for start in range(0, len(normalized_addresses), batch_size):
        chunk = normalized_addresses[start:start + batch_size]
        calls = [(BERACHAIN_VEDOLO_CONTRACT, encode_get_votes_call(address)) for address in chunk]
        results = None
        last_error = None
        for attempt in range(chunk_retries):
            try:
                results = rpc.eth_call_batch(calls)
                break
            except RpcError as exc:
                last_error = exc
                if getattr(rpc, "endpoints", None):
                    rpc._idx = (rpc._idx + 1) % len(rpc.endpoints)
                if attempt < chunk_retries - 1 and retry_delay:
                    time.sleep(retry_delay * (attempt + 1))
        if results is None:
            failed_batches += 1
            errors.append(str(last_error))
            continue
        for address, result in zip(chunk, results):
            vote_weights[address] = Decimal(decode_uint256(result)) / Decimal(10 ** 18)

    status = "ok" if len(vote_weights) == len(normalized_addresses) else "partial" if vote_weights else "error"
    return vote_weights, {
        "status": status,
        "requestedWallets": len(normalized_addresses),
        "fetchedWallets": len(vote_weights),
        "failedBatches": failed_batches,
        "source": "veDOLO.getVotes(address)",
        "contract": BERACHAIN_VEDOLO_CONTRACT,
        "batchSize": batch_size,
        "chunkRetries": chunk_retries,
        "errors": errors[:3],
    }


def merge_vedolo_vote_sources(snapshot, onchain_vote_weight_by_wallet=None, required_wallets=None, dolo_price_usd=None):
    onchain_vote_weight_by_wallet = onchain_vote_weight_by_wallet or {}
    required_wallets = required_wallets or []
    snapshot_vote_weights = {
        normalize_address(address): safe_decimal(value)
        for address, value in (snapshot.get("voteWeightByWallet") or {}).items()
        if normalize_address(address)
    }
    snapshot_locked = {
        normalize_address(address): safe_decimal(value)
        for address, value in (snapshot.get("lockedDoloByWallet") or {}).items()
        if normalize_address(address)
    }
    onchain_vote_weights = {
        normalize_address(address): safe_decimal(value)
        for address, value in onchain_vote_weight_by_wallet.items()
        if normalize_address(address)
    }

    wallets = set(snapshot_vote_weights) | set(onchain_vote_weights) | {
        normalize_address(address)
        for address in required_wallets
        if normalize_address(address)
    }

    vote_weight_by_wallet = {}
    source_by_wallet = {}
    source_counts = {
        "onchain_getVotes": 0,
        "holder_snapshot_fallback": 0,
        "none": 0,
    }
    for address in sorted(wallets):
        if address in onchain_vote_weights:
            vote_weight_by_wallet[address] = onchain_vote_weights[address]
            source_by_wallet[address] = "onchain_getVotes"
        elif address in snapshot_vote_weights:
            vote_weight_by_wallet[address] = snapshot_vote_weights[address]
            source_by_wallet[address] = "holder_snapshot_fallback"
        else:
            vote_weight_by_wallet[address] = Decimal("0")
            source_by_wallet[address] = "none"
        source_counts[source_by_wallet[address]] += 1

    total_vote_weight = sum(vote_weight_by_wallet.values(), Decimal("0"))
    price = safe_decimal(dolo_price_usd) if dolo_price_usd is not None else Decimal("0")
    return {
        **snapshot,
        "voteWeightByWallet": vote_weight_by_wallet,
        "lockedDoloByWallet": snapshot_locked,
        "voteWeightSourceByWallet": source_by_wallet,
        "sourceCounts": source_counts,
        "holderCount": sum(1 for value in vote_weight_by_wallet.values() if value > 0),
        "totalVoteWeight": total_vote_weight,
        "totalVeDoloValueUSD": total_vote_weight * price,
    }


def fetch_rebate_metadata():
    payload = http_json(BORROW_FEE_REBATE_METADATA_URL, timeout=30)
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else payload
    if not isinstance(metadata, dict):
        metadata = {}

    holding_factor = safe_decimal(metadata.get("veDoloHoldingFactor"), "5")
    all_chain_info = metadata.get("allChainRebateInfo") if isinstance(metadata.get("allChainRebateInfo"), dict) else {}
    rebate_percentage = Decimal("0.10")
    for chain_info in all_chain_info.values():
        if not isinstance(chain_info, dict):
            continue
        candidate = safe_decimal(chain_info.get("rebatePercentage"))
        if candidate > 0:
            rebate_percentage = candidate
            break

    return {
        "veDoloHoldingFactor": holding_factor,
        "rebatePercentage": rebate_percentage,
        "currentEpochIndex": metadata.get("currentEpochIndex"),
        "currentEpochStartTimestamp": metadata.get("currentEpochStartTimestamp"),
        "source": BORROW_FEE_REBATE_METADATA_URL,
    }


def debt_usd_for_token_value(token_value, oracle_prices, interest_indices):
    value_par = safe_decimal(token_value.get("valuePar"))
    if value_par >= 0:
        return Decimal("0")
    token_id = normalize_address((token_value.get("token") or {}).get("id"))
    if not token_id:
        return Decimal("0")
    price = oracle_prices.get(token_id, Decimal("0"))
    borrow_index = interest_indices.get(token_id, Decimal("1"))
    if price <= 0 or borrow_index <= 0:
        return Decimal("0")
    return abs(value_par) * borrow_index * price


def fetch_current_borrow_debt_for_chain(chain, chain_config, page_size=500):
    url = subgraph_url(chain_config)
    market_data = graphql_request(url, QUERY_MARKET_DATA)
    oracle_prices = {}
    interest_indices = {}
    for row in market_data.get("oraclePrices") or []:
        token_id = normalize_address((row.get("token") or {}).get("id"))
        if token_id:
            oracle_prices[token_id] = safe_decimal(row.get("price"))
    for row in market_data.get("interestIndexes") or []:
        token_id = normalize_address((row.get("token") or {}).get("id"))
        if token_id:
            interest_indices[token_id] = safe_decimal(row.get("borrowIndex"), "1")

    debt_by_user = {}
    account_count = 0
    skip = 0
    while True:
        data = graphql_request(
            url,
            QUERY_MARGIN_ACCOUNTS,
            variables={"skip": skip, "first": page_size},
        )
        accounts = data.get("marginAccounts") or []
        if not accounts:
            break
        account_count += len(accounts)
        for account in accounts:
            user = normalize_address((account.get("effectiveUser") or {}).get("id"))
            if not user:
                user = normalize_address((account.get("user") or {}).get("id"))
            if not user:
                continue
            debt = sum(
                debt_usd_for_token_value(token_value, oracle_prices, interest_indices)
                for token_value in account.get("tokenValues") or []
            )
            if debt > 0:
                debt_by_user[user] = debt_by_user.get(user, Decimal("0")) + debt
        if len(accounts) < page_size:
            break
        skip += page_size
        time.sleep(0.15)

    meta_block = ((market_data.get("_meta") or {}).get("block") or {})
    return {
        "chain": chain,
        "debtByUser": debt_by_user,
        "accountCount": account_count,
        "borrowerCount": len(debt_by_user),
        "currentDebtUSD": sum(debt_by_user.values(), Decimal("0")),
        "subgraphBlockNumber": meta_block.get("number"),
        "subgraphBlockTimestamp": meta_block.get("timestamp"),
        "subgraphUrl": url,
    }


def simulate_current_vedolo_rebates(
    current_debt_by_chain,
    selected_borrow_interest_by_chain,
    ve_dolo_vote_weight_by_wallet,
    dolo_price_usd,
    ve_dolo_holding_factor,
    rebate_percentages,
    ve_dolo_locked_by_wallet=None,
    display_chains=None,
    eligibility_chains=None,
    chain_revenue_factors=None,
):
    ve_dolo_locked_by_wallet = ve_dolo_locked_by_wallet or {}
    chain_revenue_factors = chain_revenue_factors or {}
    display_chains = list(display_chains or (current_debt_by_chain or {}).keys())
    eligibility_chains = list(eligibility_chains or display_chains)
    display_chain_set = set(display_chains)
    eligibility_chain_set = set(eligibility_chains)

    user_chain_debt = {}
    for chain, debt_by_user in (current_debt_by_chain or {}).items():
        if chain not in eligibility_chain_set:
            continue
        total_debt = sum(safe_decimal(value) for value in debt_by_user.values())
        borrow_interest = safe_decimal(selected_borrow_interest_by_chain.get(chain))
        if total_debt <= 0 or borrow_interest <= 0:
            continue
        for address, debt in debt_by_user.items():
            normalized = normalize_address(address)
            debt_value = safe_decimal(debt)
            if not normalized or debt_value <= 0:
                continue
            share = debt_value / total_debt
            item = user_chain_debt.setdefault(normalized, {})
            item[chain] = item.get(chain, Decimal("0")) + (borrow_interest * share)

    chain_totals = {}
    wallet_rows = []
    total_current_saved = Decimal("0")
    total_max_saved = Decimal("0")
    total_eligibility_max_saved = Decimal("0")
    total_required_vedolo = Decimal("0")
    total_display_borrow_interest = Decimal("0")

    for address, interest_by_chain in user_chain_debt.items():
        max_by_chain = {}
        eligibility_max_by_chain = {}
        display_max_rebate = Decimal("0")
        eligibility_max_rebate = Decimal("0")
        display_borrow_interest_total = Decimal("0")
        eligibility_borrow_interest_total = Decimal("0")
        for chain, interest in interest_by_chain.items():
            rebate_percentage = safe_decimal(rebate_percentages.get(chain), "0.10")
            revenue_factor = min(Decimal("1"), max(Decimal("0"), safe_decimal(chain_revenue_factors.get(chain), "1")))
            chain_eligibility_max = interest * rebate_percentage
            chain_max = chain_eligibility_max * revenue_factor
            eligibility_max_by_chain[chain] = chain_eligibility_max
            max_by_chain[chain] = chain_max
            eligibility_max_rebate += chain_eligibility_max
            eligibility_borrow_interest_total += interest
            if chain in display_chain_set:
                display_max_rebate += chain_max
                display_borrow_interest_total += interest

        if display_max_rebate <= 0:
            continue

        required_ve_dolo_value = eligibility_max_rebate * safe_decimal(ve_dolo_holding_factor, "5")
        required_ve_dolo = (
            required_ve_dolo_value / safe_decimal(dolo_price_usd)
            if safe_decimal(dolo_price_usd) > 0
            else Decimal("0")
        )
        vote_weight = safe_decimal(ve_dolo_vote_weight_by_wallet.get(address))
        ve_dolo_value = vote_weight * safe_decimal(dolo_price_usd)
        eligibility = Decimal("0")
        if required_ve_dolo_value > 0:
            eligibility = min(Decimal("1"), ve_dolo_value / required_ve_dolo_value)
        current_saved = display_max_rebate * eligibility

        chains_payload = {}
        for chain, chain_max in max_by_chain.items():
            if chain not in display_chain_set:
                continue
            chain_saved = chain_max * eligibility
            chain_item = chain_totals.setdefault(chain, {
                "borrowInterestUSD": Decimal("0"),
                "maxUsersSavedUSD": Decimal("0"),
                "eligibilityMaxUsersSavedUSD": Decimal("0"),
                "currentVeDoloSavedUSD": Decimal("0"),
                "currentDebtUSD": sum(safe_decimal(value) for value in (current_debt_by_chain.get(chain) or {}).values()),
                "borrowerCount": len(current_debt_by_chain.get(chain) or {}),
                "borrowersWithVeDoloCount": 0,
            })
            chain_item["borrowInterestUSD"] += safe_decimal(interest_by_chain.get(chain))
            chain_item["maxUsersSavedUSD"] += chain_max
            chain_item["eligibilityMaxUsersSavedUSD"] += eligibility_max_by_chain.get(chain, Decimal("0"))
            chain_item["currentVeDoloSavedUSD"] += chain_saved
            if vote_weight > 0:
                chain_item["borrowersWithVeDoloCount"] += 1
            chains_payload[chain] = {
                "borrowInterestUSD": round_number(interest_by_chain[chain]),
                "maxUsersSavedUSD": round_number(chain_max),
                "eligibilityMaxUsersSavedUSD": round_number(eligibility_max_by_chain.get(chain, Decimal("0"))),
                "currentVeDoloSavedUSD": round_number(chain_saved),
            }

        total_current_saved += current_saved
        total_max_saved += display_max_rebate
        total_eligibility_max_saved += eligibility_max_rebate
        total_required_vedolo += required_ve_dolo
        total_display_borrow_interest += display_borrow_interest_total
        wallet_rows.append({
            "address": address,
            "borrowInterestUSD": round_number(display_borrow_interest_total),
            "eligibilityBorrowInterestUSD": round_number(eligibility_borrow_interest_total),
            "maxUsersSavedUSD": round_number(display_max_rebate),
            "eligibilityMaxUsersSavedUSD": round_number(eligibility_max_rebate),
            "currentVeDoloSavedUSD": round_number(current_saved),
            "eligibilityRatio": round_number(eligibility, 8),
            "veDoloVoteWeight": round_number(vote_weight),
            "veDoloLockedDolo": round_number(safe_decimal(ve_dolo_locked_by_wallet.get(address))),
            "veDoloValueUSD": round_number(ve_dolo_value),
            "requiredVeDoloForMax": round_number(required_ve_dolo),
            "missingVeDoloForMax": round_number(max(Decimal("0"), required_ve_dolo - vote_weight)),
            "chains": chains_payload,
        })

    wallet_rows.sort(key=lambda row: row["currentVeDoloSavedUSD"], reverse=True)
    chain_output = {}
    for chain, values in chain_totals.items():
        max_saved = values["maxUsersSavedUSD"]
        current_saved = values["currentVeDoloSavedUSD"]
        chain_output[chain] = {
            "borrowInterestUSD": round_number(values["borrowInterestUSD"]),
            "maxUsersSavedUSD": round_number(max_saved),
            "eligibilityMaxUsersSavedUSD": round_number(values["eligibilityMaxUsersSavedUSD"]),
            "currentVeDoloSavedUSD": round_number(current_saved),
            "coverageRatio": round_number(current_saved / max_saved if max_saved > 0 else Decimal("0"), 8),
            "currentDebtUSD": round_number(values["currentDebtUSD"]),
            "borrowerCount": int(values["borrowerCount"]),
            "borrowersWithVeDoloCount": int(values["borrowersWithVeDoloCount"]),
        }

    return {
        "summary": {
            "borrowInterestUSD": round_number(total_display_borrow_interest),
            "maxUsersSavedUSD": round_number(total_max_saved),
            "eligibilityMaxUsersSavedUSD": round_number(total_eligibility_max_saved),
            "currentVeDoloSavedUSD": round_number(total_current_saved),
            "coverageRatio": round_number(total_current_saved / total_max_saved if total_max_saved > 0 else Decimal("0"), 8),
            "requiredVeDoloForMax": round_number(total_required_vedolo),
            "walletCount": len(wallet_rows),
            "walletsWithCurrentSavings": sum(1 for row in wallet_rows if row["currentVeDoloSavedUSD"] > 0),
        },
        "chains": chain_output,
        "wallets": wallet_rows,
    }


def build_snapshot():
    generated_at = utc_now_iso()
    dolo_price_usd, dolo_price_source = load_dolo_price()
    rebate_metadata = fetch_rebate_metadata()
    vedolo_snapshot = load_vedolo_vote_weights(dolo_price_usd=dolo_price_usd)

    chains = {}
    current_debt_by_chain = {}
    errors = {}
    for chain, chain_config in CHAIN_CONFIGS.items():
        try:
            result = fetch_current_borrow_debt_for_chain(chain, chain_config)
            current_debt_by_chain[chain] = result["debtByUser"]
            chains[chain] = {
                "chainKey": chain_config["chainKey"],
                "chainId": chain_config["chainId"],
                "status": "ok",
                "borrowerCount": result["borrowerCount"],
                "accountCount": result["accountCount"],
                "currentDebtUSD": round_number(result["currentDebtUSD"]),
                "subgraphBlockNumber": result["subgraphBlockNumber"],
                "subgraphBlockTimestamp": result["subgraphBlockTimestamp"],
                "subgraphUrl": result["subgraphUrl"],
            }
        except RuntimeError as exc:
            errors[chain] = str(exc)
            current_debt_by_chain[chain] = {}
            chains[chain] = {
                "chainKey": chain_config["chainKey"],
                "chainId": chain_config["chainId"],
                "status": "error",
                "error": str(exc),
                "borrowerCount": 0,
                "accountCount": 0,
                "currentDebtUSD": 0,
            }

    borrower_addresses = sorted({
        address
        for debt_by_user in current_debt_by_chain.values()
        for address in debt_by_user
    })
    try:
        onchain_vote_weights, onchain_vote_metadata = fetch_onchain_vedolo_vote_weights(borrower_addresses)
    except Exception as exc:
        onchain_vote_weights = {}
        onchain_vote_metadata = {
            "status": "error",
            "requestedWallets": len(borrower_addresses),
            "fetchedWallets": 0,
            "failedBatches": None,
            "source": "veDOLO.getVotes(address)",
            "contract": BERACHAIN_VEDOLO_CONTRACT,
            "errors": [str(exc)],
        }

    vedolo = merge_vedolo_vote_sources(
        vedolo_snapshot,
        onchain_vote_weights,
        required_wallets=borrower_addresses,
        dolo_price_usd=dolo_price_usd,
    )

    borrowers = {}
    total_current_debt = Decimal("0")
    for chain, debt_by_user in current_debt_by_chain.items():
        for address, debt in debt_by_user.items():
            item = borrowers.setdefault(address, {
                "address": address,
                "chains": {},
                "veDoloVoteWeight": vedolo["voteWeightByWallet"].get(address, Decimal("0")),
                "veDoloLockedDolo": vedolo["lockedDoloByWallet"].get(address, Decimal("0")),
                "veDoloVoteSource": vedolo["voteWeightSourceByWallet"].get(address, "none"),
            })
            item["chains"][chain] = {"currentDebtUSD": debt}
            total_current_debt += debt

    borrower_rows = []
    for row in borrowers.values():
        vote_weight = safe_decimal(row["veDoloVoteWeight"])
        borrower_rows.append({
            "address": row["address"],
            "chains": {
                chain: {"currentDebtUSD": round_number(values["currentDebtUSD"])}
                for chain, values in row["chains"].items()
            },
            "currentDebtUSD": round_number(sum(safe_decimal(values["currentDebtUSD"]) for values in row["chains"].values())),
            "veDoloVoteWeight": round_number(vote_weight),
            "veDoloLockedDolo": round_number(row["veDoloLockedDolo"]),
            "veDoloValueUSD": round_number(vote_weight * dolo_price_usd),
            "veDoloVoteSource": row["veDoloVoteSource"],
        })
    borrower_rows.sort(key=lambda row: row["currentDebtUSD"], reverse=True)
    borrower_vote_source_counts = {
        "onchain_getVotes": 0,
        "holder_snapshot_fallback": 0,
        "none": 0,
    }
    for row in borrower_rows:
        source = row.get("veDoloVoteSource") or "none"
        borrower_vote_source_counts[source] = borrower_vote_source_counts.get(source, 0) + 1

    status = "ok" if not errors else "partial" if any(chain["status"] == "ok" for chain in chains.values()) else "error"
    return {
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "status": status,
        "methodology": {
            "summary": "Current borrower debt by wallet is read from Dolomite subgraphs. The revenue UI allocates a selected ETH/ARB borrow-interest period by current debt share, then applies the official veBorrow rebate formula against current veDOLO vote weight. Active Berachain borrow load is included in the wallet eligibility threshold.",
            "officialFormulaSource": OFFICIAL_REBATE_SCRIPT_URL,
            "limitations": [
                "Ethereum and Arbitrum are simulation-only until Dolomite enables veBorrow rebates on those networks.",
                "Borrower debt is a current snapshot, so historical selected ranges are estimated by current debt share, not historical per-wallet borrow ledgers.",
                "Current veDOLO vote weight is fetched from Berachain veDOLO.getVotes(address) when RPC succeeds, with vedolo_holders.json used only as a fallback.",
                "Official closed epochs use historical per-wallet borrow ledgers and getPastVotes at epoch end, so this file is a current-footprint simulation, not a claim generator.",
            ],
        },
        "assurance": {
            "calculationClass": "current_snapshot_simulation",
            "displaySimulationChains": DISPLAY_SIMULATION_CHAINS,
            "eligibilityChains": ELIGIBILITY_CHAINS,
            "activeEligibilityChain": "Berachain",
            "veDoloVoteSource": "onchain_getVotes_with_holder_snapshot_fallback",
            "onchainVoteStatus": onchain_vote_metadata.get("status"),
            "onchainVoteRequestedWallets": onchain_vote_metadata.get("requestedWallets"),
            "onchainVoteFetchedWallets": onchain_vote_metadata.get("fetchedWallets"),
            "confidence": "solid for current-footprint simulation when all display and eligibility subgraphs are fresh and onchainVoteStatus is ok; not equivalent to official historical claim output.",
        },
        "sourceUrls": {
            "rebateMetadata": BORROW_FEE_REBATE_METADATA_URL,
            "officialFormula": OFFICIAL_REBATE_SCRIPT_URL,
            "veDoloContract": BERACHAIN_VEDOLO_CONTRACT,
            "subgraphs": {chain: subgraph_url(config) for chain, config in CHAIN_CONFIGS.items()},
        },
        "config": {
            "simulationChains": DISPLAY_SIMULATION_CHAINS,
            "displaySimulationChains": DISPLAY_SIMULATION_CHAINS,
            "eligibilityChains": ELIGIBILITY_CHAINS,
            "activeRebateChains": ["Berachain"],
            "rebatePercentage": round_number(rebate_metadata["rebatePercentage"], 8),
            "veDoloHoldingFactor": round_number(rebate_metadata["veDoloHoldingFactor"], 8),
            "protocolReserveFactor": round_number(PROTOCOL_RESERVE_FACTOR, 8),
            "doloPriceUSD": round_number(dolo_price_usd, 10),
            "doloPriceSource": dolo_price_source,
            "currentEpochIndex": rebate_metadata.get("currentEpochIndex"),
            "currentEpochStartTimestamp": rebate_metadata.get("currentEpochStartTimestamp"),
        },
        "chains": chains,
        "totals": {
            "borrowerCount": len(borrower_rows),
            "borrowersWithVeDoloCount": sum(1 for row in borrower_rows if row["veDoloVoteWeight"] > 0),
            "currentDebtUSD": round_number(total_current_debt),
            "veDoloHolderCount": vedolo["holderCount"],
            "veDoloVoteWeight": round_number(vedolo["totalVoteWeight"]),
            "veDoloValueUSD": round_number(vedolo["totalVeDoloValueUSD"]),
            "veDoloVoteSourceCounts": borrower_vote_source_counts,
            "onchainVoteFetch": onchain_vote_metadata,
        },
        "borrowers": borrower_rows,
        "errors": errors,
    }


def main():
    output = build_snapshot()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2)
        handle.write("\n")
    print(f"Wrote {OUTPUT_FILE}: status={output['status']} borrowers={output['totals']['borrowerCount']}")


if __name__ == "__main__":
    main()
