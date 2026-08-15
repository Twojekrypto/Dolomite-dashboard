#!/usr/bin/env python3
"""
oDOLO Token Flows — Top Accumulators & Sellers (1d / 7d / 30d)
Berachain only. Fetches ERC-20 Transfer events via eth_getLogs,
calculates net inflow/outflow per address, outputs top 5 each.
"""
import json, time, os, sys, re
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime


# ===== CONFIG =====
ODOLO_CONTRACT = "0x02E513b5B54eE216Bf836ceb471507488fC89543".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
TOP_N = 100
# Future rewards / distribution wallet. It is the claim source, not a user flow row.
REWARDS_CONTRACT = "0x79e6e932bf6686a4d357d7821e6e08835ba8a026"

# Known contract addresses to exclude
EXCLUDED_ADDRS = {
    ZERO,
    ODOLO_CONTRACT,
    REWARDS_CONTRACT,
    "0x0000000000000000000000000000000000000001",
    # oDOLO Vester
    "0x3e9b9a16743551da49b5e136c716bba7932d2cec",
    # Kodiak contract
    "0x43dac637c4383f91b4368041e7a8687da3806cae",
    # Contract
    "0x63242a4ea82847b20e506b63b0e2e2eff0cc6cb0",
    # Protocol Fly
    "0x596384bdffc9f563b53791aeec50a42ff51c3e42",
    # Contract
    "0x27f66ba3fda600239f48526bb26a1f8d5700ccf7",
    # DEX swap router
    "0xbedfac7488dccaafdd66d1d7d56349780fe0477e",
    # Contract (proxy)
    "0xf909c4ae16622898b885b89d7f839e0244851c66",
    # Contract
    "0xa575f37e869e6887564f87c07e2885e08d542c4a",
    # --- Contracts discovered during top-50 verification (2026-03-06) ---
    # LP/router contract (921 txs)
    "0x7ab286e9da6b5a1c80664b382092a8a4b91c276c",
    # Router/aggregator (393 txs)
    "0x12622dae56ec7a25f6cfeb96db88651c5bf7861d",
    # DEX/aggregator (65 txs)
    "0x089b95152253b6af73e7f7267d749058d56ce231",
    # LP/swap contract (254 txs)
    "0x16f13296c85c308b37bae567284e62b4c21a1ee9",
    # DEX contract (201 txs)
    "0x8430e3574eeb85b39b053b4022cfa27f951f48c7",
    # Router (313 txs)
    "0x8c7ba8f245aef3216698087461e05b85483f791f",
    # Bot/aggregator (1602 txs)
    "0x893785e5c2a4ccfe0790e580c8e4ef363fabde1e",
    # Protocol contract (135 txs)
    "0x4fe93ebc4ce6ae4f81601cc7ce7139023919e003",
    # LP contract (116 txs)
    "0xf5042e6ffac5a625d4e7848e0b01373d8eb9e222",
    # Bot/aggregator (2899 txs)
    "0x4be03f781c497a489e3cb0287833452ca9b9e80b",
    # Router (4403 txs)
    "0x221dd2bb8b25f5e46b00c174b0111d383eb5c0bc",
    # Bot/aggregator (4692 txs)
    "0x71355972c9e332f73ff6921f9b3a02f349ff9752",
    # Contract (576 txs)
    "0x08b14bb09ac4819c16f68d7c92f7dcc20750eaff",
    # DEX contract (249 txs)
    "0x062a2b0eea575f659a1aaf18c1df5d93e0528245",
    # LP contract (44 txs)
    "0x36f4e1803f6ff34562db567f347dea00dec87246",
    # Contract (92 txs)
    "0x74d09665900a5f29bac25befd30c73a5962d44e7",
}

# Flow tables exclude protocol infrastructure, routers, and other contracts.
# Official RewardClaimed recipients must still count toward the claimed
# allocation even when the recipient is one of those contracts.
CLAIM_SKIP_ADDRS = {
    ZERO,
    ODOLO_CONTRACT,
    REWARDS_CONTRACT,
    "0x0000000000000000000000000000000000000001",
}

# Single source of truth for endpoints (env-injected Alchemy keys first).
from rpc_client import (
    RpcError,
    get_endpoints as _rpc_endpoints,
    rpc_batch_requests,
    rpc_single_request,
    safe_host as _rpc_safe_host,
    sanitize_error as _rpc_sanitize_error,
)

import rpc_usage
from flow_tx_metadata import (
    attach_latest_flow_metadata,
    fetch_token_block_evidence,
)

RPC_URLS = _rpc_endpoints("berachain")

BLOCK_TIME = 2  # ~2 seconds per block on Berachain
CHUNK_SIZE = max(1000, int(os.environ.get("ODOLO_FLOW_CHUNK_SIZE", "10000")))
RECENT_RESCAN_BLOCKS = max(1, int(os.environ.get("ODOLO_FLOW_RECENT_RESCAN_BLOCKS", "10000")))
REORG_BUFFER_BLOCKS = max(0, int(os.environ.get("ODOLO_FLOW_REORG_BUFFER_BLOCKS", "20")))
CLAIM_COVERAGE_MAX_LAG_BLOCKS = max(
    0,
    int(os.environ.get("ODOLO_CLAIM_COVERAGE_MAX_LAG_BLOCKS", "300")),
)
FLOW_STATE_SCHEMA_VERSION = 2
DEPLOY_BLOCK = 3_500_000  # oDOLO deployed on Berachain mainnet

PERIODS = {
    "1d": 86400,
    "7d": 86400 * 7,
    "30d": 86400 * 30,
    "90d": 86400 * 90,
    "180d": 86400 * 180,
    "all": None,
}

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "odolo_flows.json")
STATE_FILE = os.path.join(DATA_DIR, "odolo_flows_state.json")
ODOLO_CLAIM_EVENTS_JSON = os.path.join(DATA_DIR, "data", "odolo-claim-events.json")
REWARD_CLAIM_EVENTS_BERA_JSON = os.path.join(DATA_DIR, "data", "reward-claim-events", "berachain.json")
EXERCISERS_BY_ADDRESS_JSON = os.path.join(DATA_DIR, "exercisers_by_address.json")
VEDOLO_FLOWS_JSON = os.path.join(DATA_DIR, "vedolo_flows.json")
RPC_BATCH_SIZE = int(os.environ.get("ODOLO_FLOW_RPC_BATCH_SIZE", "50"))
RPC_RETRIES_PER_ENDPOINT = int(os.environ.get("ODOLO_FLOW_RPC_RETRIES_PER_ENDPOINT", "2"))
# Multicall3 (same address on every EVM chain) batches many balanceOf reads into
# ONE eth_call. JSON-RPC batching does NOT cut compute units; Multicall3 does.
MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_AGG3_ABI = [{
    "inputs": [{"components": [
        {"name": "target", "type": "address"},
        {"name": "allowFailure", "type": "bool"},
        {"name": "callData", "type": "bytes"},
    ], "name": "calls", "type": "tuple[]"}],
    "name": "aggregate3",
    "outputs": [{"components": [
        {"name": "success", "type": "bool"},
        {"name": "returnData", "type": "bytes"},
    ], "name": "returnData", "type": "tuple[]"}],
    "stateMutability": "payable", "type": "function",
}]

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def normalize_address(value):
    address = str(value or "").strip().lower()
    if ADDRESS_RE.match(address):
        return address
    return None


def amount_to_float(value, decimals=18):
    try:
        return float(Decimal(str(value)) / (Decimal(10) ** decimals))
    except (InvalidOperation, ValueError, TypeError):
        return 0.0


def read_json_file(path, default=None):
    if default is None:
        default = {}
    if not path or not os.path.exists(path):
        return default
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as exc:
        print(f"⚠️ Could not read {os.path.basename(path)} for reconciliation: {exc}", flush=True)
        return default


def add_candidate(candidates, address, source):
    address = normalize_address(address)
    if not address or address in EXCLUDED_ADDRS:
        return
    candidates.setdefault(address, set()).add(source)


def collect_addresses_from_value(value, candidates, source):
    """Recursively collect address-looking fields from a local snapshot."""
    if isinstance(value, dict):
        for nested in value.values():
            collect_addresses_from_value(nested, candidates, source)
    elif isinstance(value, list):
        for item in value:
            collect_addresses_from_value(item, candidates, source)
    elif isinstance(value, str):
        add_candidate(candidates, value, source)


def load_existing_flow_candidates(path=OUTPUT_JSON):
    candidates = {}
    payload = read_json_file(path)
    if payload:
        collect_addresses_from_value(payload.get("periods", {}), candidates, "previous_odolo_flows")
        collect_addresses_from_value(payload.get("claimer_behavior", {}), candidates, "previous_odolo_claimers")
        collect_addresses_from_value(payload.get("claimer_periods", {}), candidates, "previous_odolo_claimers")
    return candidates


def load_exerciser_candidates(path=EXERCISERS_BY_ADDRESS_JSON):
    candidates = {}
    payload = read_json_file(path)
    for entry in payload.get("exercisers", []) or []:
        add_candidate(candidates, entry.get("address"), "odolo_exercisers")
    return candidates


def load_vedolo_odolo_route_candidates(path=VEDOLO_FLOWS_JSON):
    candidates = {}
    payload = read_json_file(path)
    for lock in payload.get("locks", []) or []:
        if not lock.get("isOdolo"):
            continue
        add_candidate(candidates, lock.get("address"), "vedolo_odolo_route")
        add_candidate(candidates, lock.get("beneficiaryAddress"), "vedolo_odolo_route")
    return candidates


def load_reward_claims(path, min_block=None):
    """Load oDOLO RewardClaimed amounts by wallet from a generated event index."""
    payload = read_json_file(path)
    claims = {}
    events = payload.get("events", []) if isinstance(payload, dict) else []
    for event in events or []:
        block_number = int(event.get("blockNumber") or 0)
        if min_block is not None and block_number < min_block:
            continue
        distributor = normalize_address(event.get("distributor") or payload.get("distributor"))
        token = normalize_address(event.get("tokenAddress"))
        if distributor != REWARDS_CONTRACT or token != ODOLO_CONTRACT:
            continue
        wallet = normalize_address(event.get("user"))
        if not wallet or wallet in CLAIM_SKIP_ADDRS:
            continue
        if event.get("amountWei") is not None:
            amount = amount_to_float(event.get("amountWei"))
        else:
            try:
                amount = float(Decimal(str(event.get("amount") or "0")))
            except (InvalidOperation, ValueError, TypeError):
                amount = 0.0
        if amount > 0:
            claims[wallet] = claims.get(wallet, 0) + amount
    return claims


def merge_claim_sources(primary, secondary):
    """Use event-indexed claim totals as a self-healing supplement.

    When both sources see the same wallet, keep the larger total instead of
    summing; ERC20 Transfer logs and RewardClaimed logs can describe the same
    claim from different angles.
    """
    merged = dict(primary)
    added = 0
    updated = 0
    for wallet, amount in secondary.items():
        current = merged.get(wallet, 0)
        if amount > current:
            merged[wallet] = amount
            if current > 0:
                updated += 1
            else:
                added += 1
    return merged, {"added": added, "updated": updated, "source_wallets": len(secondary)}


def load_reward_claims_from_sources(paths, min_block=None, max_block=None):
    """Deduplicate canonical events inside the confirmed snapshot, then aggregate."""
    events_by_id = {}
    for path in paths:
        payload = read_json_file(path)
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events or []:
            distributor = normalize_address(
                event.get("distributor") or payload.get("distributor")
            )
            token = normalize_address(event.get("tokenAddress"))
            if distributor != REWARDS_CONTRACT or token != ODOLO_CONTRACT:
                continue
            wallet = normalize_address(event.get("user"))
            try:
                block_number = int(event.get("blockNumber") or 0)
                log_index = int(event.get("logIndex"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Canonical oDOLO claim event lacks block/log identity") from exc
            tx_hash = str(event.get("txHash") or "").strip().lower()
            if not wallet or wallet in CLAIM_SKIP_ADDRS or not re.fullmatch(r"0x[a-f0-9]{64}", tx_hash):
                raise RuntimeError("Canonical oDOLO claim event has invalid wallet or transaction hash")
            if min_block is not None and block_number < min_block:
                continue
            if max_block is not None and block_number > max_block:
                continue
            if event.get("amountWei") is not None:
                try:
                    amount_wei = int(str(event.get("amountWei")))
                except (TypeError, ValueError) as exc:
                    raise RuntimeError("Canonical oDOLO claim event has invalid amountWei") from exc
            else:
                try:
                    amount_wei = int(Decimal(str(event.get("amount") or "0")) * (10 ** 18))
                except (InvalidOperation, ValueError, TypeError) as exc:
                    raise RuntimeError("Canonical oDOLO claim event has invalid amount") from exc
            if block_number <= 0 or log_index < 0 or amount_wei <= 0:
                raise RuntimeError("Canonical oDOLO claim event has non-positive evidence")
            event_id = (tx_hash, log_index)
            signature = (wallet, block_number, amount_wei)
            previous = events_by_id.get(event_id)
            if previous is not None and previous != signature:
                raise RuntimeError("Conflicting duplicate canonical oDOLO claim event")
            events_by_id[event_id] = signature

    claims = {}
    for wallet, _, amount_wei in events_by_id.values():
        claims[wallet] = claims.get(wallet, 0) + amount_wei / (10 ** 18)
    return claims


def load_canonical_claim_coverage(paths):
    """Load continuous Berachain RewardClaimed scan coverage metadata."""
    complete = []
    for path in paths:
        payload = read_json_file(path)
        chain = (payload.get("chains") or {}).get("berachain") if isinstance(payload, dict) else None
        if not isinstance(chain, dict):
            continue
        try:
            from_block = int(chain.get("fromBlock") or 0)
            to_block = int(chain.get("toBlock") or 0)
        except (TypeError, ValueError):
            continue
        status = str(chain.get("coverageStatus") or "").lower()
        if status == "complete" and from_block <= DEPLOY_BLOCK and to_block >= DEPLOY_BLOCK:
            complete.append((to_block, from_block))
    if not complete:
        raise RuntimeError("Canonical oDOLO RewardClaimed index lacks complete block coverage")
    to_block, from_block = max(complete)
    return {
        "status": "complete",
        "from_block": from_block,
        "to_block": to_block,
    }


def select_flow_snapshot_block(chain_head, claim_coverage_to_block):
    """Choose a confirmed snapshot fully covered by the canonical claim index."""
    chain_head = int(chain_head)
    claim_coverage_to_block = int(claim_coverage_to_block)
    confirmed_head = chain_head - REORG_BUFFER_BLOCKS
    if confirmed_head < DEPLOY_BLOCK or claim_coverage_to_block < DEPLOY_BLOCK:
        raise RuntimeError("Invalid oDOLO flow or claim coverage block")
    coverage_lag = max(0, confirmed_head - claim_coverage_to_block)
    if coverage_lag > CLAIM_COVERAGE_MAX_LAG_BLOCKS:
        raise RuntimeError(
            f"Canonical oDOLO claim index is stale by {coverage_lag} blocks"
        )
    return min(confirmed_head, claim_coverage_to_block)


def load_first_canonical_claim_block(paths):
    """Return the first exact oDOLO RewardClaimed block across indexes."""
    first_block = None
    for path in paths:
        payload = read_json_file(path)
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events or []:
            distributor = normalize_address(
                event.get("distributor") or payload.get("distributor")
            )
            token = normalize_address(event.get("tokenAddress"))
            if distributor != REWARDS_CONTRACT or token != ODOLO_CONTRACT:
                continue
            try:
                block_number = int(event.get("blockNumber"))
            except (TypeError, ValueError):
                continue
            if block_number <= 0:
                continue
            first_block = block_number if first_block is None else min(first_block, block_number)
    if first_block is None:
        raise RuntimeError("No canonical oDOLO RewardClaimed coverage found")
    return first_block


def build_canonical_claim_totals(
    transfers,
    event_claims,
    first_canonical_event_block,
):
    """Combine pre-index reward transfers with authoritative indexed claims."""
    try:
        first_canonical_event_block = int(first_canonical_event_block)
    except (TypeError, ValueError) as exc:
        raise ValueError("first canonical claim block must be an integer") from exc
    if first_canonical_event_block <= 0:
        raise ValueError("first canonical claim block must be positive")

    historical_claims = {}
    post_index_transfers = {}
    for from_addr, to_addr, value_wei, block_number in transfers:
        from_addr = normalize_address(from_addr)
        to_addr = normalize_address(to_addr)
        if (
            from_addr != REWARDS_CONTRACT
            or not to_addr
            or to_addr in CLAIM_SKIP_ADDRS
        ):
            continue
        amount = value_wei / (10 ** 18)
        target = (
            historical_claims
            if int(block_number) < first_canonical_event_block
            else post_index_transfers
        )
        target[to_addr] = target.get(to_addr, 0) + amount

    claims = dict(historical_claims)
    normalized_event_claims = {}
    for wallet, amount in (event_claims or {}).items():
        wallet = normalize_address(wallet)
        if not wallet or wallet in CLAIM_SKIP_ADDRS:
            continue
        amount = float(amount)
        if amount <= 0:
            continue
        normalized_event_claims[wallet] = normalized_event_claims.get(wallet, 0) + amount
        claims[wallet] = claims.get(wallet, 0) + amount

    excess_by_wallet = {
        wallet: max(0.0, transfer_amount - normalized_event_claims.get(wallet, 0.0))
        for wallet, transfer_amount in post_index_transfers.items()
    }
    excess_by_wallet = {
        wallet: amount for wallet, amount in excess_by_wallet.items() if amount > 1e-9
    }
    stats = {
        "first_canonical_event_block": first_canonical_event_block,
        "historical_transfer_wallets": len(historical_claims),
        "canonical_event_wallets": len(normalized_event_claims),
        "canonical_event_added": len(
            set(normalized_event_claims) - set(historical_claims)
        ),
        "canonical_event_updated": len(
            set(normalized_event_claims) & set(historical_claims)
        ),
        "historical_transfer_claimed": round(sum(historical_claims.values()), 8),
        "canonical_event_claimed": round(sum(normalized_event_claims.values()), 8),
        "post_index_transfer_observed": round(sum(post_index_transfers.values()), 8),
        "ignored_post_index_transfer_count": len(excess_by_wallet),
        "ignored_post_index_transfer_amount": round(sum(excess_by_wallet.values()), 8),
        "methodology": (
            "Reward-wallet transfers before canonical RewardClaimed coverage plus exact "
            "canonical RewardClaimed events from the first indexed block onward"
        ),
    }
    return claims, stats


def summarize_claimer_rows(rows):
    """Return display aggregates derived from the exact published rows."""
    total_keys = {
        "claimed": "total_claimed",
        "exercised": "total_exercised",
        "outflow": "total_outflow",
        "held": "total_held",
        "claim_remaining": "total_claim_remaining",
    }
    totals = {
        output_key: sum(Decimal(str(row.get(row_key) or 0)) for row in rows)
        for row_key, output_key in total_keys.items()
    }
    claimed = totals["total_claimed"]
    count = len(rows)
    bought_extra_count = sum(
        1 for row in rows if Decimal(str(row.get("bought_extra") or 0)) > 0
    )

    def display_total(value):
        return float(value.quantize(Decimal("0.01")))

    def display_pct(value, denominator):
        if denominator <= 0:
            return 0.0
        return round(float(value / denominator * Decimal(100)), 1)

    return {
        "total_claimers": count,
        **{key: display_total(value) for key, value in totals.items()},
        "pct_exercised": display_pct(totals["total_exercised"], claimed),
        "pct_outflow": display_pct(totals["total_outflow"], claimed),
        "pct_held": display_pct(totals["total_held"], claimed),
        "pct_claim_remaining": display_pct(totals["total_claim_remaining"], claimed),
        "pct_bought_extra": round(bought_extra_count / max(count, 1) * 100, 1),
        "count_bought_extra": bought_extra_count,
    }


def merge_candidate_maps(*maps):
    merged = {}
    for candidate_map in maps:
        for address, sources in (candidate_map or {}).items():
            for source in sources:
                add_candidate(merged, address, source)
    return merged


def collect_transfer_candidates(transfers):
    candidates = {}
    for from_addr, to_addr, _, _ in transfers:
        add_candidate(candidates, from_addr, "transfer_ledger")
        add_candidate(candidates, to_addr, "transfer_ledger")
    return candidates


def build_current_holder_rows(balances, candidate_sources):
    rows = []
    for address, balance in balances.items():
        if address in EXCLUDED_ADDRS or balance < 0.005:
            continue
        rows.append({
            "address": address,
            "balance": round(balance, 2),
            "sources": sorted(candidate_sources.get(address, set())),
        })
    rows.sort(key=lambda row: row["balance"], reverse=True)
    for idx, row in enumerate(rows, 1):
        row["rank"] = idx
    return rows

def load_state():
    """Load incremental sync state (cached transfers + last block)."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception as exc:
            print(f"⚠️ load_state: failed to read {STATE_FILE} ({exc}); starting full resync", flush=True)
    return {}


def save_state(state):
    """Save incremental sync state."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def get_current_block():
    errors = []
    for rpc in RPC_URLS:
        for _ in range(3):
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
                }, timeout=10, headers={"Content-Type": "application/json"})
                resp.raise_for_status()
                result = resp.json().get("result")
                block = int(result, 16)
                if block >= DEPLOY_BLOCK:
                    return block
                errors.append(f"{_rpc_safe_host(rpc)} returned invalid block {block}")
            except Exception as exc:
                errors.append(f"{_rpc_safe_host(rpc)}: {type(exc).__name__}: {_rpc_sanitize_error(exc)}")
                time.sleep(1)
    raise RuntimeError("Unable to fetch a valid Berachain block number; refusing to generate oDOLO flows. "
                       + " | ".join(errors[-5:]))


def build_cutoff_blocks(current_block):
    if not isinstance(current_block, int) or current_block < DEPLOY_BLOCK:
        raise RuntimeError(
            f"Invalid Berachain current block {current_block!r}; "
            "refusing to collapse oDOLO flow windows to deploy block."
        )
    cutoffs = {}
    for period, seconds in PERIODS.items():
        if seconds is None:
            cutoffs[period] = DEPLOY_BLOCK
        else:
            blocks_back = seconds // BLOCK_TIME
            cutoffs[period] = max(current_block - blocks_back, DEPLOY_BLOCK)
    return cutoffs


def fetch_transfer_logs(start_block, end_block):
    chunk_size = CHUNK_SIZE
    if start_block > end_block:
        return []

    total_blocks = end_block - start_block + 1
    print(f"  Berachain: scanning blocks {start_block:,} → {end_block:,} ({total_blocks:,} blocks)")

    all_transfers = []
    current = start_block
    chunks_done = 0

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        success = False
        last_error = None
        for attempt in range(len(RPC_URLS) * 2):
            rpc = RPC_URLS[attempt % len(RPC_URLS)]
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_getLogs",
                    "params": [{
                        "address": ODOLO_CONTRACT,
                        "topics": [TRANSFER_TOPIC],
                        "fromBlock": hex(current),
                        "toBlock": hex(chunk_end),
                    }], "id": 1
                }, timeout=30, headers={"Content-Type": "application/json"})

                r = resp.json()
                if "error" in r:
                    err_msg = r["error"].get("message", "")
                    last_error = f"{_rpc_safe_host(rpc)}: {err_msg or r['error']}"
                    if "range" in err_msg.lower() or "limit" in err_msg.lower():
                        chunk_size = max(chunk_size // 2, 1000)
                        chunk_end = min(current + chunk_size - 1, end_block)
                        continue
                    time.sleep(0.5)
                    continue

                logs = r.get("result", [])
                for log in logs:
                    if len(log.get("topics", [])) < 3:
                        continue
                    from_addr = "0x" + log["topics"][1][26:].lower()
                    to_addr = "0x" + log["topics"][2][26:].lower()
                    value_wei = int(log["data"], 16)
                    block_num = int(log["blockNumber"], 16)
                    all_transfers.append((from_addr, to_addr, value_wei, block_num))

                success = True
                break
            except requests.exceptions.Timeout:
                last_error = f"{_rpc_safe_host(rpc)}: timeout"
                chunk_size = max(chunk_size // 2, 1000)
                chunk_end = min(current + chunk_size - 1, end_block)
                time.sleep(1)
            except Exception as exc:
                last_error = f"{_rpc_safe_host(rpc)}: {type(exc).__name__}: {_rpc_sanitize_error(exc)}"
                time.sleep(0.5)

        if not success:
            raise RuntimeError(
                f"Failed to fetch complete oDOLO Transfer logs for block range "
                f"{current:,} → {chunk_end:,}; refusing to write partial flow data. "
                f"Last error: {last_error or 'unknown'}"
            )

        current = chunk_end + 1
        chunks_done += 1

        if chunks_done % 20 == 0 or current > end_block:
            pct = min(100, (current - start_block) * 100 // max(total_blocks, 1))
            print(f"    Berachain: {pct}% (block {current:,}/{end_block:,}, {len(all_transfers):,} txs)", flush=True)

        if chunk_size < CHUNK_SIZE:
            chunk_size = min(chunk_size * 2, CHUNK_SIZE)

        time.sleep(0.05)

    print(f"  ✅ Berachain: {len(all_transfers):,} transfers found")
    return all_transfers


def replace_transfer_range(cached, refreshed, start_block, end_block):
    """Replace an inclusive cached block range with authoritative RPC results."""
    preserved = [
        tuple(transfer) for transfer in cached
        if int(transfer[3]) < start_block or int(transfer[3]) > end_block
    ]
    merged = preserved + [tuple(transfer) for transfer in refreshed]
    return sorted(merged, key=lambda transfer: int(transfer[3]))


def detect_contracts_batch(addresses):
    contracts = set()
    payloads = []
    meta_by_id = {}
    for idx, addr in enumerate(addresses):
        request_id = f"code:{idx}"
        payloads.append({
            "jsonrpc": "2.0",
            "method": "eth_getCode",
            "params": [addr, "latest"],
            "id": request_id,
        })
        meta_by_id[request_id] = addr

    try:
        responses, missing_ids = rpc_batch_requests(
            RPC_URLS,
            payloads,
            timeout=5,
            retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
            batch_size=RPC_BATCH_SIZE,
            quiet=True,
            describe="oDOLO eth_getCode",
        )
    except RpcError:
        responses, missing_ids = {}, [payload["id"] for payload in payloads]

    for payload in payloads:
        request_id = payload["id"]
        response = responses.get(request_id)
        if request_id in missing_ids or not isinstance(response, dict) or response.get("error") or "result" not in response:
            try:
                response = rpc_single_request(
                    RPC_URLS,
                    payload,
                    timeout=5,
                    retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                    quiet=True,
                    describe="oDOLO eth_getCode fallback",
                )
            except RpcError:
                continue
        code = str(response.get("result", "0x") if isinstance(response, dict) else "0x")
        if code and len(code) > 4:
            contracts.add(meta_by_id[request_id])
    return contracts


def load_address_labels():
    """Read shared dashboard labels used to distinguish custody from infra."""
    path = os.path.join(DATA_DIR, "dolo-address-labels.js")
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as file:
            text = file.read()
    except OSError as exc:
        print(f"  ⚠️ Could not load address labels: {exc}", flush=True)
        return {}
    labels = {}
    for match in re.finditer(r'"(0x[a-fA-F0-9]{40})"\s*:\s*\{([^}]+)\}', text):
        body = match.group(2)
        type_match = re.search(r'type\s*:\s*"([^"]+)"', body)
        labels[match.group(1).lower()] = {
            "type": type_match.group(1) if type_match else "",
        }
    return labels


def select_dynamic_flow_exclusions(detected_contracts, address_labels):
    """Exclude infrastructure contracts while keeping custody wallets visible."""
    visible_label_types = {"cex", "multisig", "safe", "contract_wallet"}
    return {
        str(address).lower()
        for address in detected_contracts
        if str((address_labels.get(str(address).lower()) or {}).get("type") or "").lower()
        not in visible_label_types
    }


# oDOLO Vester — exercising oDOLO sends tokens here, NOT selling
VESTER_CONTRACT = "0x3e9b9a16743551da49b5e136c716bba7932d2cec"


def calculate_flows(transfers, excluded):
    """Calculate net flow per address.
    Transfers involving mint/burn/vester addresses are skipped —
    mints, burns and exercising are not real market flow."""
    SKIP_ADDRS = {
        ZERO,
        ODOLO_CONTRACT,
        "0x0000000000000000000000000000000000000001",
        VESTER_CONTRACT,
    }
    flows = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        if from_addr in SKIP_ADDRS or to_addr in SKIP_ADDRS:
            continue
        value = value_wei / (10 ** 18)
        flows[from_addr] = flows.get(from_addr, 0) - value
        flows[to_addr] = flows.get(to_addr, 0) + value
    return flows


def calculate_flow_components(transfers):
    """Return gross inflow, gross outflow and net flow for every wallet."""
    skip_addrs = {
        ZERO,
        ODOLO_CONTRACT,
        "0x0000000000000000000000000000000000000001",
        VESTER_CONTRACT,
    }
    components = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        if from_addr in skip_addrs or to_addr in skip_addrs:
            continue
        value = value_wei / (10 ** 18)
        sender = components.setdefault(
            from_addr, {"gross_inflow": 0.0, "gross_outflow": 0.0, "net_flow": 0.0}
        )
        receiver = components.setdefault(
            to_addr, {"gross_inflow": 0.0, "gross_outflow": 0.0, "net_flow": 0.0}
        )
        sender["gross_outflow"] += value
        sender["net_flow"] -= value
        receiver["gross_inflow"] += value
        receiver["net_flow"] += value
    return components


def calculate_gross_outflows(transfers, excluded):
    """Calculate total gross outflow per address (sum of all transfers OUT).
    Unlike net flow, this doesn't cancel against inflows.
    Excludes transfers to Vester (exercising is not selling)."""
    SKIP_ADDRS = {
        ZERO,
        ODOLO_CONTRACT,
        "0x0000000000000000000000000000000000000001",
        VESTER_CONTRACT,
    }
    outflows = {}
    for from_addr, to_addr, value_wei, _ in transfers:
        if from_addr in SKIP_ADDRS or to_addr in SKIP_ADDRS:
            continue
        value = value_wei / (10 ** 18)
        outflows[from_addr] = outflows.get(from_addr, 0) + value
    return outflows


def count_txs(transfers, excluded):
    counts = {}
    for from_addr, to_addr, _, _ in transfers:
        counts[from_addr] = counts.get(from_addr, 0) + 1
        counts[to_addr] = counts.get(to_addr, 0) + 1
    return counts


def get_top(flows, tx_counts, n, mode="accumulator", excluded=None, components=None):
    """Get top N accumulators or sellers, excluding known contracts."""
    if excluded is None:
        excluded = set()
    # 0.005 floor: float dust passes `> 0` and then rounds to a "0.00" row.
    if mode == "accumulator":
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1], reverse=True)
        filtered = [(addr, val) for addr, val in sorted_addrs if val >= 0.005 and addr not in excluded]
    else:
        # For sellers: flows values are gross outflows (positive = more sold)
        sorted_addrs = sorted(flows.items(), key=lambda x: x[1], reverse=True)
        filtered = [(addr, val) for addr, val in sorted_addrs if val >= 0.005 and addr not in excluded]

    result = []
    for addr, value in filtered[:n]:
        row = {
            "address": addr,
            "net_flow": round(value, 2),
            "tx_count": tx_counts.get(addr, 0),
        }
        if components and addr in components:
            row.update({
                key: round(component_value, 2)
                for key, component_value in components[addr].items()
            })
            if mode != "accumulator":
                row["net_flow"] = round(components[addr]["net_flow"], 2)
        result.append(row)
    return result


def _multicall_odolo_balances(rpcs, addresses):
    """Fast path: oDOLO balanceOf via Multicall3 aggregate3 — ONE eth_call per
    chunk instead of one per address.

    Returns (resolved, unresolved): `resolved` maps address -> raw uint256
    balance; `unresolved` lists addresses Multicall3 could not cleanly resolve,
    which the caller sends through the per-address fallback so the failed-vs-zero
    handling still applies. Data-identical to individual balanceOf calls. If web3
    is unavailable, every address defers to the fallback.
    """
    addresses = list(addresses)
    if not addresses:
        return {}, []
    try:
        from web3 import Web3
    except ImportError:
        return {}, addresses
    rpc_list = [r for r in (rpcs or []) if r]
    if not rpc_list:
        return {}, addresses

    token = Web3.to_checksum_address(ODOLO_CONTRACT)
    multicall_addr = Web3.to_checksum_address(MULTICALL3_ADDR)
    selector = "70a08231"  # balanceOf(address)
    chunk_size = max(1, RPC_BATCH_SIZE)
    resolved = {}
    unresolved = []
    rpc_idx = 0

    for start in range(0, len(addresses), chunk_size):
        chunk = addresses[start:start + chunk_size]
        calls = [
            (token, True, bytes.fromhex(selector + a.replace("0x", "").lower().zfill(64)))
            for a in chunk
        ]
        results = None
        for attempt in range(len(rpc_list)):
            rpc = rpc_list[(rpc_idx + attempt) % len(rpc_list)]
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
                multicall = w3.eth.contract(address=multicall_addr, abi=MULTICALL3_AGG3_ABI)
                results = multicall.functions.aggregate3(calls).call()
                rpc_usage.record_request("eth_call")  # one aggregate3 == one eth_call
                rpc_idx = (rpc_idx + attempt) % len(rpc_list)
                break
            except Exception:
                results = None
                continue
        if not results or len(results) != len(chunk):
            unresolved.extend(chunk)
            continue
        for addr, item in zip(chunk, results):
            success = bool(item[0])
            data = bytes(item[1]) if item[1] is not None else b""
            if success and len(data) >= 32:
                resolved[addr] = int.from_bytes(data[:32], "big")
            else:
                unresolved.append(addr)
    return resolved, unresolved


def fetch_odolo_balances(addresses):
    """Fetch current oDOLO balanceOf(address) values with batch RPC fallback."""
    unique = sorted({addr.lower() for addr in addresses if addr})
    balances = {}
    if not unique:
        return balances

    bal_selector = "0x70a08231"  # balanceOf(address)

    def decode_balance_result(result):
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError("missing balanceOf result")
        # balanceOf(address) returns an ABI-encoded uint256 (32 bytes). Short
        # values like 0x0/0x usually mean the batch item failed or was omitted.
        if len(result) < 66:
            raise ValueError(f"invalid balanceOf result length: {result}")
        return round(int(result, 16) / (10 ** 18), 2)

    def call_payload(addr, idx):
        padded = addr.replace("0x", "").lower().zfill(64)
        return {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": ODOLO_CONTRACT, "data": bal_selector + padded}, "latest"],
            "id": idx,
        }

    # Fast path: Multicall3 batches balanceOf into one eth_call per chunk
    # (data-identical; addresses it can't resolve fall through below).
    resolved_raw, remaining = _multicall_odolo_balances(RPC_URLS, unique)
    for resolved_addr, raw_balance in resolved_raw.items():
        balances[resolved_addr] = round(raw_balance / (10 ** 18), 2)
    if not remaining:
        return balances

    # Fallback (unchanged): batch + individual eth_call for the rest. Honor the
    # configured batch size instead of a hardcoded 100.
    batch_size = RPC_BATCH_SIZE
    for start in range(0, len(remaining), batch_size):
        batch = remaining[start:start + batch_size]
        pending = list(batch)
        for rpc in RPC_URLS:
            try:
                payload = [call_payload(addr, start + i) for i, addr in enumerate(batch)]
                resp = requests.post(rpc, json=payload, timeout=20, headers={"Content-Type": "application/json"})
                data = resp.json()
                if not isinstance(data, list):
                    raise ValueError("batch RPC response was not a list")
                by_id = {str(item.get("id")): item for item in data if isinstance(item, dict)}
                failed = []
                for i, addr in enumerate(batch):
                    item = by_id.get(str(start + i))
                    if not isinstance(item, dict) or item.get("error") or "result" not in item:
                        failed.append(addr)
                        continue
                    try:
                        balances[addr] = decode_balance_result(item.get("result"))
                    except ValueError:
                        failed.append(addr)
                pending = failed
                break
            except (requests.RequestException, ValueError, TypeError):
                time.sleep(0.5)

        if pending:
            if len(pending) < len(batch):
                print(f"    retrying {len(pending)} incomplete balanceOf calls", flush=True)
            for addr in pending:
                for rpc in RPC_URLS:
                    try:
                        resp = requests.post(rpc, json=call_payload(addr, 1), timeout=5, headers={"Content-Type": "application/json"})
                        data = resp.json()
                        if not isinstance(data, dict) or data.get("error") or "result" not in data:
                            raise ValueError("single RPC balanceOf failed")
                        balances[addr] = decode_balance_result(data.get("result"))
                        break
                    except (requests.RequestException, ValueError, TypeError):
                        time.sleep(0.3)
                time.sleep(0.03)

        print(f"    balances: {min(start + batch_size, len(unique)):,}/{len(unique):,}", flush=True)
        time.sleep(0.05)

    return balances


def calculate_balances_from_transfers(transfers, addresses):
    """Fallback balance reconstruction from the cached Transfer ledger."""
    wanted = {addr.lower() for addr in addresses if addr}
    balances = {addr: 0 for addr in wanted}
    for from_addr, to_addr, value_wei, _ in transfers:
        if from_addr in balances:
            balances[from_addr] -= value_wei
        if to_addr in balances:
            balances[to_addr] += value_wei
    return {addr: round(max(0, value_wei) / (10 ** 18), 2) for addr, value_wei in balances.items()}


def main():
    print("=" * 60)
    print("🔄 oDOLO Token Flows — Top Accumulators & Sellers")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load incremental state
    state = load_state()
    is_incremental = state.get("schema_version") == FLOW_STATE_SCHEMA_VERSION
    if is_incremental:
        print("📦 Found previous state — running incremental sync")
    else:
        print("🆕 Missing current integrity checkpoint — rebuilding full Transfer history")

    claim_paths = (ODOLO_CLAIM_EVENTS_JSON, REWARD_CLAIM_EVENTS_BERA_JSON)
    claim_coverage = load_canonical_claim_coverage(claim_paths)
    first_canonical_claim_block = load_first_canonical_claim_block(claim_paths)

    # Get current block and cap the snapshot to fully indexed claim coverage.
    print("\n📡 Getting current block number...")
    chain_head = get_current_block()
    confirmed_head = chain_head - REORG_BUFFER_BLOCKS
    current_block = select_flow_snapshot_block(
        chain_head,
        claim_coverage["to_block"],
    )
    claim_coverage_lag = max(0, confirmed_head - claim_coverage["to_block"])
    print(
        f"  Berachain: head {chain_head:,}; claim-covered scan end {current_block:,} "
        f"({claim_coverage_lag:,} block lag)"
    )

    # Calculate cutoff blocks. If current_block is invalid, fail before touching data.
    cutoff_blocks = build_cutoff_blocks(current_block)

    # Fetch transfers — incremental: only new blocks since last run
    oldest_needed = min(cutoff_blocks.values())
    print("\n📡 Fetching Transfer events...")

    cached_transfers = state.get("transfers", [])
    last_block = state.get("last_block", 0)

    if is_incremental and last_block > 0 and cached_transfers:
        # Re-read an overlap on every run. Replacing that range authoritatively
        # heals short RPC/index gaps and removes transfers orphaned by a reorg.
        rescan_start = max(oldest_needed, min(last_block, current_block) - RECENT_RESCAN_BLOCKS + 1)
        refreshed_transfers = fetch_transfer_logs(rescan_start, current_block)

        # Convert cached transfers back from lists to tuples and discard any
        # unconfirmed future rows left by an older checkpoint.
        restored = [tuple(t) for t in cached_transfers if int(t[3]) <= current_block]

        # If a previous rolling-window cache pruned early history, backfill it so
        # "all" really means from deployment, not the last 365 days.
        if restored:
            cached_min_block = min(t[3] for t in restored)
        else:
            cached_min_block = 0
        historical_backfill = []
        if cached_min_block and cached_min_block > oldest_needed:
            print(f"  Berachain: backfilling missing all-time range {oldest_needed:,} → {cached_min_block - 1:,}")
            historical_backfill = fetch_transfer_logs(oldest_needed, cached_min_block - 1)

        # Merge: historical backfill + cached, with the recent overlap replaced.
        merged = historical_backfill + replace_transfer_range(
            restored, refreshed_transfers, rescan_start, current_block
        )

        # Prune: drop transfers from blocks older than the oldest needed
        merged = [t for t in merged if t[3] >= oldest_needed]

        all_transfers = merged
        print(f"  Berachain: {len(historical_backfill):,} backfilled + {len(refreshed_transfers):,} refreshed + "
              f"{len(restored):,} cached → {len(merged):,} total (after pruning)")
    else:
        # Full scan from the oldest needed block
        all_transfers = fetch_transfer_logs(oldest_needed, current_block)

    # Update state
    state["last_block"] = current_block
    state["schema_version"] = FLOW_STATE_SCHEMA_VERSION
    state["transfers"] = [
        list(t) for t in all_transfers
        if t[3] >= oldest_needed
    ]

    # Detect contracts — check top 100 by gross outflow
    print("\n🔍 Detecting contract addresses to exclude...")
    gross_all = calculate_gross_outflows(all_transfers, EXCLUDED_ADDRS)
    top_by_outflow = sorted(gross_all.items(), key=lambda x: x[1], reverse=True)[:100]
    addrs_to_check = [addr for addr, _ in top_by_outflow if addr not in EXCLUDED_ADDRS]
    contracts = detect_contracts_batch(addrs_to_check)
    dynamic_exclusions = select_dynamic_flow_exclusions(contracts, load_address_labels())
    EXCLUDED_ADDRS.update(dynamic_exclusions)
    print(
        f"  Excluded {len(dynamic_exclusions)} infrastructure contract(s); "
        f"kept {len(contracts) - len(dynamic_exclusions)} labeled custody contract(s) visible"
    )

    # ── Identify claimer wallets first (needed for per-period filtering) ──
    event_claims = load_reward_claims_from_sources(
        claim_paths,
        max_block=current_block,
    )
    claims_by_wallet, claim_reconciliation = build_canonical_claim_totals(
        all_transfers,
        event_claims,
        first_canonical_claim_block,
    )
    claim_coverage_metadata = {
        "coverage_status": claim_coverage["status"],
        "coverage_from_block": claim_coverage["from_block"],
        "coverage_to_block": claim_coverage["to_block"],
        "coverage_lag_blocks": claim_coverage_lag,
    }
    claim_reconciliation.update(claim_coverage_metadata)
    claimer_addrs = set(claims_by_wallet.keys())
    print(f"  Found {len(claimer_addrs)} claimer wallets")
    print(
        "  Claim-source reconciliation: "
        f"{claim_reconciliation['canonical_event_claimed']:,.2f} canonical + "
        f"{claim_reconciliation['historical_transfer_claimed']:,.2f} historical; "
        f"ignored {claim_reconciliation['ignored_post_index_transfer_amount']:,.2f} "
        "unmatched post-index transfer amount"
    )

    # Calculate flows for each period
    print("\n📊 Calculating flows...")
    output_periods = {}
    flow_metadata_cache = {}
    for period, seconds in PERIODS.items():
        cutoff = cutoff_blocks[period]
        period_transfers = [t for t in all_transfers if t[3] >= cutoff]

        flow_components = calculate_flow_components(period_transfers)
        flows = {addr: values["net_flow"] for addr, values in flow_components.items()}
        gross_out = {addr: values["gross_outflow"] for addr, values in flow_components.items()}
        tx_counts = count_txs(period_transfers, EXCLUDED_ADDRS)

        accumulators = get_top(
            flows, tx_counts, TOP_N, "accumulator", EXCLUDED_ADDRS, flow_components
        )
        sellers = get_top(
            gross_out, tx_counts, TOP_N, "seller", EXCLUDED_ADDRS, flow_components
        )

        # Filter sellers to only claimers
        claimer_gross = {addr: val for addr, val in gross_out.items() if addr in claimer_addrs}
        claimer_sellers = get_top(
            claimer_gross, tx_counts, TOP_N, "seller", EXCLUDED_ADDRS, flow_components
        )

        def load_flow_evidence(blocks):
            missing = set(blocks) - set(flow_metadata_cache)
            if missing:
                flow_metadata_cache.update(fetch_token_block_evidence(
                    RPC_URLS, ODOLO_CONTRACT, missing, rpc_batch_requests,
                    retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                    batch_size=RPC_BATCH_SIZE,
                    describe="Berachain oDOLO flow transaction metadata",
                ))
            return {block: flow_metadata_cache[block] for block in blocks if block in flow_metadata_cache}

        attach_latest_flow_metadata(
            accumulators, period_transfers, "inbound", "berachain", load_flow_evidence,
        )
        attach_latest_flow_metadata(
            sellers, period_transfers, "outbound", "berachain", load_flow_evidence,
        )
        attach_latest_flow_metadata(
            claimer_sellers, period_transfers, "outbound", "berachain", load_flow_evidence,
        )

        output_periods[period] = {
            "accumulators": accumulators,
            "sellers": sellers,
            "claimer_sellers": claimer_sellers,
            "total_transfers": len(period_transfers),
        }

        if accumulators:
            print(f"  {period}: {len(period_transfers):,} transfers, "
                  f"top accumulator: {accumulators[0]['net_flow']:,.0f} oDOLO")
        else:
            print(f"  {period}: no data")

    # Collect all unique addresses from results
    all_addrs = set()
    for period_data in output_periods.values():
        for entry in period_data["accumulators"] + period_data["sellers"]:
            all_addrs.add(entry["address"])

    transfer_candidates = collect_transfer_candidates(all_transfers)
    existing_flow_candidates = load_existing_flow_candidates()
    exerciser_candidates = load_exerciser_candidates()
    vedolo_route_candidates = load_vedolo_odolo_route_candidates()
    claim_candidates = {}
    for addr in claimer_addrs:
        add_candidate(claim_candidates, addr, "odolo_claims")
    candidate_sources = merge_candidate_maps(
        transfer_candidates,
        existing_flow_candidates,
        exerciser_candidates,
        vedolo_route_candidates,
        claim_candidates,
    )

    # Fetch current oDOLO balances for every known candidate.
    # Claimer Breakdown "Held" is intended to mean the wallet's current balance,
    # not the residual amount inferred from claim/exercise/outflow history.
    balance_addrs = set(candidate_sources.keys()) | all_addrs | claimer_addrs
    print(f"\n💰 Fetching current oDOLO balances for {len(balance_addrs)} reconciled addresses...")
    ledger_balances = calculate_balances_from_transfers(all_transfers, balance_addrs)
    balances = fetch_odolo_balances(balance_addrs)
    missing_balances = [addr for addr in balance_addrs if addr.lower() not in balances]
    if missing_balances:
        print(f"  ⚠️ RPC missed {len(missing_balances)} balances; using Transfer-ledger fallback")
        for addr in missing_balances:
            balances[addr.lower()] = ledger_balances.get(addr.lower(), 0)

    current_holders = build_current_holder_rows(balances, candidate_sources)
    balance_reconciliation = {
        "methodology": (
            "Current oDOLO balances are fetched with balanceOf(wallet) for every address seen in "
            "the Transfer ledger plus self-healing candidates from previous oDOLO snapshots, "
            "oDOLO claim events, exerciser history, and oDOLO-routed veDOLO locks."
        ),
        "candidate_addresses": len(balance_addrs),
        "transfer_ledger_candidates": len(transfer_candidates),
        "previous_snapshot_candidates": len(existing_flow_candidates),
        "exerciser_candidates": len(exerciser_candidates),
        "vedolo_odolo_route_candidates": len(vedolo_route_candidates),
        "claim_candidates": len(claim_candidates),
        "claim_event_wallets": claim_reconciliation["canonical_event_wallets"],
        "claim_event_added": claim_reconciliation["canonical_event_added"],
        "claim_event_updated": claim_reconciliation["canonical_event_updated"],
        "rpc_balances": len(balances) - len(missing_balances),
        "ledger_fallback_balances": len(missing_balances),
        "current_holder_rows": len(current_holders),
    }

    # Add balances to all entries
    for period_data in output_periods.values():
        for entry in period_data["accumulators"] + period_data["sellers"]:
            entry["balance"] = balances.get(entry["address"].lower(), 0)

    # Checksum addresses
    try:
        from web3 import Web3
        for period_data in output_periods.values():
            for entry in period_data["accumulators"] + period_data["sellers"]:
                try:
                    entry["address"] = Web3.to_checksum_address(entry["address"])
                except Exception:
                    pass
    except ImportError:
        pass

    # ── Claimer behavior analysis ──
    # Trace: rewards contract (0x79e6...) → wallets = claims
    # For each claimer: exercised (→ Vester), outflow (→ others), held
    print("\n📊 Analyzing claimer behavior...")
    SKIP_ADDRS = {ZERO, ODOLO_CONTRACT, "0x0000000000000000000000000000000000000001"}

    # For each claimer, track outgoing transfers
    claimer_stats = {}
    for wallet, claimed in claims_by_wallet.items():
        exercised = 0
        outflow = 0
        for from_addr, to_addr, value_wei, _ in all_transfers:
            if from_addr == wallet:
                val = value_wei / (10 ** 18)
                if to_addr == VESTER_CONTRACT:
                    exercised += val
                elif to_addr not in SKIP_ADDRS and to_addr != REWARDS_CONTRACT:
                    outflow += val
        # Cap claimed lifecycle amounts — don't count extra purchased oDOLO.
        # Priority: exercised first, then outflow, remainder = claim_remaining.
        # Held is the wallet's current live oDOLO balance fetched via balanceOf().
        exercised_capped = min(exercised, claimed)
        remaining = claimed - exercised_capped
        outflow_capped = min(outflow, remaining)
        claim_remaining = max(0, remaining - outflow_capped)
        held = balances.get(wallet.lower(), ledger_balances.get(wallet.lower(), 0))
        claimer_stats[wallet] = {
            "claimed": round(claimed, 2),
            "exercised": round(exercised_capped, 2),
            "outflow": round(outflow_capped, 2),
            "held": round(held, 2),
            "claim_remaining": round(claim_remaining, 2),
            "bought_extra": round(max(0, exercised - claimed), 2),
        }

    # All claimers sorted by claimed desc (for the breakdown table)
    all_claimers_list = sorted(
        [{"address": addr, **stats} for addr, stats in claimer_stats.items()],
        key=lambda x: x["claimed"], reverse=True,
    )
    claimer_summary = summarize_claimer_rows(all_claimers_list)

    claimer_behavior = {
        **claimer_summary,
        "held_source": "balanceOf(wallet)",
        "claim_attribution_methodology": (
            "Fungible oDOLO provenance is estimated per wallet: claimed allocation is assigned "
            "to exercise first, then external outflow, with the residual reported as claim remaining. "
            "Held is an independent current balanceOf(wallet) value and is not part of that partition."
        ),
        "all_claimers": all_claimers_list,
    }
    print(f"  Claimers: {len(claimer_stats)}, Claimed: {claimer_summary['total_claimed']:,.0f}")
    print(f"  Exercised: {claimer_summary['total_exercised']:,.0f} ({claimer_behavior['pct_exercised']}%)")
    print(f"  Outflow: {claimer_summary['total_outflow']:,.0f} ({claimer_behavior['pct_outflow']}%)")
    print(f"  Held now: {claimer_summary['total_held']:,.0f} ({claimer_behavior['pct_held']}%)")
    print(f"  Claim remaining by flow: {claimer_summary['total_claim_remaining']:,.0f} ({claimer_behavior['pct_claim_remaining']}%)")

    # ── Per-period claimer breakdown (for date range filtering) ──
    print("\n📊 Generating per-period claimer breakdown...")
    claimer_periods = {}
    for period, seconds in PERIODS.items():
        cutoff = cutoff_blocks[period]
        p_transfers = [t for t in all_transfers if t[3] >= cutoff]

        if period == "all":
            p_claims = dict(claims_by_wallet)
            period_claim_reconciliation = dict(claim_reconciliation)
        else:
            period_event_claims = load_reward_claims_from_sources(
                claim_paths,
                min_block=cutoff,
                max_block=current_block,
            )
            p_claims, period_claim_reconciliation = build_canonical_claim_totals(
                p_transfers,
                period_event_claims,
                first_canonical_claim_block,
            )
            period_claim_reconciliation.update(claim_coverage_metadata)

        p_all = []
        for wallet, claimed in p_claims.items():
            exercised = 0
            outflow = 0
            for from_addr, to_addr, value_wei, _ in p_transfers:
                if from_addr == wallet:
                    val = value_wei / (10 ** 18)
                    if to_addr == VESTER_CONTRACT:
                        exercised += val
                    elif to_addr not in SKIP_ADDRS and to_addr != REWARDS_CONTRACT:
                        outflow += val
            ex_cap = min(exercised, claimed)
            rem = claimed - ex_cap
            out_cap = min(outflow, rem)
            claim_remaining = max(0, rem - out_cap)
            held = balances.get(wallet.lower(), ledger_balances.get(wallet.lower(), 0))
            p_all.append({
                "address": wallet,
                "claimed": round(claimed, 2),
                "exercised": round(ex_cap, 2),
                "outflow": round(out_cap, 2),
                "held": round(held, 2),
                "claim_remaining": round(claim_remaining, 2),
                "bought_extra": round(max(0, exercised - claimed), 2),
            })

        p_all.sort(key=lambda x: x["claimed"], reverse=True)
        claimer_periods[period] = {
            **summarize_claimer_rows(p_all),
            "all_claimers": p_all,
            "claim_source_reconciliation": period_claim_reconciliation,
        }
        print(f"  {period}: {len(p_all)} claimers")

    # ── Data protection: don't overwrite good data with empty data ──
    existing_cb = None
    existing_cp = None
    existing_periods = None
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON) as f:
                old = json.load(f)
            # Protect claimers
            if len(claimer_stats) == 0:
                old_count = old.get("claimer_behavior", {}).get("total_claimers", 0)
                if old_count > 0:
                    print(f"\n⚠️ This run found 0 claimers but existing file has {old_count}. Preserving old claimer data.")
                    existing_cb = old["claimer_behavior"]
                    existing_cp = old.get("claimer_periods", {})
            # Protect flow periods
            has_new_flows = any(
                len(p.get("accumulators", [])) > 0 or len(p.get("sellers", [])) > 0
                for p in output_periods.values()
            )
            if not has_new_flows:
                old_periods = old.get("periods", {})
                has_old_flows = any(
                    len(p.get("accumulators", [])) > 0 or len(p.get("sellers", [])) > 0
                    for p in old_periods.values()
                )
                if has_old_flows:
                    print(f"\n⚠️ This run found 0 flow data but existing file has data. Preserving old flow periods.")
                    existing_periods = old_periods
        except Exception as exc:
            print(f"⚠️ Could not read existing output for preservation check: {exc}", flush=True)

    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "current_block": current_block,
        "chain_head": chain_head,
        "deploy_block": DEPLOY_BLOCK,
        "cutoff_blocks": cutoff_blocks,
        "transfer_coverage": {
            "oldest_needed_block": oldest_needed,
            "scanned_from_block": oldest_needed,
            "min_cached_block": min((t[3] for t in all_transfers), default=0),
            "max_cached_block": max((t[3] for t in all_transfers), default=0),
            "transfer_count": len(all_transfers),
            "recent_rescan_blocks": RECENT_RESCAN_BLOCKS,
            "reorg_buffer_blocks": REORG_BUFFER_BLOCKS,
            "state_schema_version": FLOW_STATE_SCHEMA_VERSION,
        },
        "periods": existing_periods if existing_periods else output_periods,
        "claimer_behavior": existing_cb if existing_cb else claimer_behavior,
        "claimer_periods": existing_cp if existing_cp else claimer_periods,
        "claim_source_reconciliation": claim_reconciliation,
        "current_holders": current_holders,
        "balance_reconciliation": balance_reconciliation,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    # Save incremental state for next run
    save_state(state)

    print(f"\n💾 Saved: {OUTPUT_JSON}")
    print(f"   State saved to {STATE_FILE} for incremental sync")

    for period in PERIODS:
        data = output_periods[period]
        print(f"\n📊 {period.upper()}:")
        if data["accumulators"]:
            top = data["accumulators"][0]
            print(f"  🟢 Top accumulator: {top['address'][:14]}… +{top['net_flow']:,.0f} oDOLO")
        if data["sellers"]:
            top = data["sellers"][0]
            print(f"  🔴 Top seller: {top['address'][:14]}… -{top['net_flow']:,.0f} oDOLO")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
