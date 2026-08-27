#!/usr/bin/env python3
"""
DOLO Token Holders — ERC-20 holder generator (ETH + Berachain)
Uses eth_getLogs for 100% accuracy (catches DEX swaps, internal transfers, etc.)
With incremental sync: saves last processed block per chain.
"""
import json, time, os, sys, re
import requests
from datetime import datetime

# Single source of truth for endpoints (env-injected Alchemy keys first).
from rpc_client import (
    RpcError,
    decode_uint256,
    get_endpoints as _rpc_endpoints,
    rpc_batch_requests,
    rpc_single_request,
)

import rpc_usage

# ===== CONFIG =====
DOLO_CONTRACT = "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
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
# Single source of truth for Safe singleton addresses (all versions).
from safe_wallets import SAFE_SINGLETON_ADDRS

CHAINS = {
    "eth": {
        "name": "Ethereum",
        "rpcs": _rpc_endpoints("ethereum"),
        "start_block": 21_000_000,  # DOLO deployed around this block on ETH
        "chunk_size": 50_000,
    },
    "bera": {
        "name": "Berachain",
        "rpcs": _rpc_endpoints("berachain"),
        "start_block": 2_925_000,
        "chunk_size": 50_000,
    },
}

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "dolo_holders.json")
STATE_FILE = os.path.join(DATA_DIR, "dolo_holders_state.json")
FLOW_RECONCILE_JSON = os.path.join(DATA_DIR, "dolo_flows.json")
MIN_BALANCE = 100.0  # 100 DOLO
HOLDER_DISTRIBUTION_MIN_BALANCE = 1_000.0  # lowest bucket shown in the holder chart
HOLDER_CHART_CONTRACT_VERIFY_BATCH_SIZE = int(
    os.environ.get("DOLO_HOLDERS_CONTRACT_VERIFY_BATCH_SIZE", "250")
)
RPC_BATCH_SIZE = int(os.environ.get("DOLO_HOLDERS_RPC_BATCH_SIZE", "50"))
RPC_RETRIES_PER_ENDPOINT = int(os.environ.get("DOLO_HOLDERS_RPC_RETRIES_PER_ENDPOINT", "2"))

# ERC-1967 implementation slot. A proxy-shaped runtime is not enough to call an
# address a user wallet: protocol contracts use the same proxy standard. Only
# implementations independently verified as account-abstraction wallets belong
# in this allowlist.
ERC1967_IMPLEMENTATION_SLOT = (
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
)
VERIFIED_SMART_ACCOUNT_IMPLEMENTATIONS = {
    # Coinbase Smart Wallet implementations verified on Etherscan. Both expose
    # IAccount.validateUserOp() and entryPoint(); the second is also used by
    # EIP-7702 authorizations, while deployed accounts use the audited proxy.
    "0x00000110dcdedc9581cb5ecb8467282f2926534d",
    "0x000100abaad02f1cfc8bbe32bd5a564817339e72",
    # Unverified source on Etherscan, but the deployed bytecode exposes the
    # ERC-4337 entryPoint() selector and SmartWallet ERC-7201 storage metadata.
    # Accounts using it are deployed through an Account Abstraction bundler.
    "0x36d3cbd83961868398d056efbf50f5ce15528c0d",
}
ERC4337_ENTRY_POINT_CALLDATA = "0xb0d691fe"  # entryPoint()
KNOWN_ERC4337_ENTRY_POINTS = {
    "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",  # v0.6
    "0x0000000071727de22e5e9d8baf0edac6f37da032",  # v0.7
}
ERC1967_SMART_ACCOUNT_PROXY_RE = re.compile(
    r"0x363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076cc3735a"
    r"920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3"
    r"(?:[0-9a-fA-F]{64})?"
)


def eip7702_delegation_address(code):
    """Return the delegate for an exact EIP-7702 designator, else None."""
    match = re.fullmatch(r"0xef0100([0-9a-fA-F]{40})", str(code or ""))
    return "0x" + match.group(1).lower() if match else None


def is_erc1967_smart_account_proxy(code):
    """Return True only for the audited immutable-owner AA proxy runtime."""
    return bool(ERC1967_SMART_ACCOUNT_PROXY_RE.fullmatch(str(code or "")))


ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def load_state():
    """Load incremental sync state."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception as exc:
            print(f"⚠️ load_state: failed to read {STATE_FILE} ({exc}); starting full resync", flush=True)
    return {}


def save_state(state):
    """Save incremental sync state atomically (tmp + os.replace) so a crash mid-write
    cannot leave a truncated state file that forces a full resync."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)

def load_investors():
    """Load addresses that claimed vesting (early investors and regular investors)."""
    try:
        with open(os.path.join(DATA_DIR, "vesting_investors.json")) as f:
            data = json.load(f)
            addrs = set(data.get("early_investors", [])) | set(data.get("investors", [])) | set(data.get("team", []))
            return {a.lower() for a in addrs}
    except Exception as e:
        print(f"  ⚠️ Could not load vesting_investors.json: {e}")
        return set()


def _lower_address(value):
    value = str(value or "").strip()
    return value.lower() if ADDRESS_RE.match(value) else None


def load_flow_reconciliation_candidates(path=FLOW_RECONCILE_JSON):
    """Load address candidates from DOLO flow output for holder self-healing.

    The holder state is incremental. If an old cache missed a historical log
    range before gap tracking existed, that wallet can stay absent forever even
    though the flow pipeline later sees it and fetches its current balance. Use
    the compact flow JSON as a candidate set, then verify every candidate with
    on-chain balanceOf before adding anything to the holder list.
    """
    if not os.path.exists(path):
        return set()
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as exc:
        print(f"  ⚠️ Could not load flow reconciliation candidates: {exc}")
        return set()

    candidates = set()

    def walk(obj):
        if isinstance(obj, dict):
            addr = _lower_address(obj.get("address") or obj.get("addr"))
            if addr:
                candidates.add(addr)
            for value in obj.values():
                if isinstance(value, (dict, list)):
                    walk(value)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    walk(item)

    walk(data)
    return candidates


def _is_capacity_rpc_error(error_obj):
    """True when a JSON-RPC error means the endpoint is rate/quota limited
    (e.g. Alchemy 429 "Monthly capacity limit exceeded") — rotate endpoints
    instead of treating it as a block-range error."""
    if not isinstance(error_obj, dict):
        return False
    code = error_obj.get("code")
    msg = str(error_obj.get("message", "")).lower()
    return (
        code in (429, -32005, -32029, -32097)
        or "capacity" in msg
        or "rate limit" in msg
        or "rate-limit" in msg
        or "too many requests" in msg
        or "quota" in msg
        or "monthly" in msg
        or "compute units" in msg
    )


def get_current_block(rpc_url):
    """Get current block number from RPC."""
    for _ in range(3):
        try:
            resp = requests.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
            }, timeout=10, headers={"Content-Type": "application/json"})
            return int(resp.json().get("result", "0x0"), 16)
        except Exception:
            time.sleep(1)
    return 0


def fetch_transfer_logs(chain_key, start_block, end_block=None):
    """Fetch ERC-20 Transfer event logs via eth_getLogs.
    Returns list of (from_addr, to_addr, value_wei, block_number) tuples."""
    cfg = CHAINS[chain_key]
    rpcs = cfg["rpcs"]
    chunk_size = cfg["chunk_size"]
    rpc_idx = 0

    if end_block is None:
        end_block = get_current_block(rpcs[0])

    if start_block >= end_block:
        print(f"  {cfg['name']}: already up to date (block {start_block})")
        return [], start_block, []

    total_chunks = (end_block - start_block + chunk_size - 1) // chunk_size
    print(f"  {cfg['name']}: scanning blocks {start_block:,} → {end_block:,} ({total_chunks} chunks)")

    all_transfers = []
    current = start_block
    chunks_done = 0
    chunks_failed = 0
    skipped_ranges = []  # [start, end] of block ranges lost to persistent RPC failure

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        success = False
        for attempt in range(len(rpcs) * 2):
            rpc = rpcs[(rpc_idx + attempt) % len(rpcs)]
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_getLogs",
                    "params": [{
                        "address": DOLO_CONTRACT,
                        "topics": [TRANSFER_TOPIC],
                        "fromBlock": hex(current),
                        "toBlock": hex(chunk_end),
                    }], "id": 1
                }, timeout=30, headers={"Content-Type": "application/json"})

                r = resp.json()
                if "error" in r:
                    err_msg = r["error"].get("message", "")
                    if _is_capacity_rpc_error(r["error"]):
                        # Endpoint out of quota / rate-limited — rotate to the
                        # next RPC, do not shrink the chunk.
                        time.sleep(0.5)
                        continue
                    if "range" in err_msg.lower() or "limit" in err_msg.lower():
                        # Range too large — halve chunk
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
                # Reduce chunk size on timeout
                chunk_size = max(chunk_size // 2, 1000)
                chunk_end = min(current + chunk_size - 1, end_block)
                time.sleep(1)
            except Exception:
                time.sleep(0.5)

        if not success:
            if chunk_size > 1000:
                chunk_size = max(chunk_size // 2, 1000)
                chunk_end = min(current + chunk_size - 1, end_block)
                print(f"    ⚠️ Retrying block {current:,} with smaller chunk ({chunk_size:,} blocks)")
                continue
            # Persistent failure even at the smallest chunk: record the gap so it is
            # re-scanned on the next run instead of being silently dropped while
            # last_block still advances past it (mirrors generate_dolo_flows.py).
            chunks_failed += 1
            skipped_ranges.append([current, chunk_end])
            print(f"    ⚠️ Failed at block {current}, recording gap {current}-{chunk_end} ({chunks_failed} failures so far)")
            current = chunk_end + 1
            continue

        current = chunk_end + 1
        chunks_done += 1

        if chunks_done % 20 == 0 or chunks_done == total_chunks:
            pct = chunks_done * 100 // max(total_chunks, 1)
            print(f"    {cfg['name']}: {pct}% ({chunks_done}/{total_chunks} chunks, {len(all_transfers):,} transfers)", flush=True)

        # Restore chunk size gradually
        if chunk_size < cfg["chunk_size"]:
            chunk_size = min(chunk_size * 2, cfg["chunk_size"])

        time.sleep(0.05)

    rpc_idx = (rpc_idx + 1) % len(rpcs)
    if chunks_failed > 0:
        total_attempted = chunks_done + chunks_failed
        fail_pct = chunks_failed * 100 // max(total_attempted, 1)
        print(f"  ⚠️ {cfg['name']}: {chunks_failed}/{total_attempted} chunks FAILED ({fail_pct}%)")
        print(f"     skipped block ranges: {skipped_ranges[:10]}{' …' if len(skipped_ranges) > 10 else ''}")
        if fail_pct > 50:
            print(f"  🚨 {cfg['name']}: >50% chunk failure rate! Data may be incomplete.")
    print(f"  ✅ {cfg['name']}: {len(all_transfers):,} transfers found")
    return all_transfers, end_block, skipped_ranges


def apply_transfers(balances, transfers):
    """Apply transfer events to balance map."""
    zero = ZERO.lower()
    max_block = 0
    for from_addr, to_addr, value_wei, block_num in transfers:
        value = value_wei / (10 ** 18)
        if block_num > max_block:
            max_block = block_num
        if from_addr != zero:
            balances[from_addr] = balances.get(from_addr, 0) - value
        if to_addr != zero:
            balances[to_addr] = balances.get(to_addr, 0) + value
    return balances, max_block


def merge_holders(eth_balances, bera_balances, forced_addrs):
    """Merge holders from both chains into a single list."""
    all_addrs = set(eth_balances.keys()) | set(bera_balances.keys())

    holders = []
    for addr in all_addrs:
        bal_eth = eth_balances.get(addr, 0)
        bal_bera = bera_balances.get(addr, 0)

        if bal_eth < MIN_BALANCE:
            bal_eth = 0
        if bal_bera < MIN_BALANCE:
            bal_bera = 0
        total = round(bal_eth + bal_bera, 4)
        if total < MIN_BALANCE and addr not in forced_addrs:
            continue

        chains = []
        if bal_eth >= MIN_BALANCE:
            chains.append("eth")
        if bal_bera >= MIN_BALANCE:
            chains.append("bera")

        holders.append({
            "address": addr,
            "balance": total,
            "balance_eth": round(bal_eth, 4),
            "balance_bera": round(bal_bera, 4),
            "chains": chains,
        })

    holders.sort(key=lambda h: h["balance"], reverse=True)
    for i, h in enumerate(holders, 1):
        h["rank"] = i

    return holders


def _multicall_dolo_balances(rpcs, addresses):
    """Fast path: DOLO balanceOf via Multicall3 aggregate3 — ONE eth_call per
    chunk instead of one per address.

    Returns (resolved, unresolved): `resolved` maps address -> raw uint256
    balance; `unresolved` lists addresses Multicall3 could not cleanly resolve
    (reverted/short sub-call or unreachable endpoint), which the caller sends
    through the per-address fallback so the failed-vs-zero handling still
    applies. Data-identical to individual balanceOf calls — only request count
    drops. If web3 is unavailable, every address defers to the fallback.
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

    token = Web3.to_checksum_address(DOLO_CONTRACT)
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


def fetch_chain_dolo_balances(chain, addresses):
    """Fetch current DOLO balances for candidate addresses on one chain."""
    unique = sorted({_lower_address(addr) for addr in addresses if _lower_address(addr)})
    if not unique:
        return {}

    cfg = CHAINS[chain]
    balances = {}

    # Fast path: Multicall3 batches balanceOf into one eth_call per chunk.
    resolved, pending_addrs = _multicall_dolo_balances(cfg["rpcs"], unique)
    for addr, raw_balance in resolved.items():
        balances[addr] = raw_balance / 1e18
    if not pending_addrs:
        return balances

    bal_selector = "0x70a08231"  # balanceOf(address)
    payloads = []
    meta_by_id = {}
    for idx, addr in enumerate(pending_addrs):
        data_hex = bal_selector + addr.replace("0x", "").zfill(64)
        request_id = f"{chain}:{idx}"
        payloads.append({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": DOLO_CONTRACT, "data": data_hex}, "latest"],
            "id": request_id,
        })
        meta_by_id[request_id] = addr

    try:
        responses, missing_ids = rpc_batch_requests(
            cfg["rpcs"],
            payloads,
            timeout=10,
            retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
            batch_size=RPC_BATCH_SIZE,
            quiet=True,
            describe=f"{chain} DOLO balanceOf reconciliation",
        )
    except RpcError:
        responses, missing_ids = {}, [payload["id"] for payload in payloads]

    for payload in payloads:
        request_id = payload["id"]
        addr = meta_by_id[request_id]
        response = responses.get(request_id)
        if request_id in missing_ids or not isinstance(response, dict) or response.get("error") or "result" not in response:
            try:
                response = rpc_single_request(
                    cfg["rpcs"],
                    payload,
                    timeout=10,
                    retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                    quiet=True,
                    describe=f"{chain} DOLO balanceOf reconciliation fallback",
                )
            except RpcError:
                continue
        if not isinstance(response, dict) or response.get("error"):
            continue
        balances[addr] = decode_uint256(response.get("result", "0x0")) / 1e18

    return balances


def reconcile_candidate_balances(eth_balances, bera_balances, candidate_addrs):
    """Patch holder state with verified current balances for flow candidates."""
    candidates = {_lower_address(addr) for addr in candidate_addrs if _lower_address(addr)}
    if not candidates:
        return eth_balances, bera_balances, {"candidates": 0, "added": 0, "updated": 0}

    print(f"\n🧩 Reconciling {len(candidates):,} flow candidate addresses with on-chain balanceOf()...")
    stats = {"candidates": len(candidates), "added": 0, "updated": 0}

    for chain, target in (("eth", eth_balances), ("bera", bera_balances)):
        chain_balances = fetch_chain_dolo_balances(chain, candidates)
        for addr, onchain_bal in chain_balances.items():
            old_bal = float(target.get(addr, 0) or 0)
            if abs(onchain_bal - old_bal) <= 0.0001:
                continue
            if old_bal < MIN_BALANCE <= onchain_bal:
                stats["added"] += 1
            else:
                stats["updated"] += 1
            target[addr] = onchain_bal

    print(f"  ✅ Reconciled candidates: {stats['added']} added, {stats['updated']} updated")
    return eth_balances, bera_balances, stats


def verify_top_balances(holders, eth_balances, bera_balances, forced_addrs, max_check=200):
    """Verify top holders' balances against on-chain balanceOf().
    Fixes any residual discrepancies."""
    print(f"\n🔎 Verifying top {max_check} holders with on-chain balanceOf()...")

    BALANCE_OF_SEL = "0x70a08231"
    RPCs = {
        "eth": CHAINS["eth"]["rpcs"],
        "bera": CHAINS["bera"]["rpcs"],
    }

    to_check = holders[:max_check]
    corrections = 0

    def apply_balance(chain, holder, onchain_bal):
        nonlocal corrections
        addr = holder["address"].lower()
        bal_key = f"balance_{chain}"
        old_bal = holder.get(bal_key, 0)

        if old_bal > 0 and abs(onchain_bal - old_bal) / max(old_bal, 1) > 0.01:
            holder[bal_key] = round(onchain_bal, 4)
            corrections += 1
            if chain == "eth":
                eth_balances[addr] = onchain_bal
            else:
                bera_balances[addr] = onchain_bal
        elif old_bal == 0 and onchain_bal >= MIN_BALANCE:
            holder[bal_key] = round(onchain_bal, 4)
            if chain not in holder["chains"]:
                holder["chains"].append(chain)
            corrections += 1
            if chain == "eth":
                eth_balances[addr] = onchain_bal
            else:
                bera_balances[addr] = onchain_bal

    for chain, rpcs in RPCs.items():
        # Fast path: Multicall3 batches balanceOf into one eth_call per chunk.
        resolved, unresolved = _multicall_dolo_balances(rpcs, [h["address"].lower() for h in to_check])
        for h in to_check:
            raw = resolved.get(h["address"].lower())
            if raw is not None:
                apply_balance(chain, h, raw / 1e18)
        if not unresolved:
            continue
        pending_set = set(unresolved)
        pending_holders = [h for h in to_check if h["address"].lower() in pending_set]

        # Fallback (unchanged): per-address eth_call for holders Multicall3 missed.
        payloads = []
        meta_by_id = {}
        for idx, h in enumerate(pending_holders):
            addr = h["address"].lower()
            data_hex = BALANCE_OF_SEL + addr.replace("0x", "").zfill(64)
            request_id = f"{chain}:{idx}"
            payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": DOLO_CONTRACT, "data": data_hex}, "latest"],
                "id": request_id,
            })
            meta_by_id[request_id] = h

        try:
            responses, missing_ids = rpc_batch_requests(
                rpcs,
                payloads,
                timeout=10,
                retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                batch_size=RPC_BATCH_SIZE,
                quiet=True,
                describe=f"{chain} DOLO balanceOf",
            )
        except RpcError:
            responses, missing_ids = {}, [payload["id"] for payload in payloads]

        for payload in payloads:
            request_id = payload["id"]
            h = meta_by_id[request_id]
            r = responses.get(request_id)
            if request_id in missing_ids or not isinstance(r, dict) or r.get("error") or "result" not in r:
                try:
                    r = rpc_single_request(
                        rpcs,
                        payload,
                        timeout=10,
                        retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                        quiet=True,
                        describe=f"{chain} DOLO balanceOf fallback",
                    )
                except RpcError:
                    continue
            if not isinstance(r, dict) or r.get("error"):
                continue
            onchain_bal = decode_uint256(r.get("result", "0x0")) / 1e18
            apply_balance(chain, h, onchain_bal)

    for h in holders[:max_check]:
        h["balance"] = round(h.get("balance_eth", 0) + h.get("balance_bera", 0), 4)
        h["chains"] = []
        if h.get("balance_eth", 0) >= MIN_BALANCE:
            h["chains"].append("eth")
        if h.get("balance_bera", 0) >= MIN_BALANCE:
            h["chains"].append("bera")

    holders = [h for h in holders if h["balance"] >= MIN_BALANCE or h["address"].lower() in forced_addrs]
    holders.sort(key=lambda h: h["balance"], reverse=True)
    for i, h in enumerate(holders, 1):
        h["rank"] = i

    print(f"  ✅ Corrected {corrections} balances via on-chain verification")
    return holders, eth_balances, bera_balances


def detect_contracts(holders, max_check=200, min_balance=None):
    """Detect contracts and Safes for the requested holder coverage."""
    eligible = holders
    if min_balance is not None:
        eligible = [
            holder for holder in holders
            if float(holder.get("balance") or 0) >= float(min_balance)
        ]
    to_check = eligible if max_check is None else eligible[:max_check]
    scope = f"{len(to_check):,} holders"
    if min_balance is not None:
        scope += f" at or above {float(min_balance):,.0f} DOLO"
    print(f"\n🔍 Detecting contracts across {scope}...")

    RPC_URLS = [
        ("eth", CHAINS["eth"]["rpcs"]),
        ("bera", CHAINS["bera"]["rpcs"]),
    ]

    contract_addrs = set()
    contract_wallet_types = {}
    contract_wallet_implementations = {}
    contract_wallet_entry_points = {}
    delegated_eoa_addrs = {}

    for chain, rpcs in RPC_URLS:
        code_payloads = []
        meta_by_id = {}
        for idx, h in enumerate(to_check):
            addr = h["address"]
            request_id = f"{chain}:code:{idx}"
            code_payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_getCode",
                "params": [addr, "latest"],
                "id": request_id,
            })
            meta_by_id[request_id] = addr

        try:
            code_responses, code_missing = rpc_batch_requests(
                rpcs,
                code_payloads,
                timeout=5,
                retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                batch_size=RPC_BATCH_SIZE,
                quiet=True,
                describe=f"{chain} eth_getCode",
            )
        except RpcError:
            code_responses, code_missing = {}, [payload["id"] for payload in code_payloads]

        chain_contract_addrs = []
        chain_smart_account_candidates = set()
        for payload in code_payloads:
            request_id = payload["id"]
            addr = meta_by_id[request_id]
            response = code_responses.get(request_id)
            if request_id in code_missing or not isinstance(response, dict) or response.get("error") or "result" not in response:
                try:
                    response = rpc_single_request(
                        rpcs,
                        payload,
                        timeout=5,
                        retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                        quiet=True,
                        describe=f"{chain} eth_getCode fallback",
                    )
                except RpcError:
                    continue
            code = str(response.get("result", "0x") if isinstance(response, dict) else "0x")
            delegation_address = eip7702_delegation_address(code)
            if delegation_address:
                delegated_eoa_addrs[addr.lower()] = delegation_address
                continue
            if code and len(code) > 4:
                key = addr.lower()
                contract_addrs.add(key)
                chain_contract_addrs.append(addr)
                if is_erc1967_smart_account_proxy(code):
                    chain_smart_account_candidates.add(key)

        entry_point_payloads = []
        entry_point_meta = {}
        chain_entry_points = {}
        for idx, addr in enumerate(chain_contract_addrs):
            if addr.lower() not in chain_smart_account_candidates:
                continue
            request_id = f"{chain}:entrypoint:{idx}"
            entry_point_payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": addr, "data": ERC4337_ENTRY_POINT_CALLDATA}, "latest"],
                "id": request_id,
            })
            entry_point_meta[request_id] = addr.lower()

        if entry_point_payloads:
            try:
                entry_point_responses, entry_point_missing = rpc_batch_requests(
                    rpcs,
                    entry_point_payloads,
                    timeout=5,
                    retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                    batch_size=RPC_BATCH_SIZE,
                    quiet=True,
                    describe=f"{chain} ERC-4337 entryPoint",
                )
            except RpcError:
                entry_point_responses, entry_point_missing = {}, [
                    payload["id"] for payload in entry_point_payloads
                ]

            for payload in entry_point_payloads:
                request_id = payload["id"]
                response = entry_point_responses.get(request_id)
                if (
                    request_id in entry_point_missing
                    or not isinstance(response, dict)
                    or response.get("error")
                    or "result" not in response
                ):
                    try:
                        response = rpc_single_request(
                            rpcs,
                            payload,
                            timeout=5,
                            retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                            quiet=True,
                            describe=f"{chain} ERC-4337 entryPoint fallback",
                        )
                    except RpcError:
                        continue
                value = str(
                    response.get("result", "0x") if isinstance(response, dict) else "0x"
                ).lower()
                entry_point = "0x" + value[-40:] if len(value) >= 42 else ""
                if entry_point in KNOWN_ERC4337_ENTRY_POINTS:
                    chain_entry_points[entry_point_meta[request_id]] = entry_point

        storage_payloads = []
        storage_meta = {}
        for idx, addr in enumerate(chain_contract_addrs):
            request_id = f"{chain}:storage:safe:{idx}"
            storage_payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_getStorageAt",
                "params": [addr, "0x0", "latest"],
                "id": request_id,
            })
            storage_meta[request_id] = (addr.lower(), "safe")
            if addr.lower() in chain_smart_account_candidates:
                request_id = f"{chain}:storage:implementation:{idx}"
                storage_payloads.append({
                    "jsonrpc": "2.0",
                    "method": "eth_getStorageAt",
                    "params": [addr, ERC1967_IMPLEMENTATION_SLOT, "latest"],
                    "id": request_id,
                })
                storage_meta[request_id] = (addr.lower(), "implementation")

        if storage_payloads:
            try:
                storage_responses, storage_missing = rpc_batch_requests(
                    rpcs,
                    storage_payloads,
                    timeout=5,
                    retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                    batch_size=RPC_BATCH_SIZE,
                    quiet=True,
                    describe=f"{chain} eth_getStorageAt",
                )
            except RpcError:
                storage_responses, storage_missing = {}, [payload["id"] for payload in storage_payloads]

            for payload in storage_payloads:
                request_id = payload["id"]
                response = storage_responses.get(request_id)
                if request_id in storage_missing or not isinstance(response, dict) or response.get("error") or "result" not in response:
                    try:
                        response = rpc_single_request(
                            rpcs,
                            payload,
                            timeout=5,
                            retries_per_endpoint=RPC_RETRIES_PER_ENDPOINT,
                            quiet=True,
                            describe=f"{chain} eth_getStorageAt fallback",
                        )
                    except RpcError:
                        continue
                value = str(response.get("result", "0x") if isinstance(response, dict) else "0x").lower()
                address, storage_kind = storage_meta[request_id]
                decoded_address = "0x" + value[-40:] if len(value) >= 42 else ""
                if storage_kind == "safe" and decoded_address in SAFE_SINGLETON_ADDRS:
                    contract_wallet_types[address] = "safe"
                elif (
                    storage_kind == "implementation"
                    and decoded_address in VERIFIED_SMART_ACCOUNT_IMPLEMENTATIONS
                    and address in chain_entry_points
                    and contract_wallet_types.get(address) != "safe"
                ):
                    contract_wallet_types[address] = "smart_account"
                    contract_wallet_implementations[address] = decoded_address
                    contract_wallet_entry_points[address] = chain_entry_points[address]

    for h in holders:
        key = h["address"].lower()
        if key in contract_addrs:
            h["is_contract"] = True
        elif key in delegated_eoa_addrs:
            h.pop("is_contract", None)
            h["contract_wallet_type"] = "delegated_eoa"
            h["delegation_address"] = delegated_eoa_addrs[key]
        if key in contract_wallet_types:
            h["contract_wallet_type"] = contract_wallet_types[key]
        if key in contract_wallet_implementations:
            h["contract_wallet_implementation"] = contract_wallet_implementations[key]
        if key in contract_wallet_implementations and key in contract_wallet_entry_points:
            h["contract_wallet_entry_point"] = contract_wallet_entry_points[key]

    print(f"  ✅ Found {len(contract_addrs)} contracts across {scope}")
    return holders


# ===== MAIN =====
def verify_holder_chart_contracts_only():
    """Extend contract/Safe coverage without re-scanning token transfer logs."""
    if not os.path.exists(OUTPUT_JSON):
        raise RuntimeError(f"Missing holder output: {OUTPUT_JSON}")
    with open(OUTPUT_JSON) as f:
        output = json.load(f)
    holders = output.get("holders")
    if not isinstance(holders, list):
        raise RuntimeError("Holder output does not contain a holders list")

    eligible = [
        holder for holder in holders
        if float(holder.get("balance") or 0) >= HOLDER_DISTRIBUTION_MIN_BALANCE
    ]
    previous_coverage = output.get("holder_chart_contract_coverage") or {}
    can_resume = (
        previous_coverage.get("status") == "in_progress"
        and float(previous_coverage.get("minimumBalance") or 0) == HOLDER_DISTRIBUTION_MIN_BALANCE
        and previous_coverage.get("sourceTimestamp") == output.get("timestamp")
    )
    start = int(previous_coverage.get("nextOffset") or 0) if can_resume else 0
    start = max(0, min(start, len(eligible)))
    end = min(len(eligible), start + max(1, HOLDER_CHART_CONTRACT_VERIFY_BATCH_SIZE))
    batch = eligible[start:end]
    if batch:
        print(
            f"🔍 Checking holder-chart contracts {start + 1:,}-{end:,} of {len(eligible):,}...",
            flush=True,
        )
        detect_contracts(batch, max_check=None)

    output["holders"] = holders
    stats = output.setdefault("stats", {})
    output["holder_chart_contract_coverage"] = {
        "minimumBalance": HOLDER_DISTRIBUTION_MIN_BALANCE,
        "sourceTimestamp": output.get("timestamp"),
        "verifiedAt": datetime.utcnow().isoformat() + "Z",
        "verifiedWallets": end,
        "totalWallets": len(eligible),
        "nextOffset": end,
        "status": "complete" if end == len(eligible) else "in_progress",
    }
    if end == len(eligible):
        stats["contracts_detected"] = sum(1 for holder in holders if holder.get("is_contract"))
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(
        f"✅ Contract/Safe coverage: {end:,}/{len(eligible):,} holders at or above "
        f"{HOLDER_DISTRIBUTION_MIN_BALANCE:,.0f} DOLO "
        f"({output['holder_chart_contract_coverage']['status']})"
    )


def main():
    if "--verify-holder-chart-contracts-only" in sys.argv[1:]:
        verify_holder_chart_contracts_only()
        return
    print("=" * 60)
    print("🔄 DOLO Token Holders — Generator (RPC eth_getLogs)")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    state = load_state()
    is_incremental = bool(state)

    if is_incremental:
        print("📦 Found previous state — running incremental sync")
    else:
        print("🆕 No previous state — running full sync (first run)")

    eth_balances = state.get("eth_balances", {})
    bera_balances = state.get("bera_balances", {})
    eth_last_block = state.get("eth_last_block", CHAINS["eth"]["start_block"])
    bera_last_block = state.get("bera_last_block", CHAINS["bera"]["start_block"])

    # Re-scan any block ranges that persistently failed on previous runs. last_block
    # advances past gaps, so without this the missed transfers would never be applied.
    eth_skipped = []
    bera_skipped = []

    def rescan_gaps(chain_key, gaps):
        recovered = []
        still_failed = []
        for gap in gaps:
            try:
                g_start, g_end = int(gap[0]), int(gap[1])
            except (TypeError, ValueError, IndexError):
                continue
            if g_start >= g_end:
                continue
            print(f"  ↻ {CHAINS[chain_key]['name']}: re-scanning skipped range {g_start:,}-{g_end:,}")
            g_txs, _, g_skipped = fetch_transfer_logs(chain_key, start_block=g_start, end_block=g_end)
            recovered.extend(g_txs)
            still_failed.extend(g_skipped)
        return recovered, still_failed

    prev_eth_gaps = state.get("skipped_ranges_eth", [])
    prev_bera_gaps = state.get("skipped_ranges_bera", [])
    eth_gap_txs, eth_gap_remaining = rescan_gaps("eth", prev_eth_gaps) if prev_eth_gaps else ([], [])
    bera_gap_txs, bera_gap_remaining = rescan_gaps("bera", prev_bera_gaps) if prev_bera_gaps else ([], [])
    eth_skipped.extend(eth_gap_remaining)
    bera_skipped.extend(bera_gap_remaining)

    # Fetch new Transfer events via eth_getLogs
    print("\n📡 Fetching Transfer events via RPC logs...")
    eth_txs, eth_end, eth_new_skipped = fetch_transfer_logs("eth", start_block=eth_last_block)
    bera_txs, bera_end, bera_new_skipped = fetch_transfer_logs("bera", start_block=bera_last_block)
    eth_txs = eth_gap_txs + eth_txs
    bera_txs = bera_gap_txs + bera_txs
    eth_skipped.extend(eth_new_skipped)
    bera_skipped.extend(bera_new_skipped)

    if not eth_txs and not bera_txs and not is_incremental:
        print("⚠️  No transfers found on any chain!")
        sys.exit(1)

    # Apply new transfers
    print("\n📊 Applying transfers...")
    if eth_txs:
        eth_balances, eth_max = apply_transfers(eth_balances, eth_txs)
        eth_last_block = max(eth_last_block, eth_max, eth_end)
        print(f"  ETH: {len(eth_txs):,} transfers applied, now at block {eth_last_block:,}")

    if bera_txs:
        bera_balances, bera_max = apply_transfers(bera_balances, bera_txs)
        bera_last_block = max(bera_last_block, bera_max, bera_end)
        print(f"  BERA: {len(bera_txs):,} transfers applied, now at block {bera_last_block:,}")

    eth_clean = {a: round(b, 4) for a, b in eth_balances.items() if b >= MIN_BALANCE}
    bera_clean = {a: round(b, 4) for a, b in bera_balances.items() if b >= MIN_BALANCE}
    print(f"  ETH holders: {len(eth_clean):,} | BERA holders: {len(bera_clean):,}")

    forced_addrs = load_investors()
    if forced_addrs:
        print(f"  📌 Forcing inclusion of {len(forced_addrs)} investor addresses even if balance is 0")

    flow_candidates = load_flow_reconciliation_candidates()
    eth_balances, bera_balances, _reconcile_stats = reconcile_candidate_balances(
        eth_balances,
        bera_balances,
        flow_candidates,
    )

    # Merge
    print("\n🔀 Merging holders across chains...")
    holders = merge_holders(eth_balances, bera_balances, forced_addrs)

    # Verify top holders on-chain
    holders, eth_balances, bera_balances = verify_top_balances(
        holders, eth_balances, bera_balances, forced_addrs, max_check=200
    )

    # Detect contracts
    holders = detect_contracts(
        holders,
        max_check=None,
        min_balance=HOLDER_DISTRIBUTION_MIN_BALANCE,
    )

    # Checksum addresses
    try:
        from web3 import Web3
        for h in holders:
            try:
                h["address"] = Web3.to_checksum_address(h["address"])
            except Exception:
                pass
    except ImportError:
        pass

    # Stats
    eth_only = sum(1 for h in holders if h["chains"] == ["eth"])
    bera_only = sum(1 for h in holders if h["chains"] == ["bera"])
    both_chains = sum(1 for h in holders if len(h["chains"]) == 2)
    total_supply = sum(h["balance"] for h in holders)
    contracts = sum(1 for h in holders if h.get("is_contract"))

    stats = {
        "total_holders": len(holders),
        "eth_only": eth_only,
        "bera_only": bera_only,
        "both_chains": both_chains,
        "total_supply_tracked": round(total_supply, 2),
        "contracts_detected": contracts,
    }

    output = {
        "contract": DOLO_CONTRACT,
        "networks": ["ethereum", "berachain"],
        "timestamp": datetime.utcnow().isoformat(),
        "stats": stats,
        "holders": holders,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    # Save state for incremental sync. Persist any still-unscanned gaps (bounded) so
    # the next run retries them; clears automatically once a range scans cleanly.
    save_state({
        "eth_balances": {a: b for a, b in eth_balances.items() if abs(b) > 0.0001},
        "bera_balances": {a: b for a, b in bera_balances.items() if abs(b) > 0.0001},
        "eth_last_block": eth_last_block,
        "bera_last_block": bera_last_block,
        "skipped_ranges_eth": eth_skipped[-200:],
        "skipped_ranges_bera": bera_skipped[-200:],
    })
    if eth_skipped or bera_skipped:
        print(f"  ⚠️ Unscanned gaps carried to next run — eth: {len(eth_skipped)}, bera: {len(bera_skipped)}")

    print(f"\n💾 Saved: {OUTPUT_JSON}")
    print(f"   Total holders: {len(holders):,}")
    print(f"   ETH only: {eth_only:,} | BERA only: {bera_only:,} | Both: {both_chains:,}")
    print(f"   Supply tracked: {total_supply:,.2f} DOLO")
    print(f"   Contracts: {contracts}")
    print(f"\n🏆 TOP 5:")
    for h in holders[:5]:
        tag = " 📜" if h.get("is_contract") else ""
        print(f"   #{h['rank']:<4} {h['address'][:14]}… {h['balance']:>15,.2f} DOLO{tag}")
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
