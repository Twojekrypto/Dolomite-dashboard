#!/usr/bin/env python3
"""
veDOLO Events Pipeline — Deposit (Lock) & Withdraw (Unlock) events
Scans on-chain events from veDOLO contract deployment, outputs JSON for frontend.
Uses incremental sync — first run scans all, subsequent runs fetch only new blocks.
"""
import argparse
import json, time, os, sys
import requests
import rpc_usage
from datetime import datetime, timezone


# ===== CONFIG =====
VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
WITHDRAW_TOPIC = "0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94"
DEPOSIT_TOPIC = "0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624"

# oDOLO Vester — locks via oDOLO exercise go through this contract
ODOLO_VESTER = "0x3e9b9a16743551da49b5e136c716bba7932d2cec".lower()
AIRDROP_CLAIM_CONTRACTS = {
    "0xa3f079292cc35ba64996fe0bce3049928a838bc9",
}
# ERC-20/ERC-721 Transfer topic; useful for finding the real veDOLO NFT recipient
# and wallet-to-wallet veDOLO position transfers.
ODOLO_EXERCISE_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRANSFER_TOPIC = ODOLO_EXERCISE_TOPIC
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_TOPIC = "0x" + ("0" * 64)
POSITION_ACTION_SELECTORS = {
    4: "d1c2babb",  # merge(uint256,uint256)
    5: "4b19becc",  # split(uint256,uint256)
}

# Single source of truth for endpoints (env-injected Alchemy keys first).
from rpc_client import get_endpoints as _rpc_endpoints
RPC_URLS = _rpc_endpoints("berachain")

DEPLOY_BLOCK = 2_925_000  # veDOLO contract first events
# Keep production ranges within the strictest healthy public provider limit.
# Larger requests have returned a successful but incomplete `[]` response.
CHUNK_SIZE = 10_000
MIN_CHUNK_SIZE = 1_000
FINALITY_REORG_DEPTH = 256
BLOCK_TIME = 2  # ~2 seconds per block on Berachain
TRANSFERS_SCHEMA_VERSION = 2

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(DATA_DIR, "vedolo_flows.json")
STATE_FILE = os.path.join(DATA_DIR, "vedolo_flows_state.json")
RUN_STATUS_FILE = os.path.join(DATA_DIR, "vedolo_flows_run_status.json")
EXERCISERS_BY_ADDRESS_FILE = os.path.join(DATA_DIR, "exercisers_by_address.json")


class EventLogFetchError(RuntimeError):
    """Raised when an event range cannot be fetched completely."""


def _valid_address(value):
    text = str(value or "").lower()
    return len(text) == 42 and text.startswith("0x") and all(
        char in "0123456789abcdef" for char in text[2:]
    )


def transfer_rows_are_complete(transfers, target_block=0):
    """Return whether every transfer is safe for deterministic history replay."""
    if not isinstance(transfers, list) or not transfers:
        return False
    seen_logs = set()
    for row in transfers:
        if not isinstance(row, dict):
            return False
        try:
            block = int(row.get("block") or 0)
            log_index = int(row["logIndex"])
            timestamp = int(row.get("timestamp") or 0)
            token_id = int(row.get("tokenId") or 0)
        except (KeyError, TypeError, ValueError):
            return False
        tx_hash = str(row.get("txHash") or "").lower()
        expected_date = (
            datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
            if timestamp > 0
            else ""
        )
        if (
            block <= 0
            or (int(target_block or 0) > 0 and block > int(target_block))
            or log_index < 0
            or timestamp <= 0
            or token_id <= 0
            or len(tx_hash) != 66
            or not tx_hash.startswith("0x")
            or any(char not in "0123456789abcdef" for char in tx_hash[2:])
            or not _valid_address(row.get("from"))
            or not _valid_address(row.get("to"))
            or str(row.get("date") or "") != expected_date
        ):
            return False
        identity = (tx_hash, log_index)
        if identity in seen_logs:
            return False
        seen_logs.add(identity)
    return True


def assert_complete_transfer_history(transfers, target_block):
    """Fail closed rather than certify an incomplete transfer replay cache."""
    if transfer_rows_are_complete(transfers, target_block):
        return
    raise EventLogFetchError(
        "veDOLO transfer history is incomplete (block/logIndex/timestamp/date/identity); "
        "refusing to publish transfer schema v2"
    )


def transfer_cache_needs_full_backfill(state, cached_transfers):
    return (
        not transfer_rows_are_complete(
            cached_transfers, int(state.get("transfers_last_block") or 0)
        )
        or int(state.get("transfers_last_block") or 0) <= 0
        or int(state.get("transfers_schema_version") or 0) != TRANSFERS_SCHEMA_VERSION
    )

# Historical wallet-to-wallet veDOLO NFT transfers verified by receipt on
# Berachain. Some public RPC log scans missed this narrow July 2025 range, so
# keep these rows as an explicit backfill until the upstream scan is rebuilt.
MANUAL_TRANSFER_BACKFILLS = [
    {
        "from": "0x0365566ef442e63ea7e6905afde6bb749c845fa6",
        "to": "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
        "txHash": "0x46a17abc0fe071562c4c93e657c19ef19b8dbc554a0f0b300847589273fbe2eb",
        "tokenId": 414,
        "block": 8214230,
        "logIndex": 85,
        "timestamp": 1753463628,
        "date": "2025-07-25",
    },
    {
        "from": "0xbdfbecf4600101d4ab18d2c996cc5cfd7c68d40c",
        "to": "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
        "txHash": "0x8c53ef65104a527aa102b47a15e606aa77333fd6efa9cdb17819da71b13553e6",
        "tokenId": 464,
        "block": 8214306,
        "logIndex": 86,
        "timestamp": 1753463773,
        "date": "2025-07-25",
    },
    {
        "from": "0x2d7ea78fd36eba022f94ae14e85225736580b319",
        "to": "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
        "txHash": "0xb7d88e234f0563cc3e65ab90ea64b7f284723ca9629f8e2b278dc38ab87484c9",
        "tokenId": 442,
        "block": 8214373,
        "logIndex": 168,
        "timestamp": 1753463900,
        "date": "2025-07-25",
    },
    {
        "from": "0x1d9da1fc0f98611f5f7b254ece4f2f40738544a8",
        "to": "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
        "txHash": "0xe8863306f9a4d9490f9efaa862658af2885561312a0585a3ac5841dedc60175b",
        "tokenId": 470,
        "block": 8214432,
        "logIndex": 8,
        "timestamp": 1753464014,
        "date": "2025-07-25",
    },
    {
        "from": "0x04d04c019da3b971928e53847e9c4bbbde82d96e",
        "to": "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
        "txHash": "0x566a3e5a226a577910a80ed5084c09e2fbea1abc8438d912f46115d5d0c490d6",
        "tokenId": 475,
        "block": 8214470,
        "logIndex": 27,
        "timestamp": 1753464086,
        "date": "2025-07-25",
    },
]

MANUAL_LOCK_BENEFICIARY_BACKFILLS = {
    # Receipt 0x07aa...020c minted veDOLO #422 directly to this wallet even
    # though the Deposit provider was an intermediate contract.
    422: "0x28da3dde285d8f1f87b2d858f89961bb8b9af180",
}

# Exact Deposit blocks for active/expired veDOLO positions whose historical
# events were skipped by an older incremental state. Each entry was audited
# against the token's zero-address mint and the Deposit log in the same receipt.
# The repair fetches the source log again and fails closed unless it finds one
# exact Deposit event for the expected token ID.
AUDITED_MISSING_DEPOSIT_BLOCKS = {
    2277: 4_153_083,
    3988: 4_608_750,
    8280: 4_732_828,
    11432: 4_910_493,
    12724: 5_296_617,
    12726: 5_296_670,
    12727: 5_296_678,
    15001: 7_346_298,
    15277: 7_755_817,
    15678: 8_216_171,
    16554: 9_078_333,
    16555: 9_078_349,
    17152: 9_651_326,
    17158: 9_661_612,
    17538: 9_716_375,
    18780: 9_932_082,
    20955: 10_469_469,
    21090: 10_640_817,
    21228: 10_776_211,
    21430: 11_160_642,
    21536: 11_328_238,
    21841: 11_855_426,
    21842: 11_855_440,
    21844: 11_855_530,
    21846: 11_855_708,
    21918: 11_933_110,
    22648: 13_728_306,
    23208: 16_041_595,
    23278: 16_352_058,
    23292: 16_393_061,
    23984: 19_452_141,
    24184: 20_566_127,
    24202: 20_580_440,
    24293: 21_201_033,
    24309: 21_337_599,
    24349: 21_703_026,
    24481: 22_244_493,
    24482: 22_244_500,
    24483: 22_244_519,
    24484: 22_244_524,
    24485: 22_244_530,
    24486: 22_244_539,
    24487: 22_244_562,
    24488: 22_244_568,
    24489: 22_244_574,
    24518: 22_275_993,
    24923: 24_510_973,
    24930: 24_561_275,
    24973: 24_670_963,
}


def parse_args():
    parser = argparse.ArgumentParser(description="Generate veDOLO lock/unlock flow data")
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=int(os.environ.get("VEDOLO_FLOWS_MAX_RUNTIME_SECONDS") or 0),
        help="Exit cleanly after this many seconds, saving resumable state first.",
    )
    return parser.parse_args()


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception as exc:
            print(f"⚠️ load_state: failed to read {STATE_FILE} ({exc}); starting full resync", flush=True)
    return {}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def save_run_status(completed, **extra):
    payload = {
        "completed": bool(completed),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        **extra,
    }
    tmp = RUN_STATUS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, RUN_STATUS_FILE)


def save_pending_sync(state, target_block, unlocks, locks, tx_hashes, transfers=None):
    state["pending_vedolo_sync"] = {
        "target_block": target_block,
        "transfers_schema_version": TRANSFERS_SCHEMA_VERSION,
        "transfers_last_block": target_block,
        "unlocks": unlocks,
        "locks": locks,
        "transfers": transfers if isinstance(transfers, list) else state.get("transfers", []),
        "tx_hashes": sorted(tx_hashes),
        "updated": datetime.utcnow().isoformat() + "Z",
    }
    save_state(state)


def load_pending_sync(state):
    pending = state.get("pending_vedolo_sync")
    if not isinstance(pending, dict):
        return None

    target_block = int(pending.get("target_block") or 0)
    locks = pending.get("locks")
    unlocks = pending.get("unlocks")
    transfers = pending.get("transfers")
    tx_hashes = pending.get("tx_hashes")
    transfer_schema = int(pending.get("transfers_schema_version") or 0)
    transfers_last_block = int(pending.get("transfers_last_block") or 0)
    if target_block <= 0 or not isinstance(locks, list) or not isinstance(unlocks, list):
        return None
    if (
        transfer_schema != TRANSFERS_SCHEMA_VERSION
        or transfers_last_block < target_block
        or not transfer_rows_are_complete(transfers, target_block)
    ):
        # A receipt-check checkpoint created before the transfer schema upgrade
        # cannot bypass the required full ERC-721 Transfer rebuild.
        state.pop("pending_vedolo_sync", None)
        return None
    if not isinstance(transfers, list):
        transfers = []
    if not isinstance(tx_hashes, list):
        tx_hashes = []

    return {
        "target_block": target_block,
        "transfers_schema_version": transfer_schema,
        "transfers_last_block": transfers_last_block,
        "locks": locks,
        "unlocks": unlocks,
        "transfers": transfers,
        "tx_hashes": [str(tx).lower() for tx in tx_hashes],
    }


def load_odolo_exerciser_lookup():
    """Map oDOLO exercise tx hash -> end-user wallet."""
    if not os.path.exists(EXERCISERS_BY_ADDRESS_FILE):
        return {}
    try:
        with open(EXERCISERS_BY_ADDRESS_FILE) as f:
            payload = json.load(f)
    except Exception:
        return {}

    lookup = {}
    for entry in payload.get("exercisers", []) or []:
        address = str(entry.get("address") or "").strip().lower()
        if not address.startswith("0x") or len(address) != 42:
            continue
        for tx in entry.get("txs", []) or []:
            tx_hash = str(tx.get("hash") or "").strip().lower()
            if tx_hash.startswith("0x") and len(tx_hash) == 66 and tx_hash not in lookup:
                lookup[tx_hash] = address
    return lookup


def normalize_address(value):
    address = str(value or "").strip().lower()
    if address.startswith("0x") and len(address) == 42:
        return address
    return ""


def address_from_topic(topic):
    topic = str(topic or "").strip().lower()
    if topic.startswith("0x") and len(topic) == 66:
        return "0x" + topic[-40:]
    return ""


def extract_odolo_receipt_beneficiary(receipt):
    """Extract the end-user wallet from Vester/veDOLO receipt logs."""
    if not receipt or not receipt.get("logs"):
        return None

    for log in receipt["logs"]:
        if normalize_address(log.get("address")) != ODOLO_VESTER:
            continue
        for topic in log.get("topics", [])[1:]:
            address = address_from_topic(topic)
            if address and address not in {ZERO_ADDRESS, ODOLO_VESTER}:
                return address

    vedolo_lower = VEDOLO_CONTRACT.lower()
    for log in receipt["logs"]:
        if normalize_address(log.get("address")) != vedolo_lower:
            continue
        topics = log.get("topics", [])
        if len(topics) < 3 or str(topics[0]).lower() != ODOLO_EXERCISE_TOPIC:
            continue
        if str(topics[1]).lower() != ZERO_TOPIC:
            continue
        address = address_from_topic(topics[2])
        if address and address not in {ZERO_ADDRESS, ODOLO_VESTER}:
            return address

    return None


def remap_odolo_lock_beneficiaries(locks, exerciser_lookup):
    """Use oDOLO exercise metadata to attribute protocol-routed locks to users."""
    resolved = 0
    unresolved = 0

    for lock in locks:
        tx_hash = str(lock.get("txHash") or "").strip().lower()
        original_address = normalize_address(lock.get("address")) or ODOLO_VESTER
        protocol_address = normalize_address(lock.get("protocolAddress"))
        is_vester_provider = original_address == ODOLO_VESTER or protocol_address == ODOLO_VESTER
        lookup_beneficiary = exerciser_lookup.get(tx_hash)
        receipt_beneficiary = normalize_address(lock.get("beneficiaryAddress"))
        beneficiary = lookup_beneficiary or receipt_beneficiary
        if not lock.get("isOdolo") and not is_vester_provider and not beneficiary:
            continue

        lock["isOdolo"] = True
        lock["protocolAddress"] = ODOLO_VESTER
        if beneficiary:
            lock["address"] = beneficiary
            lock["beneficiaryAddress"] = beneficiary
            lock["addressSource"] = "odolo-exerciser" if lookup_beneficiary else "odolo-receipt"
            resolved += 1
        else:
            lock["address"] = ODOLO_VESTER
            lock["beneficiaryAddress"] = None
            lock["addressSource"] = "odolo-vester-fallback"
            unresolved += 1

    return resolved, unresolved


def is_capacity_error(error_obj):
    """True when a JSON-RPC error means the endpoint is rate/quota limited.

    These errors (e.g. Alchemy 429 "Monthly capacity limit exceeded") must
    rotate to the next endpoint — they are NOT block-range errors even though
    the message contains the word "limit".
    """
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


def is_rate_limited_exception(exc):
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) == 429:
        return True
    message = str(exc).lower()
    return any(token in message for token in (
        "429", "rate limit", "too many", "throttl", "capacity", "quota",
    ))


def rpc_call(method, params, timeout=15):
    """Call RPC with fallback across multiple endpoints."""
    for attempt in range(len(RPC_URLS) * 2):
        rpc = RPC_URLS[attempt % len(RPC_URLS)]
        try:
            resp = requests.post(rpc, json={
                "jsonrpc": "2.0", "method": method, "params": params, "id": 1
            }, timeout=timeout, headers={"Content-Type": "application/json"})
            data = resp.json()
            if "error" in data:
                err_obj = data["error"]
                err = err_obj.get("message", "")
                rpc_usage.record_provider_failure(
                    rpc,
                    rate_limited=is_capacity_error(err_obj),
                )
                if is_capacity_error(err_obj):
                    # Endpoint out of quota / rate-limited — try the next one.
                    time.sleep(0.3)
                    continue
                if "range" in err.lower() or "limit" in err.lower():
                    return {"error": err_obj}
                time.sleep(0.3)
                continue
            rpc_usage.record_request(method)
            rpc_usage.record_provider_success(rpc, served_methods=1)
            return data
        except requests.exceptions.Timeout:
            rpc_usage.record_provider_failure(rpc)
            time.sleep(1)
        except Exception as exc:
            rpc_usage.record_provider_failure(
                rpc,
                rate_limited=is_rate_limited_exception(exc),
            )
            time.sleep(0.3)
    return None


def parse_position_action_calldata(lock, input_data):
    """Resolve source/target token IDs for a merge or split Deposit."""
    deposit_type = int(lock.get("depositType") or -1)
    kind = "merge" if deposit_type == 4 else "split" if deposit_type == 5 else "position action"
    expected_selector = POSITION_ACTION_SELECTORS.get(deposit_type)
    payload = str(input_data or "").strip().lower()
    if not expected_selector or not payload.startswith("0x") or len(payload) < 138:
        raise EventLogFetchError(f"could not resolve {kind} transition: malformed calldata")
    if payload[2:10] != expected_selector:
        raise EventLogFetchError(f"could not resolve {kind} transition: unexpected selector")
    try:
        first_arg = int(payload[10:74], 16)
        second_arg = int(payload[74:138], 16)
        event_token_id = int(lock.get("tokenId") or 0)
    except (TypeError, ValueError) as exc:
        raise EventLogFetchError(f"could not resolve {kind} transition: invalid token ID") from exc

    source_token_id = first_arg
    target_token_id = second_arg if deposit_type == 4 else event_token_id
    if (
        source_token_id <= 0
        or target_token_id <= 0
        or source_token_id == target_token_id
        or target_token_id != event_token_id
    ):
        raise EventLogFetchError(f"could not resolve {kind} transition: token IDs do not reconcile")
    return {
        "sourceTokenId": source_token_id,
        "targetTokenId": target_token_id,
    }


def annotate_position_action_tokens(locks, state, fetcher=None):
    """Attach exact token transitions to every merge/split Deposit, failing closed."""
    cache = state.get("position_action_tokens")
    if not isinstance(cache, dict):
        cache = {}
        state["position_action_tokens"] = cache

    def fetch_transaction(tx_hash):
        if fetcher is not None:
            return fetcher(tx_hash)
        response = rpc_call("eth_getTransactionByHash", [tx_hash], timeout=15)
        return response.get("result") if isinstance(response, dict) else None

    annotated = 0
    for lock in locks or []:
        deposit_type = int(lock.get("depositType") or -1)
        if deposit_type not in POSITION_ACTION_SELECTORS:
            continue
        tx_hash = str(lock.get("txHash") or "").strip().lower()
        kind = "merge" if deposit_type == 4 else "split"
        if not tx_hash.startswith("0x") or len(tx_hash) != 66:
            raise EventLogFetchError(f"could not resolve {kind} transition: invalid transaction hash")

        transition = cache.get(tx_hash)
        if not isinstance(transition, dict):
            transaction = fetch_transaction(tx_hash)
            if not isinstance(transaction, dict):
                raise EventLogFetchError(f"could not resolve {kind} transition for {tx_hash}")
            transition = parse_position_action_calldata(lock, transaction.get("input"))
            cache[tx_hash] = transition

        source_token_id = int(transition.get("sourceTokenId") or 0)
        target_token_id = int(transition.get("targetTokenId") or 0)
        if target_token_id != int(lock.get("tokenId") or 0) or source_token_id <= 0:
            raise EventLogFetchError(f"could not resolve {kind} transition for {tx_hash}")
        lock["sourceTokenId"] = source_token_id
        lock["targetTokenId"] = target_token_id
        annotated += 1

    return annotated


def get_current_block():
    """Get current block number with robust retries."""
    for round_num in range(3):  # 3 full rounds across all RPCs
        for rpc in RPC_URLS:
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
                }, timeout=15, headers={"Content-Type": "application/json"})
                data = resp.json()
                if "error" in data:
                    rpc_usage.record_provider_failure(
                        rpc,
                        rate_limited=is_capacity_error(data["error"]),
                    )
                    print(
                        f"  ⚠️ RPC error from {rpc_usage.provider_name(rpc)}: {data['error']}"
                    )
                    continue
                block = int(data.get("result", "0x0"), 16)
                if block > 0:
                    rpc_usage.record_request("eth_blockNumber")
                    rpc_usage.record_provider_success(rpc, served_methods=1)
                    return block
                rpc_usage.record_provider_failure(rpc)
                print(f"  ⚠️ Got block 0 from {rpc_usage.provider_name(rpc)}")
            except requests.exceptions.Timeout:
                rpc_usage.record_provider_failure(rpc)
                print(f"  ⚠️ Timeout from {rpc_usage.provider_name(rpc)}")
            except Exception as e:
                rpc_usage.record_provider_failure(
                    rpc,
                    rate_limited=is_rate_limited_exception(e),
                )
                print(f"  ⚠️ Error from {rpc_usage.provider_name(rpc)}: {e}")
            time.sleep(0.5)
        if round_num < 2:
            print(f"  Retry round {round_num + 2}/3...")
            time.sleep(2)
    return 0



def _logs_with_topic0(logs, topic0):
    """Subset of `logs` whose topic0 equals `topic0` (case-insensitive)."""
    target = str(topic0).lower()
    return [
        log for log in logs
        if str((log.get("topics") or [None])[0]).lower() == target
    ]


def fetch_event_logs(start_block, end_block, topic):
    """Fetch event logs from the veDOLO contract.

    `topic` may be a single topic0 (str) or a list of topic0 values. A list is
    sent as an OR-match so several event types share ONE eth_getLogs request
    (e.g. Deposit + Withdraw), then the caller splits by topic0. A single string
    is wrapped to an exact match, so existing callers are unchanged.
    """
    topic0_filter = list(topic) if isinstance(topic, (list, tuple)) else [topic]
    chunk_size = CHUNK_SIZE
    if start_block > end_block:
        return []

    total_blocks = end_block - start_block
    print(f"  Scanning blocks {start_block:,} → {end_block:,} ({total_blocks:,} blocks)")

    all_logs = []
    current = start_block
    chunks_done = 0

    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)

        success = False
        last_error = ""
        accepted_logs = None
        empty_endpoints = set()
        restart_with_smaller_chunk = False
        unique_rpc_count = len(set(RPC_URLS))

        for attempt in range(len(RPC_URLS) * 2):
            rpc = RPC_URLS[attempt % len(RPC_URLS)]
            try:
                resp = requests.post(rpc, json={
                    "jsonrpc": "2.0", "method": "eth_getLogs",
                    "params": [{
                        "address": VEDOLO_CONTRACT,
                        "topics": [topic0_filter],
                        "fromBlock": hex(current),
                        "toBlock": hex(chunk_end),
                    }], "id": 1
                }, timeout=30, headers={"Content-Type": "application/json"})

                r = resp.json()
                if "error" in r:
                    err_msg = r["error"].get("message", "")
                    last_error = err_msg or str(r["error"])
                    rpc_usage.record_provider_failure(
                        rpc,
                        rate_limited=is_capacity_error(r["error"]),
                    )
                    if is_capacity_error(r["error"]):
                        # Endpoint out of quota / rate-limited — rotate, do
                        # not shrink the chunk (it is not a range problem).
                        time.sleep(0.5)
                        continue
                    if (
                        ("range" in err_msg.lower() or "limit" in err_msg.lower())
                        and chunk_size > MIN_CHUNK_SIZE
                    ):
                        chunk_size = max(chunk_size // 2, MIN_CHUNK_SIZE)
                        restart_with_smaller_chunk = True
                        break
                    time.sleep(0.5)
                    continue

                logs = r.get("result")
                if not isinstance(logs, list):
                    rpc_usage.record_provider_failure(rpc)
                    last_error = "malformed eth_getLogs result"
                    continue

                rpc_usage.record_request("eth_getLogs")
                rpc_usage.record_provider_success(rpc, served_methods=1)
                if logs:
                    accepted_logs = logs
                    success = True
                    break

                # A single provider can be used in tests/emergency config. In
                # normal multi-provider production, an empty range is accepted
                # only after an independent endpoint confirms it.
                empty_endpoints.add(rpc)
                if unique_rpc_count == 1 or (
                    attempt >= len(RPC_URLS) - 1 and len(empty_endpoints) >= 2
                ):
                    accepted_logs = []
                    success = True
                    break
            except requests.exceptions.Timeout as exc:
                rpc_usage.record_provider_failure(rpc)
                last_error = f"timeout: {exc}"
                time.sleep(1)
            except Exception as exc:
                rpc_usage.record_provider_failure(
                    rpc,
                    rate_limited=is_rate_limited_exception(exc),
                )
                last_error = str(exc)
                time.sleep(0.5)

        if restart_with_smaller_chunk:
            continue

        if not success:
            if empty_endpoints:
                last_error = (
                    "unconfirmed empty response from "
                    f"{len(empty_endpoints)} of {unique_rpc_count} independent RPC endpoints"
                )
            raise EventLogFetchError(
                f"Failed to fetch veDOLO logs for topic {topic} "
                f"from block {current:,} to {chunk_end:,}: {last_error or 'unknown RPC error'}"
            )

        all_logs.extend(accepted_logs or [])

        current = chunk_end + 1
        chunks_done += 1

        if chunks_done % 20 == 0 or current > end_block:
            pct = min(100, (current - start_block) * 100 // max(total_blocks, 1))
            print(f"    {pct}% (block {current:,}/{end_block:,}, {len(all_logs):,} events)", flush=True)

        if chunk_size < CHUNK_SIZE:
            chunk_size = min(chunk_size * 2, CHUNK_SIZE)

        time.sleep(0.05)

    print(f"  ✅ {len(all_logs):,} events found")
    return all_logs


def immutable_event_identity(row):
    """Stable identity for an immutable veDOLO Deposit/Withdraw event."""
    return (
        str(row.get("txHash") or "").lower(),
        int(row.get("tokenId") or 0),
        int(row.get("block") or 0),
    )


def assert_no_immutable_event_regression(
    previous,
    candidate,
    current_block,
    reorg_depth=FINALITY_REORG_DEPTH,
):
    """Fail closed if a refresh drops an already-published finalized event."""
    finalized_through = max(0, int(current_block) - int(reorg_depth))
    for key, singular in (("locks", "lock"), ("unlocks", "unlock")):
        old_ids = {
            immutable_event_identity(row)
            for row in previous.get(key, [])
            if int(row.get("block") or 0) <= finalized_through
        }
        new_ids = {
            immutable_event_identity(row)
            for row in candidate.get(key, [])
        }
        missing = old_ids - new_ids
        if missing:
            raise EventLogFetchError(
                f"Candidate veDOLO history dropped {len(missing):,} finalized {singular} "
                f"event{'s' if len(missing) != 1 else ''}; refusing to overwrite production data"
            )

    def transfer_identity(row):
        return (
            str(row.get("txHash") or "").lower(),
            int(row.get("tokenId") or 0),
            int(row.get("block") or 0),
            str(row.get("from") or "").lower(),
            str(row.get("to") or "").lower(),
        )

    old_transfer_ids = {
        transfer_identity(row)
        for row in previous.get("transfers", [])
        if int(row.get("block") or 0) <= finalized_through
    }
    new_transfer_ids = {
        transfer_identity(row) for row in candidate.get("transfers", [])
    }
    missing_transfers = old_transfer_ids - new_transfer_ids
    if missing_transfers:
        raise EventLogFetchError(
            f"Candidate veDOLO history dropped {len(missing_transfers):,} finalized transfer "
            f"event{'s' if len(missing_transfers) != 1 else ''}; refusing to overwrite production data"
        )


def _merge_event_rows(published_rows, cached_rows, *, transfer=False):
    def identity(row):
        base = immutable_event_identity(row)
        if not transfer:
            return base
        return base + (
            str(row.get("from") or "").lower(),
            str(row.get("to") or "").lower(),
        )

    merged = {identity(row): dict(row) for row in published_rows or []}
    # Cache values may contain newer annotations, so they win on exact identity.
    merged.update({identity(row): dict(row) for row in cached_rows or []})
    return list(merged.values())


def reconcile_state_with_published_history(state, published):
    """Use the checked-in artifact as the minimum cache coverage floor."""
    state = dict(state or {})
    if not isinstance(published, dict):
        return state

    state["locks"] = _merge_event_rows(published.get("locks"), state.get("locks"))
    state["unlocks"] = _merge_event_rows(published.get("unlocks"), state.get("unlocks"))
    state["transfers"] = _merge_event_rows(
        published.get("transfers"), state.get("transfers"), transfer=True
    )
    published_target_block = int(published.get("target_block") or 0)
    published_transfer_schema = int(published.get("transfers_schema_version") or 0)
    if (
        published_transfer_schema == TRANSFERS_SCHEMA_VERSION
        and transfer_rows_are_complete(published.get("transfers"), published_target_block)
    ):
        state["transfers_schema_version"] = TRANSFERS_SCHEMA_VERSION
        state["transfers_last_block"] = max(
            int(state.get("transfers_last_block") or 0),
            published_target_block,
        )

    if int(state.get("last_block") or 0) <= 0:
        history_blocks = [
            int(row.get("block") or 0)
            for key in ("locks", "unlocks")
            for row in state.get(key, [])
        ]
        if history_blocks:
            state["last_block"] = max(history_blocks)
    if int(state.get("transfers_last_block") or 0) <= 0:
        transfer_blocks = [int(row.get("block") or 0) for row in state.get("transfers", [])]
        if transfer_blocks:
            state["transfers_last_block"] = max(transfer_blocks)

    pending = state.get("pending_vedolo_sync")
    if isinstance(pending, dict):
        pending = dict(pending)
        if int(pending.get("target_block") or 0) < published_target_block:
            state.pop("pending_vedolo_sync", None)
            return state
        pending["locks"] = _merge_event_rows(published.get("locks"), pending.get("locks"))
        pending["unlocks"] = _merge_event_rows(published.get("unlocks"), pending.get("unlocks"))
        pending["transfers"] = _merge_event_rows(
            published.get("transfers"), pending.get("transfers"), transfer=True
        )
        state["pending_vedolo_sync"] = pending

    return state


def get_tx_receipt(tx_hash):
    """Get transaction receipt to check for oDOLO exercise events."""
    data = rpc_call("eth_getTransactionReceipt", [tx_hash], timeout=10)
    if data and "result" in data:
        return data["result"]
    return None


def get_block_timestamp(block_number):
    """Get a UTC timestamp for a block number."""
    try:
        block_number = int(block_number)
    except (TypeError, ValueError):
        return 0
    if block_number <= 0:
        return 0
    data = rpc_call("eth_getBlockByNumber", [hex(block_number), False], timeout=10)
    block = data.get("result") if isinstance(data, dict) else None
    if not block:
        return 0
    try:
        return int(block.get("timestamp", "0x0"), 16)
    except (TypeError, ValueError):
        return 0


def hydrate_transfer_timestamps(transfers, state):
    """Attach block timestamps/dates to transfer rows."""
    if not transfers:
        return transfers
    block_cache = state.get("block_timestamps")
    if not isinstance(block_cache, dict):
        block_cache = {}
        state["block_timestamps"] = block_cache
    missing_blocks = []
    for transfer in transfers:
        block = int(transfer.get("block") or 0)
        if not block or transfer.get("timestamp"):
            continue
        key = str(block)
        if key not in block_cache:
            missing_blocks.append(block)

    missing_blocks = sorted(set(missing_blocks))
    for i, block in enumerate(missing_blocks):
        ts = get_block_timestamp(block)
        if ts:
            block_cache[str(block)] = ts
        if (i + 1) % 100 == 0:
            print(f"    Hydrated transfer block timestamps: {i+1}/{len(missing_blocks)}", flush=True)
            save_state(state)
        time.sleep(0.02)

    for transfer in transfers:
        block = int(transfer.get("block") or 0)
        ts = int(transfer.get("timestamp") or 0) or int(block_cache.get(str(block)) or 0)
        if ts:
            transfer["timestamp"] = ts
            transfer["date"] = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return transfers


def dedupe_transfers(transfers):
    """Keep one row per ERC721 transfer log."""
    seen = set()
    out = []
    for transfer in transfers or []:
        key = (
            str(transfer.get("txHash") or "").lower(),
            int(transfer.get("tokenId") or 0),
            str(transfer.get("from") or "").lower(),
            str(transfer.get("to") or "").lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(transfer)
    return out


def apply_manual_transfer_backfills(transfers):
    """Merge verified transfer rows that were missed by historical RPC scans."""
    return dedupe_transfers((transfers or []) + [dict(row) for row in MANUAL_TRANSFER_BACKFILLS])


def apply_manual_lock_beneficiary_backfills(locks):
    """Attach verified NFT mint recipients for deposits routed through wrappers."""
    for lock in locks or []:
        try:
            token_id = int(lock.get("tokenId") or 0)
        except (TypeError, ValueError):
            continue
        beneficiary = MANUAL_LOCK_BENEFICIARY_BACKFILLS.get(token_id)
        if not beneficiary:
            continue
        lock["beneficiaryAddress"] = beneficiary
        lock["addressSource"] = "receipt-backfill"
    return locks


def apply_airdrop_claim_annotations(locks):
    """Mark veDOLO locks created by the DOLO airdrop claim flow."""
    for lock in locks or []:
        provider = normalize_address(lock.get("address"))
        protocol = normalize_address(lock.get("protocolAddress"))
        if provider not in AIRDROP_CLAIM_CONTRACTS and protocol not in AIRDROP_CLAIM_CONTRACTS:
            continue
        lock["isAirdropClaim"] = True
        lock["claimSource"] = "airdrop"
        lock["protocolAddress"] = provider or protocol
    return locks


def _checkpoint_receipt_checks(state, receipt_checks, pending_sync=None):
    if state is None:
        return
    state["odolo_receipt_checks"] = receipt_checks
    if pending_sync:
        state["pending_vedolo_sync"] = {
            **pending_sync,
            "updated": datetime.utcnow().isoformat() + "Z",
        }
    save_state(state)


def check_odolo_exercise_batch(
    tx_hashes,
    exerciser_lookup=None,
    receipt_checks=None,
    state=None,
    pending_sync=None,
    soft_deadline=None,
):
    """Check which tx hashes involve oDOLO exercise (transfer from Vester)."""
    exerciser_lookup = exerciser_lookup or {}
    receipt_checks = receipt_checks if isinstance(receipt_checks, dict) else {}
    exercise_txs = {}
    normalized_txs = [str(tx_hash or "").strip().lower() for tx_hash in tx_hashes]
    normalized_txs = [tx_hash for tx_hash in normalized_txs if tx_hash.startswith("0x") and len(tx_hash) == 66]
    total = len(normalized_txs)
    rpc_checks = 0

    for i, tx_hash in enumerate(normalized_txs):
        lookup_beneficiary = exerciser_lookup.get(tx_hash)
        if lookup_beneficiary:
            exercise_txs[tx_hash] = lookup_beneficiary
            continue

        if tx_hash in receipt_checks:
            cached = receipt_checks[tx_hash]
            if isinstance(cached, dict) and cached.get("isOdolo"):
                exercise_txs[tx_hash] = normalize_address(cached.get("beneficiary"))
            continue

        if soft_deadline is not None and time.monotonic() >= soft_deadline:
            _checkpoint_receipt_checks(state, receipt_checks, pending_sync)
            print(
                f"    ⏸️ Soft runtime limit reached while checking oDOLO receipts "
                f"({i}/{total}). Progress saved.",
                flush=True,
            )
            return exercise_txs, False

        receipt = get_tx_receipt(tx_hash)
        beneficiary = None
        is_odolo = False
        if receipt and receipt.get("logs"):
            for log in receipt["logs"]:
                # Check if any log is from the oDOLO Vester contract
                log_addr = log.get("address", "").lower()
                if log_addr == ODOLO_VESTER:
                    beneficiary = extract_odolo_receipt_beneficiary(receipt)
                    exercise_txs[tx_hash] = beneficiary
                    is_odolo = True
                    break

        receipt_checks[tx_hash] = {
            "isOdolo": is_odolo,
            "beneficiary": beneficiary,
        }
        rpc_checks += 1

        if (i + 1) % 50 == 0:
            print(f"    Checking oDOLO exercises: {i+1}/{total} ({rpc_checks} RPC receipts)", flush=True)
        if rpc_checks and rpc_checks % 250 == 0:
            _checkpoint_receipt_checks(state, receipt_checks, pending_sync)
        time.sleep(0.03)

    _checkpoint_receipt_checks(state, receipt_checks, pending_sync)
    return exercise_txs, True


def resolve_receipt_fallback_beneficiaries(locks):
    """Resolve Vester fallback rows from on-chain receipts when exerciser cache missed them."""
    resolved = 0
    fallback_locks = [
        lock for lock in locks
        if lock.get("addressSource") == "odolo-vester-fallback" and lock.get("txHash")
    ]

    for lock in fallback_locks:
        receipt = get_tx_receipt(lock["txHash"])
        beneficiary = extract_odolo_receipt_beneficiary(receipt)
        if beneficiary:
            lock["address"] = beneficiary
            lock["beneficiaryAddress"] = beneficiary
            lock["addressSource"] = "odolo-receipt"
            lock["isOdolo"] = True
            lock["protocolAddress"] = ODOLO_VESTER
            resolved += 1
        time.sleep(0.03)

    return resolved


def decode_withdraw(log):
    """Decode Withdraw event: Withdraw(address indexed provider) + data: [tokenId, value, timestamp]"""
    provider = "0x" + log["topics"][1][26:]
    data = log["data"][2:]
    token_id = int(data[0:64], 16)
    value = int(data[64:128], 16) / 1e18
    ts = int(data[128:192], 16)

    row = {
        "address": provider.lower(),
        "txHash": log["transactionHash"],
        "tokenId": token_id,
        "dolo": round(value, 4),
        "timestamp": ts,
        "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
        "block": int(log["blockNumber"], 16),
    }
    if log.get("logIndex") is not None:
        row["logIndex"] = int(str(log["logIndex"]), 16)
    return row


def decode_deposit(log):
    """Decode Deposit event: Deposit(address indexed provider, uint256 indexed locktime)
    + data: [tokenId, value, deposit_type, timestamp]"""
    provider = "0x" + log["topics"][1][26:]
    locktime = int(log["topics"][2], 16)
    data = log["data"][2:]
    token_id = int(data[0:64], 16)
    value = int(data[64:128], 16) / 1e18
    deposit_type = int(data[128:192], 16)
    ts = int(data[192:256], 16)

    lock_days = max(0, round((locktime - ts) / 86400))

    row = {
        "address": provider.lower(),
        "txHash": log["transactionHash"],
        "tokenId": token_id,
        "dolo": round(value, 4),
        "lockDays": lock_days,
        "locktime": locktime,
        "depositType": deposit_type,
        "timestamp": ts,
        "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
        "block": int(log["blockNumber"], 16),
    }
    if log.get("logIndex") is not None:
        row["logIndex"] = int(str(log["logIndex"]), 16)
    return row


def recover_missing_deposit_events(
    locks,
    audited_token_blocks=None,
    fetcher=None,
):
    """Recover audited historical Deposit gaps from exact on-chain blocks."""
    token_blocks = audited_token_blocks or AUDITED_MISSING_DEPOSIT_BLOCKS
    fetch_logs = fetcher or fetch_event_logs
    merged = list(locks or [])
    existing_ids = {
        int(lock.get("tokenId") or 0)
        for lock in merged
        if int(lock.get("tokenId") or 0) > 0
    }
    recovered = []

    for token_id, block in sorted(token_blocks.items()):
        token_id = int(token_id)
        block = int(block)
        if token_id in existing_ids:
            continue

        logs = fetch_logs(block, block + 1, DEPOSIT_TOPIC)
        candidates = []
        for log in _logs_with_topic0(logs or [], DEPOSIT_TOPIC):
            decoded = decode_deposit(log)
            if decoded["tokenId"] == token_id and decoded["block"] == block:
                candidates.append(decoded)

        if len(candidates) != 1:
            raise EventLogFetchError(
                f"Expected one audited Deposit for token {token_id} at block {block:,}; "
                f"found {len(candidates)}"
            )

        recovered.append(candidates[0])
        existing_ids.add(token_id)

    return merged + recovered, recovered


def decode_transfer(log):
    """Decode ERC721 Transfer(address indexed from, address indexed to, uint256 indexed tokenId)."""
    topics = log.get("topics") or []
    if len(topics) < 4:
        return None
    from_address = address_from_topic(topics[1])
    to_address = address_from_topic(topics[2])
    if not from_address or not to_address:
        return None
    # Mints and burns are already represented by Deposit/Withdraw rows.
    if from_address == ZERO_ADDRESS or to_address == ZERO_ADDRESS:
        return None
    try:
        token_id = int(str(topics[3]), 16)
        block = int(log["blockNumber"], 16)
    except (KeyError, TypeError, ValueError):
        return None

    row = {
        "from": from_address,
        "to": to_address,
        "txHash": log.get("transactionHash", ""),
        "tokenId": token_id,
        "block": block,
    }
    if log.get("logIndex") is not None:
        row["logIndex"] = int(str(log["logIndex"]), 16)
    return row


def main():
    args = parse_args()
    soft_deadline = None
    if args.max_runtime_seconds > 0:
        soft_deadline = time.monotonic() + args.max_runtime_seconds

    print("=" * 60)
    print("🔄 veDOLO Events Pipeline — Locks & Unlocks")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load incremental state. The checked-in artifact is the minimum coverage
    # floor, so a missing/stale Actions cache cannot erase historical events.
    state = load_state()
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON) as published_file:
                published_output = json.load(published_file)
            state = reconcile_state_with_published_history(state, published_output)
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise EventLogFetchError(
                f"Could not use published veDOLO history as cache floor: {exc}"
            ) from exc
    is_incremental = bool(state.get("last_block"))
    if is_incremental:
        print("📦 Found previous state — running incremental sync")
    else:
        print("🆕 No previous state — running full sync (first run)")

    pending_sync = load_pending_sync(state)
    exerciser_lookup = load_odolo_exerciser_lookup()
    if pending_sync:
        print(
            "📦 Found unfinished veDOLO sync — resuming receipt checks "
            f"for target block {pending_sync['target_block']:,}"
        )
        current_block = pending_sync["target_block"]
        all_unlocks = pending_sync["unlocks"]
        all_locks = pending_sync["locks"]
        all_transfers = pending_sync["transfers"]
        pending_tx_hashes = pending_sync["tx_hashes"]
        print(f"  Resumed: {len(all_unlocks)} unlocks, {len(all_locks)} locks, {len(all_transfers)} transfers")
    else:
        # Get current block
        print("\n📡 Getting current block number...")
        current_block = get_current_block()
        print(f"  Berachain: block {current_block:,}")

        if current_block == 0:
            print("❌ Could not get current block. Aborting.")
            sys.exit(1)

        # Determine scan range
        last_block = state.get("last_block", 0)
        cached_unlocks = state.get("unlocks", [])
        cached_locks = state.get("locks", [])
        cached_transfers = state.get("transfers", [])
        transfers_last_block = int(state.get("transfers_last_block") or 0)
        transfers_need_backfill = transfer_cache_needs_full_backfill(
            state, cached_transfers
        )

        if is_incremental and last_block > 0:
            fetch_start = last_block + 1
            if fetch_start >= current_block:
                print(f"  Already up to date (block {last_block:,})")
                new_withdraw_logs = []
                new_deposit_logs = []
            else:
                print(f"\n📡 Fetching Deposit + Withdraw events (locks & unlocks)...")
                lock_unlock_logs = fetch_event_logs(
                    fetch_start, current_block, [WITHDRAW_TOPIC, DEPOSIT_TOPIC]
                )
                new_withdraw_logs = _logs_with_topic0(lock_unlock_logs, WITHDRAW_TOPIC)
                new_deposit_logs = _logs_with_topic0(lock_unlock_logs, DEPOSIT_TOPIC)
        else:
            print(f"\n📡 Fetching ALL Deposit + Withdraw events from block {DEPLOY_BLOCK:,}...")
            lock_unlock_logs = fetch_event_logs(
                DEPLOY_BLOCK, current_block, [WITHDRAW_TOPIC, DEPOSIT_TOPIC]
            )
            new_withdraw_logs = _logs_with_topic0(lock_unlock_logs, WITHDRAW_TOPIC)
            new_deposit_logs = _logs_with_topic0(lock_unlock_logs, DEPOSIT_TOPIC)
            cached_unlocks = []
            cached_locks = []

        if transfers_need_backfill:
            print(f"\n📡 Fetching ALL veDOLO Transfer events from block {DEPLOY_BLOCK:,}...")
            new_transfer_logs = fetch_event_logs(DEPLOY_BLOCK, current_block, TRANSFER_TOPIC)
            cached_transfers = []
        else:
            transfer_fetch_start = transfers_last_block + 1
            if transfer_fetch_start >= current_block:
                print(f"\n  veDOLO transfers already up to date (block {transfers_last_block:,})")
                new_transfer_logs = []
            else:
                print(f"\n📡 Fetching veDOLO Transfer events...")
                new_transfer_logs = fetch_event_logs(transfer_fetch_start, current_block, TRANSFER_TOPIC)

        # Decode new events
        print(f"\n🔧 Decoding events...")
        new_unlocks = [decode_withdraw(log) for log in new_withdraw_logs]
        new_locks = [decode_deposit(log) for log in new_deposit_logs]
        new_transfers = [row for row in (decode_transfer(log) for log in new_transfer_logs) if row]
        print(f"  New: {len(new_unlocks)} unlocks, {len(new_locks)} locks, {len(new_transfers)} transfers")

        if new_transfers:
            print(f"\n🕒 Hydrating veDOLO transfer timestamps...")
            hydrate_transfer_timestamps(new_transfers, state)

        # Merge with cached
        all_unlocks = cached_unlocks + new_unlocks
        all_locks = cached_locks + new_locks
        all_transfers = dedupe_transfers(cached_transfers + new_transfers)
        print(f"  Total: {len(all_unlocks)} unlocks, {len(all_locks)} locks, {len(all_transfers)} transfers")
        pending_tx_hashes = list(set(l["txHash"].lower() for l in new_locks))
        if pending_tx_hashes:
            save_pending_sync(state, current_block, all_unlocks, all_locks, pending_tx_hashes, all_transfers)

    all_locks, recovered_locks = recover_missing_deposit_events(all_locks)
    if recovered_locks:
        print(f"  Recovered {len(recovered_locks):,} audited historical Deposit events")
        pending_tx_hashes = sorted(set(pending_tx_hashes) | {
            str(lock.get("txHash") or "").lower()
            for lock in recovered_locks
            if lock.get("txHash")
        })
        save_pending_sync(
            state,
            current_block,
            all_unlocks,
            all_locks,
            pending_tx_hashes,
            all_transfers,
        )

    # Check oDOLO exercise status for new lock events
    if pending_tx_hashes:
        print(f"\n🔍 Checking oDOLO exercise status for {len(pending_tx_hashes)} new locks...")
        receipt_checks = state.get("odolo_receipt_checks", {})
        exercise_txs, exercise_complete = check_odolo_exercise_batch(
            pending_tx_hashes,
            exerciser_lookup=exerciser_lookup,
            receipt_checks=receipt_checks,
            state=state,
            pending_sync=state.get("pending_vedolo_sync"),
            soft_deadline=soft_deadline,
        )
        if not exercise_complete:
            save_pending_sync(state, current_block, all_unlocks, all_locks, pending_tx_hashes, all_transfers)
            save_run_status(
                False,
                reason="soft_runtime_limit",
                target_block=current_block,
                checked_receipts=len(state.get("odolo_receipt_checks", {})),
                total_lock_txs=len(pending_tx_hashes),
            )
            print("⏸️ veDOLO sync paused before timeout. State cached for the next workflow run.")
            return
        print(f"  Found {len(exercise_txs)} locks via oDOLO exercise")

        # Tag new locks
        pending_tx_hash_set = set(pending_tx_hashes)
        for lock in all_locks:
            tx_hash = lock["txHash"].lower()
            if tx_hash not in pending_tx_hash_set:
                continue
            lock["isOdolo"] = tx_hash in exercise_txs
            beneficiary = exercise_txs.get(tx_hash)
            if beneficiary:
                lock["beneficiaryAddress"] = beneficiary
                lock["addressSource"] = "odolo-receipt"
                lock["protocolAddress"] = ODOLO_VESTER

    resolved_odolo, unresolved_odolo = remap_odolo_lock_beneficiaries(all_locks, exerciser_lookup)
    receipt_resolved_odolo = 0
    if unresolved_odolo:
        receipt_resolved_odolo = resolve_receipt_fallback_beneficiaries(all_locks)
        resolved_odolo += receipt_resolved_odolo
        unresolved_odolo = max(0, unresolved_odolo - receipt_resolved_odolo)
    if resolved_odolo or unresolved_odolo:
        print(
            f"  Remapped {resolved_odolo:,} oDOLO-routed locks to end-user wallets"
            f" ({unresolved_odolo:,} still routed through protocol fallback)"
        )
    if receipt_resolved_odolo:
        print(f"  Resolved {receipt_resolved_odolo:,} fallback locks from transaction receipts")
    all_locks = apply_manual_lock_beneficiary_backfills(all_locks)
    all_locks = apply_airdrop_claim_annotations(all_locks)
    position_action_count = annotate_position_action_tokens(all_locks, state)
    if position_action_count:
        print(f"  Resolved {position_action_count:,} merge/split token transitions")
        save_state(state)

    # Sort by timestamp desc
    all_unlocks.sort(key=lambda x: x["timestamp"], reverse=True)
    all_locks.sort(key=lambda x: x["timestamp"], reverse=True)
    all_transfers = apply_manual_transfer_backfills(all_transfers)
    hydrate_transfer_timestamps(all_transfers, state)
    all_transfers = dedupe_transfers(all_transfers)
    all_transfers.sort(key=lambda x: int(x.get("timestamp") or 0), reverse=True)

    # Data protection: don't overwrite good data with empty or incomplete data.
    previous_output = None
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON) as f:
                old = json.load(f)
            previous_output = old
            old_unlocks = len(old.get("unlocks", []))
            old_locks = len(old.get("locks", []))
            old_transfers = len(old.get("transfers", []))
            if len(all_unlocks) == 0 and old_unlocks > 0:
                print(f"\n⚠️ 0 unlocks but old file has {old_unlocks}. Preserving old data.")
                all_unlocks = old["unlocks"]
            if len(all_locks) == 0 and old_locks > 0:
                print(f"\n⚠️ 0 locks but old file has {old_locks}. Preserving old data.")
                all_locks = old["locks"]
            if len(all_transfers) == 0 and old_transfers > 0:
                print(f"\n⚠️ 0 transfers but old file has {old_transfers}. Preserving old data.")
                all_transfers = old["transfers"]
        except Exception as exc:
            print(f"⚠️ Could not read existing output for preservation check: {exc}", flush=True)

    # Build output
    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "target_block": current_block,
        "transfers_schema_version": TRANSFERS_SCHEMA_VERSION,
        "total_unlocks": len(all_unlocks),
        "total_locks": len(all_locks),
        "total_transfers": len(all_transfers),
        "unlocks": all_unlocks,
        "locks": all_locks,
        "transfers": all_transfers,
    }

    assert_complete_transfer_history(all_transfers, current_block)
    if previous_output is not None:
        assert_no_immutable_event_regression(previous_output, output, current_block)

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    # Save state for incremental sync
    state["last_block"] = current_block
    state["unlocks"] = all_unlocks
    state["locks"] = all_locks
    state["transfers"] = all_transfers
    state["transfers_last_block"] = current_block
    state["transfers_schema_version"] = TRANSFERS_SCHEMA_VERSION
    state.pop("pending_vedolo_sync", None)
    save_state(state)
    save_run_status(True, target_block=current_block)

    print(f"\n💾 Saved: {OUTPUT_JSON}")
    print(f"   {len(all_unlocks)} unlocks, {len(all_locks)} locks, {len(all_transfers)} transfers")
    print(f"   State saved for incremental sync")

    # Summary
    if all_unlocks:
        oldest = min(u["date"] for u in all_unlocks)
        newest = max(u["date"] for u in all_unlocks)
        total_dolo = sum(u["dolo"] for u in all_unlocks)
        print(f"\n📊 Unlocks: {oldest} → {newest}, {total_dolo:,.0f} DOLO total")

    if all_locks:
        oldest = min(l["date"] for l in all_locks)
        newest = max(l["date"] for l in all_locks)
        total_dolo = sum(l["dolo"] for l in all_locks)
        odolo_count = sum(1 for l in all_locks if l.get("isOdolo"))
        direct_count = len(all_locks) - odolo_count
        print(f"📊 Locks: {oldest} → {newest}, {total_dolo:,.0f} DOLO total")
        print(f"   {odolo_count} via oDOLO, {direct_count} direct")

    if all_transfers:
        dated_transfers = [t for t in all_transfers if t.get("date")]
        if dated_transfers:
            oldest = min(t["date"] for t in dated_transfers)
            newest = max(t["date"] for t in dated_transfers)
            print(f"📊 Transfers: {oldest} → {newest}, {len(all_transfers):,} wallet-to-wallet veDOLO position transfers")
        else:
            print(f"📊 Transfers: {len(all_transfers):,} wallet-to-wallet veDOLO position transfers")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
