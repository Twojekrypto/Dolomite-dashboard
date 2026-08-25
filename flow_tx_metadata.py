"""Optional, exact transaction metadata for aggregate flow rows.

The cached transfer ledgers intentionally stay compact.  This module uses their
block numbers only to recover an exact transaction hash and timestamp from the
token's canonical Transfer logs.  Incomplete RPC evidence is omitted rather
than guessed, so flow amounts and ranking remain completely independent.
"""
import re
from decimal import Decimal, InvalidOperation

from rpc_client import RpcError, rpc_single_request


ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")
TX_RE = re.compile(r"^0x[a-f0-9]{64}$")
SUPPORTED_CHAINS = {"ethereum", "berachain"}
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
V3_INCREASE_LIQUIDITY_TOPIC = "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f"
V3_DECREASE_LIQUIDITY_TOPIC = "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4"
V4_MODIFY_LIQUIDITY_TOPIC = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
V2_MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"
V2_BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
BROWNFI_MINT_TOPIC = "0x0c73bcc7a24ee727c66b44bd2e65a101ab83354fdaaf63f78c1c272765b4250a"
BULLA_INCREASE_LIQUIDITY_TOPIC = "0x8a82de7fe9b33e0e6bca0e26f5bd14a74f1164ffe236d50e0a36c3ea70f2b814"


def _normalized_log(log):
    if not isinstance(log, dict):
        return None
    from_addr = str(log.get("from") or "").lower()
    to_addr = str(log.get("to") or "").lower()
    topics = log.get("topics")
    if (not ADDRESS_RE.fullmatch(from_addr) or not ADDRESS_RE.fullmatch(to_addr)) and isinstance(topics, list) and len(topics) >= 3:
        from_addr = "0x" + str(topics[1])[-40:].lower()
        to_addr = "0x" + str(topics[2])[-40:].lower()
    tx_hash = str(log.get("transactionHash") or "").lower()
    try:
        log_index = int(str(log.get("logIndex", "0")), 16)
    except (TypeError, ValueError):
        return None
    if not ADDRESS_RE.fullmatch(from_addr) or not ADDRESS_RE.fullmatch(to_addr) or not TX_RE.fullmatch(tx_hash):
        return None
    return {"from": from_addr, "to": to_addr, "transactionHash": tx_hash, "logIndex": log_index}


def _normalized_address(value):
    address = str(value or "").strip().lower()
    return address if ADDRESS_RE.fullmatch(address) else ""


def _normalized_topic(value):
    topic = str(value or "").strip().lower()
    return topic if re.fullmatch(r"0x[a-f0-9]{64}", topic) else ""


def _address_from_topic(value):
    topic = _normalized_topic(value)
    if not topic or topic[2:26] != "0" * 24:
        return ""
    return "0x" + topic[-40:]


def _receipt_succeeded(receipt):
    status = receipt.get("status") if isinstance(receipt, dict) else None
    if isinstance(status, bool):
        return status
    if isinstance(status, int):
        return status == 1
    if isinstance(status, str):
        try:
            return int(status, 16 if status.lower().startswith("0x") else 10) == 1
        except ValueError:
            return False
    return False


def _signed_word(data, index):
    raw = str(data or "").strip().lower()
    if not re.fullmatch(r"0x(?:[a-f0-9]{64})+", raw):
        return None
    words = [raw[offset:offset + 64] for offset in range(2, len(raw), 64)]
    if index < 0 or index >= len(words):
        return None
    value = int(words[index], 16)
    return value - (1 << 256) if value >= (1 << 255) else value


def _decimal_token_amount(raw_amount):
    amount = Decimal(int(raw_amount)) / Decimal(10**18)
    text = format(amount, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _liquidity_registry_index(registry, chain):
    if chain not in SUPPORTED_CHAINS or not isinstance(registry, dict):
        return None
    chain_config = (registry.get("chains") or {}).get(chain) or {}
    adapters = chain_config.get("adapters") or {}
    pools = [
        pool for pool in (registry.get("pools") or [])
        if isinstance(pool, dict) and str(pool.get("chainKey") or "").lower() == chain
    ]
    contract_pools = {}
    v4_pools = {}
    for pool in pools:
        identifier = str(pool.get("identifier") or "").strip().lower()
        adapter = str(pool.get("adapter") or "").strip().lower()
        pair = str(pool.get("pair") or "").strip()
        item = {"adapter": adapter, "pair": pair}
        if pool.get("identifierType") == "contract" and ADDRESS_RE.fullmatch(identifier):
            contract_pools[identifier] = item
        elif pool.get("identifierType") == "poolId" and re.fullmatch(r"0x[a-f0-9]{64}", identifier):
            v4_pools[identifier] = item
    normalized_adapters = {}
    for adapter, config in adapters.items():
        if not isinstance(config, dict):
            continue
        normalized_adapters[str(adapter).lower()] = {
            key: _normalized_address(config.get(key))
            for key in ("poolManager", "positionManager")
        }
    return {
        "adapters": normalized_adapters,
        "contractPools": contract_pools,
        "v4Pools": v4_pools,
    }


def _receipt_transfer_rows(receipt, token_address, wallet):
    token = _normalized_address(token_address)
    owner = _normalized_address(wallet)
    if not token or not owner:
        return []
    rows = []
    for log in receipt.get("logs") or []:
        if not isinstance(log, dict) or _normalized_address(log.get("address")) != token:
            continue
        topics = log.get("topics") or []
        if len(topics) < 3 or _normalized_topic(topics[0]) != TRANSFER_TOPIC:
            continue
        from_addr = _address_from_topic(topics[1])
        to_addr = _address_from_topic(topics[2])
        if owner not in {from_addr, to_addr}:
            continue
        try:
            amount = int(str(log.get("data") or "0x0"), 16)
        except ValueError:
            continue
        if amount > 0:
            rows.append({"from": from_addr, "to": to_addr, "amount": amount})
    return rows


def classify_lp_receipt(receipt, wallet, chain, registry, token_address):
    """Return exact same-transaction LP attribution, or None when evidence is incomplete.

    A transfer to a pool manager alone is deliberately insufficient because it
    can also be a swap.  The receipt must include a registered pool plus the
    adapter's liquidity-specific event in the same successful transaction.
    """
    if not _receipt_succeeded(receipt):
        return None
    tx_hash = str(receipt.get("transactionHash") or "").strip().lower()
    owner = _normalized_address(wallet)
    index = _liquidity_registry_index(registry, chain)
    if not TX_RE.fullmatch(tx_hash) or not owner or not index:
        return None
    logs = [log for log in (receipt.get("logs") or []) if isinstance(log, dict)]
    transfers = _receipt_transfer_rows(receipt, token_address, owner)
    if not transfers:
        return None

    candidates = []

    v4 = index["adapters"].get("uniswap-v4") or {}
    pool_manager = v4.get("poolManager") or ""
    position_manager = v4.get("positionManager") or ""
    if pool_manager and position_manager:
        for log in logs:
            topics = log.get("topics") or []
            if (
                _normalized_address(log.get("address")) != pool_manager
                or len(topics) < 3
                or _normalized_topic(topics[0]) != V4_MODIFY_LIQUIDITY_TOPIC
                or _address_from_topic(topics[2]) != position_manager
            ):
                continue
            pool_id = _normalized_topic(topics[1])
            pool = index["v4Pools"].get(pool_id)
            delta = _signed_word(log.get("data"), 2)
            if not pool or not delta:
                continue
            direction = "deposit" if delta > 0 else "withdrawal"
            amount = sum(
                transfer["amount"] for transfer in transfers
                if (
                    direction == "deposit"
                    and transfer["from"] == owner
                    and transfer["to"] == pool_manager
                ) or (
                    direction == "withdrawal"
                    and transfer["from"] == pool_manager
                    and transfer["to"] == owner
                )
            )
            if amount > 0:
                candidates.append({**pool, "direction": direction, "amount": amount})

    for adapter, config in index["adapters"].items():
        if adapter == "uniswap-v4":
            continue
        position_manager = config.get("positionManager") or ""
        manager_topics = {
            _normalized_topic((log.get("topics") or [""])[0])
            for log in logs
            if position_manager and _normalized_address(log.get("address")) == position_manager
        }
        has_increase = bool(manager_topics & {V3_INCREASE_LIQUIDITY_TOPIC, BULLA_INCREASE_LIQUIDITY_TOPIC})
        has_decrease = V3_DECREASE_LIQUIDITY_TOPIC in manager_topics
        for pool_address, pool in index["contractPools"].items():
            if pool["adapter"] != adapter:
                continue
            if has_increase:
                amount = sum(
                    transfer["amount"] for transfer in transfers
                    if transfer["from"] == owner and transfer["to"] == pool_address
                )
                if amount > 0:
                    candidates.append({**pool, "direction": "deposit", "amount": amount})
            if has_decrease:
                amount = sum(
                    transfer["amount"] for transfer in transfers
                    if transfer["from"] == pool_address and transfer["to"] == owner
                )
                if amount > 0:
                    candidates.append({**pool, "direction": "withdrawal", "amount": amount})

    for pool_address, pool in index["contractPools"].items():
        if pool["adapter"] not in {"kodiak-v2", "uniswap-v2", "brownfi-v3"}:
            continue
        pool_topics = {
            _normalized_topic((log.get("topics") or [""])[0])
            for log in logs
            if _normalized_address(log.get("address")) == pool_address
        }
        mint_topics = {V2_MINT_TOPIC}
        if pool["adapter"] == "brownfi-v3":
            mint_topics.add(BROWNFI_MINT_TOPIC)
        if pool_topics & mint_topics:
            amount = sum(
                transfer["amount"] for transfer in transfers
                if transfer["from"] == owner and transfer["to"] == pool_address
            )
            if amount > 0:
                candidates.append({**pool, "direction": "deposit", "amount": amount})
        if V2_BURN_TOPIC in pool_topics:
            amount = sum(
                transfer["amount"] for transfer in transfers
                if transfer["from"] == pool_address and transfer["to"] == owner
            )
            if amount > 0:
                candidates.append({**pool, "direction": "withdrawal", "amount": amount})

    if not candidates:
        return None
    directions = {candidate["direction"] for candidate in candidates}
    if len(directions) != 1:
        return None
    direction = next(iter(directions))
    unique = {}
    for candidate in candidates:
        key = (candidate["adapter"], candidate["pair"], candidate["direction"], candidate["amount"])
        unique[key] = candidate
    candidates = list(unique.values())
    adapters = sorted({candidate["adapter"] for candidate in candidates})
    pairs = sorted({candidate["pair"] for candidate in candidates if candidate["pair"]})
    amount = sum(candidate["amount"] for candidate in candidates)
    return {
        "direction": direction,
        "amount": _decimal_token_amount(amount),
        "pair": pairs[0] if len(pairs) == 1 else "Multiple DOLO pools",
        "adapter": adapters[0] if len(adapters) == 1 else "multiple",
        "confidence": "verified_same_tx",
        "tx_hash": tx_hash,
    }


def attach_latest_lp_metadata(rows, chain, registry, token_address, receipt_loader):
    """Attach optional LP attribution for each row's already verified latest tx."""
    hashes = {
        str(row.get("latest_tx_hash") or "").strip().lower()
        for row in (rows or [])
        if isinstance(row, dict) and TX_RE.fullmatch(str(row.get("latest_tx_hash") or "").strip().lower())
    }
    try:
        receipts = receipt_loader(hashes) if hashes else {}
    except Exception:
        receipts = {}
    for row in rows or []:
        tx_hash = str(row.get("latest_tx_hash") or "").strip().lower()
        receipt = receipts.get(tx_hash) if isinstance(receipts, dict) else None
        activity = classify_lp_receipt(receipt, row.get("address"), chain, registry, token_address)
        if activity:
            row["latest_lp_activity"] = activity
    return rows


def _decimal_text(value):
    text = format(Decimal(value), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def collect_verified_lp_activities(
    transfers,
    chain,
    registry,
    token_address,
    evidence_loader,
    receipt_loader,
):
    """Index exact LP deposits/withdrawals before aggregate flow ranking."""
    index = _liquidity_registry_index(registry, chain)
    if not index:
        return {}
    targets = set(index["contractPools"])
    for config in index["adapters"].values():
        pool_manager = config.get("poolManager") or ""
        if pool_manager:
            targets.add(pool_manager)
    if not targets:
        return {}

    candidate_blocks = set()
    for transfer in transfers or []:
        if not isinstance(transfer, (list, tuple)) or len(transfer) < 4:
            continue
        from_addr = _normalized_address(transfer[0])
        to_addr = _normalized_address(transfer[1])
        if not from_addr or not to_addr or not ({from_addr, to_addr} & targets):
            continue
        try:
            candidate_blocks.add(int(transfer[3]))
        except (TypeError, ValueError):
            continue

    try:
        evidence = evidence_loader(candidate_blocks) if candidate_blocks else {}
    except Exception:
        evidence = {}
    transactions_by_wallet = {}
    transaction_meta = {}
    for block_number, block_evidence in (evidence or {}).items():
        if not isinstance(block_evidence, dict):
            continue
        try:
            timestamp = int(block_evidence.get("timestamp") or 0)
            block = int(block_number)
        except (TypeError, ValueError):
            continue
        if timestamp <= 0:
            continue
        for raw_log in block_evidence.get("logs") or []:
            log = _normalized_log(raw_log)
            if not log:
                continue
            if log["to"] in targets and log["from"] not in targets:
                wallet = log["from"]
            elif log["from"] in targets and log["to"] not in targets:
                wallet = log["to"]
            else:
                continue
            tx_hash = log["transactionHash"]
            transactions_by_wallet.setdefault(wallet, set()).add(tx_hash)
            transaction_meta[(wallet, tx_hash)] = {
                "timestamp": timestamp,
                "block_number": block,
            }

    tx_hashes = {
        tx_hash
        for hashes in transactions_by_wallet.values()
        for tx_hash in hashes
    }
    try:
        receipts = receipt_loader(tx_hashes) if tx_hashes else {}
    except Exception:
        receipts = {}

    result = {}
    for wallet, tx_hashes in transactions_by_wallet.items():
        deposit = Decimal(0)
        withdrawal = Decimal(0)
        latest = None
        pairs = set()
        adapters = set()
        for tx_hash in sorted(tx_hashes):
            receipt = receipts.get(tx_hash) if isinstance(receipts, dict) else None
            activity = classify_lp_receipt(
                receipt,
                wallet,
                chain,
                registry,
                token_address,
            )
            if not activity:
                continue
            try:
                amount = Decimal(str(activity.get("amount") or "0"))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            if activity["direction"] == "deposit":
                deposit += amount
            else:
                withdrawal += amount
            pair = str(activity.get("pair") or "").strip()
            adapter = str(activity.get("adapter") or "").strip()
            if pair:
                pairs.add(pair)
            if adapter:
                adapters.add(adapter)
            meta = transaction_meta.get((wallet, tx_hash), {})
            enriched = {
                **activity,
                "timestamp": int(meta.get("timestamp") or 0),
                "block_number": int(meta.get("block_number") or 0),
                "chain": chain,
            }
            if latest is None or (
                enriched["timestamp"], enriched["block_number"], tx_hash
            ) > (
                int(latest.get("timestamp") or 0),
                int(latest.get("block_number") or 0),
                str(latest.get("tx_hash") or ""),
            ):
                latest = enriched
        if latest:
            result[wallet] = {
                "deposit": _decimal_text(deposit),
                "withdrawal": _decimal_text(withdrawal),
                "pairs": sorted(pairs),
                "adapters": sorted(adapters),
                "latest": latest,
            }
    return result


def fetch_transaction_receipts(rpcs, tx_hashes, rpc_batch_requests,
                               retries_per_endpoint=2, batch_size=50,
                               describe="flow LP receipt evidence"):
    """Fetch successful transaction receipts, retrying incomplete batch items."""
    hashes = sorted({str(value or "").strip().lower() for value in tx_hashes if TX_RE.fullmatch(str(value or "").strip().lower())})
    if not hashes:
        return {}
    payloads = [
        {"jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "id": f"receipt:{tx_hash}", "params": [tx_hash]}
        for tx_hash in hashes
    ]
    try:
        responses, missing = rpc_batch_requests(
            rpcs, payloads, timeout=12, retries_per_endpoint=retries_per_endpoint,
            batch_size=batch_size, quiet=True, describe=describe,
        )
    except RpcError:
        responses, missing = {}, [payload["id"] for payload in payloads]
    responses = responses if isinstance(responses, dict) else {}
    missing = set(missing or [])
    for payload in payloads:
        item_id = payload["id"]
        response = responses.get(item_id)
        result = response.get("result") if isinstance(response, dict) and not response.get("error") else None
        if item_id not in missing and isinstance(result, dict):
            continue
        try:
            retry_response = rpc_single_request(
                rpcs, payload, timeout=12, retries_per_endpoint=retries_per_endpoint,
                quiet=True, describe=f"{describe} exact retry",
            )
        except RpcError:
            continue
        retry_result = retry_response.get("result") if isinstance(retry_response, dict) and not retry_response.get("error") else None
        if isinstance(retry_result, dict):
            responses[item_id] = retry_response
            missing.discard(item_id)
    receipts = {}
    for tx_hash in hashes:
        item_id = f"receipt:{tx_hash}"
        response = responses.get(item_id)
        result = response.get("result") if isinstance(response, dict) and not response.get("error") else None
        if item_id not in missing and isinstance(result, dict) and _receipt_succeeded(result):
            receipts[tx_hash] = result
    return receipts


def attach_latest_flow_metadata(rows, transfers, direction, chain, evidence_loader):
    """Attach all-or-none exact metadata to flow rows, mutating rows in place."""
    if direction not in {"inbound", "outbound"} or chain not in SUPPORTED_CHAINS:
        raise ValueError("Unsupported flow metadata direction or chain")
    addresses = {
        str(row.get("address") or "").lower()
        for row in rows
        if isinstance(row, dict) and ADDRESS_RE.fullmatch(str(row.get("address") or "").lower())
    }
    latest_blocks = {}
    for transfer in transfers or []:
        if not isinstance(transfer, (list, tuple)) or len(transfer) < 4:
            continue
        from_addr, to_addr = str(transfer[0]).lower(), str(transfer[1]).lower()
        address = to_addr if direction == "inbound" else from_addr
        if address not in addresses:
            continue
        try:
            block_number = int(transfer[3])
        except (TypeError, ValueError):
            continue
        if block_number > latest_blocks.get(address, -1):
            latest_blocks[address] = block_number

    try:
        evidence = evidence_loader(set(latest_blocks.values())) if latest_blocks else {}
    except Exception:
        evidence = {}
    for row in rows:
        address = str(row.get("address") or "").lower()
        block_number = latest_blocks.get(address)
        block_evidence = evidence.get(block_number, {}) if isinstance(evidence, dict) else {}
        try:
            timestamp = int(block_evidence.get("timestamp") or 0)
        except (TypeError, ValueError):
            timestamp = 0
        matching_logs = []
        for raw_log in block_evidence.get("logs") or []:
            log = _normalized_log(raw_log)
            if not log:
                continue
            is_match = log["to"] == address if direction == "inbound" else log["from"] == address
            if is_match:
                matching_logs.append(log)
        if timestamp <= 0 or not matching_logs:
            continue
        latest_log = max(matching_logs, key=lambda item: item["logIndex"])
        row["latest_tx_hash"] = latest_log["transactionHash"]
        row["latest_tx_timestamp"] = timestamp
        row["latest_tx_chain"] = chain
    return rows


def fetch_token_block_evidence(rpcs, token_address, block_numbers, rpc_batch_requests,
                               retries_per_endpoint=2, batch_size=50, describe="flow metadata"):
    """Batch exact-block Transfer logs and timestamps; return complete pairs only."""
    blocks = sorted({int(block) for block in block_numbers if int(block) >= 0})
    if not blocks:
        return {}
    payloads = []
    for block in blocks:
        payloads.extend([
            {
                "jsonrpc":"2.0", "method":"eth_getLogs", "id":f"logs:{block}",
                "params":[{"address":token_address, "topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"], "fromBlock":hex(block), "toBlock":hex(block)}],
            },
            {
                "jsonrpc":"2.0", "method":"eth_getBlockByNumber", "id":f"block:{block}",
                "params":[hex(block), False],
            },
        ])
    try:
        responses, missing = rpc_batch_requests(
            rpcs, payloads, timeout=12, retries_per_endpoint=retries_per_endpoint,
            batch_size=batch_size, quiet=True, describe=describe,
        )
    except RpcError:
        responses, missing = {}, [payload["id"] for payload in payloads]
    responses = responses if isinstance(responses, dict) else {}
    missing = set(missing or [])
    payload_by_id = {payload["id"]: payload for payload in payloads}
    for item_id, payload in payload_by_id.items():
        response = responses.get(item_id)
        result = response.get("result") if isinstance(response, dict) and not response.get("error") else None
        expected_type = list if payload["method"] == "eth_getLogs" else dict
        if item_id not in missing and isinstance(result, expected_type):
            continue
        try:
            retry_response = rpc_single_request(
                rpcs, payload, timeout=12,
                retries_per_endpoint=retries_per_endpoint,
                quiet=True, describe=f"{describe} exact retry",
            )
        except RpcError:
            continue
        retry_result = retry_response.get("result") if isinstance(retry_response, dict) and not retry_response.get("error") else None
        if isinstance(retry_result, expected_type):
            responses[item_id] = retry_response
            missing.discard(item_id)
    evidence = {}
    for block in blocks:
        logs_id, block_id = f"logs:{block}", f"block:{block}"
        logs_response = responses.get(logs_id)
        block_response = responses.get(block_id)
        if logs_id in missing or block_id in missing:
            continue
        if not isinstance(logs_response, dict) or logs_response.get("error") or not isinstance(logs_response.get("result"), list):
            continue
        if not isinstance(block_response, dict) or block_response.get("error") or not isinstance(block_response.get("result"), dict):
            continue
        try:
            timestamp = int(block_response["result"]["timestamp"], 16)
        except (KeyError, TypeError, ValueError):
            continue
        if timestamp > 0:
            evidence[block] = {"timestamp": timestamp, "logs": logs_response["result"]}
    return evidence
