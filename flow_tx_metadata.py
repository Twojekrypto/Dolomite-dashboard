"""Optional, exact transaction metadata for aggregate flow rows.

The cached transfer ledgers intentionally stay compact.  This module uses their
block numbers only to recover an exact transaction hash and timestamp from the
token's canonical Transfer logs.  Incomplete RPC evidence is omitted rather
than guessed, so flow amounts and ranking remain completely independent.
"""
import re

from rpc_client import RpcError


ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")
TX_RE = re.compile(r"^0x[a-f0-9]{64}$")
SUPPORTED_CHAINS = {"ethereum", "berachain"}


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
        return {}
    missing = set(missing or [])
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
