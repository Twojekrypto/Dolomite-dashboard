"""Receipt-proven Dolomite lending interest on the official Core and DAO Safes.

Capital flows include fee collections and rewards deposited into a margin account;
they are subtracted in full. Only interest accruing inside Dolomite is counted.
Event discovery depends on the official subgraph; raw receipts and pinned account
state independently prove each discovered transition and the final principal.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path

import requests
from eth_abi import decode, encode
from web3 import Web3

import rpc_client
from scan_earn_netflow import ALL_EVENTS, decode_log_entries


TREASURY_OWNERS = {
    "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4": "Dolomite Core Safe",
    "0x6e939bbaceb45159982a2cac3a1fcbf7e93cf682": "Dolomite DAO Safe",
}
SAFE_MAP_SOURCE = "https://github.com/dolomite-exchange/dolomite-margin-modules/blob/master/packages/base/src/utils/constants.ts"
CHAIN_IDS = {"ethereum": 1, "arbitrum": 42161, "berachain": 80094}
DEFAULTS = {"pageSize": 500, "maxPages": 1000, "requestTimeoutSeconds": 30,
            "requestAttempts": 3, "maxSourceAgeSeconds": 5400}
USER_AGENT = "doloview-operating-income-treasury/1.0 (+https://doloview.pages.dev)"
ACCOUNT_FIELDS = "{id accountNumber user{id}}"
TOKEN_FIELDS = "{id marketId symbol decimals}"
EVENTS = {
    "deposits": (["marginAccount"], [("marginAccount", "token", "amountDeltaPar")]),
    "withdrawals": (["marginAccount"], [("marginAccount", "token", "amountDeltaPar")]),
    "transfers": (["fromMarginAccount", "toMarginAccount"], [
        ("fromMarginAccount", "token", "fromAmountDeltaPar"), ("toMarginAccount", "token", "toAmountDeltaPar")]),
    "trades": (["takerMarginAccount", "makerMarginAccount"], [
        ("takerMarginAccount", "takerToken", "takerInputTokenDeltaPar"),
        ("takerMarginAccount", "makerToken", "takerOutputTokenDeltaPar"),
        ("makerMarginAccount", "makerToken", "makerInputTokenDeltaPar"),
        ("makerMarginAccount", "takerToken", "makerOutputTokenDeltaPar")]),
    "liquidations": (["solidMarginAccount", "liquidMarginAccount"], [
        ("solidMarginAccount", "heldToken", "solidHeldTokenAmountDeltaPar"),
        ("solidMarginAccount", "borrowedToken", "solidBorrowedTokenAmountDeltaPar"),
        ("liquidMarginAccount", "heldToken", "liquidHeldTokenAmountDeltaPar"),
        ("liquidMarginAccount", "borrowedToken", "liquidBorrowedTokenAmountDeltaPar")]),
    "vaporizations": (["solidMarginAccount", "vaporMarginAccount"], [
        ("solidMarginAccount", "heldToken", "solidHeldTokenAmountDeltaPar"),
        ("solidMarginAccount", "borrowedToken", "solidBorrowedTokenAmountDeltaPar"),
        ("vaporMarginAccount", "borrowedToken", "vaporBorrowedTokenAmountDeltaPar")]),
}


def _integer(value):
    if isinstance(value, bool) or not re.fullmatch(r"-?[0-9]+", str(value)):
        raise ValueError("Invalid exact integer")
    return int(value)


def _raw(value, decimals):
    with localcontext() as context:
        context.prec = 160
        result = Decimal(str(value)) * Decimal(10) ** decimals
        if not result.is_finite() or result != result.to_integral_value():
            raise ValueError("Nonintegral or invalid token amount")
        return int(result)


def _usd(wei, price):
    with localcontext() as context:
        context.prec = 160
        return format(Decimal(wei) * Decimal(price) / Decimal(10) ** 36, "f")


def _sum_usd(rows):
    with localcontext() as context:
        context.prec = 160
        return format(sum((Decimal(row["netInterestUSD"]) for row in rows), Decimal(0)), "f")


def _paginate(fetch, *, page_size, max_pages):
    cursor, result = "", []
    for _ in range(max_pages):
        rows = fetch(cursor)
        if not isinstance(rows, list) or len(rows) > page_size:
            raise ValueError("Malformed or oversized source page")
        ids = [row.get("id") for row in rows]
        if any(not isinstance(value, str) or value <= cursor for value in ids) or ids != sorted(set(ids)):
            raise ValueError("Source pagination did not advance strictly")
        result.extend(rows)
        if len(rows) < page_size:
            return result
        cursor = ids[-1]
    raise ValueError("Treasury source pagination cap reached")


class _Source:
    def __init__(self, chain, config, options):
        self.chain, self.config = chain, config
        self.options = {**DEFAULTS, **options}
        self.rpc_client = rpc_client.RpcClient(chain=chain, timeout=self.options["requestTimeoutSeconds"],
                                               retries_per_endpoint=self.options["requestAttempts"], quiet=True)
        # The normal RPC environment/ordering is authoritative. These public
        # archive fallbacks also permit local reproducibility without API keys.
        if chain == "arbitrum":
            self.rpc_client.endpoints += ["https://arbitrum-one.public.blastapi.io", "https://arbitrum.gateway.tenderly.co"]
        self.cache = None
        if options.get("cacheDir"):
            identity = hashlib.sha256(json.dumps([config["chainId"], config["margin"].lower()], separators=(",", ":")).encode()).hexdigest()[:20]
            self.cache = Path(options["cacheDir"]) / "treasury" / identity
            self.cache.mkdir(parents=True, exist_ok=True)

    def graphql(self, query):
        for attempt in range(self.options["requestAttempts"]):
            try:
                response = requests.post(self.config["subgraph"], json={"query": query},
                                         headers={"User-Agent": USER_AGENT}, timeout=self.options["requestTimeoutSeconds"])
                response.raise_for_status()
                payload = response.json()
                if payload.get("errors") or not isinstance(payload.get("data"), dict):
                    raise ValueError("Treasury subgraph returned errors or no data")
                return payload["data"]
            except (requests.RequestException, ValueError):
                if attempt + 1 == self.options["requestAttempts"]:
                    raise ValueError("Treasury subgraph request failed") from None
                time.sleep(min(2**attempt, 4))

    def _rpc_uncached(self, method, params):
        # RpcClient rotates transport/rate-limit failures, but archive/pruned-state
        # JSON-RPC errors need explicit fallback as well. Never persist endpoints.
        original = list(self.rpc_client.endpoints)
        for endpoint in original:
            client = rpc_client.RpcClient(endpoints=[endpoint], timeout=self.options["requestTimeoutSeconds"],
                                          retries_per_endpoint=self.options["requestAttempts"], quiet=True)
            try:
                result = client.call(method, params)
                if result is None:
                    raise ValueError("Missing RPC result")
                self.rpc_client.endpoints = [endpoint] + [item for item in original if item != endpoint]
                return result
            except (rpc_client.RpcError, ValueError):
                continue
        raise ValueError(f"Treasury {self.chain} RPC {method} failed on every configured endpoint")

    def rpc(self, method, params, *, immutable=False, cache_context=None):
        path = None
        if immutable and self.cache:
            digest = hashlib.sha256(json.dumps([method, params, cache_context], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            path = self.cache / (digest + ".json")
            if path.is_file():
                return json.loads(path.read_text(encoding="utf-8"))
        result = self._rpc_uncached(method, params)
        if path:
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
            temporary.replace(path)
        return result

    def calls(self, calls, block):
        target = self.config.get("multicall")
        if not target:
            return [self.rpc("eth_call", [{"to": to, "data": data}, hex(block)], immutable=self.immutable,
                             cache_context=self.state_block_hash) for to, data in calls]
        data = _encode("aggregate3((address,bool,bytes)[])", ["(address,bool,bytes)[]"],
                       [[(to, False, bytes.fromhex(value[2:])) for to, value in calls]])
        raw = self.rpc("eth_call", [{"to": target, "data": data}, hex(block)], immutable=self.immutable,
                          cache_context=self.state_block_hash)
        values = decode(["(bool,bytes)[]"], bytes.fromhex(raw[2:]))[0]
        if len(values) != len(calls) or any(not ok for ok, _ in values):
            raise ValueError("Incomplete treasury multicall")
        return ["0x" + value.hex() for _, value in values]


def _encode(signature, types, values):
    return "0x" + Web3.keccak(text=signature).hex()[:8] + encode(types, values).hex()


def _block(source, number):
    # Head and boundary bracketing must observe the current canonical chain.
    # Heavy receipts/state calls are cached separately and state keys bind hash.
    value = source.rpc("eth_getBlockByNumber", [hex(number), False])
    if not value or int(value.get("number", "-1"), 16) != number or not value.get("hash"):
        raise ValueError("Missing or inconsistent treasury block")
    return {"number": number, "timestamp": int(value["timestamp"], 16), "hash": value["hash"].lower()}


def _find_boundary(source, timestamp, head):
    low, high = 0, head
    if _block(source, high)["timestamp"] < timestamp:
        raise ValueError("Treasury source has not reached period end")
    while low < high:
        mid = (low + high + 1) // 2
        if _block(source, mid)["timestamp"] < timestamp:
            low = mid
        else:
            high = mid - 1
    result, following = _block(source, low), _block(source, low + 1)
    if result["timestamp"] >= timestamp or following["timestamp"] < timestamp:
        raise ValueError("Treasury UTC boundary not bracketed")
    return {**result, "nextTimestamp": following["timestamp"], "nextBlockHash": following["hash"]}


def _event_key(event):
    tx = event.get("transaction") or {}
    return str(tx.get("id", "")).lower(), _integer(event.get("logIndex"))


def _account_key(account, token):
    owner = str((account.get("user") or {}).get("id", "")).lower()
    number, market = _integer(account.get("accountNumber")), _integer(token.get("marketId"))
    if number < 0 or number >= 2**256 or market < 0:
        raise ValueError("Invalid treasury account or market")
    return owner, str(number), str(market)


def _expected_event_legs(entity, event):
    legs = {}
    for account_field, token_field, delta_field in EVENTS[entity][1]:
        account, token = event.get(account_field), event.get(token_field)
        if account is None:
            continue
        key = _account_key(account, token or {})
        if key[0] not in TREASURY_OWNERS:
            continue
        decimals = _integer(token.get("decimals"))
        if not 0 <= decimals <= 255:
            raise ValueError("Invalid treasury token decimals")
        if key in legs:
            raise ValueError("Ambiguous treasury source event legs")
        legs[key] = _raw(event.get(delta_field), decimals)
    if not legs:
        raise ValueError("Source event has no explicitly owned treasury account")
    return legs


def _verified_legs(events, receipts, margin, start_timestamp, end_timestamp):
    expected = {}
    for entity, rows in events.items():
        for event in rows:
            key = _event_key(event)
            item = {"event": event, "legs": _expected_event_legs(entity, event)}
            if key in expected and expected[key] != item:
                raise ValueError("Conflicting duplicate treasury event")
            expected[key] = item
    result, actual_keys = [], set()
    for tx_hash in sorted({key[0] for key in expected}):
        receipt = receipts.get(tx_hash)
        if not receipt or receipt.get("status") != "0x1" or receipt.get("transactionHash", "").lower() != tx_hash:
            raise ValueError("Missing, failed or wrong treasury receipt")
        for log in receipt.get("logs") or []:
            if log.get("address", "").lower() != margin.lower():
                continue
            topics = log.get("topics") or []
            owned_topic = any(topic.lower()[-40:] in {owner[2:] for owner in TREASURY_OWNERS} for topic in topics[1:3])
            if not owned_topic or not topics or topics[0].lower() not in ALL_EVENTS:
                continue
            key = (tx_hash, int(log["logIndex"], 16))
            if key in actual_keys or key not in expected or log.get("removed"):
                raise ValueError("Duplicate, removed or undiscovered treasury receipt event")
            item = expected[key]
            event = item["event"]
            tx = event["transaction"]
            if not start_timestamp <= _integer(tx["timestamp"]) < end_timestamp:
                raise ValueError("Treasury event outside requested UTC period")
            if (int(receipt["blockNumber"], 16) != _integer(tx["blockNumber"])
                    or log.get("blockNumber") != receipt["blockNumber"]
                    or log.get("transactionHash", "").lower() != tx_hash
                    or not receipt.get("blockHash") or log.get("blockHash") != receipt["blockHash"]):
                raise ValueError("Treasury source/receipt block mismatch")
            decoded = decode_log_entries(log)
            if not decoded:
                raise ValueError("Unsupported or malformed owned treasury event")
            actual = {}
            for leg in decoded:
                if leg["owner"] not in TREASURY_OWNERS:
                    continue
                account = leg.get("account")
                if account is None or leg.get("new_par") is None:
                    raise ValueError("Treasury event does not prove its raw account or principal")
                leg_key = (leg["owner"], str(_integer(account)), str(_integer(leg["market"])))
                if leg_key not in item["legs"] or leg_key in actual:
                    raise ValueError("Treasury source/receipt account mismatch")
                actual[leg_key] = True
                result.append({"owner": leg_key[0], "account": leg_key[1], "market": leg_key[2],
                               "deltaWei": _integer(leg["delta"]), "deltaPar": item["legs"][leg_key],
                               "newPar": _integer(leg["new_par"]), "block": int(log["blockNumber"], 16),
                               "transactionIndex": int(log["transactionIndex"], 16), "logIndex": key[1]})
            if set(actual) != set(item["legs"]):
                raise ValueError("Treasury source/receipt leg completeness mismatch")
            actual_keys.add(key)
    if actual_keys != set(expected):
        raise ValueError("Treasury receipt is missing a source event")
    return sorted(result, key=lambda row: (row["block"], row["transactionIndex"], row["logIndex"]))


def _reconcile_rows(start, end, legs, tokens, prices):
    if set(start) != set(end):
        raise ValueError("Incomplete treasury boundary state")
    running = {key: _integer(value["par"]) for key, value in start.items()}
    flows, counts = {key: 0 for key in start}, {key: 0 for key in start}
    for leg in legs:
        key = (leg["owner"], leg["account"], leg["market"])
        if key not in running:
            raise ValueError("Treasury flow has no pinned boundary state")
        running[key] += _integer(leg["deltaPar"])
        if running[key] != _integer(leg["newPar"]):
            raise ValueError("Treasury intermediate principal reconciliation failed")
        flows[key] += _integer(leg["deltaWei"])
        counts[key] += 1
    rows = []
    for key in sorted(start):
        if running[key] != _integer(end[key]["par"]):
            raise ValueError("Treasury final principal reconciliation failed")
        token, price = tokens[key[2]], prices.get(key[2])
        if price is None or _integer(price) <= 0:
            raise ValueError("Missing or nonpositive treasury protocol oracle quote")
        interest = _integer(end[key]["wei"]) - _integer(start[key]["wei"]) - flows[key]
        rows.append({"owner": key[0], "account": key[1], "marketId": key[2], "token": token["id"],
                     "symbol": token["symbol"], "decimals": _integer(token["decimals"]),
                     "startPar": str(start[key]["par"]), "endPar": str(end[key]["par"]),
                     "startWei": str(start[key]["wei"]), "endWei": str(end[key]["wei"]),
                     "capitalFlowWei": str(flows[key]), "interestWei": str(interest),
                     "oraclePrice": str(price), "netInterestUSD": _usd(interest, price),
                     "eventLegCount": counts[key], "reconciled": True})
    return rows


def _collect_chain(chain, config, start_timestamp, end_timestamp, options):
    source = _Source(chain, config, options)
    source.immutable = end_timestamp <= time.time() - 3600
    meta_fields = "deployment block{number timestamp hash} hasIndexingErrors"
    pinned_meta = source.graphql("{_meta{" + meta_fields + "}}")['_meta']
    if pinned_meta.get("hasIndexingErrors") is not False or not pinned_meta.get("deployment") or not isinstance(pinned_meta.get("block"), dict):
        raise ValueError("Treasury subgraph has indexing errors or no head")
    head = pinned_meta["block"]
    age = time.time() - _integer(head["timestamp"])
    if age < -60 or age > options["maxSourceAgeSeconds"]:
        raise ValueError("Treasury subgraph source is stale or future-dated")
    if int(source.rpc("eth_chainId", []), 16) != config["chainId"]:
        raise ValueError("Treasury RPC chain mismatch")
    def verify_head():
        block = _block(source, _integer(head["number"]))
        if block["hash"] != str(head.get("hash", "")).lower() or block["timestamp"] != _integer(head["timestamp"]):
            raise ValueError("Treasury subgraph/RPC head pin mismatch")
    verify_head()
    boundaries = {name: _find_boundary(source, stamp, _integer(head["number"]))
                  for name, stamp in [("start", start_timestamp), ("end", end_timestamp)]}
    owners = json.dumps(list(TREASURY_OWNERS))
    size = options["pageSize"]
    def pages(entity, fields, where, block):
        return _paginate(lambda cursor: source.graphql("{" + entity + "(first:" + str(size) +
            ",orderBy:id,orderDirection:asc,block:{number:" + str(block) + "},where:{and:[{id_gt:" +
            json.dumps(cursor) + "},{" + where + "}]}){" + fields + "}}" )[entity],
            page_size=size, max_pages=options["maxPages"])
    accounts = {name: pages("marginAccounts", "id accountNumber user{id} tokenValues(first:1000){valuePar token" + TOKEN_FIELDS + "}",
                            "user_in:" + owners, proof["number"]) for name, proof in boundaries.items()}
    events, tokens, expected_par, keys = {}, {}, {}, set()
    for name, rows in accounts.items():
        expected_par[name] = {}
        for account in rows:
            if str(account["user"]["id"]).lower() not in TREASURY_OWNERS:
                raise ValueError("Unowned treasury account in pinned source")
            if len(account["tokenValues"]) >= 1000:
                raise ValueError("Treasury nested token-value cap reached")
            for value in account["tokenValues"]:
                token = value["token"]
                key = _account_key(account, token)
                tokens[key[2]] = token
                expected_par[name][key] = _raw(value["valuePar"], _integer(token["decimals"]))
                keys.add(key)
    for entity, (account_fields, event_fields) in EVENTS.items():
        fields = "id logIndex serialId transaction{id timestamp blockNumber} "
        fields += " ".join(field + ACCOUNT_FIELDS for field in account_fields)
        fields += " " + " ".join(field + TOKEN_FIELDS for field in sorted({item[1] for item in event_fields}))
        fields += " " + " ".join(sorted({item[2] for item in event_fields}))
        # Graph Node disallows column filters beside OR; wrap them in AND.
        where = 'and:[{transaction_:{timestamp_gte:"' + str(start_timestamp) + '",timestamp_lt:"' + str(end_timestamp) + '"}},{or:[' + ",".join(
            "{" + field + "_:{user_in:" + owners + "}}" for field in account_fields) + "]}]"
        events[entity] = pages(entity, fields, where, _integer(head["number"]))
        for event in events[entity]:
            keys.update(_expected_event_legs(entity, event))
            for token_field in {item[1] for item in event_fields}:
                token = event[token_field]
                tokens[str(_integer(token["marketId"]))] = token
    tx_hashes = sorted({_event_key(event)[0] for rows in events.values() for event in rows})
    receipts = {tx: source.rpc("eth_getTransactionReceipt", [tx], immutable=source.immutable) for tx in tx_hashes}
    legs = _verified_legs(events, receipts, config["margin"], start_timestamp, end_timestamp)
    for rows in events.values():
        for event in rows:
            tx = event["transaction"]
            block = _block(source, _integer(tx["blockNumber"]))
            receipt = receipts[str(tx["id"]).lower()]
            if block["hash"] != str(receipt["blockHash"]).lower() or block["timestamp"] != _integer(tx["timestamp"]):
                raise ValueError("Treasury event is not on its pinned canonical block")
    states, prices = {}, {}
    for name, proof in boundaries.items():
        calls, descriptions = [], []
        for key in sorted(keys):
            for kind in ["Par", "Wei"]:
                calls.append((config["margin"], _encode("getAccount" + kind + "((address,uint256),uint256)",
                              ["(address,uint256)", "uint256"], [(key[0], int(key[1])), int(key[2])])) )
                descriptions.append((kind.lower(), key))
        if name == "end":
            for market in sorted(tokens):
                calls.append((config["margin"], _encode("getMarketPrice(uint256)", ["uint256"], [int(market)])))
                descriptions.append(("price", market))
        states[name] = {}
        source.state_block_hash = proof["hash"]
        for (kind, key), value in zip(descriptions, source.calls(calls, proof["number"]) if calls else []):
            raw = bytes.fromhex(value[2:])
            if kind == "price":
                prices[key] = decode(["uint256"], raw)[0]
            else:
                sign, amount = decode(["bool", "uint128" if kind == "par" else "uint256"], raw)
                states[name].setdefault(key, {})[kind] = amount if sign else -amount
        for key in keys:
            if states[name].get(key, {}).get("par") != expected_par[name].get(key, 0):
                raise ValueError("Treasury pinned subgraph/RPC principal mismatch")
    rows = _reconcile_rows(states["start"], states["end"], legs, tokens, prices)
    verify_head()
    final_meta = source.graphql("{_meta(block:{number:" + str(head["number"]) + "}){" + meta_fields + "}}")['_meta']
    final_block = final_meta.get("block") or {}
    if (final_meta.get("hasIndexingErrors") is not False
            or final_meta.get("deployment") != pinned_meta["deployment"]
            or final_block.get("number") != head["number"]
            or (final_block.get("hash") is not None and str(final_block["hash"]).lower() != str(head["hash"]).lower())
            or (final_block.get("timestamp") is not None and _integer(final_block["timestamp"]) != _integer(head["timestamp"]))):
        raise ValueError("Treasury source deployment or pinned head changed during collection")
    for proof in boundaries.values():
        current, following = _block(source, proof["number"]), _block(source, proof["number"] + 1)
        if current["hash"] != proof["hash"] or following["hash"] != proof["nextBlockHash"]:
            raise ValueError("Treasury cutoff block changed during collection")
    wallets = [{"address": owner, "label": label, "netInterestUSD": _sum_usd([row for row in rows if row["owner"] == owner]),
                "positions": [{key: value for key, value in row.items() if key != "owner"} for row in rows if row["owner"] == owner]}
               for owner, label in TREASURY_OWNERS.items()]
    return {"complete": True, "chainId": config["chainId"], "netInterestUSD": _sum_usd(rows), "wallets": wallets,
            "sourceHead": head, "sourceDeployment": pinned_meta["deployment"], "boundaries": boundaries,
            "proof": {"transactionCount": len(receipts), "eventCount": sum(len(value) for value in events.values()),
                      "ownedLegCount": len(legs), "positionCount": len(rows), "allPrincipalReconciled": True,
                      "sourcePinRechecked": True},
            "sources": {"subgraph": config["subgraph"], "margin": config["margin"], "safeMaps": SAFE_MAP_SOURCE}}


def collect_treasury_income(chain_configs, start_timestamp, end_timestamp, *, options=None):
    """Collect complete signed net interest, or raise without publishing partial data."""
    start_timestamp, end_timestamp = _integer(start_timestamp), _integer(end_timestamp)
    if not 0 < start_timestamp < end_timestamp or not chain_configs:
        raise ValueError("Invalid treasury period or empty chain configuration")
    settings = {**DEFAULTS, **(options or {})}
    for field in DEFAULTS:
        settings[field] = _integer(settings[field])
        if settings[field] <= 0:
            raise ValueError("Treasury operational limits must be positive")
    if settings["pageSize"] > 1000:
        raise ValueError("Treasury page size exceeds subgraph limit")
    chains = {}
    for chain, config in chain_configs.items():
        if CHAIN_IDS.get(chain) != config.get("chainId"):
            raise ValueError("Unsupported or inconsistent treasury chain configuration")
        chains[chain] = _collect_chain(chain, config, start_timestamp, end_timestamp, settings)
    return {"complete": True, "period": {"startTimestamp": start_timestamp, "endTimestamp": end_timestamp},
            "chains": chains, "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "scope": "Dolomite lending interest on official Core and DAO Safes, all raw owned subaccounts; fee-agent excluded",
            "valuation": "Signed token interest at period-end Dolomite oracle prices; principal flows, incentives and external yield excluded",
            "partnerShareApplied": False}
