#!/usr/bin/env python3
"""Pure, fail-closed EARN replay using exact archive-RPC index evidence."""

from __future__ import annotations

from typing import Dict, Iterable, Optional, Tuple


INDEX_SCALE = 10**18


def _integer(value, default=None):
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def par_to_wei_round_half_up(par: int, index: int) -> int:
    """Convert signed Par to signed Wei using protocol half-up rounding."""
    quotient, remainder = divmod(abs(int(par)) * int(index), INDEX_SCALE)
    value = quotient + (1 if remainder * 2 >= INDEX_SCALE else 0)
    return value if int(par) >= 0 else -value


def _add_settled_yield(state: dict, amount: int, position_par: int) -> int:
    settled = int(amount)
    par = int(position_par)
    if settled == 0 or par == 0:
        return 0
    state["settledYield"] += settled
    if par > 0:
        state["settledSupplyYield"] += settled
    elif par < 0:
        state["settledBorrowYield"] += settled
    return settled


def settle_reduced_exposure(state: dict, next_par: int) -> int:
    """Realize the proportional live yield removed with a partial reduction."""
    prev_par = int(state.get("par") or 0)
    next_value = int(next_par)
    live_yield = int(state.get("liveYield") or 0)
    if prev_par == 0 or live_yield == 0:
        return 0

    if prev_par > 0 and 0 <= next_value < prev_par:
        reduced_par = prev_par - next_value
        realized = _truncating_division(live_yield * reduced_par, prev_par)
        _add_settled_yield(state, realized, prev_par)
        state["liveYield"] -= realized
        return realized

    if prev_par < 0 and next_value <= 0:
        previous_debt = -prev_par
        next_debt = -next_value
        if next_debt < previous_debt:
            reduced_debt = previous_debt - next_debt
            realized = _truncating_division(live_yield * reduced_debt, previous_debt)
            _add_settled_yield(state, realized, prev_par)
            state["liveYield"] -= realized
            return realized
    return 0


def _truncating_division(numerator: int, denominator: int) -> int:
    """Match BigInt division, which truncates toward zero for signed values."""
    if denominator == 0:
        raise ZeroDivisionError("division by zero")
    value = abs(int(numerator)) // abs(int(denominator))
    return -value if (numerator < 0) != (denominator < 0) else value


def strict_event_key(account: str, market_id: str, event: dict) -> str:
    explicit = str(event.get("eventKey") or "").strip()
    if explicit:
        return explicit
    return ":".join((
        str(_integer(event.get("blockNumber"), 0)),
        str(_integer(event.get("transactionIndex"), 0)),
        str(_integer(event.get("logIndex"), 0)),
        str(account),
        str(market_id),
    ))


def _empty_state(account: str, market_id: str) -> dict:
    return {
        "account": str(account),
        "marketId": str(market_id),
        "par": 0,
        "lastIndex": None,
        "settledYield": 0,
        "settledSupplyYield": 0,
        "settledBorrowYield": 0,
        "liveYield": 0,
        "hadSupply": False,
        "hadBorrow": False,
    }


def _accrue_to(state: dict, next_index: int) -> Optional[str]:
    if next_index is None or next_index <= 0:
        return "missing_event_index"
    last_index = state.get("lastIndex")
    if state["par"] == 0:
        return None
    if last_index is None:
        state["lastIndex"] = next_index
        return None
    if next_index < last_index:
        return "non_monotonic_event_index"
    if next_index == last_index:
        return None
    delta_index = next_index - last_index
    delta_wei = (abs(state["par"]) * delta_index) // INDEX_SCALE
    state["liveYield"] += delta_wei if state["par"] > 0 else -delta_wei
    state["lastIndex"] = next_index
    return None


def _event_index_pair(evidence: dict, event_key: str) -> Optional[Tuple[int, int]]:
    raw = (evidence.get("eventIndexes") or {}).get(event_key)
    pair = (evidence.get("eventIndexPairs") or {}).get(event_key)
    if isinstance(pair, dict):
        supply = _integer(pair.get("supplyIndex"), None)
        borrow = _integer(pair.get("borrowIndex"), None)
    elif isinstance(raw, dict):
        supply = _integer(raw.get("supplyIndex"), None)
        borrow = _integer(raw.get("borrowIndex"), None)
    else:
        exact = _integer(raw, None)
        supply = exact
        borrow = exact
    if supply is None or borrow is None or supply <= 0 or borrow <= 0:
        return None
    return supply, borrow


def _ordered_history_events(history_payload: dict, comparison_block: int):
    events = []
    market_errors: Dict[str, str] = {}
    accounts = history_payload.get("accounts") or {}
    if not isinstance(accounts, dict):
        return [], {}, {"*": "unknown_account"}

    for raw_account, account_data in accounts.items():
        account = str(raw_account)
        if not isinstance(account_data, dict):
            continue
        markets = account_data.get("markets") or {}
        if not isinstance(markets, dict):
            continue
        account_known = account_data.get("accountKnown") is True
        for raw_market, market_data in markets.items():
            market_id = str(raw_market)
            if not account_known:
                market_errors[market_id] = "unknown_account"
                continue
            for event in ((market_data or {}).get("events") or []):
                if not isinstance(event, dict):
                    market_errors[market_id] = "invalid_event"
                    continue
                if event.get("accountKnown") is not True:
                    market_errors[market_id] = "unknown_account"
                    continue
                block_number = _integer(event.get("blockNumber"), None)
                new_par = _integer(event.get("newPar"), None)
                if block_number is None or new_par is None:
                    market_errors[market_id] = "invalid_event"
                    continue
                if block_number > comparison_block:
                    continue
                events.append({
                    **event,
                    "account": str(event.get("account") or account),
                    "marketId": str(event.get("marketId") or market_id),
                    "blockNumber": block_number,
                    "transactionIndex": _integer(event.get("transactionIndex"), 0),
                    "logIndex": _integer(event.get("logIndex"), 0),
                    "newPar": new_par,
                    "deltaWei": _integer(event.get("deltaWei"), 0),
                })
    events.sort(key=lambda row: (
        row["blockNumber"],
        row["transactionIndex"],
        row["logIndex"],
        row["account"],
        row["marketId"],
    ))
    return events, accounts, market_errors


def _market_ids(history_payload: dict, evidence: dict) -> set[str]:
    market_ids = {str(value) for value in (evidence.get("currentIndexes") or {})}
    for key in (evidence.get("currentPositions") or {}):
        if "|" in str(key):
            market_ids.add(str(key).split("|", 1)[1])
    for account_data in (history_payload.get("accounts") or {}).values():
        if isinstance(account_data, dict):
            market_ids.update(str(value) for value in (account_data.get("markets") or {}))
    return market_ids


def _coverage_result(history_payload: dict, evidence: dict, reason: str) -> dict:
    verification = {
        market_id: _incomplete_verification(reason)
        for market_id in sorted(_market_ids(history_payload, evidence))
    }
    return {
        "strictStatus": "coverage_incomplete",
        "reason": reason,
        "markets": {},
        "verification": verification,
        "accountStates": {},
        "replayStateData": {},
        "eventTrace": [],
        "openBorrowAccounts": [],
    }


def _incomplete_verification(reason: str) -> dict:
    return {
        "status": "coverage_incomplete",
        "strictStatus": "coverage_incomplete",
        "reason": reason,
        "counted": False,
        "canVerify": False,
        "rawVerified": False,
        "strictVerified": False,
        "snapshotIncomplete": False,
        "subgraphReplayTruncated": False,
        "replayStateAdjusted": False,
    }


def _current_index_pair(evidence: dict, market_id: str) -> Optional[Tuple[int, int]]:
    row = (evidence.get("currentIndexes") or {}).get(str(market_id))
    if not isinstance(row, dict):
        return None
    supply = _integer(row.get("supplyIndex"), None)
    borrow = _integer(row.get("borrowIndex"), None)
    if supply is None or borrow is None or supply <= 0 or borrow <= 0:
        return None
    return supply, borrow


def _aggregate_expected(states: Dict[str, dict], evidence: dict):
    account_has_borrow: Dict[str, bool] = {}
    for state in states.values():
        account_has_borrow[state["account"]] = (
            account_has_borrow.get(state["account"], False) or state["par"] < 0
        )

    expected: Dict[str, dict] = {}
    market_yield: Dict[str, dict] = {}
    for state in states.values():
        market_id = state["marketId"]
        expected_row = expected.setdefault(market_id, _empty_aggregate())
        yield_row = market_yield.setdefault(market_id, _empty_market_yield())
        yield_row["hadSupply"] = yield_row["hadSupply"] or state["hadSupply"]
        yield_row["hadBorrow"] = yield_row["hadBorrow"] or state["hadBorrow"]
        yield_row["settledSupplyYield"] += state["settledSupplyYield"]
        yield_row["settledBorrowYield"] += state["settledBorrowYield"]
        yield_row["settledYield"] += state["settledSupplyYield"]
        yield_row["earnYield"] += state["settledSupplyYield"]

        pair = _current_index_pair(evidence, market_id)
        if pair is None:
            expected_row["canVerify"] = False
            continue
        supply_index, borrow_index = pair
        if state["par"] > 0:
            expected_wei = par_to_wei_round_half_up(state["par"], supply_index)
            if account_has_borrow.get(state["account"], False):
                expected_row["collateralPar"] += state["par"]
                expected_row["collateralWei"] += expected_wei
                yield_row["currentCollateralSupplyPar"] += state["par"]
                yield_row["openCollateralYield"] += state["liveYield"]
            else:
                expected_row["supplyPar"] += state["par"]
                expected_row["supplyWei"] += expected_wei
                yield_row["currentSupplyPar"] += state["par"]
                yield_row["openSupplyYield"] += state["liveYield"]
            yield_row["earnYield"] += state["liveYield"]
        elif state["par"] < 0:
            borrow_par = -state["par"]
            borrow_wei = -par_to_wei_round_half_up(state["par"], borrow_index)
            expected_row["borrowPar"] += borrow_par
            expected_row["borrowWei"] += borrow_wei
            yield_row["currentBorrowPar"] += borrow_par
            yield_row["openBorrowYield"] += state["liveYield"]
    return expected, market_yield, account_has_borrow


def _empty_aggregate() -> dict:
    return {
        "supplyPar": 0,
        "supplyWei": 0,
        "collateralPar": 0,
        "collateralWei": 0,
        "borrowPar": 0,
        "borrowWei": 0,
        "canVerify": True,
    }


def _empty_market_yield() -> dict:
    return {
        "earnYield": 0,
        "settledYield": 0,
        "settledSupplyYield": 0,
        "settledBorrowYield": 0,
        "openBorrowYield": 0,
        "openSupplyYield": 0,
        "openCollateralYield": 0,
        "currentBorrowPar": 0,
        "currentSupplyPar": 0,
        "currentCollateralSupplyPar": 0,
        "hadSupply": False,
        "hadBorrow": False,
    }


def _aggregate_actual(evidence: dict):
    positions = evidence.get("currentPositions") or {}
    account_has_borrow: Dict[str, bool] = {}
    parsed_positions = []
    for key, row in positions.items():
        if not isinstance(row, dict) or "|" not in str(key):
            continue
        account, market_id = str(key).split("|", 1)
        par = _integer(row.get("par"), None)
        wei = _integer(row.get("wei"), None)
        if par is None or wei is None:
            continue
        parsed_positions.append((account, market_id, par, wei))
        if par < 0:
            account_has_borrow[account] = True
        elif account not in account_has_borrow:
            account_has_borrow[account] = False

    actual: Dict[str, dict] = {}
    for account, market_id, par, wei in parsed_positions:
        row = actual.setdefault(market_id, _empty_aggregate())
        if par > 0 and account_has_borrow.get(account, False):
            row["collateralPar"] += par
            row["collateralWei"] += wei
        elif par > 0:
            row["supplyPar"] += par
            row["supplyWei"] += wei
        elif par < 0:
            row["borrowPar"] += -par
            row["borrowWei"] += -wei
    return actual


def _verification_entry(expected: dict, actual: dict, status: str, reason=None) -> dict:
    entry = {
        "status": status,
        "strictStatus": status,
        "counted": status == "verified",
        "canVerify": status in {"verified", "mismatch"},
        "rawVerified": status == "verified",
        "strictVerified": status == "verified",
        "snapshotIncomplete": False,
        "subgraphReplayTruncated": False,
        "replayStateAdjusted": False,
        "expectedSupplyPar": str(expected["supplyPar"]),
        "expectedSupplyWei": str(expected["supplyWei"]),
        "expectedCollateralPar": str(expected["collateralPar"]),
        "expectedCollateralWei": str(expected["collateralWei"]),
        "expectedBorrowPar": str(expected["borrowPar"]),
        "expectedBorrowWei": str(expected["borrowWei"]),
        "actualSupplyPar": str(actual["supplyPar"]),
        "actualSupplyWei": str(actual["supplyWei"]),
        "actualCollateralPar": str(actual["collateralPar"]),
        "actualCollateralWei": str(actual["collateralWei"]),
        "actualBorrowPar": str(actual["borrowPar"]),
        "actualBorrowWei": str(actual["borrowWei"]),
        "supplyParDiff": str(actual["supplyPar"] - expected["supplyPar"]),
        "supplyWeiDiff": str(actual["supplyWei"] - expected["supplyWei"]),
        "collateralParDiff": str(actual["collateralPar"] - expected["collateralPar"]),
        "collateralWeiDiff": str(actual["collateralWei"] - expected["collateralWei"]),
        "borrowParDiff": str(actual["borrowPar"] - expected["borrowPar"]),
        "borrowWeiDiff": str(actual["borrowWei"] - expected["borrowWei"]),
        "parTolerance": "0",
        "supplyWeiTolerance": "0",
        "collateralWeiTolerance": "0",
        "borrowWeiTolerance": "0",
    }
    if reason:
        entry["reason"] = reason
    return entry


def _serialize_state(state: dict) -> dict:
    return {
        "account": state["account"],
        "marketId": state["marketId"],
        "par": str(state["par"]),
        "lastIndex": str(state["lastIndex"]) if state["lastIndex"] is not None else None,
        "settledYield": str(state["settledYield"]),
        "settledSupplyYield": str(state["settledSupplyYield"]),
        "settledBorrowYield": str(state["settledBorrowYield"]),
        "liveYield": str(state["liveYield"]),
        "hadSupply": bool(state["hadSupply"]),
        "hadBorrow": bool(state["hadBorrow"]),
    }


def _stringify_market(row: dict) -> dict:
    payload = {
        key: str(value) if isinstance(value, int) and not isinstance(value, bool) else value
        for key, value in row.items()
    }
    payload.update({
        "strictStatus": "verified",
        "strictMethod": "interest-ledger",
    })
    return payload


def _replay_state_row(expected: dict, market_yield: dict) -> dict:
    return {
        "expectedSupplyPar": str(expected["supplyPar"]),
        "expectedSupplyWei": str(expected["supplyWei"]),
        "expectedCollateralSupplyPar": str(expected["collateralPar"]),
        "expectedCollateralSupplyWei": str(expected["collateralWei"]),
        "expectedBorrowPar": str(expected["borrowPar"]),
        "expectedBorrowWei": str(expected["borrowWei"]),
        "hadSupply": bool(market_yield["hadSupply"]),
        "hadBorrow": bool(market_yield["hadBorrow"]),
        "canVerify": bool(expected["canVerify"]),
    }


def build_strict_replay(history_payload: dict, evidence: dict) -> dict:
    """Replay canonical events and publish only exactly reconciled markets."""
    if not isinstance(history_payload, dict) or not isinstance(evidence, dict):
        return _coverage_result({}, {}, "invalid_input")
    comparison_block = _integer(evidence.get("comparisonBlock"), 0)
    protocol_start = _integer(evidence.get("protocolStartBlock"), 0)
    scan_from = _integer((history_payload.get("scanRange") or {}).get("fromBlock"), 0)
    last_scanned = _integer(history_payload.get("lastScannedBlock"), 0)
    if comparison_block <= 0:
        return _coverage_result(history_payload, evidence, "missing_comparison_block")
    if protocol_start <= 0 or scan_from <= 0 or scan_from > protocol_start:
        return _coverage_result(history_payload, evidence, "history_starts_after_protocol_start")
    if last_scanned < comparison_block:
        return _coverage_result(history_payload, evidence, "stale_comparison_block")
    for flag, reason in (
        ("snapshotIncomplete", "snapshot_incomplete"),
        ("subgraphReplayTruncated", "replay_truncated"),
        ("replayStateAdjusted", "replay_state_adjusted"),
    ):
        if evidence.get(flag) is True:
            return _coverage_result(history_payload, evidence, reason)

    ordered_events, _accounts, market_errors = _ordered_history_events(
        history_payload,
        comparison_block,
    )
    states: Dict[str, dict] = {}
    event_trace = []

    for event in ordered_events:
        account = event["account"]
        market_id = event["marketId"]
        if market_id in market_errors:
            continue
        key = strict_event_key(account, market_id, event)
        pair = _event_index_pair(evidence, key)
        if pair is None:
            market_errors[market_id] = "missing_event_index"
            continue
        supply_index, borrow_index = pair
        state_key = f"{account}|{market_id}"
        state = states.setdefault(state_key, _empty_state(account, market_id))
        previous_par = state["par"]
        next_par = event["newPar"]
        event_index = (
            supply_index
            if previous_par > 0
            else borrow_index
            if previous_par < 0
            else borrow_index
            if next_par < 0
            else supply_index
        )
        accrue_error = _accrue_to(state, event_index)
        if accrue_error:
            market_errors[market_id] = accrue_error
            continue
        realized = settle_reduced_exposure(state, next_par)
        flips_sign = (
            previous_par != 0
            and next_par != 0
            and ((previous_par > 0 > next_par) or (previous_par < 0 < next_par))
        )
        state["par"] = next_par
        if previous_par > 0 or next_par > 0:
            state["hadSupply"] = True
        if previous_par < 0 or next_par < 0:
            state["hadBorrow"] = True
        if next_par == 0 or flips_sign:
            _add_settled_yield(state, state["liveYield"], previous_par)
            state["liveYield"] = 0
        state["lastIndex"] = (
            supply_index if next_par > 0 else borrow_index if next_par < 0 else None
        )
        event_trace.append({
            "order": f"{event['blockNumber']}:{event['transactionIndex']}:{event['logIndex']}",
            "eventKey": key,
            "account": account,
            "marketId": market_id,
            "flowType": str(event.get("flowType") or "u"),
            "parBefore": str(previous_par),
            "parAfter": str(next_par),
            "deltaWei": str(event["deltaWei"]),
            "index": str(event_index),
            "realizedYield": str(realized),
        })

    required_position_keys = set(states)
    supplied_position_keys = set((evidence.get("currentPositions") or {}).keys())
    for state_key in required_position_keys - supplied_position_keys:
        market_errors[state_key.split("|", 1)[1]] = "missing_current_position"

    for state in states.values():
        if state["marketId"] in market_errors or state["par"] == 0:
            continue
        pair = _current_index_pair(evidence, state["marketId"])
        if pair is None:
            market_errors[state["marketId"]] = "missing_current_index"
            continue
        current_index = pair[0] if state["par"] > 0 else pair[1]
        accrue_error = _accrue_to(state, current_index)
        if accrue_error:
            market_errors[state["marketId"]] = accrue_error

    expected_by_market, market_yield, account_has_borrow = _aggregate_expected(states, evidence)
    actual_by_market = _aggregate_actual(evidence)
    market_ids = (
        set(expected_by_market)
        | set(actual_by_market)
        | set(market_errors)
        | _market_ids(history_payload, evidence)
    )
    markets = {}
    verification = {}
    replay_state_data = {}

    for market_id in sorted(market_ids):
        if market_id in market_errors:
            verification[market_id] = _incomplete_verification(market_errors[market_id])
            continue
        expected = expected_by_market.get(market_id, _empty_aggregate())
        actual = actual_by_market.get(market_id, _empty_aggregate())
        if not expected["canVerify"]:
            verification[market_id] = _incomplete_verification("missing_current_index")
            continue
        differences = (
            actual["supplyPar"] - expected["supplyPar"],
            actual["supplyWei"] - expected["supplyWei"],
            actual["collateralPar"] - expected["collateralPar"],
            actual["collateralWei"] - expected["collateralWei"],
            actual["borrowPar"] - expected["borrowPar"],
            actual["borrowWei"] - expected["borrowWei"],
        )
        status = "verified" if all(value == 0 for value in differences) else "mismatch"
        verification[market_id] = _verification_entry(expected, actual, status)
        replay_state_data[market_id] = _replay_state_row(
            expected,
            market_yield.get(market_id, _empty_market_yield()),
        )
        if status == "verified":
            markets[market_id] = _stringify_market(
                market_yield.get(market_id, _empty_market_yield())
            )

    statuses = {row.get("status") for row in verification.values()}
    if statuses and statuses == {"verified"}:
        strict_status = "verified"
        reason = None
    elif "mismatch" in statuses:
        strict_status = "mismatch"
        reason = "exact_state_mismatch"
    else:
        strict_status = "coverage_incomplete"
        reason = "incomplete_strict_evidence"

    account_states = {
        key: _serialize_state(state)
        for key, state in states.items()
    }
    result = {
        "strictStatus": strict_status,
        "markets": markets,
        "verification": verification,
        "accountStates": account_states,
        "replayStateData": replay_state_data,
        "eventTrace": event_trace,
        "openBorrowAccounts": sorted(
            account for account, has_borrow in account_has_borrow.items() if has_borrow
        ),
    }
    if reason:
        result["reason"] = reason
    return result
