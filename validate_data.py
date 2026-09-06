#!/usr/bin/env python3
"""
Data Validation Guard — runs BEFORE git commit in CI/CD.
If any check fails, exits with code 1 → workflow skips commit → production data stays intact.

Usage:
    python3 validate_data.py                     # validate all files
    python3 validate_data.py dolo_flows.json     # validate specific file(s)
"""

import json
import math
import re
import sys
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from vedolo_vote_power import VEDOLO_CONTRACT

# ── Validation Rules ─────────────────────────────────────────────────────────
# Each file has: required top-level keys, optional nested checks, and min-size thresholds.

def _is_iso_datetime(value):
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return True
    except (TypeError, ValueError):
        return False


ALL_TVL_CHAINS = {
    "Ethereum",
    "Berachain",
    "Botanix",
    "Polygon zkEVM",
    "Mantle",
    "Arbitrum",
    "X Layer",
}
RETIRED_TVL_CHAINS = {"Botanix", "Polygon zkEVM"}
EXPECTED_TVL_CHAINS = ALL_TVL_CHAINS - RETIRED_TVL_CHAINS
EXPECTED_ASSETS_LIVE_CHAIN_COUNT = 5
RETIRED_ASSETS_LIVE_CHAINS = {"botanix", "polygonzkevm"}
ODOLO_FUTURE_REWARDS_WALLET = "0x79e6e932bf6686a4d357d7821e6e08835ba8a026"
ODOLO_ALLOCATION = 200_000_000.0
ODOLO_TOKEN_ADDRESS = "0x02e513b5b54ee216bf836ceb471507488fc89543"
ODOLO_CLAIMS_DISTRIBUTOR = "0x79e6e932bf6686a4d357d7821e6e08835ba8a026"
VEDOLO_DEPLOYMENT_BLOCK = 2_926_448
DOLO_WEI = Decimal(10) ** 18
NON_CHAIN_TVL_KEYS = {
    "borrowed",
    "staking",
    "pool2",
    "vesting",
    "offers",
    "treasury",
    "cex",
    "governance",
}


def _age_hours(value):
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600


def _fresh_timestamp(value, max_hours=4):
    try:
        return 0 <= _age_hours(value) <= max_hours
    except (TypeError, ValueError):
        return False


def _nearly_equal(a, b, rel=1e-8, abs_tol=5.0):
    try:
        a = float(a)
        b = float(b)
    except (TypeError, ValueError):
        return False
    return abs(a - b) <= max(abs_tol, max(abs(a), abs(b)) * rel)


def _early_exit_raw(value):
    if isinstance(value, bool) or not isinstance(value, str) or not value.isdigit():
        raise ValueError("raw DOLO value must be a non-negative integer string")
    return int(value)


def _early_exit_decimal_matches_raw(value, raw_value):
    try:
        return Decimal(str(value)) == Decimal(_early_exit_raw(raw_value)) / DOLO_WEI
    except (InvalidOperation, TypeError, ValueError):
        return False


def _early_exit_coverage_complete(data):
    try:
        coverage = data["coverage"]
        stats = data["stats"]
        rows = data["recent_exits"]
        early = stats["total_early_exits"]
        normal = stats["total_normal_exits"]
        withdrawals = stats["total_withdrawals"]
        unique_txs = stats["unique_withdrawal_transactions"]
        count_values = (early, normal, withdrawals, unique_txs)
        if any(type(value) is not int or value < 0 for value in count_values):
            return False
        return (
            data.get("schemaVersion") == 2
            and coverage.get("complete") is True
            and coverage.get("fromBlock") == VEDOLO_DEPLOYMENT_BLOCK
            and type(coverage.get("toBlock")) is int
            and coverage["toBlock"] >= coverage["fromBlock"]
            and coverage.get("eventCount") == withdrawals
            and coverage.get("uniqueTransactionCount") == unique_txs
            and 0 < unique_txs <= withdrawals
            and early + normal == withdrawals
            and len(rows) == early
            and _fresh_timestamp(stats.get("last_updated"), max_hours=12)
        )
    except (KeyError, TypeError):
        return False


def _early_exit_rows_reconcile(data):
    try:
        rows = data["recent_exits"]
        stats = data["stats"]
        if not isinstance(rows, list):
            return False
        seen_events = set()
        totals = {
            "burn_fee_raw": 0,
            "recoup_fee_raw": 0,
            "total_penalty_raw": 0,
            "original_locked_raw": 0,
            "user_received_raw": 0,
        }
        for row in rows:
            if not isinstance(row, dict):
                return False
            tx_hash = row.get("tx_hash")
            address = row.get("address")
            log_index = row.get("log_index")
            event_id = row.get("event_id")
            if (
                not isinstance(tx_hash, str)
                or len(tx_hash) != 66
                or not tx_hash.startswith("0x")
                or not isinstance(address, str)
                or len(address) != 42
                or not address.startswith("0x")
                or type(log_index) is not int
                or log_index < 0
                or event_id != f"{tx_hash.lower()}:{log_index}"
                or event_id in seen_events
            ):
                return False
            int(tx_hash[2:], 16)
            int(address[2:], 16)
            seen_events.add(event_id)
            burn = _early_exit_raw(row.get("burn_fee_raw"))
            recoup = _early_exit_raw(row.get("recoup_fee_raw"))
            penalty = _early_exit_raw(row.get("total_penalty_raw"))
            original = _early_exit_raw(row.get("original_locked_raw"))
            received = _early_exit_raw(row.get("user_received_raw"))
            if penalty <= 0 or burn + recoup != penalty or received + penalty != original:
                return False
            for decimal_key, raw_key in (
                ("burn_fee", "burn_fee_raw"),
                ("recoup_fee", "recoup_fee_raw"),
                ("total_penalty", "total_penalty_raw"),
                ("original_locked", "original_locked_raw"),
                ("user_received", "user_received_raw"),
            ):
                if not _early_exit_decimal_matches_raw(row.get(decimal_key), row.get(raw_key)):
                    return False
            expected_pct = (
                Decimal(penalty) * Decimal(100) / Decimal(original)
            ).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            if Decimal(str(row.get("penalty_pct"))) != expected_pct:
                return False
            for key, value in (
                ("burn_fee_raw", burn),
                ("recoup_fee_raw", recoup),
                ("total_penalty_raw", penalty),
                ("original_locked_raw", original),
                ("user_received_raw", received),
            ):
                totals[key] += value

        stat_mapping = {
            "total_burn_fee_raw": "burn_fee_raw",
            "total_recoup_fee_raw": "recoup_fee_raw",
            "total_penalty_raw": "total_penalty_raw",
            "total_original_locked_raw": "original_locked_raw",
            "total_received_raw": "user_received_raw",
        }
        for stat_raw_key, total_key in stat_mapping.items():
            if _early_exit_raw(stats.get(stat_raw_key)) != totals[total_key]:
                return False
        for decimal_key, raw_key in (
            ("total_burn_fee_dolo", "total_burn_fee_raw"),
            ("total_recoup_fee_dolo", "total_recoup_fee_raw"),
            ("total_penalty_dolo", "total_penalty_raw"),
            ("total_original_locked", "total_original_locked_raw"),
            ("total_received_dolo", "total_received_raw"),
        ):
            if not _early_exit_decimal_matches_raw(stats.get(decimal_key), stats.get(raw_key)):
                return False
        total_original = totals["original_locked_raw"]
        expected_avg = Decimal(0) if not total_original else (
            Decimal(totals["total_penalty_raw"]) * Decimal(100) / Decimal(total_original)
        ).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        return Decimal(str(stats.get("avg_penalty_pct"))) == expected_avg
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return False


def _safe_number(value):
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _finite_real_json_number(value):
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value)


def _finite_json_integer_number(value):
    return _finite_real_json_number(value) and value == int(value)


def _nonnegative_integer(value):
    if not isinstance(value, str) or not value.isdigit():
        return None
    return int(value)


def _is_exact_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _decimal_to_wei(value):
    if not isinstance(value, str) or not value or value.startswith("-"):
        return None
    whole, separator, fraction = value.partition(".")
    if not whole.isdigit() or (separator and (not fraction or not fraction.isdigit())):
        return None
    if len(fraction) > 18:
        return None
    return int(whole) * 10**18 + int((fraction + "0" * 18)[:18] or "0")


def _vedolo_vote_power_history_valid(data):
    if not isinstance(data, dict) or (
        not _is_exact_integer(data.get("schemaVersion"))
        or data.get("schemaVersion") != 1
        or data.get("metric") != "votePower"
        or data.get("chain") != "berachain"
        or str(data.get("contract", "")).lower() != VEDOLO_CONTRACT.lower()
        or data.get("source") != "global-point-history"
    ):
        return False

    target_block = data.get("targetBlock")
    target_timestamp = data.get("targetTimestamp")
    total_supply_wei = _nonnegative_integer(data.get("totalSupplyWei"))
    locked_supply_wei = _nonnegative_integer(data.get("lockedSupplyWei"))
    last_point_wei = _nonnegative_integer(data.get("lastPointWei"))
    coverage = data.get("coverage")
    points = data.get("points")
    if (
        not _is_exact_integer(target_block)
        or target_block < 0
        or not _is_exact_integer(target_timestamp)
        or target_timestamp < 0
        or total_supply_wei is None
        or locked_supply_wei is None
        or last_point_wei is None
        or not isinstance(coverage, dict)
        or not isinstance(points, list)
        or not points
    ):
        return False

    previous_timestamp = None
    point_wei = None
    for point in points:
        if not isinstance(point, list) or len(point) != 2:
            return False
        timestamp, decimal_value = point
        if (
            not _is_exact_integer(timestamp)
            or timestamp < 0
            or (previous_timestamp is not None and timestamp <= previous_timestamp)
        ):
            return False
        point_wei = _decimal_to_wei(decimal_value)
        if point_wei is None:
            return False
        previous_timestamp = timestamp

    next_daily_timestamp = (points[0][0] // 86400 + 1) * 86400
    for timestamp, _ in points[1:-1]:
        if timestamp != next_daily_timestamp:
            return False
        next_daily_timestamp += 86400

    return (
        _is_exact_integer(coverage.get("from"))
        and _is_exact_integer(coverage.get("through"))
        and coverage.get("from") == points[0][0]
        and coverage.get("through") == target_timestamp
        and points[-1][0] == target_timestamp
        and next_daily_timestamp >= target_timestamp
        and point_wei == last_point_wei == total_supply_wei
    )


def _vedolo_early_unlock_valid(data):
    if not isinstance(data, dict) or data.get("schemaVersion") != 1:
        return False
    if not _is_exact_integer(data.get("sourceBlock")) or data.get("sourceBlock") < 0:
        return False
    if not _is_iso_datetime(data.get("sourceTimestamp")) or not _is_iso_datetime(data.get("generatedAt")):
        return False
    fee_calculator = str(data.get("feeCalculator") or "")
    wallets = data.get("wallets")
    if len(fee_calculator) != 42 or not fee_calculator.startswith("0x") or not isinstance(wallets, list):
        return False
    try:
        snapshot_timestamp = int(datetime.fromisoformat(data["sourceTimestamp"].replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError, KeyError):
        return False

    position_count = 0
    previous_available = None
    for expected_rank, wallet in enumerate(wallets, start=1):
        if not isinstance(wallet, dict) or wallet.get("rank") != expected_rank:
            return False
        address = str(wallet.get("address") or "")
        positions = wallet.get("positions")
        if len(address) != 42 or not address.startswith("0x") or not isinstance(positions, list) or not positions:
            return False

        locked_total = vote_total = available_total = penalty_total = weighted_remaining = 0
        has_active = False
        for position in positions:
            if not isinstance(position, dict):
                return False
            locked = _nonnegative_integer(position.get("lockedDoloWei"))
            vote = _nonnegative_integer(position.get("veDoloWei"))
            available = _nonnegative_integer(position.get("availableAfterExitWei"))
            burn = _nonnegative_integer(position.get("burnFeeWei"))
            recoup = _nonnegative_integer(position.get("recoupFeeWei"))
            penalty = _nonnegative_integer(position.get("penaltyWei"))
            remaining = position.get("remainingSeconds")
            end = position.get("end")
            if None in (locked, vote, available, burn, recoup, penalty):
                return False
            if not _is_exact_integer(remaining) or not _is_exact_integer(end):
                return False
            expected_remaining = max(end - snapshot_timestamp, 0)
            expected_status = "early_exit" if expected_remaining else "expired"
            if remaining != expected_remaining or position.get("status") != expected_status:
                return False
            if burn + recoup != penalty or available + penalty != locked:
                return False
            if any(
                _decimal_to_wei(position.get(decimal_key)) != raw_value
                for decimal_key, raw_value in (
                    ("lockedDolo", locked),
                    ("veDolo", vote),
                    ("availableAfterExit", available),
                    ("penalty", penalty),
                )
            ):
                return False
            locked_total += locked
            vote_total += vote
            available_total += available
            penalty_total += penalty
            weighted_remaining += locked * remaining
            has_active = has_active or expected_status == "early_exit"
            position_count += 1

        wallet_values = (
            ("lockedDoloWei", "lockedDolo", locked_total),
            ("veDoloWei", "veDolo", vote_total),
            ("availableAfterExitWei", "availableAfterExit", available_total),
            ("penaltyWei", "penalty", penalty_total),
        )
        if not has_active or any(
            _nonnegative_integer(wallet.get(raw_key)) != total
            or _decimal_to_wei(wallet.get(decimal_key)) != total
            for raw_key, decimal_key, total in wallet_values
        ):
            return False
        expected_weeks = weighted_remaining / (locked_total * 7 * 24 * 60 * 60) if locked_total else 0
        if not math.isclose(_safe_number(wallet.get("avgWeeksUntilUnlock")), expected_weeks, rel_tol=1e-12, abs_tol=1e-12):
            return False
        if previous_available is not None and available_total > previous_available:
            return False
        previous_available = available_total

    return data.get("walletCount") == len(wallets) and data.get("positionCount") == position_count


def _odolo_circulating_reconciles(data):
    try:
        total_supply = float(data.get("totalSupply"))
        future_rewards_reserve = float(data.get("futureRewardsReserve"))
        in_vester_balance = float(data.get("inVesterBalance"))
        in_circulation = float(data.get("inCirculation"))
    except (TypeError, ValueError):
        return False
    expected = max(0.0, total_supply - future_rewards_reserve - in_vester_balance)
    return _nearly_equal(in_circulation, expected, abs_tol=2.0)


def _odolo_allocation_reconciles(data):
    try:
        allocation = float(data.get("allocationSupply"))
        current = float(data.get("totalSupply"))
        components = sum(float(data.get(key)) for key in (
            "futureRewardsReserve",
            "inVesterBalance",
            "inCirculation",
            "redeemedAndBurned",
        ))
        burned = float(data.get("redeemedAndBurned"))
    except (TypeError, ValueError):
        return False
    display_reconciles = (
        _nearly_equal(allocation, ODOLO_ALLOCATION, abs_tol=0.01)
        and current <= allocation
        and burned >= 0
        and _nearly_equal(components, allocation, abs_tol=2.0)
        and _nearly_equal(current + burned, allocation, abs_tol=2.0)
    )
    raw_keys = (
        "allocationSupplyWei",
        "totalSupplyWei",
        "futureRewardsReserveWei",
        "inVesterBalanceWei",
        "inCirculationWei",
        "redeemedAndBurnedWei",
    )
    if not any(key in data for key in raw_keys):
        return display_reconciles
    raw = {key: _nonnegative_integer(data.get(key)) for key in raw_keys}
    if any(value is None for value in raw.values()):
        return False
    display_raw_pairs = {
        "allocationSupply": "allocationSupplyWei",
        "totalSupply": "totalSupplyWei",
        "futureRewardsReserve": "futureRewardsReserveWei",
        "inVesterBalance": "inVesterBalanceWei",
        "inCirculation": "inCirculationWei",
        "redeemedAndBurned": "redeemedAndBurnedWei",
    }
    try:
        display_matches_raw = all(
            abs(
                Decimal(str(data.get(display_key)))
                - Decimal(raw[raw_key]) / DOLO_WEI
            ) <= Decimal("0.000001")
            for display_key, raw_key in display_raw_pairs.items()
        )
    except (InvalidOperation, TypeError, ValueError):
        return False
    return (
        display_reconciles
        and display_matches_raw
        and raw["allocationSupplyWei"] == 200_000_000 * 10**18
        and raw["totalSupplyWei"]
        == raw["futureRewardsReserveWei"]
        + raw["inVesterBalanceWei"]
        + raw["inCirculationWei"]
        and raw["allocationSupplyWei"]
        == raw["totalSupplyWei"] + raw["redeemedAndBurnedWei"]
    )


ODOLO_FLOW_PERIOD_ORDER = ["1d", "7d", "30d", "90d", "180d", "all"]


def _odolo_flow_period_counts(data):
    periods = data.get("periods") or {}
    counts = []
    for key in ODOLO_FLOW_PERIOD_ORDER:
        if key not in periods:
            return []
        try:
            counts.append(int(periods[key].get("total_transfers")))
        except (TypeError, ValueError):
            return []
    return counts


def _odolo_flow_windows_are_monotonic(data):
    counts = _odolo_flow_period_counts(data)
    return bool(counts) and all(a <= b for a, b in zip(counts, counts[1:]))


def _odolo_flow_windows_are_not_collapsed(data):
    counts = _odolo_flow_period_counts(data)
    if not counts:
        return False
    # oDOLO is a mature dataset. If every window has the same non-trivial
    # transfer count, current_block/cutoff logic almost certainly collapsed to
    # DEPLOY_BLOCK and every range became all-time.
    if counts[-1] >= 1000 and len(set(counts)) == 1:
        return False
    return True


def _odolo_flow_block_metadata_is_valid(data):
    try:
        current_block = int(data.get("current_block"))
        chain_head = int(data.get("chain_head"))
        deploy_block = int(data.get("deploy_block"))
        cutoffs = data.get("cutoff_blocks") or {}
        coverage = data.get("transfer_coverage") or {}
        oldest_needed = int(coverage.get("oldest_needed_block"))
        scanned_from = int(coverage.get("scanned_from_block"))
        min_cached = int(coverage.get("min_cached_block"))
        max_cached = int(coverage.get("max_cached_block"))
        recent_rescan_blocks = int(coverage.get("recent_rescan_blocks"))
        reorg_buffer_blocks = int(coverage.get("reorg_buffer_blocks"))
        state_schema_version = int(coverage.get("state_schema_version"))
    except (TypeError, ValueError):
        return False
    if deploy_block <= 0 or current_block < deploy_block or chain_head < current_block:
        return False
    if recent_rescan_blocks <= 0 or reorg_buffer_blocks < 0 or state_schema_version < 2:
        return False
    if oldest_needed != deploy_block or scanned_from > oldest_needed or max_cached > current_block:
        return False
    if min_cached and min_cached < scanned_from:
        return False
    last_cutoff = -1
    for key in ODOLO_FLOW_PERIOD_ORDER:
        if key not in cutoffs:
            return False
        try:
            cutoff = int(cutoffs[key])
        except (TypeError, ValueError):
            return False
        if cutoff < deploy_block or cutoff > current_block:
            return False
        if last_cutoff != -1 and cutoff > last_cutoff:
            return False
        last_cutoff = cutoff
    return int(cutoffs["all"]) == deploy_block


def _odolo_claimer_partitions_reconcile(data):
    """Claim lifecycle is a partition; live Held is intentionally independent."""
    groups = []
    behavior = data.get("claimer_behavior") or {}
    behavior_rows = behavior.get("all_claimers") or []
    groups.append((behavior_rows, behavior.get("total_claimed"), behavior.get("total_claimers")))
    for period_data in (data.get("claimer_periods") or {}).values():
        groups.append((
            period_data.get("all_claimers") or [],
            None,
            period_data.get("total_claimers"),
        ))

    for rows, expected_claimed, expected_count in groups:
        addresses = [str(row.get("address") or "").lower() for row in rows]
        if any(not address for address in addresses) or len(addresses) != len(set(addresses)):
            return False
        if expected_count is not None and int(expected_count) != len(rows):
            return False
        for row in rows:
            claimed = _safe_number(row.get("claimed"))
            partition = sum(_safe_number(row.get(key)) for key in ("exercised", "outflow", "claim_remaining"))
            if claimed < 0 or not _nearly_equal(claimed, partition, rel=1e-8, abs_tol=0.05):
                return False
        if expected_claimed is not None:
            row_total = sum(_safe_number(row.get("claimed")) for row in rows)
            if not _nearly_equal(expected_claimed, row_total, rel=1e-8, abs_tol=max(0.05, len(rows) * 0.03)):
                return False
    return True


def _odolo_claimer_aggregates_reconcile(data):
    groups = [data.get("claimer_behavior")]
    groups.extend((data.get("claimer_periods") or {}).values())
    required_totals = {
        "total_claimed": "claimed",
        "total_exercised": "exercised",
        "total_outflow": "outflow",
        "total_claim_remaining": "claim_remaining",
        "total_held": "held",
    }
    if not groups or any(not isinstance(group, dict) for group in groups):
        return False
    for group in groups:
        rows = group.get("all_claimers")
        if not isinstance(rows, list):
            return False
        if group.get("total_claimers") != len(rows):
            return False
        bought_extra_count = sum(
            1 for row in rows if _safe_number(row.get("bought_extra")) > 0
        )
        if group.get("count_bought_extra") != bought_extra_count:
            return False
        for total_key, row_key in required_totals.items():
            if not _finite_real_json_number(group.get(total_key)):
                return False
            row_total = sum(_safe_number(row.get(row_key)) for row in rows)
            if not _nearly_equal(
                group[total_key],
                row_total,
                rel=0,
                abs_tol=0.011,
            ):
                return False
    return True


def _odolo_claim_total_within_allocation(data):
    try:
        claimed = float((data.get("claimer_behavior") or {}).get("total_claimed"))
    except (TypeError, ValueError):
        return False
    return 0 <= claimed <= ODOLO_ALLOCATION


def _odolo_claim_source_reconciliation_is_valid(data):
    current_block = data.get("current_block")
    deploy_block = data.get("deploy_block")
    if not _is_exact_integer(current_block) or not _is_exact_integer(deploy_block):
        return False
    groups = [(data.get("claimer_behavior"), data.get("claim_source_reconciliation"))]
    groups.extend(
        (period, period.get("claim_source_reconciliation"))
        for period in (data.get("claimer_periods") or {}).values()
        if isinstance(period, dict)
    )
    for group, reconciliation in groups:
        if not isinstance(group, dict) or not isinstance(reconciliation, dict):
            return False
        if not isinstance(reconciliation.get("methodology"), str) or not reconciliation["methodology"]:
            return False
        first_block = reconciliation.get("first_canonical_event_block")
        ignored_count = reconciliation.get("ignored_post_index_transfer_count")
        coverage_from = reconciliation.get("coverage_from_block")
        coverage_to = reconciliation.get("coverage_to_block")
        coverage_lag = reconciliation.get("coverage_lag_blocks")
        if not _is_exact_integer(first_block) or first_block <= 0:
            return False
        if not _is_exact_integer(ignored_count) or ignored_count < 0:
            return False
        if (
            reconciliation.get("coverage_status") != "complete"
            or not _is_exact_integer(coverage_from)
            or not _is_exact_integer(coverage_to)
            or not _is_exact_integer(coverage_lag)
            or coverage_from > deploy_block
            or coverage_to < current_block
            or not coverage_from <= first_block <= coverage_to
            or coverage_lag < 0
        ):
            return False
        for key in (
            "historical_transfer_claimed",
            "canonical_event_claimed",
            "post_index_transfer_observed",
            "ignored_post_index_transfer_amount",
        ):
            value = reconciliation.get(key)
            if not _finite_real_json_number(value) or value < 0:
                return False
        if reconciliation["ignored_post_index_transfer_amount"] > reconciliation["post_index_transfer_observed"]:
            return False
        claimed = group.get("total_claimed")
        claimers = group.get("total_claimers")
        if not _finite_real_json_number(claimed) or not _is_exact_integer(claimers) or claimers < 0:
            return False
        source_total = (
            reconciliation["historical_transfer_claimed"]
            + reconciliation["canonical_event_claimed"]
        )
        row_rounding_tolerance = max(0.011, claimers * 0.0051)
        if not _nearly_equal(
            claimed,
            source_total,
            rel=0,
            abs_tol=row_rounding_tolerance,
        ):
            return False
    return True


def _odolo_flow_components_reconcile(data):
    for period_data in (data.get("periods") or {}).values():
        for key in ("accumulators", "sellers", "claimer_sellers"):
            for row in period_data.get(key, []) or []:
                try:
                    gross_inflow = float(row["gross_inflow"])
                    gross_outflow = float(row["gross_outflow"])
                    net_flow = float(row["net_flow"])
                except (KeyError, TypeError, ValueError):
                    return False
                if gross_inflow < 0 or gross_outflow < 0:
                    return False
                if not _nearly_equal(net_flow, gross_inflow - gross_outflow, abs_tol=0.05):
                    return False
    return True


def _odolo_exerciser_totals(data):
    totals = {
        "total_vedolo": 0.0,
        "total_odolo_exercised": 0.0,
        "total_odolo_exercise_usdc_paid": 0.0,
        "total_odolo_exercised_exercises": 0,
        "total_dolo_pair_vedolo": 0.0,
        "total_dolo_pair_exercises": 0,
        "total_dolo_paired": 0.0,
    }
    for exerciser in data.get("exercisers", []):
        for tx in exerciser.get("txs", []):
            vedolo = float(tx.get("vedolo") or 0)
            if vedolo <= 0:
                continue
            totals["total_vedolo"] += vedolo
            if tx.get("paid_token") == "DOLO":
                totals["total_dolo_pair_vedolo"] += vedolo
                totals["total_dolo_pair_exercises"] += 1
                totals["total_dolo_paired"] += float(tx.get("dolo_paid") or 0)
            else:
                totals["total_odolo_exercised"] += vedolo
                totals["total_odolo_exercise_usdc_paid"] += float(tx.get("usdc") or 0)
                totals["total_odolo_exercised_exercises"] += 1
    return totals


def _odolo_exerciser_totals_reconcile(data):
    totals = _odolo_exerciser_totals(data)
    return (
        _nearly_equal(data.get("total_vedolo"), totals["total_vedolo"], abs_tol=2.0)
        and _nearly_equal(data.get("total_odolo_exercised"), totals["total_odolo_exercised"], abs_tol=2.0)
        and _nearly_equal(data.get("total_odolo_exercise_usdc_paid"), totals["total_odolo_exercise_usdc_paid"], abs_tol=2.0)
        and int(data.get("total_odolo_exercised_exercises", -1)) == totals["total_odolo_exercised_exercises"]
        and _nearly_equal(data.get("total_dolo_pair_vedolo"), totals["total_dolo_pair_vedolo"], abs_tol=2.0)
        and int(data.get("total_dolo_pair_exercises", -1)) == totals["total_dolo_pair_exercises"]
        and _nearly_equal(data.get("total_dolo_paired"), totals["total_dolo_paired"], abs_tol=2.0)
    )


def _odolo_exerciser_address_totals_reconcile(data):
    required_current_keys = [
        "current_vedolo_locked",
        "current_vedolo_positions",
        "current_vedolo_vote_weight",
        "current_locked_delta_vs_usdc_exercise",
        "current_locked_delta_vs_all_exercise_history",
    ]
    for exerciser in data.get("exercisers", []):
        if any(key not in exerciser for key in required_current_keys):
            return False
        totals = _odolo_exerciser_totals({"exercisers": [exerciser]})
        current_locked = float(exerciser.get("current_vedolo_locked") or 0)
        if not (
            _nearly_equal(exerciser.get("total_vedolo"), totals["total_vedolo"], abs_tol=2.0)
            and _nearly_equal(exerciser.get("total_odolo_exercised"), totals["total_odolo_exercised"], abs_tol=2.0)
            and _nearly_equal(exerciser.get("total_odolo_exercise_usdc_paid"), totals["total_odolo_exercise_usdc_paid"], abs_tol=2.0)
            and int(exerciser.get("total_odolo_exercised_exercises", -1)) == totals["total_odolo_exercised_exercises"]
            and _nearly_equal(exerciser.get("total_dolo_pair_vedolo"), totals["total_dolo_pair_vedolo"], abs_tol=2.0)
            and int(exerciser.get("total_dolo_pair_exercises", -1)) == totals["total_dolo_pair_exercises"]
            and _nearly_equal(exerciser.get("total_dolo_paired"), totals["total_dolo_paired"], abs_tol=2.0)
            and _nearly_equal(exerciser.get("current_locked_delta_vs_usdc_exercise"), current_locked - totals["total_odolo_exercised"], abs_tol=2.0)
            and _nearly_equal(exerciser.get("current_locked_delta_vs_all_exercise_history"), current_locked - totals["total_vedolo"], abs_tol=2.0)
        ):
            return False
    return True


def _odolo_exercise_transactions_are_valid(data):
    seen_hashes = set()
    for exerciser in data.get("exercisers", []):
        for tx in exerciser.get("txs", []):
            tx_hash = str(tx.get("hash") or "").lower()
            if len(tx_hash) != 66 or not tx_hash.startswith("0x") or tx_hash in seen_hashes:
                return False
            seen_hashes.add(tx_hash)
            try:
                timestamp = int(tx.get("timestamp"))
                expected_date = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")
                vedolo = float(tx.get("vedolo") or 0)
                lock_days = float(tx.get("lock_days"))
                lock_seconds = tx.get("lock_seconds")
            except (TypeError, ValueError, OSError):
                return False
            if (
                timestamp <= 0
                or tx.get("date") != expected_date
                or vedolo <= 0
                or lock_days <= 0
                or isinstance(lock_seconds, bool)
                or type(lock_seconds) is not int
                or lock_seconds <= 0
                or abs(lock_days - lock_seconds / 86400) > 0.051
            ):
                return False
            paid_token = tx.get("paid_token")
            if paid_token == "DOLO":
                if float(tx.get("dolo_paid") or 0) < 0 or tx.get("usdc") is not None:
                    return False
            elif paid_token == "USDC.e":
                if float(tx.get("usdc") or 0) < 0 or tx.get("dolo_paid") is not None:
                    return False
            else:
                return False
    return bool(seen_hashes)


def _current_chain_tvls(data):
    current = data.get("currentChainTvls", {})
    return {
        key: float(value)
        for key, value in current.items()
        if isinstance(value, (int, float))
        and "-" not in key
        and key.lower() not in NON_CHAIN_TVL_KEYS
    }


def _borrowed_chain_tvls(data):
    current = data.get("currentChainTvls", {})
    return {
        key[:-9]: float(value)
        for key, value in current.items()
        if isinstance(value, (int, float)) and key.endswith("-borrowed")
    }


def _has_expected_tvl_chains(data):
    return EXPECTED_TVL_CHAINS.issubset(set(_current_chain_tvls(data)))


def _has_expected_tvl_retired_chains(data):
    active = set(_current_chain_tvls(data))
    retired = set(data.get("retiredChains", []))
    return RETIRED_TVL_CHAINS.issubset(active | retired)


def _has_expected_assets_live_chains(data):
    try:
        chain_count = int(data.get("chainCount"))
    except (TypeError, ValueError):
        return False
    return chain_count >= EXPECTED_ASSETS_LIVE_CHAIN_COUNT and len(data.get("chains", [])) >= EXPECTED_ASSETS_LIVE_CHAIN_COUNT


def _has_expected_assets_live_retired_chains(data):
    active = set(data.get("chains", []))
    retired = set(data.get("retiredChains", []))
    return RETIRED_ASSETS_LIVE_CHAINS.issubset(active | retired)


def _dolomite_tvl_totals_reconcile(data):
    chains = _current_chain_tvls(data)
    borrowed = _borrowed_chain_tvls(data)
    total_tvl = data.get("totalTvl")
    total_borrowed = data.get("totalBorrowed")
    supply = data.get("supplyLiquidity")
    return (
        _nearly_equal(sum(chains.values()), total_tvl)
        and _nearly_equal(sum(borrowed.values()), total_borrowed)
        and _nearly_equal(float(total_tvl) + float(total_borrowed), supply)
        and _nearly_equal(data.get("currentChainTvls", {}).get("borrowed"), total_borrowed)
    )


def _dolomite_token_sums_reconcile(data):
    chains = _current_chain_tvls(data)
    borrowed = _borrowed_chain_tvls(data)
    chain_tokens = data.get("chainTokensInUsd", {})
    for chain in EXPECTED_TVL_CHAINS:
        token_sum = sum(
            float(value)
            for value in chain_tokens.get(chain, {}).values()
            if isinstance(value, (int, float))
        )
        supply = chains.get(chain, 0.0) + borrowed.get(chain, 0.0)
        if not _nearly_equal(token_sum, supply):
            return False

    latest = (data.get("tokensInUsd") or [{}])[-1].get("tokens", {})
    total_tokens = sum(float(value) for value in latest.values() if isinstance(value, (int, float)))
    return _nearly_equal(total_tokens, data.get("supplyLiquidity"))


def _dolomite_chain_meta_complete(data):
    meta = data.get("chainMeta", {})
    if not EXPECTED_TVL_CHAINS.issubset(set(meta)):
        return False
    for chain in EXPECTED_TVL_CHAINS:
        block = meta.get(chain, {})
        if not block.get("blockNumber") or not block.get("blockHash") or not block.get("blockTimestamp"):
            return False
    return True


def _dolomite_stale_chains_known(data):
    stale = set(data.get("staleChains", []))
    return stale.issubset(ALL_TVL_CHAINS)


def _defillama_history_series_valid(data, key):
    rows = data.get(key, [])
    if len(rows) < 1000:
        return False
    previous = 0
    for row in rows:
        date = row.get("date")
        value = row.get("totalLiquidityUSD")
        if (
            isinstance(date, bool)
            or not isinstance(date, int)
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            return False
        if date <= previous:
            return False
        previous = date
    return True


def _defillama_history_valid(data):
    return _defillama_history_series_valid(data, "tvl")


def _defillama_total_supply_history_valid(data):
    return _defillama_history_series_valid(data, "totalSupply")


def _dolomite_history_matches_current(data, history_key, current_key):
    rows = data.get(history_key, [])
    current_value = data.get(current_key)
    if not rows or isinstance(current_value, bool):
        return False
    if not isinstance(current_value, (int, float)) or not math.isfinite(current_value):
        return False
    latest = rows[-1].get("totalLiquidityUSD")
    if isinstance(latest, bool) or not isinstance(latest, (int, float)):
        return False
    if not math.isfinite(latest):
        return False
    tolerance = max(0.01, abs(current_value) * 1e-9)
    return abs(latest - current_value) <= tolerance


def _dolomite_total_supply_history_current(data):
    return _dolomite_history_matches_current(data, "totalSupply", "currentSupply")


def _dolomite_net_tvl_history_current(data):
    return _dolomite_history_matches_current(data, "tvl", "currentTvl")


def _dolomite_liquidity_histories_reconcile(data):
    supply_rows = data.get("totalSupply", [])
    tvl_rows = data.get("tvl", [])
    if not supply_rows or len(supply_rows) != len(tvl_rows):
        return False
    for supply_row, tvl_row in zip(supply_rows, tvl_rows):
        if supply_row.get("date") != tvl_row.get("date"):
            return False
        supply = supply_row.get("totalLiquidityUSD")
        tvl = tvl_row.get("totalLiquidityUSD")
        if (
            isinstance(supply, bool)
            or isinstance(tvl, bool)
            or not isinstance(supply, (int, float))
            or not isinstance(tvl, (int, float))
            or not math.isfinite(supply)
            or not math.isfinite(tvl)
            or tvl < 0
        ):
            return False
        tolerance = max(0.01, abs(supply) * 1e-9)
        if tvl > supply + tolerance:
            return False
    return True


def _dolomite_total_supply_history_coverage(data):
    official = data.get("officialMarketCount")
    active = data.get("activeMarketCount")
    all_markets = data.get("allMarketCount")
    stale_count = data.get("staleOfficialMarketCount")
    stale_supply = data.get("staleOfficialMarketSupply")
    current_supply = data.get("currentSupply")
    return (
        isinstance(official, int)
        and not isinstance(official, bool)
        and isinstance(active, int)
        and not isinstance(active, bool)
        and isinstance(all_markets, int)
        and not isinstance(all_markets, bool)
        and isinstance(stale_count, int)
        and not isinstance(stale_count, bool)
        and isinstance(stale_supply, (int, float))
        and not isinstance(stale_supply, bool)
        and isinstance(current_supply, (int, float))
        and not isinstance(current_supply, bool)
        and math.isfinite(stale_supply)
        and math.isfinite(current_supply)
        and 0 < official <= active <= all_markets
        and official + stale_count == active
        and 0 <= stale_supply <= current_supply * 0.001
    )


def _dolomite_revenue_series_valid(data):
    rows = data.get("series", [])
    if len(rows) < 30:
        return False
    previous = 0
    for row in rows:
        timestamp = row.get("timestamp")
        fees = row.get("feesUSD")
        revenue = row.get("revenueUSD")
        gross_revenue = row.get("grossRevenueUSD", revenue)
        rebate = row.get("borrowFeeRebateUSD", 0)
        supply_side = row.get("supplySideRevenueUSD")
        if not isinstance(timestamp, int) or timestamp <= previous:
            return False
        if not all(isinstance(value, (int, float)) for value in (fees, revenue, gross_revenue, rebate, supply_side)):
            return False
        if fees < 0 or revenue < 0 or gross_revenue < 0 or rebate < 0 or revenue > gross_revenue + 1 or gross_revenue > fees + 1:
            return False
        if not _nearly_equal(gross_revenue, revenue + rebate, abs_tol=1):
            return False
        if not _nearly_equal(fees, gross_revenue + supply_side, abs_tol=1):
            return False
        chain_values = row.get("chains", {})
        if not isinstance(chain_values, dict):
            return False
        chain_fees = sum(_safe_number(chain.get("feesUSD")) for chain in chain_values.values())
        chain_gross = sum(_safe_number(chain.get("grossRevenueUSD", chain.get("revenueUSD"))) for chain in chain_values.values())
        chain_rebate = sum(_safe_number(chain.get("borrowFeeRebateUSD")) for chain in chain_values.values())
        chain_revenue = sum(_safe_number(chain.get("revenueUSD")) for chain in chain_values.values())
        chain_supply_side = sum(_safe_number(chain.get("supplySideRevenueUSD")) for chain in chain_values.values())
        if chain_values and (
            not _nearly_equal(fees, chain_fees, abs_tol=1)
            or not _nearly_equal(gross_revenue, chain_gross, abs_tol=1)
            or not _nearly_equal(rebate, chain_rebate, abs_tol=1)
            or not _nearly_equal(revenue, chain_revenue, abs_tol=1)
            or not _nearly_equal(supply_side, chain_supply_side, abs_tol=1)
        ):
            return False
        if not row.get("date") or not isinstance(row.get("chains", {}), dict):
            return False
        previous = timestamp
    return True


def _dolomite_revenue_window_totals_valid(data):
    totals = data.get("totals", {})
    rows = data.get("series", [])
    if len(rows) < 30:
        return False
    checks = (
        ("revenue7dUSD", "revenueUSD", 7),
        ("grossRevenue7dUSD", "grossRevenueUSD", 7),
        ("borrowFeeRebate7dUSD", "borrowFeeRebateUSD", 7),
        ("fees7dUSD", "feesUSD", 7),
        ("revenue30dUSD", "revenueUSD", 30),
        ("grossRevenue30dUSD", "grossRevenueUSD", 30),
        ("borrowFeeRebate30dUSD", "borrowFeeRebateUSD", 30),
        ("fees30dUSD", "feesUSD", 30),
    )
    return all(
        _nearly_equal(totals.get(total_key), sum(_safe_number(row.get(row_key)) for row in rows[-days:]), abs_tol=1)
        for total_key, row_key, days in checks
    )


def _dolomite_revenue_chain_windows_valid(data):
    rows = data.get("series", [])
    for days, key in ((7, "chainTotals7d"), (30, "chainTotals30d")):
        expected = {}
        for row in rows[-days:]:
            for chain, payload in (row.get("chains") or {}).items():
                item = expected.setdefault(chain, {
                    "feesUSD": 0.0,
                    "grossRevenueUSD": 0.0,
                    "borrowFeeRebateUSD": 0.0,
                    "revenueUSD": 0.0,
                    "supplySideRevenueUSD": 0.0,
                })
                item["feesUSD"] += _safe_number(payload.get("feesUSD"))
                item["grossRevenueUSD"] += _safe_number(payload.get("grossRevenueUSD", payload.get("revenueUSD")))
                item["borrowFeeRebateUSD"] += _safe_number(payload.get("borrowFeeRebateUSD"))
                item["revenueUSD"] += _safe_number(payload.get("revenueUSD"))
                item["supplySideRevenueUSD"] += _safe_number(payload.get("supplySideRevenueUSD"))
        actual = data.get(key, {})
        if set(actual) != set(expected):
            return False
        for chain, expected_values in expected.items():
            actual_values = actual.get(chain, {})
            for value_key, expected_value in expected_values.items():
                if not _nearly_equal(actual_values.get(value_key), expected_value, abs_tol=1):
                    return False
    return True


def _dolomite_revenue_borrow_fee_rebate_max_audits_valid(data):
    if "borrowFeeRebates" not in data:
        return True
    rebates = data.get("borrowFeeRebates")
    if not isinstance(rebates, dict):
        return False
    chains = rebates.get("chains")
    if not isinstance(chains, dict):
        return False
    for chain_name, chain in chains.items():
        if not isinstance(chain_name, str) or not chain_name or not isinstance(chain, dict):
            return False
        rows = chain.get("epochRebates")
        unsupported_corrections = chain.get("unsupportedCorrections")
        if unsupported_corrections is not None and (
            not isinstance(unsupported_corrections, list)
            or any(
                not isinstance(item, dict)
                or item.get("reason") != "unsupported_aggregate_correction"
                for item in unsupported_corrections
            )
        ):
            return False
        if not isinstance(rows, list):
            return False

        audit_groups = chain.get("maxRebateAuditGroups", [])
        if not isinstance(audit_groups, list):
            return False
        groups_by_id = {}
        for group in audit_groups:
            if not isinstance(group, dict):
                return False
            group_id = group.get("id")
            start_epoch = group.get("startEpoch")
            end_epoch = group.get("endEpoch")
            if (
                not isinstance(group_id, str)
                or not group_id
                or group_id in groups_by_id
                or not _finite_json_integer_number(start_epoch)
                or not _finite_json_integer_number(end_epoch)
                or start_epoch <= 0
                or end_epoch <= start_epoch
                or group_id != f"epochs-{start_epoch}-{end_epoch}"
                or not _finite_json_integer_number(group.get("periodStartTimestamp"))
                or not _finite_json_integer_number(group.get("periodEndTimestamp"))
                or group["periodEndTimestamp"] <= group["periodStartTimestamp"]
                or not _finite_json_integer_number(group.get("claimStartTimestamp"))
                or not _finite_json_integer_number(group.get("claimEndTimestamp"))
                or group["claimEndTimestamp"] <= group["claimStartTimestamp"]
                or not _finite_real_json_number(group.get("rebateUSD"))
                or group["rebateUSD"] <= 0
                or not _finite_real_json_number(group.get("maxRebateUSD"))
                or group["maxRebateUSD"] <= 0
                or group.get("auditStatus") != "verified"
                or group.get("auditReason") != "official_catchup_window_grouped"
            ):
                return False
            groups_by_id[group_id] = group

        positive_rows = [row for row in rows if isinstance(row, dict) and _safe_number(row.get("rebateUSD")) > 0]
        current_epoch = rebates.get("currentEpochIndex")
        start_epoch = chain.get("startEpoch")
        if (
            rebates.get("status") == "active"
            and chain.get("status") == "active"
            and _finite_json_integer_number(current_epoch)
            and _finite_json_integer_number(start_epoch)
            and current_epoch > start_epoch
            and not positive_rows
        ):
            return False
        authoritative_epoch = chain.get("authoritativePublishedEpoch")
        if positive_rows and (
            not _finite_json_integer_number(authoritative_epoch)
            or authoritative_epoch <= 0
        ):
            return False

        for row in rows:
            if not isinstance(row, dict):
                return False
            rebate_usd = row.get("rebateUSD")
            if not _finite_real_json_number(rebate_usd):
                return False
            calculation_mode = row.get("calculationMode")
            if calculation_mode is None:
                calculation_mode = ""
            if (
                not isinstance(calculation_mode, str)
                or calculation_mode not in {
                    "",
                    "cumulative_delta",
                    "known_epoch_snapshot_reset",
                    "same_epoch_snapshot_replacement",
                }
            ):
                return False
            if calculation_mode == "known_epoch_snapshot_reset":
                event_block = row.get("eventBlock")
                reset_market_count = row.get("resetMarketCount")
                aggregate_adjustment = row.get("aggregateAdjustmentRaw")
                if (
                    chain_name != "Berachain"
                    or not _finite_json_integer_number(row.get("epoch"))
                    or row.get("epoch") != 9
                    or str(row.get("transactionHash") or "").lower() != "0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c"
                    or not _finite_json_integer_number(event_block)
                    or event_block != 24055329
                    or not _finite_json_integer_number(reset_market_count)
                    or reset_market_count < 2
                    or not _finite_real_json_number(aggregate_adjustment)
                    or aggregate_adjustment >= 0
                    or row.get("sourceLabel") != "Published epoch snapshot reset"
                ):
                    return False
            if rebate_usd <= 0:
                continue
            epoch = row.get("epoch")
            if (
                not _finite_json_integer_number(epoch)
                or epoch <= 0
                or epoch > authoritative_epoch
            ):
                return False
            market_ids = row.get("maxRebateEligibleMarketIds")
            max_rebate_usd = row.get("maxRebateUSD")
            max_rebate_market_count = row.get("maxRebateMarketCount")
            max_rebate_day_count = row.get("maxRebateDayCount")
            claim_start = row.get("claimStartTimestamp")
            claim_end = row.get("claimEndTimestamp")
            claim_start_block = row.get("claimStartBlockNumber")
            claim_end_block = row.get("claimEndBlockNumber")
            period_seconds = row.get("maxRebatePeriodSeconds")
            coverage_status = row.get("maxRebateCoverageStatus")
            audit_status = row.get("maxRebateAuditStatus")
            if (
                not _finite_real_json_number(max_rebate_usd)
                or max_rebate_usd <= 0
                or row.get("maxRebateMethod") != "official_finalized_borrow_interest_per_market"
                or row.get("maxRebateSource") != "dolomite-liquidity-mining-data"
                or not isinstance(market_ids, list)
                or not market_ids
                or any(not isinstance(market_id, str) or not market_id.isdigit() for market_id in market_ids)
                or not _finite_json_integer_number(max_rebate_market_count)
                or max_rebate_market_count != len(market_ids)
                or not _finite_json_integer_number(max_rebate_day_count)
                or max_rebate_day_count <= 0
                or not _finite_json_integer_number(claim_start)
                or not _finite_json_integer_number(claim_end)
                or claim_end <= claim_start
                or not _finite_json_integer_number(claim_start_block)
                or not _finite_json_integer_number(claim_end_block)
                or claim_end_block < claim_start_block
                or not _finite_json_integer_number(period_seconds)
                or period_seconds != claim_end - claim_start
                or coverage_status not in {"matched", "mismatch"}
                or not isinstance(row.get("maxRebatePublishedOnlyMarketIds"), list)
                or not isinstance(row.get("maxRebateOfficialOnlyMarketIds"), list)
            ):
                return False
            group_id = row.get("maxRebateAuditGroupId")
            if audit_status == "verified_grouped":
                group = groups_by_id.get(group_id)
                if (
                    not group
                    or not group["startEpoch"] <= epoch <= group["endEpoch"]
                    or row.get("maxRebateAuditReason") != "official_catchup_window_grouped"
                ):
                    return False
                continue
            if group_id is not None:
                return False

            anomaly_limit = max_rebate_usd * 1.05 + 0.01
            if rebate_usd > anomaly_limit:
                if (
                    audit_status != "source_anomaly"
                    or row.get("maxRebateAuditReason") != "published_rebate_exceeds_official_max"
                ):
                    return False
            elif audit_status != "verified" or row.get("maxRebateAuditReason"):
                return False

        rows_by_epoch = {
            row.get("epoch"): row
            for row in rows
            if isinstance(row, dict) and _finite_json_integer_number(row.get("epoch"))
        }
        for group_id, group in groups_by_id.items():
            members = [rows_by_epoch.get(epoch) for epoch in range(group["startEpoch"], group["endEpoch"] + 1)]
            if (
                any(not isinstance(row, dict) for row in members)
                or any(row.get("maxRebateAuditGroupId") != group_id for row in members)
                or members[0].get("periodStartTimestamp") != group["periodStartTimestamp"]
                or members[-1].get("periodEndTimestamp") != group["periodEndTimestamp"]
                or members[0].get("claimStartTimestamp") != group["claimStartTimestamp"]
                or members[-1].get("claimEndTimestamp") != group["claimEndTimestamp"]
                or not _nearly_equal(
                    sum(_safe_number(row.get("rebateUSD")) for row in members),
                    group["rebateUSD"],
                    abs_tol=0.000001,
                )
                or not _nearly_equal(
                    sum(_safe_number(row.get("maxRebateUSD")) for row in members),
                    group["maxRebateUSD"],
                    abs_tol=0.000001,
                )
                or group["rebateUSD"] > group["maxRebateUSD"] * 1.05 + 0.01
            ):
                return False
    return True


def _dolomite_revenue_onchain_audit_valid(data):
    statuses = {"pass", "warn", "partial", "missing"}
    status = data.get("status")
    chains = data.get("chains")
    summary = data.get("summary") or {}
    if (
        data.get("schemaVersion") != 1
        or status not in statuses
        or not _is_iso_datetime(data.get("generatedAt"))
        or not isinstance(chains, dict)
        or not chains
    ):
        return False

    chain_statuses = [item.get("status") for item in chains.values() if isinstance(item, dict)]
    if len(chain_statuses) != len(chains) or any(item not in statuses for item in chain_statuses):
        return False

    audited = sum(1 for item in chain_statuses if item in {"pass", "warn"})
    warn = chain_statuses.count("warn")
    missing = chain_statuses.count("missing")
    passed = chain_statuses.count("pass")
    expected_summary = {
        "auditedChainCount": audited,
        "passChainCount": passed,
        "warnChainCount": warn,
        "missingChainCount": missing,
    }
    if any(summary.get(key) != value for key, value in expected_summary.items()):
        return False

    if status == "warn":
        return warn > 0
    if status == "partial":
        return audited > 0 and missing > 0 and warn == 0
    if status == "pass":
        return audited > 0 and missing == 0 and warn == 0
    return audited == 0


def _veborrow_simulation_valid(data):
    if data.get("schemaVersion") != 1 or not _is_iso_datetime(data.get("generatedAt")):
        return False
    config = data.get("config") or {}
    chains = data.get("chains") or {}
    borrowers = data.get("borrowers")
    totals = data.get("totals") or {}
    if not isinstance(chains, dict) or not isinstance(borrowers, list) or not isinstance(totals, dict):
        return False
    if not {"Ethereum", "Arbitrum", "Berachain"}.issubset(set(chains)):
        return False
    display_chains = config.get("displaySimulationChains")
    if display_chains != ["Ethereum", "Arbitrum", "Berachain"]:
        return False
    if config.get("simulationChains") != display_chains:
        return False
    if not {"Ethereum", "Arbitrum", "Berachain"}.issubset(set(config.get("eligibilityChains") or [])):
        return False
    active_market_ids_by_chain = config.get("activeRebateMarketIdsByChain") or {}
    expected_active_chains = [
        chain for chain in display_chains
        if isinstance(active_market_ids_by_chain.get(chain), list)
        and active_market_ids_by_chain.get(chain)
    ]
    if config.get("activeRebateChains") != expected_active_chains:
        return False
    if config.get("annualizationPeriods") != 52:
        return False
    if config.get("thresholdBasis") != "official_annualized_max_rebate":
        return False
    claims_enabled = config.get("claimsEnabledByChain")
    if not isinstance(claims_enabled, dict):
        return False
    rebate_percentages = config.get("rebatePercentagesByChain") or {}
    if not isinstance(rebate_percentages, dict):
        return False
    if any(_safe_number(rebate_percentages.get(chain)) <= 0 for chain in display_chains):
        return False
    if not expected_active_chains:
        return False
    if _safe_number(config.get("rebatePercentage")) <= 0 or _safe_number(config.get("veDoloHoldingFactor")) <= 0:
        return False
    if _safe_number(config.get("doloPriceUSD")) <= 0:
        return False
    if _safe_number(config.get("protocolReserveFactor")) <= 0:
        return False
    if not all(isinstance(chains.get(chain), dict) and chains[chain].get("status") in {"ok", "error"} for chain in ("Ethereum", "Arbitrum", "Berachain")):
        return False
    for chain in expected_active_chains:
        active_market_ids = active_market_ids_by_chain[chain]
        if chains[chain].get("status") != "ok" or chains[chain].get("debtMarketFilter") != "active_rebate_markets":
            return False
        if chains[chain].get("eligibleRebateMarketIds") != active_market_ids:
            return False
    ok_simulation_chains = [chain for chain in display_chains if chain not in expected_active_chains and chains[chain].get("status") == "ok"]
    if not ok_simulation_chains:
        return False
    source_counts = totals.get("veDoloVoteSourceCounts") or {}
    if not isinstance(source_counts, dict) or "onchain_getVotes" not in source_counts:
        return False
    return _safe_number(totals.get("borrowerCount")) == len(borrowers)


def _dolomite_revenue_totals_valid(data):
    totals = data.get("totals", {})
    latest = data.get("latest", {})
    daily_fees = totals.get("dailyFeesUSD")
    daily_revenue = totals.get("dailyRevenueUSD")
    daily_gross_revenue = totals.get("dailyGrossRevenueUSD", daily_revenue)
    daily_rebate = totals.get("dailyBorrowFeeRebateUSD", 0)
    daily_supply_side = totals.get("dailySupplySideRevenueUSD")
    daily_cut = totals.get("dailyProtocolCut")
    latest_fees = latest.get("feesUSD")
    latest_revenue = latest.get("revenueUSD")
    latest_gross_revenue = latest.get("grossRevenueUSD", latest_revenue)
    latest_rebate = latest.get("borrowFeeRebateUSD", 0)
    if not all(isinstance(value, (int, float)) for value in (daily_fees, daily_revenue, daily_gross_revenue, daily_rebate, daily_supply_side, daily_cut, latest_fees, latest_revenue, latest_gross_revenue, latest_rebate)):
        return False
    expected_cut = daily_revenue / daily_fees if daily_fees > 0 else 0
    return (
        daily_fees > 0
        and 0 <= daily_revenue <= daily_gross_revenue <= daily_fees
        and daily_rebate >= 0
        and _nearly_equal(daily_gross_revenue, daily_revenue + daily_rebate, abs_tol=1)
        and _nearly_equal(daily_fees, daily_gross_revenue + daily_supply_side, abs_tol=1)
        and _nearly_equal(daily_cut, expected_cut, abs_tol=0.0001)
        and _nearly_equal(daily_fees, latest_fees, abs_tol=500)
        and _nearly_equal(daily_revenue, latest_revenue, abs_tol=500)
        and _nearly_equal(daily_gross_revenue, latest_gross_revenue, abs_tol=500)
        and _nearly_equal(daily_rebate, latest_rebate, abs_tol=500)
    )


def _odolo_claim_events_have_known_distributors(data):
    distributors = set(data.get("distributors") or [])
    return bool(distributors) and all(row.get("distributor") in distributors for row in data.get("events", []))


def _odolo_claim_events_use_odolo_token(data):
    token = data.get("token", {})
    events = data.get("events", [])
    expected_address = str(token.get("address", "")).lower()
    return (
        token.get("symbol") == "oDOLO"
        and expected_address == "0x02e513b5b54ee216bf836ceb471507488fc89543"
        and all(str(row.get("tokenSymbol", "")) == "oDOLO" and str(row.get("tokenAddress", "")).lower() == expected_address for row in events)
    )


def _odolo_claim_events_are_canonical(data):
    events = data.get("events") or []
    return bool(events) and all(
        str(event.get("distributor") or "").lower() == ODOLO_CLAIMS_DISTRIBUTOR
        and str(event.get("tokenAddress") or "").lower() == ODOLO_TOKEN_ADDRESS
        for event in events
    )


def _reward_claim_events_have_known_chains(data):
    chains = data.get("chains", {})
    events = data.get("events", [])
    return (
        isinstance(chains, dict)
        and bool(chains)
        and all(row.get("chainKey") in chains for row in events)
    )


def _reward_claim_events_include_supported_claim_chains(data):
    chains = data.get("chains", {})
    supported = {"berachain", "arbitrum", "mantle", "xlayer"}
    return isinstance(chains, dict) and supported.issubset(set(chains))


def _reward_claim_events_have_chain_metadata(data):
    chains = data.get("chains", {})
    if not isinstance(chains, dict) or not chains:
        return False
    for chain in chains.values():
        if not isinstance(chain, dict):
            return False
        if not chain.get("eventEmitter") or not chain.get("distributors") or not chain.get("token"):
            return False
        if not isinstance(chain.get("fromBlock"), int) or not isinstance(chain.get("toBlock"), int):
            return False
        if chain.get("fromBlock") > chain.get("toBlock"):
            return False
        if not isinstance(chain.get("fromTimestamp"), int) or not isinstance(chain.get("toTimestamp"), int):
            return False
    return True


def _reward_claim_events_have_distributor_tokens(data):
    chains = data.get("chains", {})
    if not isinstance(chains, dict) or not chains:
        return False
    for chain in chains.values():
        distributors = set(chain.get("distributors") or [])
        tokens = chain.get("tokensByDistributor") or {}
        if not isinstance(tokens, dict):
            return False
        for distributor in distributors:
            token = tokens.get(distributor)
            if not isinstance(token, dict) or not token.get("symbol"):
                return False
    return True


def _reward_claim_events_shard_manifest_is_consistent(data):
    if not data.get("eventsShardedByChain"):
        return True
    chains = data.get("chains", {})
    files = data.get("chainEventFiles", {})
    if not isinstance(chains, dict) or not isinstance(files, dict):
        return False
    if set(files) != set(chains):
        return False
    return all(
        isinstance(path, str)
        and path == f"data/reward-claim-events/{chain_key}.json"
        for chain_key, path in files.items()
    )


def _reward_claim_events_have_transaction_evidence(data):
    events = data.get("events", [])
    return all(
        row.get("chainKey")
        and row.get("txHash")
        and row.get("timestamp")
        and row.get("blockNumber") is not None
        and row.get("logIndex") is not None
        and row.get("user")
        and row.get("distributor")
        and row.get("amount")
        and row.get("amountWei")
        and row.get("tokenSymbol")
        and row.get("source") == "RewardClaimed"
        for row in events
    )


DOLO_LIQUIDITY_POOLS = {
    ("ethereum", "uniswap-v4", "0x2d97d14362ae5a19a15adb230cf8840ee7e133bf942fd8efd754ae4d078727ea"),
    ("ethereum", "uniswap-v3", "0x003896387666c5c11458eeb3f927b72a11b19783"),
    ("ethereum", "uniswap-v4", "0x6f6f24b5a1cd819382379eb032466b8bac7ea0697cfcf31b7350b55ff4f1c472"),
    ("ethereum", "uniswap-v4", "0x728e6e3b736e28f6b52f72ecec16a056b8ac6d9e05736a84e6b6128df9b1a12a"),
    ("ethereum", "uniswap-v4", "0x330d0f19c6ef4559d2cbfaa48a2d18fe55efe642337257e3a4ebd62f5de56f18"),
    ("berachain", "kodiak-v3", "0xd5980e98a89e2d2361b3be657e8a003c6d3514e3"),
    ("berachain", "bulla-v3", "0x8991017b74f9f8070bff5b322802dd26e05e0cc7"),
    ("berachain", "kodiak-v3", "0x8194ed4d6701b7a1b40e48431de37047f0248b0b"),
    ("berachain", "brownfi-v3", "0x16b3a5e95db753fe5195244fa208301e38beae2a"),
}
DOLO_LIQUIDITY_QUALITIES = {"verified", "partial", "stale", "unavailable"}
DOLO_LIQUIDITY_POSITION_STATUSES = {"active", "custodied_unresolved"}
DOLO_LIQUIDITY_RANGE_STATUSES = {"in_range", "out_of_range", "full_range", "unavailable"}


def _dolo_liquidity_raw(value):
    return isinstance(value, str) and bool(value) and value.isdigit()


def _dolo_liquidity_address(value, *, allow_none=False):
    if value is None:
        return allow_none
    return (
        isinstance(value, str)
        and len(value) == 42
        and value.startswith("0x")
        and value == value.lower()
        and all(char in "0123456789abcdef" for char in value[2:])
    )


def _dolo_liquidity_value_valid(row):
    status = row.get("valueStatus")
    value = row.get("valueUsd")
    if status == "unavailable":
        return value is None
    return (
        status in {"verified", "partial", "stale"}
        and _finite_real_json_number(value)
        and value >= 0
    )


def _dolo_liquidity_pool_coverage_valid(pool, active):
    coverage = pool.get("coverage")
    if not isinstance(coverage, dict):
        return False
    pool_id = str(pool.get("identifier") or "").lower()
    rows = [
        row
        for row in active
        if str(row.get("poolId") or "").lower() == pool_id
        and row.get("valueUsd") is not None
    ]
    attributed = sum((Decimal(str(row["valueUsd"])) for row in rows), Decimal(0))
    verified_wallet = sum(
        (
            Decimal(str(row["valueUsd"]))
            for row in rows
            if row.get("beneficialOwner") and row.get("quality") == "verified"
        ),
        Decimal(0),
    )
    unresolved_custody = sum(
        (
            Decimal(str(row["valueUsd"]))
            for row in rows
            if not row.get("beneficialOwner")
        ),
        Decimal(0),
    )
    if not all(
        _nearly_equal(coverage.get(key), expected, abs_tol=0.000001)
        for key, expected in (
            ("attributedValueUsd", attributed),
            ("verifiedWalletValueUsd", verified_wallet),
            ("unresolvedCustodyValueUsd", unresolved_custody),
        )
    ):
        return False
    reason = coverage.get("residualReason")
    if not isinstance(reason, str) or not reason.strip():
        return False
    liquidity_value = pool.get("liquidityUsd")
    if pool.get("liquidityStatus") == "unavailable":
        return (
            coverage.get("coveragePct") is None
            and coverage.get("residualValueUsd") is None
            and coverage.get("status") == "unavailable"
        )
    if not _finite_real_json_number(liquidity_value) or liquidity_value < 0:
        return False
    liquidity = Decimal(str(liquidity_value))
    residual = liquidity - attributed
    tolerance = Decimal("0.000001")
    over_attributed = residual < -tolerance
    if over_attributed or abs(residual) <= tolerance:
        residual = Decimal(0)
    coverage_pct = (
        Decimal(100)
        if liquidity == 0 and attributed == 0
        else Decimal(0)
        if liquidity == 0
        else attributed * Decimal(100) / liquidity
    )
    expected_status = (
        "complete"
        if Decimal("99.5") <= coverage_pct <= Decimal("100.5")
        else "partial"
    )
    return (
        coverage.get("status") == expected_status
        and _nearly_equal(coverage.get("coveragePct"), coverage_pct, abs_tol=0.0001)
        and _nearly_equal(coverage.get("residualValueUsd"), residual, abs_tol=0.000001)
    )


def _dolo_liquidity_valid(data):
    """Fail closed when the LP artifact cannot prove identity and reconciliation."""
    if not isinstance(data, dict) or data.get("schemaVersion") != 1:
        return False
    if not _is_iso_datetime(data.get("generatedAt")):
        return False
    pools = data.get("pools")
    sources = data.get("sources")
    active = data.get("activePositions")
    history = data.get("history")
    summary = data.get("summary")
    quality = data.get("quality")
    if not all(
        isinstance(value, expected)
        for value, expected in (
            (pools, list),
            (sources, list),
            (active, list),
            (history, list),
            (summary, dict),
            (quality, dict),
        )
    ):
        return False

    pool_identities = set()
    pool_ids = set()
    pool_by_id = {}
    for pool in pools:
        if not isinstance(pool, dict):
            return False
        identity = (
            str(pool.get("chainKey") or "").lower(),
            str(pool.get("adapter") or "").lower(),
            str(pool.get("identifier") or "").lower(),
        )
        identifier = identity[2]
        expected_type = "poolId" if identity[1] == "uniswap-v4" else "contract"
        expected_length = 66 if expected_type == "poolId" else 42
        if (
            identity in pool_identities
            or pool.get("identifierType") != expected_type
            or len(identifier) != expected_length
            or not identifier.startswith("0x")
            or any(char not in "0123456789abcdef" for char in identifier[2:])
            or pool.get("id") != identifier
            or pool.get("sourceKey") != f"{identity[0]}:{identity[1]}"
            or pool.get("quality") not in DOLO_LIQUIDITY_QUALITIES
        ):
            return False
        liquidity_status = pool.get("liquidityStatus")
        liquidity_value = pool.get("liquidityUsd")
        if liquidity_status == "unavailable":
            if liquidity_value is not None:
                return False
        elif not (
            liquidity_status in {"verified", "partial", "stale"}
            and _finite_real_json_number(liquidity_value)
            and liquidity_value >= 0
        ):
            return False
        pool_identities.add(identity)
        pool_ids.add(identifier)
        pool_by_id[identifier] = pool
    if pool_identities != DOLO_LIQUIDITY_POOLS:
        return False

    source_keys = set()
    source_by_key = {}
    for source in sources:
        if not isinstance(source, dict):
            return False
        key = source.get("key")
        last_block = source.get("lastScannedBlock")
        latest_block = source.get("latestChainBlock")
        if (
            not isinstance(key, str)
            or not key
            or key in source_keys
            or source.get("status") not in {"complete", "partial", "stale", "unavailable"}
            or not _is_exact_integer(last_block)
            or not _is_exact_integer(latest_block)
            or not 0 <= last_block <= latest_block
            or not isinstance(source.get("errors"), list)
            or key != f"{source.get('chainKey')}:{source.get('adapter')}"
        ):
            return False
        source_keys.add(key)
        source_by_key[key] = source
    if source_keys != {f"{chain}:{adapter}" for chain, adapter, _ in pool_identities}:
        return False

    active_ids = set()
    allocation_groups = {}
    for row in active:
        if not isinstance(row, dict):
            return False
        row_id = row.get("id")
        pool_id = str(row.get("poolId") or "").lower()
        pool = pool_by_id.get(pool_id)
        source = source_by_key.get(row.get("sourceKey"))
        if (
            not isinstance(row_id, str)
            or not row_id
            or row_id in active_ids
            or row.get("sourceKey") not in source_keys
            or pool is None
            or source is None
            or row.get("chainKey") != pool.get("chainKey")
            or row.get("adapter") != pool.get("adapter")
            or row.get("pair") != pool.get("pair")
            or row.get("poolIdentifierType") != pool.get("identifierType")
            or row.get("sourceKey") != pool.get("sourceKey")
            or source.get("chainKey") != row.get("chainKey")
            or source.get("adapter") != row.get("adapter")
            or not _dolo_liquidity_address(row.get("beneficialOwner"), allow_none=True)
            or not _dolo_liquidity_address(row.get("custodian"))
            or row.get("positionStatus") not in DOLO_LIQUIDITY_POSITION_STATUSES
            or row.get("rangeStatus") not in DOLO_LIQUIDITY_RANGE_STATUSES
            or row.get("quality") not in DOLO_LIQUIDITY_QUALITIES
            or not _dolo_liquidity_raw(row.get("doloRaw"))
            or not _dolo_liquidity_raw(row.get("pairedRaw"))
            or not _dolo_liquidity_value_valid(row)
        ):
            return False
        active_ids.add(row_id)
        allocation_group = row.get("allocationGroup")
        if allocation_group is not None:
            if row.get("positionType") not in {
                "kodiak_island_share",
                "uniswap_v4_vault_share",
            }:
                return False
            if not isinstance(allocation_group, str) or not allocation_group:
                return False
            allocation_groups.setdefault(allocation_group, []).append(row)

    for rows in allocation_groups.values():
        total_dolo = {row.get("allocationTotalDoloRaw") for row in rows}
        total_paired = {row.get("allocationTotalPairedRaw") for row in rows}
        if (
            len(total_dolo) != 1
            or len(total_paired) != 1
            or not _dolo_liquidity_raw(next(iter(total_dolo)))
            or not _dolo_liquidity_raw(next(iter(total_paired)))
            or sum(int(row["doloRaw"]) for row in rows) != int(next(iter(total_dolo)))
            or sum(int(row["pairedRaw"]) for row in rows) != int(next(iter(total_paired)))
        ):
            return False

    if any(not _dolo_liquidity_pool_coverage_valid(pool, active) for pool in pools):
        return False

    history_ids = set()
    for row in history:
        if not isinstance(row, dict):
            return False
        row_id = row.get("id")
        pool_id = str(row.get("poolId") or "").lower()
        pool = pool_by_id.get(pool_id)
        source = source_by_key.get(row.get("sourceKey"))
        if (
            not isinstance(row_id, str)
            or not row_id
            or row_id in history_ids
            or row.get("sourceKey") not in source_keys
            or pool is None
            or source is None
            or row.get("chainKey") != pool.get("chainKey")
            or row.get("adapter") != pool.get("adapter")
            or row.get("pair") != pool.get("pair")
            or row.get("poolIdentifierType") != pool.get("identifierType")
            or row.get("sourceKey") != pool.get("sourceKey")
            or source.get("chainKey") != row.get("chainKey")
            or source.get("adapter") != row.get("adapter")
            or not _dolo_liquidity_address(row.get("beneficialOwner"), allow_none=True)
            or not _dolo_liquidity_address(row.get("custodian"), allow_none=True)
            or not _is_exact_integer(row.get("blockNumber"))
            or not _is_exact_integer(row.get("logIndex"))
            or not _is_exact_integer(row.get("timestamp"))
            or row.get("timestamp") < 0
            or row.get("action") not in {"Added", "Increased", "Removed", "Closed"}
            or row.get("quality") not in DOLO_LIQUIDITY_QUALITIES
            or not _dolo_liquidity_raw(row.get("doloRaw"))
            or not _dolo_liquidity_raw(row.get("pairedRaw"))
            or not _dolo_liquidity_value_valid(row)
        ):
            return False
        history_ids.add(row_id)

    valued_total = sum(
        (Decimal(str(row["valueUsd"])) for row in active if row.get("valueUsd") is not None),
        Decimal(0),
    )
    expected_summary = {
        "activePositions": len(active),
        "lpWallets": len({row.get("beneficialOwner") for row in active if row.get("beneficialOwner")}),
        "outOfRange": sum(row.get("rangeStatus") == "out_of_range" for row in active),
    }
    if any(summary.get(key) != value for key, value in expected_summary.items()):
        return False
    if not _nearly_equal(summary.get("activeLiquidityUsd"), valued_total, abs_tol=0.000001):
        return False
    expected_quality = {
        "verifiedActivePositions": sum(row.get("quality") == "verified" for row in active),
        "partialActivePositions": sum(row.get("quality") == "partial" for row in active),
        "staleActivePositions": sum(row.get("quality") == "stale" for row in active),
        "unavailableActivePositions": sum(row.get("quality") == "unavailable" for row in active),
        "unresolvedCustody": sum(not row.get("beneficialOwner") for row in active),
    }
    return all(quality.get(key) == value for key, value in expected_quality.items())


def _dolo_cex_supply_history_valid(data):
    history = data.get("cex_supply_history")
    if not isinstance(history, list) or not history:
        return False
    try:
        for point in history:
            if not isinstance(point, dict):
                return False
            liquid = point.get("liquid")
            wallets = point.get("wallets")
            if (
                not _finite_real_json_number(liquid)
                or liquid < 0
                or not _is_exact_integer(wallets)
                or wallets < 0
            ):
                return False
            exchanges = point.get("exchanges")
            if exchanges is None:
                if liquid > 0 or wallets != 0:
                    return False
                continue
            if not isinstance(exchanges, list) or (liquid > 0 and not exchanges):
                return False

            names = set()
            exchange_wallets = 0
            exchange_cents = 0
            for row in exchanges:
                if not isinstance(row, dict):
                    return False
                name = row.get("name")
                row_liquid = row.get("liquid")
                row_wallets = row.get("wallets")
                if (
                    not isinstance(name, str)
                    or not name.strip()
                    or name != name.strip()
                    or name in names
                    or not _finite_real_json_number(row_liquid)
                    or row_liquid < 0
                    or not _is_exact_integer(row_wallets)
                    or row_wallets < 0
                ):
                    return False
                names.add(name)
                exchange_wallets += row_wallets
                exchange_cents += int(
                    Decimal(str(row_liquid)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) * 100
                )

            if exchanges != sorted(exchanges, key=lambda row: (-row["liquid"], row["name"])):
                return False
            point_cents = int(
                Decimal(str(liquid)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) * 100
            )
            if exchange_cents != point_cents or exchange_wallets != wallets:
                return False
            # Additive address snapshots must reconcile with the same historical
            # endpoint; legacy artifacts may omit them, never synthesize today.
            if "walletBalances" in point:
                snapshot = point["walletBalances"]
                if not isinstance(snapshot, list) or len(snapshot) != wallets:
                    return False
                addresses = set()
                by_exchange = {name: [Decimal(0), 0] for name in names}
                for row in snapshot:
                    if not isinstance(row, dict):
                        return False
                    address = row.get("address")
                    balance = row.get("balance")
                    name = row.get("exchange")
                    if (not isinstance(address, str)
                            or not re.fullmatch(r"0x[a-f0-9]{40}", address)
                            or address in addresses or name not in by_exchange
                            or not _finite_real_json_number(balance) or balance < 0
                            or row.get("evidenceStatus") not in {"public_label", "review_needed"}):
                        return False
                    addresses.add(address)
                    by_exchange[name][0] += Decimal(str(balance))
                    by_exchange[name][1] += 1
                if abs(sum(value[0] for value in by_exchange.values()) - Decimal(str(liquid))) > Decimal("0.0051"):
                    return False
                for row in exchanges:
                    amount, count = by_exchange[row["name"]]
                    # The existing policy assigns the combined 2dp rounding
                    # residual to the largest exchange; address rows have 6dp.
                    tolerance = Decimal("0.005") * (len(exchanges) + 1) + Decimal("0.000001") * wallets
                    if count != row["wallets"] or abs(amount - Decimal(str(row["liquid"]))) > tolerance:
                        return False
        return True
    except (InvalidOperation, TypeError, ValueError):
        return False


def _flow_tx_metadata_is_valid(data):
    """Optional aggregate-flow transaction evidence must be complete and exact."""
    metadata_keys = {
        "latest_tx_hash", "latest_tx_timestamp", "latest_tx_chain",
    }
    supported_chains = {"ethereum", "berachain"}

    def row_is_valid(row):
        if not isinstance(row, dict):
            return False
        present = metadata_keys.intersection(row)
        if not present:
            return True
        if present != metadata_keys:
            return False
        tx_hash = row.get("latest_tx_hash")
        timestamp = row.get("latest_tx_timestamp")
        chain = row.get("latest_tx_chain")
        if (
            not isinstance(tx_hash, str)
            or tx_hash != tx_hash.lower()
            or len(tx_hash) != 66
            or not tx_hash.startswith("0x")
            or not _is_exact_integer(timestamp)
            or timestamp <= 0
            or chain not in supported_chains
        ):
            return False
        try:
            int(tx_hash[2:], 16)
        except ValueError:
            return False
        return True

    def containers(node):
        if not isinstance(node, dict):
            return
        if any(key in node for key in ("accumulators", "sellers", "claimer_sellers")):
            yield node
        for value in node.values():
            if isinstance(value, dict):
                yield from containers(value)

    periods = data.get("periods", {})
    if not isinstance(periods, dict):
        return False
    for container in containers(periods):
        for key in ("accumulators", "sellers", "claimer_sellers"):
            rows = container.get(key)
            if rows is None:
                continue
            if not isinstance(rows, list) or not all(row_is_valid(row) for row in rows):
                return False
    return True


def _flow_lp_metadata_is_valid(data):
    """Optional LP attribution must be exact and bound to the row's latest tx."""
    required = {"direction", "amount", "pair", "adapter", "confidence", "tx_hash"}

    def row_is_valid(row):
        if not isinstance(row, dict):
            return False
        activity = row.get("latest_lp_activity")
        if activity is None:
            return True
        if not isinstance(activity, dict) or not required.issubset(activity):
            return False
        amount = activity.get("amount")
        tx_hash = activity.get("tx_hash")
        if (
            activity.get("direction") not in {"deposit", "withdrawal"}
            or activity.get("confidence") != "verified_same_tx"
            or not isinstance(amount, str)
            or not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", amount)
            or Decimal(amount) <= 0
            or not isinstance(activity.get("pair"), str)
            or not activity["pair"].strip()
            or not isinstance(activity.get("adapter"), str)
            or not activity["adapter"].strip()
            or tx_hash != row.get("latest_tx_hash")
            or not isinstance(tx_hash, str)
            or not re.fullmatch(r"0x[a-f0-9]{64}", tx_hash)
        ):
            return False
        return True

    periods = data.get("periods", {})
    if not isinstance(periods, dict):
        return False
    for period in periods.values():
        if not isinstance(period, dict):
            return False
        for container in period.values():
            if not isinstance(container, dict):
                continue
            for key in ("accumulators", "sellers", "claimer_sellers"):
                rows = container.get(key)
                if rows is None:
                    continue
                if not isinstance(rows, list) or not all(row_is_valid(row) for row in rows):
                    return False
    return True


def _flow_dolomite_balances_are_valid(data):
    """Current Dolomite DOLO exposure must be complete, non-negative and exact."""
    meta = data.get("dolomite_balance_meta")
    if not isinstance(meta, dict):
        return False
    status = meta.get("status")
    failed_chains = meta.get("failedChains")
    chains = meta.get("chains")
    expected_chains = {"eth", "bera"}
    if (
        status not in {"complete", "unavailable"}
        or not isinstance(failed_chains, list)
        or len(set(failed_chains)) != len(failed_chains)
        or not set(failed_chains).issubset(expected_chains)
        or not isinstance(chains, dict)
        or not set(chains).issubset(expected_chains)
    ):
        return False
    if status == "complete":
        if failed_chains or set(chains) != expected_chains:
            return False
    elif not failed_chains or set(chains).union(failed_chains) != expected_chains:
        return False

    for chain_meta in chains.values():
        if (
            not isinstance(chain_meta, dict)
            or not _is_exact_integer(chain_meta.get("blockNumber"))
            or chain_meta["blockNumber"] <= 0
            or not _is_exact_integer(chain_meta.get("blockTimestamp"))
            or chain_meta["blockTimestamp"] <= 0
            or not _is_exact_integer(chain_meta.get("matchedWallets"))
            or chain_meta["matchedWallets"] < 0
            or not isinstance(chain_meta.get("custodyAddress"), str)
            or chain_meta["custodyAddress"] != chain_meta["custodyAddress"].lower()
            or not re.fullmatch(r"0x[a-f0-9]{40}", chain_meta["custodyAddress"])
        ):
            return False

    if status == "complete" and meta.get("scope") != "all-positive-effective-users":
        return False

    balances = data.get("dolomite_balances")
    if not isinstance(balances, dict):
        return False
    if status == "unavailable":
        return not balances
    for address, row in balances.items():
        if (
            not isinstance(address, str)
            or address != address.lower()
            or not re.fullmatch(r"0x[a-f0-9]{40}", address)
            or not isinstance(row, dict)
            or set(row) != {"total", "eth", "bera"}
        ):
            return False
        values = (row.get("total"), row.get("eth"), row.get("bera"))
        if any(
            not _finite_real_json_number(value) or value < 0
            for value in values
        ):
            return False
        total, ethereum, berachain = values
        if not _nearly_equal(total, ethereum + berachain, rel=0, abs_tol=0.000001):
            return False
    return True


def _holder_dolomite_exposure_history_is_valid(data):
    """Historical holder exposure must have complete cross-chain daily evidence."""
    meta = data.get("holder_dolomite_history_meta")
    history = data.get("holder_bucket_history")
    points = data.get("holder_history_points")
    if (
        data.get("holder_history_schema") != "audience-exposure-v3"
        or not isinstance(meta, dict)
        or meta.get("status") != "complete"
        or meta.get("schemaVersion") != 1
        or meta.get("chainCount") != 2
        or set(meta.get("chains") or []) != {"eth", "bera"}
        or not isinstance(history, list)
        or not isinstance(points, list)
        or len(history) != len(points)
        or len(history) < 2
        or history[-1].get("key") != "now"
        or points[-1].get("key") != "now"
        or meta.get("pointCount") != len(history) - 1
    ):
        return False
    required_sources = {
        "liquid",
        "with_vedolo",
        "total_exposure",
        "total_exposure_with_vedolo",
    }
    for row, point in zip(history, points):
        if (
            not isinstance(row, dict)
            or row.get("key") != point.get("key")
            or row.get("timestamp") != point.get("timestamp")
            or not required_sources.issubset(row)
        ):
            return False
        for source_key in required_sources:
            model = (((row.get(source_key) or {}).get("holders") or {}).get("whales"))
            if not isinstance(model, dict) or not isinstance(model.get("buckets"), list):
                return False
            for numeric_key in (
                "trackedTotal",
                "trackedLiquid",
                "trackedLocked",
            ):
                value = model.get(numeric_key)
                if not _finite_real_json_number(value) or value < 0:
                    return False
            if source_key.startswith("total_exposure"):
                protocol = model.get("trackedProtocol")
                if not _finite_real_json_number(protocol) or protocol < 0:
                    return False
    return True


def _flow_history_integrity_is_valid(data):
    integrity = data.get("flow_history_integrity")
    if not isinstance(integrity, dict):
        return False
    if (
        integrity.get("version") != 2
        or integrity.get("status") != "complete"
        or integrity.get("verification") != "independent-rpc-exact-quorum"
        or integrity.get("unresolvedGapCount") != 0
    ):
        return False

    chains = integrity.get("chains")
    if not isinstance(chains, dict) or set(chains) != {"eth", "bera"}:
        return False
    expected_deploy = {"eth": 21_500_000, "bera": 2_900_000}
    for chain_key, deploy_block in expected_deploy.items():
        row = chains.get(chain_key)
        if not isinstance(row, dict):
            return False
        numeric_keys = (
            "deployBlock",
            "coverageStartBlock",
            "verifiedThroughBlock",
            "lastPublishedBlock",
        )
        if any(
            isinstance(row.get(key), bool) or not isinstance(row.get(key), int)
            for key in numeric_keys
        ):
            return False
        if (
            row["deployBlock"] != deploy_block
            or row["coverageStartBlock"] > deploy_block
            or row["verifiedThroughBlock"] < deploy_block
            or row["lastPublishedBlock"] != row["verifiedThroughBlock"]
            or row.get("verification") != "independent-rpc-exact-quorum"
        ):
            return False
        proof = row.get("lastVerificationProof")
        if not isinstance(proof, dict):
            return False
        families = proof.get("providerFamilies")
        proof_numbers = (
            proof.get("startBlock"),
            proof.get("endBlock"),
            proof.get("verifiedChunkCount"),
            proof.get("minimumMatchingProviderFamilies"),
        )
        if (
            any(isinstance(value, bool) or not isinstance(value, int) for value in proof_numbers)
            or proof["startBlock"] > proof["endBlock"]
            or proof["verifiedChunkCount"] < 1
            or proof["minimumMatchingProviderFamilies"] < 2
            or not isinstance(families, list)
            or len(set(families)) < 2
        ):
            return False
    return True


def _flow_reconciliation_v3_is_valid(data):
    """Validate post-merge leaderboards, complete search rows and audit bounds."""
    schema_version = data.get("schemaVersion")
    if schema_version is None:
        # Transitional compatibility for the currently deployed v2 artifact.
        return True
    if not _is_exact_integer(schema_version) or schema_version not in (3, 4, 5, 6):
        return False

    periods = ("1d", "7d", "30d", "90d", "180d", "all")
    expected_chains = {"eth", "bera"}
    if data.get("tracked_flow_chains") != ["ethereum", "berachain"]:
        return False

    period_rows = data.get("periods")
    boundaries = data.get("period_boundaries")
    bridge_audit = data.get("bridge_neutralization_audit")
    if (
        not isinstance(period_rows, dict)
        or not isinstance(boundaries, dict)
        or set(boundaries) != expected_chains
        or not isinstance(bridge_audit, dict)
    ):
        return False

    def leaderboard_is_valid(container, max_rows=100):
        if not isinstance(container, dict):
            return False
        total_transfers = container.get("total_transfers")
        if not _is_exact_integer(total_transfers) or total_transfers < 0:
            return False
        seen = set()
        for kind in ("accumulators", "sellers"):
            rows = container.get(kind)
            if (
                not isinstance(rows, list)
                or (max_rows is not None and len(rows) > max_rows)
            ):
                return False
            prior_net = float("inf")
            for row in rows:
                if not isinstance(row, dict):
                    return False
                address = str(row.get("address") or "").lower()
                net_flow = row.get("net_flow")
                if (
                    not re.fullmatch(r"0x[a-f0-9]{40}", address)
                    or address in seen
                    or not _finite_real_json_number(net_flow)
                    or float(net_flow) <= 0
                    or float(net_flow) > prior_net + 0.01
                ):
                    return False
                seen.add(address)
                prior_net = float(net_flow)
                for key in (
                    "gross_inflow",
                    "gross_outflow",
                    "protocol_deposit",
                    "protocol_withdrawal",
                ):
                    value = row.get(key)
                    if not _finite_real_json_number(value) or float(value) < 0:
                        return False
                if schema_version >= 5:
                    gross_inflow = float(row["gross_inflow"])
                    gross_outflow = float(row["gross_outflow"])
                    expected_net = (
                        gross_inflow - gross_outflow
                        if kind == "accumulators"
                        else gross_outflow - gross_inflow
                    )
                    if abs(float(net_flow) - expected_net) > 0.02:
                        return False
                if schema_version >= 6:
                    for key in ("dolomite_trade_inflow", "dolomite_trade_outflow"):
                        value = row.get(key)
                        if not _finite_real_json_number(value) or float(value) < 0:
                            return False
        return True

    def complete_search_rows_are_valid(container):
        if schema_version < 4:
            return True
        search_container = {
            "total_transfers": container.get("total_transfers"),
            "accumulators": container.get("search_accumulators"),
            "sellers": container.get("search_sellers"),
        }
        if not leaderboard_is_valid(search_container, max_rows=None):
            return False
        for kind in ("accumulators", "sellers"):
            searchable = {
                str(row.get("address") or "").lower(): float(row["net_flow"])
                for row in search_container[kind]
            }
            for row in container[kind]:
                address = str(row.get("address") or "").lower()
                if (
                    address not in searchable
                    or abs(searchable[address] - float(row["net_flow"])) > 0.01
                ):
                    return False
        return True

    for period in periods:
        period_data = period_rows.get(period)
        if not isinstance(period_data, dict) or not {"eth", "bera", "all"}.issubset(period_data):
            return False
        if not all(
            leaderboard_is_valid(period_data[scope])
            and complete_search_rows_are_valid(period_data[scope])
            for scope in ("eth", "bera", "all")
        ):
            return False

        audit = bridge_audit.get(period)
        if not isinstance(audit, dict):
            return False
        for method in ("canonicalAdapter", "legacyHeuristic", "total"):
            row = audit.get(method)
            if (
                not isinstance(row, dict)
                or not _is_exact_integer(row.get("addressCount"))
                or row["addressCount"] < 0
                or not _finite_real_json_number(row.get("dolo"))
                or float(row["dolo"]) < 0
            ):
                return False
        if schema_version >= 5:
            observed = audit.get("legacyHeuristicObserved")
            if (
                not isinstance(observed, dict)
                or not _is_exact_integer(observed.get("addressCount"))
                or observed["addressCount"] < 0
                or not _finite_real_json_number(observed.get("dolo"))
                or float(observed["dolo"]) < 0
                or audit["legacyHeuristic"]["addressCount"] != 0
                or abs(float(audit["legacyHeuristic"]["dolo"])) > 0.000002
            ):
                return False
        if (
            audit["total"]["addressCount"]
            != audit["canonicalAdapter"]["addressCount"]
            + audit["legacyHeuristic"]["addressCount"]
            or abs(
                float(audit["total"]["dolo"])
                - float(audit["canonicalAdapter"]["dolo"])
                - float(audit["legacyHeuristic"]["dolo"])
            ) > 0.000002
        ):
            return False

        for chain_key in expected_chains:
            chain_boundaries = boundaries.get(chain_key)
            row = chain_boundaries.get(period) if isinstance(chain_boundaries, dict) else None
            if not isinstance(row, dict):
                return False
            values = [
                row.get("targetTimestamp"),
                row.get("startBlock"),
                row.get("startTimestamp"),
                row.get("endBlock"),
                row.get("endTimestamp"),
            ]
            if not all(_is_exact_integer(value) for value in values):
                return False
            if (
                row["startBlock"] > row["endBlock"]
                or row["targetTimestamp"] > row["startTimestamp"]
                or row["startTimestamp"] > row["endTimestamp"]
            ):
                return False

    if schema_version >= 5:
        try:
            snapshot_at = datetime.fromisoformat(
                str(data.get("timestamp") or "").replace("Z", "+00:00")
            )
            generated_at = datetime.fromisoformat(
                str(data.get("generatedAt") or "").replace("Z", "+00:00")
            )
        except (TypeError, ValueError):
            return False
        if snapshot_at.tzinfo is None or generated_at.tzinfo is None:
            return False
        expected_snapshot_timestamp = min(
            boundaries[chain_key]["all"]["endTimestamp"]
            for chain_key in expected_chains
        )
        if (
            int(snapshot_at.timestamp()) != expected_snapshot_timestamp
            or generated_at < snapshot_at
        ):
            return False
    if schema_version >= 6:
        trade_meta = data.get("dolomite_trade_meta")
        if (
            not isinstance(trade_meta, dict)
            or trade_meta.get("status") != "complete"
            or trade_meta.get("source") != "official-dolomite-subgraph-pinned-block"
            or set(trade_meta.get("chains") or {}) != expected_chains
        ):
            return False
        for chain_key in expected_chains:
            row = trade_meta["chains"].get(chain_key)
            if (
                not isinstance(row, dict)
                or row.get("status") != "complete"
                or row.get("source") != "official-dolomite-subgraph-pinned-block"
                or not _is_exact_integer(row.get("blockNumber"))
                or row["blockNumber"] != boundaries[chain_key]["all"]["endBlock"]
                or not _is_exact_integer(row.get("blockTimestamp"))
                or row["blockTimestamp"] <= 0
                or not _is_exact_integer(row.get("eventCount"))
                or row["eventCount"] < 0
            ):
                return False
    return True


RULES = {
    "dolo-liquidity.json": {
        "required_keys": ["schemaVersion", "generatedAt", "summary", "sources", "pools", "activePositions", "history", "quality"],
        "checks": [
            ("generatedAt must be fresh", lambda d: _fresh_timestamp(d.get("generatedAt"), 8)),
            ("DOLO liquidity identities and totals must reconcile", _dolo_liquidity_valid),
        ],
        "min_bytes": 500,
    },
    "dolo_flows.json": {
        "required_keys": ["timestamp", "dolo_price", "periods", "cex_supply_history", "dolomite_balance_meta", "dolomite_balances", "holder_dolomite_history_meta", "flow_history_integrity"],
        "checks": [
            ("periods must have data",     lambda d: len(d.get("periods", {})) >= 3),
            ("dolo_price must be positive", lambda d: d.get("dolo_price", 0) > 0),
            ("Transfer history must have independent RPC quorum coverage", _flow_history_integrity_is_valid),
            ("CEX exchange history must reconcile exactly", _dolo_cex_supply_history_valid),
            ("optional latest transaction metadata must be exact", _flow_tx_metadata_is_valid),
            ("optional LP activity metadata must be exact", _flow_lp_metadata_is_valid),
            ("Dolomite DOLO balances must be complete and reconcile", _flow_dolomite_balances_are_valid),
            ("Holder total-exposure history must have complete Dolomite coverage", _holder_dolomite_exposure_history_is_valid),
            ("v3-v6 combined flow rows, complete search index, exact boundaries, bridge/trade audit and freshness must reconcile", _flow_reconciliation_v3_is_valid),
        ],
        "min_bytes": 50_000,
    },
    "dolo_price_history.json": {
        "required_keys": ["updatedAt", "prices"],
        "checks": [
            ("prices must have at least 300 days", lambda d: len(d.get("prices", {})) >= 300),
            ("prices must be positive", lambda d: all(v > 0 for v in list(d.get("prices", {}).values())[:50])),
        ],
        "min_bytes": 5_000,
    },
    "dolo_holder_wallet_history.json": {
        "required_keys": ["timestamp", "holder_wallet_history"],
        "checks": [
            ("holder_wallet_history must have data", lambda d: len(d.get("holder_wallet_history", {})) >= 1),
        ],
        "min_bytes": 1_000_000,
        # Leave headroom below GitHub's hard 100 MB blob limit so generation
        # fails during validation instead of after a long scan at git push.
        "max_bytes": 95_000_000,
    },
    "dolo_holders.json": {
        "required_keys": ["contract", "timestamp", "stats", "holders"],
        "checks": [
            ("holders list must not be empty", lambda d: len(d.get("holders", [])) >= 10),
        ],
        "min_bytes": 10_000,
    },
    "vedolo_holders.json": {
        "required_keys": ["contract", "timestamp", "stats", "holders"],
        "checks": [
            ("holders must have entries", lambda d: len(d.get("holders", [])) >= 10),
        ],
        "min_bytes": 100_000,
    },
    "vedolo_early_unlock.json": {
        "required_keys": ["schemaVersion", "sourceBlock", "sourceTimestamp", "generatedAt", "feeCalculator", "walletCount", "positionCount", "wallets"],
        "checks": [
            ("early-unlock simulation must reconcile exact wei totals", _vedolo_early_unlock_valid),
            ("early-unlock simulation must have wallet rows", lambda d: len(d.get("wallets", [])) >= 10),
        ],
        "min_bytes": 10_000,
    },
    "vedolo-vote-power-history.json": {
        "required_keys": ["schemaVersion", "metric", "chain", "contract", "source", "targetBlock", "targetTimestamp", "totalSupplyWei", "lockedSupplyWei", "lastPointWei", "coverage", "points"],
        "checks": [
            ("veDOLO vote power history must be canonical and exact", _vedolo_vote_power_history_valid),
        ],
        "min_bytes": 200,
    },
    "odolo_flows.json": {
        "required_keys": ["timestamp", "current_block", "deploy_block", "cutoff_blocks", "transfer_coverage", "periods", "claimer_behavior", "claimer_periods", "claim_source_reconciliation"],
        "checks": [
            ("periods must have data", lambda d: len(d.get("periods", {})) >= 3),
            ("period transfer counts must be monotonic by window", _odolo_flow_windows_are_monotonic),
            ("period windows must not collapse to all-time", _odolo_flow_windows_are_not_collapsed),
            ("block metadata must prove full all-time coverage", _odolo_flow_block_metadata_is_valid),
            ("claimer lifecycle partitions must reconcile", _odolo_claimer_partitions_reconcile),
            ("claimer aggregates must reconcile to published rows", _odolo_claimer_aggregates_reconcile),
            ("claimer total must not exceed the 200M allocation", _odolo_claim_total_within_allocation),
            ("claimer sources must have canonical reconciliation metadata", _odolo_claim_source_reconciliation_is_valid),
            ("gross and net oDOLO flows must reconcile", _odolo_flow_components_reconcile),
            ("optional latest transaction metadata must be exact", _flow_tx_metadata_is_valid),
        ],
        "min_bytes": 50_000,
    },
    "odolo-claim-events.json": {
        "required_keys": ["schemaVersion", "generatedAt", "chainKey", "source", "fromBlock", "toBlock", "fromTimestamp", "toTimestamp", "eventEmitter", "distributors", "token", "events"],
        "checks": [
            ("generatedAt must be ISO datetime", lambda d: _is_iso_datetime(d.get("generatedAt"))),
            ("chain must be Berachain", lambda d: d.get("chainKey") == "berachain"),
            ("block range must be valid", lambda d: isinstance(d.get("fromBlock"), int) and isinstance(d.get("toBlock"), int) and d.get("fromBlock") <= d.get("toBlock")),
            ("timestamp range must be valid", lambda d: isinstance(d.get("fromTimestamp"), int) and isinstance(d.get("toTimestamp"), int) and d.get("fromTimestamp") <= d.get("toTimestamp")),
            ("events must use the canonical oDOLO distributor and token", _odolo_claim_events_are_canonical),
            ("events must have transaction evidence", lambda d: all(row.get("txHash") and row.get("timestamp") and row.get("user") and row.get("amount") for row in d.get("events", []))),
        ],
        "min_bytes": 500,
    },
    "reward-claim-events.json": {
        "required_keys": ["schemaVersion", "generatedAt", "protocol", "source", "methodology", "chains", "events"],
        "checks": [
            ("generatedAt must be ISO datetime", lambda d: _is_iso_datetime(d.get("generatedAt"))),
            ("schema version must be multi-chain", lambda d: d.get("schemaVersion") == 2),
            ("supported reward claim chains must have metadata", _reward_claim_events_include_supported_claim_chains),
            ("events must reference known chains", _reward_claim_events_have_known_chains),
            ("chain metadata must be complete", _reward_claim_events_have_chain_metadata),
            ("distributor tokens must be resolved", _reward_claim_events_have_distributor_tokens),
            ("sharded claim manifest must be consistent", _reward_claim_events_shard_manifest_is_consistent),
            ("events must have transaction evidence", _reward_claim_events_have_transaction_evidence),
        ],
        "min_bytes": 500,
    },
    "vedolo_flows.json": {
        "required_keys": ["timestamp", "total_unlocks", "total_locks", "total_transfers"],
        "checks": [],
        "min_bytes": 10_000,
    },
    "liquidation_risk.json": {
        "required_keys": ["generatedAt", "globalStats", "chainStats"],
        "checks": [
            ("chainStats must have entries", lambda d: len(d.get("chainStats", {})) >= 1),
        ],
        "min_bytes": 10_000,
    },
    "liquidation_history.json": {
        "required_keys": ["generatedAt", "liquidationHistory"],
        "checks": [
            ("liquidationHistory must be a list", lambda d: isinstance(d.get("liquidationHistory"), list)),
        ],
        "min_bytes": 100,
    },
    "exercisers_by_address.json": {
        "required_keys": [
            "updated",
            "total_addresses",
            "total_odolo_exercised",
            "total_odolo_exercise_usdc_paid",
            "total_odolo_exercised_exercises",
            "total_dolo_pair_vedolo",
            "total_dolo_pair_exercises",
            "total_dolo_paired",
            "exercisers",
        ],
        "checks": [
            ("exercisers must have entries", lambda d: len(d.get("exercisers", [])) >= 5),
            ("oDOLO exercise totals must exclude DOLO pairing", _odolo_exerciser_totals_reconcile),
            ("per-address oDOLO exercise totals must reconcile", _odolo_exerciser_address_totals_reconcile),
            ("exercise transactions must be unique and internally valid", _odolo_exercise_transactions_are_valid),
        ],
        "min_bytes": 50_000,
    },
    "early_exits.json": {
        "required_keys": ["schemaVersion", "coverage", "stats", "recent_exits"],
        "checks": [
            ("early exits must have complete event coverage", _early_exit_coverage_complete),
            ("early exit rows and totals must reconcile exactly", _early_exit_rows_reconcile),
        ],
        "min_bytes": 1_000,
    },
    "dolomite_tvl.json": {
        "required_keys": ["totalTvl", "totalBorrowed", "supplyLiquidity", "currentChainTvls", "tokensInUsd", "chainTokensInUsd", "chainMeta", "staleChains", "last_updated"],
        "checks": [
            ("totalTvl must be positive", lambda d: d.get("totalTvl", 0) > 0),
            ("last_updated must be fresh", lambda d: _fresh_timestamp(d.get("last_updated"))),
            ("all expected TVL chains must be present", _has_expected_tvl_chains),
            ("retired TVL chains must be recorded", _has_expected_tvl_retired_chains),
            ("chain metadata must be complete", _dolomite_chain_meta_complete),
            ("stale chain list must be known chains only", _dolomite_stale_chains_known),
            ("TVL totals must reconcile", _dolomite_tvl_totals_reconcile),
            ("token sums must reconcile with supply liquidity", _dolomite_token_sums_reconcile),
        ],
        "min_bytes": 1_000,
    },
    "defillama_data.json": {
        "required_keys": ["tvl", "totalSupply", "name", "currentChainTvls", "tokensInUsd", "chainTokensInUsd", "last_updated"],
        "checks": [
            ("last_updated must be fresh", lambda d: _fresh_timestamp(d.get("last_updated"))),
            ("all expected TVL chains must be present", _has_expected_tvl_chains),
            ("TVL history must be sorted and populated", _defillama_history_valid),
            ("Total Supply history must be sorted and populated", _defillama_total_supply_history_valid),
        ],
        "min_bytes": 10_000,
    },
    "dolomite_total_supply_history.json": {
        "required_keys": [
            "totalSupply",
            "currentSupply",
            "tvl",
            "currentTvl",
            "officialWindowStart",
            "officialMarketCount",
            "activeMarketCount",
            "allMarketCount",
            "staleOfficialMarketCount",
            "staleOfficialMarketSupply",
            "last_updated",
        ],
        "checks": [
            ("last_updated must be fresh", lambda d: _fresh_timestamp(d.get("last_updated"))),
            (
                "Total Supply history must be sorted and populated",
                _defillama_total_supply_history_valid,
            ),
            (
                "latest Total Supply history must match current supply",
                _dolomite_total_supply_history_current,
            ),
            (
                "Net TVL history must be sorted and populated",
                _defillama_history_valid,
            ),
            (
                "latest Net TVL history must match current Net TVL",
                _dolomite_net_tvl_history_current,
            ),
            (
                "Net TVL history must reconcile with Total Supply history",
                _dolomite_liquidity_histories_reconcile,
            ),
            (
                "official Total Supply market coverage must be complete",
                _dolomite_total_supply_history_coverage,
            ),
        ],
        "min_bytes": 10_000,
    },
    "dolomite_revenue.json": {
        "required_keys": ["schemaVersion", "protocol", "source", "generatedAt", "methodology", "assurance", "totals", "latest", "chainTotals7d", "chainTotals30d", "series"],
        "checks": [
            ("generatedAt must be fresh", lambda d: _fresh_timestamp(d.get("generatedAt"), max_hours=12)),
            ("revenue totals must reconcile with latest row", _dolomite_revenue_totals_valid),
            ("revenue rolling windows must reconcile with series", _dolomite_revenue_window_totals_valid),
            ("revenue chain windows must reconcile with series", _dolomite_revenue_chain_windows_valid),
            ("revenue history must be sorted and populated", _dolomite_revenue_series_valid),
            ("closed veBorrow rebate epochs must have audited max rebate baselines", _dolomite_revenue_borrow_fee_rebate_max_audits_valid),
        ],
        "min_bytes": 10_000,
    },
    "veborrow_simulation.json": {
        "required_keys": ["schemaVersion", "generatedAt", "status", "methodology", "sourceUrls", "config", "chains", "totals", "borrowers"],
        "checks": [
            ("veBorrow simulation shape must be valid", _veborrow_simulation_valid),
        ],
        "min_bytes": 500,
    },
    "dolomite-revenue-onchain-audit.json": {
        "required_keys": ["schemaVersion", "generatedAt", "targetDate", "targetTimestamp", "windowStartTimestamp", "windowEndTimestamp", "tolerancePct", "status", "summary", "methodology", "chains"],
        "checks": [
            ("onchain audit shape and status rollup must be valid", _dolomite_revenue_onchain_audit_valid),
        ],
        "min_bytes": 500,
    },
    "vedolo_stats.json": {
        "required_keys": ["stats", "timestamp"],
        "checks": [],
        "min_bytes": 100,
    },
    "vedolo_expiry.json": {
        "required_keys": ["buckets", "total_dolo", "timestamp"],
        "checks": [],
        "min_bytes": 100,
    },
    "dolo_price.json": {
        "required_keys": ["price"],
        "checks": [
            ("price must be positive", lambda d: d.get("price", 0) > 0),
        ],
        "min_bytes": 50,
    },
    "odolo_contract_data.json": {
        "required_keys": [
            "totalSupply",
            "totalSupplyWei",
            "allocationSupply",
            "allocationSupplyWei",
            "redeemedAndBurned",
            "redeemedAndBurnedWei",
            "allocationMethodology",
            "decimals",
            "blockNumber",
            "futureRewardsWallet",
            "futureRewardsReserve",
            "futureRewardsReserveWei",
            "inVesterBalance",
            "inVesterBalanceWei",
            "inCirculation",
            "inCirculationWei",
            "pushedTokens",
        ],
        "checks": [
            (
                "snapshot block must be a positive integer",
                lambda d: _is_exact_integer(d.get("blockNumber")) and d["blockNumber"] > 0,
            ),
            (
                "future rewards wallet must be tracked",
                lambda d: str(d.get("futureRewardsWallet", "")).lower() == ODOLO_FUTURE_REWARDS_WALLET,
            ),
            (
                "circulating supply must exclude future rewards and vester balances",
                _odolo_circulating_reconciles,
            ),
            (
                "circulating supply must be within total supply",
                lambda d: 0 <= float(d.get("inCirculation", -1)) <= float(d.get("totalSupply", 0)),
            ),
            (
                "allocation components must reconcile to 200M",
                _odolo_allocation_reconciles,
            ),
        ],
        "min_bytes": 50,
    },
    "metrics_snapshot.json": {
        "required_keys": ["snapshots"],
        "checks": [],
        "min_bytes": 100,
    },
    "avg_lock_data.json": {
        "required_keys": ["avg_lock_days", "total_exercises"],
        "checks": [],
        "min_bytes": 50,
    },
    "assets_live.json": {
        "required_keys": ["version", "generatedAt", "source", "rowCount", "chainCount", "chains", "rows"],
        "checks": [
            ("generatedAt must be ISO datetime", lambda d: _is_iso_datetime(d.get("generatedAt"))),
            ("rows must have entries", lambda d: len(d.get("rows", [])) >= 50),
            ("rowCount must match rows", lambda d: d.get("rowCount") == len(d.get("rows", []))),
            ("all active configured chains must be present", _has_expected_assets_live_chains),
            ("retired chains must be recorded", _has_expected_assets_live_retired_chains),
        ],
        "min_bytes": 10_000,
    },
}

# ── Data files stored in data/ subdirectories ────────────────────────────────
# These use glob-style matching for directories with multiple files
DATA_DIR_RULES = {
    "data/earn-netflow": {
        "min_files": 1,
        "file_ext": ".json",
    },
    "data/earn-snapshots": {
        "min_files": 1,
        "file_ext": ".json",
    },
}


# ── Validation Engine ────────────────────────────────────────────────────────

class ValidationResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.errors = []

    def ok(self, msg):
        self.passed += 1
        print(f"  ✅ {msg}")

    def fail(self, msg):
        self.failed += 1
        self.errors.append(msg)
        print(f"  ❌ {msg}")

    def skip(self, msg):
        self.skipped += 1
        print(f"  ⏭️  {msg}")


def validate_file(filepath, rules, result):
    """Validate a single JSON data file against its rules."""
    print(f"\n📄 {filepath}")

    # Check file exists
    if not os.path.exists(filepath):
        result.skip(f"File not found (may not be generated by this workflow)")
        return

    # Check minimum file size
    file_size = os.path.getsize(filepath)
    min_bytes = rules.get("min_bytes", 0)
    if file_size < min_bytes:
        result.fail(f"File too small: {file_size:,} bytes (min: {min_bytes:,})")
        return
    result.ok(f"Size: {file_size:,} bytes (min: {min_bytes:,})")
    max_bytes = rules.get("max_bytes")
    if max_bytes is not None:
        if file_size > max_bytes:
            result.fail(f"File too large: {file_size:,} bytes (max: {max_bytes:,})")
            return
        result.ok(f"Size within publish limit: {file_size:,} bytes (max: {max_bytes:,})")

    # Parse JSON
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        result.fail(f"Invalid JSON: {e}")
        return
    result.ok("Valid JSON")

    # Check required keys
    for key in rules.get("required_keys", []):
        if key in data:
            result.ok(f"Key '{key}' present")
        else:
            result.fail(f"Missing required key: '{key}'")

    # Run custom checks
    for desc, check_fn in rules.get("checks", []):
        try:
            if check_fn(data):
                result.ok(desc)
            else:
                result.fail(f"Check failed: {desc}")
        except Exception as e:
            result.fail(f"Check error ({desc}): {e}")


def validate_data_dir(dirpath, rules, result):
    """Validate a data subdirectory (must contain minimum number of files)."""
    print(f"\n📁 {dirpath}/")

    if not os.path.isdir(dirpath):
        result.skip(f"Directory not found")
        return

    ext = rules.get("file_ext", ".json")
    files = [f for f in os.listdir(dirpath) if f.endswith(ext)]
    min_files = rules.get("min_files", 1)

    if len(files) >= min_files:
        result.ok(f"{len(files)} {ext} files (min: {min_files})")
    else:
        result.fail(f"Only {len(files)} {ext} files (min: {min_files})")

    # Validate each JSON file in directory is parseable
    bad = 0
    for fname in files:
        fpath = os.path.join(dirpath, fname)
        try:
            with open(fpath) as f:
                json.load(f)
        except json.JSONDecodeError:
            bad += 1
            result.fail(f"Invalid JSON: {fname}")

    if bad == 0 and files:
        result.ok(f"All {len(files)} files valid JSON")


def main():
    print("=" * 60)
    print("🔍 Dolomite Data Validation Guard")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    result = ValidationResult()

    # Determine which files to validate
    if len(sys.argv) > 1:
        # Validate specific files passed as arguments
        targets = sys.argv[1:]
        for target in targets:
            normalized = os.path.normpath(target)
            if os.path.isdir(normalized):
                validate_data_dir(
                    normalized,
                    DATA_DIR_RULES.get(normalized, {"min_files": 1, "file_ext": ".json"}),
                    result,
                )
                continue

            basename = os.path.basename(target)
            if basename in RULES:
                validate_file(target, RULES[basename], result)
            else:
                print(f"\n⚠️  No rules defined for: {target}")
                result.skip(f"No validation rules for {target}")
    else:
        # Validate all known files
        for filename, rules in RULES.items():
            validate_file(filename, rules, result)

        for dirpath, rules in DATA_DIR_RULES.items():
            validate_data_dir(dirpath, rules, result)

    # Summary
    print("\n" + "=" * 60)
    print(f"📊 Results: {result.passed} passed, {result.failed} failed, {result.skipped} skipped")

    if result.errors:
        print("\n🚨 FAILURES:")
        for err in result.errors:
            print(f"   • {err}")
        print("\n⛔ Validation FAILED — commit will be skipped to protect production data.")
        sys.exit(1)
    else:
        print("\n✅ All validations passed — safe to commit.")
        sys.exit(0)


if __name__ == "__main__":
    main()
