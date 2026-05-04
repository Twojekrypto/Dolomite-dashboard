#!/usr/bin/env python3
"""
Run pinned strict live EARN reruns for the highest-priority unresolved markets.

Workflow per market:
  1. refresh canonical subaccount history on-chain (incremental runner)
  2. pin the live rerun to the exact canonical target block
  3. run the full unresolved cohort
  4. run a timeout-only retry cohort when needed
  5. merge latest rows, build forensic output, and explain the true tail
  6. write a combined strict-static + live report

The runner is resumable and guarded by a chain-level lock so two long live
sequences cannot step on the same Chrome/localhost environment.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set

from audit_earn_asset import (
    TIMEOUT_CATEGORIES,
    build_extracted_live_payload,
    build_forensic_live_report,
    merge_live_payloads,
    normalize_live_row_category,
    select_live_rows,
    summarize_live_results,
)
from build_earn_verified_ledger import ROOT
from earn_live_config import (
    build_endpoint_pairs,
    get_chain_live_rerun_defaults,
    get_live_preset_names,
)
from explain_earn_subaccount_history_mismatches import build_mismatch_explanations


CHAIN_LIVE_RERUN_DEFAULTS = get_chain_live_rerun_defaults()
LIVE_PRESET_NAMES = get_live_preset_names()
DEFAULT_RERUN_ROOT = ROOT / "data" / "earn-audit-reruns"
DEFAULT_LIVE_ROOT = ROOT / "data" / "earn-live-reruns"
DEFAULT_HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
DEFAULT_NETFLOW_DIR = ROOT / "data" / "earn-netflow"
DEFAULT_INCREMENTAL_RUNNER = ROOT / "run_earn_subaccount_history_incremental.py"
DEFAULT_LOCALHOST_URL = str(CHAIN_LIVE_RERUN_DEFAULTS["localhostUrl"])
DEFAULT_DEBUG_JSON_URL = str(CHAIN_LIVE_RERUN_DEFAULTS["debugJsonUrl"])
STATE_FILENAME_TEMPLATE = "{chain}-runner-state.json"
LOCK_FILENAME_TEMPLATE = "{chain}-runner.lock"
NON_BLOCKING_PATTERNS = {
    "hidden_collateral_dust",
    "tiny_snapshot_dust",
    "tiny_snapshot_balance",
    "exact_match_non_strict_inferred",
    "exact_match_non_strict_fallback",
    "exact_hidden_collateral_non_strict_inferred",
    "exact_hidden_collateral_non_strict_fallback",
}
RAW_DIAGNOSTIC_CATEGORIES = set(TIMEOUT_CATEGORIES) | {"missing_position", "no_data"}
INFORMATIONAL_TAIL_RETRY_PRESET = "targeted-slow-retry"
INFORMATIONAL_TAIL_RETRY_WORKERS = 2


def safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def raw_diagnostic_counts(counts: Optional[dict]) -> dict:
    if not isinstance(counts, dict):
        return {}
    return {
        category: value
        for category, value in sorted((category, safe_int(counts.get(category))) for category in RAW_DIAGNOSTIC_CATEGORIES)
        if value
    }


def build_raw_diagnostics(counts: Optional[dict]) -> dict:
    diagnostics = raw_diagnostic_counts(counts)
    return {
        "total": sum(diagnostics.values()),
        "counts": diagnostics,
        "blocking": False,
        "scope": "raw-live-phase",
    }


def build_audit_verdict(
    *,
    completed: bool,
    failed_count: int = 0,
    final_blocking_count: int = 0,
    final_informational_count: int = 0,
) -> dict:
    if failed_count > 0:
        status = "fail"
        reason = "failed_markets"
    elif final_blocking_count > 0:
        status = "fail"
        reason = "final_blocking_tail"
    elif final_informational_count > 0:
        status = "warn"
        reason = "final_informational_tail"
    elif not completed:
        status = "pending"
        reason = "awaiting_final_report"
    else:
        status = "pass"
        reason = "final_combined_report_clean"
    return {
        "status": status,
        "reason": reason,
        "finalBlockingTailCount": int(final_blocking_count),
        "finalInformationalTailCount": int(final_informational_count),
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def utc_now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path, default=None):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def state_path(live_root: Path, chain: str) -> Path:
    return live_root / STATE_FILENAME_TEMPLATE.format(chain=chain)


def lock_path(live_root: Path, chain: str) -> Path:
    return live_root / LOCK_FILENAME_TEMPLATE.format(chain=chain)


def load_state(live_root: Path, chain: str) -> dict:
    payload = read_json(state_path(live_root, chain), {})
    return payload if isinstance(payload, dict) else {}


def save_state(live_root: Path, chain: str, payload: dict) -> None:
    write_json(state_path(live_root, chain), payload)


def load_plan(path: Path) -> dict:
    payload = read_json(path, None)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid live rerun plan: {path}")
    return payload


def save_plan(path: Path, payload: dict) -> None:
    write_json(path, payload)


def load_current_plan(live_root: Path, chain: str) -> Optional[dict]:
    state = load_state(live_root, chain)
    raw = state.get("planPath")
    if not raw:
        return None
    path = Path(str(raw))
    if not path.exists():
        return None
    return load_plan(path)


def persist_plan_state(live_root: Path, chain: str, plan: dict) -> None:
    save_state(live_root, chain, {
        "chain": chain,
        "runId": plan.get("runId"),
        "planPath": str(Path(plan["runRoot"]) / "live-rerun-plan.json"),
        "canonicalTargetBlock": plan.get("canonicalTargetBlock"),
        "updatedAt": utc_now_iso(),
    })


def is_pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def acquire_lock(live_root: Path, chain: str, *, run_id: str) -> dict:
    path = lock_path(live_root, chain)
    payload = read_json(path, {})
    if isinstance(payload, dict):
        existing_pid = payload.get("pid")
        if is_pid_alive(existing_pid):
            raise SystemExit(
                f"Live rerun lock for {chain} is already held by pid {existing_pid} "
                f"(runId={payload.get('runId')})."
            )
    lock_payload = {
        "chain": chain,
        "pid": os.getpid(),
        "runId": run_id,
        "acquiredAt": utc_now_iso(),
    }
    write_json(path, lock_payload)
    return lock_payload


def release_lock(live_root: Path, chain: str) -> None:
    path = lock_path(live_root, chain)
    if not path.exists():
        return
    payload = read_json(path, {})
    if isinstance(payload, dict) and int(payload.get("pid") or 0) not in {0, os.getpid()} and is_pid_alive(payload.get("pid")):
        return
    try:
        path.unlink()
    except OSError:
        pass


def run_json_command(argv: Sequence[str], *, cwd: Path) -> dict:
    proc = subprocess.run(
        list(argv),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)
    stdout = proc.stdout.strip()
    if not stdout:
        return {}
    return json.loads(stdout)


def run_command(argv: Sequence[str], *, cwd: Path) -> None:
    proc = subprocess.run(
        list(argv),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def _incremental_stage_alive_count(stage_payload: dict) -> int:
    rows = stage_payload.get("workers") or []
    return sum(1 for row in rows if bool(row.get("alive")))


def _incremental_needs_continue(status: dict) -> bool:
    if bool(status.get("complete")):
        return False

    scan = status.get("scan") or {}
    new_address = status.get("newAddressBackfill") or {}
    apply_stage = status.get("apply") or {}

    if not bool(scan.get("complete")):
        return _incremental_stage_alive_count(scan) == 0
    if not bool(new_address.get("complete")):
        return _incremental_stage_alive_count(new_address) == 0
    if not bool(apply_stage.get("complete")):
        return _incremental_stage_alive_count(apply_stage) == 0
    return False


def ensure_canonical_history_ready(
    chain: str,
    *,
    incremental_runner: Path,
    refresh_history: bool,
    wait_seconds: int,
    poll_seconds: int,
) -> dict:
    def kick_continue(*, refresh_plan: bool) -> dict:
        cmd = ["python3", str(incremental_runner), "continue", "--chain", chain, "--json"]
        if refresh_plan:
            cmd.append("--refresh-plan")
        run_command(cmd, cwd=ROOT)
        return run_json_command(
            ["python3", str(incremental_runner), "status", "--chain", chain, "--json"],
            cwd=ROOT,
        )

    base_status = run_json_command(
        ["python3", str(incremental_runner), "status", "--chain", chain, "--json"],
        cwd=ROOT,
    )
    if refresh_history or not bool(base_status.get("complete")):
        base_status = kick_continue(refresh_plan=refresh_history)
    if bool(base_status.get("complete")):
        return base_status

    deadline = time.time() + max(1, int(wait_seconds))
    while time.time() < deadline:
        if _incremental_needs_continue(base_status):
            base_status = kick_continue(refresh_plan=False)
            if bool(base_status.get("complete")):
                return base_status
        time.sleep(max(1, int(poll_seconds)))
        base_status = run_json_command(
            ["python3", str(incremental_runner), "status", "--chain", chain, "--json"],
            cwd=ROOT,
        )
        if bool(base_status.get("complete")):
            return base_status
    raise SystemExit(
        f"Canonical history refresh for {chain} did not complete within {wait_seconds}s."
    )


def discover_latest_full_strict_summary(chain: str, rerun_root: Path) -> Path:
    candidates: List[tuple] = []
    for path in (rerun_root / chain).glob("*/summary.json"):
        payload = read_json(path, None)
        if not isinstance(payload, dict):
            continue
        if not bool(payload.get("strictMode")):
            continue
        market_count = int(payload.get("marketCount") or 0)
        generated_at = str(payload.get("generatedAt") or "")
        if market_count <= 0:
            continue
        candidates.append((market_count, generated_at, path.stat().st_mtime, path))
    if not candidates:
        raise SystemExit(f"No strict static rerun summaries found for {chain} in {rerun_root / chain}")
    candidates.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
    return candidates[0][3]


def resolve_history_target_block(history_dir: Path, chain: str) -> int:
    manifest = read_json(history_dir / "manifest.json", {})
    chain_manifest = ((manifest.get("chains") or {}).get(chain) or {}) if isinstance(manifest, dict) else {}
    try:
        return int(chain_manifest.get("lastBlock") or 0)
    except Exception:
        return 0


def unresolved_input_path(summary_path: Path, market_id: str, symbol: str) -> Path:
    return summary_path.parent / "unresolved" / f"{market_id}_{symbol}.json"


def static_report_path(summary_path: Path, market_id: str, symbol: str) -> Path:
    return summary_path.parent / "reports" / f"{market_id}_{symbol}.json"


def market_base_name(market: dict) -> str:
    return f"{market['marketId']}_{market['symbol']}"


def append_cache_buster(url: str, suffix: str) -> str:
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}phase={suffix}"


def collect_known_missing_wallets(live_root: Path, chain: str, market_id: str, symbol: str) -> Set[str]:
    pattern = f"{market_id}_{symbol}__*.json"
    result_paths = sorted(
        (live_root / chain).glob(f"*/results/{pattern}"),
        key=lambda path: path.stat().st_mtime,
    )
    if not result_paths:
        return set()
    try:
        merged = merge_live_payloads(result_paths)
    except Exception:
        return set()
    out: Set[str] = set()
    for row in merged.get("results") or []:
        address = str(row.get("address") or "").lower().strip()
        if not address:
            continue
        if normalize_live_row_category(row) == "missing_position":
            out.add(address)
    return out


def build_filtered_unresolved_payload(source_payload: dict, skipped_wallets: Set[str]) -> tuple[dict, int]:
    unresolved = list(source_payload.get("unresolved") or [])
    if not skipped_wallets:
        payload = dict(source_payload)
        payload["inputCount"] = len(unresolved)
        payload["unresolved"] = unresolved
        return payload, 0
    filtered = []
    skipped = 0
    for row in unresolved:
        wallet = str((row or {}).get("wallet") or "").lower().strip()
        if wallet and wallet in skipped_wallets:
            skipped += 1
            continue
        filtered.append(row)
    payload = dict(source_payload)
    payload["inputCount"] = len(filtered)
    payload["unresolved"] = filtered
    payload["knownMissingSkippedCount"] = skipped
    return payload, skipped


def build_live_plan(
    chain: str,
    *,
    summary_path: Path,
    live_root: Path,
    history_dir: Path,
    netflow_dir: Path,
    canonical_target_block: int,
    localhost_url: str,
    debug_json_url: str,
    workers_per_market: int,
    retry_workers_per_market: int,
    max_markets: Optional[int],
    min_unresolved_count: int,
    market_ids: Sequence[str],
    live_preset: Optional[str] = None,
) -> dict:
    summary = read_json(summary_path, None)
    if not isinstance(summary, dict):
        raise SystemExit(f"Invalid strict static summary: {summary_path}")
    run_id = f"{chain}-strict-live-pinned-t{canonical_target_block}-{utc_now_slug()}"
    run_root = live_root / chain / run_id
    paths = {
        "inputs": run_root / "inputs",
        "results": run_root / "results",
        "summaries": run_root / "summaries",
        "retryInputs": run_root / "retry-inputs",
        "forensic": run_root / "forensic",
        "tailExplain": run_root / "tail-explain",
        "reports": run_root / "reports",
    }
    for directory in paths.values():
        directory.mkdir(parents=True, exist_ok=True)
    selected_market_ids = {str(mid) for mid in market_ids}
    markets = []
    for row in summary.get("markets") or []:
        market_id = str(row.get("marketId") or "")
        symbol = str(row.get("symbol") or "")
        unresolved_count = int(row.get("unresolvedCount") or 0)
        if not market_id or not symbol or unresolved_count < max(0, int(min_unresolved_count)):
            continue
        if selected_market_ids and market_id not in selected_market_ids:
            continue
        input_path = unresolved_input_path(summary_path, market_id, symbol)
        static_path = static_report_path(summary_path, market_id, symbol)
        if not input_path.exists() or not static_path.exists():
            continue
        source_input_payload = read_json(input_path, None)
        if not isinstance(source_input_payload, dict):
            continue
        known_missing_wallets = collect_known_missing_wallets(live_root, chain, market_id, symbol)
        filtered_input_payload, skipped_known_missing = build_filtered_unresolved_payload(
            source_input_payload,
            known_missing_wallets,
        )
        filtered_input_count = int(filtered_input_payload.get("inputCount") or len(filtered_input_payload.get("unresolved") or []))
        if filtered_input_count < max(0, int(min_unresolved_count)):
            continue
        base = f"{market_id}_{symbol}"
        filtered_input_path = paths["inputs"] / f"{base}.json"
        write_json(filtered_input_path, filtered_input_payload)
        markets.append({
            "marketId": market_id,
            "symbol": symbol,
            "holderCount": int(row.get("holderCount") or 0),
            "resolvedCount": int(row.get("resolvedCount") or 0),
            "unresolvedCount": filtered_input_count,
            "sourceUnresolvedCount": unresolved_count,
            "skippedKnownMissingCount": skipped_known_missing,
            "staticReportPath": str(static_path),
            "sourceInputPath": str(input_path),
            "inputPath": str(filtered_input_path),
            "fullOutputPath": str(paths["results"] / f"{base}__full.json"),
            "fullSummaryPath": str(paths["summaries"] / f"{base}__full.json"),
            "timeoutRetryInputPath": str(paths["retryInputs"] / f"{base}__timeout.json"),
            "timeoutRetryOutputPath": str(paths["results"] / f"{base}__timeout-retry.json"),
            "timeoutRetrySummaryPath": str(paths["summaries"] / f"{base}__timeout-retry.json"),
            "informationalRetryInputPath": str(paths["retryInputs"] / f"{base}__informational.json"),
            "informationalRetryOutputPath": str(paths["results"] / f"{base}__informational-retry.json"),
            "informationalRetrySummaryPath": str(paths["summaries"] / f"{base}__informational-retry.json"),
            "outputPath": str(paths["results"] / f"{base}__merged.json"),
            "summaryPath": str(paths["summaries"] / f"{base}__merged.json"),
            "forensicPath": str(paths["forensic"] / f"{base}.json"),
            "tailExplainPath": str(paths["tailExplain"] / f"{base}.json"),
            "reportPath": str(paths["reports"] / f"{base}.json"),
            "status": "pending",
            "stage": "pending",
            "startedAt": None,
            "completedAt": None,
            "error": None,
        })
    markets.sort(key=lambda item: (-int(item["unresolvedCount"]), int(item["marketId"])))
    if max_markets is not None:
        markets = markets[: max(0, int(max_markets))]
    endpoint_pairs = build_endpoint_pairs(localhost_url, debug_json_url)
    localhost_urls = [str(pair["localhostUrl"]) for pair in endpoint_pairs]
    debug_json_urls = [str(pair["debugJsonUrl"]) for pair in endpoint_pairs]

    plan = {
        "runId": run_id,
        "generatedAt": utc_now_iso(),
        "chain": chain,
        "runRoot": str(run_root),
        "canonicalTargetBlock": int(canonical_target_block),
        "historyDir": str(history_dir),
        "netflowDir": str(netflow_dir),
        "sourceSummaryPath": str(summary_path),
        "sourceSummaryRunId": summary.get("runId"),
        "sourceSnapshotDate": summary.get("snapshotDate"),
        "livePreset": str(live_preset or ""),
        "localhostUrl": localhost_urls[0],
        "debugJsonUrl": debug_json_urls[0],
        "localhostUrls": localhost_urls,
        "debugJsonUrls": debug_json_urls,
        "endpointPairs": endpoint_pairs,
        "workersPerMarket": int(workers_per_market),
        "retryWorkersPerMarket": int(retry_workers_per_market),
        "informationalRetryPreset": INFORMATIONAL_TAIL_RETRY_PRESET,
        "informationalRetryWorkersPerMarket": INFORMATIONAL_TAIL_RETRY_WORKERS,
        "markets": markets,
    }
    save_plan(run_root / "live-rerun-plan.json", plan)
    persist_plan_state(live_root, chain, plan)
    write_json(run_root / "summary.json", build_run_summary(plan))
    return plan


def plan_has_full_cycle_fields(plan: dict) -> bool:
    markets = plan.get("markets") or []
    if not markets:
        return False
    sample = markets[0] or {}
    required = {
        "staticReportPath",
        "fullOutputPath",
        "fullSummaryPath",
        "timeoutRetryInputPath",
        "timeoutRetryOutputPath",
        "timeoutRetrySummaryPath",
        "informationalRetryInputPath",
        "informationalRetryOutputPath",
        "informationalRetrySummaryPath",
        "forensicPath",
        "tailExplainPath",
        "reportPath",
    }
    return all(key in sample for key in required) and "historyDir" in plan and "netflowDir" in plan


def build_run_summary(plan: dict) -> dict:
    market_rows = []
    aggregate_counts: Counter = Counter()
    aggregate_tail_causes: Counter = Counter()
    completed_count = 0
    failed_count = 0
    final_blocking_total = 0
    final_informational_total = 0
    timeout_retry_input_total = 0
    timeout_retry_completed_total = 0
    informational_retry_input_total = 0
    informational_retry_completed_total = 0
    combined_report_count = 0
    for market in plan.get("markets") or []:
        entry = {
            "marketId": market.get("marketId"),
            "symbol": market.get("symbol"),
            "unresolvedCount": market.get("unresolvedCount"),
            "sourceUnresolvedCount": market.get("sourceUnresolvedCount"),
            "skippedKnownMissingCount": market.get("skippedKnownMissingCount"),
            "status": market.get("status"),
            "stage": market.get("stage"),
            "outputPath": market.get("outputPath"),
            "summaryPath": market.get("summaryPath"),
            "reportPath": market.get("reportPath"),
        }
        summary_raw = str(market.get("summaryPath") or "").strip()
        report_raw = str(market.get("reportPath") or "").strip()
        summary_payload = read_json(Path(summary_raw), None) if summary_raw else None
        report_payload = read_json(Path(report_raw), None) if report_raw else None
        if isinstance(summary_payload, dict) and market.get("status") == "completed":
            completed_count += 1
            entry["liveSummary"] = {
                "completed": summary_payload.get("completed"),
                "verifiedChecked": summary_payload.get("verifiedChecked"),
                "activeChecked": summary_payload.get("activeChecked"),
                "realNonVerifiedChecked": summary_payload.get("realNonVerifiedChecked"),
                "blockingRealNonVerifiedChecked": summary_payload.get("blockingRealNonVerifiedChecked"),
                "informationalRealNonVerifiedChecked": summary_payload.get("informationalRealNonVerifiedChecked"),
                "timeouts": summary_payload.get("timeouts"),
                "missingPosition": summary_payload.get("missingPosition"),
                "counts": summary_payload.get("counts") or {},
            }
            entry["rawDiagnostics"] = build_raw_diagnostics(summary_payload.get("counts") or {})
            aggregate_counts.update(summary_payload.get("counts") or {})
        final_blocking_count = 0
        final_informational_count = 0
        if isinstance(report_payload, dict):
            combined_report_count += 1
            final_blocking_count = safe_int(report_payload.get("finalBlockingTailCount"))
            final_informational_count = safe_int(report_payload.get("finalInformationalTailCount"))
            timeout_retry_input_count = safe_int(report_payload.get("timeoutRetryInputCount"))
            timeout_retry_completed_count = safe_int(report_payload.get("timeoutRetryCompletedCount"))
            informational_retry_input_count = safe_int(report_payload.get("informationalRetryInputCount"))
            informational_retry_completed_count = safe_int(report_payload.get("informationalRetryCompletedCount"))
            entry["combinedReport"] = {
                "finalBlockingTailCount": final_blocking_count,
                "finalInformationalTailCount": final_informational_count,
                "timeoutRetryInputCount": timeout_retry_input_count,
                "timeoutRetryCompletedCount": timeout_retry_completed_count,
                "informationalRetryInputCount": informational_retry_input_count,
                "informationalRetryCompletedCount": informational_retry_completed_count,
                "tailLikelyCauseCounts": report_payload.get("tailLikelyCauseCounts") or {},
            }
            aggregate_tail_causes.update(report_payload.get("tailLikelyCauseCounts") or {})
            final_blocking_total += final_blocking_count
            final_informational_total += final_informational_count
            timeout_retry_input_total += timeout_retry_input_count
            timeout_retry_completed_total += timeout_retry_completed_count
            informational_retry_input_total += informational_retry_input_count
            informational_retry_completed_total += informational_retry_completed_count
        if market.get("status") == "failed":
            failed_count += 1
            entry["error"] = market.get("error")
        entry["auditVerdict"] = build_audit_verdict(
            completed=market.get("status") == "completed" and isinstance(report_payload, dict),
            failed_count=1 if market.get("status") == "failed" else 0,
            final_blocking_count=final_blocking_count,
            final_informational_count=final_informational_count,
        )
        market_rows.append(entry)
    market_count = len(plan.get("markets") or [])
    complete = all(str(row.get("status") or "") == "completed" for row in (plan.get("markets") or []))
    diagnostic_counts = raw_diagnostic_counts(dict(aggregate_counts))
    return {
        "generatedAt": utc_now_iso(),
        "runId": plan.get("runId"),
        "chain": plan.get("chain"),
        "canonicalTargetBlock": plan.get("canonicalTargetBlock"),
        "sourceSummaryPath": plan.get("sourceSummaryPath"),
        "sourceSnapshotDate": plan.get("sourceSnapshotDate"),
        "marketCount": market_count,
        "completedMarketCount": completed_count,
        "failedMarketCount": failed_count,
        "combinedReportCount": combined_report_count,
        "complete": complete,
        "auditVerdict": build_audit_verdict(
            completed=complete and completed_count == market_count and combined_report_count == market_count,
            failed_count=failed_count,
            final_blocking_count=final_blocking_total,
            final_informational_count=final_informational_total,
        ),
        "finalTailTotals": {
            "finalBlockingTailCount": final_blocking_total,
            "finalInformationalTailCount": final_informational_total,
            "timeoutRetryInputCount": timeout_retry_input_total,
            "timeoutRetryCompletedCount": timeout_retry_completed_total,
            "informationalRetryInputCount": informational_retry_input_total,
            "informationalRetryCompletedCount": informational_retry_completed_total,
            "tailLikelyCauseCounts": dict(aggregate_tail_causes),
        },
        "rawDiagnosticCounts": diagnostic_counts,
        "rawDiagnosticTotal": sum(diagnostic_counts.values()),
        "countSemantics": {
            "aggregateCounts": "Raw live-phase categories across full/merged pass results; diagnostic counts are not release blockers by themselves.",
            "finalTailTotals": "Final combined-report tail after timeout retry, forensic filtering, and true-tail explanation.",
            "auditVerdict": "Release readiness should be judged from final combined reports, not raw live-phase diagnostic counts.",
        },
        "aggregateCounts": dict(aggregate_counts),
        "aggregateTailLikelyCauseCounts": dict(aggregate_tail_causes),
        "markets": market_rows,
    }


def refresh_plan_summary(plan: dict) -> None:
    run_root = Path(str(plan["runRoot"]))
    save_plan(run_root / "live-rerun-plan.json", plan)
    write_json(run_root / "summary.json", build_run_summary(plan))


def plan_complete(plan: dict) -> bool:
    return all(str(row.get("status") or "") == "completed" for row in (plan.get("markets") or []))


def print_plan_status(plan: dict) -> None:
    markets = plan.get("markets") or []
    completed = sum(1 for row in markets if row.get("status") == "completed")
    failed = sum(1 for row in markets if row.get("status") == "failed")
    print(f"Chain: {plan['chain']}")
    print(f"Run: {plan['runId']}")
    print(f"Canonical target block: {int(plan['canonicalTargetBlock']):,}")
    print(f"Source strict summary: {plan['sourceSummaryPath']}")
    print(f"Markets: {completed}/{len(markets)} completed, {failed} failed")
    if markets:
        next_market = next((row for row in markets if row.get("status") != "completed"), None)
        if next_market:
            print(
                "Next market: "
                f"{next_market['marketId']} {next_market['symbol']} "
                f"(unresolved={next_market['unresolvedCount']}, stage={next_market.get('stage') or 'pending'})"
            )
    print(f"Complete: {plan_complete(plan)}")


def live_audit_command(
    plan: dict,
    market: dict,
    *,
    input_path: Path,
    output_path: Path,
    phase: str,
) -> List[str]:
    if phase == "informational-retry":
        worker_count = int(plan.get("informationalRetryWorkersPerMarket") or INFORMATIONAL_TAIL_RETRY_WORKERS)
        live_preset = str(plan.get("informationalRetryPreset") or INFORMATIONAL_TAIL_RETRY_PRESET)
    else:
        worker_count = (
            int(plan.get("retryWorkersPerMarket") or plan.get("workersPerMarket") or 1)
            if phase == "timeout-retry"
            else int(plan.get("workersPerMarket") or 1)
        )
        live_preset = str(plan.get("livePreset") or "").strip()
    endpoint_pairs = plan.get("endpointPairs") or build_endpoint_pairs(
        plan.get("localhostUrls") or plan.get("localhostUrl"),
        plan.get("debugJsonUrls") or plan.get("debugJsonUrl"),
    )
    cb_localhost_urls = [
        append_cache_buster(
            str(pair["localhostUrl"]),
            f"{market['marketId']}_{market['symbol']}_{phase}_{plan['canonicalTargetBlock']}_{index}",
        )
        for index, pair in enumerate(endpoint_pairs)
    ]
    debug_json_urls = [str(pair["debugJsonUrl"]) for pair in endpoint_pairs]
    cmd = [
        "python3",
        str(ROOT / "audit_earn_asset.py"),
        "live",
    ]
    if live_preset:
        cmd.extend(["--live-preset", live_preset])
    cmd.extend([
        "--input",
        str(input_path),
        "--chain",
        str(plan["chain"]),
        "--symbol",
        str(market["symbol"]),
        "--market-id",
        str(market["marketId"]),
        "--localhost-url",
        json.dumps(cb_localhost_urls, ensure_ascii=True),
        "--debug-json-url",
        json.dumps(debug_json_urls, ensure_ascii=True),
        "--workers",
        str(worker_count),
        "--block-tag",
        str(plan["canonicalTargetBlock"]),
        "--output",
        str(output_path),
    ])
    return cmd


def load_required_payload(path: Path, *, label: str) -> dict:
    payload = read_json(path, None)
    if not isinstance(payload, dict):
        raise SystemExit(f"{label} is missing or invalid: {path}")
    return payload


def write_live_summary(path: Path, payload: dict, *, plan: dict, market: dict, phase: str) -> dict:
    summary = summarize_live_results(payload)
    summary.update({
        "phase": phase,
        "marketId": market.get("marketId"),
        "symbol": market.get("symbol"),
        "canonicalTargetBlock": plan.get("canonicalTargetBlock"),
        "sourceSummaryPath": plan.get("sourceSummaryPath"),
        "liveResultsPath": str(path).replace("/summaries/", "/results/"),
    })
    write_json(path, summary)
    return summary


def build_timeout_retry_payload(payload: dict) -> dict:
    rows = select_live_rows(payload, "timeouts")
    return build_extracted_live_payload(payload, rows)


def build_informational_retry_payload(payload: dict) -> dict:
    rows = select_live_rows(payload, "informational")
    return build_extracted_live_payload(payload, rows)


def ensure_market_informational_retry_paths(plan: dict, market: dict) -> None:
    base = f"{market['marketId']}_{market['symbol']}"
    run_root = Path(str(plan["runRoot"]))
    retry_inputs = run_root / "retry-inputs"
    results = run_root / "results"
    summaries = run_root / "summaries"
    for directory in (retry_inputs, results, summaries):
        directory.mkdir(parents=True, exist_ok=True)
    market.setdefault("informationalRetryInputPath", str(retry_inputs / f"{base}__informational.json"))
    market.setdefault("informationalRetryOutputPath", str(results / f"{base}__informational-retry.json"))
    market.setdefault("informationalRetrySummaryPath", str(summaries / f"{base}__informational-retry.json"))


def build_tail_diff_report(
    plan: dict,
    *,
    market: dict,
    forensic_report: dict,
) -> dict:
    blocking_rows = list(forensic_report.get("blockingRows") or [])
    informational_rows = list(forensic_report.get("informationalRows") or [])
    addresses = sorted({
        str(row.get("address") or "").lower()
        for row in (blocking_rows + informational_rows)
        if str(row.get("address") or "").strip()
    })
    mismatch_payload = {
        "chain": plan["chain"],
        "comparisonBlock": plan["canonicalTargetBlock"],
        "selectedAddressCount": len(addresses),
        "mismatchAddressCount": 0,
        "explanations": [],
    }
    if addresses:
        mismatch_payload = build_mismatch_explanations(
            plan["chain"],
            history_dir=Path(str(plan.get("historyDir") or DEFAULT_HISTORY_DIR)),
            netflow_dir=Path(str(plan.get("netflowDir") or DEFAULT_NETFLOW_DIR)),
            addresses=addresses,
            comparison_block=int(plan["canonicalTargetBlock"]),
        )

    explanation_map = {
        str(item.get("address") or "").lower(): item
        for item in (mismatch_payload.get("explanations") or [])
    }
    likely_cause_counts: Counter = Counter()

    def build_row(entry: dict) -> dict:
        address = str(entry.get("address") or "").lower()
        explanation = explanation_map.get(address) or {}
        market_explanations = []
        for row in explanation.get("marketExplanations") or []:
            if str(row.get("marketId") or "") == str(market["marketId"]):
                market_explanations.append(row)
                likely_cause_counts.update(row.get("likelyCauses") or [])
        return {
            "address": address,
            "pattern": entry.get("pattern"),
            "severity": entry.get("severity"),
            "normalizedCategory": entry.get("normalizedCategory"),
            "staticStatus": entry.get("staticStatus"),
            "staticMethod": entry.get("staticMethod"),
            "staticReason": entry.get("staticReason"),
            "rootCauses": entry.get("rootCauses") or [],
            "resolvedSource": entry.get("resolvedSource"),
            "resolvedMethod": entry.get("resolvedMethod"),
            "resolvedVerificationStatus": entry.get("resolvedVerificationStatus"),
            "canonicalHistory": entry.get("canonicalHistory") or {},
            "verificationData": entry.get("verificationData") or {},
            "marketExplanations": market_explanations,
        }

    return {
        "generatedAt": utc_now_iso(),
        "chain": plan["chain"],
        "symbol": market["symbol"],
        "marketId": market["marketId"],
        "canonicalTargetBlock": int(plan["canonicalTargetBlock"]),
        "selectedAddressCount": len(addresses),
        "mismatchAddressCount": int(mismatch_payload.get("mismatchAddressCount") or 0),
        "likelyCauseCounts": dict(likely_cause_counts),
        "blockingRows": [build_row(row) for row in blocking_rows],
        "informationalRows": [build_row(row) for row in informational_rows],
    }


def build_combined_market_report(
    plan: dict,
    *,
    market: dict,
    static_report: dict,
    full_summary: dict,
    timeout_retry_summary: Optional[dict],
    merged_summary: dict,
    forensic_report: dict,
    tail_report: dict,
    informational_retry_summary: Optional[dict] = None,
) -> dict:
    final_blocking_count = len(forensic_report.get("blockingRows") or [])
    final_informational_count = len(forensic_report.get("informationalRows") or [])
    timeout_retry_input_count = safe_int(read_json(Path(market["timeoutRetryInputPath"]), {}).get("inputCount"))
    timeout_retry_completed_count = safe_int((timeout_retry_summary or {}).get("completed"))
    informational_retry_input_path = str(market.get("informationalRetryInputPath") or "").strip()
    informational_retry_input_count = (
        safe_int(read_json(Path(informational_retry_input_path), {}).get("inputCount"))
        if informational_retry_input_path
        else 0
    )
    informational_retry_completed_count = safe_int((informational_retry_summary or {}).get("completed"))
    return {
        "generatedAt": utc_now_iso(),
        "chain": plan["chain"],
        "symbol": market["symbol"],
        "marketId": market["marketId"],
        "canonicalTargetBlock": int(plan["canonicalTargetBlock"]),
        "sourceSnapshotDate": plan.get("sourceSnapshotDate"),
        "strictStatic": {
            "holderCount": static_report.get("holderCount"),
            "resolvedCount": static_report.get("resolvedCount"),
            "unresolvedCount": static_report.get("unresolvedCount"),
            "statusCounts": static_report.get("statusCounts") or {},
            "methodCounts": static_report.get("methodCounts") or {},
            "rootCauseCounts": static_report.get("rootCauseCounts") or {},
        },
        "liveFullPass": full_summary,
        "timeoutRetryInputCount": timeout_retry_input_count,
        "timeoutRetryCompletedCount": timeout_retry_completed_count,
        "liveTimeoutRetry": timeout_retry_summary,
        "informationalRetryInputCount": informational_retry_input_count,
        "informationalRetryCompletedCount": informational_retry_completed_count,
        "liveInformationalRetry": informational_retry_summary,
        "liveMerged": merged_summary,
        "finalBlockingTailCount": final_blocking_count,
        "finalInformationalTailCount": final_informational_count,
        "auditVerdict": build_audit_verdict(
            completed=True,
            final_blocking_count=final_blocking_count,
            final_informational_count=final_informational_count,
        ),
        "rawDiagnostics": build_raw_diagnostics((merged_summary or {}).get("counts") or {}),
        "tailLikelyCauseCounts": tail_report.get("likelyCauseCounts") or {},
        "forensicPath": market["forensicPath"],
        "tailExplainPath": market["tailExplainPath"],
        "fullOutputPath": market["fullOutputPath"],
        "timeoutRetryOutputPath": market["timeoutRetryOutputPath"],
        "informationalRetryOutputPath": market.get("informationalRetryOutputPath"),
        "mergedOutputPath": market["outputPath"],
    }


def live_phase_payload_is_complete(payload: dict, input_path: Path) -> bool:
    input_payload = read_json(input_path, {})
    expected = int(input_payload.get("inputCount") or len(input_payload.get("unresolved") or []))
    completed = int(payload.get("completed") or len(payload.get("results") or []))
    return expected <= 0 or completed >= expected


def ensure_live_phase_payload(
    plan: dict,
    *,
    market: dict,
    phase: str,
    input_path: Path,
    output_path: Path,
) -> dict:
    payload = read_json(output_path, None)
    if isinstance(payload, dict) and live_phase_payload_is_complete(payload, input_path):
        return payload
    cmd = live_audit_command(plan, market, input_path=input_path, output_path=output_path, phase=phase)
    proc = subprocess.run(cmd, cwd=str(ROOT), check=False)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)
    return load_required_payload(output_path, label=f"{phase} live output")


def run_market_cycle(plan: dict, market: dict) -> dict:
    market["status"] = "running"
    market["error"] = None
    if not market.get("startedAt"):
        market["startedAt"] = utc_now_iso()
    refresh_plan_summary(plan)

    stage = str(market.get("stage") or "pending")

    full_output = Path(market["fullOutputPath"])
    full_summary_path = Path(market["fullSummaryPath"])
    if stage == "pending":
        full_payload = ensure_live_phase_payload(
            plan,
            market=market,
            phase="full",
            input_path=Path(market["inputPath"]),
            output_path=full_output,
        )
        full_summary = write_live_summary(full_summary_path, full_payload, plan=plan, market=market, phase="full")
        market["stage"] = "full_completed"
        refresh_plan_summary(plan)
    else:
        full_payload = load_required_payload(full_output, label="full live output")
        full_summary = load_required_payload(full_summary_path, label="full live summary")

    timeout_retry_payload = build_timeout_retry_payload(full_payload)
    write_json(Path(market["timeoutRetryInputPath"]), timeout_retry_payload)
    timeout_retry_summary = None
    retry_output = Path(market["timeoutRetryOutputPath"])
    retry_summary_path = Path(market["timeoutRetrySummaryPath"])
    timeout_retry_count = int(timeout_retry_payload.get("inputCount") or 0)

    if timeout_retry_count > 0:
        if stage in {"pending", "full_completed"}:
            retry_payload = ensure_live_phase_payload(
                plan,
                market=market,
                phase="timeout-retry",
                input_path=Path(market["timeoutRetryInputPath"]),
                output_path=retry_output,
            )
            timeout_retry_summary = write_live_summary(
                retry_summary_path,
                retry_payload,
                plan=plan,
                market=market,
                phase="timeout-retry",
            )
            market["stage"] = "timeout_retry_completed"
            refresh_plan_summary(plan)
        else:
            retry_payload = load_required_payload(retry_output, label="timeout retry output")
            timeout_retry_summary = load_required_payload(retry_summary_path, label="timeout retry summary")
    else:
        retry_payload = None
        market["stage"] = "timeout_retry_completed"
        refresh_plan_summary(plan)

    merged_output = Path(market["outputPath"])
    merged_summary_path = Path(market["summaryPath"])
    if retry_payload is not None:
        merged_payload = merge_live_payloads([full_output, retry_output])
    else:
        merged_payload = full_payload
    write_json(merged_output, merged_payload)
    merged_summary = write_live_summary(
        merged_summary_path,
        merged_payload,
        plan=plan,
        market=market,
        phase="merged",
    )
    market["stage"] = "merged_completed"
    refresh_plan_summary(plan)

    forensic_report = build_forensic_live_report(merged_payload)
    write_json(Path(market["forensicPath"]), forensic_report)

    informational_retry_summary = None
    if not forensic_report.get("blockingRows") and forensic_report.get("informationalRows"):
        ensure_market_informational_retry_paths(plan, market)
        informational_retry_input = build_informational_retry_payload(merged_payload)
        write_json(Path(market["informationalRetryInputPath"]), informational_retry_input)
        informational_retry_count = int(informational_retry_input.get("inputCount") or 0)
        if informational_retry_count > 0:
            informational_retry_output = Path(market["informationalRetryOutputPath"])
            informational_retry_summary_path = Path(market["informationalRetrySummaryPath"])
            informational_retry_payload = ensure_live_phase_payload(
                plan,
                market=market,
                phase="informational-retry",
                input_path=Path(market["informationalRetryInputPath"]),
                output_path=informational_retry_output,
            )
            informational_retry_summary = write_live_summary(
                informational_retry_summary_path,
                informational_retry_payload,
                plan=plan,
                market=market,
                phase="informational-retry",
            )
            market["stage"] = "informational_retry_completed"
            refresh_plan_summary(plan)

            merged_payload = merge_live_payloads([merged_output, informational_retry_output])
            write_json(merged_output, merged_payload)
            merged_summary = write_live_summary(
                merged_summary_path,
                merged_payload,
                plan=plan,
                market=market,
                phase="merged",
            )
            market["stage"] = "merged_completed"
            refresh_plan_summary(plan)

            forensic_report = build_forensic_live_report(merged_payload)
            write_json(Path(market["forensicPath"]), forensic_report)

    tail_report = build_tail_diff_report(plan, market=market, forensic_report=forensic_report)
    write_json(Path(market["tailExplainPath"]), tail_report)

    static_report = load_required_payload(Path(market["staticReportPath"]), label="strict static report")
    combined_report = build_combined_market_report(
        plan,
        market=market,
        static_report=static_report,
        full_summary=full_summary,
        timeout_retry_summary=timeout_retry_summary,
        informational_retry_summary=informational_retry_summary,
        merged_summary=merged_summary,
        forensic_report=forensic_report,
        tail_report=tail_report,
    )
    write_json(Path(market["reportPath"]), combined_report)

    market["status"] = "completed"
    market["stage"] = "completed"
    market["completedAt"] = utc_now_iso()
    refresh_plan_summary(plan)
    return market


def run_next_market(plan: dict) -> dict:
    for market in plan.get("markets") or []:
        if str(market.get("status") or "") == "completed":
            continue
        try:
            return run_market_cycle(plan, market)
        except BaseException as exc:
            market["status"] = "failed"
            market["error"] = str(exc)
            refresh_plan_summary(plan)
            raise
    return {}


def _is_python_launcher(arg: str) -> bool:
    name = Path(str(arg)).name
    return name == "python" or name.startswith("python")


def _is_live_audit_invocation(argv: Sequence[str]) -> bool:
    if not argv:
        return False
    launcher = Path(str(argv[0])).name
    for index, arg in enumerate(argv):
        if Path(str(arg)).name != "audit_earn_asset.py":
            continue
        if len(argv) <= index + 1 or argv[index + 1] != "live":
            continue
        if index == 0 or _is_python_launcher(argv[0]):
            return True
        if launcher == "env" and any(_is_python_launcher(prior) for prior in argv[1:index]):
            return True
    return False


def detect_external_live_audits() -> List[dict]:
    try:
        proc = subprocess.run(
            ["ps", "-ax", "-o", "pid=", "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, PermissionError):
        # Some heartbeat sandboxes cannot inspect the process table. In that
        # case we skip external-live detection instead of blocking the whole
        # chained rerun pipeline.
        return []
    if proc.returncode != 0:
        return []
    current_pid = os.getpid()
    rows = []
    for line in proc.stdout.splitlines():
        text = str(line).strip()
        if not text or "audit_earn_asset.py" not in text:
            continue
        pid_text, _, command = text.partition(" ")
        try:
            pid = int(pid_text.strip())
        except ValueError:
            continue
        if pid == current_pid:
            continue
        try:
            argv = shlex.split(command)
        except ValueError:
            argv = command.split()
        if not _is_live_audit_invocation(argv):
            continue
        rows.append({"pid": pid, "command": command.strip()})
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run pinned strict live EARN reruns for top unresolved markets")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--chain", default="arbitrum")
    common.add_argument("--rerun-root", default=str(DEFAULT_RERUN_ROOT))
    common.add_argument("--live-root", default=str(DEFAULT_LIVE_ROOT))
    common.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR))
    common.add_argument("--netflow-dir", default=str(DEFAULT_NETFLOW_DIR))
    common.add_argument("--incremental-runner", default=str(DEFAULT_INCREMENTAL_RUNNER))
    common.add_argument("--summary-path", help="Optional explicit strict static summary path")
    common.add_argument("--live-preset", choices=LIVE_PRESET_NAMES, help="Optional named live rerun preset, e.g. dual-sharded")
    common.add_argument(
        "--localhost-url",
        default=DEFAULT_LOCALHOST_URL,
        help="Dashboard URL or comma-separated list of dashboard URLs for horizontally sharded live audits",
    )
    common.add_argument(
        "--debug-json-url",
        default=DEFAULT_DEBUG_JSON_URL,
        help="Chrome remote debugger /json endpoint or comma-separated list of endpoints",
    )
    common.add_argument("--workers-per-market", type=int, default=int(CHAIN_LIVE_RERUN_DEFAULTS["workersPerMarket"]))
    common.add_argument("--retry-workers-per-market", type=int, default=int(CHAIN_LIVE_RERUN_DEFAULTS["retryWorkersPerMarket"]))
    common.add_argument("--max-markets", type=int, default=int(CHAIN_LIVE_RERUN_DEFAULTS["maxMarkets"]))
    common.add_argument("--min-unresolved-count", type=int, default=int(CHAIN_LIVE_RERUN_DEFAULTS["minUnresolvedCount"]))
    common.add_argument("--market-id", action="append", default=[])

    plan_cmd = subparsers.add_parser("plan", parents=[common], help="Build a pinned live rerun plan")
    plan_cmd.add_argument("--refresh-history", action="store_true")
    plan_cmd.add_argument("--wait-seconds", type=int, default=900)
    plan_cmd.add_argument("--poll-seconds", type=int, default=5)
    plan_cmd.add_argument("--json", action="store_true")

    status_cmd = subparsers.add_parser("status", parents=[common], help="Show status for the current live rerun plan")
    status_cmd.add_argument("--json", action="store_true")

    continue_cmd = subparsers.add_parser("continue", parents=[common], help="Run or resume the current live rerun plan")
    continue_cmd.add_argument("--refresh-history", action="store_true")
    continue_cmd.add_argument("--refresh-plan", action="store_true")
    continue_cmd.add_argument("--wait-seconds", type=int, default=900)
    continue_cmd.add_argument("--poll-seconds", type=int, default=5)
    continue_cmd.add_argument("--json", action="store_true")

    return parser


def apply_live_preset_overrides(args: argparse.Namespace) -> argparse.Namespace:
    preset_name = str(getattr(args, "live_preset", "") or "").strip()
    if not preset_name:
        return args

    preset = get_chain_live_rerun_defaults(preset_name)
    if args.localhost_url == DEFAULT_LOCALHOST_URL:
        args.localhost_url = str(preset["localhostUrl"])
    if args.debug_json_url == DEFAULT_DEBUG_JSON_URL:
        args.debug_json_url = str(preset["debugJsonUrl"])
    if int(args.workers_per_market) == int(CHAIN_LIVE_RERUN_DEFAULTS["workersPerMarket"]):
        args.workers_per_market = int(preset["workersPerMarket"])
    if int(args.retry_workers_per_market) == int(CHAIN_LIVE_RERUN_DEFAULTS["retryWorkersPerMarket"]):
        args.retry_workers_per_market = int(preset["retryWorkersPerMarket"])
    return args


def maybe_build_plan(
    chain: str,
    *,
    args: argparse.Namespace,
    force_new_plan: bool,
) -> dict:
    live_root = Path(args.live_root)
    current_plan = None if force_new_plan else load_current_plan(live_root, chain)

    history_status = ensure_canonical_history_ready(
        chain,
        incremental_runner=Path(args.incremental_runner),
        refresh_history=bool(getattr(args, "refresh_history", False)),
        wait_seconds=int(getattr(args, "wait_seconds", 900)),
        poll_seconds=int(getattr(args, "poll_seconds", 5)),
    )
    target_block = int(history_status.get("targetBlock") or resolve_history_target_block(Path(args.history_dir), chain) or 0)
    if target_block <= 0:
        raise SystemExit(f"Could not resolve canonical target block for {chain}")

    if (
        current_plan
        and plan_has_full_cycle_fields(current_plan)
        and int(current_plan.get("canonicalTargetBlock") or 0) == target_block
        and not plan_complete(current_plan)
    ):
        return current_plan

    summary_path = Path(args.summary_path) if args.summary_path else discover_latest_full_strict_summary(chain, Path(args.rerun_root))
    return build_live_plan(
        chain,
        summary_path=summary_path,
        live_root=live_root,
        history_dir=Path(args.history_dir),
        netflow_dir=Path(args.netflow_dir),
        canonical_target_block=target_block,
        localhost_url=args.localhost_url,
        debug_json_url=args.debug_json_url,
        workers_per_market=max(1, int(args.workers_per_market)),
        retry_workers_per_market=max(1, int(args.retry_workers_per_market)),
        max_markets=args.max_markets,
        min_unresolved_count=max(1, int(args.min_unresolved_count)),
        market_ids=args.market_id or [],
        live_preset=getattr(args, "live_preset", None),
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args = apply_live_preset_overrides(args)
    chain = args.chain
    live_root = Path(args.live_root)

    if args.command == "status":
        plan = load_current_plan(live_root, chain)
        if not plan:
            raise SystemExit(f"No live rerun plan found for {chain}")
        if args.json:
            print(json.dumps(build_run_summary(plan), ensure_ascii=True, indent=2))
        else:
            print_plan_status(plan)
        return 0

    force_new_plan = args.command == "plan" or bool(getattr(args, "refresh_plan", False))
    plan = maybe_build_plan(chain, args=args, force_new_plan=force_new_plan)
    acquire_lock(live_root, chain, run_id=str(plan.get("runId") or ""))
    try:
        if args.command == "plan":
            payload = build_run_summary(plan)
            if args.json:
                print(json.dumps(payload, ensure_ascii=True, indent=2))
            else:
                print_plan_status(plan)
            return 0

        while not plan_complete(plan):
            external_live = detect_external_live_audits()
            if external_live:
                sample = ", ".join(f"{row['pid']}" for row in external_live[:5])
                raise SystemExit(
                    f"Detected external audit_earn_asset.py live process(es) already running: {sample}. "
                    "Wait for them to finish before starting the next chained live market."
                )
            market = run_next_market(plan)
            if not market:
                break
            plan = load_plan(Path(plan["runRoot"]) / "live-rerun-plan.json")

        payload = build_run_summary(plan)
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            print_plan_status(plan)
        return 0
    finally:
        release_lock(live_root, chain)


if __name__ == "__main__":
    raise SystemExit(main())
