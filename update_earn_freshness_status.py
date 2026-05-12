#!/usr/bin/env python3
"""Build the public EARN freshness status and watchdog trigger plan.

The dashboard treats canonical EARN history as verified while it is within the
per-chain three-hour block window. This script turns that rule into an
operational status file and tells GitHub Actions which refresh workflows should
be triggered when data starts drifting.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from scan_earn_netflow import CHAINS, get_block_number


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "data" / "earn-freshness" / "status.json"

REFRESH_AFTER_MINUTES = 60
VERIFIED_AFTER_HOURS = 3
STALE_AFTER_HOURS = 4

CHAIN_POLICIES: Dict[str, Dict[str, Any]] = {
    "ethereum": {
        "label": "Ethereum",
        "blockTimeSeconds": 12.0,
        "verifiedBlockLag": 900,
        "canonicalWorkflow": "update-earn-ethereum-canonical-history.yml",
        "canonicalSupported": True,
        "canonicalCoverageCompleteness": "required",
    },
    "berachain": {
        "label": "Berachain",
        "blockTimeSeconds": 2.0,
        "verifiedBlockLag": 5400,
        "refreshAfterMinutes": 60,
        "canonicalWorkflow": "update-earn-berachain-canonical-history.yml",
        "netflowWorkflow": "update-earn-berachain-netflow.yml",
        "canonicalSupported": True,
        "canonicalCoverageCompleteness": "advisory",
    },
    "arbitrum": {
        "label": "Arbitrum",
        "blockTimeSeconds": 0.25,
        "verifiedBlockLag": 43200,
        "canonicalWorkflow": "update-earn-arbitrum-canonical-history.yml",
        "canonicalSupported": True,
        "canonicalCoverageCompleteness": "advisory",
    },
    "botanix": {
        "label": "Botanix",
        "blockTimeSeconds": 6.0,
        "verifiedBlockLag": 1800,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalWorkflowInputs": {"chain": "botanix"},
        "canonicalSupported": True,
        "canonicalCoverageCompleteness": "required",
    },
    "mantle": {
        "label": "Mantle",
        "blockTimeSeconds": 2.0,
        "verifiedBlockLag": 5400,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalWorkflowInputs": {"chain": "mantle"},
        "canonicalSupported": True,
        "canonicalCoverageCompleteness": "advisory",
    },
    "polygonzkevm": {
        "label": "Polygon zkEVM",
        "blockTimeSeconds": 3.2,
        "verifiedBlockLag": 3400,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalWorkflowInputs": {"chain": "polygonzkevm"},
        "canonicalSupported": True,
        "canonicalCoverageCompleteness": "advisory",
    },
    "xlayer": {
        "label": "X Layer",
        "blockTimeSeconds": 1.0,
        "verifiedBlockLag": 10800,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalWorkflowInputs": {"chain": "xlayer"},
        "canonicalSupported": True,
        "canonicalCoverageCompleteness": "required",
    },
}

NETFLOW_WORKFLOW = "update-earn-netflow.yml"


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def _parse_timestamp(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _age_minutes(value: Any, now: datetime) -> Optional[float]:
    parsed = _parse_timestamp(value)
    if not parsed:
        return None
    return max(0.0, (now - parsed).total_seconds() / 60)


def _threshold_blocks(policy: Dict[str, Any], minutes: float) -> int:
    block_time = float(policy["blockTimeSeconds"])
    return max(1, int(round((minutes * 60) / block_time)))


def _status_for_lag(block_lag: Optional[int], policy: Dict[str, Any]) -> str:
    if block_lag is None:
        return "missing"
    if block_lag < 0:
        return "ahead"
    verified_lag = int(policy["verifiedBlockLag"])
    stale_lag = _threshold_blocks(policy, STALE_AFTER_HOURS * 60)
    if block_lag <= verified_lag:
        return "verified"
    if block_lag <= stale_lag:
        return "syncing"
    return "stale"


def _estimated_minutes(block_lag: Optional[int], policy: Dict[str, Any]) -> Optional[float]:
    if block_lag is None:
        return None
    return round(max(0, block_lag) * float(policy["blockTimeSeconds"]) / 60, 1)


def _format_lag_reason_minutes(value: Any) -> str:
    if value is None:
        return "unknown lag"
    return f"{value}m lag"


def _refresh_mode(status: str, refresh_recommended: bool) -> str:
    if not refresh_recommended:
        return "none"
    if status in {"verified", "ahead"}:
        return "background"
    return "catchup"


def _component_status(
    *,
    chain: str,
    component: str,
    policy: Dict[str, Any],
    live_block: Optional[int],
    last_block: Optional[int],
    updated_at: Any,
    supported: bool,
    now: datetime,
) -> Dict[str, Any]:
    if not supported:
        return {
            "status": "unsupported",
            "lastBlock": last_block,
            "liveBlock": live_block,
            "blockLag": None,
            "estimatedLagMinutes": None,
            "updatedAt": updated_at or None,
            "updatedAgeMinutes": None,
            "refreshRecommended": False,
            "refreshMode": "none",
            "reason": f"{component} is not supported for {chain}",
        }

    block_lag = None
    if live_block is not None and last_block is not None:
        block_lag = int(live_block) - int(last_block)

    status = _status_for_lag(block_lag, policy)
    refresh_after_minutes = float(policy.get("refreshAfterMinutes", REFRESH_AFTER_MINUTES))
    refresh_after_blocks = _threshold_blocks(policy, refresh_after_minutes)
    updated_age_minutes = _age_minutes(updated_at, now)
    if (
        status == "stale"
        and updated_age_minutes is not None
        and updated_age_minutes <= refresh_after_minutes
    ):
        status = "catching_up"
    refresh_recommended = status in {"missing", "syncing", "stale", "catching_up"} or (
        block_lag is not None and block_lag >= refresh_after_blocks
    )

    if live_block is None:
        status = "unknown"
        refresh_recommended = False

    refresh_mode = _refresh_mode(status, refresh_recommended)

    return {
        "status": status,
        "lastBlock": last_block,
        "liveBlock": live_block,
        "blockLag": block_lag,
        "estimatedLagMinutes": _estimated_minutes(block_lag, policy),
        "updatedAt": updated_at or None,
        "updatedAgeMinutes": round(updated_age_minutes, 1) if updated_age_minutes is not None else None,
        "refreshAfterMinutes": refresh_after_minutes,
        "refreshAfterBlockLag": refresh_after_blocks,
        "verifiedBlockLag": int(policy["verifiedBlockLag"]),
        "staleBlockLag": _threshold_blocks(policy, STALE_AFTER_HOURS * 60),
        "refreshRecommended": refresh_recommended,
        "refreshMode": refresh_mode,
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _known_addresses_from_public_data(data_dir: Path, chain: str) -> list[str]:
    addresses = set()

    snapshot_manifest = _read_json(data_dir / "earn-snapshots" / "manifest.json", {})
    chain_dates = [
        str(date)
        for date in (snapshot_manifest.get("dates") or [])
        if chain in ((snapshot_manifest.get("chains") or {}).get(date) or [])
    ]
    if chain_dates:
        latest_date = sorted(chain_dates)[-1]
        payload = _read_json(data_dir / "earn-snapshots" / f"{latest_date}.json", {})
        chain_snapshots = ((payload.get("snapshots") or {}).get(chain) or {}) if isinstance(payload, dict) else {}
        addresses.update(str(address).lower() for address in chain_snapshots.keys())

    netflow_payload = _read_json(data_dir / "earn-netflow" / f"{chain}.json", {})
    netflows = (netflow_payload.get("netflows") or {}) if isinstance(netflow_payload, dict) else {}
    addresses.update(str(address).lower() for address in netflows.keys())

    return sorted(address for address in addresses if address.startswith("0x") and len(address) == 42)


def _canonical_coverage_status(*, data_dir: Path, chain: str, target_block: Optional[int]) -> Dict[str, Any]:
    target = _safe_int(target_block)
    addresses = _known_addresses_from_public_data(data_dir, chain)
    if target <= 0:
        return {
            "status": "missing_target",
            "targetBlock": target_block,
            "knownAddressCount": len(addresses),
            "freshWalletCount": 0,
            "partialWalletCount": 0,
            "missingWalletCount": len(addresses),
            "freshCoverageRatio": 0.0,
        }
    if not addresses:
        return {
            "status": "unknown",
            "targetBlock": target,
            "knownAddressCount": 0,
            "freshWalletCount": 0,
            "partialWalletCount": 0,
            "missingWalletCount": 0,
            "freshCoverageRatio": None,
        }

    fresh_count = 0
    partial_count = 0
    missing_count = 0
    min_last_scanned: Optional[int] = None
    max_last_scanned: Optional[int] = None
    chain_dir = data_dir / "earn-subaccount-history" / chain
    for address in addresses:
        payload = _read_json(chain_dir / f"{address}.json", None)
        if not isinstance(payload, dict):
            missing_count += 1
            continue
        last_scanned = _safe_int(payload.get("lastScannedBlock"))
        min_last_scanned = last_scanned if min_last_scanned is None else min(min_last_scanned, last_scanned)
        max_last_scanned = last_scanned if max_last_scanned is None else max(max_last_scanned, last_scanned)
        if last_scanned >= target:
            fresh_count += 1
        else:
            partial_count += 1

    complete = fresh_count == len(addresses) and partial_count == 0 and missing_count == 0
    return {
        "status": "full" if complete else "partial",
        "targetBlock": target,
        "knownAddressCount": len(addresses),
        "freshWalletCount": fresh_count,
        "partialWalletCount": partial_count,
        "missingWalletCount": missing_count,
        "freshCoverageRatio": round(fresh_count / len(addresses), 6),
        "minLastScannedBlock": min_last_scanned,
        "maxLastScannedBlock": max_last_scanned,
    }


def _canonical_coverage_completeness(policy: Dict[str, Any]) -> str:
    mode = str(policy.get("canonicalCoverageCompleteness") or "required").strip().lower()
    return mode if mode in {"required", "advisory"} else "required"


def _apply_canonical_coverage(
    component: Dict[str, Any],
    coverage: Dict[str, Any],
    *,
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    completeness = _canonical_coverage_completeness(policy)
    component["coverage"] = coverage
    component["coverageCompleteness"] = completeness
    if coverage.get("status") != "partial":
        component["coverageCatchup"] = False
        component["coverageBacklog"] = False
        return component
    component["recencyStatus"] = component.get("status")
    if completeness == "advisory":
        component["coverageCatchup"] = False
        component["coverageBacklog"] = True
        component["reason"] = (
            f"canonical global backfill pending: {coverage.get('freshWalletCount')}/"
            f"{coverage.get('knownAddressCount')} wallets fresh"
        )
        return component
    component["refreshRecommended"] = True
    component["coverageCatchup"] = True
    component["coverageBacklog"] = False
    if component.get("status") in {"verified", "ahead"}:
        component["status"] = "syncing"
    component["refreshMode"] = _refresh_mode(str(component.get("status") or "syncing"), True)
    component["reason"] = (
        f"canonical coverage incomplete: {coverage.get('freshWalletCount')}/"
        f"{coverage.get('knownAddressCount')} wallets fresh"
    )
    return component


def _live_blocks(chains: Iterable[str]) -> Dict[str, Optional[int]]:
    blocks: Dict[str, Optional[int]] = {}
    for chain in chains:
        cfg = CHAINS.get(chain)
        rpcs = (cfg or {}).get("rpcs") or []
        if not rpcs:
            blocks[chain] = None
            continue
        try:
            blocks[chain] = int(get_block_number(rpcs, [0]))
        except Exception as exc:  # noqa: BLE001 - preserve watchdog output instead of failing all chains
            print(f"[earn-freshness] live block lookup failed for {chain}: {exc}")
            blocks[chain] = None
    return blocks


def _register_refresh_job(
    refresh_jobs_by_workflow: Dict[str, Dict[str, Any]],
    *,
    workflow: str,
    inputs: Optional[Dict[str, Any]] = None,
    priority: int = 50,
    mode: str = "catchup",
    reason: str = "",
) -> None:
    normalized_inputs = {
        str(key): str(value)
        for key, value in (inputs or {}).items()
        if value is not None and str(value) != ""
    }
    normalized_mode = str(mode or "catchup")
    job_key = workflow
    if normalized_inputs.get("chain") not in {"", None, "all"}:
        job_key = f"{workflow}:chain={normalized_inputs['chain']}"

    existing = refresh_jobs_by_workflow.get(job_key)
    if existing is None:
        refresh_jobs_by_workflow[job_key] = {
            "workflow": workflow,
            "inputs": normalized_inputs,
            "priority": int(priority),
            "mode": normalized_mode,
            "reason": reason,
        }
        return
    existing["priority"] = min(int(existing.get("priority", 50)), int(priority))
    if normalized_mode == "catchup" or existing.get("mode") not in {"catchup", "background"}:
        existing["mode"] = normalized_mode
    if reason and not existing.get("reason"):
        existing["reason"] = reason
    if existing.get("inputs") != normalized_inputs:
        merged_inputs: Dict[str, str] = {}
        if "chain" in (existing.get("inputs") or {}) or "chain" in normalized_inputs:
            merged_inputs["chain"] = "all"
        refresh_jobs_by_workflow[job_key] = {
            "workflow": workflow,
            "inputs": merged_inputs,
            "priority": min(int(existing.get("priority", 50)), int(priority)),
            "mode": "catchup" if "catchup" in {existing.get("mode"), normalized_mode} else normalized_mode,
            "reason": existing.get("reason") or reason,
        }


def _latest_timestamp(*values: Any) -> Optional[str]:
    timestamps = [parsed for value in values if (parsed := _parse_timestamp(value)) is not None]
    if not timestamps:
        return None
    return max(timestamps).isoformat().replace("+00:00", "Z")


def _chain_refresh_mode(*components: Dict[str, Any]) -> str:
    modes = {str(component.get("refreshMode") or "none") for component in components}
    if "catchup" in modes:
        return "catchup"
    if "background" in modes:
        return "background"
    return "none"


def _support_mode(
    *,
    policy: Dict[str, Any],
    canonical_supported: bool,
    netflow_supported: bool,
) -> str:
    if policy.get("supportMode"):
        return str(policy["supportMode"])
    if canonical_supported and netflow_supported:
        return "canonical-ledger"
    if canonical_supported:
        return "canonical-only"
    if netflow_supported:
        return "netflow-only"
    return "snapshot-first"


def _weak_point(
    *,
    support_mode: str,
    canonical: Dict[str, Any],
    netflow: Dict[str, Any],
    snapshot_available: bool,
) -> str:
    if support_mode == "snapshot-first":
        if snapshot_available:
            return "snapshot-first coverage; canonical and netflow event ledgers are not enabled"
        return "snapshot-first coverage; snapshot is currently missing"
    canonical_coverage = canonical.get("coverage") or {}
    if canonical_coverage.get("status") == "partial" and canonical.get("coverageCatchup"):
        return (
            f"canonical coverage {canonical_coverage.get('freshWalletCount')}/"
            f"{canonical_coverage.get('knownAddressCount')} wallets fresh"
        )
    for name, component in (("canonical", canonical), ("netflow", netflow)):
        if component.get("status") in {"missing", "syncing", "stale", "catching_up", "unknown"}:
            return f"{name} {component['status']}"
    for name, component in (("canonical", canonical), ("netflow", netflow)):
        if component.get("refreshMode") == "background":
            return f"{name} background refresh due"
    if canonical_coverage.get("status") == "partial" and canonical.get("coverageBacklog"):
        return (
            f"canonical global backfill {canonical_coverage.get('freshWalletCount')}/"
            f"{canonical_coverage.get('knownAddressCount')} wallets fresh"
        )
    return "none"


def _refresh_job_priority(component: Dict[str, Any], component_name: str) -> int:
    mode = str(component.get("refreshMode") or "none")
    if mode == "background":
        return 50 if component_name == "canonical" else 55
    if mode != "catchup":
        return 90
    status = str(component.get("status") or "")
    status_priority = {
        "missing": 0,
        "stale": 0,
        "syncing": 10,
        "catching_up": 20,
        "unknown": 30,
    }.get(status, 10)
    if component_name != "canonical":
        status_priority += 5
    return status_priority


def build_status(
    *,
    data_dir: Path,
    live_blocks: Optional[Dict[str, Optional[int]]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    live_blocks = dict(live_blocks) if live_blocks is not None else _live_blocks(CHAIN_POLICIES.keys())

    history_manifest = _read_json(data_dir / "earn-subaccount-history" / "manifest.json", {})
    history_chains = history_manifest.get("chains") or {}
    snapshot_manifest = _read_json(data_dir / "earn-snapshots" / "manifest.json", {})

    chains: Dict[str, Any] = {}
    chain_report = []
    refresh_workflows = set()
    refresh_jobs_by_workflow: Dict[str, Dict[str, Any]] = {}
    refresh_reasons = []

    for chain, policy in CHAIN_POLICIES.items():
        live_block = live_blocks.get(chain)
        history_meta = history_chains.get(chain) or {}
        netflow_payload = _read_json(data_dir / "earn-netflow" / f"{chain}.json", {})
        latest_snapshot = None
        for date_str in reversed(snapshot_manifest.get("dates") or []):
            if chain in (snapshot_manifest.get("chains", {}).get(date_str) or []):
                latest_snapshot = date_str
                break

        canonical_supported = bool(policy.get("canonicalSupported"))
        canonical = _component_status(
            chain=chain,
            component="canonical",
            policy=policy,
            live_block=live_block,
            last_block=history_meta.get("lastBlock"),
            updated_at=history_meta.get("updatedAt"),
            supported=canonical_supported,
            now=now,
        )
        if canonical_supported:
            coverage = _canonical_coverage_status(
                data_dir=data_dir,
                chain=chain,
                target_block=history_meta.get("lastBlock"),
            )
            canonical = _apply_canonical_coverage(canonical, coverage, policy=policy)
        netflow_supported = (CHAINS.get(chain) or {}).get("start_block", 0) >= 0
        support_mode = _support_mode(
            policy=policy,
            canonical_supported=canonical_supported,
            netflow_supported=netflow_supported,
        )
        netflow = _component_status(
            chain=chain,
            component="netflow",
            policy=policy,
            live_block=live_block,
            last_block=netflow_payload.get("lastBlock"),
            updated_at=netflow_payload.get("updatedAt"),
            supported=netflow_supported,
            now=now,
        )

        if canonical.get("refreshRecommended") and canonical_supported:
            workflow = str(policy["canonicalWorkflow"])
            refresh_workflows.add(workflow)
            coverage = canonical.get("coverage") or {}
            if coverage.get("status") == "partial" and canonical.get("coverageCatchup"):
                reason = (
                    f"{chain}: canonical coverage catchup "
                    f"({coverage.get('freshWalletCount')}/{coverage.get('knownAddressCount')} wallets fresh, "
                    f"{coverage.get('partialWalletCount')} behind, {coverage.get('missingWalletCount')} missing)"
                )
            else:
                reason = (
                    f"{chain}: canonical {canonical['refreshMode']} refresh "
                    f"({canonical['status']}, {_format_lag_reason_minutes(canonical.get('estimatedLagMinutes'))})"
                )
            _register_refresh_job(
                refresh_jobs_by_workflow,
                workflow=workflow,
                inputs=policy.get("canonicalWorkflowInputs"),
                priority=_refresh_job_priority(canonical, "canonical"),
                mode=str(canonical.get("refreshMode") or "catchup"),
                reason=reason,
            )
            refresh_reasons.append(reason)
        if netflow.get("refreshRecommended") and netflow_supported:
            workflow = str(policy.get("netflowWorkflow") or NETFLOW_WORKFLOW)
            workflow_inputs = policy.get("netflowWorkflowInputs")
            if workflow_inputs is None and workflow == NETFLOW_WORKFLOW:
                workflow_inputs = {"chain": chain}
            refresh_workflows.add(workflow)
            reason = (
                f"{chain}: netflow {netflow['refreshMode']} refresh "
                f"({netflow['status']}, {_format_lag_reason_minutes(netflow.get('estimatedLagMinutes'))})"
            )
            _register_refresh_job(
                refresh_jobs_by_workflow,
                workflow=workflow,
                inputs=workflow_inputs,
                priority=_refresh_job_priority(netflow, "netflow"),
                mode=str(netflow.get("refreshMode") or "catchup"),
                reason=reason,
            )
            refresh_reasons.append(reason)

        chain_refresh_mode = _chain_refresh_mode(canonical, netflow)
        chain_statuses = [canonical["status"], netflow["status"]]
        if support_mode == "snapshot-first":
            chain_status = "limited"
        elif all(status == "unsupported" for status in chain_statuses):
            chain_status = "unsupported"
        elif "stale" in chain_statuses:
            chain_status = "stale"
        elif "syncing" in chain_statuses or "catching_up" in chain_statuses or "missing" in chain_statuses:
            chain_status = "syncing"
        elif "unknown" in chain_statuses:
            chain_status = "unknown"
        else:
            chain_status = "verified"

        chains[chain] = {
            "label": policy["label"],
            "status": chain_status,
            "supportMode": support_mode,
            "refreshMode": chain_refresh_mode,
            "blockTimeSeconds": policy["blockTimeSeconds"],
            "canonical": canonical,
            "netflow": netflow,
            "snapshot": {
                "latestDate": latest_snapshot,
                "available": bool(latest_snapshot),
            },
        }
        chain_report.append(
            {
                "chain": chain,
                "label": policy["label"],
                "status": chain_status,
                "supportMode": support_mode,
                "refreshMode": chain_refresh_mode,
                "canonicalLagMinutes": canonical.get("estimatedLagMinutes"),
                "netflowLagMinutes": netflow.get("estimatedLagMinutes"),
                "lastRefreshAt": _latest_timestamp(canonical.get("updatedAt"), netflow.get("updatedAt")),
                "weakPoint": _weak_point(
                    support_mode=support_mode,
                    canonical=canonical,
                    netflow=netflow,
                    snapshot_available=bool(latest_snapshot),
                ),
            }
        )

    statuses = [
        payload["status"]
        for payload in chains.values()
        if payload["status"] not in {"unsupported", "limited"}
    ]
    if "stale" in statuses:
        overall = "stale"
    elif "syncing" in statuses:
        overall = "syncing"
    elif "unknown" in statuses:
        overall = "unknown"
    else:
        overall = "verified"

    refresh_modes = {payload["refreshMode"] for payload in chains.values()}

    return {
        "version": 1,
        "generatedAt": now.isoformat().replace("+00:00", "Z"),
        "policy": {
            "targetMinutes": 60,
            "refreshAfterMinutes": REFRESH_AFTER_MINUTES,
            "verifiedAfterHours": VERIFIED_AFTER_HOURS,
            "staleAfterHours": STALE_AFTER_HOURS,
        },
        "summary": {
            "status": overall,
            "refreshRecommended": bool(refresh_workflows),
            "backgroundRefreshRecommended": "background" in refresh_modes,
            "catchupRefreshRecommended": "catchup" in refresh_modes,
            "refreshWorkflows": sorted(refresh_workflows),
            "refreshJobs": sorted(
                refresh_jobs_by_workflow.values(),
                key=lambda job: (
                    int(job.get("priority", 50)),
                    job["workflow"],
                    sorted((job.get("inputs") or {}).items()),
                ),
            ),
            "refreshReasons": refresh_reasons,
            "limitedChains": sorted(
                chain for chain, payload in chains.items() if payload["status"] == "limited"
            ),
        },
        "chains": chains,
        "chainReport": chain_report,
    }


def write_actions_output(status: Dict[str, Any], path: Path) -> None:
    payload = {
        "refreshRecommended": bool(status.get("summary", {}).get("refreshRecommended")),
        "refreshWorkflows": status.get("summary", {}).get("refreshWorkflows") or [],
        "refreshJobs": status.get("summary", {}).get("refreshJobs") or [],
        "refreshReasons": status.get("summary", {}).get("refreshReasons") or [],
        "chainReport": status.get("chainReport") or [],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Update public EARN freshness status")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--actions-output", type=Path)
    args = parser.parse_args()

    status = build_status(data_dir=args.data_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.actions_output:
        write_actions_output(status, args.actions_output)
    print(json.dumps(status.get("summary", {}), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
