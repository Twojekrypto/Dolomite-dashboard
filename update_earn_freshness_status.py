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

REFRESH_AFTER_MINUTES = 90
VERIFIED_AFTER_HOURS = 3
STALE_AFTER_HOURS = 4

CHAIN_POLICIES: Dict[str, Dict[str, Any]] = {
    "ethereum": {
        "label": "Ethereum",
        "blockTimeSeconds": 12.0,
        "verifiedBlockLag": 900,
        "canonicalWorkflow": "update-earn-ethereum-canonical-history.yml",
        "canonicalSupported": True,
    },
    "berachain": {
        "label": "Berachain",
        "blockTimeSeconds": 2.0,
        "verifiedBlockLag": 5400,
        "refreshAfterMinutes": 60,
        "canonicalWorkflow": "update-earn-berachain-canonical-history.yml",
        "netflowWorkflow": "update-earn-berachain-netflow.yml",
        "canonicalSupported": True,
    },
    "arbitrum": {
        "label": "Arbitrum",
        "blockTimeSeconds": 0.25,
        "verifiedBlockLag": 43200,
        "canonicalWorkflow": "update-earn-arbitrum-canonical-history.yml",
        "canonicalSupported": True,
    },
    "botanix": {
        "label": "Botanix",
        "blockTimeSeconds": 6.0,
        "verifiedBlockLag": 1800,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalWorkflowInputs": {"chain": "botanix"},
        "canonicalSupported": True,
    },
    "mantle": {
        "label": "Mantle",
        "blockTimeSeconds": 2.0,
        "verifiedBlockLag": 5400,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalWorkflowInputs": {"chain": "mantle"},
        "canonicalSupported": True,
    },
    "polygonzkevm": {
        "label": "Polygon zkEVM",
        "blockTimeSeconds": 3.2,
        "verifiedBlockLag": 3400,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalSupported": False,
    },
    "xlayer": {
        "label": "X Layer",
        "blockTimeSeconds": 1.0,
        "verifiedBlockLag": 10800,
        "canonicalWorkflow": "update-earn-secondary-canonical-history.yml",
        "canonicalSupported": False,
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
            "reason": f"{component} is not supported for {chain}",
        }

    block_lag = None
    if live_block is not None and last_block is not None:
        block_lag = int(live_block) - int(last_block)

    status = _status_for_lag(block_lag, policy)
    refresh_after_minutes = float(policy.get("refreshAfterMinutes", REFRESH_AFTER_MINUTES))
    refresh_after_blocks = _threshold_blocks(policy, refresh_after_minutes)
    refresh_recommended = status in {"missing", "syncing", "stale"} or (
        block_lag is not None and block_lag >= refresh_after_blocks
    )

    if live_block is None:
        status = "unknown"
        refresh_recommended = False

    return {
        "status": status,
        "lastBlock": last_block,
        "liveBlock": live_block,
        "blockLag": block_lag,
        "estimatedLagMinutes": _estimated_minutes(block_lag, policy),
        "updatedAt": updated_at or None,
        "updatedAgeMinutes": (
            round(value, 1) if (value := _age_minutes(updated_at, now)) is not None else None
        ),
        "refreshAfterMinutes": refresh_after_minutes,
        "refreshAfterBlockLag": refresh_after_blocks,
        "verifiedBlockLag": int(policy["verifiedBlockLag"]),
        "staleBlockLag": _threshold_blocks(policy, STALE_AFTER_HOURS * 60),
        "refreshRecommended": refresh_recommended,
    }


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
) -> None:
    normalized_inputs = {
        str(key): str(value)
        for key, value in (inputs or {}).items()
        if value is not None and str(value) != ""
    }
    existing = refresh_jobs_by_workflow.get(workflow)
    if existing is None:
        refresh_jobs_by_workflow[workflow] = {
            "workflow": workflow,
            "inputs": normalized_inputs,
        }
        return
    if existing.get("inputs") != normalized_inputs:
        merged_inputs: Dict[str, str] = {}
        if "chain" in (existing.get("inputs") or {}) or "chain" in normalized_inputs:
            merged_inputs["chain"] = "all"
        refresh_jobs_by_workflow[workflow] = {
            "workflow": workflow,
            "inputs": merged_inputs,
        }


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
        netflow_supported = (CHAINS.get(chain) or {}).get("start_block", 0) >= 0
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
            _register_refresh_job(
                refresh_jobs_by_workflow,
                workflow=workflow,
                inputs=policy.get("canonicalWorkflowInputs"),
            )
            refresh_reasons.append(
                f"{chain}: canonical {canonical['status']} ({_format_lag_reason_minutes(canonical.get('estimatedLagMinutes'))})"
            )
        if netflow.get("refreshRecommended") and netflow_supported:
            workflow = str(policy.get("netflowWorkflow") or NETFLOW_WORKFLOW)
            refresh_workflows.add(workflow)
            _register_refresh_job(refresh_jobs_by_workflow, workflow=workflow)
            refresh_reasons.append(
                f"{chain}: netflow {netflow['status']} ({_format_lag_reason_minutes(netflow.get('estimatedLagMinutes'))})"
            )

        chain_statuses = [canonical["status"], netflow["status"]]
        if all(status == "unsupported" for status in chain_statuses):
            chain_status = "unsupported"
        elif "stale" in chain_statuses:
            chain_status = "stale"
        elif "syncing" in chain_statuses or "missing" in chain_statuses:
            chain_status = "syncing"
        elif "unknown" in chain_statuses:
            chain_status = "unknown"
        else:
            chain_status = "verified"

        chains[chain] = {
            "label": policy["label"],
            "status": chain_status,
            "blockTimeSeconds": policy["blockTimeSeconds"],
            "canonical": canonical,
            "netflow": netflow,
            "snapshot": {
                "latestDate": latest_snapshot,
                "available": bool(latest_snapshot),
            },
        }

    statuses = [payload["status"] for payload in chains.values() if payload["status"] != "unsupported"]
    if "stale" in statuses:
        overall = "stale"
    elif "syncing" in statuses:
        overall = "syncing"
    elif "unknown" in statuses:
        overall = "unknown"
    else:
        overall = "verified"

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
            "refreshWorkflows": sorted(refresh_workflows),
            "refreshJobs": sorted(
                refresh_jobs_by_workflow.values(),
                key=lambda job: (job["workflow"], sorted((job.get("inputs") or {}).items())),
            ),
            "refreshReasons": refresh_reasons,
        },
        "chains": chains,
    }


def write_actions_output(status: Dict[str, Any], path: Path) -> None:
    payload = {
        "refreshRecommended": bool(status.get("summary", {}).get("refreshRecommended")),
        "refreshWorkflows": status.get("summary", {}).get("refreshWorkflows") or [],
        "refreshJobs": status.get("summary", {}).get("refreshJobs") or [],
        "refreshReasons": status.get("summary", {}).get("refreshReasons") or [],
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
