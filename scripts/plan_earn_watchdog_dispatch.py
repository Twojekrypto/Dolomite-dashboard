#!/usr/bin/env python3
"""Build a stable TSV dispatch plan for the EARN freshness watchdog."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List


ALL_CHAINS_SENTINEL = "__all__"
DEFAULT_PRIORITY = 50
DEFAULT_MODE = "catchup"
CHAIN_LABEL_PATTERN = re.compile(r"\[([^\]]+)\]")
DEDICATED_WORKFLOW_CHAINS = {
    "update-earn-arbitrum-canonical-history.yml": "arbitrum",
    "update-earn-berachain-canonical-history.yml": "berachain",
    "update-earn-ethereum-canonical-history.yml": "ethereum",
}


def _clean_tsv_field(value: Any) -> str:
    return str(value or "").strip().replace("\t", " ").replace("\n", " ")


def _safe_priority(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return DEFAULT_PRIORITY


def run_covers_requested_chain(run: Dict[str, Any], requested_chain: str) -> bool:
    chain = str(requested_chain or "").strip().lower()
    if not chain:
        return True
    title = " ".join(
        str(run.get(key) or "").strip().lower()
        for key in ("displayTitle", "name")
        if str(run.get(key) or "").strip()
    )
    labels = {
        label.strip().lower()
        for label in CHAIN_LABEL_PATTERN.findall(title)
        if label.strip()
    }
    return chain in labels or "all" in labels


def _repair_chain_for_row(row: Dict[str, Any]) -> str:
    chain = _clean_tsv_field(row.get("chain")).lower()
    if chain and chain != ALL_CHAINS_SENTINEL:
        return chain
    workflow = _clean_tsv_field(row.get("workflow"))
    return DEDICATED_WORKFLOW_CHAINS.get(workflow, ALL_CHAINS_SENTINEL)


def collect_active_repair_chains(
    rows: Iterable[Dict[str, Any]],
    run_loader: Callable[[str], Iterable[Dict[str, Any]]],
) -> set[str]:
    active_chains: set[str] = set()
    for row in rows:
        workflow = _clean_tsv_field(row.get("workflow"))
        if not workflow:
            continue
        chain = _clean_tsv_field(row.get("chain")).lower()
        requested_chain = "" if chain == ALL_CHAINS_SENTINEL else chain
        if any(
            run_covers_requested_chain(run, requested_chain)
            for run in run_loader(workflow)
        ):
            active_chains.add(_repair_chain_for_row(row))
    return active_chains


def _load_active_runs(repo: str, workflow: str) -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    for status in ("queued", "in_progress"):
        output = subprocess.check_output(
            [
                "gh",
                "run",
                "list",
                "--repo",
                repo,
                "--workflow",
                workflow,
                "--status",
                status,
                "--limit",
                "50",
                "--json",
                "databaseId,displayTitle,name,status",
            ],
            text=True,
        )
        payload = json.loads(output or "[]")
        if isinstance(payload, list):
            runs.extend(run for run in payload if isinstance(run, dict))
    return runs


def write_active_repair_chains(chains: Iterable[str], output_path: Path) -> None:
    normalized = sorted(
        {
            str(chain or "").strip().lower()
            for chain in chains
            if str(chain or "").strip()
        }
    )
    output_path.write_text(
        ("\n".join(normalized) + "\n") if normalized else "",
        encoding="utf-8",
    )


def _raw_jobs(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    jobs = payload.get("refreshJobs")
    if isinstance(jobs, list) and jobs:
        return [job for job in jobs if isinstance(job, dict)]
    return [
        {
            "workflow": workflow,
            "inputs": {},
            "priority": DEFAULT_PRIORITY,
            "mode": DEFAULT_MODE,
        }
        for workflow in payload.get("refreshWorkflows") or []
    ]


def build_dispatch_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for job in _raw_jobs(payload):
        workflow = _clean_tsv_field(job.get("workflow"))
        if not workflow:
            continue
        inputs = job.get("inputs") if isinstance(job.get("inputs"), dict) else {}
        chain = _clean_tsv_field(inputs.get("chain"))
        rows.append(
            {
                "workflow": workflow,
                "chain": chain or ALL_CHAINS_SENTINEL,
                "priority": _safe_priority(job.get("priority", DEFAULT_PRIORITY)),
                "mode": _clean_tsv_field(job.get("mode")) or DEFAULT_MODE,
                "inputs": {
                    _clean_tsv_field(key): _clean_tsv_field(value)
                    for key, value in inputs.items()
                    if _clean_tsv_field(key)
                    and _clean_tsv_field(key) != "chain"
                    and _clean_tsv_field(value)
                },
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            int(row["priority"]),
            str(row["workflow"]),
            "" if row["chain"] == ALL_CHAINS_SENTINEL else str(row["chain"]),
        ),
    )


def write_dispatch_tsv(rows: Iterable[Dict[str, Any]], output_path: Path) -> None:
    lines = [
        "\t".join(
            [
                str(row["workflow"]),
                str(row["chain"]),
                str(row["priority"]),
                str(row["mode"]),
                json.dumps(row.get("inputs") or {}, sort_keys=True, separators=(",", ":")),
            ]
        )
        for row in rows
    ]
    output_path.write_text(("\n".join(lines) + "\n") if lines else "", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build EARN watchdog dispatch TSV")
    parser.add_argument("actions_output", type=Path)
    parser.add_argument("dispatch_tsv", type=Path)
    parser.add_argument("--active-repairs", type=Path)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    args = parser.parse_args()

    payload = json.loads(args.actions_output.read_text(encoding="utf-8"))
    rows = build_dispatch_rows(payload)
    write_dispatch_tsv(rows, args.dispatch_tsv)
    if args.active_repairs:
        if not args.repo:
            parser.error("--repo or GITHUB_REPOSITORY is required with --active-repairs")
        chains = collect_active_repair_chains(
            rows,
            lambda workflow: _load_active_runs(args.repo, workflow),
        )
        write_active_repair_chains(chains, args.active_repairs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
