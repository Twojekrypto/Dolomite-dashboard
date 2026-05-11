#!/usr/bin/env python3
"""Build a stable TSV dispatch plan for the EARN freshness watchdog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ALL_CHAINS_SENTINEL = "__all__"
DEFAULT_PRIORITY = 50
DEFAULT_MODE = "catchup"


def _clean_tsv_field(value: Any) -> str:
    return str(value or "").strip().replace("\t", " ").replace("\n", " ")


def _safe_priority(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return DEFAULT_PRIORITY


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
            ]
        )
        for row in rows
    ]
    output_path.write_text(("\n".join(lines) + "\n") if lines else "", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build EARN watchdog dispatch TSV")
    parser.add_argument("actions_output", type=Path)
    parser.add_argument("dispatch_tsv", type=Path)
    args = parser.parse_args()

    payload = json.loads(args.actions_output.read_text(encoding="utf-8"))
    write_dispatch_tsv(build_dispatch_rows(payload), args.dispatch_tsv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
