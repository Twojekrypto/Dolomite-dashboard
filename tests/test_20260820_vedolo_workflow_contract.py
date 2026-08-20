#!/usr/bin/env python3
"""Static contract tests for the independently scheduled veDOLO pipelines."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
update_data_path = ROOT / ".github/workflows/update-data.yml"
flows_path = ROOT / ".github/workflows/update-vedolo-flows.yml"

assert update_data_path.exists(), f"missing {update_data_path}"
assert flows_path.exists(), f"missing {flows_path}"

update_data = update_data_path.read_text(encoding="utf-8")
flows = flows_path.read_text(encoding="utf-8")
validator = "validate_vedolo_locked_history.py"

assert validator not in update_data, (
    "Update veDOLO Data must not reconcile a fresh holder snapshot against a stale flows snapshot"
)
assert validator in flows, (
    "Update veDOLO Flows must retain fail-closed reconciliation after refreshing flow history"
)
print("veDOLO workflow contract tests passed")
