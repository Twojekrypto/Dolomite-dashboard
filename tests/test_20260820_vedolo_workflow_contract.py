#!/usr/bin/env python3
"""Static contract tests for the block-aligned veDOLO pipeline."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
update_data_path = ROOT / ".github/workflows/update-data.yml"
flows_path = ROOT / ".github/workflows/update-vedolo-flows.yml"

assert update_data_path.exists(), f"missing {update_data_path}"
assert flows_path.exists(), f"missing {flows_path}"

update_data = update_data_path.read_text(encoding="utf-8")
flows = flows_path.read_text(encoding="utf-8")
validator = "validate_vedolo_locked_history.py"

assert "workflow_run:" in update_data
assert "workflows: ['Update veDOLO Flows']" in update_data
assert validator in update_data, (
    "Update veDOLO Data must reconcile holders against the flow snapshot block"
)
assert validator in flows, (
    "Update veDOLO Flows must retain fail-closed flow validation before publishing"
)
assert "--flows-only" in flows
assert "group: vedolo-data-pipeline" in update_data
assert "group: vedolo-data-pipeline" in flows
print("veDOLO workflow contract tests passed")
