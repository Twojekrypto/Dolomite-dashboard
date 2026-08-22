import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PAGES_WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"
ODOLO_WORKFLOW = ROOT / ".github" / "workflows" / "update-odolo-data.yml"
DOLO_FLOWS_WORKFLOW = ROOT / ".github" / "workflows" / "update-dolo-flows.yml"


class PagesWorkflowContractTests(unittest.TestCase):
    def test_active_pages_deploy_is_not_cancelled_by_new_data_commits(self):
        workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("group: pages-deploy", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertIn("with:\n          ref: master", workflow)

    def test_pages_redeploys_after_holder_data_workflows(self):
        workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("workflow_run:", workflow)
        self.assertIn("Update DOLO Flows Data", workflow)
        self.assertIn("Update veDOLO Data", workflow)
        self.assertIn("Update veDOLO Flows", workflow)
        self.assertIn("Update TVL Data", workflow)
        self.assertIn("github.event.workflow_run.conclusion == 'success'", workflow)

    def test_pages_redeploys_after_exact_odolo_workflow(self):
        workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")

        self.assertEqual(workflow.count("      - Update oDOLO Data\n"), 1)

    def test_dolo_flows_workflow_validates_generated_artifact_before_commit(self):
        workflow = DOLO_FLOWS_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("tests/test_validate_dolo_flows.py", workflow)
        generated = workflow.index("python3 generate_dolo_flows.py")
        validated = workflow.index("python3 validate_data.py dolo_flows.json")
        committed = workflow.index("- name: Commit and push if changed")
        self.assertLess(generated, validated)
        self.assertLess(validated, committed)

    def test_dolo_flows_workflow_runs_protocol_balance_rpc_tests(self):
        workflow = DOLO_FLOWS_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("tests/test_generate_dolo_flows_rpc.py", workflow)

    def test_earn_pages_deploy_is_dispatched_only_after_freshness_sla(self):
        pages = PAGES_WORKFLOW.read_text(encoding="utf-8")
        monitor = (ROOT / ".github/workflows/monitor-earn-freshness.yml").read_text(encoding="utf-8")

        self.assertIn("allow_remediation:", monitor)
        self.assertIn("Enforce EARN freshness SLA", monitor)
        self.assertIn("Deploy verified EARN snapshot", monitor)
        self.assertIn("gh run list", monitor)
        self.assertIn("--workflow pages.yml", monitor)
        self.assertIn("--status queued", monitor)
        self.assertIn("gh workflow run pages.yml", monitor)
        self.assertLess(
            monitor.index("Enforce EARN freshness SLA"),
            monitor.index("Deploy verified EARN snapshot"),
        )
        self.assertNotIn("- Monitor EARN Freshness", pages)

    def test_odolo_exercise_data_still_publishes_when_independent_early_exit_refresh_fails(self):
        workflow = ODOLO_WORKFLOW.read_text(encoding="utf-8")
        early_exit_step = workflow.split("- name: Fetch early exit penalty data", 1)[1].split(
            "- name: Fetch oDOLO contract data", 1
        )[0]

        self.assertIn("continue-on-error: true", early_exit_step)
        self.assertIn("id: early-exits", early_exit_step)
        self.assertLess(workflow.index("- name: Fetch early exit penalty data"), workflow.index("- name: Validate generated data"))
        self.assertIn("early_exits.json", workflow)
        validation_step = workflow.split("- name: Validate generated data", 1)[1].split(
            "- name: Commit & push changes", 1
        )[0]
        self.assertIn("steps.early-exits.outcome", validation_step)
        self.assertIn('files+=(early_exits.json)', validation_step)


if __name__ == "__main__":
    unittest.main()
