import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PAGES_WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"


class PagesWorkflowContractTests(unittest.TestCase):
    def test_pages_redeploys_after_holder_data_workflows(self):
        workflow = PAGES_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("workflow_run:", workflow)
        self.assertIn("Update DOLO Flows Data", workflow)
        self.assertIn("Update veDOLO Data", workflow)
        self.assertIn("Update veDOLO Flows", workflow)
        self.assertIn("Update TVL Data", workflow)
        self.assertIn("github.event.workflow_run.conclusion == 'success'", workflow)


if __name__ == "__main__":
    unittest.main()
