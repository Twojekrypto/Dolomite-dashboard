"""A successful snapshot refresh must publish its bot-generated commit."""
from pathlib import Path
import re
import unittest


class WlfiPagesWorkflowTests(unittest.TestCase):
    def test_completed_wlfi_workflow_triggers_pages_with_success_guard(self):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/pages.yml').read_text()
        trigger = re.search(r'  workflow_run:\n    workflows:\n(.*?)    types:\n(.*?)(?=\n\S)', workflow, re.S)
        self.assertIsNotNone(trigger, 'Pages must listen for bot data workflow completion')
        names = re.findall(r'^      - (.+)$', trigger.group(1), re.M)
        self.assertEqual(names.count('Update WLFI Markets'), 1)
        self.assertIn('      - completed', trigger.group(2))
        self.assertIn("github.event.workflow_run.conclusion == 'success'", workflow)


if __name__ == '__main__':
    unittest.main()
