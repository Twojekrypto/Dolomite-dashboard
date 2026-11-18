"""Execute the publication shell with local git/sleep stubs; no network or writes."""
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


class WlfiWorkflowTests(unittest.TestCase):
    def run_publication(self, *, fail_push):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/update-wlfi-markets.yml').read_text()
        block = workflow.split('      - name: Commit and push verified snapshot\n        run: |\n', 1)[1]
        shell = textwrap.dedent(block)
        with tempfile.TemporaryDirectory() as root:
            stub = '''git() {
              printf '%s\\n' "$*" >> git-calls
              case "$1" in
                diff) return 1 ;;
                push) return PUSH_RESULT ;;
              esac
              return 0
            }
            sleep() { :; }
            '''.replace('PUSH_RESULT', '1' if fail_push else '0')
            process = subprocess.run(['bash', '-e', '-c', stub + shell], cwd=root,
                                     capture_output=True, text=True)
            return process, (Path(root) / 'git-calls').read_text().splitlines()

    def test_stages_only_new_snapshot_and_pushes_master(self):
        process, calls = self.run_publication(fail_push=False)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual([c for c in calls if c.startswith('add ')], ['add data/wlfi-markets/latest.json'])
        self.assertEqual([c for c in calls if c.startswith('push ')], ['push origin HEAD:master'])

    def test_exhausted_publication_is_bounded_and_fails_the_workflow(self):
        process, calls = self.run_publication(fail_push=True)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(len([c for c in calls if c.startswith('push ')]), 12)
        self.assertIn('snapshot not published', process.stdout)


if __name__ == '__main__':
    unittest.main()
