"""Pages must explicitly invoke the bounded guard when cron/events are absent."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class PagesFreshnessBackstopTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / '.github/workflows/pages.yml').read_text()
        self.build, self.deploy = self.workflow.split('  deploy:\n', 1)

    def test_successful_pages_publication_directly_backstops_an_overdue_guard(self):
        # A workflow_run event is not guaranteed for nested publications. The
        # deployment itself must call the existing race-checked metadata gate.
        marker = '      - name: Backstop dashboard freshness cadence\n'
        self.assertIn(marker, self.deploy)
        backstop = self.deploy.split(marker, 1)[1].split('\n      - name:', 1)[0]
        self.assertIn("if: ${{ success() }}", backstop)
        self.assertIn('continue-on-error: true', backstop)
        self.assertIn('GH_TOKEN: ${{ github.token }}', backstop)
        self.assertIn('GITHUB_REPOSITORY: ${{ github.repository }}', backstop)
        self.assertIn('run: python3 scripts/check_dashboard_freshness.py --config config/dashboard_freshness.json --ensure-check-due', backstop)
        self.assertNotIn('--fail-on-stale', backstop)
        self.assertNotIn('gh workflow run', backstop)
        self.assertLess(self.deploy.index('Smoke test live Pages'), self.deploy.index(marker))

    def test_deploy_job_has_only_the_needed_dispatch_and_checkout_dependencies(self):
        self.assertIn('    permissions:\n      contents: read\n      pages: write\n      id-token: write\n      actions: write', self.deploy)
        self.assertNotIn('actions: write', self.build)
        checkout = self.deploy.split('- name: Checkout smoke scripts', 1)[1].split('- name: Smoke test live Pages', 1)[0]
        for path in ('scripts/smoke_live_pages.py', 'scripts/check_dashboard_freshness.py', 'config/dashboard_freshness.json'):
            self.assertIn('            ' + path + '\n', checkout)
        self.assertIn('sparse-checkout-cone-mode: false', checkout)


if __name__ == '__main__':
    unittest.main()
