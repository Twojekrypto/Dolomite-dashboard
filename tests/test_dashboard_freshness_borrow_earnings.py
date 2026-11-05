"""Keep the borrowing ranking fresh before its 90-minute rate expiry."""
import copy
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
NOW = 1793908800
CHAINS = ('ethereum', 'arbitrum', 'berachain')


class BorrowEarningsFreshnessTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            'ranking_freshness', ROOT / 'scripts/check_dashboard_freshness.py')
        self.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.m)
        self.config = self.m.load_config(ROOT / 'config/dashboard_freshness.json')
        self.rule = next((r for r in self.config['artifacts']
                          if r['id'] == 'borrow-earnings'), None)
        self.assertIsNotNone(self.rule, 'The ranking itself must be monitored')
        self.payload = {'generatedAt': NOW, 'sources': {
            'positionsGeneratedAt': NOW,
            'rates': {c: {'fetchedAt': NOW} for c in CHAINS}},
            'chainStatus': {c: {'status': 'ok'} for c in CHAINS}}

    def test_one_hour_old_ranking_requests_refresh_before_ui_expiry(self):
        payload = copy.deepcopy(self.payload)
        payload['generatedAt'] = NOW - 60 * 60
        payload['sources']['positionsGeneratedAt'] = NOW - 60 * 60
        for source in payload['sources']['rates'].values():
            source['fetchedAt'] = NOW - 60 * 60
        result = self.m.assess(payload, self.rule, NOW)
        self.assertEqual(result['state'], 'early_due')
        row = self.m.compare(self.rule, result, result, self.config)
        self.assertEqual(row['workflow'], 'update-liquidation-risk.yml')
        self.assertGreater(self.rule['maxBytes'], 14_055_375)

    def test_fresh_wrapper_cannot_hide_expired_rates_on_any_live_chain(self):
        for chain in CHAINS:
            with self.subTest(chain=chain):
                payload = copy.deepcopy(self.payload)
                payload['sources']['rates'][chain]['fetchedAt'] = NOW - 91 * 60
                result = self.m.assess(payload, self.rule, NOW)
                self.assertEqual(result['state'], 'source_stale')
                self.assertEqual(result['source_age_minutes'], 91)
                self.assertEqual(self.m.compare(self.rule, result, result,
                    self.config)['workflow'], 'update-liquidation-risk.yml')

    def test_expired_ranking_positions_cannot_hide_behind_fresh_rates(self):
        self.payload['sources']['positionsGeneratedAt'] = NOW - 181 * 60
        self.assertEqual(self.m.assess(self.payload, self.rule, NOW)['state'], 'source_stale')

    def test_explicitly_unavailable_chain_does_not_hide_healthy_chains(self):
        self.payload['chainStatus']['arbitrum'] = {'status': 'unavailable', 'reason': 'Rates unavailable'}
        del self.payload['sources']['rates']['arbitrum']
        self.assertEqual(self.m.assess(self.payload, self.rule, NOW)['state'], 'fresh')
        self.payload['sources']['rates']['ethereum']['fetchedAt'] = NOW - 91 * 60
        self.assertEqual(self.m.assess(self.payload, self.rule, NOW)['state'], 'source_stale')

    def test_missing_live_rate_source_is_invalid(self):
        del self.payload['sources']['rates']['arbitrum']
        self.assertEqual(self.m.assess(self.payload, self.rule, NOW)['state'], 'invalid')

    def test_future_rate_source_is_invalid(self):
        self.payload['sources']['rates']['berachain']['fetchedAt'] = NOW + 60
        self.assertEqual(self.m.assess(self.payload, self.rule, NOW)['state'], 'invalid')

    def test_fresh_repo_after_recovery_repairs_publication_only(self):
        old = copy.deepcopy(self.payload)
        old['sources']['rates']['arbitrum']['fetchedAt'] = NOW - 91 * 60
        public = self.m.assess(old, self.rule, NOW)
        repo = self.m.assess(self.payload, self.rule, NOW)
        row = self.m.compare(self.rule, public, repo, self.config)
        self.assertEqual(row['problem'], 'deployment_behind')
        self.assertEqual(row['workflow'], 'pages.yml')

    def test_safety_budget_allows_refresh_before_next_rate_expiry(self):
        # Two recent successful refreshes must leave room for the next 45-minute
        # refresh even if every cron event was omitted. Three stays bounded.
        policy = self.config['workflows']['update-liquidation-risk.yml']
        runs = [{'status': 'completed', 'conclusion': 'success',
                 'event': 'workflow_dispatch', 'created_at': NOW - age * 60,
                 'updated_at': NOW - age * 60} for age in (105, 60)]
        self.assertEqual(self.m.dispatch_gate(runs, policy, NOW), 'ready')
        runs.append({'status': 'completed', 'conclusion': 'success',
                     'event': 'workflow_dispatch', 'created_at': NOW - 15 * 60,
                     'updated_at': NOW - 15 * 60})
        self.assertEqual(self.m.dispatch_gate(runs, policy, NOW), 'retry_budget')

    def test_active_refresh_is_not_dispatched_again(self):
        old = copy.deepcopy(self.payload)
        old['sources']['rates']['arbitrum']['fetchedAt'] = NOW - 91 * 60
        result = self.m.assess(old, self.rule, NOW)
        row = self.m.compare(self.rule, result, result, self.config)
        class API:
            def runs(self, workflow):
                return [{'status': 'in_progress'}]
            def dispatch(self, workflow, inputs):
                raise AssertionError('An active producer must not be duplicated')
        decisions = self.m.remediate([row], self.config, API(), NOW, True)
        self.assertEqual(decisions[0]['action'], 'active')


if __name__ == '__main__':
    unittest.main()
