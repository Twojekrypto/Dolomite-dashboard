"""Publication retries must stay scoped to idempotent Pages requests."""
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from contextlib import redirect_stdout

ROOT = Path(__file__).resolve().parents[1]
COMMIT = 'a' * 40
NEWER = 'b' * 40
OLDER = 'c' * 40
REPO = 'Twojekrypto/Dolomite-dashboard'


def reply(code=0, data=None, error=''):
    return subprocess.CompletedProcess([], code, json.dumps(data) if data is not None else '', error)


def run_row(sha=COMMIT, status='queued', conclusion=None):
    return {'id': 123, 'head_sha': sha, 'head_branch': 'master',
            'status': status, 'conclusion': conclusion}


class PagesDispatchTests(unittest.TestCase):
    def setUp(self):
        path = ROOT / 'scripts/dispatch_pages.py'
        self.assertTrue(path.exists(), 'Pages publication needs bounded recovery')
        spec = importlib.util.spec_from_file_location('pages_dispatch', path)
        self.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.m)
        self.calls = []
        self.delays = []

    def invoke(self, responses, repo=REPO, commit=COMMIT):
        remaining = list(responses)
        def runner(args, **kwargs):
            self.calls.append(args)
            if not remaining:
                raise AssertionError('Unexpected external request')
            response = remaining.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        output = io.StringIO()
        with redirect_stdout(output):
            code = self.m.main(['--repo', repo, '--commit', commit],
                               runner=runner, sleep=self.delays.append)
        return code, [json.loads(line) for line in output.getvalue().splitlines()]

    def test_success_needs_one_publication_request(self):
        code, report = self.invoke([reply()])
        self.assertEqual(code, 0)
        self.assertEqual(report[-1]['action'], 'dispatched')
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0], ['gh', 'workflow', 'run', 'pages.yml',
                                       '--repo', REPO, '--ref', 'master'])

    def test_500_without_accepted_run_retries_only_pages(self):
        code, report = self.invoke([reply(1, error='HTTP 500'),
                                   reply(data={'workflow_runs': []}), reply()])
        self.assertEqual(code, 0)
        self.assertEqual(report[-1]['attempts'], 2)
        self.assertEqual([c[1] for c in self.calls], ['workflow', 'api', 'workflow'])
        self.assertEqual(self.delays, [2])
        self.assertTrue(all('pages.yml' in ' '.join(c) for c in self.calls))

    def test_ambiguous_error_recognizes_accepted_run_without_second_post(self):
        code, report = self.invoke([reply(1, error='HTTP 502'),
                                   reply(data={'workflow_runs': [run_row()]})])
        self.assertEqual(code, 0)
        self.assertEqual(report[-1]['action'], 'already_requested')
        self.assertEqual(report[-1]['runId'], 123)
        self.assertEqual(len(self.calls), 2)

    def test_newer_run_must_contain_the_published_commit(self):
        code, report = self.invoke([reply(1, error='HTTP 503'),
            reply(data={'workflow_runs': [run_row(NEWER)]}),
            reply(data={'status': 'ahead', 'merge_base_commit': {'sha': COMMIT}})])
        self.assertEqual(code, 0)
        self.assertEqual(report[-1]['action'], 'already_requested')
        self.assertIn('/compare/' + COMMIT + '...' + NEWER, self.calls[-1][-1])

    def test_an_older_active_run_does_not_claim_to_publish_new_data(self):
        code, report = self.invoke([reply(1, error='HTTP 500'),
            reply(data={'workflow_runs': [run_row(OLDER, 'in_progress')]}),
            reply(data={'status': 'behind', 'merge_base_commit': {'sha': OLDER}}), reply()])
        self.assertEqual(code, 0)
        self.assertEqual(report[-1]['action'], 'dispatched')
        self.assertEqual(report[-1]['attempts'], 2)

    def test_cancelled_or_failed_runs_do_not_count_as_publication(self):
        for conclusion in ('cancelled', 'failure', 'skipped'):
            with self.subTest(conclusion=conclusion):
                code, report = self.invoke([reply(1, error='HTTP 500'),
                    reply(data={'workflow_runs': [run_row(status='completed', conclusion=conclusion)]}), reply()])
                self.assertEqual(code, 0)
                self.assertEqual(report[-1]['action'], 'dispatched')

    def test_completed_success_can_confirm_publication(self):
        code, report = self.invoke([reply(1, error='HTTP 500'), reply(data={
            'workflow_runs': [run_row(status='completed', conclusion='success')]})])
        self.assertEqual(code, 0)
        self.assertEqual(report[-1]['action'], 'already_requested')

    def test_permanent_errors_are_not_retried_or_hidden(self):
        for status in (400, 401, 403, 404, 422):
            with self.subTest(status=status):
                self.calls.clear()
                code, report = self.invoke([reply(1, error='HTTP ' + str(status))])
                self.assertEqual(code, 1)
                self.assertEqual(report[-1]['action'], 'failed')
                self.assertEqual(len(self.calls), 1)

    def test_persistent_500_exhausts_three_attempts_and_remains_failed(self):
        responses = [x for _ in range(3) for x in
                     (reply(1, error='HTTP 500'), reply(data={'workflow_runs': []}))]
        code, report = self.invoke(responses)
        self.assertEqual(code, 1)
        self.assertEqual(report[-1]['reason'], 'retry_exhausted')
        self.assertEqual(sum(c[1] == 'workflow' for c in self.calls), 3)
        self.assertEqual(self.delays, [2, 4, 6])

    def test_unavailable_reconciliation_does_not_trigger_blind_retry(self):
        code, report = self.invoke([reply(1, error='HTTP 500'), reply(1, error='HTTP 503')])
        self.assertEqual(code, 1)
        self.assertEqual(report[-1]['reason'], 'metadata_unavailable')
        self.assertEqual(len(self.calls), 2)

    def test_timeout_is_reconciled_before_another_publication_request(self):
        code, report = self.invoke([subprocess.TimeoutExpired('gh', 30),
                                   reply(data={'workflow_runs': [run_row()]})])
        self.assertEqual(code, 0)
        self.assertEqual(report[-1]['action'], 'already_requested')
        self.assertEqual(len(self.calls), 2)

    def test_invalid_run_metadata_fails_closed(self):
        for payload in ({}, {'workflow_runs': None}, {'workflow_runs': 'bad'}):
            with self.subTest(payload=payload):
                code, report = self.invoke([reply(1, error='HTTP 500'), reply(data=payload)])
                self.assertEqual(code, 1)
                self.assertEqual(report[-1]['reason'], 'metadata_invalid')

    def test_logs_do_not_echo_untrusted_error_text_or_secrets(self):
        secret = 'private-api-value'
        code, report = self.invoke([reply(1, error='HTTP 500 https://example.test/' + secret),
                                   reply(data={'workflow_runs': []}), reply()])
        self.assertEqual(code, 0)
        self.assertNotIn(secret, json.dumps(report))

    def test_malformed_commit_comparison_is_not_an_unhandled_exception(self):
        code, report = self.invoke([reply(1, error='HTTP 500'),
            reply(data={'workflow_runs': [run_row(NEWER)]}),
            reply(data={'status': 'ahead', 'merge_base_commit': None})])
        self.assertEqual(code, 1)
        self.assertEqual(report[-1]['reason'], 'metadata_invalid')

    def test_other_transient_http_statuses_use_bounded_recovery(self):
        for status in (408, 429, 502, 503, 504):
            with self.subTest(status=status):
                code, report = self.invoke([reply(1, error='HTTP ' + str(status)),
                    reply(data={'workflow_runs': []}), reply()])
                self.assertEqual(code, 0)
                self.assertEqual(report[-1]['attempts'], 2)

    def test_incomplete_run_records_stop_recovery_before_another_post(self):
        for missing in ('head_branch', 'status', 'head_sha', 'id'):
            with self.subTest(missing=missing):
                run = run_row()
                del run[missing]
                code, report = self.invoke([reply(1, error='HTTP 500'),
                    reply(data={'workflow_runs': [run]})])
                self.assertEqual(code, 1)
                self.assertEqual(report[-1]['reason'], 'metadata_invalid')

    def test_incomplete_comparison_stops_recovery_before_another_post(self):
        for comparison in ({'status': 'ahead', 'merge_base_commit': {}},
                           {'status': 'unknown', 'merge_base_commit': {'sha': COMMIT}}):
            with self.subTest(comparison=comparison):
                code, report = self.invoke([reply(1, error='HTTP 500'),
                    reply(data={'workflow_runs': [run_row(NEWER)]}), reply(data=comparison)])
                self.assertEqual(code, 1)
                self.assertEqual(report[-1]['reason'], 'metadata_invalid')

    def test_bad_target_is_rejected_before_external_calls(self):
        for repo, commit in [('bad repo', COMMIT), (REPO, 'master'), (REPO, 'a' * 39)]:
            with self.subTest(repo=repo, commit=commit):
                code, report = self.invoke([], repo=repo, commit=commit)
                self.assertEqual(code, 1)
                self.assertEqual(report[-1]['reason'], 'invalid_target')
                self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()
