"""The metadata-only backstop dispatches one overdue guard, never a data scan."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
NOW = 1793872800
WORKFLOW = "dashboard-freshness.yml"
RUNS_PATH = "/actions/workflows/dashboard-freshness.yml/runs?branch=master&per_page=30"


def run(*, status="completed", conclusion="success", created=NOW - 3600, ended=NOW - 1800):
    return {"status": status, "conclusion": conclusion, "created_at": created, "updated_at": ended}


class DashboardFreshnessBackstopTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "dashboard_freshness_backstop", ROOT / "scripts/check_dashboard_freshness.py")
        self.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.m)
        self.config = self.m.load_config(ROOT / "config/dashboard_freshness.json")
        self.config["dashboardGuardMinimumIntervalMinutes"] = 15

    def invoke(self, payloads, *, no_remediation=False, dispatch_error=False):
        responses = iter(payloads)
        calls = []

        def requested(method, path, body=None):
            calls.append((method, path, body))
            if method == "GET":
                response = next(responses)
                if isinstance(response, Exception):
                    raise response
                return response
            if dispatch_error:
                raise self.m.MonitorError("github_api_failed")
            return {}

        output = io.StringIO()
        args = ["--ensure-check-due"] + (["--no-remediation"] if no_remediation else [])
        with patch.dict("os.environ", {"GH_TOKEN": "test-token"}, clear=True), \
                patch.object(self.m, "load_config", return_value=self.config), \
                patch.object(self.m.time, "time", return_value=NOW), \
                patch.object(self.m.GitHubAPI, "_request", side_effect=requested), \
                patch.object(self.m, "HttpClient", side_effect=AssertionError("backstop downloaded public data")), \
                patch("sys.stdout", output):
            code = self.m.main(args)
        return code, json.loads(output.getvalue()), calls

    def test_recent_success_or_failure_uses_completion_time_and_skips_the_scan(self):
        for conclusion in ("success", "failure"):
            with self.subTest(conclusion=conclusion):
                payload = {"workflow_runs": [run(conclusion=conclusion, created=NOW - 7200, ended=NOW - 60)]}
                code, report, calls = self.invoke([payload])
                self.assertEqual(code, 0)
                self.assertEqual(report["action"], "recent_check")
                self.assertEqual(calls, [("GET", RUNS_PATH, None)])
                self.assertNotIn("rows", report)
                self.assertNotIn("state", report)

    def test_overdue_or_first_check_is_rechecked_then_dispatched_exactly_once(self):
        for history in ([run()], []):
            with self.subTest(history=history):
                payload = {"workflow_runs": history}
                code, report, calls = self.invoke([payload, payload])
                self.assertEqual(code, 0)
                self.assertEqual(report["action"], "dispatched")
                self.assertEqual(calls, [("GET", RUNS_PATH, None), ("GET", RUNS_PATH, None),
                                         ("POST", "/actions/workflows/dashboard-freshness.yml/dispatches",
                                          {"ref": "master", "inputs": {}})])

    def test_interval_boundary_is_due_at_fifteen_minutes(self):
        for age, action in ((899, "recent_check"), (900, "dispatched")):
            with self.subTest(age=age):
                payload = {"workflow_runs": [run(ended=NOW - age)]}
                code, report, _ = self.invoke([payload, payload])
                self.assertEqual(code, 0)
                self.assertEqual(report["action"], action)

    def test_active_runs_block_a_duplicate_backstop_dispatch(self):
        for status in ("queued", "in_progress", "waiting", "pending", "requested"):
            with self.subTest(status=status):
                payload = {"workflow_runs": [run(status=status, conclusion=None, ended=NOW - 30)]}
                code, report, calls = self.invoke([payload])
                self.assertEqual(code, 0)
                self.assertEqual(report["action"], "active")
                self.assertEqual(calls, [("GET", RUNS_PATH, None)])

    def test_cancelled_and_skipped_runs_do_not_count_as_a_completed_check(self):
        payload = {"workflow_runs": [run(conclusion="cancelled", ended=NOW - 10),
                                      run(conclusion="skipped", ended=NOW - 20), run()]}
        code, report, calls = self.invoke([payload, payload])
        self.assertEqual(code, 0)
        self.assertEqual(report["action"], "dispatched")
        self.assertEqual(len(calls), 3)

    def test_second_check_catches_a_new_active_or_recent_run(self):
        old = {"workflow_runs": [run()]}
        for new, action in ((run(status="queued", conclusion=None, ended=NOW - 1), "active"),
                            (run(ended=NOW - 1), "recent_check")):
            with self.subTest(action=action):
                code, report, calls = self.invoke([old, {"workflow_runs": [new]}])
                self.assertEqual(code, 0)
                self.assertEqual(report["action"], action)
                self.assertEqual(calls, [("GET", RUNS_PATH, None)] * 2)

    def test_startup_failure_is_not_a_recent_data_check(self):
        payload = {"workflow_runs": [run(conclusion="startup_failure", ended=NOW - 10)]}
        code, report, calls = self.invoke([payload, payload])
        self.assertEqual(code, 0)
        self.assertEqual(report["action"], "dispatched")
        self.assertEqual([method for method, _path, _body in calls], ["GET", "GET", "POST"])

    def test_future_missing_or_inconsistent_metadata_never_causes_dispatch(self):
        missing_created = run()
        del missing_created["created_at"]
        missing_ended = run()
        del missing_ended["updated_at"]
        histories = [[run(created=NOW + 1)], [run(ended=NOW + 1)], [run(ended=NOW - 7200)],
                     [run(created="invalid")], [run(ended=True)], [missing_created], [missing_ended],
                     [run(status="unknown")], [run(status=[])], [run(conclusion="unknown")],
                     [run(conclusion={})], [None],
                     [run(conclusion="skipped", ended=NOW + 1)]]
        for history in histories:
            with self.subTest(history=history):
                code, report, calls = self.invoke([{"workflow_runs": history}])
                self.assertEqual(code, 1)
                self.assertEqual(report["action"], "run_metadata_invalid")
                self.assertEqual(calls, [("GET", RUNS_PATH, None)])

    def test_malformed_api_response_cannot_masquerade_as_empty_history(self):
        for payload in ({}, {"workflow_runs": None}, {"workflow_runs": {}}, []):
            with self.subTest(payload=payload):
                code, report, calls = self.invoke([payload])
                self.assertEqual(code, 1)
                self.assertEqual(report["action"], "run_metadata_invalid")
                self.assertEqual(calls, [("GET", RUNS_PATH, None)])

    def test_failed_metadata_recheck_cannot_dispatch(self):
        code, report, calls = self.invoke([
            {"workflow_runs": [run()]}, self.m.MonitorError("github_api_failed")])
        self.assertEqual(code, 1)
        self.assertEqual(report["action"], "metadata_failed")
        self.assertEqual(calls, [("GET", RUNS_PATH, None)] * 2)

    def test_ambiguous_dispatch_failure_is_reported_without_a_retry(self):
        payload = {"workflow_runs": [run()]}
        code, report, calls = self.invoke([payload, payload], dispatch_error=True)
        self.assertEqual(code, 1)
        self.assertEqual(report["action"], "dispatch_failed")
        self.assertEqual([method for method, _path, _body in calls], ["GET", "GET", "POST"])
        self.assertNotIn("test-token", json.dumps(report))

    def test_read_only_backstop_reports_due_without_dispatch(self):
        code, report, calls = self.invoke([{"workflow_runs": [run()]}], no_remediation=True)
        self.assertEqual(code, 0)
        self.assertEqual(report["action"], "dry_run")
        self.assertEqual(calls, [("GET", RUNS_PATH, None)])

    def test_interval_config_is_an_integer_within_five_to_sixty_minutes(self):
        for value in (4, 61, True, 15.5, "15", None):
            with self.subTest(value=value):
                config = copy.deepcopy(self.config)
                config["dashboardGuardMinimumIntervalMinutes"] = value
                with self.assertRaises(self.m.MonitorError):
                    self.m.validate_config(config)


if __name__ == "__main__":
    unittest.main()
