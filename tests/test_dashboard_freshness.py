"""Behavior contracts for the bounded, public-data freshness monitor."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
NOW = 1789646400  # 2026-09-17T12:00:00Z


class DashboardFreshnessTests(unittest.TestCase):
    def test_publication_during_a_slow_probe_is_not_a_future_timestamp(self):
        config = dict(self.config, artifacts=[self.asset])
        output = io.StringIO()
        with patch.dict('os.environ', {}, clear=True), \
                patch.object(self.m, 'load_config', return_value=config), \
                patch.object(self.m.time, 'time', return_value=NOW) as clock, \
                patch('sys.stdout', output):
            def fetched(*_args):
                # A producer publishes while the monitor waits for its download.
                clock.return_value = NOW + 60
                return {'generatedAt': NOW + 60}
            with patch.object(self.m.HttpClient, 'get_json', side_effect=fetched):
                self.assertEqual(self.m.main(['--no-remediation', '--fail-on-stale']), 0)
        row = json.loads(output.getvalue())['rows'][0]
        self.assertEqual(row['public']['state'], 'fresh')
        self.assertEqual(row['repo']['state'], 'fresh')

    def test_merkl_fallback_cache_has_an_age_limit_and_automatic_repair(self):
        rule = next(a for a in self.config['artifacts'] if a['id'] == 'earn-merkl-rewards')
        stale = self.m.assess({'generatedAt': NOW - 9 * 3600}, rule, NOW)
        self.assertEqual(stale['state'], 'stale')
        self.assertEqual(self.m.compare(rule, stale, stale, self.config)['workflow'],
                         'update-earn-merkl-rewards.yml')
        self.assertLessEqual(rule['maxAgeMinutes'], 120)

    def test_merkl_workflow_has_headroom_for_the_observed_full_cohort(self):
        import math
        import re
        workflow = (ROOT / '.github/workflows/update-earn-merkl-rewards.yml').read_text()
        limit = int(re.search(r'timeout-minutes:\s*(\d+)', workflow).group(1))
        # 1,674 sequential lookups, checkout, audit and push, with 2x headroom.
        required = math.ceil(2 * (1674 * 0.6 + 480 + 120 + 120) / 60)
        self.assertGreaterEqual(limit, required)
        self.assertIn('cancel-in-progress: false', workflow)

    def test_public_transient_503_is_retried_before_assessment(self):
        client = self.m.HttpClient(self.config)
        response = io.BytesIO(json.dumps({'generatedAt': NOW}).encode())
        response.headers = {}
        url = self.config['public_base_url'] + 'assets_live.json'
        with patch.object(client.opener, 'open', side_effect=[HTTPError(url, 503, 'unavailable', {}, None), response]) as opened, \
                patch.object(self.m.time, 'sleep'):
            self.assertEqual(client.get_json(url, 1024), {'generatedAt': NOW})
            self.assertEqual(opened.call_count, 2)

    def test_retry_exhaustion_still_reports_unavailable(self):
        client = self.m.HttpClient(self.config)
        with patch.object(client.opener, 'open', side_effect=URLError('offline')) as opened, \
                patch.object(self.m.time, 'sleep'):
            with self.assertRaisesRegex(self.m.MonitorError, 'request_failed'):
                client.get_json(self.config['public_base_url'] + 'assets_live.json', 1024)
            self.assertEqual(opened.call_count, 3)

    def test_deterministic_http_failure_and_invalid_json_are_not_retried(self):
        url = self.config['public_base_url'] + 'assets_live.json'
        response = io.BytesIO(b'invalid-json')
        response.headers = {}
        for error in (HTTPError(url, 404, 'missing', {}, None), response):
            client = self.m.HttpClient(self.config)
            with patch.object(client.opener, 'open', side_effect=error if isinstance(error, Exception) else [error]) as opened, \
                    patch.object(self.m.time, 'sleep'):
                with self.assertRaises(self.m.MonitorError): client.get_json(url, 1024)
                self.assertEqual(opened.call_count, 1)

    def test_flows_refresh_leaves_time_for_generation_before_eight_hour_deadline(self):
        for name in ("dolo-flows", "holder-wallet-history"):
            rule = next(a for a in self.config["artifacts"] if a["id"] == name)
            result = self.m.assess({"timestamp": NOW - 3 * 3600}, rule, NOW)
            self.assertEqual(result["state"], "early_due")
            self.assertEqual(rule["maxAgeMinutes"], 480)

    def test_old_flows_are_not_starved_by_short_cadence_refreshes(self):
        names = ["update-assets-live.yml", "update-dolo-price.yml", "update-tvl-data.yml", "update-dolo-flows.yml"]
        for flow_age in (180, 600):
            rows = [dict(workflow=name, id=name, public={"state": "early_due", "age_minutes": age})
                    for name, age in zip(names, (12, 50, 25, flow_age))]
            class API:
                def __init__(self): self.sent = []
                def runs(self, workflow): return []
                def dispatch(self, workflow, inputs): self.sent.append(workflow)
            api = API()
            self.m.remediate(rows, dict(self.config, max_dispatches_per_run=1), api, NOW, True)
            self.assertEqual(api.sent, ["update-dolo-flows.yml"])

    def test_fresh_earn_wrapper_does_not_hide_source_lag(self):
        rule = dict(self.asset, sourceLagMinutes=["chains.xlayer.netflow.estimatedLagMinutes"], sourceMaxAgeMinutes=480)
        result = self.m.assess({"generatedAt": NOW, "chains": {"xlayer": {"netflow": {"estimatedLagMinutes": 900}}}}, rule, NOW)
        self.assertEqual(result["state"], "source_stale")
        self.assertEqual(result["source_age_minutes"], 900)

    def test_missing_source_lag_is_not_zero(self):
        rule = dict(self.asset, sourceLagMinutes=["chains.ethereum.canonical.estimatedLagMinutes"])
        self.assertEqual(self.m.assess({"generatedAt": NOW}, rule, NOW)["state"], "invalid")

    def test_source_sla_is_distinct_from_status_wrapper_sla(self):
        rule = dict(self.asset, sourceTimestamps=["head"], sourceMaxAgeMinutes=480)
        self.assertEqual(self.m.assess({"generatedAt": NOW, "head": NOW - 2*3600}, rule, NOW)["state"], "fresh")

    def test_r2_production_does_not_refresh_frozen_git_baseline(self):
        rule = dict(self.asset, productionStorage="r2")
        fresh = {"state": "fresh", "age_minutes": 10}
        stale = {"state": "stale", "age_minutes": 20000}
        self.assertIsNone(self.m.compare(rule, fresh, stale, self.config)["workflow"])
        self.assertEqual(self.m.compare(rule, stale, fresh, self.config)["workflow"], rule["workflow"])

    def test_source_stale_dispatches_only_explicitly_approved_source_repair(self):
        stale = {"state": "source_stale", "age_minutes": 900}
        rule = dict(self.asset, remediateSourceStale=True)
        self.assertEqual(self.m.compare(rule, stale, stale, self.config)["workflow"], rule["workflow"])
        self.assertIsNone(self.m.compare(self.asset, stale, stale, self.config)["workflow"])

    def test_stale_public_source_with_fresh_repo_repairs_publication_not_scanner(self):
        rule = dict(self.asset, sourceTimestamps=["head"], remediateSourceStale=True)
        public = self.m.assess({"generatedAt": NOW - 25*60, "head": NOW - 532*60}, rule, NOW)
        repo = self.m.assess({"generatedAt": NOW - 5*60, "head": NOW - 6*60}, rule, NOW)
        row = self.m.compare(rule, public, repo, self.config)
        self.assertEqual(row["problem"], "deployment_behind")
        self.assertEqual(row["workflow"], "pages.yml")
        self.assertEqual(row["public"]["state"], "source_stale")

    def test_stale_r2_source_must_not_publish_git_recovery_baseline(self):
        rule = dict(self.asset, productionStorage="r2", remediateSourceStale=True)
        row = self.m.compare(rule, {"state": "source_stale", "age_minutes": 532},
                             {"state": "fresh", "age_minutes": 6}, self.config)
        self.assertEqual(row["workflow"], rule["workflow"])

    def test_fresh_wrapper_with_stale_repo_source_is_not_deployable(self):
        rule = dict(self.asset, sourceTimestamps=["head"], remediateSourceStale=True)
        stale = self.m.assess({"generatedAt": NOW, "head": NOW - 9*3600}, rule, NOW)
        row = self.m.compare(rule, stale, stale, self.config)
        self.assertEqual(row["workflow"], rule["workflow"])

    def test_alert_mode_fails_for_old_data_without_dispatch_in_read_only_mode(self):
        config = copy.deepcopy(self.config)
        config["artifacts"] = [self.asset]
        with patch.dict("os.environ", {}, clear=True), patch.object(self.m, "load_config", return_value=config), \
                patch.object(self.m.time, "time", return_value=NOW), \
                patch.object(self.m.HttpClient, "get_json", return_value={"generatedAt": NOW - 9*3600}), \
                patch("sys.stdout", io.StringIO()):
            self.assertEqual(self.m.main(["--no-remediation", "--fail-on-stale"]), 1)

    def test_alert_mode_does_not_repeat_failure_while_exact_repair_is_active(self):
        config = copy.deepcopy(self.config)
        config["artifacts"] = [self.asset]

        class ActiveRepairAPI:
            def __init__(self, *_args, **_kwargs):
                pass

            def runs(self, workflow):
                self.assert_workflow = workflow
                return [{"status": "in_progress", "created_at": "2026-09-17T11:55:00Z"}]

            def dispatch(self, workflow, inputs):
                raise AssertionError(f"active repair was dispatched again: {workflow} {inputs}")

        with patch.dict("os.environ", {"GH_TOKEN": "test-token"}, clear=True), \
                patch.object(self.m, "load_config", return_value=config), \
                patch.object(self.m.time, "time", return_value=NOW), \
                patch.object(self.m.HttpClient, "get_json", return_value={"generatedAt": NOW - 9*3600}), \
                patch.object(self.m, "GitHubAPI", ActiveRepairAPI), \
                patch("sys.stdout", io.StringIO()):
            self.assertEqual(self.m.main(["--fail-on-stale"]), 0)

    def test_r2_read_only_audit_does_not_fetch_raw_git_artifact(self):
        rule = next(a for a in self.config["artifacts"] if a["id"] == "dolo-liquidity")
        config = dict(self.config, artifacts=[rule])
        def fetch(url, limit):
            self.assertTrue(url.startswith(config["public_base_url"]))
            return {"generatedAt": NOW - 60}
        with patch.dict("os.environ", {"LP_DATA_STORAGE": "r2"}, clear=True), \
                patch.object(self.m, "load_config", return_value=config), \
                patch.object(self.m.time, "time", return_value=NOW), \
                patch.object(self.m.HttpClient, "get_json", side_effect=fetch), patch("sys.stdout", io.StringIO()):
            self.assertEqual(self.m.main(["--no-remediation", "--fail-on-stale"]), 0)

    def setUp(self):
        path = ROOT / "scripts/check_dashboard_freshness.py"
        self.assertTrue(path.is_file(), "public dashboard freshness monitor is not implemented")
        spec = importlib.util.spec_from_file_location("dashboard_freshness", path)
        self.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.m)
        self.config = self.m.load_config(ROOT / "config/dashboard_freshness.json")
        self.asset = next(a for a in self.config["artifacts"] if a["id"] == "assets")

    def test_fresh_early_due_and_stale_boundaries(self):
        for minutes, want in [(0, "fresh"), (9, "fresh"), (10, "early_due"), (15, "stale")]:
            with self.subTest(minutes=minutes):
                result = self.m.assess({"generatedAt": NOW - minutes * 60}, self.asset, NOW)
                self.assertEqual(result["state"], want)

    def test_invalid_missing_future_and_boolean_are_never_fresh(self):
        for value in [None, "broken", "", True, -1, NOW + 1, float("nan"), "2026-99-17"]:
            with self.subTest(value=value):
                result = self.m.assess({"generatedAt": value}, self.asset, NOW)
                self.assertEqual(result["state"], "invalid")

    def test_legacy_naive_iso_is_explicitly_utc(self):
        self.assertEqual(self.m.parse_timestamp("2026-09-17T12:00:00"), NOW)

    def test_flows_rebuild_does_not_reset_source_freshness(self):
        rule = next(a for a in self.config["artifacts"] if a["id"] == "dolo-flows")
        result = self.m.assess({"generatedAt": NOW, "timestamp": NOW - 9 * 3600}, rule, NOW)
        self.assertEqual(result["state"], "stale")
        self.assertEqual(result["age_minutes"], 540)

    def test_large_dashboard_artifacts_fit_bounded_monitor_limits(self):
        observed_sizes = {"vedolo-early-unlock": 12_498_421, "holder-wallet-history": 72_218_949}
        for artifact, size in observed_sizes.items():
            rule = next(a for a in self.config["artifacts"] if a["id"] == artifact)
            self.assertGreater(rule["maxBytes"], size)
            self.assertLessEqual(rule["maxBytes"], 100_000_000)

    def test_source_timestamp_overrides_fresh_wrapper(self):
        rule = dict(self.asset, timestamps=["generatedAt", "source.blockTimestamp"])
        result = self.m.assess({"generatedAt": NOW, "source": {"blockTimestamp": NOW - 7200}}, rule, NOW)
        self.assertEqual(result["state"], "source_stale")
        self.assertEqual(result["age_minutes"], 120)

    def test_missing_required_source_does_not_fall_back_to_wrapper(self):
        rule = dict(self.asset, timestamps=["generatedAt", "source.blockTimestamp"])
        self.assertEqual(self.m.assess({"generatedAt": NOW}, rule, NOW)["state"], "invalid")

    def test_cached_assets_rates_keep_original_source_age(self):
        result = self.m.assess({"generatedAt": NOW, "rateFallbackChains": ["ethereum"],
                                "rateFallbackSourceGeneratedAt": NOW - 3600}, self.asset, NOW)
        self.assertEqual(result["state"], "source_stale")

    def test_git_fresh_live_stale_is_deploy_only(self):
        public = self.m.assess({"generatedAt": NOW - 3600}, self.asset, NOW)
        repo = self.m.assess({"generatedAt": NOW - 60}, self.asset, NOW)
        row = self.m.compare(self.asset, public, repo, self.config)
        self.assertEqual(row["problem"], "deployment_behind")
        self.assertEqual(row["workflow"], "pages.yml")

    def test_public_unavailable_never_triggers_a_scanner(self):
        public = {"state": "unavailable", "error": "http_503", "age_minutes": None}
        repo = self.m.assess({"generatedAt": NOW}, self.asset, NOW)
        self.assertIsNone(self.m.compare(self.asset, public, repo, self.config)["workflow"])

    def test_both_stale_dispatches_producer_but_fresh_source_failure_does_not(self):
        old = self.m.assess({"generatedAt": NOW - 7200}, self.asset, NOW)
        self.assertEqual(self.m.compare(self.asset, old, old, self.config)["workflow"], "update-assets-live.yml")
        source = dict(old, state="source_stale", publication_age_minutes=0)
        self.assertIsNone(self.m.compare(self.asset, source, source, self.config)["workflow"])

    def test_active_run_always_blocks_dispatch(self):
        for state in ["queued", "in_progress", "waiting", "pending", "requested"]:
            runs = [{"status": state, "created_at": "2026-09-16T00:00:00Z", "event": "schedule"}]
            self.assertEqual(self.m.dispatch_gate(runs, self.config["workflows"]["update-assets-live.yml"], NOW), "active")

    def test_stuck_pages_wait_is_cancelled_once_without_dispatching_over_it(self):
        api = Mock()
        api.runs.return_value = [{"id": 42, "status": "waiting", "head_branch": "master",
                                  "created_at": NOW - 22*3600, "updated_at": NOW - 21*3600}]
        api.pending_deployments.return_value = [{"environment": {"name": "github-pages"},
                                                 "wait_timer": 0, "reviewers": []}]
        api.custom_protection_rules.return_value = {"total_count": 0, "custom_deployment_protection_rules": []}
        rows = [{"workflow": "pages.yml", "id": "odolo-data", "public": {"age_minutes": 1500}}]
        result = self.m.remediate(rows * 2, self.config, api, NOW, True)
        self.assertEqual(result[0]["action"], "cancelled_stalled_wait")
        self.assertEqual(result[0]["run_id"], 42)
        api.cancel.assert_called_once_with(42)
        api.dispatch.assert_not_called()

    def test_pages_wait_recovery_preserves_approvals_timers_and_active_deploys(self):
        ordinary_wait = {"id": 42, "status": "waiting", "head_branch": "master",
                         "created_at": NOW - 22*3600, "updated_at": NOW - 21*3600}
        ordinary_gate = {"environment": {"name": "github-pages"}, "wait_timer": 0, "reviewers": []}
        cases = [(dict(ordinary_wait, status="in_progress"), [ordinary_gate]),
                 (dict(ordinary_wait, updated_at=NOW - 60), [ordinary_gate]),
                 (dict(ordinary_wait, head_branch="other"), [ordinary_gate]),
                 (ordinary_wait, [dict(ordinary_gate, reviewers=[{"type": "User"}])]),
                 (ordinary_wait, [dict(ordinary_gate, wait_timer=60)]),
                 (ordinary_wait, [dict(ordinary_gate, wait_timer_started_at="2026-09-17T11:00:00Z")]),
                 (ordinary_wait, [dict(ordinary_gate, environment={"name": "production"})]),
                 (ordinary_wait, [dict(ordinary_gate, environment=None)]),
                 (ordinary_wait, [dict(ordinary_gate, environment=[])]),
                 (ordinary_wait, [])]
        for run, gates in cases:
            with self.subTest(run=run, gates=gates):
                api = Mock()
                api.runs.return_value = [run]
                api.pending_deployments.return_value = gates
                result = self.m.remediate([{"workflow": "pages.yml"}], self.config, api, NOW, True)
                self.assertEqual(result[0]["action"], "active")
                api.cancel.assert_not_called()
                api.dispatch.assert_not_called()

    def test_old_waiting_pages_run_is_visible_beyond_latest_thirty_cancellations(self):
        api = self.m.GitHubAPI(self.config, "test")
        recent = [{"id": n, "status": "completed", "conclusion": "cancelled"} for n in range(100, 130)]
        waiting = {"id": 42, "status": "waiting", "head_branch": "master"}
        def request(method, path, body=None):
            self.assertEqual(method, "GET")
            return {"workflow_runs": [waiting] if "status=waiting" in path else recent}
        with patch.object(api, "_request", side_effect=request):
            self.assertIn(waiting, api.runs("pages.yml"))

    def test_malformed_waiting_metadata_does_not_allow_a_new_deploy(self):
        api = self.m.GitHubAPI(self.config, "test")
        for waiting in ([None], [{"id": []}], [{"status": "waiting"}]):
            def request(method, path, body=None):
                self.assertEqual(method, "GET", "malformed metadata caused a mutation")
                return {"workflow_runs": waiting if "status=waiting" in path else []}
            with patch.object(api, "_request", side_effect=request):
                result = self.m.remediate([{"workflow": "pages.yml"}], self.config, api, NOW, True)
                self.assertEqual(result[0]["action"], "dispatch_failed")

    def test_custom_deployment_gate_is_never_cancelled(self):
        for rules in ({"total_count": 1, "custom_deployment_protection_rules": [{"enabled": True}]}, {}, None):
            api = Mock()
            api.runs.return_value = [{"id": 42, "status": "waiting", "head_branch": "master",
                                      "created_at": NOW - 22*3600, "updated_at": NOW - 21*3600}]
            api.pending_deployments.return_value = [{"environment": {"name": "github-pages"},
                                                     "wait_timer": 0, "reviewers": []}]
            api.custom_protection_rules.return_value = rules
            result = self.m.remediate([{"workflow": "pages.yml"}], self.config, api, NOW, True)
            self.assertEqual(result[0]["action"], "active")
            api.cancel.assert_not_called()
            api.dispatch.assert_not_called()

    def test_stuck_wait_cancellation_is_not_retried_after_ambiguous_response(self):
        api = Mock()
        api.runs.return_value = [{"id": 42, "status": "waiting", "head_branch": "master",
                                  "created_at": NOW - 22*3600, "updated_at": NOW - 21*3600}]
        api.pending_deployments.return_value = [{"environment": {"name": "github-pages"},
                                                 "wait_timer": 0, "reviewers": []}]
        api.custom_protection_rules.return_value = {"total_count": 0, "custom_deployment_protection_rules": []}
        api.cancel.side_effect = self.m.MonitorError("github_api_failed")
        result = self.m.remediate([{"workflow": "pages.yml"}] * 2, self.config, api, NOW, True)
        self.assertEqual(result[0]["action"], "dispatch_failed")
        api.cancel.assert_called_once_with(42)
        api.dispatch.assert_not_called()

    def test_pages_wait_recovery_policy_is_bounded_and_pages_only(self):
        for workflow, value in [("pages.yml", 0), ("pages.yml", True), ("pages.yml", 10),
                                ("pages.yml", float("inf")), ("update-assets-live.yml", 60)]:
            config = copy.deepcopy(self.config)
            config["workflows"][workflow]["stalledWaitingMinutes"] = value
            with self.assertRaises(self.m.MonitorError):
                self.m.validate_config(config)

    def test_recent_completed_failure_and_scheduled_run_apply_cooldown(self):
        runs = [{"status": "completed", "conclusion": "failure", "created_at": "2026-09-17T11:59:00Z", "event": "schedule"}]
        self.assertEqual(self.m.dispatch_gate(runs, self.config["workflows"]["update-assets-live.yml"], NOW), "cooldown")

    def test_rolling_dispatch_budget_counts_failed_attempts(self):
        policy = dict(self.config["workflows"]["update-assets-live.yml"], max_dispatches_in_window=2)
        runs = [{"status": "completed", "conclusion": "failure", "created_at": "2026-09-17T11:40:00Z", "event": "workflow_dispatch"}] * 2
        self.assertEqual(self.m.dispatch_gate(runs, policy, NOW), "retry_budget")

    def test_failure_cooldown_starts_at_completion_not_creation(self):
        runs = [{"status": "completed", "conclusion": "failure",
                 "created_at": "2026-09-17T10:00:00Z",
                 "updated_at": "2026-09-17T11:59:00Z"}]
        policy = self.config["workflows"]["update-assets-live.yml"]
        self.assertEqual(self.m.dispatch_gate(runs, policy, NOW), "cooldown")

    def test_recent_long_running_failures_consume_retry_budget(self):
        runs = [{"status": "completed", "conclusion": "failure",
                 "created_at": "2026-09-16T10:00:00Z", "updated_at": end}
                for end in ("2026-09-17T11:35:00Z", "2026-09-17T11:45:00Z")]
        policy = dict(self.config["workflows"]["update-assets-live.yml"], maxDispatchesInWindow=2)
        self.assertEqual(self.m.dispatch_gate(runs, policy, NOW), "retry_budget")

    def test_invalid_completion_time_fails_closed(self):
        policy = self.config["workflows"]["update-assets-live.yml"]
        for value in (None, "broken", "2026-09-18T00:00:00Z", "2026-09-17T09:00:00Z"):
            with self.subTest(value=value):
                run = {"status": "completed", "conclusion": "failure",
                       "created_at": "2026-09-17T10:00:00Z", "updated_at": value}
                self.assertEqual(self.m.dispatch_gate([run], policy, NOW), "run_metadata_invalid")

    def test_repeated_failures_back_off_and_success_resets_streak(self):
        policy = {"failureCooldownMinutes": 15, "maxFailureCooldownMinutes": 60,
                  "dispatchWindowMinutes": 1, "maxDispatchesInWindow": 3}
        runs = [{"status": "completed", "conclusion": "failure",
                 "created_at": end, "updated_at": end}
                for end in ("2026-09-17T10:05:00Z", "2026-09-17T10:20:00Z", "2026-09-17T11:10:00Z")]
        # Three consecutive failures: 15 -> 30 -> 60 minutes, independent of API order.
        self.assertEqual(self.m.dispatch_gate(runs, policy, NOW), "cooldown")
        self.assertEqual(self.m.dispatch_gate(list(reversed(runs)), policy, NOW), "cooldown")
        success = {"status": "completed", "conclusion": "success",
                   "created_at": "2026-09-17T11:15:00Z", "updated_at": "2026-09-17T11:20:00Z"}
        self.assertEqual(self.m.dispatch_gate([success, *runs], policy, NOW), "ready")

    def test_failure_backoff_is_bounded(self):
        policy = {"failureCooldownMinutes": 15, "maxFailureCooldownMinutes": 60,
                  "dispatchWindowMinutes": 1, "maxDispatchesInWindow": 3}
        run = {"status": "completed", "conclusion": "failure",
               "created_at": "2026-09-17T10:59:00Z", "updated_at": "2026-09-17T10:59:00Z"}
        self.assertEqual(self.m.dispatch_gate([run] * 10, policy, NOW), "ready")

    def test_invalid_failure_backoff_policy_is_rejected(self):
        for value in (0, -1, 10, None, True, "broken", float("nan"), float("inf")):
            with self.subTest(value=value):
                config = copy.deepcopy(self.config)
                config["workflows"]["update-assets-live.yml"]["maxFailureCooldownMinutes"] = value
                with self.assertRaises(self.m.MonitorError):
                    self.m.validate_config(config)

    def test_stale_alarm_remains_failure_while_remediation_backs_off(self):
        config = copy.deepcopy(self.config)
        config["artifacts"] = [self.asset]
        failed = {"status": "completed", "conclusion": "failure",
                  "created_at": "2026-09-17T10:00:00Z", "updated_at": "2026-09-17T11:59:00Z"}
        with patch.dict("os.environ", {"GH_TOKEN": "test"}), patch.object(self.m, "load_config", return_value=config), \
                patch.object(self.m.time, "time", return_value=NOW), \
                patch.object(self.m.HttpClient, "get_json", return_value={"generatedAt": NOW - 9*3600}), \
                patch.object(self.m.GitHubAPI, "runs", return_value=[failed]), \
                patch.object(self.m.GitHubAPI, "dispatch") as dispatch, patch("sys.stdout", io.StringIO()):
            self.assertEqual(self.m.main(["--fail-on-stale"]), 1)
            dispatch.assert_not_called()

    def test_bad_or_future_run_timestamps_fail_closed(self):
        policy = self.config["workflows"]["update-assets-live.yml"]
        for value in [None, "broken", "2026-09-18T00:00:00Z"]:
            self.assertEqual(self.m.dispatch_gate([{"status": "completed", "created_at": value}], policy, NOW), "run_metadata_invalid")

    def test_recheck_catches_job_starting_between_plan_and_dispatch(self):
        rows = [{"workflow": "update-assets-live.yml", "id": "assets", "problem": "stale", "target_minutes": 15}]
        class API:
            def __init__(self): self.calls = 0; self.sent = []
            def runs(self, workflow):
                self.calls += 1
                return [] if self.calls == 1 else [{"status": "queued"}]
            def dispatch(self, workflow, inputs): self.sent.append(workflow)
        api = API()
        decisions = self.m.remediate(rows, self.config, api, NOW, True)
        self.assertEqual(api.sent, [])
        self.assertEqual(decisions[0]["action"], "active")

    def test_global_budget_and_duplicate_workflows(self):
        names = ["update-assets-live.yml", "update-assets-live.yml", "update-dolo-price.yml", "update-tvl-data.yml"]
        rows = [{"workflow": name, "id": str(i), "problem": "stale", "target_minutes": 15 + i} for i, name in enumerate(names)]
        class API:
            def __init__(self): self.sent = []
            def runs(self, workflow): return []
            def dispatch(self, workflow, inputs): self.sent.append((workflow, inputs))
        api = API()
        config = dict(self.config, max_dispatches_per_run=2)
        decisions = self.m.remediate(rows, config, api, NOW, True)
        self.assertEqual(api.sent, [("update-assets-live.yml", {}), ("update-dolo-price.yml", {})])
        self.assertEqual(decisions[-1]["action"], "run_budget")

    def test_dry_run_never_dispatches(self):
        rows = [{"workflow": "update-assets-live.yml", "id": "assets", "problem": "stale", "target_minutes": 15}]
        class API:
            def runs(self, workflow): return []
            def dispatch(self, workflow, inputs): raise AssertionError("dry run dispatched")
        self.assertEqual(self.m.remediate(rows, self.config, API(), NOW, False)[0]["action"], "dry_run")

    def test_read_only_cli_reports_stale_data_without_github_credentials(self):
        config = copy.deepcopy(self.config)
        config["artifacts"] = [self.asset]
        output = io.StringIO()
        # Network only is replaced: main, assessment and remediation stay real.
        with patch.dict("os.environ", {}, clear=True), \
                patch.object(self.m, "load_config", return_value=config), \
                patch.object(self.m.time, "time", return_value=NOW), \
                patch.object(self.m.HttpClient, "get_json", return_value={"generatedAt": NOW - 3600}), \
                patch.object(self.m.GitHubAPI, "_request", side_effect=AssertionError("read-only audit called GitHub")), \
                patch("sys.stdout", output):
            self.assertEqual(self.m.main(["--no-remediation"]), 0)
        report = json.loads(output.getvalue())
        self.assertEqual(report["rows"][0]["public"]["state"], "stale")
        self.assertEqual(report["decisions"][0]["action"], "dry_run")

    def test_ambiguous_dispatch_failure_is_not_retried_in_run(self):
        rows = [{"workflow": "update-assets-live.yml", "id": "assets", "problem": "stale", "target_minutes": 15}]
        class API:
            def __init__(self): self.calls = 0
            def runs(self, workflow): return []
            def dispatch(self, workflow, inputs):
                self.calls += 1
                raise self_error("request_failed")
        self_error = self.m.MonitorError
        api = API()
        self.assertEqual(self.m.remediate(rows * 2, self.config, api, NOW, True)[0]["action"], "dispatch_failed")
        self.assertEqual(api.calls, 1)

    def test_config_rejects_unsafe_targets_and_inputs(self):
        for field, value in [("repository", "x/y/../../secret"), ("public_base_url", "https://user:secret@evil.test/"),
                             ("public_base_url", "http://127.0.0.1/"), ("branch", "master?token=secret")]:
            config = copy.deepcopy(self.config); config[field] = value
            with self.assertRaises(self.m.MonitorError): self.m.validate_config(config)
        for path in ["../secret.json", "/secret.json", "data/%2e%2e/secret.json", "https://evil.test/x.json"]:
            config = copy.deepcopy(self.config); config["artifacts"][0]["path"] = path
            with self.assertRaises(self.m.MonitorError): self.m.validate_config(config)
        config = copy.deepcopy(self.config)
        config["workflows"]["update-dolo-liquidity.yml"]["inputs"] = {"full_history": "true"}
        with self.assertRaises(self.m.MonitorError): self.m.validate_config(config)
        config = copy.deepcopy(self.config)
        config["workflows"]["update-earn-arbitrum-canonical-history.yml"] = {}
        with self.assertRaises(self.m.MonitorError): self.m.validate_config(config)

    def test_manifest_cannot_supply_arbitrary_path_or_future_date(self):
        for dates in [["../../secret"], ["2026-09-18"], []]:
            with self.assertRaises(self.m.MonitorError): self.m.snapshot_path({"dates": dates}, NOW)
        self.assertEqual(self.m.snapshot_path({"dates": ["2026-09-16", "2026-09-17"]}, NOW), "data/earn-snapshots/2026-09-17.json")

    def test_http_errors_do_not_leak_url_token_or_response(self):
        client = self.m.HttpClient(self.config, "supersecret")
        with patch.object(client.opener, "open", side_effect=URLError("https://key:supersecret@host/?token=secret")):
            with self.assertRaises(self.m.MonitorError) as caught:
                client.get_json(self.config["public_base_url"] + "assets_live.json", 1024)
        self.assertEqual(str(caught.exception), "request_failed")

    def test_http_response_size_is_bounded_and_public_request_has_no_auth(self):
        client = self.m.HttpClient(self.config, "supersecret")
        class Response(io.BytesIO):
            headers = {}
        requests = []
        def opened(request, timeout):
            requests.append(request)
            return Response(b"x" * 2000)
        with patch.object(client.opener, "open", side_effect=opened):
            with self.assertRaises(self.m.MonitorError):
                client.get_json(self.config["public_base_url"] + "assets_live.json", 100)
        self.assertFalse(requests[0].has_header("Authorization"))

    def test_api_auth_is_scoped_and_redirects_are_rejected(self):
        client = self.m.HttpClient(self.config, "supersecret")
        with self.assertRaises(self.m.MonitorError): client.get_json("https://evil.test/assets.json", 100)
        with self.assertRaises(HTTPError):
            self.m.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://evil.test/")


if __name__ == "__main__":
    unittest.main()
