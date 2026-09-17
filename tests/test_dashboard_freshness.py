"""Behavior contracts for the bounded, public-data freshness monitor."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
NOW = 1789646400  # 2026-09-17T12:00:00Z


class DashboardFreshnessTests(unittest.TestCase):
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

    def test_recent_completed_failure_and_scheduled_run_apply_cooldown(self):
        runs = [{"status": "completed", "conclusion": "failure", "created_at": "2026-09-17T11:59:00Z", "event": "schedule"}]
        self.assertEqual(self.m.dispatch_gate(runs, self.config["workflows"]["update-assets-live.yml"], NOW), "cooldown")

    def test_rolling_dispatch_budget_counts_failed_attempts(self):
        policy = dict(self.config["workflows"]["update-assets-live.yml"], max_dispatches_in_window=2)
        runs = [{"status": "completed", "conclusion": "failure", "created_at": "2026-09-17T11:40:00Z", "event": "workflow_dispatch"}] * 2
        self.assertEqual(self.m.dispatch_gate(runs, policy, NOW), "retry_budget")

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
