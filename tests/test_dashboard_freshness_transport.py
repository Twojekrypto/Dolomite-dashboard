"""Exercise the real bounded JSON reader and monitor across transport failures."""
import copy
import importlib.util
import io
import json
from collections import Counter
from http.client import IncompleteRead
from pathlib import Path
import socket
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError


ROOT = Path(__file__).resolve().parents[1]
NOW = 1793872800


class Response(io.BytesIO):
    def __init__(self, body, *, length=None, failure=None):
        super().__init__(body)
        self.headers = {"Content-Length": str(len(body) if length is None else length)}
        self.failure = failure

    def read(self, limit=-1):
        if self.failure is not None:
            raise self.failure
        return super().read(limit)


def fresh_response(**extra):
    return Response(json.dumps({"generatedAt": NOW, **extra}).encode())


class DashboardFreshnessTransportTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "dashboard_freshness_transport", ROOT / "scripts/check_dashboard_freshness.py")
        self.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.m)
        self.config = self.m.load_config(ROOT / "config/dashboard_freshness.json")
        self.asset = next(rule for rule in self.config["artifacts"] if rule["id"] == "assets")

    def run_monitor(self, config, opened):
        class Opener:
            open = staticmethod(opened)

        output = io.StringIO()
        with patch.dict("os.environ", {}, clear=True), \
                patch.object(self.m, "load_config", return_value=config), \
                patch.object(self.m, "build_opener", return_value=Opener()), \
                patch.object(self.m.time, "time", return_value=NOW), \
                patch.object(self.m.time, "sleep"), patch("sys.stdout", output):
            code = self.m.main(["--no-remediation", "--fail-on-stale"])
        return code, json.loads(output.getvalue())

    def test_reader_retries_a_short_body_even_when_its_prefix_is_valid_json(self):
        client = self.m.HttpClient(self.config)
        # read(limit) can return fewer bytes without urllib raising IncompleteRead.
        # A valid JSON prefix must not conceal a truncated advertised response.
        short = Response(b'{"generatedAt": 1}', length=100)
        responses = iter([short, fresh_response()])
        with patch.object(client.opener, "open", side_effect=lambda *_a, **_kw: next(responses)), \
                patch.object(self.m.time, "sleep"):
            payload = client.get_json(self.config["public_base_url"] + "assets_live.json", 1024)
        self.assertEqual(payload, {"generatedAt": NOW})

    def test_reader_recovers_from_incomplete_read_without_crashing_the_monitor(self):
        client = self.m.HttpClient(self.config)
        broken = Response(b"", failure=IncompleteRead(b"sensitive-body", 200))
        responses = iter([broken, fresh_response()])
        with patch.object(client.opener, "open", side_effect=lambda *_a, **_kw: next(responses)), \
                patch.object(self.m.time, "sleep"):
            payload = client.get_json(self.config["public_base_url"] + "assets_live.json", 1024)
        self.assertEqual(payload, {"generatedAt": NOW})

    def test_exhausted_transport_diagnostics_distinguish_safe_causes(self):
        cases = [
            (TimeoutError("private-host/?token=secret"), "timeout", 3),
            (URLError(socket.gaierror(-2, "private-host")), "dns_error", 3),
            (HTTPError("https://private-host/?token=secret", 503, "secret-body", {}, None), "http_503", 3),
            (HTTPError("https://private-host/?token=secret", 404, "secret-body", {}, None), "http_404", 1),
        ]
        for failure, reason, attempts in cases:
            with self.subTest(reason=reason):
                client = self.m.HttpClient(self.config)
                with patch.object(client.opener, "open", side_effect=failure), \
                        patch.object(self.m.time, "sleep"):
                    with self.assertRaises(self.m.MonitorError) as caught:
                        client.get_json(self.config["public_base_url"] + "assets_live.json", 1024)
                self.assertEqual(str(caught.exception), "request_failed")
                self.assertEqual(caught.exception.reason, reason)
                self.assertEqual(caught.exception.attempts, attempts)
                self.assertEqual(caught.exception.error_class, type(failure).__name__)
                self.assertNotIn("secret", repr(vars(caught.exception)))
                self.assertNotIn("private-host", repr(vars(caught.exception)))

    def test_read_timeout_gets_one_deferred_attempt_after_other_artifacts(self):
        second = dict(self.asset, id="second", path="second.json")
        config = dict(self.config, artifacts=[self.asset, second])
        public_url = config["public_base_url"] + self.asset["path"]
        calls = []

        def opened(request, timeout):
            calls.append(request.full_url)
            if request.full_url == public_url and calls.count(public_url) <= 3:
                return Response(b"", failure=TimeoutError("sensitive endpoint"))
            return fresh_response()

        code, report = self.run_monitor(config, opened)
        self.assertEqual(code, 0)
        row = report["rows"][0]
        self.assertEqual(row["public"]["state"], "fresh")
        self.assertEqual(row["public"]["transport"]["attempts"], 4)
        self.assertEqual(row["public"]["transport"]["initial_reason"], "timeout")
        self.assertEqual(row["public"]["transport"]["initial_error_class"], "TimeoutError")
        self.assertEqual(calls, [public_url] * 3 + [
            config["raw_base_url"] + self.asset["path"],
            config["public_base_url"] + "second.json",
            config["raw_base_url"] + "second.json", public_url])

    def test_persistent_public_failure_remains_unavailable_despite_fresh_raw(self):
        config = dict(self.config, artifacts=[self.asset])
        counts = Counter()

        def opened(request, timeout):
            counts[request.full_url] += 1
            if request.full_url.startswith(config["public_base_url"]):
                return Response(b"", failure=TimeoutError("secret"))
            return fresh_response()

        code, report = self.run_monitor(config, opened)
        self.assertEqual(code, 1)
        row = report["rows"][0]
        self.assertEqual(row["public"]["state"], "unavailable")
        self.assertEqual(row["public"]["transport"]["reason"], "timeout")
        self.assertEqual(row["public"]["transport"]["attempts"], 4)
        self.assertEqual(row["repo"]["state"], "fresh")
        self.assertEqual(counts[config["public_base_url"] + self.asset["path"]], 4)
        self.assertEqual(counts[config["raw_base_url"] + self.asset["path"]], 1)
        self.assertNotIn("secret", json.dumps(report))

    def test_deferred_recovery_rechecks_source_age_instead_of_declaring_freshness(self):
        rule = dict(self.asset, sourceTimestamps=["head"], sourceMaxAgeMinutes=480)
        config = dict(self.config, artifacts=[rule])
        counts = Counter()

        def opened(request, timeout):
            counts[request.full_url] += 1
            if request.full_url.startswith(config["public_base_url"]):
                if counts[request.full_url] <= 3:
                    return Response(b"", failure=TimeoutError())
                return fresh_response(head=NOW - 9 * 3600)
            return fresh_response(head=NOW)

        code, report = self.run_monitor(config, opened)
        self.assertEqual(code, 1)
        self.assertEqual(report["rows"][0]["public"]["state"], "source_stale")
        self.assertEqual(report["rows"][0]["public"]["source_age_minutes"], 540)

    def test_raw_failure_is_retried_independently_without_refetching_healthy_public(self):
        config = dict(self.config, artifacts=[self.asset])
        counts = Counter()

        def opened(request, timeout):
            counts[request.full_url] += 1
            if request.full_url.startswith(config["raw_base_url"]) and counts[request.full_url] <= 3:
                return Response(b"", failure=TimeoutError())
            return fresh_response()

        code, report = self.run_monitor(config, opened)
        self.assertEqual(code, 0)
        self.assertEqual(report["rows"][0]["repo"]["state"], "fresh")
        self.assertEqual(counts[config["raw_base_url"] + self.asset["path"]], 4)
        self.assertEqual(counts[config["public_base_url"] + self.asset["path"]], 1)

    def test_permanent_http_bad_json_and_oversize_are_never_deferred(self):
        for failure in ("http", "json", "size"):
            with self.subTest(failure=failure):
                config = dict(self.config, artifacts=[dict(self.asset, maxBytes=100)])
                counts = Counter()

                def opened(request, timeout):
                    counts[request.full_url] += 1
                    if request.full_url.startswith(config["public_base_url"]):
                        if failure == "http":
                            raise HTTPError(request.full_url, 404, "secret", {}, None)
                        return Response(b"invalid-json" if failure == "json" else b"x" * 101)
                    return fresh_response()

                code, report = self.run_monitor(config, opened)
                self.assertEqual(code, 1)
                self.assertEqual(report["rows"][0]["public"]["state"], "unavailable")
                self.assertEqual(counts[config["public_base_url"] + self.asset["path"]], 1)

    def test_deferred_probes_have_a_global_request_cap(self):
        rules = [dict(self.asset, id=f"artifact-{n}", path=f"artifact-{n}.json") for n in range(4)]
        config = dict(self.config, artifacts=rules, maxDeferredProbeRequests=3)
        counts = Counter()

        def opened(request, timeout):
            counts[request.full_url] += 1
            if request.full_url.startswith(config["public_base_url"]):
                raise TimeoutError()
            return fresh_response()

        code, report = self.run_monitor(config, opened)
        self.assertEqual(code, 1)
        self.assertEqual([row["public"]["state"] for row in report["rows"]], ["unavailable"] * 4)
        self.assertEqual([counts[config["public_base_url"] + rule["path"]] for rule in rules], [4, 4, 4, 3])

    def test_deferred_probe_budget_rejects_unbounded_or_ambiguous_values(self):
        for budget in (-1, 11, 1.5, True, "3", None):
            with self.subTest(budget=budget):
                config = copy.deepcopy(self.config)
                config["maxDeferredProbeRequests"] = budget
                with self.assertRaises(self.m.MonitorError):
                    self.m.validate_config(config)


if __name__ == "__main__":
    unittest.main()
