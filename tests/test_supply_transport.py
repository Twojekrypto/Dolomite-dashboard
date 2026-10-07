"""Supply generators must retry a broken response without accepting partial data."""

import io
import json
import os
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.client import BadStatusLine, HTTPResponse, IncompleteRead, RemoteDisconnected
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import generate_supply_activity as activity
import generate_supply_apr_history as apr
import generate_supply_history as history


GENERATORS = (activity, history, apr)


class ResponseSocket:
    def __init__(self, body, missing_bytes=0):
        self.stream = io.BytesIO(
            b"HTTP/1.1 200 OK\r\nContent-Length: "
            + str(len(body) + missing_bytes).encode("ascii")
            + b"\r\n\r\n"
            + body
        )

    def makefile(self, mode):
        return self.stream


def raw_response(body, missing_bytes=0):
    result = HTTPResponse(ResponseSocket(body, missing_bytes))
    result.begin()
    return result


def response(data, missing_bytes=0):
    return raw_response(json.dumps({"data": data}).encode("utf-8"), missing_bytes)


class SupplyTransportTests(unittest.TestCase):
    def test_incomplete_page_is_retried_without_restarting_successful_pages(self):
        for generator in (activity, history):
            with self.subTest(generator=generator.__name__):
                responses = [
                    response({"deposits": [{"id": "first"}]}),
                    response({"deposits": [{"id": "partial-must-not-be-used"}]}, missing_bytes=41559),
                    response({"deposits": [{"id": "second"}]}),
                    response({"deposits": []}),
                ]
                queries = []

                def open_response(req, timeout):
                    queries.append(json.loads(req.data)["query"])
                    return responses[len(queries) - 1]

                kwargs = {"block_number": 42} if generator is activity else {}
                with patch.object(generator.request, "urlopen", side_effect=open_response), patch.object(generator.time, "sleep"):
                    rows = generator.paginate_entity(
                        "https://subgraph.invalid", "deposits", 'token: "selected"', "id",
                        page_size=1, max_pages=3, **kwargs,
                    )

                self.assertEqual(rows, [{"id": "first"}, {"id": "second"}])
                self.assertEqual([int(re.search(r"skip: (\d+)", query)[1]) for query in queries], [0, 1, 1, 2])
                self.assertEqual(queries[1], queries[2])
                if generator is activity:
                    self.assertTrue(all("block: { number: 42 }" in query for query in queries))
                self.assertTrue(all(item.closed for item in responses))

    def test_transport_interruptions_are_retried_by_every_supply_stage(self):
        # These arise at either the response-header or response-body boundary.
        failures = (BadStatusLine, RemoteDisconnected, ConnectionResetError, TimeoutError)
        for generator in GENERATORS:
            for failure in failures:
                with self.subTest(generator=generator.__name__, failure=failure.__name__):
                    requests = []

                    def open_response(req, timeout):
                        requests.append(req.data)
                        if len(requests) == 1:
                            raise failure("interrupted response")
                        return response({"tokens": [{"id": "complete"}]})

                    with patch.object(generator.request, "urlopen", side_effect=open_response), patch.object(generator.time, "sleep"):
                        data = generator.post_json("https://subgraph.invalid", {"query": "{ tokens { id } }"})
                    self.assertEqual(data, {"tokens": [{"id": "complete"}]})
                    self.assertEqual(len(requests), 2)
                    self.assertEqual(requests[0], requests[1])

    def test_truncated_utf8_body_is_retried_by_every_supply_stage(self):
        for generator in GENERATORS:
            with self.subTest(generator=generator.__name__):
                responses = [
                    raw_response(b'{"data":{"tokens":[{"symbol":"\xc3'),
                    response({"tokens": [{"symbol": "USD₮0"}]}),
                ]
                with patch.object(generator.request, "urlopen", side_effect=responses), patch.object(generator.time, "sleep"):
                    data = generator.post_json("https://subgraph.invalid", {"query": "{ tokens { symbol } }"})
                self.assertEqual(data, {"tokens": [{"symbol": "USD₮0"}]})
                self.assertTrue(all(item.closed for item in responses))

    def test_incomplete_response_exhaustion_raises_with_cause_in_every_supply_stage(self):
        for generator in GENERATORS:
            with self.subTest(generator=generator.__name__):
                attempts = []

                def open_response(req, timeout):
                    result = response({"tokens": [{"id": "partial"}]}, missing_bytes=1)
                    attempts.append(result)
                    return result

                with patch.object(generator.request, "urlopen", side_effect=open_response), patch.object(generator.time, "sleep"):
                    with self.assertRaisesRegex(RuntimeError, "after 3 attempts") as raised:
                        generator.post_json("https://subgraph.invalid", {"query": "{ tokens { id } }"}, retries=3)
                self.assertIsInstance(raised.exception.__cause__, IncompleteRead)
                self.assertEqual(len(attempts), 3)
                self.assertTrue(all(item.closed for item in attempts))

    def test_exhausted_activity_transport_preserves_published_files_and_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            token = {"id": "selected", "symbol": "LINK"}
            activity.write_activity_file(out, "ethereum", token, [], activity.utc_now())
            (out / "manifest.json").write_text('{"generatedAt":"2026-10-01T00:00:00Z","chains":[]}\n')
            previous = {path.relative_to(out): path.read_bytes() for path in out.rglob("*.json")}
            args = ["generate_supply_activity.py", "--out-dir", directory, "--chains", "ethereum", "--symbols", ""]

            def incomplete_response(req, timeout):
                query = json.loads(req.data)["query"]
                if "_meta" in query:
                    return response({"_meta": {"block": {"number": 42}, "hasIndexingErrors": False}})
                if "token(id:" in query:
                    return response({"token": {"decimals": 18, "totalPar": {"supplyPar": "0", "borrowPar": "0"}}})
                for entity in ("marginAccountTokenValues", "deposits", "withdrawals", "transfers", "trades", "liquidations", "vaporizations"):
                    if entity + "(" in query:
                        # Exhaust retry in a worker after head and baseline fetch,
                        # as in the production Future.result() failure.
                        return response({entity: []}, missing_bytes=1 if entity == "deposits" else 0)
                self.fail(query)

            error = io.StringIO()
            with patch.object(sys, "argv", args), patch.object(activity, "fetch_tokens", return_value=[token]), patch.object(activity.request, "urlopen", side_effect=incomplete_response), patch.object(activity.time, "sleep"), redirect_stdout(io.StringIO()), redirect_stderr(error):
                result = activity.main()

            self.assertEqual(result, 1)
            self.assertEqual({path.relative_to(out): path.read_bytes() for path in out.rglob("*.json")}, previous)
            self.assertIn("manifest not promoted", error.getvalue())
            self.assertIn("selected", error.getvalue())
            self.assertIn("IncompleteRead", error.getvalue())

    def test_programming_errors_are_not_retried(self):
        for generator in GENERATORS:
            with self.subTest(generator=generator.__name__):
                with patch.object(generator.request, "urlopen", side_effect=TypeError("bad request usage")):
                    with self.assertRaisesRegex(TypeError, "bad request usage"):
                        generator.post_json("https://subgraph.invalid", {"query": "{}"})


if __name__ == "__main__":
    unittest.main()
