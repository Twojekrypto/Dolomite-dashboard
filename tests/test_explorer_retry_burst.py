import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

import calculate_avg_lock
import explorer_api
import generate_exercisers


THROTTLE = {
    "status": "0", "message": "NOTOK",
    "result": "Max calls per sec rate limit reached (3/sec)",
}
TX = {"hash": "first", "methodId": "0xa88f8139", "isError": "0", "txreceipt_status": "1"}


def response(payload, retry_after=None):
    result = requests.Response()
    result.status_code = 200
    result._content = json.dumps(payload).encode()
    if retry_after is not None:
        result.headers["Retry-After"] = retry_after
    return result


class ExplorerRetryBurstTests(unittest.TestCase):
    def test_three_throttles_recover_on_identical_page_with_existing_rows(self):
        for module, scan in (
            (calculate_avg_lock, calculate_avg_lock.get_all_exercise_txs),
            (generate_exercisers, generate_exercisers._get_all_transactions_once),
        ):
            with self.subTest(module=module.__name__), patch.object(module, "PAGE_SIZE", 1), patch(
                "requests.get", side_effect=[
                    response({"status": "1", "message": "OK", "result": [TX]}),
                    response(THROTTLE), response(THROTTLE), response(THROTTLE),
                    response({"status": "1", "message": "OK", "result": [{**TX, "hash": "second"}]}),
                    response({"status": "0", "message": "No transactions found", "result": []}),
                ],
            ) as get, patch("time.sleep"), contextlib.redirect_stdout(io.StringIO()):
                rows = scan()
                self.assertEqual([row["hash"] for row in rows], ["first", "second"])
                self.assertEqual([call.kwargs["params"]["page"] for call in get.call_args_list], [1, 2, 2, 2, 2, 3])
                self.assertEqual(get.call_args_list[1:5], [get.call_args_list[1]] * 4)

    def test_exhausted_default_budget_raises_without_partial_history(self):
        for module, scan in (
            (calculate_avg_lock, calculate_avg_lock.get_all_exercise_txs),
            (generate_exercisers, generate_exercisers._get_all_transactions_once),
        ):
            with self.subTest(module=module.__name__), patch.object(module, "PAGE_SIZE", 1), patch(
                "requests.get", side_effect=[
                    response({"status": "1", "message": "OK", "result": [TX]}),
                    *[response(THROTTLE) for _ in range(5)],
                ],
            ) as get, patch("time.sleep"), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(explorer_api.ExplorerError, "rate limit"):
                    scan()
                self.assertEqual([call.kwargs["params"]["page"] for call in get.call_args_list], [1, 2, 2, 2, 2, 2])

    def test_failed_average_lock_scan_keeps_existing_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "avg_lock_data.json"
            existing = b'{"avg_lock_days":262.7,"total_exercises":100}'
            path.write_bytes(existing)
            with patch.object(calculate_avg_lock, "OUTPUT_FILE", str(path)), patch(
                "requests.get", return_value=response(THROTTLE)
            ), patch("time.sleep"), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(explorer_api.ExplorerError):
                    calculate_avg_lock.main()
            self.assertEqual(path.read_bytes(), existing)

    def test_credentials_and_quota_rejections_fail_immediately(self):
        for reason in ("Invalid API Key", "Daily limit reached", "Monthly quota exceeded"):
            with self.subTest(reason=reason), patch("requests.get", return_value=response({
                "status": "0", "message": "NOTOK", "result": reason,
            })) as get, patch("time.sleep") as sleep:
                with self.assertRaises(explorer_api.ExplorerError) as raised:
                    explorer_api.explorer_get_with_retry("https://example.com/api", params={"page": 7}, timeout=1)
                self.assertEqual(raised.exception.category, "access")
                self.assertEqual(get.call_count, 1)
                sleep.assert_not_called()

    def test_retry_after_is_respected_but_capped_at_thirty_seconds(self):
        for retry_after, expected_wait in (("12", 12), ("3600", 30), ("invalid", 2)):
            with self.subTest(retry_after=retry_after), patch("requests.get", side_effect=[
                response(THROTTLE, retry_after),
                response({"status": "1", "message": "OK", "result": [TX]}),
            ]), patch("time.sleep") as sleep:
                result = explorer_api.explorer_get_with_retry("https://example.com/api", params={"page": 7}, timeout=1)
                self.assertEqual(result.json()["result"], [TX])
                sleep.assert_called_once_with(expected_wait)

    def test_pagination_waits_below_the_three_requests_per_second_limit(self):
        for module, scan in (
            (calculate_avg_lock, calculate_avg_lock.get_all_exercise_txs),
            (generate_exercisers, generate_exercisers._get_all_transactions_once),
        ):
            events = []

            def get(url, **kwargs):
                page = kwargs["params"]["page"]
                events.append(("page", page))
                return response({"status": "1", "message": "OK", "result": [TX] if page == 1 else []})

            with self.subTest(module=module.__name__), patch.object(module, "PAGE_SIZE", 1), patch(
                "requests.get", side_effect=get
            ), patch("time.sleep", side_effect=lambda delay: events.append(("wait", delay))), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(scan(), [TX])
                self.assertEqual(events[0], ("page", 1))
                self.assertEqual(events[1][0], "wait")
                self.assertGreaterEqual(events[1][1], 0.4)
                self.assertEqual(events[2], ("page", 2))


if __name__ == "__main__":
    unittest.main()
