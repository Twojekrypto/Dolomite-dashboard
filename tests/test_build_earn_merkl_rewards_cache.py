import contextlib
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

import build_earn_merkl_rewards_cache as merkl


FIRST = "0x1111111111111111111111111111111111111111"
SECOND = "0x2222222222222222222222222222222222222222"
TOKEN = "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d"
GENERATED_AT = "2026-11-03T21:44:17Z"


def reward_response():
    return io.BytesIO(json.dumps([{
        "chain": {"id": 1},
        "rewards": [{
            "token": {"symbol": "WLFI", "decimals": 18, "address": "0x" + "3" * 40},
            "breakdowns": [{
                "reason": "Dolomite_" + TOKEN,
                "amount": "2000000000000000000",
                "claimed": "1000000000000000000",
                "pending": "500000000000000000",
            }],
        }],
    }]).encode())


class ReadTimeoutResponse(io.BytesIO):
    def read(self, *args):
        raise TimeoutError("The read operation timed out")


class BuildEarnMerklRewardsCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.out_dir = self.root / "rewards"
        ledger_dir = self.root / "ledger"
        for address in (FIRST, SECOND):
            path = ledger_dir / "ethereum" / (address + ".json")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"address": address, "markets": {"0": {"token": TOKEN}}}))
        for name, value in (("OUT_DIR", self.out_dir), ("LEDGER_DIR", ledger_dir)):
            patcher = patch.object(merkl, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def build(self, urlopen):
        with patch.object(merkl.urllib.request, "urlopen", side_effect=urlopen), \
                patch.object(merkl.time, "sleep"), \
                patch.object(merkl, "utc_now", side_effect=[GENERATED_AT, "2026-11-03T22:06:17Z"]), \
                contextlib.redirect_stdout(io.StringIO()):
            return merkl.build_cache("ethereum", None, None, 0, 20, 3)

    def old_wallet_file(self):
        path = self.out_dir / "ethereum" / (FIRST + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"generatedAt":"2026-11-02T00:00:00Z","rewards":{"old":{}}}\n')
        return path

    def test_read_timeouts_recover_after_healthy_wallets_without_restamping_scan(self):
        requested = []

        def urlopen(request, timeout):
            address = request.full_url.split("/users/", 1)[1].split("/", 1)[0]
            requested.append(address)
            if address == FIRST and requested.count(FIRST) <= 3:
                return ReadTimeoutResponse()
            return reward_response()

        manifest = self.build(urlopen)

        self.assertEqual(0, manifest["chains"]["ethereum"]["failureCount"])
        self.assertEqual(2, manifest["chains"]["ethereum"]["rewardAddressCount"])
        self.assertEqual([FIRST, FIRST, FIRST, SECOND, FIRST], requested)
        self.assertEqual([], manifest["failures"]["ethereum"])
        for address in (FIRST, SECOND):
            result = json.loads((self.out_dir / "ethereum" / (address + ".json")).read_text())
            self.assertEqual(GENERATED_AT, result["generatedAt"])
            self.assertEqual(2.5, result["rewards"]["WLFI"]["accumulated"])
            self.assertEqual(1.5, result["rewards"]["WLFI"]["unclaimed"])
        self.assertEqual(GENERATED_AT, manifest["generatedAt"])

    def test_recovered_empty_response_removes_previous_wallet_rewards(self):
        stale_path = self.old_wallet_file()
        attempts = 0

        def urlopen(request, timeout):
            nonlocal attempts
            if FIRST not in request.full_url:
                return reward_response()
            attempts += 1
            return ReadTimeoutResponse() if attempts <= 3 else io.BytesIO(b"[]")

        manifest = self.build(urlopen)

        self.assertEqual(0, manifest["chains"]["ethereum"]["failureCount"])
        self.assertEqual(1, manifest["chains"]["ethereum"]["rewardAddressCount"])
        self.assertFalse(stale_path.exists())

    def test_exhausted_recovery_is_bounded_and_keeps_failed_wallet_source_bytes(self):
        stale_path = self.old_wallet_file()
        previous_bytes = stale_path.read_bytes()
        requested = []

        def urlopen(request, timeout):
            address = request.full_url.split("/users/", 1)[1].split("/", 1)[0]
            requested.append(address)
            return ReadTimeoutResponse() if address == FIRST else reward_response()

        manifest = self.build(urlopen)

        self.assertEqual([FIRST, FIRST, FIRST, SECOND, FIRST, FIRST, FIRST], requested)
        self.assertEqual(1, manifest["chains"]["ethereum"]["failureCount"])
        self.assertEqual(1, manifest["chains"]["ethereum"]["rewardAddressCount"])
        self.assertEqual([{"address": FIRST, "error": "The read operation timed out"}], manifest["failures"]["ethereum"])
        self.assertEqual(previous_bytes, stale_path.read_bytes())

    def test_permanent_http_error_is_not_retried_after_the_scan(self):
        requested = []

        def urlopen(request, timeout):
            address = request.full_url.split("/users/", 1)[1].split("/", 1)[0]
            requested.append(address)
            if address == FIRST:
                raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)
            return reward_response()

        manifest = self.build(urlopen)

        self.assertEqual([FIRST, SECOND], requested)
        self.assertEqual(1, manifest["chains"]["ethereum"]["failureCount"])
        self.assertEqual(FIRST, manifest["failures"]["ethereum"][0]["address"])

    def test_rate_limit_recovers_in_deferred_pass(self):
        attempts = 0

        def urlopen(request, timeout):
            nonlocal attempts
            if FIRST in request.full_url:
                attempts += 1
                if attempts <= 3:
                    raise urllib.error.HTTPError(request.full_url, 429, "Too Many Requests", {}, None)
            return reward_response()

        manifest = self.build(urlopen)

        self.assertEqual(0, manifest["chains"]["ethereum"]["failureCount"])
        self.assertEqual(2, manifest["chains"]["ethereum"]["rewardAddressCount"])
        self.assertEqual(2.5, json.loads((self.out_dir / "ethereum" / (FIRST + ".json")).read_text())["rewards"]["WLFI"]["accumulated"])

    def test_main_still_exits_nonzero_when_all_transport_attempts_fail(self):
        stale_path = self.old_wallet_file()
        previous_bytes = stale_path.read_bytes()
        with patch.object(merkl.urllib.request, "urlopen", side_effect=lambda *args, **kwargs: ReadTimeoutResponse()), \
                patch.object(merkl.time, "sleep"), \
                patch.object(merkl, "utc_now", return_value=GENERATED_AT), \
                patch("sys.argv", ["build_earn_merkl_rewards_cache.py", "--sleep", "0"]), \
                contextlib.redirect_stdout(io.StringIO()):
            result = merkl.main()

        self.assertEqual(1, result)
        manifest = json.loads((self.out_dir / "manifest.json").read_text())
        self.assertEqual(2, manifest["chains"]["ethereum"]["failureCount"])
        self.assertEqual(0, manifest["chains"]["ethereum"]["rewardAddressCount"])
        self.assertEqual(previous_bytes, stale_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
