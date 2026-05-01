import json
import os
import tempfile
import unittest
from pathlib import Path

from earn_live_config import (
    DEFAULT_CONFIG,
    build_endpoint_pairs,
    get_config_path,
    get_audit_earn_asset_defaults,
    get_chain_live_rerun_defaults,
    get_live_preset_names,
    load_earn_live_config,
    parse_endpoint_values,
)


class EarnLiveConfigTest(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("EARN_LIVE_CONFIG_PATH", None)

    def test_default_config_file_path(self):
        self.assertTrue(str(get_config_path()).endswith("config/earn_live_defaults.json"))

    def test_partial_override_keeps_defaults(self):
        payload = {
            "auditEarnAsset": {
                "liveDefaults": {
                    "workers": 6,
                },
                "liveJs": {
                    "settlePollMs": 275,
                },
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "earn_live_defaults.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            os.environ["EARN_LIVE_CONFIG_PATH"] = str(path)
            config = load_earn_live_config()

        self.assertEqual(config["auditEarnAsset"]["liveDefaults"]["workers"], 6)
        self.assertEqual(config["auditEarnAsset"]["liveJs"]["settlePollMs"], 275)
        self.assertEqual(
            config["runEarnChainLiveRerun"]["workersPerMarket"],
            DEFAULT_CONFIG["runEarnChainLiveRerun"]["workersPerMarket"],
        )
        self.assertEqual(
            config["auditEarnAsset"]["liveJs"]["settleStablePolls"],
            DEFAULT_CONFIG["auditEarnAsset"]["liveJs"]["settleStablePolls"],
        )

    def test_invalid_numeric_values_fall_back_to_defaults(self):
        payload = {
            "auditEarnAsset": {
                "liveDefaults": {"workers": 0},
                "liveJs": {"snapshotFlushEveryResults": -2},
            },
            "runEarnChainLiveRerun": {
                "maxMarkets": "bad",
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "earn_live_defaults.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            os.environ["EARN_LIVE_CONFIG_PATH"] = str(path)
            config = load_earn_live_config()

        self.assertEqual(
            config["auditEarnAsset"]["liveDefaults"]["workers"],
            DEFAULT_CONFIG["auditEarnAsset"]["liveDefaults"]["workers"],
        )
        self.assertEqual(
            config["auditEarnAsset"]["liveJs"]["snapshotFlushEveryResults"],
            DEFAULT_CONFIG["auditEarnAsset"]["liveJs"]["snapshotFlushEveryResults"],
        )
        self.assertEqual(
            config["runEarnChainLiveRerun"]["maxMarkets"],
            DEFAULT_CONFIG["runEarnChainLiveRerun"]["maxMarkets"],
        )
        self.assertEqual(
            config["runEarnChainLiveRerun"]["retryWorkersPerMarket"],
            DEFAULT_CONFIG["runEarnChainLiveRerun"]["retryWorkersPerMarket"],
        )

    def test_parse_endpoint_values_supports_comma_separated_and_json_lists(self):
        self.assertEqual(
            parse_endpoint_values("http://127.0.0.1:8921/index.html,http://127.0.0.1:8922/index.html"),
            [
                "http://127.0.0.1:8921/index.html",
                "http://127.0.0.1:8922/index.html",
            ],
        )
        self.assertEqual(
            parse_endpoint_values('["http://127.0.0.1:9555/json", "http://127.0.0.1:9666/json"]'),
            [
                "http://127.0.0.1:9555/json",
                "http://127.0.0.1:9666/json",
            ],
        )

    def test_build_endpoint_pairs_reuses_single_localhost_for_many_debug_endpoints(self):
        pairs = build_endpoint_pairs(
            "http://127.0.0.1:8921/index.html",
            "http://127.0.0.1:9555/json,http://127.0.0.1:9666/json",
        )
        self.assertEqual(
            pairs,
            [
                {
                    "localhostUrl": "http://127.0.0.1:8921/index.html",
                    "debugJsonUrl": "http://127.0.0.1:9555/json",
                },
                {
                    "localhostUrl": "http://127.0.0.1:8921/index.html",
                    "debugJsonUrl": "http://127.0.0.1:9666/json",
                },
            ],
        )

    def test_build_endpoint_pairs_rejects_mismatched_counts(self):
        with self.assertRaises(ValueError):
            build_endpoint_pairs(
                "http://127.0.0.1:8921/index.html,http://127.0.0.1:8922/index.html",
                "http://127.0.0.1:9555/json,http://127.0.0.1:9666/json,http://127.0.0.1:9777/json",
            )

    def test_dual_sharded_preset_exposes_multi_endpoint_defaults(self):
        self.assertIn("dual-sharded", get_live_preset_names())
        audit_defaults = get_audit_earn_asset_defaults("dual-sharded")
        rerun_defaults = get_chain_live_rerun_defaults("dual-sharded")
        self.assertEqual(audit_defaults["liveDefaults"]["workers"], 12)
        self.assertEqual(rerun_defaults["workersPerMarket"], 12)
        self.assertEqual(rerun_defaults["retryWorkersPerMarket"], 12)
        self.assertEqual(
            parse_endpoint_values(audit_defaults["liveDefaults"]["debugJsonUrl"]),
            ["http://127.0.0.1:9555/json", "http://127.0.0.1:9666/json"],
        )
        self.assertEqual(
            parse_endpoint_values(rerun_defaults["localhostUrl"]),
            [
                "http://127.0.0.1:8921/earn/?cb=earn_chain_live_dual_a",
                "http://127.0.0.1:8921/earn/?cb=earn_chain_live_dual_b",
            ],
        )


if __name__ == "__main__":
    unittest.main()
