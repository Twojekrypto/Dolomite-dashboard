import tempfile
import unittest
from pathlib import Path

import generate_supply_apr_history as apr_history


class SupplyAprHistoryTests(unittest.TestCase):
    def test_snapshot_uses_last_protocol_block_and_converts_fraction_to_percent(self):
        calls = []

        def fake_post(_endpoint, payload):
            calls.append(payload)
            if "transactions" in payload["query"]:
                return {"transactions": [{"timestamp": "1727999990", "blockNumber": "25738669"}]}
            return {
                "interestRates": [
                    {"token": {"id": "0xABC"}, "supplyInterestRate": "0.01541026102253218"},
                    {"token": {"id": "0xDEF"}, "supplyInterestRate": "0"},
                ]
            }

        snapshot = apr_history.fetch_rate_snapshot("https://official.test/subgraph", 1728000000, fake_post)

        self.assertEqual(snapshot["timestamp"], 1728000000)
        self.assertEqual(snapshot["sourceTimestamp"], 1727999990)
        self.assertEqual(snapshot["blockNumber"], 25738669)
        self.assertEqual(snapshot["rates"]["0xabc"], "1.541026102253218")
        self.assertEqual(snapshot["rates"]["0xdef"], "0")
        self.assertEqual(calls[0]["variables"], {"timestamp": "1728000000"})
        self.assertEqual(calls[1]["variables"], {"block": 25738669})

    def test_refresh_targets_only_missing_and_recent_utc_days(self):
        day = 86400
        now = 10 * day + 123
        existing = [
            {"timestamp": 7 * day, "rates": {"0xa": "1"}},
            {"timestamp": 8 * day, "rates": {"0xa": "2"}},
            {"timestamp": 9 * day, "rates": {"0xa": "3"}},
            {"timestamp": 10 * day, "rates": {"0xa": "4"}},
        ]

        targets = apr_history.build_snapshot_targets(
            now_timestamp=now,
            days=6,
            refresh_days=2,
            existing=existing,
        )

        self.assertEqual(targets, [5 * day, 6 * day, 9 * day, 10 * day])

    def test_write_chain_history_replaces_same_day_and_preserves_older_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            path = out_dir / "ethereum.json"
            path.write_text(
                '{"schemaVersion":1,"chain":"ethereum","points":['
                '{"timestamp":100,"blockNumber":1,"sourceTimestamp":99,"rates":{"0xa":"1"}},'
                '{"timestamp":200,"blockNumber":2,"sourceTimestamp":199,"rates":{"0xa":"2"}}]}',
                encoding="utf-8",
            )

            payload = apr_history.write_chain_history(
                out_dir,
                "ethereum",
                [{"timestamp": 200, "blockNumber": 3, "sourceTimestamp": 200, "rates": {"0xa": "2.5"}}],
                generated_at="2026-09-11T12:00:00Z",
                minimum_timestamp=100,
            )

            self.assertEqual([point["timestamp"] for point in payload["points"]], [100, 200])
            self.assertEqual(payload["points"][1]["blockNumber"], 3)
            self.assertEqual(payload["points"][1]["rates"]["0xa"], "2.5")
            self.assertEqual(payload["source"], "official-dolomite-subgraph")


if __name__ == "__main__":
    unittest.main()
