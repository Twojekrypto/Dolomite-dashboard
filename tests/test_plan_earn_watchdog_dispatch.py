import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "plan_earn_watchdog_dispatch.py"
spec = importlib.util.spec_from_file_location("plan_earn_watchdog_dispatch", SCRIPT)
plan_earn_watchdog_dispatch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan_earn_watchdog_dispatch)


class EarnWatchdogDispatchPlanTest(unittest.TestCase):
    def test_rows_are_priority_sorted_and_preserve_all_chain_sentinel(self):
        payload = {
            "refreshJobs": [
                {
                    "workflow": "update-earn-arbitrum-canonical-history.yml",
                    "inputs": {},
                    "priority": 10,
                    "mode": "catchup",
                },
                {
                    "workflow": "update-earn-secondary-canonical-history.yml",
                    "inputs": {"chain": "xlayer"},
                    "priority": 5,
                    "mode": "catchup",
                },
                {"workflow": "", "inputs": {"chain": "ignored"}, "priority": 0},
            ]
        }

        rows = plan_earn_watchdog_dispatch.build_dispatch_rows(payload)

        self.assertEqual(
            [
                {
                    "workflow": "update-earn-secondary-canonical-history.yml",
                    "chain": "xlayer",
                    "priority": 5,
                    "mode": "catchup",
                },
                {
                    "workflow": "update-earn-arbitrum-canonical-history.yml",
                    "chain": "__all__",
                    "priority": 10,
                    "mode": "catchup",
                },
            ],
            rows,
        )

    def test_tsv_never_contains_empty_middle_chain_field(self):
        payload = {
            "refreshJobs": [
                {
                    "workflow": "update-earn-arbitrum-canonical-history.yml",
                    "inputs": {},
                    "priority": 10,
                    "mode": "catchup",
                },
                {
                    "workflow": "update-earn-secondary-canonical-history.yml",
                    "inputs": {"chain": "polygonzkevm"},
                    "priority": 10,
                    "mode": "catchup",
                },
            ]
        }

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "dispatch.tsv"
            plan_earn_watchdog_dispatch.write_dispatch_tsv(
                plan_earn_watchdog_dispatch.build_dispatch_rows(payload),
                output,
            )
            rows = [line.split("\t") for line in output.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(2, len(rows))
        for row in rows:
            self.assertEqual(4, len(row))
            self.assertNotEqual("", row[1])
        self.assertIn(
            ["update-earn-arbitrum-canonical-history.yml", "__all__", "10", "catchup"],
            rows,
        )

    def test_legacy_refresh_workflows_fallback_is_chainless_and_background_safe(self):
        rows = plan_earn_watchdog_dispatch.build_dispatch_rows(
            {
                "refreshWorkflows": [
                    "update-earn-netflow.yml",
                    "update-earn-ethereum-canonical-history.yml",
                ]
            }
        )

        self.assertEqual(
            [
                {
                    "workflow": "update-earn-ethereum-canonical-history.yml",
                    "chain": "__all__",
                    "priority": 50,
                    "mode": "catchup",
                },
                {
                    "workflow": "update-earn-netflow.yml",
                    "chain": "__all__",
                    "priority": 50,
                    "mode": "catchup",
                },
            ],
            rows,
        )

    def test_cli_writes_dispatch_tsv(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "actions.json"
            output = tmp_path / "dispatch.tsv"
            source.write_text(
                json.dumps(
                    {
                        "refreshJobs": [
                            {
                                "workflow": "update-earn-secondary-canonical-history.yml",
                                "inputs": {"chain": "mantle"},
                                "priority": "20",
                                "mode": "background",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch("sys.argv", ["plan", str(source), str(output)]):
                self.assertEqual(0, plan_earn_watchdog_dispatch.main())

            self.assertEqual(
                "update-earn-secondary-canonical-history.yml\tmantle\t20\tbackground\n",
                output.read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
