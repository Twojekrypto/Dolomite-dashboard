import importlib.util
import io
import json
import os
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "plan_earn_watchdog_dispatch.py"
spec = importlib.util.spec_from_file_location("plan_earn_watchdog_dispatch", SCRIPT)
plan_earn_watchdog_dispatch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan_earn_watchdog_dispatch)


class EarnWatchdogDispatchPlanTest(unittest.TestCase):
    def test_planner_treats_every_nonterminal_run_as_an_active_chain_repair(self):
        workflow = "update-earn-arbitrum-canonical-history.yml"
        rows = plan_earn_watchdog_dispatch.build_dispatch_rows({"refreshWorkflows": [workflow]})
        for status, expected in (("queued", {"arbitrum"}), ("in_progress", {"arbitrum"}),
                                 ("pending", {"arbitrum"}), ("waiting", {"arbitrum"}),
                                 ("requested", {"arbitrum"}), ("completed", set())):
            with self.subTest(status=status):
                run = {"databaseId": 123, "status": status, "displayTitle": "Refresh Arbitrum Canonical EARN History"}

                def gh_output(argv, **_kwargs):
                    query_status = argv[argv.index("--status") + 1]
                    return json.dumps([run] if query_status == status else [])

                with mock.patch.object(plan_earn_watchdog_dispatch.subprocess, "check_output", side_effect=gh_output):
                    chains = plan_earn_watchdog_dispatch.collect_active_repair_chains(
                        rows, lambda name: plan_earn_watchdog_dispatch._load_active_runs("owner/repo", name),
                    )
                self.assertEqual(expected, chains)

    def test_monitor_inline_detector_blocks_nonterminal_matching_runs(self):
        source = (ROOT / ".github/workflows/monitor-earn-freshness.yml").read_text(encoding="utf-8")
        inline = source.split('active="$(python3 - "$workflow" "${chain:-}" <<\'PY\'\n', 1)[1]
        inline = textwrap.dedent(inline.split("\n          PY\n", 1)[0])
        for status, label, expected in (
            ("queued", "ethereum", "1"), ("in_progress", "ethereum", "1"),
            ("pending", "ethereum", "1"), ("waiting", "ethereum", "1"),
            ("requested", "ethereum", "1"), ("pending", "arbitrum", "0"),
            ("completed", "ethereum", "0"),
        ):
            with self.subTest(status=status, chain=label):
                run = {"databaseId": 123, "status": status, "displayTitle": f"Refresh Netflow [{label}]"}

                def gh_output(argv, **_kwargs):
                    query_status = argv[argv.index("--status") + 1]
                    return json.dumps([run] if query_status == status else [])

                output = io.StringIO()
                with mock.patch("subprocess.check_output", side_effect=gh_output), \
                     mock.patch("sys.argv", ["monitor", "update-earn-netflow.yml", "ethereum"]), \
                     mock.patch.dict(os.environ, {"GITHUB_REPOSITORY": "owner/repo"}), \
                     redirect_stdout(output), redirect_stderr(io.StringIO()):
                    exec(compile(inline, "monitor-inline-detector", "exec"), {})
                self.assertEqual(expected, output.getvalue().strip())

    def test_old_status_cannot_redispatch_retired_networks(self):
        jobs = [{"workflow": "update-earn-netflow.yml", "inputs": {"chain": chain}}
                for chain in ("mantle", "xlayer", "botanix", "polygonzkevm")]
        jobs.append({"workflow": "update-earn-secondary-canonical-history.yml", "inputs": {}})
        self.assertEqual([], plan_earn_watchdog_dispatch.build_dispatch_rows({"refreshJobs": jobs}))
        self.assertEqual([], plan_earn_watchdog_dispatch.build_dispatch_rows(
            {"refreshWorkflows": ["update-earn-secondary-canonical-history.yml"]}))

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
                    "workflow": "update-earn-netflow.yml",
                    "inputs": {"chain": "ethereum"},
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
                    "workflow": "update-earn-netflow.yml",
                    "chain": "ethereum",
                    "priority": 5,
                    "mode": "catchup",
                    "inputs": {},
                },
                {
                    "workflow": "update-earn-arbitrum-canonical-history.yml",
                    "chain": "__all__",
                    "priority": 10,
                    "mode": "catchup",
                    "inputs": {},
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
                    "workflow": "update-earn-netflow.yml",
                    "inputs": {"chain": "ethereum"},
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
            self.assertEqual(5, len(row))
            self.assertNotEqual("", row[1])
        self.assertIn(
            ["update-earn-arbitrum-canonical-history.yml", "__all__", "10", "catchup", "{}"],
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
                    "inputs": {},
                },
                {
                    "workflow": "update-earn-netflow.yml",
                    "chain": "__all__",
                    "priority": 50,
                    "mode": "catchup",
                    "inputs": {},
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
                                "workflow": "update-earn-netflow.yml",
                                "inputs": {"chain": "ethereum"},
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
                "update-earn-netflow.yml\tethereum\t20\tbackground\t{}\n",
                output.read_text(encoding="utf-8"),
            )

    def test_non_chain_inputs_are_preserved_for_workflow_dispatch(self):
        rows = plan_earn_watchdog_dispatch.build_dispatch_rows(
            {
                "refreshJobs": [
                    {
                        "workflow": "update-earn-arbitrum-canonical-history.yml",
                        "inputs": {"hot_limit": "120", "checkpoint_steps": "24"},
                        "priority": 0,
                        "mode": "catchup",
                    }
                ]
            }
        )

        self.assertEqual(
            {
                "workflow": "update-earn-arbitrum-canonical-history.yml",
                "chain": "__all__",
                "priority": 0,
                "mode": "catchup",
                "inputs": {"hot_limit": "120", "checkpoint_steps": "24"},
            },
            rows[0],
        )

    def test_generic_active_run_title_does_not_block_specific_secondary_chain(self):
        generic_run = {
            "displayTitle": "Refresh Secondary Canonical EARN History",
            "name": "Refresh Secondary Canonical EARN History",
        }
        polygon_run = {
            "displayTitle": "Refresh Secondary Canonical EARN History [ethereum]",
            "name": "Refresh Secondary Canonical EARN History",
        }
        all_run = {
            "displayTitle": "Refresh Secondary Canonical EARN History [all]",
            "name": "Refresh Secondary Canonical EARN History",
        }

        self.assertFalse(
            plan_earn_watchdog_dispatch.run_covers_requested_chain(
                generic_run,
                "ethereum",
            )
        )
        self.assertTrue(
            plan_earn_watchdog_dispatch.run_covers_requested_chain(
                polygon_run,
                "ethereum",
            )
        )
        self.assertFalse(
            plan_earn_watchdog_dispatch.run_covers_requested_chain(
                polygon_run,
                "arbitrum",
            )
        )
        self.assertTrue(
            plan_earn_watchdog_dispatch.run_covers_requested_chain(
                all_run,
                "ethereum",
            )
        )
        self.assertTrue(
            plan_earn_watchdog_dispatch.run_covers_requested_chain(
                generic_run,
                "",
            )
        )

    def test_active_dedicated_workflow_covers_its_chain_without_dispatch_input(self):
        rows = plan_earn_watchdog_dispatch.build_dispatch_rows(
            {
                "refreshJobs": [
                    {
                        "workflow": "update-earn-arbitrum-canonical-history.yml",
                        "inputs": {"hot_limit": "0", "checkpoint_steps": "24"},
                        "priority": -20,
                        "mode": "catchup",
                    }
                ]
            }
        )
        active_runs = {
            "update-earn-arbitrum-canonical-history.yml": [
                {
                    "databaseId": 33526498821,
                    "displayTitle": "Refresh Arbitrum Canonical EARN History",
                    "name": "Refresh Arbitrum Canonical EARN History",
                    "status": "in_progress",
                }
            ]
        }
        detector = getattr(
            plan_earn_watchdog_dispatch,
            "collect_active_repair_chains",
            lambda *_args, **_kwargs: set(),
        )

        chains = detector(rows, lambda workflow: active_runs.get(workflow, []))

        self.assertEqual({"arbitrum"}, chains)


if __name__ == "__main__":
    unittest.main()
