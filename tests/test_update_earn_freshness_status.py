import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from update_earn_freshness_status import (
    CHAIN_POLICIES,
    NETFLOW_WORKFLOW,
    build_status,
    write_actions_output,
)


class EarnFreshnessStatusTest(unittest.TestCase):
    def _write_json(self, root: Path, rel: str, payload: dict) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_build_status_marks_verified_window_and_refresh_recommendation(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                "earn-subaccount-history/manifest.json",
                {
                    "chains": {
                        "arbitrum": {
                            "lastBlock": 1_000_000,
                            "updatedAt": "2026-05-08T11:00:00Z",
                        },
                        "ethereum": {"lastBlock": 0, "updatedAt": "2026-05-08T11:59:00Z"},
                        "berachain": {"lastBlock": 0, "updatedAt": "2026-05-08T11:59:00Z"},
                        "botanix": {"lastBlock": 0, "updatedAt": "2026-05-08T11:59:00Z"},
                        "mantle": {"lastBlock": 0, "updatedAt": "2026-05-08T11:59:00Z"},
                    }
                },
            )
            for chain in ("ethereum", "berachain", "botanix", "mantle", "polygonzkevm"):
                self._write_json(
                    root,
                    f"earn-netflow/{chain}.json",
                    {
                        "chain": chain,
                        "lastBlock": 0,
                        "updatedAt": "2026-05-08T11:59:00Z",
                        "addressCount": 0,
                    },
                )
            self._write_json(
                root,
                "earn-netflow/arbitrum.json",
                {
                    "chain": "arbitrum",
                    "lastBlock": 1_021_600,
                    "updatedAt": "2026-05-08T11:30:00Z",
                    "addressCount": 10,
                },
            )
            self._write_json(
                root,
                "earn-snapshots/manifest.json",
                {
                    "dates": ["2026-05-08"],
                    "chains": {"2026-05-08": ["arbitrum"]},
                },
            )

            status = build_status(
                data_dir=root,
                live_blocks={
                    "arbitrum": 1_043_200,
                    "ethereum": 0,
                    "berachain": 0,
                    "botanix": 0,
                    "mantle": 0,
                    "polygonzkevm": 0,
                    "xlayer": 0,
                },
                now=now,
            )

        arbitrum = status["chains"]["arbitrum"]
        self.assertEqual(arbitrum["canonical"]["status"], "verified")
        self.assertEqual(arbitrum["canonical"]["estimatedLagMinutes"], 180.0)
        self.assertTrue(arbitrum["canonical"]["refreshRecommended"])
        self.assertEqual(arbitrum["canonical"]["refreshMode"], "background")
        self.assertEqual(arbitrum["status"], "verified")
        self.assertEqual(arbitrum["refreshMode"], "background")
        self.assertTrue(status["summary"]["backgroundRefreshRecommended"])
        self.assertFalse(status["summary"]["catchupRefreshRecommended"])
        self.assertIn("update-earn-arbitrum-canonical-history.yml", status["summary"]["refreshWorkflows"])
        self.assertIn(NETFLOW_WORKFLOW, status["summary"]["refreshWorkflows"])
        self.assertIn(
            {"workflow": NETFLOW_WORKFLOW, "inputs": {"chain": "arbitrum"}},
            status["summary"]["refreshJobs"],
        )
        report = {entry["chain"]: entry for entry in status["chainReport"]}
        self.assertEqual(report["arbitrum"]["supportMode"], "canonical-ledger")
        self.assertEqual(report["arbitrum"]["canonicalLagMinutes"], 180.0)
        self.assertEqual(report["arbitrum"]["weakPoint"], "canonical background refresh due")

    def test_missing_supported_canonical_history_triggers_refresh(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(root, "earn-subaccount-history/manifest.json", {"chains": {}})
            self._write_json(
                root,
                "earn-netflow/berachain.json",
                {"chain": "berachain", "lastBlock": 100, "updatedAt": "2026-05-08T11:59:00Z"},
            )
            self._write_json(root, "earn-snapshots/manifest.json", {"dates": [], "chains": {}})

            status = build_status(
                data_dir=root,
                live_blocks={
                    "arbitrum": 0,
                    "ethereum": 0,
                    "berachain": 100,
                    "botanix": 0,
                    "mantle": 0,
                    "polygonzkevm": 0,
                    "xlayer": 0,
                },
                now=now,
            )

        self.assertEqual(status["chains"]["berachain"]["canonical"]["status"], "missing")
        self.assertIn("update-earn-berachain-canonical-history.yml", status["summary"]["refreshWorkflows"])

    def test_berachain_netflow_uses_chain_specific_refresh_workflow(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                "earn-subaccount-history/manifest.json",
                {
                    "chains": {
                        "berachain": {
                            "lastBlock": 100,
                            "updatedAt": "2026-05-08T11:59:00Z",
                        }
                    }
                },
            )
            self._write_json(
                root,
                "earn-netflow/berachain.json",
                {"chain": "berachain", "lastBlock": 4_400, "updatedAt": "2026-05-08T10:30:00Z"},
            )
            self._write_json(root, "earn-snapshots/manifest.json", {"dates": [], "chains": {}})

            status = build_status(
                data_dir=root,
                live_blocks={
                    "arbitrum": None,
                    "ethereum": None,
                    "berachain": 10_000,
                    "botanix": None,
                    "mantle": None,
                    "polygonzkevm": None,
                    "xlayer": None,
                },
                now=now,
            )

        self.assertEqual(status["chains"]["berachain"]["netflow"]["status"], "syncing")
        self.assertIn("update-earn-berachain-netflow.yml", status["summary"]["refreshWorkflows"])
        self.assertNotIn(NETFLOW_WORKFLOW, status["summary"]["refreshWorkflows"])

    def test_snapshot_first_chain_does_not_loop_watchdog_refreshes(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            supported = {
                chain: {"lastBlock": 123, "updatedAt": "2026-05-08T11:59:00Z"}
                for chain, policy in CHAIN_POLICIES.items()
                if policy.get("canonicalSupported")
            }
            self._write_json(root, "earn-subaccount-history/manifest.json", {"chains": supported})
            for chain in supported:
                self._write_json(
                    root,
                    f"earn-netflow/{chain}.json",
                    {"chain": chain, "lastBlock": 123, "updatedAt": "2026-05-08T11:59:00Z"},
                )
            self._write_json(root, "earn-snapshots/manifest.json", {"dates": [], "chains": {}})

            status = build_status(
                data_dir=root,
                live_blocks={chain: 123 for chain in CHAIN_POLICIES},
                now=now,
            )

        self.assertEqual(status["chains"]["polygonzkevm"]["status"], "syncing")
        self.assertEqual(status["chains"]["polygonzkevm"]["netflow"]["status"], "missing")
        self.assertEqual(status["chains"]["polygonzkevm"]["supportMode"], "netflow-only")
        self.assertEqual(status["chains"]["xlayer"]["status"], "limited")
        self.assertEqual(status["chains"]["xlayer"]["supportMode"], "snapshot-first")
        self.assertEqual(status["summary"]["limitedChains"], ["xlayer"])
        self.assertIn(NETFLOW_WORKFLOW, status["summary"]["refreshWorkflows"])
        self.assertIn(
            {"workflow": NETFLOW_WORKFLOW, "inputs": {"chain": "polygonzkevm"}},
            status["summary"]["refreshJobs"],
        )
        self.assertNotIn("update-earn-secondary-canonical-history.yml", status["summary"]["refreshWorkflows"])
        report = {entry["chain"]: entry for entry in status["chainReport"]}
        self.assertEqual(report["polygonzkevm"]["weakPoint"], "netflow missing")
        self.assertIn("snapshot-first coverage", report["xlayer"]["weakPoint"])

    def test_recent_partial_netflow_is_reported_as_catching_up(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(root, "earn-subaccount-history/manifest.json", {"chains": {}})
            self._write_json(
                root,
                "earn-netflow/polygonzkevm.json",
                {
                    "chain": "polygonzkevm",
                    "lastBlock": 100,
                    "updatedAt": "2026-05-08T11:55:00Z",
                    "scanComplete": False,
                    "scanStatus": "partial",
                    "addressCount": 5,
                },
            )
            self._write_json(root, "earn-snapshots/manifest.json", {"dates": [], "chains": {}})

            status = build_status(
                data_dir=root,
                live_blocks={
                    "arbitrum": None,
                    "ethereum": None,
                    "berachain": None,
                    "botanix": None,
                    "mantle": None,
                    "polygonzkevm": 10_000,
                    "xlayer": None,
                },
                now=now,
            )

        polygon = status["chains"]["polygonzkevm"]
        self.assertEqual(polygon["netflow"]["status"], "catching_up")
        self.assertEqual(polygon["netflow"]["refreshMode"], "catchup")
        self.assertEqual(polygon["status"], "syncing")
        self.assertTrue(status["summary"]["catchupRefreshRecommended"])
        self.assertIn(
            {"workflow": NETFLOW_WORKFLOW, "inputs": {"chain": "polygonzkevm"}},
            status["summary"]["refreshJobs"],
        )
        report = {entry["chain"]: entry for entry in status["chainReport"]}
        self.assertEqual(report["polygonzkevm"]["weakPoint"], "netflow catching_up")

    def test_generic_netflow_refresh_jobs_stay_chain_specific(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_json(
                root,
                "earn-subaccount-history/manifest.json",
                {
                    "chains": {
                        "arbitrum": {"lastBlock": 1_000_000, "updatedAt": "2026-05-08T11:59:00Z"},
                        "ethereum": {"lastBlock": 1_000_000, "updatedAt": "2026-05-08T11:59:00Z"},
                    }
                },
            )
            for chain in ("arbitrum", "ethereum"):
                self._write_json(
                    root,
                    f"earn-netflow/{chain}.json",
                    {
                        "chain": chain,
                        "lastBlock": 1_000_000,
                        "updatedAt": "2026-05-08T10:00:00Z",
                        "addressCount": 1,
                    },
                )
            self._write_json(root, "earn-snapshots/manifest.json", {"dates": [], "chains": {}})

            status = build_status(
                data_dir=root,
                live_blocks={
                    "arbitrum": 1_100_000,
                    "ethereum": 1_002_000,
                    "berachain": None,
                    "botanix": None,
                    "mantle": None,
                    "polygonzkevm": None,
                    "xlayer": None,
                },
                now=now,
            )

        netflow_jobs = [
            job for job in status["summary"]["refreshJobs"]
            if job["workflow"] == NETFLOW_WORKFLOW
        ]
        self.assertIn({"workflow": NETFLOW_WORKFLOW, "inputs": {"chain": "arbitrum"}}, netflow_jobs)
        self.assertIn({"workflow": NETFLOW_WORKFLOW, "inputs": {"chain": "ethereum"}}, netflow_jobs)
        self.assertNotIn({"workflow": NETFLOW_WORKFLOW, "inputs": {"chain": "all"}}, netflow_jobs)

    def test_actions_output_contains_refresh_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "actions.json"
            write_actions_output(
                {
                    "summary": {
                        "refreshRecommended": True,
                        "refreshWorkflows": ["update-earn-netflow.yml"],
                        "refreshReasons": ["arbitrum: netflow stale"],
                    },
                    "chainReport": [{"chain": "arbitrum", "status": "syncing"}],
                },
                path,
            )
            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertTrue(payload["refreshRecommended"])
        self.assertEqual(payload["refreshWorkflows"], ["update-earn-netflow.yml"])
        self.assertEqual(payload["refreshReasons"], ["arbitrum: netflow stale"])
        self.assertEqual(payload["chainReport"], [{"chain": "arbitrum", "status": "syncing"}])


if __name__ == "__main__":
    unittest.main()
