import unittest

from scripts.check_earn_sla import evaluate_sla


class CheckEarnSlaTests(unittest.TestCase):
    def test_warning_does_not_fail_workflow(self):
        result = evaluate_sla({"summary": {"slaWarningChains": ["arbitrum"], "slaCriticalChains": []}})
        self.assertEqual(0, result["exitCode"])
        self.assertEqual("warning", result["status"])

    def test_critical_fails_after_repair_dispatch(self):
        result = evaluate_sla({"summary": {"slaWarningChains": [], "slaCriticalChains": ["berachain"]}})
        self.assertEqual(2, result["exitCode"])
        self.assertEqual("critical", result["status"])
        self.assertIn("berachain", result["message"])

    def test_critical_chain_is_repairing_while_matching_run_is_active(self):
        result = evaluate_sla(
            {"summary": {"slaWarningChains": [], "slaCriticalChains": ["berachain"]}},
            active_repair_chains={"berachain"},
        )

        self.assertEqual(0, result["exitCode"])
        self.assertEqual("repairing", result["status"])
        self.assertIn("berachain", result["message"])

    def test_uncovered_critical_chain_still_fails(self):
        result = evaluate_sla(
            {"summary": {"slaWarningChains": [], "slaCriticalChains": ["berachain", "ethereum"]}},
            active_repair_chains={"berachain"},
        )

        self.assertEqual(2, result["exitCode"])
        self.assertEqual(["ethereum"], result["criticalChains"])
