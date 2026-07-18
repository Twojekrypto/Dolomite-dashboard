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
