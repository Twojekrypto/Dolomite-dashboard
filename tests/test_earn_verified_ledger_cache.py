import subprocess
import unittest
from pathlib import Path


class EarnVerifiedLedgerCacheTests(unittest.TestCase):
    def test_runtime_cache_behavior(self):
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            ["node", "--test", "tests/earn_verified_ledger_cache.test.js"],
            cwd=root, capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
