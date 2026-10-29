import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


def workflow_scripts():
    lines = (ROOT / ".github/workflows/update-liquidation-risk.yml").read_text().splitlines()
    scripts = []
    for index, line in enumerate(lines):
        if not line.startswith("        run: "):
            continue
        first = line[len("        run: "):]
        if first == "|":
            block = []
            for following in lines[index + 1:]:
                if following and not following.startswith("          "):
                    break
                block.append(following[10:])
            scripts.append("\n".join(block))
        else:
            scripts.append(first)
    return scripts


class BorrowEarningsWorkflowTests(unittest.TestCase):
    def test_sparse_checkout_retains_pipeline_imports_and_fallback_snapshots(self):
        workflow = (ROOT / ".github/workflows/update-liquidation-risk.yml").read_text().splitlines()
        patterns = []
        for index, line in enumerate(workflow):
            if line.strip() != "sparse-checkout: |":
                continue
            for following in workflow[index + 1:]:
                if not following.startswith("            "):
                    break
                patterns.append(following.strip())
        self.assertTrue(patterns, "the hourly pipeline must avoid the unrelated full data checkout")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            required = [
                ".github/workflows/update-liquidation-risk.yml", "requirements.txt",
                "fetch_liquidation_risk.py", "build_borrow_earnings.py", "rpc_usage.py",
                "validate_data.py", "vedolo_vote_power.py",
                "tests/test_borrow_risk_rpc.py", "tests/test_borrow_risk_source.py",
                "tests/test_borrow_earnings.py", "tests/test_borrow_earnings_workflow.py",
            ]
            snapshots = ["liquidation_risk.json", "liquidation_history.json", "borrow_earnings.json",
                         "data/liquidation-risk/position-count-history.json",
                         "data/liquidation-risk/wallets/00.json"]
            for relative in required + snapshots + ["data/earn-ledger/unrelated.json"]:
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                if relative in required:
                    shutil.copyfile(ROOT / relative, target)
                else:
                    target.write_text("{}")
            for command in (["git", "init", "-q"], ["git", "add", "."],
                            ["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture"]):
                subprocess.run(command, cwd=root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "sparse-checkout", "set", "--no-cone", "--stdin"],
                           input="\n".join(patterns) + "\n", cwd=root, check=True, capture_output=True, text=True)
            for relative in required + snapshots:
                self.assertTrue((root / relative).is_file(), relative)
            self.assertFalse((root / "data/earn-ledger/unrelated.json").exists())
            result = subprocess.run([sys.executable, "-c", "import fetch_liquidation_risk, build_borrow_earnings, validate_data"],
                                    cwd=root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def run_publication(self, fail_generator=False):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "bin"
            binary.mkdir()
            (binary / "python3").write_text("""#!/bin/bash
set -eu
case "$1" in
  fetch_liquidation_risk.py) echo fresh > liquidation_risk.json; echo events > liquidation_history.json; mkdir -p data/liquidation-risk ;;
  build_borrow_earnings.py)
    if [ "${FAIL_GENERATOR:-0}" = 1 ]; then exit 17; fi
    test "$2" = --risk; test "$3" = liquidation_risk.json
    test "$4" = --output; test "$5" = borrow_earnings.json
    echo fresh > borrow_earnings.json ;;
  validate_data.py|-m) : ;;
  *) exit 45 ;;
esac
""")
            (binary / "git").write_text("""#!/bin/bash
echo "$*" >> git-commands.log
if [ "$1" = diff ]; then exit 1; fi
""")
            for path in binary.iterdir():
                path.chmod(0o755)
            environment = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ["PATH"], FAIL_GENERATOR="1" if fail_generator else "0")
            result = None
            for script in workflow_scripts():
                if script.startswith("pip "):
                    continue
                result = subprocess.run(["bash", "-e", "-c", script], cwd=root, env=environment, capture_output=True, text=True)
                if result.returncode:
                    break
            log = (root / "git-commands.log").read_text() if (root / "git-commands.log").exists() else ""
            ranking_exists = (root / "borrow_earnings.json").exists()
            return result, log, ranking_exists

    def test_success_stages_ranking_atomically_with_risk(self):
        result, log, ranking_exists = self.run_publication()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(ranking_exists, "workflow never generated the ranking")
        additions = [line for line in log.splitlines() if line.startswith("add ")]
        self.assertEqual(len(additions), 1)
        self.assertIn("liquidation_risk.json", additions[0].split())
        self.assertIn("borrow_earnings.json", additions[0].split())
        self.assertIn("push", log)

    def test_failed_generation_cannot_commit_new_risk_with_old_ranking(self):
        result, log, ranking_exists = self.run_publication(fail_generator=True)
        self.assertEqual(result.returncode, 17)
        self.assertFalse(ranking_exists)
        self.assertNotIn("add ", log)
        self.assertNotIn("commit ", log)
        self.assertNotIn("push", log)


if __name__ == "__main__":
    unittest.main()
