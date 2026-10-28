import os
import pathlib
import subprocess
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
