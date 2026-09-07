import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STAGE_HELPER = ROOT / "scripts" / "stage_earn_publishable_wallet_paths.sh"


class StageEarnPublishableWalletPathsTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp_dir.name)
        self.real_git = shutil.which("git")
        if not self.real_git:
            self.skipTest("git is required")
        subprocess.run([self.real_git, "init", "-q"], cwd=self.repo, check=True)
        subprocess.run(
            [self.real_git, "config", "user.email", "earn-test@example.invalid"],
            cwd=self.repo,
            check=True,
        )
        subprocess.run(
            [self.real_git, "config", "user.name", "Earn Test"],
            cwd=self.repo,
            check=True,
        )
        (self.repo / ".gitignore").write_text("data/\n", encoding="utf-8")
        subprocess.run([self.real_git, "add", ".gitignore"], cwd=self.repo, check=True)
        subprocess.run([self.real_git, "commit", "-qm", "initial"], cwd=self.repo, check=True)

        self.git_log = self.repo / "git-invocations.log"
        wrapper_dir = self.repo / "test-bin"
        wrapper_dir.mkdir()
        git_wrapper = wrapper_dir / "git"
        git_wrapper.write_text(
            "#!/usr/bin/env bash\n"
            "printf '%s\\n' \"$*\" >> \"$GIT_INVOCATION_LOG\"\n"
            "exec \"$REAL_GIT\" \"$@\"\n",
            encoding="utf-8",
        )
        git_wrapper.chmod(0o755)
        self.env = os.environ.copy()
        self.env.update(
            {
                "PATH": f"{wrapper_dir}{os.pathsep}{self.env['PATH']}",
                "REAL_GIT": self.real_git,
                "GIT_INVOCATION_LOG": os.fspath(self.git_log),
            }
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_data(self, relative_path, content):
        path = self.repo / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def commit_ignored(self, *relative_paths):
        subprocess.run(
            [self.real_git, "add", "-f", "--", *relative_paths],
            cwd=self.repo,
            check=True,
        )
        subprocess.run([self.real_git, "commit", "-qm", "fixtures"], cwd=self.repo, check=True)

    def run_helper(self, chain, addresses_file, *extra_args):
        return subprocess.run(
            [
                "bash",
                os.fspath(STAGE_HELPER),
                chain,
                os.fspath(addresses_file),
                *extra_args,
            ],
            cwd=self.repo,
            env=self.env,
            text=True,
            capture_output=True,
        )

    def staged_paths(self):
        output = subprocess.check_output(
            [self.real_git, "diff", "--cached", "--name-only"],
            cwd=self.repo,
            text=True,
        )
        return {line for line in output.splitlines() if line}

    def test_stages_only_selected_publishable_paths_including_deletion(self):
        selected = "0x1111111111111111111111111111111111111111"
        unselected = "0x2222222222222222222222222222222222222222"
        selected_history = f"data/earn-subaccount-history/berachain/{selected}.json"
        selected_ledger = f"data/earn-verified-ledger/berachain/{selected}.json"
        deleted_resolved = f"data/earn-resolved-interest-ledger/berachain/{selected}.json"
        unselected_history = f"data/earn-subaccount-history/berachain/{unselected}.json"
        runtime_cache = "data/earn-subaccount-history/.progress/berachain.json"

        self.write_data(selected_ledger, '{"value":"old"}\n')
        self.write_data(deleted_resolved, '{"value":"old"}\n')
        self.write_data(unselected_history, '{"value":"old"}\n')
        self.commit_ignored(selected_ledger, deleted_resolved, unselected_history)

        self.write_data(selected_history, '{"value":"new"}\n')
        self.write_data(selected_ledger, '{"value":"updated"}\n')
        (self.repo / deleted_resolved).unlink()
        self.write_data(unselected_history, '{"value":"unselected"}\n')
        self.write_data(runtime_cache, '{"cache":true}\n')
        address_file = self.repo / "addresses.txt"
        address_file.write_text(selected, encoding="utf-8")

        result = self.run_helper("berachain", address_file)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.staged_paths(),
            {selected_history, selected_ledger, deleted_resolved},
        )
        self.assertIn("Staged 3 publishable wallet paths for berachain", result.stdout)

    def test_rejects_malformed_input_before_staging_anything(self):
        valid_address = "0x3333333333333333333333333333333333333333"
        publishable = f"data/earn-subaccount-history/berachain/{valid_address}.json"
        self.write_data(publishable, '{}\n')

        cases = (("../berachain", valid_address), ("berachain", "not-an-address"))
        for chain, address in cases:
            with self.subTest(chain=chain, address=address):
                address_file = self.repo / "invalid-addresses.txt"
                address_file.write_text(f"{address}\n", encoding="utf-8")
                result = self.run_helper(chain, address_file)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.staged_paths(), set())

    def test_deduplicates_addresses_and_batches_git_for_large_cohort(self):
        addresses = [f"0x{number:040x}" for number in range(1, 101)]
        for address in addresses:
            self.write_data(
                f"data/earn-subaccount-history/arbitrum/{address}.json",
                '{}\n',
            )
        address_file = self.repo / "addresses.txt"
        address_file.write_text(
            "\n".join([addresses[0], addresses[0], *addresses[1:]]),
            encoding="utf-8",
        )

        result = self.run_helper("arbitrum", address_file)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.staged_paths()), 100)
        git_invocations = self.git_log.read_text(encoding="utf-8").splitlines()
        self.assertLess(len(git_invocations), 10, git_invocations)

    def test_verified_only_scope_batches_ledgers_without_staging_history(self):
        addresses = [f"0x{number:040x}" for number in range(1, 101)]
        expected = set()
        for address in addresses:
            for tree in (
                "data/earn-subaccount-history",
                "data/earn-verified-ledger",
                "data/earn-resolved-interest-ledger",
            ):
                relative_path = f"{tree}/mantle/{address}.json"
                self.write_data(relative_path, '{}\n')
                if tree != "data/earn-subaccount-history":
                    expected.add(relative_path)
        address_file = self.repo / "addresses.txt"
        address_file.write_text("\n".join(addresses), encoding="utf-8")

        result = self.run_helper("mantle", address_file, "--verified-only")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.staged_paths(), expected)
        git_invocations = self.git_log.read_text(encoding="utf-8").splitlines()
        self.assertLess(len(git_invocations), 10, git_invocations)


if __name__ == "__main__":
    unittest.main()
