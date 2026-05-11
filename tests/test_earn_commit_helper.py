import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts" / "commit_with_fresh_earn_status.sh"


def _run(cmd, *, cwd, env=None):
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


class EarnCommitHelperIntegrationTest(unittest.TestCase):
    def _prepare_repo(self, tmp_path: Path) -> tuple[Path, Path]:
        remote = tmp_path / "remote.git"
        work = tmp_path / "work"
        _run(["git", "init", "--bare", str(remote)], cwd=tmp_path)
        _run(["git", "init", str(work)], cwd=tmp_path)
        _run(["git", "checkout", "-b", "master"], cwd=work)
        _run(["git", "config", "user.name", "Test Bot"], cwd=work)
        _run(["git", "config", "user.email", "test@example.invalid"], cwd=work)

        (work / "scripts").mkdir()
        shutil.copy2(HELPER, work / "scripts" / "commit_with_fresh_earn_status.sh")
        (work / "scripts" / "sync_earn_verified_manifest.py").write_text(
            textwrap.dedent(
                """\
                #!/usr/bin/env python3
                import json
                import sys
                from pathlib import Path

                Path("sync-args.txt").write_text("\\n".join(sys.argv[1:]), encoding="utf-8")
                manifest = Path("data/earn-verified-ledger/manifest.json")
                manifest.write_text(json.dumps({
                    "version": 2,
                    "generatedAt": "synced",
                    "chains": {"mantle": {"snapshotDate": "2026-05-11", "lastNetflowBlock": 1, "addressCount": 1}},
                }), encoding="utf-8")
                """
            ),
            encoding="utf-8",
        )
        (work / "update_earn_freshness_status.py").write_text(
            textwrap.dedent(
                """\
                #!/usr/bin/env python3
                import argparse
                from pathlib import Path

                parser = argparse.ArgumentParser()
                parser.add_argument("--output", required=True)
                parser.add_argument("--actions-output")
                args = parser.parse_args()
                Path(args.output).parent.mkdir(parents=True, exist_ok=True)
                Path(args.output).write_text('{"status":"ok"}\\n', encoding="utf-8")
                """
            ),
            encoding="utf-8",
        )
        (work / "data" / "earn-verified-ledger").mkdir(parents=True)
        (work / "data" / "earn-freshness").mkdir(parents=True)
        (work / "data" / "earn-verified-ledger" / "manifest.json").write_text(
            '{"version":2,"generatedAt":"initial","chains":{}}\n',
            encoding="utf-8",
        )
        (work / "data" / "earn-freshness" / "status.json").write_text(
            '{"status":"initial"}\n',
            encoding="utf-8",
        )
        _run(["git", "add", "."], cwd=work)
        _run(["git", "commit", "-m", "initial"], cwd=work)
        _run(["git", "remote", "add", "origin", str(remote)], cwd=work)
        _run(["git", "push", "origin", "master"], cwd=work)
        return remote, work

    def test_manifest_only_change_uses_chain_env_for_manifest_sync(self):
        with tempfile.TemporaryDirectory() as tmp:
            _remote, work = self._prepare_repo(Path(tmp))
            manifest = work / "data" / "earn-verified-ledger" / "manifest.json"
            manifest.write_text(
                '{"version":2,"generatedAt":"local","chains":{"mantle":{"snapshotDate":"2026-05-10","lastNetflowBlock":1,"addressCount":1}}}\n',
                encoding="utf-8",
            )
            _run(["git", "add", "-f", "data/earn-verified-ledger/manifest.json"], cwd=work)

            env = {
                **dict(os.environ),
                "CHAIN": "mantle",
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
            }
            _run(["bash", "scripts/commit_with_fresh_earn_status.sh", "manifest only"], cwd=work, env=env)

            sync_args = (work / "sync-args.txt").read_text(encoding="utf-8").splitlines()
            self.assertIn("--chain", sync_args)
            self.assertIn("mantle", sync_args)
            self.assertNotIn("--all-chains", sync_args)


if __name__ == "__main__":
    unittest.main()
