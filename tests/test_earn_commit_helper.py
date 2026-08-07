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
        shutil.copy2(
            ROOT / "scripts" / "sync_earn_subaccount_manifest.py",
            work / "scripts" / "sync_earn_subaccount_manifest.py",
        )
        shutil.copy2(
            ROOT / "scripts" / "sync_earn_resolved_manifest.py",
            work / "scripts" / "sync_earn_resolved_manifest.py",
        )
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
        (work / "data" / "earn-resolved-interest-ledger").mkdir(parents=True)
        (work / "data" / "earn-subaccount-history" / "ethereum").mkdir(parents=True)
        (work / "data" / "earn-subaccount-history" / "xlayer").mkdir(parents=True)
        (work / "data" / "earn-freshness").mkdir(parents=True)
        (work / "data" / "earn-snapshots").mkdir(parents=True)
        (work / "data" / "earn-verified-ledger" / "manifest.json").write_text(
            '{"version":2,"generatedAt":"initial","chains":{}}\n',
            encoding="utf-8",
        )
        (work / "data" / "earn-resolved-interest-ledger" / "manifest.json").write_text(
            '{"version":1,"generatedAt":"initial","chains":{}}\n',
            encoding="utf-8",
        )
        (work / "data" / "earn-freshness" / "status.json").write_text(
            '{"status":"initial"}\n',
            encoding="utf-8",
        )
        (work / "data" / "earn-snapshots" / "manifest.json").write_text(
            '{"dates":["2026-05-10"]}\n',
            encoding="utf-8",
        )
        (work / "data" / "earn-snapshots" / "2026-05-10.json").write_text(
            '{"generatedAt":"2026-05-10T10:00:00Z","chains":{}}\n',
            encoding="utf-8",
        )
        (work / "data" / "earn-subaccount-history" / "manifest.json").write_text(
            json.dumps(
                {
                    "chains": {
                        "ethereum": {"lastBlock": 100, "updatedAt": "2026-05-10T10:00:00Z"},
                        "xlayer": {"lastBlock": 200, "updatedAt": "2026-05-10T10:00:00Z"},
                    }
                }
            )
            + "\n",
            encoding="utf-8",
        )
        _run(["git", "add", "."], cwd=work)
        _run(["git", "commit", "-m", "initial"], cwd=work)
        _run(["git", "remote", "add", "origin", str(remote)], cwd=work)
        _run(["git", "push", "origin", "master"], cwd=work)
        return remote, work

    def test_rebase_keeps_new_remote_canonical_entry_for_other_chain(self):
        with tempfile.TemporaryDirectory() as tmp:
            remote, work = self._prepare_repo(Path(tmp))
            manifest = work / "data" / "earn-subaccount-history" / "manifest.json"
            local_payload = json.loads(manifest.read_text(encoding="utf-8"))
            local_payload["chains"]["ethereum"] = {
                "lastBlock": 150,
                "updatedAt": "2026-05-10T11:00:00Z",
            }
            manifest.write_text(json.dumps(local_payload) + "\n", encoding="utf-8")
            _run(["git", "add", "data/earn-subaccount-history/manifest.json"], cwd=work)

            peer = Path(tmp) / "peer"
            _run(["git", "clone", str(remote), str(peer)], cwd=Path(tmp))
            _run(["git", "config", "user.name", "Remote Bot"], cwd=peer)
            _run(["git", "config", "user.email", "remote@example.invalid"], cwd=peer)
            remote_manifest = peer / "data" / "earn-subaccount-history" / "manifest.json"
            remote_payload = json.loads(remote_manifest.read_text(encoding="utf-8"))
            remote_payload["chains"]["xlayer"] = {
                "lastBlock": 250,
                "updatedAt": "2026-05-10T11:05:00Z",
            }
            remote_manifest.write_text(json.dumps(remote_payload) + "\n", encoding="utf-8")
            _run(["git", "add", "data/earn-subaccount-history/manifest.json"], cwd=peer)
            _run(["git", "commit", "-m", "refresh xlayer"], cwd=peer)
            _run(["git", "push", "origin", "master"], cwd=peer)

            env = {
                **dict(os.environ),
                "CHAIN": "ethereum",
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
            }
            _run(["bash", "scripts/commit_with_fresh_earn_status.sh", "refresh ethereum"], cwd=work, env=env)

            merged = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(150, merged["chains"]["ethereum"]["lastBlock"])
            self.assertEqual(250, merged["chains"]["xlayer"]["lastBlock"])

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
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
            }
            _run(["bash", "scripts/commit_with_fresh_earn_status.sh", "manifest only"], cwd=work, env=env)

            sync_args = (work / "sync-args.txt").read_text(encoding="utf-8").splitlines()
            self.assertIn("--chain", sync_args)
            self.assertIn("mantle", sync_args)
            self.assertNotIn("--all-chains", sync_args)

    def test_successful_push_dispatches_pages_when_token_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            _remote, work = self._prepare_repo(Path(tmp))
            status = work / "data" / "earn-freshness" / "status.json"
            status.write_text('{"status":"changed"}\n', encoding="utf-8")
            _run(["git", "add", "data/earn-freshness/status.json"], cwd=work)

            fake_bin = work / "fake-bin"
            fake_bin.mkdir()
            capture = work / "gh-args.txt"
            fake_gh = fake_bin / "gh"
            fake_gh.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$GH_CAPTURE\"\n",
                encoding="utf-8",
            )
            fake_gh.chmod(0o755)

            env = {
                **dict(os.environ),
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "true",
                "GH_TOKEN": "test-token",
                "GH_CAPTURE": str(capture),
                "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
            }
            _run(["bash", "scripts/commit_with_fresh_earn_status.sh", "status update"], cwd=work, env=env)

            gh_args = capture.read_text(encoding="utf-8").splitlines()
            self.assertEqual(gh_args, ["workflow", "run", "pages.yml", "--ref", "master"])

    def test_successful_producer_push_dispatches_freshness_monitor_without_direct_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            _remote, work = self._prepare_repo(Path(tmp))
            status = work / "data" / "earn-freshness" / "status.json"
            status.write_text('{"status":"changed"}\n', encoding="utf-8")
            _run(["git", "add", "data/earn-freshness/status.json"], cwd=work)

            fake_bin = work / "fake-bin"
            fake_bin.mkdir()
            capture = work / "gh-args.txt"
            fake_gh = fake_bin / "gh"
            fake_gh.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$GH_CAPTURE\"\n",
                encoding="utf-8",
            )
            fake_gh.chmod(0o755)

            env = {
                **dict(os.environ),
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_FRESHNESS_AFTER_PUSH": "true",
                "EARN_FRESHNESS_ALLOW_REMEDIATION_AFTER_PUSH": "false",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
                "GH_TOKEN": "test-token",
                "GH_CAPTURE": str(capture),
                "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
            }
            _run(["bash", "scripts/commit_with_fresh_earn_status.sh", "producer update"], cwd=work, env=env)

            gh_args = capture.read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                gh_args,
                [
                    "workflow",
                    "run",
                    "monitor-earn-freshness.yml",
                    "--ref",
                    "master",
                    "-f",
                    "allow_remediation=false",
                ],
            )

    def test_failed_freshness_dispatch_warns_after_successful_push(self):
        with tempfile.TemporaryDirectory() as tmp:
            remote, work = self._prepare_repo(Path(tmp))
            status = work / "data" / "earn-freshness" / "status.json"
            status.write_text('{"status":"changed"}\n', encoding="utf-8")
            _run(["git", "add", "data/earn-freshness/status.json"], cwd=work)

            fake_bin = work / "fake-bin"
            fake_bin.mkdir()
            fake_gh = fake_bin / "gh"
            fake_gh.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    if [[ "$1 $2" == "run list" ]]; then
                      echo 0
                      exit 0
                    fi
                    if [[ "$1 $2" == "workflow run" ]]; then
                      echo "simulated Actions outage" >&2
                      exit 1
                    fi
                    exit 2
                    """
                ),
                encoding="utf-8",
            )
            fake_gh.chmod(0o755)

            env = {
                **dict(os.environ),
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_FRESHNESS_AFTER_PUSH": "true",
                "EARN_FRESHNESS_ALLOW_REMEDIATION_AFTER_PUSH": "false",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
                "GH_TOKEN": "test-token",
                "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
            }
            result = subprocess.run(
                ["bash", "scripts/commit_with_fresh_earn_status.sh", "producer update"],
                cwd=str(work),
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            remote_subject = _run(
                ["git", f"--git-dir={remote}", "log", "-1", "--pretty=%s"],
                cwd=work,
            ).stdout.strip()
            self.assertEqual("producer update", remote_subject)
            self.assertIn("warning", result.stdout.lower())
            self.assertIn("freshness", result.stdout.lower())

    def test_queued_freshness_monitor_suppresses_duplicate_dispatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            _remote, work = self._prepare_repo(Path(tmp))
            status = work / "data" / "earn-freshness" / "status.json"
            status.write_text('{"status":"changed"}\n', encoding="utf-8")
            _run(["git", "add", "data/earn-freshness/status.json"], cwd=work)

            fake_bin = work / "fake-bin"
            fake_bin.mkdir()
            dispatched = work / "gh-dispatched"
            fake_gh = fake_bin / "gh"
            fake_gh.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    if [[ "$1 $2" == "run list" ]]; then
                      echo 1
                      exit 0
                    fi
                    if [[ "$1 $2" == "workflow run" ]]; then
                      touch "$GH_DISPATCHED"
                    fi
                    """
                ),
                encoding="utf-8",
            )
            fake_gh.chmod(0o755)

            env = {
                **dict(os.environ),
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_FRESHNESS_AFTER_PUSH": "true",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
                "GH_TOKEN": "test-token",
                "GH_DISPATCHED": str(dispatched),
                "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
            }
            result = _run(
                ["bash", "scripts/commit_with_fresh_earn_status.sh", "producer update"],
                cwd=work,
                env=env,
            )

            self.assertFalse(dispatched.exists(), result.stdout + result.stderr)
            self.assertIn("already queued", result.stdout)

    def test_no_data_change_still_publishes_freshness_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            _remote, work = self._prepare_repo(Path(tmp))

            env = {
                **dict(os.environ),
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
            }
            _run(
                ["bash", "scripts/commit_with_fresh_earn_status.sh", "status only"],
                cwd=work,
                env=env,
            )

            self.assertEqual(
                '{"status":"ok"}\n',
                (work / "data" / "earn-freshness" / "status.json").read_text(encoding="utf-8"),
            )
            latest_subject = _run(
                ["git", "log", "-1", "--pretty=%s"],
                cwd=work,
            ).stdout.strip()
            self.assertEqual("status only", latest_subject)

    def test_snapshot_change_during_rebase_rebuilds_staged_ledger_addresses(self):
        with tempfile.TemporaryDirectory() as tmp:
            remote, work = self._prepare_repo(Path(tmp))
            address = "0x" + "a" * 40
            ledger_dir = work / "data" / "earn-verified-ledger" / "mantle"

            for script_name in (
                "build_earn_resolved_interest_ledger.py",
                "build_earn_verified_ledger.py",
                "build_earn_verified_ledger_shards.py",
            ):
                (work / script_name).write_text(
                    textwrap.dedent(
                        f"""\
                        import sys
                        from pathlib import Path
                        args = sys.argv[1:]
                        addresses = ""
                        if "--address-file" in args:
                            addresses = Path(args[args.index("--address-file") + 1]).read_text(encoding="utf-8").strip()
                        with Path("rebuild-calls.txt").open("a", encoding="utf-8") as handle:
                            handle.write("{script_name} " + " ".join(args) + " addresses=" + addresses + "\\n")
                        """
                    ),
                    encoding="utf-8",
                )
            (work / "build_earn_representative_audit.py").write_text(
                'from pathlib import Path\nPath("audit-ran.txt").write_text("yes", encoding="utf-8")\n',
                encoding="utf-8",
            )
            _run(["git", "add", "build_earn_resolved_interest_ledger.py", "build_earn_verified_ledger.py", "build_earn_verified_ledger_shards.py", "build_earn_representative_audit.py"], cwd=work)
            _run(["git", "commit", "-m", "test builders"], cwd=work)
            _run(["git", "push", "origin", "master"], cwd=work)
            ledger_dir.mkdir()
            (ledger_dir / f"{address}.json").write_text('{"snapshotDate":"2026-05-10"}\n', encoding="utf-8")
            _run(["git", "add", "-f", str(ledger_dir / f"{address}.json")], cwd=work)

            peer = Path(tmp) / "peer"
            _run(["git", "clone", str(remote), str(peer)], cwd=Path(tmp))
            _run(["git", "config", "user.name", "Remote Bot"], cwd=peer)
            _run(["git", "config", "user.email", "remote@example.invalid"], cwd=peer)
            manifest = peer / "data" / "earn-snapshots" / "manifest.json"
            manifest.write_text('{"dates":["2026-05-11"]}\n', encoding="utf-8")
            _run(["git", "add", "data/earn-snapshots/manifest.json"], cwd=peer)
            _run(["git", "commit", "-m", "new snapshot"], cwd=peer)
            _run(["git", "push", "origin", "master"], cwd=peer)

            env = {
                **dict(os.environ),
                "CHAIN": "mantle",
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
            }
            _run(["bash", "scripts/commit_with_fresh_earn_status.sh", "ledger update"], cwd=work, env=env)

            calls = (work / "rebuild-calls.txt").read_text(encoding="utf-8")
            self.assertIn("build_earn_resolved_interest_ledger.py --chain mantle --address-file", calls)
            self.assertIn("build_earn_verified_ledger.py --chain mantle --address-file", calls)
            self.assertIn("build_earn_verified_ledger_shards.py --chain mantle --address-file", calls)
            self.assertIn(f"addresses={address}", calls)
            self.assertEqual("yes", (work / "audit-ran.txt").read_text(encoding="utf-8"))

    def test_same_day_snapshot_payload_change_rebuilds_staged_ledger_addresses(self):
        with tempfile.TemporaryDirectory() as tmp:
            remote, work = self._prepare_repo(Path(tmp))
            address = "0x" + "b" * 40
            ledger_dir = work / "data" / "earn-verified-ledger" / "mantle"

            for script_name in (
                "build_earn_resolved_interest_ledger.py",
                "build_earn_verified_ledger.py",
                "build_earn_verified_ledger_shards.py",
            ):
                (work / script_name).write_text(
                    textwrap.dedent(
                        f"""\
                        import sys
                        from pathlib import Path
                        args = sys.argv[1:]
                        addresses = ""
                        if "--address-file" in args:
                            addresses = Path(args[args.index("--address-file") + 1]).read_text(encoding="utf-8").strip()
                        with Path("rebuild-calls.txt").open("a", encoding="utf-8") as handle:
                            handle.write("{script_name} " + " ".join(args) + " addresses=" + addresses + "\\n")
                        """
                    ),
                    encoding="utf-8",
                )
            (work / "build_earn_representative_audit.py").write_text(
                'from pathlib import Path\nPath("audit-ran.txt").write_text("yes", encoding="utf-8")\n',
                encoding="utf-8",
            )
            _run(
                [
                    "git",
                    "add",
                    "build_earn_resolved_interest_ledger.py",
                    "build_earn_verified_ledger.py",
                    "build_earn_verified_ledger_shards.py",
                    "build_earn_representative_audit.py",
                ],
                cwd=work,
            )
            _run(["git", "commit", "-m", "test builders"], cwd=work)
            _run(["git", "push", "origin", "master"], cwd=work)

            ledger_dir.mkdir()
            ledger = ledger_dir / f"{address}.json"
            ledger.write_text('{"snapshotDate":"2026-05-10"}\n', encoding="utf-8")
            _run(["git", "add", "-f", str(ledger)], cwd=work)

            peer = Path(tmp) / "peer"
            _run(["git", "clone", str(remote), str(peer)], cwd=Path(tmp))
            _run(["git", "config", "user.name", "Remote Bot"], cwd=peer)
            _run(["git", "config", "user.email", "remote@example.invalid"], cwd=peer)
            snapshot = peer / "data" / "earn-snapshots" / "2026-05-10.json"
            snapshot.write_text(
                '{"generatedAt":"2026-05-10T11:00:00Z","chains":{"mantle":{}}}\n',
                encoding="utf-8",
            )
            _run(["git", "add", "data/earn-snapshots/2026-05-10.json"], cwd=peer)
            _run(["git", "commit", "-m", "refresh same-day snapshot"], cwd=peer)
            _run(["git", "push", "origin", "master"], cwd=peer)

            env = {
                **dict(os.environ),
                "CHAIN": "mantle",
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
            }
            _run(
                ["bash", "scripts/commit_with_fresh_earn_status.sh", "ledger update"],
                cwd=work,
                env=env,
            )

            calls = (work / "rebuild-calls.txt").read_text(encoding="utf-8")
            self.assertIn("build_earn_resolved_interest_ledger.py --chain mantle --address-file", calls)
            self.assertIn("build_earn_verified_ledger.py --chain mantle --address-file", calls)
            self.assertIn("build_earn_verified_ledger_shards.py --chain mantle --address-file", calls)
            self.assertIn(f"addresses={address}", calls)
            self.assertEqual("yes", (work / "audit-ran.txt").read_text(encoding="utf-8"))

    def test_rebase_preserves_fresh_local_ledger_deletion_after_remote_modification(self):
        with tempfile.TemporaryDirectory() as tmp:
            remote, work = self._prepare_repo(Path(tmp))
            address = "0x" + "d" * 40
            ledger = work / "data" / "earn-resolved-interest-ledger" / "mantle" / f"{address}.json"
            ledger.parent.mkdir()
            ledger.write_text('{"snapshotDate":"2026-05-10","source":"initial"}\n', encoding="utf-8")
            _run(["git", "add", "-f", str(ledger)], cwd=work)
            _run(["git", "commit", "-m", "seed resolved ledger"], cwd=work)
            _run(["git", "push", "origin", "master"], cwd=work)

            _run(["git", "rm", str(ledger)], cwd=work)

            peer = Path(tmp) / "peer"
            _run(["git", "clone", str(remote), str(peer)], cwd=Path(tmp))
            _run(["git", "config", "user.name", "Remote Bot"], cwd=peer)
            _run(["git", "config", "user.email", "remote@example.invalid"], cwd=peer)
            peer_ledger = peer / "data" / "earn-resolved-interest-ledger" / "mantle" / f"{address}.json"
            peer_ledger.write_text(
                '{"snapshotDate":"2026-05-10","source":"parallel-workflow"}\n',
                encoding="utf-8",
            )
            _run(["git", "add", "-f", str(peer_ledger)], cwd=peer)
            _run(["git", "commit", "-m", "parallel ledger refresh"], cwd=peer)
            _run(["git", "push", "origin", "master"], cwd=peer)

            env = {
                **dict(os.environ),
                "CHAIN": "mantle",
                "EARN_PUSH_ATTEMPTS": "1",
                "EARN_GIT_REMOTE": "origin",
                "EARN_GIT_BRANCH": "master",
                "EARN_DISPATCH_PAGES_AFTER_PUSH": "false",
            }
            _run(["bash", "scripts/commit_with_fresh_earn_status.sh", "fresh snapshot"], cwd=work, env=env)

            verification = Path(tmp) / "verification"
            _run(["git", "clone", str(remote), str(verification)], cwd=Path(tmp))
            self.assertFalse(
                (verification / "data" / "earn-resolved-interest-ledger" / "mantle" / f"{address}.json").exists()
            )


if __name__ == "__main__":
    unittest.main()
