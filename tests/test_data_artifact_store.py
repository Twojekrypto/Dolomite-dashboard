"""LP storage contract: real validator and local files, only S3 is replaced."""
import hashlib
import importlib.util
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from tests.test_validate_dolo_liquidity import valid_payload

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/data_artifact_store.py"
if SCRIPT.exists():
    from scripts import data_artifact_store as storage
else:
    storage = None


class S3Error(Exception):
    def __init__(self, code):
        super().__init__("SECRET endpoint credential bucket must never escape")
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.fail_key = None
        self.before_put = None

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise S3Error("NoSuchKey")
        body = self.objects[Key]
        return {"Body": io.BytesIO(body), "ETag": '"' + hashlib.md5(body).hexdigest() + '"'}

    def put_object(self, *, Bucket, Key, Body, ContentType, IfMatch=None, IfNoneMatch=None):
        if self.before_put:
            hook, self.before_put = self.before_put, None
            hook(Key)
        if Key == self.fail_key:
            raise S3Error("AccessDenied")
        if IfNoneMatch == "*" and Key in self.objects:
            raise S3Error("PreconditionFailed")
        if IfMatch is not None:
            if Key not in self.objects or self.get_object(Bucket=Bucket, Key=Key)["ETag"] != IfMatch:
                raise S3Error("PreconditionFailed")
        self.objects[Key] = bytes(Body)
        return {"ETag": self.get_object(Bucket=Bucket, Key=Key)["ETag"]}


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(storage, "LP storage publish/restore implementation is missing")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "dolo-liquidity.json"
        self.out = Path(self.temp.name) / "restored.json"
        self.s3 = FakeS3()
        self.store = storage.ArtifactStore(self.s3, "private-bucket", "scope-identity")
        self.now = datetime.now(timezone.utc) - timedelta(minutes=2)

    def payload(self, age=0, block=10):
        payload = valid_payload()
        payload["generatedAt"] = (self.now + timedelta(seconds=age)).isoformat()
        for source in payload["sources"]:
            source["lastScannedBlock"] = block
            source["latestChainBlock"] = block
        return payload

    def write(self, payload=None):
        self.path.write_text(json.dumps(payload or self.payload()), encoding="utf-8")
        return self.path

    def publish(self, age=0, block=10):
        return self.store.publish(self.write(self.payload(age, block)))

    def test_round_trip_preserves_exact_bytes_and_previous_versions(self):
        first = self.publish()
        expected = self.path.read_bytes()
        second = self.publish(1, 11)
        self.store.restore(self.out, digest=first, purpose="rollback")
        self.assertEqual(expected, self.out.read_bytes())
        self.store.restore(self.out, purpose="current")
        self.assertEqual(self.path.read_bytes(), self.out.read_bytes())
        self.assertNotEqual(first, second)

    def test_corrupt_object_cannot_replace_local_known_good(self):
        digest = self.publish()
        self.out.write_bytes(b"known-good")
        self.s3.objects[f"lp/v1/objects/{digest}.json"] = b"corrupt"
        with self.assertRaises(storage.StorageError):
            self.store.restore(self.out, purpose="current")
        self.assertEqual(self.out.read_bytes(), b"known-good")

    def test_manifest_size_and_payload_validation_are_checked_after_download(self):
        digest = self.publish()
        manifest_key = f"lp/v1/versions/{digest}.json"
        manifest = json.loads(self.s3.objects[manifest_key])
        manifest["size"] += 1
        self.s3.objects[manifest_key] = json.dumps(manifest).encode()
        with self.assertRaises(storage.StorageError):
            self.store.restore(self.out)
        broken = self.payload()
        broken["summary"]["lpWallets"] = 99
        body = json.dumps(broken).encode()
        digest = hashlib.sha256(body).hexdigest()
        manifest.update(sha256=digest, size=len(body))
        self.s3.objects[f"lp/v1/versions/{digest}.json"] = json.dumps(manifest).encode()
        self.s3.objects[f"lp/v1/objects/{digest}.json"] = body
        with self.assertRaises(storage.StorageError):
            self.store.restore(self.out, digest=digest, purpose="rollback")
        self.assertFalse(self.out.exists())

    def test_interrupted_upload_never_advances_active_manifest(self):
        self.publish()
        before = self.s3.objects["lp/v1/active.json"]
        self.write(self.payload(1, 11))
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        self.s3.fail_key = f"lp/v1/versions/{digest}.json"
        with self.assertRaises(storage.StorageError) as caught:
            self.store.publish(self.path)
        self.assertNotIn("SECRET", str(caught.exception))
        self.assertEqual(before, self.s3.objects["lp/v1/active.json"])

    def test_missing_empty_and_invalid_files_never_bootstrap(self):
        for body in (None, b"", b"{}", b"not json"):
            if body is not None:
                self.path.write_bytes(body)
            with self.assertRaises(storage.StorageError):
                self.store.publish(self.path, bootstrap=True)
        self.assertEqual({}, self.s3.objects)

    def test_real_validator_blocks_failed_totals_before_upload(self):
        payload = self.payload()
        payload["summary"]["lpWallets"] = 99
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.write(payload))
        self.assertEqual({}, self.s3.objects)

    def test_stale_bootstrap_requires_explicit_resume_and_never_serves_current(self):
        self.write(self.payload(-9 * 3600))
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.path, bootstrap=True)
        digest = self.store.publish(self.path, bootstrap=True, purpose="resume")
        self.store.restore(self.out, purpose="resume")
        self.assertEqual(self.path.read_bytes(), self.out.read_bytes())
        with self.assertRaises(storage.StorageError):
            self.store.restore(self.out, purpose="current")
        with self.assertRaises(storage.StorageError):
            self.store.verify(digest, mark_ready=True)
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.path, purpose="resume")

    def test_older_time_cursor_and_missing_source_publications_are_rejected(self):
        self.publish(1, 11)
        before = self.s3.objects["lp/v1/active.json"]
        for payload in (self.payload(0, 12), self.payload(2, 10)):
            with self.assertRaises(storage.StorageError):
                self.store.publish(self.write(payload))
        payload = self.payload(2, 12)
        payload["sources"].pop()
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.write(payload))
        self.assertEqual(before, self.s3.objects["lp/v1/active.json"])

    def test_concurrent_publish_rejects_loser_even_when_source_is_newer(self):
        self.publish()
        other = Path(self.temp.name) / "other.json"
        other.write_text(json.dumps(self.payload(1, 11)))
        self.write(self.payload(2, 12))
        winner = []
        self.s3.before_put = lambda key: winner.append(self.store.publish(other))
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.path)
        self.store.restore(self.out, purpose="current")
        self.assertEqual(other.read_bytes(), self.out.read_bytes())

    def test_bootstrap_cannot_replace_existing_active(self):
        self.publish()
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.write(self.payload(1)), bootstrap=True)

    def test_corrupt_empty_active_manifest_cannot_be_overwritten(self):
        self.s3.objects["lp/v1/active.json"] = b"{}"
        with self.assertRaises(storage.StorageError):
            self.publish()
        self.assertEqual(self.s3.objects["lp/v1/active.json"], b"{}")

    def test_resume_never_accepts_a_future_timestamp(self):
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.write(self.payload(3600)), bootstrap=True, purpose="resume")
        self.assertEqual({}, self.s3.objects)

    def test_two_first_writers_cannot_both_initialize_active(self):
        other = Path(self.temp.name) / "other.json"
        other.write_text(json.dumps(self.payload(1, 11)))
        self.write()
        self.s3.before_put = lambda key: self.store.publish(other, bootstrap=True)
        with self.assertRaises(storage.StorageError):
            self.store.publish(self.path, bootstrap=True)
        self.store.restore(self.out)
        self.assertEqual(self.out.read_bytes(), other.read_bytes())

    def test_failed_rollback_cas_never_mints_rollback_proof(self):
        first = self.publish()
        second = self.publish(1, 11)
        self.s3.before_put = lambda key: self.publish(2, 12)
        with self.assertRaises(storage.StorageError):
            self.store.rollback(first, reason="pilot-drill-123")
        self.assertNotIn(f"lp/v1/rollback-proofs/{second}/{first}.json", self.s3.objects)
        self.store.restore(self.out)
        self.assertEqual(self.out.read_bytes(), self.path.read_bytes())

    def test_shadow_publish_and_gated_r2_prepare_use_real_storage(self):
        self.write()
        before = self.path.read_bytes()
        with patch.dict(os.environ, {"LP_DATA_STORAGE": "shadow"}, clear=True), patch.object(storage, "configured_store", return_value=self.store), redirect_stdout(io.StringIO()):
            self.assertEqual(storage.main(["publish", "--path", str(self.path)]), 0)
        self.assertEqual(before, self.path.read_bytes())
        first = hashlib.sha256(before).hexdigest()
        second = self.publish(1, 11)
        self.store.rollback(first, reason="pilot-drill-123")
        self.store.publish(self.path)
        self.store.verify(second, mark_ready=True, rollback_digest=first)
        with patch.dict(os.environ, {"LP_DATA_STORAGE": "r2", "LP_R2_READY_DIGEST": second}, clear=True), patch.object(storage, "configured_store", return_value=self.store), redirect_stdout(io.StringIO()):
            self.assertEqual(storage.main(["prepare", "--path", str(self.out)]), 0)
        self.assertEqual(self.path.read_bytes(), self.out.read_bytes())

    def test_rollback_requires_reason_and_preserves_audited_version(self):
        first = self.publish()
        second = self.publish(1, 11)
        with self.assertRaises(storage.StorageError):
            self.store.rollback(first, reason="")
        self.store.rollback(first, reason="pilot-drill-123")
        self.store.restore(self.out, purpose="current")
        self.assertEqual(json.loads(self.out.read_bytes())["sources"][0]["lastScannedBlock"], 10)
        self.store.restore(self.out, digest=second, purpose="rollback")
        self.assertEqual(self.path.read_bytes(), self.out.read_bytes())
        self.store.publish(self.path)
        audits = [json.loads(body) for key, body in self.s3.objects.items() if key.startswith("lp/v1/rollback-audit/")]
        self.assertEqual(len(audits), 1)
        self.assertEqual(audits[0]["reason"], "pilot-drill-123")
        self.assertEqual(audits[0]["previousDigest"], second)
        self.assertEqual(audits[0]["sha256"], first)

    def test_readiness_requires_round_trip_and_real_rollback_in_same_scope(self):
        first = self.publish()
        second = self.publish(1, 11)
        with self.assertRaises(storage.StorageError):
            self.store.require_ready(second)
        with self.assertRaises(storage.StorageError):
            self.store.verify(second, mark_ready=True, rollback_digest=first)
        self.store.rollback(first, reason="pilot-drill-123")
        self.store.publish(self.path)
        self.store.verify(second, mark_ready=True, rollback_digest=first)
        self.store.require_ready(second)
        foreign = storage.ArtifactStore(self.s3, "private-bucket", "different-endpoint")
        with self.assertRaises(storage.StorageError):
            foreign.require_ready(second)

    def test_old_readiness_proof_remains_valid_but_current_data_must_stay_fresh(self):
        first = self.publish()
        second = self.publish(1, 11)
        self.store.rollback(first, reason="pilot-drill-123")
        self.store.publish(self.path)
        self.store.verify(second, mark_ready=True, rollback_digest=first)

        class Later(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(hours=9)

        with patch("validate_data.datetime", Later), patch.object(storage, "datetime", Later):
            self.store.require_ready(second)
            with self.assertRaises(storage.StorageError):
                self.store.restore(self.out, purpose="current")
            self.publish(9 * 3600, 12)
            self.store.require_ready(second)
            self.store.restore(self.out, purpose="current")
            self.assertEqual(self.path.read_bytes(), self.out.read_bytes())

    def test_explicit_old_digest_requires_rollback_or_resume_purpose(self):
        first = self.publish()
        self.publish(1, 11)
        with self.assertRaises(storage.StorageError):
            self.store.restore(self.out, digest=first, purpose="current")

    def test_missing_config_and_unauthorized_are_sanitized(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(storage.StorageError):
                storage.configured_store()
        self.s3.get_object = lambda **kwargs: (_ for _ in ()).throw(S3Error("AccessDenied"))
        with self.assertRaises(storage.StorageError) as caught:
            self.store.restore(self.out)
        self.assertNotIn("SECRET", str(caught.exception))


class ModeTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(storage, "LP storage mode integration is missing")
        self.stdout = io.StringIO()
        self.stderr = io.StringIO()
        # Keep expected failure diagnostics out of the test runner output.
        self.output_context = redirect_stdout(self.stdout)
        self.error_context = redirect_stderr(self.stderr)
        self.output_context.__enter__()
        self.error_context.__enter__()
        self.addCleanup(self.output_context.__exit__, None, None, None)
        self.addCleanup(self.error_context.__exit__, None, None, None)

    def test_default_git_needs_no_sdk_credentials_or_generated_file(self):
        for command in ("prepare", "publish", "check-mode"):
            with patch.dict(os.environ, {}, clear=True), patch.object(storage, "configured_store", side_effect=AssertionError("R2 accessed")):
                self.assertEqual(storage.main([command]), 0)
        result = subprocess.run([sys.executable, "-S", str(SCRIPT), "prepare"],
                                env={"LP_DATA_STORAGE": "git"}, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_shadow_prepare_does_not_replace_git_production(self):
        with patch.dict(os.environ, {"LP_DATA_STORAGE": "shadow"}, clear=True), patch.object(storage, "configured_store", side_effect=AssertionError("R2 accessed")):
            self.assertEqual(storage.main(["prepare"]), 0)

    def test_unknown_mode_and_r2_without_proof_fail_closed(self):
        for mode in ("typo", "r2"):
            with patch.dict(os.environ, {"LP_DATA_STORAGE": mode}, clear=True):
                self.assertEqual(storage.main(["prepare"]), 1)

    def test_guard_rejects_only_lp_changes_after_cutover(self):
        self.assertFalse(storage.lp_commit_allowed("r2", ["data/dolo-liquidity.json"]))
        self.assertTrue(storage.lp_commit_allowed("r2", ["data/dolo-liquidity-pools.json"]))
        self.assertTrue(storage.lp_commit_allowed("git", ["data/dolo-liquidity.json"]))
        self.assertTrue(storage.lp_commit_allowed("shadow", ["data/dolo-liquidity.json"]))

    @unittest.skipUnless(importlib.util.find_spec("boto3"), "optional storage SDK not installed")
    def test_real_sdk_accepts_conditional_put_and_uses_r2_checksum_config(self):
        from botocore.stub import Stubber
        config = {"R2_ENDPOINT_URL": "https://example.invalid", "R2_ACCESS_KEY_ID": "test-key",
                  "R2_SECRET_ACCESS_KEY": "test-secret", "R2_BUCKET": "test-bucket"}
        with patch.dict(os.environ, config, clear=True):
            store = storage.configured_store()
        self.assertEqual(store.client.meta.config.request_checksum_calculation, "when_required")
        self.assertEqual(store.client.meta.config.response_checksum_validation, "when_required")
        with Stubber(store.client) as stub:
            for condition in ({"IfNoneMatch": "*"}, {"IfMatch": '"prior-etag"'}):
                params = {"Bucket": "test-bucket", "Key": "lp/v1/active.json", "Body": b"{}",
                          "ContentType": "application/json", **condition}
                stub.add_response("put_object", {"ETag": '"new-etag"'}, params)
                store._put("active.json", b"{}", etag=condition.get("IfMatch"))
            stub.assert_no_pending_responses()


def workflow_run_block(filename, step_name):
    text = (ROOT / ".github/workflows" / filename).read_text()
    marker = f"      - name: {step_name}\n"
    if marker not in text:
        raise AssertionError(f"Missing workflow integration step: {step_name}")
    step = text.split(marker, 1)[1].split("\n      - name:", 1)[0]
    run = step.split("        run: |\n", 1)[1]
    return "\n".join(line[10:] for line in run.splitlines() if line.startswith("          "))


class WorkflowModeTests(unittest.TestCase):
    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.repo, text=True, capture_output=True, check=True).stdout

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Storage test")
        (self.repo / "data").mkdir()
        for name in ("data/dolo-liquidity.json", "data/dolo-liquidity-pools.json", "unrelated.txt"):
            (self.repo / name).write_text("baseline")
        self.git("add", ".")
        self.git("commit", "-qm", "baseline")

    def execute(self, code, mode):
        subprocess.run(["bash", "-e", "-c", code], cwd=self.repo,
                       env={**os.environ, "LP_DATA_STORAGE": mode}, check=True, capture_output=True)

    def test_publication_stages_git_and_shadow_but_only_registry_in_r2(self):
        code = workflow_run_block("update-dolo-liquidity.yml", "Stage LP publication files")
        for mode in ("git", "shadow", "r2"):
            with self.subTest(mode=mode):
                for name in ("data/dolo-liquidity.json", "data/dolo-liquidity-pools.json", "unrelated.txt"):
                    (self.repo / name).write_text("generated")
                self.execute(code, mode)
                staged = self.git("diff", "--cached", "--name-only").splitlines()
                self.assertIn("data/dolo-liquidity-pools.json", staged)
                self.assertEqual("data/dolo-liquidity.json" in staged, mode != "r2")
                self.assertEqual((self.repo / "unrelated.txt").read_text(), "generated")
                if mode == "r2":
                    self.assertEqual((self.repo / "data/dolo-liquidity.json").read_text(), "baseline")
                self.git("restore", "--staged", ".")

    def test_untracked_future_cutover_does_not_stage_or_delete_lp(self):
        code = workflow_run_block("update-dolo-liquidity.yml", "Stage LP publication files")
        self.git("rm", "--cached", "data/dolo-liquidity.json")
        self.git("commit", "-qm", "explicit reviewed migration")
        (self.repo / "data/dolo-liquidity.json").write_text("r2-restored")
        self.execute(code, "r2")
        self.assertEqual(self.git("diff", "--cached", "--name-only"), "")
        self.assertEqual((self.repo / "data/dolo-liquidity.json").read_text(), "r2-restored")

    def test_flow_cleanup_restores_only_tracked_lp_before_rebase(self):
        code = workflow_run_block("update-dolo-flows.yml", "Clean restored LP checkout copy")
        (self.repo / "data/dolo-liquidity.json").write_text("r2-restored")
        (self.repo / "unrelated.txt").write_text("new-flow-data")
        self.execute(code, "r2")
        self.assertEqual((self.repo / "data/dolo-liquidity.json").read_text(), "baseline")
        self.assertEqual((self.repo / "unrelated.txt").read_text(), "new-flow-data")

    def test_real_git_changed_path_guard_rejects_only_lp_publication(self):
        for path, expected in (("data/dolo-liquidity.json", 1), ("data/dolo-liquidity-pools.json", 0)):
            base = self.git("rev-parse", "HEAD").strip()
            (self.repo / path).write_text("new data")
            self.git("add", path)
            self.git("commit", "-qm", "data update")
            result = subprocess.run([sys.executable, str(SCRIPT), "guard-git", "--base", base],
                                    cwd=self.repo, env={**os.environ, "LP_DATA_STORAGE": "r2"},
                                    capture_output=True)
            self.assertEqual(result.returncode, expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
