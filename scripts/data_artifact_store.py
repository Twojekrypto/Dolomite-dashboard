#!/usr/bin/env python3
"""Private LP artifact pilot. Git remains authoritative unless explicitly gated.

No boto3 import/configuration is needed in default Git mode. Objects and version
manifests are immutable; only active.json is mutable and always uses S3 CAS.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from urllib.parse import urlsplit
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
PREFIX = "lp/v1"
LP_PATH = "data/dolo-liquidity.json"


class StorageError(Exception):
    """Only fixed, secret-free messages may cross the storage boundary."""


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def checked_digest(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise StorageError("A full SHA256 digest is required")
    return value


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed
    except (ValueError, TypeError, AttributeError):
        raise StorageError("Invalid LP timestamp") from None


def validate_payload(body, purpose):
    # Reuse every LP contract without validate_file's missing-file skip or output.
    # Only explicit historical operations omit freshness/current-publication gates.
    from lp_publication import source_refresh_failed
    from validate_data import RULES
    if purpose not in ("current", "resume", "rollback"):
        raise StorageError("Invalid validation purpose")
    rules = RULES["dolo-liquidity.json"]
    try:
        data = json.loads(body)
        valid = isinstance(data, dict) and len(body) >= rules["min_bytes"]
        valid = valid and all(key in data for key in rules["required_keys"])
        for name, check in rules["checks"]:
            if purpose != "current" and name == "generatedAt must be fresh":
                continue
            valid = valid and check(data)
        if not valid:
            raise ValueError()
        if purpose == "current" and any(source_refresh_failed(source) for source in data["sources"]):
            raise ValueError()
        if timestamp(data["generatedAt"]) > datetime.now(timezone.utc):
            raise ValueError()
        return data
    except Exception:
        raise StorageError("LP artifact validation failed") from None


def manifest_for(body, data):
    return {
        "schemaVersion": 1,
        "sha256": hashlib.sha256(body).hexdigest(),
        "size": len(body),
        "generatedAt": data["generatedAt"],
        "sourceCursors": {row["key"]: row["lastScannedBlock"] for row in data["sources"]},
    }


class ArtifactStore:
    def __init__(self, client, bucket, scope):
        self.client = client
        self.bucket = bucket
        self.scope = hashlib.sha256(encode([scope, bucket, PREFIX])).hexdigest()

    def _get(self, key, optional=False):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=f"{PREFIX}/{key}")
            return response["Body"].read(), response["ETag"]
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if optional and code in ("NoSuchKey", "404", "NotFound"):
                return None, None
            raise StorageError("Storage read failed; check access and object availability") from None

    def _put(self, key, body, *, etag=None, immutable=False):
        condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
        try:
            self.client.put_object(Bucket=self.bucket, Key=f"{PREFIX}/{key}",
                                   Body=body, ContentType="application/json", **condition)
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if immutable and code in ("PreconditionFailed", "412"):
                previous, _ = self._get(key)
                if previous == body:
                    return
            raise StorageError("Storage write failed or concurrent publication rejected") from None

    def _json(self, key, optional=False):
        body, etag = self._get(key, optional)
        if body is None:
            return None, etag
        try:
            data = json.loads(body)
            if not isinstance(data, dict):
                raise ValueError()
            return data, etag
        except (ValueError, TypeError):
            raise StorageError("Storage manifest is invalid") from None

    def _active(self, *, allow_missing=False):
        active, etag = self._json("active.json", optional=allow_missing)
        if active is None:
            return None, etag
        try:
            if set(active) != {"schemaVersion", "sha256", "publicationId", "publishedAt",
                               "operation", "previousDigest", "reason"}:
                raise ValueError()
            if type(active["schemaVersion"]) is not int or active["schemaVersion"] != 1:
                raise ValueError()
            checked_digest(active["sha256"])
            if not re.fullmatch(r"[0-9a-f]{32}", active["publicationId"]):
                raise ValueError()
            if timestamp(active["publishedAt"]) > datetime.now(timezone.utc):
                raise ValueError()
            operation = active["operation"]
            if operation not in ("bootstrap", "publish", "rollback"):
                raise ValueError()
            if operation == "bootstrap":
                if active["previousDigest"] is not None:
                    raise ValueError()
            else:
                checked_digest(active["previousDigest"])
            if operation == "rollback":
                if not re.fullmatch(r"[A-Za-z0-9_-]{3,80}", active["reason"]):
                    raise ValueError()
            elif active["reason"] != "":
                raise ValueError()
        except (StorageError, ValueError, TypeError):
            raise StorageError("Active LP pointer schema is invalid") from None
        return active, etag

    def _snapshot(self, digest, purpose):
        digest = checked_digest(digest)
        manifest, _ = self._json(f"versions/{digest}.json")
        body, _ = self._get(f"objects/{digest}.json")
        if manifest.get("sha256") != digest or hashlib.sha256(body).hexdigest() != digest or manifest.get("size") != len(body):
            raise StorageError("LP artifact hash or size mismatch")
        data = validate_payload(body, purpose)
        if manifest != manifest_for(body, data):
            raise StorageError("LP artifact metadata mismatch")
        return manifest, body

    def _activate(self, manifest, previous, etag, operation, reason=""):
        # Unique publication ID prevents an A->B->A pointer ABA race.
        active = {"schemaVersion": 1, "sha256": manifest["sha256"],
                  "publicationId": uuid4().hex, "publishedAt": datetime.now(timezone.utc).isoformat(),
                  "operation": operation, "previousDigest": previous.get("sha256") if previous else None,
                  "reason": reason}
        self._put("active.json", encode(active), etag=etag)
        return active

    def publish(self, path, *, bootstrap=False, purpose="current"):
        if purpose != "current" and not (bootstrap and purpose == "resume"):
            raise StorageError("Only explicit recovery bootstrap may publish an old baseline")
        try:
            body = Path(path).read_bytes()
        except OSError:
            raise StorageError("LP artifact is missing or unreadable") from None
        data = validate_payload(body, purpose)
        manifest = manifest_for(body, data)
        previous, etag = self._active(allow_missing=bootstrap)
        if bootstrap and previous is not None:
            raise StorageError("Bootstrap requires an empty LP store")
        if previous is not None:
            old, _ = self._snapshot(previous.get("sha256"), "resume")
            cursors = manifest["sourceCursors"]
            if timestamp(manifest["generatedAt"]) < timestamp(old["generatedAt"]) or any(
                key not in cursors or cursors[key] < value for key, value in old["sourceCursors"].items()
            ):
                raise StorageError("Publication would regress LP source data")
            if manifest["sha256"] != old["sha256"] and timestamp(manifest["generatedAt"]) == timestamp(old["generatedAt"]):
                raise StorageError("Different payloads cannot share a publication timestamp")
        digest = manifest["sha256"]
        self._put(f"objects/{digest}.json", body, immutable=True)
        self._put(f"versions/{digest}.json", encode(manifest), immutable=True)
        self._snapshot(digest, purpose)  # Verify remote bytes before active pointer CAS.
        self._activate(manifest, previous, etag, "bootstrap" if bootstrap else "publish")
        return digest

    def restore(self, path, *, digest=None, purpose="current"):
        if digest is not None and purpose == "current":
            raise StorageError("Explicit version restore requires resume or rollback purpose")
        if digest is None:
            active, _ = self._active()
            digest = active.get("sha256")
        _, body = self._snapshot(digest, purpose)
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Temporary files never contain config and never survive failed restoration.
        staging = None
        try:
            with tempfile.NamedTemporaryFile(dir=destination.parent, prefix=".lp-restore-", delete=False) as stream:
                staging = Path(stream.name)
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            validate_payload(staging.read_bytes(), purpose)
            os.replace(staging, destination)
        finally:
            if staging is not None:
                staging.unlink(missing_ok=True)
        return digest

    def rollback(self, digest, *, reason):
        # Reason is an audit ticket identifier, not free-form text that might leak secrets.
        if not re.fullmatch(r"[A-Za-z0-9_-]{3,80}", reason or ""):
            raise StorageError("Rollback requires an audit ticket identifier")
        previous, etag = self._active()
        manifest, _ = self._snapshot(digest, "rollback")
        if previous.get("sha256") == digest:
            raise StorageError("Rollback must select a different known-good version")
        self._snapshot(previous.get("sha256"), "resume")
        active = self._activate(manifest, previous, etag, "rollback", reason)
        # Keep the audit ticket after later normal publication replaces active.json.
        self._put(f"rollback-audit/{active['publicationId']}.json", encode(active), immutable=True)
        # This receipt only exists after a successful CAS rollback. No deletion.
        proof = {"schemaVersion": 1, "scope": self.scope,
                 "fromDigest": previous["sha256"], "toDigest": digest}
        key = f"rollback-proofs/{previous['sha256']}/{digest}.json"
        self._put(key, encode(proof), immutable=True)
        return digest

    def verify(self, digest=None, *, mark_ready=False, rollback_digest=None):
        active, etag = self._active()
        digest = checked_digest(digest or active.get("sha256"))
        manifest, body = self._snapshot(digest, "current")
        with tempfile.TemporaryDirectory(prefix="lp-storage-verify-") as directory:
            destination = Path(directory) / "dolo-liquidity.json"
            self.restore(destination, digest=digest, purpose="rollback")
            if destination.read_bytes() != body:
                raise StorageError("LP round-trip verification failed")
            if mark_ready:
                # Require evidence of a real B->A rollback followed by B republish.
                rollback_digest = checked_digest(rollback_digest)
                proof, _ = self._json(f"rollback-proofs/{digest}/{rollback_digest}.json")
                expected = {"schemaVersion": 1, "scope": self.scope,
                            "fromDigest": digest, "toDigest": rollback_digest}
                if proof != expected:
                    raise StorageError("Rollback proof does not match this storage scope")
                self.restore(destination, digest=rollback_digest, purpose="rollback")
        if mark_ready:
            current, current_etag = self._active()
            if active.get("sha256") != digest or current_etag != etag:
                raise StorageError("Active version changed during readiness verification")
            marker = {"schemaVersion": 1, "scope": self.scope, "sha256": digest,
                      "rollbackDigest": rollback_digest, "size": manifest["size"]}
            self._put(f"readiness/{digest}.json", encode(marker), immutable=True)
        return digest

    def require_ready(self, digest):
        self._active()
        digest = checked_digest(digest)
        marker, _ = self._json(f"readiness/{digest}.json")
        manifest, _ = self._snapshot(digest, "resume")
        rollback_digest = checked_digest(marker.get("rollbackDigest"))
        expected = {"schemaVersion": 1, "scope": self.scope, "sha256": digest,
                    "rollbackDigest": rollback_digest, "size": manifest["size"]}
        if marker != expected:
            raise StorageError("LP readiness marker does not match this storage scope")
        self._snapshot(rollback_digest, "rollback")


def configured_store():
    names = ("R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
    values = [os.environ.get(name, "").strip() for name in names]
    if not all(values):
        raise StorageError("R2 configuration is incomplete; activation is blocked")
    endpoint, access, secret, bucket = values
    url = urlsplit(endpoint)
    if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise StorageError("Invalid R2 endpoint configuration")
    try:
        import boto3
        from botocore.config import Config
        client = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access,
                              aws_secret_access_key=secret, region_name="auto",
                              config=Config(signature_version="s3v4", retries={"max_attempts": 3},
                                            connect_timeout=10, read_timeout=60,
                                            request_checksum_calculation="when_required",
                                            response_checksum_validation="when_required"))
        return ArtifactStore(client, bucket, endpoint.rstrip("/"))
    except Exception:
        raise StorageError("Cannot initialize storage SDK; install requirements-storage.txt") from None


def storage_mode():
    mode = os.environ.get("LP_DATA_STORAGE", "git") or "git"
    if mode not in ("git", "shadow", "r2"):
        raise StorageError("LP_DATA_STORAGE must be git, shadow, or r2")
    return mode


def lp_commit_allowed(mode, changed_paths):
    return mode != "r2" or LP_PATH not in changed_paths


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check-mode", "prepare", "publish", "bootstrap", "restore", "verify", "rollback", "guard-git"))
    parser.add_argument("--path", default=LP_PATH)
    parser.add_argument("--purpose", choices=("current", "resume", "rollback"), default="current")
    parser.add_argument("--digest")
    parser.add_argument("--rollback-digest")
    parser.add_argument("--mark-ready", action="store_true")
    parser.add_argument("--reason", default="")
    parser.add_argument("--base")
    parser.add_argument("--head", default="HEAD")
    args = parser.parse_args(argv)
    try:
        mode = storage_mode()
        if args.command == "check-mode":
            print(f"LP storage mode: {mode}")
            return 0
        if args.command == "guard-git":
            if not args.base:
                raise StorageError("Git guard requires a base revision")
            diff = subprocess.run(["git", "diff", "--name-only", "--no-renames", args.base, args.head, "--", LP_PATH],
                                  check=True, capture_output=True, text=True)
            if not lp_commit_allowed(mode, diff.stdout.splitlines()):
                raise StorageError("LP generated JSON changes are forbidden after R2 cutover")
            return 0
        if (args.command in ("prepare", "publish") and mode == "git") or (args.command == "prepare" and mode == "shadow"):
            return 0
        store = configured_store()
        if mode == "r2":
            store.require_ready(os.environ.get("LP_R2_READY_DIGEST"))
        if args.command == "prepare":
            digest = store.restore(args.path, purpose=args.purpose)
        elif args.command in ("publish", "bootstrap"):
            digest = store.publish(args.path, bootstrap=args.command == "bootstrap", purpose=args.purpose)
        elif args.command == "restore":
            digest = store.restore(args.path, digest=args.digest, purpose=args.purpose)
        elif args.command == "verify":
            digest = store.verify(args.digest, mark_ready=args.mark_ready, rollback_digest=args.rollback_digest)
        else:
            digest = store.rollback(checked_digest(args.digest), reason=args.reason)
        print(f"LP {args.command} verified SHA256: {digest}")
        return 0
    except Exception:
        # Never dump SDK exceptions, parser payloads, endpoint URLs or credentials.
        print("LP storage operation failed; check mode, configuration, validation and publication/readiness proof.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
