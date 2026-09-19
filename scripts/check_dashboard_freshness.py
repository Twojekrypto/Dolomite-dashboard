#!/usr/bin/env python3
"""Check public dashboard freshness and dispatch bounded repairs.

The monitor is intentionally conservative: it observes public JSON and the
raw ``master`` copy, never invents data, never retries an ambiguous dispatch,
and never treats an RPC/source timestamp as fresh merely because a wrapper was
recently committed.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import socket
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener


class MonitorError(RuntimeError):
    """Safe, fixed-message monitor failure."""


ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SAFE_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
SAFE_REF_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
SAFE_WORKFLOW_RE = re.compile(r"^[A-Za-z0-9_.-]+\.ya?ml$")


class NoRedirect(HTTPRedirectHandler):
    """Reject redirects so a public fetch cannot leave the configured host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        raise HTTPError(req, code, "redirect rejected", headers, newurl)


def _path_value(payload: Any, path: str) -> Any:
    current = payload
    for part in str(path).split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def parse_timestamp(value: Any) -> int:
    """Parse seconds or ISO-8601 into epoch seconds, failing closed."""
    if isinstance(value, bool) or value is None:
        raise MonitorError("invalid_timestamp")
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)) or float(value) <= 0:
            raise MonitorError("invalid_timestamp")
        numeric = float(value)
        if numeric > 100_000_000_000:
            numeric /= 1000
        if numeric <= 0 or not math.isfinite(numeric):
            raise MonitorError("invalid_timestamp")
        return int(numeric)
    if not isinstance(value, str) or not value.strip():
        raise MonitorError("invalid_timestamp")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise MonitorError("invalid_timestamp") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    timestamp = parsed.timestamp()
    if not math.isfinite(timestamp) or timestamp <= 0:
        raise MonitorError("invalid_timestamp")
    return int(timestamp)


def _safe_age(timestamp: int, now: int) -> float:
    if timestamp > now:
        raise MonitorError("future_timestamp")
    return max(0.0, (now - timestamp) / 60.0)


def assess(payload: Any, rule: Dict[str, Any], now: int) -> Dict[str, Any]:
    """Assess publication and required source timestamps for one artifact."""
    if not isinstance(payload, dict):
        return {"state": "invalid", "error": "payload_not_object", "age_minutes": None}
    timestamp_fields = rule.get("timestamps") or ["generatedAt"]
    if isinstance(timestamp_fields, str):
        timestamp_fields = [timestamp_fields]
    if not isinstance(timestamp_fields, list) or not timestamp_fields:
        return {"state": "invalid", "error": "timestamp_policy_invalid", "age_minutes": None}
    source_fields = rule.get("sourceTimestamps") or timestamp_fields[1:]
    if isinstance(source_fields, str):
        source_fields = [source_fields]
    try:
        publication = parse_timestamp(_path_value(payload, timestamp_fields[0]))
        publication_age = _safe_age(publication, now)
        source_ages: List[float] = []
        for field in source_fields:
            source_value = _path_value(payload, field)
            if source_value is None:
                raise MonitorError("required_source_timestamp_missing")
            source_ages.append(_safe_age(parse_timestamp(source_value), now))
        for field in rule.get("sourceLagMinutes", []):
            lag = _path_value(payload, field)
            if isinstance(lag, bool) or not isinstance(lag, (int, float)) or not math.isfinite(lag) or lag < 0:
                raise MonitorError("required_source_lag_invalid")
            source_ages.append(float(lag))

        # Cached rate rows are source data even when the wrapper snapshot is new.
        fallback_chains = payload.get("rateFallbackChains") or []
        if fallback_chains:
            fallback_value = payload.get("rateFallbackSourceGeneratedAt")
            if fallback_value is None:
                raise MonitorError("required_source_timestamp_missing")
            source_ages.append(_safe_age(parse_timestamp(fallback_value), now))

        max_age = float(rule.get("maxAgeMinutes", 360))
        source_max_age = float(rule.get("sourceMaxAgeMinutes", max_age))
        early_due = float(rule.get("earlyDueMinutes", max_age * 0.75))
        if not math.isfinite(max_age) or max_age <= 0 or early_due < 0 or not math.isfinite(source_max_age) or source_max_age <= 0:
            raise MonitorError("timestamp_policy_invalid")
    except MonitorError as exc:
        return {"state": "invalid", "error": str(exc), "age_minutes": None}

    source_age = max(source_ages, default=0.0)
    if source_age >= source_max_age:
        return {
            "state": "source_stale",
            "age_minutes": round(source_age, 1),
            "publication_age_minutes": round(publication_age, 1),
            "source_age_minutes": round(source_age, 1),
            "max_age_minutes": source_max_age,
        }
    if publication_age >= max_age:
        state = "stale"
    elif publication_age >= early_due:
        state = "early_due"
    else:
        state = "fresh"
    return {
        "state": state,
        "age_minutes": round(publication_age, 1),
        "publication_age_minutes": round(publication_age, 1),
        "source_age_minutes": round(source_age, 1),
        "max_age_minutes": max_age,
    }


def _is_publicly_usable(result: Dict[str, Any]) -> bool:
    return result.get("state") in {"fresh", "early_due", "stale"}


def compare(rule: Dict[str, Any], public_result: Dict[str, Any], repo_result: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Choose a safe repair: Pages, producer, or no action."""
    row = {
        "id": rule.get("id"),
        "path": rule.get("path"),
        "public": public_result,
        "repo": repo_result,
        "workflow": None,
        "problem": "none",
        "target_minutes": rule.get("maxAgeMinutes"),
    }
    if public_result.get("state") in {"unavailable", "invalid", "source_stale"}:
        row["problem"] = public_result.get("state")
        if public_result.get("state") == "source_stale" and rule.get("remediateSourceStale"):
            row["workflow"] = rule.get("workflow")
        return row
    public_due = public_result.get("state") in {"early_due", "stale"}
    if rule.get("productionStorage") == "r2":
        # Git is only a frozen recovery baseline after cutover, not production.
        if public_due:
            row.update(problem="stale", workflow=rule.get("workflow"))
        return row
    repo_stale = repo_result.get("state") in {"stale", "early_due"}
    if public_due and repo_result.get("state") in {"fresh", "early_due"} and not repo_stale:
        row["problem"] = "deployment_behind"
        row["workflow"] = config.get("pages_workflow", "pages.yml")
    elif public_due and repo_stale:
        row["problem"] = "stale"
        row["workflow"] = rule.get("workflow")
    elif repo_stale and public_result.get("state") == "fresh":
        # The deployed copy is currently usable; refresh the producer so the
        # next publication does not inherit an old raw baseline.
        row["problem"] = "repository_behind"
        row["workflow"] = rule.get("workflow")
    return row


def _run_created_at(run: Dict[str, Any], now: int) -> Optional[int]:
    value = run.get("created_at") or run.get("createdAt")
    if value is None:
        raise MonitorError("run_metadata_invalid")
    timestamp = parse_timestamp(value)
    if timestamp > now:
        raise MonitorError("run_metadata_invalid")
    return timestamp


def dispatch_gate(runs: Iterable[Dict[str, Any]], policy: Dict[str, Any], now: int) -> str:
    runs = list(runs or [])
    if any(str(run.get("status", "")).lower() in {"queued", "in_progress", "waiting", "pending", "requested"} for run in runs if isinstance(run, dict)):
        return "active"
    parsed = []
    for run in runs:
        if not isinstance(run, dict):
            return "run_metadata_invalid"
        try:
            timestamp = _run_created_at(run, now)
        except MonitorError:
            return "run_metadata_invalid"
        parsed.append((run, timestamp))
    window = float(policy.get("dispatchWindowMinutes", 60))
    max_dispatches = int(policy.get("max_dispatches_in_window", policy.get("maxDispatchesInWindow", 3)))
    recent = [(run, ts) for run, ts in parsed if (now - ts) / 60 <= window]
    if len(recent) >= max_dispatches:
        return "retry_budget"
    cooldown = float(policy.get("failureCooldownMinutes", 15))
    if any(
        str(run.get("status", "")).lower() == "completed"
        and str(run.get("conclusion", "")).lower() in {"failure", "timed_out", "cancelled"}
        and (now - ts) / 60 < cooldown
        for run, ts in parsed
    ):
        return "cooldown"
    return "ready"


def remediate(rows: Iterable[Dict[str, Any]], config: Dict[str, Any], api: Any, now: int, allow_remediation: bool) -> List[Dict[str, Any]]:
    decisions: List[Dict[str, Any]] = []
    seen = set()
    dispatched = 0
    global_budget = int(config.get("max_dispatches_per_run", config.get("maxDispatchesPerRun", 3)))
    for row in rows:
        workflow = str(row.get("workflow") or "")
        if not workflow:
            continue
        if workflow in seen:
            continue
        seen.add(workflow)
        decision = {"workflow": workflow, "id": row.get("id"), "action": None}
        if not allow_remediation:
            decision["action"] = "dry_run"
            decisions.append(decision)
            continue
        if dispatched >= global_budget:
            decision["action"] = "run_budget"
            decisions.append(decision)
            continue
        policy = config["workflows"].get(workflow)
        if not isinstance(policy, dict):
            decision["action"] = "invalid_policy"
            decisions.append(decision)
            continue
        try:
            gate = dispatch_gate(api.runs(workflow), policy, now)
            if gate != "ready":
                decision["action"] = gate
                decisions.append(decision)
                continue
            # A queued run can appear between the first check and dispatch.
            gate = dispatch_gate(api.runs(workflow), policy, now)
            if gate != "ready":
                decision["action"] = gate
                decisions.append(decision)
                continue
            inputs = row.get("inputs") if isinstance(row.get("inputs"), dict) else {}
            api.dispatch(workflow, inputs)
        except MonitorError:
            decision["action"] = "dispatch_failed"
            decisions.append(decision)
            continue
        dispatched += 1
        decision["action"] = "dispatched"
        decisions.append(decision)
    return decisions


class HttpClient:
    def __init__(self, config: Dict[str, Any], _unused_secret: str = ""):
        self.config = config
        self.opener = build_opener(NoRedirect())
        self.timeout = float(config.get("requestTimeoutSeconds", 20))

    def _allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        public = urlparse(self.config["public_base_url"])
        raw = urlparse(self.config["raw_base_url"])
        if parsed.scheme != "https" or parsed.username or parsed.password:
            return False
        for base in (public, raw):
            if parsed.hostname == base.hostname and parsed.path.startswith(base.path):
                return True
        return False

    def get_json(self, url: str, max_bytes: int) -> Any:
        if not self._allowed(url):
            raise MonitorError("request_failed")
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "dashboard-freshness-guard/1"})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                length = response.headers.get("Content-Length") if getattr(response, "headers", None) else None
                if length and int(length) > max_bytes:
                    raise MonitorError("response_too_large")
                body = response.read(max_bytes + 1)
                if len(body) > max_bytes:
                    raise MonitorError("response_too_large")
            return json.loads(body.decode("utf-8"))
        except MonitorError:
            raise
        except (HTTPError, URLError, TimeoutError, socket.timeout, ValueError, OSError):
            raise MonitorError("request_failed") from None


class GitHubAPI:
    def __init__(self, config: Dict[str, Any], token: str):
        self.config = config
        self.token = token
        self.base = f"https://api.github.com/repos/{config['repository']}"
        if not token:
            raise MonitorError("github_token_missing")

    def _request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        request = Request(
            self.base + path,
            method=method,
            headers={"Accept": "application/vnd.github+json", "Authorization": f"Bearer {self.token}", "User-Agent": "dashboard-freshness-guard/1"},
            data=json.dumps(body).encode("utf-8") if body is not None else None,
        )
        try:
            with build_opener(NoRedirect()).open(request, timeout=20) as response:
                data = response.read(2_000_000)
            return json.loads(data.decode("utf-8")) if data else {}
        except (HTTPError, URLError, TimeoutError, socket.timeout, ValueError, OSError):
            raise MonitorError("github_api_failed") from None

    def runs(self, workflow: str) -> List[Dict[str, Any]]:
        payload = self._request("GET", f"/actions/workflows/{workflow}/runs?branch={self.config['branch']}&per_page=30")
        runs = payload.get("workflow_runs") if isinstance(payload, dict) else []
        return runs if isinstance(runs, list) else []

    def dispatch(self, workflow: str, inputs: Dict[str, Any]) -> None:
        self._request("POST", f"/actions/workflows/{workflow}/dispatches", {"ref": self.config["branch"], "inputs": inputs})


def _validate_relative_path(path: Any) -> None:
    if not isinstance(path, str) or not path or path.startswith("/") or "://" in path or "\\" in path:
        raise MonitorError("config_invalid")
    decoded = unquote(path)
    if any(part in {"", ".", ".."} for part in decoded.split("/")):
        raise MonitorError("config_invalid")


def validate_config(config: Dict[str, Any]) -> None:
    if not isinstance(config, dict) or not SAFE_REPOSITORY_RE.fullmatch(str(config.get("repository", ""))):
        raise MonitorError("config_invalid")
    for key in ("public_base_url", "raw_base_url"):
        parsed = urlparse(str(config.get(key, "")))
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.query or parsed.fragment or not parsed.hostname:
            raise MonitorError("config_invalid")
    if urlparse(config["public_base_url"]).hostname != "twojekrypto.github.io":
        raise MonitorError("config_invalid")
    if urlparse(config["raw_base_url"]).hostname != "raw.githubusercontent.com":
        raise MonitorError("config_invalid")
    if not SAFE_REF_RE.fullmatch(str(config.get("branch", ""))):
        raise MonitorError("config_invalid")
    workflows = config.get("workflows")
    if not isinstance(workflows, dict) or not workflows:
        raise MonitorError("config_invalid")
    for name, policy in workflows.items():
        if not SAFE_WORKFLOW_RE.fullmatch(str(name)) or not isinstance(policy, dict):
            raise MonitorError("config_invalid")
        if policy.get("workflow") != name or not isinstance(policy.get("allowedInputs", []), list) or "inputs" in policy:
            raise MonitorError("config_invalid")
    artifacts = config.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise MonitorError("config_invalid")
    for rule in artifacts:
        if not isinstance(rule, dict) or not rule.get("id") or not rule.get("workflow"):
            raise MonitorError("config_invalid")
        _validate_relative_path(rule.get("path"))
        if rule["workflow"] not in workflows:
            raise MonitorError("config_invalid")
        allowed = set(workflows[rule["workflow"]].get("allowedInputs", []))
        inputs = rule.get("inputs") or {}
        if not isinstance(inputs, dict) or any(str(key) not in allowed for key in inputs):
            raise MonitorError("config_invalid")


def load_config(path: Path) -> Dict[str, Any]:
    try:
        config = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise MonitorError("config_invalid") from None
    validate_config(config)
    return config


def snapshot_path(manifest: Dict[str, Any], now: int) -> str:
    dates = manifest.get("dates") if isinstance(manifest, dict) else None
    if not isinstance(dates, list) or not dates:
        raise MonitorError("snapshot_manifest_invalid")
    valid = []
    today = datetime.fromtimestamp(now, timezone.utc).date()
    for value in dates:
        if not isinstance(value, str) or not ISO_DATE_RE.fullmatch(value):
            raise MonitorError("snapshot_manifest_invalid")
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            raise MonitorError("snapshot_manifest_invalid") from None
        if parsed > today:
            raise MonitorError("snapshot_manifest_invalid")
        valid.append(value)
    return f"data/earn-snapshots/{max(valid)}.json"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/dashboard_freshness.json")
    parser.add_argument("--no-remediation", action="store_true")
    parser.add_argument("--fail-on-stale", action="store_true", help="Fail after remediation when a source is invalid/stale or publication exceeds 8h")
    args = parser.parse_args(argv)
    config = load_config(Path(args.config))
    now = int(time.time())
    client = HttpClient(config)
    rows = []
    for configured_rule in config["artifacts"]:
        rule = dict(configured_rule)
        if rule.get("id") == "dolo-liquidity" and os.environ.get("LP_DATA_STORAGE", "git") == "r2":
            rule["productionStorage"] = "r2"
        try:
            public_payload = client.get_json(config["public_base_url"] + rule["path"], int(rule.get("maxBytes", config.get("maxResponseBytes", 20_000_000))))
            public_result = assess(public_payload, rule, now)
        except MonitorError as exc:
            public_result = {"state": "unavailable", "error": str(exc), "age_minutes": None}
        if rule.get("productionStorage") == "r2":
            repo_result = {"state": "recovery_baseline", "age_minutes": None}
        else:
            try:
                repo_payload = client.get_json(config["raw_base_url"] + rule["path"], int(rule.get("maxBytes", config.get("maxResponseBytes", 20_000_000))))
                repo_result = assess(repo_payload, rule, now)
            except MonitorError as exc:
                repo_result = {"state": "unavailable", "error": str(exc), "age_minutes": None}
        rows.append(compare(rule, public_result, repo_result, config))

    candidates = [row for row in rows if row.get("workflow")]
    token = __import__("os").environ.get("GH_TOKEN", "")
    decisions = []
    if candidates:
        # A public, read-only audit needs no Actions token or GitHub API calls.
        api = None if args.no_remediation else GitHubAPI(config, token)
        decisions = remediate(candidates, config, api, now, not args.no_remediation)
    summary = {"generatedAt": datetime.now(timezone.utc).isoformat(), "rows": rows, "decisions": decisions}
    print(json.dumps(summary, sort_keys=True))
    summary_path = __import__("os").environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as stream:
            stream.write("## Dashboard freshness guard\n\n")
            for row in rows:
                stream.write(f"- `{row.get('id')}`: public **{row['public'].get('state')}** ({row['public'].get('age_minutes')}m), raw **{row['repo'].get('state')}** ({row['repo'].get('age_minutes')}m), action **{row.get('problem')}**\n")
            if decisions:
                stream.write("\n### Remediation decisions\n\n")
                for decision in decisions:
                    stream.write(f"- `{decision['workflow']}`: **{decision['action']}**\n")
    blocking = any(
        row["public"].get("state") in {"source_stale", "invalid", "unavailable"}
        or float(row["public"].get("age_minutes") or 0) >= 480
        for row in rows
    )
    return 1 if args.fail_on_stale and blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
