# Dashboard Freshness Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Keep public dashboard artifacts inside their declared freshness budgets by detecting publication lag, avoiding duplicate producer runs, and improving Berachain RPC failover without weakening data quorum.

**Architecture:** Add a read-only public-artifact monitor driven by a validated JSON policy. It compares the deployed GitHub Pages artifact with the current raw `master` artifact, dispatching the producer only when both are stale and dispatching Pages only when Git is fresh but deployment is behind. Harden the existing DOLO/veDOLO RPC helpers so provider range errors and duplicate-vendor empty responses do not create false failures or false quorum.

**Tech Stack:** Python 3.11 standard library, existing `requests`, GitHub Actions `gh` CLI, YAML workflows, unittest.

**Spec:** Approved freshness scope in conversation: producer schedules remain, but public data age is monitored against target budgets; retries are bounded, active runs are not duplicated, and Git remains production while `LP_DATA_STORAGE=shadow`.

## Global Constraints

- Never read, print, rotate, recreate, or modify R2 credentials.
- Never set `LP_DATA_STORAGE=r2` or `LP_R2_READY_DIGEST`.
- Do not modify generated timestamps in data artifacts.
- Do not bypass RPC quorum or treat two keys from one vendor as independent witnesses.
- Do not publish a stale or source-stale artifact; the monitor may dispatch but cannot fabricate data.
- Preserve current GitHub Pages production path and existing workflow concurrency groups.

---

### Task 1: Public freshness monitor and validated policy

**Files:**
- Create: `scripts/check_dashboard_freshness.py`
- Create: `config/dashboard_freshness.json`
- Test: `tests/test_dashboard_freshness.py` (existing behavior contract)

**Interfaces:**
- `load_config(path) -> dict`, `validate_config(config) -> None`.
- `parse_timestamp(value) -> int` and `assess(payload, rule, now) -> dict`.
- `compare(rule, public_result, repo_result, config) -> dict`.
- `dispatch_gate(runs, policy, now) -> str`.
- `remediate(rows, config, api, now, allow_remediation) -> list[dict]`.
- `HttpClient.get_json(url, max_bytes) -> dict`; `NoRedirect` rejects redirects; `MonitorError` carries only safe fixed messages.
- `snapshot_path(manifest, now) -> str` safely selects the latest non-future dated EARN snapshot.

- [ ] Implement strict config validation (HTTPS GitHub Pages host, exact repository/branch patterns, safe relative paths, explicit workflow/input allowlists, bounded sizes and budgets).
- [ ] Implement timestamp parsing with naive ISO timestamps treated as UTC; reject booleans, malformed, negative, and future timestamps.
- [ ] Implement artifact assessment including publication timestamp, required source timestamp, and cached fallback source timestamp; source staleness must not dispatch a scanner.
- [ ] Implement public-vs-raw comparison and dispatch gate with active-run blocking, recent-failure cooldown, rolling retry budget, and fixed safe decisions.
- [ ] Implement idempotent remediation: deduplicate workflows, recheck active runs immediately before dispatch, enforce global per-run budget, and never retry an ambiguous dispatch failure in the same run.
- [ ] Add policy entries for the major dashboard files (Assets, price, TVL/revenue, liquidation, DOLO/oDOLO/veDOLO, LP, Supply, Rewards, and Earn snapshot/status artifacts) with declared target/max age and producer workflow. Use empty inputs for expensive workflows unless explicitly allowlisted.

### Task 2: Scheduled monitor workflow and status publication

**Files:**
- Create: `.github/workflows/dashboard-freshness.yml`
- Modify: `.gitignore` only if required for local monitor output (do not track runtime reports)

**Interfaces:**
- Calls `scripts/check_dashboard_freshness.py --config config/dashboard_freshness.json` with `GH_TOKEN` and `GITHUB_REPOSITORY`.
- Writes a concise `GITHUB_STEP_SUMMARY` report and dispatches producer/Pages workflows through the validated policy.

- [ ] Schedule every 15 minutes at a non-hour boundary; keep `concurrency.cancel-in-progress: false`.
- [ ] Check public GitHub Pages and raw master JSON with bounded HTTP response sizes and no auth headers.
- [ ] Dispatch at most the configured global budget, respecting each producer’s active runs, cooldown and workflow input allowlist.
- [ ] Exit non-zero only for monitor/config/API failures, not merely because a producer is stale or already active; retain evidence in the step summary.
- [ ] Run the monitor in dry-run mode in a local test before enabling remediation in Actions.

### Task 3: RPC failover and quorum hardening

**Files:**
- Modify: `generate_dolo_flows.py`
- Modify: `generate_vedolo_flows.py`
- Modify: `rpc_client.py`
- Test: `tests/test_flow_rpc_failover.py` (existing behavior contract)

**Interfaces:**
- Preserve `fetch_transfer_logs`, `_request_transfer_logs`, `fetch_event_logs`, and `rpc_provider_family` public behavior while adding provider-family-aware failover.

- [ ] Classify HTTP 400/range-limit JSON-RPC responses as `TransferLogRangeError` immediately, without generic retries on that endpoint.
- [ ] When one provider rejects a range, try healthy independent endpoints at the original range before shrinking the chunk; only shrink after the independent candidates fail.
- [ ] Deduplicate event rows returned by a healthy endpoint and preserve the requested topic set.
- [ ] Make the official Berachain alias share the same provider family as `rpc.berachain.com`; it remains a fallback, never an independent vote.
- [ ] Prevent two Alchemy keys from confirming an empty veDOLO range; require independent provider families for empty-response quorum.
- [ ] Keep all failure output sanitized and never log endpoint credentials.

### Task 4: Verification and deployment

**Files:**
- Modify: the files from Tasks 1–3 only.

- [ ] Run `python3 -m unittest tests.test_dashboard_freshness tests.test_flow_rpc_failover` and relevant existing flow/RPC tests.
- [ ] Run `python3 -m py_compile scripts/check_dashboard_freshness.py generate_dolo_flows.py generate_vedolo_flows.py rpc_client.py`.
- [ ] Parse the new workflow and policy; verify no cutover variable is changed and the worktree contains no credentials.
- [ ] Commit source/workflow/config/test changes to the isolated freshness branch, push to `master` using the repository’s normal controlled retry flow, and verify the monitor workflow is accepted by GitHub.
- [ ] Confirm the first monitor run and report whether any producer dispatch was made; do not wait for every producer to complete before reporting the guard deployment.
