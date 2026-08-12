# DOLO Liquidity Low-RPC Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce DOLO LP RPC load, preserve truthful current-position verification, and prevent scheduled jobs from publishing degraded `Stale` snapshots after provider failures.

**Architecture:** Scheduled runs use the checked-in artifact as an incremental cursor and token-ID index, scan only a 128-block overlap plus new blocks, then re-verify current on-chain position state. Full historical replay remains available through an explicit manual workflow input. A promotion guard runs before the atomic write and rejects any candidate containing failed stale sources when a prior artifact exists.

**Tech Stack:** Python 3.11, unittest, Node test runner, EVM JSON-RPC, GitHub Actions, static JSON/GitHub Pages.

## Global Constraints

- Scheduled refresh cadence is four times daily at minute 17: `17 */6 * * *`.
- DOLO liquidity freshness validation accepts artifacts up to 8 hours old.
- Unknown ownership and RPC classification failures remain fail-closed; no automatic quality promotion.
- Scheduled failures preserve the last checked-in artifact byte-for-byte and exit nonzero.
- Full-history mode is manual and explicit; scheduled runs remain incremental.

---

### Task 1: Cadence and freshness contract

**Files:**
- Modify: `.github/workflows/update-dolo-liquidity.yml`
- Modify: `validate_data.py`
- Test: `tests/dolo-liquidity-workflow.test.js`
- Test: `tests/test_validate_dolo_liquidity.py`

**Interfaces:**
- Consumes: existing `RULES["dolo-liquidity.json"]` and workflow cron.
- Produces: six-hour schedule and eight-hour freshness contract.

- [ ] Write tests requiring `cron: '17 */6 * * *'`, a manual `full_history` boolean input, and 7-hour-pass/9-hour-fail freshness behavior.
- [ ] Run the focused Node and Python tests and confirm the old hourly/3-hour settings fail.
- [ ] Update the workflow schedule/input and the validator freshness threshold.
- [ ] Run the focused tests and confirm they pass.

### Task 2: Reject degraded scheduled artifacts

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Test: `tests/test_generate_dolo_liquidity.py`

**Interfaces:**
- Produces: `assert_refresh_not_degraded(previous: dict, candidate: dict) -> None`.
- Raises: `RuntimeError` when a prior artifact exists and any candidate source is `stale` or has refresh errors.

- [ ] Write tests proving a failed stale candidate is rejected, the previous output is not written, and an improved candidate is accepted.
- [ ] Run the focused tests and confirm the current generator publishes the degraded candidate.
- [ ] Add the promotion guard immediately before `write_artifact_atomic`.
- [ ] Run focused and full liquidity tests.

### Task 3: Incremental Ethereum position refresh

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Test: `tests/test_generate_dolo_liquidity.py`

**Interfaces:**
- Produces: `incremental_pool_context(previous, source_key, pool_id, configured_start)` returning `scanStart`, active numeric `tokenIds`, and preserved history rows.
- Updates: `_build_uniswap_v3_live_source(..., previous_artifact=None, full_history=False)` and `_build_uniswap_v4_live_source(..., previous_artifact=None, full_history=False)`.
- Updates: `build_registered_source(..., previous_artifact=None, full_history=False)`.

- [ ] Write tests proving cursor overlap starts at `lastScannedBlock - 127`, prior active token IDs seed current reconciliation, and pool history is preserved without `staleSince`.
- [ ] Confirm the tests fail before the helper exists.
- [ ] Implement strict context extraction and deterministic history merge.
- [ ] Wire scheduled builders to scan only the incremental range and use prior token IDs; retain full discovery behavior when no prior artifact exists or `full_history=True`.
- [ ] Add `--full-history` to the CLI and workflow manual input path.
- [ ] Run focused tests, the full liquidity suites, `py_compile`, and `git diff --check`.

### Task 4: Recover production data and publish

**Files:**
- Modify: `data/dolo-liquidity.json`

**Interfaces:**
- Consumes: incremental generator and the current stale artifact.
- Produces: fresh artifact with zero stale active positions or no write on external failure.

- [ ] Run the official generator and record RPC usage and source outcomes.
- [ ] Validate the artifact and assert source errors are empty and `staleActivePositions == 0`.
- [ ] Run the complete LP, workflow, validator, and repository audit checks.
- [ ] Verify the live table locally at desktop and mobile sizes.
- [ ] Commit, rebase onto the latest production `master`, rerun checks, push `HEAD:master`, and watch Pages plus audit workflows.
- [ ] Confirm the public artifact and rendered table expose the new timestamp, zero stale positions, and visible `Verified` badges.
