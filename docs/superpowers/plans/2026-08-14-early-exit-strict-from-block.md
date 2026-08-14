# Early Exit Strict fromBlock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject a floating-point or boolean `coverage.fromBlock` before the Early Exits checkpoint can reach any RPC call.

**Architecture:** Extend the existing strict checkpoint type gate; do not change counters, baseline floors, merge logic, or workflows.

**Tech Stack:** Python 3, `unittest`.

## Global Constraints

- `coverage.fromBlock` must be `type(value) is int` and equal exactly `2_926_448`.
- A malformed checkpoint must fail before any RPC call.
- Preserve every other audited baseline guard and generated artifact unchanged.

---

### Task 1: Strict deployment-block type

**Files:**
- Modify: `tests/test_fetch_early_exits.py`
- Modify: `fetch_early_exits.py`

**Interfaces:**
- Consumes: `validate_checkpoint(checkpoint)` and `main()`.
- Produces: strict integer-only `coverage.fromBlock` validation.

- [ ] **Step 1: Write failing regressions**

Create otherwise-valid audited checkpoint fixtures with `fromBlock = 2926448.0` and `fromBlock = True`. Assert validation raises and a `main()` path with the float checkpoint performs zero RPC calls.

- [ ] **Step 2: Verify RED**

Run the new focused tests and confirm the float case reaches the current numeric-equality branch instead of failing closed.

- [ ] **Step 3: Implement strict type validation**

Require `type(coverage.get("fromBlock")) is int` before comparing it to `VEDOLO_DEPLOYMENT_BLOCK`.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
python3 -m unittest tests.test_fetch_early_exits
python3 -m unittest tests.test_validate_early_exits tests.test_pages_workflow_contracts
python3 -m py_compile fetch_early_exits.py validate_data.py
python3 validate_data.py early_exits.json
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add fetch_early_exits.py tests/test_fetch_early_exits.py
git commit -m "fix: require integer early exit deployment block"
```
