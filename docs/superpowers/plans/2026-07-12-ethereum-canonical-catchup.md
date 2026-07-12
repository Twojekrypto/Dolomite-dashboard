# Ethereum Canonical Coverage Catch-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop Ethereum canonical EARN checkpoints from resetting before they can publish fresh coverage.

**Architecture:** Retune only the existing GitHub Actions checkpoint inputs. The strict event scanner, adaptive range fallback, canonical classification, and UI warning remain unchanged.

**Tech Stack:** GitHub Actions YAML, Python `unittest` contracts.

## Global Constraints

- Do not weaken strict canonical coverage or hide the freshness warning.
- Preserve exact event replay and the scanner's adaptive RPC fallback.
- Keep secrets in GitHub Actions secrets; no credentials enter the repository.

---

### Task 1: Guard the catch-up configuration

**Files:**
- Modify: `tests/test_earn_dashboard_contracts.py`
- Modify: `tests/test_update_earn_freshness_status.py`

**Interfaces:**
- Consumes: the Ethereum canonical workflow and `CANONICAL_CATCHUP_INPUTS`.
- Produces: executable regression coverage for resumable, bounded Ethereum checkpoints.

- [x] **Step 1: Write failing expectations**

Assert a 240-wallet, 1,200-step workflow with 28,800-block resume tolerance, 1,000-block task partition, 12 incremental scan workers, and no `--existing-history-only` filter. Assert the watchdog dispatches the matching 240/1,200 inputs.

- [x] **Step 2: Run the two tests and confirm failure**

Run:

```bash
python3 -m unittest \
  tests.test_earn_dashboard_contracts.EarnDashboardContractsTest.test_ethereum_canonical_workflow_rebuilds_verified_ledger_on_fresh_history \
  tests.test_update_earn_freshness_status.UpdateEarnFreshnessStatusTest.test_ethereum_stale_canonical_dispatch_uses_long_catchup_checkpoint -v
```

Expected: FAIL because the workflow still uses the reset-prone 80/2,400/600/10/4 values.

- [x] **Step 3: Apply the minimal configuration change**

Change only the Ethereum workflow inputs and `CANONICAL_CATCHUP_INPUTS`; retain `timeout-minutes: 90` and the existing scanner arguments.

- [x] **Step 4: Run the targeted tests**

Run the command from Step 2 and expect both tests to pass.

### Task 2: Verify and publish

**Files:**
- Verify: workflow YAML, freshness planner, Earn audit suite.

- [x] **Step 1: Run syntax and audit checks**

```bash
python3 -m py_compile update_earn_freshness_status.py
npm run check:earn-audit
```

- [ ] **Step 2: Review and publish**

Run `git diff --check`, stage only the workflow, planner, tests, and this documentation, commit, rebase if remote moves, and push `master` without force.
