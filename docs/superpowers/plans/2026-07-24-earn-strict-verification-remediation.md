# EARN Strict Verification Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Increase honest EARN `Verified` coverage for active wallets through complete canonical replay, exact-zero cycle proof, and exact on-chain mismatch repair.

**Architecture:** Keep global canonical coverage and active strict remediation separate. Reuse canonical per-address action histories, add a pure exact replay engine plus bounded archive-RPC evidence fetcher, publish only exactly reconciled resolved ledgers, and leave incomplete or mismatched markets unchanged.

**Tech Stack:** Python 3.11, `unittest`, static JavaScript, GitHub Actions, JSON-RPC, GitHub Pages.

## Global Constraints

- Active chains are exactly Ethereum, Arbitrum, Berachain, Mantle, and X Layer.
- Polygon zkEVM and Botanix receive no new scheduled long-term remediation.
- Strict `Verified` requires complete history from the configured chain start, exact event-index evidence, and exact pinned `Par`/`Wei` reconciliation.
- No snapshot supplement, replay-state adjustment, inferred carry, derived index, tolerance widening, or clamping may produce strict `Verified`.
- Operational limits belong in workflow inputs/config; financial classification stays in code.
- RPC credentials remain in GitHub Actions secrets and must not appear in logs or generated files.

---

### Task 1: Require exact-zero cycle proof

**Files:**
- Modify: `tests/test_scan_earn_netflow.py`
- Modify: `tests/test_build_earn_verified_ledger.py`
- Modify: `tests/test_earn_dashboard_contracts.py`
- Modify: `scan_earn_netflow.py`
- Modify: `build_earn_verified_ledger.py`
- Modify: `dashboard-core.js`
- Modify: `earn/earn-core.js`

**Interfaces:**
- Consumes: ordered balance updates already tracked by the netflow scanner and runtime replay.
- Produces: `recentNetFlow`, `resetPar: "0"`, and `cycleStartProof: "exact-zero"` only after a proven zero boundary.

- [ ] **Step 1: Write failing backend cycle tests**

```python
def test_cycle_metadata_requires_exact_zero_boundary(self):
    netflows = {"0x" + "1" * 40: {"7": {"t": "150"}}}
    state = {
        ("0x" + "1" * 40, "7"): {
            "endingPar": 100,
            "peakPar": 100,
            "totalWei": 150,
            "suffixCandidates": [
                {"balance": 10, "prefixWei": 20},
                {"balance": 100, "prefixWei": 150},
            ],
        }
    }
    scan_earn_netflow.apply_cycle_metadata(netflows, state)
    self.assertNotIn("recentNetFlow", netflows["0x" + "1" * 40]["7"])

def test_cycle_metadata_emits_exact_zero_proof(self):
    address = "0x" + "2" * 40
    netflows = {address: {"7": {"t": "150"}}}
    state = {
        (address, "7"): {
            "endingPar": 100,
            "peakPar": 100,
            "totalWei": 150,
            "suffixCandidates": [
                {"balance": 0, "prefixWei": 40},
                {"balance": 100, "prefixWei": 150},
            ],
        }
    }
    scan_earn_netflow.apply_cycle_metadata(netflows, state)
    self.assertEqual("110", netflows[address]["7"]["recentNetFlow"])
    self.assertEqual("0", netflows[address]["7"]["resetPar"])
    self.assertEqual("exact-zero", netflows[address]["7"]["cycleStartProof"])
```

- [ ] **Step 2: Run backend cycle tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_scan_earn_netflow -v
```

Expected: the low non-zero reset test fails because the current 20% heuristic emits a cycle, and the exact-zero proof field is missing.

- [ ] **Step 3: Write failing static/runtime consumption tests**

Add tests proving that:

```python
self.assertIn('cycleStartProof === "exact-zero"', source)
self.assertIn('flow_entry.get("cycleStartProof") == "exact-zero"', verified_builder)
```

Run:

```bash
python3 -m unittest tests.test_build_earn_verified_ledger tests.test_earn_dashboard_contracts -v
```

Expected: FAIL because legacy recent-cycle data is still consumed without a proof.

- [ ] **Step 4: Implement exact-zero production behavior**

In the backend and both JavaScript copies:

```python
reset_candidate = next(
    (candidate for candidate in reversed(state["suffixCandidates"])
     if int(candidate.get("balance") or 0) == 0),
    None,
)
if reset_candidate is not None:
    storage_entry["recentNetFlow"] = str(
        int(state["totalWei"]) - int(reset_candidate["prefixWei"])
    )
    storage_entry["resetPar"] = "0"
    storage_entry["cycleStartProof"] = "exact-zero"
```

The builders and runtime include recent-cycle baselines only when
`cycleStartProof == "exact-zero"`. Raw all-time netflow remains available.

- [ ] **Step 5: Run cycle tests and verify GREEN**

```bash
python3 -m unittest \
  tests.test_scan_earn_netflow \
  tests.test_build_earn_verified_ledger \
  tests.test_earn_dashboard_contracts -v
node --check dashboard-core.js
node --check earn/earn-core.js
```

- [ ] **Step 6: Commit**

```bash
git add tests/test_scan_earn_netflow.py tests/test_build_earn_verified_ledger.py \
  tests/test_earn_dashboard_contracts.py scan_earn_netflow.py \
  build_earn_verified_ledger.py dashboard-core.js earn/earn-core.js
git commit -m "fix: require exact zero for EARN cycles"
```

### Task 2: Select unresolved active wallets for strict remediation

**Files:**
- Modify: `tests/test_select_earn_canonical_hot_addresses.py`
- Modify: `select_earn_canonical_hot_addresses.py`

**Interfaces:**
- Consumes: latest active snapshots, canonical history files, and public verified-ledger files.
- Produces: `build_selection(..., strict_remediation=True)` plus CLI flag `--strict-remediation`.

- [ ] **Step 1: Write failing selection tests**

Create fixtures with:

- one cold missing address;
- one active address with fresh history and `strictStatus: mismatch`;
- one active address with validated nested resolved `strictStatus: verified`.

Assert:

```python
selected, metadata = build_selection(
    "arbitrum",
    limit=1,
    priority_files=[],
    include_priority_even_if_unknown=False,
    history_dir=history_dir,
    ledger_dir=ledger_dir,
    strict_remediation=True,
)
self.assertEqual([active_mismatch], selected)
self.assertEqual(1, metadata["activeStrictBlockingAddressCount"])
```

- [ ] **Step 2: Run selector tests and verify RED**

```bash
python3 -m unittest tests.test_select_earn_canonical_hot_addresses -v
```

Expected: FAIL because the selector has no `ledger_dir` or strict-remediation mode.

- [ ] **Step 3: Implement strict-remediation selection**

Add:

```python
def _active_strict_quality(
    chain: str,
    active_addresses: set[str],
    ledger_dir: Path,
) -> Dict[str, str]:
    ...
```

Return `verified`, `mismatch`, `coverage_incomplete`, `inferred`, or `missing`
per active address. A validated nested `resolvedInterestLedger.markets[mid]`
with `strictStatus == "verified"` satisfies only that market; every current
active market must be exact for the address to be fully verified.

Strict remediation order is:

```python
[
    *priority,
    *active_missing_history,
    *active_stale_history,
    *active_mismatch,
    *active_coverage_incomplete,
    *active_inferred,
]
```

- [ ] **Step 4: Run selector tests and verify GREEN**

```bash
python3 -m unittest tests.test_select_earn_canonical_hot_addresses -v
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_select_earn_canonical_hot_addresses.py \
  select_earn_canonical_hot_addresses.py
git commit -m "feat: prioritize active EARN verification blockers"
```

### Task 3: Build a pure exact replay engine

**Files:**
- Create: `tests/test_earn_strict_replay.py`
- Create: `earn_strict_replay.py`

**Interfaces:**
- Consumes:
  - canonical ordered events with exact `eventIndex`;
  - pinned current indexes per market;
  - pinned on-chain current positions per account and market.
- Produces:
  - `build_strict_replay(history_payload, evidence) -> {"markets": ..., "verification": ..., "accountStates": ...}`.

- [ ] **Step 1: Write failing pure replay tests**

Cover:

```python
result = build_strict_replay(history, evidence)
self.assertEqual("verified", result["markets"]["7"]["strictStatus"])
self.assertEqual("0", result["verification"]["7"]["supplyParDiff"])
self.assertEqual("0", result["verification"]["7"]["supplyWeiDiff"])
```

Separate cases cover:

- open supply;
- exact-zero closed and reopened cycles;
- partial supply reduction;
- transfer and trade updates;
- liquidation and vaporization updates;
- collateral in an account with open borrow;
- open borrow;
- sign flips;
- missing event index;
- unknown account;
- history starting after protocol start;
- stale comparison block;
- exact `Par` mismatch;
- exact `Wei` mismatch.

- [ ] **Step 2: Run pure replay tests and verify RED**

```bash
python3 -m unittest tests.test_earn_strict_replay -v
```

Expected: import failure because `earn_strict_replay.py` does not exist.

- [ ] **Step 3: Implement replay primitives**

Create:

```python
INDEX_SCALE = 10**18

def par_to_wei_round_half_up(par: int, index: int) -> int:
    quotient, remainder = divmod(abs(par) * index, INDEX_SCALE)
    value = quotient + (1 if remainder * 2 >= INDEX_SCALE else 0)
    return value if par >= 0 else -value

def settle_reduced_exposure(state: dict, next_par: int) -> int:
    ...

def build_strict_replay(history_payload: dict, evidence: dict) -> dict:
    ...
```

The implementation mirrors `earn_settleReducedExposureYield`,
`earn_addSettledYield`, and `earn_summarizeReplayAccountStates`. It rejects
missing indexes rather than deriving them.

- [ ] **Step 4: Run pure replay tests and verify GREEN**

```bash
python3 -m unittest tests.test_earn_strict_replay -v
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_earn_strict_replay.py earn_strict_replay.py
git commit -m "feat: add exact EARN replay engine"
```

### Task 4: Fetch pinned archive-RPC evidence

**Files:**
- Create: `tests/test_earn_strict_rpc_evidence.py`
- Create: `earn_strict_rpc_evidence.py`
- Modify: `rpc_client.py`
- Modify: `tests/test_rpc_client.py`

**Interfaces:**
- Consumes: chain, address, canonical history, pinned comparison block, optional injected `RpcClient`.
- Produces: `fetch_strict_evidence(...)` with exact event indexes, current indexes, and account positions.

- [ ] **Step 1: Write failing ABI and evidence tests**

Use a fake client and fixed ABI payloads to assert:

```python
evidence = fetch_strict_evidence(
    "arbitrum", ADDRESS, history, comparison_block=123, client=fake_client
)
self.assertEqual("1200000000000000000", evidence["eventIndexes"][event_key])
self.assertEqual("-50", evidence["currentPositions"]["5|7"]["par"])
self.assertNotIn("rpcUrl", json.dumps(evidence))
```

Also assert a missing `LogIndexUpdate` before an event is reported as
`missing_event_index` and is never replaced by a delta ratio.

- [ ] **Step 2: Run evidence tests and verify RED**

```bash
python3 -m unittest tests.test_earn_strict_rpc_evidence -v
```

Expected: import failure because the evidence module does not exist.

- [ ] **Step 3: Implement exact evidence fetching**

Create constants for the default and Arbitrum index-update topics and existing
selectors:

```python
INDEX_UPDATE_TOPICS = {
    "default": "0x247e2f5b851dd23ef755d9ad527e801ee202c4097acd70c21e82dc5602cdd879",
    "arbitrum": "0xf4626fd1187f91e6761ffb8a6ac3e8d9235a4a92da54e43feb0c57c4a4a322ab",
}
GET_MARKET_CURRENT_INDEX = "0x56ea84b2"
GET_ACCOUNT_BALANCES = "0x6a8194e7"
```

Fetch index logs for the exact action blocks/markets, choose the last index log
before each action, then batch pinned `eth_call` requests for current indexes
and account balances. Decode all signed `Par`/`Wei` values and fail closed on
missing batch items.

Extend `rpc_client.CHAIN_ENV_KEYS` only with existing repository secret names
for Mantle and X Layer; never add values.

- [ ] **Step 4: Run evidence and RPC tests and verify GREEN**

```bash
python3 -m unittest tests.test_earn_strict_rpc_evidence tests.test_rpc_client -v
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_earn_strict_rpc_evidence.py tests/test_rpc_client.py \
  earn_strict_rpc_evidence.py rpc_client.py
git commit -m "feat: fetch pinned EARN replay evidence"
```

### Task 5: Publish exact resolved ledgers without rewriting mismatches

**Files:**
- Modify: `tests/test_build_earn_resolved_interest_ledger.py`
- Modify: `tests/test_build_earn_verified_ledger.py`
- Modify: `build_earn_resolved_interest_ledger.py`
- Modify: `build_earn_verified_ledger.py`

**Interfaces:**
- Consumes: pure replay engine and pinned evidence fetcher.
- Produces: validated nested `resolvedInterestLedger` markets and a bounded diagnostic status output.

- [ ] **Step 1: Write failing builder tests**

Assert that:

```python
ledger = build_resolved_ledger(..., strict_evidence=evidence)
self.assertEqual("verified", ledger["markets"]["7"]["strictStatus"])
```

and that exact mismatch returns no strict market plus:

```python
self.assertEqual("mismatch", diagnostics["markets"]["7"]["status"])
```

Tests also prove `snapshotIncomplete`, `subgraphReplayTruncated`, or
`replayStateAdjusted` cannot be accepted.

- [ ] **Step 2: Run builder tests and verify RED**

```bash
python3 -m unittest \
  tests.test_build_earn_resolved_interest_ledger \
  tests.test_build_earn_verified_ledger -v
```

- [ ] **Step 3: Wire strict replay into the builder**

Add CLI options:

```text
--fetch-strict-rpc-evidence
--status-output PATH
```

For each selected address:

1. validate snapshot and canonical coverage;
2. fetch strict evidence when enabled;
3. call `build_strict_replay`;
4. atomically write only exact markets;
5. remove a stale resolved file when no exact market survives;
6. write aggregate diagnostics to the requested status path.

The existing conservative offline pure-supply path remains available for local
and historical compatibility.

- [ ] **Step 4: Run builder tests and verify GREEN**

```bash
python3 -m unittest \
  tests.test_build_earn_resolved_interest_ledger \
  tests.test_build_earn_verified_ledger -v
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_build_earn_resolved_interest_ledger.py \
  tests/test_build_earn_verified_ledger.py \
  build_earn_resolved_interest_ledger.py build_earn_verified_ledger.py
git commit -m "feat: publish exact resolved EARN ledgers"
```

### Task 6: Add bounded active mismatch remediation workflow and truthful quality reporting

**Files:**
- Modify: `tests/test_build_earn_quality_status.py`
- Modify: `tests/test_earn_dashboard_contracts.py`
- Modify: `build_earn_quality_status.py`
- Create: `.github/workflows/repair-earn-strict-verification.yml`
- Modify: `run_earn_audit_checks.py`
- Modify: `lessons.md`

**Interfaces:**
- Consumes: strict-remediation selector, canonical public histories, archive RPC secrets, exact resolved builder.
- Produces: refreshed resolved ledgers, verified-ledger shards, quality status, and a workflow diagnostic artifact.

- [ ] **Step 1: Write failing quality and workflow contract tests**

Quality test:

```python
self.assertEqual(
    "verified",
    _ledger_market_quality(data_dir, "arbitrum", address, "7")["status"],
)
```

when the raw market is `mismatch` but the validated nested resolved market is
strict `verified`. Assert raw forensic fields remain unchanged in the ledger.

Workflow contracts assert:

- five active chains only;
- bounded `wallet_limit` input;
- `--strict-remediation`;
- `--fetch-strict-rpc-evidence`;
- per-chain canonical concurrency groups;
- `run_earn_audit_checks.py`;
- deterministic address-file staging;
- existing secret names, never literal RPC credentials.

- [ ] **Step 2: Run quality/workflow tests and verify RED**

```bash
python3 -m unittest \
  tests.test_build_earn_quality_status \
  tests.test_earn_dashboard_contracts -v
```

- [ ] **Step 3: Implement quality precedence**

In `_ledger_market_quality`, prefer a nested resolved market only when all are
true:

```python
resolved_market.get("strictStatus") == "verified"
and resolved_market.get("strictMethod") == "interest-ledger"
and verification.get("rawVerified") is True
and verification.get("snapshotIncomplete") is not True
and verification.get("subgraphReplayTruncated") is not True
and verification.get("replayStateAdjusted") is not True
```

Return `strictReason: exact_replay_reconciled`. Otherwise retain the raw market
classification.

- [ ] **Step 4: Implement the bounded workflow**

The workflow:

1. runs on a staggered schedule and manual dispatch;
2. selects unresolved active wallets per active chain;
3. skips empty cohorts;
4. fetches strict evidence and builds resolved ledgers;
5. rebuilds verified ledgers and shards for the same address file;
6. runs EARN audit checks;
7. stages only deterministic cohort files and manifests;
8. commits with the existing race-safe helper;
9. uploads the non-secret diagnostic JSON as an Actions artifact.

- [ ] **Step 5: Update audit runner and production lesson**

Add new focused unit modules to `run_earn_audit_checks.py`. Document that cycle
proof must be exact zero and strict mismatch repair requires pinned archive
evidence plus exact account reconciliation.

- [ ] **Step 6: Run quality/workflow tests and verify GREEN**

```bash
python3 -m unittest \
  tests.test_build_earn_quality_status \
  tests.test_earn_dashboard_contracts -v
```

- [ ] **Step 7: Commit**

```bash
git add tests/test_build_earn_quality_status.py \
  tests/test_earn_dashboard_contracts.py build_earn_quality_status.py \
  .github/workflows/repair-earn-strict-verification.yml \
  run_earn_audit_checks.py lessons.md
git commit -m "feat: repair active EARN verification mismatches"
```

### Task 7: Full review, verification, and production publish

**Files:**
- Review all changed files.

**Interfaces:**
- Consumes: completed implementation.
- Produces: verified production commit on `dolomite-dashboard/master`.

- [ ] **Step 1: Correctness and regression review**

Check the complete diff against every acceptance criterion in the design. Pay
special attention to:

- no strict promotion from snapshots/netflow;
- exact comparison block alignment;
- no fallback index in strict replay;
- no adjusted current state;
- exact-zero cycle behavior in both JavaScript copies;
- deterministic workflow staging.

- [ ] **Step 2: Maintainability and security review**

Check:

- no RPC keys or full credential URLs in output;
- new operational limits are workflow inputs;
- no bare `except`;
- all JSON writes are atomic;
- active-chain scope excludes retired chains;
- no unrelated formatting.

- [ ] **Step 3: Run the complete verification gate**

```bash
python3 -m unittest \
  tests.test_select_earn_canonical_hot_addresses \
  tests.test_scan_earn_netflow \
  tests.test_earn_strict_replay \
  tests.test_earn_strict_rpc_evidence \
  tests.test_build_earn_resolved_interest_ledger \
  tests.test_build_earn_verified_ledger \
  tests.test_build_earn_quality_status \
  tests.test_earn_dashboard_contracts
python3 run_earn_audit_checks.py
python3 -m py_compile \
  select_earn_canonical_hot_addresses.py \
  scan_earn_netflow.py \
  earn_strict_replay.py \
  earn_strict_rpc_evidence.py \
  build_earn_resolved_interest_ledger.py \
  build_earn_verified_ledger.py \
  build_earn_quality_status.py
node --check dashboard-core.js
node --check earn/earn-core.js
git diff --check
```

- [ ] **Step 4: Rebase on the moving production branch**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
```

Resolve only conflicts in files changed by this plan, then rerun the complete
verification gate.

- [ ] **Step 5: Push to production**

```bash
git push dolomite-dashboard HEAD:master
```

- [ ] **Step 6: Confirm remote commit and workflow dispatchability**

```bash
git fetch dolomite-dashboard master
git rev-parse HEAD
git rev-parse dolomite-dashboard/master
gh workflow view repair-earn-strict-verification.yml \
  --repo Twojekrypto/Dolomite-dashboard
```
