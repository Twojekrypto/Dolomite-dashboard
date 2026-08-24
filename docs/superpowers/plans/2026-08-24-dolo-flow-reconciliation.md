# DOLO Flow Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DOLO Flow exact across Ethereum + Berachain for every period and correct wallet classification without changing unrelated dashboard data logic.

**Architecture:** Generate a dedicated combined leaderboard from complete market-flow maps before applying `TOP_N`, while retaining current per-chain rows. Resolve period cutoffs by block timestamp, enrich dynamic exclusions with current holder wallet structure and known trading labels, and publish bridge-method telemetry for auditability.

**Tech Stack:** Python 3 `unittest`, static HTML/JavaScript, GitHub Actions, JSON artifacts.

**Spec:** `docs/superpowers/specs/2026-08-24-dolo-flow-reconciliation-design.md`

## Global Constraints

- Track Ethereum and Berachain only; do not add Arbitrum.
- Do not relax RPC quorum, transfer-history completeness, custody neutralization, or bridge rules.
- Rank the combined view only after full cross-chain aggregation.
- Use tests first and keep existing generated-data compatibility.

---

### Task 1: Exact combined leaderboard

**Files:**
- Modify: `generate_dolo_flows.py`
- Modify: `validate_data.py`
- Test: `tests/test_generate_dolo_flows_integrity.py`
- Test: `tests/test_validate_dolo_flows.py`

**Interfaces:**
- Produces: `merge_chain_flow_maps(...)`, `merge_chain_flow_components(...)`, and `periods.<period>.all`.
- Consumes: full `market_flows_by_chain`, components, and transaction counts before `get_top(...)`.

- [x] Add a failing regression where an address is outside each chain Top 100 but is material after aggregation, and where equal opposing chain legs reconcile to zero.
- [x] Run the focused tests and confirm failure because no combined pre-ranking path exists.
- [x] Implement complete-map aggregation, create the `all` rows, and validate exact row reconciliation.
- [x] Run focused tests and confirm the false `0x1576…1891` style outflow can no longer be emitted.

### Task 2: Timestamp-exact periods

**Files:**
- Modify: `generate_dolo_flows.py`
- Test: `tests/test_generate_dolo_flows_rpc.py`

**Interfaces:**
- Produces: `find_first_block_at_or_after_timestamp(...)` and exact `cutoff_blocks` metadata.
- Consumes: pinned confirmed head block and `eth_getBlockByNumber` timestamps.

- [x] Add failing boundary tests using a hand-built irregular block-time fixture.
- [x] Confirm the old `seconds // block_time` calculation selects the wrong first block.
- [x] Implement binary search for the first block whose timestamp is at or after the requested cutoff; fail closed if a timestamp cannot be verified.
- [x] Publish the selected block and timestamp for each period/network and rerun focused tests.

### Task 3: Safe and Bot/MM visibility

**Files:**
- Modify: `generate_dolo_flows.py`
- Test: `tests/test_generate_dolo_flows_integrity.py`

**Interfaces:**
- Produces: verified user-contract addresses derived from `dolo_holders.json`.
- Consumes: current holder `contract_wallet_type` and shared address labels.

- [x] Add failing tests for a detected Safe with 20M DOLO and for all four known Bot/MM labels.
- [x] Confirm the current classifier excludes those addresses.
- [x] Preserve verified Safe/multisig/delegated-EOA holder structures and known bot/mm/trader labels while continuing to exclude unlabeled infrastructure contracts.
- [x] Run the classification regressions and existing contract exclusion suite.

### Task 4: Bridge audit telemetry and combined UI

**Files:**
- Modify: `generate_dolo_flows.py`
- Modify: `dolo-preview.html`
- Test: `tests/test_generate_dolo_flows_integrity.py`
- Test: `tests/dolo-flow-protocol-filter.test.js`

**Interfaces:**
- Produces: bridge cancellation stats split into `canonicalAdapter` and `legacyHeuristic`.
- Consumes: existing canonical adapter match and legacy fallback results.

- [x] Add failing tests proving the two bridge methods are reported separately and the combined UI reads `periods.<period>.all`.
- [x] Implement telemetry without changing the cancellation arithmetic.
- [x] Route the combined UI to the generated `all` rows and label the option `Ethereum + Berachain`.
- [x] Run Python and Node flow tests.

### Task 5: BrownFi evidence and deployment

**Files:**
- Modify only if verified: `data/dolo-liquidity-pools.json`
- Test only if registry changes: `tests/test_generate_dolo_liquidity.py`

**Interfaces:**
- Consumes: DeBank discovery, explorer/on-chain contract evidence, and current liquidity-registry schema.
- Produces: either a verified registry entry or a documented reason for no code change.

- [x] Inspect `0x16b3a5e95db753fe5195244fa208301e38beae2a` on DeBank and on-chain.
- [x] Distinguish a user wallet position from a pool/vault contract before editing the registry.
- [x] If evidence is sufficient, add the exact BrownFi pool adapter and a fixture-backed test; otherwise leave data logic unchanged and report the evidence gap.
- [ ] Run complete targeted tests, syntax checks, validators, review the diff twice, commit, push to production `master`, run the DOLO Flow workflow, and verify the live artifact with cache busting.
