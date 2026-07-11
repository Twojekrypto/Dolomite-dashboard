# Earn Yield Verification Hardening

**Goal:** Prevent incomplete or synthetic replay data from being presented as strict, verified lifetime yield while preserving useful diagnostic values and existing fallback UX.

## Evidence

- A truncated subgraph replay with matching end balances currently evaluates to `strictStatus = verified` because the snapshot-supplement path does not distinguish missing events from a missing live-balance response.
- `earn_reconcileReplayToCurrentPositions` can alter replay state to fit a current position before replay verification. The adjusted market can then pass the comparison it was changed to satisfy.
- The snapshot pairwise formula is only a historical fallback when the principal changes within a snapshot interval; exact lifetime yield requires ordered events and market indexes.

## Design

1. Track replay provenance per market. A market is **incomplete** if its subgraph history was truncated, if its current-state balance came from a snapshot supplement, or if replay state was reconciled to a newer current position.
2. Keep reconciled state available for the UI, but mark it as `coverage_incomplete`; it must not be eligible for `strictStatus = verified`, `trustedForTotal`, or the verified-yield summary.
3. Keep exact replay eligibility limited to complete event history plus an unmodified current-state reconciliation. Snapshot and public-netflow values remain visible as labeled fallback/inferred evidence.
4. Add executable behaviour tests for both failure modes. Structural source-presence tests are insufficient for financial classification logic.
5. Reuse a per-render result cache for the default yield calculation, so the table and summary do not repeat equivalent BigInt/metadata work during the same render.

## Scope

- Modify `dashboard-core.js` and `tests/test_earn_dashboard_contracts.py` only, plus these implementation notes.
- Do not regenerate production ledgers or change public data schemas in this patch.
- Do not change a numerical yield merely to make it look safer; only its verification eligibility and aggregation are tightened.

## Acceptance Criteria

- Truncated replay is always `coverage_incomplete`, even if all current balances match.
- Any market whose replay state was adjusted to current positions is always `coverage_incomplete`.
- Complete, unmodified replay still becomes `verified` when its balances reconcile.
- The verified portfolio total excludes the two incomplete classes.
- Targeted regression tests, the full Earn audit suite, static JavaScript syntax check, and local browser checks pass.
