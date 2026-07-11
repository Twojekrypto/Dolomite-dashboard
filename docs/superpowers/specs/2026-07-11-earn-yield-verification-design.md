# Earn Yield Verification Hardening

**Goal:** Prevent incomplete or synthetic replay data from being presented as strict, verified lifetime yield while preserving useful diagnostic values and existing fallback UX.

## Evidence

- A truncated subgraph replay with matching end balances currently evaluates to `strictStatus = verified` because the former snapshot-supplement path did not distinguish missing events from a missing live-balance response.
- A current balance that exists only in a subgraph snapshot is not an onchain reconciliation proof, even when its value matches replay; it must not be elevated to strict verification.
- `earn_reconcileReplayToCurrentPositions` can alter replay state to fit a current position before replay verification. The adjusted market can then pass the comparison it was changed to satisfy.
- The snapshot pairwise formula is only a historical fallback when the principal changes within a snapshot interval; exact lifetime yield requires ordered events and market indexes.
- A replay with matching `Par` but a non-zero `Wei` correction could be labelled strict `Verified`; this is a diagnostic reconciliation, not proof of an exact replay.
- A replay could also be labelled strict `Verified` when it still expected borrow or collateral that was absent from the live position.
- Rebuilding the public ledger rewrote every unchanged entry because `generatedAt` always changed, creating avoidable deployment churn.

## Design

1. Track replay provenance per market. A market is **incomplete** if its subgraph history was truncated, if its current-state balance came from a snapshot supplement, or if replay state was reconciled to a newer current position. When a truncated subgraph query cannot identify every affected market, conservatively mark the entire replay incomplete.
2. Keep reconciled state available for the UI, but mark it as `coverage_incomplete`; it must not be eligible for `strictStatus = verified`, `trustedForTotal`, the verified-yield summary, or a verified historical header.
3. Keep exact replay eligibility limited to complete event history plus an unmodified current-state reconciliation. Snapshot and public-netflow values remain visible as labeled fallback/inferred evidence.
4. Add executable behaviour tests for both failure modes. Structural source-presence tests are insufficient for financial classification logic.
5. Reuse a per-render result cache for the default yield calculation, so the table and summary do not repeat equivalent BigInt/metadata work during the same render.
6. Treat strict verification as exact raw reconciliation only. Keep `Wei` drift corrections and incomplete live exposure as diagnostics, but exclude them from strict status and the aggregate.
7. During an existing-ledger refresh, prune files with no current markets and retain an existing `generatedAt` when every other generated field is unchanged.

## Scope

- Modify `dashboard-core.js`, the two static-ledger classifiers, their targeted tests, and these implementation notes.
- Regenerate the public verified-ledger cache and `data/earn-quality/status.json`; public data schemas remain unchanged.
- Do not change a numerical yield merely to make it look safer; only its verification eligibility and aggregation are tightened.

## Acceptance Criteria

- Truncated replay is always `coverage_incomplete`, even if all current balances match.
- A globally truncated subgraph fallback with unknown affected markets gives no market in that replay a strict status.
- Snapshot-supplemented current state is always `coverage_incomplete`, even if all balances match.
- Any market whose replay state was adjusted to current positions is always `coverage_incomplete`.
- Complete, unmodified replay still becomes `verified` when its balances reconcile.
- The verified portfolio total excludes the two incomplete classes.
- A static `netflow+snapshot` or `recent-cycle+snapshot` match is reported as `inferred`, not strict verification, because snapshot intervals do not preserve every principal change.
- The lookup-cache version changes so a cached pre-hardening status cannot be rendered after deployment.
- A historical checkmark is shown only when every contributing historical ledger entry is strict; inferred evidence is labelled as such.
- A non-zero replay `Wei` correction or a missing expected borrow/collateral never receives strict `Verified`.
- An existing-ledger rebuild only rewrites entries whose semantic payload changed, while removing obsolete empty ledgers.
- Targeted regression tests, the full Earn audit suite, static JavaScript syntax check, and local browser checks pass.
