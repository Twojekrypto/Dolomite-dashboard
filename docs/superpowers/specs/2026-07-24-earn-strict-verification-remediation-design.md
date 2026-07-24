# EARN Strict Verification Remediation Design

**Date:** 2026-07-24

## Goal

Increase the number of honestly `Verified` EARN positions by prioritizing active
wallets, rebuilding complete canonical evidence, and repairing exact replay
mismatches. Never promote snapshot, inferred, carry, adjusted, truncated, or
otherwise incomplete data to `Verified`.

## Scope

The remediation applies to the five active EARN chains:

- Ethereum
- Arbitrum
- Berachain
- Mantle
- X Layer

Polygon zkEVM and Botanix remain archived. This work does not add new long-term
backfills or product guarantees for retired chains.

## Considered Approaches

### 1. Relabel existing snapshot/netflow matches

This would immediately increase the displayed `Verified` count, but it would
misrepresent aggregate evidence as an event-by-event proof. It is rejected.

### 2. Brute-force every known wallet before serving active users

This would eventually improve global coverage, but Arbitrum and Berachain each
have tens of thousands of unbackfilled wallets. It would consume substantial
archive RPC capacity while active lookups remain unresolved. It is retained
only as the existing background coverage queue.

### 3. Active-first strict replay remediation

This is the selected approach. The existing global backfill continues, while a
bounded repair path prioritizes active wallets with missing, stale, inferred,
carry, or mismatched evidence. A wallet becomes `Verified` only after a complete
canonical replay and exact current-state reconciliation at one pinned block.

## Architecture

### Active-wallet selection

Extend the existing canonical selector with a strict-remediation mode.

Selection order:

1. explicitly pinned priority wallets;
2. active wallets with missing canonical history;
3. active wallets with stale canonical history;
4. active wallets whose current public ledger contains `mismatch` or
   `coverage_incomplete`;
5. active wallets whose best current evidence is `inferred` or `Carry`.

Cold backlog addresses never displace an unresolved active wallet in a bounded
strict-remediation cohort. The existing background coverage workflow continues
processing cold addresses separately.

### Canonical action history

The existing on-chain scanner remains the source of balance-changing actions.
Its strict event family is:

- deposit;
- withdrawal;
- transfer, including internal subaccount transfers;
- buy, sell, and trade;
- liquidation;
- vaporization;
- borrow and repay transitions represented by negative `Par`.

Every strict replay requires:

- `scanRange.fromBlock` at or before the configured protocol start block;
- `lastScannedBlock` at or after the pinned comparison block;
- known account numbers for every contributing action;
- no truncated shard or incomplete materialization state.

Legacy events without an account number remain under `legacy-unknown` and can
never participate in strict verification.

### On-chain interest-index evidence

Create a focused strict replay module that consumes the canonical history and
fetches only the additional evidence required for the selected cohort.

For every action block and market used by a selected wallet, fetch the
chain-specific `LogIndexUpdate` evidence from an archive-capable RPC. The event
replay uses the last index update before the action within the transaction.
Ratio-derived indexes may remain diagnostic fallback data, but are not accepted
as strict evidence.

At the snapshot comparison block, fetch:

- `getMarketCurrentIndex(marketId)` for every contributing market;
- `getAccountBalances(address, accountNumber)` for every known subaccount.

All calls use the repository's RPC endpoint loader: injected secrets first,
public endpoints second, provider rotation, bounded retry, and sanitized logs.
No endpoint or key is written into generated artifacts.

If archive state or an index log is unavailable, the market keeps its previous
`Pending`, `Inferred`, `Carry`, `Cycle`, or `mismatch` classification.

### Exact replay

The pure replay engine mirrors the dashboard's proven interest-ledger rules:

1. Order events by block, transaction index, and log index.
2. Track signed `Par`, the last exact interest index, live yield, settled supply
   yield, and settled borrow yield per `account|market`.
3. Accrue yield to the exact event index before applying the event's new `Par`.
4. Settle proportional yield on partial reductions.
5. Settle the previous side when a position reaches or crosses zero.
6. Advance open positions to the exact current index at the comparison block.
7. Classify positive balances in accounts with open debt as collateral,
   negative balances as borrow, and other positive balances as visible supply.

The replay result is compared to `getAccountBalances` for every contributing
account and market. `Par` and `Wei` must match exactly. The repair path never
changes replay state to fit the live result and never uses a snapshot supplement
to fill a missing account.

Only exact markets produce a resolved `interest-ledger` result with:

- `strictStatus: verified`;
- `snapshotIncomplete: false`;
- `subgraphReplayTruncated: false`;
- `replayStateAdjusted: false`;
- zero `Par` and `Wei` differences.

### Carry remediation

Canonical replay starts at the configured protocol deployment block, not at the
first local snapshot. Therefore, older principal and yield can be proven even
when the position predates local snapshots.

This requires archive RPC access for historical index and balance evidence. If
the configured start block is later than the first possible position event, or
the provider cannot serve historical evidence, `Carry` remains non-strict.

### Cycle proof

Remove the current “small balance” cycle heuristic. A recent cycle is emitted
only when the ordered event history contains an exact aggregate `Par == 0`
boundary before the current positive position.

The public netflow entry records `cycleStartProof: exact-zero`. Older
`recentNetFlow` values without that proof are ignored by both static and runtime
inference.

Even an exact-zero cycle remains `Cycle Inferred` when it is supported only by
snapshot/netflow evidence. Full `Verified` is granted through the exact
interest-ledger replay described above, never by the cycle label itself.

### Mismatch repair

Create a bounded scheduled workflow for active strict-remediation cohorts. It
does not rescan the entire chain. It uses already complete canonical histories,
fetches pinned archive evidence, rebuilds resolved interest ledgers, rebuilds
public verified-ledger shards, and regenerates quality status.

For a mismatched market:

- if exact replay and on-chain balances agree, publish the strict resolved
  ledger and count it as `Verified`;
- if evidence is incomplete, keep `coverage_incomplete`;
- if exact replay and on-chain balances disagree, keep `mismatch`;
- never rewrite current state, widen tolerances, clamp values, or promote
  inferred evidence.

The workflow uses the same per-chain concurrency groups as canonical head
refreshes, preventing simultaneous writers and competing RPC scans.

### Quality reporting

The global quality builder currently classifies only the static snapshot/netflow
market row. It must also recognize a validated nested resolved interest-ledger
market as strict evidence.

Raw `status`, `method`, and carry diagnostics remain unchanged. Quality
reporting prefers the validated resolved market only for `strictStatus`,
`strictMethod`, and `strictReason`.

## Data Flow

```text
latest active snapshot
        +
canonical subaccount history from protocol start
        |
        v
active strict-remediation selector
        |
        v
archive index logs + pinned current indexes + pinned account balances
        |
        v
pure exact replay
        |
        +--> incomplete evidence ------> keep Pending/Carry/Inferred
        |
        +--> exact state mismatch -----> keep mismatch
        |
        `--> exact reconciliation -----> resolved interest-ledger Verified
                                             |
                                             v
                                  public shards + quality status
```

## Error Handling

- Archive RPC failures are explicit and sanitized; they never become zero data.
- A failed address does not block publishing independently verified addresses
  from the same cohort.
- Missing index evidence rejects only affected markets.
- Partial account-balance batches reject affected markets; subgraph data may be
  used for diagnostics but not strict reconciliation.
- Workflow status reports attempted, verified, incomplete, and mismatched
  market counts.
- Generated files are written atomically and only the deterministic selected
  cohort is staged.

## Configuration and Secrets

Business rules stay in code:

- exact-zero cycle proof;
- strict replay eligibility;
- exact `Par`/`Wei` reconciliation;
- status classification.

Operational values stay in workflow inputs/config:

- cohort size;
- worker count;
- retry count;
- timeout and schedule.

RPC URLs with credentials remain GitHub Actions secrets and environment
variables. Existing public RPC fallbacks may be committed, but full credential
URLs and provider keys are never logged or serialized.

## Testing

Add focused tests before implementation for:

- active strict blockers outranking the cold backlog;
- exact-zero cycle metadata and rejection of non-zero reset heuristics;
- rejection of legacy cycle metadata without `cycleStartProof`;
- replay of deposits, withdrawals, partial reductions, transfers, trades,
  liquidations, vaporization, supply, collateral, and borrow states;
- exact event-index ordering;
- rejection of missing archive index evidence;
- exact account-level `Par`/`Wei` reconciliation;
- rejection of adjusted, supplemented, truncated, or partial replay;
- resolved strict markets overriding only the quality classification, not raw
  forensic fields;
- workflow concurrency, bounded selection, secret wiring, deterministic
  staging, and audit execution.

The final verification gate is:

```bash
python3 -m unittest \
  tests.test_select_earn_canonical_hot_addresses \
  tests.test_scan_earn_netflow \
  tests.test_build_earn_resolved_interest_ledger \
  tests.test_build_earn_verified_ledger \
  tests.test_build_earn_quality_status \
  tests.test_earn_dashboard_contracts
python3 run_earn_audit_checks.py
node --check dashboard-core.js
node --check earn/earn-core.js
```

## Acceptance Criteria

1. A bounded cohort always selects unresolved active wallets before cold
   backlog addresses.
2. Every supported balance-changing event and every contributing subaccount is
   included in strict replay.
3. A market cannot become `Verified` without full history from the configured
   chain start, exact index evidence, and exact pinned account reconciliation.
4. `Carry` becomes `Verified` only through full canonical replay that predates
   the first snapshot; unavailable old history remains `Carry`.
5. Recent-cycle inference requires a proven exact-zero boundary. A non-zero
   low-balance reset is no longer emitted or consumed.
6. A repaired mismatch is promoted only when the new exact replay matches
   on-chain `Par` and `Wei`; otherwise it remains a mismatch.
7. Raw inferred/carry provenance stays visible after a strict resolved result is
   added.
8. No retired chain receives a new scheduled long-term backfill.
9. Targeted EARN tests, the full EARN audit suite, JavaScript syntax checks, and
   workflow contract checks pass before deployment.
