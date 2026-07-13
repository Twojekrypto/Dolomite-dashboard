# EARN Canonical Round-Robin Design

## Goal

Make Ethereum canonical history catch-up finite and measurable. The scheduled
selector must not repeatedly choose the same high-value wallets, and the EARN
status must distinguish historical backfill completion from head freshness.

## Selection

Use each wallet history file's `lastScannedBlock` as the durable scheduling
watermark. After pinned priority addresses, select wallets in this order:

1. wallets with no canonical history,
2. wallets with the oldest `lastScannedBlock`,
3. fully current wallets only when the batch still has capacity.

Scores remain a tie-breaker inside the same watermark so active wallets retain
priority without starving older or missing wallets. This needs no new state
file and survives workflow cache loss because the watermark is committed with
the canonical history.

## Coverage Metrics

Keep head coverage and historical coverage separate:

- `freshWalletCount` / `headFreshWalletCount`: history reaches the current
  canonical target block.
- `backfilledWalletCount`: history exists and its `scanRange.fromBlock` reaches
  the chain manifest's canonical `fromBlock`.
- `incompleteBackfillWalletCount`: history exists but does not cover that
  canonical start.
- `missingWalletCount`: no canonical history file exists.

Global backfill status is complete only when every known wallet is backfilled.
Head recency continues to use block lag and remains independently bounded by
the existing two-hour policy.

## UI

The EARN freshness pill uses `backfilledWalletCount/knownAddressCount` for the
`canonical backfill` label. It must never present the moving head-fresh count as
a finite historical progress bar.

## Verification

Add regression tests for fair oldest-first selection, missing-wallet priority,
historical coverage accounting, and the dashboard data contract. Run the
targeted unit tests, the complete EARN audit suite, syntax checks, and a local
browser check of the EARN status pill.
