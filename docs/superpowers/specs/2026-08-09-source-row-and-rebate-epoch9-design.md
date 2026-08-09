# Source-Row Address Highlighting and veDOLO Epoch 9 Rebate Recovery Design

**Date:** 2026-08-09

**Status:** Design approved; pending written-spec review before implementation

## Goal

Complete the repeated-wallet interaction and restore the missing veDOLO Borrow Fee Rebate period without weakening the dashboard's financial-data standards.

1. Hovering a supported table row quietly highlights the canonical address in that source row and exact copies of that address in other visible rows of the same table.
2. Hovering or focusing the address itself upgrades only the directly targeted address to the existing strong gold treatment; exact copies remain quiet peers.
3. The rebate period from 2026-07-16 through 2026-07-22 is recovered from the confirmed Dolomite epoch 9 snapshot reset rather than displayed as zero or estimated from unrelated revenue data.

## Confirmed Root Causes

### Source-row address state

`shared-hover-tooltips.js` currently excludes every address wrapper contained by the hovered source row before applying `address-match-peer`. That matched the previous design, but it conflicts with the updated requirement. The controller already distinguishes row mode from direct-address mode, so the fix is limited to which wrappers receive the quiet peer state.

### veDOLO rebate epoch 9

The rebate fetcher treats `MarketIdToMerkleRootSet.totalAmount` as an always-increasing aggregate and calculates `delta = newTotal - previousTotal`. It then discards every non-positive delta. This is normally correct for the rolling-claims contract, whose claim proofs are aggregate amounts.

Epoch 9 is a confirmed exception:

- Dolomite's initial epoch 9 finalized file, commit `f1c3a4e49daf`, was cumulative relative to epoch 8.
- Dolomite then published manual corrections in commits `8cd53fdcc62d` and `2901d1f3b62f`.
- The corrected epoch 9 totals match the epoch 9 increment from the original cumulative file for six markets exactly and the remaining four within token-unit rounding dust.
- The corrected roots were published on Berachain in transaction `0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c`, block `24055329`.
- Because the corrected roots reset every previously populated market below its epoch 8 aggregate, the current parser sees only negative deltas and omits epoch 9 entirely.
- Epoch 10 increases from the corrected epoch 9 baseline and is therefore already parsed normally.

The missing period is a source-interpretation bug, not a chart rendering bug and not an RPC coverage gap.

## Considered Approaches

### 1. Known-reset recovery from the on-chain transaction — selected

Treat the new root totals from the confirmed epoch 9 reset transaction as that epoch's rebate amounts. Keep the raw negative aggregate adjustment as provenance, but do not subtract it from historical user savings or protocol revenue a second time.

This uses the values actually published by Dolomite, has no recurring dependency on GitHub history, restores the period deterministically, and limits the exceptional interpretation to one audited transaction.

### 2. Mark epoch 9 as corrected but unavailable

This is conservative, but it leaves the requested chart gap and understates the published closed-epoch rebate total despite the official epoch values being recoverable.

### 3. Reconstruct every epoch from historical GitHub files

This can reproduce the source pipeline, but it adds a fragile runtime dependency on repository history and makes normal data refreshes dependent on file commits that may be rewritten or removed. It is unnecessary for a single confirmed on-chain reset.

## Address Interaction Design

The existing opt-in table scope remains unchanged. Matching stays inside the nearest `table[data-address-match-cells]` and requires an exact normalized `0x` plus 40 hexadecimal characters.

### Row hover

- Collect every visible canonical address wrapper in the hovered `tbody` row.
- Search the same opted-in table for all visible wrappers carrying those addresses, including wrappers in the source row.
- Apply `address-match-active address-match-peer` to one displayed address wrapper per canonical address in the source row and to matching address wrappers in other rows.
- Continue excluding label-only wrappers, hidden wrappers, malformed values, other tables, and unrelated addresses.
- If no address has a visible occurrence in another row, apply no match state; a unique source address should not look related to anything.

### Direct address hover or focus

- The directly targeted address receives `address-match-source`, preserving the current strong gold frame and glow.
- Exact visible copies receive `address-match-peer`.
- Direct-address mode replaces row mode while active and restores the appropriate row-derived state when the pointer returns to another part of the row.
- No address changes to a help cursor, and no row, cell, padding, border width, or geometry changes.

### Source-row duplicates

If a source cell contains both a label wrapper and a displayed shortened-address wrapper for the same canonical value, only the displayed address participates. If a row contains two different wallet addresses, each displayed source address can receive the quiet state when its own address has a visible peer elsewhere.

## Rebate Data Design

### Narrow exception contract

Add an audited allowlist keyed by chain and transaction hash for known aggregate-snapshot resets. The initial allowlist contains only the Berachain epoch 9 transaction.

For a listed transaction, the parser must still validate all of the following before recovery:

- decoded expected epoch is `9`;
- the transaction increments the rolling-claims epoch;
- every listed root event belongs to that transaction;
- at least two markets have a previous non-zero aggregate;
- every market with a previous non-zero aggregate resets to a smaller positive total;
- no event has a malformed or zero total.

If any validation fails, the parser must not recover a positive rebate. It records an unsupported correction and leaves the financial amount unapplied.

### Recovered epoch values

For a validated reset transaction:

- `rebateRaw` for each market equals the newly published `totalAmount`, not the negative aggregate delta;
- the normal historical token-price resolver values that amount at the event timestamp;
- the epoch row receives `calculationMode: "known_epoch_snapshot_reset"`;
- the row includes the source transaction hash, reset market count, and aggregate-adjustment metadata;
- `rebateUSD` remains a positive closed-epoch rebate and is allocated across the epoch's seven earning days by the existing borrow-interest weighting;
- the rebate is included once in cumulative user savings and once in net-revenue deductions.

Normal increasing roots keep the existing delta behavior. Unknown negative or mixed corrections are never promoted automatically.

### Financial effects

Recovering epoch 9 changes more than the chart:

- the daily veDOLO Borrow Fee Rebates bars for 2026-07-16 through 2026-07-22 become non-zero;
- cumulative saved increases by the recovered epoch total;
- Berachain and protocol-wide net revenue for those dates decreases by the same applied rebate, capped by gross protocol revenue as today;
- gross borrow interest, supply-side revenue, and unrelated chains remain unchanged;
- 7D, 30D, and all-time revenue/rebate windows recompute from the corrected daily series.

## UI Treatment

Recovered epoch 9 bars use the normal published-data color because the amount is sourced from a published on-chain root, not an estimate. The tooltip adds a concise provenance row such as `Source · Published epoch snapshot reset` for affected days. It does not show a warning color that could be confused with pending data.

Dates at or after `latestRebateDate` remain striped and labeled pending until Dolomite publishes the corresponding rolling-claims data. The fix must not turn 2026-07-30 onward into zero-value published epochs.

## Error Handling and Preservation

- RPC failure continues to preserve the previous closed-epoch rebate dataset.
- Previous audited max-rebate fields continue to merge into refreshed epoch rows.
- A previously recovered known-reset epoch must not disappear when a later refresh temporarily cannot read its source transaction.
- Unknown aggregate decreases are surfaced in output metadata as unsupported corrections and are excluded from revenue netting.
- Validation must reject duplicate application of the known reset, mixed positive/negative root sets, incorrect epoch metadata, and a known hash observed on the wrong chain.

## Testing

### Address behavior

The Node behavior test will first fail while asserting that row hover quietly styles the source displayed address and its exact peers. It will also cover direct-address priority, two-address rows, unique addresses, hidden wrappers, dynamically rendered rows, exit cleanup, touch cleanup, focus, scroll/resize reconciliation, and table isolation.

### Rebate parser and accounting

Python tests will use controlled root-event fixtures to prove:

- the known epoch 9 full reset produces positive per-market rebate amounts from the new totals;
- an identical reset under an unknown transaction hash produces no positive rebate;
- a mixed reset is rejected;
- normal cumulative increases remain unchanged;
- the recovered rebate is applied once to the seven earning days and netted once from revenue;
- output provenance and validator contracts accept the known recovery and reject unsupported promotion;
- fallback preservation keeps recovered epoch metadata.

### Browser verification

Serve the dashboard with `python3 -m http.server` and inspect at desktop and phone widths:

- a supported row highlights its own repeated address quietly plus identical addresses in other rows;
- direct address hover makes only the pointer target strong;
- address, cell, row, and table bounding boxes do not move;
- the veDOLO chart shows published data for 2026-07-16 through 2026-07-22;
- the affected tooltip exposes reset provenance;
- 2026-07-30 onward remains pending;
- chart, summary, and generated JSON totals reconcile exactly.

## Deployment

Advance the shared hover asset cache key and revenue page cache key, run targeted and full relevant checks, commit the implementation on the feature branch, publish it through a pull request to `master`, wait for required checks, and verify the GitHub Pages deployment with a unique query parameter.

## Done Criteria

The work is complete when the source address receives quiet row-derived highlighting only when an exact peer exists, direct address hover remains stronger, epoch 9 is recovered only through the audited transaction-specific rule, generated revenue totals reconcile, targeted tests and browser checks pass, and the live `master` deployment is verified.
