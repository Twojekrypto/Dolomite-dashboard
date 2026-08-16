# veDOLO Position Activity Design

## Goal

Separate new DOLO entering veDOLO from internal veNFT management operations so `Recent DOLO Locks`, `Locked DOLO Over Time`, and the new activity view all describe economically correct events.

## Verified event semantics

The deployed `VotingEscrow` contract defines these `DepositType` values:

- `0` — deposit for an existing position; new DOLO enters escrow.
- `1` — create a position; new DOLO enters escrow.
- `2` — increase position amount; new DOLO enters escrow.
- `3` — extend unlock time; no DOLO enters escrow.
- `4` — merge; a source NFT is burned and its existing DOLO moves into a target position. Total locked DOLO does not change.
- `5` — split; existing DOLO moves from one position into a newly minted position. Total locked DOLO does not change.

A wallet-to-wallet ERC-721 transfer changes ownership only. It is not a lock or unlock.

## User-facing design

Keep `Recent DOLO Locks` and `Recent veDOLO Unlocks` as the economic-flow pair. `Recent DOLO Locks` contains only deposit types `0`, `1`, and `2`; route badges continue to distinguish direct, oDOLO, airdrop, and protocol-routed locks.

Add a compact full-width `veDOLO Position Activity` card below the two flow tables. It uses the existing Graphite + Gold table system and a segmented control with `All`, `Transfers`, `Merges`, `Splits`, and `Extensions`. The table shows six rows per page and exposes:

- the affected wallet using the canonical wallet-name renderer,
- the action type with a plain-language tooltip,
- the affected position ID,
- the amount of DOLO moved for merge/split actions without a positive-flow sign,
- the resulting lock end where applicable,
- transaction date and explorer link as secondary wallet metadata.

Transfers show the receiving wallet as the primary wallet and the sending address as secondary route context. Essential action labels and amounts remain visible without hover; tooltips add explanation only.

## Data and replay design

The generator annotates merge and split deposits with their source and target token IDs from transaction calldata. The annotation is cached and generation fails closed when an internal action cannot be resolved. Existing external transfer rows remain unchanged.

`Locked DOLO Over Time` is rebuilt with a per-token state replay:

- external deposits add principal,
- extensions update only the end date,
- merges move the source state into the target and use the longer end date,
- splits move principal into the new token without changing the aggregate,
- withdrawals remove the token state,
- transfers do not affect the aggregate.

The chart emits end-of-day active, unexpired balances and retains the holder-snapshot endpoint reconciliation. The semantic validator uses the same state-transition rules and requires complete merge/split annotations.

## Responsive and accessibility contract

The card follows existing table widths and intentionally scrolls inside its table wrapper on narrow screens rather than expanding the document. The segmented control wraps or horizontally scrolls within the card. Action filters expose `aria-pressed`; action help is keyboard focusable; no required value exists only in a hover tooltip.

## Scope

No RPC source, token amount, holder balance, default sort order of existing tables, or unrelated dashboard surface changes. Generated flow data gains only the token-transition annotations required for exact replay.
