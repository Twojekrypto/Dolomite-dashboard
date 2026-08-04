# Identical Address Cell Highlighting Design

## Goal

Make repeated wallet activity easier to recognize without changing the meaning or visual priority of the surrounding table rows. When a user hovers or keyboard-focuses a wallet address, only address cells containing the same full EVM address in the same table should be highlighted.

This is a visual relationship aid. It must not change table data, sorting, filtering, pagination, links, copy actions, labels, or existing row-level risk and hover states.

## Audited Scope

The feature is opt-in and applies only to event- or position-level tables where one wallet can legitimately occupy more than one row.

### veDOLO

- `#exits-table` — Recent Early Exits
- `#locks-table` — Recent DOLO Locks
- `#unlocks-table` — Recent veDOLO Unlocks
- `#claimable-table` — Expired veDOLO Ready to Claim

The current snapshots confirm repeated wallets in all four datasets:

- Recent Early Exits: 7,970 rows, 602 wallets repeated
- Recent DOLO Locks: 25,173 rows, 383 wallets repeated
- Recent veDOLO Unlocks: 9,043 rows, 747 wallets repeated
- Expired veDOLO Ready to Claim: 129 current positions, 12 wallets repeated

### oDOLO

- `#tbl-latest-ex` — Latest oDOLO Exercises
- `#tbl-latest-pair` — Latest oDOLO Pair

The current all-time snapshots contain 4,243 exercise rows across 492 wallets and 4,773 pairing rows across 709 wallets. Repeated wallets are expected because these are transaction tables.

### Borrow

- `.sim-atrisk-table` — Risk Simulator wallet results
- `#positions-table` — Lending Positions
- `#liquidation-history-table` — Liquidation History

The position snapshot contains 13,234 rows across 11,022 wallets; the same wallet can have multiple accounts, subaccounts, chains, or positions. Liquidation History contains 5,180 events across 2,878 liquidated wallets.

### Supply

- `#supply-activity-table` — Asset Activity

This is an event table. A wallet can recur across deposits, withdrawals, transfers, and liquidations, and a transfer row can contain both a primary and a secondary wallet. Each address cell participates independently.

## Explicit Exclusions

Do not enable the feature for tables that aggregate to one row per address or primarily display token/market contract addresses. This includes:

- DOLO Holders and Fresh 10K+ DOLO Wallets
- veDOLO Holders
- Top oDOLO Exercisers and Claimer Breakdown
- Supplier Leaderboard
- Top users saved with current veDOLO
- Dolomite Assets, Supply Assets, Borrow Positions, Past and routed assets
- Portfolio asset and position summaries
- Selected Market and other token/market selectors

These tables do not present repeated wallet activity in the same visible table. Highlighting them would either never produce peers or imply a wallet relationship where the value is actually a token contract address.

## Interaction

1. A supported table opts in with a dedicated data attribute.
2. The interaction starts from an existing `.addr-tooltip-wrap[data-full-addr]` element inside a table cell.
3. The full address is normalized to lowercase and accepted only when it is a complete 20-byte EVM address represented by `0x` plus 40 hexadecimal characters.
4. The source cell receives the stronger gold treatment.
5. Every other visible cell in the same opted-in table containing that exact normalized address receives a quieter matching treatment.
6. Different addresses, partial strings, shortened display text, labels, and addresses in other tables do not match.
7. Leaving the source cell or moving keyboard focus away clears the state immediately.

The match is scoped to the currently rendered page of one table. There is no off-page count or cross-table highlighting.

## Visual Treatment

- Source cell: restrained gold-tinted background plus a stronger inset gold edge.
- Matching cells: softer gold-tinted background plus a subtle inset edge.
- Transition: approximately 140 ms for background and inset edge.
- No cell size, row height, column width, or layout changes.
- No full-row highlighting and no new left-side rail.
- Existing row hover, liquidation risk colors, zebra surfaces, links, and badges remain visible and keep their current semantics.

The treatment uses the existing Graphite + Gold palette and CSS custom properties where available. It must remain legible in both darker and lighter table surfaces.

## Shared Implementation

The behavior belongs in `shared-hover-tooltips.js` because the supported address renderers already expose their canonical value through `.addr-tooltip-wrap[data-full-addr]`. Shared styles belong in `shared-hover-tooltips.css`.

The helper must:

- use delegated pointer and focus events so dynamically rendered and paginated rows work automatically;
- deduplicate by `td`, because known-address cells can contain more than one address wrapper for the same wallet label and shortened hexadecimal value;
- limit queries to the nearest opted-in table;
- remove stale classes before applying a new match;
- ignore empty, malformed, or non-EVM `data-full-addr` values;
- preserve copy, DeBank, explorer, sorting, filtering, and row-click interactions.

No new dependency, generated data file, workflow change, or API request is needed.

## Keyboard and Touch Behavior

- `focusin` on an address trigger produces the same source and peer treatment as hover.
- `focusout` clears it when focus leaves that address trigger.
- Existing focusable links and buttons keep their current tab order. Non-interactive address text will not receive a new tab stop solely for this cosmetic feature.
- Touch taps must not be intercepted and must not leave a persistent stuck highlight. Existing address actions remain authoritative.

## Verification

### Source and contract checks

- Verify that only the ten audited tables carry the opt-in attribute.
- Verify that excluded aggregate and token-contract tables do not carry it.
- Verify exact full-address normalization, malformed-value rejection, per-table scoping, and `td` deduplication.

### Browser checks

Serve the dashboard with `python3 -m http.server` and validate with real browser-computed styles and bounding boxes:

- hover a repeated wallet and confirm only the source address cell and exact peer address cells change;
- confirm a different address in the same table is unchanged;
- confirm the same address in another table is unchanged;
- confirm row backgrounds and left rails remain unchanged;
- confirm layout dimensions do not change while entering or leaving the state;
- confirm sorting, filtering, pagination, copy, DeBank, and explorer actions still work;
- repeat checks after a dynamic table rerender;
- inspect desktop, laptop, tablet, and phone widths for overflow or stuck state.

## Done Criteria

The change is complete when all ten audited tables use the shared interaction, excluded tables remain untouched, targeted checks pass, browser-computed styles confirm cell-only highlighting without layout shift, and the production `master` branch is pushed successfully.
